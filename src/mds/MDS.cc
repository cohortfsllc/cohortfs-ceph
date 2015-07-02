// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/thread/lock_algorithms.hpp>

#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "MDSMap.h"
#include "MDS.h"
#include "Cache.h"
#include "Dentry.h"
#include "Dir.h"
#include "Inode.h"
#include "Storage.h"
#include "messages/MMDSBeacon.h"

#define dout_subsys ceph_subsys_mds

using namespace cohort::mds;

MDS::MDS(int whoami, Messenger *m, MonClient *mc)
  : Dispatcher(m->cct),
    whoami(whoami),
    volume_cache(gc, sizeof(Volume), "volumes"),
    inode_cache(gc, sizeof(Inode), "inodes"),
    inode_storage_cache(gc, sizeof(InodeStorage), "inode_store"),
    dir_cache(gc, sizeof(Inode), "dirs"),
    dir_storage_cache(gc, sizeof(InodeStorage), "dir_store"),
    dentry_cache(gc, sizeof(Dentry), "dentries"),
    messenger(m),
    monc(mc),
    objecter(new Objecter(cct, messenger, monc,
                          cct->_conf->rados_mon_op_timeout,
                          cct->_conf->rados_osd_op_timeout)),
    mdsmap(cct),
    beacon_last_seq(0),
    last_state(0),
    state(0),
    want_state(0),
    last_tid(0),
    volumes(gc, volume_cache)
{
}

MDS::~MDS()
{
  delete objecter;
  delete monc;
  delete messenger;
}

int MDS::init()
{
  messenger->add_dispatcher_tail(objecter);
  messenger->start();

  monc->set_messenger(messenger);
  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  int r = monc->init();
  if (r) {
    lderr(cct) << "MonClient::init failed with " << r << dendl;
    shutdown();
    return r;
  }

  objecter->start();
  monc->renew_subs();

  storage.reset(new Storage(gc, inode_storage_cache, dir_storage_cache));

  // start beacon timer
  beacon_timer.reset(new cohort::Timer<ceph::mono_clock>);
  beacon_timer->add_event(cct->_conf->mds_beacon_interval,
                          &MDS::beacon_send, this);
  return 0;
}

void MDS::shutdown()
{
  beacon_timer.reset();
  objecter->shutdown();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}

void MDS::handle_signal(int signum)
{
  // XXX suicide
}

cohort::mds::VolumeRef MDS::get_volume(const mcas::gc_guard &guard,
                                       volume_t volume)
{
  auto p = reinterpret_cast<const boost::uuids::uuid*>(volume);
  auto vol = volumes.get_or_create(guard, *p);
  // TODO: look up the volume in the osd map and attach it
  if (vol)
    vol->mkfs(gc, guard, inode_cache, dir_cache, dentry_cache,
              storage.get(), cct->_conf);
  return vol;
}

uint32_t MDS::pick_dir_stripe(const std::string &name) const
{
  if (cct->_conf->mds_dir_stripes < 2)
    return 0;
  auto hash = ceph_str_hash(CEPH_STR_HASH_XXHASH, name.c_str(), name.size());
  return hash % cct->_conf->mds_dir_stripes;
}

int MDS::get_root(volume_t volume, ino_t *ino)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, volume);
  if (!vol)
    return -ENODEV;

  *ino = vol->cache->get_root()->ino();
  return 0;
}

int MDS::create(const fileid_t &parent, const std::string &name,
                int mode, const identity_t &who, ino_t *ino, struct stat *st)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent.volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the parent directory
  auto dir = cache->get_dir(guard, parent.ino, pick_dir_stripe(name));
  if (!dir)
    return -ENOENT;

  // look up the directory entry
  auto dn = cache->lookup(guard, dir.get(), name, true);

  // lock cache objects
  std::lock(dir->mutex, dn->mutex);
  std::lock_guard<std::mutex>
      dirlock(dir->mutex, std::adopt_lock),
      dnlock(dn->mutex, std::adopt_lock);

  if (dn->is_valid())
    return -EEXIST;

  // create the child object
  uint32_t stripes = S_ISDIR(mode) ? cct->_conf->mds_dir_stripes : 0;
  auto inode = cache->create_inode(guard, who, mode, stripes);

  // create storage for directory stripes
  std::vector<DirStorageRef> dirs(stripes);
  for (uint32_t i = 0; i < stripes; i++)
    dirs[i] = storage->get_or_create_dir(guard, vol->get_uuid(),
                                         inode->ino(), i);

  // link the parent to the child
  int r = dir->link(name, inode->ino());
  if (r == 0) {
    // on success, update the directory entry
    dn->link(inode->ino());
  } else {
    // on failure, destroy the inode we created
    std::lock_guard<std::mutex> ilock(inode->mutex);
    inode->destroy(guard, storage.get());
    for (auto &dir : dirs)
      storage->destroy(guard, std::move(dir));
  }
  return r;
}

int MDS::link(const fileid_t &parent, const std::string &name, ino_t ino)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent.volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the parent directory
  auto dir = cache->get_dir(guard, parent.ino, pick_dir_stripe(name));
  if (!dir)
    return -ENOENT;

  // find the target inode
  auto inode = cache->get_inode(guard, ino);
  if (!inode)
    return -ENOENT;

  // look up the directory entry
  auto dn = cache->lookup(guard, dir.get(), name, true);

  // lock cache objects
  std::lock(dir->mutex, dn->mutex, inode->mutex);
  std::lock_guard<std::mutex>
      dirlock(dir->mutex, std::adopt_lock),
      dnlock(dn->mutex, std::adopt_lock),
      ilock(inode->mutex, std::adopt_lock);

  // link the parent to the inode
  int r = dir->link(name, ino);
  if (r == 0) {
    inode->adjust_nlinks(1);
    dn->link(ino);
  }
  return r;
}

int MDS::rename(const fileid_t &src_parent, const std::string &src_name,
                const fileid_t &dst_parent, const std::string &dst_name)
{
  if (!std::equal(src_parent.volume, src_parent.volume + LIBMDS_VOLUME_LEN,
                  dst_parent.volume))
    return -EXDEV;

  const bool same_parent = src_parent.ino == dst_parent.ino;
  const bool same_name = src_name == dst_name;

  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, src_parent.volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the initial parent object
  auto srcdir = cache->get_dir(guard, src_parent.ino,
                               pick_dir_stripe(src_name));
  if (!srcdir)
    return -ENOENT;

  // find the initial directory entry
  auto srcdn = cache->lookup(guard, srcdir.get(), src_name);
  if (!srcdn)
    return -ENOENT;

  // find the destination parent object
  auto dstdir = same_parent ? srcdir :
      cache->get_dir(guard, dst_parent.ino, pick_dir_stripe(dst_name));
  if (!dstdir)
    return -ENOENT;

  // look up the destination entry
  auto dstdn = same_parent && same_name ? srcdn :
      cache->lookup(guard, dstdir.get(), dst_name, true);

  // lock cache objects
  std::unique_lock<std::mutex>
      srcdirlock(srcdir->mutex, std::defer_lock),
      srcdnlock(srcdn->mutex, std::defer_lock),
      dstdirlock(dstdir->mutex, std::defer_lock),
      dstdnlock(dstdn->mutex, std::defer_lock);
  if (same_parent) {
    if (same_name)
      std::lock(srcdirlock, srcdnlock);
    else
      std::lock(srcdirlock, srcdnlock, dstdnlock);
  } else
    std::lock(srcdirlock, srcdnlock, dstdirlock, dstdnlock);

  // add a link to the destination
  int r = dstdir->link(dst_name, srcdn->ino());
  if (r)
    return r;

  // unlink the initial directory entry
  r = srcdir->unlink(src_name);
  if (r == 0) {
    // on success, link up the destination directory entry
    dstdn->link(srcdn->ino());
  } else {
    // on failure, unlink the new entry and remove from the cache
    dstdir->unlink(dst_name);
  }
  return r;
}

int MDS::unlink(const fileid_t &parent, const std::string &name)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent.volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  std::vector<std::unique_lock<std::mutex>> locks;

  // find the parent directory
  auto dir = cache->get_dir(guard, parent.ino, pick_dir_stripe(name));
  if (!dir)
    return -ENOENT;
  locks.emplace_back(dir->mutex, std::defer_lock);

  // look up the directory entry
  auto dn = cache->lookup(guard, dir.get(), name);
  if (!dn)
    return -ENOENT;
  locks.emplace_back(dn->mutex, std::defer_lock);

  // find the child object
  auto inode = cache->get_inode(guard, dn->ino());
  if (!inode)
    return -ENOENT;
  locks.emplace_back(inode->mutex, std::defer_lock);

  // fetch all of the child's stripes
  std::vector<DirRef> subdirs(inode->get_stripes());
  for (uint32_t i = 0; i < inode->get_stripes(); i++) {
    auto subdir = cache->get_dir(guard, dn->ino(), i);
    locks.emplace_back(subdir->mutex, std::defer_lock);
    subdirs[i].swap(subdir);
  }

  // lock cache objects
  boost::lock(locks.begin(), locks.end());

  // verify all subdirs are empty
  for (auto &subdir : subdirs)
    if (subdir->is_dir_notempty())
      return -ENOTEMPTY;

  dir->unlink(name);
  dn->unlink();

  // update inode nlinks
  if (inode->adjust_nlinks(-1) == 0)
    inode->destroy(guard, storage.get());
  // destroy subdirs
  for (auto &subdir : subdirs)
    subdir->destroy(guard, storage.get());
  return 0;
}

int MDS::lookup(const fileid_t &parent, const std::string &name, ino_t *ino)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent.volume);
  if (!vol)
    return -ENODEV;

  // find the parent object
  auto dn = vol->cache->lookup(guard, parent.ino, pick_dir_stripe(name), name);
  if (!dn)
    return -ENOENT;

  *ino = dn->ino();
  return 0;
}

int MDS::readdir(const fileid_t &dir, uint64_t pos, uint64_t gen,
                 libmds_readdir_fn cb, void *user)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, dir.volume);
  if (!vol)
    return -ENODEV;

  int stripe = pos >> 32;
  int r = -EOF;
  int successes = 0;
  while (stripe < cct->_conf->mds_dir_stripes) {
    // find the directory
    auto d = vol->cache->get_dir(guard, dir.ino, stripe);
    if (!d)
      return -ENOENT;

    r = d->readdir(pos, gen, cb, user);
    if (r && r != -EOF)
      return r;
    if (r == 0)
      successes++;

    stripe++;
    pos = static_cast<uint64_t>(stripe) << 32;
  }
  return successes ? 0 : r;
}

int MDS::getattr(const fileid_t &file, int mask, ObjAttr &attr)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, file.volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get_inode(guard, file.ino);
  if (!inode)
    return -ENOENT;

  return inode->getattr(mask, attr);
}

int MDS::setattr(const fileid_t &file, int mask, const ObjAttr &attr)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, file.volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get_inode(guard, file.ino);
  if (!inode)
    return -ENOENT;

  return inode->setattr(mask, attr);
}

bool MDS::ms_dispatch(Message *m)
{
  bool ret = true;
  if (ret) {
    m->put();
  }
  return ret;
}

bool MDS::ms_handle_reset(Connection *con)
{
  // dout(5) << "ms_handle_reset on " << con->get_peer_addr() << dendl;
  switch(con->get_peer_type()) {
    case CEPH_ENTITY_TYPE_OSD:
      objecter->ms_handle_reset(con);
      break;
    case CEPH_ENTITY_TYPE_CLIENT:
      // XXX handle session here
      messenger->mark_down(con);
      break;
  }
  return false;
}

void MDS::ms_handle_remote_reset(Connection *con)
{
  // dout(5) << "ms_handle_remote_reset on " << con->get_peer_addr() << dendl;
  switch(con->get_peer_type()) {
    case CEPH_ENTITY_TYPE_OSD:
      objecter->ms_handle_reset(con);
      break;
    case CEPH_ENTITY_TYPE_CLIENT:
      // XXX handle session here
      messenger->mark_down(con);
      break;
  }
}

#if 0
void MDS::ms_handle_connect(Connection *con)
{
  // dout(5) << "ms_handle_connect on " << con->get_peer_addr() << dendl;
  objecter->ms_handle_connect(con);
}

void MDS::ms_handle_accept(Connection *con)
{
  // XXX if existing session, send any queued messages
}
#endif

void MDS::request_state(int s)
{
  dout(3) << "request_state " << ceph_mds_state_name(s) << dendl;
  want_state = s;
  beacon_send();
}

void MDS::beacon_send()
{
  ++beacon_last_seq;
  dout(10) << "beacon_send " << ceph_mds_state_name(want_state)
           << " seq " << beacon_last_seq
           << " (currently " << ceph_mds_state_name(state) << ")" << dendl;

  const std::string &name = messenger->cct->_conf->name.get_id();
  MMDSBeacon *beacon = new MMDSBeacon(monc->get_fsid(), monc->get_global_id(),
                                      name, mdsmap.get_epoch(),
                                      want_state, beacon_last_seq);
  monc->send_mon_message(beacon);

  beacon_timer->reschedule_me(cct->_conf->mds_beacon_interval);
}
