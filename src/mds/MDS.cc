// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "MDSMap.h"
#include "MDS.h"
#include "Cache.h"
#include "Dentry.h"
#include "Inode.h"
#include "Storage.h"
#include "messages/MMDSBeacon.h"

#define dout_subsys ceph_subsys_mds

using namespace cohort::mds;

MDS::MDS(int whoami, Messenger *m, MonClient *mc)
  : Dispatcher(m->cct),
    whoami(whoami),
    volume_cache(gc, sizeof(Volume), "volumes"),
    storage_cache(gc, sizeof(InodeStorage), "inode_store"),
    inode_cache(gc, sizeof(Inode), "inodes"),
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

  storage.reset(new Storage(gc, storage_cache));

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
    vol->mkfs(gc, guard, inode_cache, dentry_cache, storage.get(), cct->_conf);
  return vol;
}

int MDS::create(const fileid_t *parent, const std::string &name,
                const identity &who, int type)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent->volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the parent object
  auto p = cache->get(guard, parent->ino);
  if (!p)
    return -ENOENT;

  if (!p->is_dir())
    return -ENOTDIR;

  // look up the directory entry
  auto dn = cache->lookup(guard, p.get(), name, true);

  // lock cache objects
  std::lock(p->dir_mutex, dn->mutex);
  std::lock_guard<std::mutex>
      plock(p->dir_mutex, std::adopt_lock),
      dnlock(dn->mutex, std::adopt_lock);

  if (dn->is_valid())
    return -EEXIST;

  // create the child object
  auto inode = cache->create(guard, who, type);

  // link the parent to the child
  int r = p->link(name, inode->ino());
  if (r == 0) {
    // on success, update the directory entry
    dn->link(inode->ino());
  } else {
    // on failure, destroy the inode we created
    std::lock_guard<std::mutex> ilock(inode->mutex);
    inode->destroy(guard, storage.get());
  }
  return r;
}

int MDS::link(const fileid_t *parent, const std::string &name, ino_t ino)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent->volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the parent object
  auto p = cache->get(guard, parent->ino);
  if (!p)
    return -ENOENT;
  if (!p->is_dir())
    return -ENOTDIR;

  // find the target inode
  auto inode = cache->get(guard, ino);
  if (!inode)
    return -ENOENT;

  // look up the directory entry
  auto dn = cache->lookup(guard, p.get(), name, true);

  // lock cache objects
  std::lock(p->dir_mutex, dn->mutex, inode->mutex);
  std::lock_guard<std::mutex>
      plock(p->dir_mutex, std::adopt_lock),
      dnlock(dn->mutex, std::adopt_lock),
      ilock(inode->mutex, std::adopt_lock);

  // link the parent to the inode
  int r = p->link(name, ino);
  if (r == 0) {
    inode->adjust_nlinks(1);
    dn->link(ino);
  }
  return r;
}

int MDS::rename(const fileid_t *src_parent, const std::string &src_name,
                const fileid_t *dst_parent, const std::string &dst_name)
{
  if (!std::equal(src_parent->volume, src_parent->volume + LIBMDS_VOLUME_LEN,
                  dst_parent->volume))
    return -EXDEV;

  const bool same_parent = src_parent->ino == dst_parent->ino;

  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, src_parent->volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the initial parent object
  auto srcp = cache->get(guard, src_parent->ino);
  if (!srcp)
    return -ENOENT;

  // find the initial directory entry
  auto srcdn = cache->lookup(guard, srcp.get(), src_name);
  if (!srcdn)
    return -ENOENT;

  // find the destination parent object
  auto dstp = same_parent ? srcp : cache->get(guard, dst_parent->ino);
  if (!dstp)
    return -ENOENT;

  // look up the destination entry
  auto dstdn = cache->lookup(guard, dstp.get(), dst_name, true);

  // lock cache objects
  std::unique_lock<std::mutex>
      srcplock(srcp->dir_mutex, std::defer_lock),
      srcdnlock(srcdn->mutex, std::defer_lock),
      dstplock(dstp->dir_mutex, std::defer_lock),
      dstdnlock(dstdn->mutex, std::defer_lock);
  if (same_parent)
    std::lock(srcplock, srcdnlock, dstdnlock);
  else
    std::lock(srcplock, srcdnlock, dstplock, dstdnlock);

  // add a link to the destination
  int r = dstp->link(dst_name, srcdn->ino());
  if (r)
    return r;

  // unlink the initial directory entry
  r = srcp->unlink(src_name);
  if (r == 0) {
    // on success, link up the destination directory entry
    dstdn->link(srcdn->ino());
  } else {
    // on failure, unlink the new entry and remove from the cache
    dstp->unlink(dst_name);
  }
  return r;
}

int MDS::unlink(const fileid_t *parent, const std::string &name)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent->volume);
  if (!vol)
    return -ENODEV;
  auto cache = vol->cache.get();

  // find the parent object
  auto p = cache->get(guard, parent->ino);
  if (!p)
    return -ENOENT;

  // look up the directory entry
  auto dn = cache->lookup(guard, p.get(), name);
  if (!dn)
    return -ENOENT;

  // find the child object
  auto inode = cache->get(guard, dn->ino());
  if (!inode)
    return -ENOENT;

  // lock cache objects
  std::lock(p->dir_mutex, dn->mutex, inode->mutex);
  std::lock_guard<std::mutex>
      plock(p->dir_mutex, std::adopt_lock),
      dnlock(dn->mutex, std::adopt_lock),
      ilock(inode->mutex, std::adopt_lock);

  if (inode->is_dir_notempty())
    return -ENOTEMPTY;

  p->unlink(name);
  dn->unlink();

  // update inode nlinks
  if (inode->adjust_nlinks(-1) == 0)
    inode->destroy(guard, storage.get());

  return 0;
}

int MDS::lookup(const fileid_t *parent, const std::string &name,
                ino_t *ino)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, parent->volume);
  if (!vol)
    return -ENODEV;

  // find the parent object
  auto dn = vol->cache->lookup(guard, parent->ino, name);
  if (!dn)
    return -ENOENT;

  *ino = dn->ino();
  return 0;
}

int MDS::readdir(const fileid_t *dir, uint64_t pos, uint64_t gen,
                 libmds_readdir_fn cb, void *user)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, dir->volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get(guard, dir->ino);
  if (!inode)
    return -ENOENT;

  return inode->readdir(pos, gen, cb, user);
}

int MDS::getattr(const fileid_t *file, int mask, ObjAttr &attr)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, file->volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get(guard, file->ino);
  if (!inode)
    return -ENOENT;

  return inode->getattr(mask, attr);
}

int MDS::setattr(const fileid_t *file, int mask, const ObjAttr &attr)
{
  mcas::gc_guard guard(gc);

  auto vol = get_volume(guard, file->volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get(guard, file->ino);
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
