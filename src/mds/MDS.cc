// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "MDSMap.h"
#include "MDS.h"
#include "Inode.h"
#include "Cache.h"
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
  beacon_timer.add_event(cct->_conf->mds_beacon_interval,
                         &MDS::beacon_send, this);
  return 0;
}

void MDS::shutdown()
{
  objecter->shutdown();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}

void MDS::handle_signal(int signum)
{
  // XXX suicide
}

cohort::mds::VolumeRef MDS::get_volume(libmds_volume_t volume)
{
  auto p = reinterpret_cast<const boost::uuids::uuid*>(volume);
  auto vol = volumes.get_or_create(*p);
  // TODO: look up the volume in the osd map and attach it
  if (vol)
    vol->mkfs(gc, inode_cache, storage.get(), cct->_conf);
  return vol;
}

int MDS::create(const libmds_fileid_t *parent, const char *name,
                const identity &who, int type)
{
  auto vol = get_volume(parent->volume);
  if (!vol)
    return -ENODEV;

  // find the parent object
  auto p = vol->cache->get(parent->ino);
  if (!p)
    return -ENOENT;

  if (!p->is_dir())
    return -ENOTDIR;

  // create the child object
  auto inode = vol->cache->create(who, type);

  // link the parent to the child
  int r = p->link(name, inode->ino());
  if (r)
    inode->destroy(storage.get());
  return r;
}

int MDS::unlink(const libmds_fileid_t *parent, const char *name)
{
  auto vol = get_volume(parent->volume);
  if (!vol)
    return -ENODEV;

  // find the parent object
  auto p = vol->cache->get(parent->ino);
  if (!p)
    return -ENOENT;

  // unlink the child from its parent
  InodeRef inode;
  int r = p->unlink(name, vol->cache.get(), &inode);
  if (r)
    return r;

  // update inode nlinks
  if (inode->adjust_nlinks(-1) == 0)
    inode->destroy(storage.get());

  return 0;
}

int MDS::lookup(const libmds_fileid_t *parent, const char *name,
                libmds_ino_t *ino)
{
  auto vol = get_volume(parent->volume);
  if (!vol)
    return -ENODEV;

  // find the parent object
  auto p = vol->cache->get(parent->ino);
  if (!p)
    return -ENOENT;

  // find the child object
  return p->lookup(name, ino);
}

int MDS::readdir(const libmds_fileid_t *dir, uint64_t pos, uint64_t gen,
                 libmds_readdir_fn cb, void *user)
{
  auto vol = get_volume(dir->volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get(dir->ino);
  if (!inode)
    return -ENOENT;

  return inode->readdir(pos, gen, cb, user);
}

int MDS::getattr(const libmds_fileid_t *file, int mask, ObjAttr &attr)
{
  auto vol = get_volume(file->volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get(file->ino);
  if (!inode)
    return -ENOENT;

  return inode->getattr(mask, attr);
}

int MDS::setattr(const libmds_fileid_t *file, int mask, const ObjAttr &attr)
{
  auto vol = get_volume(file->volume);
  if (!vol)
    return -ENODEV;

  // find the object
  auto inode = vol->cache->get(file->ino);
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

  beacon_timer.reschedule_me(cct->_conf->mds_beacon_interval);
}
