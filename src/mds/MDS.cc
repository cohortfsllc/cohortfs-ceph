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
    last_tid(0)
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

  r = mkfs();
  if (r) {
    lderr(cct) << "mkfs failed with " << r << dendl;
    shutdown();
    return r;
  }

  // start beacon timer
  beacon_timer.add_event(cct->_conf->mds_beacon_interval,
                         &MDS::beacon_send, this);
  return 0;
}

int MDS::mkfs()
{
  if (cache || storage)
    return -EINVAL;

  // create the storage and cache
  std::unique_ptr<Storage> s(new Storage(gc));
  std::unique_ptr<Cache> c(new Cache(gc, s.get(),
                                     cct->_conf->mds_cache_highwater,
                                     cct->_conf->mds_cache_lowwater));

  // create the root directory inode
  const identity who = {0, 0, 0};
  auto root = c->create(who, S_IFDIR);
  assert(root);

  std::swap(s, storage);
  std::swap(c, cache);
  return 0;
}

void MDS::shutdown()
{
  cache.reset();
  storage.reset();
  objecter->shutdown();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}

void MDS::handle_signal(int signum)
{
  // XXX suicide
}

int MDS::create(const boost::uuids::uuid &volume, _inodeno_t parent,
                const char *name, const identity &who, int type)
{
  if (!cache)
    return -ENODEV;

  // find the parent object
  auto p = cache->get(parent);
  if (!p)
    return -ENOENT;

  if (!p->is_dir())
    return -ENOTDIR;

  // create the child object
  auto inode = cache->create(who, type);

  // link the parent to the child
  int r = p->link(name, inode->ino());
  if (r)
    inode->destroy(storage.get());
  return r;
}

int MDS::unlink(const boost::uuids::uuid &volume,
                _inodeno_t parent, const char *name)
{
  if (!cache)
    return -ENODEV;

  // find the parent object
  auto p = cache->get(parent);
  if (!p)
    return -ENOENT;

  // unlink the child from its parent
  InodeRef inode;
  int r = p->unlink(name, cache.get(), &inode);
  if (r)
    return r;

  // update inode nlinks
  if (inode->adjust_nlinks(-1) == 0)
    inode->destroy(storage.get());

  return 0;
}

int MDS::lookup(const boost::uuids::uuid &volume, _inodeno_t parent,
                const char *name, _inodeno_t *ino)
{
  if (!cache)
    return -ENODEV;

  // find the parent object
  auto p = cache->get(parent);
  if (!p)
    return -ENOENT;

  // find the child object
  return p->lookup(name, ino);
}

int MDS::readdir(const boost::uuids::uuid &volume, _inodeno_t ino,
                 uint64_t pos, uint64_t gen, libmds_readdir_fn cb, void *user)
{
  if (!cache)
    return -ENODEV;

  // find the object
  auto inode = cache->get(ino);
  if (!inode)
    return -ENOENT;

  return inode->readdir(pos, gen, cb, user);
}

int MDS::getattr(const boost::uuids::uuid &volume, _inodeno_t ino,
                 int mask, ObjAttr &attr)
{
  if (!cache)
    return -ENODEV;

  // find the object
  auto inode = cache->get(ino);
  if (!inode)
    return -ENOENT;

  return inode->getattr(mask, attr);
}

int MDS::setattr(const boost::uuids::uuid &volume, _inodeno_t ino,
                 int mask, const ObjAttr &attr)
{
  if (!cache)
    return -ENODEV;

  // find the object
  auto inode = cache->get(ino);
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
