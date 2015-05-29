// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "MDSMap.h"
#include "MDS.h"
#include "FSObj.h"
#include "messages/MMDSBeacon.h"

#define dout_subsys ceph_subsys_mds

using namespace cohort::mds;

class MDS::Cache {
 private:
  std::atomic<_inodeno_t> next;
  std::unordered_map<_inodeno_t, std::unique_ptr<FSObj>> inodes;
 public:
  Cache() : next(0) {}

  _inodeno_t get_next() { return next++; }

  FSObj* find(_inodeno_t ino) const {
    auto i = inodes.find(ino);
    return i == inodes.end() ? nullptr : i->second.get();
  }
  bool insert(_inodeno_t ino, std::unique_ptr<FSObj> &&obj) {
    return inodes.insert(std::make_pair(ino, std::move(obj))).second;
  }
  void destroy(_inodeno_t ino) {
    auto i = inodes.find(ino);
    if (i != inodes.end())
      inodes.erase(i);
  }
};

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
  if (cache)
    return -EINVAL;

  // create the cache
  cache.reset(new Cache);

  // create the root directory inode
  const _inodeno_t ino = cache->get_next(); // 0
  const identity who = {0, 0, 0};
  cache->insert(ino, std::unique_ptr<FSObj>(new FSObj(ino, who, S_IFDIR)));
  return 0;
}

void MDS::shutdown()
{
  cache.reset();
  objecter->shutdown();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}

void MDS::handle_signal(int signum)
{
  // XXX suicide
}

int MDS::create(_inodeno_t parent, const char *name,
                const identity &who, int type)
{
  if (!cache)
    return -ENODEV;

  // find the parent object
  FSObj *p = cache->find(parent);
  if (p == nullptr)
    return -ENOENT;

  if (!p->is_dir())
    return -ENOTDIR;

  // create the child object
  _inodeno_t ino = cache->get_next();
  std::unique_ptr<FSObj> obj(new FSObj(ino, who, type));

  // link the parent to the child
  int r = p->link(name, obj.get());
  if (r == 0)
    cache->insert(ino, std::move(obj));
  return r;
}

int MDS::unlink(_inodeno_t parent, const char *name)
{
  if (!cache)
    return -ENODEV;

  // find the parent object
  FSObj *p = cache->find(parent);
  if (p == nullptr)
    return -ENOENT;

  // unlink the child from its parent
  FSObj *obj;
  int r = p->unlink(name, &obj);
  if (r)
    return r;

  // last link?
  if (obj->adjust_nlinks(-1) == 0)
    cache->destroy(obj->ino);
  return 0;
}

int MDS::lookup(_inodeno_t parent, const char *name, _inodeno_t *ino)
{
  if (!cache)
    return -ENODEV;

  // find the parent object
  FSObj *p = cache->find(parent);
  if (p == nullptr)
    return -ENOENT;

  // find the child object
  FSObj *obj;
  int r = p->lookup(name, &obj);
  if (r)
    return r;

  *ino = obj->ino;
  return 0;
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
