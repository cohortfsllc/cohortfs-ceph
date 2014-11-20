// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <atomic>
#include <cassert>
#include <iostream>
#include <string>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/string_generator.hpp>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <pthread.h>
#include <errno.h>

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "include/buffer.h"
#include "include/stringify.h"

#include "messages/MWatchNotify.h"
#include "messages/MLog.h"
#include "msg/SimpleMessenger.h"
#ifdef HAVE_XIO
#include "msg/XioMessenger.h"
#include "msg/QueueStrategy.h"
#endif

// needed for static_cast
#include "messages/PaxosServiceMessage.h"
#include "messages/MStatfsReply.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "AioCompletionImpl.h"
#include "IoCtxImpl.h"
#include "RadosClient.h"


#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

std::atomic<uint64_t> rados_instance;

bool librados::RadosClient::ms_get_authorizer(int dest_type,
					      AuthAuthorizer **authorizer,
					      bool force_new) {
  //ldout(cct, 0) << "RadosClient::ms_get_authorizer type=" << dest_type << dendl;
  /* monitor authorization is being handled on different layer */
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;
  *authorizer = monclient.auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

librados::RadosClient::RadosClient(CephContext *cct_)
  : Dispatcher(cct_),
    cct(cct_->get()),
    conf(cct_->_conf),
    state(DISCONNECTED),
    monclient(cct_),
    messenger(NULL),
    instance_id(0),
    objecter(NULL),
    osdmap_epoch(0),
    timer(cct, lock),
    refcnt(1),
    log_last_version(0), log_cb(NULL), log_cb_arg(NULL),
    finisher(cct),
    max_watch_cookie(0)
{
}

boost::uuids::uuid librados::RadosClient::lookup_volume(const string& name)
{
  Mutex::Locker l(lock);

  int r = wait_for_osdmap();
  if (r < 0)
    return boost::uuids::nil_uuid();
  VolumeRef v;
  if (!osdmap.find_by_name(name, v)) {
    return boost::uuids::nil_uuid();
  }
  return v->id;
}

string librados::RadosClient::lookup_volume(const boost::uuids::uuid& id)
{
  Mutex::Locker l(lock);

  int r = wait_for_osdmap();
  if (r < 0)
    return string();
  VolumeRef v;
  if (!osdmap.find_by_uuid(id, v)) {
    return string();
  }
  return v->name;
}

int librados::RadosClient::get_fsid(std::string *s)
{
  if (!s)
    return -EINVAL;
  Mutex::Locker l(lock);
  ostringstream oss;
  oss << monclient.get_fsid();
  *s = oss.str();
  return 0;
}

int librados::RadosClient::ping_monitor(const string mon_id, string *result)
{
  int err = 0;
  /* If we haven't yet connected, we have no way of telling whether we
   * already built monc's initial monmap.  IF we are in CONNECTED state,
   * then it is safe to assume that we went through connect(), which does
   * build a monmap.
   */
  if (state != CONNECTED) {
    ldout(cct, 10) << __func__ << " build monmap" << dendl;
    err = monclient.build_initial_monmap();
  }
  if (err < 0) {
    return err;
  }

  err = monclient.ping_monitor(mon_id, result);
  return err;
}

int librados::RadosClient::connect()
{
  common_init_finish(cct);

  int err;
  uint64_t nonce;

  // already connected?
  if (state == CONNECTING)
    return -EINPROGRESS;
  if (state == CONNECTED)
    return -EISCONN;
  state = CONNECTING;

  /* dout and friends use g_ceph_context to print stuff.  If we want
   * to use dout to debug library calls, these have to be set.	There's
   * no way for python code (ie, the ceph command) to set them (or know
   * that it should).  Eventually, should use ldout(cct,x)
   * everywhere (and make sure cct is passed more places).  For now, ...
   */
  if (!g_ceph_context) g_ceph_context = cct;

  // get monmap
  err = monclient.build_initial_monmap();
  if (err < 0)
    goto out;

  err = -ENOMEM;
  nonce = getpid() + (1000000 * ++rados_instance);

#ifdef HAVE_XIO
  if (cct->_conf->client_rdma) {
    XioMessenger *xmsgr
      = new XioMessenger(cct, entity_name_t::CLIENT(-1), "radosclient",
			 nonce, 0 /* portals */,
			 new QueueStrategy(2) /* dispatch strategy */);
    xmsgr->set_port_shift(111) /* XXX */;
    messenger = xmsgr;
  }
  else
#endif
  {
    messenger = new SimpleMessenger(cct, entity_name_t::CLIENT(-1),
				    "radosclient", nonce);
  }

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its consituent pieces.
  messenger->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

  ldout(cct, 1) << "starting msgr at " << messenger->get_myaddr() << dendl;

  ldout(cct, 1) << "starting objecter" << dendl;

  err = -ENOMEM;
  objecter = new Objecter(cct, messenger, &monclient, &osdmap, lock, timer,
			  cct->_conf->rados_mon_op_timeout,
			  cct->_conf->rados_osd_op_timeout);
  if (!objecter)
    goto out;
  objecter->set_balanced_budget();

  monclient.set_messenger(messenger);

  messenger->add_dispatcher_head(this);

  messenger->start();

  ldout(cct, 1) << "setting wanted keys" << dendl;
  monclient.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  ldout(cct, 1) << "calling monclient init" << dendl;
  err = monclient.init();
  if (err) {
    ldout(cct, 0) << conf->name << " initialization error " << cpp_strerror(-err) << dendl;
    shutdown();
    goto out;
  }

  err = monclient.authenticate(conf->client_mount_timeout);
  if (err) {
    ldout(cct, 0) << conf->name << " authentication error " << cpp_strerror(-err) << dendl;
    shutdown();
    goto out;
  }
  messenger->set_myname(entity_name_t::CLIENT(monclient.get_global_id()));

  lock.Lock();

  timer.init();

  objecter->set_client_incarnation(0);
  objecter->init();
  monclient.renew_subs();

  finisher.start();

  state = CONNECTED;
  instance_id = monclient.get_global_id();

  lock.Unlock();

  ldout(cct, 1) << "init done" << dendl;
  err = 0;

 out:
  if (err)
    state = DISCONNECTED;
  return err;
}

void librados::RadosClient::shutdown()
{
  lock.Lock();
  if (state == DISCONNECTED) {
    lock.Unlock();
    return;
  }
  if (state == CONNECTED) {
    finisher.stop();
  }
  bool need_objecter = false;
  if (objecter && state == CONNECTED) {
    need_objecter = true;
    objecter->shutdown();
  }
  state = DISCONNECTED;
  instance_id = 0;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  monclient.shutdown();
  if (messenger) {
    messenger->shutdown();
    messenger->wait();
  }
  ldout(cct, 1) << "shutdown" << dendl;
}

uint64_t librados::RadosClient::get_instance_id()
{
  return instance_id;
}

librados::RadosClient::~RadosClient()
{
  if (messenger)
    delete messenger;
  if (objecter)
    delete objecter;
  if (g_ceph_context == cct)
    g_ceph_context = NULL;
  cct->put();
  cct = NULL;
}

int librados::RadosClient::create_ioctx(const string &name, IoCtxImpl **io)
{
  boost::uuids::string_generator parse;
  boost::uuids::uuid id;
  VolumeRef v;
  Mutex::Locker l(lock);
  int r = wait_for_osdmap();
  if (r < 0)
    return -EIO;

  try {
    id = parse(name);
    if (!osdmap.find_by_uuid(id, v))
      return -ENOENT;
  } catch (std::runtime_error &e) {
    if (!osdmap.find_by_name(name, v))
      return -ENOENT;
  }
  
  *io = new librados::IoCtxImpl(this, objecter, &lock, v);

  return 0;
}

int librados::RadosClient::create_ioctx(const boost::uuids::uuid& id, IoCtxImpl **io)
{
  Mutex::Locker l(lock);
  int r = wait_for_osdmap();
  VolumeRef volume;
  if (r < 0)
    return -EIO;

  if (!osdmap.find_by_uuid(id, volume))
    return -EEXIST;

  *io = new librados::IoCtxImpl(this, objecter, &lock, volume);
  return 0;
}

bool librados::RadosClient::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);
  bool ret;

  if (state == DISCONNECTED) {
    ldout(cct, 10) << "disconnected, discarding " << *m << dendl;
    m->put();
    ret = true;
  } else {
    ret = _dispatch(m);
  }
  return ret;
}

void librados::RadosClient::ms_handle_connect(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_connect(con);
}

bool librados::RadosClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_reset(con);
  return false;
}

void librados::RadosClient::ms_handle_remote_reset(Connection *con)
{
  Mutex::Locker l(lock);
  objecter->ms_handle_remote_reset(con);
}


bool librados::RadosClient::_dispatch(Message *m)
{
  switch (m->get_type()) {
  // OSD
  case CEPH_MSG_OSD_OPREPLY:
    objecter->handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
    break;
  case CEPH_MSG_OSD_MAP:
    objecter->handle_osd_map(static_cast<MOSDMap*>(m));
    osdmap_epoch = osdmap.get_epoch();
    cond.Signal();
    break;

  case CEPH_MSG_MDS_MAP:
    break;

  case CEPH_MSG_STATFS_REPLY:
    objecter->handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    watch_notify(static_cast<MWatchNotify *>(m));
    break;

  case MSG_LOG:
    handle_log(static_cast<MLog *>(m));
    break;

  default:
    return false;
  }

  return true;
}

int librados::RadosClient::wait_for_osdmap()
{
  assert(lock.is_locked());

  utime_t timeout;
  if (cct->_conf->rados_mon_op_timeout > 0)
    timeout.set_from_double(cct->_conf->rados_mon_op_timeout);

  if (osdmap.get_epoch() == 0) {
    ldout(cct, 10) << __func__ << " waiting" << dendl;
    utime_t start = ceph_clock_now(cct);

    while (osdmap.get_epoch() == 0) {
      cond.WaitInterval(cct, lock, timeout);

      utime_t elapsed = ceph_clock_now(cct) - start;
      if (!timeout.is_zero() && elapsed > timeout)
	break;
    }

    ldout(cct, 10) << __func__ << " done waiting" << dendl;

    if (osdmap.get_epoch() == 0) {
      lderr(cct) << "timed out waiting for first osdmap from monitors" << dendl;
      return -ETIMEDOUT;
    }
  }
  return 0;
}

int librados::RadosClient::wait_for_latest_osdmap()
{
  Mutex mylock;
  Cond cond;
  bool done;

  lock.Lock();
  objecter->wait_for_latest_osdmap(new C_SafeCond(&mylock, &cond, &done));
  lock.Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return 0;
}

int librados::RadosClient::get_fs_stats(ceph_statfs& stats)
{
  Mutex mylock;
  Cond cond;
  bool done;
  int ret = 0;

  lock.Lock();
  objecter->get_fs_stats(stats, new C_SafeCond(&mylock, &cond, &done, &ret));
  lock.Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();

  return ret;
}

int librados::RadosClient::vol_create(string& name)
{
  int reply;

  Mutex mylock;
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  lock.Lock();
  reply = objecter->create_volume(name, onfinish);
  lock.Unlock();

  if (reply < 0) {
    delete onfinish;
  } else {
    mylock.Lock();
    while(!done)
      cond.Wait(mylock);
    mylock.Unlock();
  }
  return reply;
}

void librados::RadosClient::get() {
  Mutex::Locker l(lock);
  assert(refcnt > 0);
  refcnt++;
}

bool librados::RadosClient::put() {
  Mutex::Locker l(lock);
  assert(refcnt > 0);
  refcnt--;
  return (refcnt == 0);
}

void librados::RadosClient::register_watcher(WatchContext *wc,
					     uint64_t *cookie)
{
  assert(lock.is_locked());
  wc->cookie = *cookie = ++max_watch_cookie;
  watchers[wc->cookie] = wc;
}

void librados::RadosClient::unregister_watcher(uint64_t cookie)
{
  assert(lock.is_locked());
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(cookie);
  if (iter != watchers.end()) {
    WatchContext *ctx = iter->second;
    if (ctx->linger_id)
      objecter->unregister_linger(ctx->linger_id);

    watchers.erase(iter);
    lock.Unlock();
    ldout(cct, 10) << "unregister_watcher, dropping reference, waiting ctx=" << (void *)ctx << dendl;
    ctx->put_wait();
    ldout(cct, 10) << "unregister_watcher, done ctx=" << (void *)ctx << dendl;
    lock.Lock();
  }
}

void librados::RadosClient::blacklist_self(bool set) {
  Mutex::Locker l(lock);
  objecter->blacklist_self(set);
}

class C_WatchNotify : public Context {
  librados::WatchContext *ctx;
  Mutex *client_lock;
  uint8_t opcode;
  uint64_t ver;
  uint64_t notify_id;
  bufferlist bl;

public:
  C_WatchNotify(librados::WatchContext *_ctx, Mutex *_client_lock,
		uint8_t _o, uint64_t _v, uint64_t _n, bufferlist& _bl) :
		ctx(_ctx), client_lock(_client_lock), opcode(_o), ver(_v), notify_id(_n), bl(_bl) {}

  void finish(int r) {
    ctx->notify(client_lock, opcode, ver, notify_id, bl);
    ctx->put();
  }
};

void librados::RadosClient::watch_notify(MWatchNotify *m)
{
  assert(lock.is_locked());
  map<uint64_t, WatchContext *>::iterator iter = watchers.find(m->cookie);
  if (iter != watchers.end()) {
    WatchContext *wc = iter->second;
    assert(wc);
    wc->get();
    finisher.queue(new C_WatchNotify(wc, &lock, m->opcode, m->ver, m->notify_id, m->bl));
  }
  m->put();
}

int librados::RadosClient::mon_command(const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock("RadosClient::mon_command::mylock");
  Cond cond;
  bool done;
  int rval;
  lock.Lock();
  monclient.start_mon_command(cmd, inbl, outbl, outs,
			       new C_SafeCond(&mylock, &cond, &done, &rval));
  lock.Unlock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return rval;
}

int librados::RadosClient::mon_command(int rank, const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock("RadosClient::mon_command::mylock");
  Cond cond;
  bool done;
  int rval;
  lock.Lock();
  monclient.start_mon_command(rank, cmd, inbl, outbl, outs,
			       new C_SafeCond(&mylock, &cond, &done, &rval));
  lock.Unlock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return rval;
}

int librados::RadosClient::mon_command(string name, const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock("RadosClient::mon_command::mylock");
  Cond cond;
  bool done;
  int rval;
  lock.Lock();
  monclient.start_mon_command(name, cmd, inbl, outbl, outs,
			       new C_SafeCond(&mylock, &cond, &done, &rval));
  lock.Unlock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  return rval;
}

int librados::RadosClient::monitor_log(const string& level, rados_log_callback_t cb, void *arg)
{
  if (cb == NULL) {
    // stop watch
    ldout(cct, 10) << __func__ << " removing cb " << (void*)log_cb << dendl;
    monclient.sub_unwant(log_watch);
    log_watch.clear();
    log_cb = NULL;
    log_cb_arg = NULL;
    return 0;
  }

  string watch_level;
  if (level == "debug") {
    watch_level = "log-debug";
  } else if (level == "info") {
    watch_level = "log-info";
  } else if (level == "warn" || level == "warning") {
    watch_level = "log-warn";
  } else if (level == "err" || level == "error") {
    watch_level = "log-error";
  } else if (level == "sec") {
    watch_level = "log-sec";
  } else {
    ldout(cct, 10) << __func__ << " invalid level " << level << dendl;
    return -EINVAL;
  }

  if (log_cb)
    monclient.sub_unwant(log_watch);

  // (re)start watch
  ldout(cct, 10) << __func__ << " add cb " << (void*)cb << " level " << level << dendl;
  monclient.sub_want(watch_level, 0, 0);
  monclient.renew_subs();
  log_cb = cb;
  log_cb_arg = arg;
  log_watch = watch_level;
  return 0;
}

void librados::RadosClient::handle_log(MLog *m)
{
  ldout(cct, 10) << __func__ << " version " << m->version << dendl;

  if (log_last_version < m->version) {
    log_last_version = m->version;

    if (log_cb) {
      for (std::deque<LogEntry>::iterator it = m->entries.begin(); it != m->entries.end(); ++it) {
	LogEntry e = *it;
	ostringstream ss;
	ss << e.stamp << " " << e.who.name << " " << e.type << " " << e.msg;
	string line = ss.str();
	string who = stringify(e.who);
	string level = stringify(e.type);
	struct timespec stamp;
	e.stamp.to_timespec(&stamp);

	ldout(cct, 20) << __func__ << " delivering " << ss.str() << dendl;
	log_cb(log_cb_arg, line.c_str(), who.c_str(),
	       stamp.tv_sec, stamp.tv_nsec,
	       e.seq, level.c_str(), e.msg.c_str());
      }

      /*
	this was present in the old cephtool code, but does not appear to be necessary. :/

	version_t v = log_last_version + 1;
	ldout(cct, 10) << __func__ << " wanting " << log_watch << " ver " << v << dendl;
	monclient.sub_want(log_watch, v, 0);
      */
    }
  }

  m->put();
}
