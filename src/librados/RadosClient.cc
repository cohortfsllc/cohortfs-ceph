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
  /* monitor authorization is being handled on different layer */
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;
  *authorizer = monclient.auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

librados::RadosClient::RadosClient(CephContext *cct_)
  : Dispatcher(cct_),
    cct(cct_),
    conf(cct_->_conf),
    state(DISCONNECTED),
    monclient(cct_),
    messenger(NULL),
    instance_id(0),
    objecter(NULL),
    lock("librados::RadosClient::lock"),
    timer(cct, lock),
    refcnt(1),
    log_last_version(0), log_cb(NULL), log_cb_arg(NULL),
    finisher(cct),
    max_watch_notify_cookie(0)
{
}

boost::uuids::uuid librados::RadosClient::lookup_volume(const string& name)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return boost::uuids::nil_uuid();

  const OSDMap *osdmap = objecter->get_osdmap_read();
  VolumeRef v;
  boost::uuids::uuid id = boost::uuids::nil_uuid();
  if (osdmap->find_by_name(name, v)) {
    id = v->id;
  }
  objecter->put_osdmap_read();
  return id;
}

string librados::RadosClient::lookup_volume(const boost::uuids::uuid& id)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return string();

  const OSDMap *osdmap = objecter->get_osdmap_read();
  VolumeRef v;
  string name;
  if (osdmap->find_by_uuid(id, v)) {
    name = v->name;
  }
  objecter->put_osdmap_read();
  return name;
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

  // get monmap
  err = monclient.build_initial_monmap();
  if (err < 0)
    goto out;

  err = -ENOMEM;
  nonce = getpid() + (1000000 * (uint64_t)++rados_instance);
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

  if (!messenger)
    goto out;

  // require OSDREPLYMUX feature.  this means we will fail to talk to
  // old servers.  this is necessary because otherwise we won't know
  // how to decompose the reply data into its consituent pieces.
  messenger->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

  ldout(cct, 1) << "starting msgr at " << messenger->get_myaddr() << dendl;

  ldout(cct, 1) << "starting objecter" << dendl;

  err = -ENOMEM;
  objecter = new Objecter(cct, messenger, &monclient,
			  cct->_conf->rados_mon_op_timeout,
			  cct->_conf->rados_osd_op_timeout);
  if (!objecter)
    goto out;
  objecter->set_balanced_budget();

  monclient.set_messenger(messenger);

  objecter->init();
  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

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

  objecter->set_client_incarnation(0);
  objecter->start();
  lock.Lock();

  timer.init();

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
  if (objecter && objecter->initialized) {
    need_objecter = true;
  }
  state = DISCONNECTED;
  instance_id = 0;
  timer.shutdown();   // will drop+retake lock
  lock.Unlock();
  if (need_objecter)
    objecter->shutdown();
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
  common_cleanup(cct);
  cct = NULL;
}

int librados::RadosClient::create_ioctx(const string &name, IoCtxImpl **io)
{
  boost::uuids::string_generator parse;
  boost::uuids::uuid id;
  VolumeRef v;
  int r = wait_for_osdmap();
  if (r < 0)
    return r;

  const OSDMap *osdmap = objecter->get_osdmap_read();
  try {
    id = parse(name);
    if (!osdmap->find_by_uuid(id, v)) {
      r = -ENOENT;
      goto err;
    }
  } catch (std::runtime_error &e) {
    if (!osdmap->find_by_name(name, v)) {
      r = -ENOENT;
      goto err;
    }
  }
  objecter->put_osdmap_read();

  r = v->attach(cct);
  if (r < 0) {
    return r;
  }

  *io = new librados::IoCtxImpl(this, objecter, &lock, v);

  return 0;

err:

  objecter->put_osdmap_read();
  return r;
}

int librados::RadosClient::create_ioctx(const boost::uuids::uuid& id, IoCtxImpl **io)
{
  int r = wait_for_osdmap();
  if (r < 0)
    return r;
  VolumeRef volume;

  const OSDMap *osdmap = objecter->get_osdmap_read();
  if (!osdmap->find_by_uuid(id, volume)) {
    r = -ENOENT;
    goto err;
  }
  objecter->put_osdmap_read();

  r = volume->attach(cct);
  if (r < 0) {
    goto err;
  }

  *io = new librados::IoCtxImpl(this, objecter, &lock, volume);
  return 0;

err:
  objecter->put_osdmap_read();
  return r;
}

bool librados::RadosClient::ms_dispatch(Message *m)
{
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
}

bool librados::RadosClient::ms_handle_reset(Connection *con)
{
  return false;
}

void librados::RadosClient::ms_handle_remote_reset(Connection *con)
{
}


bool librados::RadosClient::_dispatch(Message *m)
{
  switch (m->get_type()) {
  // OSD
  case CEPH_MSG_OSD_MAP:
    lock.Lock();
    cond.Signal();
    lock.Unlock();
    m->put();
    break;

  case CEPH_MSG_MDS_MAP:
    break;

  case CEPH_MSG_WATCH_NOTIFY:
    handle_watch_notify(static_cast<MWatchNotify *>(m));
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
  assert(!lock.is_locked_by_me());

  if (objecter == NULL) {
    return -ENOTCONN;
  }

  bool need_map = false;
  const OSDMap *osdmap = objecter->get_osdmap_read();
  if (osdmap->get_epoch() == 0) {
    need_map = true;
  }
  objecter->put_osdmap_read();

  if (need_map) {
    Mutex::Locker l(lock);

    utime_t timeout;
    if (cct->_conf->rados_mon_op_timeout > 0)
      timeout.set_from_double(cct->_conf->rados_mon_op_timeout);

    const OSDMap *osdmap = objecter->get_osdmap_read();
    if (osdmap->get_epoch() == 0) {
      ldout(cct, 10) << __func__ << " waiting" << dendl;
      utime_t start = ceph_clock_now(cct);
      while (osdmap->get_epoch() == 0) {
	objecter->put_osdmap_read();
	cond.WaitInterval(cct, lock, timeout);
	utime_t elapsed = ceph_clock_now(cct) - start;
	if (!timeout.is_zero() && elapsed > timeout) {
	  lderr(cct) << "timed out waiting for first osdmap from monitors"
		     << dendl;
	  return -ETIMEDOUT;
	}
	osdmap = objecter->get_osdmap_read();
      }
      ldout(cct, 10) << __func__ << " done waiting" << dendl;
    }
    objecter->put_osdmap_read();
    return 0;
  } else {
    return 0;
  }
}

int librados::RadosClient::wait_for_latest_osdmap()
{
  Mutex mylock;
  Cond cond;
  bool done;

  objecter->wait_for_latest_osdmap(new C_SafeCond(&mylock, &cond, &done));

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

  objecter->get_fs_stats(stats, new C_SafeCond(&mylock, &cond, &done, &ret));

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

int librados::RadosClient::vol_delete(string& name)
{
  int reply;

  Mutex mylock;
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &reply);
  lock.Lock();
  reply = objecter->delete_volume(name, onfinish);
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

void librados::RadosClient::blacklist_self(bool set) {
  Mutex::Locker l(lock);
  objecter->blacklist_self(set);
}

// -----------
// watch/notify

void librados::RadosClient::register_watch_notify_callback(
  WatchNotifyInfo *wc,
  uint64_t *cookie)
{
  assert(lock.is_locked_by_me());
  wc->cookie = *cookie = ++max_watch_notify_cookie;
  ldout(cct,10) << __func__ << " cookie " << wc->cookie << dendl;
  watch_notify_info[wc->cookie] = wc;
}

void librados::RadosClient::unregister_watch_notify_callback(uint64_t cookie)
{
  ldout(cct,10) << __func__ << " cookie " << cookie << dendl;
  assert(lock.is_locked_by_me());
  map<uint64_t, WatchNotifyInfo *>::iterator iter =
    watch_notify_info.find(cookie);
  if (iter != watch_notify_info.end()) {
    WatchNotifyInfo *ctx = iter->second;
    if (ctx->linger_id)
      objecter->unregister_linger(ctx->linger_id);

    watch_notify_info.erase(iter);
    lock.Unlock();
    ldout(cct, 10) << __func__ << " dropping reference, waiting ctx="
		   << (void *)ctx << dendl;
    ctx->put_wait();
    ldout(cct, 10) << __func__ << " done ctx=" << (void *)ctx << dendl;
    lock.Lock();
  }
}

struct C_DoWatchNotify : public Context {
  librados::RadosClient *rados;
  MWatchNotify *m;
  C_DoWatchNotify(librados::RadosClient *r, MWatchNotify *m)
    : rados(r),m(m) {}
  void finish(int r) {
    rados->do_watch_notify(m);
  }
};

void librados::RadosClient::handle_watch_notify(MWatchNotify *m)
{
  Mutex::Locker l(lock);

  if (watch_notify_info.count(m->cookie)) {
    ldout(cct,10) << __func__ << " queueing async " << *m << dendl;
    // deliver this async via a finisher thread
    finisher.queue(new C_DoWatchNotify(this, m));
  } else {
    // drop it on the floor
    ldout(cct,10) << __func__ << " cookie " << m->cookie << " unknown"
		  << dendl;
    m->put();
  }
}

void librados::RadosClient::do_watch_notify(MWatchNotify *m)
{
  Mutex::Locker l(lock);
  map<uint64_t, WatchNotifyInfo *>::iterator iter =
    watch_notify_info.find(m->cookie);
  if (iter != watch_notify_info.end()) {
    WatchNotifyInfo *wc = iter->second;
    assert(wc);
    if (wc->notify_lock) {
      // we sent a notify and it completed (or failed)
      ldout(cct,10) << __func__ << " completed notify " << *m << dendl;
      wc->notify_lock->Lock();
      *wc->notify_done = true;
      // TODO
      // *wc->notify_rval = m->return_code;
      wc->notify_cond->Signal();
      wc->notify_lock->Unlock();
    } else {
      // we are watcher and got a notify
      ldout(cct,10) << __func__ << " got notify " << *m << dendl;
      wc->get();

      // trigger the callback
      lock.Unlock();
      wc->watch_ctx->notify(m->opcode, m->ver, m->bl);
      lock.Lock();

      // send ACK back to the OSD
      wc->io_ctx_impl->_notify_ack(wc->obj, m->notify_id, m->ver, m->cookie);

      ldout(cct,10) << __func__ << " notify done" << dendl;
      wc->put();
    }
  } else {
    ldout(cct, 4) << __func__ << " unknown cookie " << m->cookie << dendl;
  }
  m->put();
}

int librados::RadosClient::mon_command(const vector<string>& cmd,
				       const bufferlist &inbl,
				       bufferlist *outbl, string *outs)
{
  Mutex mylock;
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
  Mutex mylock;
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
  Mutex mylock;
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

int librados::RadosClient::monitor_log(const string& level,
				       rados_log_callback_t cb, void *arg)
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
  ldout(cct, 10) << __func__ << " add cb " << (void*)cb << " level " << level
		 << dendl;
  monclient.sub_want(watch_level, 0, 0);
  monclient.renew_subs();
  log_cb = cb;
  log_cb_arg = arg;
  log_watch = watch_level;
  return 0;
}

void librados::RadosClient::handle_log(MLog *m)
{
  Mutex::Locker l(lock);
  ldout(cct, 10) << __func__ << " version " << m->version << dendl;

  if (log_last_version < m->version) {
    log_last_version = m->version;

    if (log_cb) {
      for (std::deque<LogEntry>::iterator it = m->entries.begin();
	   it != m->entries.end(); ++it) {
	LogEntry e = *it;
	ostringstream ss;
	ss << e.stamp << " " << e.who.name << " " << " " << e.msg;
	string line = ss.str();
	string who = stringify(e.who);
	struct timespec stamp;
	e.stamp.to_timespec(&stamp);

	ldout(cct, 20) << __func__ << " delivering " << ss.str() << dendl;
	log_cb(log_cb_arg, line.c_str(), who.c_str(),
	       stamp.tv_sec, stamp.tv_nsec,
	       e.seq, e.msg.c_str());
      }
    }
  }

  m->put();
}
