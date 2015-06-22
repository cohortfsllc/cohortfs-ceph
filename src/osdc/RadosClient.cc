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
#include "Objecter.h"

// needed for static_cast
#include "messages/PaxosServiceMessage.h"
#include "messages/MStatfsReply.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "RadosClient.h"
#include "common/ceph_argparse.h"


#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

namespace rados {

  std::atomic<uint64_t> rados_instance;

  bool RadosClient::ms_get_authorizer(int dest_type,
				      AuthAuthorizer **authorizer,
				      bool force_new) {
    /* monitor authorization is being handled on different layer */
    if (dest_type == CEPH_ENTITY_TYPE_MON)
      return true;
    *authorizer = monclient.auth->build_authorizer(dest_type);
    return *authorizer != NULL;
  }

  CephContext* RadosClient::construct_cct(const string& clustername,
					  const string& id)
  {
    CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
    if (!id.empty()) {
      iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
    }
    CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
    if (!clustername.empty())
      cct->_conf->cluster = clustername;
    cct->_conf->parse_env(); // environment variables override
    std::deque<std::string> parse_errors;
    std::stringstream cerr;
    cct->_conf->parse_config_files(nullptr, &parse_errors, &cerr, 0);
    cct->_conf->apply_changes(NULL);
    cct->init();
    return cct;
  }

  RadosClient::RadosClient()
    : Dispatcher(construct_cct()),
      conf(cct->_conf),
      state(DISCONNECTED),
      monclient(cct),
      factory(&monclient.factory),
      messenger(NULL),
      instance_id(0),
      log_last_version(0),
      objecter(nullptr),
      finisher(cct),
      max_watch_notify_cookie(0),
      own_cct(true)
  {
  }

  RadosClient::RadosClient(CephContext *cct_)
    : Dispatcher(cct_),
      conf(cct->_conf),
      state(DISCONNECTED),
      monclient(cct),
      factory(&monclient.factory),
      messenger(NULL),
      instance_id(0),
      log_last_version(0),
      objecter(nullptr),
      finisher(cct),
      max_watch_notify_cookie(0),
      own_cct(false)
  {
  }

  Volume RadosClient::lookup_volume(const string& name)
  {
    wait_for_osdmap();
    return objecter->vol_by_name(name);
  }

  Volume RadosClient::lookup_volume(const boost::uuids::uuid& id)
  {
    wait_for_osdmap();

    return objecter->vol_by_uuid(id);
  }

  AVolRef RadosClient::attach_volume(const string& name)
  {
    wait_for_osdmap();
    return objecter->attach_by_name(name);
  }

  AVolRef RadosClient::attach_volume(const boost::uuids::uuid& id)
  {
    wait_for_osdmap();
    return objecter->attach_by_uuid(id);
  }

  int RadosClient::get_fsid(std::string *s)
  {
    if (!s)
      return -EINVAL;
    lock_guard l(lock);
    ostringstream oss;
    oss << monclient.get_fsid();
    *s = oss.str();
    return 0;
  }

  int RadosClient::ping_monitor(const string mon_id, string *result)
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

  int RadosClient::connect()
  {
    unique_lock l(lock, std::defer_lock);
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
			   nonce, &factory, 0 /* portals */,
			   new QueueStrategy(2) /* dispatch strategy */);
      xmsgr->set_port_shift(111) /* XXX */;
      messenger = xmsgr;
    }
    else
#endif
    {
      messenger = new SimpleMessenger(cct, entity_name_t::CLIENT(-1),
				      "radosclient", nonce, &factory);
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

    monclient.set_messenger(messenger);

    messenger->add_dispatcher_tail(objecter);
    messenger->add_dispatcher_tail(this);

    // To wake up any waiters whenever a new OSDMap comes in.
    objecter->add_osdmap_notifier([&](){
	unique_lock rcl(lock);
	cond.notify_all();
	rcl.unlock();
      });

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
    l.lock();

    monclient.renew_subs();

    finisher.start();

    state = CONNECTED;
    instance_id = monclient.get_global_id();

    l.unlock();

    ldout(cct, 1) << "init done" << dendl;
    err = 0;

  out:
    if (err)
      state = DISCONNECTED;
    return err;
  }

  void RadosClient::shutdown()
  {
    unique_lock l(lock);
    if (state == DISCONNECTED) {
      l.unlock();
      return;
    }
    if (state == CONNECTED) {
      finisher.stop();
    }
    bool need_objecter = false;
    if (objecter) {
      need_objecter = true;
    }
    state = DISCONNECTED;
    instance_id = 0;
    l.unlock();
    if (need_objecter)
      objecter->shutdown();
    monclient.shutdown();
    if (messenger) {
      messenger->shutdown();
      messenger->wait();
    }
    ldout(cct, 1) << "shutdown" << dendl;
  }

  uint64_t RadosClient::get_instance_id()
  {
    return instance_id;
  }

  RadosClient::~RadosClient()
  {
    shutdown();
    if (messenger)
      delete messenger;
    if (objecter)
      delete objecter;
    if (own_cct)
      common_cleanup(cct);
  }

  bool RadosClient::ms_dispatch(Message *m)
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

  void RadosClient::ms_handle_connect(Connection *con)
  {
  }

  bool RadosClient::ms_handle_reset(Connection *con)
  {
    return false;
  }

  void RadosClient::ms_handle_remote_reset(Connection *con)
  {
  }


  bool RadosClient::_dispatch(Message *m)
  {
    unique_lock l(lock, std::defer_lock);
    switch (m->get_type()) {
    case CEPH_MSG_MDS_MAP:
      break;

    case MSG_LOG:
      handle_log(static_cast<MLog *>(m));
      break;

    default:
      return false;
    }

    return true;
  }

  int RadosClient::wait_for_osdmap()
  {
    if (objecter == NULL) {
      return -ENOTCONN;
    }

    ceph::timespan timeout = 0s;
    if (cct->_conf->rados_mon_op_timeout > 0ns)
      timeout = cct->_conf->rados_mon_op_timeout;

    auto got_it = [&](){
      bool have_map;
      objecter->with_osdmap([&](auto o){have_map = (o.get_epoch() != 0);});
      return have_map;
    };

    unique_lock l(lock);
    if (timeout > 0s) {
      if (!cond.wait_for(l, timeout, got_it))
	return -ETIMEDOUT;
    } else {
      cond.wait(l, got_it);
    }
    return 0;
  }

  int RadosClient::wait_for_latest_osdmap()
  {
    std::mutex mylock;
    std::condition_variable cond;
    bool done;

    objecter->wait_for_latest_osdmap(new C_SafeCond(&mylock, &cond, &done));

    unique_lock l(mylock);
    cond.wait(l, [&](){ return done; });
    l.unlock();

    return 0;
  }

  int RadosClient::get_fs_stats(ceph_statfs& stats)
  {
    std::mutex mylock;
    std::condition_variable cond;
    bool done;
    int ret = 0;

    objecter->get_fs_stats(stats, new C_SafeCond(&mylock, &cond, &done, &ret));

    unique_lock l(mylock);
    cond.wait(l, [&](){ return done; });
    l.unlock();

    return ret;
  }


  int RadosClient::vol_create(string& name)
  {
    int reply;
    CB_Waiter w;

    reply = objecter->create_volume(name, w);

    if (reply != 0)
      reply = w.wait();

    return reply;
  }

  int RadosClient::vol_delete(string& name)
  {
    int reply;
    CB_Waiter w;

    reply = objecter->delete_volume(name, w);

    if (reply != 0)
      reply = w.wait();

    return reply;
  }

  void RadosClient::blacklist_self(bool set) {
    objecter->blacklist_self(set);
  }

  std::tuple<string, bufferlist> RadosClient::mon_command(
    const vector<string>& cmd, const bufferlist &inbl)
  {
    MonClient::waiter w;

    monclient.start_mon_command(cmd, inbl, w);

    return w.wait();
  }

  std::tuple<string, bufferlist> RadosClient::mon_command(
    int rank, const vector<string>& cmd, const bufferlist &inbl) {
    MonClient::waiter w;

    monclient.start_mon_command(rank, cmd, inbl, w);

    return w.wait();
  }

  std::tuple<string, bufferlist> RadosClient::mon_command(
    const string& name, const vector<string>& cmd, const bufferlist &inbl) {
    MonClient::waiter w;

    monclient.start_mon_command(name, cmd, inbl, w);

    return w.wait();
  }

  int RadosClient::monitor_log(const string& level,
			       log_cb_t cb)
  {
    if (!cb) {
      // stop watch
      monclient.sub_unwant(log_watch);
      log_watch.clear();
      log_cb = nullptr;
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
    monclient.sub_want(watch_level, 0, 0);
    monclient.renew_subs();
    log_cb = cb;
    log_watch = watch_level;
    return 0;
  }

  void RadosClient::handle_log(MLog *m)
  {
    lock_guard l(lock);
    ldout(cct, 10) << __func__ << " version " << m->version << dendl;

    if (log_last_version < m->version) {
      log_last_version = m->version;

      if (log_cb) {
	for (std::deque<LogEntry>::iterator it = m->entries.begin();
	     it != m->entries.end(); ++it) {
	  LogEntry e = *it;
	  string who = stringify(e.who);

	  log_cb(who, e.stamp, e.seq, clog_type_to_syslog_level(e.type),
		 e.msg);
	}
      }
    }

    m->put();
  }
};
