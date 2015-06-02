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
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRADOS_RADOSCLIENT_H
#define CEPH_LIBRADOS_RADOSCLIENT_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include "common/Timer.h"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "osd/OSDMap.h"
#include "osdc/Objecter.h"

struct AuthAuthorizer;
class CephContext;
struct Connection;
struct md_config_t;
class Message;
class MessageFactory;
class MWatchNotify;
class MLog;


namespace rados {

  class RadosClient : public Dispatcher
  {
  public:
    enum State {
      DISCONNECTED,
      CONNECTING,
      CONNECTED,
    };
    md_config_t *conf;
    typedef std::function<void(
      const string& who, ceph::real_time when,
      uint64_t seq, int level, const string& what)> log_cb_t;
  private:
    State state;

    MonClient monclient;
    MessageFactory factory;
    Messenger *messenger;

    uint64_t instance_id;

    bool _dispatch(Message *m);
    bool ms_dispatch(Message *m);

    bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			   bool force_new);
    void ms_handle_connect(Connection *con);
    bool ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con);

    std::mutex lock;
    typedef std::unique_lock<std::mutex> unique_lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    std::condition_variable cond;

    version_t log_last_version;
    log_cb_t log_cb;
    string log_watch;

    int wait_for_osdmap();

    static CephContext* construct_cct(
      const string& clustername = string("ceph"),
      const string& id = string());

  public:
    Objecter *objecter;
    Finisher finisher;

    RadosClient();
    RadosClient(CephContext *cct_);
    ~RadosClient();
    int ping_monitor(string mon_id, string *result);
    int connect();
    void shutdown();
    State get_state() { return state; }

    uint64_t get_instance_id();

    int wait_for_latest_osdmap();

    int get_fsid(std::string *s);
    int get_fs_stats(ceph_statfs& result);

    int vol_delete(string& name);
    int vol_create(string& name);


    // watch/notify
    uint64_t max_watch_notify_cookie;
    bool own_cct;
    int mon_command(const vector<string>& cmd, const bufferlist &inbl,
		    bufferlist *outbl, string *outs);
    int mon_command(int rank,
		    const vector<string>& cmd, const bufferlist &inbl,
		    bufferlist *outbl, string *outs);
    int mon_command(string name,
		    const vector<string>& cmd, const bufferlist &inbl,
		    bufferlist *outbl, string *outs);

    void handle_log(MLog *m);
    int monitor_log(const string& level, log_cb_t cb);

    void blacklist_self(bool set);
    VolumeRef lookup_volume(const string& name);
    VolumeRef lookup_volume(const boost::uuids::uuid& name);

    AVolRef attach_volume(const std::string& name);
    AVolRef attach_volume(const boost::uuids::uuid& name);
  };
};

#endif
