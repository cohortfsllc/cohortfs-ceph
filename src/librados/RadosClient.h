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
#include <mutex>
#include "common/Timer.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "osd/OSDMap.h"

#include "IoCtxImpl.h"

struct AuthAuthorizer;
class CephContext;
struct Connection;
struct md_config_t;
class Message;
class MessageFactory;
class MWatchNotify;
class MLog;

class librados::RadosClient : public Dispatcher
{
public:
  CephContext *cct;
  md_config_t *conf;
private:
  enum {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
  } state;

  MonClient monclient;
  OSDC::MessageFactory factory;
  Messenger *messenger;

  uint64_t instance_id;

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m);

  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			 bool force_new);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);

  Objecter *objecter;

  std::mutex lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  std::condition_variable cond;
  SafeTimer<ceph::mono_clock> timer;
  int refcnt;

  version_t log_last_version;
  rados_log_callback_t log_cb;
  void *log_cb_arg;
  string log_watch;

  int wait_for_osdmap();

public:
  Finisher finisher;

  RadosClient(CephContext *cct_);
  ~RadosClient();
  int ping_monitor(string mon_id, string *result);
  int connect();
  void shutdown();

  uint64_t get_instance_id();

  int wait_for_latest_osdmap();

  int create_ioctx(const string &name, IoCtxImpl **io);
  int create_ioctx(const boost::uuids::uuid& volume, IoCtxImpl **io);

  int get_fsid(std::string *s);
  int get_fs_stats(ceph_statfs& result);

  int vol_delete(string& name);
  int vol_create(string& name);


  // watch/notify
  uint64_t max_watch_notify_cookie;
  map<uint64_t, librados::WatchNotifyInfo *> watch_notify_info;

  void register_watch_notify_callback(librados::WatchNotifyInfo *wc,
				      uint64_t *cookie);
  void unregister_watch_notify_callback(uint64_t cookie);
  void handle_watch_notify(MWatchNotify *m);
  void do_watch_notify(MWatchNotify *m);

  int mon_command(const vector<string>& cmd, const bufferlist &inbl,
		  bufferlist *outbl, string *outs);
  int mon_command(int rank,
		  const vector<string>& cmd, const bufferlist &inbl,
		  bufferlist *outbl, string *outs);
  int mon_command(string name,
		  const vector<string>& cmd, const bufferlist &inbl,
		  bufferlist *outbl, string *outs);

  void handle_log(MLog *m);
  int monitor_log(const string& level, rados_log_callback_t cb, void *arg);

  void get();
  bool put();
  void blacklist_self(bool set);
  boost::uuids::uuid lookup_volume(const string& name);
  string lookup_volume(const boost::uuids::uuid& name);
};

#endif
