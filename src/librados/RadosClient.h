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

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
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
class MWatchNotify;
class MLog;
class SimpleMessenger;
class XioMessenger;

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

  OSDMap osdmap;
  MonClient monclient;
  Messenger *messenger;

  uint64_t instance_id;

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m);

  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);

  Objecter *objecter;

  epoch_t osdmap_epoch;

  Mutex lock;
  Cond cond;
  SafeTimer timer;
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
  int create_ioctx(const uuid_d &volume, IoCtxImpl **io);
  int get_fsid(std::string *s);
  int get_fs_stats(ceph_statfs& result);

  // watch/notify
  uint64_t max_watch_cookie;
  map<uint64_t, librados::WatchContext *> watchers;

  void register_watcher(librados::WatchContext *wc, uint64_t *cookie);
  void unregister_watcher(uint64_t cookie);
  void watch_notify(MWatchNotify *m);
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

  uuid_d lookup_volume(const string& name);
  string lookup_volume(const uuid_d& name);
};

#endif
