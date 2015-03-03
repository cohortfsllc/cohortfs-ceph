// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_LOGMONITOR_H
#define CEPH_LOGMONITOR_H

#include <map>
#include <set>
#include "include/types.h"
#include "msg/Messenger.h"
#include "PaxosService.h"

#include "common/LogEntry.h"
#include "messages/MLog.h"

class MMonCommand;

class LogMonitor : public PaxosService {
private:
  multimap<ceph::real_time,LogEntry> pending_log;
  LogSummary pending_summary, summary;

  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void create_pending();  // prepare a new pending
  // propose pending update to peers
  void encode_pending(MonitorDBStore::Transaction *t);
  virtual void encode_full(MonitorDBStore::Transaction *t);
  version_t get_trim_to();
  // true if processed.
  bool preprocess_query(PaxosServiceMessage *m, unique_lock& l);
  bool prepare_update(PaxosServiceMessage *m, unique_lock& l);

  bool preprocess_log(MLog *m);
  bool prepare_log(MLog *m);
  void _updated_log(MLog *m);

  bool should_propose(ceph::timespan& delay);

  bool should_stash_full() {
    // commit a LogSummary on every commit
    return true;
  }

  struct CB_Log {
    LogMonitor *logmon;
    MLog *ack;
    CB_Log(LogMonitor *p, MLog *a) : logmon(p), ack(a) {}
    void operator()(int r, unique_lock& l) {
      if (r == -ECANCELED) {
	if (ack)
	  ack->put();
	return;
      }
      logmon->_updated_log(ack);
    }
  };

  bool preprocess_command(MMonCommand *m, unique_lock& l);
  bool prepare_command(MMonCommand *m, unique_lock& l);

  bool _create_sub_summary(MLog *mlog, int level);
  void _create_sub_incremental(MLog *mlog, int level, version_t sv);

  void store_do_append(MonitorDBStore::Transaction *t,
		       const string& key, bufferlist& bl);

 public:
  LogMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name) { }

  void tick(unique_lock& l); // check state, take actions

  void check_subs();
  void check_sub(Subscription *s);

  /**
   * translate log sub name ('log-info') to integer id
   *
   * @param n name
   * @return id, or -1 if unrecognized
   */
  int sub_name_to_id(const string& n);

};

#endif
