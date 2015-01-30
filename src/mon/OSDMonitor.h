// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

/* Object Store Device (OSD) Monitor
 */

#ifndef CEPH_OSDMONITOR_H
#define CEPH_OSDMONITOR_H

#include <map>
#include <set>
#include "include/types.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"

#include "PaxosService.h"
#include "Session.h"

class Monitor;
#include "messages/MOSDBoot.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDFailure.h"

#define OSD_METADATA_PREFIX "osd_metadata"

/// information about a particular peer's failure reports for one osd
struct failure_reporter_t {
  int num_reports;	    ///< reports from this reporter
  ceph::real_time failed_since;	///< when they think it failed
  MOSDFailure *msg;	    ///< most recent failure message

  failure_reporter_t() : num_reports(0), msg(NULL) {}
  failure_reporter_t(ceph::real_time s) : num_reports(1), failed_since(s),
					  msg(NULL) {}
};

/// information about all failure reports for one osd
struct failure_info_t {
  map<int, failure_reporter_t> reporters;  ///< reporter -> # reports
  ceph::real_time max_failed_since; ///< most recent failed_since
  int num_reports;

  failure_info_t() : num_reports(0) {}

  ceph::real_time get_failed_since() {
    if (max_failed_since == ceph::real_time::min() && !reporters.empty()) {
      // the old max must have canceled; recalculate.
      for (const auto& p : reporters) {
	if (p.second.failed_since > max_failed_since)
	  max_failed_since = p.second.failed_since;
      }
    }
    return max_failed_since;
  }

  // set the message for the latest report.  return any old message we had,
  // if any, so we can discard it.
  MOSDFailure *add_report(int who, ceph::real_time failed_since,
			  MOSDFailure *msg) {
    map<int, failure_reporter_t>::iterator p = reporters.find(who);
    if (p == reporters.end()) {
      if (max_failed_since == ceph::real_time::min())
	max_failed_since = failed_since;
      else if (max_failed_since < failed_since)
	max_failed_since = failed_since;
      p = reporters.insert(map<int, failure_reporter_t>::value_type(who, failure_reporter_t(failed_since))).first;
    } else {
      p->second.num_reports++;
    }
    num_reports++;

    MOSDFailure *ret = p->second.msg;
    p->second.msg = msg;
    return ret;
  }

  void take_report_messages(list<MOSDFailure*>& ls) {
    for (map<int, failure_reporter_t>::iterator p = reporters.begin();
	 p != reporters.end();
	 ++p) {
      if (p->second.msg) {
	ls.push_back(p->second.msg);
	p->second.msg = NULL;
      }
    }
  }

  void cancel_report(int who) {
    map<int, failure_reporter_t>::iterator p = reporters.find(who);
    if (p == reporters.end())
      return;
    num_reports -= p->second.num_reports;
    reporters.erase(p);
    if (reporters.empty())
      max_failed_since = ceph::real_time::min();
  }
};

class OSDMonitor : public PaxosService {
public:
  OSDMap osdmap;

private:
  // [leader]
  OSDMap::Incremental pending_inc;
  map<int, bufferlist> pending_metadata;
  set<int>	       pending_metadata_rm;
  map<int, failure_info_t> failure_info;
  map<int,ceph::real_time> down_pending_out; // osd down -> out

  map<int,double> osd_weight;

  /*
   * cache what epochs we think osds have.  this is purely
   * optimization to try to avoid sending the same inc maps twice.
   */
  map<int,epoch_t> osd_epoch;

  void check_failures(ceph::real_time now);
  bool check_failure(ceph::real_time now, int target_osd, failure_info_t& fi);

  // svc
public:
  void create_initial();
private:
  void update_from_paxos(bool *need_bootstrap);
  void create_pending();  // prepare a new pending
  void encode_pending(MonitorDBStore::Transaction *t);
  void on_active();
  void on_shutdown();

  /**
   * we haven't delegated full version stashing to paxosservice for some time
   * now, making this function useless in current context.
   */
  virtual void encode_full(MonitorDBStore::Transaction *t) { }
  /**
   * do not let paxosservice periodically stash full osdmaps, or we will break our
   * locally-managed full maps.	 (update_from_paxos loads the latest and writes them
   * out going forward from there, but if we just synced that may mean we skip some.)
   */
  virtual bool should_stash_full() {
    return false;
  }

  /**
   * hook into trim to include the oldest full map in the trim transaction
   *
   * This ensures that anyone post-sync will have enough to rebuild their
   * full osdmaps.
   */
  void encode_trim_extra(MonitorDBStore::Transaction *tx, version_t first);

  void update_msgr_features();
  int check_cluster_features(uint64_t features, stringstream &ss);

  void share_map_with_random_osd();

  void handle_query(PaxosServiceMessage *m);
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);
  bool should_propose(ceph::timespan &delay);

  version_t get_trim_to();

  bool can_mark_down(int o);
  bool can_mark_up(int o);
  bool can_mark_out(int o);
  bool can_mark_in(int o);

  // ...
  MOSDMap *build_latest_full();
  MOSDMap *build_incremental(epoch_t first, epoch_t last);
  void send_full(PaxosServiceMessage *m);
  void send_incremental(PaxosServiceMessage *m, epoch_t first);
  void send_incremental(epoch_t first, entity_inst_t& dest, bool onetime);
  void send_incremental(epoch_t first, ConnectionRef &con, bool onetime);

  bool check_source(PaxosServiceMessage *m, const boost::uuids::uuid& fsid);

  bool preprocess_mark_me_down(class MOSDMarkMeDown *m);

  friend class C_AckMarkedDown;
  bool preprocess_failure(class MOSDFailure *m);
  bool prepare_failure(class MOSDFailure *m);
  bool prepare_mark_me_down(class MOSDMarkMeDown *m);
  void process_failures();
  void take_all_failures(list<MOSDFailure*>& ls);

  bool preprocess_boot(class MOSDBoot *m);
  bool prepare_boot(class MOSDBoot *m);
  void _booted(MOSDBoot *m, bool logit);

  bool preprocess_alive(class MOSDAlive *m);
  bool prepare_alive(class MOSDAlive *m);
  void _reply_map(PaxosServiceMessage *m, epoch_t e);

  bool prepare_set_flag(MMonCommand *m, int flag);
  bool prepare_unset_flag(MMonCommand *m, int flag);

  struct C_Booted : public Context {
    OSDMonitor *cmon;
    MOSDBoot *m;
    bool logit;
    C_Booted(OSDMonitor *cm, MOSDBoot *m_, bool l=true) :
      cmon(cm), m(m_), logit(l) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_booted(m, logit);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	cmon->dispatch((PaxosServiceMessage*)m);
      else
	assert(0 == "bad C_Booted return value");
    }
  };

  struct C_ReplyMap : public Context {
    OSDMonitor *osdmon;
    PaxosServiceMessage *m;
    epoch_t e;
    C_ReplyMap(OSDMonitor *o, PaxosServiceMessage *mm, epoch_t ee) : osdmon(o), m(mm), e(ee) {}
    void finish(int r) {
      if (r >= 0)
	osdmon->_reply_map(m, e);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	osdmon->dispatch(m);
      else
	assert(0 == "bad C_ReplyMap return value");
    }
  };

 public:
  OSDMonitor(Monitor *mn, Paxos *p, string service_name)
  : PaxosService(mn, p, service_name) { }

  void tick();	// check state, take actions

  int parse_osd_id(const char *s, stringstream *pss);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;
  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);
  bool prepare_command_impl(MMonCommand *m, map<string,cmd_vartype> &cmdmap);

  void handle_osd_timeouts(const ceph::real_time &now,
			   std::map<int,ceph::real_time> &last_osd_report);
  void mark_all_down();

  void send_latest(PaxosServiceMessage *m, epoch_t start=0);
  void send_latest_now_nodelete(PaxosServiceMessage *m, epoch_t start=0) {
    send_incremental(m, start);
  }

  epoch_t blacklist(const entity_addr_t& a, ceph::real_time until);

  void dump_info(Formatter *f);
  int dump_osd_metadata(int osd, Formatter *f, ostream *err);

  void check_subs();
  void check_sub(Subscription *sub);

  void add_flag(int flag) {
    if (!(osdmap.flags & flag)) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags |= flag;
    }
  }

  void remove_flag(int flag) {
    if(osdmap.flags & flag) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags &= ~flag;
    }
  }
};

#endif
