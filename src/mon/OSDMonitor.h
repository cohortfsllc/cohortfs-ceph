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
 * Foundation.  See file COPYING.
 * 
 */

/* Object Store Device (OSD) Monitor
 */

#ifndef CEPH_OSDMONITOR_H
#define CEPH_OSDMONITOR_H

#include <map>
#include <set>
#include <memory>

using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"

#include "PaxosService.h"
#include "Session.h"

class Monitor;
class MOSDBoot;
class MMonCommand;
class MPoolSnap;
class MOSDMap;

class OSDMonitor : public PaxosService {
public:
  auto_ptr<OSDMap> osdmap;

protected:
  map<epoch_t, list<PaxosServiceMessage*> > waiting_for_map;

  // [leader]
  auto_ptr<OSDMap::Incremental> pending_inc;
  multimap<int, pair<int, int> > failed_notes; // <failed_osd, <reporter, #reports> >
  map<int,utime_t>    down_pending_out;  // osd down -> out

  map<int,double> osd_weight;

  // map thrashing
  int thrash_map;
  int thrash_last_up_osd;
  bool thrash();

  // svc
public:  
  void create_initial();
protected:
  void update_from_paxos();
  void create_pending_super();  // prepare a new pending
  virtual void create_pending() = 0;  // prepare a new pending
  void encode_pending(bufferlist &bl);
  void on_active();

  void share_map_with_random_osd();

  void update_logger();

  void handle_query(PaxosServiceMessage *m);
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  virtual bool preprocess_query_sub(PaxosServiceMessage *m) = 0;  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);
  virtual bool prepare_update_sub(PaxosServiceMessage *m) = 0;
  bool should_propose(double &delay);

  bool can_mark_down(int o);
  bool can_mark_up(int o);
  bool can_mark_out(int o);
  bool can_mark_in(int o);

  // ...
  void send_to_waiting();     // send current map to waiters.
  MOSDMap *build_latest_full();
  MOSDMap *build_incremental(epoch_t first, epoch_t last);
  void send_full(PaxosServiceMessage *m);
  void send_incremental(PaxosServiceMessage *m, epoch_t first);
  void send_incremental(epoch_t first, entity_inst_t& dest, bool onetime);

  int reweight_by_utilization(int oload, std::string& out_str);
 
  bool preprocess_failure(class MOSDFailure *m);
  bool prepare_failure(class MOSDFailure *m);
  void _reported_failure(MOSDFailure *m);

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
      else
	cmon->dispatch((PaxosServiceMessage*)m);
    }
  };

  struct C_ReplyMap : public Context {
    OSDMonitor *osdmon;
    PaxosServiceMessage *m;
    epoch_t e;
    C_ReplyMap(OSDMonitor *o, PaxosServiceMessage *mm, epoch_t ee) : osdmon(o), m(mm), e(ee) {}
    void finish(int r) {
      osdmon->_reply_map(m, e);
    }    
  };
  struct C_Reported : public Context {
    OSDMonitor *cmon;
    MOSDFailure *m;
    C_Reported(OSDMonitor *cm, MOSDFailure *m_) : 
      cmon(cm), m(m_) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_reported_failure(m);
      else
	cmon->dispatch((PaxosServiceMessage*)m);
    }
  };

  bool preprocess_remove_snaps(class MRemoveSnaps *m);
  virtual bool preprocess_remove_snaps_sub(class MRemoveSnaps *m) = 0;
  virtual bool prepare_remove_snaps(class MRemoveSnaps *m) = 0;
  virtual void preprocess_command_sub(MMonCommand *m, int& r) = 0;

 public:
  OSDMonitor(Monitor *mn, Paxos *p);
  virtual ~OSDMonitor() { };

  virtual OSDMap* newOSDMap() const = 0;

  void tick();  // check state, take actions
  virtual void tick_sub(bool& do_propose) = 0;

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;
  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  void handle_osd_timeouts(const utime_t &now,
			   std::map<int,utime_t> &last_osd_report);
  void mark_all_down();

  void send_latest(PaxosServiceMessage *m, epoch_t start=0);
  void send_latest_now_nodelete(PaxosServiceMessage *m, epoch_t start=0) {
    send_incremental(m, start);
  }

  epoch_t blacklist(entity_addr_t a, utime_t until);

  void check_subs();
  void check_sub(Subscription *sub);

  void add_flag(int flag) {
    if (!(osdmap->flags & flag)) {
      if (pending_inc->new_flags < 0)
	pending_inc->new_flags = osdmap->flags;
      pending_inc->new_flags |= flag;
    }
  }

  void remove_flag(int flag) {
    if(osdmap->flags & flag) {
      if (pending_inc->new_flags < 0)
	pending_inc->new_flags = osdmap->flags;
      pending_inc->new_flags &= ~flag;
    }
  }
};

#endif
