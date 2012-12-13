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

#ifndef CEPH_PGOSD_H
#define CEPH_PGOSD_H


#include "osd/OSD.h"
#include "PG.h"


class ReplicatedPG;


class PGOSDService : public OSDService {

public:

  SharedPtrRegistry<pg_t, DeletingState> deleting_pgs;
  SharedPtrRegistry<pg_t, ObjectStore::Sequencer> osr_registry;
  PGRecoveryStats &pg_recovery_stats;
  ThreadPool::WorkQueue<PG> &op_wq;
  ThreadPool::BatchWorkQueue<PG> &peering_wq;
  ThreadPool::WorkQueue<PG> &recovery_wq;
  ThreadPool::WorkQueue<PG> &snap_trim_wq;
  ThreadPool::WorkQueue<PG> &scrub_wq;
  ThreadPool::WorkQueue<PG> &scrub_finalize_wq;
  ThreadPool::WorkQueue<MOSDRepScrub> &rep_scrub_wq;

  set< pair<utime_t,pg_t> > last_scrub_pg;

  void reg_last_pg_scrub(pg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    last_scrub_pg.insert(pair<utime_t,pg_t>(t, pgid));
  }
  void unreg_last_pg_scrub(pg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    pair<utime_t,pg_t> p(t, pgid);
    assert(last_scrub_pg.count(p));
    last_scrub_pg.erase(p);
  }
  bool next_scrub_stamp(pair<utime_t, pg_t> after,
			pair<utime_t, pg_t> *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (last_scrub_pg.size() == 0) return false;
    set< pair<utime_t, pg_t> >::iterator iter = last_scrub_pg.lower_bound(after);
    if (iter == last_scrub_pg.end()) return false;
    ++iter;
    if (iter == last_scrub_pg.end()) return false;
    *out = *iter;
    return true;
  }

  void handle_misdirected_op(PG *pg, OpRequestRef op);

  // -- pg_temp --
  Mutex pg_temp_lock;
  map<pg_t, vector<int> > pg_temp_wanted;
  void queue_want_pg_temp(pg_t pgid, vector<int>& want);
  void remove_want_pg_temp(pg_t pgid) {
    Mutex::Locker l(pg_temp_lock);
    pg_temp_wanted.erase(pgid);
  }
  void send_pg_temp();

  void queue_for_peering(PG *pg);
  void queue_for_op(PG *pg);
  bool queue_for_recovery(PG *pg);
  bool queue_for_snap_trim(PG *pg) {
    return snap_trim_wq.queue(pg);
  }
  bool queue_for_scrub(PG *pg) {
    return scrub_wq.queue(pg);
  }

  void pg_stat_queue_enqueue(PG *pg);
  void pg_stat_queue_dequeue(PG *pg);

  PGOSD* get_pgosd() const;

  PGOSDService(PGOSD *osd);

  virtual OSDMap* newOSDMap() const { return new PGOSDMap(); }
}; // class PGOSDService


class PGOSD : public OSD {

public:

  struct PGSession : public Session {
    std::map<void *, pg_t> watches;

    PGSession() : Session() {}
  };


  PGOSD(int id, Messenger *internal, Messenger *external, Messenger *hbmin,
	Messenger *hbmout,
	MonClient *mc, const std::string &dev, const std::string &jdev,
	OSDService* osdSvc);

  virtual OSDService* newOSDService(OSD* osd) const {
    PGOSD* const pgosd = dynamic_cast<PGOSD*>(osd);
    return new PGOSDService(pgosd);
  }

  virtual OSDMap* newOSDMap() const {
    return new PGOSDMap();
  }

  virtual int init();
  virtual int shutdown();

  virtual bool do_command_sub(Connection *con,
			      tid_t tid,
			      vector<string>& cmd,
			      bufferlist& data,
			      bufferlist& odata,
			      int& r,
			      ostringstream& ss);
  virtual bool do_command_debug_sub(vector<string>& cmd,
				    int& r,
				    ostringstream& ss);

  virtual void handle_op_sub(OpRequestRef op);
  virtual bool handle_sub_op_sub(OpRequestRef op);
  virtual bool handle_sub_op_reply_sub(OpRequestRef op);

  virtual void build_heartbeat_peers_list();
  virtual void tick_sub(const utime_t& now);

  virtual void do_mon_report_sub(const utime_t& now);

  virtual void ms_handle_connect_sub(Connection *con);

  virtual void ms_handle_reset_sub(PGSession* session);

  virtual void advance_map_sub(ObjectStore::Transaction& t,
			       C_Contexts *tfin);
  virtual void activate_map_sub();
  virtual void dispatch_op_sub(OpRequestRef op);
  virtual bool _dispatch_sub(Message *m);

  hobject_t make_pg_log_oid(pg_t pg) {
    stringstream ss;
    ss << "pglog_" << pg;
    string s;
    getline(ss, s);
    return hobject_t(sobject_t(object_t(s.c_str()), 0));
  }
  
  hobject_t make_pg_biginfo_oid(pg_t pg) {
    stringstream ss;
    ss << "pginfo_" << pg;
    string s;
    getline(ss, s);
    return hobject_t(sobject_t(object_t(s.c_str()), 0));
  }


  // -- op queue --
  list<PG*> op_queue;
  int op_queue_len;

  struct OpWQ : public ThreadPool::WorkQueue<PG> {
    PGOSD *osd;
    OpWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::OpWQ", ti, ti*10, tp), osd(o) {}

    bool _enqueue(PG *pg);
    void _dequeue(PG *pg) {
      for (list<PG*>::iterator i = osd->op_queue.begin();
	   i != osd->op_queue.end();
	   ) {
	if (*i == pg) {
	  osd->op_queue.erase(i++);
	  pg->put();
	} else {
	  ++i;
	}
      }
    }
    bool _empty() {
      return osd->op_queue.empty();
    }
    PG *_dequeue();
    void _process(PG *pg) {
      osd->dequeue_op(pg);
    }
    void _clear() {
      assert(osd->op_queue.empty());
    }
  } op_wq;

  void enqueue_op(PG *pg, OpRequestRef op);
  void dequeue_op(PG *pg);
  static void static_dequeueop(PGOSD *o, PG *pg) {
    o->dequeue_op(pg);
  };

  // -- peering queue --
  struct PeeringWQ : public ThreadPool::BatchWorkQueue<PG> {
    list<PG*> peering_queue;
    PGOSD *osd;
    set<PG*> in_use;
    const size_t batch_size;
    PeeringWQ(PGOSD *o, time_t ti, ThreadPool *tp, size_t batch_size)
      : ThreadPool::BatchWorkQueue<PG>(
	"OSD::PeeringWQ", ti, ti*10, tp), osd(o), batch_size(batch_size) {}

    void _dequeue(PG *pg) {
      for (list<PG*>::iterator i = peering_queue.begin();
	   i != peering_queue.end();
	   ) {
	if (*i == pg) {
	  peering_queue.erase(i++);
	  pg->put();
	} else {
	  ++i;
	}
      }
    }
    bool _enqueue(PG *pg) {
      pg->get();
      peering_queue.push_back(pg);
      return true;
    }
    bool _empty() {
      return peering_queue.empty();
    }
    void _dequeue(list<PG*> *out) {
      set<PG*> got;
      for (list<PG*>::iterator i = peering_queue.begin();
	   i != peering_queue.end() && out->size() < batch_size;
	   ) {
	if (in_use.count(*i)) {
	  ++i;
	} else {
	  out->push_back(*i);
	  got.insert(*i);
	  peering_queue.erase(i++);
	}
      }
      in_use.insert(got.begin(), got.end());
    }
    void _process(const list<PG *> &pgs) {
      osd->process_peering_events(pgs);
      for (list<PG *>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	(*i)->put();
      }
    }
    void _process_finish(const list<PG *> &pgs) {
      for (list<PG*>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	in_use.erase(*i);
      }
    }
    void _clear() {
      assert(peering_queue.empty());
    }
  } peering_wq;

  void process_peering_events(const list<PG*> &pg);

  friend class PG;
  friend class ReplicatedPG;

protected:

  const PGOSDMap* get_pgosdmap() {
    return dynamic_cast<const PGOSDMap*>(get_osdmap().get());
  }

  const PGOSDMap* get_pgosdmap(OSDMapRef ref) const {
    return dynamic_cast<const PGOSDMap*>(ref.get());
  }

  PGOSDService* get_pgosdservice() const {
    return dynamic_cast<PGOSDService*>(serviceRef.get());
  }

  void advance_pg(epoch_t advance_to, PG *pg, PG::RecoveryCtx *rctx);


protected:

  // -- placement groups --
  hash_map<pg_t, PG*> pg_map;
  map<pg_t, list<OpRequestRef> > waiting_for_pg;
  PGRecoveryStats pg_recovery_stats;

  PGPool _get_pool(int id, OSDMapRef createmap);

  bool  _have_pg(pg_t pgid);
  PG   *_lookup_lock_pg(pg_t pgid);
  PG   *_lookup_lock_pg_with_map_lock_held(pg_t pgid);
  PG   *_open_lock_pg(OSDMapRef createmap,
		      pg_t pg, bool no_lockdep_check=false,
		      bool hold_map_lock=false);
  PG   *_create_lock_pg(OSDMapRef createmap,
			pg_t pgid, bool newly_created,
			bool hold_map_lock, int role,
			vector<int>& up, vector<int>& acting,
			pg_history_t history,
			pg_interval_map_t& pi,
			ObjectStore::Transaction& t);

  PG *lookup_lock_raw_pg(pg_t pgid);

  PG *get_or_create_pg(const pg_info_t& info,
		       pg_interval_map_t& pi,
		       epoch_t epoch, int from, int& pcreated,
		       bool primary);
  
  void load_pgs();
  void calc_priors_during(pg_t pgid, epoch_t start, epoch_t end, set<int>& pset);
  void project_pg_history(pg_t pgid, pg_history_t& h, epoch_t from,
			  const vector<int>& lastup, const vector<int>& lastacting);

  void wake_pg_waiters(pg_t pgid) {
    if (waiting_for_pg.count(pgid)) {
      take_waiters(waiting_for_pg[pgid]);
      waiting_for_pg.erase(pgid);
    }
  }
  void wake_all_pg_waiters() {
    for (map<pg_t, list<OpRequestRef> >::iterator p = waiting_for_pg.begin();
	 p != waiting_for_pg.end();
	 p++)
      take_waiters(p->second);
    waiting_for_pg.clear();
  }


  // -- pg creation --
  struct create_pg_info {
    pg_history_t history;
    vector<int> acting;
    set<int> prior;
    pg_t parent;
    int split_bits;
  };
  hash_map<pg_t, create_pg_info> creating_pgs;
  double debug_drop_pg_create_probability;
  int debug_drop_pg_create_duration;
  int debug_drop_pg_create_left;  // 0 if we just dropped the last one, -1 if we can drop more

  bool can_create_pg(pg_t pgid);
  void handle_pg_create(OpRequestRef op);

  void do_split(PG *parent, set<pg_t>& children, ObjectStore::Transaction &t, C_Contexts *tfin);
  void split_pg(PG *parent, map<pg_t,PG*>& children, ObjectStore::Transaction &t);


  /* if our monitor dies, we want to notice it and reconnect.
   *  So we keep track of when it last acked our stat updates,
   *  and if too much time passes (and we've been sending
   *  more updates) then we can call it dead and reconnect
   *  elsewhere.
   */
  utime_t last_pg_stats_ack;
  bool outstanding_pg_stats; // some stat updates haven't been acked yet

  // -- pg stats --
  Mutex pg_stat_queue_lock;
  Cond pg_stat_queue_cond;
  xlist<PG*> pg_stat_queue;
  uint64_t pg_stat_tid, pg_stat_tid_flushed;

  void send_pg_stats(const utime_t &now);
  void handle_pg_stats_ack(class MPGStatsAck *ack);
  void flush_pg_stats();

  void pg_stat_queue_enqueue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->is_primary() && !pg->stat_queue_item.is_on_list()) {
      pg->get();
      pg_stat_queue.push_back(&pg->stat_queue_item);
    }
    osd_stat_updated = true;
    pg_stat_queue_lock.Unlock();
  }
  void pg_stat_queue_dequeue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->stat_queue_item.remove_myself())
      pg->put();
    pg_stat_queue_lock.Unlock();
  }
  void clear_pg_stat_queue() {
    pg_stat_queue_lock.Lock();
    while (!pg_stat_queue.empty()) {
      PG *pg = pg_stat_queue.front();
      pg_stat_queue.pop_front();
      pg->put();
    }
    pg_stat_queue_lock.Unlock();
  }

  // -- generic pg peering --
  PG::RecoveryCtx create_context();
  void dispatch_context(PG::RecoveryCtx &ctx, PG *pg, OSDMapRef curmap);
  void dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg);
  void do_notifies(map< int,vector<pair<pg_notify_t, pg_interval_map_t> > >& notify_list,
		   OSDMapRef map);
  void do_queries(map< int, map<pg_t,pg_query_t> >& query_map,
		  OSDMapRef map);
  void do_infos(map<int, vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		OSDMapRef map);
  void repeer(PG *pg, map< int, map<pg_t,pg_query_t> >& query_map);

  void handle_pg_query(OpRequestRef op);
  void handle_pg_notify(OpRequestRef op);
  void handle_pg_log(OpRequestRef op);
  void handle_pg_info(OpRequestRef op);
  void handle_pg_trim(OpRequestRef op);

  void handle_pg_scan(OpRequestRef op);

  void handle_pg_backfill(OpRequestRef op);

  void handle_pg_remove(OpRequestRef op);
  void _remove_pg(PG *pg);

  // -- pg recovery --
  xlist<PG*> recovery_queue;
  utime_t defer_recovery_until;
  int recovery_ops_active;
#ifdef DEBUG_RECOVERY_OIDS
  map<pg_t, set<hobject_t> > recovery_oids;
#endif

  void start_recovery_op(PG *pg, const hobject_t& soid);
  void finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue);
  void defer_recovery(PG *pg);
  void do_recovery(PG *pg);
  bool _recover_now();

  // replay / delayed pg activation
  Mutex replay_queue_lock;
  list< pair<pg_t, utime_t > > replay_queue;
  
  virtual void check_replay_queue();


  // -- snap trimming --
  xlist<PG*> snap_trim_queue;
  
  struct SnapTrimWQ : public ThreadPool::WorkQueue<PG> {
    PGOSD *osd;
    SnapTrimWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::SnapTrimWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->snap_trim_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (pg->snap_trim_item.is_on_list())
	return false;
      pg->get();
      osd->snap_trim_queue.push_back(&pg->snap_trim_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->snap_trim_item.remove_myself())
	pg->put();
    }
    PG *_dequeue() {
      if (osd->snap_trim_queue.empty())
	return NULL;
      PG *pg = osd->snap_trim_queue.front();
      osd->snap_trim_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->snap_trimmer();
      pg->put();
    }
    void _clear() {
      osd->snap_trim_queue.clear();
    }
  } snap_trim_wq;


  // -- scrubbing --
  virtual void sched_scrub();
  void handle_rep_scrub(MOSDRepScrub *m);
  void handle_scrub(class MOSDScrub *m);

  xlist<PG*> scrub_queue;


  struct ScrubWQ : public ThreadPool::WorkQueue<PG> {
    PGOSD *osd;
    ScrubWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::ScrubWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->scrub_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (pg->scrub_item.is_on_list()) {
	return false;
      }
      pg->get();
      osd->scrub_queue.push_back(&pg->scrub_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_item.remove_myself()) {
	pg->put();
      }
    }
    PG *_dequeue() {
      if (osd->scrub_queue.empty())
	return NULL;
      PG *pg = osd->scrub_queue.front();
      osd->scrub_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->scrub();
      pg->put();
    }
    void _clear() {
      while (!osd->scrub_queue.empty()) {
	PG *pg = osd->scrub_queue.front();
	osd->scrub_queue.pop_front();
	pg->put();
      }
    }
  } scrub_wq;

  struct ScrubFinalizeWQ : public ThreadPool::WorkQueue<PG> {
  private:
    PGOSD *osd;
    xlist<PG*> scrub_finalize_queue;

  public:
    ScrubFinalizeWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::ScrubFinalizeWQ", ti, ti*10, tp), osd(o) {}

    bool _empty() {
      return scrub_finalize_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (pg->scrub_finalize_item.is_on_list()) {
	return false;
      }
      pg->get();
      scrub_finalize_queue.push_back(&pg->scrub_finalize_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_finalize_item.remove_myself()) {
	pg->put();
      }
    }
    PG *_dequeue() {
      if (scrub_finalize_queue.empty())
	return NULL;
      PG *pg = scrub_finalize_queue.front();
      scrub_finalize_queue.pop_front();
      return pg;
    }
    void _process(PG *pg) {
      pg->scrub_finalize();
      pg->put();
    }
    void _clear() {
      while (!scrub_finalize_queue.empty()) {
	PG *pg = scrub_finalize_queue.front();
	scrub_finalize_queue.pop_front();
	pg->put();
      }
    }
  } scrub_finalize_wq;

  struct RecoveryWQ : public ThreadPool::WorkQueue<PG> {
    PGOSD *osd;
    RecoveryWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::RecoveryWQ", ti, ti*10, tp), osd(o) {}

    bool _empty() {
      return osd->recovery_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (!pg->recovery_item.is_on_list()) {
	pg->get();
	osd->recovery_queue.push_back(&pg->recovery_item);

	if (g_conf->osd_recovery_delay_start > 0) {
	  osd->defer_recovery_until = ceph_clock_now(g_ceph_context);
	  osd->defer_recovery_until += g_conf->osd_recovery_delay_start;
	}
	return true;
      }
      return false;
    }
    void _dequeue(PG *pg) {
      if (pg->recovery_item.remove_myself())
	pg->put();
    }
    PG *_dequeue() {
      if (osd->recovery_queue.empty())
	return NULL;
      
      if (!osd->_recover_now())
	return NULL;

      PG *pg = osd->recovery_queue.front();
      osd->recovery_queue.pop_front();
      return pg;
    }
    void _queue_front(PG *pg) {
      if (!pg->recovery_item.is_on_list()) {
	pg->get();
	osd->recovery_queue.push_front(&pg->recovery_item);
      }
    }
    void _process(PG *pg) {
      osd->do_recovery(pg);
      pg->put();
    }
    void _clear() {
      while (!osd->recovery_queue.empty()) {
	PG *pg = osd->recovery_queue.front();
	osd->recovery_queue.pop_front();
	pg->put();
      }
    }
  } recovery_wq;


  struct RepScrubWQ : public ThreadPool::WorkQueue<MOSDRepScrub> {
  private: 
    PGOSD *osd;
    list<MOSDRepScrub*> rep_scrub_queue;

  public:
    RepScrubWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<MOSDRepScrub>("OSD::RepScrubWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return rep_scrub_queue.empty();
    }
    bool _enqueue(MOSDRepScrub *msg) {
      rep_scrub_queue.push_back(msg);
      return true;
    }
    void _dequeue(MOSDRepScrub *msg) {
      assert(0); // Not applicable for this wq
      return;
    }
    MOSDRepScrub *_dequeue() {
      if (rep_scrub_queue.empty())
	return NULL;
      MOSDRepScrub *msg = rep_scrub_queue.front();
      rep_scrub_queue.pop_front();
      return msg;
    }
    void _process(MOSDRepScrub *msg) {
      osd->osd_lock.Lock();
      if (osd->_have_pg(msg->pgid)) {
	PG *pg = osd->_lookup_lock_pg(msg->pgid);
	osd->osd_lock.Unlock();
	pg->replica_scrub(msg);
	msg->put();
	pg->unlock();
      } else {
	msg->put();
	osd->osd_lock.Unlock();
      }
    }
    void _clear() {
      while (!rep_scrub_queue.empty()) {
	MOSDRepScrub *msg = rep_scrub_queue.front();
	rep_scrub_queue.pop_front();
	msg->put();
      }
    }
  } rep_scrub_wq;


  uint64_t next_removal_seq;
  coll_t get_next_removal_coll(pg_t pgid) {
    return coll_t::make_removal_coll(next_removal_seq++, pgid);
  }


  /// check if op has sufficient caps
  bool op_has_sufficient_caps(PG *pg, class MOSDOp *m);

  void handle_watch_timeout(void *obc,
			    ReplicatedPG *pg,
			    entity_name_t entity,
			    utime_t expire);
  void handle_notify_timeout(void *notif);

  void put_object_context(void *_obc, pg_t pgid);
  void ack_notification(entity_name_t& peer_addr, void *notif, void *obc,
			ReplicatedPG *pg);

  void disconnect_session_watches(PGSession *session);

  friend class PGOSDService;
};


#endif // CEPH_PGOSD_H
