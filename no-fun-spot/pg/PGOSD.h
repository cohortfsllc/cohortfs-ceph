// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 *
 */

#ifndef CEPH_PGOSD_H
#define CEPH_PGOSD_H

#include "include/types.h"

// stl
#include <string>
#include <set>
#include <map>
#include <fstream>
using std::set;
using std::map;
using std::fstream;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/filepath.h"
#include "include/interval_set.h"
#include "include/lru.h"

#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/Finisher.h"

#include "common/compiler_extensions.h"


#include "osd/OSD.h"
#include "PG.h"
#include "common/AsyncReserver.h"
#include "Watch.h"
#include "messages/MOSDScrub.h"

class PGOSD;
class PGOSDService;
class ReplicatedPG;
struct C_CompleteSplits;
class TestOpsSocketHook;

class DeletingState {
  Mutex lock;
  Cond cond;
  enum {
    QUEUED,
    CLEARING_DIR,
    DELETING_DIR,
    DELETED_DIR,
    CANCELED,
  } status;
  bool stop_deleting;
public:
  const pg_t pgid;
  const PGRef old_pg_state;
  DeletingState(const pair<pg_t, PGRef> &in) :
    lock("DeletingState::lock"), status(QUEUED), stop_deleting(false),
    pgid(in.first), old_pg_state(in.second) {}

  /// check whether removal was canceled
  bool check_canceled() {
    Mutex::Locker l(lock);
    assert(status == CLEARING_DIR);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    return true;
  } ///< @return false if canceled, true if we should continue

  /// transition status to clearing
  bool start_clearing() {
    Mutex::Locker l(lock);
    assert(
      status == QUEUED ||
      status == DELETED_DIR);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = CLEARING_DIR;
    return true;
  } ///< @return false if we should cancel deletion

  /// transition status to deleting
  bool start_deleting() {
    Mutex::Locker l(lock);
    assert(status == CLEARING_DIR);
    if (stop_deleting) {
      status = CANCELED;
      cond.Signal();
      return false;
    }
    status = DELETING_DIR;
    return true;
  } ///< @return false if we should cancel deletion

  /// signal collection removal queued
  void finish_deleting() {
    Mutex::Locker l(lock);
    assert(status == DELETING_DIR);
    status = DELETED_DIR;
    cond.Signal();
  }

  /// try to halt the deletion
  bool try_stop_deletion() {
    Mutex::Locker l(lock);
    stop_deleting = true;
    /**
     * If we are in DELETING_DIR or CLEARING_DIR, there are in progress
     * operations we have to wait for before continuing on.  States
     * DELETED_DIR, QUEUED, and CANCELED either check for stop_deleting
     * prior to performing any operations or signify the end of the
     * deleting process.  We don't want to wait to leave the QUEUED
     * state, because this might block the caller behind an entire pg
     * removal.
     */
    while (status == DELETING_DIR || status == CLEARING_DIR)
      cond.Wait(lock);
    return status != DELETED_DIR;
  } ///< @return true if we don't need to recreate the collection
};
typedef std::tr1::shared_ptr<DeletingState> DeletingStateRef;



class PGOSDService : public OSDService {

public:

  virtual bool test_ops_sub(ObjectStore *store,
			    std::string command,
			    std::string args,
			    ostream &ss);
  SharedPtrRegistry<pg_t, DeletingState> deleting_pgs;
  SharedPtrRegistry<pg_t, ObjectStore::Sequencer> osr_registry;
  PGRecoveryStats &pg_recovery_stats;
  ThreadPool::WorkQueueVal<pair<PGRef, OpRequestRef>, PGRef> &op_wq;
  ThreadPool::BatchWorkQueue<PG> &peering_wq;
  ThreadPool::WorkQueue<PG> &recovery_wq;
  ThreadPool::WorkQueue<PG> &snap_trim_wq;
  ThreadPool::WorkQueue<PG> &scrub_wq;
  ThreadPool::WorkQueue<PG> &scrub_finalize_wq;
  ThreadPool::WorkQueue<MOSDRepScrub> &rep_scrub_wq;

  set< pair<utime_t,pg_t> > last_scrub_pg;

  void dequeue_pg(PG *pg, list<OpRequestRef> *dequeued);

  void reg_last_pg_scrub(pg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    last_scrub_pg.insert(pair<utime_t,pg_t>(t, pgid));
  }
  void unreg_last_pg_scrub(pg_t pgid, utime_t t) {
    Mutex::Locker l(sched_scrub_lock);
    pair<utime_t,pg_t> p(t, pgid);
    set<pair<utime_t,pg_t> >::iterator it = last_scrub_pg.find(p);
    assert(it != last_scrub_pg.end());
    last_scrub_pg.erase(it);
  }
  bool first_scrub_stamp(pair<utime_t, pg_t> *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (last_scrub_pg.empty())
      return false;
    set< pair<utime_t, pg_t> >::iterator iter = last_scrub_pg.begin();
    *out = *iter;
    return true;
  }
  bool next_scrub_stamp(pair<utime_t, pg_t> next,
			pair<utime_t, pg_t> *out) {
    Mutex::Locker l(sched_scrub_lock);
    if (last_scrub_pg.empty())
      return false;
    set< pair<utime_t, pg_t> >::iterator iter = last_scrub_pg.lower_bound(next);
    if (iter == last_scrub_pg.end())
      return false;
    ++iter;
    if (iter == last_scrub_pg.end())
      return false;
    *out = *iter;
    return true;
  }

  void handle_misdirected_op(PG *pg, OpRequestRef op);

  // -- pg_temp --
  Mutex pg_temp_lock;
  map<pg_t, vector<int> > pg_temp_wanted;

  // -- Watch --
  Mutex watch_lock;
  SafeTimer watch_timer;
  uint64_t next_notif_id;
  uint64_t get_next_id(epoch_t cur_epoch) {
    Mutex::Locker l(watch_lock);
    return (((uint64_t)cur_epoch) << 32) | ((uint64_t)(next_notif_id++));
  }


  // -- backfill_reservation --
  Finisher reserver_finisher;
  AsyncReserver<pg_t> local_reserver;
  AsyncReserver<pg_t> remote_reserver;

  void queue_want_pg_temp(pg_t pgid, vector<int>& want);
  void remove_want_pg_temp(pg_t pgid) {
    Mutex::Locker l(pg_temp_lock);
    pg_temp_wanted.erase(pgid);
  }
  void send_pg_temp();

  void queue_for_peering(PG *pg);
  bool queue_for_recovery(PG *pg);
  bool queue_for_snap_trim(PG *pg) {
    return snap_trim_wq.queue(pg);
  }
  bool queue_for_scrub(PG *pg) {
    return scrub_wq.queue(pg);
  }

  void pg_stat_queue_enqueue(PG *pg);
  void pg_stat_queue_dequeue(PG *pg);

  void init_sub();
  void shutdown_sub();

  // split
  Mutex in_progress_split_lock;
  map<pg_t, pg_t> pending_splits; // child -> parent
  map<pg_t, set<pg_t> > rev_pending_splits; // parent -> [children]
  set<pg_t> in_progress_splits;       // child

  void _start_split(pg_t parent, const set<pg_t> &children);
  void start_split(pg_t parent, const set<pg_t> &children) {
    Mutex::Locker l(in_progress_split_lock);
    return _start_split(parent, children);
  }
  void mark_split_in_progress(pg_t parent, const set<pg_t> &pgs);
  void complete_split(const set<pg_t> &pgs);
  void cancel_pending_splits_for_parent(pg_t parent);
  void _cancel_pending_splits_for_parent(pg_t parent);
  bool splitting(pg_t pgid);
  void expand_pg_num(PGOSDMapRef old_map,
		     PGOSDMapRef new_map);
  void _maybe_split_pgid(PGOSDMapRef old_map,
			 PGOSDMapRef new_map,
			 pg_t pgid);
  void init_splits_between(pg_t pgid, PGOSDMapRef frommap, PGOSDMapRef tomap);

#ifdef PG_DEBUG_REFS
  Mutex pgid_lock;
  map<pg_t, int> pgid_tracker;
  map<pg_t, PG*> live_pgs;
  void add_pgid(pg_t pgid, PG *pg) {
    Mutex::Locker l(pgid_lock);
    if (!pgid_tracker.count(pgid)) {
      pgid_tracker[pgid] = 0;
      live_pgs[pgid] = pg;
    }
    pgid_tracker[pgid]++;
  }
  void remove_pgid(pg_t pgid, PG *pg) {
    Mutex::Locker l(pgid_lock);
    assert(pgid_tracker.count(pgid));
    assert(pgid_tracker[pgid] > 0);
    pgid_tracker[pgid]--;
    if (pgid_tracker[pgid] == 0) {
      pgid_tracker.erase(pgid);
      live_pgs.erase(pgid);
    }
  }
  void dump_live_pgids() {
    Mutex::Locker l(pgid_lock);
    derr << "live pgids:" << dendl;
    for (map<pg_t, int>::iterator i = pgid_tracker.begin();
	 i != pgid_tracker.end();
	 ++i) {
      derr << "\t" << *i << dendl;
      live_pgs[i->first]->dump_live_ids();
    }
  }
#endif

  PGOSD* pgosd() const;

  PGOSDService(PGOSD *osd);

  virtual OSDMap* newOSDMap() const { return new PGOSDMap(); }
  PGOSDMapRef get_map(epoch_t e) {
    return dynamic_pointer_cast<const PGOSDMap>(get_map(e));
  }

  const PGOSDMapRef pgosdmap() {
    return dynamic_pointer_cast<const PGOSDMap>(get_osdmap());
  }
}; // class PGOSDService

typedef shared_ptr<PGOSDService> PGOSDServiceRef;


class PGOSD : public OSD {

public:

  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

  struct PGSession : public Session {
    std::map<void *, pg_t> watches;

    WatchConState wstate;
    PGSession() : Session() {}
  };


  PGOSD(int id, Messenger *internal, Messenger *external,
	Messenger *hb_client, Messenger *hb_front_server, Messenger *hb_back_server,
	MonClient *mc, const std::string &dev, const std::string &jdev);

  virtual PGOSDService* newOSDService(OSD* osd) const {
    PGOSD* pgosd = dynamic_cast<PGOSD*>(osd);
    return new PGOSDService(pgosd);
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

  virtual void ms_handle_reset_sub(OSD::Session* session);

  virtual void advance_map_sub(ObjectStore::Transaction& t,
			       C_Contexts *tfin);
  virtual void consume_map_sub();
  virtual void dispatch_op_sub(OpRequestRef op);
  virtual bool _dispatch_sub(Message *m);
  void handle_rep_scrub(MOSDRepScrub *m);
  void handle_scrub(MOSDScrub *m);

  static hobject_t make_pg_log_oid(pg_t pg) {
    stringstream ss;
    ss << "pglog_" << pg;
    string s;
    getline(ss, s);
    return hobject_t(sobject_t(object_t(s.c_str()), 0));
  }

  static hobject_t make_pg_biginfo_oid(pg_t pg) {
    stringstream ss;
    ss << "pginfo_" << pg;
    string s;
    getline(ss, s);
    return hobject_t(sobject_t(object_t(s.c_str()), 0));
  }

  // -- op queue --
  list<PG*> op_queue;
  int op_queue_len;

  struct OpWQ: public ThreadPool::WorkQueueVal<pair<PGRef, OpRequestRef>,
					       PGRef > {
    Mutex qlock;
    map<PG*, list<OpRequestRef> > pg_for_processing;
    PGOSD *osd;
    PrioritizedQueue<pair<PGRef, OpRequestRef>, entity_inst_t > pqueue;
    OpWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<pair<PGRef, OpRequestRef>, PGRef >(
	"PGOSD::OpWQ", ti, ti*10, tp),
	qlock("OpWQ::qlock"),
	osd(o),
	pqueue(o->cct->_conf->osd_op_pq_max_tokens_per_priority,
	       o->cct->_conf->osd_op_pq_min_cost)
    {}

    void dump(Formatter *f) {
      Mutex::Locker l(qlock);
      pqueue.dump(f);
    }

    void _enqueue_front(pair<PGRef, OpRequestRef> item);
    void _enqueue(pair<PGRef, OpRequestRef> item);
    PGRef _dequeue();

    struct Pred {
      PG *pg;
      Pred(PG *pg) : pg(pg) {}
      bool operator()(const pair<PGRef, OpRequestRef> &op) {
	return op.first == pg;
      }
    };
    void dequeue(PG *pg, list<OpRequestRef> *dequeued = 0) {
      lock();
      if (!dequeued) {
	pqueue.remove_by_filter(Pred(pg));
	pg_for_processing.erase(pg);
      } else {
	list<pair<PGRef, OpRequestRef> > _dequeued;
	pqueue.remove_by_filter(Pred(pg), &_dequeued);
	for (list<pair<PGRef, OpRequestRef> >::iterator i = _dequeued.begin();
	     i != _dequeued.end();
	     ++i) {
	  dequeued->push_back(i->second);
	}
	if (pg_for_processing.count(pg)) {
	  dequeued->splice(
	    dequeued->begin(),
	    pg_for_processing[pg]);
	  pg_for_processing.erase(pg);
	}
      }
      unlock();
    }
    bool _empty() {
      return pqueue.empty();
    }
    void _process(PGRef pg);
  } op_wq;

  void enqueue_op(PG *pg, OpRequestRef op);
  void dequeue_op(PGRef pg, OpRequestRef op);

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
	  pg->put("PeeringWQ");
	} else {
	  ++i;
	}
      }
    }
    bool _enqueue(PG *pg) {
      pg->get("PeeringWQ");
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
    void _process(
      const list<PG *> &pgs,
      ThreadPool::TPHandle &handle) {
      osd->process_peering_events(pgs, handle);
      for (list<PG *>::const_iterator i = pgs.begin();
	   i != pgs.end();
	   ++i) {
	(*i)->put("PeeringWQ");
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

  void process_peering_events(
    const list<PG*> &pg,
    ThreadPool::TPHandle &handle);

  friend class PG;
  friend class ReplicatedPG;

protected:

  PGOSDMapRef get_map(epoch_t e) {
    return dynamic_pointer_cast<const PGOSDMap>(get_map(e));
  }

  const PGOSDMapRef pgosdmap() {
    return dynamic_pointer_cast<const PGOSDMap>(get_osdmap());
  }

  const PGOSDMapRef pgosdmap(OSDMapRef ref) const {
    return dynamic_pointer_cast<const PGOSDMap>(ref);
  }

  const PGOSDServiceRef pgosdservice() const {
    return dynamic_pointer_cast<PGOSDService>(service);
  }

  void advance_pg(
    epoch_t advance_to, PG *pg,
    ThreadPool::TPHandle &handle,
    PG::RecoveryCtx *rctx,
    set<boost::intrusive_ptr<PG> > *split_pgs
  );

protected:

  // -- placement groups --
  hash_map<pg_t, PG*> pg_map;
  map<pg_t, list<OpRequestRef> > waiting_for_pg;
  map<pg_t, list<PG::CephPeeringEvtRef> > peering_wait_for_split;
  PGRecoveryStats pg_recovery_stats;

  PGPool _get_pool(int id, OSDMapRef createmap);

  bool  _have_pg(pg_t pgid);
  PG   *_lookup_lock_pg_with_map_lock_held(pg_t pgid);
  PG   *_lookup_lock_pg(pg_t pgid);
  PG   *_lookup_pg(pg_t pgid);
  PG   *_open_lock_pg(OSDMapRef createmap,
		      pg_t pg, bool no_lockdep_check=false,
		      bool hold_map_lock=false);
  enum res_result {
    RES_PARENT,    // resurrected a parent
    RES_SELF,      // resurrected self
    RES_NONE       // nothing relevant deleting
  };
  res_result _try_resurrect_pg(
    OSDMapRef curmap, pg_t pgid, pg_t *resurrected, PGRef *old_pg_state);
  PG   *_create_lock_pg(PGOSDMapRef createmap,
			pg_t pgid,
			bool newly_created,
			bool hold_map_lock,
			bool backfill,
			int role,
			vector<int>& up,
			vector<int>& acting,
			pg_history_t history,
			pg_interval_map_t& pi,
			ObjectStore::Transaction& t);
  PG   *_lookup_qlock_pg(pg_t pgid);

  PG* _make_pg(PGOSDMapRef createmap, pg_t pgid);
  void add_newly_split_pg(PG *pg,
			  PG::RecoveryCtx *rctx);

  void handle_pg_peering_evt(
    const pg_info_t& info,
    pg_interval_map_t& pi,
    epoch_t epoch, int from,
    bool primary,
    PG::CephPeeringEvtRef evt);

  void load_pgs();
  void build_past_intervals_parallel();

  void calc_priors_during(pg_t pgid, epoch_t start, epoch_t end, set<int>& pset);
  void project_pg_history(pg_t pgid, pg_history_t& h, epoch_t from,
			  const vector<int>& lastup, const vector<int>& lastacting);

  void wake_pg_waiters(pg_t pgid) {
    if (waiting_for_pg.count(pgid)) {
      take_waiters_front(waiting_for_pg[pgid]);
      waiting_for_pg.erase(pgid);
    }
  }
  void wake_all_pg_waiters() {
    for (map<pg_t, list<OpRequestRef> >::iterator p = waiting_for_pg.begin();
	 p != waiting_for_pg.end();
	 ++p)
      take_waiters_front(p->second);
    waiting_for_pg.clear();
  }


  // -- pg creation --
  struct create_pg_info {
    pg_history_t history;
    vector<int> acting;
    set<int> prior;
    pg_t parent;
  };
  hash_map<pg_t, create_pg_info> creating_pgs;
  double debug_drop_pg_create_probability;
  int debug_drop_pg_create_duration;
  int debug_drop_pg_create_left;  // 0 if we just dropped the last one, -1 if we can drop more

  bool can_create_pg(pg_t pgid);
  void handle_pg_create(OpRequestRef op);

  void split_pgs(
    PG *parent,
    const set<pg_t> &childpgids, set<boost::intrusive_ptr<PG> > *out_pgs,
    PGOSDMapRef curmap,
    PGOSDMapRef nextmap,
    PG::RecoveryCtx *rctx);
  /* if our monitor dies, we want to notice it and reconnect.
   *  So we keep track of when it last acked our stat updates,
   *  and if too much time passes (and we've been sending
   *  more updates) then we can call it dead and reconnect
   *  elsewhere.
   */
  utime_t last_pg_stats_ack;
  utime_t last_pg_stats_sent;
  bool outstanding_pg_stats; // some stat updates haven't been acked yet

  // -- pg stats --
  Mutex pg_stat_queue_lock;
  Cond pg_stat_queue_cond;
  xlist<PG*> pg_stat_queue;
  bool osd_stat_updated;
  uint64_t pg_stat_tid, pg_stat_tid_flushed;

  void send_pg_stats(const utime_t &now);
  void handle_pg_stats_ack(class MPGStatsAck *ack);
  void flush_pg_stats();

  void pg_stat_queue_enqueue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->is_primary() && !pg->stat_queue_item.is_on_list()) {
      pg->get("pg_stat_queue");
      pg_stat_queue.push_back(&pg->stat_queue_item);
    }
    osd_stat_updated = true;
    pg_stat_queue_lock.Unlock();
  }
  void pg_stat_queue_dequeue(PG *pg) {
    pg_stat_queue_lock.Lock();
    if (pg->stat_queue_item.remove_myself())
      pg->put("pg_stat_queue");
    pg_stat_queue_lock.Unlock();
  }
  void clear_pg_stat_queue() {
    pg_stat_queue_lock.Lock();
    while (!pg_stat_queue.empty()) {
      PG *pg = pg_stat_queue.front();
      pg_stat_queue.pop_front();
      pg->put("pg_stat_queue");
    }
    pg_stat_queue_lock.Unlock();
  }

  // -- generic pg peering --
  PG::RecoveryCtx create_context();
  bool compat_must_dispatch_immediately(PG *pg);
  void dispatch_context(PG::RecoveryCtx &ctx, PG *pg, PGOSDMapRef curmap);
  void dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg);
  void do_notifies(map< int,vector<pair<pg_notify_t, pg_interval_map_t> > >& notify_list,
		   PGOSDMapRef map);
  void do_queries(map< int, map<pg_t,pg_query_t> >& query_map,
		  PGOSDMapRef map);
  void do_infos(map<int, vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		PGOSDMapRef map);
  void repeer(PG *pg, map< int, map<pg_t,pg_query_t> >& query_map);

  void handle_pg_query(OpRequestRef op);
  void handle_pg_notify(OpRequestRef op);
  void handle_pg_log(OpRequestRef op);
  void handle_pg_info(OpRequestRef op);
  void handle_pg_trim(OpRequestRef op);

  void handle_pg_scan(OpRequestRef op);

  void handle_pg_backfill(OpRequestRef op);
  void handle_pg_backfill_reserve(OpRequestRef op);
  void handle_pg_recovery_reserve(OpRequestRef op);

  void handle_pg_remove(OpRequestRef op);
  void _remove_pg(PG *pg);

  // -- pg recovery --
  xlist<PG*> recovery_queue;
  utime_t defer_recovery_until;
  int recovery_ops_active;
#ifdef DEBUG_RECOVERY_OIDS
  map<pg_t, set<hobject_t> > recovery_oids;
#endif

  struct RecoveryWQ : public ThreadPool::WorkQueue<PG> {
    PGOSD *osd;
    RecoveryWQ(PGOSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<PG>("OSD::RecoveryWQ", ti, ti*10, tp), osd(o) {}

    bool _empty() {
      return osd->recovery_queue.empty();
    }
    bool _enqueue(PG *pg) {
      if (!pg->recovery_item.is_on_list()) {
	pg->get("RecoveryWQ");
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
	pg->put("RecoveryWQ");
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
	pg->get("RecoveryWQ");
	osd->recovery_queue.push_front(&pg->recovery_item);
      }
    }
    void _process(PG *pg) {
      osd->do_recovery(pg);
      pg->put("RecoveryWQ");
    }
    void _clear() {
      while (!osd->recovery_queue.empty()) {
	PG *pg = osd->recovery_queue.front();
	osd->recovery_queue.pop_front();
	pg->put("RecoveryWQ");
      }
    }
  } recovery_wq;

  void start_recovery_op(PG *pg, const hobject_t& soid);
  void finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue);
  void do_recovery(PG *pg);
  bool _recover_now();

  // replay / delayed pg activation
  Mutex replay_queue_lock;
  list< pair<pg_t, utime_t > > replay_queue;

  void check_replay_queue();

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
      pg->get("SnapTrimWQ");
      osd->snap_trim_queue.push_back(&pg->snap_trim_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->snap_trim_item.remove_myself())
	pg->put("SnapTrimWQ");
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
      pg->put("SnapTrimWQ");
    }
    void _clear() {
      osd->snap_trim_queue.clear();
    }
  } snap_trim_wq;

  // -- scrubbing --
  void sched_scrub();
  bool scrub_random_backoff();
  bool scrub_should_schedule();

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
      pg->get("ScrubWQ");
      osd->scrub_queue.push_back(&pg->scrub_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_item.remove_myself()) {
	pg->put("ScrubWQ");
      }
    }
    PG *_dequeue() {
      if (osd->scrub_queue.empty())
	return NULL;
      PG *pg = osd->scrub_queue.front();
      osd->scrub_queue.pop_front();
      return pg;
    }
    void _process(
      PG *pg,
      ThreadPool::TPHandle &handle) {
      pg->scrub(handle);
      pg->put("ScrubWQ");
    }
    void _clear() {
      while (!osd->scrub_queue.empty()) {
	PG *pg = osd->scrub_queue.front();
	osd->scrub_queue.pop_front();
	pg->put("ScrubWQ");
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
      pg->get("ScrubFinalizeWQ");
      scrub_finalize_queue.push_back(&pg->scrub_finalize_item);
      return true;
    }
    void _dequeue(PG *pg) {
      if (pg->scrub_finalize_item.remove_myself()) {
	pg->put("ScrubFinalizeWQ");
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
      pg->put("ScrubFinalizeWQ");
    }
    void _clear() {
      while (!scrub_finalize_queue.empty()) {
	PG *pg = scrub_finalize_queue.front();
	scrub_finalize_queue.pop_front();
	pg->put("ScrubFinalizeWQ");
      }
    }
  } scrub_finalize_wq;

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
    void _process(
      MOSDRepScrub *msg,
      ThreadPool::TPHandle &handle) {
      osd->osd_lock.Lock();
      if (osd->is_stopping()) {
	osd->osd_lock.Unlock();
	return;
      }
      if (osd->_have_pg(msg->pgid)) {
	PG *pg = osd->_lookup_lock_pg(msg->pgid);
	osd->osd_lock.Unlock();
	pg->replica_scrub(msg, handle);
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

  virtual bool asok_command_sub(string command, string args, ostream& ss);

  // -- removing --
  struct RemoveWQ :
    public ThreadPool::WorkQueueVal<pair<PGRef, DeletingStateRef> > {
    ObjectStore *&store;
    list<pair<PGRef, DeletingStateRef> > remove_queue;
    RemoveWQ(ObjectStore *&o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<pair<PGRef, DeletingStateRef> >(
	"OSD::RemoveWQ", ti, 0, tp),
	store(o) {}

    bool _empty() {
      return remove_queue.empty();
    }
    void _enqueue(pair<PGRef, DeletingStateRef> item) {
      remove_queue.push_back(item);
    }
    void _enqueue_front(pair<PGRef, DeletingStateRef> item) {
      remove_queue.push_front(item);
    }
    bool _dequeue(pair<PGRef, DeletingStateRef> item) {
      assert(0);
    }
    pair<PGRef, DeletingStateRef> _dequeue() {
      assert(!remove_queue.empty());
      pair<PGRef, DeletingStateRef> item = remove_queue.front();
      remove_queue.pop_front();
      return item;
    }
    void _process(pair<PGRef, DeletingStateRef>);
    void _clear() {
      remove_queue.clear();
    }
  } remove_wq;


public:

  /// check if op has sufficient caps
  bool op_has_sufficient_caps(PG *pg, class MOSDOp *m);

  void handle_watch_timeout(void *obc,
			    ReplicatedPG *pg,
			    entity_name_t entity,
			    utime_t expire);
  void put_object_context(void *_obc, pg_t pgid);

  void disconnect_session_watches(PGSession *session);

  friend class PGOSDService;

  OSDMap* newOSDMap() const {
    return new PGOSDMap();
  }
  friend struct C_CompleteSplits;
};


#endif // CEPH_PGOSD_H
