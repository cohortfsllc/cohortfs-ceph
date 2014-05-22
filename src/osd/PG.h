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

#ifndef CEPH_PG_H
#define CEPH_PG_H

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>
#include <boost/statechart/event_base.hpp>
#include <boost/scoped_ptr.hpp>
#include "include/memory.h"

// re-include our assert to clobber boost's
#include "include/assert.h"

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"
#include "include/atomic.h"

#include "PGLog.h"
#include "OpRequest.h"
#include "OSDMap.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"
#include "common/cmdparse.h"
#include "common/tracked_int_ptr.hpp"
#include "common/WorkQueue.h"
#include "common/ceph_context.h"
#include "include/str_list.h"
#include "PGBackend.h"
#include "OSDriver.h"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include "include/unordered_map.h"
#include "include/unordered_set.h"


class OSD;
class OSDService;
class MOSDOp;

class PG;

void intrusive_ptr_add_ref(PG *pg);
void intrusive_ptr_release(PG *pg);

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg);
  void put_with_id(PG *pg, uint64_t id);
  typedef TrackedIntPtr<PG> PGRef;
#else
  typedef boost::intrusive_ptr<PG> PGRef;
#endif

struct PGPool {
  int64_t id;
  string name;
  uint64_t auid;

  pg_pool_t info;

  PGPool(int64_t i, const char *_name, uint64_t au) :
    id(i), auid(au) {
    if (_name)
      name = _name;
  }

  void update(OSDMapRef map);
};

/** PG - Replica Placement Group
 *
 */

class PG {
public:
  std::string gen_prefix() const;

  /*** PG ****/
protected:
  OSDService *osd;
  CephContext *cct;
  OSDriver osdriver;

  virtual PGBackend *get_pgbackend() = 0;

  // Ops waiting for map, should be queued at back
  Mutex map_lock;
  list<OpRequestRef> waiting_for_map;
  OSDMapRef osdmap_ref;
  OSDMapRef last_persisted_osdmap_ref;
  PGPool pool;

  void queue_op(OpRequestRef op);
  void take_op_map_waiters();

  void update_osdmap_ref(OSDMapRef newmap) {
    assert(_lock.is_locked_by_me());
    Mutex::Locker l(map_lock);
    osdmap_ref = newmap;
  }

  OSDMapRef get_osdmap_with_maplock() const {
    assert(map_lock.is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }

  OSDMapRef get_osdmap() const {
    assert(is_locked());
    assert(osdmap_ref);
    return osdmap_ref;
  }

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  Mutex _lock;
  atomic_t ref;

#ifdef PG_DEBUG_REFS
  Mutex _ref_id_lock;
  map<uint64_t, string> _live_ids;
  map<string, uint64_t> _tag_counts;
  uint64_t _ref_id;
#endif

public:
  bool deleting;  // true while in removing or OSD is shutting down


  int whoami();
  void lock_suspend_timeout(ThreadPool::TPHandle &handle);
  void lock(bool no_lockdep = false);
  void unlock() {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    assert(!dirty_info);
    _lock.Unlock();
  }

  void assert_locked() {
    assert(_lock.is_locked());
  }
  bool is_locked() const {
    return _lock.is_locked();
  }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id();
  void put_with_id(uint64_t);
  void dump_live_ids();
#endif
  void get(const string &tag);
  void put(const string &tag);

  bool dirty_info;

public:
  // pg state
  pg_info_t info;
  uint8_t info_struct_v;
  static const uint8_t cur_struct_v = 7;
  const coll_t coll;
  PGLog  pg_log;
  static string get_info_key(pg_t pgid) {
    return stringify(pgid) + "_info";
  }
  static string get_epoch_key(pg_t pgid) {
    return stringify(pgid) + "_epoch";
  }
  hobject_t    log_oid;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item stat_queue_item;


protected:
  unsigned    state;   // PG_STATE_*

public:
  eversion_t  last_update_ondisk;    // last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_complete_ondisk;  // last_complete that has committed.
  eversion_t  last_update_applied;

protected:

  friend class OSD;

  list<OpRequestRef> waiting_for_active;
  map<eversion_t,list<OpRequestRef> > waiting_for_ack, waiting_for_ondisk;

  void requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m);
  void requeue_op(OpRequestRef op);
  void requeue_ops(list<OpRequestRef> &l);

  // stats that persist lazily
  object_stat_collection_t unstable_stats;

  // publish stats
  Mutex pg_stats_publish_lock;
  bool pg_stats_publish_valid;
  pg_stat_t pg_stats_publish;

  // for ordering writes
  ceph::shared_ptr<ObjectStore::Sequencer> osr;

  void _update_calc_stats();
  void publish_stats_to_osd();
  void clear_publish_stats();

public:
  void clear_primary_state();

 public:
  struct LogEntryTrimmer : public ObjectModDesc::Visitor {
    const hobject_t &soid;
    PG *pg;
    ObjectStore::Transaction *t;
    LogEntryTrimmer(const hobject_t &soid, PG *pg, ObjectStore::Transaction *t)
      : soid(soid), pg(pg), t(t) {}
    void rmobject(version_t old_version) {
      pg->get_pgbackend()->trim_stashed_object(
	soid,
	old_version,
	t);
    }
  };

  struct PGLogEntryHandler : public PGLog::LogEntryHandler {
    list<pg_log_entry_t> to_rollback;
    set<hobject_t> to_remove;
    list<pg_log_entry_t> to_trim;

    // LogEntryHandler
    void remove(const hobject_t &hoid) {
      to_remove.insert(hoid);
    }
    void rollback(const pg_log_entry_t &entry) {
      to_rollback.push_back(entry);
    }
    void trim(const pg_log_entry_t &entry) {
      to_trim.push_back(entry);
    }

    void apply(PG *pg, ObjectStore::Transaction *t) {
      for (set<hobject_t>::iterator i = to_remove.begin();
	   i != to_remove.end();
	   ++i) {
	pg->remove_object(*t, *i);
      }
      for (list<pg_log_entry_t>::reverse_iterator i = to_trim.rbegin();
	   i != to_trim.rend();
	   ++i) {
	LogEntryTrimmer trimmer(i->soid, pg, t);
	i->mod_desc.visit(&trimmer);
      }
    }
  };

  friend struct PGLogEntryHandler;
  friend struct LogEntryTrimmer;
  void remove_object(
    ObjectStore::Transaction& t, const hobject_t& soid);

  void trim_write_ahead();

  void activate(ObjectStore::Transaction& t, epoch_t query_epoch);
  void _activate_committed(epoch_t e);
  void all_activated_and_committed();

  virtual void check_local() = 0;

  Context *finish_sync_event;

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;

  int active_pushes;

 public:
  PG(OSDService *o, OSDMapRef curmap,
     const PGPool &pool, pg_t p, const hobject_t& loid, const hobject_t& ioid);
  virtual ~PG();

 private:
  // Prevent copying
  PG(const PG& rhs);
  PG& operator=(const PG& rhs);

 public:
  pg_t      get_pgid() const { return info.pgid; }

  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }

  int get_state() const { return state; }
  bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  void init(pg_history_t& history, ObjectStore::Transaction *t);

private:
  void write_info(ObjectStore::Transaction& t);

public:
  static int _write_info(ObjectStore::Transaction& t, epoch_t epoch,
    pg_info_t &info, coll_t coll, hobject_t &infos_oid,
    uint8_t info_struct_v, bool force_ver = false);
  void write_if_dirty(ObjectStore::Transaction& t);

  eversion_t get_next_version() const {
    eversion_t at_version(get_osdmap()->get_epoch(),
			  pg_log.get_head().version+1);
    assert(at_version > info.last_update);
    assert(at_version > pg_log.get_head());
    return at_version;
  }

  void add_log_entry(pg_log_entry_t& e, bufferlist& log_bl);
  void append_log(
    vector<pg_log_entry_t>& logv, ObjectStore::Transaction &t,
    bool transaction_applied = true);
  bool check_log_for_corruption(ObjectStore *store);

  std::string get_corrupt_pg_log_name() const;
  static int read_info(
    ObjectStore *store, const coll_t coll,
    bufferlist &bl, pg_info_t &info,
    hobject_t &infos_oid, uint8_t &);
  void read_state(ObjectStore *store, bufferlist &bl);
  static epoch_t peek_map_epoch(ObjectStore *store, coll_t coll,
				hobject_t &infos_oid, bufferlist *bl);
  void log_weirdness();

  virtual void get_colls(list<coll_t> *out) = 0;

  // OpRequest queueing
  bool can_discard_op(OpRequestRef op);
  bool can_discard_scan(OpRequestRef op);
  bool can_discard_request(OpRequestRef op);

  template<typename T, int MSGTYPE>
  bool can_discard_replica_op(OpRequestRef op);

  static bool op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op);

  static bool have_same_or_newer_map(OSDMapRef osdmap, epoch_t e) {
    return e <= osdmap->get_epoch();
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap()->get_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef op);


  void take_waiters();
  void handle_advance_map(
    OSDMapRef osdmap, OSDMapRef lastmap);
  void handle_activate_map();

  virtual void on_removal(ObjectStore::Transaction *t) = 0;


  // abstract bits
  virtual void do_request(
    OpRequestRef op,
    ThreadPool::TPHandle &handle
  ) = 0;

  virtual void do_op(OpRequestRef op) = 0;
  virtual int do_command(cmdmap_t cmdmap, ostream& ss,
			 bufferlist& idata, bufferlist& odata) = 0;

  virtual void on_pool_change() = 0;
  virtual void on_change(ObjectStore::Transaction *t) = 0;
  virtual void on_activate() = 0;
  virtual void on_shutdown() = 0;
  virtual void check_blacklisted_watchers() = 0;
  virtual void get_watchers(std::list<obj_watch_item_t>&) = 0;
};

ostream& operator<<(ostream& out, const PG& pg);

#endif
