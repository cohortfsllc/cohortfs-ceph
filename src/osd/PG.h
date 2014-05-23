// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser Generansactionl Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_PG_H
#define CEPH_PG_H

#include <boost/scoped_ptr.hpp>
#include <boost/optional.hpp>
#include "include/memory.h"

// re-include our assert to clobber boost's
#include "include/assert.h"

#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"
#include "include/atomic.h"

#include "OpRequest.h"
#include "OSDMap.h"
#include "Watch.h"
#include "OpRequest.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"
#include "common/cmdparse.h"
#include "common/tracked_int_ptr.hpp"
#include "common/WorkQueue.h"
#include "common/ceph_context.h"
#include "common/LogClient.h"
#include "include/str_list.h"
#include "OSDriver.h"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include "include/unordered_map.h"
#include "include/unordered_set.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "common/sharedptr_registry.hpp"


class OSD;
class OSDService;
class MOSDOp;

class PG;

void intrusive_ptr_add_ref(PG *pg);
void intrusive_ptr_release(PG *pg);

typedef boost::intrusive_ptr<PG> PGRef;

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

class PGLSFilter {
protected:
  string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(bufferlist& xattr_data, bufferlist& outdata) = 0;
  virtual string& get_xattr() { return xattr; }
};

class PGLSPlainFilter : public PGLSFilter {
  string val;
public:
  PGLSPlainFilter(bufferlist::iterator& params) {
    ::decode(xattr, params);
    ::decode(val, params);
  }
  virtual ~PGLSPlainFilter() {}
  virtual bool filter(bufferlist& xattr_data, bufferlist& outdata);
};

class PGLSParentFilter : public PGLSFilter {
  inodeno_t parent_ino;
public:
  PGLSParentFilter(bufferlist::iterator& params) {
    xattr = "_parent";
    ::decode(parent_ino, params);
    generic_dout(0) << "parent_ino=" << parent_ino << dendl;
  }
  virtual ~PGLSParentFilter() {}
  virtual bool filter(bufferlist& xattr_data, bufferlist& outdata);
};


/** PG - Replica Placement Group
 */

class PG {
  friend class OSD;
  friend class Watch;
  friend class C_OSD_OnOpCommit;
  friend class C_OSD_OnOpApplied;

public:
  std::string gen_prefix() const;

  struct OpContext;
  struct InProgressOp {
    ceph_tid_t tid;
    Context *on_commit;
    Context *on_applied;
    OpRequestRef op;
    eversion_t v;
    InProgressOp(
      ceph_tid_t tid, Context *on_commit, Context *on_applied,
      OpRequestRef op, eversion_t v)
      : tid(tid), on_commit(on_commit), on_applied(on_applied),
	op(op), v(v) {}
  };
  map<ceph_tid_t, InProgressOp> in_progress_ops;
  /**
   * Client IO Interface
   */
  class Transaction {
    coll_t coll;
    ObjectStore::Transaction *t;
    const coll_t &get_coll_ct(const hobject_t &hoid) {
      return get_coll(hoid);
    }
    const coll_t &get_coll_rm(const hobject_t &hoid) {
      return get_coll(hoid);
    }
    const coll_t &get_coll(const hobject_t &hoid) {
      return coll;
    }

  public:
    Transaction(coll_t coll) :
      coll(coll), t(new ObjectStore::Transaction) {}
    ObjectStore::Transaction *get_transaction();

    void touch(const hobject_t &hoid);
    void stash(const hobject_t &hoid, version_t former_version);
    void remove(const hobject_t &hoid);
    void setattrs(const hobject_t &hoid, map<string, bufferlist> &attrs);
    void setattr(const hobject_t &hoid, const string &attrname,
		 bufferlist &bl);
    void rmattr(const hobject_t &hoid, const string &attrname);
    void rename(const hobject_t &from, const hobject_t &to);
    void set_alloc_hint(const hobject_t &hoid, uint64_t expected_object_size,
			uint64_t expected_write_size);
    void write(const hobject_t &hoid, uint64_t off,
	       uint64_t len, bufferlist &bl);
    void omap_setkeys(const hobject_t &hoid,
		      map<string, bufferlist> &keys);
    void omap_rmkeys(const hobject_t &hoid,
		     set<string> &keys);
    void omap_clear(const hobject_t &hoid);
    void omap_setheader(const hobject_t &hoid, bufferlist &header);
    void truncate(const hobject_t &hoid, uint64_t off);
    void zero(const hobject_t &hoid, uint64_t off, uint64_t len);
    /// off must be the current object size
    void append(const hobject_t &hoid, uint64_t off,
		uint64_t len, bufferlist &bl) {
      write(hoid, off, len, bl);
    }
    void append(Transaction *to_append);
    void nop();
    bool empty() const;
    uint64_t get_bytes_written() const;
    ~Transaction();
  };

  void send_message(int to_osd, Message *m);
  void queue_transaction(ObjectStore::Transaction *t, OpRequestRef op);
  epoch_t get_epoch() const {
    return get_osdmap()->get_epoch();
  }

  std::string gen_dbg_prefix() const { return gen_prefix(); }

  OSDMapRef pgb_get_osdmap() const {
    return get_osdmap();
  }
  const pg_info_t &get_info() const {
    return info;
  }
  const pg_pool_t &get_pool() const {
    return pool.info;
  }
  ObjectContextRef get_obc(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs) {
    return get_object_context(hoid, true, &attrs);
  }

  void op_applied(InProgressOp *op);
  void op_commit(InProgressOp *op);

  void update_stats(
    const pg_stat_t &stat) {
    info.stats = stat;
  }

  entity_name_t get_cluster_msgr_name();

  PerfCounters *get_logger();

  ceph_tid_t get_tid();

  LogClientTemp clog_error();

  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    OpRequestRef op;
    osd_reqid_t reqid;
    vector<OSDOp> &ops;

    const ObjectState *obs; // Old objectstate

    ObjectState new_obs;  // resulting ObjectState
    object_stat_sum_t delta_stats;

    bool modify; // (force) modification (even if op_t is empty)
    bool user_modify; // user-visible modification

    // side effects
    list<watch_info_t> watch_connects;
    list<watch_info_t> watch_disconnects;
    list<notify_info_t> notifies;
    struct NotifyAck {
      boost::optional<uint64_t> watch_cookie;
      uint64_t notify_id;
      NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
      NotifyAck(uint64_t notify_id, uint64_t cookie)
	: watch_cookie(cookie), notify_id(notify_id) {}
    };
    list<NotifyAck> notify_acks;

    uint64_t bytes_written, bytes_read;

    utime_t mtime;
    eversion_t at_version;       // pg's current version pointer
    version_t user_at_version;   // pg's current user version pointer

    int current_osd_subop_num;

    Transaction *op_t;

    interval_set<uint64_t> modified_ranges;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef> src_obc;

    int data_off; // FIXME: may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    PG *pg;

    int num_read;    ///< count read ops
    int num_write;   ///< count update ops

    // pending xattr updates
    map<ObjectContextRef,
	map<string, boost::optional<bufferlist> > > pending_attrs;
    void apply_pending_attrs() {
      for (map<ObjectContextRef,
	     map<string, boost::optional<bufferlist> > >::iterator i =
	     pending_attrs.begin();
	   i != pending_attrs.end();
	   ++i) {
	if (i->first->obs.exists) {
	  for (map<string, boost::optional<bufferlist> >::iterator j =
		 i->second.begin();
	       j != i->second.end();
	       ++j) {
	    if (j->second)
	      i->first->attr_cache[j->first] = j->second.get();
	    else
	      i->first->attr_cache.erase(j->first);
	  }
	} else {
	  i->first->attr_cache.clear();
	}
      }
      pending_attrs.clear();
    }

    // pending async reads <off, len> -> <outbl, outr>
    list<pair<pair<uint64_t, uint64_t>,
	      pair<bufferlist*, Context*> > > pending_async_reads;
    int async_read_result;
    unsigned inflightreads;
    friend struct OnReadComplete;
    void start_async_reads(PG *pg);
    void finish_read(PG *pg);
    bool async_reads_complete() {
      return inflightreads == 0;
    }

    ObjectModDesc mod_desc;

    enum { W_LOCK, R_LOCK, NONE } lock_to_release;

    Context *on_finish;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, PG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(_obs),
      new_obs(_obs->oi, _obs->exists),
      modify(false), user_modify(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      op_t(NULL),
      data_off(0), reply(NULL), pg(_pg),
      num_read(0),
      num_write(0),
      async_read_result(0),
      inflightreads(0),
      lock_to_release(NONE),
      on_finish(NULL) {
    }
    void reset_obs(ObjectContextRef obc) {
      new_obs = ObjectState(obc->obs.oi, obc->obs.exists);
    }
    ~OpContext() {
      assert(!op_t);
      assert(lock_to_release == NONE);
      if (reply)
	reply->put();
      for (list<pair<pair<uint64_t, uint64_t>,
	     pair<bufferlist*, Context*> > >::iterator i =
	     pending_async_reads.begin();
	   i != pending_async_reads.end();
	   pending_async_reads.erase(i++)) {
	delete i->second.second;
      }
      assert(on_finish == NULL);
    }
    void finish(int r) {
      if (on_finish) {
	on_finish->complete(r);
	on_finish = NULL;
      }
    }
  };
  friend struct OpContext;

  /*
   * State on the PG primary associated with the replicated mutation
   */
  class RepGather {
  public:
    xlist<RepGather*>::item queue_item;
    int nref;

    eversion_t v;

    OpContext *ctx;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef> src_obc;

    ceph_tid_t rep_tid;

    bool rep_aborted, rep_done;

    bool all_applied;
    bool all_committed;
    bool sent_ack;
    bool sent_disk;

    utime_t   start;

    Context *on_applied;

    RepGather(OpContext *c, ObjectContextRef pi, ceph_tid_t rt) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      rep_tid(rt),
      rep_aborted(false), rep_done(false),
      all_applied(false), all_committed(false), sent_ack(false),
      sent_disk(false),
      on_applied(NULL) { }

    RepGather *get() {
      nref++;
      return this;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	delete ctx; // must already be unlocked
	assert(on_applied == NULL);
	delete this;
	//generic_dout(0) << "deleting " << this << dendl;
      }
    }
  };



  /*** PG ****/
protected:
  OSDService *osd;
  CephContext *cct;
  OSDriver osdriver;

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

  /**
   * Grabs locks for OpContext, should be cleaned up in close_op_ctx
   *
   * @param ctx [in,out] ctx to get locks for
   * @return true on success, false if we are queued
   */
  bool get_rw_locks(OpContext *ctx) {
    if (ctx->op->may_write() || ctx->op->may_cache()) {
      if (ctx->obc->get_write(ctx->op)) {
	ctx->lock_to_release = OpContext::W_LOCK;
	return true;
      }
    } else {
      assert(ctx->op->may_read());
      if (ctx->obc->get_read(ctx->op)) {
	ctx->lock_to_release = OpContext::R_LOCK;
	return true;
      }
    }
    return false;
  }

  /**
   * Cleans up OpContext
   *
   * @param ctx [in] ctx to clean up
   */
  void close_op_ctx(OpContext *ctx, int r) {
    release_op_ctx_locks(ctx);
    delete ctx->op_t;
    ctx->op_t = NULL;
    ctx->finish(r);
    delete ctx;
  }

  /**
   * Releases ctx locks
   *
   * @param ctx [in] ctx to clean up
   */
  void release_op_ctx_locks(OpContext *ctx) {
    list<OpRequestRef> to_req;
    bool requeue_recovery = false;
    switch (ctx->lock_to_release) {
    case OpContext::W_LOCK:
      ctx->obc->put_write(
	&to_req,
	&requeue_recovery);
      break;
    case OpContext::R_LOCK:
      ctx->obc->put_read(&to_req);
      break;
    case OpContext::NONE:
      break;
    default:
      assert(0);
    };
    ctx->lock_to_release = OpContext::NONE;
    requeue_ops(to_req);
  }

  // replica ops
  // [primary|tail]
  xlist<RepGather*> repop_queue;
  map<ceph_tid_t, RepGather*> repop_map;

  friend class C_OSD_RepopApplied;
  friend class C_OSD_RepopCommit;
  void repop_all_applied(RepGather *repop);
  void repop_all_committed(RepGather *repop);
  void eval_repop(RepGather*);
  void issue_repop(RepGather *repop, utime_t now);
  RepGather *new_repop(OpContext *ctx, ObjectContextRef obc, ceph_tid_t rep_tid);
  void remove_repop(RepGather *repop);

  RepGather *simple_repop_create(ObjectContextRef obc);
  void simple_repop_submit(RepGather *repop);

  // projected object info
  SharedPtrRegistry<hobject_t, ObjectContext> object_contexts;

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

  void get(const string &tag);
  void put(const string &tag);

  bool dirty_info;

  // pg state
  pg_info_t info;
  uint8_t info_struct_v;
  static const uint8_t cur_struct_v = 7;
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

  void handle_watch_timeout(WatchRef watch);

protected:
  unsigned    state;   // PG_STATE_*
  ObjectContextRef create_object_context(const object_info_t& oi);
  ObjectContextRef get_object_context(
    const hobject_t& soid,
    bool can_create,
    map<string, bufferlist> *attrs = 0
    );

  void context_registry_on_change();
  void object_context_destructor_callback(ObjectContext *obc);
  struct C_PG_ObjectContext : public Context {
    PGRef pg;
    ObjectContext *obc;
    C_PG_ObjectContext(PG *p, ObjectContext *o) :
      pg(p), obc(o) {}
    void finish(int r) {
      pg->object_context_destructor_callback(obc);
    }
  };

  int find_object_context(const hobject_t& oid,
			  ObjectContextRef *pobc,
			  bool can_create,
			  hobject_t *missing_oid=NULL);

  void add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *stat);

  void get_src_oloc(const object_t& oid, const object_locator_t& oloc,
		    object_locator_t& src_oloc);

  // low level ops

  void execute_ctx(OpContext *ctx);
  void finish_ctx(OpContext *ctx);
  void reply_ctx(OpContext *ctx, int err);
  void reply_ctx(OpContext *ctx, int err, eversion_t v, version_t uv);
  void make_writeable(OpContext *ctx);

  void write_update_size_and_usage(object_stat_sum_t& stats, object_info_t& oi,
				   interval_set<uint64_t>& modified,
				   uint64_t offset, uint64_t length,
				   bool count_bytes);
  void add_interval_usage(interval_set<uint64_t>& s,
			  object_stat_sum_t& st);

  int prepare_transaction(OpContext *ctx);
  list<pair<OpRequestRef, OpContext*> > in_progress_async_reads;
  void complete_read_ctx(int result, OpContext *ctx);

  struct C_OSD_OndiskWriteUnlock : public Context {
    ObjectContextRef obc, obc2, obc3;
    C_OSD_OndiskWriteUnlock(
      ObjectContextRef o,
      ObjectContextRef o2 = ObjectContextRef(),
      ObjectContextRef o3 = ObjectContextRef()) : obc(o), obc2(o2), obc3(o3) {}
    void finish(int r) {
      obc->ondisk_write_unlock();
      if (obc2)
	obc2->ondisk_write_unlock();
      if (obc3)
	obc3->ondisk_write_unlock();
    }
  };
  struct C_OSD_OndiskWriteUnlockList : public Context {
    list<ObjectContextRef> *pls;
    C_OSD_OndiskWriteUnlockList(list<ObjectContextRef> *l) : pls(l) {}
    void finish(int r) {
      for (list<ObjectContextRef>::iterator p = pls->begin(); p != pls->end(); ++p)
	(*p)->ondisk_write_unlock();
    }
  };


public:
  // last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_update_ondisk;
  eversion_t  last_update_applied;

  void apply_repops(bool requeue);

  int do_xattr_cmp_uint64_t(int op, uint64_t v1, bufferlist& xattr);
  int do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr);

  bool pgls_filter(PGLSFilter *filter, hobject_t& sobj, bufferlist& outdata);
  int get_pgls_filter(bufferlist::iterator& iter, PGLSFilter **pfilter);

  void do_pg_op(OpRequestRef op);
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops);

  int _get_tmap(OpContext *ctx, bufferlist *header, bufferlist *vals);
  int do_tmap2omap(OpContext *ctx, unsigned flags);
  int do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op);
  int do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op, bufferlist& bl);

  void do_osd_op_effects(OpContext *ctx);

protected:

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

private:
  int _delete_oid(OpContext *ctx, bool no_whiteout);
  void _on_change(ObjectStore::Transaction *t);

public:
  void clear_primary_state();
  void remove_object(
    ObjectStore::Transaction& t, const hobject_t& soid);

  void trim_write_ahead();

  void activate(ObjectStore::Transaction& t, epoch_t query_epoch);
  void _activate_committed(epoch_t e);
  void all_activated_and_committed();

  Context *finish_sync_event;

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;

  PG(OSDService *o, OSDMapRef curmap, const PGPool &_pool, pg_t p,
     const hobject_t& oid, const hobject_t& ioid);


  ~PG();
  // attr cache handling
  void replace_cached_attrs(
    OpContext *ctx,
    ObjectContextRef obc,
    const map<string, bufferlist> &new_attrs);
  void setattr_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    Transaction *t,
    const string &key,
    bufferlist &val);
  void rmattr_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    Transaction *t,
    const string &key);
  int getattr_maybe_cache(
    ObjectContextRef obc,
    const string &key,
    bufferlist *val);
  int getattrs_maybe_cache(
    ObjectContextRef obc,
    map<string, bufferlist> *out,
    bool user_only = false);
  void log_op_stats(OpContext *ctx);

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

  int get_state() const { return state; }
  bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  void init(pg_history_t& history, ObjectStore::Transaction *t);

private:
  void write_info(ObjectStore::Transaction& t);
  void populate_obc_watchers(ObjectContextRef obc);
  void get_obc_watchers(ObjectContextRef obc,
			list<obj_watch_item_t> &pg_watchers);
  void check_blacklisted_obc_watchers(ObjectContextRef obc);

public:
  static int _write_info(ObjectStore::Transaction& t, epoch_t epoch,
			 pg_info_t &info, coll_t coll, hobject_t &infos_oid,
			 uint8_t info_struct_v, bool force_ver = false);
  void write_if_dirty(ObjectStore::Transaction& t);

  eversion_t get_next_version() const {
    // XXX Be careful here, we're not sure if this is thread safe.  It
    // probably isn't and is just protected by too many locks.  Come
    // back and find an atomic way to do this later that works with a
    // compound version.
    eversion_t at_version(get_osdmap()->get_epoch(),
			  info.last_update.version + 1);
    assert(at_version > info.last_update);
    return at_version;
  }

  static int read_info(
    ObjectStore *store, const coll_t coll,
    bufferlist &bl, pg_info_t &info,
    hobject_t &infos_oid, uint8_t &);
  void read_state(ObjectStore *store, bufferlist &bl);
  static epoch_t peek_map_epoch(ObjectStore *store, coll_t coll,
				hobject_t &infos_oid, bufferlist *bl);
  void get_colls(list<coll_t> *out) {
    out->push_back(coll);
  }

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

  void on_removal(ObjectStore::Transaction *t);


  void do_request(OpRequestRef op, ThreadPool::TPHandle &handle);

  void do_op(OpRequestRef op);
  int do_command(cmdmap_t cmdmap, ostream& ss, bufferlist& idata,
		 bufferlist& odata);

  void on_change(ObjectStore::Transaction *t);
  void on_activate();
  void on_shutdown();
  void check_blacklisted_watchers();
  void get_watchers(std::list<obj_watch_item_t>&);

  // From the Backend
protected:
  const coll_t coll;

private:
  struct RepModify {
    OpRequestRef op;
    bool applied, committed;
    int ackerosd;
    eversion_t last_complete;
    epoch_t epoch_started;

    uint64_t bytes_written;

    ObjectStore::Transaction opt, localt;

    RepModify() : applied(false), committed(false), ackerosd(-1),
		  epoch_started(0), bytes_written(0) {}
  };

  /// execute implementation specific transaction
  void submit_transaction(const hobject_t &hoid,
			  const eversion_t &at_version,
			  Transaction *t,
			  Context *on_local_applied_sync,
			  Context *on_all_applied,
			  Context *on_all_commit,
			  ceph_tid_t tid,
			  osd_reqid_t reqid,
			  OpRequestRef op);
  /// Trim object stashed at stashed_version
  void trim_stashed_object(
    const hobject_t &hoid,
    version_t stashed_version,
    ObjectStore::Transaction *t);

  /// List objects in collection
  int objects_list_partial(
    const hobject_t &begin,
    int min,
    int max,
    vector<hobject_t> *ls,
    hobject_t *next);

  int objects_list_range(
    const hobject_t &start,
    const hobject_t &end,
    vector<hobject_t> *ls);

  int objects_get_attr(
    const hobject_t &hoid,
    const string &attr,
    bufferlist *out);

  int objects_get_attrs(const hobject_t &hoid, map<string, bufferlist> *out);

  int objects_read_sync(const hobject_t &hoid, uint64_t off,
			uint64_t len, bufferlist *bl);

  void objects_read_async(const hobject_t &hoid,
			  const list<pair<pair<uint64_t, uint64_t>,
			  pair<bufferlist*, Context*> > > &to_read,
			  Context *on_complete);

  Transaction* get_transaction() {
    return new Transaction(coll);
  }
};

ostream& operator<<(ostream& out, const PG& pg);
inline ostream& operator<<(ostream& out, PG::RepGather& repop)
{
  out << "repgather(" << &repop
      << " " << repop.v
      << " rep_tid=" << repop.rep_tid
      << " committed?=" << repop.all_committed
      << " applied?=" << repop.all_applied;
  if (repop.ctx->lock_to_release != PG::OpContext::NONE)
    out << " lock=" << (int)repop.ctx->lock_to_release;
  if (repop.ctx->op)
    out << " op=" << *(repop.ctx->op->get_req());
  out << ")";
  return out;
}

void intrusive_ptr_add_ref(PG::RepGather *repop);
void intrusive_ptr_release(PG::RepGather *repop);

#endif
