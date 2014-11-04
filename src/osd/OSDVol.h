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

#ifndef CEPH_OSDVOL_H
#define CEPH_OSDVOL_H

#include <atomic>
#include <cassert>
#include <boost/scoped_ptr.hpp>
#include <boost/optional.hpp>

// re-include our assert to clobber boost's
#include "include/types.h"
#include "include/stringify.h"
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"

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
#include <unordered_map>
#include <unordered_set>
#include "include/lru.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "common/sharedptr_registry.hpp"


class OSD;
class OSDService;
class MOSDOp;

class OSDVol;

void intrusive_ptr_add_ref(OSDVol *vol);
void intrusive_ptr_release(OSDVol *vol);

typedef boost::intrusive_ptr<OSDVol> OSDVolRef;


/** OSDVol - Volume abstraction in the OSD
 */

class OSDVol : public LRUObject {
  friend class OSD;
  friend class Watch;

  static const int cur_struct_v = 0;

public:
  std::string gen_prefix() const;

  struct OpContext;
  epoch_t get_epoch() const {
    return get_osdmap()->get_epoch();
  }

  std::string gen_dbg_prefix() const { return gen_prefix(); }

  const vol_info_t &get_info() const {
    return info;
  }
  ObjectContextRef get_obc(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs) {
    return get_object_context(hoid, true, &attrs);
  }

  entity_name_t get_cluster_msgr_name();

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
    eversion_t at_version;       // vol's current version pointer
    version_t user_at_version;   // vol's current user version pointer

    int current_osd_subop_num;

    ObjectStore::Transaction *op_t;

    interval_set<uint64_t> modified_ranges;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef> src_obc;

    int data_off; // FIXME: may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    OSDVol *vol;

    int num_read;    ///< count read ops
    int num_write;   ///< count update ops

    // pending async reads <off, len> -> <outbl, outr>
    list<pair<pair<uint64_t, uint64_t>,
	      pair<bufferlist*, Context*> > > pending_async_reads;
    int async_read_result;
    unsigned inflightreads;
    friend struct OnReadComplete;
    void start_async_reads(OSDVol *vol);
    void finish_read(OSDVol *vol);
    bool async_reads_complete() {
      return inflightreads == 0;
    }

    ObjectModDesc mod_desc;

    enum { W_LOCK, R_LOCK, NONE } lock_to_release;

    Context *on_finish;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, OSDVol *_vol) :
      op(_op), reqid(_reqid), ops(_ops), obs(_obs),
      new_obs(_obs->oi, _obs->exists),
      modify(false), user_modify(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      op_t(NULL),
      data_off(0), reply(NULL), vol(_vol),
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
   * State associated with a mutation
   */
  class Mutation {
  public:
    xlist<Mutation*>::item queue_item;
    std::atomic<uint64_t> nref;

    eversion_t v;

    OpContext *ctx;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef> src_obc;

    ceph_tid_t tid;

    bool aborted, done;

    bool applied;
    bool committed;

    bool sent_disk;

    Context *on_applied;

    Mutation(OpContext *c, ObjectContextRef pi, ceph_tid_t tid) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      tid(tid),
      aborted(false), done(false),
      applied(false), committed(false), sent_disk(false),
      on_applied(NULL) { }

    Mutation *get() {
      ++nref;
      return this;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	delete ctx; // must already be unlocked
	assert(on_applied == NULL);
	delete this;
      }
    }
  };

protected:
  OSDService *osd;
  CephContext *cct;
  OSDriver osdriver;

  // Ops waiting for map, should be queued at back
  Mutex map_lock;
  list<OpRequestRef> waiting_for_map;
  OSDMapRef osdmap_ref;
  OSDMapRef last_persisted_osdmap_ref;

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
  std::atomic<uint64_t> ref;

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
  Mutex mutation_lock;
  xlist<Mutation*> mutation_queue;

  friend class C_OSD_MutationApplied;
  friend class C_OSD_MutationCommit;
  void mutations_all_applied(Mutation *mutation);
  void mutations_all_committed(Mutation *mutation);
  void eval_mutation(Mutation *mutation);
  void issue_mutation(Mutation *mutation, utime_t now);
  Mutation *new_mutation(OpContext *ctx, ObjectContextRef obc, ceph_tid_t rep_tid);
  void remove_mutation(Mutation *mutation);

  Mutation *simple_mutation_create(ObjectContextRef obc);
  void simple_mutation_submit(Mutation *mutation);

  // projected object info
  SharedPtrRegistry<hobject_t, ObjectContext> object_contexts;

public:
  bool deleting;  // true while in removing or OSD is shutting down


  int whoami();
  void lock_suspend_timeout(ThreadPool::TPHandle &handle);
  void lock();
  void unlock() {
    assert(!dirty_info);
    _lock.Unlock();
  }

  void assert_locked() {
    assert(_lock.is_locked());
  }
  bool is_locked() const {
    return _lock.is_locked();
  }

  void get();
  void put();

  bool dirty_info;

  // vol state
  boost::uuids::uuid id;
  vol_info_t info;
  static string get_info_key(boost::uuids::uuid& vol) {
    return stringify(vol) + "_info";
  }
  static string get_epoch_key(boost::uuids::uuid& vol) {
    return stringify(vol) + "_epoch";
  }

  void handle_watch_timeout(WatchRef watch);

protected:
  ObjectContextRef create_object_context(const object_info_t& oi);
  ObjectContextRef get_object_context(
    const hobject_t& soid,
    bool can_create,
    map<string, bufferlist> *attrs = 0
    );

  void context_registry_on_change();
  void object_context_destructor_callback(ObjectContext *obc);
  struct C_Vol_ObjectContext : public Context {
    OSDVolRef osdvol;
    ObjectContext *obc;
    C_Vol_ObjectContext(OSDVol *v, ObjectContext *o) :
      osdvol(v), obc(o) {}
    void finish(int r) {
      osdvol->object_context_destructor_callback(obc);
    }
  };

  int find_object_context(const hobject_t& oid,
			  ObjectContextRef *pobc,
			  bool can_create);

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

  void apply_mutations(bool requeue);

  int do_xattr_cmp_uint64_t(int op, uint64_t v1, bufferlist& xattr);
  int do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr);

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

  // for ordering writes
  std::shared_ptr<ObjectStore::Sequencer> osr;

private:
  int _delete_oid(OpContext *ctx, bool no_whiteout);
  static int _write_info(ObjectStore::Transaction& t, epoch_t epoch,
			 vol_info_t &info, coll_t coll,
			 hobject_t &infos_oid);

public:
  void clear_primary_state();
  void remove_object(
    ObjectStore::Transaction& t, const hobject_t& soid);

  void trim_write_ahead();

  void activate(ObjectStore::Transaction& t, epoch_t query_epoch);
  void _activate_committed(epoch_t e);

  Context *finish_sync_event;

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;

  OSDVol(OSDService *o, OSDMapRef curmap,
	 const boost::uuids::uuid& vol);

  ~OSDVol();

private:
  // Prevent copying
  OSDVol(const OSDVol& rhs);
  OSDVol& operator=(const OSDVol& rhs);

public:
  const boost::uuids::uuid& get_volid() const { return info.volume; }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }


private:
  void init();
  void read_info();
  void write_info(ObjectStore::Transaction& t);
  void populate_obc_watchers(ObjectContextRef obc);
  void get_obc_watchers(ObjectContextRef obc,
			list<obj_watch_item_t> &vol_watchers);
  void check_blacklisted_obc_watchers(ObjectContextRef obc);

public:
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

  coll_t get_coll(void) {
    return coll;
  }

  // OpRequest queueing
  bool can_discard_op(OpRequestRef op);
  bool can_discard_request(OpRequestRef op);

  bool can_discard_replica_op(OpRequestRef op);

  static bool op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op);

  static bool have_same_or_newer_map(OSDMapRef osdmap, epoch_t e) {
    return e <= osdmap->get_epoch();
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap()->get_epoch();
  }

  void take_waiters();
  void handle_advance_map(OSDMapRef osdmap);
  void handle_activate_map();

  void on_removal(ObjectStore::Transaction *t);


  void do_request(OpRequestRef op, ThreadPool::TPHandle &handle);

  void do_op(OpRequestRef op);
  int do_command(cmdmap_t cmdmap, ostream& ss, bufferlist& idata,
		 bufferlist& odata);

  void on_change(ObjectStore::Transaction *t);
  void check_blacklisted_watchers();
  void get_watchers(std::list<obj_watch_item_t>&);

  // From the Backend
protected:
  const coll_t coll;
  utime_t last_became_active;

  void on_shutdown();

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

  void objects_read_async(const hobject_t &hoid,
			  const list<pair<pair<uint64_t, uint64_t>,
			  pair<bufferlist*, Context*> > > &to_read,
			  Context *on_complete);
};

ostream& operator<<(ostream& out, const OSDVol& vol);
void intrusive_ptr_add_ref(OSDVol::Mutation *mutation);
void intrusive_ptr_release(OSDVol::Mutation *mutation);

#endif
