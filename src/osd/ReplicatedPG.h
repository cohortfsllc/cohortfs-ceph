// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_REPLICATEDPG_H
#define CEPH_REPLICATEDPG_H

#include <boost/optional.hpp>
#include <boost/tuple/tuple.hpp>

#include "include/assert.h"
#include "common/cmdparse.h"

#include "OSD.h"
#include "PG.h"
#include "Watch.h"
#include "OpRequest.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "common/sharedptr_registry.hpp"

#include "PGBackend.h"
#include "ReplicatedBackend.h"

class MOSDSubOpReply;

class ReplicatedPG;
void intrusive_ptr_add_ref(ReplicatedPG *pg);
void intrusive_ptr_release(ReplicatedPG *pg);
uint64_t get_with_id(ReplicatedPG *pg);
void put_with_id(ReplicatedPG *pg, uint64_t id);

#ifdef PG_DEBUG_REFS
  typedef TrackedIntPtr<ReplicatedPG> ReplicatedPGRef;
#else
  typedef boost::intrusive_ptr<ReplicatedPG> ReplicatedPGRef;
#endif

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

class ReplicatedPG : public PG, public PGBackend::Listener {
  friend class OSD;
  friend class Watch;

public:

  struct OpContext;

  boost::scoped_ptr<PGBackend> pgbackend;
  PGBackend *get_pgbackend() {
    return pgbackend.get();
  }

  /// Listener methods

  template <typename T>
  class BlessedGenContext : public GenContext<T> {
    ReplicatedPGRef pg;
    GenContext<T> *c;
  public:
    BlessedGenContext(ReplicatedPG *pg, GenContext<T> *c, epoch_t e)
      : pg(pg), c(c) {}
    void finish(T t) {
      pg->lock();
      if (pg->deleting)
	delete c;
      else
	c->complete(t);
      pg->unlock();
    }
  };
  class BlessedContext : public Context {
    ReplicatedPGRef pg;
    Context *c;
  public:
    BlessedContext(ReplicatedPG *pg, Context *c)
      : pg(pg), c(c) {}
    void finish(int r) {
      pg->lock();
      if (pg->deleting)
	delete c;
      else
	c->complete(r);
      pg->unlock();
    }
  };
  Context *bless_context(Context *c) {
    return new BlessedContext(this, c);
  }
  GenContext<ThreadPool::TPHandle&> *bless_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) {
    return new BlessedGenContext<ThreadPool::TPHandle&>(
      this, c, get_osdmap()->get_epoch());
  }

  void send_message(int to_osd, Message *m) {
    osd->send_message_osd_cluster(to_osd, m, get_osdmap()->get_epoch());
  }
  void queue_transaction(ObjectStore::Transaction *t, OpRequestRef op) {
    list<ObjectStore::Transaction *> tls;
    tls.push_back(t);
    osd->store->queue_transaction(osr.get(), t, 0, 0, 0, op);
  }
  epoch_t get_epoch() const {
    return get_osdmap()->get_epoch();
  }

  std::string gen_dbg_prefix() const { return gen_prefix(); }

  const PGLog &get_log() const {
    return pg_log;
  }
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
  void log_operation(
    vector<pg_log_entry_t> &logv,
    bool transaction_applied,
    ObjectStore::Transaction *t) {
    append_log(logv, *t, transaction_applied);
  }

  void op_applied(
    const eversion_t &applied_version);

  void update_last_complete_ondisk(
    eversion_t lcod) {
    last_complete_ondisk = lcod;
  }

  void update_stats(
    const pg_stat_t &stat) {
    info.stats = stat;
  }

  void schedule_work(
    GenContext<ThreadPool::TPHandle&> *c);

  entity_name_t get_cluster_msgr_name() {
    return osd->get_cluster_msgr_name();
  }

  PerfCounters *get_logger();

  ceph_tid_t get_tid() { return osd->get_tid(); }

  LogClientTemp clog_error() { return osd->clog.error(); }

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

    PGBackend::PGTransaction *op_t;
    vector<pg_log_entry_t> log;

    interval_set<uint64_t> modified_ranges;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef> src_obc;

    int data_off; // FIXME: may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    ReplicatedPG *pg;

    int num_read;    ///< count read ops
    int num_write;   ///< count update ops

    hobject_t new_temp_oid,
      discard_temp_oid;  ///< temp objects we should start/stop tracking

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
    void start_async_reads(ReplicatedPG *pg);
    void finish_read(ReplicatedPG *pg);
    bool async_reads_complete() {
      return inflightreads == 0;
    }

    ObjectModDesc mod_desc;

    enum { W_LOCK, R_LOCK, NONE } lock_to_release;

    Context *on_finish;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, ReplicatedPG *_pg) :
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
    //bool sent_nvram;
    bool sent_disk;

    utime_t   start;

    eversion_t pg_local_last_complete;

    Context *on_applied;

    RepGather(OpContext *c, ObjectContextRef pi, ceph_tid_t rt,
	      eversion_t lc) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      rep_tid(rt),
      rep_aborted(false), rep_done(false),
      all_applied(false), all_committed(false), sent_ack(false),
      //sent_nvram(false),
      sent_disk(false),
      pg_local_last_complete(lc),
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


protected:

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

  // debug order that client ops are applied
  map<hobject_t, map<client_t, ceph_tid_t> > debug_op_order;

  void populate_obc_watchers(ObjectContextRef obc);
  void check_blacklisted_obc_watchers(ObjectContextRef obc);
  void check_blacklisted_watchers();
  void get_watchers(list<obj_watch_item_t> &pg_watchers);
  void get_obc_watchers(ObjectContextRef obc,
			list<obj_watch_item_t> &pg_watchers);
public:
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
  struct C_PG_ObjectContext : public Context {
    ReplicatedPGRef pg;
    ObjectContext *obc;
    C_PG_ObjectContext(ReplicatedPG *p, ObjectContext *o) :
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
  void finish_ctx(OpContext *ctx, int log_op_type);
  void reply_ctx(OpContext *ctx, int err);
  void reply_ctx(OpContext *ctx, int err, eversion_t v, version_t uv);
  void make_writeable(OpContext *ctx);
  void log_op_stats(OpContext *ctx);

  void write_update_size_and_usage(object_stat_sum_t& stats, object_info_t& oi,
				   interval_set<uint64_t>& modified,
				   uint64_t offset, uint64_t length,
				   bool count_bytes);
  void add_interval_usage(interval_set<uint64_t>& s,
			  object_stat_sum_t& st);

  int prepare_transaction(OpContext *ctx);
  list<pair<OpRequestRef, OpContext*> > in_progress_async_reads;
  void complete_read_ctx(int result, OpContext *ctx);

  // pg on-disk content
  void check_local();

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
  struct C_OSD_AppliedRecoveredObject : public Context {
    ReplicatedPGRef pg;
    ObjectContextRef obc;
    C_OSD_AppliedRecoveredObject(ReplicatedPG *p, ObjectContextRef o) :
      pg(p), obc(o) {}
    void finish(int r) {
      pg->_applied_recovered_object(obc);
    }
  };
  struct C_OSD_CommittedPushedObject : public Context {
    ReplicatedPGRef pg;
    epoch_t epoch;
    eversion_t last_complete;
    C_OSD_CommittedPushedObject(
      ReplicatedPG *p, epoch_t epoch, eversion_t lc) :
      pg(p), epoch(epoch), last_complete(lc) {
    }
    void finish(int r) {
      pg->_committed_pushed_object(epoch, last_complete);
    }
  };
  struct C_OSD_AppliedRecoveredObjectReplica : public Context {
    ReplicatedPGRef pg;
    C_OSD_AppliedRecoveredObjectReplica(ReplicatedPG *p) :
      pg(p) {}
    void finish(int r) {
      pg->_applied_recovered_object_replica();
    }
  };

  void _applied_recovered_object(ObjectContextRef obc);
  void _applied_recovered_object_replica();
  void _committed_pushed_object(epoch_t epoch, eversion_t lc);
  void recover_got(hobject_t oid, eversion_t v);

  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  void apply_repops(bool requeue);

  int do_xattr_cmp_uint64_t(int op, uint64_t v1, bufferlist& xattr);
  int do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr);

  bool pgls_filter(PGLSFilter *filter, hobject_t& sobj, bufferlist& outdata);
  int get_pgls_filter(bufferlist::iterator& iter, PGLSFilter **pfilter);

public:
  ReplicatedPG(OSDService *o, OSDMapRef curmap,
	       const PGPool &_pool, pg_t p, const hobject_t& oid,
	       const hobject_t& ioid);
  ~ReplicatedPG() {}

  int do_command(cmdmap_t cmdmap, ostream& ss, bufferlist& idata,
		 bufferlist& odata);

  void do_request(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_op(OpRequestRef op);
  void do_pg_op(OpRequestRef op);
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops);

  int _get_tmap(OpContext *ctx, bufferlist *header, bufferlist *vals);
  int do_tmap2omap(OpContext *ctx, unsigned flags);
  int do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op);
  int do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op, bufferlist& bl);

  void do_osd_op_effects(OpContext *ctx);
private:
  uint64_t temp_seq; ///< last id for naming temp objects
  coll_t get_temp_coll(ObjectStore::Transaction *t);
  hobject_t generate_temp_object();  ///< generate a new temp object name
public:
  void get_colls(list<coll_t> *out) {
    out->push_back(coll);
    return pgbackend->temp_colls(out);
  }
private:
  struct NotTrimming;
  struct Reset : boost::statechart::event< Reset > {
    Reset() : boost::statechart::event< Reset >() {}
  };

  int _delete_oid(OpContext *ctx, bool no_whiteout);
public:

  void on_pool_change();
  void on_change(ObjectStore::Transaction *t);
  void on_activate();
  void on_removal(ObjectStore::Transaction *t);
  void on_shutdown();

  // attr cache handling
  void replace_cached_attrs(
    OpContext *ctx,
    ObjectContextRef obc,
    const map<string, bufferlist> &new_attrs);
  void setattr_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    PGBackend::PGTransaction *t,
    const string &key,
    bufferlist &val);
  void rmattr_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    PGBackend::PGTransaction *t,
    const string &key);
  int getattr_maybe_cache(
    ObjectContextRef obc,
    const string &key,
    bufferlist *val);
  int getattrs_maybe_cache(
    ObjectContextRef obc,
    map<string, bufferlist> *out,
    bool user_only = false);
};

inline ostream& operator<<(ostream& out, ReplicatedPG::RepGather& repop)
{
  out << "repgather(" << &repop
      << " " << repop.v
      << " rep_tid=" << repop.rep_tid 
      << " committed?=" << repop.all_committed
      << " applied?=" << repop.all_applied;
  if (repop.ctx->lock_to_release != ReplicatedPG::OpContext::NONE)
    out << " lock=" << (int)repop.ctx->lock_to_release;
  if (repop.ctx->op)
    out << " op=" << *(repop.ctx->op->get_req());
  out << ")";
  return out;
}

void intrusive_ptr_add_ref(ReplicatedPG::RepGather *repop);
void intrusive_ptr_release(ReplicatedPG::RepGather *repop);


#endif
