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

#ifndef CEPH_OBJECTER_H
#define CEPH_OBJECTER_H

#include <boost/uuid/nil_generator.hpp>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <shared_mutex>
#include <boost/intrusive/slist.hpp>
#include <boost/intrusive/set.hpp>
#include "include/types.h"
#include "include/buffer.h"

#include "osd/OSDMap.h"
#include "msg/MessageFactory.h"
#include "messages/MOSDOp.h"

#include "common/Timer.h"
#include "common/zipkin_trace.h"
#include "include/rados/rados_types.h"
#include "include/rados/rados_types.hpp"

#include "common/shunique_lock.h"

#include "ObjectOperation.h"

class Messenger;
class OSDMap;
class MonClient;
class Message;
class MStatfsReply;
class MOSDOpReply;
class MOSDMap;

namespace OSDC {
  using boost::intrusive::slist;
  using boost::intrusive::set;
  using boost::intrusive::slist_base_hook;
  using boost::intrusive::set_base_hook;
  using boost::intrusive::link_mode;
  using boost::intrusive::auto_unlink;
  using boost::intrusive::constant_time_size;
  using boost::intrusive::linear;
  using std::vector;
  using std::string;
  using std::shared_ptr;
  using std::unique_ptr;
  using std::move;
  using std::function;
  using std::ref;
  using std::move;
  typedef function<void(int)> op_callback;
  typedef function<void(int, uint64_t, ceph::real_time)> stat_callback;
  typedef function<void(int, bufferlist&&)> read_callback;

  class CB_Waiter {
    std::mutex lock;
    std::condition_variable cond;
    bool done;
    int r;

  public:

    CB_Waiter() : done(false) { }

    int wait() {
      std::unique_lock<std::mutex> l(lock);
      cond.wait(l, [this](){ return done; });
      return r;
    }

    void operator()(int _r) {
      std::unique_lock<std::mutex> l(lock);
      done = true;
      r = _r;
      cond.notify_one();
    }

    void reset() {
      done = false;
    }
  };

  class MessageFactory : public ::MessageFactory {
   private:
    ::MessageFactory *parent;
   public:
    MessageFactory(::MessageFactory *parent) : parent(parent) {}
    Message* create(int type);
  };

  class Objecter: public Dispatcher {
  public:
    Messenger *messenger;
    MonClient *monc;
  private:
    OSDMap *osdmap;
    vector<function<void()> > osdmap_notifiers;
  public:
    CephContext *cct;

  private:
    std::atomic<uint64_t> last_tid;
    std::atomic<uint32_t> client_inc;
    uint64_t max_linger_id;
    std::atomic<uint32_t> num_unacked;
    std::atomic<uint32_t> num_uncommitted;
    // flags which are applied to each IO op
    std::atomic<uint32_t> global_op_flags;
    bool keep_balanced_budget;
    bool honor_osdmap_full;
    ZTracer::Endpoint trace_endpoint;

  public:
    void maybe_request_map();
  private:

    void _maybe_request_map();

    version_t last_seen_osdmap_version;

    std::shared_timed_mutex rwlock;
    typedef std::shared_lock<std::shared_timed_mutex> shared_lock;
    typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
    typedef ceph::shunique_lock<std::shared_timed_mutex> shunique_lock;

    cohort::Timer<ceph::mono_clock> timer;
    uint64_t tick_event;
    void schedule_tick();
    void tick();

  public:

    struct OSDSession;
    struct Op;

    struct SubOp : public set_base_hook<link_mode<auto_unlink> > {
      ceph_tid_t tid;
      int incarnation;
      OSDSession *session;
      int osd;
      oid_t hoid;
      vector<OSDOp> ops;

      eversion_t replay_version; // for op replay
      ceph::mono_time stamp;
      int attempts;
      bool done;
      Op& parent;

      // Never call this. Stupid STL.

      SubOp(Op& p) : tid(0), incarnation(0), session(nullptr), osd(-1),
		     attempts(0), done(false), parent(p) { }
      SubOp(const oid_t& h, const vector<OSDOp>& o, Op& p)
	: tid(0), incarnation(0), session(nullptr), osd(-1), hoid(h), ops(o),
	  attempts(0), done(false), parent(p) { }
      SubOp(oid_t&& h, vector<OSDOp>&& o, Op& p)
	: tid(0), incarnation(0), session(nullptr), osd(-1), hoid(h), ops(o),
	  attempts(0), done(false), parent(p) { }
    };

    struct op_base : public set_base_hook<link_mode<auto_unlink> >,
		     public RefCountedObject {
      int flags;
      oid_t oid;
      VolumeRef volume;
      unique_ptr<ObjOp> op;
      vector<SubOp> subops;
      ceph::real_time mtime;
      bufferlist *outbl;
      version_t *objver;
      ceph_tid_t tid;
      epoch_t map_dne_bound;
      bool paused;
      uint16_t priority;
      bool should_resend;

      std::mutex lock;
      typedef std::unique_lock<std::mutex> unique_lock;
      typedef std::lock_guard<std::mutex> lock_guard;

      op_base() : flags(0), volume(nullptr), op(nullptr), outbl(nullptr),
		  tid(0), map_dne_bound(0), paused(false), priority(0),
		  should_resend(true) { }

      op_base(oid_t oid, const VolumeRef& volume,
	      unique_ptr<ObjOp>& _op, int flags, version_t* ov)
	: flags(flags), oid(oid), volume(volume),
	  op(move(_op)), outbl(nullptr), objver(ov),
	  tid(0), map_dne_bound(0), paused(false), priority(0),
	  should_resend(true) {
	if (objver)
	  *objver = 0;
      }
    };

    struct Op : public op_base {
      op_callback onack, oncommit;
      uint64_t ontimeout;
      uint32_t acks, commits;
      int rc;
      epoch_t *reply_epoch;
      bool budgeted;
      // true if the throttle budget is get/put on a series of OPs,
      // instead of per OP basis, when this flag is set, the budget is
      // acquired before sending the very first OP of the series and
      // released upon receiving the last OP reply.
      bool ctx_budgeted;
      bool finished;
      ZTracer::Trace trace;

      Op(const oid_t& o, const VolumeRef& volume,
	 unique_ptr<ObjOp>& _op,
	 int f, op_callback&& ac, op_callback&& co, version_t *ov,
	 ZTracer::Trace *parent) :
	op_base(o, volume, _op, f, ov),
	onack(ac), oncommit(co), ontimeout(0),
	acks(0), commits(0), rc(0), reply_epoch(nullptr),
	budgeted(false), ctx_budgeted(false), finished(false) {
	subops.reserve(op->width());
	op->realize(
	  oid,
	  [this](oid_t&& h, vector<OSDOp>&& o) {
	    this->subops.emplace_back(h, o, *this);
	  });
	if (parent && parent->valid())
	  trace.init("op", nullptr, parent);
      }
      ~Op() { }

      bool operator<(const Op& other) const {
	return tid < other.tid;
      }
    };

    struct C_Op_Map_Latest : public Context {
      Objecter *objecter;
      ceph_tid_t tid;
      version_t latest;
      C_Op_Map_Latest(Objecter *o, ceph_tid_t t) : objecter(o), tid(t),
						   latest(0) {}
      void finish(int r);
    };

    struct C_Command_Map_Latest : public Context {
      Objecter *objecter;
      uint64_t tid;
      version_t latest;
      C_Command_Map_Latest(Objecter *o, ceph_tid_t t) :
	objecter(o), tid(t), latest(0) {}
      void finish(int r);
    };

    struct StatfsOp : public set_base_hook<link_mode<auto_unlink> > {
      ceph_tid_t tid;
      struct ceph_statfs *stats;
      Context *onfinish;
      uint64_t ontimeout;
      ceph::mono_time last_submit;
    };


    // -- lingering ops --

    struct LingerOp : public op_base {
      uint64_t linger_id;
      bufferlist inbl;
      bool registered;
      bool canceled;
      Context *on_reg_ack, *on_reg_commit;
      ceph_tid_t register_tid;

      LingerOp() : linger_id(0),
		   registered(false),
		   on_reg_ack(nullptr), on_reg_commit(nullptr) {}

      // no copy!
      const LingerOp &operator=(const LingerOp& r) = delete;
      LingerOp(const LingerOp& o) = delete;
    private:
      ~LingerOp() {}
    };

    struct C_Linger_Ack : public Context {
      Objecter *objecter;
      LingerOp& info;
      C_Linger_Ack(Objecter *o, LingerOp& l) : objecter(o), info(l) {
	info.get();
      }
      ~C_Linger_Ack() {
	info.put();
      }
      void finish(int r) {
	objecter->_linger_ack(info, r);
      }
    };

    struct C_Linger_Commit : public Context {
      Objecter* objecter;
      LingerOp& info;
      C_Linger_Commit(Objecter *o, LingerOp& l) : objecter(o), info(l) {
	info.get();
      }
      ~C_Linger_Commit() {
	info.put();
      }
      void finish(int r) {
	objecter->_linger_commit(info, r);
      }
    };

    struct C_Linger_Map_Latest : public Context {
      Objecter *objecter;
      uint64_t linger_id;
      version_t latest;
      C_Linger_Map_Latest(Objecter *o, uint64_t id) :
	objecter(o), linger_id(id), latest(0) {}
      void finish(int r);
    };

    // -- osd sessions --
    struct OSDSession : public set_base_hook<link_mode<auto_unlink> >,
			public RefCountedObject {
      std::shared_timed_mutex lock;
      typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
      typedef std::shared_lock<std::shared_timed_mutex> shared_lock;

      set<SubOp, constant_time_size<false> > subops;
      set<SubOp, constant_time_size<false> > linger_subops;
      int osd;
      int incarnation;
      int num_locks;
      ConnectionRef con;

      OSDSession(CephContext *cct, int o) :
	osd(o), incarnation(0), con(nullptr) { }
      ~OSDSession();

      bool is_homeless() { return (osd == -1); }
    };
    set<OSDSession, constant_time_size<false> > osd_sessions;

  private:
    set<Op, constant_time_size<false> > inflight_ops;
    set<LingerOp, constant_time_size<false> > linger_ops;
    set<StatfsOp, constant_time_size<false> > statfs_ops;

    OSDSession *homeless_session;

    // ops waiting for an osdmap with a new volume or confirmation
    // that the volume does not exist (may be expanded to other uses
    // later)
    map<uint64_t, LingerOp*> check_latest_map_lingers;
    map<ceph_tid_t, Op*> check_latest_map_ops;

    map<epoch_t,list< pair<Context*, int> > > waiting_for_map;

    ceph::timespan mon_timeout, osd_timeout;

    MOSDOp *_prepare_osd_subop(SubOp& op);
    void _send_subop(SubOp& op, MOSDOp *m = nullptr);
    void _cancel_linger_op(LingerOp& op);
    void _cancel_op(Op& op);
    void _finish_subop(SubOp& subop);
    void _finish_op(Op& op, Op::unique_lock& ol); // Releases ol

    enum target_result {
      TARGET_NO_ACTION = 0,
      TARGET_NEED_RESEND,
      TARGET_VOLUME_DNE
    };
    bool osdmap_full_flag() const;
    bool target_should_be_paused(op_base& op);

    int _calc_targets(op_base& t, Op::unique_lock& ol);
    int _map_session(SubOp& subop, OSDSession **s,
		     Op::unique_lock& ol, const shunique_lock& lc);

    void _session_subop_assign(OSDSession& to, SubOp& subop);
    void _session_subop_remove(OSDSession& from, SubOp& subop);
    void _session_linger_subop_assign(OSDSession& to, SubOp& subop);
    void _session_linger_subop_remove(OSDSession& from, SubOp& subop);

    int _get_osd_session(int osd, shunique_lock& sl,
			 OSDSession **session);
    int _assign_subop_target_session(SubOp& op, shared_lock& lc,
				     bool src_session_locked,
				     bool dst_session_locked);
    int _get_subop_target_session(SubOp& op, shunique_lock& sl,
				  OSDSession** session);
    int _recalc_linger_op_targets(LingerOp& op, shunique_lock& sl);

    void _linger_submit(LingerOp& info);
    void _send_linger(LingerOp& info);
    void _linger_ack(LingerOp& info, int r);
    void _linger_commit(LingerOp& info, int r);

    void _check_op_volume_dne(Op& op, Op::unique_lock& ol);
    void _send_op_map_check(Op& op);
    void _op_cancel_map_check(Op& op);
    void _check_linger_volume_dne(LingerOp *op, bool *need_unregister);
    void _send_linger_map_check(LingerOp *op);
    void _linger_cancel_map_check(LingerOp *op);

    void kick_requests(OSDSession& session);
    void _kick_requests(OSDSession& session,
			map<uint64_t, LingerOp *>& lresend);
    void _linger_ops_resend(map<uint64_t, LingerOp *>& lresend);

    int _get_session(int osd, OSDSession **session,
		     const shunique_lock& shl);
    void put_session(OSDSession& s);
    void get_session(OSDSession& s);
    void _reopen_session(OSDSession& session);
    void close_session(OSDSession& session);

    void resend_mon_ops();

    /**
     * handle a budget for in-flight ops
     * budget is taken whenever an op goes into the ops map
     * and returned whenever an op is removed from the map
     * If throttle_op needs to throttle it will unlock client_lock.
     */
    int calc_op_budget(Op& op);
    void _throttle_op(Op& op, shunique_lock& sl, int op_size = 0);
    int _take_op_budget(Op& op, shunique_lock& sl) {
      int op_budget = calc_op_budget(op);
      if (keep_balanced_budget) {
	_throttle_op(op, sl, op_budget);
      } else {
	op_throttle_bytes.take(op_budget);
	op_throttle_ops.take(1);
      }
      op.budgeted = true;
      return op_budget;
    }
    void put_op_budget_bytes(int op_budget) {
      assert(op_budget >= 0);
      op_throttle_bytes.put(op_budget);
      op_throttle_ops.put(1);
    }
    void put_op_budget(Op& op) {
      assert(op.budgeted);
      int op_budget = calc_op_budget(op);
      put_op_budget_bytes(op_budget);
    }
    Throttle op_throttle_bytes, op_throttle_ops;

  public:
    Objecter(CephContext *cct_, Messenger *m, MonClient *mc,
	     ceph::timespan mon_timeout = std::chrono::seconds(0),
	     ceph::timespan osd_timeout = std::chrono::seconds(0)) :
      Dispatcher(cct_), messenger(m), monc(mc),
      osdmap(new OSDMap), cct(cct_),
      last_tid(0),
      client_inc(-1), max_linger_id(0),
      num_unacked(0), num_uncommitted(0),
      global_op_flags(0),
      keep_balanced_budget(false), honor_osdmap_full(true),
      trace_endpoint("0.0.0.0", 0, "Objecter"),
      last_seen_osdmap_version(0),
      tick_event(0),
      homeless_session(new OSDSession(cct, -1)),
      mon_timeout(mon_timeout),
      osd_timeout(osd_timeout),
      op_throttle_bytes(cct, cct->_conf->objecter_inflight_op_bytes),
      op_throttle_ops(cct, cct->_conf->objecter_inflight_ops)
      { }
    ~Objecter();

    void start();
    void shutdown();

    // The old version, returning both a pointer and a lock context
    // was clumsy and awkward to use. I had thought of making a
    // lockable pointer, but that has problems of its own, like the
    // fact that one couldn't reasonably copy it.

    // With this version, you call with a function that does what you
    // want to do, and it's executed under the lock with access to the
    // OSD Map. Obviously you oughtn't have a lock already, but since
    // the lock is private, you can't really get one.
    void with_osdmap(
      std::function<void(const OSDMap&)> f) {
      shared_lock l(rwlock);
      f(*osdmap);
    }


    /**
     * Tell the objecter to throttle outgoing ops according to its
     * budget (in _conf). If you do this, ops can block, in
     * which case it will unlock client_lock and sleep until
     * incoming messages reduce the used budget low enough for
     * the ops to continue going; then it will lock client_lock again.
     */

    void set_balanced_budget() { keep_balanced_budget = true; }
    void unset_balanced_budget() { keep_balanced_budget = false; }

    void set_honor_osdmap_full() { honor_osdmap_full = true; }
    void unset_honor_osdmap_full() { honor_osdmap_full = false; }

    void _scan_requests(OSDSession& session,
			bool force_resend,
			bool force_resend_writes,
			map<ceph_tid_t, SubOp*>& need_resend,
			list<LingerOp*>& need_resend_linger,
			shunique_lock& shl);
    // messages
  public:
    bool ms_dispatch(Message *m);
    bool ms_can_fast_dispatch_any() const {
      return false;
    }
    bool ms_can_fast_dispatch(Message *m) const {
      switch (m->get_type()) {
      case CEPH_MSG_OSD_OPREPLY:
	/* sadly, we need to solve a deadlock before reenabling.
	 * See tracker issue #9462 */
	return false;
      default:
	return false;
      }
    }
    void ms_fast_dispatch(Message *m) {
      ms_dispatch(m);
    }

    void handle_osd_subop_reply(MOSDOpReply *m);
    bool possibly_complete_op(Op& op, Op::unique_lock& ol,
			      bool do_or_die = false);
    void handle_osd_map(MOSDMap *m);
    void wait_for_osd_map();

    // The function supplied is called with no lock. If it wants to do
    // something with the OSDMap, it can call with_osdmap on a
    // captured objecter.
    void add_osdmap_notifier(const function<void()>& f);

  private:
    bool _promote_lock_check_race(shunique_lock& sl);

    // low-level
    ceph_tid_t _op_submit(Op& op, Op::unique_lock& ol, shunique_lock& sl);
    ceph_tid_t _op_submit_with_budget(Op& op, Op::unique_lock& ol,
				      shunique_lock& sl,
				      int *ctx_budget = nullptr);
    // public interface
  public:
    ceph_tid_t op_submit(Op *op, int* ctx_budget = nullptr);
    bool is_active() {
      return !(inflight_ops.empty() && linger_ops.empty() &&
	       statfs_ops.empty());
    }

    int get_client_incarnation() const { return client_inc; }
    void set_client_incarnation(int inc) { client_inc = inc; }

    // wait for epoch; true if we already have it
    bool wait_for_map(epoch_t epoch, Context *c, int err=0);
    void _wait_for_new_map(Context *c, epoch_t epoch, int err=0);
    void wait_for_latest_osdmap(Context *fin);
    void get_latest_version(epoch_t oldest, epoch_t neweset, Context *fin);
    void _get_latest_version(epoch_t oldest, epoch_t neweset, Context *fin);

    /** Get the current set of global op flags */
    int get_global_op_flags() { return global_op_flags; }
    /** Add a flag to the global op flags, not really atomic operation */
    void add_global_op_flags(int flag) {
      global_op_flags |= flag;
    }
    /** Clear the passed flags from the global op flag set, not really
	atomic operation */
    void clear_global_op_flag(int flags) {
      global_op_flags &= ~flags;
    }

    /// cancel an in-progress request with the given return code
  private:
    void op_cancel(ceph_tid_t tid, int r);


  public:
    // mid-level helpers
    Op *prepare_mutate_op(const oid_t& oid,
			  const VolumeRef& volume,
			  unique_ptr<ObjOp>& op,
			  ceph::real_time mtime, int flags,
			  op_callback&& onack, op_callback&& oncommit,
			  version_t *objver = nullptr,
			  ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, op,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack),
		     move(oncommit), objver, trace);
      o->mtime = mtime;
      return o;
    }

    ceph_tid_t mutate(const oid_t& oid,
		      const VolumeRef& volume,
		      unique_ptr<ObjOp>& op,
		      ceph::real_time mtime, int flags,
		      op_callback&& onack, op_callback&& oncommit,
		      version_t *objver = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      Op *o = prepare_mutate_op(oid, volume, op, mtime, flags,
				move(onack),
				move(oncommit),
				objver, trace);
      return op_submit(o);
    }

    ceph_tid_t mutate(const oid_t& oid,
		      const shared_ptr<const Volume>& volume,
		      unique_ptr<ObjOp>& op,
		      ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      mutate(oid, volume, op, mtime, 0, nullptr, ref(w),
	     nullptr, trace);

      return w.wait();
    }

    Op *prepare_read_op(const oid_t& oid,
			const VolumeRef& volume,
			unique_ptr<ObjOp>& op, bufferlist *pbl,
			int flags, op_callback&& onack,
			version_t *objver = nullptr,
			ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, op,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onack),
		     nullptr, objver, trace);
      o->outbl = pbl;
      return o;
    }
    ceph_tid_t read(const oid_t& oid,
		    const VolumeRef& volume,
		    unique_ptr<ObjOp>& op, bufferlist *pbl,
		    int flags, op_callback&& onack,
		    version_t *objver = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      Op *o = prepare_read_op(oid, volume, op, pbl, flags,
			      move(onack),
			      objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read(const oid_t& oid,
		    const VolumeRef& volume,
		    unique_ptr<ObjOp>& op, bufferlist *pbl,
		    ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;

      read(oid, volume, op, pbl, 0, ref(w), nullptr, trace);

      return w.wait();
    }

    ceph_tid_t linger_mutate(const oid_t& oid,
			     const VolumeRef& volume,
			     unique_ptr<ObjOp>& op, ceph::real_time mtime,
			     bufferlist& inbl, int flags, Context *onack,
			     Context *onfinish, version_t *objver);
    ceph_tid_t linger_read(const oid_t& oid,
			   const VolumeRef& volume,
			   unique_ptr<ObjOp>& op, bufferlist& inbl,
			   bufferlist *outbl, int flags, Context *onack,
			   version_t *objver);
    void unregister_linger(uint64_t linger_id);
    void _unregister_linger(uint64_t linger_id);

    unique_ptr<ObjOp> init_ops(
      const VolumeRef& vol,
      const unique_ptr<ObjOp>& extra_ops) {

      if (!extra_ops)
	return vol->op();

      return extra_ops->clone();
    }


    // high-level helpers
    ceph_tid_t stat(const oid_t& oid,
		    const VolumeRef& volume,
		    uint64_t *psize, ceph::real_time *pmtime, int flags,
		    op_callback&& onfinish, version_t *objver = nullptr,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->stat(psize, pmtime);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t stat(const oid_t& oid,
		    const VolumeRef& volume, int flags,
		    function<void(int, uint64_t, ceph::real_time)>&& cb,
		    version_t *objver = nullptr,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->stat(move(cb));
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, objver, trace);
      return op_submit(o);
    }

    int stat(const oid_t& oid, const VolumeRef& volume,
	     uint64_t *psize, ceph::real_time *pmtime,
	     version_t *objver = nullptr,
	     ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      stat(oid, volume, psize, pmtime, 0, ref(w), nullptr, nullptr,
	   trace);

      return w.wait();
    }

    ceph_tid_t read(const oid_t& oid,
		    const VolumeRef& volume,
		    uint64_t off, uint64_t len, bufferlist *pbl, int flags,
		    op_callback&& onfinish,
		    version_t *objver = nullptr,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read(off, len, pbl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read(const oid_t& oid,
		    const VolumeRef& volume,
		    uint64_t off, uint64_t len, int flags,
		    read_callback&& onfinish,
		    version_t *objver = nullptr,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read(off, len, std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, objver, trace);
      return op_submit(o);
    }

    int read(const oid_t& oid,
	     const VolumeRef& volume,
	     uint64_t off, uint64_t len, bufferlist *pbl,
	     ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      read(oid, volume, off, len, pbl, 0, ref(w), nullptr, nullptr,
	   trace);

      return w.wait();
    }

    ceph_tid_t read_trunc(const oid_t& oid,
			  const VolumeRef& volume,
			  uint64_t off, uint64_t len, bufferlist *pbl,
			  int flags, uint64_t trunc_size, uint32_t trunc_seq,
			  op_callback&& onfinish, version_t *objver = nullptr,
			  const unique_ptr<ObjOp>& extra_ops = nullptr,
			  ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read(off, len, pbl, trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read_trunc(const oid_t& oid,
			  const VolumeRef& volume,
			  uint64_t off, uint64_t len,
			  int flags, uint64_t trunc_size, uint32_t trunc_seq,
			  read_callback&& onfinish,
			  version_t *objver = nullptr,
			  const unique_ptr<ObjOp>& extra_ops = nullptr,
			  ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read(off, len, trunc_size, trunc_seq, std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read_trunc(const oid_t& oid,
			  const VolumeRef& volume,
			  uint64_t off, uint64_t len, bufferlist *pbl,
			  uint64_t trunc_size, uint32_t trunc_seq,
			  ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      read_trunc(oid, volume, off, len, pbl, 0, trunc_size, trunc_seq,
		 ref(w), nullptr, nullptr, trace);

      return w.wait();
    }

    ceph_tid_t getxattr(const oid_t& oid,
			const VolumeRef& volume,
			const char *name, bufferlist *bl, int flags,
			op_callback&& onfinish, version_t *objver = nullptr,
			const unique_ptr<ObjOp>& extra_ops = nullptr,
			ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->getxattr(name, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t getxattr(const oid_t& oid,
			const VolumeRef& volume,
			const char *name, bufferlist *bl,
			ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      getxattr(oid, volume, name, bl, 0, ref(w), nullptr, nullptr,
	       trace);

      return w.wait();
    }

    ceph_tid_t getxattrs(const oid_t& oid,
			 const VolumeRef& volume,
			 map<string,bufferlist>& attrset, int flags,
			 op_callback&& onfinish, version_t *objver = nullptr,
			 const unique_ptr<ObjOp>& extra_ops = nullptr,
			 ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->getxattrs(attrset);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t getxattrs(const oid_t& oid,
			 const VolumeRef& volume,
			 map<string,bufferlist>& attrset,
			 ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      getxattrs(oid, volume, attrset, 0, ref(w), nullptr, nullptr,
		trace);

      return w.wait();
    }

    ceph_tid_t read_full(const oid_t& oid,
			 const VolumeRef& volume,
			 bufferlist *pbl, int flags, op_callback&& onfinish,
			 version_t *objver = nullptr,
			 const unique_ptr<ObjOp>& extra_ops = nullptr,
			 ZTracer::Trace *trace = nullptr);

    int read_full(const oid_t& oid, const VolumeRef& volume,
		  bufferlist *pbl, ZTracer::Trace *trace = nullptr) {
      return read(oid, volume, 0, 0, pbl, trace);
    }

    ceph_tid_t read_full(const oid_t& oid,
			 const VolumeRef& volume,
			 read_callback&& onfinish,
			 const unique_ptr<ObjOp>& extra_ops = nullptr,
			 ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read_full(std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, nullptr, trace);
      return op_submit(o);
    }

    // writes
    ceph_tid_t _modify(const oid_t& oid,
		       const VolumeRef& volume,
		       unique_ptr<ObjOp>& ops, ceph::real_time mtime, int flags,
		       op_callback&& onack, op_callback&& oncommit,
		       version_t *objver = nullptr,
		       ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, ops, flags | global_op_flags |
		     CEPH_OSD_FLAG_WRITE, move(onack),
		     move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int _modify(const oid_t& oid,
		const VolumeRef& volume,
		unique_ptr<ObjOp>& ops,
		ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      _modify(oid, volume, ops, mtime, 0, nullptr, ref(w), nullptr,
	      trace);

      return w.wait();
    }

    ceph_tid_t write(const oid_t& oid,
		     const VolumeRef& volume,
		     uint64_t off, uint64_t len, const bufferlist &bl,
		     ceph::real_time mtime, int flags, op_callback&& onack,
		     op_callback&& oncommit, version_t *objver = nullptr,
		     const unique_ptr<ObjOp>& extra_ops = nullptr,
		     ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->write(off, len, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int write(const oid_t& oid,
	      const VolumeRef& volume,
	      uint64_t off, uint64_t len, const bufferlist& bl,
	      ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      write(oid, volume, off, len, bl, mtime, 0, nullptr, ref(w),
	    nullptr, nullptr, trace);

      return w.wait();
    }

    ceph_tid_t append(const oid_t& oid,
		      const VolumeRef& volume,
		      uint64_t len, const bufferlist &bl,
		      ceph::real_time mtime,
		      int flags, op_callback&& onack,
		      op_callback&& oncommit,
		      version_t *objver = nullptr,
		      const unique_ptr<ObjOp>& extra_ops = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->append(len, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int append(const oid_t& oid,
	       const VolumeRef& volume,
	       uint64_t len, const bufferlist &bl,
	       ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;

      append(oid, volume, len, bl, mtime, 0, nullptr, ref(w),
	     nullptr, nullptr, trace);

      return w.wait();
    }

    ceph_tid_t write_trunc(const oid_t& oid,
			   const VolumeRef& volume,
			   uint64_t off, uint64_t len, const bufferlist &bl,
			   ceph::real_time mtime, int flags,
			   uint64_t trunc_size, uint32_t trunc_seq,
			   op_callback&& onack, op_callback&& oncommit,
			   version_t *objver = nullptr,
			   const unique_ptr<ObjOp>& extra_ops = nullptr,
			   ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->write(off, len, bl, trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int write_trunc(const oid_t& oid,
		    const VolumeRef& volume,
		    uint64_t off, uint64_t len, const bufferlist &bl,
		    uint64_t trunc_size,
		    uint32_t trunc_seq,
		    ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      write_trunc(oid, volume, off, len, bl, mtime, 0, trunc_size, trunc_seq,
		  nullptr, ref(w), nullptr, nullptr, trace);

      return w.wait();
    }

    ceph_tid_t write_full(const oid_t& oid,
			  const VolumeRef& volume,
			  const bufferlist &bl, ceph::real_time mtime,
			  int flags, op_callback&& onack,
			  op_callback&& oncommit,
			  version_t *objver = nullptr,
			  const unique_ptr<ObjOp>& extra_ops = nullptr,
			  ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->write_full(bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int write_full(const oid_t& oid,
		   const VolumeRef& volume,
		   const bufferlist &bl,
		   ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      write_full(oid, volume, bl, mtime, 0, nullptr, ref(w), nullptr,
		 nullptr, trace);

      return w.wait();
    }

    ceph_tid_t trunc(const oid_t& oid,
		     const VolumeRef& volume,
		     ceph::real_time mtime, int flags, uint64_t trunc_size,
		     uint32_t trunc_seq, op_callback&& onack,
		     op_callback&& oncommit, version_t *objver = nullptr,
		     const unique_ptr<ObjOp>& extra_ops = nullptr,
		     ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->truncate(trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int trunc(const oid_t& oid,
	      const VolumeRef& volume,
	      uint64_t trunc_size,
	      uint32_t trunc_seq = 0,
	      ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      trunc(oid, volume, mtime, 0, trunc_size, trunc_seq, nullptr, ref(w),
	    nullptr, nullptr, trace);

      return w.wait();
    }

    ceph_tid_t zero(const oid_t& oid,
		    const VolumeRef& volume,
		    uint64_t off, uint64_t len, ceph::real_time mtime,
		    int flags, op_callback&& onack, op_callback&& oncommit,
		    version_t *objver = nullptr,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->zero(off, len);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int zero(const oid_t& oid, const VolumeRef& volume,
	     uint64_t off, uint64_t len, ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      zero(oid, volume, off, len, mtime, 0, nullptr, ref(w), nullptr,
	   nullptr, trace);

      return w.wait();
    }

    ceph_tid_t create(const oid_t& oid,
		      const VolumeRef& volume,
		      ceph::real_time mtime, int global_flags,
		      int create_flags,
		      op_callback&& onack, op_callback&& oncommit,
		      version_t *objver = nullptr,
		      const unique_ptr<ObjOp>& extra_ops = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->create(create_flags);
      Op *o = new Op(oid, volume, ops,
		     global_flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int create(const oid_t& oid,
	       const VolumeRef& volume,
	       int create_flags,
	       ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      create(oid, volume, mtime, 0, create_flags, nullptr, ref(w), nullptr,
	     nullptr, trace);

      return w.wait();
    }

    ceph_tid_t remove(const oid_t& oid,
		      const VolumeRef& volume,
		      ceph::real_time mtime, int flags, op_callback&& onack,
		      op_callback&& oncommit, version_t *objver = nullptr,
		      const unique_ptr<ObjOp>& extra_ops = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->remove();
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    ceph_tid_t remove(const oid_t& oid,
		      const VolumeRef& volume,
		      ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      remove(oid, volume, mtime, 0, nullptr, ref(w), nullptr, nullptr,
	     trace);

      return w.wait();
    }

    ceph_tid_t lock(const oid_t& oid,
		    const VolumeRef& volume, int op,
		    int flags, op_callback&& onack,
		    op_callback&& oncommit, version_t *objver = nullptr,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->add_op(op);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      return op_submit(o);
    }


    ceph_tid_t setxattr(const oid_t& oid,
			const VolumeRef& volume,
			const char *name, const bufferlist &bl,
			ceph::real_time mtime, int flags,
			op_callback&& onack, op_callback&& oncommit,
			version_t *objver = nullptr,
			const unique_ptr<ObjOp>& extra_ops = nullptr,
			ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->setxattr(name, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int setxattr(const oid_t& oid, const VolumeRef& volume,
		 const char *name, const bufferlist &bl,
		 ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      setxattr(oid, volume, name, bl, mtime, 0, nullptr, ref(w),
	       nullptr, nullptr, trace);

      return w.wait();
    }

    ceph_tid_t removexattr(const oid_t& oid,
			   const VolumeRef& volume,
			   const char *name, ceph::real_time mtime, int flags,
			   op_callback&& onack, op_callback&& oncommit,
			   version_t *objver = nullptr,
			   const unique_ptr<ObjOp>& extra_ops = nullptr,
			   ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->rmxattr(name);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int removexattr(const oid_t& oid,
		    const VolumeRef& volume,
		    const char *name, ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      removexattr(oid, volume, name, mtime, 0, nullptr, ref(w), nullptr,
		  nullptr, trace);

      return w.wait();
    }

    int create_volume(const string& name, Context *onfinish);
    int delete_volume(const string& name, Context *onfinish);

    // ---------------------------
    // df stats
  private:
    void _fs_stats_submit(StatfsOp& op);
  public:
    void handle_fs_stats_reply(MStatfsReply *m);
    void get_fs_stats(struct ceph_statfs& result, Context *onfinish);
    void statfs_op_cancel(ceph_tid_t tid, int r);
    void _finish_statfs_op(StatfsOp& op);

    void ms_handle_connect(Connection *con);
    bool ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con);
    bool ms_get_authorizer(int dest_type,
			   AuthAuthorizer **authorizer,
			   bool force_new);

    void blacklist_self(bool set);
    VolumeRef vol_by_uuid(const boost::uuids::uuid& id);
    VolumeRef vol_by_name(const string& name);
  };

  inline bool operator==(const Objecter::OSDSession &l,
			 const Objecter::OSDSession &r) {
    return l.osd == r.osd;
  }
  inline bool operator!=(const Objecter::OSDSession &l,
			 const Objecter::OSDSession &r) {
    return l.osd != r.osd;
  }

  inline bool operator>(const Objecter::OSDSession &l,
			const Objecter::OSDSession &r) {
    return l.osd > r.osd;
  }
  inline bool operator<(const Objecter::OSDSession &l,
			const Objecter::OSDSession &r) {
    return l.osd < r.osd;
  }
  inline bool operator>=(const Objecter::OSDSession &l,
			 const Objecter::OSDSession &r) {
    return l.osd >= r.osd;
  }
  inline bool operator<=(const Objecter::OSDSession &l,
			 const Objecter::OSDSession &r) {	\
    return l.osd <= r.osd;
  }


  inline bool operator==(const Objecter::SubOp &l,
			 const Objecter::SubOp &r) {
    return l.tid == r.tid;
  }
  inline bool operator!=(const Objecter::SubOp &l,
			 const Objecter::SubOp &r) {
    return l.tid != r.tid;
  }

  inline bool operator>(const Objecter::SubOp &l,
			const Objecter::SubOp &r) {
    return l.tid > r.tid;
  }
  inline bool operator<(const Objecter::SubOp &l,
			const Objecter::SubOp &r) {
    return l.tid < r.tid;
  }
  inline bool operator>=(const Objecter::SubOp &l,
			 const Objecter::SubOp &r) {
    return l.tid >= r.tid;
  }
  inline bool operator<=(const Objecter::SubOp &l,
			 const Objecter::SubOp &r) {
    return l.tid <= r.tid;
  }

  inline bool operator==(const Objecter::op_base &l,
			 const Objecter::op_base &r) {
    return l.tid == r.tid;
  }
  inline bool operator!=(const Objecter::op_base &l,
			 const Objecter::op_base &r) {
    return l.tid != r.tid;
  }

  inline bool operator>(const Objecter::op_base &l,
			const Objecter::op_base &r) {
    return l.tid > r.tid;
  }
  inline bool operator<(const Objecter::op_base &l,
			const Objecter::op_base &r) {
    return l.tid < r.tid;
  }
  inline bool operator>=(const Objecter::op_base &l,
			 const Objecter::op_base &r) {
    return l.tid >= r.tid;
  }
  inline bool operator<=(const Objecter::op_base &l,
			 const Objecter::op_base &r) {
    return l.tid <= r.tid;
  }

  inline bool operator==(const Objecter::StatfsOp &l,
			 const Objecter::StatfsOp &r) {
    return l.tid == r.tid;
  }
  inline bool operator!=(const Objecter::StatfsOp &l,
			 const Objecter::StatfsOp &r) {
    return l.tid != r.tid;
  }

  inline bool operator>(const Objecter::StatfsOp &l,
			const Objecter::StatfsOp &r) {
    return l.tid > r.tid;
  }
  inline bool operator<(const Objecter::StatfsOp &l,
			const Objecter::StatfsOp &r) {
    return l.tid < r.tid;
  }
  inline bool operator>=(const Objecter::StatfsOp &l,
			 const Objecter::StatfsOp &r) {
    return l.tid >= r.tid;
  }
  inline bool operator<=(const Objecter::StatfsOp &l,
			 const Objecter::StatfsOp &r) {
    return l.tid <= r.tid;
  }

  // A very simple class, but since we don't need anything more
  // involved for RBD, I own't spend the time trying to WRITE anything
  // more involved for RBD.

  class Flusher {
    std::mutex lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    typedef std::unique_lock<std::mutex> unique_lock;
    std::condition_variable cond;
    struct flush_queue;
    class BatchComplete : public slist_base_hook<link_mode<auto_unlink> >
    {
      friend Flusher;
      Flusher& f;
      int* r;
      flush_queue* fq;
      op_callback cb;

      BatchComplete(Flusher& _f,
		    int *r,
		    op_callback _cb) : f(_f), fq(nullptr), cb(_cb) {}

    public:
      void operator()(int s) {
	{
	  {
	    unique_lock l(f.lock);
	    unlink();
	    if (r && ((s < 0) && (*r == 0)))
	      *r = s;
	    if (fq)
	      (*fq)(s, l);
	  }
	}
	if (cb)
	  cb(s);
      }
    };
    slist<BatchComplete, constant_time_size<false> > completions;
    int r;

    struct flush_queue {
      int r;
      op_callback cb;
      slist<BatchComplete, constant_time_size<false> > completions;
      uint32_t ref;
      std::condition_variable c;
      flush_queue(
	int _r,
	slist<BatchComplete, constant_time_size<false> >& _completions,
	op_callback&& _cb) : r(_r), cb(_cb), ref(0) {
	while (!_completions.empty()) {
	  // Since I have to iterate over it anyway and I lose element
	  // unlink if I turn auto_unlink off, I may as well do it
	  // inline rather than loop twice
	  auto& c = _completions.front();
	  c.unlink();
	  c.r = nullptr;
	  c.fq = this;
	  completions.push_front(c);
	}
	assert(!completions.empty());
      }
      int wait(unique_lock& l) {
	assert(l.owns_lock());
	++ref;
	assert(!completions.empty());
	c.wait(l, [this](){ return completions.empty(); });
	int _r = r;
	if (--ref == 0)
	  delete this;
	return _r;
      }
      void operator()(int _r, unique_lock& l) {
	assert(l.owns_lock());
	// Our caller holds the lock.
	if (r == 0) {
	  r = _r;
	}
	if (completions.empty()) {
	  c.notify_all();
	  if (cb) {
	    l.unlock();
	    cb(r);
	  }
	  if (!ref)
	    delete this;
	}
      }
    };

  public:
    Flusher() : r(0) {}
    ~Flusher() {
      lock_guard l(lock);
      for (auto& c : completions) {
	c(-EINTR);
      }
    }

    op_callback completion(op_callback&& c = nullptr) {
      BatchComplete* cb = new BatchComplete(*this, &r, c);
      {
	lock_guard l(lock);
	completions.push_front(*cb);
      }
      return ref(*cb);
    }

    int flush() {
      // Returns 0 if no writes have failed since the last flush.
      unique_lock l(lock);

      if (completions.empty()) {
	int my_r = r;
	r = 0;
	return my_r;
      }
      auto nf = new flush_queue(r, completions, nullptr);
      return nf->wait(l);
    }


    void flush(OSDC::op_callback&& cb) {
      unique_lock l(lock);

      if (!completions.empty())
	new flush_queue(r, completions, std::move(cb));

      if (cb) {
	cb(r);
	r = 0;
	return;
      }
    }
  };
};

using OSDC::Objecter;

#endif
