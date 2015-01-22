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
#include <list>
#include <map>
#include <memory>
#include <sstream>
#include <boost/intrusive/slist.hpp>
#include "include/types.h"
#include "include/buffer.h"

#include "osd/OSDMap.h"
#include "messages/MOSDOp.h"

#include "common/Timer.h"
#include "common/RWLock.h"
#include "common/zipkin_trace.h"
#include "include/rados/rados_types.h"
#include "include/rados/rados_types.hpp"

#include "ObjectOperation.h"

class Context;
class Messenger;
class OSDMap;
class MonClient;
class Message;
class MStatfsReply;
class MOSDOpReply;
class MOSDMap;

namespace OSDC {
  using boost::intrusive::slist;
  using boost::intrusive::slist_base_hook;
  using boost::intrusive::link_mode;
  using boost::intrusive::auto_unlink;
  using boost::intrusive::constant_time_size;
  using std::vector;
  using std::set;
  using std::string;
  using std::shared_ptr;
  using std::unique_ptr;
  using std::move;

  class Objecter: public Dispatcher {
  public:
    Messenger *messenger;
    MonClient *monc;
  private:
    OSDMap    *osdmap;
  public:
    CephContext *cct;

    std::atomic<bool> initialized;

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

    int _maybe_request_map();

    version_t last_seen_osdmap_version;

    RWLock rwlock;
    Mutex timer_lock;
    SafeTimer timer;

    class C_Tick : public Context {
      Objecter *ob;
    public:
      C_Tick(Objecter *o) : ob(o) {}
      void finish(int r) { ob->tick(); }
    } *tick_event;

    void schedule_tick();
    void tick();

  public:

    struct OSDSession;
    struct Op;

    struct SubOp : public slist_base_hook<link_mode<auto_unlink> > {
      ceph_tid_t tid;
      int incarnation;
      OSDSession *session;
      int osd;
      hobject_t hoid;
      vector<OSDOp> ops;

      eversion_t replay_version; // for op replay
      utime_t stamp;
      int attempts;
      bool done;
      Op& parent;

      // Never call this. Stupid STL.

      SubOp(Op& p) : tid(0), incarnation(0), session(nullptr), osd(-1),
		     attempts(0), done(false), parent(p) { }
      SubOp(const hobject_t& h, const vector<OSDOp>& o, Op& p)
	: tid(0), incarnation(0), session(nullptr), osd(-1), hoid(h), ops(o),
	  attempts(0), done(false), parent(p) { }
      SubOp(hobject_t&& h, vector<OSDOp>&& o, Op& p)
	: tid(0), incarnation(0), session(nullptr), osd(-1), hoid(h), ops(o),
	  attempts(0), done(false), parent(p) { }
    };

    struct op_base : public RefCountedObject {
      int flags;
      object_t oid;
      shared_ptr<const Volume> volume;
      unique_ptr<ObjOp> op;
      vector<SubOp> subops;
      utime_t mtime;
      bufferlist *outbl;
      version_t *objver;
      ceph_tid_t tid;
      epoch_t map_dne_bound;

      bool paused;

      Mutex lock;

      op_base() : flags(0), volume(nullptr), op(nullptr), outbl(nullptr),
		  tid(0), map_dne_bound(0),
		  paused(false) { }

      op_base(object_t oid, const shared_ptr<const Volume>& volume,
	      unique_ptr<ObjOp>& _op, int flags, version_t* ov)
	: flags(flags), oid(oid), volume(volume),
	  op(move(_op)), outbl(nullptr), objver(ov),
	  tid(0), map_dne_bound(0), paused(false) {
	if (objver)
	  *objver = 0;
      }
    };

    struct Op : public op_base {
      Context *onack, *oncommit, *ontimeout;
      epoch_t *reply_epoch;
      bool budgeted;
      // true if the throttle budget is get/put on a series of OPs,
      // instead of per OP basis, when this flag is set, the budget is
      // acquired before sending the very first OP of the series and
      // released upon receiving the last OP reply.
      bool ctx_budgeted;
      /// true if we should resend this message on failure
      bool should_resend;
      ZTracer::Trace trace;

      Op(const object_t& o, const shared_ptr<const Volume>& volume,
	 unique_ptr<ObjOp>& _op,
	 int f, Context *ac, Context *co, version_t *ov,
	 ZTracer::Trace *parent) :
	op_base(o, volume, _op, f, ov),
	onack(ac), oncommit(co),
	ontimeout(NULL), reply_epoch(NULL),
	budgeted(false), should_resend(true) {
	subops.reserve(op->width());
	op->realize(
	  oid,
	  [this](hobject_t&& h, vector<OSDOp>&& o) {
	    this->subops.emplace_back(h, o, *this);
	  });
	if (parent && parent->valid())
	  trace.init("op", NULL, parent);
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


    struct C_Stat : public Context {
      bufferlist bl;
      uint64_t *psize;
      utime_t *pmtime;
      Context *fin;
      C_Stat(uint64_t *ps, utime_t *pm, Context *c) :
	psize(ps), pmtime(pm), fin(c) {}
      void finish(int r) {
	if (r >= 0) {
	  bufferlist::iterator p = bl.begin();
	  uint64_t s;
	  utime_t m;
	  ::decode(s, p);
	  ::decode(m, p);
	  if (psize)
	    *psize = s;
	  if (pmtime)
	    *pmtime = m;
	}
	fin->complete(r);
      }
    };

    struct C_GetAttrs : public Context {
      bufferlist bl;
      map<string,bufferlist>& attrset;
      Context *fin;
      C_GetAttrs(map<string, bufferlist>& set, Context *c) : attrset(set), fin(c) {}
      void finish(int r) {
	if (r >= 0) {
	  bufferlist::iterator p = bl.begin();
	  ::decode(attrset, p);
	}
	fin->complete(r);
      }
    };

    struct StatfsOp {
      ceph_tid_t tid;
      struct ceph_statfs *stats;
      Context *onfinish, *ontimeout;
      utime_t last_submit;
    };


    // -- lingering ops --

    struct LingerOp : public op_base {
      uint64_t linger_id;
      bufferlist inbl;
      bool registered;
      Context *on_reg_ack, *on_reg_commit;

      LingerOp() : linger_id(0),
		   registered(false),
		   on_reg_ack(NULL), on_reg_commit(NULL) {}

      // no copy!
      const LingerOp &operator=(const LingerOp& r) = delete;
      LingerOp(const LingerOp& o) = delete;
    private:
      ~LingerOp() {}
    };

    struct C_Linger_Ack : public Context {
      Objecter *objecter;
      LingerOp *info;
      C_Linger_Ack(Objecter *o, LingerOp *l) : objecter(o), info(l) {
	info->get();
      }
      ~C_Linger_Ack() {
	info->put();
      }
      void finish(int r) {
	objecter->_linger_ack(info, r);
      }
    };

    struct C_Linger_Commit : public Context {
      Objecter *objecter;
      LingerOp *info;
      C_Linger_Commit(Objecter *o, LingerOp *l) : objecter(o), info(l) {
	info->get();
      }
      ~C_Linger_Commit() {
	info->put();
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
    struct OSDSession : public RefCountedObject {
      RWLock lock;
      Mutex **completion_locks; // Needed?
      slist<SubOp, constant_time_size<false> > subops;
      slist<SubOp, constant_time_size<false> > linger_subops;
      int osd;
      int incarnation;
      int num_locks;
      ConnectionRef con;

      OSDSession(CephContext *cct, int o) :
	osd(o), incarnation(0), con(NULL) {
	// We may not need or want this. It's not as if we need to
	// serialize all object responses
	num_locks = 32;
	completion_locks = new Mutex *[num_locks];
	for (int i = 0; i < num_locks; i++) {
	  completion_locks[i] = new Mutex;
	}
      }
      ~OSDSession();

      Mutex *get_lock(object_t& oid);
      bool is_homeless() { return (osd == -1); }
    };
    map<int,OSDSession*> osd_sessions;


  private:
    map<ceph_tid_t, Op*> inflight_ops;
    map<uint64_t, LingerOp*> linger_ops;
    map<ceph_tid_t,StatfsOp*> statfs_ops;

    OSDSession *homeless_session;

    // ops waiting for an osdmap with a new volume or confirmation
    // that the volume does not exist (may be expanded to other uses
    // later)
    map<uint64_t, LingerOp*> check_latest_map_lingers;
    map<ceph_tid_t, Op*> check_latest_map_ops;

    map<epoch_t,list< pair<Context*, int> > > waiting_for_map;

    double mon_timeout, osd_timeout;

    MOSDOp *_prepare_osd_op(Op *op);
    void _send_subop(SubOp &subop, int flags);
    void _send_op(Op *op, MOSDOp *m = nullptr);
    void _cancel_linger_op(Op *op);
    void _finish_subop(OSDSession *session, ceph_tid_t tid);
    void _finish_subop(SubOp& subop);
    void _finish_op(Op *op);

    enum target_result {
      TARGET_NO_ACTION = 0,
      TARGET_NEED_RESEND,
      TARGET_VOLUME_DNE
    };
    bool osdmap_full_flag() const;
    bool target_should_be_paused(op_base *op);

    int _calc_targets(op_base *t, bool any_change=false);
    int _map_session(op_base *op, OSDSession **s,
		     RWLock::Context& lc);

    void _session_subop_assign(OSDSession* s, SubOp& subop);
    void _session_subop_remove(OSDSession* s, SubOp& subop);
    void _session_linger_subop_assign(OSDSession* to, SubOp& subop);
    void _session_linger_subop_remove(OSDSession* from, SubOp& subop);

    int _get_osd_session(int osd, RWLock::Context& lc,
			 OSDSession **session);
    int _assign_subop_target_session(SubOp& op, RWLock::Context& lc,
				     bool src_session_locked,
				     bool dst_session_locked);
    int _get_subop_target_session(SubOp& op, RWLock::Context& lc,
				  OSDSession** session);
    int _recalc_linger_op_targets(LingerOp *op, RWLock::Context& lc);

    void _linger_submit(LingerOp *info);
    void _send_linger(LingerOp *info);
    void _linger_ack(LingerOp *info, int r);
    void _linger_commit(LingerOp *info, int r);

    void _check_op_volume_dne(Op *op, bool session_locked);
    void _send_subop_map_check(Op *op);
    void _op_cancel_map_check(Op *op);
    void _check_linger_volume_dne(LingerOp *op, bool *need_unregister);
    void _send_linger_map_check(LingerOp *op);
    void _linger_cancel_map_check(LingerOp *op);

    void kick_requests(OSDSession *session);
    void _kick_requests(OSDSession *session,
			map<uint64_t, LingerOp *>& lresend);
    void _linger_ops_resend(map<uint64_t, LingerOp *>& lresend);

    int _get_session(int osd, OSDSession **session, RWLock::Context& lc);
    void put_session(OSDSession *s);
    void get_session(OSDSession *s);
    void _reopen_session(OSDSession *session);
    void close_session(OSDSession *session);

    void resend_mon_ops();

    /**
     * handle a budget for in-flight ops
     * budget is taken whenever an op goes into the ops map
     * and returned whenever an op is removed from the map
     * If throttle_op needs to throttle it will unlock client_lock.
     */
    int calc_op_budget(Op *op);
    void _throttle_op(Op *op, int op_size=0);
    int _take_op_budget(Op *op) {
      assert(rwlock.is_locked());
      int op_budget = calc_op_budget(op);
      if (keep_balanced_budget) {
	_throttle_op(op, op_budget);
      } else {
	op_throttle_bytes.take(op_budget);
	op_throttle_ops.take(1);
      }
      op->budgeted = true;
      return op_budget;
    }
    void put_op_budget_bytes(int op_budget) {
      assert(op_budget >= 0);
      op_throttle_bytes.put(op_budget);
      op_throttle_ops.put(1);
    }
    void put_op_budget(Op *op) {
      assert(op->budgeted);
      int op_budget = calc_op_budget(op);
      put_op_budget_bytes(op_budget);
    }
    Throttle op_throttle_bytes, op_throttle_ops;

  public:
    Objecter(CephContext *cct_, Messenger *m, MonClient *mc,
	     double mon_timeout, double osd_timeout) :
      Dispatcher(cct_), messenger(m), monc(mc),
      osdmap(new OSDMap), cct(cct_),
      initialized(false), last_tid(0),
      client_inc(-1), max_linger_id(0),
      num_unacked(0), num_uncommitted(0),
      global_op_flags(0),
      keep_balanced_budget(false), honor_osdmap_full(true),
      trace_endpoint("0.0.0.0", 0, "Objecter"),
      last_seen_osdmap_version(0),
      timer(cct, timer_lock, false),
      tick_event(NULL),
      homeless_session(new OSDSession(cct, -1)),
      mon_timeout(mon_timeout),
      osd_timeout(osd_timeout),
      op_throttle_bytes(cct, "objecter_bytes",
			cct->_conf->objecter_inflight_op_bytes),
      op_throttle_ops(cct, "objecter_ops", cct->_conf->objecter_inflight_ops)
      { }
    ~Objecter();

    void init();
    void start();
    void shutdown();

    const OSDMap *get_osdmap_read() {
      rwlock.get_read();
      return osdmap;
    }
    void put_osdmap_read() {
      rwlock.put_read();
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

    void _scan_requests(bool force_resend,
			bool force_resend_writes,
			map<ceph_tid_t, Op*>& need_resend,
			list<LingerOp*>& need_resend_linger);
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

    void handle_osd_op_reply(MOSDOpReply *m);
    void handle_osd_map(MOSDMap *m);
    void wait_for_osd_map();

  private:
    bool _promote_lock_check_race(RWLock::Context& lc);

    // low-level
    ceph_tid_t _op_submit(Op *op, RWLock::Context& lc);
    ceph_tid_t _op_submit_with_budget(Op *op, RWLock::Context& lc,
				      int *ctx_budget = nullptr);
    inline void unregister_op(Op *op);

    // public interface
  public:
    ceph_tid_t op_submit(Op *op, int *ctx_budget = nullptr);
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
    int op_cancel(OSDSession *s, ceph_tid_t tid, int r);
    friend class C_CancelOp;


    // mid-level helpers
    Op *prepare_mutate_op(const object_t& oid,
			  const shared_ptr<const Volume>& volume,
			  unique_ptr<ObjOp>& op,
			  utime_t mtime, int flags,
			  Context *onack, Context *oncommit,
			  version_t *objver = NULL,
			  ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, op,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return o;
    }
    ceph_tid_t mutate(const object_t& oid,
		      const shared_ptr<const Volume>& volume,
		      unique_ptr<ObjOp>& op,
		      utime_t mtime, int flags,
		      Context *onack, Context *oncommit,
		      version_t *objver = NULL,
		      ZTracer::Trace *trace = nullptr) {
      Op *o = prepare_mutate_op(oid, volume, op, mtime, flags, onack,
				oncommit, objver, trace);
      return op_submit(o);
    }
    Op *prepare_read_op(const object_t& oid,
			const shared_ptr<const Volume>& volume,
			unique_ptr<ObjOp>& op, bufferlist *pbl,
			int flags, Context *onack, version_t *objver = NULL,
			ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, op,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ, onack,
		     NULL, objver, trace);
      o->outbl = pbl;
      return o;
    }
    ceph_tid_t read(const object_t& oid,
		    const shared_ptr<const Volume>& volume,
		    unique_ptr<ObjOp>& op, bufferlist *pbl,
		    int flags, Context *onack, version_t *objver = NULL,
		    ZTracer::Trace *trace = nullptr) {
      Op *o = prepare_read_op(oid, volume, op, pbl, flags, onack,
			      objver, trace);
      return op_submit(o);
    }
    ceph_tid_t linger_mutate(const object_t& oid,
			     const shared_ptr<const Volume>& volume,
			     unique_ptr<ObjOp>& op, utime_t mtime,
			     bufferlist& inbl, int flags, Context *onack,
			     Context *onfinish, version_t *objver);
    ceph_tid_t linger_read(const object_t& oid,
			   const shared_ptr<const Volume>& volume,
			   unique_ptr<ObjOp>& op, bufferlist& inbl,
			   bufferlist *outbl, int flags, Context *onack,
			   version_t *objver);
    void unregister_linger(uint64_t linger_id);
    void _unregister_linger(uint64_t linger_id);

    unique_ptr<ObjOp> init_ops(
      const shared_ptr<const Volume>& vol,
      const unique_ptr<ObjOp>& extra_ops) {

      if (!extra_ops)
	return vol->op();

      return extra_ops->clone();
    }


    // high-level helpers
    ceph_tid_t stat(const object_t& oid,
		    const shared_ptr<const Volume>& volume,
		    uint64_t *psize, utime_t *pmtime, int flags,
		    Context *onfinish, version_t *objver = NULL,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->stat(psize, pmtime);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     onfinish, 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read(const object_t& oid,
		    const shared_ptr<const Volume>& volume,
		    uint64_t off, uint64_t len, bufferlist *pbl, int flags,
		    Context *onfinish,
		    version_t *objver = NULL,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read(off, len, pbl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     onfinish, 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read_trunc(const object_t& oid,
			  const shared_ptr<const Volume>& volume,
			  uint64_t off, uint64_t len, bufferlist *pbl,
			  int flags, uint64_t trunc_size, uint32_t trunc_seq,
			  Context *onfinish, version_t *objver = NULL,
			  const unique_ptr<ObjOp>& extra_ops = nullptr,
			  ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->read(off, len, pbl, trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     onfinish, 0, objver, trace);
      return op_submit(o);
    }
    ceph_tid_t getxattr(const object_t& oid,
			const shared_ptr<const Volume>& volume,
			const char *name, bufferlist *bl, int flags,
			Context *onfinish, version_t *objver = NULL,
			const unique_ptr<ObjOp>& extra_ops = nullptr,
			ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->getxattr(name, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     onfinish, 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t getxattrs(const object_t& oid,
			 const shared_ptr<const Volume>& volume,
			 map<string,bufferlist>& attrset, int flags,
			 Context *onfinish, version_t *objver = NULL,
			 const unique_ptr<ObjOp>& extra_ops = nullptr,
			 ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->getxattrs(attrset);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     onfinish, 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read_full(const object_t& oid,
			 const shared_ptr<const Volume>& volume,
			 bufferlist *pbl, int flags, Context *onfinish,
			 version_t *objver = NULL,
			 const unique_ptr<ObjOp>& extra_ops = nullptr,
			 ZTracer::Trace *trace = nullptr) {
      return read(oid, volume, 0, 0, pbl,
		  flags | global_op_flags | CEPH_OSD_FLAG_READ, onfinish,
		  objver, nullptr, trace);
    }

    // writes
    ceph_tid_t _modify(const object_t& oid,
		       const shared_ptr<const Volume>& volume,
		       unique_ptr<ObjOp>& ops, utime_t mtime, int flags,
		       Context *onack, Context *oncommit,
		       version_t *objver = NULL,
		       ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, ops, flags | global_op_flags |
		     CEPH_OSD_FLAG_WRITE, onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }
    ceph_tid_t write(const object_t& oid,
		     const shared_ptr<const Volume>& volume,
		     uint64_t off, uint64_t len, const bufferlist &bl,
		     utime_t mtime, int flags, Context *onack,
		     Context *oncommit, version_t *objver = NULL,
		     const unique_ptr<ObjOp>& extra_ops = nullptr,
		     ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->write(off, len, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }
    ceph_tid_t append(const object_t& oid,
		      const shared_ptr<const Volume>& volume,
		      uint64_t len, const bufferlist &bl, utime_t mtime,
		      int flags, Context *onack, Context *oncommit,
		      version_t *objver = NULL,
		      const unique_ptr<ObjOp>& extra_ops = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->append(len, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }
    ceph_tid_t write_trunc(const object_t& oid,
			   const shared_ptr<const Volume>& volume,
			   uint64_t off, uint64_t len, const bufferlist &bl,
			   utime_t mtime, int flags, uint64_t trunc_size,
			   uint32_t trunc_seq, Context *onack,
			   Context *oncommit, version_t *objver = NULL,
			   const unique_ptr<ObjOp>& extra_ops = nullptr,
			   ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->write(off, len, bl, trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }
    ceph_tid_t write_full(const object_t& oid,
			  const shared_ptr<const Volume>& volume,
			  const bufferlist &bl, utime_t mtime, int flags,
			  Context *onack, Context *oncommit,
			  version_t *objver = NULL,
			  const unique_ptr<ObjOp>& extra_ops = nullptr,
			  ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->write_full(bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    ceph_tid_t trunc(const object_t& oid,
		     const shared_ptr<const Volume>& volume,
		     utime_t mtime, int flags, uint64_t trunc_size,
		     uint32_t trunc_seq, Context *onack, Context *oncommit,
		     version_t *objver = NULL,
		     const unique_ptr<ObjOp>& extra_ops = nullptr,
		     ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->truncate(trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    ceph_tid_t zero(const object_t& oid,
		    const shared_ptr<const Volume>& volume,
		    uint64_t off, uint64_t len, utime_t mtime, int flags,
		    Context *onack, Context *oncommit,
		    version_t *objver = NULL,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->zero(off, len);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    ceph_tid_t create(const object_t& oid,
		      const shared_ptr<const Volume>& volume,
		      utime_t mtime, int global_flags, int create_flags,
		      Context *onack, Context *oncommit,
		      version_t *objver = NULL,
		      const unique_ptr<ObjOp>& extra_ops = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->create(create_flags);
      Op *o = new Op(oid, volume, ops,
		     global_flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    ceph_tid_t remove(const object_t& oid,
		      const shared_ptr<const Volume>& volume,
		      utime_t mtime, int flags, Context *onack,
		      Context *oncommit, version_t *objver = NULL,
		      const unique_ptr<ObjOp>& extra_ops = nullptr,
		      ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->remove();
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    ceph_tid_t lock(const object_t& oid,
		    const shared_ptr<const Volume>& volume, int op,
		    int flags, Context *onack, Context *oncommit,
		    version_t *objver = NULL,
		    const unique_ptr<ObjOp>& extra_ops = nullptr,
		    ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->add_op(op);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t setxattr(const object_t& oid,
			const shared_ptr<const Volume>& volume,
			const char *name, const bufferlist &bl,
			utime_t mtime, int flags,
			Context *onack, Context *oncommit,
			version_t *objver = NULL,
			const unique_ptr<ObjOp>& extra_ops = nullptr,
			ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->setxattr(name, bl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }
    ceph_tid_t removexattr(const object_t& oid,
			   const shared_ptr<const Volume>& volume,
			   const char *name, utime_t mtime, int flags,
			   Context *onack, Context *oncommit,
			   version_t *objver = NULL,
			   const unique_ptr<ObjOp>& extra_ops = nullptr,
			   ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
      if (!ops)
	return -EDOM;
      ops->rmxattr(name);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_WRITE,
		     onack, oncommit, objver, trace);
      o->mtime = mtime;
      return op_submit(o);
    }

    int create_volume(const string& name, Context *onfinish);
    int delete_volume(const string& name, Context *onfinish);

    // ---------------------------
    // df stats
  private:
    void _fs_stats_submit(StatfsOp *op);
  public:
    void handle_fs_stats_reply(MStatfsReply *m);
    void get_fs_stats(struct ceph_statfs& result, Context *onfinish);
    int statfs_op_cancel(ceph_tid_t tid, int r);
    void _finish_statfs_op(StatfsOp *op);

    void ms_handle_connect(Connection *con);
    bool ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con);
    bool ms_get_authorizer(int dest_type,
			   AuthAuthorizer **authorizer,
			   bool force_new);

    void blacklist_self(bool set);
  };
};

using OSDC::Objecter;

#endif
