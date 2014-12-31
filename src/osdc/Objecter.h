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

#include "common/admin_socket.h"
#include "common/Timer.h"
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

  class Objecter {
  public:
    Messenger *messenger;
    MonClient *monc;
    OSDMap    *osdmap;
    CephContext *cct;

    bool initialized;

  private:
    ceph_tid_t last_tid;
    int client_inc;
    uint64_t max_linger_id;
    int num_unacked;
    int num_uncommitted;
    int global_op_flags; // flags which are applied to each IO op
    bool keep_balanced_budget;
    bool honor_osdmap_full;
    ZTracer::Endpoint trace_endpoint;

  public:
    void maybe_request_map();
  private:

    version_t last_seen_osdmap_version;
    version_t last_seen_pgmap_version;

    Mutex &client_lock;
    SafeTimer &timer;

    class C_Tick : public Context {
      Objecter *ob;
    public:
      C_Tick(Objecter *o) : ob(o) {}
      void finish(int r) { ob->tick(); }
    } *tick_event;

    void schedule_tick();
    void tick();

  public:
    /*** track pending operations ***/
    // read
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

    struct op_base {
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

    struct LingerOp : public RefCountedObject,
		      public op_base {
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
    struct OSDSession {
      slist<SubOp, constant_time_size<false> > subops;
      slist<SubOp, constant_time_size<false> > linger_subops;
      int osd;
      int incarnation;
      ConnectionRef con;

      OSDSession(int o) : osd(o), incarnation(0), con(NULL) {}
    };
    map<int,OSDSession*> osd_sessions;


  private:
    // pending ops
    map<ceph_tid_t,Op*> inflight_ops; // Should probably be an slist
    map<ceph_tid_t,SubOp*> subops;
    int num_homeless_ops;
    map<uint64_t, LingerOp*> linger_ops;
    map<ceph_tid_t,StatfsOp*> statfs_ops;

    map<uint64_t, LingerOp*> check_latest_map_lingers;
    map<ceph_tid_t, Op*> check_latest_map_ops;

    map<epoch_t,list< pair<Context*, int> > > waiting_for_map;

    double mon_timeout, osd_timeout;

    void send_subop(SubOp &subop, int flags);
    void send_op(Op *op);
    void cancel_op(Op *op);
    void finish_op(Op *op);
    enum recalc_op_target_result {
      RECALC_OP_TARGET_NO_ACTION = 0,
      RECALC_OP_TARGET_NEED_RESEND,
      RECALC_OP_TARGET_OSD_DNE,
      RECALC_OP_TARGET_OSD_DOWN,
    };
    bool osdmap_full_flag() const;
    bool target_should_be_paused(op_base *op);

    int calc_target(op_base *t);
    int recalc_op_target(Op *op);
    bool recalc_linger_op_target(LingerOp *op);

    void send_linger(LingerOp *info);
    void _linger_ack(LingerOp *info, int r);
    void _linger_commit(LingerOp *info, int r);

    void _send_op_map_check(Op *op);
    void op_cancel_map_check(Op *op);
    void _send_linger_map_check(LingerOp *op);
    void linger_cancel_map_check(LingerOp *op);

    void kick_requests(OSDSession *session);

    OSDSession *get_session(int osd);
    void reopen_session(OSDSession *session);
    void close_session(OSDSession *session);

    void resend_mon_ops();

    /**
     * handle a budget for in-flight ops
     * budget is taken whenever an op goes into the ops map
     * and returned whenever an op is removed from the map
     * If throttle_op needs to throttle it will unlock client_lock.
     */
    void throttle_op(Op *op, uint64_t op_size = 0);
    void take_op_budget(Op *op) {
      uint64_t op_budget = op->op->get_budget();
      if (keep_balanced_budget) {
	throttle_op(op, op_budget);
      } else {
	op_throttle_bytes.take(op_budget);
	op_throttle_ops.take(1);
      }
      op->budgeted = true;
    }
    void put_op_budget(Op *op) {
      assert(op->budgeted);
      uint64_t op_budget = op->op->get_budget();
      op_throttle_bytes.put(op_budget);
      op_throttle_ops.put(1);
    }
    Throttle op_throttle_bytes, op_throttle_ops;

  public:
    Objecter(CephContext *cct_, Messenger *m, MonClient *mc,
	     OSDMap *om, Mutex& l, SafeTimer& t, double mon_timeout,
	     double osd_timeout) :
      messenger(m), monc(mc), osdmap(om), cct(cct_),
      initialized(false),
      last_tid(0), client_inc(-1), max_linger_id(0),
      num_unacked(0), num_uncommitted(0),
      global_op_flags(0),
      keep_balanced_budget(false), honor_osdmap_full(true),
      trace_endpoint("0.0.0.0", 0, "Objecter"),
      last_seen_osdmap_version(0),
      last_seen_pgmap_version(0),
      client_lock(l), timer(t),
      tick_event(NULL),
      num_homeless_ops(0),
      mon_timeout(mon_timeout),
      osd_timeout(osd_timeout),
      op_throttle_bytes(cct, "objecter_bytes", cct->_conf->objecter_inflight_op_bytes),
      op_throttle_ops(cct, "objecter_ops", cct->_conf->objecter_inflight_ops)
      { }
    ~Objecter() {
      assert(!tick_event);
    }

    void init();
    void shutdown();

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

    void scan_requests(bool force_resend,
		       bool force_resend_writes,
		       map<ceph_tid_t, Op*>& need_resend,
		       list<LingerOp*>& need_resend_linger);

    // messages
  public:
    void dispatch(Message *m);
    void handle_osd_op_reply(MOSDOpReply *m);
    void handle_osd_map(MOSDMap *m);
    void wait_for_osd_map();

  private:
    // low-level
    ceph_tid_t _op_submit(Op *op);
    inline void unregister_op(Op *op);

    // public interface
  public:
    ceph_tid_t op_submit_special(Op *op); // Stupid Stupid Stupid
    ceph_tid_t op_submit(Op *op);
    bool is_active() {
      return !(inflight_ops.empty() && linger_ops.empty());
    }

    /**
     * Output in-flight requests
     */
    int get_client_incarnation() const { return client_inc; }
    void set_client_incarnation(int inc) { client_inc = inc; }

    void wait_for_new_map(Context *c, epoch_t epoch, int err=0);
    void wait_for_latest_osdmap(Context *fin);
    void _get_latest_version(epoch_t oldest, epoch_t neweset, Context *fin);

    /** Get the current set of global op flags */
    int get_global_op_flags() { return global_op_flags; }
    /** Add a flag to the global op flags */
    void add_global_op_flags(int flag) { global_op_flags |= flag; }
    /** Clear the passed flags from the global op flag set */
    void clear_global_op_flag(int flags) { global_op_flags &= ~flags; }

    /// cancel an in-progress request with the given return code
    int op_cancel(ceph_tid_t tid, int r);

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
      ops->read(off, len, pbl);
      Op *o = new Op(oid, volume, ops,
		     flags | global_op_flags | CEPH_OSD_FLAG_READ,
		     onfinish, 0, objver, trace);
      return op_submit(o);
    }

    ceph_tid_t read_trunc(const object_t& oid,
			  const shared_ptr<const Volume>& volume,
			  uint64_t off, uint64_t len, bufferlist *pbl, int flags,
			  uint64_t trunc_size, uint32_t trunc_seq,
			  Context *onfinish, version_t *objver = NULL,
			  const unique_ptr<ObjOp>& extra_ops = nullptr,
                          ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
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
		     utime_t mtime, int flags, Context *onack, Context *oncommit,
		     version_t *objver = NULL,
		     const unique_ptr<ObjOp>& extra_ops = nullptr,
                     ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
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
			   uint32_t trunc_seq, Context *onack, Context *oncommit,
			   version_t *objver = NULL,
			   const unique_ptr<ObjOp>& extra_ops = nullptr,
                           ZTracer::Trace *trace = nullptr) {
      unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
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
    void fs_stats_submit(StatfsOp *op);
  public:
    void handle_fs_stats_reply(MStatfsReply *m);
    void get_fs_stats(struct ceph_statfs& result, Context *onfinish);
    int statfs_op_cancel(ceph_tid_t tid, int r);
    void finish_statfs_op(StatfsOp *op);

    // ---------------------------
    // some scatter/gather hackery

    void _sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl,
			 bufferlist *bl, Context *onfinish);

    struct C_SGRead : public Context {
      Objecter *objecter;
      vector<ObjectExtent> extents;
      vector<bufferlist> resultbl;
      bufferlist *bl;
      Context *onfinish;
      C_SGRead(Objecter *ob,
	       vector<ObjectExtent>& e, vector<bufferlist>& r,
	       bufferlist *b, Context *c) :
	objecter(ob), bl(b), onfinish(c) {
	extents.swap(e);
	resultbl.swap(r);
      }
      void finish(int r) {
	objecter->_sg_read_finish(extents, resultbl, bl, onfinish);
      }
    };

    void sg_read_trunc(vector<ObjectExtent>& extents,
		       const shared_ptr<const Volume>& volume,
		       bufferlist *bl, int flags, uint64_t trunc_size,
		       uint32_t trunc_seq, Context *onfinish) {
      if (extents.size() == 1) {
	read_trunc(extents[0].oid, volume, extents[0].offset,
		   extents[0].length, bl, flags, extents[0].truncate_size,
		   trunc_seq, onfinish);
      } else {
	C_GatherBuilder gather;
	vector<bufferlist> resultbl(extents.size());
	int i=0;
	for (vector<ObjectExtent>::iterator p = extents.begin();
	     p != extents.end(); ++p) {
	  read_trunc(p->oid, volume, p->offset, p->length,
		     &resultbl[i++], flags, p->truncate_size, trunc_seq,
		     gather.new_sub());
	}
	gather.set_finisher(new C_SGRead(this, extents, resultbl, bl, onfinish));
	gather.activate();
      }
    }

    void sg_read(vector<ObjectExtent>& extents,
		 const shared_ptr<const Volume>& volume, bufferlist *bl,
		 int flags, Context *onfinish) {
      sg_read_trunc(extents, volume, bl, flags, 0, 0, onfinish);
    }

    void sg_write_trunc(vector<ObjectExtent>& extents,
			const shared_ptr<const Volume>& volume,
			const bufferlist& bl, utime_t mtime, int flags,
			uint64_t trunc_size, uint32_t trunc_seq, Context *onack,
			Context *oncommit) {
      if (extents.size() == 1) {
	write_trunc(extents[0].oid, volume, extents[0].offset,
		    extents[0].length, bl, mtime, flags,
		    extents[0].truncate_size, trunc_seq, onack, oncommit);
      } else {
	C_GatherBuilder gack(onack);
	C_GatherBuilder gcom(oncommit);
	for (vector<ObjectExtent>::iterator p = extents.begin();
	     p != extents.end(); ++p) {
	  bufferlist cur;
	  for (vector<pair<uint64_t,uint64_t> >::iterator bit
		 = p->buffer_extents.begin();
	       bit != p->buffer_extents.end();
	       ++bit)
	    bl.copy(bit->first, bit->second, cur);
	  assert(cur.length() == p->length);
	  write_trunc(p->oid, volume, p->offset, p->length,
		      cur, mtime, flags, p->truncate_size, trunc_seq,
		      onack ? gack.new_sub():0,
		      oncommit ? gcom.new_sub():0);
	}
	gack.activate();
	gcom.activate();
      }
    }

    void sg_write(vector<ObjectExtent>& extents,
		  const shared_ptr<const Volume>& volume,
		  const bufferlist& bl, utime_t mtime, int flags, Context *onack,
		  Context *oncommit) {
      sg_write_trunc(extents, volume, bl, mtime, flags, 0, 0, onack, oncommit);
    }

    void ms_handle_connect(Connection *con);
    void ms_handle_reset(Connection *con);
    void ms_handle_remote_reset(Connection *con);
    void blacklist_self(bool set);
  };
};

using OSDC::Objecter;

#endif
