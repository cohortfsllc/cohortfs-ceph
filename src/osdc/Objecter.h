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

#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>

#include <boost/intrusive/set.hpp>
#include <boost/intrusive/slist.hpp>
#include <boost/uuid/nil_generator.hpp>

#include "include/buffer.h"
#include "include/types.h"

#include "common/cohort_function.h"
#include "common/shunique_lock.h"
#include "common/Timer.h"
#include "common/zipkin_trace.h"

#include "messages/MOSDOp.h"
#include "msg/MessageFactory.h"
#include "ObjectOperation.h"
#include "rados_err.h"
#include "osd/OSDMap.h"

class Messenger;
class OSDMap;
class MonClient;
class Message;
class MStatfsReply;
class MOSDOpReply;
class MOSDMap;

namespace rados{
  typedef cohort::function<void(int)> op_callback;
  typedef cohort::function<void(int, uint64_t,
				ceph::real_time)> stat_callback;
  typedef cohort::function<void(int, bufferlist&&)> read_callback;

  class CB_Waiter {
  protected:
    std::mutex lock;
    std::condition_variable cond;
    bool done;
    int r;

  public:

    CB_Waiter() : done(false) { }
    CB_Waiter(const CB_Waiter&) = delete;
    CB_Waiter(CB_Waiter&&) = delete;

    CB_Waiter& operator=(const CB_Waiter&) = delete;
    CB_Waiter& operator=(CB_Waiter&&) = delete;
    virtual ~CB_Waiter() {}

    operator cohort::function<void(int)>() {
      return std::ref(*this);
    }

    operator std::function<void(int)>() {
      return std::ref(*this);
    }


    int wait() {
      std::unique_lock<std::mutex> l(lock);
      cond.wait(l, [this](){ return done; });
      return r;
    }

    bool complete() {
      std::unique_lock<std::mutex> l(lock);
      return done;
    }

    virtual void work(void) {}

    void operator()(int _r) {
      std::unique_lock<std::mutex> l(lock);
      if (done)
	return;

      done = true;
      r = _r;
      work();
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
    vector<cohort::function<void()> > osdmap_notifiers;
  public:
    CephContext *cct;

  private:
    std::atomic<uint64_t> last_tid;
    std::atomic<uint32_t> client_inc;
    std::atomic<uint32_t> num_unacked;
    std::atomic<uint32_t> num_uncommitted;
    // flags which are applied to each IO op
    std::atomic<uint32_t> global_op_flags;
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

    struct SubOp : public boost::intrusive::set_base_hook<
      boost::intrusive::link_mode<boost::intrusive::normal_link>> {
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
      boost::intrusive::slist_member_hook<
	boost::intrusive::link_mode<boost::intrusive::normal_link>> qlink;

      // Never call this. Stupid STL.

      SubOp(Op& p) : tid(0), incarnation(0), session(nullptr), osd(-1),
		     attempts(0), done(false), parent(p) { }
      SubOp(const oid_t& h, const vector<OSDOp>& o, Op& p)
	: tid(0), incarnation(0), session(nullptr), osd(-1), hoid(h), ops(o),
	  attempts(0), done(false), parent(p) { }
      SubOp(oid_t&& h, vector<OSDOp>&& o, Op& p)
	: tid(0), incarnation(0), session(nullptr), osd(-1), hoid(h), ops(o),
	  attempts(0), done(false), parent(p) { }
      bool is_homeless() const {
	return (osd == -1);
      }
    };

    struct Op : public boost::intrusive::set_base_hook<
      boost::intrusive::link_mode<boost::intrusive::normal_link>>,
		public RefCountedObject {
      oid_t oid;
      AVolRef volume;
      std::vector<SubOp> subops;
      std::unique_ptr<ObjOp> op;
      int flags;
      ceph::real_time mtime;
      ceph_tid_t tid;
      epoch_t map_dne_bound;
      bool paused;
      uint16_t priority;
      bool should_resend;
      std::mutex lock;
      typedef std::unique_lock<std::mutex> unique_lock;
      typedef std::lock_guard<std::mutex> lock_guard;
      op_callback onack, oncommit;
      uint64_t ontimeout;
      uint32_t acks, commits;
      int rc;
      epoch_t *reply_epoch;
      bool finished;
      ZTracer::Trace trace;

      Op(const oid_t& o, const AVolRef& _volume,
	 std::unique_ptr<ObjOp>& _op, int f,
	 op_callback&& ac, op_callback&& co,
	 ZTracer::Trace *parent) :
	oid(o), volume(_volume), op(std::move(_op)), flags(f),
	ontimeout(0), acks(0), commits(0), rc(0),
	reply_epoch(nullptr), finished(false) {
	onack.swap(ac);
	oncommit.swap(co);
	subops.reserve(op->width());
	op->realize(
	  oid,
	  [this](oid_t&& h, vector<OSDOp>&& o) {
	    this->subops.emplace_back(h, o, *this);
	  });
	if (parent && parent->valid())
	  trace.init("op", nullptr, parent);
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

    struct StatfsOp : public boost::intrusive::set_base_hook<
      boost::intrusive::link_mode<
      boost::intrusive::normal_link>> {
      ceph_tid_t tid;
      struct ceph_statfs *stats;
      Context *onfinish;
      uint64_t ontimeout;
      ceph::mono_time last_submit;
    };

    // -- osd sessions --
    struct OSDSession : public boost::intrusive::set_base_hook<
      boost::intrusive::link_mode< boost::intrusive::normal_link>>,
				     public RefCountedObject {
      std::shared_timed_mutex lock;
      typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
      typedef std::shared_lock<std::shared_timed_mutex> shared_lock;

      static constexpr const uint32_t max_ops_inflight = 5;
      boost::intrusive::set<SubOp> subops_inflight;

      static constexpr const uint64_t max_ops_queued = 100;
      boost::intrusive::set<SubOp> subops_queued;

      int osd;
      int incarnation;
      int num_locks;
      ConnectionRef con;

      OSDSession(CephContext *cct, int o) :
	osd(o), incarnation(0), con(nullptr) { }
      ~OSDSession();
    };
    boost::intrusive::set<OSDSession> osd_sessions;

  private:
    boost::intrusive::set<Op> inflight_ops;
    boost::intrusive::set<StatfsOp> statfs_ops;

    static constexpr const uint32_t max_homeless_subops = 500;
    boost::intrusive::set<SubOp> homeless_subops;

    // ops waiting for an osdmap with a new volume or confirmation
    // that the volume does not exist (may be expanded to other uses
    // later)
    map<ceph_tid_t, Op*> check_latest_map_ops;

    map<epoch_t,list< pair<Context*, int> > > waiting_for_map;

    ceph::timespan mon_timeout, osd_timeout;

    MOSDOp *_prepare_osd_subop(SubOp& op);
    void _send_subop(SubOp& op, MOSDOp *m = nullptr);
    void _cancel_op(Op& op);
    void _finish_subop(SubOp& subop);
    void _finish_op(Op& op, Op::unique_lock& ol); // Releases ol

    enum target_result {
      TARGET_NO_ACTION = 0,
      TARGET_NEED_RESEND,
      TARGET_VOLUME_DNE
    };
    bool osdmap_full_flag() const;
    bool target_should_be_paused(Op& op);

    int _calc_targets(Op& t, Op::unique_lock& ol);
    OSDSession* _map_session(SubOp& subop, Op::unique_lock& ol,
			     const shunique_lock& lc);

    void _session_subop_assign(OSDSession& to, SubOp& subop);
    void _session_subop_remove(OSDSession& from, SubOp& subop);

    int _assign_subop_target_session(SubOp& op, shared_lock& lc,
				     bool src_session_locked,
				     bool dst_session_locked);

    void _check_op_volume_dne(Op& op, Op::unique_lock& ol);
    void _send_op_map_check(Op& op);
    void _op_cancel_map_check(Op& op);

    void kick_requests(OSDSession& session);
    void _kick_requests(OSDSession& session);
    OSDSession* _get_session(int osd, const shunique_lock& shl);
    void put_session(OSDSession& s);
    void get_session(OSDSession& s);
    void _reopen_session(OSDSession& session);
    void close_session(OSDSession& session);

    void resend_mon_ops();

  public:
    Objecter(CephContext *cct_, Messenger *m, MonClient *mc,
	     ceph::timespan mon_timeout = std::chrono::seconds(0),
	     ceph::timespan osd_timeout = std::chrono::seconds(0)) :
      Dispatcher(cct_), messenger(m), monc(mc),
      osdmap(new OSDMap),
      last_tid(0),
      client_inc(-1),
      num_unacked(0), num_uncommitted(0),
      global_op_flags(0),
      honor_osdmap_full(true),
      trace_endpoint("0.0.0.0", 0, "Objecter"),
      last_seen_osdmap_version(0),
      tick_event(0),
      mon_timeout(mon_timeout),
      osd_timeout(osd_timeout)
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
    template<typename C>
    void with_osdmap(
      const C& f) {
      shared_lock l(rwlock);
      f(const_cast<const OSDMap&>(*osdmap));
    }


    void set_honor_osdmap_full() { honor_osdmap_full = true; }
    void unset_honor_osdmap_full() { honor_osdmap_full = false; }

    void _scan_requests(boost::intrusive::set<SubOp>& subops,
			shunique_lock& sl,
			bool force_resend,
			bool force_resend_writes,
			map<ceph_tid_t, SubOp*>& need_resend);
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
    void add_osdmap_notifier(const cohort::function<void()>& f);

  private:

    // low-level
    ceph_tid_t _op_submit(Op& op, Op::unique_lock& ol, shunique_lock& sl);
    // public interface
  public:
    ceph_tid_t op_submit(Op *op);
    bool is_active() {
      return !(inflight_ops.empty() && statfs_ops.empty());
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
			  const AVolRef& volume,
			  std::unique_ptr<ObjOp>& op,
			  ceph::real_time mtime,
			  op_callback&& onack, op_callback&& oncommit,
			  ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, op,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack),
		     move(oncommit), trace);
      o->mtime = mtime;
      return o;
    }

    ceph_tid_t mutate(const oid_t& oid,
		      const AVolRef& volume,
		      std::unique_ptr<ObjOp>& op,
		      ceph::real_time mtime,
		      op_callback&& onack, op_callback&& oncommit,
		      ZTracer::Trace *trace = nullptr) {
      Op *o = prepare_mutate_op(oid, volume, op, mtime, move(onack),
				move(oncommit), trace);
      return op_submit(o);
    }

    ceph_tid_t mutate(const oid_t& oid,
		      const AVolRef& volume,
		      std::unique_ptr<ObjOp>& op,
		      ZTracer::Trace *trace = nullptr) {
      auto mtime = ceph::real_clock::now();
      CB_Waiter w;
      mutate(oid, volume, op, mtime, w, nullptr, trace);

      return w.wait();
    }

    Op *prepare_read_op(const oid_t& oid,
			const AVolRef& volume,
			std::unique_ptr<ObjOp>& op,
			op_callback&& onack,
			ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, op,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onack),
		     nullptr, trace);
      return o;
    }
    ceph_tid_t read(const oid_t& oid, const AVolRef& volume,
		    std::unique_ptr<ObjOp>& op, op_callback&& onack,
		    ZTracer::Trace *trace = nullptr) {
      Op *o = prepare_read_op(oid, volume, op, move(onack), trace);
      return op_submit(o);
    }

    ceph_tid_t read(const oid_t& oid,
		    const AVolRef& volume,
		    std::unique_ptr<ObjOp>& op,
		    ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;

      read(oid, volume, op, w, trace);

      return w.wait();
    }

    // high-level helpers
    ceph_tid_t stat(const oid_t& oid,
		    const AVolRef& volume,
		    uint64_t *psize, ceph::real_time *pmtime,
		    op_callback&& onfinish,
		    ZTracer::Trace *trace = nullptr) {
      auto ops = volume->op();
      ops->stat(psize, pmtime);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t stat(const oid_t& oid,
		    const AVolRef& volume,
		    cohort::function<void(int, uint64_t, ceph::real_time)>&& cb,
		    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->stat(move(cb));
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, trace);
      return op_submit(o);
    }

    int stat(const oid_t& oid, const AVolRef& volume,
	     uint64_t *psize, ceph::real_time *pmtime,
	     ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      stat(oid, volume, psize, pmtime, w, trace);

      return w.wait();
    }

    ceph_tid_t read(const oid_t& oid,
		    const AVolRef& volume,
		    uint64_t off, uint64_t len, bufferlist *pbl,
		    op_callback&& onfinish,
		    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->read(off, len, pbl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t read(const oid_t& oid,
		    const AVolRef& volume,
		    uint64_t off, uint64_t len,
		    read_callback&& onfinish,
		    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->read(off, len, std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, trace);
      return op_submit(o);
    }

    int read(const oid_t& oid,
	     const AVolRef& volume,
	     uint64_t off, uint64_t len, bufferlist *pbl,
	     ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      read(oid, volume, off, len, pbl, w, trace);

      return w.wait();
    }

    ceph_tid_t read_trunc(const oid_t& oid,
			  const AVolRef& volume,
			  uint64_t off, uint64_t len, bufferlist *pbl,
			  uint64_t trunc_size, uint32_t trunc_seq,
			  op_callback&& onfinish,
			  ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->read(off, len, pbl, trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t read_trunc(const oid_t& oid,
			  const AVolRef& volume,
			  uint64_t off, uint64_t len,
			  uint64_t trunc_size, uint32_t trunc_seq,
			  read_callback&& onfinish,
			  ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->read(off, len, trunc_size, trunc_seq, std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, trace);
      return op_submit(o);
    }

    ceph_tid_t read_trunc(const oid_t& oid,
			  const AVolRef& volume,
			  uint64_t off, uint64_t len, bufferlist *pbl,
			  uint64_t trunc_size, uint32_t trunc_seq,
			  ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      read_trunc(oid, volume, off, len, pbl, trunc_size, trunc_seq,
		 w, trace);

      return w.wait();
    }

    ceph_tid_t getxattr(const oid_t& oid,
			const AVolRef& volume,
			const string& name, bufferlist *bl,
			op_callback&& onfinish,
			ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->getxattr(name, bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t getxattr(const oid_t& oid,
			const AVolRef& volume,
			const string& name,
			read_callback&& onfinish,
			ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->getxattr(name, std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, nullptr, trace);
      return op_submit(o);
    }

    ceph_tid_t getxattr(const oid_t& oid,
			const AVolRef& volume,
			const string& name, bufferlist *bl,
			ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      getxattr(oid, volume, name, bl, w, trace);

      return w.wait();
    }

    ceph_tid_t getxattrs(const oid_t& oid,
			 const AVolRef& volume,
			 map<string,bufferlist>& attrset,
			 op_callback&& onfinish,
			 ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->getxattrs(attrset);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t getxattrs(const oid_t& oid,
			 const AVolRef& volume,
			 map<string,bufferlist>& attrset,
			 ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      getxattrs(oid, volume, attrset, w, trace);

      return w.wait();
    }

    ceph_tid_t read_full(const oid_t& oid,
			 const AVolRef& volume,
			 bufferlist *pbl, op_callback&& onfinish,
			 ZTracer::Trace *trace = nullptr);

    int read_full(const oid_t& oid, const AVolRef& volume,
		  bufferlist *pbl, ZTracer::Trace *trace = nullptr) {
      return read(oid, volume, 0, 0, pbl, trace);
    }

    ceph_tid_t read_full(const oid_t& oid,
			 const AVolRef& volume,
			 read_callback&& onfinish,
			 ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->read_full(std::move(onfinish));
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     nullptr, 0, trace);
      return op_submit(o);
    }

    // writes
    ceph_tid_t _modify(const oid_t& oid,
		       const AVolRef& volume,
		       std::unique_ptr<ObjOp>& ops,
		       op_callback&& onack, op_callback&& oncommit,
		       ZTracer::Trace *trace = nullptr) {
      Op *o = new Op(oid, volume, ops, global_op_flags |
		     CEPH_OSD_FLAG_WRITE, move(onack),
		     move(oncommit), trace);
      return op_submit(o);
    }

    int _modify(const oid_t& oid,
		const AVolRef& volume,
		std::unique_ptr<ObjOp>& ops,
		ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      _modify(oid, volume, ops, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t write(const oid_t& oid,
		     const AVolRef& volume,
		     uint64_t off, uint64_t len, const bufferlist &bl,
		     op_callback&& onack,
		     op_callback&& oncommit,
		     ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->write(off, len, bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int write(const oid_t& oid,
	      const AVolRef& volume,
	      uint64_t off, uint64_t len, const bufferlist& bl,
	      ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      write(oid, volume, off, len, bl, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t append(const oid_t& oid,
		      const AVolRef& volume,
		      uint64_t len, const bufferlist &bl,
		      op_callback&& onack,
		      op_callback&& oncommit,
		      ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->append(len, bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int append(const oid_t& oid,
	       const AVolRef& volume,
	       uint64_t len, const bufferlist &bl,
	       ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;

      append(oid, volume, len, bl, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t write_trunc(const oid_t& oid,
			   const AVolRef& volume,
			   uint64_t off, uint64_t len, const bufferlist &bl,
			   uint64_t trunc_size, uint32_t trunc_seq,
			   op_callback&& onack, op_callback&& oncommit,
			   ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->write(off, len, bl, trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int write_trunc(const oid_t& oid,
		    const AVolRef& volume,
		    uint64_t off, uint64_t len, const bufferlist &bl,
		    uint64_t trunc_size,
		    uint32_t trunc_seq,
		    ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      write_trunc(oid, volume, off, len, bl, trunc_size, trunc_seq,
		  nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t write_full(const oid_t& oid,
			  const AVolRef& volume,
			  const bufferlist &bl,
			  op_callback&& onack,
			  op_callback&& oncommit,
			  ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->write_full(bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int write_full(const oid_t& oid,
		   const AVolRef& volume,
		   const bufferlist &bl,
		   ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      write_full(oid, volume, bl, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t trunc(const oid_t& oid,
		     const AVolRef& volume,
		     uint64_t trunc_size,
		     uint32_t trunc_seq, op_callback&& onack,
		     op_callback&& oncommit,
		     ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->truncate(trunc_size, trunc_seq);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int trunc(const oid_t& oid,
	      const AVolRef& volume,
	      uint64_t trunc_size,
	      uint32_t trunc_seq = 0,
	      ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      trunc(oid, volume, trunc_size, trunc_seq, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t zero(const oid_t& oid,
		    const AVolRef& volume,
		    uint64_t off, uint64_t len,
		    op_callback&& onack, op_callback&& oncommit,
		    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->zero(off, len);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int zero(const oid_t& oid, const AVolRef& volume,
	     uint64_t off, uint64_t len, ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      zero(oid, volume, off, len, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t create(const oid_t& oid,
		      const AVolRef& volume,
		      int create_flags,
		      op_callback&& onack, op_callback&& oncommit,
		      ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->create(create_flags);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int create(const oid_t& oid,
	       const AVolRef& volume,
	       int create_flags,
	       ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      create(oid, volume, create_flags, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t remove(const oid_t& oid,
		      const AVolRef& volume,
		      op_callback&& onack,
		      op_callback&& oncommit,
		      ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->remove();
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    ceph_tid_t remove(const oid_t& oid,
		      const AVolRef& volume,
		      ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      remove(oid, volume, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t lock(const oid_t& oid,
		    const AVolRef& volume, int op,
		    op_callback&& onack,
		    op_callback&& oncommit,
		    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->add_op(op);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }


    ceph_tid_t setxattr(const oid_t& oid,
			const AVolRef& volume,
			const string& name, const bufferlist &bl,
			op_callback&& onack, op_callback&& oncommit,
			ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->setxattr(name, bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int setxattr(const oid_t& oid, const AVolRef& volume,
		 const string& name, const bufferlist &bl,
		 ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      setxattr(oid, volume, name, bl, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t removexattr(const oid_t& oid, const AVolRef& volume,
			   const string& name, op_callback&& onack,
			   op_callback&& oncommit,
			   ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->rmxattr(name);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int removexattr(const oid_t& oid,
		    const AVolRef& volume,
		    const string& name, ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      removexattr(oid, volume, name, nullptr, w, trace);

      return w.wait();
    }

    ceph_tid_t omap_get_header(const oid_t& oid,
			       const AVolRef& volume,
			       bufferlist* bl,
			       op_callback&& onack,
			       ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->omap_get_header(bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onack), nullptr, trace);
      return op_submit(o);
    }

    int omap_get_header(const oid_t& oid,
			const AVolRef& volume,
			bufferlist* bl,
			ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      omap_get_header(oid, volume, bl, w, trace);
      return w.wait();
    }

    ceph_tid_t omap_set_header(const oid_t& oid, const AVolRef& volume,
			       bufferlist& bl, op_callback&& onack,
			       op_callback&& oncommit,
			       ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->omap_set_header(bl);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int omap_set_header(const oid_t& oid,
			const AVolRef& volume,
			bufferlist& bl,
			ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      omap_set_header(oid, volume, bl, nullptr, w, trace);
      return w.wait();
    }

    ceph_tid_t omap_get_vals(const oid_t& oid, const AVolRef& volume,
			     const string& start_after,
			     const string& filter_prefix,
			     uint64_t max_to_get,
			     std::map<std::string,bufferlist> &out_set,
      op_callback&& onfinish, ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->omap_get_vals(start_after, filter_prefix, max_to_get, out_set);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t omap_get_vals(const oid_t& oid, const AVolRef& volume,
			     const string& start_after,
			     const string& filter_prefix,
			     uint64_t max_to_get,
			     std::map<std::string,bufferlist> &out_set,
      ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      omap_get_vals(oid, volume, start_after, filter_prefix, max_to_get,
		    out_set, trace);
      return w.wait();
    }

    ceph_tid_t omap_get_vals_by_keys(
      const oid_t& oid, const AVolRef& volume,
      const std::set<std::string> &to_get,
      std::map<std::string,bufferlist> &out_set,
      op_callback&& onfinish, ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->omap_get_vals_by_keys(to_get, out_set);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_READ,
		     move(onfinish), 0, trace);
      return op_submit(o);
    }

    ceph_tid_t omap_get_vals_by_keys(
      const oid_t& oid, const AVolRef& volume,
      const std::set<std::string> &to_get,
      std::map<std::string,bufferlist> &out_set,
      ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      omap_get_vals_by_keys(oid, volume, to_get, out_set, trace);
      return w.wait();
    }

    ceph_tid_t omap_set(const oid_t& oid, const AVolRef& volume,
			const map<string, bufferlist>& map,
			op_callback&& onack,
			op_callback&& oncommit,
			ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->omap_set(map);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int omap_set(const oid_t& oid, const AVolRef& volume,
		 const map<string, bufferlist>& map,
		 ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      omap_set(oid, volume, map, nullptr, w, trace);
      return w.wait();
    }

    ceph_tid_t omap_rm_keys(const oid_t& oid, const AVolRef& volume,
			    const std::set<string>& keys,
			    op_callback&& onack,
			    op_callback&& oncommit,
			    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->omap_rm_keys(keys);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int omap_rm_keys(const oid_t& oid, const AVolRef& volume,
		     const std::set<string>& keys,
		     ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      omap_rm_keys(oid, volume, keys, nullptr, w, trace);
      return w.wait();
    }

    ceph_tid_t set_alloc_hint(const oid_t& oid, const AVolRef& volume,
			      uint64_t expected_object_size,
			      uint64_t expected_write_size,
			      op_callback&& onack,
			      op_callback&& oncommit,
			      ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops = volume->op();
      ops->set_alloc_hint(expected_object_size, expected_write_size);
      Op *o = new Op(oid, volume, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int set_alloc_hint(const oid_t& oid, const AVolRef& volume,
		       uint64_t expected_object_size,
		       uint64_t expected_write_size,
		       ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      set_alloc_hint(oid, volume, expected_object_size, expected_write_size,
		     nullptr, w, trace);
      return w.wait();
    }

    ceph_tid_t call(const oid_t& oid, const AVolRef& vol,
		    const string& cname, const string& method,
		    bufferlist &indata, bufferlist* outdata,
		    op_callback&& onack, op_callback&& oncommit,
		    ZTracer::Trace *trace = nullptr) {
      ObjectOperation ops(vol->op());
      ops->call(cname, method, indata);
      Op *o = new Op(oid, vol, ops,
		     global_op_flags | CEPH_OSD_FLAG_WRITE,
		     move(onack), move(oncommit), trace);
      return op_submit(o);
    }

    int call(const oid_t& oid, const AVolRef& vol,
	     const string& cname, const string& method, bufferlist &indata,
	     bufferlist* outdata = nullptr,
	     ZTracer::Trace *trace = nullptr) {
      CB_Waiter w;
      call(oid, vol, cname, method, indata, outdata,
	   w, nullptr, trace);
      return w.wait();
    }

    int create_volume(const string& name, op_callback&& onfinish);
    int delete_volume(const string& name, op_callback&& onfinish);

    int create_volume(const string& name) {
      CB_Waiter w;
      create_volume(name, w);
      return w.wait();
    }
    int delete_volume(const string& name) {
      CB_Waiter w;
      delete_volume(name, w);
      return w.wait();
    }

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
    Volume vol_by_uuid(const boost::uuids::uuid& id);
    AVolRef attach_by_uuid(const boost::uuids::uuid& id);
    Volume vol_by_name(const string& name);
    AVolRef attach_by_name(const string& name);
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

  inline bool operator==(const Objecter::Op& l,
			 const Objecter::Op& r) {
    return l.tid == r.tid;
  }
  inline bool operator!=(const Objecter::Op& l,
			 const Objecter::Op& r) {
    return l.tid != r.tid;
  }

  inline bool operator>(const Objecter::Op& l,
			const Objecter::Op& r) {
    return l.tid > r.tid;
  }
  inline bool operator<(const Objecter::Op& l,
			const Objecter::Op& r) {
    return l.tid < r.tid;
  }
  inline bool operator>=(const Objecter::Op& l,
			 const Objecter::Op& r) {
    return l.tid >= r.tid;
  }
  inline bool operator<=(const Objecter::Op& l,
			 const Objecter::Op& r) {
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
    struct flush_queue;
    friend flush_queue;
    flush_queue* fq;
    std::mutex lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    typedef std::unique_lock<std::mutex> unique_lock;

    struct BatchComplete : public boost::intrusive::slist_base_hook<
      boost::intrusive::link_mode<
      boost::intrusive::normal_link>> {
      flush_queue& fq;
      op_callback cb;

      BatchComplete(flush_queue& _fq,
		    op_callback _cb) : fq(_fq), cb(_cb) {}

      void operator()(int r) {
	(fq)(r, *this);
	if (cb)
	  cb(r);
      }
    };

    struct flush_queue {
      friend Flusher;
      Flusher& f;
      bool flushed;
      int r;
      boost::intrusive::slist<BatchComplete> completions;
      op_callback cb;
      std::condition_variable c;
      flush_queue(Flusher& _f) : f(_f), flushed(false), r(0) { }
      ~flush_queue() {
	lock_guard l(f.lock);
	for (auto& c : completions) {
	  c(-EINTR);
	}
      }

      int flush(unique_lock& l) {
	assert(l.owns_lock());
	assert(!completions.empty());
	flushed = true;
	c.wait(l, [this](){ return completions.empty(); });
	int _r = r;
	delete this;
	return _r;
      }

      void flush(op_callback&& _cb) {
	assert(!completions.empty());
	flushed = true;
	cb.swap(_cb);
      }

      void operator()(int _r, BatchComplete& b) {
	unique_lock l(f.lock);
	// Our caller holds the lock.
	if (r == 0) {
	  r = _r;
	}
	completions.erase(completions.iterator_to(b));
	delete &b;
	if (completions.empty() && flushed) {
	  if (cb) {
	    l.unlock();
	    cb(r);
	    // We have no thread waiting on us, so delete after
	    // calling calback.
	    delete this;
	  } else {
	    // We do have a thread waiting, so wake it up and let it
	    // delete us.
	    c.notify_one();
	  }
	}
      }
    };

  public:
    Flusher() : fq(new flush_queue(*this)) { }
    op_callback completion(op_callback&& c = nullptr) {
      BatchComplete* cb = new BatchComplete(*fq, c);
      {
	lock_guard l(lock);
	fq->completions.push_front(*cb);
      }
      return std::ref(*cb);
    }

    int flush() {
      // Returns 0 if no writes have failed since the last flush.
      unique_lock l(lock);

      if (fq->completions.empty()) {
	int r = fq->r;
	fq->r = 0;
	return r;
      }

      flush_queue* oq = fq;
      fq = new flush_queue(*this);
      return oq->flush(l);
    }

    void flush(op_callback&& cb) {
      unique_lock l(lock);

      if (fq->completions.empty()) {
	int r = fq->r;
	fq->r = 0;
	l.unlock();
	cb(r);
      }

      flush_queue* oq = fq;
      fq = new flush_queue(*this);
      oq->flush(move(cb));
    }
  };
};

#endif
