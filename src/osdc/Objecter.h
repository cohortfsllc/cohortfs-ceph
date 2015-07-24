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
#include "common/FunQueue.h"
#include "common/is_iterator.h"
#include "common/shunique_lock.h"
#include "common/Timer.h"
#include "common/zipkin_trace.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDMap.h"
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

namespace rados {
  namespace rados_detail {
    using cohort::function;
    using std::error_code;
    using std::mutex;
    using std::condition_variable;
    using std::unique_lock;
    using std::lock_lock;
    using std::shared_lock;
    using std::system_error;
    using std::move;
    using std::tuple;
    using std::forward_as_tuple;

    typedef function<void()> thunk;
    typedef function<void(error_code)> op_callback;
    typedef function<void(error_code, ceph_statfs&)> statfs_callback;
  }

  using thunk;
  using op_callback;
  using statfs_callback;

  namespace rados_detail {
    class waiter_base {
    protected:
      mutex lock;
      condition_variable cond;
      bool done;
      error_code r;

      waiter_base() : done(false) { }
      waiter_base(const waiter_base&) = delete;
      waiter_base(waiter_base&&) = delete;

      waiter_base& operator=(const waiter_base&) = delete;
      waiter_base& operator=(waiter_base&&) = delete;
      virtual ~waiter_base() {}

      void wait_base() {
	unique_lock<mutex> l(lock);
	cond.wait(l, [this](){ return done; });
	if (r)
	  throw system_error(r);
      }

      void exec_base(error_code _r) {
	unique_lock<mutex> l(lock);
	if (done)
	  return;

	done = true;
	r = _r;
	work();
	cond.notify_one();
      }

      virtual void work(void) {}

    public:

      bool complete() {
	unique_lock<mutex> l(lock);
	return done;
      }


      void reset() {
	done = false;
      }
    };

    template<typename ...S>
    class waiter;

    template<>
    class waiter<void> : public waiter_base {
    public:
      operator op_callback() {
	return ref(*this);
      }
      operator thunk() {
	return ref(*this);
      }

      void wait() {
	wait_base();
      }

      void operator()(error_code _r) {
	exec_base(_r);
      }

      void operator()() {
	exec_base(error_code());
      }
    };

    template<typename Ret>
    class waiter<Ret> : public waiter_base {
      Ret ret;

    public:

      operator function<void(error_code, Ret&)>() {
	return ref(*this);
      }

      Ret&& wait() {
	wait_base();
	return move(ret);
      }

      void operator()(error_code _r, Ret& _ret) {
	ret = move(_ret);
	exec_base(_r);
      }
    };

    template<typename ...Ret>
    class waiter : public waiter_base {
      tuple<Ret...> ret;
    public:
      operator function<void(std::error_code, Ret...)>() {
	return ref(*this);
      }

      tuple<Ret...>&& wait() {
	wait_base();
	return move(ret);
      }

      void operator()(error_code _r, Ret&&... _ret) {
	ret = forward_as_tuple(_ret...);
	exec_base(_r);
      }
    };
  };

  typedef rados_detail::waiter<void> CB_Waiter;
  typedef rados_detail::waiter<uint64_t, ceph::real_time> Stat_Waiter;
  typedef rados_detail::waiter<bufferlist> Read_Waiter;
  typedef rados_detail::waiter<ceph_statfs> Statfs_Waiter;

  class MessageFactory : public ::MessageFactory {
   private:
    ::MessageFactory *parent;
   public:
    MessageFactory(::MessageFactory *parent) : parent(parent) {}
    Message* create(int type);
  };

  namespace rados_detail {
    using boost::intrusive::set;
    using boost::intrusive::slist;
    using boost::intrusive::slist_base_hook;
    using boost::intrusive::slist_member_hook;
    using boost::intrusive::set_base_hook;
    using boost::intrusive::link_mode;
    using boost::intrusive::normal_link;
    using std::errc;
    using std::atomic;
    using std::shared_timed_mutex;
    using cohort::Timer;
    using cohort::shunique_lock;
    using ceph::mono_clock;

    template<typename D>
    class Objecter {
      static constexpr const int dout_subsys = ceph_subsys_objecter;
    public:
      CephContext* cct;
    protected:
      OSDMap *osdmap;
      vector<thunk> osdmap_notifiers;

    protected:
      atomic<uint64_t> last_tid;
      atomic<uint32_t> client_inc;
      atomic<uint32_t> num_unacked;
      atomic<uint32_t> num_uncommitted;
      // flags which are applied to each IO op
      atomic<uint32_t> global_op_flags;
      bool honor_osdmap_full;
      ZTracer::Endpoint trace_endpoint;

    public:
      void maybe_request_map();
    protected:

      void _maybe_request_map();

      version_t last_seen_osdmap_version;

      shared_timed_mutex rwlock;
      typedef shared_lock<shared_timed_mutex> shared_lock;
      typedef unique_lock<shared_timed_mutex> unique_lock;
      typedef shunique_lock<shared_timed_mutex> shunique_lock;

      Timer<mono_clock> timer;
      uint64_t tick_event;
      void schedule_tick();
      void tick();

    public:

      struct OSDSession;
      struct Op;

      struct SubOp : public set_base_hook<link_mode<normal_link>> {
	ceph_tid_t tid;
	int incarnation;
	typename D::OSDSession *session;
	int osd;
	oid_t hoid;
	vector<OSDOp> ops;

	eversion_t replay_version; // for op replay
	mono_time stamp;
	int attempts;
	bool done;
	Op& parent;
	slist_member_hook<link_mode<normal_link>> qlink;

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

	bool operator==(const SubOp& r) {
	  return tid == r.tid;
	}
	bool operator!=(const SubOp& r) {
	  return tid != r.tid;
	}

	bool operator>(const SubOp& r) {
	  return tid > r.tid;
	}
	bool operator<(const SubOp& r) {
	  return tid < r.tid;
	}
	bool operator>=(const SubOp& r) {
	  return tid >= r.tid;
	}
	bool operator<=(const SubOp& r) {
	  return tid <= r.tid;
	}
      };

      struct Op : public set_base_hook<link_mode<normal_link>>,
		  public RefCountedObject {
	oid_t oid;
	AVolRef volume;
	vector<SubOp> subops;
	unique_ptr<ObjOp> op;
	int flags;
	real_time mtime;
	ceph_tid_t tid;
	epoch_t map_dne_bound;
	bool paused;
	uint16_t priority;
	bool should_resend;
	mutex lock;
	typedef unique_lock<mutex> unique_lock;
	typedef lock_guard<mutex> lock_guard;
	op_callback onack, oncommit;
	uint64_t ontimeout;
	uint32_t acks, commits;
	error_code rc;
	epoch_t *reply_epoch;
	bool finished;
	ZTracer::Trace trace;

	Op(const oid_t& o, const AVolRef& _volume,
	   unique_ptr<ObjOp>& _op, int f,
	   op_callback&& ac, op_callback&& co,
	   ZTracer::Trace *parent) :
	  oid(o), volume(_volume), op(move(_op)), flags(f),
	  paused(false), priority(0), ontimeout(0), acks(0), commits(0),
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
	bool operator==(const Op& r) {
	  return tid == r.tid;
	}
	bool operator!=(const Op& r) {
	  return tid != r.tid;
	}

	bool operator>(const Op& r) {
	  return tid > r.tid;
	}
	bool operator<(const Op& r) {
	  return tid < r.tid;
	}
	bool operator>=(const Op& r) {
	  return tid >= r.tid;
	}
	bool operator<=(const Op& r) {
	  return tid <= r.tid;
	}
      };

      // -- osd sessions --
      struct OSDSession : public set_base_hook<link_mode<normal_link>>,
			  public RefCountedObject {
	shared_timed_mutex lock;
	typedef unique_lock<shared_timed_mutex> unique_lock;
	typedef shared_lock<shared_timed_mutex> shared_lock;

	static constexpr const uint32_t max_ops_inflight = 5;
	set<SubOp> subops_inflight;

	static constexpr const uint64_t max_ops_queued = 100;
	set<SubOp> subops_queued;

	int osd;
	int incarnation;
	int num_locks;
	ConnectionRef con;

	OSDSession(int o) :
	  osd(o), incarnation(0), con(nullptr) { }
	~OSDSession();

	bool operator==(const OSDSession& r) {
	  return osd == r.osd;
	}
	bool operator!=(const OSDSession& r) {
	  return osd != r.osd;
	}

	bool operator>(const OSDSession& r) {
	  return osd > r.osd;
	}
	bool operator<(const OSDSession& r) {
	  return osd < r.osd;
	}
	bool operator>=(const OSDSession& r) {
	  return osd >= r.osd;
	}
	bool operator<=(const OSDSession &r) {
	  return osd <= r.osd;
	}
      };
      set<OSDSession> osd_sessions;


      struct Op_Map_Latest {
	Objecter& objecter;
	ceph_tid_t tid;
	version_t latest;
	Op_Map_Latest(Objecter& o, ceph_tid_t t) : objecter(o), tid(t),
						   latest(0) {}
	void operator()(error_code r,
			version_t newest,
			version_t oldest);
      };

      struct StatfsOp : public set_base_hook<link_mode<normal_link>> {
	ceph_tid_t tid;
	statfs_callback onfinish;
	uint64_t ontimeout;
	mono_time last_submit;

	bool operator==(const StatfsOp& r) {
	  return tid == r.tid;
	}
	bool operator!=(const StatfsOp& r) {
	  return tid != r.tid;
	}

	bool operator>(const StatfsOp& r) {
	  return tid > r.tid;
	}
	bool operator<(const StatfsOp& r) {
	  return tid < r.tid;
	}
	bool operator>=(const StatfsOp& r) {
	  return tid >= r.tid;
	}
	bool operator<=(const StatfsOp& r) {
	  return tid <= r.tid;
	}
      };

    protected:
      set<Op> inflight_ops;
      set<StatfsOp> statfs_ops;

      static constexpr const uint32_t max_homeless_subops = 500;
      set<SubOp> homeless_subops;

      // ops waiting for an osdmap with a new volume or confirmation
      // that the volume does not exist (may be expanded to other uses
      // later)
      map<ceph_tid_t, Op*> check_latest_map_ops;

      map<epoch_t, FunQueue<void()>> waiting_for_map;

      timespan mon_timeout, osd_timeout;

      MOSDOp* _prepare_osd_subop(SubOp& op);
      void _cancel_op(Op& op);
      void _finish_subop(SubOp& subop);
      void _finish_op(Op& op, typename Op::unique_lock& ol); // Releases ol

      enum target_result {
	TARGET_NO_ACTION = 0,
	TARGET_NEED_RESEND,
	TARGET_VOLUME_DNE
      };
      bool osdmap_full_flag() const;
      bool target_should_be_paused(Op& op);

      int _calc_targets(Op& t, typename Op::unique_lock& ol);

      void _check_op_volume_dne(Op& op, typename Op::unique_lock& ol);
      void _send_op_map_check(Op& op);
      void _op_cancel_map_check(Op& op);

    public:
      Objecter(CephContext* _cct, timespan mon_timeout = 0s,
	       timespan osd_timeout = 0s) :
	cct(_cct),
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

      void _scan_requests(set<SubOp>& subops,
			  shunique_lock& sl,
			  bool force_resend,
			  bool force_resend_writes,
			  map<ceph_tid_t, SubOp*>& need_resend);
      // messages
    public:

      void handle_osd_subop_reply(MOSDOpReply *m);
      bool possibly_complete_op(Op& op, typename Op::unique_lock& ol,
				bool do_or_die = false);
      void handle_osd_map(MOSDMap *m);
      void wait_for_osd_map();

      // The function supplied is called with no lock. If it wants to do
      // something with the OSDMap, it can call with_osdmap on a
      // captured objecter.
      void add_osdmap_notifier(thunk&& f);

    protected:

      // low-level
      ceph_tid_t _op_submit(Op& op, typename Op::unique_lock& ol,
			    shunique_lock& sl);
      // public interface
    public:
      ceph_tid_t op_submit(Op *op);
      bool is_active() {
	return !(inflight_ops.empty() && statfs_ops.empty());
      }

      int get_client_incarnation() const { return client_inc; }
      void set_client_incarnation(int inc) { client_inc = inc; }

      // wait for epoch; true if we already have it
      bool wait_for_map(epoch_t epoch, thunk&& f);
      void _wait_for_new_map(thunk&& f, epoch_t e);
      void wait_for_latest_osdmap(thunk&& fin);
      void get_latest_version(epoch_t oldest, epoch_t neweset, thunk&& fin);
      void _get_latest_version(epoch_t oldest, epoch_t neweset, thunk&& fin);

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
      void op_cancel(ceph_tid_t tid,
		     error_code r = make_error_code(
		       errc::operation_canceled));


    public:
      // mid-level helpers
      Op *prepare_mutate_op(const oid_t& oid,
			    const AVolRef& volume,
			    unique_ptr<ObjOp>& op,
			    real_time mtime,
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
			unique_ptr<ObjOp>& op,
			real_time mtime, op_callback&& onack,
			op_callback&& oncommit,
			ZTracer::Trace *trace = nullptr) {
	Op *o = prepare_mutate_op(oid, volume, op, mtime, move(onack),
				  move(oncommit), trace);
	return op_submit(o);
      }

      void mutate(const oid_t& oid,
		  const AVolRef& volume,
		  unique_ptr<ObjOp>& op,
		  ZTracer::Trace *trace = nullptr) {
	auto mtime = real_clock::now();
	CB_Waiter w;
	mutate(oid, volume, op, mtime, w, nullptr, trace);
	w.wait();
      }

      Op *prepare_read_op(const oid_t& oid,
			  const AVolRef& volume,
			  unique_ptr<ObjOp>& op,
			  op_callback&& onack,
			  ZTracer::Trace *trace = nullptr) {
	Op *o = new Op(oid, volume, op,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       move(onack),
		       nullptr, trace);
	return o;
      }
      ceph_tid_t read(const oid_t& oid, const AVolRef& volume,
		      unique_ptr<ObjOp>& op, op_callback&& onack,
		      ZTracer::Trace *trace = nullptr) {
	Op *o = prepare_read_op(oid, volume, op, move(onack), trace);
	return op_submit(o);
      }

      void read(const oid_t& oid, const AVolRef& volume,
		unique_ptr<ObjOp>& op,
		ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;

	read(oid, volume, op, w, trace);

	w.wait();
      }

      ceph_tid_t stat(const oid_t& oid,
		      const AVolRef& volume,
		      stat_callback&& cb,
		      ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->stat(move(cb));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       nullptr, 0, trace);
	return op_submit(o);
      }

      tuple<uint64_t, real_time> stat(
	const oid_t& oid, const AVolRef& volume,
	ZTracer::Trace *trace = nullptr) {
	Stat_Waiter w;
	stat(oid, volume, w, trace);

	return w.wait();
      }

      ceph_tid_t read(const oid_t& oid,
		      const AVolRef& volume,
		      uint64_t off, uint64_t len,
		      read_callback&& onfinish,
		      ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->read(off, len, move(onfinish));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       nullptr, 0, trace);
	return op_submit(o);
      }

      bufferlist read(const oid_t& oid,
		      const AVolRef& volume,
		      uint64_t off, uint64_t len,
		      ZTracer::Trace *trace = nullptr) {
	Read_Waiter w;
	read(oid, volume, off, len, w, trace);

	return w.wait();
      }

      ceph_tid_t read_trunc(const oid_t& oid,
			    const AVolRef& volume,
			    uint64_t off, uint64_t len,
			    uint64_t trunc_size, uint32_t trunc_seq,
			    read_callback&& onfinish,
			    ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->read(off, len, trunc_size, trunc_seq, move(onfinish));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       nullptr, 0, trace);
	return op_submit(o);
      }

      bufferlist read_trunc(const oid_t& oid,
			    const AVolRef& volume,
			    uint64_t off, uint64_t len,
			    uint64_t trunc_size, uint32_t trunc_seq,
			    ZTracer::Trace *trace = nullptr) {
	Read_Waiter w;
	read_trunc(oid, volume, off, len, trunc_size, trunc_seq,
		   w, trace);

	return w.wait();
      }

      ceph_tid_t getxattr(const oid_t& oid,
			  const AVolRef& volume,
			  const string& name,
			  read_callback&& onfinish,
			  ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->getxattr(name, move(onfinish));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       nullptr, nullptr, trace);
	return op_submit(o);
      }

      bufferlist getxattr(const oid_t& oid,
			  const AVolRef& volume,
			  const string& name,
			  ZTracer::Trace *trace = nullptr) {
	Read_Waiter w;
	getxattr(oid, volume, name, w, trace);

	return w.wait();
      }

      ceph_tid_t getxattrs(const oid_t& oid,
			   const AVolRef& volume,
			   keyval_callback&& kvl,
			   op_callback&& onfinish,
			   ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->getxattrs(move(kvl));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       move(onfinish), 0, trace);
	return op_submit(o);
      }

      template<typename OutputIterator>
      auto getxattrs(const oid_t& oid, const AVolRef& volume,
		     OutputIterator i, op_callback&& onfinish,
		     ZTracer::Trace *trace = nullptr)
	-> enable_if_t<is_iterator<OutputIterator>::value,
	ceph_tid_t> {
	return getxattrs(oid, volume,
			 [i](error_code e,
			     string& s,
			     bufferlist& b) mutable {
			   *i = make_pair(s, move(b));
			   ++i;
			 }, move(onfinish), trace);

      }

      ceph_tid_t getxattrs(const oid_t& oid,
			   const AVolRef& volume,
			   map<string, bufferlist>& attrset,
			   op_callback&& onfinish,
			   ZTracer::Trace *trace = nullptr) {

	return getxattrs(oid, volume,
			 inserter(attrset, attrset.begin()),
			 move(onfinish),
			 trace);
      }

      map<string, bufferlist> getxattrs(const oid_t& oid,
					const AVolRef& volume,
					ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	map<string, bufferlist> attrset;
	getxattrs(oid, volume, attrset, w, trace);
	w.wait();
	return attrset;
      }

      ceph_tid_t read_full(const oid_t& oid,
			   const AVolRef& volume,
			   read_callback&& onfinish,
			   ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->read_full(move(onfinish));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       nullptr, 0, trace);
	return op_submit(o);
      }

      bufferlist read_full(const oid_t& oid,
			   const AVolRef& volume,
			   ZTracer::Trace *trace = nullptr) {
	Read_Waiter w;
	read_full(oid, volume, w, trace);
	return w.wait();
      }

      // writes
      ceph_tid_t _modify(const oid_t& oid,
			 const AVolRef& volume,
			 unique_ptr<ObjOp>& ops,
			 op_callback&& onack, op_callback&& oncommit,
			 ZTracer::Trace *trace = nullptr) {
	Op *o = new Op(oid, volume, ops, global_op_flags |
		       CEPH_OSD_FLAG_WRITE, move(onack),
		       move(oncommit), trace);
	return op_submit(o);
      }

      void _modify(const oid_t& oid,
		   const AVolRef& volume,
		   unique_ptr<ObjOp>& ops,
		   ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	_modify(oid, volume, ops, nullptr, w, trace);

	w.wait();
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

      void write(const oid_t& oid,
		 const AVolRef& volume,
		 uint64_t off, uint64_t len, const bufferlist& bl,
		 ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	write(oid, volume, off, len, bl, nullptr, w, trace);

	w.wait();
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

      void append(const oid_t& oid,
		  const AVolRef& volume,
		  uint64_t len, const bufferlist &bl,
		  ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;

	append(oid, volume, len, bl, nullptr, w, trace);

	w.wait();
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

      void write_trunc(const oid_t& oid,
		       const AVolRef& volume,
		       uint64_t off, uint64_t len, const bufferlist &bl,
		       uint64_t trunc_size,
		       uint32_t trunc_seq,
		       ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	write_trunc(oid, volume, off, len, bl, trunc_size, trunc_seq,
		    nullptr, w, trace);

	w.wait();
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

      void write_full(const oid_t& oid,
		      const AVolRef& volume,
		      const bufferlist &bl,
		      ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	write_full(oid, volume, bl, nullptr, w, trace);

	w.wait();
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

      void trunc(const oid_t& oid,
		 const AVolRef& volume,
		 uint64_t trunc_size,
		 uint32_t trunc_seq = 0,
		 ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	trunc(oid, volume, trunc_size, trunc_seq, nullptr, w, trace);

	w.wait();
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

      void zero(const oid_t& oid, const AVolRef& volume,
		uint64_t off, uint64_t len, ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	zero(oid, volume, off, len, nullptr, w, trace);

	w.wait();
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

      void create(const oid_t& oid,
		  const AVolRef& volume,
		  int create_flags,
		  ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	create(oid, volume, create_flags, nullptr, w, trace);

	w.wait();
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

      void remove(const oid_t& oid, const AVolRef& volume,
		  ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	remove(oid, volume, nullptr, w, trace);
	w.wait();
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

      void setxattr(const oid_t& oid, const AVolRef& volume,
		    const string& name, const bufferlist &bl,
		    ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	setxattr(oid, volume, name, bl, nullptr, w, trace);

	w.wait();
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

      void removexattr(const oid_t& oid,
		       const AVolRef& volume,
		       const string& name, ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	removexattr(oid, volume, name, nullptr, w, trace);

	w.wait();
      }

      ceph_tid_t omap_get_header(const oid_t& oid,
				 const AVolRef& volume,
				 read_callback&& cb,
				 ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_get_header(move(cb));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       nullptr, nullptr, trace);
	return op_submit(o);
      }

      bufferlist omap_get_header(const oid_t& oid,
				 const AVolRef& volume,
				 ZTracer::Trace *trace = nullptr) {
	Read_Waiter w;
	omap_get_header(oid, volume, w, trace);
	return w.wait();
      }

      ceph_tid_t omap_set_header(const oid_t& oid, const AVolRef& volume,
				 const bufferlist& bl, op_callback&& onack,
				 op_callback&& oncommit,
				 ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_set_header(bl);
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_WRITE,
		       move(onack), move(oncommit), trace);
	return op_submit(o);
      }

      void omap_set_header(const oid_t& oid,
			   const AVolRef& volume,
			   const bufferlist& bl,
			   ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	omap_set_header(oid, volume, bl, nullptr, w, trace);
	w.wait();
      }

      ceph_tid_t omap_get_vals(const oid_t& oid, const AVolRef& volume,
			       const string& start_after,
			       const string& filter_prefix,
			       uint64_t max_to_get,
			       keyval_callback&& kv,
			       op_callback&& onack,
			       ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_get_vals(start_after, filter_prefix, max_to_get, move(kv));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       move(onack), nullptr, trace);
	return op_submit(o);
      }

      template<typename OutputIterator>
      auto omap_get_vals(const oid_t& oid, const AVolRef& volume,
			 const string& start_after,
			 const string& filter_prefix,
			 uint64_t max_to_get, OutputIterator i,
			 op_callback&& onack, ZTracer::Trace *trace = nullptr)
	-> enable_if_t<is_iterator<OutputIterator>::value,
	ceph_tid_t> {
	return omap_get_vals(oid, volume, start_after, filter_prefix,
			     max_to_get,
			     [i](error_code e,
				 string& s,
				 bufferlist& b) mutable {
			       *i = make_pair(s, move(b));
			       ++i;
			     }, move(onack), trace);
      }

      ceph_tid_t omap_get_vals(const oid_t& oid, const AVolRef& volume,
			       const string& start_after,
			       const string& filter_prefix,
			       uint64_t max_to_get,
			       map<string, bufferlist>& attrset,
			       op_callback&& onack,
			       ZTracer::Trace *trace = nullptr) {
	return omap_get_vals(oid, volume, start_after, filter_prefix,
			     max_to_get,
			     inserter(attrset, attrset.begin()),
			     move(onack), trace);
      }

      map<string, bufferlist> omap_get_vals(
	const oid_t& oid, const AVolRef& volume,
	const string& start_after, const string& filter_prefix,
	uint64_t max_to_get, ZTracer::Trace *trace = nullptr) {
	map<string, bufferlist> attrset;
	CB_Waiter w;
	omap_get_vals(oid, volume, start_after, filter_prefix, max_to_get,
		      attrset, w, trace);
	w.wait();
	return attrset;
      }

      template<typename InputIterator>
      ceph_tid_t omap_get_vals_by_keys(const oid_t& oid, const AVolRef& volume,
				       InputIterator begin, InputIterator end,
				       keyval_callback&& kv,
				       op_callback&& onfinish,
				       ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_get_vals_by_keys(begin, end, move(kv));
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_READ,
		       move(onfinish), 0, trace);
	return op_submit(o);
      }

      template<typename InputIterator, typename OutputIterator>
      auto omap_get_vals_by_keys(const oid_t& oid, const AVolRef& volume,
				 InputIterator begin, InputIterator end,
				 OutputIterator i, op_callback&& onfinish,
				 ZTracer::Trace *trace = nullptr)
	-> enable_if_t<is_iterator<OutputIterator>::value,
	ceph_tid_t> {
	return omap_get_vals_by_keys(oid, volume, begin, end,
				     [i](error_code e,
					 string& s,
					 bufferlist& b) mutable {
				       *i = make_pair(s, move(b));
				       ++i;
				     }, move(onfinish), trace);
      }

      template<typename InputIterator>
      ceph_tid_t omap_get_vals_by_keys(const oid_t& oid, const AVolRef& volume,
				       InputIterator begin, InputIterator end,
				       map<string, bufferlist>& attrset,
				       op_callback&& onfinish,
				       ZTracer::Trace *trace = nullptr) {
	return omap_get_vals_by_keys(oid, volume, begin, end,
				     inserter(attrset, attrset.begin()),
				     onfinish, trace);
      }

      ceph_tid_t omap_get_vals_by_keys(const oid_t& oid, const AVolRef& volume,
				       const set<string>& keys,
				       map<string, bufferlist>& attrset,
				       op_callback&& onfinish,
				       ZTracer::Trace *trace = nullptr) {
	return omap_get_vals_by_keys(oid, volume, keys.begin(), keys.end(),
				     inserter(attrset, attrset.begin()),
				     move(onfinish), trace);
      }

      map<string,bufferlist> omap_get_vals_by_keys(
	const oid_t& oid, const AVolRef& volume,
	const set<string> &to_get, ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	map<string,bufferlist> out_set;
	omap_get_vals_by_keys(oid, volume, to_get, out_set, w, trace);
	w.wait();
	return out_set;
      }

      template<typename InputIterator>
      ceph_tid_t omap_set(const oid_t& oid, const AVolRef& volume,
			  InputIterator begin, InputIterator end,
			  op_callback&& onack,
			  op_callback&& oncommit,
			  ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_set(begin, end);
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_WRITE,
		       move(onack), move(oncommit), trace);
	return op_submit(o);
      }

      ceph_tid_t omap_set(const oid_t& oid, const AVolRef& volume,
			  const map<string, bufferlist>& attrset,
			  op_callback&& onack, op_callback&& oncommit,
			  ZTracer::Trace *trace = nullptr) {
	return omap_set(oid, volume,
			attrset.cbegin(), attrset.cend(),
			move(onack), move(oncommit),
			trace);
      }

      void omap_set(const oid_t& oid, const AVolRef& volume,
		    const map<string, bufferlist>& map,
		    ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	omap_set(oid, volume, map, nullptr, w, trace);
	w.wait();
      }

      template<typename InputIterator>
      ceph_tid_t omap_rm_keys(const oid_t& oid, const AVolRef& volume,
			      InputIterator begin, InputIterator end,
			      op_callback&& onack,
			      op_callback&& oncommit,
			      ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_rm_keys(begin, end);
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_WRITE,
		       move(onack), move(oncommit), trace);
	return op_submit(o);
      }

      template<typename InputIterator>
      void omap_rm_keys(const oid_t& oid, const AVolRef& volume,
			InputIterator begin, InputIterator end,
			ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	omap_rm_keys(oid, volume, begin, end,
		     nullptr, w, trace);
	w.wait();
      }

      ceph_tid_t omap_rm_keys(const oid_t& oid, const AVolRef& volume,
			      const set<string>& keys,
			      op_callback&& onack,
			      op_callback&& oncommit,
			      ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops = volume->op();
	ops->omap_rm_keys(keys.begin(), keys.end());
	Op *o = new Op(oid, volume, ops,
		       global_op_flags | CEPH_OSD_FLAG_WRITE,
		       move(onack), move(oncommit), trace);
	return op_submit(o);
      }

      void omap_rm_keys(const oid_t& oid, const AVolRef& volume,
			const set<string>& keys,
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

      void set_alloc_hint(const oid_t& oid, const AVolRef& volume,
			  uint64_t expected_object_size,
			  uint64_t expected_write_size,
			  ZTracer::Trace *trace = nullptr) {
	CB_Waiter w;
	set_alloc_hint(oid, volume, expected_object_size, expected_write_size,
		       nullptr, w, trace);
	w.wait();
      }

      ceph_tid_t call(const oid_t& oid, const AVolRef& vol,
		      const string& cname, const string& method,
		      bufferlist &indata, op_callback&& onack,
		      op_callback&& oncommit,
		      ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops(vol->op());
	ops->call(cname, method, indata, nullptr);
	Op *o = new Op(oid, vol, ops,
		       global_op_flags | CEPH_OSD_FLAG_WRITE,
		       move(onack), move(oncommit), trace);
	return op_submit(o);
      }

      ceph_tid_t call(const oid_t& oid, const AVolRef& vol,
		      const string& cname, const string& method,
		      bufferlist &indata, read_callback&& onack,
		      op_callback&& oncommit,
		      ZTracer::Trace *trace = nullptr) {
	ObjectOperation ops(vol->op());
	ops->call(cname, method, indata, move(onack));
	Op *o = new Op(oid, vol, ops,
		       global_op_flags | CEPH_OSD_FLAG_WRITE,
		       nullptr, move(oncommit), trace);
	return op_submit(o);
      }

      bufferlist call(const oid_t& oid, const AVolRef& vol,
		      const string& cname, const string& method,
		      bufferlist &indata,
		      ZTracer::Trace *trace = nullptr) {
	Read_Waiter w;
	call(oid, vol, cname, method, indata, w, nullptr, trace);
	return w.wait();
      }

      void create_volume(const string& name, op_callback&& onfinish);
      void delete_volume(const string& name, op_callback&& onfinish);

      void create_volume(const string& name) {
	CB_Waiter w;
	create_volume(name, w);
	w.wait();
      }
      void delete_volume(const string& name) {
	CB_Waiter w;
	delete_volume(name, w);
	w.wait();
      }

      // ---------------------------
      // df stats
    public:
      void handle_fs_stats_reply(MStatfsReply *m);
      void get_fs_stats(statfs_callback&& onfinish);
      void statfs_op_cancel(ceph_tid_t tid,
			    error_code r = make_error_code(
			      errc::operation_canceled));
      void _finish_statfs_op(StatfsOp& op);

      void blacklist_self(bool set);
      Volume vol_by_uuid(const uuid& id);
      AVolRef attach_by_uuid(const uuid& id);
      Volume vol_by_name(const string& name);
      AVolRef attach_by_name(const string& name);
    };

    template<typename D>
    void Objecter<D>::start() {
      shared_lock rl(rwlock);

      schedule_tick();
      if (osdmap->get_epoch() == 0) {
	_maybe_request_map();
      }
    }

    template<typename D>
    void Objecter<D>::shutdown() {
      while (osd_sessions.empty()) {
	auto p = osd_sessions.begin();
	close_session(*p);
      }

      while(!check_latest_map_ops.empty()) {
	auto i = check_latest_map_ops.begin();
	i->second->put();
	check_latest_map_ops.erase(i->first);
      }

      while (!statfs_ops.empty()) {
	auto i = statfs_ops.begin();
	auto r = &(*i);
	statfs_ops.erase(i);
	delete r;
      }

      while(!homeless_subops.empty()) {
	auto i = homeless_subops.begin();
	ldout(cct, 10) << " op " << i->tid << dendl;
	{
	  unique_lock wl(rwlock);
	  i->session = nullptr;
	  homeless_subops.erase(i);
	}
      }

      if (tick_event) {
	if (timer.cancel_event(tick_event))
	  tick_event = 0;
      }
    }

    template<typename D>
    void Objecter<D>::_scan_requests(
      set<SubOp>& subops,
      shunique_lock& sl,
      bool force_resend, bool force_resend_writes,
      map<ceph_tid_t, Objecter<D>::SubOp*>& need_resend) {
      assert(sl.owns_lock());
      // Check for changed request mappings
      auto p = subops.begin();
      while (p != subops.end()) {
	SubOp& subop = *p;
	// check_op_volume_dne() may touch ops; prevent iterator invalidation
	++p;
	ldout(cct, 10) << " checking op " << subop.tid << dendl;
	typename Op::unique_lock ol(subop.parent.lock);
	int r = _calc_targets(subop.parent, ol);
	switch (r) {
	case TARGET_NO_ACTION:
	  if (!force_resend &&
	      (!force_resend_writes ||
	       !(subop.parent.flags & CEPH_OSD_FLAG_WRITE)))
	    break;
	case TARGET_NEED_RESEND:
	  if (subop.session) {
	    static_cast<D*>(this)->_session_subop_remove(*subop.session,
							 subop);
	  }
	  need_resend[subop.tid] = &subop;
	  _op_cancel_map_check(subop.parent);
	  break;
	case TARGET_VOLUME_DNE:
	  _check_op_volume_dne(subop.parent, ol);
	  break;
	}
	ol.unlock();
      }
    }

    template<typename D>
    void Objecter<D>::handle_osd_map(MOSDMap *m) {
      shunique_lock shl(rwlock, acquire_unique);
      auto& monc = static_cast<D*>(this)->monc;

      assert(osdmap);

      if (m->fsid != monc->get_fsid()) {
	ldout(cct, 0) << "handle_osd_map fsid " << m->fsid
		      << " != " << monc->get_fsid() << dendl;
	return;
      }

      bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
      bool was_full = osdmap_full_flag();
      bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || was_full;

      map<ceph_tid_t, SubOp*> need_resend;

      if (m->get_last() <= osdmap->get_epoch()) {
	ldout(cct, 3) << "handle_osd_map ignoring epochs ["
		      << m->get_first() << "," << m->get_last()
		      << "] <= " << osdmap->get_epoch() << dendl;
      } else {
	ldout(cct, 3) << "handle_osd_map got epochs ["
		      << m->get_first() << "," << m->get_last()
		      << "] > " << osdmap->get_epoch()
		      << dendl;

	if (osdmap->get_epoch()) {
	  bool skipped_map = false;
	  // we want incrementals
	  for (epoch_t e = osdmap->get_epoch() + 1;
	       e <= m->get_last();
	       e++) {

	    if (osdmap->get_epoch() == e-1 &&
		m->incremental_maps.count(e)) {
	      ldout(cct, 3) << "handle_osd_map decoding incremental epoch "
			    << e << dendl;
	      OSDMap::Incremental inc(m->incremental_maps[e]);
	      osdmap->apply_incremental(inc);
	    } else if (m->maps.count(e)) {
	      ldout(cct, 3) << "handle_osd_map decoding full epoch "
			    << e << dendl;
	      osdmap->decode(m->maps[e]);
	    } else {
	      if (e && e > m->get_oldest()) {
		ldout(cct, 3) << "handle_osd_map requesting missing epoch "
			      << osdmap->get_epoch()+1 << dendl;
		_maybe_request_map();
		break;
	      }
	      ldout(cct, 3) << "handle_osd_map missing epoch "
			    << osdmap->get_epoch()+1
			    << ", jumping to " << m->get_oldest() << dendl;
	      e = m->get_oldest() - 1;
	      skipped_map = true;
	      continue;
	    }

	    was_full = was_full || osdmap_full_flag();
	    _scan_requests(homeless_subops, shl, skipped_map, was_full,
			   need_resend);

	    // osd addr changes?
	    for (auto p = osd_sessions.begin(); p != osd_sessions.end(); ) {
	      OSDSession& s = *p;
	      shunique_lock sl(s.lock, acquire_unique);
	      _scan_requests(s.subops_inflight, sl, skipped_map, was_full,
			     need_resend);
	      sl.unlock();
	      ++p;
	      if (!osdmap->is_up(s.osd) ||
		  (s.con &&
		   s.con->get_peer_addr() != osdmap->get_inst(s.osd).addr)) {
		close_session(s);
	      }
	    }

	    assert(e == osdmap->get_epoch());
	  }
	} else {
	  // first map.  we want the full thing.
	  if (m->maps.count(m->get_last())) {
	    for (auto& s : osd_sessions) {
	      shunique_lock sl(s.lock, acquire_unique);
	      _scan_requests(s.subops_inflight, sl, false, false, need_resend);
	      sl.unlock();
	    }
	    ldout(cct, 3) << "handle_osd_map decoding full epoch "
			  << m->get_last() << dendl;
	    osdmap->decode(m->maps[m->get_last()]);
	    _scan_requests(homeless_subops, shl, false, false, need_resend);

	  } else {
	    ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting"
			  << dendl;
	    monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	    monc->renew_subs();
	  }
	}
      }

      bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
      bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)
	|| osdmap_full_flag();

      // was/is paused?
      if (was_pauserd || was_pausewr || pauserd || pausewr) {
	_maybe_request_map();
      }

      // resend requests
      for (auto& kv : need_resend) {
	SubOp* subop = kv.second;
	OSDSession* s = subop->session;
	bool mapped_session = false;
	if (!s) {
	  typename Op::unique_lock ol(subop->parent.lock);
	  s = _map_session(*subop, ol, shl);
	  ol.unlock();
	  mapped_session = true;
	} else {
	  get_session(*s);
	}
	typename OSDSession::unique_lock sl(s->lock);
	if (mapped_session) {
	  _session_subop_assign(*s, *subop);
	}
	if (subop->parent.should_resend) {
	  if (!subop->is_homeless() && !subop->parent.paused) {
	    _send_subop(*subop);
	  }
	} else {
	  _cancel_op(subop->parent);
	}
	sl.unlock();
	put_session(*s);
      }

      // We could switch to a read lock here, perhaps add a flag, mutex,
      // and condition variable to block future updates. Probably not
      // worth it.

      // finish any functions that were waiting on a map update
      auto p = waiting_for_map.begin();
      while (p != waiting_for_map.end() &&
	     p->first <= osdmap->get_epoch()) {
	p->second.execute();
	waiting_for_map.erase(p++);
      }

      monc->sub_got("osdmap", osdmap->get_epoch());

      if (!waiting_for_map.empty()) {
	_maybe_request_map();
      }

      // Unlock before calling notifiers
      shl.unlock();

      for(const auto& f : osdmap_notifiers)
	f();
    }

    template<typename D>
    void Objecter<D>::Op_Map_Latest::operator()(error_code r,
						version_t newest,
						version_t oldest) {
      if (r == errc::resource_unavailable_try_again ||
	  r == errc::operation_canceled)
	return;
      else if (r)
	// Nothing else should be possible
	abort();

      lgeneric_subdout(objecter.cct, objecter, 10)
	<< "op_map_latest r=" << r << " tid=" << tid
	<< " latest " << newest << dendl;

      Objecter::unique_lock wl(objecter.rwlock);

      auto iter = objecter.check_latest_map_ops.find(tid);
      if (iter == objecter.check_latest_map_ops.end()) {
	lgeneric_subdout(objecter.cct, objecter, 10)
	  << "op_map_latest op " << tid << " not found" << dendl;
	return;
      }

      Op *op = iter->second;
      objecter.check_latest_map_ops.erase(iter);

      lgeneric_subdout(objecter.cct, objecter, 20)
	<< "op_map_latest op " << op << dendl;

      if (op->map_dne_bound == 0)
	op->map_dne_bound = latest;

      typename Op::unique_lock ol(op->lock);
      objecter._check_op_volume_dne(*op, ol);
      ol.unlock();

      op->put();
    }

    template<typename D>
    void Objecter<D>::_check_op_volume_dne(Op& op,
					   typename Op::unique_lock& ol) {
      ldout(cct, 10) << "check_op_volume_dne tid " << op.tid
		     << " current " << osdmap->get_epoch()
		     << " map_dne_bound " << op.map_dne_bound
		     << dendl;
      if (op.map_dne_bound > 0) {
	if (osdmap->get_epoch() >= op.map_dne_bound) {
	  // we had a new enough map
	  ldout(cct, 10) << "check_op_volume_dne tid " << op.tid
			 << " concluding volume " << op.volume << " dne"
			 << dendl;
	  if (op.onack) {
	    op.onack(vol_errc::no_such_volume);
	    op.onack = nullptr;
	  }
	  if (op.oncommit) {
	    op.oncommit(vol_errc::no_such_volume);
	    op.oncommit = nullptr;
	  }

	  _finish_op(op, ol);
	}
      } else {
	_send_op_map_check(op);
      }
    }

    template<typename D>
    void Objecter<D>::_send_op_map_check(Op& op) {
      auto& monc = static_cast<D*>(this)->monc;
      // ask the monitor
      if (check_latest_map_ops.count(op.tid) == 0) {
	op.get();
	check_latest_map_ops[op.tid] = &op;
	monc->get_version("osdmap", Op_Map_Latest(*this, op.tid));
      }
    }

    template<typename D>
    void Objecter<D>::_op_cancel_map_check(Op& op) {
      auto iter = check_latest_map_ops.find(op.tid);
      if (iter != check_latest_map_ops.end()) {
	Op *op = iter->second;
	op->put();
	check_latest_map_ops.erase(iter);
      }
    }

    // A very simple class, but since we don't need anything more
    // involved for RBD, I won't spend the time trying to WRITE anything
    // more involved for RBD.

    class Flusher {
      struct flush_queue;
      friend flush_queue;
      flush_queue* fq;
      mutex lock;
      typedef lock_guard<mutex> lock_guard;
      typedef unique_lock<mutex> unique_lock;

      struct BatchComplete : public slist_base_hook<link_mode<normal_link>> {
	flush_queue& fq;
	op_callback cb;

	BatchComplete(flush_queue& _fq,
		      op_callback _cb) : fq(_fq), cb(_cb) {}

	void operator()(error_code r) {
	  (fq)(r, *this);
	  if (cb)
	    cb(r);
	}
      };

      struct flush_queue {
	friend Flusher;
	Flusher& f;
	bool flushed;
	error_code r;
	slist<BatchComplete> completions;
	op_callback cb;
	condition_variable c;
	flush_queue(Flusher& _f) : f(_f), flushed(false) { }
	~flush_queue() {
	  lock_guard l(f.lock);
	  for (auto& c : completions) {
	    c(make_error_code(errc::operation_canceled));
	  }
	}

	void flush(unique_lock& l) {
	  assert(l.owns_lock());
	  assert(!completions.empty());
	  flushed = true;
	  c.wait(l, [this](){ return completions.empty(); });
	  error_code _r = r;
	  delete this;
	  if (_r)
	    throw system_error(_r);
	}

	void flush(op_callback&& _cb) {
	  assert(!completions.empty());
	  flushed = true;
	  cb.swap(_cb);
	}

	void operator()(error_code _r, BatchComplete& b) {
	  unique_lock l(f.lock);
	  // Our caller holds the lock.
	  if (!r) {
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
	return ref(*cb);
      }

      void flush() {
	// Returns 0 if no writes have failed since the last flush.
	unique_lock l(lock);

	if (fq->completions.empty()) {
	  error_code r = fq->r;
	  fq->r.clear();
	  if (r)
	    throw system_error(r);
	}

	flush_queue* oq = fq;
	fq = new flush_queue(*this);
	return oq->flush(l);
      }

      void flush(op_callback&& cb) {
	unique_lock l(lock);

	if (fq->completions.empty()) {
	  error_code r = fq->r;
	  fq->r.clear();
	  l.unlock();
	  cb(r);
	}

	flush_queue* oq = fq;
	fq = new flush_queue(*this);
	oq->flush(move(cb));
      }
    };
  };

  using rados_detail::Flusher;
};

#endif
