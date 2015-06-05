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

#include <algorithm>
#include "Objecter.h"
#include "osd/OSDMap.h"

#include "mon/MonClient.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MOSDFailure.h"
#include "messages/MMonCommand.h"

#include <errno.h>

#include "common/config.h"
#include "include/str_list.h"
#include "common/errno.h"


#define dout_subsys ceph_subsys_objecter
#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "

namespace rados {
  using ceph::Formatter;
  using ceph::new_formatter;
  using std::all_of;
  using std::count_if;
  using boost::intrusive::set;
  using std::shared_timed_mutex;
  using std::mem_fun_ref;
  using std::not1;

  struct ObjComp {
    bool operator()(const int osd, const Objecter::OSDSession& c) const {
      return osd < c.osd;
    }

    bool operator()(const Objecter::OSDSession& c, const int osd) const {
      return c.osd < osd;
    }

    bool operator()(const ceph_tid_t tid, const Objecter::Op& b) const {
      return tid < b.tid;
    }

    bool operator()(const Objecter::Op& b, const ceph_tid_t tid) const {
      return b.tid < tid;
    }

    bool operator()(const ceph_tid_t tid, const Objecter::SubOp& s) const {
      return tid < s.tid;
    }

    bool operator()(const Objecter::SubOp& s, const ceph_tid_t tid) const {
      return s.tid < tid;
    }

    bool operator()(const ceph_tid_t tid, const Objecter::StatfsOp& s) const {
      return tid < s.tid;
    }

    bool operator()(const Objecter::StatfsOp& s, const ceph_tid_t tid) const {
      return s.tid < tid;
    }
  };

  ObjComp oc;

  // messages ------------------------------

  void Objecter::start()
  {
    shared_lock rl(rwlock);

    schedule_tick();
    if (osdmap->get_epoch() == 0) {
      _maybe_request_map();
    }
  }

  void Objecter::shutdown()
  {
    unique_lock wl(rwlock);

    while (!osd_sessions.empty()) {
      auto p = osd_sessions.begin();
      close_session(*p);
    }

    while(!check_latest_map_ops.empty()) {
      map<ceph_tid_t, Op*>::iterator i = check_latest_map_ops.begin();
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

  bool Objecter::ms_dispatch(Message *m)
  {
    ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;

    switch (m->get_type()) {
      // these we exlusively handle
    case CEPH_MSG_OSD_OPREPLY:
      handle_osd_subop_reply(static_cast<MOSDOpReply*>(m));
      return true;

    case CEPH_MSG_STATFS_REPLY:
      handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
      return true;

    // these we give others a chance to inspect

    // MDS, OSD
    case CEPH_MSG_OSD_MAP:
      handle_osd_map(static_cast<MOSDMap*>(m));
      return true;
    }
    return false;
  }

  void Objecter::_scan_requests(set<SubOp>& subops,
				shunique_lock& sl,
				bool force_resend,
				bool force_resend_writes,
				map<ceph_tid_t, SubOp*>& need_resend)
  {
    assert(sl.owns_lock());
    // Check for changed request mappings
    auto p = subops.begin();
    while (p != subops.end()) {
      SubOp& subop = *p;
      // check_op_volume_dne() may touch ops; prevent iterator invalidation
      ++p;
      ldout(cct, 10) << " checking op " << subop.tid << dendl;
      Op::unique_lock ol(subop.parent.lock);
      int r = _calc_targets(subop.parent, ol);
      switch (r) {
      case TARGET_NO_ACTION:
	if (!force_resend &&
	    (!force_resend_writes ||
	     !(subop.parent.flags & CEPH_OSD_FLAG_WRITE)))
	  break;
      case TARGET_NEED_RESEND:
	if (subop.session) {
	  _session_subop_remove(*subop.session, subop);
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

  void Objecter::handle_osd_map(MOSDMap *m)
  {
    shunique_lock shl(rwlock, cohort::acquire_unique);

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
	    ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e
			  << dendl;
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
	    shunique_lock sl(s.lock, cohort::acquire_unique);
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
	    shunique_lock sl(s.lock, cohort::acquire_unique);
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
	Op::unique_lock ol(subop->parent.lock);
	s = _map_session(*subop, ol, shl);
	ol.unlock();
	mapped_session = true;
      } else {
	get_session(*s);
      }
      OSDSession::unique_lock sl(s->lock);
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

    // finish any Contexts that were waiting on a map update
    map<epoch_t,list< pair< Context*, int > > >::iterator p =
      waiting_for_map.begin();
    while (p != waiting_for_map.end() &&
	   p->first <= osdmap->get_epoch()) {
      //go through the list and call the onfinish methods
      for (list<pair<Context*, int> >::iterator i = p->second.begin();
	   i != p->second.end(); ++i) {
	i->first->complete(i->second);
      }
      waiting_for_map.erase(p++);
    }

    monc->sub_got("osdmap", osdmap->get_epoch());

    if (!waiting_for_map.empty()) {
      _maybe_request_map();
    }

    // Unlock before calling notifiers
    shl.unlock();

    for (const auto& f : osdmap_notifiers) {
      f();
    }
  }

  // op volume check

  void Objecter::C_Op_Map_Latest::finish(int r)
  {
    if (r == -EAGAIN || r == -ECANCELED)
      return;

    lgeneric_subdout(objecter->cct, objecter, 10)
      << "op_map_latest r=" << r << " tid=" << tid
      << " latest " << latest << dendl;

    Objecter::unique_lock wl(objecter->rwlock);

    map<ceph_tid_t, Op*>::iterator iter =
      objecter->check_latest_map_ops.find(tid);
    if (iter == objecter->check_latest_map_ops.end()) {
      lgeneric_subdout(objecter->cct, objecter, 10)
	<< "op_map_latest op " << tid << " not found" << dendl;
      return;
    }

    Op *op = iter->second;
    objecter->check_latest_map_ops.erase(iter);

    lgeneric_subdout(objecter->cct, objecter, 20)
      << "op_map_latest op " << op << dendl;

    if (op->map_dne_bound == 0)
      op->map_dne_bound = latest;

    Op::unique_lock ol(op->lock);
    objecter->_check_op_volume_dne(*op, ol);
    ol.unlock();

    op->put();
  }

  void Objecter::_check_op_volume_dne(Op& op, Op::unique_lock& ol)
  {
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
	  op.onack(-ENXIO);
	  op.onack = nullptr;
	}
	if (op.oncommit) {
	  op.oncommit(-ENXIO);
	  op.oncommit = nullptr;
	}

	_finish_op(op, ol);
      }
    } else {
      _send_op_map_check(op);
    }
  }

  void Objecter::_send_op_map_check(Op& op)
  {
    // ask the monitor
    if (check_latest_map_ops.count(op.tid) == 0) {
      op.get();
      check_latest_map_ops[op.tid] = &op;
      C_Op_Map_Latest *c = new C_Op_Map_Latest(this, op.tid);
      monc->get_version("osdmap", &c->latest, NULL, c);
    }
  }

  void Objecter::_op_cancel_map_check(Op& op)
  {
    map<ceph_tid_t, Op*>::iterator iter =
      check_latest_map_ops.find(op.tid);
    if (iter != check_latest_map_ops.end()) {
      Op *op = iter->second;
      op->put();
      check_latest_map_ops.erase(iter);
    }
  }

  /**
   * Look up OSDSession by OSD id.
   *
   * @returns 0 on success, or -EAGAIN if we need a unique_lock
   */
  Objecter::OSDSession* Objecter::_get_session(int osd,
					       const shunique_lock& shl)
  {
    if (osd < 0) {
      ldout(cct, 20) << __func__ << " osd=" << osd << ". Refusign to "
		     << "put up with this." << dendl;
      throw std::system_error(errc::invalid_osd);
    }

    auto p = osd_sessions.find(osd, oc);
    if (p != osd_sessions.end()) {
      OSDSession& s = *p;
      s.get();
      return &s;
    }
    if (!shl.owns_lock()) {
      throw std::system_error(errc::need_unique_lock);
    }
    OSDSession *s = new OSDSession(cct, osd);
    osd_sessions.insert(*s);
    s->con = messenger->get_connection(osdmap->get_inst(osd));
    s->get();
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		   << s->nref << dendl;
    return s;
  }

  void Objecter::put_session(OSDSession& s)
  {
    s.put();
  }

  void Objecter::get_session(OSDSession& s)
  {
    s.get();
  }

  void Objecter::_reopen_session(OSDSession& s)
  {
    entity_inst_t inst = osdmap->get_inst(s.osd);
    ldout(cct, 10) << "reopen_session osd." << s.osd
		   << " session, addr now " << inst << dendl;
    if (s.con) {
      messenger->mark_down(s.con);
    }
    s.con = messenger->get_connection(inst);
    s.incarnation++;
  }

  void Objecter::close_session(OSDSession& s)
  {
    ldout(cct, 10) << "close_session for osd." << s.osd << dendl;
    if (s.con) {
      messenger->mark_down(s.con);
    }
    OSDSession::unique_lock sl(s.lock);

    std::list<SubOp*> homeless_ops;

    while (!s.subops_inflight.empty()) {
      auto& so = *(s.subops_inflight.begin());
      ldout(cct, 10) << " op " << so.tid << dendl;
      _session_subop_remove(s, so);
      homeless_ops.push_back(&so);
    }

    osd_sessions.erase(s);
    sl.unlock();
    put_session(s);

    // Assign any leftover ops to the homeless session
    {
      OSDSession::unique_lock wl(rwlock);
      for (auto i : homeless_ops) {
	homeless_subops.insert(*i);
      }
    }
  }

  void Objecter::wait_for_osd_map()
  {
    unique_lock wl(rwlock);
    if (osdmap->get_epoch()) {
      wl.unlock();
      return;
    }

    std::mutex lock;
    std::condition_variable cond;
    bool done;
    std::unique_lock<std::mutex> l(lock);
    C_SafeCond *context = new C_SafeCond(&lock, &cond, &done, NULL);
    waiting_for_map[0].push_back(pair<Context*, int>(context, 0));
    wl.unlock();
    cond.wait(l, [&](){ return done; });
    l.unlock();
  }

  void Objecter::add_osdmap_notifier(const cohort::function<void()>& f) {
    osdmap_notifiers.push_back(f);
  }

  struct C_Objecter_GetVersion : public Context {
    Objecter *objecter;
    uint64_t oldest, newest;
    Context *fin;
    C_Objecter_GetVersion(Objecter *o, Context *c)
      : objecter(o), oldest(0), newest(0), fin(c) {}
    void finish(int r) {
      if (r >= 0) {
	objecter->get_latest_version(oldest, newest, fin);
      } else if (r == -EAGAIN) { // try again as instructed
	objecter->wait_for_latest_osdmap(fin);
      } else {
	// it doesn't return any other error codes!
	assert(0);
      }
    }
  };

  void Objecter::wait_for_latest_osdmap(Context *fin)
  {
    ldout(cct, 10) << __func__ << dendl;
    C_Objecter_GetVersion *c = new C_Objecter_GetVersion(this, fin);
    monc->get_version("osdmap", &c->newest, &c->oldest, c);
  }


  void Objecter::get_latest_version(epoch_t oldest, epoch_t newest,
				    Context *fin)
  {
    unique_lock wl(rwlock);
    _get_latest_version(oldest, newest, fin);
  }

  void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest,
				     Context *fin)
  {
    if (osdmap->get_epoch() >= newest) {
      ldout(cct, 10) << __func__ << " latest " << newest << ", have it"
		     << dendl;
      if (fin)
	fin->complete(0);
      return;
    }

    ldout(cct, 10) << __func__ << " latest " << newest << ", waiting" << dendl;
    _wait_for_new_map(fin, newest, 0);
  }

  void Objecter::maybe_request_map()
  {
    shared_lock rl(rwlock);
    _maybe_request_map();
  }

  void Objecter::_maybe_request_map()
  {
    int flag = 0;
    if (osdmap_full_flag()) {
      ldout(cct, 10) << "_maybe_request_map subscribing (continuous) to "
		     << "next osd map (FULL flag is set)" << dendl;
    } else {
      ldout(cct, 10) << "_maybe_request_map subscribing (onetime) to next "
		     << "osd map" << dendl;
      flag = CEPH_SUBSCRIBE_ONETIME;
    }
    epoch_t epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
    if (monc->sub_want("osdmap", epoch, flag)) {
      monc->renew_subs();
    }
  }


  void Objecter::_wait_for_new_map(Context *c, epoch_t epoch, int err)
  {
    waiting_for_map[epoch].push_back(pair<Context *, int>(c, err));
    _maybe_request_map();
  }

  bool Objecter::wait_for_map(epoch_t epoch, Context *c, int err)
  {
    unique_lock wl(rwlock);
    if (osdmap->get_epoch() >= epoch) {
      return true;
    }
    _wait_for_new_map(c, epoch, err);
    return false;
  }

  void Objecter::kick_requests(OSDSession& session)
  {
    ldout(cct, 10) << "kick_requests for osd." << session.osd << dendl;

    unique_lock wl(rwlock);

    OSDSession::unique_lock sl(session.lock);
    _kick_requests(session);
    sl.unlock();
  }

  void Objecter::_kick_requests(OSDSession& session)
  {
    // resend ops
    map<ceph_tid_t,SubOp*> resend;  // resend in tid order

    for (auto &subop : session.subops_inflight) {
      if (subop.parent.should_resend) {
	if (!subop.parent.paused)
	  resend[subop.tid] = &subop;
      } else {
	_cancel_op(subop.parent); // TODO: WTF?
      }
    }
    while (!resend.empty()) {
      _send_subop(*resend.begin()->second);
      resend.erase(resend.begin());
    }
  }

  void Objecter::schedule_tick()
  {
    assert(tick_event == 0);
    tick_event = timer.add_event(
      cct->_conf->objecter_tick_interval,
      &Objecter::tick, this);
  }

  void Objecter::tick()
  {
    shared_lock rl(rwlock);

    ldout(cct, 10) << "tick" << dendl;

    // we are only called by C_Tick
    assert(tick_event);
    tick_event = 0;

    std::set<OSDSession*> toping;

    int r = 0;

    // look for laggy requests
    ceph::mono_time cutoff = ceph::mono_clock::now();
    cutoff -= cct->_conf->objecter_timeout;  // timeout

    unsigned laggy_ops;

    do {
      laggy_ops = 0;
      for (auto& s : osd_sessions) {
	for (auto& subop : s.subops_inflight) {
	  assert(subop.session);
	  if (subop.session && subop.stamp < cutoff) {
	    ldout(cct, 2) << " tid " << subop.tid << " on osd."
			  << subop.session->osd << " is laggy" << dendl;
	    toping.insert(subop.session);
	    ++laggy_ops;
	  }
	}
      }

      if (!homeless_subops.empty() || !toping.empty()) {
	_maybe_request_map();
      }
    } while (r == -EAGAIN);

    if (!toping.empty()) {
      // send a ping to these osds, to ensure we detect any session resets
      // (osd reply message policy is lossy)
      for (auto i = toping.begin();
	   i != toping.end();
	   ++i) {
	messenger->send_message(new MPing, (*i)->con);
      }
    }

    // reschedule
    tick_event = timer.reschedule_me(cct->_conf->objecter_tick_interval);
  }

  void Objecter::resend_mon_ops()
  {
    unique_lock wl(rwlock);

    ldout(cct, 10) << "resend_mon_ops" << dendl;

    for (auto& statfs_op : statfs_ops) {
      _fs_stats_submit(statfs_op);
    }

    for (auto kv : check_latest_map_ops) {
      C_Op_Map_Latest *c = new C_Op_Map_Latest(this, kv.second->tid);
      monc->get_version("osdmap", &c->latest, NULL, c);
    }
  }


// read | write ---------------------------

  ceph_tid_t Objecter::read_full(const oid_t& oid,
				 const AVolRef& volume,
				 bufferlist *pbl,
				 op_callback&& onfinish,
				 ZTracer::Trace *trace)
  {
    ObjectOperation ops = volume->op();
    ops->read_full(pbl);
    Op *o = new Op(oid, volume, ops,
		   global_op_flags | CEPH_OSD_FLAG_READ,
		   move(onfinish), 0, trace);
    return op_submit(o);
  }

  ceph_tid_t Objecter::op_submit(Op *op)
  {
    shunique_lock sl(rwlock, cohort::acquire_shared);
    Op::unique_lock ol(op->lock);
    return _op_submit(*op, ol, sl);
  }

  ceph_tid_t Objecter::_op_submit(Op& op, Op::unique_lock& ol,
				  shunique_lock& shl)
  {
    op.trace.keyval("tid", op.tid);
    op.trace.event("op_submit", &trace_endpoint);

    // pick target

    // This function no longer creates the session
    const bool check_for_latest_map = _calc_targets(op, ol)
      == TARGET_VOLUME_DNE;

    // Try to get sessions, including a retry if we need to take write lock
    for (auto& subop : op.subops) {
      OSDSession *session = nullptr;
      if (subop.tid == 0)
	subop.tid = ++last_tid;
      if (subop.osd == -1) {
	subop.session = nullptr;
	homeless_subops.insert(subop);
	continue;
      }
      try {
      session = _get_session(subop.osd, shl);
      } catch (std::system_error& e) {
	if (e.code() == errc::need_unique_lock) {
	  shl.unlock();
	  shl.lock();
	  session = _get_session(subop.osd, shl);
	} else {
	  throw;
	}
      }
      assert(session);  // may be homeless
      OSDSession::unique_lock sl(session->lock);
      _session_subop_assign(*session, subop);
      sl.unlock();
    }

    // We may need to take wlock if we will need to _set_op_map_check later.
    if (check_for_latest_map && shl.owns_lock_shared()) {
      shl.unlock();
      shl.lock();
    }

    inflight_ops.insert(op);

    // add to gather set(s)
    if (op.onack) {
      ++num_unacked;
    } else {
      ldout(cct, 20) << " note: not requesting ack" << dendl;
    }
    if (op.oncommit) {
      ++num_uncommitted;
    } else {
      ldout(cct, 20) << " note: not requesting commit" << dendl;
    }

    // send?
    ldout(cct, 10) << "_op_submit oid " << op.oid
		   << " " << op.volume << " "
		   << " tid " << op.tid << dendl;

    assert(op.flags & (CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_WRITE));

    bool need_send = false;

    if ((op.flags & CEPH_OSD_FLAG_WRITE) &&
	osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
      op.paused = true;
      _maybe_request_map();
    } else if ((op.flags & CEPH_OSD_FLAG_READ) &&
	       osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
      op.paused = true;
      _maybe_request_map();
    } else if ((op.flags & CEPH_OSD_FLAG_WRITE) &&
	       osdmap_full_flag()) {
      op.paused = true;
      _maybe_request_map();
    } else if (count_if(op.subops.cbegin(),
			op.subops.cend(),
			not1(mem_fun_ref(&SubOp::is_homeless)))
	       >= op.volume->quorum()) {
      need_send = true;
    } else {
      _maybe_request_map();
    }

    MOSDOp *m = NULL;
    for (auto& subop : op.subops) {
      if (need_send && !subop.is_homeless()) {
	OSDSession::shared_lock sl(subop.session->lock);
	m = _prepare_osd_subop(subop);
	_send_subop(subop, m);
	sl.unlock();
      }
      put_session(*subop.session);
    }

    // Last chance to touch Op here, after giving up session lock it can be
    // freed at any time by response handler.
    ceph_tid_t tid = op.tid;
    if (check_for_latest_map) {
      _send_op_map_check(op);
    }

    ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted
		  << " uncommitted" << dendl;

    return tid;
  }

  void Objecter::op_cancel(ceph_tid_t tid, int r)
  {
    Op* op;
    {
      shared_lock l(rwlock);

      auto p = inflight_ops.find(tid, oc);
      if (p == inflight_ops.end()) {
	ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
      }
      op = &(*p);
      _op_cancel_map_check(*op);
    }

    Op::unique_lock ol(op->lock);
    // Give it one last chance
    if (possibly_complete_op(*op, ol, true)) {
      return;
    }
    ldout(cct, 10) << __func__ << " tid " << tid << dendl;
    if (op->onack) {
      op->onack(r);
      op->onack = nullptr;
    }
    if (op->oncommit) {
      op->oncommit(r);
      op->oncommit = nullptr;
    }
    _finish_op(*op, ol);
  }

  bool Objecter::target_should_be_paused(Op& t)
  {
    bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
    bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) ||
      osdmap_full_flag();

    return (t.flags & CEPH_OSD_FLAG_READ && pauserd) ||
      (t.flags & CEPH_OSD_FLAG_WRITE && pausewr);
  }


  /**
   * Wrapper around osdmap->test_flag for special handling of the FULL flag.
   */
  bool Objecter::osdmap_full_flag() const
  {
    // Ignore the FULL flag if we are working on behalf of an MDS, in
    // order to permit MDS journal writes for file deletions.
    return osdmap->test_flag(CEPH_OSDMAP_FULL) &&
      (messenger->get_myname().type() != entity_name_t::TYPE_MDS);
  }

  int Objecter::_calc_targets(Op& t, Op::unique_lock& ol)
  {
    bool need_resend = false;

    if (!osdmap->vol_exists(t.volume->v.id)) {
      return TARGET_VOLUME_DNE;
    }

    // Volume should know how many OSDs to supply for its own
    // operations. But it never hurts to fail spectacularly in case I
    // goof up.
    uint32_t i = 0;
    t.volume->place(
      t.oid, *osdmap, [&](int osd) {
	if (likely((i < t.subops.size()) && !t.subops[i].done &&
		   (t.subops[i].osd != osd))) {
	  t.subops[i].osd = osd;
	  need_resend = true;
	}
	++i;
      });

    if (unlikely(i < t.subops.size())) {
      for (;i < t.subops.size(); ++i) {
	t.subops[i].osd = -1;
      }
    }

    bool paused = target_should_be_paused(t);
    if (!paused && paused != t.paused) {
      t.paused = false;
      need_resend = true;
    }

    if (need_resend) {
      return TARGET_NEED_RESEND;
    }
    return TARGET_NO_ACTION;
  }

  Objecter::OSDSession* Objecter::_map_session(SubOp& subop,
					       Op::unique_lock& ol,
					       const shunique_lock& shl)
  {
    int r = _calc_targets(subop.parent, ol);
    if (r == TARGET_VOLUME_DNE) {
      throw std::system_error(vol_err::no_such_volume);
    }
    return  _get_session(subop.osd, shl);
  }

  void Objecter::_session_subop_assign(OSDSession& to, SubOp& subop)
  {
    assert(subop.session == NULL);
    assert(subop.tid);

    get_session(to);
    subop.session = &to;
    to.subops_inflight.insert(subop);
  }

  void Objecter::_session_subop_remove(OSDSession& from, SubOp& subop)
  {
    assert(subop.session == &from);

    from.subops_inflight.erase(subop);
    put_session(from);
    subop.session = nullptr;

    ldout(cct, 15) << __func__ << " " << from.osd << " " << subop.tid << dendl;
  }

  void Objecter::_cancel_op(Op& op)
  {
    ldout(cct, 15) << "cancel_op " << op.tid << dendl;
    assert(!op.should_resend);
    Op::unique_lock ol(op.lock);
    op.onack = nullptr;
    op.oncommit = nullptr;
    _finish_op(op, ol);
  }

  void Objecter::_finish_subop(SubOp& subop)
  {
    for (auto& op : subop.ops) {
      bufferlist bl;
      if (op.out_bl)
	// So contexts depending ont his don't crash.
	*op.out_bl = bl;
      if (op.out_rval)
	*op.out_rval = -ETIMEDOUT;
      if (op.ctx) {
	op.ctx->complete(op.rval);
	op.ctx = nullptr;
      }
    }
    if (subop.session) {
      OSDSession::unique_lock wl(subop.session->lock);
      _session_subop_remove(*subop.session, subop);
    }
  }


  void Objecter::_finish_op(Op& op, Op::unique_lock& ol)
  {
    ldout(cct, 15) << "finish_op " << op.tid << dendl;

    op.finished = true;

    if (op.ontimeout) {
      timer.cancel_event(op.ontimeout);
    }

    for (auto& subop : op.subops) {
      _finish_subop(subop);
    }

    assert(check_latest_map_ops.find(op.tid) == check_latest_map_ops.end());

    op.trace.event("finish_op", &trace_endpoint);

    ol.unlock();

    {
      unique_lock l(rwlock);
      inflight_ops.erase(op);
    }

    op.put();
  }

  MOSDOp *Objecter::_prepare_osd_subop(SubOp& subop)
  {
    int flags = subop.parent.flags;
    subop.parent.trace.keyval("want onack", subop.parent.onack ? 1 : 0);
    subop.parent.trace.keyval("want oncommit", subop.parent.oncommit ? 1 : 0);
    subop.parent.trace.event("send_op", &trace_endpoint);
    if (subop.parent.oncommit)
      flags |= CEPH_OSD_FLAG_ONDISK;
    if (subop.parent.onack)
      flags |= CEPH_OSD_FLAG_ACK;

    subop.stamp = ceph::mono_clock::now();

    MOSDOp *m = new MOSDOp(client_inc, subop.tid,
			   subop.hoid, subop.parent.volume->v.id,
			   osdmap->get_epoch(),
			   flags);

    m->ops = subop.ops;
    m->set_mtime(subop.parent.mtime);
    m->set_retry_attempt(subop.attempts++);

    if (subop.replay_version != eversion_t())
      m->set_version(subop.replay_version);  // we're replaying this op!

    if (subop.parent.priority)
      m->set_priority(subop.parent.priority);
    else
      m->set_priority(cct->_conf->osd_client_op_priority);

    m->trace = subop.parent.trace;

    return m;
  }

  void Objecter::_send_subop(SubOp& subop, MOSDOp *m)
  {
    if (!m) {
      assert(subop.tid > 0);
      m = _prepare_osd_subop(subop);
    }

    ldout(cct, 15) << __func__ << " " << subop.tid << " to osd."
		   << subop.session->osd << " op " << subop.ops[0] << dendl;

    ConnectionRef con = subop.session->con;
    assert(con);

    subop.incarnation = subop.session->incarnation;

    m->set_tid(subop.tid);

    messenger->send_message(m, subop.session->con);
  }

  void Objecter::handle_osd_subop_reply(MOSDOpReply *m)
  {
    ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

    // get pio
    ceph_tid_t tid = m->get_tid();

    int osd_num = (int)m->get_source().num();

    shared_lock rl(rwlock);

    auto siter = osd_sessions.find(osd_num, oc);
    if (siter == osd_sessions.end()) {
      ldout(cct, 7) << "handle_osd_op_reply " << tid
		    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ?
						  " onnvram":" ack"))
		    << " ... unknown osd" << dendl;
      m->put();
      return;
    }

    OSDSession& s = *siter;
    get_session(s);
    rl.unlock();


    OSDSession::unique_lock sl(s.lock);

    auto iter = s.subops_inflight.find(tid, oc);
    if (iter == s.subops_inflight.end()) {
      ldout(cct, 7) << "handle_osd_subop_reply " << tid
		    << (m->is_ondisk() ? " ondisk" :
			(m->is_onnvram() ? " onnvram" : " ack"))
		    << " ... stray" << dendl;
      sl.unlock();
      put_session(s);
      m->put();
      return;
    }


    ldout(cct, 7) << "handle_osd_subop_reply " << tid
		  << (m->is_ondisk() ? " ondisk"
		      : (m->is_onnvram() ? " onnvram" : " ack"))
		  << " v " << m->get_replay_version() << " uv "
		  << m->get_user_version()
		  << " attempt " << m->get_retry_attempt()
		  << dendl;
    SubOp& subop = *iter;

    if (m->get_retry_attempt() >= 0) {
      if (m->get_retry_attempt() != (subop.attempts - 1)) {
	ldout(cct, 7) << " ignoring reply from attempt "
		      << m->get_retry_attempt()
		      << " from " << m->get_source_inst()
		      << "; last attempt " << (subop.attempts - 1)
		      << " sent to " << subop.session->con->get_peer_addr()
		      << dendl;
	m->put();
	sl.unlock();
	put_session(s);
	return;
      }
    } else {
      // we don't know the request attempt because the server is old,
      // so just accept this one.  we may do ACK callbacks we
      // shouldn't have, but that is better than doing callbacks out
      // of order.
    }

    int rc = m->get_result();
    if (rc == -EAGAIN) {
      ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;

      // new tid
      _session_subop_remove(s, subop);
      subop.tid = ++last_tid;
      _session_subop_assign(s, subop);

      _send_subop(subop);
      sl.unlock();
      put_session(s);
      m->put();
      return;
    }
    subop.parent.get();
    sl.unlock();
    Op::unique_lock ol(subop.parent.lock); // So we can't race with _finish_op
    if (subop.parent.finished) {
      ol.unlock();
      subop.parent.put();
      m->put();
      put_session(s);
      return;
    }

    // per-op result demuxing
    vector<OSDOp> out_ops;
    m->claim_ops(out_ops);

    if (out_ops.size() != subop.ops.size())
      ldout(cct, 0) << "WARNING: tid " << subop.tid << " reply ops " << out_ops
		    << " != request ops " << subop.ops
		    << " from " << m->get_source_inst() << dendl;

    for (uint32_t i = 0; i < out_ops.size(); ++i) {
      ldout(cct, 10) << " op " << i << " rval " << out_ops[i].rval
		     << " len " << out_ops[i].outdata.length() << dendl;
      if (subop.ops[i].out_bl)
	*subop.ops[i].out_bl = out_ops[i].outdata;

      // set rval before running handlers so that handlers
      // can change it if e.g. decoding fails
      if (subop.ops[i].out_rval)
	*subop.ops[i].out_rval = out_ops[i].rval;

      if (subop.ops[i].ctx) {
	ldout(cct, 10) << " op " << i << " handler " << subop.ops[i].ctx
		       << dendl;
	subop.ops[i].ctx->complete(out_ops[i].rval);
      }
      subop.ops[i].out_bl = nullptr;
      subop.ops[i].out_rval = nullptr;
      subop.ops[i].ctx = nullptr;
    }

    if (subop.parent.reply_epoch &&
	*subop.parent.reply_epoch < m->get_map_epoch())
      *subop.parent.reply_epoch = m->get_map_epoch();


    if (subop.parent.onack) {
      ldout(cct, 15) << "handle_osd_subop_reply ack" << dendl;
      subop.replay_version = m->get_replay_version();
      if (!subop.done)
	++subop.parent.acks; // Don't double count acks
    }

    if (subop.parent.oncommit && (m->is_ondisk() || rc)) {
      ldout(cct, 15) << "handle_osd_subop_reply safe" << dendl;
      ++subop.parent.commits; // Don't double count acks
    }

    if (subop.parent.rc == 0)
      subop.parent.rc = rc;

    subop.done = true;

    possibly_complete_op(subop.parent, ol);
    assert(!ol);
    subop.parent.put();
    m->put();
    put_session(s);
  }

  bool Objecter::possibly_complete_op(Op& op, Op::unique_lock& ol,
				      bool do_or_die)
  {
    assert(ol && ol.mutex() == &op.lock);

    if (op.finished) {
      // Someone else got here first, be happy.
      ol.unlock();
      return true;
    }

    uint32_t done = 0;
    uint32_t homeless = 0;
    uint32_t still_coming = 0;

    for (auto& subop : op.subops) {
      if (subop.done)
	done++;
      else if (subop.is_homeless())
	homeless++;
      else
	still_coming++;
    }

    if (!op.finished && done >= op.volume->quorum() &&
	(do_or_die || !still_coming)) {
      // ack|commit -> ack
      op_callback onack = nullptr;
      op_callback oncommit = nullptr;

      if (op.onack && op.acks >= op.volume->quorum()) {
	onack = move(op.onack);
	op.onack = nullptr;  // only do callback once
	assert(num_unacked > 0);
	num_unacked--;
	op.trace.event("onack", &trace_endpoint);
      }
      if (op.oncommit && op.commits >= op.volume->quorum()) {
	oncommit = move(op.oncommit);
	op.oncommit = nullptr;
	assert(num_uncommitted > 0);
	num_uncommitted--;
	op.trace.event("oncommit", &trace_endpoint);
      }

      // done with this tid?
      if (!op.onack && !op.oncommit) {
	ldout(cct, 15) << "handle_osd_op_reply completed tid "
		       << op.tid << dendl;
	_finish_op(op, ol); // This releases the lock.
      } else {
	ol.unlock();
      }

      ldout(cct, 5) << num_unacked << " unacked, "
		    << num_uncommitted << " uncommitted" << dendl;

      // do callbacks
      if (onack) {
	onack(op.rc);
      }
      if (oncommit) {
	oncommit(op.rc);
      }
      return true;
    } else {
      ol.unlock();
    }
    return false;
  }

  void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
  {
    ldout(cct, 10) << "get_fs_stats" << dendl;
    unique_lock l(rwlock);

    StatfsOp* op = new StatfsOp;
    op->tid = ++last_tid;
    op->stats = &result;
    op->onfinish = onfinish;
    op->ontimeout = 0;
    if (mon_timeout > 0ns) {
      op->ontimeout =
	timer.add_event(
	  mon_timeout,
	  &Objecter::statfs_op_cancel, this, op->tid, -ETIMEDOUT);
    }
    statfs_ops.insert(*op);

    _fs_stats_submit(*op);
  }

  void Objecter::_fs_stats_submit(StatfsOp& op)
  {
    ldout(cct, 10) << "fs_stats_submit" << op.tid << dendl;
    monc->send_mon_message(new MStatfs(monc->get_fsid(), op.tid, 0));
    op.last_submit = ceph::mono_clock::now();
  }

  void Objecter::handle_fs_stats_reply(MStatfsReply *m)
  {
    unique_lock wl(rwlock);

    ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
    ceph_tid_t tid = m->get_tid();

    auto stiter = statfs_ops.find(tid, oc);
    if (stiter != statfs_ops.end()) {
      StatfsOp& op = *stiter;
      ldout(cct, 10) << "have request " << tid << dendl;
      *(op.stats) = m->h.st;
      op.onfinish->complete(0);
      _finish_statfs_op(op);
    } else {
      ldout(cct, 10) << "unknown request " << tid << dendl;
    }
    m->put();
    ldout(cct, 10) << "done" << dendl;
  }

  void Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
  {
    unique_lock wl(rwlock);

    auto it = statfs_ops.find(tid, oc);
    if (it == statfs_ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    }

    ldout(cct, 10) << __func__ << " tid " << tid << dendl;

    StatfsOp& op = *it;
    if (op.onfinish)
      op.onfinish->complete(r);
    _finish_statfs_op(op);
  }

  void Objecter::_finish_statfs_op(StatfsOp& op)
  {
    statfs_ops.erase(op);

    if (op.ontimeout) {
      timer.cancel_event(op.ontimeout);
    }

    delete &op;
  }

  void Objecter::ms_handle_connect(Connection *con)
  {
    ldout(cct, 10) << "ms_handle_connect " << con << dendl;

    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
      resend_mon_ops();
  }

  bool Objecter::ms_handle_reset(Connection *con)
  {
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
      //
      int osd = osdmap->identify_osd(con->get_peer_addr());
      if (osd >= 0) {
	ldout(cct, 1) << "ms_handle_reset on osd." << osd << dendl;
	unique_lock wl(rwlock);

	auto p = osd_sessions.find(osd, oc);
	if (p != osd_sessions.end()) {
	  OSDSession& session = *p;
	  OSDSession::unique_lock sl(session.lock);
	  _reopen_session(session);
	  _kick_requests(session);
	  sl.unlock();
	  wl.unlock();
	  maybe_request_map();
	} else {
	  wl.unlock();
	}
      } else {
	ldout(cct, 10) << "ms_handle_reset on unknown osd addr "
		       << con->get_peer_addr() << dendl;
      }
      return true;
    }
    return false;
  }

  void Objecter::ms_handle_remote_reset(Connection *con)
  {
    /*
     * treat these the same.
     */
    ms_handle_reset(con);
  }

  bool Objecter::ms_get_authorizer(int dest_type,
				   AuthAuthorizer **authorizer,
				   bool force_new)
  {
    if (dest_type == CEPH_ENTITY_TYPE_MON)
      return true;
    *authorizer = monc->auth->build_authorizer(dest_type);
    return *authorizer != NULL;
  }

  void Objecter::blacklist_self(bool set)
  {
    ldout(cct, 10) << "blacklist_self " << (set ? "add" : "rm") << dendl;

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd blacklist\", ");
    if (set)
      cmd.push_back("\"blacklistop\":\"add\",");
    else
      cmd.push_back("\"blacklistop\":\"rm\",");
    stringstream ss;
    ss << messenger->get_myaddr();
    cmd.push_back("\"addr\":\"" + ss.str() + "\"");

    MMonCommand *m = new MMonCommand(monc->get_fsid());
    m->cmd = cmd;

    monc->send_mon_message(m);
  }

  int Objecter::create_volume(const string& name, op_callback&& onfinish)
  {
    ldout(cct, 10) << "create_volume name=" << name << dendl;

    if (osdmap->vol_exists(name) > 0)
      return -EEXIST;

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd volume create\", ");
    cmd.push_back("\"volumeName\":\"" + name + "\"");

    bufferlist bl;
    monc->start_mon_command(cmd, bl, nullptr, nullptr,
			    std::move(onfinish));
    return 0;
  }

  int Objecter::delete_volume(const string& name, op_callback&& onfinish)
  {
    ldout(cct, 10) << "delete_volume name=" << name << dendl;

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd volume delete\", ");
    cmd.push_back("\"volumeName\":\"" + name + "\"");

    bufferlist bl;
    monc->start_mon_command(cmd, bl, nullptr, nullptr,
			    std::move(onfinish));
    return 0;
  }

  Objecter::OSDSession::~OSDSession()
  {
    // Caller is responsible for re-assigning or
    // destroying any ops that were assigned to us
    assert(subops_inflight.empty());
  }

  Objecter::~Objecter()
  {
    delete osdmap;

    assert(homeless_subops.empty());

    assert(osd_sessions.empty());
    assert(statfs_ops.empty());
    assert(waiting_for_map.empty());
    assert(check_latest_map_ops.empty());

    assert(!tick_event);
  }

  Volume Objecter::vol_by_uuid(const boost::uuids::uuid& id) {
    shared_lock rl(rwlock);
    return osdmap->lookup_volume(id);
  }

  AVolRef Objecter::attach_by_uuid(const boost::uuids::uuid& id) {
    shared_lock rl(rwlock);
    return osdmap->lookup_volume(id).attach(cct, *osdmap);
  }

  Volume Objecter::vol_by_name(const string& name) {
    shared_lock rl(rwlock);
    return osdmap->lookup_volume(name);
  }

  AVolRef Objecter::attach_by_name(const string& name) {
    shared_lock rl(rwlock);
    return osdmap->lookup_volume(name).attach(cct, *osdmap);
  }

  Message* MessageFactory::create(int type)
  {
    switch (type) {
    case CEPH_MSG_OSD_MAP:      return new MOSDMap;
    case CEPH_MSG_OSD_OPREPLY:  return new MOSDOpReply;
    case CEPH_MSG_STATFS_REPLY: return new MStatfsReply;
    default: return parent ? parent->create(type) : nullptr;
    }
  }
};
