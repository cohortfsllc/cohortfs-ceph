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


#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "

namespace rados {
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
    OSDSession *s = new OSDSession(osd);
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

    CB_Waiter w;
    waiting_for_map[0].add(std::ref(w));
    w.wait();
  }

  void Objecter::add_osdmap_notifier(thunk&& f) {
    osdmap_notifiers.emplace_back(std::move(f));
  }

  struct Objecter_GetVersion {
    Objecter& objecter;
    thunk fin;
    Objecter_GetVersion(Objecter& o, thunk&& f)
      : objecter(o), fin(f) {}
    void operator()(std::error_code r, version_t newest, version_t oldest) {
      if (!r) {
	objecter.get_latest_version(oldest, newest, std::move(fin));
      } else if (r == std::errc::resource_unavailable_try_again) {
	// try again as instructed
	objecter.wait_for_latest_osdmap(std::move(fin));
      } else {
	// it doesn't return any other error codes!
	abort();
      }
    }
  };

  void Objecter::wait_for_latest_osdmap(thunk&& fin)
  {
    ldout(cct, 10) << __func__ << dendl;
    monc->get_version("osdmap", Objecter_GetVersion(*this, std::move(fin)));
  }


  void Objecter::get_latest_version(epoch_t oldest, epoch_t newest,
				    thunk&& fin)
  {
    unique_lock wl(rwlock);
    _get_latest_version(oldest, newest, std::move(fin));
  }

  void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest,
				     thunk&& fin)
  {
    if (osdmap->get_epoch() >= newest) {
      ldout(cct, 10) << __func__ << " latest " << newest << ", have it"
		     << dendl;
      if (fin)
	fin();
      return;
    }

    ldout(cct, 10) << __func__ << " latest " << newest << ", waiting" << dendl;
    _wait_for_new_map(std::move(fin), newest);
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


  void Objecter::_wait_for_new_map(thunk&& c, epoch_t epoch)
  {
    waiting_for_map[epoch].add(std::move(c));
    _maybe_request_map();
  }

  bool Objecter::wait_for_map(epoch_t epoch, thunk&& c)
  {
    unique_lock wl(rwlock);
    if (osdmap->get_epoch() >= epoch) {
      return true;
    }
    _wait_for_new_map(std::move(c), epoch);
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
      monc->get_version("osdmap", Op_Map_Latest(*this, kv.second->tid));
    }
  }



  ceph_tid_t Objecter::op_submit(Op* op)
  {
    shunique_lock sl(rwlock, cohort::acquire_shared);
    Op::unique_lock ol(op->lock);
    op->tid = ++last_tid;
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

  void Objecter::op_cancel(ceph_tid_t tid, std::error_code r)
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
      throw std::system_error(vol_errc::no_such_volume);
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
      if (op.f) {
	op.f(std::make_error_code(std::errc::timed_out), bl);
	op.f = nullptr;
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

    std::error_code rc = m->get_result();
    if (rc == std::errc::resource_unavailable_try_again) {
      ldout(cct, 7) << " got " << rc << ", resubmitting" << dendl;

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
      if (subop.ops[i].f) {
	try {
	  subop.ops[i].f(out_ops[i].rval, out_ops[i].outdata);
	} catch (const std::system_error& e) {
	  if (!out_ops[i].rval)
	    out_ops[i].rval = e.code();
	  if (!rc)
	    rc = e.code();
	}
      }
      subop.ops[i].f = nullptr;
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

    if (!subop.parent.rc)
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

  void Objecter::get_fs_stats(statfs_callback&& onfinish)
  {
    ldout(cct, 10) << "get_fs_stats" << dendl;
    unique_lock l(rwlock);

    StatfsOp* op = new StatfsOp;
    op->tid = ++last_tid;
    op->onfinish.swap(onfinish);
    op->ontimeout = 0;
    if (mon_timeout > 0ns) {
      op->ontimeout =
	timer.add_event(
	  mon_timeout,
	  &Objecter::statfs_op_cancel, this, op->tid,
	  std::make_error_code(std::errc::timed_out));
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
      op.onfinish(std::error_code(), m->h.st);
      _finish_statfs_op(op);
    } else {
      ldout(cct, 10) << "unknown request " << tid << dendl;
    }
    m->put();
    ldout(cct, 10) << "done" << dendl;
  }

  void Objecter::statfs_op_cancel(ceph_tid_t tid, std::error_code r)
  {
    unique_lock wl(rwlock);

    auto it = statfs_ops.find(tid, oc);
    if (it == statfs_ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    }

    ldout(cct, 10) << __func__ << " tid " << tid << dendl;

    StatfsOp& op = *it;
    ceph_statfs s;
    if (op.onfinish)
      op.onfinish(r, s);
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

  void Objecter::create_volume(const string& name, op_callback&& onfinish)
  {
    ldout(cct, 10) << "create_volume name=" << name << dendl;

    if (osdmap->vol_exists(name) > 0)
      onfinish(vol_errc::exists);

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd volume create\", ");
    cmd.push_back("\"volumeName\":\"" + name + "\"");

    bufferlist bl;
    monc->start_mon_command(
      cmd, bl,
      [onfinish = std::move(onfinish)](std::error_code err, const string& s,
				       bufferlist& bl) mutable {
	onfinish(err); });
  }

  void Objecter::delete_volume(const string& name, op_callback&& onfinish)
  {
    ldout(cct, 10) << "delete_volume name=" << name << dendl;

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd volume delete\", ");
    cmd.push_back("\"volumeName\":\"" + name + "\"");

    bufferlist bl;
    monc->start_mon_command(
      cmd, bl,
      [onfinish = std::move(onfinish)](std::error_code err, const string& s,
				       bufferlist& bl) mutable {
	onfinish(err); });
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
