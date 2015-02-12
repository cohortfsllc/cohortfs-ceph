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

namespace OSDC {
  using ceph::Formatter;
  using ceph::new_formatter;
  using std::all_of;
  using std::count_if;

  struct ObjComp {
    bool operator()(const int osd, const Objecter::OSDSession& c) const {
      return osd < c.osd;
    }

    bool operator()(const Objecter::OSDSession& c, const int osd) const {
      return c.osd < osd;
    }

    bool operator()(const ceph_tid_t tid, const Objecter::op_base& b) const {
      return tid < b.tid;
    }

    bool operator()(const Objecter::op_base& b, const ceph_tid_t tid) const {
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

  // initialize only internal data structures, don't initiate cluster
  // interaction
  void Objecter::init()
  {
    assert(!initialized);

    unique_timer_lock tl(timer_lock);
    timer.init();
    tl.unlock();

    initialized = true;
  }

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
    assert(initialized);

    unique_lock wl(rwlock);

    initialized = false;

    while (!osd_sessions.empty()) {
      auto p = osd_sessions.begin();
      close_session(*p);
    }

    while (!check_latest_map_lingers.empty()) {
      map<uint64_t, LingerOp*>::iterator i = check_latest_map_lingers.begin();
      i->second->put();
      check_latest_map_lingers.erase(i->first);
    }

    while(!check_latest_map_ops.empty()) {
      map<ceph_tid_t, Op*>::iterator i = check_latest_map_ops.begin();
      i->second->put();
      check_latest_map_ops.erase(i->first);
    }

    while(!statfs_ops.empty()) {
      auto i = statfs_ops.begin();
      i->unlink();
      delete &(*i);
    }

#ifdef LINGER
    ldout(cct, 20) << __func__ << " clearing up homeless session..." << dendl;
    while (!homeless_session->linger_subops.empty()) {
      auto i = homeless_session->linger_subops.begin();
      ldout(cct, 10) << " linger_op " << i->tid << dendl;
      {
	RWLock::WLocker wl(homeless_session->lock);
	_session_linger_subop_remove(homeless_session, *i);
      }
      linger_ops.erase(i->second->linger_id);
      i->second->put();
    }
#endif

    while(!homeless_session->subops.empty()) {
      auto i = homeless_session->subops.begin();
      ldout(cct, 10) << " op " << i->tid << dendl;
      {
	unique_lock wl(homeless_session->lock);
	_session_subop_remove(*homeless_session, *i);
      }
    }

    if (tick_event) {
      unique_timer_lock l(timer_lock);
      if (timer.cancel_event(tick_event))
	tick_event = NULL;
    }

    {
      unique_timer_lock l(timer_lock);
      timer.shutdown(l);
    }
  }

  void Objecter::_send_linger(LingerOp& info)
  {
#ifdef LINGER
    assert(rwlock.is_wlocked());

    RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

    ldout(cct, 15) << "send_linger " << info->linger_id << dendl;
    vector<OSDOp> opv = info->ops; // need to pass a copy to ops
    Context *onack = (!info->registered && info->on_reg_ack) ?
      new C_Linger_Ack(this, info) : NULL;
    Context *oncommit = new C_Linger_Commit(this, info);
    Op *o = new Op(info->target.base_obj, info->target.base_oloc,
		   opv, info->target.flags | CEPH_OSD_FLAG_READ,
		   onack, oncommit,
		   info->pobjver);
    o->mtime = info->mtime;

    o->target = info->target;
    o->tid = last_tid.inc();

    // do not resend this; we will send a new op to reregister
    o->should_resend = false;

    if (info->register_tid) {
      // repeat send.  cancel old registeration op, if any.
      info->session->lock.get_write();
      if (info->session->ops.count(info->register_tid)) {
	Op *o = info->session->ops[info->register_tid];
	_op_cancel_map_check(o);
	_cancel_linger_op(o);
      }
      info->session->lock.unlock();

      info->register_tid = _op_submit(o, lc);
    } else {
      // first send
      info->register_tid = _op_submit_with_budget(o, lc);
    }
#endif // LINGER
  }

  void Objecter::_linger_ack(LingerOp& info, int r)
  {
#ifdef LINGER
    ldout(cct, 10) << "_linger_ack " << info->linger_id << dendl;
    if (info->on_reg_ack) {
      info->on_reg_ack->complete(r);
      info->on_reg_ack = NULL;
    }
#endif // LINGER
  }

  void Objecter::_linger_commit(LingerOp& info, int r)
  {
#ifdef LINGER
    ldout(cct, 10) << "_linger_commit " << info->linger_id << dendl;
    if (info->on_reg_commit) {
      info->on_reg_commit->complete(r);
      info->on_reg_commit = NULL;
    }

    // only tell the user the first time we do this
    info->registered = true;
    info->pobjver = NULL;
#endif // LINGER
  }

  void Objecter::unregister_linger(uint64_t linger_id)
  {
#ifdef LINGER
    RWLock::WLocker wl(rwlock);
    _unregister_linger(linger_id);
#endif // LINGER
  }

  void Objecter::_unregister_linger(uint64_t linger_id)
  {
#ifdef LINGER
    assert(rwlock.is_wlocked());
    ldout(cct, 20) << __func__ << " linger_id=" << linger_id << dendl;

    map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
    if (iter != linger_ops.end()) {
      LingerOp *info = iter->second;
      OSDSession *s = info->session;
      s->lock.get_write();
      _session_linger_op_remove(s, info);
      s->lock.unlock();

      linger_ops.erase(iter);
      info->canceled = true;
      info->put();

    }
#endif // LINGER
  }

  ceph_tid_t Objecter::linger_mutate(const oid& obj,
				     const shared_ptr<const Volume>& volume,
				     unique_ptr<ObjOp>& op,
				     ceph::real_time mtime,
				     bufferlist& inbl, int flags,
				     Context *onack, Context *oncommit,
				     version_t *objver)
  {
#ifdef LINGER
    LingerOp *info = new LingerOp;
    info->base_obj = obj;
    info->volume = volume;
    info->mtime = mtime;
    info->target.flags = flags | CEPH_OSD_FLAG_WRITE;
    info->ops = op.ops;
    info->inbl = inbl;
    info->poutbl = NULL;
    info->pobjver = objver;
    info->on_reg_ack = onack;
    info->on_reg_commit = oncommit;

    RWLock::WLocker wl(rwlock);
    _linger_submit(info);
    return info->linger_id;
#endif // LINGER
    return 0;
  }

  ceph_tid_t Objecter::linger_read(const oid& obj,
				   const shared_ptr<const Volume>& volume,
				   unique_ptr<ObjOp>& op,
				   bufferlist& inbl, bufferlist *poutbl,
				   int flags,
				   Context *onfinish,
				   version_t *objver)
  {
#ifdef LINGER
    LingerOp *info = new LingerOp;
    info->target.base_obj = obj;
    info->target.base_oloc = oloc;
    if (info->target.base_oloc.key == obj)
      info->target.base_oloc.key.clear();
    info->target.flags = flags | CEPH_OSD_FLAG_READ;
    info->ops = op.ops;
    info->inbl = inbl;
    info->poutbl = poutbl;
    info->pobjver = objver;
    info->on_reg_commit = onfinish;

    RWLock::WLocker wl(rwlock);
    _linger_submit(info);
    return info->linger_id;
#endif // LINGER
    return 0;
  }

  void Objecter::_linger_submit(LingerOp& info)
  {
#ifdef LINGER
    assert(rwlock.is_wlocked());
    RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

    // Acquire linger ID
    info->linger_id = ++max_linger_id;
    ldout(cct, 10) << __func__ << " info " << info
		   << " linger_id " << info->linger_id << dendl;
    linger_ops[info->linger_id] = info;

    // Populate Op::target
    OSDSession *s = NULL;
    _calc_target(&info->target);

    // Create LingerOp<->OSDSession relation
    int r = _get_session(info->target.osd, &s, lc);
    assert(r == 0);
    s->lock.get_write();
    _session_linger_op_assign(s, info);
    s->lock.unlock();
    put_session(s);

    _send_linger(info);
#endif /* LINGER */
  }

  bool Objecter::ms_dispatch(Message *m)
  {
    ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;
    if (!initialized)
      return false;

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

  void Objecter::_scan_requests(OSDSession& s,
				bool force_resend,
				bool force_resend_writes,
				map<ceph_tid_t, SubOp*>& need_resend,
				list<LingerOp*>& need_resend_linger,
				shunique_lock& shl)
  {
    list<uint64_t> unregister_lingers;

    OSDSession::unique_lock sl(s.lock);

#ifdef LINGER
    // check for changed linger mappings (_before_ regular ops)
    auto& lp = s.linger_ops.begin();
    while (lp != s.linger_ops.end()) {
      LingerOp *op = lp->second;
      assert(op->session == s);
      ++lp;   // check_linger_pool_dne() may touch linger_ops; prevent iterator invalidation
      ldout(cct, 10) << " checking linger op " << op->linger_id << dendl;
      bool unregister;
      int r = _recalc_linger_op_target(op, lc);
      switch (r) {
      case RECALC_OP_TARGET_NO_ACTION:
	if (!force_resend && !force_resend_writes)
	  break;
	// -- fall-thru --
      case RECALC_OP_TARGET_NEED_RESEND:
	need_resend_linger.push_back(op);
	_linger_cancel_map_check(op);
	break;
      case RECALC_OP_TARGET_VOLUME_DNE:
	_check_linger_pool_dne(op, &unregister);
	if (unregister) {
	  ldout(cct, 10) << " need to unregister linger op " << op->linger_id << dendl;
	  unregister_lingers.push_back(op->linger_id);
	}
	break;
      }
    }
#endif

    // Check for changed request mappings
    auto p = s.subops.begin();
    while (p != s.subops.end()) {
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

    sl.unlock();

    for (auto &linger : unregister_lingers) {
      _unregister_linger(linger);
    }
  }

  void Objecter::handle_osd_map(MOSDMap *m)
  {
    shunique_lock shl(rwlock, ceph::acquire_unique);
    if (!initialized)
      return;

    assert(osdmap);

    if (m->fsid != monc->get_fsid()) {
      ldout(cct, 0) << "handle_osd_map fsid " << m->fsid
		    << " != " << monc->get_fsid() << dendl;
      return;
    }

    bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
    bool was_full = osdmap_full_flag();
    bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || was_full;

    list<LingerOp*> need_resend_linger;
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
	  _scan_requests(*homeless_session, skipped_map, was_full,
			 need_resend, need_resend_linger, shl);

	  // osd addr changes?
	  for (auto p = osd_sessions.begin(); p != osd_sessions.end(); ) {
	    OSDSession& s = *p;
	    _scan_requests(s, skipped_map, was_full,
			   need_resend, need_resend_linger, shl);
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
	    _scan_requests(s, false, false, need_resend, need_resend_linger,
			   shl);
	  }
	  ldout(cct, 3) << "handle_osd_map decoding full epoch "
			<< m->get_last() << dendl;
	  osdmap->decode(m->maps[m->get_last()]);
	  _scan_requests(*homeless_session, false, false,
			 need_resend, need_resend_linger, shl);

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
	int r = _map_session(*subop, &s, ol, shl);
	ol.unlock();
	assert(r == 0);
	mapped_session = true;
      } else {
	get_session(*s);
      }
      OSDSession::unique_lock sl(s->lock);
      if (mapped_session) {
	_session_subop_assign(*s, *subop);
      }
      if (subop->parent.should_resend) {
	if (!subop->session->is_homeless() && !subop->parent.paused) {
	  _send_subop(*subop);
	}
      } else {
	_cancel_op(subop->parent);
      }
      sl.unlock();
      put_session(*s);
    }
#ifdef LINGER
    for (list<LingerOp*>::iterator p = need_resend_linger.begin();
	 p != need_resend_linger.end(); ++p) {
      LingerOp *op = *p;
      if (!op->session) {
	_calc_target(&op->target);
	OSDSession *s = NULL;
	int const r = _get_session(op->target.osd, &s, shl);
	assert(r == 0);
	assert(s != NULL);
	op->session = s;
	put_session(s);
      }
      if (!op->session->is_homeless()) {
	_send_linger(op);
      }
    }
#endif

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
	  op.onack->complete(-ENXIO);
	  op.onack = nullptr;
	}
	if (op.oncommit) {
	  op.oncommit->complete(-ENXIO);
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

  // linger pool check

  void Objecter::C_Linger_Map_Latest::finish(int r)
  {
    if (r == -EAGAIN || r == -ECANCELED) {
      // ignore callback; we will retry in resend_mon_ops()
      return;
    }

    Objecter::unique_lock wl(objecter->rwlock);

    map<uint64_t, LingerOp*>::iterator iter =
      objecter->check_latest_map_lingers.find(linger_id);
    if (iter == objecter->check_latest_map_lingers.end()) {
      return;
    }

    LingerOp *op = iter->second;
    objecter->check_latest_map_lingers.erase(iter);

    if (op->map_dne_bound == 0)
      op->map_dne_bound = latest;

    bool unregister;
    objecter->_check_linger_volume_dne(op, &unregister);

    if (unregister) {
      objecter->_unregister_linger(op->linger_id);
    }

    op->put();
  }

  void Objecter::_check_linger_volume_dne(LingerOp *op, bool *need_unregister)
  {
    *need_unregister = false;

    ldout(cct, 10) << "_check_linger_vol_dne linger_id " << op->linger_id
		   << " current " << osdmap->get_epoch()
		   << " map_dne_bound " << op->map_dne_bound
		   << dendl;
    if (op->map_dne_bound > 0) {
      if (osdmap->get_epoch() >= op->map_dne_bound) {
	if (op->on_reg_ack) {
	  op->on_reg_ack->complete(-ENOENT);
	}
	if (op->on_reg_commit) {
	  op->on_reg_commit->complete(-ENOENT);
	}
	*need_unregister = true;
      }
    } else {
      _send_linger_map_check(op);
    }
  }

  void Objecter::_send_linger_map_check(LingerOp *op)
  {
    // ask the monitor
    if (check_latest_map_lingers.count(op->linger_id) == 0) {
      op->get();
      check_latest_map_lingers[op->linger_id] = op;
      C_Linger_Map_Latest *c = new C_Linger_Map_Latest(this, op->linger_id);
      monc->get_version("osdmap", &c->latest, NULL, c);
    }
  }

  void Objecter::_linger_cancel_map_check(LingerOp *op)
  {
    map<uint64_t, LingerOp*>::iterator iter =
      check_latest_map_lingers.find(op->linger_id);
    if (iter != check_latest_map_lingers.end()) {
      LingerOp *op = iter->second;
      op->put();
      check_latest_map_lingers.erase(iter);
    }
  }

  /**
   * Look up OSDSession by OSD id.
   *
   * @returns 0 on success, or -EAGAIN if we need a unique_lock
   */
  int Objecter::_get_session(int osd, OSDSession **session,
			     const shunique_lock& shl)
  {
    if (osd < 0) {
      *session = homeless_session;
      ldout(cct, 20) << __func__ << " osd=" << osd << " returning homeless"
		     << dendl;
      return 0;
    }

    auto p = osd_sessions.find(osd, oc);
    if (p != osd_sessions.end()) {
      OSDSession& s = *p;
      s.get();
      *session = &s;
      return 0;
    }
    if (!shl.owns_lock()) {
      return -EAGAIN;
    }
    OSDSession *s = new OSDSession(cct, osd);
    osd_sessions.insert(*s);
    s->con = messenger->get_connection(osdmap->get_inst(osd));
    s->get();
    *session = s;
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		   << s->nref << dendl;
    return 0;
  }

  void Objecter::put_session(OSDSession& s)
  {
    if (!s.is_homeless()) {
      s.put();
    }
  }

  void Objecter::get_session(OSDSession& s)
  {
    if (!s.is_homeless()) {
      s.get();
    }
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

    std::list<SubOp*> homeless_lingers;
    std::list<SubOp*> homeless_ops;

    while(!s.linger_subops.empty()) {
      auto& l = *(s.linger_subops.begin());
      ldout(cct, 10) << " linger_op " << l.tid << dendl;
      _session_linger_subop_remove(s, l);
      homeless_lingers.push_back(&l);
    }

    while (!s.subops.empty()) {
      auto& so = *(s.subops.begin());
      ldout(cct, 10) << " op " << so.tid << dendl;
      _session_subop_remove(s, so);
      homeless_ops.push_back(&so);
    }

    osd_sessions.erase(s);
    sl.unlock();
    put_session(s);

    // Assign any leftover ops to the homeless session
    {
      OSDSession::unique_lock wl(homeless_session->lock);
#ifdef LINGER
      for (std::list<LingerOp*>::iterator i = homeless_lingers.begin();
	   i != homeless_lingers.end(); ++i) {
	_session_linger_subop_assign(homeless_session, *i);
      }
#endif // LINGER
      for (auto i : homeless_ops) {
	_session_subop_assign(*homeless_session, *i);
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

    map<uint64_t, LingerOp *> lresend;
    unique_lock wl(rwlock);

    OSDSession::unique_lock sl(session.lock);
    _kick_requests(session, lresend);
    sl.unlock();

    _linger_ops_resend(lresend);
  }

  void Objecter::_kick_requests(OSDSession& session,
				map<uint64_t, LingerOp *>& lresend)
  {
    // resend ops
    map<ceph_tid_t,SubOp*> resend;  // resend in tid order

    for (auto &subop : session.subops) {
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

#ifdef LINGER
    // resend lingers
    for (auto j = session->linger_ops.begin();
	 j != session->linger_ops.end(); ++j) {
      LingerOp *op = j->second;
      op->get();
      assert(lresend.count(j->first) == 0);
      lresend[j->first] = op;
    }
#endif
  }

  void Objecter::_linger_ops_resend(map<uint64_t, LingerOp *>& lresend)
  {
    while (!lresend.empty()) {
      LingerOp *op = lresend.begin()->second;
      if (!op->canceled) {
	_send_linger(*op);
      }
      op->put();
      lresend.erase(lresend.begin());
    }
  }

  void Objecter::schedule_tick()
  {
    unique_timer_lock l(timer_lock);
    assert(tick_event == NULL);
    tick_event = new C_Tick(this);
    timer.add_event_after(
      ceph::span_from_double(cct->_conf->objecter_tick_interval),
      tick_event);
  }

  void Objecter::tick()
  {
    shared_lock rl(rwlock);

    ldout(cct, 10) << "tick" << dendl;

    // we are only called by C_Tick
    assert(tick_event);
    tick_event = NULL;

    if (!initialized) {
      // we raced with shutdown
      return;
    }

    std::set<OSDSession*> toping;

    int r = 0;

    // look for laggy requests
    ceph::mono_time cutoff = ceph::mono_clock::now();
    cutoff -= ceph::span_from_double(cct->_conf->objecter_timeout);  // timeout

    unsigned laggy_ops;

    do {
      laggy_ops = 0;
      for (auto& s : osd_sessions) {
	for (auto& subop : s.subops) {
	  assert(subop.session);
	  if (subop.session && subop.stamp < cutoff) {
	    ldout(cct, 2) << " tid " << subop.tid << " on osd."
			  << subop.session->osd << " is laggy" << dendl;
	    toping.insert(subop.session);
	    ++laggy_ops;
	  }
	}
#ifdef LINGER
	for (map<uint64_t,LingerOp*>::iterator p = s->linger_ops.begin();
	     p != s->linger_ops.end();
	     ++p) {
	  LingerOp *op = p->second;
	  assert(op->session);
	  ldout(cct, 10) << " pinging osd that serves lingering tid "
			 << p->first << " (osd." << op->session->osd << ")"
			 << dendl;
	  toping.insert(op->session);
	}
#endif
      }

      if (!homeless_session->subops.empty() || !toping.empty()) {
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
    schedule_tick();
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

    for (map<uint64_t, LingerOp*>::iterator p = check_latest_map_lingers.begin();
	 p != check_latest_map_lingers.end();
	 ++p) {
      C_Linger_Map_Latest *c = new C_Linger_Map_Latest(this, p->second->linger_id);
      monc->get_version("osdmap", &c->latest, NULL, c);
    }
  }


// read | write ---------------------------

  ceph_tid_t Objecter::read_full(const oid& obj,
				 const shared_ptr<const Volume>& volume,
				 bufferlist *pbl, int flags,
				 Context *onfinish, version_t *objver,
				 const unique_ptr<ObjOp>& extra_ops,
				 ZTracer::Trace *trace)
  {
    unique_ptr<ObjOp> ops = init_ops(volume, extra_ops);
    ops->read_full(pbl);
    Op *o = new Op(obj, volume, ops,
      flags | global_op_flags | CEPH_OSD_FLAG_READ,
      onfinish, 0, objver, trace);
    return op_submit(o);
  }

  class C_CancelOp : public Context
  {
    ceph_tid_t tid;
    Objecter *objecter;
  public:
    C_CancelOp(ceph_tid_t t, Objecter *objecter)
      : tid(t), objecter(objecter) {}
    void finish(int r) {
      objecter->op_cancel(tid, -ETIMEDOUT);
    }
  };

  ceph_tid_t Objecter::op_submit(Op *op, int *ctx_budget)
  {
    shunique_lock sl(rwlock, ceph::acquire_shared);
    sl.lock_shared(); // We actually acquire this
    Op::unique_lock ol(op->lock);
    return _op_submit_with_budget(*op, ol, sl, ctx_budget);
  }

  ceph_tid_t Objecter::_op_submit_with_budget(Op& op, Op::unique_lock& ol,
					      shunique_lock& sl,
					      int *ctx_budget)
  {
    assert(initialized);

    // throttle.  before we look at any state, because
    // take_op_budget() may drop our lock while it blocks.
    if (!op.ctx_budgeted || (ctx_budget && (*ctx_budget == -1))) {
      int op_budget = _take_op_budget(op, sl);
      // take and pass out the budget for the first OP
      // in the context session
      if (ctx_budget && (*ctx_budget == -1)) {
	*ctx_budget = op_budget;
      }
    }

    ceph_tid_t tid = _op_submit(op, ol, sl);

    if (osd_timeout > ceph::timespan(0)) {
      unique_timer_lock l(timer_lock);
      op.ontimeout = new C_CancelOp(tid, this);
      timer.add_event_after(osd_timeout, op.ontimeout);
    }

    return tid;
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
      int r = _get_session(subop.osd, &session, shl);
      if (r == -EAGAIN) {
	assert(!session);
	shl.unlock();
	shl.lock();
	r = _get_session(subop.osd, &session, shl);
	assert(r == 0);
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
    ldout(cct, 10) << "_op_submit obj " << op.obj
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
			[](const SubOp& subop){
			  return !subop.session->is_homeless();})
	       >= op.volume->quorum()) {
      need_send = true;
    } else {
      _maybe_request_map();
    }

    MOSDOp *m = NULL;
    for (auto& subop : op.subops) {
      if (need_send && !subop.session->is_homeless()) {
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

  int Objecter::op_cancel(ceph_tid_t tid, int r)
  {
    assert(initialized);

    Op* op;
    {
      shared_lock l(rwlock);

      auto p = inflight_ops.find(tid, oc);
      if (p == inflight_ops.end()) {
	ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
	return -ENOENT;
      }
      op = &(*p);
      _op_cancel_map_check(*op);
    }

    Op::unique_lock ol(op->lock);
    // Give it one last chance
    if (possibly_complete_op(*op, ol, true)) {
      return 0;
    }
    ldout(cct, 10) << __func__ << " tid " << tid << dendl;
    if (op->onack) {
      op->onack->complete(r);
      op->onack = nullptr;
    }
    if (op->oncommit) {
      op->oncommit->complete(r);
      op->oncommit = nullptr;
    }
    _finish_op(*op, ol);
    return 0;
  }

  bool Objecter::target_should_be_paused(op_base& t)
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

  int Objecter::_calc_targets(op_base& t, Op::unique_lock& ol)
  {
    bool need_resend = false;

    if (!osdmap->vol_exists(t.volume->id)) {
      return TARGET_VOLUME_DNE;
    }

    // Volume should know how many OSDs to supply for its own
    // operations. But it never hurts to fail spectacularly in case I
    // goof up.
    uint32_t i = 0;
    t.volume->place(
      t.obj, *osdmap, [&](int osd) {
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

  int Objecter::_map_session(SubOp& subop, OSDSession **s,
			     Op::unique_lock& ol,
			     const shunique_lock& shl)
  {
    int r = _calc_targets(subop.parent, ol);
    if (r == TARGET_VOLUME_DNE) {
      return -ENXIO;
    }
    return _get_session(subop.osd, s, shl);
  }

  void Objecter::_session_subop_assign(OSDSession& to, SubOp& subop)
  {
    assert(subop.session == NULL);
    assert(subop.tid);

    get_session(to);
    subop.session = &to;
    to.subops.insert(subop);
  }

  void Objecter::_session_subop_remove(OSDSession& from, SubOp& subop)
  {
    assert(subop.session == &from);

    subop.unlink();
    put_session(from);
    subop.session = nullptr;

    ldout(cct, 15) << __func__ << " " << from.osd << " " << subop.tid << dendl;
  }

  void Objecter::_session_linger_subop_assign(OSDSession& to, SubOp& subop)
  {
    assert(subop.session == NULL);

    get_session(to);
    subop.session = &to;
    to.linger_subops.insert(subop);
  }

  void Objecter::_session_linger_subop_remove(OSDSession& from, SubOp& subop)
  {
    assert(&from == subop.session);

    from.linger_subops.erase(subop);
    put_session(from);
    subop.session = NULL;
  }

  int Objecter::_get_osd_session(int osd, shunique_lock& shl,
				 OSDSession **psession)
  {
    int r;
    do {
      r = _get_session(osd, psession, shl);
      if (r == -EAGAIN) {
	if (!_promote_lock_check_race(shl)) {
	  return r;
	}
      }
    } while (r == -EAGAIN);
    assert(r == 0);

    return 0;
  }

  int Objecter::_get_subop_target_session(SubOp& subop,
					  shunique_lock& shl,
					  OSDSession **session)
  {
    return _get_osd_session(subop.osd, shl, session);
  }

  // This is called pretty much precisely because it's not a
  // "promote". It's a bad name for it but that's what the ceph people
  // called it.
  bool Objecter::_promote_lock_check_race(shunique_lock& shl)
  {
    assert(shl.owns_lock());
    epoch_t epoch = osdmap->get_epoch();
    shl.unlock();
    shl.lock();
    return (epoch == osdmap->get_epoch());
  }

  int Objecter::_recalc_linger_op_targets(LingerOp& linger_op,
					  shunique_lock& shl)
  {
#ifdef LINGER
    assert(rwlock.is_wlocked());

    int r = _calc_target(&linger_op->target, true);
    if (r == RECALC_OP_TARGET_NEED_RESEND) {
      ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
		     << " pgid " << linger_op->target.pgid
		     << " acting " << linger_op->target.acting << dendl;

      OSDSession *s;
      r = _get_osd_session(linger_op->target.osd, rl, wl, &s);
      if (r < 0) {
	// We have no session for the new destination for the op, so
	// leave it in this session to be handled again next time we
	// scan requests
	return r;
      }

      if (linger_op->session != s) {
	// NB locking two sessions (s and linger_op->session) at the
	// same time here is only safe because we are the only one
	// that takes two, and we are holding rwlock for write.
	// Disable lockdep because it doesn't know that.
	  s->lock.get_write(false);
	  _session_linger_op_remove(linger_op->session, linger_op);
	  _session_linger_op_assign(s, linger_op);
	  s->lock.unlock(false);
      }

      put_session(s);
      return RECALC_OP_TARGET_NEED_RESEND;
    }
      return r;
#endif // LINGER
      return 0;
  }

  void Objecter::_cancel_op(Op& op)
  {
    ldout(cct, 15) << "cancel_op " << op.tid << dendl;
    assert(!op.should_resend);
    Op::unique_lock ol(op.lock);
    delete op.onack;
    op.onack = nullptr;
    delete op.oncommit;
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

    if (!op.ctx_budgeted && op.budgeted)
      put_op_budget(op);

    if (op.ontimeout) {
      unique_timer_lock l(timer_lock);
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
			   subop.hoid, subop.parent.volume->id,
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

    ldout(cct, 15) << "_send_subop " << subop.tid << " to osd."
		   << subop.session->osd << dendl;

    ConnectionRef con = subop.session->con;
    assert(con);

    subop.incarnation = subop.session->incarnation;

    m->set_tid(subop.tid);

    messenger->send_message(m, subop.session->con);
  }

  int Objecter::calc_op_budget(Op& op)
  {
    return op.op->get_budget();
  }

  void Objecter::_throttle_op(Op& op, shunique_lock& shl, int op_budget)
  {
    assert(shl);

    bool locked_for_write = shl.owns_lock();

    if (!op_budget)
      op_budget = calc_op_budget(op);
    if (!op_throttle_bytes.get_or_fail(op_budget)) { //couldn't take right now
      shl.unlock();
      op_throttle_bytes.get(op_budget);
      if (locked_for_write)
	shl.lock();
      else
	shl.lock_shared();
    }
    if (!op_throttle_ops.get_or_fail(1)) { //couldn't take right now
      shl.unlock();
      op_throttle_ops.get(1);
      if (locked_for_write)
	shl.lock();
      else
	shl.lock_shared();
    }
  }

  void Objecter::handle_osd_subop_reply(MOSDOpReply *m)
  {
    ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

    // get pio
    ceph_tid_t tid = m->get_tid();

    int osd_num = (int)m->get_source().num();

    shared_lock rl(rwlock);
    if (!initialized) {
      m->put();
      return;
    }

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

    auto iter = s.subops.find(tid, oc);
    if (iter == s.subops.end()) {
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

    if (subop.parent.objver &&
	(*subop.parent.objver < m->get_user_version()))
      *subop.parent.objver = m->get_user_version();

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
      else if (subop.session->is_homeless())
	homeless++;
      else
	still_coming++;
    }

    if (!op.finished && done >= op.volume->quorum() &&
	(do_or_die || !still_coming)) {
      // ack|commit -> ack
      Context *onack = nullptr;
      Context *oncommit = nullptr;

      if (op.onack && op.acks >= op.volume->quorum()) {
	onack = op.onack;
	op.onack = 0;  // only do callback once
	assert(num_unacked > 0);
	num_unacked--;
	op.trace.event("onack", &trace_endpoint);
      }
      if (op.oncommit && op.commits >= op.volume->quorum()) {
	oncommit = op.oncommit;
	op.oncommit = 0;
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
	onack->complete(op.rc);
      }
      if (oncommit) {
	oncommit->complete(op.rc);
      }
      return true;
    } else {
      ol.unlock();
    }
    return false;
  }

  class C_CancelStatfsOp : public Context
  {
    ceph_tid_t tid;
    Objecter *objecter;
  public:
    C_CancelStatfsOp(ceph_tid_t tid, Objecter *objecter)
      : tid(tid), objecter(objecter) {}
    void finish(int r) {
      objecter->statfs_op_cancel(tid, -ETIMEDOUT);
    }
  };

  void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
  {
    ldout(cct, 10) << "get_fs_stats" << dendl;
    unique_lock l(rwlock);

    StatfsOp* op = new StatfsOp;
    op->tid = ++last_tid;
    op->stats = &result;
    op->onfinish = onfinish;
    op->ontimeout = NULL;
    if (mon_timeout > ceph::timespan(0)) {
      unique_timer_lock l(timer_lock);
      op->ontimeout = new C_CancelStatfsOp(op->tid, this);
      timer.add_event_after(mon_timeout, op->ontimeout);
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
    if (!initialized) {
      m->put();
      return;
    }

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

  int Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
  {
    assert(initialized);

    unique_lock wl(rwlock);

    auto it = statfs_ops.find(tid, oc);
    if (it == statfs_ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
      return -ENOENT;
    }

    ldout(cct, 10) << __func__ << " tid " << tid << dendl;

    StatfsOp& op = *it;
    if (op.onfinish)
      op.onfinish->complete(r);
    _finish_statfs_op(op);
    return 0;
  }

  void Objecter::_finish_statfs_op(StatfsOp& op)
  {
    op.unlink();

    if (op.ontimeout) {
      unique_timer_lock tl(timer_lock);
      timer.cancel_event(op.ontimeout);
    }

    delete &op;
  }

  void Objecter::ms_handle_connect(Connection *con)
  {
    ldout(cct, 10) << "ms_handle_connect " << con << dendl;
    if (!initialized)
      return;

    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
      resend_mon_ops();
  }

  bool Objecter::ms_handle_reset(Connection *con)
  {
    if (!initialized)
      return false;
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
      //
      int osd = osdmap->identify_osd(con->get_peer_addr());
      if (osd >= 0) {
	ldout(cct, 1) << "ms_handle_reset on osd." << osd << dendl;
	unique_lock wl(rwlock);
	if (!initialized) {
	  wl.unlock();
	  return false;
	}
	auto p = osd_sessions.find(osd, oc);
	if (p != osd_sessions.end()) {
	  OSDSession& session = *p;
	  map<uint64_t, LingerOp *> lresend;
	  OSDSession::unique_lock sl(session.lock);
	  _reopen_session(session);
	  _kick_requests(session, lresend);
	  sl.unlock();
	  _linger_ops_resend(lresend);
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
    if (!initialized)
      return false;
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

  int Objecter::create_volume(const string& name, Context *onfinish)
  {
    ldout(cct, 10) << "create_volume name=" << name << dendl;

    if (osdmap->vol_exists(name) > 0)
      return -EEXIST;

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd volume create\", ");
    cmd.push_back("\"volumeName\":\"" + name + "\"");

    bufferlist bl;
    monc->start_mon_command(cmd, bl, nullptr, nullptr,
			    onfinish);
    return 0;
  }

  int Objecter::delete_volume(const string& name, Context *onfinish)
  {
    ldout(cct, 10) << "delete_volume name=" << name << dendl;

    vector<string> cmd;
    cmd.push_back("{\"prefix\":\"osd volume delete\", ");
    cmd.push_back("\"volumeName\":\"" + name + "\"");

    bufferlist bl;
    monc->start_mon_command(cmd, bl, nullptr, nullptr,
			    onfinish);
    return 0;
  }

  Objecter::OSDSession::~OSDSession()
  {
    // Caller is responsible for re-assigning or
    // destroying any ops that were assigned to us
    assert(subops.empty());
    assert(linger_subops.empty());
  }

  Objecter::~Objecter()
  {
    delete osdmap;

    assert(homeless_session->nref== 1);
    assert(homeless_session->subops.empty());
    assert(homeless_session->linger_subops.empty());
    homeless_session->put();

    assert(osd_sessions.empty());
    assert(statfs_ops.empty());
    assert(waiting_for_map.empty());
    assert(linger_ops.empty());
    assert(check_latest_map_lingers.empty());
    assert(check_latest_map_ops.empty());

    assert(!tick_event);
  }

  VolumeRef Objecter::vol_by_uuid(const boost::uuids::uuid& id) {
    shared_lock rl(rwlock);
    VolumeRef v;
    osdmap->find_by_uuid(id, v);
    return v;
  }

  VolumeRef Objecter::vol_by_name(const string& name) {
    shared_lock rl(rwlock);
    VolumeRef v;
    osdmap->find_by_name(name, v);
    return v;
  }
};
