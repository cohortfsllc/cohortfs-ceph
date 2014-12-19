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
#include "Filer.h"
#include "Striper.h"

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

// messages ------------------------------

  void Objecter::init()
  {
    assert(client_lock.is_locked());
    assert(!initialized);

    schedule_tick();
    if (osdmap->get_epoch() == 0)
      maybe_request_map();

    initialized = true;
  }

  void Objecter::shutdown()
  {
    assert(client_lock.is_locked());
    assert(initialized);
    initialized = false;

    map<int,OSDSession*>::iterator p;
    while (!osd_sessions.empty()) {
      p = osd_sessions.begin();
      close_session(p->second);
    }

    if (tick_event) {
      timer.cancel_event(tick_event);
      tick_event = NULL;
    }
  }

  void Objecter::send_linger(LingerOp *info)
  {
#ifdef LINGER
    ldout(cct, 15) << "send_linger " << info->linger_id << dendl;
    unique_ptr<ObjOp> op = move(info->ops); // need to pass a copy to ops
    Context *onack = (!info->registered && info->on_reg_ack) ? new C_Linger_Ack(this, info) : NULL;
    Context *oncommit = new C_Linger_Commit(this, info);
    Op *o = new Op(info->oid, info->volume,
		   op, info->flags | CEPH_OSD_FLAG_READ,
		   onack, oncommit, info->pobjver);
    o->mtime = info->mtime;

    // do not resend this; we will send a new op to reregister
    o->should_resend = false;

    if (info->session) {
      recalc_op_target(o);
    }

    if (info->register_tid) {
      // repeat send.  cancel old registeration op, if any.
      if (ops.count(info->register_tid)) {
	Op *o = ops[info->register_tid];
	op_cancel_map_check(o);
	cancel_linger_op(o);
      }
      info->register_tid = _op_submit(o);
    } else {
      // first send
      // populate info->pgid and info->acting so we
      // don't resend the linger op on the next osdmap update
      recalc_linger_op_target(info);
      info->register_tid = op_submit(o);
    }

    OSDSession *s = o->session;
    if (info->session != s) {
      info->session_item.remove_myself();
      info->session = s;
      if (info->session)
	s->linger_ops.push_back(&info->session_item);
    }
#endif
  }

  void Objecter::_linger_ack(LingerOp *info, int r)
  {
    ldout(cct, 10) << "_linger_ack " << info->linger_id << dendl;
    if (info->on_reg_ack) {
      info->on_reg_ack->complete(r);
      info->on_reg_ack = NULL;
    }
  }

  void Objecter::_linger_commit(LingerOp *info, int r)
  {
    ldout(cct, 10) << "_linger_commit " << info->linger_id << dendl;
    if (info->on_reg_commit) {
      info->on_reg_commit->complete(r);
      info->on_reg_commit = NULL;
    }

    // only tell the user the first time we do this
    info->registered = true;
    info->objver = NULL;
  }

  void Objecter::unregister_linger(uint64_t linger_id)
  {
#ifdef LINGER
    map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
    if (iter != linger_ops.end()) {
      LingerOp *info = iter->second;
      info->session_item.remove_myself();
      linger_ops.erase(iter);
      info->put();
    }
#endif
  }

  ceph_tid_t Objecter::linger_mutate(const object_t& oid,
				     const shared_ptr<const Volume>& volume,
				     unique_ptr<ObjOp>& op,
				     utime_t mtime,
				     bufferlist& inbl, int flags,
				     Context *onack, Context *oncommit,
				     version_t *objver)
  {
    LingerOp *info = new LingerOp;
    info->oid = oid;
    info->volume = volume;
    info->mtime = mtime;
    info->flags = flags | CEPH_OSD_FLAG_WRITE;
    info->op = move(op);
    info->inbl = inbl;
    info->outbl = nullptr;
    info->objver = objver;
    info->on_reg_ack = onack;
    info->on_reg_commit = oncommit;

    info->linger_id = ++max_linger_id;
    linger_ops[info->linger_id] = info;

    send_linger(info);

    return info->linger_id;
  }

  ceph_tid_t Objecter::linger_read(const object_t& oid,
				   const shared_ptr<const Volume>& volume,
				   unique_ptr<ObjOp>& op, bufferlist& inbl,
				   bufferlist *outbl, int flags,
				   Context *onfinish, version_t *objver)
  {
    LingerOp *info = new LingerOp;
    info->oid = oid;
    info->volume = volume;
    info->flags = flags;
    info->op = move(op);
    info->inbl = inbl;
    info->outbl = outbl;
    info->objver = objver;
    info->on_reg_commit = onfinish;

    info->linger_id = ++max_linger_id;
    linger_ops[info->linger_id] = info;

    send_linger(info);

    return info->linger_id;
  }

  void Objecter::dispatch(Message *m)
  {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OPREPLY:
      handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
      break;

    case CEPH_MSG_OSD_MAP:
      handle_osd_map(static_cast<MOSDMap*>(m));
      break;

    case CEPH_MSG_STATFS_REPLY:
      handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
      break;

    default:
      ldout(cct, 0) << "don't know message type " << m->get_type() << dendl;
      assert(0);
    }
  }

  void Objecter::scan_requests(bool force_resend,
			       bool force_resend_writes,
			       map<ceph_tid_t, Op*>& need_resend,
			       list<LingerOp*>& need_resend_linger)
  {
#ifdef LINGER
    // check for changed linger mappings (_before_ regular ops)
    map<ceph_tid_t,LingerOp*>::iterator lp = linger_ops.begin();
    while (lp != linger_ops.end()) {
      LingerOp *op = lp->second;
      ++lp;
      ldout(cct, 10) << " checking linger op " << op->linger_id << dendl;
      int r = recalc_linger_op_target(op);
      switch (r) {
      case RECALC_OP_TARGET_NO_ACTION:
	if (!force_resend && !force_resend_writes)
	  break;
	// -- fall-thru --
      case RECALC_OP_TARGET_NEED_RESEND:
	need_resend_linger.push_back(op);
	linger_cancel_map_check(op);
	break;
      }
    }
#endif

    // check for changed request mappings
    map<ceph_tid_t,Op*>::iterator p = inflight_ops.begin();
    while (p != inflight_ops.end()) {
      Op *op = p->second;
      ++p;
      ldout(cct, 10) << " checking op " << op->tid << dendl;
      int r = recalc_op_target(op);
      switch (r) {
      case RECALC_OP_TARGET_NO_ACTION:
	if (!force_resend &&
	    (!force_resend_writes || !(op->flags & CEPH_OSD_FLAG_WRITE)))
	  break;
	// -- fall-thru --
      case RECALC_OP_TARGET_NEED_RESEND:
	need_resend[op->tid] = op;
	op_cancel_map_check(op);
	break;
      }
    }
  }

  void Objecter::handle_osd_map(MOSDMap *m)
  {
    assert(client_lock.is_locked());
    assert(initialized);
    assert(osdmap);

    if (m->fsid != monc->get_fsid()) {
      ldout(cct, 0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
      m->put();
      return;
    }

    bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
    bool was_full = osdmap_full_flag();
    bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || was_full;

    list<LingerOp*> need_resend_linger;
    map<ceph_tid_t, Op*> need_resend;

    if (m->get_last() <= osdmap->get_epoch()) {
      ldout(cct, 3) << "handle_osd_map ignoring epochs ["
		    << m->get_first() << "," << m->get_last()
		    << "] <= " << osdmap->get_epoch() << dendl;
    }
    else {
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
	    ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e << dendl;
	    OSDMap::Incremental inc(m->incremental_maps[e]);
	    osdmap->apply_incremental(inc);
	  }
	  else if (m->maps.count(e)) {
	    ldout(cct, 3) << "handle_osd_map decoding full epoch " << e << dendl;
	    osdmap->decode(m->maps[e]);
	  }
	  else {
	    if (e && e > m->get_oldest()) {
	      ldout(cct, 3) << "handle_osd_map requesting missing epoch " << osdmap->get_epoch()+1 << dendl;
	      maybe_request_map();
	      break;
	    }
	    ldout(cct, 3) << "handle_osd_map missing epoch " << osdmap->get_epoch()+1
			  << ", jumping to " << m->get_oldest() << dendl;
	    e = m->get_oldest() - 1;
	    skipped_map = true;
	    continue;
	  }

	  was_full = was_full || osdmap_full_flag();
	  scan_requests(skipped_map, was_full, need_resend, need_resend_linger);

	  // osd addr changes?
	  for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	       p != osd_sessions.end(); ) {
	    OSDSession *s = p->second;
	    ++p;
	    if (osdmap->is_up(s->osd)) {
	      if (s->con && s->con->get_peer_addr() != osdmap->get_inst(s->osd).addr)
		close_session(s);
	    } else {
	      close_session(s);
	    }
	  }

	  assert(e == osdmap->get_epoch());
	}

      } else {
	// first map.  we want the full thing.
	if (m->maps.count(m->get_last())) {
	  ldout(cct, 3) << "handle_osd_map decoding full epoch " << m->get_last() << dendl;
	  osdmap->decode(m->maps[m->get_last()]);

	  scan_requests(false, false, need_resend, need_resend_linger);
	} else {
	  ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting" << dendl;
	  monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	  monc->renew_subs();
	}
      }
    }

    bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
    bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap_full_flag();

    // was/is paused?
    if (was_pauserd || was_pausewr || pauserd || pausewr)
      maybe_request_map();

    // resend requests
    for (map<ceph_tid_t, Op*>::iterator p = need_resend.begin(); p != need_resend.end(); ++p) {
      Op *op = p->second;
      if (op->should_resend) {
	if (all_of(op->subops.cbegin(),
		   op->subops.cend(),
		   [](const SubOp& s){return s.session;})
	    && !op->paused) {
	  send_op(op);
	}
      } else {
	cancel_op(op);
      }
    }
#ifdef LINGER
    for (list<LingerOp*>::iterator p = need_resend_linger.begin(); p != need_resend_linger.end(); ++p) {
      LingerOp *op = *p;
      if (op->session) {
	send_linger(op);
      }
    }
#endif

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

    m->put();

    monc->sub_got("osdmap", osdmap->get_epoch());

    if (!waiting_for_map.empty())
      maybe_request_map();
  }

  void Objecter::C_Op_Map_Latest::finish(int r)
  {
    if (r == -EAGAIN || r == -ECANCELED)
      return;

    lgeneric_subdout(objecter->cct, objecter, 10) << "op_map_latest r=" << r << " tid=" << tid
						  << " latest " << latest << dendl;

    Mutex::Locker l(objecter->client_lock);

    map<ceph_tid_t, Op*>::iterator iter =
      objecter->check_latest_map_ops.find(tid);
    if (iter == objecter->check_latest_map_ops.end()) {
      lgeneric_subdout(objecter->cct, objecter, 10) << "op_map_latest op " << tid << " not found" << dendl;
      return;
    }

    Op *op = iter->second;
    objecter->check_latest_map_ops.erase(iter);

    lgeneric_subdout(objecter->cct, objecter, 20) << "op_map_latest op " << op << dendl;

    if (op->map_dne_bound == 0)
      op->map_dne_bound = latest;
  }

  void Objecter::_send_op_map_check(Op *op)
  {
    assert(client_lock.is_locked());
    // ask the monitor
    if (check_latest_map_ops.count(op->tid) == 0) {
      check_latest_map_ops[op->tid] = op;
      C_Op_Map_Latest *c = new C_Op_Map_Latest(this, op->tid);
      monc->get_version("osdmap", &c->latest, NULL, c);
    }
  }

  void Objecter::op_cancel_map_check(Op *op)
  {
    assert(client_lock.is_locked());
    map<ceph_tid_t, Op*>::iterator iter =
      check_latest_map_ops.find(op->tid);
    if (iter != check_latest_map_ops.end()) {
      check_latest_map_ops.erase(iter);
    }
  }

  void Objecter::C_Linger_Map_Latest::finish(int r)
  {
    if (r == -EAGAIN || r == -ECANCELED) {
      // ignore callback; we will retry in resend_mon_ops()
      return;
    }

    Mutex::Locker l(objecter->client_lock);

    map<uint64_t, LingerOp*>::iterator iter =
      objecter->check_latest_map_lingers.find(linger_id);
    if (iter == objecter->check_latest_map_lingers.end()) {
      return;
    }

    LingerOp *op = iter->second;
    objecter->check_latest_map_lingers.erase(iter);
    op->put();

    if (op->map_dne_bound == 0)
      op->map_dne_bound = latest;
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

  void Objecter::linger_cancel_map_check(LingerOp *op)
  {
    map<uint64_t, LingerOp*>::iterator iter =
      check_latest_map_lingers.find(op->linger_id);
    if (iter != check_latest_map_lingers.end()) {
      LingerOp *op = iter->second;
      op->put();
      check_latest_map_lingers.erase(iter);
    }
  }

  Objecter::OSDSession *Objecter::get_session(int osd)
  {
    map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
    if (p != osd_sessions.end())
      return p->second;
    OSDSession *s = new OSDSession(osd);
    osd_sessions[osd] = s;
    s->con = messenger->get_connection(osdmap->get_inst(osd));
    return s;
  }

  void Objecter::reopen_session(OSDSession *s)
  {
    entity_inst_t inst = osdmap->get_inst(s->osd);
    ldout(cct, 10) << "reopen_session osd." << s->osd << " session, addr now " << inst << dendl;
    if (s->con) {
      messenger->mark_down(s->con);
    }
    s->con = messenger->get_connection(inst);
    s->incarnation++;
  }

  void Objecter::close_session(OSDSession *s)
  {
    ldout(cct, 10) << "close_session for osd." << s->osd << dendl;
    if (s->con) {
      messenger->mark_down(s->con);
    }
    for (auto& o : s->subops) {
      assert(!o.session || o.session == s);
      o.session = nullptr;
    }
    s->subops.clear();
    s->linger_subops.clear();
    osd_sessions.erase(s->osd);
    delete s;
  }

  void Objecter::wait_for_osd_map()
  {
    if (osdmap->get_epoch()) return;
    Mutex lock;
    Cond cond;
    bool done;
    lock.Lock();
    C_SafeCond *context = new C_SafeCond(&lock, &cond, &done, NULL);
    waiting_for_map[0].push_back(pair<Context*, int>(context, 0));
    while (!done)
      cond.Wait(lock);
    lock.Unlock();
  }

  struct C_Objecter_GetVersion : public Context {
    Objecter *objecter;
    uint64_t oldest, newest;
    Context *fin;
    C_Objecter_GetVersion(Objecter *o, Context *c)
      : objecter(o), oldest(0), newest(0), fin(c) {}
    void finish(int r) {
      if (r >= 0)
	objecter->_get_latest_version(oldest, newest, fin);
      else if (r == -EAGAIN) { // try again as instructed
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

  void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest, Context *fin)
  {
    if (osdmap->get_epoch() >= newest) {
      ldout(cct, 10) << __func__ << " latest " << newest << ", have it" << dendl;
      if (fin)
	fin->complete(0);
      return;
    }

    ldout(cct, 10) << __func__ << " latest " << newest << ", waiting" << dendl;
    wait_for_new_map(fin, newest, 0);
  }

  void Objecter::maybe_request_map()
  {
    int flag = 0;
    if (osdmap_full_flag()) {
      ldout(cct, 10) << "maybe_request_map subscribing (continuous) to next osd map (FULL flag is set)" << dendl;
    } else {
      ldout(cct, 10) << "maybe_request_map subscribing (onetime) to next osd map" << dendl;
      flag = CEPH_SUBSCRIBE_ONETIME;
    }
    epoch_t epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
    if (monc->sub_want("osdmap", epoch, flag))
      monc->renew_subs();
  }

  void Objecter::wait_for_new_map(Context *c, epoch_t epoch, int err)
  {
    waiting_for_map[epoch].push_back(pair<Context *, int>(c, err));
    maybe_request_map();
  }

  void Objecter::kick_requests(OSDSession *session)
  {
    ldout(cct, 10) << "kick_requests for osd." << session->osd << dendl;

    // resend ops
    map<ceph_tid_t,Op*> resend;  // resend in tid order

    for (auto &subop : session->subops) {
      if (subop.parent.should_resend) {
	if (!subop.parent.paused)
	  resend[subop.parent.tid] = &subop.parent;
      } else {
	cancel_op(&subop.parent);
      }
    }
    while (!resend.empty()) {
      send_op(resend.begin()->second);
      resend.erase(resend.begin());
    }

#ifdef LINGER
    // resend lingers
    map<uint64_t, LingerOp*> lresend;  // resend in order
    for (xlist<LingerOp*>::iterator j = session->linger_ops.begin(); !j.end(); ++j) {
      lresend[(*j)->linger_id] = *j;
    }
    while (!lresend.empty()) {
      send_linger(lresend.begin()->second);
      lresend.erase(lresend.begin());
    }
#endif
  }

  void Objecter::schedule_tick()
  {
    assert(tick_event == NULL);
    tick_event = new C_Tick(this);
    timer.add_event_after(cct->_conf->objecter_tick_interval, tick_event);
  }

  void Objecter::tick()
  {
    ldout(cct, 10) << "tick" << dendl;
    assert(client_lock.is_locked());
    assert(initialized);

    // we are only called by C_Tick
    assert(tick_event);
    tick_event = NULL;

    set<OSDSession*> toping;

    // look for laggy requests
    utime_t cutoff = ceph_clock_now(cct);
    cutoff -= cct->_conf->objecter_timeout;  // timeout

    unsigned laggy_ops = 0;
    for (auto& kv : subops) {
      SubOp* subop = kv.second;
      if (subop->session && subop->stamp < cutoff) {
	toping.insert(subop->session);
	++laggy_ops;
      }
    }
#ifdef LINGER
    for (map<uint64_t,LingerOp*>::iterator p = linger_ops.begin();
	 p != linger_ops.end();
	 ++p) {
      LingerOp *op = p->second;
      if (op->session) {
	ldout(cct, 10) << " pinging osd that serves lingering tid " <<
	  p->first << " (osd." << op->session->osd << ")" << dendl;
	toping.insert(op->session);
      } else {
	ldout(cct, 10) << " lingering tid " << p->first << " does not have session" << dendl;
      }
    }
#endif

    if (num_homeless_ops || !toping.empty())
      maybe_request_map();

    if (!toping.empty()) {
      // send a ping to these osds, to ensure we detect any session resets
      // (osd reply message policy is lossy)
      for (set<OSDSession*>::iterator i = toping.begin();
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
    assert(client_lock.is_locked());
    ldout(cct, 10) << "resend_mon_ops" << dendl;

    for (map<ceph_tid_t,StatfsOp*>::iterator p = statfs_ops.begin(); p!=statfs_ops.end(); ++p) {
      fs_stats_submit(p->second);
    }

    for (map<ceph_tid_t, Op*>::iterator p = check_latest_map_ops.begin();
	 p != check_latest_map_ops.end();
	 ++p) {
      C_Op_Map_Latest *c = new C_Op_Map_Latest(this, p->second->tid);
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

  class C_CancelOp : public Context
  {
    Objecter::Op *op;
    Objecter *objecter;
  public:
    C_CancelOp(Objecter::Op *op, Objecter *objecter) : op(op),
						       objecter(objecter) {}
    void finish(int r) {
      // note that objecter lock == timer lock, and is already held
      objecter->op_cancel(op->tid, -ETIMEDOUT);
    }
  };

  ceph_tid_t Objecter::op_submit(Op *op)
  {
    assert(client_lock.is_locked());
    assert(initialized);

    if (osd_timeout > 0) {
      op->ontimeout = new C_CancelOp(op, this);
      timer.add_event_after(osd_timeout, op->ontimeout);
    }

    // throttle.	before we look at any state, because
    // take_op_budget() may drop our lock while it blocks.
    take_op_budget(op);

    return _op_submit(op);
  }

  ceph_tid_t Objecter::_op_submit(Op *op)
  {
    // pick tid
    op->tid = ++last_tid;
    assert(client_inc >= 0);

    // pick target
    num_homeless_ops++;  // initially; recalc_op_target() will decrement if it finds a target
    recalc_op_target(op);

    // add to gather set(s)
    if (op->onack) {
      ++num_unacked;
    } else {
      ldout(cct, 20) << " note: not requesting ack" << dendl;
    }
    if (op->oncommit) {
      ++num_uncommitted;
    } else {
      ldout(cct, 20) << " note: not requesting commit" << dendl;
    }
    inflight_ops[op->tid] = op;

    // send?

    assert(op->flags & (CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_WRITE));

    if ((op->flags & CEPH_OSD_FLAG_WRITE) &&
	osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
      ldout(cct, 10) << " paused modify " << op << " tid " << last_tid << dendl;
      op->paused = true;
      maybe_request_map();
    } else if ((op->flags & CEPH_OSD_FLAG_READ) &&
	       osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
      ldout(cct, 10) << " paused read " << op << " tid " << last_tid << dendl;
      op->paused = true;
      maybe_request_map();
    } else if ((op->flags & CEPH_OSD_FLAG_WRITE) && osdmap_full_flag()) {
      ldout(cct, 0) << " FULL, paused modify " << op << " tid " << last_tid << dendl;
      op->paused = true;
      maybe_request_map();
    } else if (all_of(op->subops.cbegin(),
		      op->subops.cend(),
		      [](const SubOp& s){return s.session;})) {
      send_op(op);
    } else {
      maybe_request_map();
    }

    ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

    return op->tid;
  }

  int Objecter::op_cancel(ceph_tid_t tid, int r)
  {
    assert(client_lock.is_locked());
    assert(initialized);

    map<ceph_tid_t, Op*>::iterator p = inflight_ops.find(tid);
    if (p == inflight_ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
      return -ENOENT;
    }

    ldout(cct, 10) << __func__ << " tid " << tid << dendl;
    Op *op = p->second;
    if (op->onack) {
      op->onack->complete(r);
      op->onack = NULL;
    }
    if (op->oncommit) {
      op->oncommit->complete(r);
      op->oncommit = NULL;
    }
    op_cancel_map_check(op);
    // Do something for the subops
    finish_op(op);
    return 0;
  }

  bool Objecter::target_should_be_paused(op_base *t)
  {
    bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
    bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap_full_flag();

    return (t->flags & CEPH_OSD_FLAG_READ && pauserd) ||
      (t->flags & CEPH_OSD_FLAG_WRITE && pausewr);
  }


/**
 * Wrapper around osdmap->test_flag for special handling of the FULL flag.
 */
  bool Objecter::osdmap_full_flag() const
  {
    // Ignore the FULL flag if we are working on behalf of an MDS, in order to permit
    // MDS journal writes for file deletions.
    return osdmap->test_flag(CEPH_OSDMAP_FULL) && (messenger->get_myname().type() != entity_name_t::TYPE_MDS);
  }


  int Objecter::calc_target(op_base *t)
  {
    // Oh, yay!
    size_t i = 0;
    bool need_resend = false;

    // Volume should know how many OSDs to supply for its own
    // operations. But it never hurts to fail spectacularly in case I
    // goof up.
    t->volume->place(
      t->oid, *osdmap, [&](int osd) {
	assert(i < t->subops.size());
	if ((t->subops[i].osd != osd) && !t->subops[i].done) {
	  t->subops[i].osd = osd;
	  need_resend = true;
	}
	++i;
      });
    assert(i == t->subops.size());

    bool paused = target_should_be_paused(t);
    if (!paused && paused != t->paused) {
      t->paused = false;
      need_resend = true;
    }

    if (need_resend) {
      return RECALC_OP_TARGET_NEED_RESEND;
    }
    return RECALC_OP_TARGET_NO_ACTION;
  }

  int Objecter::recalc_op_target(Op *op)
  {
    int r = calc_target(op);
    if (r == RECALC_OP_TARGET_NEED_RESEND) {
      for (auto& subop : op->subops) {
	OSDSession *s = NULL;
	if (subop.osd >= 0)
	  s = get_session(subop.osd);
	if (subop.session != s) {
	  if (!subop.session)
	    num_homeless_ops--;
	  subop.unlink();
	  subop.session = s;
	  if (s)
	    s->subops.push_front(subop);
	  else
	    num_homeless_ops++;
	}
      }
    }
    return r;
  }

  bool Objecter::recalc_linger_op_target(LingerOp *linger_op)
  {
#ifdef LINGER
    int r = calc_target(&linger_op->target);
    if (r == RECALC_OP_TARGET_NEED_RESEND) {
      ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
		     << " pgid " << linger_op->target.pgid
		     << " osd " << linger_op->target.osd << dendl;

      OSDSession *s = linger_op->target.osd != -1 ?
	get_session(linger_op->target.osd) : NULL;
      if (linger_op->session != s) {
	linger_op->session_item.remove_myself();
	linger_op->session = s;
	if (s)
	  s->linger_ops.push_back(&linger_op->session_item);
      }
    }
    return r;
#endif
    return 0;
  }

  void Objecter::cancel_op(Op *op)
  {
    ldout(cct, 15) << "cancel_op " << op->tid << dendl;

    assert(!op->should_resend);
    delete op->onack;
    delete op->oncommit;

    finish_op(op);
  }

  void Objecter::finish_op(Op *op)
  {
    ldout(cct, 15) << "finish_op " << op->tid << dendl;

    for(auto& subop : op->subops) {
      subop.unlink();
      subops.erase(subop.tid);
    }

    if (op->budgeted)
      put_op_budget(op);

    inflight_ops.erase(op->tid);
    assert(check_latest_map_ops.find(op->tid) == check_latest_map_ops.end());

    if (op->ontimeout)
      timer.cancel_event(op->ontimeout);

    delete op;
  }

  void Objecter::send_subop(SubOp &subop, int flags)
  {
    assert(subop.session->con);
    ldout(cct, 15) << "send_op " << subop.parent.tid << " to osd."
		   << subop.osd << dendl;

    subop.incarnation = subop.session->incarnation;
    subop.stamp = ceph_clock_now(cct);

    MOSDOp *m = new MOSDOp(client_inc, subop.tid,
			   subop.hoid, subop.parent.volume->id,
			   osdmap->get_epoch(),
			   flags);

    m->ops = subop.ops;
    m->set_mtime(subop.parent.mtime);
    m->set_retry_attempt(subop.attempts++);

    if (subop.replay_version != eversion_t())
      m->set_version(subop.replay_version);	 // we're replaying this op!

    if (subop.parent.op->priority)
      m->set_priority(subop.parent.op->priority);
    else
      m->set_priority(cct->_conf->osd_client_op_priority);

    messenger->send_message(m, subop.session->con);
  }

  void Objecter::send_op(Op *op)
  {
    int flags = op->flags;
    if (op->oncommit)
      flags |= CEPH_OSD_FLAG_ONDISK;
    if (op->onack)
      flags |= CEPH_OSD_FLAG_ACK;


    op->paused = false;

    for(auto &subop : op->subops) {
      subop.tid = ++last_tid;
      subops[subop.tid] = &subop;
      send_subop(subop, flags);
    }
  }

  void Objecter::throttle_op(Op *op, uint64_t op_budget)
  {
    if (!op_budget)
      op_budget = op->op->get_budget();
    if (!op_throttle_bytes.get_or_fail(op_budget)) { //couldn't take right now
      client_lock.Unlock();
      op_throttle_bytes.get(op_budget);
      client_lock.Lock();
    }
    if (!op_throttle_ops.get_or_fail(1)) { //couldn't take right now
      client_lock.Unlock();
      op_throttle_ops.get(1);
      client_lock.Lock();
    }
  }

  void Objecter::unregister_op(Op *op)
  {
    if (op->onack)
      num_unacked--;
    if (op->oncommit)
      num_uncommitted--;
    inflight_ops.erase(op->tid);
  }

/* This function DOES put the passed message before returning */
  void Objecter::handle_osd_op_reply(MOSDOpReply *m)
  {
    assert(client_lock.is_locked());
    assert(initialized);
    ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

    // get pio
    ceph_tid_t tid = m->get_tid();

    auto p = subops.find(tid);

    if (p == subops.end()) {
      ldout(cct, 7) << "handle_osd_op_reply " << tid
		    << (m->is_ondisk() ? " ondisk" :
			(m->is_onnvram() ? " onnvram" : " ack"))
		    << " ... stray" << dendl;
      m->put();
      return;
    }

    ldout(cct, 7) << "handle_osd_op_reply " << tid
		  << (m->is_ondisk() ? " ondisk" : (m->is_onnvram()
						    ? " onnvram":" ack"))
		  << " v " << m->get_replay_version() << " uv "
		  << m->get_user_version()
		  << " attempt " << m->get_retry_attempt()
		  << dendl;
    SubOp *subop = p->second;

    if (m->get_retry_attempt() >= 0) {
      if (m->get_retry_attempt() != (subop->attempts - 1)) {
	ldout(cct, 7) << " ignoring reply from attempt "
		      << m->get_retry_attempt()
		      << " from " << m->get_source_inst()
		      << "; last attempt " << (subop->attempts - 1)
		      << " sent to " << subop->session->con->get_peer_addr()
		      << dendl;
	m->put();
	return;
      }
    } else {
      // we don't know the request attempt because the server is old, so
      // just accept this one.  we may do ACK callbacks we shouldn't
      // have, but that is better than doing callbacks out of order.
    }

    int rc = m->get_result();

    if (rc == -EAGAIN) {
      ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;
      unregister_op(&subop->parent);
      _op_submit(&subop->parent);
      m->put();
      return;
    }

    if (subop->parent.objver) {
      if (*subop->parent.objver < m->get_user_version()) {
	*subop->parent.objver = m->get_user_version();
      }
    }
    if (subop->parent.reply_epoch && *subop->parent.reply_epoch
	< m->get_map_epoch())
      *subop->parent.reply_epoch = m->get_map_epoch();

    // per-op result demuxing
    vector<OSDOp> out_ops;
    m->claim_ops(out_ops);

    if (out_ops.size() != subop->ops.size())
      ldout(cct, 0) << "WARNING: tid " << subop->tid << " reply ops "
		    << out_ops << " != request ops " << subop->ops
		    << " from " << m->get_source_inst() << dendl;


    for (size_t i = 0; i < out_ops.size(); ++i) {
      ldout(cct, 10) << " op " << i << " rval " << out_ops[i].rval
		     << " len " << out_ops[i].outdata.length() << dendl;
      if (subop->ops[i].out_bl)
	*subop->ops[i].out_bl = out_ops[i].outdata;

      // set rval before running handlers so that handlers
      // can change it if e.g. decoding fails
      if (subop->ops[i].out_rval)
	*subop->ops[i].out_rval = out_ops[i].rval;

      if (subop->ops[i].ctx) {
	ldout(cct, 10) << " op " << i << " handler " << subop->ops[i].ctx
		       << dendl;
	subop->ops[i].ctx->complete(out_ops[i].rval);
	subop->ops[i].ctx = nullptr;
      }
    }

    subop->done = true;

    if (all_of(subop->parent.subops.cbegin(),
	       subop->parent.subops.cend(),
	       [](const SubOp& s){return s.done;})) {
      // ack|commit -> ack
      Context *onack = nullptr;
      Context *oncommit = nullptr;

      if (subop->parent.onack) {
	ldout(cct, 15) << "handle_osd_op_reply ack" << dendl;
	subop->replay_version = m->get_replay_version();
	onack = subop->parent.onack;
	subop->parent.onack = 0;  // only do callback once
	num_unacked--;
      }
      if (subop->parent.oncommit && (m->is_ondisk() || rc)) {
	ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
	oncommit = subop->parent.oncommit;
	subop->parent.oncommit = 0;
	num_uncommitted--;
      }

      // done with this tid?
      if (!subop->parent.onack && !subop->parent.oncommit) {
	ldout(cct, 15) << "handle_osd_op_reply completed tid "
		       << subop->parent.tid << dendl;
	finish_op(&subop->parent);
      }

      ldout(cct, 5) << num_unacked << " unacked, "
		    << num_uncommitted << " uncommitted" << dendl;

      // do callbacks
      if (onack) {
	onack->complete(rc);
      }
      if (oncommit) {
	oncommit->complete(rc);
      }
      m->put();
    }
  }

  class C_CancelStatfsOp : public Context
  {
    ceph_tid_t tid;
    Objecter *objecter;
  public:
    C_CancelStatfsOp(ceph_tid_t tid, Objecter *objecter) : tid(tid),
							   objecter(objecter) {}
    void finish(int r) {
      // note that objecter lock == timer lock, and is already held
      objecter->statfs_op_cancel(tid, -ETIMEDOUT);
    }
  };

  void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
  {
    ldout(cct, 10) << "get_fs_stats" << dendl;

    StatfsOp *op = new StatfsOp;
    op->tid = ++last_tid;
    op->stats = &result;
    op->onfinish = onfinish;
    op->ontimeout = NULL;
    if (mon_timeout > 0) {
      op->ontimeout = new C_CancelStatfsOp(op->tid, this);
      timer.add_event_after(mon_timeout, op->ontimeout);
    }
    statfs_ops[op->tid] = op;

    fs_stats_submit(op);
  }

  void Objecter::fs_stats_submit(StatfsOp *op)
  {
    ldout(cct, 10) << "fs_stats_submit" << op->tid << dendl;
    monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid, last_seen_pgmap_version));
    op->last_submit = ceph_clock_now(cct);
  }


  void Objecter::handle_fs_stats_reply(MStatfsReply *m)
  {
    assert(client_lock.is_locked());
    assert(initialized);
    ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
    ceph_tid_t tid = m->get_tid();

    if (statfs_ops.count(tid)) {
      StatfsOp *op = statfs_ops[tid];
      ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
      *(op->stats) = m->h.st;
      if (m->h.version > last_seen_pgmap_version)
	last_seen_pgmap_version = m->h.version;
      op->onfinish->complete(0);
      finish_statfs_op(op);
    } else {
      ldout(cct, 10) << "unknown request " << tid << dendl;
    }
    ldout(cct, 10) << "done" << dendl;
    m->put();
  }

  int Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
  {
    assert(client_lock.is_locked());
    assert(initialized);

    map<ceph_tid_t, StatfsOp*>::iterator it = statfs_ops.find(tid);
    if (it == statfs_ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
      return -ENOENT;
    }

    ldout(cct, 10) << __func__ << " tid " << tid << dendl;

    StatfsOp *op = it->second;
    if (op->onfinish)
      op->onfinish->complete(r);
    finish_statfs_op(op);
    return 0;
  }

  void Objecter::finish_statfs_op(StatfsOp *op)
  {
    statfs_ops.erase(op->tid);

    if (op->ontimeout)
      timer.cancel_event(op->ontimeout);

    delete op;
  }

// scatter/gather

  void Objecter::_sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl,
				 bufferlist *bl, Context *onfinish)
  {
    // all done
    ldout(cct, 15) << "_sg_read_finish" << dendl;

    if (extents.size() > 1) {
      Striper::StripedReadResult r;
      vector<bufferlist>::iterator bit = resultbl.begin();
      for (vector<ObjectExtent>::iterator eit = extents.begin();
	   eit != extents.end();
	   ++eit, ++bit) {
	r.add_partial_result(cct, *bit, eit->buffer_extents);
      }
      bl->clear();
      r.assemble_result(cct, *bl, false);
    } else {
      ldout(cct, 15) << "	 only one frag" << dendl;
      bl->claim(resultbl[0]);
    }

    // done
    uint64_t bytes_read = bl->length();
    ldout(cct, 7) << "_sg_read_finish " << bytes_read << " bytes" << dendl;

    if (onfinish) {
      onfinish->complete(bytes_read);// > 0 ? bytes_read:m->get_result());
    }
  }


  void Objecter::ms_handle_connect(Connection *con)
  {
    ldout(cct, 10) << "ms_handle_connect " << con << dendl;
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
      resend_mon_ops();
  }

  void Objecter::ms_handle_reset(Connection *con)
  {
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
      //
      int osd = osdmap->identify_osd(con->get_peer_addr());
      if (osd >= 0) {
	ldout(cct, 1) << "ms_handle_reset on osd." << osd << dendl;
	map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
	if (p != osd_sessions.end()) {
	  OSDSession *session = p->second;
	  reopen_session(session);
	  kick_requests(session);
	  maybe_request_map();
	}
      } else {
	ldout(cct, 10) << "ms_handle_reset on unknown osd addr " << con->get_peer_addr() << dendl;
      }
    }
  }

  void Objecter::ms_handle_remote_reset(Connection *con)
  {
    /*
     * treat these the same.
     */
    ms_handle_reset(con);
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
};
