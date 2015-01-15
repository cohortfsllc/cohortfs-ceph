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

  Mutex *Objecter::OSDSession::get_lock(object_t& oid)
  {
#define HASH_PRIME 1021
    uint32_t h = ceph_str_hash_linux(oid.name.c_str(),
				     oid.name.size()) % HASH_PRIME;

    return completion_locks[h % num_locks];
  }


  // messages ------------------------------

  // initialize only internal data structures, don't initiate cluster
  // interaction
  void Objecter::init()
  {
    assert(!initialized);

    timer_lock.Lock();
    timer.init();
    timer_lock.Unlock();

    initialized = true;
  }

  void Objecter::start()
  {
    RWLock::RLocker rl(rwlock);

    schedule_tick();
    if (osdmap->get_epoch() == 0) {
      int r = _maybe_request_map();
      assert (r == 0 || osdmap->get_epoch() > 0);
    }
  }

  void Objecter::shutdown()
  {
    assert(initialized);

    RWLock::WLocker wl(rwlock);

    initialized = false;

    map<int,OSDSession*>::iterator p;
    while (!osd_sessions.empty()) {
      p = osd_sessions.begin();
      close_session(p->second);
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
      map<ceph_tid_t, StatfsOp*>::iterator i = statfs_ops.begin();
      delete i->second;
      statfs_ops.erase(i->first);
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
	RWLock::WLocker wl(homeless_session->lock);
	_session_subop_remove(homeless_session, *i);
      }
    }

    if (tick_event) {
      Mutex::Locker l(timer_lock);
      if (timer.cancel_event(tick_event))
	tick_event = NULL;
    }

    {
      Mutex::Locker l(timer_lock);
      timer.shutdown();
    }
  }

#ifdef LINGER
  void Objecter::_send_linger(LingerOp *info)
  {
    assert(rwlock.is_wlocked());

    RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

    ldout(cct, 15) << "send_linger " << info->linger_id << dendl;
    vector<OSDOp> opv = info->ops; // need to pass a copy to ops
    Context *onack = (!info->registered && info->on_reg_ack) ?
      new C_Linger_Ack(this, info) : NULL;
    Context *oncommit = new C_Linger_Commit(this, info);
    Op *o = new Op(info->target.base_oid, info->target.base_oloc,
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
    info->pobjver = NULL;
  }

  void Objecter::unregister_linger(uint64_t linger_id)
  {
    RWLock::WLocker wl(rwlock);
    _unregister_linger(linger_id);
  }

  void Objecter::_unregister_linger(uint64_t linger_id)
  {
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
  }

  ceph_tid_t Objecter::linger_mutate(const object_t& oid,
				     const shared_ptr<const volume>& volume,
				     ObjectOperation& op,
				     utime_t mtime,
				     bufferlist& inbl, int flags,
				     Context *onack, Context *oncommit,
				     version_t *objver)
  {
    LingerOp *info = new LingerOp;
    info->base_oid = oid;
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
  }

  ceph_tid_t Objecter::linger_read(const object_t& oid,
				   const object_locator_t& oloc,
				   ObjectOperation& op,
				   bufferlist& inbl, bufferlist *poutbl,
				   int flags,
				   Context *onfinish,
				   version_t *objver)
  {
    LingerOp *info = new LingerOp;
    info->target.base_oid = oid;
    info->target.base_oloc = oloc;
    if (info->target.base_oloc.key == oid)
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
  }

  void Objecter::_linger_submit(LingerOp *info)
  {
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
  }
#endif /* LINGER */

  bool Objecter::ms_dispatch(Message *m)
  {
    ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;
    if (!initialized)
      return false;

    switch (m->get_type()) {
      // these we exlusively handle
    case CEPH_MSG_OSD_OPREPLY:
      handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
      return true;

    case CEPH_MSG_STATFS_REPLY:
      handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
      return true;

    // these we give others a chance to inspect

    // MDS, OSD
    case CEPH_MSG_OSD_MAP:
      handle_osd_map(static_cast<MOSDMap*>(m));
      return false;
    }
    return false;
  }

  void Objecter::_scan_requests(bool force_resend,
				bool force_resend_writes,
				map<ceph_tid_t, Op*>& need_resend,
				list<LingerOp*>& need_resend_linger)
  {
    assert(rwlock.is_wlocked());

    list<uint64_t> unregister_lingers;

    RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

#ifdef LINGER
    // check for changed linger mappings (_before_ regular ops)
    map<ceph_tid_t,LingerOp*>::iterator lp = s->linger_ops.begin();
    while (lp != s->linger_ops.end()) {
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

    // TODO: ACE: Fix this up to handle subops properly, since it
    // turns the data structures/model used here inside out

    // Check for changed request mappings
    auto p = inflight_ops.begin();
    while (p != inflight_ops.end()) {
      Op* op = p->second;
      // check_op_volume_dne() may touch ops; prevent iterator invalidation
      ++p;
      ldout(cct, 10) << " checking op " << op->tid << dendl;
      int r = _calc_targets(op);
      switch (r) {
      case TARGET_NO_ACTION:
	if (!force_resend &&
	    (!force_resend_writes || !(op->flags & CEPH_OSD_FLAG_WRITE)))
	  break;
      case TARGET_NEED_RESEND:
	for(auto &subop : op->subops) {
	  OSDSession* s = subop.session;
	  s->lock.get_write();
	  if (subop.session == s) {
	    _session_subop_remove(s, subop);
	  }
	  s->lock.put_write();
	}
	need_resend[op->tid] = op;
	_op_cancel_map_check(op);
	break;
      case TARGET_VOLUME_DNE:
	_check_op_volume_dne(op, true);
	break;
      }
    }

    for (auto &linger : unregister_lingers) {
      _unregister_linger(linger);
    }
  }

  void Objecter::handle_osd_map(MOSDMap *m)
  {
    RWLock::WLocker wl(rwlock);
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
    map<ceph_tid_t, Op*> need_resend;

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
	      int r = _maybe_request_map();
	      assert(r == 0);
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
	  _scan_requests(skipped_map, was_full, need_resend,
			 need_resend_linger);

	  // osd addr changes?
	  for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	       p != osd_sessions.end(); ) {
	    OSDSession *s = p->second;
	    ++p;
	    if (!osdmap->is_up(s->osd) ||
		(s->con &&
		 s->con->get_peer_addr() != osdmap->get_inst(s->osd).addr)) {
	      close_session(s);
	    }
	  }

	  assert(e == osdmap->get_epoch());
	}

      } else {
	// first map.  we want the full thing.
	if (m->maps.count(m->get_last())) {
	  _scan_requests(false, false, need_resend, need_resend_linger);
	  ldout(cct, 3) << "handle_osd_map decoding full epoch "
			<< m->get_last() << dendl;
	  osdmap->decode(m->maps[m->get_last()]);

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
      int r = _maybe_request_map();
      assert(r == 0);
    }

    RWLock::Context lc(rwlock, RWLock::Context::TakenForWrite);

    // resend requests
    for (map<ceph_tid_t, Op*>::iterator p = need_resend.begin();
	 p != need_resend.end(); ++p) {
      Op *op = p->second;
      OSDSession *s = op->session;
      bool mapped_session = false;
      if (!s) {
	int r = _map_session(&op->target, &s, lc);
	assert(r == 0);
	mapped_session = true;
      } else {
	get_session(s);
      }
      s->lock.get_write();
      if (mapped_session) {
	_session_op_assign(s, op);
      }
      if (op->should_resend) {
	if (!op->session->is_homeless() && !op->target.paused) {
	  logger->inc(l_osdc_op_resend);
	  _send_op(op);
	}
      } else {
	_cancel_linger_op(op);
      }
      s->lock.unlock();
      put_session(s);
    }
    for (list<LingerOp*>::iterator p = need_resend_linger.begin();
	 p != need_resend_linger.end(); ++p) {
      LingerOp *op = *p;
      if (!op->session) {
	_calc_target(&op->target);
	OSDSession *s = NULL;
	int const r = _get_session(op->target.osd, &s, lc);
	assert(r == 0);
	assert(s != NULL);
	op->session = s;
	put_session(s);
      }
      if (!op->session->is_homeless()) {
	logger->inc(l_osdc_linger_resend);
	_send_linger(op);
      }
    }
    for (map<ceph_tid_t,CommandOp*>::iterator p = need_resend_command.begin();
	 p != need_resend_command.end(); ++p) {
      CommandOp *c = p->second;
      _assign_command_session(c);
      if (c->session && !c->session->is_homeless()) {
	_send_command(c);
      }
    }

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
      int r = _maybe_request_map();
      assert(r == 0);
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

    RWLock::WLocker wl(objecter->rwlock);

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

    objecter->_check_op_vol_dne(op, false);

    op->put();
  }

  void Objecter::_check_op_vol_dne(Op *op, bool session_locked)
  {
    assert(rwlock.is_wlocked());

    ldout(cct, 10) << "check_op_vol_dne tid " << op->tid
		   << " current " << osdmap->get_epoch()
		   << " map_dne_bound " << op->map_dne_bound
		   << dendl;
    if (op->map_dne_bound > 0) {
      if (osdmap->get_epoch() >= op->map_dne_bound) {
	// we had a new enough map
	ldout(cct, 10) << "check_op_pool_dne tid " << op->tid
		       << " concluding volume " << op->target.volume << " dne"
		       << dendl;
	if (op->onack) {
	  op->onack->complete(-ENXIO);
	}
	if (op->oncommit) {
	  op->oncommit->complete(-ENXIO);
	}

	OSDSession *s = op->session;
	assert(s != NULL);

	if (!session_locked) {
	  s->lock.get_write();
	}
	_finish_op(op);
	if (!session_locked) {
	  s->lock.unlock();
	}
      }
    } else {
      _send_op_map_check(op);
    }
  }

  void Objecter::_send_op_map_check(Op *op)
  {
    assert(rwlock.is_wlocked());
    // ask the monitor
    if (check_latest_map_ops.count(op->tid) == 0) {
      op->get();
      check_latest_map_ops[op->tid] = op;
      C_Op_Map_Latest *c = new C_Op_Map_Latest(this, op->tid);
      monc->get_version("osdmap", &c->latest, NULL, c);
    }
  }

  void Objecter::_op_cancel_map_check(Op *op)
  {
    assert(rwlock.is_wlocked());
    map<ceph_tid_t, Op*>::iterator iter =
      check_latest_map_ops.find(op->tid);
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

    RWLock::WLocker wl(objecter->rwlock);

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
    objecter->_check_linger_vol_dne(op, &unregister);

    if (unregister) {
      objecter->_unregister_linger(op->linger_id);
    }

    op->put();
  }

  void Objecter::_check_linger_vol_dne(LingerOp *op, bool *need_unregister)
  {
    assert(rwlock.is_wlocked());

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
    assert(rwlock.is_wlocked());

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
   * @returns 0 on success, or -EAGAIN if the lock context requires
   * promotion to write.
   */
  int Objecter::_get_session(int osd, OSDSession **session,
			     RWLock::Context& lc)
  {
    assert(rwlock.is_locked());

    if (osd < 0) {
      *session = homeless_session;
      ldout(cct, 20) << __func__ << " osd=" << osd << " returning homeless"
		     << dendl;
      return 0;
    }

    map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
    if (p != osd_sessions.end()) {
      OSDSession *s = p->second;
      s->get();
      *session = s;
      ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		     << s->get_nref() << dendl;
      return 0;
    }
    if (!lc.is_wlocked()) {
      return -EAGAIN;
    }
    OSDSession *s = new OSDSession(cct, osd);
    osd_sessions[osd] = s;
    s->con = messenger->get_connection(osdmap->get_inst(osd));
    s->get();
    *session = s;
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		   << s->get_nref() << dendl;
    return 0;
  }

  void Objecter::put_session(Objecter::OSDSession *s)
  {
    if (s && !s->is_homeless()) {
      ldout(cct, 20) << __func__ << " s=" << s << " osd=" << s->osd << " "
		     << s->get_nref() << dendl;
      s->put();
    }
  }

  void Objecter::get_session(Objecter::OSDSession *s)
  {
    assert(s != NULL);

    if (!s->is_homeless()) {
      ldout(cct, 20) << __func__ << " s=" << s << " osd=" << s->osd << " "
		     << s->get_nref() << dendl;
      s->get();
    }
  }

  void Objecter::_reopen_session(OSDSession *s)
  {
    assert(s->lock.is_locked());

    entity_inst_t inst = osdmap->get_inst(s->osd);
    ldout(cct, 10) << "reopen_session osd." << s->osd
		   << " session, addr now " << inst << dendl;
    if (s->con) {
      s->con->mark_down();
    }
    s->con = messenger->get_connection(inst);
    s->incarnation++;
  }

  void Objecter::close_session(OSDSession *s)
  {
    assert(rwlock.is_wlocked());

    ldout(cct, 10) << "close_session for osd." << s->osd << dendl;
    if (s->con) {
      s->con->mark_down();
    }
    s->lock.get_write();

    std::list<LingerOp*> homeless_lingers;
    std::list<CommandOp*> homeless_commands;
    std::list<Op*> homeless_ops;

    while(!s->linger_ops.empty()) {
      std::map<uint64_t, LingerOp*>::iterator i = s->linger_ops.begin();
      ldout(cct, 10) << " linger_op " << i->first << dendl;
      _session_linger_op_remove(s, i->second);
      homeless_lingers.push_back(i->second);
    }

    while(!s->ops.empty()) {
      std::map<ceph_tid_t, Op*>::iterator i = s->ops.begin();
      ldout(cct, 10) << " op " << i->first << dendl;
      _session_op_remove(s, i->second);
      homeless_ops.push_back(i->second);
    }

    osd_sessions.erase(s->osd);
    s->lock.unlock();
    put_session(s);

    // Assign any leftover ops to the homeless session
    {
      RWLock::WLocker wl(homeless_session->lock);
      for (std::list<LingerOp*>::iterator i = homeless_lingers.begin();
	   i != homeless_lingers.end(); ++i) {
	_session_linger_op_assign(homeless_session, *i);
      }
      for (std::list<Op*>::iterator i = homeless_ops.begin();
	   i != homeless_ops.end(); ++i) {
	_session_op_assign(homeless_session, *i);
      }
    }
  }

  void Objecter::wait_for_osd_map()
  {
    rwlock.get_write();
    if (osdmap->get_epoch()) {
      rwlock.put_write();
      return;
    }

    Mutex lock;
    Cond cond;
    bool done;
    lock.Lock();
    C_SafeCond *context = new C_SafeCond(&lock, &cond, &done, NULL);
    waiting_for_map[0].push_back(pair<Context*, int>(context, 0));
    rwlock.put_write();
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
    RWLock::WLocker wl(rwlock);
    _get_latest_version(oldest, newest, fin);
  }

  void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest,
				     Context *fin)
  {
    assert(rwlock.is_wlocked());
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
    RWLock::RLocker rl(rwlock);
    int r;
    do {
      r = _maybe_request_map();
    } while (r == -EAGAIN);
  }

  int Objecter::_maybe_request_map()
  {
    assert(rwlock.is_locked());
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
    return 0;
  }


  void Objecter::_wait_for_new_map(Context *c, epoch_t epoch, int err)
  {
    assert(rwlock.is_wlocked());
    waiting_for_map[epoch].push_back(pair<Context *, int>(c, err));
    int r = _maybe_request_map();
    assert(r == 0);
  }

  bool Objecter::wait_for_map(epoch_t epoch, Context *c, int err)
  {
    RWLock::WLocker wl(rwlock);
    if (osdmap->get_epoch() >= epoch) {
      return true;
    }
    _wait_for_new_map(c, epoch, err);
    return false;
  }

  void Objecter::kick_requests(OSDSession *session)
  {
    ldout(cct, 10) << "kick_requests for osd." << session->osd << dendl;

    map<uint64_t, LingerOp *> lresend;
    RWLock::WLocker wl(rwlock);

    session->lock.get_write();
    _kick_requests(session, lresend);
    session->lock.unlock();

    _linger_ops_resend(lresend);
  }

  void Objecter::_kick_requests(OSDSession *session,
				map<uint64_t, LingerOp *>& lresend)
  {
    assert(rwlock.is_locked());

    // resend ops
    map<ceph_tid_t,Op*> resend;  // resend in tid order


    for (auto &subop : session->subops) {
      if (subop.parent.should_resend) {
	if (!subop.parent.paused)
	  if (!op->paused)
	    resend[subop.parent.tid] = &subop.parent;
      } else {
	_cancel_linger_op(&subop.parent);
      }
    }
    while (!resend.empty()) {
      _send_op(resend.begin()->second);
      resend.erase(resend.begin());
    }

    // resend lingers
    for (auto j = session->linger_ops.begin();
	 j != session->linger_ops.end(); ++j) {
      LingerOp *op = j->second;
      op->get();
      assert(lresend.count(j->first) == 0);
      lresend[j->first] = op;
    }
  }

  void Objecter::_linger_ops_resend(map<uint64_t, LingerOp *>& lresend)
  {
    assert(rwlock.is_locked());

    while (!lresend.empty()) {
      LingerOp *op = lresend.begin()->second;
      if (!op->canceled) {
	_send_linger(op);
      }
      op->put();
      lresend.erase(lresend.begin());
    }
  }

  void Objecter::schedule_tick()
  {
    Mutex::Locker l(timer_lock);
    assert(tick_event == NULL);
    tick_event = new C_Tick(this);
    timer.add_event_after(cct->_conf->objecter_tick_interval, tick_event);
  }

  void Objecter::tick()
  {
    RWLock::RLocker rl(rwlock);

    ldout(cct, 10) << "tick" << dendl;

    // we are only called by C_Tick
    assert(tick_event);
    tick_event = NULL;

    if (!initialized) {
      // we raced with shutdown
      return;
    }

    set<OSDSession*> toping;

    int r = 0;

    // look for laggy requests
    utime_t cutoff = ceph_clock_now(cct);
    cutoff -= cct->_conf->objecter_timeout;  // timeout

    unsigned laggy_ops;

    do {
      laggy_ops = 0;
      for (auto siter : osd_sessions) {
	OSDSession *s = siter.second;
	for (auto& kv : subops) {
	  SubOp* subop = kv.second;
	  assert(subop->session);
	  if (subop->session && subop->stamp < cutoff) {
	    ldout(cct, 2) << " tid " << kv.first << " on osd."
			  << subop->session->osd << " is laggy" << dendl;
	    toping.insert(subop->session);
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
      }
#endif
      if (num_homeless_ops || !toping.empty()) {
	r = _maybe_request_map();
	if (r == -EAGAIN) {
	  toping.clear();
	}
      }
    } while (r == -EAGAIN);

    if (!toping.empty()) {
      // send a ping to these osds, to ensure we detect any session resets
      // (osd reply message policy is lossy)
      for (set<OSDSession*>::const_iterator i = toping.begin();
	   i != toping.end();
	   ++i) {
	(*i)->con->send_message(new MPing);
      }
    }

    // reschedule
    schedule_tick();
  }

  void Objecter::resend_mon_ops()
  {
    RWLock::WLocker wl(rwlock);

    ldout(cct, 10) << "resend_mon_ops" << dendl;

    for (auto kv : statfs_ops) {
      _fs_stats_submit(kv.second);
    }

    for (auto kv : check_latest_map_ops) {
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
    RWLock::RLocker rl(rwlock);
    RWLock::Context lc(rwlock, RWLock::Context::TakenForRead);
    return _op_submit_with_budget(op, lc, ctx_budget);
  }

  ceph_tid_t Objecter::_op_submit_with_budget(Op *op, RWLock::Context& lc,
					      int *ctx_budget)
  {
    assert(initialized.read());

    assert(op->ops.size() == op->out_bl.size());
    assert(op->ops.size() == op->out_rval.size());
    assert(op->ops.size() == op->out_handler.size());

    // throttle.  before we look at any state, because
    // take_op_budget() may drop our lock while it blocks.
    if (!op->ctx_budgeted || (ctx_budget && (*ctx_budget == -1))) {
      int op_budget = _take_op_budget(op);
      // take and pass out the budget for the first OP
      // in the context session
      if (ctx_budget && (*ctx_budget == -1)) {
	*ctx_budget = op_budget;
      }
    }

    ceph_tid_t tid = _op_submit(op, lc);

    if (osd_timeout > 0) {
      Mutex::Locker l(timer_lock);
      op->ontimeout = new C_CancelOp(tid, this);
      timer.add_event_after(osd_timeout, op->ontimeout);
    }

    return tid;
  }

  ceph_tid_t Objecter::_op_submit(Op *op, RWLock::Context& lc)
  {
    assert(rwlock.is_locked());

    op->trace.keyval("tid", op->tid);
    op->trace.event("op_submit", &trace_endpoint);

    ldout(cct, 10) << __func__ << " op " << op << dendl;

    // pick target

    // This function no longer creates the session
    const bool check_for_latest_map = _calc_target(&op->target)
      == RECALC_OP_TARGET_VOL_DNE;

#if MULTI
    // Try to get a session, including a retry if we need to take write lock
    int r = _get_session(op->target.osd, &s, lc);
    if (r == -EAGAIN) {
      assert(s == NULL);
      lc.promote();
      r = _get_session(op->target.osd, &s, lc);
    }
    assert(r == 0);
    assert(s);  // may be homeless
#endif

    // We may need to take wlock if we will need to _set_op_map_check later.
    if (check_for_latest_map && !lc.is_wlocked()) {
      lc.promote();
    }

    //inflight_ops[op->tid] = op;
    ++inflight_ops;

    // add to gather set(s)
    if (op->onack) {
      num_unacked.inc();
    } else {
      ldout(cct, 20) << " note: not requesting ack" << dendl;
    }
    if (op->oncommit) {
      num_uncommitted.inc();
    } else {
      ldout(cct, 20) << " note: not requesting commit" << dendl;
    }

    // send?
    ldout(cct, 10) << "_op_submit oid " << op->target.base_oid
		   << " " << op->target.base_oloc << " "
		   << op->target.target_oloc
		   << " " << op->ops << " tid " << op->tid
		   << " osd." << (!s->is_homeless() ? s->osd : -1)
		   << dendl;

    assert(op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

    bool need_send = false;

    if ((op->target.flags & CEPH_OSD_FLAG_WRITE) &&
	osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
      ldout(cct, 10) << " paused modify " << op << " tid " << last_tid
		     << dendl;
      op->paused = true;
      _maybe_request_map();
    } else if ((op->target.flags & CEPH_OSD_FLAG_READ) &&
	       osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
      ldout(cct, 10) << " paused read " << op << " tid " << last_tid << dendl;
      op->target.paused = true;
      _maybe_request_map();
    } else if ((op->target.flags & CEPH_OSD_FLAG_WRITE) &&
	       osdmap_full_flag()) {
      ldout(cct, 0) << " FULL, paused modify " << op << " tid " << last_tid
		    << dendl;
      op->target.paused = true;
      _maybe_request_map();
    } else if (!s->is_homeless()) {
      need_send = true;
    } else {
      _maybe_request_map();
    }

    MOSDOp *m = NULL;
    if (need_send) {
      m = _prepare_osd_op(op);
    }

    s->lock.get_write();
    if (op->tid == 0)
      op->tid = last_tid.inc();
    _session_op_assign(s, op);

#if MULTI
    if (all_of(op->subops.cbegin(),
	       op->subops.cend(),
	       [](const SubOp& s){return s.session;})) {
      send_op(op);
    }
#endif MULTI

    if (need_send) {
      _send_op(op, m);
    }

    // Last chance to touch Op here, after giving up session lock it can be
    // freed at any time by response handler.
    ceph_tid_t tid = op->tid;
    if (check_for_latest_map) {
      _send_op_map_check(op);
    }
    op = NULL;

    s->lock.unlock();
    put_session(s);

    ldout(cct, 5) << num_unacked.read() << " unacked, " << num_uncommitted
		  << " uncommitted" << dendl;

    return tid;
  }

  int Objecter::op_cancel(OSDSession *s, ceph_tid_t tid, int r)
  {
    assert(initialized);

    s->lock.get_write();

    map<ceph_tid_t, Op*>::iterator p = s->ops.find(tid);
    if (p == s->ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
      return -ENOENT;
    }

    if (s->con) {
      ldout(cct, 20) << " revoking rx buffer for " << tid
		     << " on " << s->con << dendl;
      s->con->revoke_rx_buffer(tid);
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
    _op_cancel_map_check(op);
    _finish_op(op);
    s->lock.unlock();

    return 0;
  }

  int Objecter::op_cancel(ceph_tid_t tid, int r)
  {
    int ret = 0;

    rwlock.get_write();

  start:

    for (auto &s = osd_sessions) {
      s.lock.get_read();
      if (s.ops.find(tid) != s.ops.end()) {
	s.lock.unlock();
	ret = op_cancel(s, tid, r);
	if (ret == -ENOENT) {
	  /* oh no! raced, maybe tid moved to another session, restarting */
	  goto start;
	}
	rwlock.unlock();
	return ret;
      }
      s.lock.unlock();
    }

    // Handle case where the op is in homeless session
    homeless_session->lock.get_read();
    if (homeless_session->ops.find(tid) != homeless_session->ops.end()) {
      homeless_session->lock.unlock();
      ret = op_cancel(homeless_session, tid, r);
      if (ret == -ENOENT) {
	/* oh no! raced, maybe tid moved to another session, restarting */
	goto start;
      } else {
	rwlock.unlock();
	return ret;
      }
    } else {
      homeless_session->lock.unlock();
    }

    rwlock.unlock();

    return ret;
  }

  bool Objecter::target_should_be_paused(op_base *t)
  {
    bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
    bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) ||
      osdmap_full_flag();

    return (t->flags & CEPH_OSD_FLAG_READ && pauserd) ||
      (t->flags & CEPH_OSD_FLAG_WRITE && pausewr);
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

  int Objecter::_calc_target(op_base *t, bool any_change)
  {
    assert(rwlock.is_locked());

    bool is_read = t->flags & CEPH_OSD_FLAG_READ;
    bool is_write = t->flags & CEPH_OSD_FLAG_WRITE;
    bool need_resend = false;

    if (!osdmap->vol_exists(op_base->volume.id)) {
      return RECALC_OP_TARGET_VOL_DNE;
    }

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

  int Objecter::_map_session(op_target_t *target, OSDSession **s,
			     RWLock::Context& lc)
  {
    int r = _calc_target(target);
    if (r < 0) {
      return r;
    }
    return _get_session(target->osd, s, lc);
  }

  void Objecter::_session_op_assign(OSDSession *to, Op *op)
  {
    assert(to->lock.is_locked());
    assert(op->session == NULL);
    assert(op->tid);

    get_session(to);
    op->session = to;
    to->ops[op->tid] = op;

    if (to->is_homeless()) {
      num_homeless_ops.inc();
    }

    ldout(cct, 15) << __func__ << " " << to->osd << " " << op->tid << dendl;
  }

  void Objecter::_session_op_remove(OSDSession *from, Op *op)
  {
    assert(op->session == from);
    assert(from->lock.is_locked());

    if (from->is_homeless()) {
      --num_homeless_ops;
    }

    from->ops.erase(op->tid);
    put_session(from);
    op->session = NULL;

    ldout(cct, 15) << __func__ << " " << from->osd << " " << op->tid << dendl;
  }

  void Objecter::_session_linger_op_assign(OSDSession *to, LingerOp *op)
  {
    assert(to->lock.is_wlocked());
    assert(op->session == NULL);

    if (to->is_homeless()) {
      num_homeless_ops.inc();
    }

    get_session(to);
    op->session = to;
    to->linger_ops[op->linger_id] = op;

    ldout(cct, 15) << __func__ << " " << to->osd << " " << op->linger_id
		   << dendl;
  }

  void Objecter::_session_linger_op_remove(OSDSession *from, LingerOp *op)
  {
    assert(from == op->session);
    assert(from->lock.is_locked());

    if (from->is_homeless()) {
      num_homeless_ops.dec();
    }

    from->linger_ops.erase(op->linger_id);
    put_session(from);
    op->session = NULL;

    ldout(cct, 15) << __func__ << " " << from->osd << " " << op->linger_id
		   << dendl;
  }

  int Objecter::_get_osd_session(int osd, RWLock::Context& lc,
				 OSDSession **psession)
  {
    int r;
    do {
      r = _get_session(osd, psession, lc);
      if (r == -EAGAIN) {
	assert(!lc.is_wlocked());

	if (!_promote_lock_check_race(lc)) {
	  return r;
	}
      }
    } while (r == -EAGAIN);
    assert(r == 0);

    return 0;
  }

  int Objecter::_get_op_target_session(Op *op, RWLock::Context& lc,
				       OSDSession **psession)
  {
    return _get_osd_session(op->target.osd, lc, psession);
  }

  bool Objecter::_promote_lock_check_race(RWLock::Context& lc)
  {
    epoch_t epoch = osdmap->get_epoch();
    lc.promote();
    return (epoch == osdmap->get_epoch());
  }

  int Objecter::_recalc_linger_op_target(LingerOp *linger_op,
					 RWLock::Context& lc)
  {
    assert(rwlock.is_wlocked());

    int r = _calc_target(&linger_op->target, true);
    if (r == RECALC_OP_TARGET_NEED_RESEND) {
      ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
		     << " pgid " << linger_op->target.pgid
		     << " acting " << linger_op->target.acting << dendl;

      OSDSession *s;
      r = _get_osd_session(linger_op->target.osd, lc, &s);
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
  }

  void Objecter::_cancel_linger_op(Op *op)
  {
    ldout(cct, 15) << "cancel_op " << op->tid << dendl;

    assert(!op->should_resend);
    delete op->onack;
    delete op->oncommit;

    _finish_op(op);
  }

  void Objecter::_finish_op(Op *op)
  {
    ldout(cct, 15) << "finish_op " << op->tid << dendl;

    assert(op->session->lock.is_wlocked());

    if (!op->ctx_budgeted && op->budgeted)
      put_op_budget(op);

    if (op->ontimeout) {
      Mutex::Locker l(timer_lock);
      timer.cancel_event(op->ontimeout);
    }

    for(auto& subop : op->subops) {
      subop.unlink();
      subops.erase(subop.tid);
    }


    _session_op_remove(op->session, op);

    assert(check_latest_map_ops.find(op->tid) == check_latest_map_ops.end());

    inflight_ops.dec();

    op->trace.event("finish_op", &trace_endpoint);

    op->put();
  }

  void objecter::finish_op(osdsession *session, ceph_tid_t tid)
  {
    ldout(cct, 15) << "finish_op " << tid << dendl;
    rwlock::rlocker rl(rwlock);

    rwlock::wlocker wl(session->lock);

    map<ceph_tid_t, op *>::iterator iter = session->ops.find(tid);
    if (iter == session->ops.end())
      return;

    op *op = iter->second;

    _finish_op(op);
  }

  MOSDOp *Objecter::_prepare_osd_op(Op *op)
  {
    assert(rwlock.is_locked());

    int flags = op->target.flags;
    flags |= CEPH_OSD_FLAG_KNOWN_REDIR;
    if (op->oncommit)
      flags |= CEPH_OSD_FLAG_ONDISK;
    if (op->onack)
      flags |= CEPH_OSD_FLAG_ACK;

    op->target.paused = false;
    op->stamp = ceph_clock_now(cct);

    MOSDOp *m = new MOSDOp(client_inc.read(), op->tid,
			   op->target.target_oid, op->target.target_oloc,
			   op->target.pgid,
			   osdmap->get_epoch(),
			   flags);

    m->ops = op->ops;
    m->set_mtime(op->mtime);
    m->set_retry_attempt(op->attempts++);

    if (op->replay_version != eversion_t())
      m->set_version(op->replay_version);  // we're replaying this op!

    if (op->priority)
      m->set_priority(op->priority);
    else
      m->set_priority(cct->_conf->osd_client_op_priority);

    return m;
  }


  void Objecter::_send_subop(SubOp &subop, int flags)
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

    subop.parent.trace.keyval("want onack", subop.parent.onack ? 1 : 0);
    subop.parent.trace.keyval("want oncommit", subop.parent.oncommit ? 1 : 0);
    subop.parent.trace.event("send_op", &trace_endpoint);
    m->trace = subop.parent.trace;

    if (subop.replay_version != eversion_t())
      m->set_version(subop.replay_version);	 // we're replaying this op!

    if (subop.parent.op->priority)
      m->set_priority(subop.parent.op->priority);
    else
      m->set_priority(cct->_conf->osd_client_op_priority);

    messenger->send_message(m, subop.session->con);
  }

  void Objecter::_send_op(Op *op, MOSDOp *m)
  {
    assert(rwlock.is_locked());
    assert(op->session->lock.is_locked());

    if (!m) {
      assert(op->tid > 0);
      m = _prepare_osd_op(op);
    }

    ldout(cct, 15) << "_send_op " << op->tid << " to osd."
		   << op->session->osd << dendl;

    ConnectionRef con = op->session->con;
    assert(con);

    op->incarnation = op->session->incarnation;

    m->set_tid(op->tid);

    op->session->con->send_message(m);
  }

  void Objecter::_send_op(Op *op)
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

  int Objecter::calc_op_budget(Op *op)
  {
    int op_budget = 0;
    for (vector<OSDOp>::iterator i = op->ops.begin();
	 i != op->ops.end();
	 ++i) {
      if (i->op.op & CEPH_OSD_OP_MODE_WR) {
	op_budget += i->indata.length();
      } else if (ceph_osd_op_mode_read(i->op.op)) {
	if (ceph_osd_op_type_data(i->op.op)) {
	  if ((int64_t)i->op.extent.length > 0)
	    op_budget += (int64_t)i->op.extent.length;
	} else if (ceph_osd_op_type_attr(i->op.op)) {
	  op_budget += i->op.xattr.name_len + i->op.xattr.value_len;
	}
      }
    }
    return op_budget;
  }

  void Objecter::_throttle_op(Op *op, int op_budget)
  {
    assert(rwlock.is_locked());

    bool locked_for_write = rwlock.is_wlocked();

    if (!op_budget)
      op_budget = calc_op_budget(op);
    if (!op_throttle_bytes.get_or_fail(op_budget)) { //couldn't take right now
      rwlock.unlock();
      op_throttle_bytes.get(op_budget);
      rwlock.get(locked_for_write);
    }
    if (!op_throttle_ops.get_or_fail(1)) { //couldn't take right now
      rwlock.unlock();
      op_throttle_ops.get(1);
      rwlock.get(locked_for_write);
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
    op->session->lock.get_write();
    op->session->ops.erase(op->tid);
    op->session->lock.unlock();
    put_session(op->session);
    op->session = NULL;

    --inflight_ops.dec();
  }


  void Objecter::handle_osd_op_reply(MOSDOpReply *m)
  {
    ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

    // get pio
    ceph_tid_t tid = m->get_tid();

    int osd_num = (int)m->get_source().num();

    RWLock::RLocker l(rwlock);
    if (!initialized.read()) {
      m->put();
      return;
    }

    RWLock::Context lc(rwlock, RWLock::Context::TakenForRead);

    map<int, OSDSession *>::iterator siter = osd_sessions.find(osd_num);
    if (siter == osd_sessions.end()) {
      ldout(cct, 7) << "handle_osd_op_reply " << tid
		    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ?
						  " onnvram":" ack"))
		    << " ... unknown osd" << dendl;
      m->put();
      return;
    }

    OSDSession *s = siter->second;
    get_session(s);

    s->lock.get_write();

    map<ceph_tid_t, Op *>::iterator iter = s->ops.find(tid);
    if (iter == s->ops.end()) {
      ldout(cct, 7) << "handle_osd_op_reply " << tid
		    << (m->is_ondisk() ? " ondisk" : (m->is_onnvram()
						    ? " onnvram" : " ack"))
		    << " ... stray" << dendl;
      s->lock.unlock();
      put_session(s);
      m->put();
      return;
    }

    ldout(cct, 7) << "handle_osd_op_reply " << tid
		  << (m->is_ondisk() ? " ondisk" : (m->is_onnvram() ?
						  " onnvram" : " ack"))
		  << " v " << m->get_replay_version() << " uv "
		  << m->get_user_version()
		  << " in " << m->get_pg()
		  << " attempt " << m->get_retry_attempt()
		  << dendl;
    Op *op = iter->second;

    if (m->get_retry_attempt() >= 0) {
      if (m->get_retry_attempt() != (op->attempts - 1)) {
	ldout(cct, 7) << " ignoring reply from attempt "
		      << m->get_retry_attempt()
		      << " from " << m->get_source_inst()
		      << "; last attempt " << (op->attempts - 1) << " sent to "
		      << op->session->con->get_peer_addr() << dendl;
	m->put();
	s->lock.unlock();
	put_session(s);
	return;
      }
    } else {
      // we don't know the request attempt because the server is old,
      // so just accept this one.  we may do ACK callbacks we
      // shouldn't have, but that is better than doing callbacks out
      // of order.
    }

    Context *onack = 0;
    Context *oncommit = 0;

    int rc = m->get_result();

    if (m->is_redirect_reply()) {
      ldout(cct, 5) << " got redirect reply; redirecting" << dendl;
      if (op->onack)
	num_unacked.dec();
      if (op->oncommit)
	num_uncommitted.dec();
      _session_op_remove(s, op);
      s->lock.unlock();
      put_session(s);

      // FIXME: two redirects could race and reorder

      op->tid = 0;
      m->get_redirect().combine_with_locator(op->target.target_oloc,
					   op->target.target_oid.name);
      op->target.flags |= CEPH_OSD_FLAG_REDIRECTED;
      _op_submit(op, lc);
      m->put();
      return;
    }

    if (rc == -EAGAIN) {
      ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;

      // new tid
      s->ops.erase(op->tid);
      op->tid = last_tid.inc();

      _send_op(op);
      s->lock.unlock();
      put_session(s);
      m->put();
      return;
    }

    l.unlock();
    lc.set_state(RWLock::Context::Untaken);

    if (op->objver)
      *op->objver = m->get_user_version();
    if (op->reply_epoch)
      *op->reply_epoch = m->get_map_epoch();

    // per-op result demuxing
    vector<OSDOp> out_ops;
    m->claim_ops(out_ops);

    if (out_ops.size() != op->ops.size())
      ldout(cct, 0) << "WARNING: tid " << op->tid << " reply ops " << out_ops
		    << " != request ops " << op->ops
		    << " from " << m->get_source_inst() << dendl;

    vector<bufferlist*>::iterator pb = op->out_bl.begin();
    vector<int*>::iterator pr = op->out_rval.begin();
    vector<Context*>::iterator ph = op->out_handler.begin();
    assert(op->out_bl.size() == op->out_rval.size());
    assert(op->out_bl.size() == op->out_handler.size());
    vector<OSDOp>::iterator p = out_ops.begin();
    for (unsigned i = 0;
	 p != out_ops.end() && pb != op->out_bl.end();
	 ++i, ++p, ++pb, ++pr, ++ph) {
      ldout(cct, 10) << " op " << i << " rval " << p->rval
		     << " len " << p->outdata.length() << dendl;
      if (*pb)
	**pb = p->outdata;
      // set rval before running handlers so that handlers
      // can change it if e.g. decoding fails
      if (*pr)
	**pr = p->rval;
      if (*ph) {
	ldout(cct, 10) << " op " << i << " handler " << *ph << dendl;
	(*ph)->complete(p->rval);
	*ph = NULL;
      }
    }

    // ack|commit -> ack
    if (op->onack) {
      ldout(cct, 15) << "handle_osd_op_reply ack" << dendl;
      op->replay_version = m->get_replay_version();
      onack = op->onack;
      op->onack = 0;  // only do callback once
      num_unacked.dec();
    }
    if (op->oncommit && (m->is_ondisk() || rc)) {
      ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
      oncommit = op->oncommit;
      op->oncommit = 0;
      num_uncommitted.dec();
    }

    // got data?
    if (op->outbl) {
      m->claim_data(*op->outbl);
      op->outbl = 0;
    }

    /* get it before we call _finish_op() */
    Mutex *completion_lock = (op->target.base_oid.name.size() ?
			      s->get_lock(op->target.base_oid) : NULL);

    // done with this tid?
    if (!op->onack && !op->oncommit) {
      ldout(cct, 15) << "handle_osd_op_reply completed tid " << tid << dendl;
      _finish_op(op);
    }

    ldout(cct, 5) << num_unacked.read() << " unacked, "
		  << num_uncommitted << " uncommitted" << dendl;

    // serialize completions
    if (completion_lock) {
      completion_lock->Lock();
    }
    s->lock.unlock();

    // do callbacks
    if (onack) {
      onack->complete(rc);
    }
    if (oncommit) {
      oncommit->complete(rc);
    }
    if (completion_lock) {
      completion_lock->Unlock();
    }

    m->put();
    put_session(s);
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
      subop->parent.trace.event("reply with EAGAIN", &trace_endpoint);
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
        subop->parent.trace.event("onack", &trace_endpoint);
      }
      if (subop->parent.oncommit && (m->is_ondisk() || rc)) {
	ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
	oncommit = subop->parent.oncommit;
	subop->parent.oncommit = 0;
	num_uncommitted--;
        subop->parent.trace.event("oncommit", &trace_endpoint);
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
    C_CancelStatfsOp(ceph_tid_t tid, Objecter *objecter)
      : tid(tid), objecter(objecter) {}
    void finish(int r) {
      objecter->statfs_op_cancel(tid, -ETIMEDOUT);
    }
  };

  void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
  {
    ldout(cct, 10) << "get_fs_stats" << dendl;
    RWLock::WLocker l(rwlock);

    StatfsOp *op = new StatfsOp;
    op->tid = last_tid.inc();
    op->stats = &result;
    op->onfinish = onfinish;
    op->ontimeout = NULL;
    if (mon_timeout > 0) {
      Mutex::Locker l(timer_lock);
      op->ontimeout = new C_CancelStatfsOp(op->tid, this);
      timer.add_event_after(mon_timeout, op->ontimeout);
    }
    statfs_ops[op->tid] = op;

    _fs_stats_submit(op);
  }

  void Objecter::_fs_stats_submit(StatfsOp *op)
  {
    assert(rwlock.is_wlocked());

    ldout(cct, 10) << "fs_stats_submit" << op->tid << dendl;
    monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid,
				       last_seen_pgmap_version));
    op->last_submit = ceph_clock_now(cct);

    logger->inc(l_osdc_statfs_send);
  }

  void Objecter::handle_fs_stats_reply(MStatfsReply *m)
  {
    RWLock::WLocker wl(rwlock);
    if (!initialized.read()) {
      m->put();
      return;
    }

    ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
    ceph_tid_t tid = m->get_tid();

    if (statfs_ops.count(tid)) {
      StatfsOp *op = statfs_ops[tid];
      ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
      *(op->stats) = m->h.st;
      if (m->h.version > last_seen_pgmap_version)
	last_seen_pgmap_version = m->h.version;
      op->onfinish->complete(0);
      _finish_statfs_op(op);
    } else {
      ldout(cct, 10) << "unknown request " << tid << dendl;
    }
    m->put();
    ldout(cct, 10) << "done" << dendl;
  }

  int Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
  {
    assert(initialized.read());

    RWLock::WLocker wl(rwlock);

    map<ceph_tid_t, StatfsOp*>::iterator it = statfs_ops.find(tid);
    if (it == statfs_ops.end()) {
      ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
      return -ENOENT;
    }

    ldout(cct, 10) << __func__ << " tid " << tid << dendl;

    StatfsOp *op = it->second;
    if (op->onfinish)
      op->onfinish->complete(r);
    _finish_statfs_op(op);
    return 0;
  }

  void Objecter::_finish_statfs_op(StatfsOp *op)
  {
    assert(rwlock.is_wlocked());

    statfs_ops.erase(op->tid);

    if (op->ontimeout) {
      Mutex::Locker l(timer_lock);
      timer.cancel_event(op->ontimeout);
    }

    delete op;
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
	rwlock.get_write();
	if (!initialized.read()) {
	  rwlock.put_write();
	  return false;
	}
	map<int,OSDSession*>::iterator p = osd_sessions.find(osd);
	if (p != osd_sessions.end()) {
	  OSDSession *session = p->second;
	  map<uint64_t, LingerOp *> lresend;
	  session->lock.get_write();
	  _reopen_session(session);
	  _kick_requests(session, lresend);
	  session->lock.unlock();
	  _linger_ops_resend(lresend);
	  rwlock.unlock();
	  maybe_request_map();
	} else {
	  rwlock.unlock();
	}
      } else {
	ldout(cct, 10) << "ms_handle_reset on unknown osd addr " << con->get_peer_addr() << dendl;
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
    assert(ops.empty());
    assert(linger_ops.empty());

    for (int i = 0; i < num_locks; i++) {
      delete completion_locks[i];
    }
    delete[] completion_locks;
  }

  Objecter::~Objecter()
  {
    delete osdmap;

    assert(homeless_session->get_nref() == 1);
    assert(num_homeless_ops == 0);
    homeless_session->put();

    assert(osd_sessions.empty());
    assert(statfs_ops.empty());
    assert(pool_ops.empty());
    assert(waiting_for_map.empty());
    assert(linger_ops.empty());
    assert(check_latest_map_lingers.empty());
    assert(check_latest_map_ops.empty());

    assert(!tick_event);
    assert(!m_request_state_hook);
  }
};
