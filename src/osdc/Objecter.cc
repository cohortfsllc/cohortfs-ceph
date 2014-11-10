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


enum {
  l_osdc_first = 123200,
  l_osdc_op_active,
  l_osdc_op_laggy,
  l_osdc_op_send,
  l_osdc_op_send_bytes,
  l_osdc_op_resend,
  l_osdc_op_ack,
  l_osdc_op_commit,

  l_osdc_op,
  l_osdc_op_r,
  l_osdc_op_w,
  l_osdc_op_rmw,
  l_osdc_op_pg,

  l_osdc_osdop_stat,
  l_osdc_osdop_create,
  l_osdc_osdop_read,
  l_osdc_osdop_write,
  l_osdc_osdop_writefull,
  l_osdc_osdop_append,
  l_osdc_osdop_zero,
  l_osdc_osdop_truncate,
  l_osdc_osdop_delete,
  l_osdc_osdop_sparse_read,
  l_osdc_osdop_clonerange,
  l_osdc_osdop_getxattr,
  l_osdc_osdop_setxattr,
  l_osdc_osdop_cmpxattr,
  l_osdc_osdop_rmxattr,
  l_osdc_osdop_resetxattrs,
  l_osdc_osdop_tmap_up,
  l_osdc_osdop_tmap_put,
  l_osdc_osdop_tmap_get,
  l_osdc_osdop_call,
  l_osdc_osdop_watch,
  l_osdc_osdop_notify,
  l_osdc_osdop_src_cmpxattr,
  l_osdc_osdop_pgls,
  l_osdc_osdop_pgls_filter,
  l_osdc_osdop_other,

  l_osdc_linger_active,
  l_osdc_linger_send,
  l_osdc_linger_resend,

  l_osdc_statfs_active,
  l_osdc_statfs_send,
  l_osdc_statfs_resend,

  l_osdc_command_active,
  l_osdc_command_send,
  l_osdc_command_resend,

  l_osdc_map_epoch,
  l_osdc_map_full,
  l_osdc_map_inc,

  l_osdc_osd_sessions,
  l_osdc_osd_session_open,
  l_osdc_osd_session_close,
  l_osdc_osd_laggy,
  l_osdc_last,
};


// config obs ----------------------------

static const char *config_keys[] = {
  "crush_location",
  NULL
};

const char** Objecter::get_tracked_conf_keys() const
{
  return config_keys;
}

// messages ------------------------------

void Objecter::init_unlocked()
{
  assert(!initialized);


  m_request_state_hook = new RequestStateHook(this);
  AdminSocket* admin_socket = cct->get_admin_socket();
  int ret = admin_socket->register_command("objecter_requests",
					   "objecter_requests",
					   m_request_state_hook,
					   "show in-progress osd requests");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(ret) << dendl;
  }
}

void Objecter::init_locked()
{
  assert(client_lock.is_locked());
  assert(!initialized);

  schedule_tick();
  if (osdmap->get_epoch() == 0)
    maybe_request_map();

  initialized = true;
}

void Objecter::shutdown_locked()
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

void Objecter::shutdown_unlocked()
{
  if (m_request_state_hook) {
    AdminSocket* admin_socket = cct->get_admin_socket();
    admin_socket->unregister_command("objecter_requests");
    delete m_request_state_hook;
    m_request_state_hook = NULL;
  }
}

void Objecter::send_linger(LingerOp *info)
{
  ldout(cct, 15) << "send_linger " << info->linger_id << dendl;
  vector<OSDOp> opv = info->ops; // need to pass a copy to ops
  Context *onack = (!info->registered && info->on_reg_ack) ? new C_Linger_Ack(this, info) : NULL;
  Context *oncommit = new C_Linger_Commit(this, info);
  Op *o = new Op(info->target.oid, info->target.volume,
		 opv, info->target.flags | CEPH_OSD_FLAG_READ,
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
  map<uint64_t, LingerOp*>::iterator iter = linger_ops.find(linger_id);
  if (iter != linger_ops.end()) {
    LingerOp *info = iter->second;
    info->session_item.remove_myself();
    linger_ops.erase(iter);
    info->put();
  }
}

ceph_tid_t Objecter::linger_mutate(const object_t& oid, VolumeRef volume,
				   ObjectOperation& op,
				   utime_t mtime,
				   bufferlist& inbl, int flags,
				   Context *onack, Context *oncommit,
				   version_t *objver)
{
  LingerOp *info = new LingerOp;
  info->target.oid = oid;
  info->target.volume = volume->id;
  info->mtime = mtime;
  info->target.flags = flags | CEPH_OSD_FLAG_WRITE;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = NULL;
  info->pobjver = objver;
  info->on_reg_ack = onack;
  info->on_reg_commit = oncommit;

  info->linger_id = ++max_linger_id;
  linger_ops[info->linger_id] = info;

  send_linger(info);

  return info->linger_id;
}

ceph_tid_t Objecter::linger_read(const object_t& oid, VolumeRef volume,
				 ObjectOperation& op, bufferlist& inbl,
				 bufferlist *poutbl, int flags,
				 Context *onfinish, version_t *objver)
{
  LingerOp *info = new LingerOp;
  info->target.oid = oid;
  info->target.volume = volume->id;
  info->target.flags = flags;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = poutbl;
  info->pobjver = objver;
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

  // check for changed request mappings
  map<ceph_tid_t,Op*>::iterator p = ops.begin();
  while (p != ops.end()) {
    Op *op = p->second;
    ++p;
    ldout(cct, 10) << " checking op " << op->tid << dendl;
    int r = recalc_op_target(op);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!force_resend &&
	  (!force_resend_writes || !(op->target.flags & CEPH_OSD_FLAG_WRITE)))
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
      if (op->session && !op->target.paused) {
	send_op(op);
      }
    } else {
      cancel_linger_op(op);
    }
  }
  for (list<LingerOp*>::iterator p = need_resend_linger.begin(); p != need_resend_linger.end(); ++p) {
    LingerOp *op = *p;
    if (op->session) {
      send_linger(op);
    }
  }

  dump_active();

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
    s->con = nullptr;
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
  for (xlist<Op*>::iterator p = s->ops.begin(); !p.end();) {
    Op *op = *p;
    ++p;
    assert(!op->session || op->session == s);
    op->session_item.remove_myself();	// yes?
    op->session = 0;
  }
  s->ops.clear();
  s->linger_ops.clear();
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
  for (xlist<Op*>::iterator p = session->ops.begin(); !p.end();) {
    Op *op = *p;
    ++p;
    if (op->should_resend) {
      if (!op->target.paused)
	resend[op->tid] = op;
    } else {
      cancel_linger_op(op);
    }
  }
  while (!resend.empty()) {
    send_op(resend.begin()->second);
    resend.erase(resend.begin());
  }

  // resend lingers
  map<uint64_t, LingerOp*> lresend;  // resend in order
  for (xlist<LingerOp*>::iterator j = session->linger_ops.begin(); !j.end(); ++j) {
    lresend[(*j)->linger_id] = *j;
  }
  while (!lresend.empty()) {
    send_linger(lresend.begin()->second);
    lresend.erase(lresend.begin());
  }
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
  for (map<ceph_tid_t,Op*>::iterator p = ops.begin();
       p != ops.end();
       ++p) {
    Op *op = p->second;
    if (op->session && op->stamp < cutoff) {
      ldout(cct, 2) << " tid " << p->first << " on osd." << op->session->osd << " is laggy" << dendl;
      toping.insert(op->session);
      ++laggy_ops;
    }
  }
  for (map<uint64_t,LingerOp*>::iterator p = linger_ops.begin();
       p != linger_ops.end();
       ++p) {
    LingerOp *op = p->second;
    if (op->session) {
      ldout(cct, 10) << " pinging osd that serves lingering tid " << p->first << " (osd." << op->session->osd << ")" << dendl;
      toping.insert(op->session);
    } else {
      ldout(cct, 10) << " lingering tid " << p->first << " does not have session" << dendl;
    }
  }

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

  assert(op->ops.size() == op->out_bl.size());
  assert(op->ops.size() == op->out_rval.size());
  assert(op->ops.size() == op->out_handler.size());

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
  ceph_tid_t mytid = ++last_tid;
  op->tid = mytid;
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
  ops[op->tid] = op;

  // send?
  ldout(cct, 10) << "op_submit oid " << op->target.oid
		 << " " << op->target.volume << " " << op->ops
		 << " tid " << op->tid << " osd."
		 << (op->session ? op->session->osd : -1)
		 << dendl;

  assert(op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

  if ((op->target.flags & CEPH_OSD_FLAG_WRITE) &&
      osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    ldout(cct, 10) << " paused modify " << op << " tid " << last_tid << dendl;
    op->target.paused = true;
    maybe_request_map();
  } else if ((op->target.flags & CEPH_OSD_FLAG_READ) &&
	     osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    ldout(cct, 10) << " paused read " << op << " tid " << last_tid << dendl;
    op->target.paused = true;
    maybe_request_map();
  } else if ((op->target.flags & CEPH_OSD_FLAG_WRITE) && osdmap_full_flag()) {
    ldout(cct, 0) << " FULL, paused modify " << op << " tid " << last_tid << dendl;
    op->target.paused = true;
    maybe_request_map();
  } else if (op->session) {
    send_op(op);
  } else {
    maybe_request_map();
  }

  ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

  return op->tid;
}

ceph_tid_t Objecter::op_submit_special(Op *op)
{
  // pick tid
  ceph_tid_t mytid = ++last_tid;
  op->tid = mytid;
  assert(client_inc >= 0);

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
  ops[op->tid] = op;

  op->target.osd = op->osd;
  op->session = get_session(op->target.osd);
  op->session->ops.push_back(&op->session_item);

  // send?
  ldout(cct, 10) << "op_submit_special oid " << op->target.oid
		 << " " << op->target.volume << " " << op->ops
		 << " tid " << op->tid << " osd."
		 << (op->session ? op->session->osd : -1)
		 << dendl;

  assert(op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

  send_op(op);

  ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

  return op->tid;
}

int Objecter::op_cancel(ceph_tid_t tid, int r)
{
  assert(client_lock.is_locked());
  assert(initialized);

  map<ceph_tid_t, Op*>::iterator p = ops.find(tid);
  if (p == ops.end()) {
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
  finish_op(op);
  return 0;
}

bool Objecter::target_should_be_paused(op_target_t *t)
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


int Objecter::calc_target(op_target_t *t)
{
  // Oh, yay!
#if 0
  int ret = osdmap->do_stuff(t->oid, t->volume);
  int osd;
  osdmap->pg_to_osd(pgid, osd);

  bool need_resend = false;

  bool paused = target_should_be_paused(t);
  if (!paused && paused != t->paused) {
    t->paused = false;
    need_resend = true;
  }

  if (t->pgid != pgid ||
      t->osd != osd) {
    t->pgid = pgid;
    t->osd = osd;
    need_resend = true;
  }
  if (need_resend) {
    return RECALC_OP_TARGET_NEED_RESEND;
  }
#endif
  return RECALC_OP_TARGET_NO_ACTION;
}

int Objecter::recalc_op_target(Op *op)
{
#if 0
  int r = calc_target(&op->target);
  if (r == RECALC_OP_TARGET_NEED_RESEND) {
    OSDSession *s = NULL;
    if (op->target.osd >= 0)
      s = get_session(op->target.osd);
    if (op->session != s) {
      if (!op->session)
	num_homeless_ops--;
      op->session_item.remove_myself();
      op->session = s;
      if (s)
	s->ops.push_back(&op->session_item);
      else
	num_homeless_ops++;
    }
  }
  return r;
#else
    ldout(cct, 0) << "!!! recalc_op_target tid " << op->tid
		   << " volume " << op->target.volume
		   << " oid " << op->target.oid
		   << " osd " << op->target.osd << dendl;
#endif
  return 0;
}

bool Objecter::recalc_linger_op_target(LingerOp *linger_op)
{
#if 0
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
#else
    ldout(cct, 0) << "!!! recalc_linger_op_target tid " << linger_op->linger_id
		   << " volume " << linger_op->target.volume
		   << " oid " << linger_op->target.oid
		   << " osd " << linger_op->target.osd << dendl;
#endif
  return 0;
}

void Objecter::cancel_linger_op(Op *op)
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

  op->session_item.remove_myself();
  if (op->budgeted)
    put_op_budget(op);

  ops.erase(op->tid);
  assert(check_latest_map_ops.find(op->tid) == check_latest_map_ops.end());

  if (op->ontimeout)
    timer.cancel_event(op->ontimeout);

  delete op;
}

void Objecter::send_op(Op *op)
{
  ldout(cct, 15) << "send_op " << op->tid << " to osd." << op->session->osd << dendl;

  int flags = op->target.flags;
  if (op->oncommit)
    flags |= CEPH_OSD_FLAG_ONDISK;
  if (op->onack)
    flags |= CEPH_OSD_FLAG_ACK;

  assert(op->session->con);

  // preallocated rx buffer?
  if (op->con) {
    assert(op->con->post_buffers_p());
    ldout(cct, 20) << " revoking rx buffer for " << op->tid << " on "
		   << op->con << dendl;
    op->con->revoke_rx_buffer(op->tid);
  }

  if (op->session->con->post_buffers_p()) {
      if (op->outbl && op->outbl->length()) {
	ldout(cct, 20) << " posting rx buffer for " << op->tid << " on "
		       << op->session->con << dendl;
	if (!op->con)
	  op->con = op->session->con;
	op->con->post_rx_buffer(op->tid, *op->outbl);
      }
  }

  op->target.paused = false;
  op->incarnation = op->session->incarnation;
  op->stamp = ceph_clock_now(cct);

  MOSDOp *m = new MOSDOp(client_inc, op->tid,
			 op->target.oid, op->target.volume,
			 osdmap->get_epoch(),
			 flags);

  m->ops = op->ops;
  m->set_mtime(op->mtime);
  m->set_retry_attempt(op->attempts++);

  if (op->replay_version != eversion_t())
    m->set_version(op->replay_version);	 // we're replaying this op!

  if (op->priority)
    m->set_priority(op->priority);
  else
    m->set_priority(cct->_conf->osd_client_op_priority);

  messenger->send_message(m, op->session->con);
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

void Objecter::throttle_op(Op *op, int op_budget)
{
  if (!op_budget)
    op_budget = calc_op_budget(op);
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
  ops.erase(op->tid);
}

/* This function DOES put the passed message before returning */
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

  // get pio
  ceph_tid_t tid = m->get_tid();

  if (ops.count(tid) == 0) {
    ldout(cct, 7) << "handle_osd_op_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    m->put();
    return;
  }

  ldout(cct, 7) << "handle_osd_op_reply " << tid
		<< (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
		<< " v " << m->get_replay_version() << " uv " << m->get_user_version()
		<< " attempt " << m->get_retry_attempt()
		<< dendl;
  Op *op = ops[tid];

  if (m->get_retry_attempt() >= 0) {
    if (m->get_retry_attempt() != (op->attempts - 1)) {
      ldout(cct, 7) << " ignoring reply from attempt " << m->get_retry_attempt()
		    << " from " << m->get_source_inst()
		    << "; last attempt " << (op->attempts - 1) << " sent to "
		    << op->session->con->get_peer_addr() << dendl;
      m->put();
      return;
    }
  } else {
    // we don't know the request attempt because the server is old, so
    // just accept this one.  we may do ACK callbacks we shouldn't
    // have, but that is better than doing callbacks out of order.
  }

  Context *onack = 0;
  Context *oncommit = 0;

  int rc = m->get_result();

  if (rc == -EAGAIN) {
    ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;
    unregister_op(op);
    _op_submit(op);
    m->put();
    return;
  }

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
    num_unacked--;
  }
  if (op->oncommit && (m->is_ondisk() || rc)) {
    ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
    oncommit = op->oncommit;
    op->oncommit = 0;
    num_uncommitted--;
  }

  // got data?
  if (op->outbl) {
    if (op->con) {
      assert(op->con->post_buffers_p());
      op->con->revoke_rx_buffer(op->tid);
    }
    m->claim_data(*op->outbl);
    op->outbl = 0;
  }

  // done with this tid?
  if (!op->onack && !op->oncommit) {
    ldout(cct, 15) << "handle_osd_op_reply completed tid " << tid << dendl;
    finish_op(op);
  }

  ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

  // do callbacks
  if (onack) {
    onack->complete(rc);
  }
  if (oncommit) {
    oncommit->complete(rc);
  }

  m->put();
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


void Objecter::op_target_t::dump(Formatter *f) const
{
  f->dump_int("osd", osd);
  f->dump_stream("object_id") << oid;
  f->dump_stream("volume") << volume;
  f->dump_int("paused", (int)paused);
}

void Objecter::dump_active()
{
  ldout(cct, 20) << "dump_active .. " << num_homeless_ops << " homeless" << dendl;
  for (map<ceph_tid_t,Op*>::iterator p = ops.begin(); p != ops.end(); ++p) {
    Op *op = p->second;
    ldout(cct, 20) << op->tid << "\t"
		   << "\tosd." << (op->session ? op->session->osd : -1)
		   << "\t" << op->target.oid << "\t" << op->target.volume
		   << "\t" << op->ops << dendl;
  }
}

void Objecter::dump_requests(Formatter *fmt) const
{
  assert(client_lock.is_locked());

  fmt->open_object_section("requests");
  dump_ops(fmt);
  dump_linger_ops(fmt);
  dump_statfs_ops(fmt);
  fmt->close_section(); // requests object
}

void Objecter::dump_ops(Formatter *fmt) const
{
  fmt->open_array_section("ops");
  for (map<ceph_tid_t,Op*>::const_iterator p = ops.begin();
       p != ops.end();
       ++p) {
    Op *op = p->second;
    fmt->open_object_section("op");
    fmt->dump_unsigned("tid", op->tid);
    op->target.dump(fmt);
    fmt->dump_stream("last_sent") << op->stamp;
    fmt->dump_int("attempts", op->attempts);
    fmt->dump_stream("mtime") << op->mtime;

    fmt->open_array_section("osd_ops");
    for (vector<OSDOp>::const_iterator it = op->ops.begin();
	 it != op->ops.end();
	 ++it) {
      fmt->dump_stream("osd_op") << *it;
    }
    fmt->close_section(); // osd_ops array

    fmt->close_section(); // op object
  }
  fmt->close_section(); // ops array
}

void Objecter::dump_linger_ops(Formatter *fmt) const
{
  fmt->open_array_section("linger_ops");
  for (map<uint64_t, LingerOp*>::const_iterator p = linger_ops.begin();
       p != linger_ops.end();
       ++p) {
    LingerOp *op = p->second;
    fmt->open_object_section("linger_op");
    fmt->dump_unsigned("linger_id", op->linger_id);
    op->target.dump(fmt);
    fmt->dump_stream("registered") << op->registered;
    fmt->close_section(); // linger_op object
  }
  fmt->close_section(); // linger_ops array
}

void Objecter::dump_statfs_ops(Formatter *fmt) const
{
  fmt->open_array_section("statfs_ops");
  for (map<ceph_tid_t, StatfsOp*>::const_iterator p = statfs_ops.begin();
       p != statfs_ops.end();
       ++p) {
    StatfsOp *op = p->second;
    fmt->open_object_section("statfs_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_stream("last_sent") << op->last_submit;
    fmt->close_section();
  }
  fmt->close_section();
}

Objecter::RequestStateHook::RequestStateHook(Objecter *objecter) :
  m_objecter(objecter)
{
}

bool Objecter::RequestStateHook::call(std::string command, cmdmap_t& cmdmap,
				      std::string format, bufferlist& out)
{
  Formatter *f = new_formatter(format);
  if (!f)
    f = new_formatter("json-pretty");
  m_objecter->client_lock.Lock();
  m_objecter->dump_requests(f);
  m_objecter->client_lock.Unlock();
  f->flush(out);
  delete f;
  return true;
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
