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
 * Foundation.  See file COPYING.
 * 
 */

#include "Objecter.h"
#include "osd/OSDMap.h"

#include "mon/MonClient.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"

#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MOSDFailure.h"

#include <errno.h>

#include "common/config.h"
#include "common/perf_counters.h"


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
  l_osdc_osdop_mapext,
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

  l_osdc_poolop_active,
  l_osdc_poolop_send,
  l_osdc_poolop_resend,

  l_osdc_poolstat_active,
  l_osdc_poolstat_send,
  l_osdc_poolstat_resend,

  l_osdc_statfs_active,
  l_osdc_statfs_send,
  l_osdc_statfs_resend,

  l_osdc_map_epoch,
  l_osdc_map_full,
  l_osdc_map_inc,

  l_osdc_osd_sessions,
  l_osdc_osd_session_open,
  l_osdc_osd_session_close,
  l_osdc_osd_laggy,
  l_osdc_last,
};


// messages ------------------------------

void Objecter::init()
{
  assert(client_lock.is_locked());
  assert(!initialized);

  if (!logger) {
    PerfCountersBuilder pcb(cct, "objecter", l_osdc_first, l_osdc_last);

    pcb.add_u64(l_osdc_op_active, "op_active");
    pcb.add_u64(l_osdc_op_laggy, "op_laggy");
    pcb.add_u64_counter(l_osdc_op_send, "op_send");
    pcb.add_u64_counter(l_osdc_op_send_bytes, "op_send_bytes");
    pcb.add_u64_counter(l_osdc_op_resend, "op_resend");
    pcb.add_u64_counter(l_osdc_op_ack, "op_ack");
    pcb.add_u64_counter(l_osdc_op_commit, "op_commit");

    pcb.add_u64_counter(l_osdc_op, "op");
    pcb.add_u64_counter(l_osdc_op_r, "op_r");
    pcb.add_u64_counter(l_osdc_op_w, "op_w");
    pcb.add_u64_counter(l_osdc_op_rmw, "op_rmw");
    pcb.add_u64_counter(l_osdc_op_pg, "op_pg");

    pcb.add_u64_counter(l_osdc_osdop_stat, "osdop_stat");
    pcb.add_u64_counter(l_osdc_osdop_create, "osdop_create");
    pcb.add_u64_counter(l_osdc_osdop_read, "osdop_read");
    pcb.add_u64_counter(l_osdc_osdop_write, "osdop_write");
    pcb.add_u64_counter(l_osdc_osdop_writefull, "osdop_writefull");
    pcb.add_u64_counter(l_osdc_osdop_append, "osdop_append");
    pcb.add_u64_counter(l_osdc_osdop_zero, "osdop_zero");
    pcb.add_u64_counter(l_osdc_osdop_truncate, "osdop_truncate");
    pcb.add_u64_counter(l_osdc_osdop_delete, "osdop_delete");
    pcb.add_u64_counter(l_osdc_osdop_mapext, "osdop_mapext");
    pcb.add_u64_counter(l_osdc_osdop_sparse_read, "osdop_sparse_read");
    pcb.add_u64_counter(l_osdc_osdop_clonerange, "osdop_clonerange");
    pcb.add_u64_counter(l_osdc_osdop_getxattr, "osdop_getxattr");
    pcb.add_u64_counter(l_osdc_osdop_setxattr, "osdop_setxattr");
    pcb.add_u64_counter(l_osdc_osdop_cmpxattr, "osdop_cmpxattr");
    pcb.add_u64_counter(l_osdc_osdop_rmxattr, "osdop_rmxattr");
    pcb.add_u64_counter(l_osdc_osdop_resetxattrs, "osdop_resetxattrs");
    pcb.add_u64_counter(l_osdc_osdop_tmap_up, "osdop_tmap_up");
    pcb.add_u64_counter(l_osdc_osdop_tmap_put, "osdop_tmap_put");
    pcb.add_u64_counter(l_osdc_osdop_tmap_get, "osdop_tmap_get");
    pcb.add_u64_counter(l_osdc_osdop_call, "osdop_call");
    pcb.add_u64_counter(l_osdc_osdop_watch, "osdop_watch");
    pcb.add_u64_counter(l_osdc_osdop_notify, "osdop_notify");
    pcb.add_u64_counter(l_osdc_osdop_src_cmpxattr, "osdop_src_cmpxattr");
    pcb.add_u64_counter(l_osdc_osdop_pgls, "osdop_pgls");
    pcb.add_u64_counter(l_osdc_osdop_pgls_filter, "osdop_pgls_filter");
    pcb.add_u64_counter(l_osdc_osdop_other, "osdop_other");

    pcb.add_u64(l_osdc_linger_active, "linger_active");
    pcb.add_u64_counter(l_osdc_linger_send, "linger_send");
    pcb.add_u64_counter(l_osdc_linger_resend, "linger_resend");

    pcb.add_u64(l_osdc_poolop_active, "poolop_active");
    pcb.add_u64_counter(l_osdc_poolop_send, "poolop_send");
    pcb.add_u64_counter(l_osdc_poolop_resend, "poolop_resend");

    pcb.add_u64(l_osdc_poolstat_active, "poolstat_active");
    pcb.add_u64_counter(l_osdc_poolstat_send, "poolstat_send");
    pcb.add_u64_counter(l_osdc_poolstat_resend, "poolstat_resend");

    pcb.add_u64(l_osdc_statfs_active, "statfs_active");
    pcb.add_u64_counter(l_osdc_statfs_send, "statfs_send");
    pcb.add_u64_counter(l_osdc_statfs_resend, "statfs_resend");

    pcb.add_u64(l_osdc_map_epoch, "map_epoch");
    pcb.add_u64_counter(l_osdc_map_full, "map_full");
    pcb.add_u64_counter(l_osdc_map_inc, "map_inc");

    pcb.add_u64(l_osdc_osd_sessions, "osd_sessions");  // open sessions
    pcb.add_u64_counter(l_osdc_osd_session_open, "osd_session_open");
    pcb.add_u64_counter(l_osdc_osd_session_close, "osd_session_close");
    pcb.add_u64(l_osdc_osd_laggy, "osd_laggy");

    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  m_request_state_hook = new RequestStateHook(this);
  AdminSocket* admin_socket = cct->get_admin_socket();
  int ret = admin_socket->register_command("objecter_requests",
					   m_request_state_hook,
					   "show in-progress osd requests");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }

  schedule_tick();
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

  if (m_request_state_hook) {
    AdminSocket* admin_socket = cct->get_admin_socket();
    admin_socket->unregister_command("objecter_requests");
    delete m_request_state_hook;
    m_request_state_hook = NULL;
  }

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }
}

void Objecter::send_linger(LingerOp *info)
{
  ldout(cct, 15) << "send_linger " << info->linger_id << dendl;
  vector<OSDOp> opv = info->ops; // need to pass a copy to ops
  Context *onack = (!info->registered && info->on_reg_ack) ? new C_Linger_Ack(this, info) : NULL;
  Context *oncommit = new C_Linger_Commit(this, info);
  Op *o = new Op(info->oid, info->oloc, opv, info->flags | CEPH_OSD_FLAG_READ,
		 onack, oncommit,
		 info->pobjver);
  o->snapid = info->snap;

  // do not resend this; we will send a new op to reregister
  o->should_resend = false;

  if (info->session) {
    int r = recalc_op_target(o);
    if (r == RECALC_OP_TARGET_POOL_DNE) {
      linger_check_for_latest_map(info);
    }
  }

  if (info->register_tid) {
    // repeat send.  cancel old registeration op, if any.
    if (ops.count(info->register_tid)) {
      Op *o = ops[info->register_tid];
      cancel_op(o);
    }
    info->register_tid = _op_submit(o);
  } else {
    // first send
    info->register_tid = op_submit(o);
  }

  OSDSession *s = o->session;
  if (info->session != s) {
    info->session_item.remove_myself();
    info->session = s;
    if (info->session)
      s->linger_ops.push_back(&info->session_item);
  }
  info->registering = true;

  logger->inc(l_osdc_linger_send);
}

void Objecter::_linger_ack(LingerOp *info, int r) 
{
  ldout(cct, 10) << "_linger_ack " << info->linger_id << dendl;
  if (info->on_reg_ack) {
    info->on_reg_ack->finish(r);
    delete info->on_reg_ack;
    info->on_reg_ack = NULL;
  }
}

void Objecter::_linger_commit(LingerOp *info, int r) 
{
  ldout(cct, 10) << "_linger_commit " << info->linger_id << dendl;
  if (info->on_reg_commit) {
    info->on_reg_commit->finish(r);
    delete info->on_reg_commit;
    info->on_reg_commit = NULL;
  }

  // only tell the user the first time we do this
  info->registered = true;
  info->registering = false;
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
    logger->set(l_osdc_linger_active, linger_ops.size());
  }
}

tid_t Objecter::linger(const object_t& oid, const object_locator_t& oloc, 
		       ObjectOperation& op,
		       snapid_t snap, bufferlist& inbl, bufferlist *poutbl, int flags,
		       Context *onack, Context *onfinish,
		       eversion_t *objver)
{
  LingerOp *info = new LingerOp;
  info->oid = oid;
  info->oloc = oloc;
  if (info->oloc.key == oid)
    info->oloc.key.clear();
  info->snap = snap;
  info->flags = flags;
  info->ops = op.ops;
  info->inbl = inbl;
  info->poutbl = poutbl;
  info->pobjver = objver;
  info->on_reg_ack = onack;
  info->on_reg_commit = onfinish;

  info->linger_id = ++max_linger_id;
  linger_ops[info->linger_id] = info;

  logger->set(l_osdc_linger_active, linger_ops.size());

  send_linger(info);

  return info->linger_id;
}

void Objecter::dispatch(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OPREPLY:
    handle_osd_op_reply((MOSDOpReply*)m);
    break;
    
  case CEPH_MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    break;

  case MSG_GETPOOLSTATSREPLY:
    handle_get_pool_stats_reply((MGetPoolStatsReply*)m);
    break;

  case CEPH_MSG_STATFS_REPLY:
    handle_fs_stats_reply((MStatfsReply*)m);
    break;

  case CEPH_MSG_POOLOP_REPLY:
    handle_pool_op_reply((MPoolOpReply*)m);
    break;

  default:
    ldout(cct, 0) << "don't know message type " << m->get_type() << dendl;
    assert(0);
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
  bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap->test_flag(CEPH_OSDMAP_FULL);
  
  list<LingerOp*> need_resend_linger;
  map<tid_t, Op*> need_resend;

  bool skipped_map = false;

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
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1;
	   e <= m->get_last();
	   e++) {
 
	if (osdmap->get_epoch() == e-1 &&
	    m->incremental_maps.count(e)) {
	  ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e << dendl;
	  OSDMap::Incremental inc(m->incremental_maps[e]);
	  osdmap->apply_incremental(inc);
	  logger->inc(l_osdc_map_inc);
	}
	else if (m->maps.count(e)) {
	  ldout(cct, 3) << "handle_osd_map decoding full epoch " << e << dendl;
	  osdmap->decode(m->maps[e]);
	  logger->inc(l_osdc_map_full);
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
	logger->set(l_osdc_map_epoch, osdmap->get_epoch());
	
	// check for changed linger mappings (_before_ regular ops)
	for (map<tid_t,LingerOp*>::iterator p = linger_ops.begin();
	     p != linger_ops.end();
	     p++) {
	  LingerOp *op = p->second;
	  ldout(cct, 10) << " checking linger op " << op->linger_id << dendl;
	  int r = recalc_linger_op_target(op);
	  if (skipped_map)
	    r = RECALC_OP_TARGET_NEED_RESEND;
	  switch (r) {
	  case RECALC_OP_TARGET_NO_ACTION:
	    // do nothing
	    break;
	  case RECALC_OP_TARGET_NEED_RESEND:
	    need_resend_linger.push_back(op);
	    linger_cancel_map_check(op);
	    break;
	  case RECALC_OP_TARGET_POOL_DNE:
	    linger_check_for_latest_map(op);
	    break;
	  }
	}

	// check for changed request mappings
	for (hash_map<tid_t,Op*>::iterator p = ops.begin();
	     p != ops.end();
	     ++p) {
	  Op *op = p->second;
	  ldout(cct, 10) << " checking op " << op->tid << dendl;
	  int r = recalc_op_target(op);
	  if (skipped_map)
	    r = RECALC_OP_TARGET_NEED_RESEND;
	  switch (r) {
	  case RECALC_OP_TARGET_NO_ACTION:
	    // do nothing
	    break;
	  case RECALC_OP_TARGET_NEED_RESEND:
	    need_resend[op->tid] = op;
	    op_cancel_map_check(op);
	    break;
	  case RECALC_OP_TARGET_POOL_DNE:
	    op_check_for_latest_map(op);
	    break;
	  }
	}

	// osd addr changes?
	for (map<int,OSDSession*>::iterator p = osd_sessions.begin();
	     p != osd_sessions.end(); ) {
	  OSDSession *s = p->second;
	  p++;
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
      } else {
	ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting" << dendl;
	monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	monc->renew_subs();
      }
    }
  }

  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || osdmap->test_flag(CEPH_OSDMAP_FULL);

  // was/is paused?
  if (was_pauserd || was_pausewr || pauserd || pausewr)
    maybe_request_map();
  
  // unpause requests?
  if ((was_pauserd && !pauserd) ||
      (was_pausewr && !pausewr))
    for (hash_map<tid_t,Op*>::iterator p = ops.begin();
	 p != ops.end();
	 p++) {
      Op *op = p->second;
      if (op->paused &&
	  !((op->flags & CEPH_OSD_FLAG_READ) && pauserd) &&   // not still paused as a read
	  !((op->flags & CEPH_OSD_FLAG_WRITE) && pausewr))    // not still paused as a write
	need_resend[op->tid] = op;
    }

  // resend requests
  for (map<tid_t, Op*>::iterator p = need_resend.begin(); p != need_resend.end(); p++) {
    Op *op = p->second;
    if (op->should_resend) {
      if (op->session) {
	logger->inc(l_osdc_op_resend);
	send_op(op);
      }
    } else {
      cancel_op(op);
    }
  }
  for (list<LingerOp*>::iterator p = need_resend_linger.begin(); p != need_resend_linger.end(); p++) {
    LingerOp *op = *p;
    if (op->session) {
      logger->inc(l_osdc_linger_resend);
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
      i->first->finish(i->second);
      delete i->first;
    }
    waiting_for_map.erase(p++);
  }

  m->put();

  monc->sub_got("osdmap", osdmap->get_epoch());
}

void Objecter::C_Op_Map_Latest::finish(int r)
{
  if (r < 0)
    return;

  Mutex::Locker l(objecter->client_lock);

  map<tid_t, Op*>::iterator iter =
    objecter->check_latest_map_ops.find(tid);
  if (iter == objecter->check_latest_map_ops.end()) {
    return;
  }

  Op *op = iter->second;
  objecter->check_latest_map_ops.erase(iter);

  if (r == 0) { // we had the latest map
    if (op->onack) {
      op->onack->complete(-ENOENT);
    }
    if (op->oncommit) {
      op->oncommit->complete(-ENOENT);
    }
    op->session_item.remove_myself();
    objecter->ops.erase(op->tid);
    delete op;
  }
}

void Objecter::C_Linger_Map_Latest::finish(int r)
{
  if (r < 0)
    return;

  Mutex::Locker l(objecter->client_lock);

  map<uint64_t, LingerOp*>::iterator iter =
    objecter->check_latest_map_lingers.find(linger_id);
  if (iter == objecter->check_latest_map_lingers.end()) {
    return;
  }

  LingerOp *op = iter->second;
  objecter->check_latest_map_lingers.erase(iter);

  if (r == 0) { // we had the latest map
    if (op->on_reg_ack) {
      op->on_reg_ack->complete(-ENOENT);
    }
    if (op->on_reg_commit) {
      op->on_reg_commit->complete(-ENOENT);
    }
    objecter->unregister_linger(op->linger_id);
  }
  op->put();
}

void Objecter::op_check_for_latest_map(Op *op)
{
  check_latest_map_ops[op->tid] = op;
  monc->is_latest_map("osdmap", osdmap->get_epoch(),
		      new C_Op_Map_Latest(this, op->tid));
}

void Objecter::linger_check_for_latest_map(LingerOp *op)
{
  op->get();
  check_latest_map_lingers[op->linger_id] = op;
  monc->is_latest_map("osdmap", osdmap->get_epoch(),
		      new C_Linger_Map_Latest(this, op->linger_id));
}

void Objecter::op_cancel_map_check(Op *op)
{
  map<tid_t, Op*>::iterator iter =
    check_latest_map_ops.find(op->tid);
  if (iter != check_latest_map_ops.end()) {
    check_latest_map_ops.erase(iter);
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
  logger->inc(l_osdc_osd_session_open);
  logger->inc(l_osdc_osd_sessions, osd_sessions.size());
  return s;
}

void Objecter::reopen_session(OSDSession *s)
{
  entity_inst_t inst = osdmap->get_inst(s->osd);
  ldout(cct, 10) << "reopen_session osd." << s->osd << " session, addr now " << inst << dendl;
  if (s->con) {
    messenger->mark_down(s->con);
    s->con->put();
    logger->inc(l_osdc_osd_session_close);
  }
  s->con = messenger->get_connection(inst);
  s->incarnation++;
  logger->inc(l_osdc_osd_session_open);
}

void Objecter::close_session(OSDSession *s)
{
  ldout(cct, 10) << "close_session for osd." << s->osd << dendl;
  if (s->con) {
    messenger->mark_down(s->con);
    s->con->put();
    logger->inc(l_osdc_osd_session_close);
  }
  s->ops.clear();
  s->linger_ops.clear();
  osd_sessions.erase(s->osd);
  delete s;

  logger->set(l_osdc_osd_sessions, osd_sessions.size());
}

void Objecter::wait_for_osd_map()
{
  if (osdmap->get_epoch()) return;
  Mutex lock("");
  Cond cond;
  bool done;
  lock.Lock();
  C_SafeCond *context = new C_SafeCond(&lock, &cond, &done, NULL);
  waiting_for_map[0].push_back(pair<Context*, int>(context, 0));
  while (!done)
    cond.Wait(lock);
  lock.Unlock();
}


void Objecter::maybe_request_map(epoch_t epoch)
{
  int flag = 0;
  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    ldout(cct, 10) << "maybe_request_map subscribing (continuous) to next osd map (FULL flag is set)" << dendl;
  } else {
    ldout(cct, 10) << "maybe_request_map subscribing (onetime) to next osd map" << dendl;
    flag = CEPH_SUBSCRIBE_ONETIME;
  }
  if (!epoch) {
    epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
  }
  if (monc->sub_want("osdmap", epoch, flag))
    monc->renew_subs();
}


void Objecter::kick_requests(OSDSession *session)
{
  ldout(cct, 10) << "kick_requests for osd." << session->osd << dendl;

  // resend ops
  for (xlist<Op*>::iterator p = session->ops.begin(); !p.end();) {
    Op *op = *p;
    ++p;
    logger->inc(l_osdc_op_resend);
    if (op->should_resend) {
      send_op(op);
    } else {
      cancel_op(op);
    }
  }

  // resend lingers
  for (xlist<LingerOp*>::iterator j = session->linger_ops.begin(); !j.end(); ++j) {
    logger->inc(l_osdc_linger_resend);
    send_linger(*j);
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
  for (hash_map<tid_t,Op*>::iterator p = ops.begin();
       p != ops.end();
       p++) {
    Op *op = p->second;
    if (op->session && op->stamp < cutoff) {
      ldout(cct, 2) << " tid " << p->first << " on osd." << op->session->osd << " is laggy" << dendl;
      toping.insert(op->session);
      ++laggy_ops;
    }
  }
  for (map<uint64_t,LingerOp*>::iterator p = linger_ops.begin();
       p != linger_ops.end();
       p++) {
    LingerOp *op = p->second;
    if (op->session) {
      ldout(cct, 10) << " pinging osd that serves lingering tid " << p->first << " (osd." << op->session->osd << ")" << dendl;
      toping.insert(op->session);
    } else {
      ldout(cct, 10) << " lingering tid " << p->first << " does not have session" << dendl;
    }
  }
  logger->set(l_osdc_op_laggy, laggy_ops);
  logger->set(l_osdc_osd_laggy, toping.size());

  if (num_homeless_ops || !toping.empty())
    maybe_request_map();

  if (!toping.empty()) {
    // send a ping to these osds, to ensure we detect any session resets
    // (osd reply message policy is lossy)
    for (set<OSDSession*>::iterator i = toping.begin();
	 i != toping.end();
	 i++) {
      messenger->send_message(new MPing, (*i)->con);
    }
  }
    
  // reschedule
  schedule_tick();
}

void Objecter::resend_mon_ops()
{
  utime_t cutoff = ceph_clock_now(cct);
  cutoff -= cct->_conf->objecter_mon_retry_interval;


  for (map<tid_t,PoolStatOp*>::iterator p = poolstat_ops.begin(); p!=poolstat_ops.end(); ++p) {
    if (p->second->last_submit < cutoff) {
      poolstat_submit(p->second);
      logger->inc(l_osdc_poolstat_resend);
    }
  }

  for (map<tid_t,StatfsOp*>::iterator p = statfs_ops.begin(); p!=statfs_ops.end(); ++p) {
    if (p->second->last_submit < cutoff) {
      fs_stats_submit(p->second);
      logger->inc(l_osdc_statfs_resend);
    }
  }

  for (map<tid_t,PoolOp*>::iterator p = pool_ops.begin(); p!=pool_ops.end(); ++p) {
    if (p->second->last_submit < cutoff) {
      pool_op_submit(p->second);
      logger->inc(l_osdc_poolop_resend);
    }
  }
}



// read | write ---------------------------

tid_t Objecter::op_submit(Op *op)
{
  assert(client_lock.is_locked());
  assert(initialized);

  assert(op->ops.size() == op->out_bl.size());
  assert(op->ops.size() == op->out_rval.size());
  assert(op->ops.size() == op->out_handler.size());

  // throttle.  before we look at any state, because
  // take_op_budget() may drop our lock while it blocks.
  take_op_budget(op);

  return _op_submit(op);
}

tid_t Objecter::_op_submit(Op *op)
{
  // pick tid
  tid_t mytid = ++last_tid;
  op->tid = mytid;
  assert(client_inc >= 0);

  // pick target
  bool check_for_latest_map = false;
  num_homeless_ops++;  // initially; recalc_op_target() will decrement if it finds a target
  int r = recalc_op_target(op);
  check_for_latest_map = (r == RECALC_OP_TARGET_POOL_DNE);

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

  logger->set(l_osdc_op_active, ops.size());

  logger->inc(l_osdc_op);
  if ((op->flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE)) == (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE))
    logger->inc(l_osdc_op_rmw);
  else if (op->flags & CEPH_OSD_FLAG_WRITE)
    logger->inc(l_osdc_op_w);
  else if (op->flags & CEPH_OSD_FLAG_READ)
    logger->inc(l_osdc_op_r);

  if (op->flags & CEPH_OSD_FLAG_PGOP)
    logger->inc(l_osdc_op_pg);

  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); ++p) {
    int code = l_osdc_osdop_other;
    switch (p->op.op) {
    case CEPH_OSD_OP_STAT: code = l_osdc_osdop_stat; break;
    case CEPH_OSD_OP_CREATE: code = l_osdc_osdop_create; break;
    case CEPH_OSD_OP_READ: code = l_osdc_osdop_read; break;
    case CEPH_OSD_OP_WRITE: code = l_osdc_osdop_write; break;
    case CEPH_OSD_OP_WRITEFULL: code = l_osdc_osdop_writefull; break;
    case CEPH_OSD_OP_APPEND: code = l_osdc_osdop_append; break;
    case CEPH_OSD_OP_ZERO: code = l_osdc_osdop_zero; break;
    case CEPH_OSD_OP_TRUNCATE: code = l_osdc_osdop_truncate; break;
    case CEPH_OSD_OP_DELETE: code = l_osdc_osdop_delete; break;
    case CEPH_OSD_OP_MAPEXT: code = l_osdc_osdop_mapext; break;
    case CEPH_OSD_OP_SPARSE_READ: code = l_osdc_osdop_sparse_read; break;
    case CEPH_OSD_OP_CLONERANGE: code = l_osdc_osdop_clonerange; break;
    case CEPH_OSD_OP_GETXATTR: code = l_osdc_osdop_getxattr; break;
    case CEPH_OSD_OP_SETXATTR: code = l_osdc_osdop_setxattr; break;
    case CEPH_OSD_OP_CMPXATTR: code = l_osdc_osdop_cmpxattr; break;
    case CEPH_OSD_OP_RMXATTR: code = l_osdc_osdop_rmxattr; break;
    case CEPH_OSD_OP_RESETXATTRS: code = l_osdc_osdop_resetxattrs; break;
    case CEPH_OSD_OP_TMAPUP: code = l_osdc_osdop_tmap_up; break;
    case CEPH_OSD_OP_TMAPPUT: code = l_osdc_osdop_tmap_put; break;
    case CEPH_OSD_OP_TMAPGET: code = l_osdc_osdop_tmap_get; break;
    case CEPH_OSD_OP_CALL: code = l_osdc_osdop_call; break;
    case CEPH_OSD_OP_WATCH: code = l_osdc_osdop_watch; break;
    case CEPH_OSD_OP_NOTIFY: code = l_osdc_osdop_notify; break;
    case CEPH_OSD_OP_SRC_CMPXATTR: code = l_osdc_osdop_src_cmpxattr; break;
    }
    if (code)
      logger->inc(code);
  }

  // send?
  ldout(cct, 10) << "op_submit oid " << op->oid
           << " " << op->oloc 
	   << " " << op->ops << " tid " << op->tid
           << " osd." << (op->session ? op->session->osd : -1)
           << dendl;

  assert(op->flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

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
  } else if ((op->flags & CEPH_OSD_FLAG_WRITE) &&
	     osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    ldout(cct, 0) << " FULL, paused modify " << op << " tid " << last_tid << dendl;
    op->paused = true;
    maybe_request_map();
  } else if (op->session) {
    send_op(op);
  } else {
    maybe_request_map();
  }

  if (check_for_latest_map) {
    op_check_for_latest_map(op);
  }

  ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return op->tid;
}

bool Objecter::is_pg_changed(vector<int>& o, vector<int>& n, bool any_change)
{
  if (o.empty() && n.empty())
    return false;    // both still empty
  if (o.empty() ^ n.empty())
    return true;     // was empty, now not, or vice versa
  if (o[0] != n[0])
    return true;     // primary changed
  if (any_change && o != n)
    return true;
  return false;      // same primary (tho replicas may have changed)
}

int Objecter::recalc_op_target(Op *op)
{
  vector<int> acting;
  pg_t pgid = op->pgid;
  if (!op->precalc_pgid) {
    int ret = osdmap->object_locator_to_pg(op->oid, op->oloc, pgid);
    if (ret == -ENOENT)
      return RECALC_OP_TARGET_POOL_DNE;
  }
  osdmap->pg_to_acting_osds(pgid, acting);

#warning integrate the following here
  // int ret = osdmap->getPlacement(op->oloc, op->oid, acting);

  if (op->pgid != pgid || is_pg_changed(op->acting, acting, op->used_replica)) {
    op->pgid = pgid;
    op->acting = acting;
    ldout(cct, 10) << "recalc_op_target tid " << op->tid
	     << " pgid " << pgid << " acting " << acting << dendl;

    OSDSession *s = NULL;
    op->used_replica = false;
    if (acting.size()) {
      int osd;
      bool read = (op->flags & CEPH_OSD_FLAG_READ) && (op->flags & CEPH_OSD_FLAG_WRITE) == 0;
      if (read && (op->flags & CEPH_OSD_FLAG_BALANCE_READS)) {
	int p = rand() % acting.size();
	if (p)
	  op->used_replica = true;
	osd = acting[p];
	ldout(cct, 10) << " chose random osd." << osd << " of " << acting << dendl;
      } else if (read && (op->flags & CEPH_OSD_FLAG_LOCALIZE_READS)) {
	// look for a local replica
	int i;
	/* loop through the OSD replicas and see if any are local to read from.
	 * We don't need to check the primary since we default to it. (Be
         * careful to preserve that default, which is why we iterate in reverse
         * order.) */
	for (i = acting.size()-1; i > 0; --i) {
	  if (osdmap->get_addr(acting[i]).is_same_host(messenger->get_myaddr())) {
	    op->used_replica = true;
	    ldout(cct, 10) << " chose local osd." << acting[i] << " of " << acting << dendl;
	    break;
	  }
	}
	osd = acting[i];
      } else
	osd = acting[0];
      s = get_session(osd);
    }

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
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  return RECALC_OP_TARGET_NO_ACTION;
}

bool Objecter::recalc_linger_op_target(LingerOp *linger_op)
{
  vector<int> acting;
  pg_t pgid;
  int ret = osdmap->object_locator_to_pg(linger_op->oid, linger_op->oloc, pgid);
  if (ret == -ENOENT) {
    return RECALC_OP_TARGET_POOL_DNE;
  }
  osdmap->pg_to_acting_osds(pgid, acting);

  if (pgid != linger_op->pgid || is_pg_changed(linger_op->acting, acting, true)) {
    linger_op->pgid = pgid;
    linger_op->acting = acting;
    ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
	     << " pgid " << pgid << " acting " << acting << dendl;
    
    OSDSession *s = acting.size() ? get_session(acting[0]) : NULL;
    if (linger_op->session != s) {
      linger_op->session_item.remove_myself();
      linger_op->session = s;
      if (s)
	s->linger_ops.push_back(&linger_op->session_item);
    }
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  return RECALC_OP_TARGET_NO_ACTION;
}

void Objecter::cancel_op(Op *op)
{
  ldout(cct, 15) << "cancel_op " << op->tid << dendl;

  // currently this only works for linger registrations, since we just
  // throw out the callbacks.
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
  if (op->con)
    op->con->put();

  ops.erase(op->tid);
  logger->set(l_osdc_op_active, ops.size());

  delete op;
}

void Objecter::send_op(Op *op)
{
  ldout(cct, 15) << "send_op " << op->tid << " to osd." << op->session->osd << dendl;

  int flags = op->flags;
  if (op->oncommit)
    flags |= CEPH_OSD_FLAG_ONDISK;
  if (op->onack)
    flags |= CEPH_OSD_FLAG_ACK;

  assert(op->session->con);

  // preallocated rx buffer?
  if (op->con) {
    ldout(cct, 20) << " revoking rx buffer for " << op->tid << " on " << op->con << dendl;
    op->con->revoke_rx_buffer(op->tid);
    op->con->put();
  }
  if (op->outbl && op->outbl->length()) {
    ldout(cct, 20) << " posting rx buffer for " << op->tid << " on " << op->session->con << dendl;
    op->con = op->session->con->get();
    op->con->post_rx_buffer(op->tid, *op->outbl);
  }

  op->paused = false;
  op->incarnation = op->session->incarnation;
  op->stamp = ceph_clock_now(cct);

  MOSDOp *m = new MOSDOp(client_inc, op->tid, 
			 op->oid, op->oloc, op->pgid, osdmap->get_epoch(),
			 flags);

  m->set_snapid(op->snapid);
  m->set_snap_seq(op->snapc.seq);
  m->set_snaps(op->snapc.snaps);

  m->ops = op->ops;
  m->set_mtime(op->mtime);
  m->set_retry_attempt(op->attempts++);

  if (op->version != eversion_t())
    m->set_version(op->version);  // we're replaying this op!

  if (op->priority)
    m->set_priority(op->priority);

  logger->inc(l_osdc_op_send);
  logger->inc(l_osdc_op_send_bytes, m->get_data().length());

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
    } else if (i->op.op & CEPH_OSD_OP_MODE_RD) {
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

/* This function DOES put the passed message before returning */
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

  // get pio
  tid_t tid = m->get_tid();

  if (ops.count(tid) == 0) {
    ldout(cct, 7) << "handle_osd_op_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    m->put();
    return;
  }

  ldout(cct, 7) << "handle_osd_op_reply " << tid
		<< (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
		<< " v " << m->get_version() << " in " << m->get_pg()
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
    if (op->onack)
      num_unacked--;
    if (op->oncommit)
      num_uncommitted--;
    op_submit(op);
    m->put();
    return;
  }

  if (op->objver)
    *op->objver = m->get_version();
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
    if (*pr)
      **pr = p->rval;
    if (*ph) {
      ldout(cct, 10) << " op " << i << " handler " << *ph << dendl;
      (*ph)->complete(p->rval);
    }
  }

  // ack|commit -> ack
  if (op->onack) {
    ldout(cct, 15) << "handle_osd_op_reply ack" << dendl;
    op->version = m->get_version();
    onack = op->onack;
    op->onack = 0;  // only do callback once
    num_unacked--;
    logger->inc(l_osdc_op_ack);
  }
  if (op->oncommit && (m->is_ondisk() || rc)) {
    ldout(cct, 15) << "handle_osd_op_reply safe" << dendl;
    oncommit = op->oncommit;
    op->oncommit = 0;
    num_uncommitted--;
    logger->inc(l_osdc_op_commit);
  }

  // got data?
  if (op->outbl) {
    if (op->con)
      op->con->revoke_rx_buffer(op->tid);
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
    onack->finish(rc);
    delete onack;
  }
  if (oncommit) {
    oncommit->finish(rc);
    delete oncommit;
  }

  m->put();
}


void Objecter::list_objects(ListContext *list_context, Context *onfinish) {

  ldout(cct, 10) << "list_objects" << dendl;
  ldout(cct, 20) << "pool_id " << list_context->pool_id
	   << "\npool_snap_seq " << list_context->pool_snap_seq
	   << "\nmax_entries " << list_context->max_entries
	   << "\nlist_context " << list_context
	   << "\nonfinish " << onfinish
	   << "\nlist_context->current_pg" << list_context->current_pg
	   << "\nlist_context->cookie" << list_context->cookie << dendl;

  if (list_context->at_end) {
    onfinish->finish(0);
    delete onfinish;
    return;
  }

  const pg_pool_t *pool = osdmap->get_pg_pool(list_context->pool_id);
  int pg_num = pool->get_pg_num();

  if (list_context->starting_pg_num == 0) {     // there can't be zero pgs!
    list_context->starting_pg_num = pg_num;
    ldout(cct, 20) << pg_num << " placement groups" << dendl;
  }
  if (list_context->starting_pg_num != pg_num) {
    // start reading from the beginning; the pgs have changed
    ldout(cct, 10) << "The placement groups have changed, restarting with " << pg_num << dendl;
    list_context->current_pg = 0;
    list_context->cookie = collection_list_handle_t();
    list_context->current_pg_epoch = 0;
    list_context->starting_pg_num = pg_num;
  }
  if (list_context->current_pg == pg_num){ //this context got all the way through
    onfinish->finish(0);
    delete onfinish;
    return;
  }

  ObjectOperation op;
  op.pg_ls(list_context->max_entries, list_context->filter, list_context->cookie,
	   list_context->current_pg_epoch);

  bufferlist *bl = new bufferlist();
  C_List *onack = new C_List(list_context, onfinish, bl, this);

  object_t oid;
  object_locator_t oloc(list_context->pool_id);

  // 
  Op *o = new Op(oid, oloc, op.ops, CEPH_OSD_FLAG_READ, onack, NULL, NULL);
  o->priority = op.priority;
  o->snapid = list_context->pool_snap_seq;
  o->outbl = bl;
  o->reply_epoch = &onack->epoch;

  o->pgid = pg_t(list_context->current_pg, list_context->pool_id, -1);
  o->precalc_pgid = true;

  op_submit(o);
}

void Objecter::_list_reply(ListContext *list_context, int r, bufferlist *bl,
			   Context *final_finish, epoch_t reply_epoch)
{
  ldout(cct, 10) << "_list_reply" << dendl;

  bufferlist::iterator iter = bl->begin();
  pg_ls_response_t response;
  bufferlist extra_info;
  ::decode(response, iter);
  if (!iter.end()) {
    ::decode(extra_info, iter);
  }
  list_context->cookie = response.handle;
  if (!list_context->current_pg_epoch) {
    // first pgls result, set epoch marker
    ldout(cct, 20) << "first pgls piece, reply_epoch is " << reply_epoch << dendl;
    list_context->current_pg_epoch = reply_epoch;
  }

  int response_size = response.entries.size();
  ldout(cct, 20) << "response.entries.size " << response_size
	   << ", response.entries " << response.entries << dendl;
  list_context->extra_info.append(extra_info);
  if (response_size) {
    ldout(cct, 20) << "got a response with objects, proceeding" << dendl;
    list_context->list.merge(response.entries);
    if (response_size >= list_context->max_entries) {
      final_finish->finish(0);
      delete bl;
      delete final_finish;
      return;
    }

    // ask for fewer objects next time around
    list_context->max_entries -= response_size;

    // if the osd returns 1 (newer code), or no entries, it means we
    // hit the end of the pg.
    if (r == 0 && response_size > 0) {
      // not yet done with this pg
      delete bl;
      list_objects(list_context, final_finish);
      return;
    }
  }

  // if we make this this far, there are no objects left in the current pg, but we want more!
  ++list_context->current_pg;
  list_context->current_pg_epoch = 0;
  ldout(cct, 20) << "emptied current pg, moving on to next one:" << list_context->current_pg << dendl;
  if (list_context->current_pg < list_context->starting_pg_num){ // we have more pgs to go through
    list_context->cookie = collection_list_handle_t();
    delete bl;
    list_objects(list_context, final_finish);
    return;
  }
  
  // if we make it this far, there are no more pgs
  ldout(cct, 20) << "out of pgs, returning to" << final_finish << dendl;
  list_context->at_end = true;
  delete bl;
  final_finish->finish(0);
  delete final_finish;
  return;
}


//snapshots

int Objecter::create_pool_snap(int64_t pool, string& snapName, Context *onfinish) {
  ldout(cct, 10) << "create_pool_snap; pool: " << pool << "; snap: " << snapName << dendl;
  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snapName;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_CREATE_SNAP;
  pool_ops[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

struct C_SelfmanagedSnap : public Context {
  bufferlist bl;
  snapid_t *psnapid;
  Context *fin;
  C_SelfmanagedSnap(snapid_t *ps, Context *f) : psnapid(ps), fin(f) {}
  void finish(int r) {
    if (r == 0) {
      bufferlist::iterator p = bl.begin();
      ::decode(*psnapid, p);
    }
    fin->finish(r);
    delete fin;
  }
};

int Objecter::allocate_selfmanaged_snap(int64_t pool, snapid_t *psnapid,
					Context *onfinish)
{
  ldout(cct, 10) << "allocate_selfmanaged_snap; pool: " << pool << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  C_SelfmanagedSnap *fin = new C_SelfmanagedSnap(psnapid, onfinish);
  op->onfinish = fin;
  op->blp = &fin->bl;
  op->pool_op = POOL_OP_CREATE_UNMANAGED_SNAP;
  pool_ops[op->tid] = op;

  pool_op_submit(op);
  return 0;
}

int Objecter::delete_pool_snap(int64_t pool, string& snapName, Context *onfinish)
{
  ldout(cct, 10) << "delete_pool_snap; pool: " << pool << "; snap: " << snapName << dendl;
  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snapName;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE_SNAP;
  pool_ops[op->tid] = op;
  
  pool_op_submit(op);
  
  return 0;
}

int Objecter::delete_selfmanaged_snap(int64_t pool, snapid_t snap,
				      Context *onfinish) {
  ldout(cct, 10) << "delete_selfmanaged_snap; pool: " << pool << "; snap: " 
	   << snap << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE_UNMANAGED_SNAP;
  op->snapid = snap;
  pool_ops[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

int Objecter::create_pool(string& name, Context *onfinish, uint64_t auid,
			  int crush_rule)
{
  ldout(cct, 10) << "create_pool name=" << name << dendl;
  PoolOp *op = new PoolOp;
  if (!op)
    return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = 0;
  op->name = name;
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_CREATE;
  pool_ops[op->tid] = op;
  op->auid = auid;
  op->crush_rule = crush_rule;

  pool_op_submit(op);

  return 0;
}

int Objecter::delete_pool(int64_t pool, Context *onfinish)
{
  ldout(cct, 10) << "delete_pool " << pool << dendl;

  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = "delete";
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_DELETE;
  pool_ops[op->tid] = op;

  pool_op_submit(op);

  return 0;
}

/**
 * change the auid owner of a pool by contacting the monitor.
 * This requires the current connection to have write permissions
 * on both the pool's current auid and the new (parameter) auid.
 * Uses the standard Context callback when done.
 */
int Objecter::change_pool_auid(int64_t pool, Context *onfinish, uint64_t auid)
{
  ldout(cct, 10) << "change_pool_auid " << pool << " to " << auid << dendl;
  PoolOp *op = new PoolOp;
  if (!op) return -ENOMEM;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = "change_pool_auid";
  op->onfinish = onfinish;
  op->pool_op = POOL_OP_AUID_CHANGE;
  op->auid = auid;
  pool_ops[op->tid] = op;

  logger->set(l_osdc_poolop_active, pool_ops.size());

  pool_op_submit(op);
  return 0;
}

void Objecter::pool_op_submit(PoolOp *op)
{
  ldout(cct, 10) << "pool_op_submit " << op->tid << dendl;
  MPoolOp *m = new MPoolOp(monc->get_fsid(), op->tid, op->pool,
			   op->name, op->pool_op,
			   op->auid, last_seen_osdmap_version);
  if (op->snapid) m->snapid = op->snapid;
  if (op->crush_rule) m->crush_rule = op->crush_rule;
  monc->send_mon_message(m);
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_poolop_send);
}

/**
 * Handle a reply to a PoolOp message. Check that we sent the message
 * and give the caller responsibility for the returned bufferlist.
 * Then either call the finisher or stash the PoolOp, depending on if we
 * have a new enough map.
 * Lastly, clean up the message and PoolOp.
 */
void Objecter::handle_pool_op_reply(MPoolOpReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "handle_pool_op_reply " << *m << dendl;
  tid_t tid = m->get_tid();
  if (pool_ops.count(tid)) {
    PoolOp *op = pool_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << " Op: " << ceph_pool_op_name(op->pool_op) << dendl;
    if (op->blp)
      op->blp->claim(m->response_data);
    if (m->version > last_seen_osdmap_version)
      last_seen_osdmap_version = m->version;
    if (osdmap->get_epoch() < m->epoch) {
      ldout(cct, 20) << "waiting for client to reach epoch " << m->epoch << " before calling back" << dendl;
      wait_for_new_map(op->onfinish, m->epoch, m->replyCode);
    }
    else {
      op->onfinish->finish(m->replyCode);
      delete op->onfinish;
    }
    op->onfinish = NULL;
    delete op;
    pool_ops.erase(tid);

    logger->set(l_osdc_poolop_active, pool_ops.size());

  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  ldout(cct, 10) << "done" << dendl;
  m->put();
}


// pool stats

void Objecter::get_pool_stats(list<string>& pools, map<string,pool_stat_t> *result,
			      Context *onfinish)
{
  ldout(cct, 10) << "get_pool_stats " << pools << dendl;

  PoolStatOp *op = new PoolStatOp;
  op->tid = ++last_tid;
  op->pools = pools;
  op->pool_stats = result;
  op->onfinish = onfinish;
  poolstat_ops[op->tid] = op;

  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  poolstat_submit(op);
}

void Objecter::poolstat_submit(PoolStatOp *op)
{
  ldout(cct, 10) << "poolstat_submit " << op->tid << dendl;
  monc->send_mon_message(new MGetPoolStats(monc->get_fsid(), op->tid, op->pools, last_seen_pgmap_version));
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_poolstat_send);
}

void Objecter::handle_get_pool_stats_reply(MGetPoolStatsReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "handle_get_pool_stats_reply " << *m << dendl;
  tid_t tid = m->get_tid();

  if (poolstat_ops.count(tid)) {
    PoolStatOp *op = poolstat_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    *op->pool_stats = m->pool_stats;
    if (m->version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->version;
    op->onfinish->finish(0);
    delete op->onfinish;
    poolstat_ops.erase(tid);
    delete op;

    logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  } 
  ldout(cct, 10) << "done" << dendl;
  m->put();
}


void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish)
{
  ldout(cct, 10) << "get_fs_stats" << dendl;

  StatfsOp *op = new StatfsOp;
  op->tid = ++last_tid;
  op->stats = &result;
  op->onfinish = onfinish;
  statfs_ops[op->tid] = op;

  logger->set(l_osdc_statfs_active, statfs_ops.size());

  fs_stats_submit(op);
}

void Objecter::fs_stats_submit(StatfsOp *op)
{
  ldout(cct, 10) << "fs_stats_submit" << op->tid << dendl;
  monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid, last_seen_pgmap_version));
  op->last_submit = ceph_clock_now(cct);

  logger->inc(l_osdc_statfs_send);
}

void Objecter::handle_fs_stats_reply(MStatfsReply *m)
{
  assert(client_lock.is_locked());
  assert(initialized);
  ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
  tid_t tid = m->get_tid();

  if (statfs_ops.count(tid)) {
    StatfsOp *op = statfs_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    *(op->stats) = m->h.st;
    if (m->h.version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->h.version;
    op->onfinish->finish(0);
    delete op->onfinish;
    statfs_ops.erase(tid);
    delete op;

    logger->set(l_osdc_statfs_active, statfs_ops.size());

  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  ldout(cct, 10) << "done" << dendl;
  m->put();
}


// scatter/gather

void Objecter::_sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl, 
			       bufferlist *bl, Context *onfinish)
{
  // all done
  uint64_t bytes_read = 0;
  
  ldout(cct, 15) << "_sg_read_finish" << dendl;

  if (extents.size() > 1) {
    /** FIXME This doesn't handle holes efficiently.
     * It allocates zero buffers to fill whole buffer, and
     * then discards trailing ones at the end.
     *
     * Actually, this whole thing is pretty messy with temporary bufferlist*'s all over
     * the heap. 
     */
    
    // map extents back into buffer
    map<uint64_t, bufferlist*> by_off;  // buffer offset -> bufferlist
    
    // for each object extent...
    vector<bufferlist>::iterator bit = resultbl.begin();
    for (vector<ObjectExtent>::iterator eit = extents.begin();
	 eit != extents.end();
	 eit++, bit++) {
      bufferlist& ox_buf = *bit;
      unsigned ox_len = ox_buf.length();
      unsigned ox_off = 0;
      assert(ox_len <= eit->length);           
      
      // for each buffer extent we're mapping into...
      for (map<uint64_t, uint64_t>::iterator bit = eit->buffer_extents.begin();
	   bit != eit->buffer_extents.end();
	   bit++) {
	ldout(cct, 21) << " object " << eit->oid
		 << " extent " << eit->offset << "~" << eit->length
		 << " : ox offset " << ox_off
		 << " -> buffer extent " << bit->first << "~" << bit->second << dendl;
	by_off[bit->first] = new bufferlist;
	
	if (ox_off + bit->second <= ox_len) {
	  // we got the whole bx
	  by_off[bit->first]->substr_of(ox_buf, ox_off, bit->second);
	  if (bytes_read < bit->first + bit->second) 
	    bytes_read = bit->first + bit->second;
	} else if (ox_off + bit->second > ox_len && ox_off < ox_len) {
	  // we got part of this bx
	  by_off[bit->first]->substr_of(ox_buf, ox_off, (ox_len-ox_off));
	  if (bytes_read < bit->first + ox_len-ox_off) 
	    bytes_read = bit->first + ox_len-ox_off;
	  
	  // zero end of bx
	  ldout(cct, 21) << "  adding some zeros to the end " << ox_off + bit->second-ox_len << dendl;
	  bufferptr z(ox_off + bit->second - ox_len);
	  z.zero();
	  by_off[bit->first]->append( z );
	} else {
	  // we got none of this bx.  zero whole thing.
	  assert(ox_off >= ox_len);
	  ldout(cct, 21) << "  adding all zeros for this bit " << bit->second << dendl;
	  bufferptr z(bit->second);
	  z.zero();
	  by_off[bit->first]->append( z );
	}
	ox_off += bit->second;
      }
      assert(ox_off == eit->length);
    }
    
    // sort and string bits together
    for (map<uint64_t, bufferlist*>::iterator it = by_off.begin();
	 it != by_off.end();
	 it++) {
      assert(it->second->length());
      if (it->first < (uint64_t)bytes_read) {
	ldout(cct, 21) << "  concat buffer frag off " << it->first << " len " << it->second->length() << dendl;
	bl->claim_append(*(it->second));
      } else {
	ldout(cct, 21) << "  NO concat zero buffer frag off " << it->first << " len " << it->second->length() << dendl;          
      }
      delete it->second;
    }
    
    // trim trailing zeros?
    if (bl->length() > bytes_read) {
      ldout(cct, 10) << " trimming off trailing zeros . bytes_read=" << bytes_read 
	       << " len=" << bl->length() << dendl;
      bl->splice(bytes_read, bl->length() - bytes_read);
      assert(bytes_read == bl->length());
    }
    
  } else {
    ldout(cct, 15) << "  only one frag" << dendl;
  
    // only one fragment, easy
    bl->claim(resultbl[0]);
    bytes_read = bl->length();
  }
  
  // finish, clean up
  ldout(cct, 7) << " " << bytes_read << " bytes " 
	  << bl->length()
	  << dendl;
    
  // done
  if (onfinish) {
    onfinish->finish(bytes_read);// > 0 ? bytes_read:m->get_result());
    delete onfinish;
  }
}


void Objecter::ms_handle_connect(Connection *con)
{
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


void Objecter::dump_active()
{
  ldout(cct, 20) << "dump_active .. " << num_homeless_ops << " homeless" << dendl;
  for (hash_map<tid_t,Op*>::iterator p = ops.begin(); p != ops.end(); p++) {
    Op *op = p->second;
    ldout(cct, 20) << op->tid << "\t" << op->pgid << "\tosd." << (op->session ? op->session->osd : -1)
	    << "\t" << op->oid << "\t" << op->ops << dendl;
  }
}

void Objecter::dump_requests(Formatter& fmt) const
{
  assert(client_lock.is_locked());

  fmt.open_object_section("requests");
  dump_ops(fmt);
  dump_linger_ops(fmt);
  dump_pool_ops(fmt);
  dump_pool_stat_ops(fmt);
  dump_statfs_ops(fmt);
  fmt.close_section(); // requests object
}

void Objecter::dump_ops(Formatter& fmt) const
{
  fmt.open_array_section("ops");
  for (hash_map<tid_t,Op*>::const_iterator p = ops.begin();
       p != ops.end();
       ++p) {
    Op *op = p->second;
    fmt.open_object_section("op");
    fmt.dump_unsigned("tid", op->tid);
    fmt.dump_stream("pg") << op->pgid;
    fmt.dump_int("osd", op->session ? op->session->osd : -1);
    fmt.dump_stream("last_sent") << op->stamp;
    fmt.dump_int("attempts", op->attempts);
    fmt.dump_stream("object_id") << op->oid;
    fmt.dump_stream("object_locator") << op->oloc;
    fmt.dump_stream("snapid") << op->snapid;
    fmt.dump_stream("snap_context") << op->snapc;
    fmt.dump_stream("mtime") << op->mtime;

    fmt.open_array_section("osd_ops");
    for (vector<OSDOp>::const_iterator it = op->ops.begin();
	 it != op->ops.end();
	 ++it) {
      fmt.dump_stream("osd_op") << *it;
    }
    fmt.close_section(); // osd_ops array

    fmt.close_section(); // op object
  }
  fmt.close_section(); // ops array
}

void Objecter::dump_linger_ops(Formatter& fmt) const
{
  fmt.open_array_section("linger_ops");
  for (map<uint64_t, LingerOp*>::const_iterator p = linger_ops.begin();
       p != linger_ops.end();
       ++p) {
    LingerOp *op = p->second;
    fmt.open_object_section("linger_op");
    fmt.dump_unsigned("linger_id", op->linger_id);
    fmt.dump_stream("pg") << op->pgid;
    fmt.dump_int("osd", op->session ? op->session->osd : -1);
    fmt.dump_stream("object_id") << op->oid;
    fmt.dump_stream("object_locator") << op->oloc;
    fmt.dump_stream("snapid") << op->snap;
    fmt.dump_stream("registering") << op->snap;
    fmt.dump_stream("registered") << op->snap;
    fmt.close_section(); // linger_op object
  }
  fmt.close_section(); // linger_ops array
}

void Objecter::dump_pool_ops(Formatter& fmt) const
{
  fmt.open_array_section("pool_ops");
  for (map<tid_t, PoolOp*>::const_iterator p = pool_ops.begin();
       p != pool_ops.end();
       ++p) {
    PoolOp *op = p->second;
    fmt.open_object_section("pool_op");
    fmt.dump_unsigned("tid", op->tid);
    fmt.dump_int("pool", op->pool);
    fmt.dump_string("name", op->name);
    fmt.dump_int("operation_type", op->pool_op);
    fmt.dump_unsigned("auid", op->auid);
    fmt.dump_unsigned("crush_rule", op->crush_rule);
    fmt.dump_stream("snapid") << op->snapid;
    fmt.dump_stream("last_sent") << op->last_submit;
    fmt.close_section(); // pool_op object
  }
  fmt.close_section(); // pool_ops array
}

void Objecter::dump_pool_stat_ops(Formatter& fmt) const
{
  fmt.open_array_section("pool_stat_ops");
  for (map<tid_t, PoolStatOp*>::const_iterator p = poolstat_ops.begin();
       p != poolstat_ops.end();
       ++p) {
    PoolStatOp *op = p->second;
    fmt.open_object_section("pool_stat_op");
    fmt.dump_unsigned("tid", op->tid);
    fmt.dump_stream("last_sent") << op->last_submit;

    fmt.open_array_section("pools");
    for (list<string>::const_iterator it = op->pools.begin();
	 it != op->pools.end();
	 ++it) {
      fmt.dump_string("pool", *it);
    }
    fmt.close_section(); // pool_op object

    fmt.close_section(); // pool_stat_op object
  }
  fmt.close_section(); // pool_stat_ops array
}

void Objecter::dump_statfs_ops(Formatter& fmt) const
{
  fmt.open_array_section("statfs_ops");
  for (map<tid_t, StatfsOp*>::const_iterator p = statfs_ops.begin();
       p != statfs_ops.end();
       ++p) {
    StatfsOp *op = p->second;
    fmt.open_object_section("statfs_op");
    fmt.dump_unsigned("tid", op->tid);
    fmt.dump_stream("last_sent") << op->last_submit;
    fmt.close_section(); // pool_stat_op object
  }
  fmt.close_section(); // pool_stat_ops array
}

Objecter::RequestStateHook::RequestStateHook(Objecter *objecter) :
  m_objecter(objecter)
{
}

bool Objecter::RequestStateHook::call(std::string command, std::string args, bufferlist& out)
{
  stringstream ss;
  JSONFormatter formatter(true);
  m_objecter->client_lock.Lock();
  m_objecter->dump_requests(formatter);
  m_objecter->client_lock.Unlock();
  formatter.flush(ss);
  out.append(ss);
  return true;
}
