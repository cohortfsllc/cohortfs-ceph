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

#include <system_error>
#include "boost/tuple/tuple.hpp"
#include "OSDVol.h"
#include "common/errno.h"
#include "common/config.h"
#include "common/cmdparse.h"
#include "OSD.h"
#include "OpRequest.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDPing.h"
#include "messages/MWatchNotify.h"
#include "Watch.h"
#include <sstream>
#include <utility>
#include <cassert>
#include <errno.h>
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
template <typename T>
static ostream& _prefix(std::ostream *_dout, T *vol) {
  return *_dout << vol->gen_prefix();
}



void OSDVol::get()
{
  ++ref;
}

void OSDVol::put()
{
  if (--ref == 0)
    delete this;
}

OSDVol::OSDVol(OSDService *o, OSDMapRef curmap,
	       const boost::uuids::uuid& v) :
  osd(o),
  cct(o->cct),
  osdriver(osd->store, coll_t()),
  osdmap_ref(curmap), last_persisted_osdmap_ref(curmap),
  trace_endpoint("0.0.0.0", 0, NULL),
  ref(0), deleting(false), dirty_info(false),
  id(v), info(v),
  osr(osd->osr_registry.lookup_or_create(v, (stringify(v)))),
  finish_sync_event(NULL), coll(v),
  last_became_active(ceph::mono_clock::now())
{
  // construct name for trace_endpoint
  {
    ostringstream name;
    name << "OSDVol";

    VolumeRef vol;
    if (osdmap_ref->find_by_uuid(v, vol))
      name << ": " << vol->name;

    trace_endpoint.copy_name(name.str());
  }

  // RAII, Baby

  lock_guard l(lock);

  if (osd->store->collection_exists(coll)) {
    read_info();
  } else {
    init();
  }
}

OSDVol::~OSDVol()
{
  on_shutdown();
}

OSDVol::unique_lock OSDVol::lock_suspend_timeout(ThreadPool::TPHandle &handle)
{
  handle.suspend_tp_timeout();
  unique_lock vl(lock);
  handle.reset_tp_timeout();
  return vl;
}

std::string OSDVol::gen_prefix() const
{
  stringstream out;
  OSDMapRef mapref = osdmap_ref;
  out << "osd." << osd->whoami
      << " vol_epoch: " << (mapref ? mapref->get_epoch():0)
      << " vol[" << info.volume << "(unlocked)] ";
  return out.str();
}

void OSDVol::remove_object(
  ObjectStore::Transaction &t, const oid_t &soid)
{
  t.remove(coll, soid);
}

void OSDVol::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  last_update_ondisk = eversion_t();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread
}

struct C_Vol_ActivateCommitted : public Context {
  OSDVolRef vol;
  epoch_t epoch;
  C_Vol_ActivateCommitted(OSDVol *v, epoch_t e)
    : vol(v), epoch(e) {}
  void finish(int r) {
    OSDVol::lock_guard vl(vol->lock);
    vol->_activate_committed(epoch);
  }
};

void OSDVol::activate(ObjectStore::Transaction& t,
		      epoch_t query_epoch)
{
  // twiddle volume state

  info.last_epoch_started = query_epoch;

  last_update_ondisk = info.last_update;
  last_update_applied = info.last_update;

  // write volume info
  dirty_info = true;

  // find out when we commit
  t.register_on_complete(new C_Vol_ActivateCommitted(this, query_epoch));
}

void OSDVol::take_op_map_waiters()
{
  lock_guard ml(map_lock);
  for (auto i = waiting_for_map.begin();
       i != waiting_for_map.end();) {
    if (op_must_wait_for_map(get_osdmap_with_maplock(), *i)) {
      break;
    } else {
      osd->op_wq.queue(make_pair(OSDVolRef(this), *i));
      waiting_for_map.erase(++i);
    }
  }
}

void OSDVol::queue_op(OpRequestRef op)
{
  lock_guard ml(map_lock);
  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    return;
  }
  if (op_must_wait_for_map(get_osdmap_with_maplock(), op)) {
    waiting_for_map.push_back(op);
    return;
  }
  op->trace.event("queue_op", &trace_endpoint);
  osd->op_wq.queue(make_pair(OSDVolRef(this), op));
}

void OSDVol::_activate_committed(epoch_t e)
{
  unique_lock l(lock);
  if (dirty_info) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
    assert(tr == 0);
  }

  l.unlock();
}

/**
 * initialize a newly instantiated vol
 *
 * Initialize state, as when a vol is initially created, or when it
 * is first instantiated on the current node.
 */
void OSDVol::init(void)
{
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  t->create_collection(coll);
  dirty_info = true;
  write_info(*t);
  int r = osd->store->apply_transaction(*t);
  if (r < 0) {
    throw std::system_error(-r, std::system_category(),
			    "initializing volume");
  }
}

int OSDVol::_write_info(ObjectStore::Transaction& t, epoch_t epoch,
			vol_info_t &info, coll_t coll,
			oid_t &infos_obj)
{
  // info.
  map<string,bufferlist> v;
  ::encode(epoch, v[get_epoch_key(info.volume)]);
  ::encode(info, v[get_info_key(info.volume)]);

  t.omap_setkeys(coll_t::META_COLL, infos_obj, v);

  return 0;
}

void OSDVol::write_info(ObjectStore::Transaction& t)
{
  int ret = _write_info(t, get_osdmap()->get_epoch(), info, coll,
			osd->infos_obj);
  assert(ret == 0);
  last_persisted_osdmap_ref = osdmap_ref;

  dirty_info = false;
}

void OSDVol::write_if_dirty(ObjectStore::Transaction& t)
{
  if (dirty_info)
    write_info(t);
}

void OSDVol::read_info()
{
  // We shouldn't even be called if this isn't the case
  assert(osd->store->collection_exists(coll));

  // get info out of leveldb
  string k = get_info_key(info.volume);
  set<string> keys;
  keys.insert(k);
  map<string,bufferlist> values;
  int r = osd->store->omap_get_values(coll_t::META_COLL, osd->infos_obj, keys,
				      &values);
  if (r < 0) {
    throw std::system_error(-r, std::system_category(),
			    "reading volume info");
  }
  assert(values.size() == 1);
  bufferlist bl = values[k];
  bufferlist::iterator p = bl.begin();
  ::decode(info, p);
}

void OSDVol::requeue_object_waiters(map<oid_t, list<OpRequestRef> >& m)
{
  for (map<oid_t, list<OpRequestRef> >::iterator it = m.begin();
       it != m.end();
       ++it)
    requeue_ops(it->second);
  m.clear();
}

void OSDVol::requeue_op(OpRequestRef op)
{
  osd->op_wq.queue_front(make_pair(OSDVolRef(this), op));
}

void OSDVol::requeue_ops(list<OpRequestRef> &ls)
{
  dout(15) << " requeue_ops " << ls << dendl;
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    osd->op_wq.queue_front(make_pair(OSDVolRef(this), *i));
  }
  ls.clear();
}

ostream& operator<<(ostream& out, const OSDVol& vol)
{
  out << "vol[" << vol.info;

  if (vol.last_update_ondisk != vol.info.last_update)
    out << " luod=" << vol.last_update_ondisk;
  out << "]";


  return out;
}

bool OSDVol::can_discard_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  if (OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  }

  return false;
}

bool OSDVol::can_discard_request(OpRequestRef op)
{
  if (op->get_req()->get_type() == CEPH_MSG_OSD_OP)
    return can_discard_op(op);
  return true;
}

bool OSDVol::op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op)
{
  if (op->get_req()->get_type() == CEPH_MSG_OSD_OP)
    return !have_same_or_newer_map(
      curmap, static_cast<MOSDOp*>(op->get_req())->get_map_epoch());

  return false;
}

void OSDVol::take_waiters()
{
  dout(10) << "take_waiters" << dendl;
  take_op_map_waiters();
}

void OSDVol::handle_advance_map(OSDMapRef osdmap)
{
  update_osdmap_ref(osdmap);
}

void OSDVol::handle_activate_map()
{
  if (osdmap_ref->check_new_blacklist_entries()) check_blacklisted_watchers();
}

void OSDVol::on_removal(ObjectStore::Transaction *t)
{
  dout(10) << "on_removal" << dendl;

  // adjust info to backfill
  dirty_info = true;
  write_if_dirty(*t);

  on_shutdown();
}

void OSDVol::do_request(OpRequestRef op, ThreadPool::TPHandle &handle)
{
  // There should be a permission check here, but it was done in
  // termse of namespaces and pools and is sort of sloppy and is based
  // on pool AUIDs and user AUIDs and is something we almost certainly
  // do not want. However we do WANT a permissions check and once we
  // have a system of permissions worked out, this is where we should
  // check it.
  assert(!op_must_wait_for_map(get_osdmap(), op));
  if (can_discard_request(op)) {
    return;
  }

  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    do_op(op); // do it now
    break;

  default:
    assert(0 == "bad message type in do_request");
  }
}

/**
 * @brief do_op - do an op
 * vol lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void OSDVol::do_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  assert(m->get_header().type == CEPH_MSG_OSD_OP);

  op->trace.event("do_op", &trace_endpoint);

  if (get_osdmap()->is_blacklisted(m->get_source_addr())) {
    dout(10) << "do_op " << m->get_source_addr() << " is blacklisted" << dendl;
    osd->reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // order this op as a write?
  bool write_ordered =
    op->may_write() ||
    op->may_cache() ||
    (m->get_flags() & CEPH_OSD_FLAG_RWORDERED);

  dout(10) << "do_op " << *m
	   << (op->may_write() ? " may_write" : "")
	   << (op->may_read() ? " may_read" : "")
	   << (op->may_cache() ? " may_cache" : "")
	   << " -> " << (write_ordered ? "write-ordered" : "read-ordered")
	   << " flags " << ceph_osd_flag_string(m->get_flags())
	   << dendl;

  ObjectContextRef obc;
  bool can_create = op->may_write() || op->may_cache();
  // XXX For Stripulation
  oid_t oid(m->get_oid());

  int r = find_object_context(
    oid, &obc, can_create);

  if (r) {
    osd->reply_op_error(op, r);
    return;
  }

  dout(25) << __func__ << " oi " << obc->obs.oi << dendl;

  // src_objs
  map<oid_t,ObjectContextRef> src_obc;
  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); ++p) {
    OSDOp& osd_op = *p;

    if (!ceph_osd_op_type_multi(osd_op.op.op))
      continue;
    if (osd_op.oid.name.length()) {
      // For Stripulation
      oid_t src_obj(osd_op.oid);
      if (!src_obc.count(src_obj)) {
	ObjectContextRef sobc;
	oid_t wait_obj;

	if (sobc->obs.oi.soid != obc->obs.oi.soid) {
	  dout(1) << " src_obj " << sobc->obs.oi.soid << " != "
		  << obc->obs.oi.soid << dendl;
	  osd->reply_op_error(op, -EINVAL);
	} else {
	  dout(10) << " src_obj " << src_obj << " obc " << src_obc << dendl;
	  src_obc[src_obj] = sobc;
	  continue;
	}
	// Error cleanup below
      } else {
	continue;
      }
      // Error cleanup below
    } else {
      dout(10) << "no src oid specified for multi op " << osd_op << dendl;
      osd->reply_op_error(op, -EINVAL);
    }
    return;
  }

  OpContext *ctx = new OpContext(op, m->get_reqid(), m->ops,
				 &obc->obs,
				 this);
  ctx->op_t = new ObjectStore::Transaction();
  ctx->obc = obc;

  if (m->get_flags() & CEPH_OSD_FLAG_SKIPRWLOCKS) {
    dout(20) << __func__ << ": skipping rw locks" << dendl;
  } else if (!get_rw_locks(ctx)) {
    dout(20) << __func__ << " waiting for rw locks " << dendl;
    op->mark_delayed("waiting for rw locks");
    close_op_ctx(ctx, -EBUSY);
    return;
  }

  if (!op->may_write() &&
      (!obc->obs.exists)) {
    reply_ctx(ctx, -ENOENT);
    return;
  }

  op->mark_started();
  ctx->src_obc = src_obc;

  execute_ctx(ctx);
}

void OSDVol::on_change(ObjectStore::Transaction *t)
{
  dout(10) << "on_change" << dendl;

  // requeue everything in the reverse order they should be
  // reexamined.

  context_registry_on_change();

  for (list<pair<OpRequestRef, OpContext*> >::iterator i =
	 in_progress_async_reads.begin();
       i != in_progress_async_reads.end();
       in_progress_async_reads.erase(i++)) {
    close_op_ctx(i->second, -ECANCELED);
    requeue_op(i->first);
  }

  // this will requeue ops we were working on but didn't finish, and
  // any dups
  apply_mutations(true);

  dout(10) << __func__ << dendl;
}

void OSDVol::on_shutdown()
{
  dout(10) << "on_shutdown" << dendl;

  // remove from queues
  osd->dequeue_vol(this, 0);

  // handles queue races
  deleting = true;

  apply_mutations(false);
  context_registry_on_change();

  clear_primary_state();
}

void OSDVol::get_obc_watchers(ObjectContextRef obc,
			      list<obj_watch_item_t> &vol_watchers)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	 obc->watchers.begin();
	j != obc->watchers.end();
	++j) {
    obj_watch_item_t owi;

    owi.oid = obc->obs.oi.soid;
    owi.wi.addr = j->second->get_peer_addr();
    owi.wi.name = j->second->get_entity();
    owi.wi.cookie = j->second->get_cookie();
    owi.wi.timeout_seconds = j->second->get_timeout();

    dout(30) << "watch: Found oid=" << owi.oid << " addr=" << owi.wi.addr
      << " name=" << owi.wi.name << " cookie=" << owi.wi.cookie << dendl;

    vol_watchers.push_back(owi);
  }
}

void OSDVol::populate_obc_watchers(ObjectContextRef obc)
{
  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (const auto& p : obc->obs.oi.watchers) {
    ceph::mono_time expire = last_became_active +
      p.second.timeout_seconds * 1s;
    dout(10) << "  unconnected watcher " << p.first << " will expire "
	     << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
	this, osd, obc, p.second.timeout_seconds, p.first.first,
	p.first.second, p.second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
	make_pair(p.first.first, p.first.second),
	watch));
  }
  // Look for watchers from blacklisted clients and drop
  check_blacklisted_obc_watchers(obc);
}


void OSDVol::check_blacklisted_obc_watchers(ObjectContextRef obc)
{
  dout(20) << "OSDVol::check_blacklisted_obc_watchers for obc "
	   << obc->obs.oi.soid << dendl;
  for (auto k = obc->watchers.begin();
       k != obc->watchers.end();) {
    //Advance iterator now so handle_watch_timeout() can erase element
    auto j = k++;
    dout(30) << "watch: Found " << j->second->get_entity()
	     << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (get_osdmap()->is_blacklisted(ea)) {
      dout(10) << "watch: Found blacklisted watcher for " << ea << dendl;
      assert(j->second->get_vol() == this);
      handle_watch_timeout(j->second);
    }
  }
}


void OSDVol::check_blacklisted_watchers()
{
  dout(20) << "OSDVol::check_blacklisted_watchers for vol " << info.volume
	   << dendl;
  pair<oid_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i))
    check_blacklisted_obc_watchers(i.second);
}

void OSDVol::get_watchers(list<obj_watch_item_t> &vol_watchers)
{
  pair<oid_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    get_obc_watchers(obc, vol_watchers);
  }
}

void intrusive_ptr_add_ref(OSDVol *vol) { vol->get(); }
void intrusive_ptr_release(OSDVol *vol) { vol->put(); }

int OSDVol::whoami() {
  return osd->whoami;
}

struct OnReadComplete : public Context {
  OSDVol *vol;
  OSDVol::OpContext *opcontext;
  OnReadComplete(
    OSDVol *vol,
    OSDVol::OpContext *ctx) : vol(vol), opcontext(ctx) {}
  void finish(int r) {
    OSDVol::unique_lock vl(vol->lock);
    if (r < 0)
      opcontext->async_read_result = r;
    opcontext->finish_read(vol);
    vl.unlock();
  }
  ~OnReadComplete() {}
};

// OpContext
void OSDVol::OpContext::start_async_reads(OSDVol *vol)
{
  inflightreads = 1;
  vol->objects_read_async(
    obc->obs.oi.soid,
    pending_async_reads,
    new OnReadComplete(vol, this));
  pending_async_reads.clear();
}

void OSDVol::OpContext::finish_read(OSDVol *vol)
{
  assert(inflightreads > 0);
  --inflightreads;
  if (async_reads_complete()) {
    assert(vol->in_progress_async_reads.size());
    assert(vol->in_progress_async_reads.front().second == this);
    vol->in_progress_async_reads.pop_front();
    vol->complete_read_ctx(async_read_result, this);
  }
}


// ==========================================================

void OSDVol::execute_ctx(OpContext *ctx)
{
  dout(10) << __func__ << " " << ctx << dendl;
  ctx->reset_obs(ctx->obc);
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  ObjectContextRef obc = ctx->obc;
  const oid_t& soid = obc->obs.oi.soid;
  map<oid_t, ObjectContextRef>& src_obc = ctx->src_obc;

  // this method must be idempotent since we may call it several times
  // before we finally apply the resulting transaction.
  delete ctx->op_t;
  ctx->op_t = new ObjectStore::Transaction();

  if (op->may_write() || op->may_cache()) {
    op->mark_started();

    // version
    ctx->at_version = get_next_version();
    ctx->mtime = m->get_mtime();

    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version
	     << dendl;
  } else {
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;
  }

  if (!ctx->user_at_version)
    ctx->user_at_version = obc->obs.oi.user_version;
  dout(30) << __func__ << " user_at_version " << ctx->user_at_version << dendl;

  if (op->may_read()) {
    dout(10) << " taking ondisk_read_lock" << dendl;
    obc->ondisk_read_lock();
  }
  for (map<oid_t,ObjectContextRef>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " taking ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_lock();
  }

  int result = prepare_transaction(ctx);

  if (op->may_read()) {
    dout(10) << " dropping ondisk_read_lock" << dendl;
    obc->ondisk_read_unlock();
  }
  for (map<oid_t,ObjectContextRef>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " dropping ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_unlock();
  }

  if (result == -EINPROGRESS) {
    // come back later.
    return;
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    close_op_ctx(ctx, result);
    return;
  }

  bool successful_write = !ctx->op_t->empty() && op->may_write() && result >= 0;
  // prepare the reply
  ctx->reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0,
			       successful_write);

  // Write operations aren't allowed to return a data payload because
  // we can't do so reliably. If the client has to resend the request
  // and it has already been applied, we will return 0 with no
  // payload.  Non-deterministic behavior is no good.  However, it is
  // possible to construct an operation that does a read, does a guard
  // check (e.g., CMPXATTR), and then a write.	Then we either succeed
  // with the write, or return a CMPXATTR and the read value.
  if (successful_write) {
    // write.  normalize the result code.
    dout(20) << " zeroing write result code " << result << dendl;
    result = 0;
  }
  ctx->reply->set_result(result);

  // read or error?
  if (ctx->op_t->empty() || result < 0) {
    if (ctx->pending_async_reads.empty()) {
      complete_read_ctx(result, ctx);
    } else {
      in_progress_async_reads.push_back(make_pair(op, ctx));
      ctx->start_async_reads(this);
    }
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  assert(op->may_write() || op->may_cache());
  // issue replica writes
  ceph_tid_t tid = osd->get_tid();
  Mutation *mutation = new_mutation(ctx, obc, tid);

  mutation->src_obc.swap(src_obc); // and src_obc.

  issue_mutation(mutation);
  eval_mutation(mutation);
  mutation->put();
}

void OSDVol::reply_ctx(OpContext *ctx, int r)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r);
  close_op_ctx(ctx, r);
}

void OSDVol::reply_ctx(OpContext *ctx, int r, eversion_t v, version_t uv)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r, v, uv);
  close_op_ctx(ctx, r);
}

int OSDVol::do_xattr_cmp_uint64_t(int op, uint64_t v1, bufferlist& xattr)
{
  uint64_t v2;
  if (xattr.length())
    v2 = atoll(xattr.c_str());
  else
    v2 = 0;

  dout(20) << "do_xattr_cmp_u64 '" << v1 << "' vs '" << v2 << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1 == v2);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1 != v2);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1 > v2);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1 >= v2);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1 < v2);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1 <= v2);
  default:
    return -EINVAL;
  }
}

int OSDVol::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
{
  string v2s(xattr.c_str(), xattr.length());

  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2s << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1s.compare(v2s) == 0);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1s.compare(v2s) != 0);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1s.compare(v2s) > 0);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1s.compare(v2s) >= 0);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1s.compare(v2s) < 0);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1s.compare(v2s) <= 0);
  default:
    return -EINVAL;
  }
}

// ========================================================================
// low level osd ops

static int check_offset_and_length(uint64_t offset, uint64_t length, uint64_t max)
{
  if (offset >= max ||
      length > max ||
      offset + length > max)
    return -EFBIG;

  return 0;
}

struct FillInExtent : public Context {
  uint64_t *r;
  FillInExtent(uint64_t *r) : r(r) {}
  void finish(int _r) {
    if (_r >= 0) {
      *r = _r;
    }
  }
};

int OSDVol::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
{
  int result = 0;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const oid_t& soid = oi.soid;

  bool first_read = true;

  ObjectStore::Transaction* t = ctx->op_t;

  dout(10) << __func__ << " " << soid << " " << ops << dendl;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p, ctx->current_osd_subop_num++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;

    dout(10) << __func__ << "  " << osd_op << dendl;

    bufferlist::iterator bp = osd_op.indata.begin();

    // user-visible modifcation?
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
      break;
    default:
      if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;
    }

    ObjectContextRef src_obc;
    if (ceph_osd_op_type_multi(op.op)) {
      // For stripulation
      oid_t src_obj(osd_op.oid);
      src_obc = ctx->src_obc[src_obj];
      dout(10) << " src_obj " << src_obj << " obc " << src_obc << dendl;
      assert(src_obc);
    }

    // munge -1 truncate to 0 truncate
    if (op.extent.truncate_seq == 1 && op.extent.truncate_size == (-1ULL)) {
      op.extent.truncate_size = 0;
      op.extent.truncate_seq = 0;
    }

    // munge ZERO -> TRUNCATE?	(don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
	obs.exists &&
	op.extent.offset < cct->_conf->osd_max_object_size &&
	op.extent.length >= 1 &&
	op.extent.length <= cct->_conf->osd_max_object_size &&
	op.extent.offset + op.extent.length >= oi.size) {
      if (op.extent.offset >= oi.size) {
	// no-op
	goto fail;
      }
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      // --- READS ---

    case CEPH_OSD_OP_SYNC_READ:
    case CEPH_OSD_OP_READ:
      ++ctx->num_read;
      {
	uint32_t seq = oi.truncate_seq;
	uint64_t size = oi.size;
	bool trimmed_read = false;
	// are we beyond truncate_size?
	if ( (seq < op.extent.truncate_seq) &&
	     (op.extent.offset + op.extent.length > op.extent.truncate_size) )
	  size = op.extent.truncate_size;

	if (op.extent.offset >= size) {
	  op.extent.length = 0;
	  trimmed_read = true;
	} else if (op.extent.offset + op.extent.length > size) {
	  op.extent.length = size - op.extent.offset;
	  trimmed_read = true;
	}

	// read into a buffer
	bufferlist bl;
	if (trimmed_read && op.extent.length == 0) {
	  // read size was trimmed to zero and it is expected to do nothing
	  // a read operation of 0 bytes does *not* do nothing, this is why
	  // the trimmed_read boolean is needed
	} else {
	  int r = osd->store->read(coll, soid, op.extent.offset,
				   op.extent.length, osd_op.outdata);
	  if (r >= 0)
	    op.extent.length = r;
	  else {
	    result = r;
	    op.extent.length = 0;
	  }
	  dout(10) << " read got " << r << " / " << op.extent.length
		   << " bytes from oid " << soid << dendl;
	}
	if (first_read) {
	  first_read = false;
	  ctx->data_off = op.extent.offset;
	}
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;

      }
      break;

    /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      ++ctx->num_read;
      {
	if (op.extent.truncate_seq) {
	  dout(0) << "sparse_read does not support truncation sequence " << dendl;
	  result = -EINVAL;
	  break;
	}
	// read into a buffer
	bufferlist bl;
	int total_read = 0;
	int r = osd->store->fiemap(coll, soid, op.extent.offset, op.extent.length, bl);
	if (r < 0)  {
	  result = r;
	  break;
	}
	map<uint64_t, uint64_t> m;
	bufferlist::iterator iter = bl.begin();
	::decode(m, iter);
	map<uint64_t, uint64_t>::iterator miter;
	bufferlist data_bl;
	uint64_t last = op.extent.offset;
	for (miter = m.begin(); miter != m.end(); ++miter) {
	  // verify hole?
	  if (cct->_conf->osd_verify_sparse_read_holes &&
	      last < miter->first) {
	    bufferlist t;
	    uint64_t len = miter->first - last;
	    r = osd->store->read(coll, soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid
				<< " sparse-read found data in hole "
				<< last << "~" << len << "\n";
	    }
	  }

	  bufferlist tmpbl;
	  r = osd->store->read(coll, soid, miter->first, miter->second,
			       tmpbl);
	  if (r < 0)
	    break;

	  /* this is usually happen when we get extent that exceeds
	     the actual file size */
	  if (r < (int)miter->second)
	    miter->second = r;
	  total_read += r;
	  dout(10) << "sparse-read " << miter->first << "@" << miter->second << dendl;
	  data_bl.claim_append(tmpbl);
	  last = miter->first + r;
	}

	// verify trailing hole?
	if (cct->_conf->osd_verify_sparse_read_holes) {
	  uint64_t end = MIN(op.extent.offset + op.extent.length, oi.size);
	  if (last < end) {
	    bufferlist t;
	    uint64_t len = end - last;
	    r = osd->store->read(coll, soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid
				<< " sparse-read found data in hole "
				<< last << "~" << len << "\n";
	    }
	  }
	}

	if (r < 0) {
	  result = r;
	  break;
	}

	op.extent.length = total_read;

	::encode(m, osd_op.outdata);
	::encode(data_bl, osd_op.outdata);

	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;

	dout(10) << " sparse_read got " << total_read << " bytes from object " << soid << dendl;
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	string cname, mname;
	bufferlist indata;
	try {
	  bp.copy(op.cls.class_len, cname);
	  bp.copy(op.cls.method_len, mname);
	  bp.copy(op.cls.indata_len, indata);
	} catch (buffer::error& e) {
	  dout(10) << "call unable to decode class + method + indata" << dendl;
	  dout(30) << "in dump: ";
	  osd_op.indata.hexdump(*_dout);
	  *_dout << dendl;
	  result = -EINVAL;
	  break;
	}

	ClassHandler::ClassData *cls;
	result = osd->class_handler->open_class(cname, &cls);
	assert(result == 0);   // init_op_flags() already verified this works.

	ClassHandler::ClassMethod *method = cls->get_method(mname.c_str());
	if (!method) {
	  dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	  result = -EOPNOTSUPP;
	  break;
	}

	int flags = method->get_flags();
	if (flags & CLS_METHOD_WR)
	  ctx->user_modify = true;

	bufferlist outdata;
	dout(10) << "call method " << cname << "." << mname << dendl;
	int prev_rd = ctx->num_read;
	int prev_wr = ctx->num_write;
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);

	if (ctx->num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
	  derr << "method " << cname << "." << mname << " tried to read object but is not marked RD" << dendl;
	  result = -EIO;
	  break;
	}
	if (ctx->num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
	  derr << "method " << cname << "." << mname << " tried to update object but is not marked WR" << dendl;
	  result = -EIO;
	  break;
	}

	dout(10) << "method called response length=" << outdata.length() << dendl;
	op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
	dout(30) << "out dump: ";
	osd_op.outdata.hexdump(*_dout);
	*_dout << dendl;
      }
      break;

    case CEPH_OSD_OP_STAT:
      // note: stat does not require RD
      {
	if (obs.exists) {
	  ::encode(oi.size, osd_op.outdata);
	  ::encode(oi.mtime, osd_op.outdata);
	  ::encode(oi.total_real_length, osd_op.outdata);
	  dout(10) << "stat oi has " << oi.size << " " << oi.mtime <<
		" " << oi.total_real_length  << dendl;
	}
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = objects_get_attr(ctx->obc->obs.oi.soid,
				 name,
				 &(osd_op.outdata));
	if (r >= 0) {
	  op.xattr.value_len = r;
	  result = 0;
	  ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(r, 10);
	  ctx->delta_stats.num_rd++;
	} else
	  result = r;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
      ++ctx->num_read;
      {
	map<string, bufferlist> out;
	result = osd->store->getattrs(coll, ctx->obc->obs.oi.soid, out, true);

	bufferlist bl;
	::encode(out, bl);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(bl.length(), 10);
	ctx->delta_stats.num_rd++;
	osd_op.outdata.claim_append(bl);
      }
      break;

    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_SRC_CMPXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;

	bufferlist xattr;
	if (op.op == CEPH_OSD_OP_CMPXATTR)
	  result = objects_get_attr(
	    ctx->obc->obs.oi.soid,
	    name,
	    &xattr);
	else
	  result = objects_get_attr(
	    src_obc->obs.oi.soid,
	    name,
	    &xattr);
	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;

	ctx->delta_stats.num_rd++;
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(xattr.length(), 10);

	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	  {
	    string val;
	    bp.copy(op.xattr.value_len, val);
	    val[op.xattr.value_len] = 0;
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	  }
	  break;

	case CEPH_OSD_CMPXATTR_MODE_U64:
	  {
	    uint64_t u64val;
	    try {
	      ::decode(u64val, bp);
	    }
	    catch (buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << u64val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_uint64_t(op.xattr.cmp_op, u64val, xattr);
	  }
	  break;

	default:
	  dout(10) << "bad cmp mode " << (int)op.xattr.cmp_mode << dendl;
	  result = -EINVAL;
	}

	if (!result) {
	  dout(10) << "comparison returned false" << dendl;
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  dout(10) << "comparison returned " << result << " " << cpp_strerror(-result) << dendl;
	  break;
	}

	dout(10) << "comparison returned true" << dendl;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      ++ctx->num_read;
      {
	uint64_t ver = op.watch.ver;
	if (!ver)
	  result = -EINVAL;
	else if (ver < oi.user_version)
	  result = -ERANGE;
	else if (ver > oi.user_version)
	  result = -EOVERFLOW;
      }
      break;

    case CEPH_OSD_OP_LIST_WATCHERS:
      ++ctx->num_read;
      {
	obj_list_watch_response_t resp;

	map<pair<uint64_t, entity_name_t>,
	    watch_info_t>::const_iterator oi_iter;
	for (oi_iter = oi.watchers.begin(); oi_iter != oi.watchers.end();
	     ++oi_iter) {
	  dout(20) << "key cookie=" << oi_iter->first.first
		   << " entity=" << oi_iter->first.second << " "
		   << oi_iter->second << dendl;
	  assert(oi_iter->first.first == oi_iter->second.cookie);
	  assert(oi_iter->first.second.is_client());

	  watch_item_t wi(oi_iter->first.second, oi_iter->second.cookie,
		 oi_iter->second.timeout_seconds, oi_iter->second.addr);
	  resp.entries.push_back(wi);
	}

	resp.encode(osd_op.outdata);
	result = 0;

	ctx->delta_stats.num_rd++;
	break;
      }

    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      ++ctx->num_read;
      {
	uint64_t ver = op.assert_ver.ver;
	if (!ver)
	  result = -EINVAL;
	else if (ver < src_obc->obs.oi.user_version)
	  result = -ERANGE;
	else if (ver > src_obc->obs.oi.user_version)
	  result = -EOVERFLOW;
	break;
      }

   case CEPH_OSD_OP_NOTIFY:
      ++ctx->num_read;
      {
	uint32_t ver;
	uint32_t timeout;
	bufferlist bl;

	try {
	  ::decode(ver, bp);
	  ::decode(timeout, bp);
	  ::decode(bl, bp);
	} catch (const buffer::error &e) {
	  timeout = 0;
	}
	if (!timeout)
	  timeout = cct->_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.cookie = op.watch.cookie;
	n.bl = bl;
	ctx->notifies.push_back(n);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      ++ctx->num_read;
      {
	try {
	  uint64_t notify_id = 0;
	  uint64_t watch_cookie = 0;
	  ::decode(notify_id, bp);
	  ::decode(watch_cookie, bp);
	  OpContext::NotifyAck ack(notify_id, watch_cookie);
	  ctx->notify_acks.push_back(ack);
	} catch (const buffer::error &e) {
	  OpContext::NotifyAck ack(
	    // op.watch.cookie is actually the notify_id for historical reasons
	    op.watch.cookie
	    );
	  ctx->notify_acks.push_back(ack);
	}
      }
      break;

    case CEPH_OSD_OP_SETALLOCHINT:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->mod_desc.create();
	  t->touch(coll, soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t->set_alloc_hint(coll, soid, op.alloc_hint.expected_object_size,
			  op.alloc_hint.expected_write_size);
	ctx->delta_stats.num_wr++;
	result = 0;
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      ++ctx->num_write;
      { // write
	if (op.extent.length != osd_op.indata.length()) {
	  dout(10) << __func__ << " write length bad: extent.length " <<
	    op.extent.length << " indata length " << osd_op.indata.length() <<
	    dendl;
	  result = -EINVAL;
	  break;
	}

	if (!obs.exists) {
	  ctx->mod_desc.create();
	} else if (op.extent.offset == oi.size) {
	  ctx->mod_desc.append(oi.size);
	}


	uint32_t seq = oi.truncate_seq;
	dout(20) << "write: total_real_length oi="
		<< oi.total_real_length << " op=" << op.extent.total_real_length
		<< dendl;
	if (oi.total_real_length < op.extent.total_real_length) {
	  oi.total_real_length = op.extent.total_real_length;
	}
	if (seq && (seq > op.extent.truncate_seq) &&
	    (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
	  bufferlist t;
	  t.substr_of(osd_op.indata, 0, op.extent.length);
	  osd_op.indata.swap(t);
	}
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  if (obs.exists) {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", truncating to " << op.extent.truncate_size << dendl;
	    t->truncate(coll, soid, op.extent.truncate_size);
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	    if (op.extent.truncate_size != oi.size) {
	      ctx->delta_stats.num_bytes -= oi.size;
	      ctx->delta_stats.num_bytes += op.extent.truncate_size;
	      oi.size = op.extent.truncate_size;
	    }
	  } else {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq
		     << " > current " << seq << ", but object is new" << dendl;
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	  }
	}
	result = check_offset_and_length(op.extent.offset, op.extent.length,
					 cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;
	t->write(coll, soid, op.extent.offset, op.extent.length,
		 osd_op.indata);
	write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
				    op.extent.offset, op.extent.length, true);
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
      }
      break;

    case CEPH_OSD_OP_WRITEFULL:
      ++ctx->num_write;
      { // write full object
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}
	result = check_offset_and_length(op.extent.offset, op.extent.length, cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;

	if (obs.exists) {
	  t->truncate(coll, soid, 0);
	}
	t->write(coll, soid, op.extent.offset, op.extent.length,
		 osd_op.indata);
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	interval_set<uint64_t> ch;
	if (oi.size > 0)
	  ch.insert(0, oi.size);
	ctx->modified_ranges.union_of(ch);
	if (op.extent.length + op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  oi.size = op.extent.length + op.extent.offset;
	  ctx->delta_stats.num_bytes += oi.size;
	}
	dout(20) << "writefull: total_real_length = "
		<< op.extent.total_real_length << dendl;
	oi.total_real_length = op.extent.total_real_length;
	ctx->delta_stats.num_wr++;
	ctx->delta_stats.num_wr_kb += SHIFT_ROUND_UP(op.extent.length, 10);
      }
      break;

    case CEPH_OSD_OP_ZERO:
      ++ctx->num_write;
      { // zero
	result = check_offset_and_length(op.extent.offset, op.extent.length, cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;
	assert(op.extent.length);
	if (obs.exists) {
	  t->zero(coll, soid, op.extent.offset, op.extent.length);
	  interval_set<uint64_t> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->delta_stats.num_wr++;
	  dout(20) << "zero: total_real_length oi="
		<< oi.total_real_length << " op=" << op.extent.total_real_length
		<< dendl;
	  if (oi.total_real_length < op.extent.total_real_length) {
	    oi.total_real_length = op.extent.total_real_length;
	  }
	} else {
	  // no-op
	}
      }
      break;
    case CEPH_OSD_OP_CREATE:
      ++ctx->num_write;
      {
	int flags = op.flags;
	if (obs.exists && (flags & CEPH_OSD_OP_FLAG_EXCL)) {
	  result = -EEXIST; /* this is an exclusive create */
	} else {
	  if (result >= 0) {
	    if (!obs.exists)
	      ctx->mod_desc.create();
	    t->touch(coll, soid);
	    if (!obs.exists) {
	      ctx->delta_stats.num_objects++;
	      obs.exists = true;
	    }
	  }
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      ++ctx->num_write;
      {
	// truncate
	if (!obs.exists) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}

	if (op.extent.offset > cct->_conf->osd_max_object_size) {
	  result = -EFBIG;
	  break;
	}

	if (op.extent.truncate_seq) {
	  assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
		     << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
		   << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	t->truncate(coll, soid, op.extent.offset);
	if (oi.size > op.extent.offset) {
	  interval_set<uint64_t> trim;
	  trim.insert(op.extent.offset, oi.size-op.extent.offset);
	  ctx->modified_ranges.union_of(trim);
	}
	if (op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  ctx->delta_stats.num_bytes += op.extent.offset;
	  oi.size = op.extent.offset;
	dout(20) << "truncate: total_real_length = "
		<< op.extent.total_real_length << dendl;
	  oi.total_real_length = op.extent.total_real_length;
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.
      }
      break;

    case CEPH_OSD_OP_DELETE:
      ++ctx->num_write;
      if (ctx->obc->obs.oi.watchers.size()) {
	// Cannot delete an object with watchers
	result = -EBUSY;
      } else {
	result = _delete_obj(ctx, false);
      }
      break;

    case CEPH_OSD_OP_WATCH:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	uint64_t cookie = op.watch.cookie;
	bool do_watch = op.watch.flag & 1;
	entity_name_t entity = ctx->reqid.name;
	ObjectContextRef obc = ctx->obc;

	dout(10) << "watch: ctx->obc=" << (void *)obc.get() << " cookie=" << cookie
		 << " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(10) << "watch: oi.user_version=" << oi.user_version<< dendl;
	dout(10) << "watch: peer_addr="
	  << ctx->op->get_req()->get_connection()->get_peer_addr() << dendl;

	watch_info_t w(cookie, cct->_conf->osd_client_watch_timeout,
	  ctx->op->get_req()->get_connection()->get_peer_addr());
	if (do_watch) {
	  if (oi.watchers.count(make_pair(cookie, entity))) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[make_pair(cookie, entity)] = w;
	    t->nop();  // make sure update the object_info on disk!
	  }
	  ctx->watch_connects.push_back(w);
	} else {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by "
		     << entity << dendl;
	    oi.watchers.erase(oi_iter);
	    t->nop();  // update oi on disk
	    ctx->watch_disconnects.push_back(w);
	  } else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
	}
      }
      break;


      // -- object attrs --

    case CEPH_OSD_OP_SETXATTR:
      ++ctx->num_write;
      {
	if (cct->_conf->osd_max_attr_size > 0 &&
	    op.xattr.value_len > cct->_conf->osd_max_attr_size) {
	  result = -EFBIG;
	  break;
	}
	if (!obs.exists) {
	  ctx->mod_desc.create();
	  t->touch(coll, soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;

	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t->setattr(coll, soid, name, bl);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      ++ctx->num_write;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	t->rmattr(coll, soid, name);
	ctx->delta_stats.num_wr++;
      }
      break;

      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
      {
	// just do it inline; this works because we are happy to execute
	// fancy op on replicas as well.
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITE;
	newop.op.extent.offset = oi.size;
	newop.op.extent.length = op.extent.length;
	newop.op.extent.truncate_seq = oi.truncate_seq;
	newop.op.extent.total_real_length = op.extent.total_real_length;
	dout(20) << "append total_real_length = "
		<< newop.op.extent.total_real_length << dendl;
	newop.indata = osd_op.indata;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      t->nop();
      break;


      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	try {
	  ::decode(start_after, bp);
	  ::decode(max_return, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	set<string> out_set;

	ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	  coll, soid
	  );
	assert(iter);
	iter->upper_bound(start_after);
	for (uint64_t i = 0;
	     i < max_return && iter->valid();
	     ++i, iter->next()) {
	  out_set.insert(iter->key());
	}
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	string filter_prefix;
	try {
	  ::decode(start_after, bp);
	  ::decode(max_return, bp);
	  ::decode(filter_prefix, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	map<string, bufferlist> out_set;

	ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	  coll, soid
	  );
	if (!iter) {
	  result = -ENOENT;
	  goto fail;
	}
	iter->upper_bound(start_after);
	if (filter_prefix >= start_after) iter->lower_bound(filter_prefix);
	for (uint64_t i = 0;
	     i < max_return && iter->valid() &&
	       iter->key().substr(0, filter_prefix.size()) == filter_prefix;
	     ++i, iter->next()) {
	  dout(20) << "Found key " << iter->key() << dendl;
	  out_set.insert(make_pair(iter->key(), iter->value()));
	}
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETHEADER:
      ++ctx->num_read;
      {
	osd->store->omap_get_header(coll, soid, &osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
      ++ctx->num_read;
      {
	set<string> keys_to_get;
	try {
	  ::decode(keys_to_get, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	map<string, bufferlist> out;
	osd->store->omap_get_values(coll, soid, keys_to_get, &out);
	::encode(out, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAP_CMP:
      ++ctx->num_read;
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	map<string, pair<bufferlist, int> > assertions;
	try {
	  ::decode(assertions, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}

	map<string, bufferlist> out;

	set<string> to_get;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i)
	  to_get.insert(i->first);
	int r = osd->store->omap_get_values(coll, soid, to_get, &out);
	if (r < 0) {
	  result = r;
	  break;
	}
	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	bufferlist empty;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i) {
	  bufferlist &bl = out.count(i->first) ?
	    out[i->first] : empty;
	  switch (i->second.second) {
	  case CEPH_OSD_CMPXATTR_OP_EQ:
	    if (!(bl == i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_LT:
	    if (!(bl < i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_GT:
	    if (!(bl > i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  default:
	    r = -EINVAL;
	    break;
	  }
	  if (r < 0)
	    break;
	}
	if (r < 0) {
	  result = r;
	}
      }
      break;

      // OMAP Write ops
    case CEPH_OSD_OP_OMAPSETVALS:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t->touch(coll, soid);
	map<string, bufferlist> to_set;
	try {
	  ::decode(to_set, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	dout(20) << "setting vals: " << dendl;
	for (map<string, bufferlist>::iterator i = to_set.begin();
	     i != to_set.end();
	     ++i) {
	  dout(20) << "\t" << i->first << dendl;
	}
	t->omap_setkeys(coll, soid, to_set);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_OMAPSETHEADER:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t->touch(coll, soid);
	t->omap_setheader(coll, soid, osd_op.indata);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_OMAPCLEAR:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	t->touch(coll, soid);
	t->omap_clear(coll, soid);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_OMAPRMKEYS:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	t->touch(coll, soid);
	set<string> to_rm;
	try {
	  ::decode(to_rm, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t->omap_rmkeys(coll, soid, to_rm);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    default:
      dout(1) << "unrecognized osd op " << op.op
	      << " " << ceph_osd_op_name(op.op)
	      << dendl;
      result = -EOPNOTSUPP;
    }

    ctx->bytes_read += osd_op.outdata.length();

    fail:
    osd_op.rval = result;
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK))
      result = 0;

    if (result < 0)
      break;
  }
  return result;
}

inline int OSDVol::_delete_obj(OpContext *ctx, bool no_whiteout)
{
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const oid_t& soid = oi.soid;
  ObjectStore::Transaction* t = ctx->op_t;

  if (!obs.exists)
    return -ENOENT;

  t->remove(coll, soid);

  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
  }

  ctx->delta_stats.num_wr++;
  ctx->delta_stats.num_bytes -= oi.size;
  oi.size = 0;

  ctx->delta_stats.num_objects--;
  obs.exists = false;
  return 0;
}

void OSDVol::make_writeable(OpContext *ctx)
{
  const oid_t& soid = ctx->obs->oi.soid;
  ObjectStore::Transaction *t = new ObjectStore::Transaction();

  if ((ctx->new_obs.exists &&
       ctx->new_obs.oi.is_omap()) &&
      (!ctx->obc->obs.exists ||
       !ctx->obc->obs.oi.is_omap())) {
    ++ctx->delta_stats.num_objects_omap;
  }
  if ((!ctx->new_obs.exists ||
       !ctx->new_obs.oi.is_omap()) &&
      (ctx->obc->obs.exists &&
       ctx->obc->obs.oi.is_omap())) {
    --ctx->delta_stats.num_objects_omap;
  }

  // prepend transaction to op_t
  t->append(*ctx->op_t);
  delete ctx->op_t;
  ctx->op_t = t;

  dout(20) << "make_writeable " << soid << dendl;
}


void OSDVol::write_update_size_and_usage(
  object_stat_sum_t& delta_stats, object_info_t& oi,
  interval_set<uint64_t>& modified, uint64_t offset, uint64_t length,
  bool count_bytes)
{
  interval_set<uint64_t> ch;
  if (length)
    ch.insert(offset, length);
  modified.union_of(ch);
  if (length && (offset + length > oi.size)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes += new_size - oi.size;
    oi.size = new_size;
  }
  delta_stats.num_wr++;
  if (count_bytes)
    delta_stats.num_wr_kb += SHIFT_ROUND_UP(length, 10);
}

void OSDVol::do_osd_op_effects(OpContext *ctx)
{
  ConnectionRef conn(ctx->op->get_req()->get_connection());
  entity_name_t entity = ctx->reqid.name;

  for (list<watch_info_t>::iterator i = ctx->watch_connects.begin();
       i != ctx->watch_connects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, entity);
    WatchRef watch;
    if (ctx->obc->watchers.count(watcher)) {
      dout(15) << "do_osd_op_effects found existing watch watcher " << watcher
	       << dendl;
      watch = ctx->obc->watchers[watcher];
    } else {
      dout(15) << "do_osd_op_effects new watcher " << watcher
	       << dendl;
      watch = Watch::makeWatchRef(
	this, osd, ctx->obc, i->timeout_seconds,
	i->cookie, entity, conn->get_peer_addr());
      ctx->obc->watchers.insert(
	make_pair(
	  watcher,
	  watch));
    }
    watch->connect(conn);
  }

  for (list<watch_info_t>::iterator i = ctx->watch_disconnects.begin();
       i != ctx->watch_disconnects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, entity);
    if (ctx->obc->watchers.count(watcher)) {
      WatchRef watch = ctx->obc->watchers[watcher];
      dout(10) << "do_osd_op_effects applying disconnect found watcher "
	       << watcher << dendl;
      ctx->obc->watchers.erase(watcher);
      watch->remove();
    } else {
      dout(10) << "do_osd_op_effects failed to find watcher "
	       << watcher << dendl;
    }
  }

  for (list<notify_info_t>::iterator p = ctx->notifies.begin();
       p != ctx->notifies.end();
       ++p) {
    dout(10) << "do_osd_op_effects, notify " << *p << dendl;
    NotifyRef notif(
      Notify::makeNotifyRef(
	conn,
	ctx->obc->watchers.size(),
	p->bl,
	p->timeout,
	p->cookie,
	osd->get_next_id(get_osdmap()->get_epoch()),
	ctx->obc->obs.oi.user_version,
	osd));
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      dout(10) << "starting notify on watch " << i->first << dendl;
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    dout(10) << "notify_ack " << make_pair(p->watch_cookie, p->notify_id) << dendl;
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
	  p->watch_cookie.get() != i->first.first) continue;
      dout(10) << "acking notify on watch " << i->first << dendl;
      i->second->notify_ack(p->notify_id);
    }
  }
}

int OSDVol::prepare_transaction(OpContext *ctx)
{
  assert(!ctx->ops.empty());

  // prepare the actual mutation
  int result = do_osd_ops(ctx, ctx->ops);
  if (result < 0)
    return result;

  // finish side-effects
  if (result == 0)
    do_osd_op_effects(ctx);

  // read-op?  done?
  if (ctx->op_t->empty() && !ctx->modify) {
    return result;
  }

  make_writeable(ctx);

  finish_ctx(ctx);

  return result;
}

void OSDVol::finish_ctx(OpContext *ctx)
{
  const oid_t& soid = ctx->obs->oi.soid;
  dout(20) << __func__ << " " << soid << " " << ctx
	   << dendl;

  // finish and log the op.
  if (ctx->user_modify) {
    // update the user_version for any modify ops, except for the watch op
    ctx->user_at_version = MAX(info.last_user_version, ctx->new_obs.oi.user_version) + 1;
    /* In order for new clients and old clients to interoperate properly
     * when exchanging versions, we need to lower bound the user_version
     * (which our new clients pay proper attention to)
     * by the at_version (which is all the old clients can ever see). */
    if (ctx->at_version.version > ctx->user_at_version)
      ctx->user_at_version = ctx->at_version.version;
    ctx->new_obs.oi.user_version = ctx->user_at_version;
  }
  ctx->bytes_written = ctx->op_t->get_encoded_bytes();

  if (ctx->new_obs.exists) {
    // on the head object
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = ctx->obs->oi.version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != ceph::real_time::min()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->new_obs.oi, bv);
    ctx->op_t->setattr(coll, ctx->obc->obs.oi.soid, OI_ATTR, bv);
  } else {
    ctx->new_obs.oi = object_info_t(ctx->obc->obs.oi.soid);
  }

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;
}

void OSDVol::complete_read_ctx(int result, OpContext *ctx)
{
  MOSDOp *m = static_cast<MOSDOp*>(ctx->op->get_req());
  assert(ctx->async_reads_complete());
  ctx->reply->claim_op_out_data(ctx->ops);
  ctx->reply->get_header().data_off = ctx->data_off;

  MOSDOpReply *reply = ctx->reply;
  ctx->reply = NULL;

  if (result >= 0) {
    // on read, return the current object version
    reply->set_reply_versions(eversion_t(), ctx->obs->oi.user_version);
  } else if (result == -ENOENT) {
    // on ENOENT, set a floor for what the next user version will be.
    reply->set_enoent_reply_versions(info.last_update, info.last_user_version);
  }

  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
  close_op_ctx(ctx, 0);
}

// ========================================================================
// rep op gather

class C_OSD_MutationApplied : public Context {
  OSDVolRef vol;
  boost::intrusive_ptr<OSDVol::Mutation> mutation;
public:
  C_OSD_MutationApplied(OSDVol *vol, OSDVol::Mutation *mutation)
  : vol(vol), mutation(mutation) {}
  void finish(int) {
    OSDVol::lock_guard vl(vol->lock);
    vol->mutations_all_applied(mutation.get());
  }
};


void OSDVol::mutations_all_applied(Mutation *mutation)
{
  dout(10) << __func__ << ": mutation tid "
	   << mutation->tid << " all applied " << dendl;
  mutation->applied = true;
  if (!mutation->aborted) {
    eval_mutation(mutation);
    if (mutation->on_applied) {
     mutation->on_applied->complete(0);
     mutation->on_applied = NULL;
    }
  }
}

class C_OSD_MutationCommit : public Context {
  OSDVolRef vol;
  boost::intrusive_ptr<OSDVol::Mutation> mutation;
public:
  C_OSD_MutationCommit(OSDVol *vol, OSDVol::Mutation *mutation)
    : vol(vol), mutation(mutation) {}
  void finish(int) {
    OSDVol::lock_guard vl(vol->lock);
    vol->mutations_all_committed(mutation.get());
  }
};

void OSDVol::mutations_all_committed(Mutation *mutation)
{
  dout(10) << __func__ << ": mutation tid " << mutation->tid
	   << " all committed " << dendl;
  mutation->committed = true;

  if (!mutation->aborted) {
    if (mutation->v != eversion_t()) {
      last_update_ondisk = mutation->v;
    }
    eval_mutation(mutation);
  }
}

void OSDVol::eval_mutation(Mutation *mutation)
{
  MOSDOp *m = NULL;
  if (mutation->ctx->op)
    m = static_cast<MOSDOp *>(mutation->ctx->op->get_req());

  if (mutation->done)
    return;

  if (m) {
    // an 'ondisk' reply implies 'ack'. so, prefer to send just one
    // ondisk instead of ack followed by ondisk.

    // ondisk?
    if (mutation->committed) {
      // send dup commits, in order
      if (waiting_for_ondisk.count(mutation->v)) {
	assert(waiting_for_ondisk.begin()->first == mutation->v);
	for (list<OpRequestRef>::iterator i
	       = waiting_for_ondisk[mutation->v].begin();
	     i != waiting_for_ondisk[mutation->v].end();
	     ++i) {
	  osd->reply_op_error(*i, 0, mutation->ctx->at_version,
			      mutation->ctx->user_at_version);
	}
	waiting_for_ondisk.erase(mutation->v);
      }

      // clear out acks, we sent the commits above
      if (waiting_for_ack.count(mutation->v)) {
	assert(waiting_for_ack.begin()->first == mutation->v);
	waiting_for_ack.erase(mutation->v);
      }

      if (m->wants_ondisk() && !mutation->sent_disk) {
	// send commit.
	MOSDOpReply *reply = mutation->ctx->reply;
	if (reply)
	  mutation->ctx->reply = NULL;
	else {
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0, true);
	  reply->set_reply_versions(mutation->ctx->at_version,
				    mutation->ctx->user_at_version);
	}
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending commit on " << mutation->tid << " "
		 << reply << dendl;

        if (mutation->ctx->op->trace) {
          mutation->ctx->op->trace.event("eval_mutation sending commit",
                                         &trace_endpoint);
          // send reply with a child span
          Messenger *msgr = m->get_connection()->get_messenger();
          reply->trace.init("MOSDOpReply", msgr->get_trace_endpoint(),
                            &mutation->ctx->op->trace);
        }
	osd->send_message_osd_client(reply, m->get_connection());
	mutation->sent_disk = true;
	mutation->ctx->op->mark_commit_sent();
      }
    }

    // applied?
    if (mutation->applied) {
      // send dup acks, in order
      if (waiting_for_ack.count(mutation->v)) {
	assert(waiting_for_ack.begin()->first == mutation->v);
	for (list<OpRequestRef>::iterator i
	       = waiting_for_ack[mutation->v].begin();
	     i != waiting_for_ack[mutation->v].end();
	     ++i) {
	  MOSDOp *m = (MOSDOp*)(*i)->get_req();
	  MOSDOpReply *reply = new MOSDOpReply(
	    m, 0,get_osdmap()->get_epoch(), 0, true);
	  reply->set_reply_versions(mutation->ctx->at_version,
				    mutation->ctx->user_at_version);
	  reply->add_flags(CEPH_OSD_FLAG_ACK);

          if ((*i)->trace) {
            (*i)->trace.event("eval_mutation sending ack", &trace_endpoint);
            // send reply with a child span
            Messenger *msgr = m->get_connection()->get_messenger();
            reply->trace.init("MOSDOpReply", msgr->get_trace_endpoint(),
                              &(*i)->trace);
          }

	  osd->send_message_osd_client(reply, m->get_connection());
	}
	waiting_for_ack.erase(mutation->v);
      }

      if (m->wants_ack() && !mutation->sent_disk) {
	// send ack
	MOSDOpReply *reply = mutation->ctx->reply;
	if (reply)
	  mutation->ctx->reply = NULL;
	else {
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0, true);
	  reply->set_reply_versions(mutation->ctx->at_version,
				    mutation->ctx->user_at_version);
	}
	reply->add_flags(CEPH_OSD_FLAG_ACK);
	dout(10) << " sending ack on " << mutation->tid << " " << reply
		 << dendl;

        if (mutation->ctx->op->trace) {
          mutation->ctx->op->trace.event("eval_mutation sending ack",
                                         &trace_endpoint);
          // send reply with a child span
          Messenger *msgr = m->get_connection()->get_messenger();
          reply->trace.init("MOSDOpReply", msgr->get_trace_endpoint(),
                            &mutation->ctx->op->trace);
        }

	assert(entity_name_t::TYPE_OSD != m->get_connection()->peer_type);
	osd->send_message_osd_client(reply, m->get_connection());
      }

      // note the write is now readable (for rlatency calc).  note
      // that this will only be defined if the write is readable
      // _prior_ to being committed; it will not get set with
      // writeahead journaling, for instance.
      if (mutation->ctx->readable_stamp ==
	  ceph::mono_time::min())
	mutation->ctx->readable_stamp = ceph::mono_clock::now();
    }
  }

  // done.
  if (mutation->applied && mutation->committed) {
    lock_guard ml(mutation_lock);
    mutation->done = true;

    release_op_ctx_locks(mutation->ctx);

    dout(10) << " removing " << mutation->tid << dendl;
    assert(!mutation_queue.empty());
    dout(20) << "   q front is " << mutation_queue.front()->tid << dendl;
    remove_mutation(mutation);
  }
}

OSDVol::Mutation *OSDVol::new_mutation(OpContext *ctx, ObjectContextRef obc,
				ceph_tid_t tid)
{
  if (ctx->op)
    dout(10) << "new_mutation tid " << tid << " on " << *ctx->op->get_req() << dendl;
  else
    dout(10) << "new_mutation _tid " << tid << " (no op)" << dendl;

  Mutation *mutation = new Mutation(ctx, obc, tid);

  lock_guard ml(mutation_lock);
  mutation_queue.push_back(&mutation->queue_item);
  mutation->get();

  return mutation;
}

void OSDVol::remove_mutation(Mutation *mutation)
{
  // Should be called with mutation_lock locked
  dout(20) << __func__ << " " << mutation->tid << dendl;
  release_op_ctx_locks(mutation->ctx);
  mutation->ctx->finish(0);  // FIXME: return value here is sloppy
  mutation_queue.remove(&mutation->queue_item);
  mutation->put();
}

OSDVol::Mutation *OSDVol::simple_mutation_create(ObjectContextRef obc)
{
  dout(20) << __func__ << " " << obc->obs.oi.soid << dendl;
  vector<OSDOp> ops;
  ceph_tid_t tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, this);
  ctx->op_t = new ObjectStore::Transaction;
  ctx->mtime = ceph::real_clock::now();
  ctx->obc = obc;
  Mutation *mutation = new_mutation(ctx, obc, tid);
  return mutation;
}

void OSDVol::simple_mutation_submit(Mutation *mutation)
{
  dout(20) << __func__ << " " << mutation->tid << dendl;
  issue_mutation(mutation);
  eval_mutation(mutation);
  mutation->put();
}

// -------------------------------------------------------

void OSDVol::handle_watch_timeout(WatchRef watch)
{
  ObjectContextRef obc = watch->get_obc(); // handle_watch_timeout owns this ref
  dout(10) << "handle_watch_timeout obc " << obc << dendl;

  obc->watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  obc->obs.oi.watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  watch->remove();

  vector<OSDOp> ops;
  ceph_tid_t tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, this);
  ctx->op_t = new ObjectStore::Transaction();
  ctx->mtime = ceph::real_clock::now();
  ctx->at_version = get_next_version();

  entity_inst_t nobody;

  Mutation *mutation = new_mutation(ctx, obc, tid);

  ObjectStore::Transaction *t = ctx->op_t;

  obc->obs.oi.prior_version = mutation->obc->obs.oi.version;
  obc->obs.oi.version = ctx->at_version;
  bufferlist bl;
  ::encode(obc->obs.oi, bl);
  t->setattr(coll, obc->obs.oi.soid, OI_ATTR, bl);

  // obc ref swallowed by mutation!
  issue_mutation(mutation);
  eval_mutation(mutation);
  mutation->put();
}

ObjectContextRef OSDVol::create_object_context(const object_info_t& oi)
{
  ObjectContextRef obc(object_contexts.lookup_or_create(oi.soid));
  assert(obc->destructor_callback == NULL);
  obc->destructor_callback = new C_Vol_ObjectContext(this, obc.get());
  obc->obs.oi = oi;
  obc->obs.exists = false;
  dout(10) << "create_object_context " << (void*)obc.get() << " " << oi.soid
	   << " " << dendl;
  populate_obc_watchers(obc);
  return obc;
}

ObjectContextRef OSDVol::get_object_context(
  const oid_t& soid, bool can_create, map<string, bufferlist> *attrs)
{
  ObjectContextRef obc = object_contexts.lookup(soid);
  if (obc) {
    dout(10) << __func__ << ": found obc in cache: " << obc
	     << dendl;
  } else {
    // check disk
    bufferlist bv;
    if (attrs) {
      assert(attrs->count(OI_ATTR));
      bv = attrs->find(OI_ATTR)->second;
    } else {
      int r = objects_get_attr(soid, OI_ATTR, &bv);
      if (r < 0) {
	if (!can_create) {
	  dout(10) << __func__ << ": no obc for soid "
		   << soid << " and !can_create"
		   << dendl;
	  return ObjectContextRef();   // -ENOENT!
	}

	dout(10) << __func__ << ": no obc for soid "
		 << soid << " but can_create"
		 << dendl;
	// new object.
	object_info_t oi(soid);
	obc = create_object_context(oi);
	dout(10) << __func__ << ": " << obc << " " << soid
		 << " " << obc->rwstate
		 << " oi: " << obc->obs.oi << dendl;
	return obc;
      }
    }

    object_info_t oi(bv);

    obc = object_contexts.lookup_or_create(oi.soid);
    obc->destructor_callback = new C_Vol_ObjectContext(this, obc.get());
    obc->obs.oi = oi;
    obc->obs.exists = true;

    populate_obc_watchers(obc);

    dout(10) << __func__ << ": creating obc from disk: " << obc
	     << dendl;
  }
  dout(10) << __func__ << ": " << obc << " " << soid
	   << " " << obc->rwstate
	   << " oi: " << obc->obs.oi << dendl;
  return obc;
}

void OSDVol::context_registry_on_change()
{
  pair<oid_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    if (obc) {
      for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	     obc->watchers.begin();
	   j != obc->watchers.end();
	   obc->watchers.erase(j++)) {
	j->second->discard();
      }
    }
  }
}


/*
 * If we return an error, and set *pmissing, then promoting that
 * object may help.
 *
 * If we return -EAGAIN, we will always set *pmissing to the missing
 * object to wait for.
 *
 * If we return an error but do not set *pmissing, then we know the
 * object does not exist.
 */
int OSDVol::find_object_context(const oid_t& oid,
				ObjectContextRef *pobc,
				bool can_create)
{
  ObjectContextRef obc = get_object_context(oid, can_create);
  if (!obc) {
    return -ENOENT;
  }
  dout(10) << "find_object_context " << oid
	   << " oi=" << obc->obs.oi
	   << dendl;
  *pobc = obc;
  return 0;
}

void OSDVol::object_context_destructor_callback(ObjectContext *obc)
{
  return;
}

/*
 * Volume status change notification
 */

void OSDVol::apply_mutations(bool requeue)
{
  list<OpRequestRef> rq;

  // apply all mutations
  unique_lock ml(mutation_lock); // Not exception safe, fix.
  while (!mutation_queue.empty()) {
    Mutation *mutation = mutation_queue.front();
    mutation_queue.pop_front();
    dout(10) << " applying mutation tid " << mutation->tid << dendl;
    mutation->aborted = true;
    if (mutation->on_applied) {
      delete mutation->on_applied;
      mutation->on_applied = NULL;
    }

    if (requeue) {
      if (mutation->ctx->op) {
	dout(10) << " requeuing " << *mutation->ctx->op->get_req() << dendl;
	rq.push_back(mutation->ctx->op);
	mutation->ctx->op = OpRequestRef();
      }

      // also requeue any dups, interleaved into position
      map<eversion_t, list<OpRequestRef> >::iterator p = waiting_for_ondisk.find(mutation->v);
      if (p != waiting_for_ondisk.end()) {
	dout(10) << " also requeuing ondisk waiters " << p->second << dendl;
	rq.splice(rq.end(), p->second);
	waiting_for_ondisk.erase(p);
      }
    }

    remove_mutation(mutation);
  }
  ml.unlock();

  if (requeue) {
    requeue_ops(rq);
    if (!waiting_for_ondisk.empty()) {
      for (map<eversion_t, list<OpRequestRef> >::iterator i =
	     waiting_for_ondisk.begin();
	   i != waiting_for_ondisk.end();
	   ++i) {
	for (list<OpRequestRef>::iterator j = i->second.begin();
	     j != i->second.end();
	     ++j) {
	  derr << __func__ << ": op " << *((*j)->get_req()) << " waiting on "
	       << i->first << dendl;
	}
      }
      assert(waiting_for_ondisk.empty());
    }
  }

  waiting_for_ondisk.clear();
  waiting_for_ack.clear();
}

entity_name_t OSDVol::get_cluster_msgr_name() {
  return osd->get_cluster_msgr_name();
}

ceph_tid_t OSDVol::get_tid() {
  return osd->get_tid();
}

LogClientTemp OSDVol::clog_error() {
  return osd->clog.error();
}

void intrusive_ptr_add_ref(OSDVol::Mutation *mutation) { mutation->get(); }
void intrusive_ptr_release(OSDVol::Mutation *mutation) { mutation->put(); }

// From the Backend

int OSDVol::objects_list_partial(const oid_t &begin,
			     int min, int max,
			     vector<oid_t> *ls,
			     oid_t *next)
{
  assert(ls);
  oid_t _next(begin);
  ls->reserve(max);
  int r = 0;
  while (ls->size() < (unsigned)min) {
    vector<oid_t> objects;
    int r = osd->store->collection_list_partial(
      coll,
      _next,
      min - ls->size(),
      max - ls->size(),
      &objects,
      &_next);
    if (r != 0)
      break;
    for (vector<oid_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      ls->push_back(*i);
    }
  }
  if (r == 0)
    *next = _next;
  return r;
}

int OSDVol::objects_list_range(const oid_t &start, const oid_t &end,
			   vector<oid_t> *ls)
{
  assert(ls);
  vector<oid_t> objects;
  int r = osd->store->collection_list_range(
    coll,
    start,
    end,
    &objects);
  ls->reserve(objects.size());
  for (vector<oid_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
      ls->push_back(*i);
  }
  return r;
}

int OSDVol::objects_get_attr(const oid_t &hoid, const string &attr,
			 bufferlist *out)
{
  bufferptr bp;
  int r = osd->store->getattr(coll, hoid, attr.c_str(), bp);
  if (r >= 0 && out) {
    out->clear();
    out->push_back(bp);
  }
  return r;
}

void OSDVol::objects_read_async(const oid_t &hoid,
			    const list<pair<pair<uint64_t, uint64_t>,
			    pair<bufferlist*, Context*> > > &to_read,
			    Context *on_complete)
{
  int r = 0;
  for (list<pair<pair<uint64_t, uint64_t>,
	 pair<bufferlist*, Context*> > >::const_iterator i =
	 to_read.begin();
       i != to_read.end() && r >= 0;
       ++i) {
    int _r = osd->store->read(coll, hoid, i->first.first,
			 i->first.second, *(i->second.first));
    if (i->second.second) {
      i->second.second->complete(_r);
    }
    if (_r < 0)
      r = _r;
  }
  on_complete->complete(r);
}

void OSDVol::issue_mutation(Mutation *mutation)
{
  OpContext *ctx = mutation->ctx;
  const oid_t& soid = ctx->obs->oi.soid;
  ObjectStore::Transaction *op_t = ctx->op_t;

  dout(7) << "issue_mutation tid " << mutation->tid
	  << " o " << soid
	  << dendl;

  mutation->v = ctx->at_version;
  mutation->obc->ondisk_write_lock();

  Context *on_all_commit = new C_OSD_MutationCommit(this, mutation);
  Context *on_all_applied = new C_OSD_MutationApplied(this, mutation);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(mutation->obc,
							ObjectContextRef(),
							ObjectContextRef());

  op_t->register_on_applied_sync(onapplied_sync);
  op_t->register_on_applied(on_all_applied);
  op_t->register_on_applied(new ObjectStore::C_DeleteTransaction(op_t));
  op_t->register_on_commit(on_all_commit);

  osd->store->queue_transaction(osr.get(), op_t, 0, 0, 0, mutation->ctx->op);
  mutation->ctx->op_t = NULL;
}
