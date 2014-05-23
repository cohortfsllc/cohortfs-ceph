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

#include "PG.h"
#include "common/errno.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGScan.h"

#include "common/BackTrace.h"

#include <sstream>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
template <class T>
static ostream& _prefix(std::ostream *_dout, T *t)
{
  return *_dout << t->gen_prefix();
}

void PG::get(const string &tag) 
{
  ref.inc();
#ifdef PG_DEBUG_REFS
  Mutex::Locker l(_ref_id_lock);
  if (!_tag_counts.count(tag)) {
    _tag_counts[tag] = 0;
  }
  _tag_counts[tag]++;
#endif
}

void PG::put(const string &tag)
{
#ifdef PG_DEBUG_REFS
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_tag_counts.count(tag));
    _tag_counts[tag]--;
    if (_tag_counts[tag] == 0) {
      _tag_counts.erase(tag);
    }
  }
#endif
  if (ref.dec() == 0)
    delete this;
}

#ifdef PG_DEBUG_REFS
uint64_t PG::get_with_id()
{
  ref.inc();
  Mutex::Locker l(_ref_id_lock);
  uint64_t id = ++_ref_id;
  BackTrace bt(0);
  stringstream ss;
  bt.print(ss);
  dout(20) << __func__ << ": " << info.pgid << " got id " << id << dendl;
  assert(!_live_ids.count(id));
  _live_ids.insert(make_pair(id, ss.str()));
  return id;
}

void PG::put_with_id(uint64_t id)
{
  dout(20) << __func__ << ": " << info.pgid << " put id " << id << dendl;
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_live_ids.count(id));
    _live_ids.erase(id);
  }
  if (ref.dec() == 0)
    delete this;
}

void PG::dump_live_ids()
{
  Mutex::Locker l(_ref_id_lock);
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live ids:" << dendl;
  for (map<uint64_t, string>::iterator i = _live_ids.begin();
       i != _live_ids.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live tags:" << dendl;
  for (map<string, uint64_t>::iterator i = _tag_counts.begin();
       i != _tag_counts.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
}
#endif

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  assert(pi);
  info = *pi;
  auid = pi->auid;
  name = map->get_pool_name(id);
}

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, pg_t p, const hobject_t& loid,
       const hobject_t& ioid) :
  osd(o),
  cct(o->cct),
  osdriver(osd->store, coll_t()),
  map_lock("PG::map_lock"),
  osdmap_ref(curmap), last_persisted_osdmap_ref(curmap), pool(_pool),
  _lock("PG::_lock"),
  ref(0),
  #ifdef PG_DEBUG_REFS
  _ref_id_lock("PG::_ref_id_lock"), _ref_id(0),
  #endif
  deleting(false), dirty_info(false),
  info(p),
  info_struct_v(0),
  coll(p), log_oid(loid),
  stat_queue_item(this),
  state(0),
  pg_stats_publish_lock("PG::pg_stats_publish_lock"),
  pg_stats_publish_valid(false),
  osr(osd->osr_registry.lookup_or_create(p, (stringify(p)))),
  finish_sync_event(NULL),
  active_pushes(0)
{
#ifdef PG_DEBUG_REFS
  osd->add_pgid(p, this);
#endif
}

PG::~PG()
{
#ifdef PG_DEBUG_REFS
  osd->remove_pgid(info.pgid, this);
#endif
}

void PG::lock_suspend_timeout(ThreadPool::TPHandle &handle)
{
  handle.suspend_tp_timeout();
  lock();
  handle.reset_tp_timeout();
}

void PG::lock(bool no_lockdep)
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  assert(!dirty_info);

  dout(30) << "lock" << dendl;
}

std::string PG::gen_prefix() const
{
  stringstream out;
  OSDMapRef mapref = osdmap_ref;
  if (_lock.is_locked_by_me()) {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " " << *this << " ";
  } else {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " pg[" << info.pgid << "(unlocked)] ";
  }
  return out.str();
}
  
/********* PG **********/

void PG::remove_object(
  ObjectStore::Transaction &t, const hobject_t &soid)
{
  t.remove(coll, soid);
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  last_update_ondisk = eversion_t();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  osd->remove_want_pg_temp(info.pgid);
}

struct C_PG_ActivateCommitted : public Context {
  PGRef pg;
  epoch_t epoch;
  C_PG_ActivateCommitted(PG *p, epoch_t e)
    : pg(p), epoch(e) {}
  void finish(int r) {
    pg->_activate_committed(epoch);
  }
};

void PG::activate(ObjectStore::Transaction& t,
		  epoch_t query_epoch)
{
  assert(!is_active());

  // twiddle pg state
  state_clear(PG_STATE_DOWN);

  info.last_epoch_started = query_epoch;

  last_update_ondisk = info.last_update;
  last_update_applied = info.last_update;

  // write pg info, log
  dirty_info = true;

  // find out when we commit
  t.register_on_complete(new C_PG_ActivateCommitted(this, query_epoch));
}

bool PG::op_has_sufficient_caps(OpRequestRef op)
{
  // only check MOSDOp
  if (op->get_req()->get_type() != CEPH_MSG_OSD_OP)
    return true;

  MOSDOp *req = static_cast<MOSDOp*>(op->get_req());

  OSD::Session *session = (OSD::Session *)req->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();

  string key = req->get_object_locator().key;
  if (key.length() == 0)
    key = req->get_oid().name;

  bool cap = caps.is_capable(pool.name, req->get_object_locator().nspace,
                             pool.auid, key,
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->need_class_read_cap(),
			     op->need_class_write_cap());

  dout(20) << "op_has_sufficient_caps pool=" << pool.id << " (" << pool.name
		   << " " << req->get_object_locator().nspace
	   << ") owner=" << pool.auid
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " need_class_read_cap=" << op->need_class_read_cap()
	   << " need_class_write_cap=" << op->need_class_write_cap()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void PG::take_op_map_waiters()
{
  Mutex::Locker l(map_lock);
  for (list<OpRequestRef>::iterator i = waiting_for_map.begin();
       i != waiting_for_map.end();
       ) {
    if (op_must_wait_for_map(get_osdmap_with_maplock(), *i)) {
      break;
    } else {
      osd->op_wq.queue(make_pair(PGRef(this), *i));
      waiting_for_map.erase(i++);
    }
  }
}

void PG::queue_op(OpRequestRef op)
{
  Mutex::Locker l(map_lock);
  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    return;
  }
  if (op_must_wait_for_map(get_osdmap_with_maplock(), op)) {
    waiting_for_map.push_back(op);
    return;
  }
  osd->op_wq.queue(make_pair(PGRef(this), op));
}

void PG::_activate_committed(epoch_t e)
{
  lock();
  all_activated_and_committed();

  if (dirty_info) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
    assert(tr == 0);
  }

  unlock();
}

/*
 * update info.history.last_epoch_started ONLY after we and all
 * replicas have activated AND committed the activate transaction
 * (i.e. the peering results are stable on disk).
 */
void PG::all_activated_and_committed()
{
  dout(10) << "all_activated_and_committed" << dendl;

  // info.last_epoch_started is set during activate()
  info.history.last_epoch_started = info.last_epoch_started;
  state_clear(PG_STATE_CREATING);

  publish_stats_to_osd();
}

void PG::_update_calc_stats()
{
  info.stats.version = info.last_update;
  info.stats.created = info.history.epoch_created;
}

void PG::publish_stats_to_osd()
{
  pg_stats_publish_lock.Lock();
  // update our stat summary
  info.stats.reported_epoch = get_osdmap()->get_epoch();
  ++info.stats.reported_seq;

  utime_t now = ceph_clock_now(cct);
  info.stats.last_fresh = now;
  if (info.stats.state != state) {
    info.stats.state = state;
    info.stats.last_change = now;
    if ((state & PG_STATE_ACTIVE) &&
	!(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_became_active = now;
  }
  if (info.stats.state & PG_STATE_ACTIVE)
    info.stats.last_active = now;
  info.stats.last_unstale = now;

  pg_stats_publish_valid = true;
  pg_stats_publish = info.stats;
  pg_stats_publish.stats.add(unstable_stats);

  dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported_epoch
	   << ":" << pg_stats_publish.reported_seq << dendl;
  pg_stats_publish_lock.Unlock();
  osd->pg_stat_queue_enqueue(this);
}

void PG::clear_publish_stats()
{
  dout(15) << "clear_stats" << dendl;
  pg_stats_publish_lock.Lock();
  pg_stats_publish_valid = false;
  pg_stats_publish_lock.Unlock();

  osd->pg_stat_queue_dequeue(this);
}

/**
 * initialize a newly instantiated pg
 *
 * Initialize PG state, as when a PG is initially created, or when it
 * is first instantiated on the current node.
 *
 * @param role our role/rank
 * @param newup up set
 * @param newacting acting set
 * @param history pg history
 * @param pi past_intervals
 * @param backfill true if info should be marked as backfill
 * @param t transaction to write out our new state in
 */
void PG::init(pg_history_t& history, ObjectStore::Transaction *t)
{
  info.history = history;

  info.stats.osd = whoami();

  dirty_info = true;
  write_if_dirty(*t);
}

int PG::_write_info(ObjectStore::Transaction& t, epoch_t epoch,
    pg_info_t &info, coll_t coll,
    hobject_t &infos_oid, uint8_t info_struct_v, bool force_ver)
{
  // pg state

  if (info_struct_v > cur_struct_v)
    return -EINVAL;

  // Only need to write struct_v to attr when upgrading
  if (force_ver || info_struct_v < cur_struct_v) {
    bufferlist attrbl;
    info_struct_v = cur_struct_v;
    ::encode(info_struct_v, attrbl);
    t.collection_setattr(coll, "info", attrbl);
  }

  // info.
  map<string,bufferlist> v;
  ::encode(epoch, v[get_epoch_key(info.pgid)]);
  ::encode(info, v[get_info_key(info.pgid)]);

  t.omap_setkeys(coll_t::META_COLL, infos_oid, v);

  return 0;
}

void PG::write_info(ObjectStore::Transaction& t)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();

  int ret = _write_info(t, get_osdmap()->get_epoch(), info, coll,
			osd->infos_oid, info_struct_v);
  assert(ret == 0);
  last_persisted_osdmap_ref = osdmap_ref;

  dirty_info = false;
}

epoch_t PG::peek_map_epoch(ObjectStore *store, coll_t coll, hobject_t &infos_oid, bufferlist *bl)
{
  assert(bl);
  pg_t pgid;
  bool ok = coll.is_pg(pgid);
  assert(ok);
  int r = store->collection_getattr(coll, "info", *bl);
  assert(r > 0);
  bufferlist::iterator bp = bl->begin();
  uint8_t struct_v = 0;
  ::decode(struct_v, bp);
  if (struct_v < 5)
    return 0;
  epoch_t cur_epoch = 0;
  if (struct_v < 6) {
    ::decode(cur_epoch, bp);
  } else {
    // get epoch out of leveldb
    bufferlist tmpbl;
    string ek = get_epoch_key(pgid);
    set<string> keys;
    keys.insert(get_epoch_key(pgid));
    map<string,bufferlist> values;
    store->omap_get_values(coll_t::META_COLL, infos_oid, keys, &values);
    assert(values.size() == 1);
    tmpbl = values[ek];
    bufferlist::iterator p = tmpbl.begin();
    ::decode(cur_epoch, p);
  }
  return cur_epoch;
}

void PG::write_if_dirty(ObjectStore::Transaction& t)
{
  if (dirty_info)
    write_info(t);
}

int PG::read_info(
  ObjectStore *store, const coll_t coll, bufferlist &bl,
  pg_info_t &info, hobject_t &infos_oid, uint8_t &struct_v)
{
  bufferlist::iterator p = bl.begin();
  bufferlist lbl;

  // info
  ::decode(struct_v, p);
  if (struct_v < 4)
    ::decode(info, p);
  // get info out of leveldb
  string k = get_info_key(info.pgid);
  set<string> keys;
  keys.insert(k);
  map<string,bufferlist> values;
  store->omap_get_values(coll_t::META_COLL, infos_oid, keys, &values);
  assert(values.size() == 1);
  lbl = values[k];
  p = lbl.begin();
  ::decode(info, p);
  return 0;
}

void PG::read_state(ObjectStore *store, bufferlist &bl)
{
  int r = read_info(store, coll, bl, info, osd->infos_oid, info_struct_v);
  assert(r >= 0);

  ostringstream oss;
  if (oss.str().length())
    osd->clog.error() << oss;
}

void PG::requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m)
{
  for (map<hobject_t, list<OpRequestRef> >::iterator it = m.begin();
       it != m.end();
       ++it)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_op(OpRequestRef op)
{
  osd->op_wq.queue_front(make_pair(PGRef(this), op));
}

void PG::requeue_ops(list<OpRequestRef> &ls)
{
  dout(15) << " requeue_ops " << ls << dendl;
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    osd->op_wq.queue_front(make_pair(PGRef(this), *i));
  }
  ls.clear();
}

ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info;

  if (pg.last_update_ondisk != pg.info.last_update)
    out << " luod=" << pg.last_update_ondisk;

  out << " " << pg_state_string(pg.get_state());
  out << "]";


  return out;
}

bool PG::can_discard_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  if (OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  }

  if (m->get_map_epoch() < info.history.same_primary_since) {
    dout(7) << " changed after " << m->get_map_epoch()
	    << ", dropping " << *m << dendl;
    return true;
  }

  return false;
}

bool PG::can_discard_scan(OpRequestRef op)
{
  MOSDPGScan *m = static_cast<MOSDPGScan *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_SCAN);

  return false;
}

bool PG::can_discard_request(OpRequestRef op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op);
  case MSG_OSD_PG_SCAN:
    return can_discard_scan(op);
  }
  return true;
}

bool PG::op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDOp*>(op->get_req())->get_map_epoch());

  case MSG_OSD_PG_SCAN:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGScan*>(op->get_req())->map_epoch);
  }
  assert(0);
  return false;
}

void PG::take_waiters()
{
  dout(10) << "take_waiters" << dendl;
  take_op_map_waiters();
}

void PG::handle_advance_map(OSDMapRef osdmap, OSDMapRef lastmap)
{
  assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  assert(lastmap == osdmap_ref);
  dout(10) << "handle_advance_map " << dendl;
  update_osdmap_ref(osdmap);
  pool.update(osdmap);
  if (pool.info.last_change == osdmap_ref->get_epoch())
    on_pool_change();
}

void PG::handle_activate_map()
{
  dout(10) << "handle_activate_map " << dendl;
  if (osdmap_ref->get_epoch() - last_persisted_osdmap_ref->get_epoch() >
    cct->_conf->osd_pg_epoch_persisted_max_stale) {
    dout(20) << __func__ << ": Dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    dout(20) << __func__ << ": Not dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  if (osdmap_ref->check_new_blacklist_entries()) check_blacklisted_watchers();
}

void intrusive_ptr_add_ref(PG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(PG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg) { return pg->get_with_id(); }
  void put_with_id(PG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif

int PG::whoami() {
  return osd->whoami;
}
