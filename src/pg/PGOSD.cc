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


#include <fstream>

#include "mon/MonClient.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGMissing.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"

#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"

#include "ReplicatedPG.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "PGOSD.h"
#include "PGPlaceSystem.h"


#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, pgosdmap())

static ostream& _prefix(std::ostream* _dout, int whoami, PGOSDMapRef osdmap) {
  return *_dout << "osd." << whoami << " "
		<< (osdmap ? osdmap->get_epoch():0)
		<< " ";
}


const PGOSDPlaceSystem placeSystem(PGPlaceSystem::systemName,
				   PGPlaceSystem::systemIdentifier);


PGOSDService::PGOSDService(PGOSD *osd) :
  OSDService(osd),
  pg_recovery_stats(osd->pg_recovery_stats),
  op_wq(osd->op_wq),
  peering_wq(osd->peering_wq),
  recovery_wq(osd->recovery_wq),
  snap_trim_wq(osd->snap_trim_wq),
  scrub_wq(osd->scrub_wq),
  scrub_finalize_wq(osd->scrub_finalize_wq),
  rep_scrub_wq(osd->rep_scrub_wq),
  pg_temp_lock("OSDService::pg_temp_lock"),
  reserver_finisher(g_ceph_context),
  local_reserver(&reserver_finisher, g_conf->osd_max_backfills),
  remote_reserver(&reserver_finisher, g_conf->osd_max_backfills),
  in_progress_split_lock("OSDService::in_progress_split_lock")
#ifdef PG_DEBUG_REFS
  , pgid_lock("OSDService::pgid_lock")
#endif
{
  // empty
}

void PGOSDService::expand_pg_num(PGOSDMapRef old_map,
				 PGOSDMapRef new_map)
{
  Mutex::Locker l(in_progress_split_lock);
  for (set<pg_t>::iterator i = in_progress_splits.begin();
       i != in_progress_splits.end();
    ) {
    if (!new_map->have_pg_pool(i->pool())) {
      in_progress_splits.erase(i++);
    } else {
      _maybe_split_pgid(old_map, new_map, *i);
      ++i;
    }
  }
  for (map<pg_t, pg_t>::iterator i = pending_splits.begin();
       i != pending_splits.end();
    ) {
    if (!new_map->have_pg_pool(i->first.pool())) {
      rev_pending_splits.erase(i->second);
      pending_splits.erase(i++);
    } else {
      _maybe_split_pgid(old_map, new_map, i->first);
      ++i;
    }
  }
}

bool PGOSDService::splitting(pg_t pgid)
{
  Mutex::Locker l(in_progress_split_lock);
  return in_progress_splits.count(pgid) ||
    pending_splits.count(pgid);
}

void PGOSDService::complete_split(const set<pg_t> &pgs)
{
  Mutex::Locker l(in_progress_split_lock);
  for (set<pg_t>::const_iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    assert(!pending_splits.count(*i));
    assert(in_progress_splits.count(*i));
    in_progress_splits.erase(*i);
  }
}



PGOSD* PGOSDService::pgosd() const {
  return dynamic_cast<PGOSD*>(osd);
}

void PGOSDService::pg_stat_queue_enqueue(PG *pg)
{
  pgosd()->pg_stat_queue_enqueue(pg);
}

void PGOSDService::pg_stat_queue_dequeue(PG *pg)
{
  pgosd()->pg_stat_queue_dequeue(pg);
}

void PGOSDService::queue_want_pg_temp(pg_t pgid, vector<int>& want)
{
  Mutex::Locker l(pg_temp_lock);
  pg_temp_wanted[pgid] = want;
}

void PGOSDService::send_pg_temp()
{
  Mutex::Locker l(pg_temp_lock);
  if (pg_temp_wanted.empty())
    return;
  MOSDPGTemp *m = new MOSDPGTemp(osdmap->get_epoch());
  m->pg_temp = pg_temp_wanted;
  monc->send_mon_message(m);
}

void PGOSDService::shutdown_sub()
{
  reserver_finisher.stop();
  {
    Mutex::Locker l(watch_lock);
    watch_timer.shutdown();
  }
  {
    Mutex::Locker l(backfill_request_lock);
    backfill_request_timer.shutdown();
  }
}

void PGOSDService::init_sub()
{
  reserver_finisher.start();
  watch_timer.init();
}

void PGOSDService::_start_split(pg_t parent, const set<pg_t> &children)
{
  for (set<pg_t>::const_iterator i = children.begin();
       i != children.end();
       ++i) {
    assert(!pending_splits.count(*i));
    assert(!in_progress_splits.count(*i));
    pending_splits.insert(make_pair(*i, parent));

    assert(!rev_pending_splits[parent].count(*i));
    rev_pending_splits[parent].insert(*i);
  }
}

void PGOSDService::mark_split_in_progress(pg_t parent,
					  const set<pg_t> &children)
{
  Mutex::Locker l(in_progress_split_lock);
  map<pg_t, set<pg_t> >::iterator piter = rev_pending_splits.find(parent);
  assert(piter != rev_pending_splits.end());
  for (set<pg_t>::const_iterator i = children.begin();
       i != children.end();
       ++i) {
    assert(piter->second.count(*i));
    assert(pending_splits.count(*i));
    assert(!in_progress_splits.count(*i));
    assert(pending_splits[*i] == parent);

    pending_splits.erase(*i);
    piter->second.erase(*i);
    in_progress_splits.insert(*i);
  }
  if (piter->second.empty())
    rev_pending_splits.erase(piter);
}

void PGOSDService::cancel_pending_splits_for_parent(pg_t parent)
{
  Mutex::Locker l(in_progress_split_lock);
  return _cancel_pending_splits_for_parent(parent);
}

void PGOSDService::_cancel_pending_splits_for_parent(pg_t parent)
{
  map<pg_t, set<pg_t> >::iterator piter = rev_pending_splits.find(parent);
  if (piter == rev_pending_splits.end())
    return;

  for (set<pg_t>::iterator i = piter->second.begin();
       i != piter->second.end();
       ++i) {
    assert(pending_splits.count(*i));
    assert(!in_progress_splits.count(*i));
    pending_splits.erase(*i);
    _cancel_pending_splits_for_parent(*i);
  }
  rev_pending_splits.erase(piter);
}

void PGOSDService::_maybe_split_pgid(PGOSDMapRef old_map,
				     PGOSDMapRef new_map,
				     pg_t pgid)
{
  assert(old_map->have_pg_pool(pgid.pool()));
  if (pgid.ps() < static_cast<unsigned>(old_map->get_pg_num(pgid.pool()))) {
    set<pg_t> children;
    pgid.is_split(old_map->get_pg_num(pgid.pool()),
		  new_map->get_pg_num(pgid.pool()), &children);
    _start_split(pgid, children);
  } else {
    assert(pgid.ps() < static_cast<unsigned>(new_map->get_pg_num(pgid.pool())));
  }
}

void PGOSDService::init_splits_between(pg_t pgid,
				       PGOSDMapRef frommap,
				       PGOSDMapRef tomap)
{
  // First, check whether we can avoid this potentially expensive check
  if (tomap->have_pg_pool(pgid.pool()) &&
      pgid.is_split(
	frommap->get_pg_num(pgid.pool()),
	tomap->get_pg_num(pgid.pool()),
	NULL)) {
    // Ok, a split happened, so we need to walk the osdmaps
    set<pg_t> new_pgs; // pgs to scan on each map
    new_pgs.insert(pgid);
    for (epoch_t e = frommap->get_epoch() + 1;
	 e <= tomap->get_epoch();
	 ++e) {
      PGOSDMapRef curmap(get_map(e-1));
      PGOSDMapRef nextmap(get_map(e));
      set<pg_t> even_newer_pgs; // pgs added in this loop
      for (set<pg_t>::iterator i = new_pgs.begin(); i != new_pgs.end(); ++i) {
	set<pg_t> split_pgs;
	if (i->is_split(curmap->get_pg_num(i->pool()),
			nextmap->get_pg_num(i->pool()),
			&split_pgs)) {
	  start_split(*i, split_pgs);
	  even_newer_pgs.insert(split_pgs.begin(), split_pgs.end());
	}
      }
      new_pgs.insert(even_newer_pgs.begin(), even_newer_pgs.end());
    }
  }
}



// OSD

PGOSD::PGOSD(int id, Messenger *internal_messenger,
	     Messenger *external_messenger,
             Messenger *hbclientm, Messenger *hbserverm, MonClient *mc,
             const std::string &dev, const std::string &jdev) :
  OSD(id, internal_messenger, external_messenger, hbclientm, hbserverm,
      mc, dev, jdev, new PGOSDService(this)),
  op_queue_len(0),
  op_wq(this, g_conf->osd_op_thread_timeout, &op_tp),
  peering_wq(this, g_conf->osd_op_thread_timeout, &op_tp, 200),
  debug_drop_pg_create_probability(g_conf->osd_debug_drop_pg_create_probability),
  debug_drop_pg_create_duration(g_conf->osd_debug_drop_pg_create_duration),
  debug_drop_pg_create_left(-1),
  outstanding_pg_stats(false),
  pg_stat_queue_lock("OSD::pg_stat_queue_lock"),
  pg_stat_tid(0),
  pg_stat_tid_flushed(0),
  recovery_ops_active(0),
  recovery_wq(this, g_conf->osd_recovery_thread_timeout, &recovery_tp),
  scrub_finalize_wq(this, g_conf->osd_scrub_finalize_thread_timeout, &op_tp),
  scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  snap_trim_wq(this, g_conf->osd_snap_trim_thread_timeout, &disk_tp),
  replay_queue_lock("OSD::replay_queue_lock"),
  rep_scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  next_removal_seq(0)
{
  // empty
}


int PGOSD::init() {
  const int result = init_super();

  // load up pgs (as they previously existed)
  load_pgs();
  peering_wq.drain();

  return result;
}


// NB: unsure if this will order the key steps correctly
int PGOSD::shutdown() {
  // then kick all pgs,
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    dout(20) << " kicking pg " << p->first << dendl;
    p->second->lock();
    p->second->kick();
    p->second->unlock();
  }
  dout(20) << " kicked all pgs" << dendl;

  // then stop thread.
  disk_tp.stop();
  dout(10) << "disk tp stopped" << dendl;

  // tell pgs we're shutting down
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    p->second->lock();
    p->second->on_shutdown();
    p->second->unlock();
  }

  clear_pg_stat_queue();

  peering_wq.clear();
  // Remove PGs
#ifdef PG_DEBUG_REFS
  service.dump_live_pgids();
#endif
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       ++p) {
    dout(20) << " kicking pg " << p->first << dendl;
    p->second->lock();
    if (p->second->ref.read() != 1) {
      derr << "pgid " << p->first << " has ref count of "
	   << p->second->ref.read() << dendl;
      assert(0);
    }
    p->second->unlock();
    p->second->put("PGMap");
  }
  pg_map.clear();
#ifdef PG_DEBUG_REFS
  service.dump_live_pgids();
#endif

  // finish ops
  op_wq.drain();
  peering_wq.clear();

  dout(10) << "no ops" << dendl;

  return shutdown_super();

}


// ======================================================
// PG's

PGPool PGOSD::_get_pool(int id, PGOSDMapRef createmap_p)
{
  const PGOSDMap* createmap = dynamic_cast<const PGOSDMap*>(createmap_p.get());

  if (!createmap->have_pg_pool(id)) {
    dout(5) << __func__ << ": the OSDmap does not contain a PG pool with id = "
	    << id << dendl;
    assert(0);
  }

  PGPool p = PGPool(id, createmap->get_pool_name(id),
		    createmap->get_pg_pool(id)->auid);

  const pg_pool_t *pi = createmap->get_pg_pool(id);
  p.info = *pi;
  p.snapc = pi->get_snap_context();

  pi->build_removed_snaps(p.cached_removed_snaps);
  dout(10) << "_get_pool " << p.id << dendl;
  return p;
}

PG *PGOSD::_open_lock_pg(PGOSDMapRef createmap,
			 pg_t pgid, bool no_lockdep_check,
			 bool hold_map_lock)
{
  assert(osd_lock.is_locked());

  PG* pg = _make_pg(createmap, pgid);

  pg_map[pgid] = pg;

  pg->lock(no_lockdep_check);
  pg->get("PGMap");  // because it's in pg_map
  return pg;
}

PG* PGOSD::_make_pg(PGOSDMapRef createmap, pg_t pgid)
{
  dout(10) << "_open_lock_pg " << pgid << dendl;
  PGPool pool = _get_pool(pgid.pool(), createmap);

  // create
  PG *pg;
  hobject_t logoid = make_pg_log_oid(pgid);
  hobject_t infooid = make_pg_biginfo_oid(pgid);
  if (pgosdmap()->get_pg_type(pgid) == pg_pool_t::TYPE_REP)
    pg = new ReplicatedPG(pgosdservice(), createmap, pool, pgid,
			  logoid, infooid);
  else
    assert(0);

  return pg;
}

void PGOSD::add_newly_split_pg(PG *pg, PG::RecoveryCtx *rctx)
{
  epoch_t e(service->get_osdmap()->get_epoch());
  pg->get("PGMap");  // For pg_map
  pg_map[pg->info.pgid] = pg;
  dout(10) << "Adding newly split pg " << *pg << dendl;
  vector<int> up, acting;
  pg->get_osdmap()->pg_to_up_acting_osds(pg->info.pgid, up, acting);
  int role = pg->get_osdmap()->calc_pg_role(service->whoami, acting);
  pg->set_role(role);
  pg->reg_next_scrub();
  pg->handle_loaded(rctx);
  pg->write_if_dirty(*(rctx->transaction));
  pg->queue_null(e, e);
  map<pg_t, list<PG::CephPeeringEvtRef> >::iterator to_wake =
    peering_wait_for_split.find(pg->info.pgid);
  if (to_wake != peering_wait_for_split.end()) {
    for (list<PG::CephPeeringEvtRef>::iterator i =
	   to_wake->second.begin();
	 i != to_wake->second.end();
	 ++i) {
      pg->queue_peering_event(*i);
    }
    peering_wait_for_split.erase(to_wake);
  }
  wake_pg_waiters(pg->info.pgid);
  if (!pgosdmap()->have_pg_pool(pg->info.pgid.pool()))
    _remove_pg(pg);
}

PG *PGOSD::_create_lock_pg(PGOSDMapRef createmap,
			   pg_t pgid, bool newly_created, bool hold_map_lock,
			   int role, vector<int>& up, vector<int>& acting,
			   pg_history_t history, pg_interval_map_t& pi,
			   ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());
  dout(20) << "_create_lock_pg pgid " << pgid << dendl;

  PG *pg = _open_lock_pg(createmap, pgid, true, hold_map_lock);

  t.create_collection(coll_t(pgid));

  pg->init(role, up, acting, history, pi, &t);

  dout(7) << "_create_lock_pg " << *pg << dendl;
  return pg;
}

bool PGOSD::_have_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  return pg_map.count(pgid);
}

PG *PGOSD::_lookup_lock_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  if (!pg_map.count(pgid))
    return NULL;
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}

PG *PGOSD::_lookup_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  if (!pg_map.count(pgid))
    return NULL;
  PG *pg = pg_map[pgid];
  return pg;
}


PG *PGOSD::_lookup_lock_pg_with_map_lock_held(pg_t pgid)
{
  assert(osd_lock.is_locked());
  assert(pg_map.count(pgid));
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}

void PGOSD::load_pgs()
{
  assert(osd_lock.is_locked());
  dout(10) << "load_pgs" << dendl;
  assert(pg_map.empty());

  vector<coll_t> ls;
  int r = store->list_collections(ls);
  if (r < 0) {
    derr << "failed to list pgs: " << cpp_strerror(-r) << dendl;
  }

  set<pg_t> head_pgs;
  map<pg_t, interval_set<snapid_t> > pgs;
  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       ++it) {
    pg_t pgid;
    snapid_t snap;

    if (it->is_temp(pgid)) {
      dout(10) << "load_pgs " << *it << " clearing temp" << dendl;
      clear_temp(store, *it);
      continue;
    }

    if (it->is_pg(pgid, snap)) {
      if (snap != CEPH_NOSNAP) {
	dout(10) << "load_pgs skipping snapped dir " << *it
		 << " (pg " << pgid << " snap " << snap << ")" << dendl;
	pgs[pgid].insert(snap);
      } else {
	pgs[pgid];
	head_pgs.insert(pgid);
      }
      continue;
    }

    uint64_t seq;
    if (it->is_removal(&seq, &pgid)) {
      if (seq >= next_removal_seq)
	next_removal_seq = seq + 1;
      dout(10) << "load_pgs queueing " << *it << " for removal, seq is "
	       << seq << " pgid is " << pgid << dendl;
      boost::tuple<coll_t, SequencerRef, DeletingStateRef> *to_queue =
	new boost::tuple<coll_t, SequencerRef, DeletingStateRef>;
      to_queue->get<0>() = *it;
      to_queue->get<1>() = pgosdservice()->osr_registry.lookup_or_create(pgid, stringify(pgid));
      to_queue->get<2>() = pgosdservice()->deleting_pgs.lookup_or_create(pgid);
      remove_wq.queue(to_queue);
      continue;
    }

    dout(10) << "load_pgs ignoring unrecognized " << *it << dendl;
  }

  bool has_upgraded = false;
  for (map<pg_t, interval_set<snapid_t> >::iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    pg_t pgid(i->first);

    if (!head_pgs.count(pgid)) {
      dout(10) << __func__ << ": " << pgid << " has orphan snap collections " << i->second
	       << " with no head" << dendl;
      continue;
    }

    if (!pgosdmap()->have_pg_pool(pgid.pool())) {
      dout(10) << __func__ << ": skipping PG " << pgid << " because we don't have pool "
	       << pgid.pool() << dendl;
      continue;
    }

    if (pgid.preferred() >= 0) {
      dout(10) << __func__ << ": skipping localized PG " << pgid << dendl;
      // FIXME: delete it too, eventually
      continue;
    }

    dout(10) << "pgid " << pgid << " coll " << coll_t(pgid) << dendl;
    bufferlist bl;
    epoch_t map_epoch = PG::peek_map_epoch(store, coll_t(pgid),
					   service->infos_oid, &bl);

    PG *pg = _open_lock_pg(map_epoch == 0 ? pgosdmap() :
			   pgosdservice()->get_map(map_epoch), pgid);

    // read pg state, log
    pg->read_state(store, bl);

    if (pg->must_upgrade()) {
      if (!has_upgraded) {
	derr << "PGs are upgrading" << dendl;
	has_upgraded = true;
      }
      dout(10) << "PG " << pg->info.pgid
	       << " must upgrade..." << dendl;
      pg->upgrade(store, i->second);
    } else if (!i->second.empty()) {
      // handle upgrade bug
      for (interval_set<snapid_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	for (snapid_t k = j.get_start();
	     k != j.get_start() + j.get_len();
	     ++k) {
	  assert(store->collection_empty(coll_t(pgid, k)));
	  ObjectStore::Transaction t;
	  t.remove_collection(coll_t(pgid, k));
	  store->apply_transaction(t);
	}
      }
    }

    if (!pg->snap_collections.empty()) {
      pg->snap_collections.clear();
      pg->dirty_big_info = true;
      pg->dirty_info = true;
      ObjectStore::Transaction t;
      pg->write_if_dirty(t);
      store->apply_transaction(t);
    }

    pgosdservice()->init_splits_between(pg->info.pgid,
					pg->get_osdmap(), pgosdmap());

    pg->reg_next_scrub();

    // generate state for PG's current mapping
    pg->get_osdmap()->pg_to_up_acting_osds(pgid, pg->up, pg->acting);
    int role = pg->get_osdmap()->calc_pg_role(whoami, pg->acting);
    pg->set_role(role);

    PG::RecoveryCtx rctx(0, 0, 0, 0, 0, 0);
    pg->handle_loaded(&rctx);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->log << dendl;
    pg->unlock();
  }
  dout(10) << "load_pgs done" << dendl;

  build_past_intervals_parallel();
}

/*
 * build past_intervals efficiently on old, degraded, and buried
 * clusters.  this is important for efficiently catching up osds that
 * are way behind on maps to the current cluster state.
 *
 * this is a parallel version of PG::generate_past_intervals().
 * follow the same logic, but do all pgs at the same time so that we
 * can make a single pass across the osdmap history.
 */
struct pistate {
  epoch_t start, end;
  vector<int> old_acting, old_up;
  epoch_t same_interval_since;
};

void PGOSD::build_past_intervals_parallel()
{
  map<PG*,pistate> pis;

  // calculate untion of map range
  epoch_t end_epoch = superblock.oldest_map;
  epoch_t cur_epoch = superblock.newest_map;
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       ++i) {
    PG *pg = i->second;

    epoch_t start, end;
    if (!pg->_calc_past_interval_range(&start, &end))
      continue;

    dout(10) << pg->info.pgid << " needs " << start << "-" << end << dendl;
    pistate& p = pis[pg];
    p.start = start;
    p.end = end;
    p.same_interval_since = 0;

    if (start < cur_epoch)
      cur_epoch = start;
    if (end > end_epoch)
      end_epoch = end;
  }
  if (pis.empty()) {
    dout(10) << __func__ << " nothing to build" << dendl;
    return;
  }

  dout(1) << __func__ << " over " << cur_epoch << "-" << end_epoch << dendl;
  assert(cur_epoch <= end_epoch);

  PGOSDMapRef cur_map, last_map;
  for ( ; cur_epoch <= end_epoch; cur_epoch++) {
    dout(10) << __func__ << " epoch " << cur_epoch << dendl;
    last_map = cur_map;
    cur_map = pgosdservice()->get_map(cur_epoch);

    for (map<PG*,pistate>::iterator i = pis.begin(); i != pis.end(); ++i) {
      PG *pg = i->first;
      pistate& p = i->second;

      if (cur_epoch < p.start || cur_epoch > p.end)
	continue;

      vector<int> acting, up;
      cur_map->pg_to_up_acting_osds(pg->info.pgid, up, acting);

      if (p.same_interval_since == 0) {
	dout(10) << __func__ << " epoch " << cur_epoch << " pg " << pg->info.pgid
		 << " first map, acting " << acting
		 << " up " << up << ", same_interval_since = " << cur_epoch << dendl;
	p.same_interval_since = cur_epoch;
	p.old_up = up;
	p.old_acting = acting;
	continue;
      }
      assert(last_map);

      std::stringstream debug;
      bool new_interval = pg_interval_t::check_new_interval(p.old_acting, acting,
							    p.old_up, up,
							    p.same_interval_since,
							    pg->info.history.last_epoch_clean,
							    cur_map, last_map,
							    pg->info.pgid.pool(),
	                                                    pg->info.pgid,
							    &pg->past_intervals,
							    &debug);
      if (new_interval) {
	dout(10) << __func__ << " epoch " << cur_epoch << " pg " << pg->info.pgid
		 << " " << debug.str() << dendl;
	p.old_up = up;
	p.old_acting = acting;
	p.same_interval_since = cur_epoch;
      }
    }
  }

  // write info only at the end.  this is necessary because we check
  // whether the past_intervals go far enough back or forward in time,
  // but we don't check for holes.  we could avoid it by discarding
  // the previous past_intervals and rebuilding from scratch, or we
  // can just do this and commit all our work at the end.
  ObjectStore::Transaction t;
  int num = 0;
  for (map<PG*,pistate>::iterator i = pis.begin(); i != pis.end(); ++i) {
    PG *pg = i->first;
    pg->lock();
    pg->dirty_big_info = true;
    pg->dirty_info = true;
    pg->write_if_dirty(t);
    pg->unlock();

    // don't let the transaction get too big
    if (++num >= g_conf->osd_target_transaction_size) {
      store->apply_transaction(t);
      t = ObjectStore::Transaction();
      num = 0;
    }
  }
  if (!t.empty())
    store->apply_transaction(t);
}

/*
 * look up a pg.  if we have it, great.  if not, consider creating it IF the pg mapping
 * hasn't changed since the given epoch and we are the primary.
 */
PG *PGOSD::get_or_create_pg(
  const pg_info_t& info, pg_interval_map_t& pi,
  epoch_t epoch, int from, int& created, bool primary)
{
  PG *pg;

  if (!_have_pg(info.pgid)) {
    // same primary?
    if (!pgosdmap()->have_pg_pool(info.pgid.pool()))
      return 0;
    vector<int> up, acting;
    pgosdmap()->pg_to_up_acting_osds(info.pgid, up, acting);
    int role = pgosdmap()->calc_pg_role(whoami, acting, acting.size());

    pg_history_t history = info.history;
    project_pg_history(info.pgid, history, epoch, up, acting);

    if (epoch < history.same_interval_since) {
      dout(10) << "get_or_create_pg " << info.pgid << " acting changed in "
	       << history.same_interval_since << " (msg from " << epoch << ")" << dendl;
      return NULL;
    }

    if (pgosdservice()->splitting(info.pgid)) {
      assert(0);
    }

    bool create = false;
    if (primary) {
      assert(role == 0);  // otherwise, probably bug in project_pg_history.

      // DNE on source?
      if (info.dne()) {
	// is there a creation pending on this pg?
	if (creating_pgs.count(info.pgid)) {
	  creating_pgs[info.pgid].prior.erase(from);
	  if (!can_create_pg(info.pgid))
	    return NULL;
	  history = creating_pgs[info.pgid].history;
	  create = true;
	} else {
	  dout(10) << "get_or_create_pg " << info.pgid
		   << " DNE on source, but creation probe, ignoring" << dendl;
	  return NULL;
	}
      }
      creating_pgs.erase(info.pgid);
    } else {
      assert(role != 0);    // i should be replica
      assert(!info.dne());  // and pg exists if we are hearing about it
    }

    // ok, create PG locally using provided Info and History
    PG::RecoveryCtx rctx = create_context();
    pg = _create_lock_pg(
      pgosdservice()->get_map(epoch),
      info.pgid, create, false, role, up, acting, history, pi,
      *rctx.transaction);
    pg->handle_create(&rctx);
    pg->write_if_dirty(*rctx.transaction);
    dispatch_context(rctx, pg, pgosdmap());

    created++;
    dout(10) << *pg << " is new" << dendl;

    // kick any waiters
    wake_pg_waiters(pg->info.pgid);

  } else {
    // already had it.  did the mapping change?
    pg = _lookup_lock_pg(info.pgid);
    if (epoch < pg->info.history.same_interval_since) {
      dout(10) << *pg << " get_or_create_pg acting changed in "
	       << pg->info.history.same_interval_since
	       << " (msg from " << epoch << ")" << dendl;
      pg->unlock();
      return NULL;
    }
  }
  return pg;
}

/*
 * calculate prior pg members during an epoch interval [start,end)
 *  - from each epoch, include all osds up then AND now
 *  - if no osds from then are up now, include them all, even tho they're not reachable now
 */
void PGOSD::calc_priors_during(pg_t pgid, epoch_t start, epoch_t end, set<int>& pset)
{
  dout(15) << "calc_priors_during " << pgid << " [" << start << "," << end << ")" << dendl;

  for (epoch_t e = start; e < end; e++) {
    PGOSDMapRef oldmap = pgosdservice()->get_map(e);
    vector<int> acting;
    pgosdmap(oldmap)->pg_to_acting_osds(pgid, acting);
    dout(20) << "  " << pgid << " in epoch " << e << " was " << acting << dendl;
    int up = 0;
    for (unsigned i=0; i<acting.size(); i++)
      if (osdmap->is_up(acting[i])) {
	if (acting[i] != whoami)
	  pset.insert(acting[i]);
	up++;
      }
    if (!up && !acting.empty()) {
      // sucky.  add down osds, even tho we can't reach them right now.
      for (unsigned i=0; i<acting.size(); i++)
	if (acting[i] != whoami)
	  pset.insert(acting[i]);
    }
  }
  dout(10) << "calc_priors_during " << pgid
	   << " [" << start << "," << end
	   << ") = " << pset << dendl;
}

/**
 * Fill in the passed history so you know same_interval_since, same_up_since,
 * and same_primary_since.
 */
void PGOSD::project_pg_history(pg_t pgid, pg_history_t& h, epoch_t from,
			       const vector<int>& currentup,
			       const vector<int>& currentacting)
{
  dout(15) << "project_pg_history " << pgid
	   << " from " << from << " to " << osdmap->get_epoch()
	   << ", start " << h
	   << dendl;

  epoch_t e;
  for (e = osdmap->get_epoch();
       e > from;
       e--) {
    // verify during intermediate epoch (e-1)
    PGOSDMapRef oldmap = pgosdservice()->get_map(e-1);
    assert(oldmap->have_pg_pool(pgid.pool()));

    vector<int> up, acting;
    pgosdmap(oldmap)->pg_to_up_acting_osds(pgid, up, acting);

    // acting set change?
    if ((acting != currentacting || up != currentup) && e > h.same_interval_since) {
      dout(15) << "project_pg_history " << pgid << " acting|up changed in " << e 
	       << " from " << acting << "/" << up
	       << " -> " << currentacting << "/" << currentup
	       << dendl;
      h.same_interval_since = e;
    }
    // split?
    if (pgid.is_split(oldmap->get_pg_num(pgid.pool()),
		      pgosdmap()->get_pg_num(pgid.pool()),
		      0)) {
      h.same_interval_since = e;
    }
    // up set change?
    if (up != currentup && e > h.same_up_since) {
      dout(15) << "project_pg_history " << pgid << " up changed in " << e 
                << " from " << up << " -> " << currentup << dendl;
      h.same_up_since = e;
    }

    // primary change?
    if (!(!acting.empty() && !currentacting.empty() && acting[0] == currentacting[0]) &&
        e > h.same_primary_since) {
      dout(15) << "project_pg_history " << pgid << " primary changed in " << e << dendl;
      h.same_primary_since = e;
    }

    if (h.same_interval_since >= e && h.same_up_since >= e && h.same_primary_since >= e)
      break;
  }

  // base case: these floors should be the creation epoch if we didn't
  // find any changes.
  if (e == h.epoch_created) {
    if (!h.same_interval_since)
      h.same_interval_since = e;
    if (!h.same_up_since)
      h.same_up_since = e;
    if (!h.same_primary_since)
      h.same_primary_since = e;
  }

  dout(15) << "project_pg_history end " << h << dendl;
}

void PGOSD::build_heartbeat_peers_list()
{
  // build heartbeat from set
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       ++i) {
    PG *pg = i->second;
    pg->heartbeat_peer_lock.Lock();
    dout(20) << i->first << " heartbeat_peers " << pg->heartbeat_peers << dendl;
    for (set<int>::iterator p = pg->heartbeat_peers.begin();
	 p != pg->heartbeat_peers.end();
	 ++p)
      if (osdmap->is_up(*p))
	_add_heartbeat_peer(*p);
    for (set<int>::iterator p = pg->probe_targets.begin();
	 p != pg->probe_targets.end();
	 ++p)
      if (osdmap->is_up(*p))
	_add_heartbeat_peer(*p);
    pg->heartbeat_peer_lock.Unlock();
  }
}

void PGOSD::sched_scrub()
{
  assert(osd_lock.is_locked());

  bool load_is_low = scrub_should_schedule();

  dout(20) << "sched_scrub load_is_low=" << (int)load_is_low << dendl;

  utime_t max = ceph_clock_now(g_ceph_context);
  utime_t min = max;
  min -= g_conf->osd_scrub_min_interval;
  max -= g_conf->osd_scrub_max_interval;

  //dout(20) << " " << last_scrub_pg << dendl;

  pair<utime_t, pg_t> pos;
  if (pgosdservice()->first_scrub_stamp(&pos)) {
    do {
      utime_t t = pos.first;
      pg_t pgid = pos.second;
      dout(30) << " " << pgid << " at " << t << dendl;

      if (t > min) {
	dout(10) << " " << pgid << " at " << t
		 << " > min " << min << " (" << g_conf->osd_scrub_min_interval
		 << " seconds ago)" << dendl;
	break;
      }
      if (t > max && !load_is_low) {
	// save ourselves some effort
	break;
      }

      PG *pg = _lookup_lock_pg(pgid);
      if (pg) {
	if (pg->is_active() &&
	    (load_is_low ||
	     t < max ||
	     pg->scrubber.must_scrub)) {
	  dout(10) << " " << pgid << " at " << t
		   << (pg->scrubber.must_scrub ? ", explicitly requested" : "")
		   << (t < max ? ", last_scrub < max" : "")
		   << dendl;
	  if (pg->sched_scrub()) {
	    pg->unlock();
	    break;
	  }
	}
	pg->unlock();
      }
    } while (pgosdservice()->next_scrub_stamp(pos, &pos));
  }
  dout(20) << "sched_scrub done" << dendl;
}

void PGOSD::tick_sub(const utime_t& now) {
  if (is_active()) {
    if (outstanding_pg_stats &&
	(now - g_conf->osd_mon_ack_timeout) > last_pg_stats_ack) {
      dout(1) << "mon hasn't acked PGStats in " << now - last_pg_stats_ack
	      << " seconds, reconnecting elsewhere" << dendl;
      monc->reopen_session();
      last_pg_stats_ack = ceph_clock_now(g_ceph_context);  // reset clock
      last_pg_stats_sent = utime_t();
    }
    if (now - last_pg_stats_sent > g_conf->osd_mon_report_interval_max) {
      osd_stat_updated = true;
      do_mon_report();
    } else if (now - last_mon_report > g_conf->osd_mon_report_interval_min) {
      do_mon_report();
    }
  }
}


void PGOSD::do_mon_report_sub(const utime_t& now) {
  pgosdservice()->send_pg_temp();
  send_pg_stats(now);
}


void PGOSD::ms_handle_connect_sub(Connection *con) {
  pgosdservice()->send_pg_temp();
  send_pg_stats(ceph_clock_now(g_ceph_context));

  monc->sub_want("osd_pg_creates", 0, CEPH_SUBSCRIBE_ONETIME);
}


void PGOSD::put_object_context(void *_obc, pg_t pgid)
{
  ObjectContext *obc = (ObjectContext *)_obc;
  ReplicatedPG *pg = (ReplicatedPG *)_lookup_lock_pg(pgid);
  // If pg is being deleted, (which is the only case in which
  // it will be NULL) it will clean up its object contexts itself
  if (pg) {
    pg->put_object_context(obc);
    pg->unlock();
  }
}

void PGOSD::ms_handle_reset_sub(OSD::Session* session) {
  PGSession* pg_session = dynamic_cast<PGSession*>(session);
  pg_session->wstate.reset();
}

void PGOSD::send_pg_stats(const utime_t &now)
{
  assert(osd_lock.is_locked());

  dout(20) << "send_pg_stats" << dendl;

  stat_lock.Lock();
  osd_stat_t cur_stat = osd_stat;
  stat_lock.Unlock();
   
  pg_stat_queue_lock.Lock();

  if (osd_stat_updated || !pg_stat_queue.empty()) {
    last_pg_stats_sent = now;
    osd_stat_updated = false;

    dout(10) << "send_pg_stats - " << pg_stat_queue.size() << " pgs updated" << dendl;

    utime_t had_for(now);
    had_for -= had_map_since;

    MPGStats *m = new MPGStats(monc->get_fsid(), osdmap->get_epoch(), had_for);
    m->set_tid(++pg_stat_tid);
    m->osd_stat = cur_stat;

    xlist<PG*>::iterator p = pg_stat_queue.begin();
    while (!p.end()) {
      PG *pg = *p;
      ++p;
      if (!pg->is_primary()) {  // we hold map_lock; role is stable.
	pg->stat_queue_item.remove_myself();
	pg->put("pg_stat_queue");
	continue;
      }
      pg->pg_stats_publish_lock.Lock();
      if (pg->pg_stats_publish_valid) {
	m->pg_stat[pg->info.pgid] = pg->pg_stats_publish;
	dout(25) << " sending " << pg->info.pgid << " " << pg->pg_stats_publish.reported << dendl;
      } else {
	dout(25) << " NOT sending " << pg->info.pgid << " " << pg->pg_stats_publish.reported << ", not valid" << dendl;
      }
      pg->pg_stats_publish_lock.Unlock();
    }

    if (!outstanding_pg_stats) {
      outstanding_pg_stats = true;
      last_pg_stats_ack = ceph_clock_now(g_ceph_context);
    }
    monc->send_mon_message(m);
  }

  pg_stat_queue_lock.Unlock();
}

void PGOSD::handle_pg_stats_ack(MPGStatsAck *ack)
{
  dout(10) << "handle_pg_stats_ack " << dendl;

  if (!require_mon_peer(ack)) {
    ack->put();
    return;
  }

  last_pg_stats_ack = ceph_clock_now(g_ceph_context);

  pg_stat_queue_lock.Lock();

  if (ack->get_tid() > pg_stat_tid_flushed) {
    pg_stat_tid_flushed = ack->get_tid();
    pg_stat_queue_cond.Signal();
  }

  xlist<PG*>::iterator p = pg_stat_queue.begin();
  while (!p.end()) {
    PG *pg = *p;
    PGRef _pg(pg);
    ++p;

    if (ack->pg_stat.count(pg->info.pgid)) {
      eversion_t acked = ack->pg_stat[pg->info.pgid];
      pg->pg_stats_publish_lock.Lock();
      if (acked == pg->pg_stats_publish.reported) {
	dout(25) << " ack on " << pg->info.pgid << " " << pg->pg_stats_publish.reported << dendl;
	pg->stat_queue_item.remove_myself();
	pg->put("pg_stat_queue");
      } else {
	dout(25) << " still pending " << pg->info.pgid << " " << pg->pg_stats_publish.reported
		 << " > acked " << acked << dendl;
      }
      pg->pg_stats_publish_lock.Unlock();
    } else {
      dout(30) << " still pending " << pg->info.pgid << " " << pg->pg_stats_publish.reported << dendl;
    }
  }
  
  if (!pg_stat_queue.size()) {
    outstanding_pg_stats = false;
  }

  pg_stat_queue_lock.Unlock();

  ack->put();
}

void PGOSD::flush_pg_stats()
{
  dout(10) << "flush_pg_stats" << dendl;
  utime_t now = ceph_clock_now(cct);
  send_pg_stats(now);

  osd_lock.Unlock();

  pg_stat_queue_lock.Lock();
  uint64_t tid = pg_stat_tid;
  dout(10) << "flush_pg_stats waiting for stats tid " << tid << " to flush" << dendl;
  while (tid > pg_stat_tid_flushed)
    pg_stat_queue_cond.Wait(pg_stat_queue_lock);
  dout(10) << "flush_pg_stats finished waiting for stats tid " << tid << " to flush" << dendl;
  pg_stat_queue_lock.Unlock();

  osd_lock.Lock();
}

bool PGOSD::do_command_sub(Connection *con,
			   tid_t tid,
			   vector<string>& cmd,
			   bufferlist& data,
			   bufferlist& odata,
			   int& r,
			   ostringstream& ss)
{
  if (cmd[0] == "pg") {
    pg_t pgid;

    if (cmd.size() < 2) {
      ss << "no pgid specified";
      r = -EINVAL;
    } else if (!pgid.parse(cmd[1].c_str())) {
      ss << "couldn't parse pgid '" << cmd[1] << "'";
      r = -EINVAL;
    } else {
      PG *pg = _lookup_lock_pg(pgid);
      if (!pg) {
	ss << "i don't have pgid " << pgid;
	r = -ENOENT;
      } else {
	cmd.erase(cmd.begin(), cmd.begin() + 2);
	r = pg->do_command(cmd, ss, data, odata);
	pg->unlock();
      }
    }
    return true;
  }

  else if (cmd.size() >= 1 && cmd[0] == "flush_pg_stats") {
    flush_pg_stats();
    return true;
  }

  else if (!cmd.empty() && cmd[0] == "flush_pg_stats") {
    flush_pg_stats();
    return true;
  }

  else if (cmd[0] == "dump_pg_recovery_stats") {
    stringstream s;
    pg_recovery_stats.dump(s);
    ss << "dump pg recovery stats: " << s.str();
    return true;
  }

  else if (cmd[0] == "reset_pg_recovery_stats") {
    ss << "reset pg recovery stats";
    pg_recovery_stats.reset();
    return true;
  }

  else {
    return false;
  }
}


void PGOSD::advance_map_sub(ObjectStore::Transaction& t,
			       C_Contexts *tfin)
{
  assert(osd_lock.is_locked());

  dout(7) << "advance_map epoch " << osdmap->get_epoch()
	  << "  " << pg_map.size() << " pgs"
	  << dendl;

  if (!up_epoch &&
      osdmap->is_up(whoami) &&
      osdmap->get_inst(whoami) == client_messenger->get_myinst()) {
    up_epoch = osdmap->get_epoch();
    dout(10) << "up_epoch is " << up_epoch << dendl;
    if (!boot_epoch) {
      boot_epoch = osdmap->get_epoch();
      dout(10) << "boot_epoch is " << boot_epoch << dendl;
    }
  }

  // scan pg creations
  hash_map<pg_t, create_pg_info>::iterator n = creating_pgs.begin();
  while (n != creating_pgs.end()) {
    hash_map<pg_t, create_pg_info>::iterator p = n++;
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = pgosdmap()->pg_to_acting_osds(pgid, acting);
    int role = pgosdmap()->calc_pg_role(whoami, acting, nrep);
    if (role != 0) {
      dout(10) << " no longer primary for " << pgid << ", stopping creation" << dendl;
      creating_pgs.erase(p);
    } else {
      /*
       * adding new ppl to our pg has no effect, since we're still primary,
       * and obviously haven't given the new nodes any data.
       */
      p->second.acting.swap(acting);  // keep the latest
    }
  }

  // scan pgs with waiters
  map<pg_t, list<OpRequestRef> >::iterator p = waiting_for_pg.begin();
  while (p != waiting_for_pg.end()) {
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = pgosdmap()->pg_to_acting_osds(pgid, acting);
    int role = pgosdmap()->calc_pg_role(whoami, acting, nrep);
    if (role >= 0) {
      ++p;  // still me
    } else {
      dout(10) << " discarding waiting ops for " << pgid << dendl;
      while (!p->second.empty()) {
	p->second.pop_front();
      }
      waiting_for_pg.erase(p++);
    }
  }
  map<pg_t, list<PG::CephPeeringEvtRef> >::iterator q =
    peering_wait_for_split.begin();
  while (q != peering_wait_for_split.end()) {
    pg_t pgid = q->first;

    // am i still primary?
    vector<int> acting;
    int nrep = pgosdmap()->pg_to_acting_osds(pgid, acting);
    int role = pgosdmap()->calc_pg_role(whoami, acting, nrep);
    if (role >= 0) {
      ++q;  // still me
    } else {
      dout(10) << " discarding waiting ops for " << pgid << dendl;
      peering_wait_for_split.erase(q++);
    }
  }
}

void PGOSD::consume_map_sub()
{
  int num_pg_primary = 0, num_pg_replica = 0, num_pg_stray = 0;
  list<PGRef> to_remove;

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       ++it) {
    PG *pg = it->second;
    pg->lock();
    if (pg->is_primary())
      num_pg_primary++;
    else if (pg->is_replica())
      num_pg_replica++;
    else
      num_pg_stray++;

    if (!pgosdmap()->have_pg_pool(pg->info.pgid.pool())) {
      //pool is deleted!
      to_remove.push_back(PGRef(pg));
    } else {
      pgosdservice()->init_splits_between(it->first,
					  pgosdservice()->pgosd()->pgosdmap(),
					  pgosdmap());
    }

    pg->unlock();
  }

  for (list<PGRef>::iterator i = to_remove.begin();
       i != to_remove.end();
       to_remove.erase(i++)) {
    (*i)->lock();
    _remove_pg(&**i);
    (*i)->unlock();
  }
  to_remove.clear();

  pgosdservice()->expand_pg_num(pgosdservice()->pgosdmap(), pgosdmap());

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       ++it) {
    PG *pg = it->second;
    pg->lock();
    pg->queue_null(osdmap->get_epoch(), osdmap->get_epoch());
    pg->unlock();
  }

  logger->set(l_osd_pg, pg_map.size());
  logger->set(l_osd_pg_primary, num_pg_primary);
  logger->set(l_osd_pg_replica, num_pg_replica);
  logger->set(l_osd_pg_stray, num_pg_stray);

  if (!is_active()) {
    dout(10) << " not yet active; waiting for peering wq to drain" << dendl;
    peering_wq.drain();
  }
}

bool PGOSD::do_command_debug_sub(vector<string>& cmd,
				 int& r,
				 ostringstream& ss)
{
  if (cmd.size() == 3 && cmd[1] == "dump_missing") {
    const string &file_name(cmd[2]);
    std::ofstream fout(file_name.c_str());
    if (!fout.is_open()) {
      ss << "failed to open file '" << file_name << "'";
      r = -EINVAL;
      return false;
    }

    std::set <pg_t> keys;
    for (hash_map<pg_t, PG*>::const_iterator pg_map_e = pg_map.begin();
	 pg_map_e != pg_map.end(); ++pg_map_e) {
      keys.insert(pg_map_e->first);
    }

    fout << "*** osd " << whoami << ": dump_missing ***" << std::endl;
    for (std::set <pg_t>::iterator p = keys.begin();
	 p != keys.end(); ++p) {
      hash_map<pg_t, PG*>::iterator q = pg_map.find(*p);
      assert(q != pg_map.end());
      PG *pg = q->second;
      pg->lock();

      fout << *pg << std::endl;
      std::map<hobject_t, pg_missing_t::item>::iterator mend = pg->missing.missing.end();
      std::map<hobject_t, pg_missing_t::item>::iterator mi = pg->missing.missing.begin();
      for (; mi != mend; ++mi) {
	fout << mi->first << " -> " << mi->second << std::endl;
	map<hobject_t, set<int> >::const_iterator mli =
	  pg->missing_loc.find(mi->first);
	if (mli == pg->missing_loc.end())
	  continue;
	const set<int> &mls(mli->second);
	if (mls.empty())
	  continue;
	fout << "missing_loc: " << mls << std::endl;
      } // for mi
      pg->unlock();
      fout << std::endl;
    } // for p

    fout.close();

    return true;
  }

  else if (cmd.size() == 3 && cmd[1] == "kick_recovery_wq") {
    r = g_conf->set_val("osd_recovery_delay_start", cmd[2].c_str());
    if (r != 0) {	
      ss << "kick_recovery_wq: error setting "
	 << "osd_recovery_delay_start to '" << cmd[2] << "': error "
	 << r;
      return false;
    }
    g_conf->apply_changes(NULL);
    ss << "kicking recovery queue. set osd_recovery_delay_start "
       << "to " << g_conf->osd_recovery_delay_start;
    defer_recovery_until = ceph_clock_now(g_ceph_context);
    defer_recovery_until += g_conf->osd_recovery_delay_start;
    recovery_wq.wake();

    return true;
  }

  else {
    return true;
  }
}


void PGOSD::dispatch_op_sub(OpRequestRef op)
{
  switch (op->request->get_type()) {
  case MSG_OSD_PG_CREATE:
    handle_pg_create(op);
    break;

  case MSG_OSD_PG_NOTIFY:
    handle_pg_notify(op);
    break;
  case MSG_OSD_PG_QUERY:
    handle_pg_query(op);
    break;
  case MSG_OSD_PG_LOG:
    handle_pg_log(op);
    break;
  case MSG_OSD_PG_REMOVE:
    handle_pg_remove(op);
    break;
  case MSG_OSD_PG_INFO:
    handle_pg_info(op);
    break;
  case MSG_OSD_PG_TRIM:
    handle_pg_trim(op);
    break;
  case MSG_OSD_PG_MISSING:
    assert(0 ==
	   "received MOSDPGMissing; this message is supposed to be unused!?!");
    break;
  case MSG_OSD_PG_SCAN:
    handle_pg_scan(op);
    break;
  case MSG_OSD_PG_BACKFILL:
    handle_pg_backfill(op);
    break;

  case MSG_OSD_BACKFILL_RESERVE:
    handle_pg_backfill_reserve(op);
    break;
  case MSG_OSD_RECOVERY_RESERVE:
    handle_pg_recovery_reserve(op);
    break;

  } // switch
}


bool PGOSD::_dispatch_sub(Message *m)
{
  switch (m->get_type()) {

  case MSG_PGSTATSACK:
    handle_pg_stats_ack(static_cast<MPGStatsAck*>(m));
    break;

  case MSG_OSD_SCRUB:
    handle_scrub(static_cast<MOSDScrub*>(m));
    break;

  case MSG_OSD_REP_SCRUB:
    handle_rep_scrub(static_cast<MOSDRepScrub*>(m));
    break;

  default:
    // not handled
    return false;
  }

  // handled
  return true;
}

void PGOSD::handle_rep_scrub(MOSDRepScrub *m)
{
  dout(10) << "queueing MOSDRepScrub " << *m << dendl;
  rep_scrub_wq.queue(m);
}

void PGOSD::handle_scrub(MOSDScrub *m)
{
  dout(10) << "handle_scrub " << *m << dendl;
  if (!require_mon_peer(m))
    return;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_scrub fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  if (m->scrub_pgs.empty()) {
    for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
	 p != pg_map.end();
	 ++p) {
      PG *pg = p->second;
      pg->lock();
      if (pg->is_primary()) {
	pg->unreg_next_scrub();
	pg->scrubber.must_scrub = true;
	pg->scrubber.must_deep_scrub = m->deep || m->repair;
	pg->scrubber.must_repair = m->repair;
	pg->reg_next_scrub();
	dout(10) << "marking " << *pg << " for scrub" << dendl;
      }
      pg->unlock();
    }
  } else {
    for (vector<pg_t>::iterator p = m->scrub_pgs.begin();
	 p != m->scrub_pgs.end();
	 ++p)
      if (pg_map.count(*p)) {
	PG *pg = pg_map[*p];
	pg->lock();
	if (pg->is_primary()) {
	  pg->unreg_next_scrub();
	  pg->scrubber.must_scrub = true;
	  pg->scrubber.must_deep_scrub = m->deep || m->repair;
	  pg->scrubber.must_repair = m->repair;
	  pg->reg_next_scrub();
	  dout(10) << "marking " << *pg << " for scrub" << dendl;
	}
	pg->unlock();
      }
  }

  m->put();
}

void PGOSD::advance_pg(epoch_t osd_epoch, PG *pg,
		       ThreadPool::TPHandle &handle,
		       PG::RecoveryCtx *rctx,
		       set<boost::intrusive_ptr<PG> > *new_pgs)
{
  assert(pg->is_locked());
  epoch_t next_epoch = pg->get_osdmap()->get_epoch() + 1;
  PGOSDMapRef lastmap = pg->get_osdmap();

  if (lastmap->get_epoch() == osd_epoch)
    return;
  assert(lastmap->get_epoch() < osd_epoch);

  for (;
       next_epoch <= osd_epoch;
       ++next_epoch) {
    PGOSDMapRef nextmap = get_map(next_epoch);

    vector<int> newup, newacting;
    pgosdmap(nextmap)->pg_to_up_acting_osds(pg->info.pgid,
					    newup,
					    newacting);
    pg->handle_advance_map(nextmap, lastmap, newup, newacting, rctx);

    // Check for split!
    set<pg_t> children;
    if (pg->info.pgid.is_split(
	lastmap->get_pg_num(pg->pool.id),
	nextmap->get_pg_num(pg->pool.id),
	&children)) {
      pgosdservice()->mark_split_in_progress(pg->info.pgid, children);
      split_pgs(
	pg, children, new_pgs, lastmap, nextmap,
	rctx);
    }

    lastmap = nextmap;
    handle.reset_tp_timeout();
  }
  pg->handle_activate_map(rctx);
}

// ----------------------------------------
// pg creation


bool PGOSD::can_create_pg(pg_t pgid)
{
  assert(creating_pgs.count(pgid));

  // priors empty?
  if (!creating_pgs[pgid].prior.empty()) {
    dout(10) << "can_create_pg " << pgid
	     << " - waiting for priors " << creating_pgs[pgid].prior << dendl;
    return false;
  }

  dout(10) << "can_create_pg " << pgid << " - can create now" << dendl;
  return true;
}

void PGOSD::split_pg(PG *parent, map<pg_t,PG*>& children,
		     ObjectStore::Transaction &t)
{
  dout(10) << "split_pg " << *parent << dendl;
  pg_t parentid = parent->info.pgid;

  // split objects
  vector<hobject_t> olist;
  store->collection_list(coll_t(parent->info.pgid), olist);

  for (vector<hobject_t>::iterator p = olist.begin(); p != olist.end(); ++p) {
    hobject_t poid = *p;
    object_locator_t oloc(parentid.pool());
    if (poid.get_key().size())
      oloc.key = poid.get_key();
    pg_t rawpg = pgosdmap()->object_locator_to_pg(poid.oid, oloc);
    pg_t pgid = pgosdmap()->raw_pg_to_pg(rawpg);
    if (pgid != parentid) {
      dout(20) << "  moving " << poid << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];
      assert(child);
      bufferlist bv;

      struct stat st;
      store->stat(coll_t(parentid), poid, &st);
      store->getattr(coll_t(parentid), poid, OI_ATTR, bv);
      object_info_t oi(bv);

      t.collection_move(coll_t(pgid), coll_t(parentid), poid);
      if (!oi.snaps.empty()) {
	snapid_t first = oi.snaps[0];
	t.collection_move(coll_t(pgid, first), coll_t(parentid), poid);
	if (oi.snaps.size() > 1) {
	  snapid_t last = oi.snaps[oi.snaps.size()-1];
	  t.collection_move(coll_t(pgid, last), coll_t(parentid), poid);
	}
      }

      // add to child stats
      child->info.stats.stats.sum.num_bytes += st.st_size;
      child->info.stats.stats.sum.num_objects++;
      if (poid.snap && poid.snap != CEPH_NOSNAP)
	child->info.stats.stats.sum.num_object_clones++;
    } else {
      dout(20) << " leaving " << poid << "   in " << parentid << dendl;
    }
  }

  // split log
  parent->log.index();
  dout(20) << " parent " << parent->info.pgid << " log was ";
  parent->log.print(*_dout);
  *_dout << dendl;
  parent->log.unindex();

  list<pg_log_entry_t>::iterator p = parent->log.log.begin();
  while (p != parent->log.log.end()) {
    list<pg_log_entry_t>::iterator cur = p;
    ++p;
    hobject_t& poid = cur->soid;
    object_locator_t oloc(parentid.pool());
    if (poid.get_key().size())
      oloc.key = poid.get_key();
    pg_t rawpg = pgosdmap()->object_locator_to_pg(poid.oid, oloc);
    pg_t pgid = pgosdmap()->raw_pg_to_pg(rawpg);
    if (pgid != parentid) {
      dout(20) << "  moving " << *cur << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];

      child->log.log.splice(child->log.log.end(), parent->log.log, cur);
    }
  }

  parent->log.index();
  dout(20) << " parent " << parent->info.pgid << " log now ";
  parent->log.print(*_dout);
  *_dout << dendl;

  for (map<pg_t,PG*>::iterator p = children.begin();
       p != children.end();
       ++p) {
    PG *child = p->second;

    // fix log bounds
    if (!child->log.empty()) {
      child->log.head = child->log.log.rbegin()->version;
      child->log.tail =  parent->log.tail;
      child->log.index();
    }
    child->info.last_update = child->log.head;
    child->info.last_complete = child->info.last_update;
    child->info.log_tail = parent->log.tail;
    child->info.history.last_epoch_split = osdmap->get_epoch();

    child->snap_trimq = parent->snap_trimq;

    dout(20) << " child " << p->first << " log now ";
    child->log.print(*_dout);
    *_dout << dendl;

    // sub off child stats
    parent->info.stats.sub(child->info.stats);
  }
}

void PGOSD::split_pgs(PG *parent,
		      const set<pg_t> &childpgids,
		      set<boost::intrusive_ptr<PG> > *out_pgs,
		      PGOSDMapRef curmap,
		      PGOSDMapRef nextmap,
		      PG::RecoveryCtx *rctx)
{
  unsigned pg_num = nextmap->get_pg_num(
    parent->pool.id);
  parent->update_snap_mapper_bits(
    parent->info.pgid.get_split_bits(pg_num)
    );
  for (set<pg_t>::const_iterator i = childpgids.begin();
       i != childpgids.end();
       ++i) {
    dout(10) << "Splitting " << *parent << " into " << *i << dendl;
    assert(pgosdservice()->splitting(*i));
    PG* child = _make_pg(nextmap, *i);
    child->lock(true);
    out_pgs->insert(child);

    unsigned split_bits = i->get_split_bits(pg_num);
    dout(10) << "pg_num is " << pg_num << dendl;
    dout(10) << "m_seed " << i->ps() << dendl;
    dout(10) << "split_bits is " << split_bits << dendl;

    rctx->transaction->create_collection(
      coll_t(*i));
    rctx->transaction->split_collection(
      coll_t(parent->info.pgid),
      split_bits,
      i->m_seed,
      coll_t(*i));
    if (parent->have_temp_coll()) {
      rctx->transaction->create_collection(
	coll_t::make_temp_coll(*i));
      rctx->transaction->split_collection(
	coll_t::make_temp_coll(parent->info.pgid),
	split_bits,
	i->m_seed,
	coll_t::make_temp_coll(*i));
    }
    parent->split_into(
      *i,
      child,
      split_bits);

    child->write_if_dirty(*(rctx->transaction));
    child->unlock();
  }
  parent->write_if_dirty(*(rctx->transaction));
}

void PGOSD::do_split(PG *parent, set<pg_t>& childpgids, ObjectStore::Transaction& t,
		     C_Contexts *tfin)
{
  dout(10) << "do_split to " << childpgids << " on " << *parent << dendl;

  parent->lock();

  // create and lock children
  map<pg_t,PG*> children;
  for (set<pg_t>::iterator q = childpgids.begin();
       q != childpgids.end();
       ++q) {
    pg_history_t history;
    history.epoch_created = history.same_up_since =
      history.same_interval_since = history.same_primary_since =
      osdmap->get_epoch();
    pg_interval_map_t pi;
    PG *pg = _create_lock_pg(pgosdservice()->pgosd()->pgosdmap(),
			     *q, true, true, parent->get_role(),
			     parent->up, parent->acting, history, pi, t);
    children[*q] = pg;
    dout(10) << "  child " << *pg << dendl;
  }

  split_pg(parent, children, t); 

  // reset pg
  map< int, vector<pair<pg_notify_t, pg_interval_map_t> > >  notify_list;  // primary -> list
  map< int, map<pg_t,pg_query_t> > query_map;    // peer -> PG -> get_summary_since
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > info_map;  // peer -> message
  PG::RecoveryCtx rctx(&query_map, &info_map, &notify_list,
		       tfin, tfin, &t);

  // FIXME: this breaks if we have a map discontinuity
  //parent->handle_split(osdmap, get_map(osdmap->get_epoch() - 1), &rctx);

  // unlock parent, children
  parent->unlock();

  for (map<pg_t,PG*>::iterator q = children.begin(); q != children.end(); ++q) {
    PG *pg = q->second;
    pg->handle_create(&rctx);
    pg->write_if_dirty(t);
    wake_pg_waiters(pg->info.pgid);
    pg->unlock();
  }
}


/*
 * holding osd_lock
 */
void PGOSD::handle_pg_create(OpRequestRef op)
{
  MOSDPGCreate *m = (MOSDPGCreate*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_CREATE);

  dout(10) << "handle_pg_create " << *m << dendl;

  // drop the next N pg_creates in a row?
  if (debug_drop_pg_create_left < 0 &&
      g_conf->osd_debug_drop_pg_create_probability >
      ((((double)(rand()%100))/100.0))) {
    debug_drop_pg_create_left = debug_drop_pg_create_duration;
  }
  if (debug_drop_pg_create_left >= 0) {
    --debug_drop_pg_create_left;
    if (debug_drop_pg_create_left >= 0) {
      dout(0) << "DEBUG dropping/ignoring pg_create, will drop the next "
	      << debug_drop_pg_create_left << " too" << dendl;
      return;
    }
  }

  if (!require_mon_peer(op->request)) {
    // we have to hack around require_mon_peer's interface limits
    op->request = NULL;
    return;
  }

  if (!require_same_or_newer_map(op, m->epoch)) return;

  op->mark_started();

  int num_created = 0;

  for (map<pg_t,pg_create_t>::iterator p = m->mkpg.begin();
       p != m->mkpg.end();
       p++) {
    pg_t pgid = p->first;
    epoch_t created = p->second.created;
    pg_t parent = p->second.parent;
    int split_bits = p->second.split_bits;
    pg_t on = pgid;

    if (pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    if (split_bits) {
      on = parent;
      dout(20) << "mkpg " << pgid << " e" << created << " from parent " << parent
	       << " split by " << split_bits << " bits" << dendl;
    } else {
      dout(20) << "mkpg " << pgid << " e" << created << dendl;
    }
   
    // is it still ours?
    vector<int> up, acting;
    pgosdmap()->pg_to_up_acting_osds(on, up, acting);
    int role = pgosdmap()->calc_pg_role(whoami, acting, acting.size());

    if (role != 0) {
      dout(10) << "mkpg " << pgid << "  not primary (role=" << role << "), skipping" << dendl;
      continue;
    }
    if (up != acting) {
      dout(10) << "mkpg " << pgid << "  up " << up << " != acting " << acting << dendl;
      clog.error() << "mkpg " << pgid << " up " << up << " != acting "
	    << acting << "\n";
      continue;
    }

    // does it already exist?
    if (_have_pg(pgid)) {
      dout(10) << "mkpg " << pgid << "  already exists, skipping" << dendl;
      continue;
    }

    // does parent exist?
    if (split_bits && !_have_pg(parent)) {
      dout(10) << "mkpg " << pgid << "  missing parent " << parent << ", skipping" << dendl;
      continue;
    }

    // figure history
    pg_history_t history;
    history.epoch_created = created;
    history.last_epoch_clean = created;
    project_pg_history(pgid, history, created, up, acting);
    
    // register.
    creating_pgs[pgid].history = history;
    creating_pgs[pgid].parent = parent;
    creating_pgs[pgid].split_bits = split_bits;
    creating_pgs[pgid].acting.swap(acting);
    calc_priors_during(pgid, created, history.same_interval_since, 
		       creating_pgs[pgid].prior);

    PG::RecoveryCtx rctx = create_context();
    // poll priors
    set<int>& pset = creating_pgs[pgid].prior;
    dout(10) << "mkpg " << pgid << " e" << created
	     << " h " << history
	     << " : querying priors " << pset << dendl;
    for (set<int>::iterator p = pset.begin(); p != pset.end(); p++) 
      if (osdmap->is_up(*p))
	(*rctx.query_map)[*p][pgid] = pg_query_t(pg_query_t::INFO, history,
						 osdmap->get_epoch());

    PG *pg = NULL;
    if (can_create_pg(pgid)) {
      pg_interval_map_t pi;
      pg = _create_lock_pg(
	pgosdmap(), pgid, true, false,
	0, creating_pgs[pgid].acting, creating_pgs[pgid].acting,
	history, pi,
	*rctx.transaction);
      creating_pgs.erase(pgid);
      wake_pg_waiters(pg->info.pgid);
      pg->handle_create(&rctx);
      pg->write_if_dirty(*rctx.transaction);
      pg->unlock();
      num_created++;
    }
    dispatch_context(rctx, pg, pgosdmap());
  }

  maybe_update_heartbeat_peers();
}


// ----------------------------------------
// peering and recovery

PG::RecoveryCtx PGOSD::create_context()
{
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  C_Contexts *on_applied = new C_Contexts(g_ceph_context);
  C_Contexts *on_safe = new C_Contexts(g_ceph_context);
  map< int, map<pg_t,pg_query_t> > *query_map =
    new map<int, map<pg_t, pg_query_t> >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  PG::RecoveryCtx rctx(query_map, info_map, notify_list,
		       on_applied, on_safe, t);
  return rctx;
}

void PGOSD::dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg)
{
  if (!ctx.transaction->empty()) {
    ctx.on_applied->add(new ObjectStore::C_DeleteTransaction(ctx.transaction));
    int tr = store->queue_transaction(
      pg->osr.get(),
      ctx.transaction, ctx.on_applied, ctx.on_safe);
    assert(tr == 0);
    ctx.transaction = new ObjectStore::Transaction;
    ctx.on_applied = new C_Contexts(g_ceph_context);
    ctx.on_safe = new C_Contexts(g_ceph_context);
  }
}

bool PGOSD::compat_must_dispatch_immediately(PG *pg)
{
  assert(pg->is_locked());
  for (vector<int>::iterator i = pg->acting.begin();
       i != pg->acting.end();
       ++i) {
    if (*i == whoami)
      continue;
    ConnectionRef conn =
      service->get_con_osd_cluster(*i, pg->get_osdmap()->get_epoch());
    if (conn && !(conn->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      return true;
    }
  }
  return false;
}

void PGOSD::dispatch_context(PG::RecoveryCtx &ctx, PG *pg,
			     PGOSDMapRef curmap)
{
  if (service->get_osdmap()->is_up(whoami)) {
    do_notifies(*ctx.notify_list, curmap);
    do_queries(*ctx.query_map, curmap);
    do_infos(*ctx.info_map, curmap);
  }
  delete ctx.notify_list;
  delete ctx.query_map;
  delete ctx.info_map;
  if ((ctx.on_applied->empty() &&
       ctx.on_safe->empty() &&
       ctx.transaction->empty()) || !pg) {
    delete ctx.transaction;
    delete ctx.on_applied;
    delete ctx.on_safe;
  } else {
    ctx.on_applied->add(new ObjectStore::C_DeleteTransaction(ctx.transaction));
    int tr = store->queue_transaction(
      pg->osr.get(),
      ctx.transaction, ctx.on_applied, ctx.on_safe);
    assert(tr == 0);
  }
}

/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void PGOSD::do_notifies(
  map< int,vector<pair<pg_notify_t,pg_interval_map_t> > >& notify_list,
  PGOSDMapRef curmap)
{
  for (map< int, vector<pair<pg_notify_t,pg_interval_map_t> > >::iterator it = notify_list.begin();
       it != notify_list.end();
       ++it) {
    if (it->first == whoami) {
      dout(7) << "do_notify osd." << it->first << " is self, skipping" << dendl;
      continue;
    }
    if (!curmap->is_up(it->first))
      continue;
    ConnectionRef con = service->get_con_osd_cluster(it->first, curmap->get_epoch());
    if (!con)
      continue;
    _share_map_outgoing(it->first, con.get(), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_notify osd." << it->first
	      << " on " << it->second.size() << " PGs" << dendl;
      MOSDPGNotify *m = new MOSDPGNotify(curmap->get_epoch(),
					 it->second);
      cluster_messenger->send_message(m, con.get());
    } else {
      dout(7) << "do_notify osd." << it->first
	      << " sending seperate messages" << dendl;
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     it->second.begin();
	   i != it->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > list(1);
	list[0] = *i;
	MOSDPGNotify *m = new MOSDPGNotify(i->first.epoch_sent,
					   list);
	cluster_messenger->send_message(m, con.get());
      }
    }
  }
}

/** do_queries
 * send out pending queries for info | summaries
 */
void PGOSD::do_queries(map< int, map<pg_t,pg_query_t> >& query_map,
		       PGOSDMapRef curmap)
{
  for (map< int, map<pg_t,pg_query_t> >::iterator pit = query_map.begin();
       pit != query_map.end();
       ++pit) {
    if (!curmap->is_up(pit->first))
      continue;
    int who = pit->first;
    ConnectionRef con = service->get_con_osd_cluster(who, curmap->get_epoch());
    if (!con)
      continue;
    _share_map_outgoing(who, con.get(), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_queries querying osd." << who
	      << " on " << pit->second.size() << " PGs" << dendl;
      MOSDPGQuery *m = new MOSDPGQuery(curmap->get_epoch(), pit->second);
      cluster_messenger->send_message(m, con.get());
    } else {
      dout(7) << "do_queries querying osd." << who
	      << " sending seperate messages "
	      << " on " << pit->second.size() << " PGs" << dendl;
      for (map<pg_t, pg_query_t>::iterator i = pit->second.begin();
	   i != pit->second.end();
	   ++i) {
	map<pg_t, pg_query_t> to_send;
	to_send.insert(*i);
	MOSDPGQuery *m = new MOSDPGQuery(i->second.epoch_sent, to_send);
	cluster_messenger->send_message(m, con.get());
      }
    }
  }
}



void PGOSD::do_infos(map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		     PGOSDMapRef curmap)
{
  for (map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >::iterator p = info_map.begin();
       p != info_map.end();
       ++p) {
    if (!curmap->is_up(p->first))
      continue;
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator i = p->second.begin();
	 i != p->second.end();
	 ++i) {
      dout(20) << "Sending info " << i->first.info << " to osd." << p->first << dendl;
    }
    ConnectionRef con = service->get_con_osd_cluster(p->first, curmap->get_epoch());
    if (!con)
      continue;
    _share_map_outgoing(p->first, con.get(), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      MOSDPGInfo *m = new MOSDPGInfo(curmap->get_epoch());
      m->pg_list = p->second;
      cluster_messenger->send_message(m, con.get());
    } else {
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     p->second.begin();
	   i != p->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > to_send(1);
	to_send[0] = *i;
	MOSDPGInfo *m = new MOSDPGInfo(i->first.epoch_sent);
	m->pg_list = to_send;
	cluster_messenger->send_message(m, con.get());
      }
    }
  }
  info_map.clear();
}



/** PGNotify
 * from non-primary to primary
 * includes pg_info_t.
 * NOTE: called with opqueue active.
 */
void PGOSD::handle_pg_notify(OpRequestRef op)
{
  MOSDPGNotify *m = (MOSDPGNotify*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_NOTIFY);

  dout(7) << "handle_pg_notify from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (!require_osd_peer(op))
    return;

  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       ++it) {
    PG *pg = 0;

    if (it->first.info.pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << it->first.info.pgid << dendl;
      continue;
    }

    int created = 0;
    if (pgosdservice()->splitting(it->first.info.pgid)) {
      peering_wait_for_split[it->first.info.pgid].push_back(
	PG::CephPeeringEvtRef(
	  new PG::CephPeeringEvt(
	    it->first.epoch_sent, it->first.query_epoch,
	    PG::MNotifyRec(from, it->first))));
      continue;
    }

    pg = get_or_create_pg(it->first.info, it->second,
			  it->first.query_epoch, from, created, true);
    if (!pg)
      continue;
    pg->queue_notify(it->first.epoch_sent, it->first.query_epoch, from, it->first);
    pg->unlock();
  }
}

void PGOSD::handle_pg_log(OpRequestRef op)
{
  MOSDPGLog *m = (MOSDPGLog*) op->request;
  assert(m->get_header().type == MSG_OSD_PG_LOG);
  dout(7) << "handle_pg_log " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  if (m->info.pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->info.pgid << dendl;
    return;
  }

  if (pgosdservice()->splitting(m->info.pgid)) {
    peering_wait_for_split[m->info.pgid].push_back(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->get_epoch(), m->get_query_epoch(),
	  PG::MLogRec(from, m))));
    return;
  }

  int created = 0;
  PG *pg = get_or_create_pg(m->info, m->past_intervals, m->get_epoch(), 
			    from, created, false);
  if (!pg)
    return;
  op->mark_started();
  pg->queue_log(m->get_epoch(), m->get_query_epoch(), from, m);
  pg->unlock();
}

void PGOSD::handle_pg_info(OpRequestRef op)
{
  MOSDPGInfo *m = static_cast<MOSDPGInfo *>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_INFO);
  dout(7) << "handle_pg_info " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  int created = 0;

  for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator p = m->pg_list.begin();
       p != m->pg_list.end();
       ++p) {
    if (p->first.info.pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << p->first.info.pgid << dendl;
      continue;
    }

    if (pgosdservice()->splitting(p->first.info.pgid)) {
      peering_wait_for_split[p->first.info.pgid].push_back(
	PG::CephPeeringEvtRef(
	  new PG::CephPeeringEvt(
	    p->first.epoch_sent, p->first.query_epoch,
	    PG::MInfoRec(from, p->first.info, p->first.epoch_sent))));
      continue;
    }
    PG *pg = get_or_create_pg(p->first.info, p->second, p->first.epoch_sent,
			      from, created, false);
    if (!pg)
      continue;
    pg->queue_info(p->first.epoch_sent, p->first.query_epoch, from,
		   p->first.info);
    pg->unlock();
  }
}

void PGOSD::handle_pg_trim(OpRequestRef op)
{
  MOSDPGTrim *m = (MOSDPGTrim *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_TRIM);

  dout(7) << "handle_pg_trim " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->epoch)) return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  op->mark_started();

  if (!_have_pg(m->pgid)) {
    dout(10) << " don't have pg " << m->pgid << dendl;
  } else {
    PG *pg = _lookup_lock_pg(m->pgid);
    if (m->epoch < pg->info.history.same_interval_since) {
      dout(10) << *pg << " got old trim to " << m->trim_to << ", ignoring" << dendl;
      pg->unlock();
      return;
    }
    assert(pg);

    if (pg->is_primary()) {
      // peer is informing us of their last_complete_ondisk
      dout(10) << *pg << " replica osd." << from << " lcod " << m->trim_to << dendl;
      pg->peer_last_complete_ondisk[from] = m->trim_to;
      if (pg->calc_min_last_complete_ondisk()) {
	dout(10) << *pg << " min lcod now " << pg->min_last_complete_ondisk << dendl;
	pg->trim_peers();
      }
    } else {
      // primary is instructing us to trim
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      pg->trim(*t, m->trim_to);
      pg->dirty_info = true;
      pg->write_if_dirty(*t);
      int tr = store->queue_transaction(pg->osr.get(), t,
					new ObjectStore::C_DeleteTransaction(t));
      assert(tr == 0);
    }
    pg->unlock();
  }
}

void PGOSD::handle_pg_scan(OpRequestRef op)
{
  MOSDPGScan *m = static_cast<MOSDPGScan*>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_SCAN);
  dout(10) << "handle_pg_scan " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  PG *pg;

  if (!_have_pg(m->pgid)) {
    return;
  }

  pg = _lookup_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
}

void PGOSD::handle_pg_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = static_cast<MOSDPGBackfill*>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);
  dout(10) << "handle_pg_backfill " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  PG *pg;

  if (!_have_pg(m->pgid)) {
    return;
  }

  pg = _lookup_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
}

void PGOSD::handle_pg_backfill_reserve(OpRequestRef op)
{
  MBackfillReserve *m = static_cast<MBackfillReserve*>(op->request);
  assert(m->get_header().type == MSG_OSD_BACKFILL_RESERVE);

  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  PG *pg = 0;
  if (!_have_pg(m->pgid))
    return;

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  if (m->type == MBackfillReserve::REQUEST) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RequestBackfill())));
  } else if (m->type == MBackfillReserve::GRANT) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RemoteBackfillReserved())));
  } else if (m->type == MBackfillReserve::REJECT) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RemoteReservationRejected())));
  } else {
    assert(0);
  }
  pg->unlock();
}

void PGOSD::handle_pg_recovery_reserve(OpRequestRef op)
{
  MRecoveryReserve *m = static_cast<MRecoveryReserve*>(op->request);
  assert(m->get_header().type == MSG_OSD_RECOVERY_RESERVE);

  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  PG *pg = 0;
  if (!_have_pg(m->pgid))
    return;

  pg = _lookup_lock_pg(m->pgid);
  if (!pg)
    return;

  if (m->type == MRecoveryReserve::REQUEST) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RequestRecovery())));
  } else if (m->type == MRecoveryReserve::GRANT) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RemoteRecoveryReserved())));
  } else if (m->type == MRecoveryReserve::RELEASE) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RecoveryDone())));
  } else {
    assert(0);
  }
  pg->unlock();
}

/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
 */
void PGOSD::handle_pg_query(OpRequestRef op)
{
  assert(osd_lock.is_locked());

  MOSDPGQuery *m = (MOSDPGQuery*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_QUERY);

  if (!require_osd_peer(op))
    return;

  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << dendl;
  int from = m->get_source().num();
  
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > notify_list;
  
  for (map<pg_t,pg_query_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       ++it) {
    pg_t pgid = it->first;

    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    if (pgosdservice()->splitting(pgid)) {
      peering_wait_for_split[pgid].push_back(
	PG::CephPeeringEvtRef(
	  new PG::CephPeeringEvt(
	    it->second.epoch_sent, it->second.epoch_sent,
	    PG::MQuery(from, it->second, it->second.epoch_sent))));
      continue;
    }

    if (pg_map.count(pgid)) {
      PG *pg = 0;
      pg = _lookup_lock_pg(pgid);
      pg->queue_query(it->second.epoch_sent, it->second.epoch_sent,
		      from, it->second);
      pg->unlock();
      continue;
    }

    if (!pgosdmap()->have_pg_pool(pgid.pool()))
      continue;

    // get active crush mapping
    vector<int> up, acting;
    pgosdmap()->pg_to_up_acting_osds(pgid, up, acting);
    int role = pgosdmap()->calc_pg_role(whoami, acting, acting.size());

    // same primary?
    pg_history_t history = it->second.history;
    project_pg_history(pgid, history, it->second.epoch_sent, up, acting);

    if (it->second.epoch_sent < history.same_interval_since) {
      dout(10) << " pg " << pgid << " dne, and pg has changed in "
	       << history.same_interval_since
	       << " (msg from " << it->second.epoch_sent << ")" << dendl;
      continue;
    }

    assert(role != 0);
    dout(10) << " pg " << pgid << " dne" << dendl;
    pg_info_t empty(pgid);
    if (it->second.type == pg_query_t::LOG ||
	it->second.type == pg_query_t::FULLLOG) {
      ConnectionRef con = service->get_con_osd_cluster(from,
						       osdmap->get_epoch());
      if (con) {
	MOSDPGLog *mlog = new MOSDPGLog(osdmap->get_epoch(), empty,
					it->second.epoch_sent);
	_share_map_outgoing(from, con.get(), osdmap);
	cluster_messenger->send_message(mlog, con.get());
      }
    } else {
      notify_list[from].push_back(make_pair(pg_notify_t(it->second.epoch_sent,
							osdmap->get_epoch(),
							empty),
					    pg_interval_map_t()));
    }
  }
  do_notifies(notify_list, pgosdmap());
}

void PGOSD::handle_pg_remove(OpRequestRef op)
{
  MOSDPGRemove *m = (MOSDPGRemove *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_REMOVE);
  assert(osd_lock.is_locked());

  if (!require_osd_peer(op))
    return;

  dout(7) << "handle_pg_remove from " << m->get_source() << " on "
	  << m->pg_list.size() << " pgs" << dendl;

  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  for (vector<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       ++it) {
    pg_t pgid = *it;
    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << dendl;
      continue;
    }
    dout(5) << "queue_pg_for_deletion: " << pgid << dendl;
    PG *pg = _lookup_lock_pg(pgid);
    pg_history_t history = pg->info.history;
    vector<int> up, acting;
    pgosdmap()->pg_to_up_acting_osds(pgid, up, acting);
    project_pg_history(pg->info.pgid, history, pg->get_osdmap()->get_epoch(),
		       up, acting);
    if (history.same_interval_since <= m->get_epoch()) {
      assert(pg->get_primary() == m->get_source().num());
      PGRef _pg(pg);
      _remove_pg(pg);
      pg->unlock();
    } else {
      dout(10) << *pg << " ignoring remove request, pg changed in epoch "
	       << history.same_interval_since
	       << " > " << m->get_epoch() << dendl;
      pg->unlock();
    }
  }
}

void PGOSD::_remove_pg(PG *pg)
{
  vector<coll_t> removals;
  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;

  // on_removal, which calls remove_watchers_and_notifies, and the erasure from
  // the pg_map must be done together without unlocking the pg lock,
  // to avoid racing with watcher cleanup in ms_handle_reset
  // and handle_notify_timeout
  pg->on_removal(rmt);

  pgosdservice()->cancel_pending_splits_for_parent(pg->info.pgid);

  coll_t to_remove = get_next_removal_coll(pg->info.pgid);
  removals.push_back(to_remove);
  rmt->collection_rename(coll_t(pg->info.pgid), to_remove);
  if (pg->have_temp_coll()) {
    to_remove = get_next_removal_coll(pg->info.pgid);
    removals.push_back(to_remove);
    rmt->collection_rename(pg->get_temp_coll(), to_remove);
  }

  store->queue_transaction(
    pg->osr.get(), rmt,
    new ObjectStore::C_DeleteTransactionHolder<
      SequencerRef>(rmt, pg->osr),
    new ContainerContext<
      SequencerRef>(pg->osr));

  DeletingStateRef deleting
    = pgosdservice()->deleting_pgs.lookup_or_create(pg->info.pgid);
  for (vector<coll_t>::iterator i = removals.begin();
       i != removals.end();
       ++i) {
    remove_wq.queue(new boost::tuple<coll_t, SequencerRef, DeletingStateRef>(
		      *i, pg->osr, deleting));
  }


  // remove from map
  pg_map.erase(pg->info.pgid);
  pg->put("PGMap"); // since we've taken it out of map
}


// =========================================================
// RECOVERY

/*
 * caller holds osd_lock
 */
void PGOSD::check_replay_queue()
{
  assert(osd_lock.is_locked());

  utime_t now = ceph_clock_now(g_ceph_context);
  list< pair<pg_t,utime_t> > pgids;
  replay_queue_lock.Lock();
  while (!replay_queue.empty() &&
	 replay_queue.front().second <= now) {
    pgids.push_back(replay_queue.front());
    replay_queue.pop_front();
  }
  replay_queue_lock.Unlock();

  for (list< pair<pg_t,utime_t> >::iterator p = pgids.begin(); p != pgids.end(); p++) {
    pg_t pgid = p->first;
    if (pg_map.count(pgid)) {
      PG *pg = _lookup_lock_pg_with_map_lock_held(pgid);
      dout(10) << "check_replay_queue " << *pg << dendl;
      if (pg->is_active() &&
	  pg->is_replay() &&
	  pg->get_role() == 0 &&
	  pg->replay_until == p->second) {
	pg->replay_queued_ops();
      }
      pg->unlock();
    } else {
      dout(10) << "check_replay_queue pgid " << pgid << " (not found)" << dendl;
    }
  }
  
  // wake up _all_ pg waiters; raw pg -> actual pg mapping may have shifted
  wake_all_pg_waiters();
}


bool PGOSDService::queue_for_recovery(PG *pg)
{
  bool b = recovery_wq.queue(pg);
  if (b)
    dout(10) << "queue_for_recovery queued " << *pg << dendl;
  else
    dout(10) << "queue_for_recovery already queued " << *pg << dendl;
  return b;
}

bool PGOSD::_recover_now()
{
  if (recovery_ops_active >= g_conf->osd_recovery_max_active) {
    dout(15) << "_recover_now active " << recovery_ops_active
	     << " >= max " << g_conf->osd_recovery_max_active << dendl;
    return false;
  }
  if (ceph_clock_now(g_ceph_context) < defer_recovery_until) {
    dout(15) << "_recover_now defer until " << defer_recovery_until << dendl;
    return false;
  }

  return true;
}

/*
 * caller holds osd_lock
 */

void PGOSD::do_recovery(PG *pg)
{
  // see how many we should try to start.  note that this is a bit racy.
  recovery_wq.lock();
  int max = g_conf->osd_recovery_max_active - recovery_ops_active;
  recovery_wq.unlock();
  if (max == 0) {
    dout(10) << "do_recovery raced and failed to start anything; requeuing " << *pg << dendl;
    recovery_wq.queue(pg);
  } else {
    pg->lock();
    if (pg->deleting || !(pg->is_active() && pg->is_primary())) {
      pg->unlock();
      return;
    }

    dout(10) << "do_recovery starting " << max
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;
#ifdef DEBUG_RECOVERY_OIDS
    dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
#endif

    PG::RecoveryCtx rctx = create_context();
    int started = pg->start_recovery_ops(max, &rctx);
    dout(10) << "do_recovery started " << started
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;

    /*
     * if we couldn't start any recovery ops and things are still
     * unfound, see if we can discover more missing object locations.
     * It may be that our initial locations were bad and we errored
     * out while trying to pull.
     */
    if (!started && pg->have_unfound()) {
      pg->discover_all_missing(*rctx.query_map);
      if (rctx.query_map->empty()) {
	dout(10) << "do_recovery  no luck, giving up on this pg for now" << dendl;
	recovery_wq.lock();
	recovery_wq._dequeue(pg);
	recovery_wq.unlock();
      }
    }

    pg->write_if_dirty(*rctx.transaction);
    PGOSDMapRef curmap = pg->get_osdmap();
    pg->unlock();
    dispatch_context(rctx, pg, curmap);
  }
}

void PGOSD::start_recovery_op(PG *pg, const hobject_t& soid)
{
  recovery_wq.lock();
  dout(10) << "start_recovery_op " << *pg << " " << soid
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid) == 0);
  recovery_oids[pg->info.pgid].insert(soid);
#endif

  recovery_wq.unlock();
}

void PGOSD::finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue)
{
  recovery_wq.lock();
  dout(10) << "finish_recovery_op " << *pg << " " << soid
	   << " dequeue=" << dequeue
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;

  // adjust count
  recovery_ops_active--;
  assert(recovery_ops_active >= 0);

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active oids was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid));
  recovery_oids[pg->info.pgid].erase(soid);
#endif

  if (dequeue)
    recovery_wq._dequeue(pg);
  else {
    recovery_wq._queue_front(pg);
  }

  recovery_wq._wake();
  recovery_wq.unlock();
}

void PGOSD::defer_recovery(PG *pg)
{
  dout(10) << "defer_recovery " << *pg << dendl;

  // move pg to the end of the queue...
  recovery_wq.queue(pg);
}

void PGOSDService::handle_misdirected_op(PG *pg, OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  assert(m->get_header().type == CEPH_MSG_OSD_OP);

  if (m->get_map_epoch() < pg->info.history.same_primary_since) {
    dout(7) << *pg << " changed after " << m->get_map_epoch() << ", dropping" << dendl;
    return;
  }

  dout(7) << *pg << " misdirected op in " << m->get_map_epoch() << dendl;
  clog.warn() << m->get_source_inst() << " misdirected " << m->get_reqid()
	      << " pg " << m->get_pg()
	      << " to osd." << whoami
	      << " not " << pg->acting
	      << " in e" << m->get_map_epoch() << "/" << osdmap->get_epoch() << "\n";
  reply_op_error(op, -ENXIO);
}



bool PGOSD::handle_sub_op_sub(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);

  dout(10) << "handle_sub_op " << *m << " epoch " << m->map_epoch << dendl;
  if (m->map_epoch < up_epoch) {
    dout(3) << "replica op from before up" << dendl;
    return false;
  }

  if (!require_osd_peer(op))
    return false;

  // must be a rep op.
  assert(m->get_source().is_osd());

  // make sure we have the pg
  const pg_t pgid = m->pgid;

  // require same or newer map
  if (!require_same_or_newer_map(op, m->map_epoch))
    return false;

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source(), m->get_connection(), m->map_epoch,
		      static_cast<Session*>(m->get_connection()->get_priv()));

  if (pgosdservice()->splitting(pgid)) {
    waiting_for_pg[pgid].push_back(op);
    return false;
  }

  PG *pg = _have_pg(pgid) ? _lookup_pg(pgid) : NULL;
  if (!pg) {
    return false;
  }
  enqueue_op(pg, op);

  return true;
}

bool PGOSD::handle_sub_op_reply_sub(OpRequestRef op)
{
  MOSDSubOpReply *m = static_cast<MOSDSubOpReply*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOPREPLY);
  if (m->get_map_epoch() < up_epoch) {
    dout(3) << "replica op reply from before up" << dendl;
    return false;
  }

  if (!require_osd_peer(op))
    return false;

  // must be a rep op.
  assert(m->get_source().is_osd());

  // make sure we have the pg
  const pg_t pgid = m->get_pg();

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_map_epoch())) return false;

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source(), m->get_connection(), m->get_map_epoch(),
		      static_cast<Session*>(m->get_connection()->get_priv()));

  PG *pg = _have_pg(pgid) ? _lookup_pg(pgid) : NULL;
  if (!pg) {
    return false;
  }
  enqueue_op(pg, op);
  return true;
}

/*
 * enqueue called with osd_lock held
 */
void PGOSD::enqueue_op(PG *pg, OpRequestRef op)
{
  utime_t latency = ceph_clock_now(g_ceph_context) - op->request->get_recv_stamp();
  dout(15) << "enqueue_op " << op << " prio " << op->request->get_priority()
	   << " cost " << op->request->get_cost()
	   << " latency " << latency
	   << " " << *(op->request) << dendl;
  pg->queue_op(op);
}


void PGOSD::OpWQ::_enqueue(pair<PGRef, OpRequestRef> item)
{
  unsigned priority = item.second->request->get_priority();
  unsigned cost = item.second->request->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict(
      item.second->request->get_source_inst(),
      priority, item);
  else
    pqueue.enqueue(item.second->request->get_source_inst(),
      priority, cost, item);
  osd->logger->set(l_osd_opq, pqueue.length());
}

void PGOSD::OpWQ::_enqueue_front(pair<PGRef, OpRequestRef> item)
{
  {
    Mutex::Locker l(qlock);
    if (pg_for_processing.count(&*(item.first))) {
      pg_for_processing[&*(item.first)].push_front(item.second);
      item.second = pg_for_processing[&*(item.first)].back();
      pg_for_processing[&*(item.first)].pop_back();
    }
  }
  unsigned priority = item.second->request->get_priority();
  unsigned cost = item.second->request->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict_front(
      item.second->request->get_source_inst(),
      priority, item);
  else
    pqueue.enqueue_front(item.second->request->get_source_inst(),
      priority, cost, item);
  osd->logger->set(l_osd_opq, pqueue.length());
}

PGRef PGOSD::OpWQ::_dequeue()
{
  assert(!pqueue.empty());
  PGRef pg;
  {
    Mutex::Locker l(qlock);
    pair<PGRef, OpRequestRef> ret = pqueue.dequeue();
    pg = ret.first;
    pg_for_processing[&*pg].push_back(ret.second);
  }
  osd->logger->set(l_osd_opq, pqueue.length());
  return pg;
}

void PGOSD::OpWQ::_process(PGRef pg)
{
  pg->lock();
  OpRequestRef op;
  {
    Mutex::Locker l(qlock);
    if (!pg_for_processing.count(&*pg)) {
      pg->unlock();
      return;
    }
    assert(pg_for_processing[&*pg].size());
    op = pg_for_processing[&*pg].front();
    pg_for_processing[&*pg].pop_front();
    if (!(pg_for_processing[&*pg].size()))
      pg_for_processing.erase(&*pg);
  }
  osd->dequeue_op(pg, op);
  pg->unlock();
}


void PGOSDService::queue_for_peering(PG *pg)
{
  peering_wq.queue(pg);
}

struct C_CompleteSplits : public Context {
  PGOSD *osd;
  set<boost::intrusive_ptr<PG> > pgs;
  C_CompleteSplits(PGOSD *osd, const set<boost::intrusive_ptr<PG> > &in)
    : osd(osd), pgs(in) {}
  void finish(int r) {
    Mutex::Locker l(osd->osd_lock);
    if (osd->is_stopping())
      return;
    PG::RecoveryCtx rctx = osd->create_context();
    set<pg_t> to_complete;
    for (set<boost::intrusive_ptr<PG> >::iterator i = pgs.begin();
	 i != pgs.end();
	 ++i) {
      (*i)->lock();
      osd->add_newly_split_pg(&**i, &rctx);
      osd->dispatch_context_transaction(rctx, &**i);
      if (!((*i)->deleting))
	to_complete.insert((*i)->info.pgid);
      (*i)->unlock();
    }
    osd->pgosdservice()->complete_split(to_complete);
    osd->dispatch_context(rctx, 0, osd->pgosdservice()->pgosdmap());
  }
};

void PGOSD::process_peering_events(const list<PG*> &pgs,
				   ThreadPool::TPHandle &handle)
{
  bool need_up_thru = false;
  epoch_t same_interval_since = 0;
  PGOSDMapRef curmap = pgosdservice()->pgosdmap();
  PG::RecoveryCtx rctx = create_context();
  for (list<PG*>::const_iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    set<boost::intrusive_ptr<PG> > split_pgs;
    PG *pg = *i;
    pg->lock();
    curmap = pgosdmap();
    if (pg->deleting) {
      pg->unlock();
      continue;
    }
    advance_pg(curmap->get_epoch(), pg, handle, &rctx, &split_pgs);
    if (!pg->peering_queue.empty()) {
      PG::CephPeeringEvtRef evt = pg->peering_queue.front();
      pg->peering_queue.pop_front();
      pg->handle_peering_event(evt, &rctx);
    }
    need_up_thru = pg->need_up_thru || need_up_thru;
    same_interval_since = MAX(pg->info.history.same_interval_since,
			      same_interval_since);
    pg->write_if_dirty(*rctx.transaction);
    if (!split_pgs.empty()) {
      rctx.on_applied->add(new C_CompleteSplits(this, split_pgs));
      split_pgs.clear();
    }
    if (compat_must_dispatch_immediately(pg)) {
      dispatch_context(rctx, pg, curmap);
      rctx = create_context();
    } else {
      dispatch_context_transaction(rctx, pg);
    }
    pg->unlock();
    handle.reset_tp_timeout();
  }
  if (need_up_thru)
    queue_want_up_thru(same_interval_since);
  dispatch_context(rctx, 0, curmap);

  pgosdservice()->send_pg_temp();
}

void PGOSDService::dequeue_pg(PG *pg, list<OpRequestRef> *dequeued)
{
  pgosd()->op_wq.dequeue(pg, dequeued);
}


/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void PGOSD::dequeue_op(PGRef pg, OpRequestRef op)
{
  utime_t latency = ceph_clock_now(g_ceph_context) - op->request->get_recv_stamp();
  dout(10) << "dequeue_op " << op << " prio " << op->request->get_priority()
	   << " cost " << op->request->get_cost()
	   << " latency " << latency
	   << " " << *(op->request)
	   << " pg " << *pg << dendl;
  if (pg->deleting)
    return;

  op->mark_reached_pg();

  pg->do_request(op);

  // finish
  dout(10) << "dequeue_op " << op << " finish" << dendl;
}


void PGOSD::handle_op_sub(OpRequestRef op)
{
  MOSDOp *m = (MOSDOp*)op->request;

  // calc actual pgid
  pg_t pgid = m->get_pg();
  int64_t pool = pgid.pool();
  if ((m->get_flags() & CEPH_OSD_FLAG_PGOP) == 0 &&
      pgosdmap()->have_pg_pool(pool))
    pgid = pgosdmap()->raw_pg_to_pg(pgid);

  // get and lock *pg.
  PG *pg = _have_pg(pgid) ? _lookup_pg(pgid) : NULL;
  if (!pg) {
    dout(7) << "hit non-existent pg " << pgid << dendl;

    if (pgosdmap()->get_pg_acting_role(pgid, whoami) >= 0) {
      dout(7) << "we are valid target for op, waiting" << dendl;
      waiting_for_pg[pgid].push_back(op);
      op->mark_delayed("waiting for pg to exist locally");
      return;
    }

    // okay, we aren't valid now; check send epoch
    if (m->get_map_epoch() < superblock.oldest_map) {
      dout(7) << "don't have sender's osdmap; assuming it was valid and that client will resend" << dendl;
      return;
    }
    PGOSDMapRef send_map = get_map(m->get_map_epoch());

    if (send_map->get_pg_acting_role(pgid, whoami) >= 0) {
      dout(7) << "dropping request; client will resend when they get new map" << dendl;
    } else if (!send_map->have_pg_pool(pgid.pool())) {
      dout(7) << "dropping request; pool did not exist" << dendl;
      clog.warn() << m->get_source_inst() << " invalid " << m->get_reqid()
		  << " pg " << m->get_pg()
		  << " to osd." << whoami
		  << " in e" << osdmap->get_epoch()
		  << ", client e" << m->get_map_epoch()
		  << " when pool " << m->get_pg().pool() << " did not exist"
		  << "\n";
    } else {
      dout(7) << "we are invalid target" << dendl;
      pgid = m->get_pg();
      if ((m->get_flags() & CEPH_OSD_FLAG_PGOP) == 0)
	pgid = send_map->raw_pg_to_pg(pgid);
      clog.warn() << m->get_source_inst() << " misdirected " << m->get_reqid()
		  << " pg " << m->get_pg()
		  << " to osd." << whoami
		  << " in e" << osdmap->get_epoch()
		  << ", client e" << m->get_map_epoch()
		  << " pg " << pgid
		  << " features " << m->get_connection()->get_features()
		  << "\n";
      service->reply_op_error(op, -ENXIO);
    }
    return;
  }

  enqueue_op(pg, op);
  pg->unlock();
}

bool PGOSD::asok_command_sub(string command, string args, ostream& ss)
{
  if (command == "dump_op_pq_state") {
    JSONFormatter f(true);
    f.open_object_section("pq");
    op_wq.dump(&f);
    f.close_section();
    f.flush(ss);
  } else {
    return false;
  }

  return true;
}

bool PGOSDService::test_ops_sub(ObjectStore *store,
				std::string command,
				std::string args,
				ostream &ss)
{
  if (command == "setomapval" || command == "rmomapkey" ||
      command == "setomapheader" || command == "getomap" ||
      command == "truncobj" || command == "injectmdataerr" ||
      command == "injectdataerr"
    ) {
    std::vector<std::string> argv;
    pg_t rawpg, pgid;
    int64_t pool;
    PGOSDMapRef curmap = pgosdmap();
    int r;

    argv.push_back(command);
    string_to_vec(argv, args);
    int argc = argv.size();

    if (argc < 3) {
      ss << "Illegal request";
      return false;
    }

    pool = curmap->const_lookup_pg_pool_name(argv[1].c_str());
    //If we can't find it my name then maybe id specified
    if (pool < 0 && isdigit(argv[1].c_str()[0]))
      pool = atoll(argv[1].c_str());
    r = -1;
    if (pool >= 0)
        r = curmap->object_locator_to_pg(object_t(argv[2]),
          object_locator_t(pool), rawpg);
    if (r < 0) {
        ss << "Invalid pool " << argv[1];
        return false;
    }
    pgid = curmap->raw_pg_to_pg(rawpg);

    hobject_t obj(object_t(argv[2]), string(""), CEPH_NOSNAP, rawpg.ps(), pool);
    ObjectStore::Transaction t;

    if (command == "setomapval") {
      if (argc != 5) {
        ss << "usage: setomapval <pool> <obj-name> <key> <val>";
        return false;
      }
      map<string, bufferlist> newattrs;
      bufferlist val;
      string key(argv[3]);
 
      val.append(argv[4]);
      newattrs[key] = val;
      t.omap_setkeys(coll_t(pgid), obj, newattrs);
      r = store->apply_transaction(t);
      if (r < 0)
        ss << "error=" << r;
      else
        ss << "ok";
    } else if (command == "rmomapkey") {
      if (argc != 4) {
        ss << "usage: rmomapkey <pool> <obj-name> <key>";
        return false;
      }
      set<string> keys;

      keys.insert(string(argv[3]));
      t.omap_rmkeys(coll_t(pgid), obj, keys);
      r = store->apply_transaction(t);
      if (r < 0)
        ss << "error=" << r;
      else
        ss << "ok";
    } else if (command == "setomapheader") {
      if (argc != 4) {
        ss << "usage: setomapheader <pool> <obj-name> <header>";
        return false;
      }
      bufferlist newheader;

      newheader.append(argv[3]);
      t.omap_setheader(coll_t(pgid), obj, newheader);
      r = store->apply_transaction(t);
      if (r < 0)
        ss << "error=" << r;
      else
        ss << "ok";
    } else if (command == "getomap") {
      if (argc != 3) {
        ss << "usage: getomap <pool> <obj-name>";
        return false;
      }
      //Debug: Output entire omap
      bufferlist hdrbl;
      map<string, bufferlist> keyvals;
      r = store->omap_get(coll_t(pgid), obj, &hdrbl, &keyvals);
      if (r >= 0) {
          ss << "header=" << string(hdrbl.c_str(), hdrbl.length());
          for (map<string, bufferlist>::iterator it = keyvals.begin();
              it != keyvals.end(); ++it)
            ss << " key=" << (*it).first << " val="
               << string((*it).second.c_str(), (*it).second.length());
      } else {
          ss << "error=" << r;
      }
    } else if (command == "truncobj") {
      if (argc != 4) {
	ss << "usage: truncobj <pool> <obj-name> <val>";
	return false;
      }
      t.truncate(coll_t(pgid), obj, atoi(argv[3].c_str()));
      r = store->apply_transaction(t);
      if (r < 0)
	ss << "error=" << r;
      else
	ss << "ok";
    } else if (command == "injectdataerr") {
      store->inject_data_error(obj);
      ss << "ok";
    } else if (command == "injectmdataerr") {
      store->inject_mdata_error(obj);
      ss << "ok";
    }
    return true;
  }
  return false;
}

void PGOSD::handle_conf_change(const struct md_config_t *conf,
			       const std::set <std::string> &changed)
{
  OSD::handle_conf_change(conf, changed);
  if (changed.count("osd_max_backfills")) {
    pgosdservice()->local_reserver.set_max(g_conf->osd_max_backfills);
    pgosdservice()->remote_reserver.set_max(g_conf->osd_max_backfills);
  }
}
