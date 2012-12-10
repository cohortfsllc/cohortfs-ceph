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

#include "PGOSD.h"
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

#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"

#include "ReplicatedPG.h"
#include "common/errno.h"
#include "common/perf_counters.h"


#define dout_subsys ceph_subsys_osd


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
  pg_temp_lock("OSDService::pg_temp_lock")
{
  // empty
}

PGOSD* PGOSDService::get_pgosd() const {
  return dynamic_cast<PGOSD*>(osd);
}

void PGOSDService::pg_stat_queue_enqueue(PG *pg)
{
  get_pgosd()->pg_stat_queue_enqueue(pg);
}

void PGOSDService::pg_stat_queue_dequeue(PG *pg)
{
  get_pgosd()->pg_stat_queue_dequeue(pg);
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
  dout(10) << "send_pg_temp " << pg_temp_wanted << dendl;
  MOSDPGTemp *m = new MOSDPGTemp(osdmap->get_epoch());
  m->pg_temp = pg_temp_wanted;
  monc->send_mon_message(m);
}


// OSD

PGOSD::PGOSD(int id, Messenger *internal_messenger, Messenger *external_messenger,
             Messenger *hbclientm, Messenger *hbserverm, MonClient *mc,
             const std::string &dev, const std::string &jdev,
             OSDService* osdSvc) :
  OSD(id, internal_messenger, external_messenger, hbclientm, hbserverm,
      mc, dev, jdev, osdSvc),
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
  replay_queue_lock("OSD::replay_queue_lock"),
  snap_trim_wq(this, g_conf->osd_snap_trim_thread_timeout, &disk_tp),
  scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  scrub_finalize_wq(this, g_conf->osd_scrub_finalize_thread_timeout, &op_tp),
  recovery_wq(this, g_conf->osd_recovery_thread_timeout, &recovery_tp),
  rep_scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  next_removal_seq(0)
{
  // empty
}


int PGOSD::init() {
  const int result = init_super();

  // load up pgs (as they previously existed)
  load_pgs();

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

  // close pgs
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    PG *pg = p->second;
    pg->put();
  }
  pg_map.clear();


  // finish ops
  op_wq.drain();
  dout(10) << "no ops" << dendl;


  return shutdown_super();
}


// ======================================================
// PG's

PGPool PGOSD::_get_pool(int id, OSDMapRef createmap_p)
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

PG *PGOSD::_open_lock_pg(OSDMapRef createmap,
			 pg_t pgid, bool no_lockdep_check, bool hold_map_lock)
{
  assert(osd_lock.is_locked());

  dout(10) << "_open_lock_pg " << pgid << dendl;
  PGPool pool = _get_pool(pgid.pool(), createmap);

  // create
  PG *pg;
  hobject_t logoid = make_pg_log_oid(pgid);
  hobject_t infooid = make_pg_biginfo_oid(pgid);
  if (get_pgosdmap()->get_pg_type(pgid) == pg_pool_t::TYPE_REP)
    pg = new ReplicatedPG(serviceRef, createmap, pool, pgid, logoid, infooid);
  else 
    assert(0);

  assert(pg_map.count(pgid) == 0);
  pg_map[pgid] = pg;

  if (hold_map_lock)
    pg->lock_with_map_lock_held(no_lockdep_check);
  else
    pg->lock(no_lockdep_check);
  pg->get();  // because it's in pg_map
  return pg;
}


PG *PGOSD::_create_lock_pg(OSDMapRef createmap,
			   pg_t pgid, bool newly_created, bool hold_map_lock,
			   int role, vector<int>& up, vector<int>& acting,
			   pg_history_t history,
			   pg_interval_map_t& pi,
			   ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());
  dout(20) << "_create_lock_pg pgid " << pgid << dendl;

  PG *pg = _open_lock_pg(createmap, pgid, true, hold_map_lock);

  t.create_collection(coll_t(pgid));

  if (newly_created) {
    /* This is weird, but all the peering code needs last_epoch_start
     * to be less than same_interval_since. Make it so!
     * This is easier to deal with if you remember that the PG, while
     * now created in memory, still hasn't peered and started -- and
     * the map epoch could change before that happens! */
    history.last_epoch_started = history.epoch_created - 1;
  }

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

PG *PGOSD::_lookup_lock_pg_with_map_lock_held(pg_t pgid)
{
  assert(osd_lock.is_locked());
  assert(pg_map.count(pgid));
  PG *pg = pg_map[pgid];
  pg->lock_with_map_lock_held();
  return pg;
}

PG *PGOSD::lookup_lock_raw_pg(pg_t pgid)
{
  Mutex::Locker l(osd_lock);
  if (get_pgosdmap()->have_pg_pool(pgid.pool())) {
    pgid = get_pgosdmap()->raw_pg_to_pg(pgid);
  }
  if (!_have_pg(pgid)) {
    return NULL;
  }
  PG *pg = _lookup_lock_pg(pgid);
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

  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    pg_t pgid;
    snapid_t snap;
    if (!it->is_pg(pgid, snap)) {
      if (it->is_temp(pgid))
	clear_temp(store, *it);
      dout(10) << "load_pgs skipping non-pg " << *it << dendl;
      if (it->is_temp(pgid)) {
	clear_temp(store, *it);
	continue;
      }
      uint64_t seq;
      if (it->is_removal(&seq, &pgid)) {
	if (seq >= next_removal_seq)
	  next_removal_seq = seq + 1;
	dout(10) << "queueing coll " << *it << " for removal, seq is "
		 << seq << "pgid is " << pgid << dendl;
	boost::tuple<coll_t, SequencerRef, DeletingStateRef> *to_queue =
	  new boost::tuple<coll_t, SequencerRef, DeletingStateRef>;
	to_queue->get<0>() = *it;
	to_queue->get<1>() =
	  get_pgosdservice()->osr_registry.lookup_or_create(pgid,
							    stringify(pgid));
	to_queue->get<2>() = get_pgosdservice()->deleting_pgs.lookup_or_create(pgid);
	remove_wq.queue(to_queue);
	continue;
      }
      continue;
    }
    if (snap != CEPH_NOSNAP) {
      dout(10) << "load_pgs skipping snapped dir " << *it
	       << " (pg " << pgid << " snap " << snap << ")" << dendl;
      continue;
    }

    if (!get_pgosdmap()->have_pg_pool(pgid.pool())) {
      dout(10) << __func__ << ": skipping PG " << pgid << " because we don't have pool "
	       << pgid.pool() << dendl;
      continue;
    }

    if (pgid.preferred() >= 0) {
      dout(10) << __func__ << ": skipping localized PG " << pgid << dendl;
      // FIXME: delete it too, eventually
      continue;
    }

    PG *pg = _open_lock_pg(osdmap, pgid);

    // read pg state, log
    pg->read_state(store);

    get_pgosdservice()->reg_last_pg_scrub(pg->info.pgid,
					  pg->info.history.last_scrub_stamp);

    // generate state for current mapping
    get_pgosdmap()->pg_to_up_acting_osds(pgid, pg->up, pg->acting);
    int role = get_pgosdmap()->calc_pg_role(whoami, pg->acting);
    pg->set_role(role);

    PG::RecoveryCtx rctx(0, 0, 0, 0, 0, 0);
    pg->handle_loaded(&rctx);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->log << dendl;
    pg->unlock();
  }
  dout(10) << "load_pgs done" << dendl;
}
 

/*
 * look up a pg.  if we have it, great.  if not, consider creating it IF the pg mapping
 * hasn't changed since the given epoch and we are the primary.
 */
PG *PGOSD::get_or_create_pg(const pg_info_t& info, pg_interval_map_t& pi,
			    epoch_t epoch, int from, int& created, bool primary)
{
  PG *pg;

  if (!_have_pg(info.pgid)) {
    // same primary?
    vector<int> up, acting;
    get_pgosdmap()->pg_to_up_acting_osds(info.pgid, up, acting);
    int role = get_pgosdmap()->calc_pg_role(whoami, acting, acting.size());

    pg_history_t history = info.history;
    project_pg_history(info.pgid, history, epoch, up, acting);

    if (epoch < history.same_interval_since) {
      dout(10) << "get_or_create_pg " << info.pgid << " acting changed in "
	       << history.same_interval_since << " (msg from " << epoch << ")" << dendl;
      return NULL;
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
			 get_map(epoch),
			 info.pgid, create, false, role, up, acting, history, pi,
			 *rctx.transaction);
    pg->handle_create(&rctx);
    pg->write_if_dirty(*rctx.transaction);
    dispatch_context(rctx, pg, osdmap);
      
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
    OSDMapRef oldmap = get_map(e);
    vector<int> acting;
    get_pgosdmap(oldmap)->pg_to_acting_osds(pgid, acting);
    dout(20) << "  " << pgid << " in epoch " << e << " was " << acting << dendl;
    int up = 0;
    for (unsigned i=0; i<acting.size(); i++)
      if (osdmap->is_up(acting[i])) {
	if (acting[i] != whoami)
	  pset.insert(acting[i]);
	up++;
      }
    if (!up && acting.size()) {
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
    OSDMapRef oldmap = get_map(e-1);

    vector<int> up, acting;
    get_pgosdmap(oldmap)->pg_to_up_acting_osds(pgid, up, acting);

    // acting set change?
    if ((acting != currentacting || up != currentup) && e > h.same_interval_since) {
      dout(15) << "project_pg_history " << pgid << " acting|up changed in " << e 
	       << " from " << acting << "/" << up
	       << " -> " << currentacting << "/" << currentup
	       << dendl;
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


void PGOSD::build_heartbeat_peers_list() {
  // build heartbeat from set
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
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

  dout(20) << "sched_scrub" << dendl;

  utime_t max = ceph_clock_now(g_ceph_context);
  max -= g_conf->osd_scrub_max_interval;
  
  //dout(20) << " " << last_scrub_pg << dendl;

  pair<utime_t, pg_t> pos;
  while (get_pgosdservice()->next_scrub_stamp(pos, &pos)) {
    utime_t t = pos.first;
    pg_t pgid = pos.second;

    if (t > max) {
      dout(10) << " " << pgid << " at " << t
	       << " > " << max << " (" << g_conf->osd_scrub_max_interval << " seconds ago)" << dendl;
      break;
    }

    dout(10) << " on " << t << " " << pgid << dendl;
    PG *pg = _lookup_lock_pg(pgid);
    if (pg) {
      if (pg->is_active() && !pg->sched_scrub()) {
	pg->unlock();
	break;
      }
      pg->unlock();
    }
  }    
  dout(20) << "sched_scrub done" << dendl;
}


void PGOSD::tick_sub(const utime_t& now) {
  if (outstanding_pg_stats
      && (now - g_conf->osd_mon_ack_timeout) > last_pg_stats_ack) {
    dout(1) << "mon hasn't acked PGStats in " << now - last_pg_stats_ack
            << " seconds, reconnecting elsewhere" << dendl;
    monc->reopen_session();
    last_pg_stats_ack = ceph_clock_now(g_ceph_context);  // reset clock
  }
}


void PGOSD::do_mon_report_sub(const utime_t& now) {
  get_pgosdservice()->send_pg_temp();
  send_pg_stats(now);
}


void PGOSD::ms_handle_connect_sub(Connection *con) {
  get_pgosdservice()->send_pg_temp();
  send_pg_stats(ceph_clock_now(g_ceph_context));
    
  monc->sub_want("osd_pg_creates", 0, CEPH_SUBSCRIBE_ONETIME);
}


void PGOSD::put_object_context(void *_obc, pg_t pgid)
{
  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)_obc;
  ReplicatedPG *pg = (ReplicatedPG *)lookup_lock_raw_pg(pgid);
  // If pg is being deleted, (which is the only case in which
  // it will be NULL) it will clean up its object contexts itself
  if (pg) {
    pg->put_object_context(obc);
    pg->unlock();
  }
}


void PGOSD::ack_notification(entity_name_t& name,
			     void *_notif,
			     void *_obc,
			     ReplicatedPG *pg)
{
  assert(serviceRef->watch_lock.is_locked());
  pg->assert_locked();
  Watch::Notification *notif = (Watch::Notification *)_notif;
  if (serviceRef->watch->ack_notification(name, notif)) {
    complete_notify(notif, _obc);
    pg->put_object_context(static_cast<ReplicatedPG::ObjectContext *>(_obc));
  }
}


void PGOSD::handle_watch_timeout(void *obc,
				 ReplicatedPG *pg,
				 entity_name_t entity,
				 utime_t expire)
{
  // watch_lock is inside pg->lock; handle_watch_timeout checks for the race.
  serviceRef->watch_lock.Unlock();
  pg->lock();
  serviceRef->watch_lock.Lock();

  pg->handle_watch_timeout(obc, entity, expire);
  pg->unlock();
  pg->put();
}


void PGOSD::ms_handle_reset_sub(PGSession* session) {
  disconnect_session_watches(session);
}


void PGOSD::disconnect_session_watches(PGSession *session)
{
  // get any watched obc's
  map<ReplicatedPG::ObjectContext *, pg_t> obcs;
  serviceRef->watch_lock.Lock();
  for (map<void *, pg_t>::iterator iter = session->watches.begin();
       iter != session->watches.end();
       ++iter) {
    ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)iter->first;
    obcs[obc] = iter->second;
  }
  serviceRef->watch_lock.Unlock();

  for (map<ReplicatedPG::ObjectContext *, pg_t>::iterator oiter = obcs.begin();
       oiter != obcs.end();
       ++oiter) {
    ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)oiter->first;
    dout(10) << "obc=" << (void *)obc << dendl;

    ReplicatedPG *pg = static_cast<ReplicatedPG *>(lookup_lock_raw_pg(oiter->second));
    assert(pg);
    serviceRef->watch_lock.Lock();
    /* NOTE! fix this one, should be able to just lookup entity name,
       however, we currently only keep EntityName on the session and not
       entity_name_t. */
    map<entity_name_t, PGSession *>::iterator witer = obc->watchers.begin();
    while (1) {
      while (witer != obc->watchers.end() && witer->second == session) {
        dout(10) << "removing watching session entity_name=" << session->entity_name
		<< " from " << obc->obs.oi << dendl;
	entity_name_t entity = witer->first;
	watch_info_t& w = obc->obs.oi.watchers[entity];
	utime_t expire = ceph_clock_now(g_ceph_context);
	expire += w.timeout_seconds;
	pg->register_unconnected_watcher(obc, entity, expire);
	dout(10) << " disconnected watch " << w << " by " << entity << " session " << session
		 << ", expires " << expire << dendl;
        obc->watchers.erase(witer++);
	pg->put_object_context(obc);
	session->put();
      }
      if (witer == obc->watchers.end())
        break;
      ++witer;
    }
    serviceRef->watch_lock.Unlock();
    pg->unlock();
  }
}


void PGOSD::handle_notify_timeout(void *_notif)
{
  assert(serviceRef->watch_lock.is_locked());
  Watch::Notification *notif = (Watch::Notification *)_notif;
  dout(10) << "OSD::handle_notify_timeout notif " << notif->id << dendl;

  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)notif->obc;

  complete_notify(_notif, obc);
  serviceRef->watch_lock.Unlock(); /* drop lock to change locking order */

  put_object_context(obc, notif->pgid);
  serviceRef->watch_lock.Lock();
  /* exiting with watch_lock held */
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
    last_stats_sent = now;
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
	pg->put();
	continue;
      }
      pg->pg_stats_lock.Lock();
      if (pg->pg_stats_valid) {
	m->pg_stat[pg->info.pgid] = pg->pg_stats_stable;
	dout(25) << " sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
      } else {
	dout(25) << " NOT sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << ", not valid" << dendl;
      }
      pg->pg_stats_lock.Unlock();
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
    ++p;

    if (ack->pg_stat.count(pg->info.pgid)) {
      eversion_t acked = ack->pg_stat[pg->info.pgid];
      pg->pg_stats_lock.Lock();
      if (acked == pg->pg_stats_stable.reported) {
	dout(25) << " ack on " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
	pg->stat_queue_item.remove_myself();
	pg->put();
      } else {
	dout(25) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported
		 << " > acked " << acked << dendl;
      }
      pg->pg_stats_lock.Unlock();
    } else
      dout(30) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
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

  map<int64_t, int> pool_resize;  // poolid -> old size

  // scan pg creations
  hash_map<pg_t, create_pg_info>::iterator n = creating_pgs.begin();
  while (n != creating_pgs.end()) {
    hash_map<pg_t, create_pg_info>::iterator p = n++;
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = get_pgosdmap()->pg_to_acting_osds(pgid, acting);
    int role = get_pgosdmap()->calc_pg_role(whoami, acting, nrep);
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
    int nrep = get_pgosdmap()->pg_to_acting_osds(pgid, acting);
    int role = get_pgosdmap()->calc_pg_role(whoami, acting, nrep);
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
}


void PGOSD::activate_map_sub()
{
  map< int, vector<pair<pg_notify_t,pg_interval_map_t> > >  notify_list;  // primary -> list
  map< int, map<pg_t,pg_query_t> > query_map;    // peer -> PG -> get_summary_since
  map<int,MOSDPGInfo*> info_map;  // peer -> message

  int num_pg_primary = 0, num_pg_replica = 0, num_pg_stray = 0;

  epoch_t oldest_last_clean = osdmap->get_epoch();

  list<PG*> to_remove;

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock();
    if (pg->is_primary())
      num_pg_primary++;
    else if (pg->is_replica())
      num_pg_replica++;
    else
      num_pg_stray++;

    if (pg->is_primary() && pg->info.history.last_epoch_clean < oldest_last_clean)
      oldest_last_clean = pg->info.history.last_epoch_clean;

    if (!get_pgosdmap()->have_pg_pool(pg->info.pgid.pool())) {
      //pool is deleted!
      pg->get();
      to_remove.push_back(pg);
    }
    pg->unlock();
  }

  for (list<PG*>::iterator i = to_remove.begin();
       i != to_remove.end();
       ++i) {
    (*i)->lock();
    _remove_pg((*i));
    (*i)->unlock();
    (*i)->put();
  }
  to_remove.clear();

  serviceRef->publish_map(osdmap);

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock();
    pg->queue_null(osdmap->get_epoch(), osdmap->get_epoch());
    pg->unlock();
  }

  
  logger->set(l_osd_pg, pg_map.size());
  logger->set(l_osd_pg_primary, num_pg_primary);
  logger->set(l_osd_pg_replica, num_pg_replica);
  logger->set(l_osd_pg_stray, num_pg_stray);

  wake_all_pg_waiters();   // the pg mapping may have shifted
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
  } // switch
}


bool PGOSD::_dispatch_sub(Message *m)
{
  switch (m->get_type()) {

  case MSG_PGSTATSACK:
    handle_pg_stats_ack((MPGStatsAck*)m);
    break;

  case MSG_OSD_SCRUB:
    handle_scrub((MOSDScrub*)m);
    break;    

  case MSG_OSD_REP_SCRUB:
    handle_rep_scrub((MOSDRepScrub*)m);
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
	 p++) {
      PG *pg = p->second;
      pg->lock();
      if (pg->is_primary()) {
	if (m->repair)
	  pg->state_set(PG_STATE_REPAIR);
	if (pg->queue_scrub()) {
	  dout(10) << "queueing " << *pg << " for scrub" << dendl;
	}
      }
      pg->unlock();
    }
  } else {
    for (vector<pg_t>::iterator p = m->scrub_pgs.begin();
	 p != m->scrub_pgs.end();
	 p++)
      if (pg_map.count(*p)) {
	PG *pg = pg_map[*p];
	pg->lock();
	if (pg->is_primary()) {
	  if (m->repair)
	    pg->state_set(PG_STATE_REPAIR);
	  if (pg->queue_scrub()) {
	    dout(10) << "queueing " << *pg << " for scrub" << dendl;
	  }
	}
	pg->unlock();
      }
  }
  
  m->put();
}


void PGOSD::advance_pg(epoch_t osd_epoch, PG *pg, PG::RecoveryCtx *rctx)
{
  assert(pg->is_locked());
  epoch_t next_epoch = pg->get_osdmap()->get_epoch() + 1;
  OSDMapRef lastmap = pg->get_osdmap();

  if (lastmap->get_epoch() == osd_epoch)
    return;
  assert(lastmap->get_epoch() < osd_epoch);

  for (;
       next_epoch <= osd_epoch;
       ++next_epoch) {
    OSDMapRef nextmap = get_map(next_epoch);
    vector<int> newup, newacting;
    get_pgosdmap(nextmap)->pg_to_up_acting_osds(pg->info.pgid,
						newup,
						newacting);
    pg->handle_advance_map(nextmap, lastmap, newup, newacting, rctx);
    lastmap = nextmap;
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

  if (creating_pgs[pgid].split_bits) {
    dout(10) << "can_create_pg " << pgid << " - split" << dendl;
    return false;
  }

  dout(10) << "can_create_pg " << pgid << " - can create now" << dendl;
  return true;
}

void PGOSD::do_split(PG *parent,
		     set<pg_t>& childpgids,
		     ObjectStore::Transaction& t,
		     C_Contexts *tfin)
{
  dout(10) << "do_split to " << childpgids << " on " << *parent << dendl;

  parent->lock_with_map_lock_held();
 
  // create and lock children
  map<pg_t,PG*> children;
  for (set<pg_t>::iterator q = childpgids.begin();
       q != childpgids.end();
       q++) {
    pg_history_t history;
    history.epoch_created = history.same_up_since =
      history.same_interval_since = history.same_primary_since =
      osdmap->get_epoch();
    pg_interval_map_t pi;
    PG *pg = _create_lock_pg(serviceRef->get_osdmap(), *q, true, true,
			     parent->get_role(), parent->up, parent->acting, history, pi, t);
    children[*q] = pg;
    dout(10) << "  child " << *pg << dendl;
  }

  split_pg(parent, children, t); 

#if 0
  // reset pg
  map< int, vector<pair<pg_notify_t, pg_interval_map_t> > >  notify_list;  // primary -> list
  map< int, map<pg_t,pg_query_t> > query_map;    // peer -> PG -> get_summary_since
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > info_map;  // peer -> message
  PG::RecoveryCtx rctx(&query_map, &info_map, &notify_list, &tfin->contexts, &t);

  // FIXME: this breaks if we have a map discontinuity
  //parent->handle_split(osdmap, get_map(osdmap->get_epoch() - 1), &rctx);

  // unlock parent, children
  parent->unlock();

  for (map<pg_t,PG*>::iterator q = children.begin(); q != children.end(); q++) {
    PG *pg = q->second;
    pg->handle_create(&rctx);
    pg->write_if_dirty(t);
    wake_pg_waiters(pg->info.pgid);
    pg->unlock();
  }

  do_notifies(notify_list);
  do_queries(query_map);
  do_infos(info_map);
#endif
}

void PGOSD::split_pg(PG *parent,
		     map<pg_t,PG*>& children,
		     ObjectStore::Transaction &t)
{
  dout(10) << "split_pg " << *parent << dendl;
  pg_t parentid = parent->info.pgid;

  // split objects
  vector<hobject_t> olist;
  store->collection_list(coll_t(parent->info.pgid), olist);

  for (vector<hobject_t>::iterator p = olist.begin(); p != olist.end(); p++) {
    hobject_t poid = *p;
    object_locator_t oloc(parentid.pool());
    if (poid.get_key().size())
      oloc.key = poid.get_key();
    pg_t rawpg = get_pgosdmap()->object_locator_to_pg(poid.oid, oloc);
    pg_t pgid = get_pgosdmap()->raw_pg_to_pg(rawpg);
    if (pgid != parentid) {
      dout(20) << "  moving " << poid << " from " << parentid
	       << " -> " << pgid << dendl;
      PG *child = children[pgid];
      assert(child);
      bufferlist bv;

      struct stat st;
      store->stat(coll_t(parentid), poid, &st);
      store->getattr(coll_t(parentid), poid, OI_ATTR, bv);
      object_info_t oi(bv);

      t.collection_move(coll_t(pgid), coll_t(parentid), poid);
      if (oi.snaps.size()) {
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
    p++;
    hobject_t& poid = cur->soid;
    object_locator_t oloc(parentid.pool());
    if (poid.get_key().size())
      oloc.key = poid.get_key();
    pg_t rawpg = get_pgosdmap()->object_locator_to_pg(poid.oid, oloc);
    pg_t pgid = get_pgosdmap()->raw_pg_to_pg(rawpg);
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
       p++) {
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
    get_pgosdmap()->pg_to_up_acting_osds(on, up, acting);
    int role = get_pgosdmap()->calc_pg_role(whoami, acting, acting.size());

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
	osdmap, pgid, true, false,
	0, creating_pgs[pgid].acting, creating_pgs[pgid].acting,
	history, pi,
	*rctx.transaction);
      creating_pgs.erase(pgid);
      wake_pg_waiters(pg->info.pgid);
      pg->handle_create(&rctx);
      pg->write_if_dirty(*rctx.transaction);
      pg->update_stats();
      pg->unlock();
      num_created++;
    }
    dispatch_context(rctx, pg, osdmap);
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

void PGOSD::dispatch_context(PG::RecoveryCtx &ctx, PG *pg, OSDMapRef curmap)
{
  do_notifies(*ctx.notify_list, curmap);
  delete ctx.notify_list;
  do_queries(*ctx.query_map, curmap);
  delete ctx.query_map;
  do_infos(*ctx.info_map, curmap);
  delete ctx.info_map;
  if (ctx.transaction->empty() || !pg) {
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
  OSDMapRef curmap)
{
  for (map< int, vector<pair<pg_notify_t,pg_interval_map_t> > >::iterator it = notify_list.begin();
       it != notify_list.end();
       it++) {
    if (it->first == whoami) {
      dout(7) << "do_notify osd." << it->first << " is self, skipping" << dendl;
      continue;
    }
    if (!curmap->is_up(it->first))
      continue;
    Connection *con =
      cluster_messenger->get_connection(curmap->get_cluster_inst(it->first));
    _share_map_outgoing(curmap->get_cluster_inst(it->first), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_notify osd." << it->first
	      << " on " << it->second.size() << " PGs" << dendl;
      MOSDPGNotify *m = new MOSDPGNotify(curmap->get_epoch(),
					 it->second);
      cluster_messenger->send_message(m, curmap->get_cluster_inst(it->first));
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
	cluster_messenger->send_message(m, curmap->get_cluster_inst(it->first));
      }
    }
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void PGOSD::do_queries(map< int, map<pg_t,pg_query_t> >& query_map,
		     OSDMapRef curmap)
{
  for (map< int, map<pg_t,pg_query_t> >::iterator pit = query_map.begin();
       pit != query_map.end();
       pit++) {
    if (!curmap->is_up(pit->first))
      continue;
    int who = pit->first;
    Connection *con =
      cluster_messenger->get_connection(curmap->get_cluster_inst(pit->first));
    _share_map_outgoing(curmap->get_cluster_inst(who), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_queries querying osd." << who
	      << " on " << pit->second.size() << " PGs" << dendl;
      MOSDPGQuery *m = new MOSDPGQuery(curmap->get_epoch(), pit->second);
      cluster_messenger->send_message(m, curmap->get_cluster_inst(who));
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
	cluster_messenger->send_message(m, curmap->get_cluster_inst(who));
      }
    }
  }
}


void PGOSD::do_infos(map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		   OSDMapRef curmap)
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
    Connection *con =
      cluster_messenger->get_connection(curmap->get_cluster_inst(p->first));
    _share_map_outgoing(curmap->get_cluster_inst(p->first), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      MOSDPGInfo *m = new MOSDPGInfo(curmap->get_epoch());
      m->pg_list = p->second;
      cluster_messenger->send_message(m, curmap->get_cluster_inst(p->first));
    } else {
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     p->second.begin();
	   i != p->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > to_send(1);
	to_send[0] = *i;
	MOSDPGInfo *m = new MOSDPGInfo(i->first.epoch_sent);
	m->pg_list = to_send;
	cluster_messenger->send_message(m, curmap->get_cluster_inst(p->first));
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
       it++) {
    PG *pg = 0;

    if (it->first.info.pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << it->first.info.pgid << dendl;
      continue;
    }

    int created = 0;
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
  MOSDPGInfo *m = (MOSDPGInfo *)op->request;
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
      pg->write_info(*t);
      int tr = store->queue_transaction(pg->osr.get(), t,
					new ObjectStore::C_DeleteTransaction(t));
      assert(tr == 0);
    }
    pg->unlock();
  }
}

void PGOSD::handle_pg_scan(OpRequestRef op)
{
  MOSDPGScan *m = (MOSDPGScan*)op->request;
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

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
  pg->unlock();
}

void PGOSD::handle_pg_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = (MOSDPGBackfill*)op->request;
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

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
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
       it++) {
    pg_t pgid = it->first;

    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    PG *pg = 0;

    if (pg_map.count(pgid)) {
      pg = _lookup_lock_pg(pgid);
      pg->queue_query(it->second.epoch_sent, it->second.epoch_sent,
		      from, it->second);
      pg->unlock();
      continue;
    }

    // get active crush mapping
    vector<int> up, acting;
    get_pgosdmap()->pg_to_up_acting_osds(pgid, up, acting);
    int role = get_pgosdmap()->calc_pg_role(whoami, acting, acting.size());

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
      MOSDPGLog *mlog = new MOSDPGLog(osdmap->get_epoch(), empty,
				      it->second.epoch_sent);
      _share_map_outgoing(osdmap->get_cluster_inst(from));
      cluster_messenger->send_message(mlog,
				      osdmap->get_cluster_inst(from));
    } else {
      notify_list[from].push_back(make_pair(pg_notify_t(it->second.epoch_sent,
							osdmap->get_epoch(),
							empty),
					    pg_interval_map_t()));
    }
  }
  do_notifies(notify_list, osdmap);
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
       it++) {
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
    get_pgosdmap()->pg_to_up_acting_osds(pgid, up, acting);
    project_pg_history(pg->info.pgid, history, pg->get_osdmap()->get_epoch(),
		       up, acting);
    if (history.same_interval_since <= m->get_epoch()) {
      assert(pg->get_primary() == m->get_source().num());
      pg->get();
      _remove_pg(pg);
      pg->unlock();
      pg->put();
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
  for (interval_set<snapid_t>::iterator p = pg->snap_collections.begin();
       p != pg->snap_collections.end();
       ++p) {
    for (snapid_t cur = p.get_start();
	 cur < p.get_start() + p.get_len();
	 ++cur) {
      coll_t to_remove = get_next_removal_coll(pg->info.pgid);
      removals.push_back(to_remove);
      rmt->collection_rename(coll_t(pg->info.pgid, cur), to_remove);
    }
  }
  coll_t to_remove = get_next_removal_coll(pg->info.pgid);
  removals.push_back(to_remove);
  rmt->collection_rename(coll_t(pg->info.pgid), to_remove);
  if (pg->have_temp_coll()) {
    to_remove = get_next_removal_coll(pg->info.pgid);
    removals.push_back(to_remove);
    rmt->collection_rename(pg->get_temp_coll(), to_remove);
  }
  rmt->remove(coll_t::META_COLL, pg->log_oid);
  rmt->remove(coll_t::META_COLL, pg->biginfo_oid);

  store->queue_transaction(
    pg->osr.get(), rmt,
    new ObjectStore::C_DeleteTransactionHolder<
      SequencerRef>(rmt, pg->osr),
    new ContainerContext<
      SequencerRef>(pg->osr));

  // on_removal, which calls remove_watchers_and_notifies, and the erasure from 
  // the pg_map must be done together without unlocking the pg lock,
  // to avoid racing with watcher cleanup in ms_handle_reset
  // and handle_notify_timeout
  pg->on_removal();

  DeletingStateRef deleting =
    get_pgosdservice()->deleting_pgs.lookup_or_create(pg->info.pgid);
  for (vector<coll_t>::iterator i = removals.begin();
       i != removals.end();
       ++i) {
    remove_wq.queue(new boost::tuple<coll_t, SequencerRef, DeletingStateRef>(
		      *i, pg->osr, deleting));
  }

  recovery_wq.dequeue(pg);
  scrub_wq.dequeue(pg);
  scrub_finalize_wq.dequeue(pg);
  snap_trim_wq.dequeue(pg);
  pg_stat_queue_dequeue(pg);
  op_wq.dequeue(pg);
  peering_wq.dequeue(pg);

  pg->deleting = true;

  // remove from map
  pg_map.erase(pg->info.pgid);
  pg->put(); // since we've taken it out of map

  get_pgosdservice()->unreg_last_pg_scrub(pg->info.pgid,
					  pg->info.history.last_scrub_stamp);
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
      if (!rctx.query_map->size()) {
	dout(10) << "do_recovery  no luck, giving up on this pg for now" << dendl;
	recovery_wq.lock();
	recovery_wq._dequeue(pg);
	recovery_wq.unlock();
      }
    }

    pg->write_if_dirty(*rctx.transaction);
    OSDMapRef curmap = pg->get_osdmap();
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
