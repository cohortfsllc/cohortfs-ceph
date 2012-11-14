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


#include "PGOSD.h"


PGOSDService::PGOSDService(PGOSD *osd) :
  OSDService(osd),
  pg_recovery_stats(osd->pg_recovery_stats),
  pg_temp_lock("OSDService::pg_temp_lock"),
  op_wq(osd->op_wq),
  peering_wq(osd->peering_wq),
  recovery_wq(osd->recovery_wq),
  snap_trim_wq(osd->snap_trim_wq),
  scrub_wq(osd->scrub_wq),
  scrub_finalize_wq(osd->scrub_finalize_wq),
  rep_scrub_wq(osd->rep_scrub_wq)
{
  // empty
}


void PGOSDService::pg_stat_queue_enqueue(PG *pg)
{
  osd->pg_stat_queue_enqueue(pg);
}

void PGOSDService::pg_stat_queue_dequeue(PG *pg)
{
  osd->pg_stat_queue_dequeue(pg);
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
  OSD(id, internal_messenger, external_messenger, hbclientm, hbservem,
      mc, dev, jdev, osdSvc),
  op_queue_len(0),
  op_wq(this, g_conf->osd_op_thread_timeout, &op_tp),
  peering_wq(this, g_conf->osd_op_thread_timeout, &op_tp, 200),
  debug_drop_pg_create_probability(g_conf->osd_debug_drop_pg_create_probability),
  debug_drop_pg_create_duration(g_conf->osd_debug_drop_pg_create_duration),
  debug_drop_pg_create_left(-1),
  outstanding_pg_stats(false),
  pg_stat_queue_lock("OSD::pg_stat_queue_lock"),
  osd_stat_updated(false),
  pg_stat_tid(0),
  pg_stat_tid_flushed(0),
  recovery_ops_active(0),
  recovery_wq(this, g_conf->osd_recovery_thread_timeout, &recovery_tp),
  replay_queue_lock("OSD::replay_queue_lock"),
  snap_trim_wq(this, g_conf->osd_snap_trim_thread_timeout, &disk_tp),
  scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  scrub_finalize_wq(this, g_conf->osd_scrub_finalize_thread_timeout, &op_tp),
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

PGPool PGOSD::_get_pool(int id, OSDMapRef createmap)
{
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

PG *OSD::_open_lock_pg(
		       OSDMapRef createmap,
		       pg_t pgid, bool no_lockdep_check, bool hold_map_lock)
{
  assert(osd_lock.is_locked());

  dout(10) << "_open_lock_pg " << pgid << dendl;
  PGPool pool = _get_pool(pgid.pool(), createmap);

  // create
  PG *pg;
  hobject_t logoid = make_pg_log_oid(pgid);
  hobject_t infooid = make_pg_biginfo_oid(pgid);
  if (osdmap->get_pg_type(pgid) == pg_pool_t::TYPE_REP)
    pg = new ReplicatedPG(&service, createmap, pool, pgid, logoid, infooid);
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

PG *OSD::_create_lock_pg(
			 OSDMapRef createmap,
			 pg_t pgid, bool newly_created, bool hold_map_lock,
			 int role, vector<int>& up, vector<int>& acting, pg_history_t history,
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

PG *OSD::_lookup_lock_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  if (!pg_map.count(pgid))
    return NULL;
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}

PG *OSD::_lookup_lock_pg_with_map_lock_held(pg_t pgid)
{
  assert(osd_lock.is_locked());
  assert(pg_map.count(pgid));
  PG *pg = pg_map[pgid];
  pg->lock_with_map_lock_held();
  return pg;
}

PG *OSD::lookup_lock_raw_pg(pg_t pgid)
{
  Mutex::Locker l(osd_lock);
  if (osdmap->have_pg_pool(pgid.pool())) {
    pgid = osdmap->raw_pg_to_pg(pgid);
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
	to_queue->get<1>() = serviceRef->osr_registry.lookup_or_create(
								       pgid, stringify(pgid));
	to_queue->get<2>() = serviceRef->deleting_pgs.lookup_or_create(pgid);
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

    if (!osdmap->have_pg_pool(pgid.pool())) {
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

    serviceRef->reg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);

    // generate state for current mapping
    osdmap->pg_to_up_acting_osds(pgid, pg->up, pg->acting);
    int role = osdmap->calc_pg_role(whoami, pg->acting);
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
PG *OSD::get_or_create_pg(const pg_info_t& info, pg_interval_map_t& pi,
			  epoch_t epoch, int from, int& created, bool primary)
{
  PG *pg;

  if (!_have_pg(info.pgid)) {
    // same primary?
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(info.pgid, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

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
    oldmap->pg_to_acting_osds(pgid, acting);
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
    oldmap->pg_to_up_acting_osds(pgid, up, acting);

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


void PGOSD::build_heartbeat_peers_list() const {
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
  while (serviceRef->next_scrub_stamp(pos, &pos)) {
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


void PGOSD::tick_sub() {
  if (outstanding_pg_stats
      &&(now - g_conf->osd_mon_ack_timeout) > last_pg_stats_ack) {
    dout(1) << "mon hasn't acked PGStats in " << now - last_pg_stats_ack
            << " seconds, reconnecting elsewhere" << dendl;
    monc->reopen_session();
    last_pg_stats_ack = ceph_clock_now(g_ceph_context);  // reset clock
  }
}


void PGOSD::do_mon_report_sub() {
  pgosdmap()->send_pg_temp();
  send_pg_stats(now);
}


void PGOSD::ms_handle_connect_sub(Connection *con) {
  serviceRef->send_pg_temp();
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


void PGOSD::ms_handle_reset_sub(OSD::Session* session) {
  Session* pgsession = dynamic_cast<Session*>(session);
  disconnect_session_watches(pgsession);
}
