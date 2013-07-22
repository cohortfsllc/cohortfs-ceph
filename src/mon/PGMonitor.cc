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


#include "json_spirit/json_spirit.h"
#include "common/debug.h"		// undo damage
#include "PGMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "OSDMonitor.h"
#include "MonitorDBStore.h"

#include "pg/PGOSDMap.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"
#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDScrub.h"

#include "common/Cond.h"
#include "common/Timer.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/TextTable.h"

#include "include/stringify.h"

#include "osd/osd_types.h"

#include "pg/pg_types.h"

#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/str_list.h"
#include <sstream>
#include <boost/variant.hpp>
#include "common/cmdparse.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, pg_map)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon, const PGMap& pg_map) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").pg v" << pg_map.version << " ";
}

/*
 Tick function to update the map based on performance every N seconds
*/

void PGMonitor::on_restart()
{
  // clear leader state
  last_sent_pg_create.clear();
  last_osd_report.clear();
}

void PGMonitor::on_active()
{
  if (mon->is_leader()) {
    check_osd_map(mon->osdmon()->osdmap->epoch);
    need_check_down_pgs = true;
  }

  update_logger();

  if (mon->is_leader())
    mon->clog.info() << "pgmap " << pg_map << "\n";
}

void PGMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  mon->cluster_logger->set(l_cluster_osd_kb, pg_map.osd_sum.kb);
  mon->cluster_logger->set(l_cluster_osd_kb_used, pg_map.osd_sum.kb_used);
  mon->cluster_logger->set(l_cluster_osd_kb_avail, pg_map.osd_sum.kb_avail);

  mon->cluster_logger->set(l_cluster_num_pool, pg_map.pg_pool_sum.size());
  mon->cluster_logger->set(l_cluster_num_pg, pg_map.pg_stat.size());

  unsigned active = 0, active_clean = 0, peering = 0;
  for (hash_map<int,int>::iterator p = pg_map.num_pg_by_state.begin();
       p != pg_map.num_pg_by_state.end();
       ++p) {
    if (p->first & PG_STATE_ACTIVE) {
      active += p->second;
      if (p->first & PG_STATE_CLEAN)
	active_clean += p->second;
    }
    if (p->first & PG_STATE_PEERING)
      peering += p->second;
  }
  mon->cluster_logger->set(l_cluster_num_pg_active_clean, active_clean);
  mon->cluster_logger->set(l_cluster_num_pg_active, active);
  mon->cluster_logger->set(l_cluster_num_pg_peering, peering);

  mon->cluster_logger->set(l_cluster_num_object, pg_map.pg_sum.stats.sum.num_objects);
  mon->cluster_logger->set(l_cluster_num_object_degraded, pg_map.pg_sum.stats.sum.num_objects_degraded);
  mon->cluster_logger->set(l_cluster_num_object_unfound, pg_map.pg_sum.stats.sum.num_objects_unfound);
  mon->cluster_logger->set(l_cluster_num_bytes, pg_map.pg_sum.stats.sum.num_bytes);
}

void PGMonitor::tick() 
{
  if (!is_active()) return;

  handle_osd_timeouts();

  if (mon->is_leader()) {
    bool propose = false;
    
    if (need_check_down_pgs && check_down_pgs())
      propose = true;
    
    if (propose) {
      propose_pending();
    }
  }

  if (!pg_map.pg_sum_deltas.empty()) {
    utime_t age = ceph_clock_now(g_ceph_context) - pg_map.stamp;
    if (age > 2 * g_conf->mon_delta_reset_interval) {
      dout(10) << " clearing pg_map delta (" << age << " > " << g_conf->mon_delta_reset_interval << " seconds old)" << dendl;
      pg_map.clear_delta();
    }
  }

  dout(10) << pg_map << dendl;
}

void PGMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;
}

void PGMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_version();
  if (version == pg_map.version)
    return;
  assert(version >= pg_map.version);

  /* Obtain latest full pgmap version, if available and whose version is
   * greater than the current pgmap's version.
   */
  version_t latest_full = get_version_latest_full();
  if ((latest_full > 0) && (latest_full > pg_map.version)) {
    bufferlist latest_bl;
    int err = get_version_full(latest_full, latest_bl);
    assert(err == 0);
    dout(7) << __func__ << " loading latest full pgmap v"
	    << latest_full << dendl;
    try {
      PGMap tmp_pg_map;
      bufferlist::iterator p = latest_bl.begin();
      tmp_pg_map.decode(p);
      pg_map = tmp_pg_map;
    } catch (const std::exception& e) {
      dout(0) << __func__ << ": error parsing update: "
	      << e.what() << dendl;
      assert(0 == "update_from_paxos: error parsing update");
      return;
    }
  }

  // walk through incrementals
  while (version > pg_map.version) {
    bufferlist bl;
    int err = get_version(pg_map.version+1, bl);
    assert(err == 0);
    assert(bl.length());

    dout(7) << "update_from_paxos  applying incremental " << pg_map.version+1 << dendl;
    PGMap::Incremental inc;
    try {
      bufferlist::iterator p = bl.begin();
      inc.decode(p);
    }
    catch (const std::exception &e) {
      dout(0) << "update_from_paxos: error parsing "
	      << "incremental update: " << e.what() << dendl;
      assert(0 == "update_from_paxos: error parsing incremental update");
      return;
    }

    pg_map.apply_incremental(g_ceph_context, inc);
    
    dout(10) << pg_map << dendl;

    if (inc.pg_scan)
      last_sent_pg_create.clear();  // reset pg_create throttle timer
  }

  assert(version == pg_map.version);

  /* If we dump the summaries onto the k/v store, they hardly would be useful
   * without a tool created with reading them in mind.
   * Comment this out until we decide what is the best course of action.
   *
  // dump pgmap summaries?  (useful for debugging)
  if (0) {
    stringstream ds;
    pg_map.dump(ds);
    bufferlist d;
    d.append(ds);
    mon->store->put_bl_sn(d, "pgmap_dump", version);
  }
  */

  update_trim();

  if (mon->osdmon()->osdmap->get_epoch()) {
    map_pg_creates();
    send_pg_creates();
  }

  update_logger();
}

void PGMonitor::init()
{
  if (mon->osdmon()->osdmap->get_epoch()) {
    map_pg_creates();
  }
}

void PGMonitor::handle_osd_timeouts()
{
  if (!mon->is_leader())
    return;
  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t timeo(g_conf->mon_osd_report_timeout, 0);
  if (now - mon->get_leader_since() < timeo) {
    // We haven't been the leader for long enough to consider OSD timeouts
    return;
  }

  if (mon->osdmon()->is_writeable())
    mon->osdmon()->handle_osd_timeouts(now, last_osd_report);
}

void PGMonitor::create_pending()
{
  pending_inc = PGMap::Incremental();
  pending_inc.version = pg_map.version + 1;
  if (pg_map.version == 0) {
    // pull initial values from first leader mon's config
    pending_inc.full_ratio = g_conf->mon_osd_full_ratio;
    if (pending_inc.full_ratio > 1.0)
      pending_inc.full_ratio /= 100.0;
    pending_inc.nearfull_ratio = g_conf->mon_osd_nearfull_ratio;
    if (pending_inc.nearfull_ratio > 1.0)
      pending_inc.nearfull_ratio /= 100.0;
  } else {
    pending_inc.full_ratio = pg_map.full_ratio;
    pending_inc.nearfull_ratio = pg_map.nearfull_ratio;
  }
  dout(10) << "create_pending v " << pending_inc.version << dendl;
}

void PGMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  version_t version = pending_inc.version;
  dout(10) << __func__ << " v " << version << dendl;
  assert(get_version() + 1 == version);
  pending_inc.stamp = ceph_clock_now(g_ceph_context);

  bufferlist bl;
  pending_inc.encode(bl, mon->get_quorum_features());

  put_version(t, version, bl);
  put_last_committed(t, version);
}

void PGMonitor::encode_full(MonitorDBStore::Transaction *t)
{
  dout(10) << __func__ << " pgmap v " << pg_map.version << dendl;
  assert(get_version() == pg_map.version);

  bufferlist full_bl;
  pg_map.encode(full_bl, mon->get_quorum_features());

  put_version_full(t, pg_map.version, full_bl);
  put_version_latest_full(t, pg_map.version);
}

void PGMonitor::update_trim()
{
  unsigned max = g_conf->mon_max_pgmap_epochs;
  version_t version = get_version();
  if (mon->is_leader() && (version > max))
    set_trim_to(version - max);
}


bool PGMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case CEPH_MSG_STATFS:
    handle_statfs(static_cast<MStatfs*>(m));
    return true;
  case MSG_GETPOOLSTATS:
    return preprocess_getpoolstats(static_cast<MGetPoolStats*>(m));
    
  case MSG_PGSTATS:
    return preprocess_pg_stats(static_cast<MPGStats*>(m));

  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m));


  default:
    assert(0);
    m->put();
    return true;
  }
}

bool PGMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    return prepare_pg_stats((MPGStats*)m);

  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));

  default:
    assert(0);
    m->put();
    return false;
  }
}

void PGMonitor::handle_statfs(MStatfs *statfs)
{
  // check caps
  MonSession *session = statfs->get_session();
  if (!session)
    goto out;
  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MStatfs received from entity with insufficient privileges "
	    << session->caps << dendl;
    goto out;
  }
  MStatfsReply *reply;

  dout(10) << "handle_statfs " << *statfs << " from " << statfs->get_orig_source() << dendl;

  if (statfs->fsid != mon->monmap->fsid) {
    dout(0) << "handle_statfs on fsid " << statfs->fsid << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  // fill out stfs
  reply = new MStatfsReply(mon->monmap->fsid, statfs->get_tid(), get_version());

  // these are in KB.
  reply->h.st.kb = pg_map.osd_sum.kb;
  reply->h.st.kb_used = pg_map.osd_sum.kb_used;
  reply->h.st.kb_avail = pg_map.osd_sum.kb_avail;
  reply->h.st.num_objects = pg_map.pg_sum.stats.sum.num_objects;

  // reply
  mon->send_reply(statfs, reply);
 out:
  statfs->put();
}

bool PGMonitor::preprocess_getpoolstats(MGetPoolStats *m)
{
  MGetPoolStatsReply *reply;
  MonSession *session = m->get_session();
  PGOSDMap* const l_osdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());

  if (!session)
    goto out;
  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MGetPoolStats received from entity with insufficient caps "
	    << session->caps << dendl;
    goto out;
  }

  if (m->fsid != mon->monmap->fsid) {
    dout(0) << "preprocess_getpoolstats on fsid " << m->fsid << " != " <<
      mon->monmap->fsid << dendl;
    goto out;
  }
  
  reply = new MGetPoolStatsReply(m->fsid, m->get_tid(), get_version());

  for (list<string>::iterator p = m->pools.begin();
       p != m->pools.end();
       ++p) {
    int64_t poolid = l_osdmap->lookup_pg_pool_name(p->c_str());
    if (poolid < 0)
      continue;
    if (pg_map.pg_pool_sum.count(poolid) == 0)
      continue;
    reply->pool_stats[*p] = pg_map.pg_pool_sum[poolid];
  }

  mon->send_reply(m, reply);

 out:
  m->put();
  return true;
}


bool PGMonitor::preprocess_pg_stats(MPGStats *stats)
{
  // check caps
  MonSession *session = stats->get_session();
  if (!session) {
    dout(10) << "PGMonitor::preprocess_pg_stats: no monitor session!" << dendl;
    stats->put();
    return true;
  }
  if (!session->is_capable("pg", MON_CAP_R)) {
    derr << "PGMonitor::preprocess_pg_stats: MPGStats received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    stats->put();
    return true;
  }

  // First, just see if they need a new osdmap. But
  // only if they've had the map for a while.
  if (stats->had_map_for > 30.0 && 
      mon->osdmon()->is_readable() &&
      stats->epoch < mon->osdmon()->osdmap->get_epoch())
    mon->osdmon()->send_latest_now_nodelete(stats, stats->epoch+1);

  // Always forward the PGStats to the leader, even if they are the same as
  // the old PGStats. The leader will mark as down osds that haven't sent
  // PGStats for a few minutes.
  return false;
}

bool PGMonitor::pg_stats_have_changed(int from, const MPGStats *stats) const
{
  // any new osd info?
  hash_map<int,osd_stat_t>::const_iterator s = pg_map.osd_stat.find(from);
  if (s == pg_map.osd_stat.end())
    return true;
  if (s->second != stats->osd_stat)
    return true;

  // any new pg info?
  for (map<pg_t,pg_stat_t>::const_iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end(); ++p) {
    hash_map<pg_t,pg_stat_t>::const_iterator t = pg_map.pg_stat.find(p->first);
    if (t == pg_map.pg_stat.end())
      return true;
    if (t->second.reported != p->second.reported)
      return true;
  }

  return false;
}

bool PGMonitor::prepare_pg_stats(MPGStats *stats) 
{
  dout(10) << "prepare_pg_stats " << *stats << " from " << stats->get_orig_source() << dendl;
  int from = stats->get_orig_source().num();

  if (stats->fsid != mon->monmap->fsid) {
    dout(0) << "prepare_pg_stats on fsid " << stats->fsid << " != " << mon->monmap->fsid << dendl;
    stats->put();
    return false;
  }

  last_osd_report[from] = ceph_clock_now(g_ceph_context);

  if (!stats->get_orig_source().is_osd() ||
      !mon->osdmon()->osdmap->is_up(from) ||
      stats->get_orig_source_inst() != mon->osdmon()->osdmap->get_inst(from)) {
    dout(1) << " ignoring stats from non-active osd." << dendl;
    stats->put();
    return false;
  }
      
  if (!pg_stats_have_changed(from, stats)) {
    dout(10) << " message contains no new osd|pg stats" << dendl;
    MPGStatsAck *ack = new MPGStatsAck;
    for (map<pg_t,pg_stat_t>::const_iterator p = stats->pg_stat.begin();
	 p != stats->pg_stat.end();
	 ++p) {
      ack->pg_stat[p->first] = p->second.reported;
    }
    mon->send_reply(stats, ack);
    stats->put();
    return false;
  }

  // osd stat
  pending_inc.osd_stat_updates[from] = stats->osd_stat;
  
  if (pg_map.osd_stat.count(from))
    dout(10) << " got osd." << from << " " << stats->osd_stat << " (was " << pg_map.osd_stat[from] << ")" << dendl;
  else
    dout(10) << " got osd." << from << " " << stats->osd_stat << " (first report)" << dendl;

  // apply to live map too (screw consistency)
  /*
    actually, no, don't.  that screws up our "latest" stash.  and can
    lead to weird output where things appear to jump backwards in
    time... that's just confusing.

  if (pg_map.osd_stat.count(from))
    pg_map.stat_osd_sub(pg_map.osd_stat[from]);
  pg_map.osd_stat[from] = stats->osd_stat;
  pg_map.stat_osd_add(stats->osd_stat);
  */

  // pg stats
  MPGStatsAck *ack = new MPGStatsAck;
  ack->set_tid(stats->get_tid());
  for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end();
       ++p) {
    pg_t pgid = p->first;
    ack->pg_stat[pgid] = p->second.reported;

    if ((pg_map.pg_stat.count(pgid) && 
	 pg_map.pg_stat[pgid].reported > p->second.reported)) {
      dout(15) << " had " << pgid << " from " << pg_map.pg_stat[pgid].reported << dendl;
      continue;
    }
    if (pending_inc.pg_stat_updates.count(pgid) && 
	pending_inc.pg_stat_updates[pgid].reported > p->second.reported) {
      dout(15) << " had " << pgid << " from " << pending_inc.pg_stat_updates[pgid].reported
	       << " (pending)" << dendl;
      continue;
    }

    if (pg_map.pg_stat.count(pgid) == 0) {
      dout(15) << " got " << pgid << " reported at " << p->second.reported
	       << " state " << pg_state_string(p->second.state)
	       << " but DNE in pg_map; pool was probably deleted."
	       << dendl;
      continue;
    }
      
    dout(15) << " got " << pgid
	     << " reported at " << p->second.reported
	     << " state " << pg_state_string(pg_map.pg_stat[pgid].state)
	     << " -> " << pg_state_string(p->second.state)
	     << dendl;
    pending_inc.pg_stat_updates[pgid] = p->second;

    /*
    // we don't care much about consistency, here; apply to live map.
    pg_map.stat_pg_sub(pgid, pg_map.pg_stat[pgid]);
    pg_map.pg_stat[pgid] = p->second;
    pg_map.stat_pg_add(pgid, pg_map.pg_stat[pgid]);
    */
  }
  
  wait_for_finished_proposal(new C_Stats(this, stats, ack));
  return true;
}

void PGMonitor::_updated_stats(MPGStats *req, MPGStatsAck *ack)
{
  dout(7) << "_updated_stats for " << req->get_orig_source_inst() << dendl;
  mon->send_reply(req, ack);
  req->put();
}



// ------------------------

struct RetryCheckOSDMap : public Context {
  PGMonitor *pgmon;
  epoch_t epoch;
  RetryCheckOSDMap(PGMonitor *p, epoch_t e) : pgmon(p), epoch(e) {}
  void finish(int r) {
    if (r == -ECANCELED)
      return;
    pgmon->check_osd_map(epoch);
  }
};

void PGMonitor::check_osd_map(epoch_t epoch)
{
  if (mon->is_peon()) 
    return; // whatever.

  if (pg_map.last_osdmap_epoch >= epoch) {
    dout(10) << "check_osd_map already seen " << pg_map.last_osdmap_epoch << " >= " << epoch << dendl;
    return;
  }

  if (!mon->osdmon()->is_readable()) {
    dout(10) << "check_osd_map -- osdmap not readable, waiting" << dendl;
    mon->osdmon()->wait_for_readable(new RetryCheckOSDMap(this, epoch));
    return;
  }

  if (!is_writeable()) {
    dout(10) << "check_osd_map -- pgmap not writeable, waiting" << dendl;
    wait_for_writeable(new RetryCheckOSDMap(this, epoch));
    return;
  }

  // apply latest map(s)
  for (epoch_t e = pg_map.last_osdmap_epoch+1;
       e <= epoch;
       e++) {
    dout(10) << "check_osd_map applying osdmap e" << e << " to pg_map" << dendl;
    bufferlist bl;
    int err = mon->osdmon()->get_version(e, bl);
    assert(err == 0);

    assert(bl.length());
    auto_ptr<PGOSDMap::Incremental> inc(new PGOSDMap::Incremental(bl));
    for (map<int32_t,uint32_t>::iterator p = inc->new_weight.begin();
	 p != inc->new_weight.end();
	 ++p)
      if (p->second == CEPH_OSD_OUT) {
	dout(10) << "check_osd_map  osd." << p->first << " went OUT" << dendl;
	pending_inc.osd_stat_rm.insert(p->first);
      } else {
	dout(10) << "check_osd_map  osd." << p->first << " is IN" << dendl;
	pending_inc.osd_stat_rm.erase(p->first);
	pending_inc.osd_stat_updates[p->first]; 
      }

    // this is conservative: we want to know if any osds (maybe) got marked down.
    for (map<int32_t,uint8_t>::iterator p = inc->new_state.begin();
	 p != inc->new_state.end();
	 ++p) {
      if (p->second & CEPH_OSD_UP) {   // true if marked up OR down, but we're too lazy to check which
	need_check_down_pgs = true;

	// clear out the last_osd_report for this OSD
        map<int, utime_t>::iterator report = last_osd_report.find(p->first);
        if (report != last_osd_report.end()) {
          last_osd_report.erase(report);
        }
      }

      if (p->second & CEPH_OSD_EXISTS) {
	// whether it was created *or* destroyed, we can safely drop
	// it's osd_stat_t record.
	dout(10) << "check_osd_map  osd." << p->first << " created or destroyed" << dendl;
	pending_inc.osd_stat_rm.insert(p->first);

	// and adjust full, nearfull set
	pg_map.nearfull_osds.erase(p->first);
	pg_map.full_osds.erase(p->first);
      }
    }
  }

  bool propose = false;
  if (pg_map.last_osdmap_epoch < epoch) {
    pending_inc.osdmap_epoch = epoch;
    propose = true;
  }

  // scan pg space?
  if (register_new_pgs())
    propose = true;

  if (need_check_down_pgs && check_down_pgs())
    propose = true;
  
  if (propose)
    propose_pending();

  if (mon->osdmon()->osdmap->get_epoch()) {
    map_pg_creates();
    send_pg_creates();
  }
}

void PGMonitor::register_pg(pg_pool_t& pool, pg_t pgid, epoch_t epoch, bool new_pool)
{
  pg_t parent;
  int split_bits = 0;
  if (!new_pool) {
    parent = pgid;
    while (1) {
      // remove most significant bit
      int msb = pool.calc_bits_of(parent.ps());
      if (!msb) break;
      parent.set_ps(parent.ps() & ~(1<<(msb-1)));
      split_bits++;
      dout(10) << " is " << pgid << " parent " << parent << " ?" << dendl;
      //if (parent.u.pg.ps < mon->osdmon->osdmap.get_pgp_num()) {
      if (pg_map.pg_stat.count(parent) &&
	  pg_map.pg_stat[parent].state != PG_STATE_CREATING) {
	dout(10) << "  parent is " << parent << dendl;
	break;
      }
    }
  }
  
  pending_inc.pg_stat_updates[pgid].state = PG_STATE_CREATING;
  pending_inc.pg_stat_updates[pgid].created = epoch;
  pending_inc.pg_stat_updates[pgid].parent = parent;
  pending_inc.pg_stat_updates[pgid].parent_split_bits = split_bits;

  if (split_bits == 0) {
    dout(10) << "register_new_pgs  will create " << pgid << dendl;
  } else {
    dout(10) << "register_new_pgs  will create " << pgid
	     << " parent " << parent
	     << " by " << split_bits << " bits"
	     << dendl;
  }
}

bool PGMonitor::register_new_pgs()
{
  // iterate over crush mapspace
  epoch_t epoch = mon->osdmon()->osdmap->get_epoch();
  dout(10) << "register_new_pgs checking pg pools for osdmap epoch " << epoch
	   << ", last_pg_scan " << pg_map.last_pg_scan << dendl;

  PGOSDMap *osdmap = dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());

  int created = 0;
  for (map<int64_t,pg_pool_t>::iterator p = osdmap->pools.begin();
       p != osdmap->pools.end();
       ++p) {
    int64_t poolid = p->first;
    pg_pool_t &pool = p->second;
    int ruleno = pool.get_crush_ruleset();
    if (!osdmap->crush->rule_exists(ruleno)) 
      continue;

    if (pool.get_last_change() <= pg_map.last_pg_scan ||
	pool.get_last_change() <= pending_inc.pg_scan) {
      dout(10) << " no change in pool " << p->first << " " << pool << dendl;
      continue;
    }

    dout(10) << "register_new_pgs scanning pool " << p->first << " " << pool << dendl;

    bool new_pool = pg_map.pg_pool_sum.count(poolid) == 0;  // first pgs in this pool

    for (ps_t ps = 0; ps < pool.get_pg_num(); ps++) {
      pg_t pgid(ps, poolid, -1);
      if (pg_map.pg_stat.count(pgid)) {
	dout(20) << "register_new_pgs  have " << pgid << dendl;
	continue;
      }
      created++;
      register_pg(pool, pgid, pool.get_last_change(), new_pool);
    }
  }

  int removed = 0;
  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    if (p->preferred() >= 0) {
      dout(20) << " removing creating_pg " << *p << " because it is localized and obsolete" << dendl;
      pending_inc.pg_remove.insert(*p);
      removed++;
    }
    if (!osdmap->have_pg_pool(p->pool())) {
      dout(20) << " removing creating_pg " << *p << " because containing pool deleted" << dendl;
      pending_inc.pg_remove.insert(*p);
      ++removed;
    }
  }

  // deleted pools?
  for (hash_map<pg_t,pg_stat_t>::const_iterator p = pg_map.pg_stat.begin();
       p != pg_map.pg_stat.end(); ++p) {
    if (!osdmap->have_pg_pool(p->first.pool())) {
      dout(20) << " removing pg_stat " << p->first << " because "
	       << "containing pool deleted" << dendl;
      pending_inc.pg_remove.insert(p->first);
      ++removed;
    }
    if (p->first.preferred() >= 0) {
      dout(20) << " removing localized pg " << p->first << dendl;
      pending_inc.pg_remove.insert(p->first);
      ++removed;
    }
  }

  dout(10) << "register_new_pgs registered " << created << " new pgs, removed "
	   << removed << " uncreated pgs" << dendl;
  if (created || removed) {
    pending_inc.pg_scan = epoch;
    return true;
  }
  return false;
}

void PGMonitor::map_pg_creates()
{
  dout(10) << "map_pg_creates to " << pg_map.creating_pgs.size() << " pgs" << dendl;

  PGOSDMap* const pgosdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());

  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    pg_t pgid = *p;
    pg_t on = pgid;
    pg_stat_t& s = pg_map.pg_stat[pgid];
    if (s.parent_split_bits)
      on = s.parent;
    vector<int> acting;
    int nrep = pgosdmap->pg_to_acting_osds(on, acting);

    if (s.acting.size()) {
      pg_map.creating_pgs_by_osd[s.acting[0]].erase(pgid);
      if (pg_map.creating_pgs_by_osd[s.acting[0]].size() == 0)
        pg_map.creating_pgs_by_osd.erase(s.acting[0]);
    }
    s.acting = acting;

    // don't send creates for localized pgs
    if (pgid.preferred() >= 0)
      continue;

    // don't send creates for splits
    if (s.parent_split_bits)
      continue;

    if (nrep) {
      pg_map.creating_pgs_by_osd[acting[0]].insert(pgid);
    } else {
      dout(20) << "mon_pg_creates  " << pgid << " -> no osds in epoch "
	       << pgosdmap->get_epoch() << ", skipping" << dendl;
      continue;  // blarney!
    }
  }
  for (map<int, set<pg_t> >::iterator p = pg_map.creating_pgs_by_osd.begin();
       p != pg_map.creating_pgs_by_osd.end();
       ++p) {
    dout(10) << "map_pg_creates osd." << p->first << " has " << p->second.size() << " pgs" << dendl;
  }
}

void PGMonitor::send_pg_creates()
{
  dout(10) << "send_pg_creates to " << pg_map.creating_pgs.size() << " pgs" << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);
  for (map<int, set<pg_t> >::iterator p = pg_map.creating_pgs_by_osd.begin();
       p != pg_map.creating_pgs_by_osd.end();
       ++p) {
    int osd = p->first;

    // throttle?
    if (last_sent_pg_create.count(osd) &&
	now - g_conf->mon_pg_create_interval < last_sent_pg_create[osd]) 
      continue;

    if (mon->osdmon()->osdmap->is_up(osd))
      send_pg_creates(osd, NULL);
  }
}

void PGMonitor::send_pg_creates(int osd, Connection *con)
{
  PGOSDMap* const pgosdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());

  map<int, set<pg_t> >::iterator p = pg_map.creating_pgs_by_osd.find(osd);
  if (p == pg_map.creating_pgs_by_osd.end())
    return;
  assert(p->second.size() > 0);

  dout(20) << "send_pg_creates osd." << osd << " pgs " << p->second << dendl;
  MOSDPGCreate *m = new MOSDPGCreate(pgosdmap->get_epoch());
  for (set<pg_t>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
    m->mkpg[*q] = pg_create_t(pg_map.pg_stat[*q].created,
			      pg_map.pg_stat[*q].parent,
			      pg_map.pg_stat[*q].parent_split_bits);
  }

  if (con) {
    mon->messenger->send_message(m, con);
  } else {
    assert(pgosdmap->is_up(osd));
    mon->messenger->send_message(m, pgosdmap->get_inst(osd));
  }
  last_sent_pg_create[osd] = ceph_clock_now(g_ceph_context);
}

bool PGMonitor::check_down_pgs()
{
  dout(10) << "check_down_pgs" << dendl;

  PGOSDMap* const osdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());
  bool ret = false;

  for (hash_map<pg_t,pg_stat_t>::iterator p = pg_map.pg_stat.begin();
       p != pg_map.pg_stat.end();
       ++p) {
    if ((p->second.state & PG_STATE_STALE) == 0 &&
	p->second.acting.size() &&
	osdmap->is_down(p->second.acting[0])) {
      dout(10) << " marking pg " << p->first << " stale with acting " << p->second.acting << dendl;

      map<pg_t,pg_stat_t>::iterator q = pending_inc.pg_stat_updates.find(p->first);
      pg_stat_t *stat;
      if (q == pending_inc.pg_stat_updates.end()) {
	stat = &pending_inc.pg_stat_updates[p->first];
	*stat = p->second;
      } else {
	stat = &q->second;
      }
      stat->state |= PG_STATE_STALE;
      stat->last_unstale = ceph_clock_now(g_ceph_context);
      ret = true;
    }
  }
  need_check_down_pgs = false;

  return ret;
}

inline string percentify(const float& a) {
  stringstream ss;
  if (a < 0.01)
    ss << "0";
  else
    ss << std::fixed << std::setprecision(2) << a;
  return ss.str();
}

//void PGMonitor::dump_object_stat_sum(stringstream& ss, Formatter *f,
void PGMonitor::dump_object_stat_sum(TextTable &tbl, Formatter *f,
    object_stat_sum_t &sum, bool verbose)
{
  if (f) {
    f->dump_int("kb_used", SHIFT_ROUND_UP(sum.num_bytes, 10));
    f->dump_int("bytes_used", sum.num_bytes);
    f->dump_int("objects", sum.num_objects);
    if (verbose) {
      f->dump_int("rd", sum.num_rd);
      f->dump_int("rd_kb", sum.num_rd_kb);
      f->dump_int("wr", sum.num_wr);
      f->dump_int("wr_kb", sum.num_wr_kb);
    }
  } else {
    tbl << stringify(si_t(sum.num_bytes));
    int64_t kb_used = SHIFT_ROUND_UP(sum.num_bytes, 10);
    tbl << percentify(((float)kb_used / pg_map.osd_sum.kb)*100);
    tbl << sum.num_objects;
    if (verbose) {
      tbl << stringify(si_t(sum.num_rd))
          << stringify(si_t(sum.num_wr));
    }
  }
}

void PGMonitor::dump_pool_stats(stringstream &ss, Formatter *f, bool verbose)
{
  TextTable tbl;

  if (f) {
    f->open_array_section("pools");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("ID", TextTable::LEFT, TextTable::LEFT);
    if (verbose)
      tbl.define_column("CATEGORY", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("USED", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("\%USED", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::LEFT);
    if (verbose) {
      tbl.define_column("READ", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("WRITE", TextTable::LEFT, TextTable::LEFT);
    }
  }

  PGOSDMap* const l_osdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());
  for (map<int64_t,pg_pool_t>::const_iterator p
	 = l_osdmap->get_pools().begin();
       p != l_osdmap->get_pools().end(); ++p) {
    int64_t pool_id = p->first;
    if ((pool_id < 0) || (pg_map.pg_pool_sum.count(pool_id) == 0))
      continue;
    string pool_name = l_osdmap->get_pool_name(pool_id);
    pool_stat_t &stat = pg_map.pg_pool_sum[pool_id];

    if (f) {
      f->open_object_section("pool");
      f->dump_string("name", pool_name);
      f->dump_int("id", pool_id);
      f->open_object_section("stats");
    } else {
      tbl << pool_name
          << pool_id;
      if (verbose)
        tbl << "-";
    }
    dump_object_stat_sum(tbl, f, stat.stats.sum, verbose);
    if (f)
      f->close_section(); // stats
    else
      tbl << TextTable::endrow;

    if (verbose) {
      if (f)
        f->open_array_section("categories");

      for (map<string,object_stat_sum_t>::iterator it = stat.stats.cat_sum.begin();
          it != stat.stats.cat_sum.end(); ++it) {
        if (f) {
          f->open_object_section(it->first.c_str());
        } else {
          tbl << ""
              << ""
              << it->first;
        }
        dump_object_stat_sum(tbl, f, it->second, verbose);
        if (f)
          f->close_section(); // category name
        else
          tbl << TextTable::endrow;
      }
      if (f)
        f->close_section(); // categories
    }
    if (f)
      f->close_section(); // pool
  }
  if (f)
    f->close_section();
  else {
    ss << "POOLS:\n";
    tbl.set_indent(4);
    ss << tbl;
  }
}

void PGMonitor::dump_fs_stats(stringstream &ss, Formatter *f, bool verbose)
{
  if (f) {
    f->open_object_section("stats");
    f->dump_int("total_space", pg_map.osd_sum.kb);
    f->dump_int("total_used", pg_map.osd_sum.kb_used);
    f->dump_int("total_avail", pg_map.osd_sum.kb_avail);
    if (verbose) {
      f->dump_int("total_objects", pg_map.pg_sum.stats.sum.num_objects);
    }
    f->close_section();
  } else {
    TextTable tbl;
    tbl.define_column("SIZE", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("AVAIL", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("RAW USED", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("\%RAW USED", TextTable::LEFT, TextTable::LEFT);
    if (verbose) {
      tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::LEFT);
    }
    tbl << stringify(si_t(pg_map.osd_sum.kb*1024))
        << stringify(si_t(pg_map.osd_sum.kb_avail*1024))
        << stringify(si_t(pg_map.osd_sum.kb_used*1024));
    tbl << percentify(((float)pg_map.osd_sum.kb_used / pg_map.osd_sum.kb)*100);
    if (verbose) {
      tbl << stringify(si_t(pg_map.pg_sum.stats.sum.num_objects));
    }
    tbl << TextTable::endrow;

    ss << "GLOBAL:\n";
    tbl.set_indent(4);
    ss << tbl;
  }
}


void PGMonitor::dump_info(Formatter *f)
{
  f->open_object_section("pgmap");
  pg_map.dump(f);
  f->close_section();
}

bool PGMonitor::preprocess_command(MMonCommand *m)
{
  PGOSDMap* const pgosdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());


  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("pg", MON_CAP_R) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  // perhaps these would be better in the parsing, but it's weird
  if (prefix == "pg dump_json") {
    cmd_putval(g_ceph_context, cmdmap, "format", string("json"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", string("all"));
    prefix = "pg dump";
  } else if (prefix == "pg dump_pools_json") {
    cmd_putval(g_ceph_context, cmdmap, "format", string("json"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", string("pool"));
    prefix = "pg dump";
  }

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  if (prefix == "pg stat") {
    ds << pg_map;
    rdata.append(ds);
    r = 0;
  } else if (prefix == "pg getmap") {
    pg_map.encode(rdata);
    ss << "got pgmap version " << pg_map.version;
    r = 0;
  } else if (prefix == "pg map_pg_creates") {
    map_pg_creates();
    ss << "mapped pg creates ";
    r = 0;
  } else if (prefix == "pg send_pg_creates") {
    send_pg_creates();
    ss << "sent pg creates ";
    r = 0;
  } else if (prefix == "pg dump") {
    string val;
    vector<string> dumpcontents;
    set<string> what;
    if (cmd_getval(g_ceph_context, cmdmap, "dumpcontents", dumpcontents)) {
      copy(dumpcontents.begin(), dumpcontents.end(),
	   inserter(what, what.end()));
    }
    if (what.empty())
      what.insert("all");
    if (f) {
      vector<string> dumpcontents;
      if (cmd_getval(g_ceph_context, cmdmap, "dumpcontents", dumpcontents)) {
	copy(dumpcontents.begin(), dumpcontents.end(),
	     inserter(what, what.end()));
      }
      if (what.count("all")) {
	f->open_object_section("pg_map");
	pg_map.dump(f.get());
	f->close_section();
      } else if (what.count("summary") || what.count("sum")) {
	f->open_object_section("pg_map");
	pg_map.dump_basic(f.get());
	f->close_section();
      } else {
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(f.get());
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(f.get());
	}
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(f.get());
	}
      }
      f->flush(ds);
    } else {
      // plain format ignores dumpcontents
      pg_map.dump(ds);
    }
    ss << "dumped " << what << " in format " << format;
    r = 0;
  } else if (prefix == "pg dump_stuck") {
    vector<string> stuckop_vec;
    cmd_getval(g_ceph_context, cmdmap, "stuckops", stuckop_vec);
    if (stuckop_vec.empty())
      stuckop_vec.push_back("unclean");
    int64_t threshold;
    cmd_getval(g_ceph_context, cmdmap, "threshold", threshold,
	       int64_t(g_conf->mon_pg_stuck_threshold));

    boost::scoped_ptr<Formatter> f(new_formatter("json"));
    r = dump_stuck_pg_stats(ds, f.get(), (int)threshold, stuckop_vec);
    ss << "ok";
    r = 0;
  } else if (prefix == "pg map") {
    pg_t pgid;
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      r = -EINVAL;
      goto reply;
    }
    vector<int> up, acting;
    if (!pgosdmap->have_pg_pool(pgid.pool())) {
      r = -ENOENT;
      ss << "pg '" << pgidstr << "' does not exist";
      goto reply;
    }
    pg_t mpgid = pgosdmap->raw_pg_to_pg(pgid);
    pgosdmap->pg_to_up_acting_osds(pgid, up, acting);
    ds << "osdmap e" << mon->osdmon()->osdmap->get_epoch()
       << " pg " << pgid << " (" << mpgid << ")"
       << " -> up " << up << " acting " << acting;
    r = 0;
  } else if (prefix == "pg scrub" || 
	     prefix == "pg repair" || 
	     prefix == "pg deep-scrub") {
    string scrubop = prefix.substr(3, string::npos);
    pg_t pgid;
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      r = -EINVAL;
      goto reply;
    }
    if (!pg_map.pg_stat.count(pgid)) {
      ss << "pg " << pgid << " dne";
      r = -ENOENT;
      goto reply;
    }
    if (!pg_map.pg_stat[pgid].acting.size()) {
      ss << "pg " << pgid << " has no primary osd";
      r = -EAGAIN;
      goto reply;
    }
    int osd = pg_map.pg_stat[pgid].acting[0];
    if (!mon->osdmon()->osdmap->is_up(osd)) {
      ss << "pg " << pgid << " primary osd." << osd << " not up";
      r = -EAGAIN;
      goto reply;
    }
    vector<pg_t> pgs(1);
    pgs[0] = pgid;
    mon->try_send_message(new MOSDScrub(mon->monmap->fsid, pgs,
					scrubop == "repair",
					scrubop == "deep-scrub"),
			  mon->osdmon()->osdmap->get_inst(osd));
    ss << "instructing pg " << pgid << " on osd." << osd << " to " << scrubop;
    r = 0;
  } else if (prefix == "pg debug") {
    string debugop;
    cmd_getval(g_ceph_context, cmdmap, "debugop", debugop, string("unfound_objects_exist"));
    if (debugop == "unfound_objects_exist") {
      bool unfound_objects_exist = false;
      hash_map<pg_t,pg_stat_t>::const_iterator end = pg_map.pg_stat.end();
      for (hash_map<pg_t,pg_stat_t>::const_iterator s = pg_map.pg_stat.begin();
	   s != end; ++s) {
	if (s->second.stats.sum.num_objects_unfound > 0) {
	  unfound_objects_exist = true;
	  break;
	}
      }
      if (unfound_objects_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      r = 0;
    } else if (debugop == "degraded_pgs_exist") {
      bool degraded_pgs_exist = false;
      hash_map<pg_t,pg_stat_t>::const_iterator end = pg_map.pg_stat.end();
      for (hash_map<pg_t,pg_stat_t>::const_iterator s = pg_map.pg_stat.begin();
	   s != end; ++s) {
	if (s->second.stats.sum.num_objects_degraded > 0) {
	  degraded_pgs_exist = true;
	  break;
	}
      }
      if (degraded_pgs_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      r = 0;
    }
  }

  if (r == -1)
    return false;

 reply:
  string rs;
  getline(ss, rs);
  rdata.append(ds);
  mon->reply_command(m, r, rs, rdata, get_version());
  return true;
}

bool PGMonitor::prepare_command(MMonCommand *m)
{
  PGOSDMap* const pgosdmap =
    dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());
  stringstream ss;
  pg_t pgid;
  epoch_t epoch = pgosdmap->get_epoch();
  int r = 0;
  string rs;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("pg", MON_CAP_W) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", get_version());
    return true;
  }

  if (prefix == "pg force_create_pg") {
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "pg " << pgidstr << " invalid";
      r = -EINVAL;
      goto reply;
    }
    if (!pg_map.pg_stat.count(pgid)) {
      ss << "pg " << pgid << " dne";
      r = -ENOENT;
      goto reply;
    }
    if (pg_map.creating_pgs.count(pgid)) {
      ss << "pg " << pgid << " already creating";
      r = 0;
      goto reply;
    }
    {
      pg_stat_t& s = pending_inc.pg_stat_updates[pgid];
      s.state = PG_STATE_CREATING;
      s.created = epoch;
      s.last_change = ceph_clock_now(g_ceph_context);
    }
    ss << "pg " << pgidstr << " now creating, ok";
    goto update;
  } else if (prefix == "pg set_full_ratio" ||
	     prefix == "pg set_nearfull_ratio") {
    double n;
    cmd_getval(g_ceph_context, cmdmap, "ratio", n);
    string op = prefix.substr(3, string::npos);
    if (op == "set_full_ratio")
      pending_inc.full_ratio = n;
    else if (op == "set_nearfull_ratio")
      pending_inc.nearfull_ratio = n;
    goto update;
  } else {
    r = -EINVAL;
    goto reply;
  }

 reply:
  getline(ss, rs);
  if (r < 0 && rs.length() == 0)
    rs = cpp_strerror(r);
  mon->reply_command(m, r, rs, get_version());
  return false;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, r, rs, get_version()));
  return true;
}

static void note_stuck_detail(enum PGMap::StuckPG what,
			      hash_map<pg_t,pg_stat_t>& stuck_pgs,
			      list<pair<health_status_t,string> > *detail)
{
  for (hash_map<pg_t,pg_stat_t>::iterator p = stuck_pgs.begin();
       p != stuck_pgs.end();
       ++p) {
    ostringstream ss;
    utime_t since;
    const char *whatname = 0;
    switch (what) {
    case PGMap::STUCK_INACTIVE:
      since = p->second.last_active;
      whatname = "inactive";
      break;
    case PGMap::STUCK_UNCLEAN:
      since = p->second.last_clean;
      whatname = "unclean";
      break;
    case PGMap::STUCK_STALE:
      since = p->second.last_unstale;
      whatname = "stale";
      break;
    default:
      assert(0);
    }
    ss << "pg " << p->first << " is stuck " << whatname;
    if (since == utime_t()) {
      ss << " since forever";
    }else {
      utime_t dur = ceph_clock_now(g_ceph_context) - since;
      ss << " for " << dur;
    }
    ss << ", current state " << pg_state_string(p->second.state)
       << ", last acting " << p->second.acting;
    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
  }
}

void PGMonitor::get_health(list<pair<health_status_t,string> >& summary,
			   list<pair<health_status_t,string> > *detail) const
{
  map<string,int> note;
  hash_map<int,int>::const_iterator p = pg_map.num_pg_by_state.begin();
  hash_map<int,int>::const_iterator p_end = pg_map.num_pg_by_state.end();
  for (; p != p_end; ++p) {
    if (p->first & PG_STATE_STALE)
      note["stale"] += p->second;
    if (p->first & PG_STATE_DOWN)
      note["down"] += p->second;
    if (p->first & PG_STATE_DEGRADED)
      note["degraded"] += p->second;
    if (p->first & PG_STATE_INCONSISTENT)
      note["inconsistent"] += p->second;
    if (p->first & PG_STATE_PEERING)
      note["peering"] += p->second;
    if (p->first & PG_STATE_REPAIR)
      note["repair"] += p->second;
    if (p->first & PG_STATE_SPLITTING)
      note["splitting"] += p->second;
    if (p->first & PG_STATE_RECOVERING)
      note["recovering"] += p->second;
    if (p->first & PG_STATE_RECOVERY_WAIT)
      note["recovery_wait"] += p->second;
    if (p->first & PG_STATE_INCOMPLETE)
      note["incomplete"] += p->second;
    if (p->first & PG_STATE_BACKFILL_WAIT)
      note["backfill"] += p->second;
    if (p->first & PG_STATE_BACKFILL)
      note["backfilling"] += p->second;
    if (p->first & PG_STATE_BACKFILL_TOOFULL)
      note["backfill_toofull"] += p->second;
  }

  hash_map<pg_t, pg_stat_t> stuck_pgs;
  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t cutoff = now - utime_t(g_conf->mon_pg_stuck_threshold, 0);

  pg_map.get_stuck_stats(PGMap::STUCK_INACTIVE, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck inactive"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_INACTIVE, stuck_pgs, detail);
  }
  stuck_pgs.clear();

  pg_map.get_stuck_stats(PGMap::STUCK_UNCLEAN, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck unclean"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_UNCLEAN, stuck_pgs, detail);
  }
  stuck_pgs.clear();

  pg_map.get_stuck_stats(PGMap::STUCK_STALE, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck stale"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_STALE, stuck_pgs, detail);
  }

  if (!note.empty()) {
    for (map<string,int>::iterator p = note.begin(); p != note.end(); ++p) {
      ostringstream ss;
      ss << p->second << " pgs " << p->first;
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    }
    if (detail) {
      for (hash_map<pg_t,pg_stat_t>::const_iterator p = pg_map.pg_stat.begin();
	   p != pg_map.pg_stat.end();
	   ++p) {
	if ((p->second.state & (PG_STATE_STALE |
			       PG_STATE_DOWN |
			       PG_STATE_DEGRADED |
			       PG_STATE_INCONSISTENT |
			       PG_STATE_PEERING |
			       PG_STATE_REPAIR |
			       PG_STATE_SPLITTING |
			       PG_STATE_RECOVERING |
			       PG_STATE_RECOVERY_WAIT |
			       PG_STATE_INCOMPLETE |
			       PG_STATE_BACKFILL_WAIT |
			       PG_STATE_BACKFILL |
			       PG_STATE_BACKFILL_TOOFULL)) &&
	    stuck_pgs.count(p->first) == 0) {
	  ostringstream ss;
	  ss << "pg " << p->first << " is " << pg_state_string(p->second.state);
	  ss << ", acting " << p->second.acting;
	  if (p->second.stats.sum.num_objects_unfound)
	    ss << ", " << p->second.stats.sum.num_objects_unfound << " unfound";
	  if (p->second.state & PG_STATE_INCOMPLETE) {
	    PGOSDMap* const l_osdmap =
	      dynamic_cast<PGOSDMap*>(mon->osdmon()->osdmap.get());
	    const pg_pool_t *pi = l_osdmap->get_pg_pool(p->first.pool());
	    if (pi && pi->min_size > 1) {
	      ss << " (reducing pool " << l_osdmap->get_pool_name(p->first.pool())
		 << " min_size from " << (int)pi->min_size << " may help; search ceph.com/docs for 'incomplete')";
	    }
	  }
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }
  }

  stringstream rss;
  pg_map.recovery_summary(rss);
  if (!rss.str().empty()) {
    summary.push_back(make_pair(HEALTH_WARN, "recovery " + rss.str()));
    if (detail)
      detail->push_back(make_pair(HEALTH_WARN, "recovery " + rss.str()));
  }
  
  check_full_osd_health(summary, detail, pg_map.full_osds, "full", HEALTH_ERR);
  check_full_osd_health(summary, detail, pg_map.nearfull_osds, "near full", HEALTH_WARN);

  if (pg_map.pg_sum.stats.sum.num_scrub_errors) {
    ostringstream ss;
    ss << pg_map.pg_sum.stats.sum.num_scrub_errors << " scrub errors";
    summary.push_back(make_pair(HEALTH_ERR, ss.str()));
    if (detail) {
      detail->push_back(make_pair(HEALTH_ERR, ss.str()));
    }
  }
}

void PGMonitor::check_full_osd_health(list<pair<health_status_t,string> >& summary,
				      list<pair<health_status_t,string> > *detail,
				      const set<int>& s, const char *desc,
				      health_status_t sev) const
{
  if (!s.empty()) {
    ostringstream ss;
    ss << s.size() << " " << desc << " osd(s)";
    summary.push_back(make_pair(sev, ss.str()));
    if (detail) {
      for (set<int>::const_iterator p = s.begin(); p != s.end(); ++p) {
	ostringstream ss;
	const osd_stat_t& os = pg_map.osd_stat.find(*p)->second;
	int ratio = (int)(((float)os.kb_used) / (float) os.kb * 100.0);
	ss << "osd." << *p << " is " << desc << " at " << ratio << "%";
	detail->push_back(make_pair(sev, ss.str()));
      }
    }
  }
}

int PGMonitor::dump_stuck_pg_stats(stringstream &ds,
				   Formatter *f,
				   int threshold,
				   vector<string>& args) const
{
  PGMap::StuckPG stuck_type;
  string type = args[0];
  if (type == "inactive")
    stuck_type = PGMap::STUCK_INACTIVE;
  if (type == "unclean")
    stuck_type = PGMap::STUCK_UNCLEAN;
  if (type == "stale")
    stuck_type = PGMap::STUCK_STALE;

  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t cutoff = now - utime_t(threshold, 0);

  if (!f) {
    pg_map.dump_stuck_plain(ds, stuck_type, cutoff);
  } else {
    pg_map.dump_stuck(f, stuck_type, cutoff);
    f->flush(ds);
  }

  return 0;
}

void PGMonitor::check_sub(Subscription *sub)
{
  if (sub->type == "osd_pg_creates") {
    send_pg_creates(sub->session->inst.name.num(),
		    sub->session->con.get());
  }
}
