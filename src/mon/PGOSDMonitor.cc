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

#include "PGOSDMonitor.h"
#include "mon/PGMonitor.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MMonCommand.h"
#include "common/errno.h"
#include "pg/PGPlaceSystem.h"
#include "Monitor.h"
#include "PGMonitor.h"
#include "pg/PGOSDMap.h"

#include "MonitorDBStore.h"

#include "osd/PlaceSystem.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"

#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MMonCommand.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MOSDScrub.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/strtol.h"

#include "common/config.h"
#include "common/errno.h"

#include "include/compat.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "include/util.h"


const PGOSDMonitorPlaceSystem placeSystem(PGPlaceSystem::systemName,
					  PGPlaceSystem::systemIdentifier);


#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, auto_ptr<OSDMap> osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap->get_epoch() << " ";
}



PGOSDMonitor::PGOSDMonitor(Monitor *mn, Paxos *p, const string& service_name)
  : OSDMonitor(mn, p, service_name)
{
  osdmap.reset(newOSDMap());
  pending_inc.reset(new PGOSDMap::Incremental());
}

void PGOSDMonitor::update_trim()
{
  if (mon->pgmon()->is_readable() &&
      mon->pgmon()->pg_map.creating_pgs.empty()) {
    epoch_t floor = mon->pgmon()->pg_map.calc_min_last_epoch_clean();
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    unsigned min = g_conf->mon_min_osdmap_epochs;
    if (floor + min > get_version()) {
      if (min < get_version())
	floor = get_version() - min;
      else
	floor = 0;
    }
    if (floor > get_first_committed())
      if (get_trim_to() < floor)
	set_trim_to(floor);
  }
}

void PGOSDMonitor::create_pending()
{
  pending_inc.reset(new PGOSDMap::Incremental(osdmap->epoch+1));
  create_pending_super();

  // drop any redundant pg_temp entries
  remove_redundant_pg_temp();
}


void PGOSDMonitor::remove_redundant_pg_temp()
{
  PGOSDMap* const pgosdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  dout(10) << "remove_redundant_pg_temp" << dendl;

  PGOSDMap::Incremental* l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  for (map<pg_t,vector<int> >::iterator p = pgosdmap->pg_temp->begin();
       p != pgosdmap->pg_temp->end();
       ++p) {
    if (l_pending_inc->new_pg_temp.count(p->first) == 0) {
      vector<int> raw_up;
      pgosdmap->pg_to_raw_up(p->first, raw_up);
      if (raw_up == p->second) {
	dout(10) << " removing unnecessary pg_temp " << p->first << " -> " << p->second << dendl;
	l_pending_inc->new_pg_temp[p->first].clear();
      }
    }
  }
}


// -------------
// pg_temp changes

void PGOSDMonitor::remove_down_pg_temp()
{
  dout(10) << "remove_down_pg_temp" << dendl;
  PGOSDMap* l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap tmpmap(*l_osdmap);
  PGOSDMap::Incremental* l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());
  tmpmap.apply_incremental(*l_pending_inc);

  for (map<pg_t,vector<int> >::iterator p = tmpmap.pg_temp->begin();
       p != tmpmap.pg_temp->end();
       ++p) {
    unsigned num_up = 0;
    for (vector<int>::iterator i = p->second.begin();
	 i != p->second.end();
	 ++i) {
      if (!tmpmap.is_down(*i))
	++num_up;
    }
    if (num_up == 0)
      l_pending_inc->new_pg_temp[p->first].clear();
  }
}

// -------------
// pg_temp changes

bool PGOSDMonitor::preprocess_pgtemp(MOSDPGTemp *m)
{
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  vector<int> empty;
  int from = m->get_orig_source().num();
  PGOSDMap* pgosdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  // check caps
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDPGTemp from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap->is_up(from) ||
      osdmap->get_inst(from) != m->get_orig_source_inst()) {
    dout(7) << "ignoring pgtemp message from down " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
    dout(20) << " " << p->first
	     << (pgosdmap->pg_temp->count(p->first) ? (*pgosdmap->pg_temp)[p->first] : empty)
	     << " -> " << p->second << dendl;
    // removal?
    if (p->second.empty() && pgosdmap->pg_temp->count(p->first))
      return false;
    // change?
    if (p->second.size() && (pgosdmap->pg_temp->count(p->first) == 0 ||
			     (*pgosdmap->pg_temp)[p->first] != p->second))
      return false;
  }

  dout(7) << "preprocess_pgtemp e" << m->map_epoch << " no changes from " << m->get_orig_source_inst() << dendl;
  _reply_map(m, m->map_epoch);
  return true;

 ignore:
  m->put();
  return true;
}

bool PGOSDMonitor::prepare_pgtemp(MOSDPGTemp *m)
{
  PGOSDMap::Incremental* l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());
  int from = m->get_orig_source().num();
  dout(7) << "prepare_pgtemp e" << m->map_epoch << " from "
	  << m->get_orig_source_inst() << dendl;
  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin();
       p != m->pg_temp.end(); ++p)
    l_pending_inc->new_pg_temp[p->first] = p->second;
  // set up_thru too, so the osd doesn't have to ask again
  l_pending_inc->new_up_thru[from] = m->map_epoch;
  wait_for_finished_proposal(new C_ReplyMap(this, m, m->map_epoch));
  return true;
}

int PGOSDMonitor::prepare_new_pool(MPoolOp *m)
{
  dout(10) << "prepare_new_pool from " << m->get_connection() << dendl;
  MonSession *session = m->get_session();
  if (!session)
    return -EPERM;
  if (m->auid)
    return prepare_new_pool(m->name, m->auid, m->crush_rule, 0, 0);
  else
    return prepare_new_pool(m->name, session->auid, m->crush_rule, 0, 0);
}


/**
 * @param name The name of the new pool
 * @param auid The auid of the pool owner. Can be -1
 * @param crush_rule The crush rule to use. If <0, will use the system default
 * @param pg_num The pg_num to use. If set to 0, will use the system default
 * @param pgp_num The pgp_num to use. If set to 0, will use the system default
 *
 * @return 0 in all cases. That's silly.
 */
int PGOSDMonitor::prepare_new_pool(string& name, uint64_t auid, int crush_rule,
				   unsigned pg_num, unsigned pgp_num)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  for (map<int64_t,string>::iterator p = l_pending_inc->new_pool_names.begin();
       p != l_pending_inc->new_pool_names.end();
       ++p) {
    if (p->second == name)
      return 0;
  }

  if (-1 == l_pending_inc->new_pool_max)
    l_pending_inc->new_pool_max = l_osdmap->pool_max;
  int64_t pool = ++l_pending_inc->new_pool_max;
  l_pending_inc->new_pools[pool].type = pg_pool_t::TYPE_REP;
  l_pending_inc->new_pools[pool].flags = g_conf->osd_pool_default_flags;
  if (g_conf->osd_pool_default_flag_hashpspool)
    l_pending_inc->new_pools[pool].flags |= pg_pool_t::FLAG_HASHPSPOOL;

  l_pending_inc->new_pools[pool].size = g_conf->osd_pool_default_size;
  l_pending_inc->new_pools[pool].min_size = g_conf->get_osd_pool_default_min_size();
  if (crush_rule >= 0)
    l_pending_inc->new_pools[pool].crush_ruleset = crush_rule;
  else
    l_pending_inc->new_pools[pool].crush_ruleset = g_conf->osd_pool_default_crush_rule;
  l_pending_inc->new_pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
  l_pending_inc->new_pools[pool].set_pg_num(pg_num ? pg_num : g_conf->osd_pool_default_pg_num);
  l_pending_inc->new_pools[pool].set_pgp_num(pgp_num ? pgp_num : g_conf->osd_pool_default_pgp_num);
  l_pending_inc->new_pools[pool].last_change = pending_inc->epoch;
  l_pending_inc->new_pools[pool].auid = auid;
  l_pending_inc->new_pool_names[pool] = name;
  return 0;
}


bool PGOSDMonitor::preprocess_query_sub(PaxosServiceMessage *m)
{
  switch (m->get_type()) {
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp(static_cast<MOSDPGTemp*>(m));

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op(static_cast<MPoolOp*>(m));

  default:
    assert(0);
    m->put();
    return true;
  }
}


bool PGOSDMonitor::prepare_update_sub(PaxosServiceMessage *m)
{
  switch (m->get_type()) {
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp(static_cast<MOSDPGTemp*>(m));

  case CEPH_MSG_POOLOP:
    return prepare_pool_op(static_cast<MPoolOp*>(m));

  default:
    assert(0);
    m->put();
    return true;
  }
}


bool PGOSDMonitor::preprocess_remove_snaps_sub(class MRemoveSnaps *m) {
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  for (map<int, vector<snapid_t> >::iterator q = m->snaps.begin();
       q != m->snaps.end();
       ++q) {
    if (!l_osdmap->have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second
	       << " on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = l_osdmap->get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin();
	 p != q->second.end();
	 ++p) {
      if (*p > pi->get_snap_seq() ||
	  !pi->removed_snaps.contains(*p))
	return false;
    }
  }

  return true;
}


bool PGOSDMonitor::prepare_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "prepare_remove_snaps " << *m << dendl;

  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  for (map<int, vector<snapid_t> >::iterator p = m->snaps.begin();
       p != m->snaps.end();
       ++p) {
    pg_pool_t& pi = l_osdmap->pools[p->first];
    for (vector<snapid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      if (!pi.removed_snaps.contains(*q) &&
	  (!l_pending_inc->new_pools.count(p->first) ||
	   !l_pending_inc->new_pools[p->first].removed_snaps.contains(*q))) {
	if (l_pending_inc->new_pools.count(p->first) == 0)
	  l_pending_inc->new_pools[p->first] = pi;
	pg_pool_t& newpi = l_pending_inc->new_pools[p->first];
	newpi.removed_snaps.insert(*q);
	dout(10) << " pool " << p->first << " removed_snaps added " << *q
		 << " (now " << newpi.removed_snaps << ")" << dendl;
	if (*q > newpi.get_snap_seq()) {
	  dout(10) << " pool " << p->first << " snap_seq "
		   << newpi.get_snap_seq() << " -> " << *q << dendl;
	  newpi.set_snap_seq(*q);
	}
	newpi.set_snap_epoch(l_pending_inc->epoch);
      }
    }
  }

  m->put();
  return true;
}

void PGOSDMonitor::tick_sub(bool& do_propose)
{
  const PGOSDMap* l_osdmap = static_cast<const PGOSDMap*>(osdmap.get());
  const PGOSDMap::Incremental* l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());
  int type = l_osdmap->crush->get_type_id(g_conf->mon_osd_down_out_subtree_limit);
  utime_t now = ceph_clock_now(g_ceph_context);
  set<int> down_cache;  // quick cache of down subtrees
  map<int,utime_t>::iterator i = down_pending_out.begin();

  while (i != down_pending_out.end()) {
    int o = i->first;
    utime_t down = now;
    down -= i->second;
    ++i;
    // is this an entire large subtree down?
    if (g_conf->mon_osd_down_out_subtree_limit.length()) {
      if (type > 0) {
	if (l_osdmap->containing_subtree_is_down(g_ceph_context, o, type, &down_cache)) {
	  dout(10) << "tick entire containing " << g_conf->mon_osd_down_out_subtree_limit
		   << " subtree for osd." << o << " is down; resetting timer" << dendl;
	  // reset timer, too.
	  down_pending_out[o] = now;
	  continue;
	}
      }
    }
  }


  //if map full setting has changed, get that info out there!
  if (mon->pgmon()->is_readable()) {
    if (!mon->pgmon()->pg_map.full_osds.empty()) {
      dout(5) << "There are full osds, setting full flag" << dendl;
      add_flag(CEPH_OSDMAP_FULL);
    } else if (l_osdmap->test_flag(CEPH_OSDMAP_FULL)){
      dout(10) << "No full osds, removing full flag" << dendl;
      remove_flag(CEPH_OSDMAP_FULL);
    }
    if (l_pending_inc->new_flags != -1 &&
	(l_pending_inc->new_flags ^ l_osdmap->flags) & CEPH_OSDMAP_FULL) {
      dout(1) << "New setting for CEPH_OSDMAP_FULL -- doing propose" << dendl;
      do_propose = true;
    }
  }
  // ---------------
#define SWAP_PRIMARIES_AT_START 0
#define SWAP_TIME 1
#if 0
  if (SWAP_PRIMARIES_AT_START) {
    // For all PGs that have OSD 0 as the primary,
    // switch them to use the first replca
    ps_t numps = osdmap.get_pg_num();
    for (int64_t pool=0; pool<1; pool++)
      for (ps_t ps = 0; ps < numps; ++ps) {
	pg_t pgid = pg_t(pg_t::TYPE_REP, ps, pool, -1);
	vector<int> osds;
	osdmap.pg_to_osds(pgid, osds);
	if (osds[0] == 0) {
	  pending_inc.new_pg_swap_primary[pgid] = osds[1];
	  dout(3) << "Changing primary for PG " << pgid << " from "
		  << osds[0] << " to " << osds[1] << dendl;
	  do_propose = true;
	}
      }
  }
#endif
  // ---------------

  if (update_pools_status())
    do_propose = true;

  if (do_propose ||
      !l_pending_inc->new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();

  if (mon->pgmon()->is_readable() &&
      mon->pgmon()->pg_map.creating_pgs.empty()) {
    epoch_t floor = mon->pgmon()->pg_map.calc_min_last_epoch_clean();
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    unsigned min = g_conf->mon_min_osdmap_epochs;
    if (floor + min > paxos->get_version()) {
      if (min < get_version())
	floor = get_version() - min;
      else
	floor = 0;
    }
    if (floor > get_first_committed())
      set_trim_to(floor);
  }
}


void PGOSDMonitor::update_pool_flags(int64_t pool_id, uint64_t flags)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());
  const pg_pool_t *pool = l_osdmap->get_pg_pool(pool_id);
  if (l_pending_inc->new_pools.count(pool_id) == 0)
    l_pending_inc->new_pools[pool_id] = *pool;
  l_pending_inc->new_pools[pool_id].flags = flags;
}

bool PGOSDMonitor::update_pools_status()
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  if (!mon->pgmon()->is_readable())
    return false;

  bool ret = false;

  const map<int64_t,pg_pool_t>& pools = l_osdmap->get_pools();
  for (map<int64_t,pg_pool_t>::const_iterator it = pools.begin();
       it != pools.end();
       ++it) {
    if (!mon->pgmon()->pg_map.pg_pool_sum.count(it->first))
      continue;
    pool_stat_t& stats = mon->pgmon()->pg_map.pg_pool_sum[it->first];
    object_stat_sum_t& sum = stats.stats.sum;
    const pg_pool_t &pool = it->second;
    const char *pool_name = l_osdmap->get_pool_name(it->first);

    bool pool_is_full =
      (pool.quota_max_bytes > 0 && (uint64_t)sum.num_bytes >= pool.quota_max_bytes) ||
      (pool.quota_max_objects > 0 && (uint64_t)sum.num_objects >= pool.quota_max_objects);

    if (pool.get_flags() & pg_pool_t::FLAG_FULL) {
      if (pool_is_full)
        continue;

      mon->clog.info() << "pool '" << pool_name
                       << "' no longer full; removing FULL flag";

      update_pool_flags(it->first, pool.get_flags() & ~pg_pool_t::FLAG_FULL);
      ret = true;
    } else {
      if (!pool_is_full)
	continue;

      if (pool.quota_max_bytes > 0 &&
          (uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
        mon->clog.warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_bytes: "
                         << si_t(pool.quota_max_bytes) << ")";
      } else if (pool.quota_max_objects > 0 &&
		 (uint64_t)sum.num_objects >= pool.quota_max_objects) {
        mon->clog.warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_objects: "
                         << pool.quota_max_objects << ")";
      } else {
        assert(0 == "we shouldn't reach this");
      }
      update_pool_flags(it->first, pool.get_flags() | pg_pool_t::FLAG_FULL);
      ret = true;
    }
  }
  return ret;
}

void PGOSDMonitor::get_health_sub(
  list<pair<health_status_t,string> >& summary,
  list<pair<health_status_t,string> > *detail) const
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  const map<int64_t,pg_pool_t>& pools = l_osdmap->get_pools();
  for (map<int64_t,pg_pool_t>::const_iterator it = pools.begin();
       it != pools.end(); ++it) {
    if (!mon->pgmon()->pg_map.pg_pool_sum.count(it->first))
      continue;
    pool_stat_t& stats = mon->pgmon()->pg_map.pg_pool_sum[it->first];
    object_stat_sum_t& sum = stats.stats.sum;
    const pg_pool_t &pool = it->second;
    const char *pool_name = l_osdmap->get_pool_name(it->first);

    if (pool.get_flags() & pg_pool_t::FLAG_FULL) {
      // uncomment these asserts if/when we update the FULL flag on pg_stat update
      //assert((pool.quota_max_objects > 0) || (pool.quota_max_bytes > 0));

      stringstream ss;
      ss << "pool '" << pool_name << "' is full";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    float warn_threshold = g_conf->mon_pool_quota_warn_threshold/100;
    float crit_threshold = g_conf->mon_pool_quota_crit_threshold/100;

    if (pool.quota_max_objects > 0) {
      stringstream ss;
      health_status_t status = HEALTH_OK;
      if ((uint64_t)sum.num_objects >= pool.quota_max_objects) {
	// uncomment these asserts if/when we update the FULL flag on pg_stat update
        //assert(pool.get_flags() & pg_pool_t::FLAG_FULL);
      } else if (crit_threshold > 0 &&
		 sum.num_objects >= pool.quota_max_objects*crit_threshold) {
        ss << "pool '" << pool_name
           << "' has " << sum.num_objects << " objects"
           << " (max " << pool.quota_max_objects << ")";
        status = HEALTH_ERR;
      } else if (warn_threshold > 0 &&
		 sum.num_objects >= pool.quota_max_objects*warn_threshold) {
        ss << "pool '" << pool_name
           << "' has " << sum.num_objects << " objects"
           << " (max " << pool.quota_max_objects << ")";
        status = HEALTH_WARN;
      }
      if (status != HEALTH_OK) {
        pair<health_status_t,string> s(status, ss.str());
        summary.push_back(s);
        if (detail)
          detail->push_back(s);
      }
    }

    if (pool.quota_max_bytes > 0) {
      health_status_t status = HEALTH_OK;
      stringstream ss;
      if ((uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
	// uncomment these asserts if/when we update the FULL flag on pg_stat update
	//assert(pool.get_flags() & pg_pool_t::FLAG_FULL);
      } else if (crit_threshold > 0 &&
		 sum.num_bytes >= pool.quota_max_bytes*crit_threshold) {
        ss << "pool '" << pool_name
           << "' has " << si_t(sum.num_bytes) << " bytes"
           << " (max " << si_t(pool.quota_max_bytes) << ")";
        status = HEALTH_ERR;
      } else if (warn_threshold > 0 &&
		 sum.num_bytes >= pool.quota_max_bytes*warn_threshold) {
        ss << "pool '" << pool_name
           << "' has " << si_t(sum.num_bytes) << " objects"
           << " (max " << si_t(pool.quota_max_bytes) << ")";
        status = HEALTH_WARN;
      }
      if (status != HEALTH_OK) {
        pair<health_status_t,string> s(status, ss.str());
        summary.push_back(s);
        if (detail)
          detail->push_back(s);
      }
    }
  }
}



bool PGOSDMonitor::preprocess_command_sub(MMonCommand *m, int& r,
					  stringstream& ss)
{
  PGOSDMap *l_osdmap = static_cast<PGOSDMap*>(osdmap.get());
  bufferlist rdata;

  if (m->cmd[1] == "map" && m->cmd.size() == 4) {
    int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[2].c_str());
    if (pool < 0) {
      ss << "pool " << m->cmd[2] << " does not exist";
      r = -ENOENT;
    } else {
      object_locator_t oloc(pool);
      object_t oid(m->cmd[3]);
      pg_t pgid = l_osdmap->object_locator_to_pg(oid, oloc);
      pg_t mpgid = l_osdmap->raw_pg_to_pg(pgid);
      vector<int> up, acting;
      l_osdmap->pg_to_up_acting_osds(mpgid, up, acting);
      ss << "osdmap e" << l_osdmap->get_epoch()
	 << " pool '" << m->cmd[2] << "' (" << pool << ") object '" << oid << "' ->"
	 << " pg " << pgid << " (" << mpgid << ")"
	 << " -> up " << up << " acting " << acting;
      r = 0;
    }
  }
  else if (m->cmd[1] == "lspools") {
    uint64_t uid_pools = 0;
    if (m->cmd.size() > 2) {
      uid_pools = strtol(m->cmd[2].c_str(), NULL, 10);
    }
    for (map<int64_t, pg_pool_t>::iterator p = l_osdmap->pools.begin();
	 p != l_osdmap->pools.end();
	 ++p) {
      if (!uid_pools || p->second.auid == uid_pools) {
	ss << p->first << ' ' << l_osdmap->pool_name[p->first] << ',';
      }
    }
    r = 0;
  } else if (m->cmd.size() >= 1 && m->cmd[1] == "crush") {
    if (m->cmd.size() >= 4 && m->cmd[1] == "crush" &&
	m->cmd[2] == "rule" && (m->cmd[3] == "list" ||
				     m->cmd[3] == "ls")) {
      JSONFormatter jf(true);
      jf.open_array_section("rules");
      l_osdmap->crush->list_rules(&jf);
      jf.close_section();
      ostringstream rs;
      jf.flush(rs);
      rs << "\n";
      rdata.append(rs.str());
      r = 0;
    }
    else if (m->cmd.size() >= 4 && m->cmd[1] == "crush" && m->cmd[2] == "rule" && m->cmd[3] == "dump") {
      JSONFormatter jf(true);
      jf.open_array_section("rules");
      l_osdmap->crush->dump_rules(&jf);
      jf.close_section();
      ostringstream rs;
      jf.flush(rs);
      rs << "\n";
      rdata.append(rs.str());
      r = 0;
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "crush" && m->cmd[2] == "dump") {
      JSONFormatter jf(true);
      jf.open_object_section("crush_map");
      l_osdmap->crush->dump(&jf);
      jf.close_section();
      ostringstream rs;
      jf.flush(rs);
      rs << "\n";
      rdata.append(rs.str());
      r = 0;
    }
  } else {
    return false;
  }
  return true;
}


bool PGOSDMonitor::prepare_command_sub(MMonCommand *m,
				       int& err,
				       stringstream& ss,
				       string& rs)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  if (m->cmd.size() > 1) {
    if ((m->cmd.size() == 2 && m->cmd[1] == "setcrushmap") ||
	(m->cmd.size() == 3 && m->cmd[1] == "crush" && m->cmd[2] == "set")) {
      dout(10) << "prepare_command setting new crush map" << dendl;
      bufferlist data(m->get_data());
      CrushWrapper crush;
      try {
	bufferlist::iterator bl(data.begin());
	crush.decode(bl);
      }
      catch (const std::exception &e) {
	err = -EINVAL;
	ss << "Failed to parse crushmap: " << e.what();
	goto out;
      }

      if (crush.get_max_devices() > osdmap->get_max_osd()) {
	err = -ERANGE;
	ss << "crushmap max_devices " << crush.get_max_devices()
	   << " > osdmap max_osd " << osdmap->get_max_osd();
	goto out;
      }

      // sanity check: test some inputs to make sure this map isn't totally broken
      dout(10) << " testing map" << dendl;
      stringstream ess;
      CrushTester tester(crush, ess, 1);
      tester.test();
      dout(10) << " result " << ess.str() << dendl;

      l_pending_inc->crush = data;
      string rs = "set crush map";
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
    else if (m->cmd.size() == 5 && m->cmd[1] == "crush" && (m->cmd[2] == "add-bucket")) {
      do {
	// osd crush add-bucket <name> <type>
	if (!_have_pending_crush() &&
	    _get_stable_crush().name_exists(m->cmd[3])) {
	  ss << "bucket '" << m->cmd[3] << "' already exists";
	  err = 0;
	  break;
	}

	CrushWrapper newcrush;
	_get_pending_crush(newcrush);
	if (newcrush.name_exists(m->cmd[3])) {
	  ss << "bucket '" << m->cmd[3] << "' already exists";
	  err = 0;
	} else {
	  int type = newcrush.get_type_id(m->cmd[4]);
	  if (type <= 0) {
	    ss << "type '" << m->cmd[4] << "' does not exist";
	    err = -EINVAL;
	    break;
	  }
	  int bucketno = newcrush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT,
					     type, 0, NULL, NULL);
	  newcrush.set_item_name(bucketno, m->cmd[3]);

	  l_pending_inc->crush.clear();
	  newcrush.encode(l_pending_inc->crush);
	  ss << "added bucket " << m->cmd[3] << " type " << m->cmd[4] << " to crush map";
	}
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      } while (false);
    }
    else if (m->cmd.size() >= 5 && m->cmd[1] == "crush" && (m->cmd[2] == "set" ||
							    m->cmd[2] == "add")) {
      do {
	// osd crush set <osd-id> [<osd.* name>] <weight> <loc1> [<loc2> ...]
	int id = parse_osd_id(m->cmd[3].c_str(), &ss);
	if (id < 0) {
	  err = -EINVAL;
	  goto out;
	}
	if (!l_osdmap->exists(id)) {
	  err = -ENOENT;
	  ss << "osd." << m->cmd[3] << " does not exist.  create it before updating the crush map";
	  goto out;
	}

	int argpos = 4;
	string name;
	if (m->cmd[argpos].find("osd.") == 0) {
	  // old annoying usage, explicitly specifying osd.NNN name
	  name = m->cmd[argpos];
	  argpos++;
	} else {
	  // new usage; infer name
	  name = "osd." + stringify(id);
	}
	float weight = atof(m->cmd[argpos].c_str());
	argpos++;
	map<string,string> loc;
	parse_loc_map(m->cmd, argpos, &loc);

	dout(0) << "adding/updating crush item id " << id << " name '" << name << "' weight " << weight
		<< " at location " << loc << dendl;

	CrushWrapper newcrush;
	_get_pending_crush(newcrush);

	if (m->cmd[2] == "set") {
	  err = newcrush.update_item(g_ceph_context, id, weight, name, loc);
	} else {
	  err = newcrush.insert_item(g_ceph_context, id, weight, name, loc);
	  if (err == 0)
	    err = 1;
	}
	if (err == 0) {
	  ss << m->cmd[2] << " item id " << id << " name '" << name << "' weight " << weight
	     << " at location " << loc << " to crush map";
	  break;
	}
	if (err > 0) {
	  l_pending_inc->crush.clear();
	  newcrush.encode(l_pending_inc->crush);
	  ss << m->cmd[2] << " item id " << id << " name '" << name << "' weight " << weight
	     << " at location " << loc << " to crush map";
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd.size() >= 5 && m->cmd[1] == "crush" && m->cmd[2] == "create-or-move") {
      do {
	// osd crush create-or-move <id> <initial_weight> <loc1> [<loc2> ...]
	int id = parse_osd_id(m->cmd[3].c_str(), &ss);
	if (id < 0) {
	  err = -EINVAL;
	  goto out;
	}
	if (!l_osdmap->exists(id)) {
	  err = -ENOENT;
	  ss << "osd." << m->cmd[3] << " does not exist.  create it before updating the crush map";
	  goto out;
	}
	string name = "osd." + stringify(id);
	float weight = atof(m->cmd[4].c_str());
	map<string,string> loc;
	parse_loc_map(m->cmd, 5, &loc);

	dout(0) << "create-or-move crush item id " << id << " name '" << name << "' initial_weight " << weight
		<< " at location " << loc << dendl;
	CrushWrapper newcrush;
	_get_pending_crush(newcrush);

	err = newcrush.create_or_move_item(g_ceph_context, id, weight, name, loc);
	if (err == 0) {
	  ss << "create-or-move updated item id " << id << " name '" << name << "' weight " << weight
	     << " at location " << loc << " to crush map";
	  break;
	}
	if (err > 0) {
	  l_pending_inc->crush.clear();
	  newcrush.encode(l_pending_inc->crush);
	  ss << "create-or-move updating item id " << id << " name '" << name << "' weight " << weight
	     << " at location " << loc << " to crush map";
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd.size() >= 5 && m->cmd[1] == "crush" && m->cmd[2] == "move") {
      do {
	// osd crush move <name> <loc1> [<loc2> ...]
	string name = m->cmd[3];
	map<string,string> loc;
	parse_loc_map(m->cmd, 4, &loc);

	dout(0) << "moving crush item name '" << name << "' to location " << loc << dendl;
	CrushWrapper newcrush;
	_get_pending_crush(newcrush);

	if (!newcrush.name_exists(name.c_str())) {
	  err = -ENOENT;
	  ss << "item " << name << " does not exist";
	  break;
	}
	int id = newcrush.get_item_id(name.c_str());

	if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	  err = newcrush.move_bucket(g_ceph_context, id, loc);
	  if (err >= 0) {
	    ss << "moved item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	    pending_inc->crush.clear();
	    newcrush.encode(pending_inc->crush);
	    getline(ss, rs);
	    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	    return true;
	  }
	} else {
	  ss << "no need to move item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  err = 0;
	}
      } while (false);
    }
    else if (m->cmd.size() >= 5 && m->cmd[1] == "crush" && m->cmd[2] == "link") {
      do {
	// osd crush link <name> <loc1> [<loc2> ...]
	string name = m->cmd[3];
	map<string,string> loc;
	parse_loc_map(m->cmd, 4, &loc);
	dout(0) << "linking crush item name '" << name << "' at location " << loc << dendl;
	CrushWrapper newcrush;
	_get_pending_crush(newcrush);

	if (!newcrush.name_exists(name.c_str())) {
	  err = -ENOENT;
	  ss << "item " << name << " does not exist";
	  break;
	}
	int id = newcrush.get_item_id(name.c_str());

	if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	  err = newcrush.link_bucket(g_ceph_context, id, loc);
	  if (err >= 0) {
	    ss << "linked item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	    l_pending_inc->crush.clear();
	    newcrush.encode(l_pending_inc->crush);
	    getline(ss, rs);
	    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	    return true;
	  }
	} else {
	  ss << "no need to move item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  err = 0;
	}
      } while (false);
    }
    else if (m->cmd.size() > 3 && m->cmd[1] == "crush" && (m->cmd[2] == "rm" ||
							   m->cmd[2] == "remove" ||
							   m->cmd[2] == "unlink")) {
      do {
	// osd crush rm <id> [ancestor]
	CrushWrapper newcrush;
	_get_pending_crush(newcrush);

	if (!newcrush.name_exists(m->cmd[3].c_str())) {
	  err = 0;
	  ss << "device '" << m->cmd[3] << "' does not appear in the crush map";
	  break;
	}
	int id = newcrush.get_item_id(m->cmd[3].c_str());
	bool unlink_only = m->cmd[2] == "unlink";
	if (m->cmd.size() > 4) {
	  if (!newcrush.name_exists(m->cmd[4])) {
	    err = -ENOENT;
	    ss << "ancestor item '" << m->cmd[4] << "' does not appear in the crush map";
	    break;
	  }
	  int ancestor = newcrush.get_item_id(m->cmd[4]);
	  err = newcrush.remove_item_under(g_ceph_context, id, ancestor, unlink_only);
	} else {
	  err = newcrush.remove_item(g_ceph_context, id, unlink_only);
	}
	if (err == -ENOENT) {
	  ss << "item " << m->cmd[3] << " does not appear in that position";
	  err = 0;
	  break;
	}
	if (err == 0) {
	  pending_inc->crush.clear();
	  newcrush.encode(pending_inc->crush);
	  ss << "removed item id " << id << " name '" << m->cmd[3] << "' from crush map";
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd.size() > 4 && m->cmd[1] == "crush" && m->cmd[2] == "reweight") {
      do {
	// osd crush reweight <name> <weight>
	CrushWrapper newcrush;
	_get_pending_crush(newcrush);

	if (!newcrush.name_exists(m->cmd[3].c_str())) {
	  err = -ENOENT;
	  ss << "device '" << m->cmd[3] << "' does not appear in the crush map";
	  break;
	}

	int id = newcrush.get_item_id(m->cmd[3].c_str());
	if (id < 0) {
	  ss << "device '" << m->cmd[3] << "' is not a leaf in the crush map";
	  break;
	}
	float w = atof(m->cmd[4].c_str());

	err = newcrush.adjust_item_weightf(g_ceph_context, id, w);
	if (err >= 0) {
	  pending_inc->crush.clear();
	  newcrush.encode(l_pending_inc->crush);
	  ss << "reweighted item id " << id << " name '" << m->cmd[3] << "' to " << w
	     << " in crush map";
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd.size() == 4 && m->cmd[1] == "crush" && m->cmd[2] == "tunables") {
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      err = 0;
      if (m->cmd[3] == "legacy" || m->cmd[3] == "argonaut") {
	newcrush.set_tunables_legacy();
      } else if (m->cmd[3] == "bobtail") {
	newcrush.set_tunables_bobtail();
      } else if (m->cmd[3] == "optimal") {
	newcrush.set_tunables_optimal();
      } else if (m->cmd[3] == "default") {
	newcrush.set_tunables_default();
      } else {
	err = -EINVAL;
	ss << "unknown tunables profile '" << m->cmd[3] << "'; allowed values are argonaut, bobtail, optimal, or default";
      }
      if (err == 0) {
	l_pending_inc->crush.clear();
	newcrush.encode(l_pending_inc->crush);
	ss << "adjusted tunables profile to " << m->cmd[3];
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    }
    else if (m->cmd.size() == 7 &&
	     m->cmd[1] == "crush" &&
	     m->cmd[2] == "rule" &&
	     m->cmd[3] == "create-simple") {
      string name = m->cmd[4];
      string root = m->cmd[5];
      string type = m->cmd[6];

      if (l_osdmap->crush->rule_exists(name)) {
	ss << "rule " << name << " already exists";
	err = 0;
	goto out;
      }

      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      if (newcrush.rule_exists(name)) {
	ss << "rule " << name << " already exists";
	err = 0;
      } else {
	int rule = newcrush.add_simple_rule(name, root, type);
	if (rule < 0) {
	  err = rule;
	  goto out;
	}

	l_pending_inc->crush.clear();
	newcrush.encode(l_pending_inc->crush);
      }
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
    else if (m->cmd.size() == 5 &&
	     m->cmd[1] == "crush" &&
	     m->cmd[2] == "rule" &&
	     m->cmd[3] == "rm") {
      string name = m->cmd[4];

      if (!l_osdmap->crush->rule_exists(name)) {
	ss << "rule " << name << " does not exist";
	err = 0;
	goto out;
      }

      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      if (!newcrush.rule_exists(name)) {
	ss << "rule " << name << " does not exist";
	err = 0;
      } else {
	int ruleno = newcrush.get_rule_id(name);
	assert(ruleno >= 0);

	// make sure it is not in use.
	// FIXME: this is ok in some situations, but let's not bother with that
	// complexity now.
	int ruleset = newcrush.get_rule_mask_ruleset(ruleno);
	if (l_osdmap->crush_ruleset_in_use(ruleset)) {
	  ss << "crush rule " << name << " ruleset " << ruleset << " is in use";
	  err = -EBUSY;
	  goto out;
	}

	err = newcrush.remove_rule(ruleno);
	if (err < 0) {
	  goto out;
	}

	l_pending_inc->crush.clear();
	newcrush.encode(l_pending_inc->crush);
      }
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
  }
  if (m->cmd[1] == "pool" && m->cmd.size() >= 3) {
    if (m->cmd.size() >= 5 && m->cmd[2] == "mksnap") {
      int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
      if (pool < 0) {
	ss << "unrecognized pool '" << m->cmd[3] << "'";
	err = -ENOENT;
      } else {
	const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	pg_pool_t *pp = 0;
	if (l_pending_inc->new_pools.count(pool))
	  pp = &l_pending_inc->new_pools[pool];
	const string& snapname = m->cmd[4];
	if (p->snap_exists(snapname.c_str()) ||
	    (pp && pp->snap_exists(snapname.c_str()))) {
	  ss << "pool " << m->cmd[3] << " snap " << snapname << " already exists";
	  err = -EEXIST;
	} else {
	  if (!pp) {
	    pp = &l_pending_inc->new_pools[pool];
	    *pp = *p;
	  }
	  pp->add_snap(snapname.c_str(), ceph_clock_now(g_ceph_context));
	  pp->set_snap_epoch(l_pending_inc->epoch);
	  ss << "created pool " << m->cmd[3] << " snap " << snapname;
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
      }
    }
    else if (m->cmd.size() >= 5 && m->cmd[2] == "rmsnap") {
      int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
      if (pool < 0) {
	ss << "unrecognized pool '" << m->cmd[3] << "'";
	err = -ENOENT;
      } else {
	const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	pg_pool_t *pp = 0;
	if (l_pending_inc->new_pools.count(pool))
	  pp = &l_pending_inc->new_pools[pool];
	const string& snapname = m->cmd[4];
	if (!p->snap_exists(snapname.c_str()) &&
	    (!pp || !pp->snap_exists(snapname.c_str()))) {
	  ss << "pool " << m->cmd[3] << " snap " << snapname << " does not exists";
	  err = -ENOENT;
	} else {
	  if (!pp) {
	    pp = &l_pending_inc->new_pools[pool];
	    *pp = *p;
	  }
	  snapid_t sn = pp->snap_exists(snapname.c_str());
	  pp->remove_snap(sn);
	  pp->set_snap_epoch(l_pending_inc->epoch);
	  ss << "removed pool " << m->cmd[3] << " snap " << snapname;
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
      }
    }
    else if (m->cmd[2] == "create" && m->cmd.size() >= 4) {
      int pg_num = 0;
      int pgp_num = 0;
      if (m->cmd.size() > 4) { // try to parse out pg_num and pgp_num
	const char *start = m->cmd[4].c_str();
	char *end = (char*)start;
	pgp_num = pg_num = strtol(start, &end, 10);
	if (*end != '\0') { // failed to parse
	  err = -EINVAL;
	  ss << "usage: osd pool create <poolname> [pg_num [pgp_num]]";
	  goto out;
	} else if (m->cmd.size() > 5) { // check for pgp_num too
	  start = m->cmd[5].c_str();
	  end = (char *)start;
	  pgp_num = strtol(start, &end, 10);
	  if (*end != '\0') { // failed to parse
	    err = -EINVAL;
	    ss << "usage: osd pool create <poolname> [pg_num [pgp_num]]";
	    goto out;
	  }
	}
      }
      err = prepare_new_pool(m->cmd[3], 0,  // auid=0 for admin created pool
			     -1,            // default crush rule
			     pg_num, pgp_num);
      if (err < 0) {
	if (err == -EEXIST)
	  ss << "pool '" << m->cmd[3] << "' exists";
	goto out;
      }
      ss << "pool '" << m->cmd[3] << "' created";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    } else if (m->cmd[2] == "delete" && m->cmd.size() >= 4) {
      //hey, let's delete a pool!
      int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
      if (pool < 0) {
	ss << "unrecognized pool '" << m->cmd[3] << "'";
	err = -ENOENT;
      } else {
	int ret = _prepare_remove_pool(pool);
	if (ret == 0)
	  ss << "pool '" << m->cmd[3] << "' deleted";
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, ret, rs, paxos->get_version()));
	return true;
      }
    } else if (m->cmd[2] == "rename" && m->cmd.size() == 5) {
      int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
      if (pool < 0) {
	ss << "unrecognized pool '" << m->cmd[3] << "'";
	err = -ENOENT;
      } else if (l_osdmap->lookup_pg_pool_name(m->cmd[4].c_str()) >= 0) {
	ss << "pool '" << m->cmd[4] << "' already exists";
	err = -EEXIST;
      } else {
	int ret = _prepare_rename_pool(pool, m->cmd[4]);
	if (ret == 0) {
	  ss << "pool '" << m->cmd[3] << "' renamed to '" << m->cmd[4] << "'";
	} else {
	  ss << "failed to rename pool '" << m->cmd[3] << "' to '" << m->cmd[4] << "': "
	     << cpp_strerror(ret);
	}
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, ret, rs, paxos->get_version()));
	return true;
      }
    } else if (m->cmd[2] == "set") {
      if (m->cmd.size() != 6) {
	err = -EINVAL;
	ss << "usage: osd pool set <poolname> <field> <value>";
	goto out;
      }
      int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
      if (pool < 0) {
	ss << "unrecognized pool '" << m->cmd[3] << "'";
	err = -ENOENT;
      } else {
	const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	const char *start = m->cmd[5].c_str();
	char *end = (char *)start;
	unsigned n = strtol(start, &end, 10);
	if (*end == '\0') {
	  if (m->cmd[4] == "size") {
	    if (l_pending_inc->new_pools.count(pool) == 0)
	      l_pending_inc->new_pools[pool] = *p;
	    l_pending_inc->new_pools[pool].size = n;
	    l_pending_inc->new_pools[pool].last_change = l_pending_inc->epoch;
	    ss << "set pool " << pool << " size to " << n;
	    getline(ss, rs);
	    paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	    return true;
	  } else if (m->cmd[4] == "crash_replay_interval") {
	    if (l_pending_inc->new_pools.count(pool) == 0)
	      l_pending_inc->new_pools[pool] = *p;
	    l_pending_inc->new_pools[pool].crash_replay_interval = n;
	    ss << "set pool " << pool << " to crash_replay_interval to " << n;
	    getline(ss, rs);
	    paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	    return true;
	  } else if (m->cmd[4] == "pg_num") {
	    if (true) {
	      // ** DISABLE THIS FOR NOW **
	      ss << "pg_num adjustment currently disabled (broken implementation)";
	      // ** DISABLE THIS FOR NOW **
	    } else
	      if (n <= p->get_pg_num()) {
		ss << "specified pg_num " << n << " <= current " << p->get_pg_num();
	      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
		ss << "currently creating pgs, wait";
		err = -EAGAIN;
	      } else {
		if (l_pending_inc->new_pools.count(pool) == 0)
		  l_pending_inc->new_pools[pool] = *p;
		l_pending_inc->new_pools[pool].set_pg_num(n);
		l_pending_inc->new_pools[pool].last_change = l_pending_inc->epoch;
		ss << "set pool " << pool << " pg_num to " << n;
		getline(ss, rs);
		paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
		return true;
	      }
	  } else if (m->cmd[4] == "pgp_num") {
	    if (n > p->get_pg_num()) {
	      ss << "specified pgp_num " << n << " > pg_num " << p->get_pg_num();
	    } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
	      ss << "still creating pgs, wait";
	      err = -EAGAIN;
	    } else {
	      if (l_pending_inc->new_pools.count(pool) == 0)
		l_pending_inc->new_pools[pool] = *p;
	      l_pending_inc->new_pools[pool].set_pgp_num(n);
	      l_pending_inc->new_pools[pool].last_change = l_pending_inc->epoch;
	      ss << "set pool " << pool << " pgp_num to " << n;
	      getline(ss, rs);
	      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	      return true;
	    }
	  } else if (m->cmd[4] == "crush_ruleset") {
	    if (l_osdmap->crush->rule_exists(n)) {
	      if (l_pending_inc->new_pools.count(pool) == 0)
		l_pending_inc->new_pools[pool] = *p;
	      l_pending_inc->new_pools[pool].crush_ruleset = n;
	      l_pending_inc->new_pools[pool].last_change = l_pending_inc->epoch;
	      ss << "set pool " << pool << " crush_ruleset to " << n;
	      getline(ss, rs);
	      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	      return true;
	    } else {
	      ss << "crush ruleset " << n << " dne";
	      err = -ENOENT;
	    }
	  } else {
	    ss << "unrecognized pool field " << m->cmd[4];
	  }
	}
      }
    }
    else if (m->cmd[2] == "get") {
      if (m->cmd.size() != 5) {
	err = -EINVAL;
	ss << "usage: osd pool get <poolname> <field>";
	goto out;
      }
      int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
      if (pool < 0) {
	ss << "unrecognized pool '" << m->cmd[3] << "'";
	err = -ENOENT;
	goto out;
      }

      const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
      if (m->cmd[4] == "pg_num") {
	ss << "PG_NUM: " << p->get_pg_num();
	err = 0;
	goto out;
      }
      if (m->cmd[4] == "pgp_num") {
	ss << "PGP_NUM: " << p->get_pgp_num();
	err = 0;
	goto out;
      }
      ss << "don't know how to get pool field " << m->cmd[4];
      goto out;
    }
    else if (m->cmd[1] == "pool" && m->cmd.size() >= 3) {
      if (m->cmd.size() >= 5 && m->cmd[2] == "mksnap") {
	int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	  pg_pool_t *pp = 0;
	  if (l_pending_inc->new_pools.count(pool))
	    pp = &l_pending_inc->new_pools[pool];
	  const string& snapname = m->cmd[4];
	  if (p->snap_exists(snapname.c_str()) ||
	      (pp && pp->snap_exists(snapname.c_str()))) {
	    ss << "pool " << m->cmd[3] << " snap " << snapname << " already exists";
	    err = 0;
	  } else {
	    if (!pp) {
	      pp = &l_pending_inc->new_pools[pool];
	      *pp = *p;
	    }
	    pp->add_snap(snapname.c_str(), ceph_clock_now(g_ceph_context));
	    pp->set_snap_epoch(l_pending_inc->epoch);
	    ss << "created pool " << m->cmd[3] << " snap " << snapname;
	    getline(ss, rs);
	    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	    return true;
	  }
	}
      }
      else if (m->cmd.size() >= 5 && m->cmd[2] == "rmsnap") {
	int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	  pg_pool_t *pp = 0;
	  if (l_pending_inc->new_pools.count(pool))
	    pp = &l_pending_inc->new_pools[pool];
	  const string& snapname = m->cmd[4];
	  if (!p->snap_exists(snapname.c_str()) &&
	      (!pp || !pp->snap_exists(snapname.c_str()))) {
	    ss << "pool " << m->cmd[3] << " snap " << snapname << " does not exists";
	    err = 0;
	  } else {
	    if (!pp) {
	      pp = &l_pending_inc->new_pools[pool];
	      *pp = *p;
	    }
	    snapid_t sn = pp->snap_exists(snapname.c_str());
	    pp->remove_snap(sn);
	    pp->set_snap_epoch(l_pending_inc->epoch);
	    ss << "removed pool " << m->cmd[3] << " snap " << snapname;
	    getline(ss, rs);
	    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	    return true;
	  }
	}
      }
      else if (m->cmd[2] == "create" && m->cmd.size() >= 3) {
	if (m->cmd.size() < 5) {
	  ss << "usage: osd pool create <poolname> <pg_num> [pgp_num]";
	  err = -EINVAL;
	  goto out;
	}
        int pg_num = 0;
        int pgp_num = 0;

        pg_num = parse_pos_long(m->cmd[4].c_str(), &ss);
        if ((pg_num == 0) || (pg_num > g_conf->mon_max_pool_pg_num)) {
          ss << "'pg_num' must be greater than 0 and less than or equal to "
             << g_conf->mon_max_pool_pg_num
             << " (you may adjust 'mon max pool pg num' for higher values)";
          err = -ERANGE;
          goto out;
        }

        if (pg_num < 0) {
	  err = -EINVAL;
	  goto out;
        }

        pgp_num = pg_num;
        if (m->cmd.size() > 5) {
          pgp_num = parse_pos_long(m->cmd[5].c_str(), &ss);
          if (pgp_num < 0) {
            err = -EINVAL;
            goto out;
          }

          if ((pgp_num == 0) || (pgp_num > pg_num)) {
            ss << "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'"
               << ", which in this case is " << pg_num;
            err = -ERANGE;
            goto out;
          }
        }

	if (l_osdmap->name_pool.count(m->cmd[3])) {
	  ss << "pool '" << m->cmd[3] << "' already exists";
	  err = 0;
	  goto out;
	}

        err = prepare_new_pool(m->cmd[3], 0,  // auid=0 for admin created pool
			       -1,            // default crush rule
			       pg_num, pgp_num);
        if (err < 0 && err != -EEXIST) {
          goto out;
        }
	if (err == -EEXIST) {
	  ss << "pool '" << m->cmd[3] << "' already exists";
	} else {
	  ss << "pool '" << m->cmd[3] << "' created";
	}
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      } else if (m->cmd[2] == "delete" && m->cmd.size() >= 4) {
	// osd pool delete <poolname> <poolname again> --yes-i-really-really-mean-it
	int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "pool '" << m->cmd[3] << "' does not exist";
	  err = 0;
	  goto out;
	}
	if (m->cmd.size() != 6 ||
	    m->cmd[3] != m->cmd[4] ||
	    m->cmd[5] != "--yes-i-really-really-mean-it") {
	  ss << "WARNING: this will *PERMANENTLY DESTROY* all data stored in pool " << m->cmd[3]
	     << ".  If you are *ABSOLUTELY CERTAIN* that is what you want, pass the pool name *twice*, "
	     << "followed by --yes-i-really-really-mean-it.";
	  err = -EPERM;
	  goto out;
	}
	int ret = _prepare_remove_pool(pool);
	if (ret == 0)
	  ss << "pool '" << m->cmd[3] << "' deleted";
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, ret, rs, get_version()));
	return true;
      } else if (m->cmd[2] == "rename" && m->cmd.size() == 5) {
	int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else if (l_osdmap->lookup_pg_pool_name(m->cmd[4].c_str()) >= 0) {
	  ss << "pool '" << m->cmd[4] << "' already exists";
	  err = -EEXIST;
	} else {
	  int ret = _prepare_rename_pool(pool, m->cmd[4]);
	  if (ret == 0) {
	    ss << "pool '" << m->cmd[3] << "' renamed to '" << m->cmd[4] << "'";
	  } else {
	    ss << "failed to rename pool '" << m->cmd[3] << "' to '" << m->cmd[4] << "': "
	       << cpp_strerror(ret);
	  }
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, ret, rs, get_version()));
	  return true;
	}
      } else if (m->cmd[2] == "set") {
	if (m->cmd.size() < 6) {
	  err = -EINVAL;
	  ss << "usage: osd pool set <poolname> <field> <value>";
	  goto out;
	}
	int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	  const char *start = m->cmd[5].c_str();
	  char *end = (char *)start;
	  unsigned n = strtol(start, &end, 10);
	  if (*end == '\0') {
	    if (m->cmd[4] == "size") {
	      if (n == 0 || n > 10) {
		ss << "pool size must be between 1 and 10";
		err = -EINVAL;
		goto out;
	      }
	      if (l_pending_inc->new_pools.count(pool) == 0)
		l_pending_inc->new_pools[pool] = *p;
	      l_pending_inc->new_pools[pool].size = n;
	      if (n < p->min_size)
		l_pending_inc->new_pools[pool].min_size = n;
	      l_pending_inc->new_pools[pool].last_change
		= l_pending_inc->epoch;
	      ss << "set pool " << pool << " size to " << n;
	      getline(ss, rs);
	      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	      return true;
	    } else if (m->cmd[4] == "min_size") {
	      if (l_pending_inc->new_pools.count(pool) == 0)
		l_pending_inc->new_pools[pool] = *p;
	      l_pending_inc->new_pools[pool].min_size = n;
	      l_pending_inc->new_pools[pool].last_change
		= l_pending_inc->epoch;
	      ss << "set pool " << pool << " min_size to " << n;
	      getline(ss, rs);
	      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	      return true;
	    } else if (m->cmd[4] == "crash_replay_interval") {
	      if (l_pending_inc->new_pools.count(pool) == 0)
		l_pending_inc->new_pools[pool] = *p;
	      l_pending_inc->new_pools[pool].crash_replay_interval = n;
	      ss << "set pool " << pool << " to crash_replay_interval to " << n;
	      getline(ss, rs);
	      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	      return true;
	    } else if (m->cmd[4] == "pg_num") {
	      if (n <= p->get_pg_num()) {
		ss << "specified pg_num " << n << " <= current " << p->get_pg_num();
	      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
		ss << "currently creating pgs, wait";
		err = -EAGAIN;
	      } else {
		if (l_pending_inc->new_pools.count(pool) == 0)
		  l_pending_inc->new_pools[pool] = *p;
		l_pending_inc->new_pools[pool].set_pg_num(n);
		l_pending_inc->new_pools[pool].last_change
		  = l_pending_inc->epoch;
		ss << "set pool " << pool << " pg_num to " << n;
		getline(ss, rs);
		wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
		return true;
	      }
	    } else if (m->cmd[4] == "pgp_num") {
	      if (n > p->get_pg_num()) {
		ss << "specified pgp_num " << n << " > pg_num " << p->get_pg_num();
	      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
		ss << "still creating pgs, wait";
		err = -EAGAIN;
	      } else {
		if (l_pending_inc->new_pools.count(pool) == 0)
		  l_pending_inc->new_pools[pool] = *p;
		l_pending_inc->new_pools[pool].set_pgp_num(n);
		l_pending_inc->new_pools[pool].last_change
		  = pending_inc->epoch;
		ss << "set pool " << pool << " pgp_num to " << n;
		getline(ss, rs);
		wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
		return true;
	      }
	    } else if (m->cmd[4] == "crush_ruleset") {
	      if (l_osdmap->crush->rule_exists(n)) {
		if (l_pending_inc->new_pools.count(pool) == 0)
		  l_pending_inc->new_pools[pool] = *p;
		l_pending_inc->new_pools[pool].crush_ruleset = n;
		l_pending_inc->new_pools[pool].last_change
		  = l_pending_inc->epoch;
		ss << "set pool " << pool << " crush_ruleset to " << n;
		getline(ss, rs);
		wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
		return true;
	      } else {
		ss << "crush ruleset " << n << " does not exist";
		err = -ENOENT;
	      }
            } else {
	      ss << "unrecognized pool field " << m->cmd[4];
	    }
	  }
	}
      } else if (m->cmd[2] == "set-quota" && m->cmd.size() == 6) {
        int64_t pool_id = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
        if (pool_id < 0) {
          ss << "unrecognized pool '" << m->cmd[3] << "'";
          err = -ENOENT;
          goto out;
        }

        string field = m->cmd[4];
        if (field != "max_objects" && field != "max_bytes") {
          ss << "unrecognized field '" << field << "'; max_bytes of max_objects";
          err = -EINVAL;
          goto out;
        }

        stringstream tss;
        int64_t value = unit_to_bytesize(m->cmd[5], &tss);
        if (value < 0) {
          ss << "error parsing value '" << value << "': " << tss.str();
          err = value;
          goto out;
        }

        if (l_pending_inc->new_pools.count(pool_id) == 0)
          l_pending_inc->new_pools[pool_id] = *l_osdmap->get_pg_pool(pool_id);

        if (field == "max_objects") {
          l_pending_inc->new_pools[pool_id].quota_max_objects = value;
        } else if (field == "max_bytes") {
          l_pending_inc->new_pools[pool_id].quota_max_bytes = value;
        } else {
          assert(0 == "unrecognized option");
        }
        ss << "set-quota " << field << " = " << value << " for pool " << m->cmd[3];
        rs = ss.str();
        wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
        return true;
      }
      else if (m->cmd[2] == "get") {
	if (m->cmd.size() != 5) {
	  err = -EINVAL;
	  ss << "usage: osd pool get <poolname> <field>";
	  goto out;
	}
	int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	  goto out;
	}

	const pg_pool_t *p = l_osdmap->get_pg_pool(pool);
	if (m->cmd[4] == "pg_num") {
	  ss << "pg_num: " << p->get_pg_num();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "pgp_num") {
	  ss << "pgp_num: " << p->get_pgp_num();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "size") {
          ss << "size: " << p->get_size();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "min_size") {
	  ss << "min_size: " << p->get_min_size();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "crash_replay_interval") {
	  ss << "crash_replay_interval: " << p->get_crash_replay_interval();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "crush_ruleset") {
	  ss << "crush_ruleset: " << p->get_crush_ruleset();
	  err = 0;
	  goto out;
	}
	ss << "don't know how to get pool field " << m->cmd[4];
	goto out;
      }
    }
  } else {
    ss << "no command?";
  }

 out:
  return false;
}

bool PGOSDMonitor::preprocess_pool_op(MPoolOp *m) 
{
  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(m);

  if (!osdmap.get_pg_pool(m->pool)) {
    dout(10) << "attempt to delete non-existent pool id " << m->pool << dendl;
    _pool_op_reply(m, 0, osdmap.get_epoch());
    return true;
  }
  
  // check if the snap and snapname exists
  bool snap_exists = false;
  const pg_pool_t *p = osdmap.get_pg_pool(m->pool);
  if (p->snap_exists(m->name.c_str()))
    snap_exists = true;
  
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (p->is_unmanaged_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (snap_exists) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_SNAP:
    if (p->is_unmanaged_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (!snap_exists) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (p->is_removed_snap(m->snapid)) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE:
    if (osdmap.lookup_pg_pool_name(m->name.c_str()) >= 0) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_AUID_CHANGE:
    return false;
  default:
    assert(0);
    break;
  }

  return false;
}

bool PGOSDMonitor::preprocess_pool_op_create(MPoolOp *m)
{
  MonSession *session = m->get_session();
  if (!session) {
    _pool_op_reply(m, -EPERM, osdmap.get_epoch());
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_W)) {
    dout(5) << "attempt to create new pool without sufficient auid privileges!"
	    << "message: " << *m  << std::endl
	    << "caps: " << session->caps << dendl;
    _pool_op_reply(m, -EPERM, osdmap.get_epoch());
    return true;
  }

  int64_t pool = osdmap.lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op_reply(m, 0, osdmap.get_epoch());
    return true;
  }

  return false;
}

bool PGOSDMonitor::prepare_pool_op(MPoolOp *m)
{
  dout(10) << "prepare_pool_op " << *m << dendl;
  if (m->op == POOL_OP_CREATE) {
    return prepare_pool_op_create(m);
  } else if (m->op == POOL_OP_DELETE) {
    return prepare_pool_op_delete(m);
  }

  int ret = 0;
  bool changed = false;

  // projected pool info
  pg_pool_t pp;
  if (pending_inc.new_pools.count(m->pool))
    pp = pending_inc.new_pools[m->pool];
  else
    pp = *osdmap.get_pg_pool(m->pool);

  bufferlist reply_data;

  // pool snaps vs unmanaged snaps are mutually exclusive
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
  case POOL_OP_DELETE_SNAP:
    if (pp.is_unmanaged_snaps_mode()) {
      ret = -EINVAL;
      goto out;
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (pp.is_pool_snaps_mode()) {
      ret = -EINVAL;
      goto out;
    }
  }
 
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (!pp.snap_exists(m->name.c_str())) {
      pp.add_snap(m->name.c_str(), ceph_clock_now(g_ceph_context));
      dout(10) << "create snap in pool " << m->pool << " " << m->name << " seq " << pp.get_snap_epoch() << dendl;
      changed = true;
    }
    break;

  case POOL_OP_DELETE_SNAP:
    {
      snapid_t s = pp.snap_exists(m->name.c_str());
      if (s) {
	pp.remove_snap(s);
	changed = true;
      }
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP: 
    {
      uint64_t snapid;
      pp.add_unmanaged_snap(snapid);
      ::encode(snapid, reply_data);
      changed = true;
    }
    break;

  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (!pp.is_removed_snap(m->snapid)) {
      pp.remove_unmanaged_snap(m->snapid);
      changed = true;
    }
    break;

  case POOL_OP_AUID_CHANGE:
    if (pp.auid != m->auid) {
      pp.auid = m->auid;
      changed = true;
    }
    break;

  default:
    assert(0);
    break;
  }

  if (changed) {
    pp.set_snap_epoch(pending_inc.epoch);
    pending_inc.new_pools[m->pool] = pp;
  }

 out:
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, ret, pending_inc.epoch, &reply_data));
  propose_pending();
  return false;
}

bool PGOSDMonitor::prepare_pool_op_create(MPoolOp *m)
{
  int err = prepare_new_pool(m);
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, err, pending_inc.epoch));
  return true;
}

int PGOSDMonitor::_prepare_remove_pool(uint64_t pool)
{
  dout(10) << "_prepare_remove_pool " << pool << dendl;
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_remove_pool " << pool << " pending removal" << dendl;    
    return 0;  // already removed
  }
  pending_inc.old_pools.insert(pool);

  // remove any pg_temp mappings for this pool too
  for (map<pg_t,vector<int32_t> >::iterator p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {
    if (p->first.pool() == pool) {
      dout(10) << "_prepare_remove_pool " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc.new_pg_temp[p->first].clear();
    }
  }
  return 0;
}

int PGOSDMonitor::_prepare_rename_pool(uint64_t pool, string newname)
{
  dout(10) << "_prepare_rename_pool " << pool << dendl;
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_rename_pool " << pool << " pending removal" << dendl;    
    return -ENOENT;
  }
  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == newname) {
      return -EEXIST;
    }
  }

  pending_inc.new_pool_names[pool] = newname;
  return 0;
}

bool PGOSDMonitor::prepare_pool_op_delete(MPoolOp *m)
{
  int ret = _prepare_remove_pool(m->pool);
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, ret, pending_inc.epoch));
  return true;
}

void PGOSDMonitor::_pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp)
{
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, get_version(), blp);
  mon->send_reply(m, reply);
  m->put();
}

bool PGOSDMonitor::_have_pending_crush()
{
  return pending_inc->crush.length();
}

CrushWrapper &PGOSDMonitor::_get_stable_crush()
{
  return *osdmap->crush;
}

void PGOSDMonitor::_get_pending_crush(CrushWrapper& newcrush)
{
  bufferlist bl;
  if (pending_inc->crush.length())
    bl = pending_inc->crush;
  else
    osdmap->crush->encode(bl);

  bufferlist::iterator p = bl.begin();
  newcrush.decode(p);
}

void dump_info_sub(Formatter *f)
{
  PGOSDMap* p = static_cast<PGOSDMap*>(osdmap);
  f->open_object_section("crushmap");
  p->crush->dump(f);
  f->close_section();
}
