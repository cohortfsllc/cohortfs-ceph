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



PGOSDMonitor::PGOSDMonitor(Monitor *mn, Paxos *p)
  : OSDMonitor(mn, p)
{
  osdmap.reset(newOSDMap());
  pending_inc.reset(new PGOSDMap::Incremental());
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
       p++) {
    if (l_pending_inc->new_pg_temp.count(p->first) == 0) {
      vector<int> raw_up;
      pgosdmap->pg_to_raw_up(p->first, raw_up);
      if (raw_up == p->second) {
	dout(10) << " removing unnecessary pg_temp " << p->first <<
	  " -> " << p->second << dendl;
	l_pending_inc->new_pg_temp[p->first].clear();
      }
    }
  }
}



// -------------
// pg_temp changes

bool PGOSDMonitor::preprocess_pgtemp(MOSDPGTemp *m)
{
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  vector<int> empty;

  PGOSDMap* l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  // check caps
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "attempt to send MOSDPGTemp from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); p++) {
    dout(20) << " " << p->first
	     << (l_osdmap->pg_temp->count(p->first) ? (*l_osdmap->pg_temp)[p->first] : empty)
	     << " -> " << p->second << dendl;
    // removal?
    if (p->second.empty() && l_osdmap->pg_temp->count(p->first))
      return false;
    // change?
    if (p->second.size() && (l_osdmap->pg_temp->count(p->first) == 0 ||
			     (*l_osdmap->pg_temp)[p->first] != p->second))
      return false;
  }

  dout(7) << "preprocess_pgtemp e" << m->map_epoch << " no changes from " <<
    m->get_orig_source_inst() << dendl;
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
  dout(7) << "prepare_pgtemp e" << m->map_epoch << " from " <<
    m->get_orig_source_inst() << dendl;
  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin();
       p != m->pg_temp.end();
       p++) {
    l_pending_inc->new_pg_temp[p->first] = p->second;
  }
  // set up_thru too, so the osd doesn't have to ask again
  l_pending_inc->new_up_thru[from] = m->map_epoch;
  paxos->wait_for_commit(new C_ReplyMap(this, m, m->map_epoch));
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
    return prepare_new_pool(m->name, session->caps.auid, m->crush_rule, 0, 0);
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

  if (l_osdmap->name_pool.count(name)) {
    return -EEXIST;
  }
  for (map<int64_t,string>::iterator p = l_pending_inc->new_pool_names.begin();
       p != l_pending_inc->new_pool_names.end();
       ++p) {
    if (p->second == name)
      return -EEXIST;
  }

  if (-1 == l_pending_inc->new_pool_max) {
    l_pending_inc->new_pool_max = l_osdmap->pool_max;
  }
  int64_t pool = ++l_pending_inc->new_pool_max;
  l_pending_inc->new_pools[pool].type = pg_pool_t::TYPE_REP;

  l_pending_inc->new_pools[pool].size = g_conf->osd_pool_default_size;
  if (crush_rule >= 0)
    l_pending_inc->new_pools[pool].crush_ruleset = crush_rule;
  else
    l_pending_inc->new_pools[pool].crush_ruleset = g_conf->osd_pool_default_crush_rule;
  l_pending_inc->new_pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
  l_pending_inc->new_pools[pool].set_pg_num(pg_num ?
					    pg_num :
					    g_conf->osd_pool_default_pg_num);
  l_pending_inc->new_pools[pool].set_pgp_num(pgp_num ?
					     pgp_num :
					     g_conf->osd_pool_default_pgp_num);
  l_pending_inc->new_pools[pool].last_change = l_pending_inc->epoch;
  l_pending_inc->new_pools[pool].auid = auid;
  l_pending_inc->new_pool_names[pool] = name;

  return 0;
}


bool PGOSDMonitor::preprocess_query_sub(PaxosServiceMessage *m)
{
  switch (m->get_type()) {
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp((MOSDPGTemp*)m);
  case CEPH_MSG_POOLOP:
    return preprocess_pool_op((MPoolOp*)m);

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
    return prepare_pgtemp((MOSDPGTemp*)m);

  case CEPH_MSG_POOLOP:
    return prepare_pool_op((MPoolOp*)m);

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
       q++) {
    if (!l_osdmap->have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second <<
	" on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = l_osdmap->get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin(); 
	 p != q->second.end();
	 p++) {
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
       p++) {
    pg_pool_t& pi = l_osdmap->pools[p->first];
    for (vector<snapid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
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
	  dout(10) << " pool " << p->first << " snap_seq " <<
	    newpi.get_snap_seq() << " -> " << *q << dendl;
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
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  //if map full setting has changed, get that info out there!
  if (mon->pgmon()->paxos->is_readable()) {
    if (!mon->pgmon()->pg_map.full_osds.empty()) {
      dout(5) << "There are full osds, setting full flag" << dendl;
      add_flag(CEPH_OSDMAP_FULL);
    } else if (osdmap->test_flag(CEPH_OSDMAP_FULL)){
      dout(10) << "No full osds, removing full flag" << dendl;
      remove_flag(CEPH_OSDMAP_FULL);
    }
    if (l_pending_inc->new_flags != -1 &&
	(l_pending_inc->new_flags ^ osdmap->flags) & CEPH_OSDMAP_FULL) {
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
    ps_t numps = osdmap->get_pg_num();
    for (int64_t pool=0; pool<1; pool++)
      for (ps_t ps = 0; ps < numps; ++ps) {
	pg_t pgid = pg_t(pg_t::TYPE_REP, ps, pool, -1);
	vector<int> osds;
	osdmap->pg_to_osds(pgid, osds); 
	if (osds[0] == 0) {
	  l_pending_inc->new_pg_swap_primary[pgid] = osds[1];
	  dout(3) << "Changing primary for PG " << pgid << " from " << osds[0] << " to "
		  << osds[1] << dendl;
	  do_propose = true;
	}
      }
  }
#endif
  // ---------------

  if (do_propose ||
      !l_pending_inc->new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();

  if (mon->pgmon()->paxos->is_readable() &&
      mon->pgmon()->pg_map.creating_pgs.empty()) {
    epoch_t floor = mon->pgmon()->pg_map.calc_min_last_epoch_clean();
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    unsigned min = g_conf->mon_min_osdmap_epochs;
    if (floor + min > paxos->get_version()) {
      if (min < paxos->get_version())
	floor = paxos->get_version() - min;
      else
	floor = 0;
    }
    if (floor > paxos->get_first_committed())
      paxos->trim_to(floor);
  }    
}


void PGOSDMonitor::preprocess_command_sub(MMonCommand *m, int& r, stringstream& ss)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  if (m->cmd[1] == "map" && m->cmd.size() == 4) {
    int64_t pool = l_osdmap->lookup_pg_pool_name(m->cmd[2].c_str());
    if (pool < 0) {
      ss << "pool " << m->cmd[2] << " dne";
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
  }
}


bool PGOSDMonitor::prepare_command_sub(MMonCommand *m,
				       int& err,
				       stringstream& ss,
				       string& rs)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

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
  } else {
    ss << "no command?";
  }

 out:
  return false;
}


bool PGOSDMonitor::preprocess_pool_op(MPoolOp *m) 
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(m);

  if (!l_osdmap->get_pg_pool(m->pool)) {
    dout(10) << "attempt to delete non-existent pool id " << m->pool << dendl;
    _pool_op_reply(m, -ENOENT, l_osdmap->get_epoch());
    return true;
  }
  
  // check if the snap and snapname exists
  bool snap_exists = false;
  if (l_osdmap->get_pg_pool(m->pool)->snap_exists(m->name.c_str()))
    snap_exists = true;
  
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (snap_exists) {
      _pool_op_reply(m, -EEXIST, l_osdmap->get_epoch());
      return true;
    }
    return false; // continue processing
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    return false; // continue processing
  case POOL_OP_DELETE_SNAP:
    if (!snap_exists) {
      _pool_op_reply(m, -ENOENT, l_osdmap->get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    return false;
  case POOL_OP_DELETE: //can't delete except on master
    return false;
  case POOL_OP_AUID_CHANGE:
    return false; //can't change except on master
  default:
    assert(0);
    break;
  }

  return false;
}

bool PGOSDMonitor::preprocess_pool_op_create(MPoolOp *m)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());

  MonSession *session = m->get_session();
  if (!session) {
    _pool_op_reply(m, -EPERM, l_osdmap->get_epoch());
    return true;
  }
  if ((m->auid &&
       !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, m->auid)) &&
      !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W)) {
    dout(5) << "attempt to create new pool without sufficient auid privileges!"
	    << "message: " << *m  << std::endl
	    << "caps: " << session->caps << dendl;
    _pool_op_reply(m, -EPERM, l_osdmap->get_epoch());
    return true;
  }

  int64_t pool = l_osdmap->lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op_reply(m, -EEXIST, l_osdmap->get_epoch());
    return true;
  }

  return false;
}

bool PGOSDMonitor::prepare_pool_op(MPoolOp *m)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  dout(10) << "prepare_pool_op " << *m << dendl;
  if (m->op == POOL_OP_CREATE) {
    return prepare_pool_op_create(m);
  } else if (m->op == POOL_OP_DELETE) {
    return prepare_pool_op_delete(m);
  } else if (m->op == POOL_OP_AUID_CHANGE) {
    return prepare_pool_op_auid(m);
  }

  bufferlist *blp = NULL;
  int ret = 0;

  // projected pool info
  pg_pool_t pp;
  if (l_pending_inc->new_pools.count(m->pool))
    pp = l_pending_inc->new_pools[m->pool];
  else
    pp = *l_osdmap->get_pg_pool(m->pool);

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
    if (pp.snap_exists(m->name.c_str()))
      ret = -EEXIST;
    else {
      pp.add_snap(m->name.c_str(), ceph_clock_now(g_ceph_context));
      dout(10) << "create snap in pool " << m->pool << " " <<
	m->name << " seq " << pp.get_snap_epoch() << dendl;
    }
    break;

  case POOL_OP_DELETE_SNAP:
    {
      snapid_t s = pp.snap_exists(m->name.c_str());
      if (s)
	pp.remove_snap(s);
      else
	ret = -ENOENT;
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP: 
    {
      blp = new bufferlist();
      uint64_t snapid;
      pp.add_unmanaged_snap(snapid);
      ::encode(snapid, *blp);
    }
    break;

  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (pp.is_removed_snap(m->snapid))
      ret = -ENOENT;
    else
      pp.remove_unmanaged_snap(m->snapid);
    break;

  default:
    assert(0);
    break;
  }

  if (ret == 0) {
    pp.set_snap_epoch(pending_inc->epoch);
    l_pending_inc->new_pools[m->pool] = pp;
  }

 out:
  paxos->wait_for_commit(new PGOSDMonitor::C_PoolOp(this, m, ret,
						    l_pending_inc->epoch,
						    blp));
  propose_pending();
  return false;
}


bool PGOSDMonitor::prepare_pool_op_create(MPoolOp *m)
{
  int err = prepare_new_pool(m);
  paxos->wait_for_commit(new PGOSDMonitor::C_PoolOp(this, m, err,
						    pending_inc->epoch));
  return true;
}

int PGOSDMonitor::_prepare_remove_pool(uint64_t pool)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  dout(10) << "_prepare_remove_pool " << pool << dendl;
  if (l_pending_inc->old_pools.count(pool)) {
    dout(10) << "_prepare_remove_pool " << pool << " pending removal" << dendl;    
    return -ENOENT;  // already removed
  }
  l_pending_inc->old_pools.insert(pool);

  // remove any pg_temp mappings for this pool too
  for (map<pg_t,vector<int32_t> >::iterator p = l_osdmap->pg_temp->begin();
       p != l_osdmap->pg_temp->end();
       ++p)
    if (p->first.pool() == pool) {
      dout(10) << "_prepare_remove_pool " << pool <<
	" removing obsolete pg_temp " <<
	p->first << dendl;
      l_pending_inc->new_pg_temp[p->first].clear();
    }
  return 0;
}

int PGOSDMonitor::_prepare_rename_pool(uint64_t pool, string newname)
{
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  dout(10) << "_prepare_rename_pool " << pool << dendl;
  if (l_pending_inc->old_pools.count(pool)) {
    dout(10) << "_prepare_rename_pool " << pool << " pending removal" << dendl;    
    return -ENOENT;
  }
  for (map<int64_t,string>::iterator p = l_pending_inc->new_pool_names.begin();
       p != l_pending_inc->new_pool_names.end();
       ++p) {
    if (p->second == newname) {
      return -EEXIST;
    }
  }

  l_pending_inc->new_pool_names[pool] = newname;
  return 0;
}

bool PGOSDMonitor::prepare_pool_op_delete(MPoolOp *m)
{
  int ret = _prepare_remove_pool(m->pool);
  paxos->wait_for_commit(new PGOSDMonitor::C_PoolOp(this, m, ret, pending_inc->epoch));
  return true;
}

bool PGOSDMonitor::prepare_pool_op_auid(MPoolOp *m)
{
  PGOSDMap* const l_osdmap = dynamic_cast<PGOSDMap*>(osdmap.get());
  PGOSDMap::Incremental* const l_pending_inc =
    dynamic_cast<PGOSDMap::Incremental*>(pending_inc.get());

  // check that current user can write to new auid
  MonSession *session = m->get_session();
  if (!session)
    goto fail;
  if (session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, m->auid)) {
    // check that current user can write to old auid
    int old_auid = l_osdmap->get_pg_pool(m->pool)->auid;
    if (session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, old_auid)) {
      // update pg_pool_t with new auid
      if (l_pending_inc->new_pools.count(m->pool) == 0)
	l_pending_inc->new_pools[m->pool] = *(l_osdmap->get_pg_pool(m->pool));
      l_pending_inc->new_pools[m->pool].auid = m->auid;
      paxos->wait_for_commit(new PGOSDMonitor::C_PoolOp(this, m, 0, l_pending_inc->epoch));
      return true;
    }
  }

 fail:
  // if it gets here it failed a permissions check
  _pool_op_reply(m, -EPERM, l_pending_inc->epoch);
  return true;
}

void PGOSDMonitor::_pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp)
{
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, paxos->get_version(), blp);
  mon->send_reply(m, reply);
  m->put();
}
