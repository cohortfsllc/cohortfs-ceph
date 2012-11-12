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

#include <sstream>
#include <memory>

#include "OSDMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "PGMonitor.h"

#include "MonitorStore.h"

#include "osd/PlaceSystem.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"

#include "messages/MOSDFailure.h"
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

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, auto_ptr<OSDMap> osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap->get_epoch() << " ";
}



/************ MAPS ****************/
OSDMonitor::OSDMonitor(Monitor *mn, Paxos *p)
  : PaxosService(mn, p),
    thrash_map(0), thrash_last_up_osd(-1)
{
  // we need to trim this too
  p->add_extra_state_dir("osdmap_full");
  PlaceSystem* placeSystem = PlaceSystem::getSystem(g_conf->osd_placement_system);
  osdmap.reset(placeSystem->newMap());
  pending_inc.reset(placeSystem->newMapIncremental());
}


void OSDMonitor::create_initial()
{
  dout(10) << "create_initial for " << mon->monmap->fsid << dendl;

  PlaceSystem* placeSystem = PlaceSystem::getSystem(g_conf->osd_placement_system);
  auto_ptr<OSDMap> newmap(placeSystem->newMap());

  bufferlist bl;
  mon->store->get_bl_ss(bl, "mkfs", "osdmap");
  if (bl.length()) {
    newmap->decode(bl);
    newmap->set_fsid(mon->monmap->fsid);
  } else {
    newmap->build_simple(g_ceph_context, 0, mon->monmap->fsid, 0);
  }
  newmap->set_epoch(1);
  newmap->created = newmap->modified = ceph_clock_now(g_ceph_context);

  // encode into pending incremental
  newmap->encode(pending_inc->fullmap);
}

void OSDMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();
  if (paxosv == osdmap->epoch)
    return;
  assert(paxosv >= osdmap->epoch);

  dout(15) << "update_from_paxos paxos e " << paxosv 
	   << ", my e " << osdmap->epoch << dendl;

  if (osdmap->epoch != paxos->get_stashed_version()) {
    bufferlist latest;
    version_t v = paxos->get_stashed(latest);
    dout(7) << "update_from_paxos loading latest full map e" << v << dendl;
    osdmap->decode(latest);
  } 
  
  // walk through incrementals
  bufferlist bl;
  while (paxosv > osdmap->epoch) {
    bool success = paxos->read(osdmap->epoch+1, bl);
    assert(success);
    
    dout(7) << "update_from_paxos  applying incremental " << osdmap->epoch+1 << dendl;
    auto_ptr<OSDMap::Incremental> inc(osdmap->newIncremental());
    inc->decode(bl);
    
    osdmap->apply_incremental(*inc);

    // write out the full map for all past epochs
    bl.clear();
    osdmap->encode(bl);
    mon->store->put_bl_sn(bl, "osdmap_full", osdmap->epoch);

    // share
    dout(1) << *osdmap << dendl;

    if (osdmap->epoch == 1)
      mon->store->erase_ss("mkfs", "osdmap");
  }

  // save latest
  paxos->stash_latest(paxosv, bl);

  // populate down -> out map
  for (int o = 0; o < osdmap->get_max_osd(); o++)
    if (osdmap->is_down(o) && osdmap->is_in(o) &&
	down_pending_out.count(o) == 0) {
      dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
      down_pending_out[o] = ceph_clock_now(g_ceph_context);
    }

  if (mon->is_leader()) {
    // kick pgmon, make sure it's seen the latest map
    mon->pgmon()->check_osd_map(osdmap->epoch);
  }

  send_to_waiting();
  check_subs();

  share_map_with_random_osd();
  update_logger();
}


bool OSDMonitor::thrash()
{
  if (!thrash_map)
    return false;

  thrash_map--;
  int o;

  // mark a random osd up_thru.. 
  if (rand() % 4 == 0 || thrash_last_up_osd < 0)
    o = rand() % osdmap->get_num_osds();
  else
    o = thrash_last_up_osd;
  if (osdmap->is_up(o)) {
    dout(5) << "thrash_map osd." << o << " up_thru" << dendl;
    pending_inc->new_up_thru[o] = osdmap->get_epoch();
  }

  // mark a random osd up/down
  o = rand() % osdmap->get_num_osds();
  if (osdmap->is_up(o)) {
    dout(5) << "thrash_map osd." << o << " down" << dendl;
    pending_inc->new_state[o] = CEPH_OSD_UP;
  } else if (osdmap->exists(o)) {
    dout(5) << "thrash_map osd." << o << " up" << dendl;
    pending_inc->new_state[o] = CEPH_OSD_UP;
    pending_inc->new_up_client[o] = entity_addr_t();
    pending_inc->new_up_internal[o] = entity_addr_t();
    pending_inc->new_hb_up[o] = entity_addr_t();
    pending_inc->new_weight[o] = CEPH_OSD_IN;
    thrash_last_up_osd = o;
  }

  // mark a random osd in
  o = rand() % osdmap->get_num_osds();
  if (osdmap->exists(o)) {
    dout(5) << "thrash_map osd." << o << " in" << dendl;
    pending_inc->new_weight[o] = CEPH_OSD_IN;
  }

  // mark a random osd out
  o = rand() % osdmap->get_num_osds();
  if (osdmap->exists(o)) {
    dout(5) << "thrash_map osd." << o << " out" << dendl;
    pending_inc->new_weight[o] = CEPH_OSD_OUT;
  }

  osdmap->thrash(mon, *pending_inc);

  return true;
}

void OSDMonitor::on_active()
{
  update_logger();

  if (thrash_map && thrash())
    propose_pending();

  if (mon->is_leader()) {
    mon->clog.info() << "osdmap " << *osdmap << "\n"; 
  }
}

void OSDMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;
  
  mon->cluster_logger->set(l_cluster_num_osd, osdmap->get_num_osds());
  mon->cluster_logger->set(l_cluster_num_osd_up, osdmap->get_num_up_osds());
  mon->cluster_logger->set(l_cluster_num_osd_in, osdmap->get_num_in_osds());
  mon->cluster_logger->set(l_cluster_osd_epoch, osdmap->get_epoch());
}

void OSDMonitor::remove_redundant_pg_temp()
{
  dout(10) << "remove_redundant_pg_temp" << dendl;

  for (map<pg_t,vector<int> >::iterator p = osdmap->pg_temp->begin();
       p != osdmap->pg_temp->end();
       p++) {
    if (pending_inc->new_pg_temp.count(p->first) == 0) {
      vector<int> raw_up;
      osdmap->pg_to_raw_up(p->first, raw_up);
      if (raw_up == p->second) {
	dout(10) << " removing unnecessary pg_temp " << p->first << " -> " << p->second << dendl;
	pending_inc->new_pg_temp[p->first].clear();
      }
    }
  }
}

/* Assign a lower weight to overloaded OSDs.
 *
 * The osds that will get a lower weight are those with with a utilization
 * percentage 'oload' percent greater than the average utilization.
 */
int OSDMonitor::reweight_by_utilization(int oload, std::string& out_str)
{
  if (oload <= 100) {
    ostringstream oss;
    oss << "You must give a percentage higher than 100. "
      "The reweighting threshold will be calculated as <average-utilization> "
      "times <input-percentage>. For example, an argument of 200 would "
      "reweight OSDs which are twice as utilized as the average OSD.\n";
    out_str = oss.str();
    dout(0) << "reweight_by_utilization: " << out_str << dendl;
    return -EINVAL;
  }

  // Avoid putting a small number (or 0) in the denominator when calculating
  // average_util
  const PGMap &pgm = mon->pgmon()->pg_map;
  if (pgm.osd_sum.kb < 1024) {
    ostringstream oss;
    oss << "Refusing to reweight: we only have " << pgm.osd_sum << " kb "
      "across all osds!\n";
    out_str = oss.str();
    dout(0) << "reweight_by_utilization: " << out_str << dendl;
    return -EDOM;
  }

  if (pgm.osd_sum.kb_used < 5 * 1024) {
    ostringstream oss;
    oss << "Refusing to reweight: we only have " << pgm.osd_sum << " kb "
      "used across all osds!\n";
    out_str = oss.str();
    dout(0) << "reweight_by_utilization: " << out_str << dendl;
    return -EDOM;
  }

  float average_util = pgm.osd_sum.kb_used;
  average_util /= pgm.osd_sum.kb;
  float overload_util = average_util * oload / 100.0;

  ostringstream oss;
  char buf[128];
  snprintf(buf, sizeof(buf), "average_util: %04f, overload_util: %04f. ",
	   average_util, overload_util);
  oss << buf;
  std::string sep;
  oss << "overloaded osds: ";
  bool changed = false;
  for (hash_map<int,osd_stat_t>::const_iterator p = pgm.osd_stat.begin();
       p != pgm.osd_stat.end();
       ++p) {
    float util = p->second.kb_used;
    util /= p->second.kb;
    if (util >= overload_util) {
      sep = ", ";
      // Assign a lower weight to overloaded OSDs. The current weight
      // is a factor to take into account the original weights,
      // to represent e.g. differing storage capacities
      unsigned weight = osdmap->get_weight(p->first);
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      pending_inc->new_weight[p->first] = new_weight;
      char buf[128];
      snprintf(buf, sizeof(buf), "%d [%04f -> %04f]", p->first,
	       (float)weight / (float)0x10000,
	       (float)new_weight / (float)0x10000);
      oss << buf << sep;
      changed = true;
    }
  }
  if (sep.empty()) {
    oss << "(none)";
  }
  out_str = oss.str();
  dout(0) << "reweight_by_utilization: finished with " << out_str << dendl;
  return changed;
}


void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap->epoch+1);
  pending_inc->fsid = mon->monmap->fsid;
  
  dout(10) << "create_pending e " << pending_inc->epoch << dendl;

  // drop any redundant pg_temp entries
  remove_redundant_pg_temp();
}


void OSDMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending e " << pending_inc->epoch
	   << dendl;
  
  // finalize up pending_inc
  pending_inc->modified = ceph_clock_now(g_ceph_context);

  // tell me about it
  for (map<int32_t,uint8_t>::iterator i = pending_inc->new_state.begin();
       i != pending_inc->new_state.end();
       i++) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if (s & CEPH_OSD_UP)
      dout(2) << " osd." << i->first << " DOWN" << dendl;
    if (s & CEPH_OSD_EXISTS)
      dout(2) << " osd." << i->first << " DNE" << dendl;
  }
  for (map<int32_t,entity_addr_t>::iterator i = pending_inc->new_up_client.begin();
       i != pending_inc->new_up_client.end();
       i++) { 
    dout(2) << " osd." << i->first << " UP " << i->second << dendl; //FIXME: insert cluster addresses too
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc->new_weight.begin();
       i != pending_inc->new_weight.end();
       i++) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd." << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd." << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd." << i->first << " WEIGHT " << hex << i->second << dec << dendl;
    }
  }

  // encode
  assert(paxos->get_version() + 1 == pending_inc->epoch);
  ::encode(pending_inc, bl, CEPH_FEATURES_ALL);
}


void OSDMonitor::share_map_with_random_osd()
{
  // tell any osd
  MonSession *s = mon->session_map.get_random_osd_session();
  if (s) {
    dout(10) << "committed, telling random " << s->inst << " all about it" << dendl;
    MOSDMap *m = build_incremental(osdmap->get_epoch() - 1, osdmap->get_epoch());  // whatev, they'll request more if they need it
    mon->messenger->send_message(m, s->inst);
  }
}


// -------------

bool OSDMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

    // damp updates
  case MSG_OSD_FAILURE:
    return preprocess_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return preprocess_boot((MOSDBoot*)m);
  case MSG_OSD_ALIVE:
    return preprocess_alive((MOSDAlive*)m);
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp((MOSDPGTemp*)m);

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op((MPoolOp*)m);

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps((MRemoveSnaps*)m);
    
  default:
    assert(0);
    m->put();
    return true;
  }
}

bool OSDMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_FAILURE:
    return prepare_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return prepare_boot((MOSDBoot*)m);
  case MSG_OSD_ALIVE:
    return prepare_alive((MOSDAlive*)m);
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp((MOSDPGTemp*)m);

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
    
  case CEPH_MSG_POOLOP:
    return prepare_pool_op((MPoolOp*)m);

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps((MRemoveSnaps*)m);

  default:
    assert(0);
    m->put();
  }

  return false;
}

bool OSDMonitor::should_propose(double& delay)
{
  dout(10) << "should_propose" << dendl;

  // if full map, propose immediately!  any subsequent changes will be clobbered.
  if (pending_inc->fullmap.length())
    return true;

  // adjust osd weights?
  if (osd_weight.size() == (unsigned)osdmap->get_max_osd()) {
    dout(0) << " adjusting osd weights based on " << osd_weight << dendl;
    osdmap->adjust_osd_weights(osd_weight, pending_inc);
    delay = 0.0;
    osd_weight.clear();
    return true;
  }

  return PaxosService::should_propose(delay);
}



// ---------------------------
// READs


// ---------------------------
// UPDATEs

// failure --

bool OSDMonitor::preprocess_failure(MOSDFailure *m)
{
  // who is target_osd
  int badboy = m->get_target().name.num();

  // check permissions
  MonSession *session = m->get_session();
  if (!session)
    goto didit;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    goto didit;
  }

  if (m->fsid != mon->monmap->fsid) {
    dout(0) << "preprocess_failure on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    goto didit;
  }

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap->exists(from) ||
	osdmap->get_addr(from) != m->get_orig_source_inst().addr ||
	osdmap->is_down(from)) {
      dout(5) << "preprocess_failure from dead osd." << from << ", ignoring" << dendl;
      send_incremental(m, m->get_epoch()+1);
      goto didit;
    }
  }
  

  // weird?
  if (!osdmap->have_inst(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap->get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }
  if (osdmap->get_inst(badboy) != m->get_target()) {
    dout(5) << "preprocess_failure wrong osd: report " << m->get_target() << " != map's " << osdmap->get_inst(badboy)
	    << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap->get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }

  // already reported?
  if (osdmap->is_down(badboy)) {
    dout(5) << "preprocess_failure dup: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap->get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }

  if (!can_mark_down(badboy)) {
    dout(5) << "preprocess_failure ignoring report of " << m->get_target() << " from " << m->get_orig_source_inst() << dendl;
    goto didit;
  }

  dout(10) << "preprocess_failure new: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
  return false;

 didit:
  m->put();
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap->test_flag(CEPH_OSDMAP_NODOWN)) {
    dout(5) << "can_mark_down NODOWN flag set, will not mark osd." << i << " down" << dendl;
    return false;
  }
  int up = osdmap->get_num_up_osds() - pending_inc->get_net_marked_down(&osdmap);
  float up_ratio = (float)up / (float)osdmap->get_num_osds();
  if (up_ratio < g_conf->mon_osd_min_up_ratio) {
    dout(5) << "can_mark_down current up_ratio " << up_ratio << " < min "
	    << g_conf->mon_osd_min_up_ratio
	    << ", will not mark osd." << i << " down" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::can_mark_up(int i)
{
  if (osdmap->test_flag(CEPH_OSDMAP_NOUP)) {
    dout(5) << "can_mark_up NOUP flag set, will not mark osd." << i << " up" << dendl;
    return false;
  }
  return true;
}

/**
 * @note the parameter @p i apparently only exists here so we can output the
 *	 osd's id on messages.
 */
bool OSDMonitor::can_mark_out(int i)
{
  if (osdmap->test_flag(CEPH_OSDMAP_NOOUT)) {
    dout(5) << "can_mark_out NOOUT flag set, will not mark osds out" << dendl;
    return false;
  }
  int in = osdmap->get_num_in_osds() - pending_inc->get_net_marked_out(&osdmap);
  float in_ratio = (float)in / (float)osdmap->get_num_osds();
  if (in_ratio < g_conf->mon_osd_min_in_ratio) {
    if (i >= 0)
      dout(5) << "can_mark_down current in_ratio " << in_ratio << " < min "
	      << g_conf->mon_osd_min_in_ratio
	      << ", will not mark osd." << i << " out" << dendl;
    else
      dout(5) << "can_mark_down current in_ratio " << in_ratio << " < min "
	      << g_conf->mon_osd_min_in_ratio
	      << ", will not mark osds out" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::can_mark_in(int i)
{
  if (osdmap->test_flag(CEPH_OSDMAP_NOIN)) {
    dout(5) << "can_mark_in NOIN flag set, will not mark osd." << i << " in" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  dout(1) << "prepare_failure " << m->get_target() << " from " << m->get_orig_source_inst()
          << " is reporting failure:" << m->if_osd_failed() << dendl;
  mon->clog.info() << m->get_target() << " failed (by "
		   << m->get_orig_source_inst() << ")\n";
  
  int target_osd = m->get_target().name.num();
  int reporter = m->get_orig_source().num();
  assert(osdmap->is_up(target_osd));
  assert(osdmap->get_addr(target_osd) == m->get_target().addr);
  
  if (m->if_osd_failed()) {
    int reports = 0;
    int reporters = 0;

    if (failed_notes.count(target_osd)) {
      multimap<int, pair<int, int> >::iterator i = failed_notes.lower_bound(target_osd);
      while ((i != failed_notes.end()) && (i->first == target_osd)) {
        if (i->second.first == reporter) {
          ++i->second.second;
          dout(10) << "adding new failure report from osd." << reporter
                   << " on osd." << target_osd << dendl;
          reporter = -1;
        }
        ++reporters;
        reports += i->second.second;
        ++i;
      }
    }
    if (reporter != -1) { //didn't get counted yet
      failed_notes.insert(pair<int, pair<int, int> >
                          (target_osd, pair<int, int>(reporter, 1)));
      ++reporters;
      ++reports;
      dout(10) << "osd." << reporter
               << " is adding failure report on osd." << target_osd << dendl;
    }

    if ((reporters >= g_conf->osd_min_down_reporters) &&
        (reports >= g_conf->osd_min_down_reports)) {
      dout(1) << "have enough reports/reporters to mark osd." << target_osd
              << " as down" << dendl;
      pending_inc->new_state[target_osd] = CEPH_OSD_UP;
      paxos->wait_for_commit(new C_Reported(this, m));
      //clear out failure reports
      failed_notes.erase(failed_notes.lower_bound(target_osd),
                         failed_notes.upper_bound(target_osd));
      return true;
    }
  } else { //remove the report
    multimap<int, pair<int, int> >::iterator i = failed_notes.lower_bound(target_osd);
    while ((i != failed_notes.end()) && (i->first == target_osd)
                                && (i->second.first != reporter))
      ++i;
    if ((i == failed_notes.end()) || (i->second.first != reporter))
      dout(0) << "got an OSD not-failed report from osd." << reporter
              << " that hasn't reported failure! (or in previous epoch?)" << dendl;
    else failed_notes.erase(i);
  }
  
  return false;
}

void OSDMonitor::_reported_failure(MOSDFailure *m)
{
  dout(7) << "_reported_failure on " << m->get_target() << ", telling " << m->get_orig_source_inst() << dendl;
  send_latest(m, m->get_epoch());
}


// boot --

bool OSDMonitor::preprocess_boot(MOSDBoot *m)
{
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "got preprocess_boot message from entity with insufficient caps"
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->sb.cluster_fsid != mon->monmap->fsid) {
    dout(0) << "preprocess_boot on fsid " << m->sb.cluster_fsid
	    << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  if (m->get_orig_source_inst().addr.is_blank_ip()) {
    dout(0) << "preprocess_boot got blank addr for " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  assert(m->get_orig_source_inst().name.is_osd());
  
  // already booted?
  if (osdmap->is_up(from) &&
      osdmap->get_inst(from) == m->get_orig_source_inst()) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source_inst()
	    << " == " << osdmap->get_inst(from) << dendl;
    _booted(m, false);
    return true;
  }

  // noup?
  if (!can_mark_up(from)) {
    dout(7) << "preprocess_boot ignoring boot from " << m->get_orig_source_inst() << dendl;
    send_latest(m, m->sb.current_epoch+1);
    return true;
  }

  dout(10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_boot(MOSDBoot *m)
{
  dout(7) << "prepare_boot from " << m->get_orig_source_inst() << " sb " << m->sb
	  << " cluster_addr " << m->cluster_addr << " hb_addr " << m->hb_addr
	  << dendl;

  assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();
  
  // does this osd exist?
  if (from >= osdmap->get_max_osd()) {
    dout(1) << "boot from osd." << from << " >= max_osd " << osdmap->get_max_osd() << dendl;
    m->put();
    return false;
  }

  int oldstate = osdmap->exists(from) ? osdmap->get_state(from) : CEPH_OSD_NEW;
  if (pending_inc->new_state.count(from))
    oldstate ^= pending_inc->new_state[from];

  // already up?  mark down first?
  if (osdmap->is_up(from)) {
    dout(7) << "prepare_boot was up, first marking down " << osdmap->get_inst(from) << dendl;
    assert(osdmap->get_inst(from) != m->get_orig_source_inst());  // preproces should have caught it
    
    if (pending_inc->new_state.count(from) == 0 ||
	(pending_inc->new_state[from] & CEPH_OSD_UP) == 0) {
      // mark previous guy down
      pending_inc->new_state[from] = CEPH_OSD_UP;
    }
    paxos->wait_for_commit(new C_RetryMessage(this, m));
  } else if (pending_inc->new_up_client.count(from)) { //FIXME: should this be using new_up_client?
    // already prepared, just wait
    dout(7) << "prepare_boot already prepared, waiting on " << m->get_orig_source_addr() << dendl;
    paxos->wait_for_commit(new C_RetryMessage(this, m));
  } else {
    // mark new guy up.
    pending_inc->new_up_client[from] = m->get_orig_source_addr();
    if (!m->cluster_addr.is_blank_ip())
      pending_inc->new_up_internal[from] = m->cluster_addr;
    pending_inc->new_hb_up[from] = m->hb_addr;

    // mark in?
    if ((g_conf->mon_osd_auto_mark_auto_out_in && (oldstate & CEPH_OSD_AUTOOUT)) ||
	(g_conf->mon_osd_auto_mark_new_in && (oldstate & CEPH_OSD_NEW)) ||
	(g_conf->mon_osd_auto_mark_in)) {
      if (can_mark_in(from)) {
	pending_inc->new_weight[from] = CEPH_OSD_IN;
      } else {
	dout(7) << "prepare_boot NOIN set, will not mark in " << m->get_orig_source_addr() << dendl;
      }
    }

    down_pending_out.erase(from);  // if any

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // set uuid?
    dout(10) << " setting osd." << from << " uuid to " << m->sb.osd_fsid << dendl;
    if (!osdmap->exists(from) || osdmap->get_uuid(from) != m->sb.osd_fsid)
      pending_inc->new_uuid[from] = m->sb.osd_fsid;

    // fresh osd?
    if (m->sb.newest_map == 0 && osdmap->exists(from)) {
      const osd_info_t& i = osdmap->get_info(from);
      if (i.up_from > i.lost_at) {
	dout(10) << " fresh osd; marking lost_at too" << dendl;
	pending_inc->new_lost[from] = osdmap->get_epoch();
      }
    }

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap->get_info(from);
    dout(10) << " old osd_info: " << info << dendl;
    if (m->sb.mounted > info.last_clean_begin ||
	(m->sb.mounted == info.last_clean_begin &&
	 m->sb.clean_thru > info.last_clean_end)) {
      epoch_t begin = m->sb.mounted;
      epoch_t end = m->sb.clean_thru;

      dout(10) << "prepare_boot osd." << from << " last_clean_interval "
	       << "[" << info.last_clean_begin << "," << info.last_clean_end << ")"
	       << " -> [" << begin << "-" << end << ")"
	       << dendl;
      pending_inc->new_last_clean_interval[from] = pair<epoch_t,epoch_t>(begin, end);
    }

    // wait
    paxos->wait_for_commit(new C_Booted(this, m));
  }
  return true;
}

void OSDMonitor::_booted(MOSDBoot *m, bool logit)
{
  dout(7) << "_booted " << m->get_orig_source_inst() 
	  << " w " << m->sb.weight << " from " << m->sb.current_epoch << dendl;

  if (logit) {
    mon->clog.info() << m->get_orig_source_inst() << " boot\n";
  }

  send_latest(m, m->sb.current_epoch+1);
}


// -------------
// alive

bool OSDMonitor::preprocess_alive(MOSDAlive *m)
{
  int from = m->get_orig_source().num();

  // check permissions, ignore if failed
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (osdmap->is_up(from) &&
      osdmap->get_inst(from) == m->get_orig_source_inst() &&
      osdmap->get_up_thru(from) >= m->want) {
    // yup.
    dout(7) << "preprocess_alive want up_thru " << m->want << " dup from " << m->get_orig_source_inst() << dendl;
    _reply_map(m, m->version);
    return true;
  }
  
  dout(10) << "preprocess_alive want up_thru " << m->want
	   << " from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_alive(MOSDAlive *m)
{
  int from = m->get_orig_source().num();

  if (0) {  // we probably don't care much about these
    mon->clog.debug() << m->get_orig_source_inst() << " alive\n";
  }

  dout(7) << "prepare_alive want up_thru " << m->want << " have " << m->version
	  << " from " << m->get_orig_source_inst() << dendl;
  pending_inc->new_up_thru[from] = m->version;  // set to the latest map the OSD has
  paxos->wait_for_commit(new C_ReplyMap(this, m, m->version));
  return true;
}

void OSDMonitor::_reply_map(PaxosServiceMessage *m, epoch_t e)
{
  dout(7) << "_reply_map " << e
	  << " from " << m->get_orig_source_inst()
	  << dendl;
  send_latest(m, e);
}

// -------------
// pg_temp changes

bool OSDMonitor::preprocess_pgtemp(MOSDPGTemp *m)
{
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  vector<int> empty;

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
	     << (osdmap->pg_temp->count(p->first) ? (*osdmap->pg_temp)[p->first] : empty)
	     << " -> " << p->second << dendl;
    // removal?
    if (p->second.empty() && osdmap->pg_temp->count(p->first))
      return false;
    // change?
    if (p->second.size() && (osdmap->pg_temp->count(p->first) == 0 ||
			     (*osdmap->pg_temp)[p->first] != p->second))
      return false;
  }

  dout(7) << "preprocess_pgtemp e" << m->map_epoch << " no changes from " << m->get_orig_source_inst() << dendl;
  _reply_map(m, m->map_epoch);
  return true;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_pgtemp(MOSDPGTemp *m)
{
  int from = m->get_orig_source().num();
  dout(7) << "prepare_pgtemp e" << m->map_epoch << " from " << m->get_orig_source_inst() << dendl;
  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); p++)
    pending_inc->new_pg_temp[p->first] = p->second;
  pending_inc->new_up_thru[from] = m->map_epoch;   // set up_thru too, so the osd doesn't have to ask again
  paxos->wait_for_commit(new C_ReplyMap(this, m, m->map_epoch));
  return true;
}


// ---

bool OSDMonitor::preprocess_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "preprocess_remove_snaps " << *m << dendl;

  // check privilege, ignore if failed
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_RW)) {
    dout(0) << "got preprocess_remove_snaps from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<int, vector<snapid_t> >::iterator q = m->snaps.begin();
       q != m->snaps.end();
       q++) {
    if (!osdmap->have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second << " on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = osdmap->get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin(); 
	 p != q->second.end();
	 p++) {
      if (*p > pi->get_snap_seq() ||
	  !pi->removed_snaps.contains(*p))
	return false;
    }
  }

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "prepare_remove_snaps " << *m << dendl;

  for (map<int, vector<snapid_t> >::iterator p = m->snaps.begin(); 
       p != m->snaps.end();
       p++) {
    pg_pool_t& pi = osdmap->pools[p->first];
    for (vector<snapid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      if (!pi.removed_snaps.contains(*q) &&
	  (!pending_inc->new_pools.count(p->first) ||
	   !pending_inc->new_pools[p->first].removed_snaps.contains(*q))) {
	if (pending_inc->new_pools.count(p->first) == 0)
	  pending_inc->new_pools[p->first] = pi;
	pg_pool_t& newpi = pending_inc->new_pools[p->first];
	newpi.removed_snaps.insert(*q);
	dout(10) << " pool " << p->first << " removed_snaps added " << *q
		 << " (now " << newpi.removed_snaps << ")" << dendl;
	if (*q > newpi.get_snap_seq()) {
	  dout(10) << " pool " << p->first << " snap_seq " << newpi.get_snap_seq() << " -> " << *q << dendl;
	  newpi.set_snap_seq(*q);
	}
	newpi.set_snap_epoch(pending_inc->epoch);
      }
    }
  }

  m->put();
  return true;
}


// ---------------
// map helpers

void OSDMonitor::send_to_waiting()
{
  dout(10) << "send_to_waiting " << osdmap->get_epoch() << dendl;

  map<epoch_t, list<PaxosServiceMessage*> >::iterator p = waiting_for_map.begin();
  while (p != waiting_for_map.end()) {
    epoch_t from = p->first;
    
    if (from) {
      if (from <= osdmap->get_epoch()) {
	while (!p->second.empty()) {
	  send_incremental(p->second.front(), from);
	  p->second.front()->put();
	  p->second.pop_front();
	}
      } else {
	dout(10) << "send_to_waiting from " << from << dendl;
	p++;
	continue;
      }
    } else {
      while (!p->second.empty()) {
	send_full(p->second.front());
	p->second.front()->put();
	p->second.pop_front();
      }
    }

    waiting_for_map.erase(p++);
  }
}

void OSDMonitor::send_latest(PaxosServiceMessage *m, epoch_t start)
{
  if (paxos->is_readable()) {
    dout(5) << "send_latest to " << m->get_orig_source_inst()
	    << " start " << start << dendl;
    if (start == 0)
      send_full(m);
    else
      send_incremental(m, start);
    m->put();
  } else {
    dout(5) << "send_latest to " << m->get_orig_source_inst()
	    << " start " << start << " later" << dendl;
    waiting_for_map[start].push_back(m);
  }
}


MOSDMap *OSDMonitor::build_latest_full()
{
  MOSDMap *r = new MOSDMap(mon->monmap->fsid, &osdmap);
  r->oldest_map = paxos->get_first_committed();
  r->newest_map = osdmap->get_epoch();
  return r;
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to)
{
  dout(10) << "build_incremental [" << from << ".." << to << "]" << dendl;
  MOSDMap *m = new MOSDMap(mon->monmap->fsid);
  m->oldest_map = paxos->get_first_committed();
  m->newest_map = osdmap->get_epoch();

  for (epoch_t e = to;
       e >= from && e > 0;
       e--) {
    bufferlist bl;
    if (mon->store->get_bl_sn(bl, "osdmap", e) > 0) {
      dout(20) << "build_incremental    inc " << e << " " << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } 
    else if (mon->store->get_bl_sn(bl, "osdmap_full", e) > 0) {
      dout(20) << "build_incremental   full " << e << " " << bl.length() << " bytes" << dendl;
      m->maps[e] = bl;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }
  return m;
}

void OSDMonitor::send_full(PaxosServiceMessage *m)
{
  dout(5) << "send_full to " << m->get_orig_source_inst() << dendl;
  mon->send_reply(m, build_latest_full());
}

void OSDMonitor::send_incremental(PaxosServiceMessage *req, epoch_t first)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap->get_epoch() << "]"
	  << " to " << req->get_orig_source_inst() << dendl;
  if (first < paxos->get_first_committed()) {
    first = paxos->get_first_committed();
    bufferlist bl;
    mon->store->get_bl_sn(bl, "osdmap_full", first);
    dout(20) << "send_incremental starting with base full " << first << " " << bl.length() << " bytes" << dendl;
    MOSDMap *m = new MOSDMap(osdmap->get_fsid());
    m->oldest_map = paxos->get_first_committed();
    m->newest_map = osdmap->get_epoch();
    m->maps[first] = bl;
    mon->send_reply(req, m);
    return;
  }

  // send some maps.  it may not be all of them, but it will get them
  // started.
  epoch_t last = MIN(first + g_conf->osd_map_message_max, osdmap->get_epoch());
  MOSDMap *m = build_incremental(first, last);
  m->oldest_map = paxos->get_first_committed();
  m->newest_map = osdmap->get_epoch();
  mon->send_reply(req, m);
}

void OSDMonitor::send_incremental(epoch_t first, entity_inst_t& dest, bool onetime)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap->get_epoch() << "]"
	  << " to " << dest << dendl;

  if (first < paxos->get_first_committed()) {
    first = paxos->get_first_committed();
    bufferlist bl;
    mon->store->get_bl_sn(bl, "osdmap_full", first);
    dout(20) << "send_incremental starting with base full " << first << " " << bl.length() << " bytes" << dendl;
    MOSDMap *m = new MOSDMap(osdmap->get_fsid());
    m->oldest_map = paxos->get_first_committed();
    m->newest_map = osdmap->get_epoch();
    m->maps[first] = bl;
    mon->messenger->send_message(m, dest);
    first++;
  }

  while (first <= osdmap->get_epoch()) {
    epoch_t last = MIN(first + g_conf->osd_map_message_max, osdmap->get_epoch());
    MOSDMap *m = build_incremental(first, last);
    mon->messenger->send_message(m, dest);
    first = last + 1;
    if (onetime)
      break;
  }
}




epoch_t OSDMonitor::blacklist(entity_addr_t a, utime_t until)
{
  dout(10) << "blacklist " << a << " until " << until << dendl;
  pending_inc->new_blacklist[a] = until;
  return pending_inc->epoch;
}


void OSDMonitor::check_subs()
{
  string type = "osdmap";
  if (mon->session_map.subs.count(type) == 0)
    return;
  xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void OSDMonitor::check_sub(Subscription *sub)
{
  if (sub->next <= osdmap->get_epoch()) {
    if (sub->next >= 1)
      send_incremental(sub->next, sub->session->inst, sub->incremental_onetime);
    else
      mon->messenger->send_message(build_latest_full(),
				   sub->session->inst);
    if (sub->onetime)
      mon->session_map.remove_sub(sub);
    else
      sub->next = osdmap->get_epoch() + 1;
  }
}

// TICK


void OSDMonitor::tick()
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << osdmap << dendl;

  if (!mon->is_leader()) return;

  bool do_propose = false;

  // mark down osds out?
  utime_t now = ceph_clock_now(g_ceph_context);


  /* can_mark_out() checks if we can mark osds as being out. The -1 has no
   * influence at all. The decision is made based on the ratio of "in" osds,
   * and the function returns false if this ratio is lower that the minimum
   * ratio set by g_conf->mon_osd_min_in_ratio. So it's not really up to us.
   */
  if (can_mark_out(-1)) {
    map<int,utime_t>::iterator i = down_pending_out.begin();
    while (i != down_pending_out.end()) {
      int o = i->first;
      utime_t down = now;
      down -= i->second;
      i++;

      if (osdmap->is_down(o) &&
	  osdmap->is_in(o) &&
	  can_mark_out(o)) {
	if (g_conf->mon_osd_down_out_interval > 0 &&
	    down.sec() >= g_conf->mon_osd_down_out_interval) {
	  dout(10) << "tick marking osd." << o << " OUT after " << down
		   << " sec (target " << g_conf->mon_osd_down_out_interval << ")" << dendl;
	  pending_inc->new_weight[o] = CEPH_OSD_OUT;

	  // set the AUTOOUT bit.
	  if (pending_inc->new_state.count(o) == 0)
	    pending_inc->new_state[o] = 0;
	  pending_inc->new_state[o] |= CEPH_OSD_AUTOOUT;

	  do_propose = true;
	
	  mon->clog.info() << "osd." << o << " out (down for " << down << ")\n";
	} else
	  continue;
      }

      down_pending_out.erase(o);
    }
  } else {
    dout(10) << "tick NOOUT flag set, not checking down osds" << dendl;
  }

  // expire blacklisted items?
  for (hash_map<entity_addr_t,utime_t>::iterator p = osdmap->blacklist.begin();
       p != osdmap->blacklist.end();
       p++) {
    if (p->second < now) {
      dout(10) << "expiring blacklist item " << p->first << " expired " << p->second << " < now " << now << dendl;
      pending_inc->old_blacklist.push_back(p->first);
      do_propose = true;
    }
  }

  //if map full setting has changed, get that info out there!
  if (mon->pgmon()->paxos->is_readable()) {
    if (!mon->pgmon()->pg_map.full_osds.empty()) {
      dout(5) << "There are full osds, setting full flag" << dendl;
      add_flag(CEPH_OSDMAP_FULL);
    } else if (osdmap->test_flag(CEPH_OSDMAP_FULL)){
      dout(10) << "No full osds, removing full flag" << dendl;
      remove_flag(CEPH_OSDMAP_FULL);
    }
    if (pending_inc->new_flags != -1 &&
	(pending_inc->new_flags ^ osdmap->flags) & CEPH_OSDMAP_FULL) {
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
	  pending_inc->new_pg_swap_primary[pgid] = osds[1];
	  dout(3) << "Changing primary for PG " << pgid << " from " << osds[0] << " to "
		  << osds[1] << dendl;
	  do_propose = true;
	}
      }
  }
#endif
  // ---------------

  if (do_propose ||
      !pending_inc->new_pg_temp.empty())  // also propose if we adjusted pg_temp
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


void OSDMonitor::handle_osd_timeouts(const utime_t &now,
				     std::map<int,utime_t> &last_osd_report)
{
  utime_t timeo(g_conf->mon_osd_report_timeout, 0);
  int max_osd = osdmap->get_max_osd();
  bool new_down = false;

  for (int i=0; i < max_osd; ++i) {
    dout(30) << "handle_osd_timeouts: checking up on osd " << i << dendl;
    if (!osdmap->exists(i))
      continue;
    if (!osdmap->is_up(i))
      continue;
    const std::map<int,utime_t>::const_iterator t = last_osd_report.find(i);
    if (t == last_osd_report.end()) {
      // it wasn't in the map; start the timer.
      last_osd_report[i] = now;
    } else if (can_mark_down(i)) {
      utime_t diff = now - t->second;
      if (diff > timeo) {
	derr << "no osd or pg stats from osd." << i << " since " << t->second << ", " << diff
	     << " seconds ago.  marking down" << dendl;
	pending_inc->new_state[i] = CEPH_OSD_UP;
	new_down = true;
      }
    }
  }
  if (new_down) {
    propose_pending();
  }
}

void OSDMonitor::mark_all_down()
{
  assert(mon->is_leader());

  dout(7) << "mark_all_down" << dendl;

  set<int32_t> ls;
  osdmap->get_all_osds(ls);
  for (set<int32_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    if (osdmap->is_down(*it)) continue;
    pending_inc->new_state[*it] = CEPH_OSD_UP;
  }

  propose_pending();
}

void OSDMonitor::get_health(list<pair<health_status_t,string> >& summary,
			    list<pair<health_status_t,string> > *detail) const
{
  int num_osds = osdmap->get_num_osds();
  int num_up_osds = osdmap->get_num_up_osds();
  int num_in_osds = osdmap->get_num_in_osds();

  if (num_osds == 0) {
    summary.push_back(make_pair(HEALTH_ERR, "no osds"));
  } else {
    if (num_up_osds < num_in_osds) {
      ostringstream ss;
      ss << (num_in_osds - num_up_osds) << "/" << num_in_osds << " in osds are down";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));

      if (detail) {
	for (int i = 0; i < osdmap->get_max_osd(); i++) {
	  if (osdmap->exists(i) && !osdmap->is_up(i)) {
	    const osd_info_t& info = osdmap->get_info(i);
	    ostringstream ss;
	    ss << "osd." << i << " is down since epoch " << info.down_at
	       << ", last address " << osdmap->get_addr(i);
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
	}
      }
    }
  }
}

bool OSDMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  MonSession *session = m->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_R) &&
       !mon->_allowed_command(session, m->cmd))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, paxos->get_version());
    return true;
  }

  vector<const char*> args;
  for (unsigned i = 1; i < m->cmd.size(); i++)
    args.push_back(m->cmd[i].c_str());

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      osdmap->print_summary(ss);
      r = 0;
    }
    else if (m->cmd[1] == "dump" ||
	     m->cmd[1] == "tree" ||
	     m->cmd[1] == "getmap" ||
	     m->cmd[1] == "getcrushmap") {
      string format = "plain";
      string val;
      epoch_t epoch = 0;
      string cmd = args[0];
      for (std::vector<const char*>::iterator i = args.begin()+1; i != args.end(); ) {
	if (ceph_argparse_double_dash(args, i))
	  break;
	else if (ceph_argparse_witharg(args, i, &val, "-f", "--format", (char*)NULL))
	  format = val;
	else if (!epoch)
	  epoch = atoi(*i++);
	else
	  i++;
      }

      OSDMap *p = &osdmap;
      if (epoch) {
	bufferlist b;
	mon->store->get_bl_sn(b,"osdmap_full", epoch);
	if (!b.length()) {
	  p = 0;
	  r = -ENOENT;
	} else {
	  p = new OSDMap;
	  p->decode(b);
	}
      }
      if (p) {
	if (cmd == "dump") {
	  stringstream ds;
	  if (format == "json") {
	    p->dump_json(ds);
	    r = 0;
	  } else if (format == "plain") {
	    p->print(ds);
	    r = 0;
	  } else {
	    ss << "unrecognized format '" << format << "'";
	    r = -EINVAL;
	  }
	  if (r == 0) {
	    rdata.append(ds);
	    ss << "dumped osdmap epoch " << p->get_epoch();
	  }
	} else if (cmd == "tree") {
	  stringstream ds;
	  p->print_tree(ds);
	  rdata.append(ds);
	  ss << "dumped osdmap tree epoch " << p->get_epoch();
	  r = 0;
	} else if (cmd == "getmap") {
	  p->encode(rdata);
	  ss << "got osdmap epoch " << p->get_epoch();
	  r = 0;
	} else if (cmd == "getcrushmap") {
	  p->crush->encode(rdata);
	  ss << "got crush map from osdmap epoch " << p->get_epoch();
	  r = 0;
	}
	if (p != &osdmap)
	  delete p;
      }
    }
    else if (m->cmd[1] == "getmaxosd") {
      ss << "max_osd = " << osdmap->get_max_osd() << " in epoch " << osdmap->get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "tell") {
      if (m->cmd.size() < 4) {
	r = -EINVAL;
	ss << "usage: osd tell <who> <what>";
	goto out;
      }
      m->cmd.erase(m->cmd.begin()); //take out first two args; don't need them
      m->cmd.erase(m->cmd.begin());
      if (m->cmd[0] == "*") {
	m->cmd.erase(m->cmd.begin()); //and now we're done with the target num
	for (int i = 0; i < osdmap->get_max_osd(); ++i)
	  if (osdmap->is_up(i))
	    mon->send_command(osdmap->get_inst(i), m->cmd, paxos->get_version());
	r = 0;
	ss << "ok";
      } else {
	errno = 0;
	int who = strtol(m->cmd[0].c_str(), 0, 10);
	m->cmd.erase(m->cmd.begin()); //done with target num now
	if (!errno && who >= 0) {
	  if (osdmap->is_up(who)) {
	    mon->send_command(osdmap->get_inst(who), m->cmd, paxos->get_version());
	    r = 0;
	    ss << "ok";
	  } else {
	    ss << "osd." << who << " not up";
	    r = -ENOENT;
	  }
	} else ss << "specify osd number or *";
      }
    }
    else if (m->cmd[1] == "map" && m->cmd.size() == 4) {
      int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[2].c_str());
      if (pool < 0) {
	ss << "pool " << m->cmd[2] << " dne";
	r = -ENOENT;
      } else {
	object_locator_t oloc(pool);
	object_t oid(m->cmd[3]);
	pg_t pgid = osdmap->object_locator_to_pg(oid, oloc);
	pg_t mpgid = osdmap->raw_pg_to_pg(pgid);
	vector<int> up, acting;
	osdmap->pg_to_up_acting_osds(mpgid, up, acting);
	ss << "osdmap e" << osdmap->get_epoch()
	   << " pool '" << m->cmd[2] << "' (" << pool << ") object '" << oid << "' ->"
	   << " pg " << pgid << " (" << mpgid << ")"
	   << " -> up " << up << " acting " << acting;
	r = 0;
      }
    }
    else if ((m->cmd[1] == "scrub" || m->cmd[1] == "repair")) {
      if (m->cmd.size() <= 2) {
	r = -EINVAL;
	ss << "usage: osd [scrub|repair] <who>";
	goto out;
      }
      if (m->cmd[2] == "*") {
	ss << "osds ";
	int c = 0;
	for (int i=0; i<osdmap->get_max_osd(); i++)
	  if (osdmap->is_up(i)) {
	    ss << (c++ ? ",":"") << i;
	    mon->try_send_message(new MOSDScrub(osdmap->get_fsid(),
						m->cmd[1] == "repair"),
				  osdmap->get_inst(i));
	  }	    
	r = 0;
	ss << " instructed to " << m->cmd[1];
      } else {
	long osd = strtol(m->cmd[2].c_str(), 0, 10);
	if (osdmap->is_up(osd)) {
	  mon->try_send_message(new MOSDScrub(osdmap->get_fsid(),
					      m->cmd[1] == "repair"),
				osdmap->get_inst(osd));
	  r = 0;
	  ss << "osd." << osd << " instructed to " << m->cmd[1];
	} else 
	  ss << "osd." << osd << " is not up";
      }
    }
    else if (m->cmd[1] == "lspools") {
      uint64_t uid_pools = 0;
      if (m->cmd.size() > 2) {
	uid_pools = strtol(m->cmd[2].c_str(), NULL, 10);
      }
      for (map<int64_t, pg_pool_t>::iterator p = osdmap->pools.begin();
	   p != osdmap->pools.end();
	   ++p) {
	if (!uid_pools || p->second.auid == uid_pools) {
	  ss << p->first << ' ' << osdmap->pool_name[p->first] << ',';
	}
      }
      r = 0;
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "blacklist" && m->cmd[2] == "ls") {
      for (hash_map<entity_addr_t,utime_t>::iterator p = osdmap->blacklist.begin();
	   p != osdmap->blacklist.end();
	   p++) {
	stringstream ss;
	string s;
	ss << p->first << " " << p->second;
	getline(ss, s);
	s += "\n";
	rdata.append(s);
      }
      ss << "listed " << osdmap->blacklist.size() << " entries";
      r = 0;
    }
  }
 out:
  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else
    return false;
}

int OSDMonitor::prepare_new_pool(MPoolOp *m)
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
int OSDMonitor::prepare_new_pool(string& name, uint64_t auid, int crush_rule,
                                 unsigned pg_num, unsigned pgp_num)
{
  if (osdmap->name_pool.count(name)) {
    return -EEXIST;
  }
  for (map<int64_t,string>::iterator p = pending_inc->new_pool_names.begin();
       p != pending_inc->new_pool_names.end();
       ++p) {
    if (p->second == name)
      return -EEXIST;
  }

  if (-1 == pending_inc->new_pool_max)
    pending_inc->new_pool_max = osdmap->pool_max;
  int64_t pool = ++pending_inc->new_pool_max;
  pending_inc->new_pools[pool].type = pg_pool_t::TYPE_REP;

  pending_inc->new_pools[pool].size = g_conf->osd_pool_default_size;
  if (crush_rule >= 0)
    pending_inc->new_pools[pool].crush_ruleset = crush_rule;
  else
    pending_inc->new_pools[pool].crush_ruleset = g_conf->osd_pool_default_crush_rule;
  pending_inc->new_pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
  pending_inc->new_pools[pool].set_pg_num(pg_num ? pg_num : g_conf->osd_pool_default_pg_num);
  pending_inc->new_pools[pool].set_pgp_num(pgp_num ? pgp_num : g_conf->osd_pool_default_pgp_num);
  pending_inc->new_pools[pool].last_change = pending_inc->epoch;
  pending_inc->new_pools[pool].auid = auid;
  pending_inc->new_pool_names[pool] = name;
  return 0;
}

bool OSDMonitor::prepare_set_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc->new_flags < 0)
    pending_inc->new_flags = osdmap->get_flags();
  pending_inc->new_flags |= flag;
  ss << "set " << OSDMap::get_flag_string(flag);
  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, ss.str(), paxos->get_version()));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc->new_flags < 0)
    pending_inc->new_flags = osdmap->get_flags();
  pending_inc->new_flags &= ~flag;
  ss << "unset " << OSDMap::get_flag_string(flag);
  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, ss.str(), paxos->get_version()));
  return true;
}

bool OSDMonitor::prepare_command(MMonCommand *m)
{
  bool ret = false;
  stringstream ss;
  string rs;
  int err = -EINVAL;

  MonSession *session = m->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W) &&
       !mon->_allowed_command(session, m->cmd))) {
    mon->reply_command(m, -EACCES, "access denied", paxos->get_version());
    return true;
  }

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

      pending_inc->crush = data;
      string rs = "set crush map";
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd.size() >= 6 && m->cmd[1] == "crush" && m->cmd[2] == "set") {
      do {
	// osd crush set <id> <name> <weight> [<loc1> [<loc2> ...]]
	int id = atoi(m->cmd[3].c_str());
	if (!osdmap->exists(id)) {
	  err = -ENOENT;
	  ss << "osd." << id << " does not exist.  create it before updating the crush map";
	  goto out;
	}

	string name = m->cmd[4];
	float weight = atof(m->cmd[5].c_str());
	map<string,string> loc;
	for (unsigned i = 6; i < m->cmd.size(); ++i) {
	  const char *s = m->cmd[i].c_str();
	  const char *pos = strchr(s, '=');
	  if (!pos)
	    break;
	  string key(s, 0, pos-s);
	  string value(pos+1);
	  if (value.length())
	    loc[key] = value;
	  else
	    loc.erase(key);
	}

	dout(0) << "adding/updating crush item id " << id << " name '" << name << "' weight " << weight
		<< " at location " << loc << dendl;
	bufferlist bl;
	if (pending_inc->crush.length())
	  bl = pending_inc->crush;
	else
	  osdmap->crush->encode(bl);

	CrushWrapper newcrush;
	bufferlist::iterator p = bl.begin();
	newcrush.decode(p);

	err = newcrush.update_item(g_ceph_context, id, weight, name, loc);
	if (err == 0) {
	  ss << "updated item id " << id << " name '" << name << "' weight " << weight
	     << " at location " << loc << " to crush map";
	  break;
	}
	if (err > 0) {
	  pending_inc->crush.clear();
	  newcrush.encode(pending_inc->crush);
	  ss << "updated item id " << id << " name '" << name << "' weight " << weight
	     << " at location " << loc << " to crush map";
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd.size() >= 5 && m->cmd[1] == "crush" && m->cmd[2] == "move") {
      do {
	// osd crush move <name> [<loc1> [<loc2> ...]]
	string name = m->cmd[3];
	map<string,string> loc;
	for (unsigned i = 4; i < m->cmd.size(); ++i) {
	  const char *s = m->cmd[i].c_str();
	  const char *pos = strchr(s, '=');
	  if (!pos)
	    break;
	  string key(s, 0, pos-s);
	  string value(pos+1);
	  if (value.length())
	    loc[key] = value;
	  else
	    loc.erase(key);
	}

	dout(0) << "moving crush item name '" << name << "' to location " << loc << dendl;
	bufferlist bl;
	if (pending_inc->crush.length())
	  bl = pending_inc->crush;
	else
	  osdmap->crush->encode(bl);

	CrushWrapper newcrush;
	bufferlist::iterator p = bl.begin();
	newcrush.decode(p);

	if (!newcrush.name_exists(name.c_str())) {
	  err = -ENOENT;
	  ss << "item " << name << " dne";
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
	    paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	    return true;
	  }
	} else {
	    ss << "no need to move item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	}
      } while (false);
    }
    else if (m->cmd.size() > 3 && m->cmd[1] == "crush" && (m->cmd[2] == "rm" || m->cmd[2] == "remove")) {
      do {
	// osd crush rm <id>
	bufferlist bl;
	if (pending_inc->crush.length())
	  bl = pending_inc->crush;
	else
	  osdmap->crush->encode(bl);

	CrushWrapper newcrush;
	bufferlist::iterator p = bl.begin();
	newcrush.decode(p);

	int id = newcrush.get_item_id(m->cmd[3].c_str());
	if (id < 0) {
	  ss << "device '" << m->cmd[3] << "' does not appear in the crush map";
	  break;
	}
	err = newcrush.remove_item(g_ceph_context, id);
	if (err == 0) {
	  pending_inc->crush.clear();
	  newcrush.encode(pending_inc->crush);
	  ss << "removed item id " << id << " name '" << m->cmd[3] << "' from crush map";
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd.size() > 4 && m->cmd[1] == "crush" && m->cmd[2] == "reweight") {
      do {
	// osd crush reweight <name> <weight>
	bufferlist bl;
	if (pending_inc->crush.length())
	  bl = pending_inc->crush;
	else
	  osdmap->crush->encode(bl);

	CrushWrapper newcrush;
	bufferlist::iterator p = bl.begin();
	newcrush.decode(p);

	int id = newcrush.get_item_id(m->cmd[3].c_str());
	if (id < 0) {
	  ss << "device '" << m->cmd[3] << "' does not appear in the crush map";
	  break;
	}
	float w = atof(m->cmd[4].c_str());

	err = newcrush.adjust_item_weightf(g_ceph_context, id, w);
	if (err == 0) {
	  pending_inc->crush.clear();
	  newcrush.encode(pending_inc->crush);
	  ss << "reweighted item id " << id << " name '" << m->cmd[3] << "' to " << w
	     << " in crush map";
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
      } while (false);
    }
    else if (m->cmd[1] == "setmaxosd" && m->cmd.size() > 2) {
      int newmax = atoi(m->cmd[2].c_str());
      if (newmax < osdmap->crush->get_max_devices()) {
	err = -ERANGE;
	ss << "cannot set max_osd to " << newmax << " which is < crush max_devices "
	   << osdmap->crush->get_max_devices();
	goto out;
      }

      pending_inc->new_max_osd = newmax;
      ss << "set new max_osd = " << pending_inc->new_max_osd;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "pause") {
      return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    }
    else if (m->cmd[1] == "unpause") {
      return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "set" && m->cmd[2] == "pause") {
      return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "unset" && m->cmd[2] == "pause") {
      return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "set" && m->cmd[2] == "noup") {
      return prepare_set_flag(m, CEPH_OSDMAP_NOUP);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "unset" && m->cmd[2] == "noup") {
      return prepare_unset_flag(m, CEPH_OSDMAP_NOUP);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "set" && m->cmd[2] == "nodown") {
      return prepare_set_flag(m, CEPH_OSDMAP_NODOWN);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "unset" && m->cmd[2] == "nodown") {
      return prepare_unset_flag(m, CEPH_OSDMAP_NODOWN);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "set" && m->cmd[2] == "noout") {
      return prepare_set_flag(m, CEPH_OSDMAP_NOOUT);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "unset" && m->cmd[2] == "noout") {
      return prepare_unset_flag(m, CEPH_OSDMAP_NOOUT);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "set" && m->cmd[2] == "noin") {
      return prepare_set_flag(m, CEPH_OSDMAP_NOIN);
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "unset" && m->cmd[2] == "noin") {
      return prepare_unset_flag(m, CEPH_OSDMAP_NOIN);
    }
    else if (m->cmd[1] == "cluster_snap" && m->cmd.size() == 3) {
      // ** DISABLE THIS FOR NOW **
      ss << "cluster snapshot currently disabled (broken implementation)";
      // ** DISABLE THIS FOR NOW **
    }
    else if (m->cmd[1] == "down" && m->cmd.size() >= 3) {
      bool any = false;
      for (unsigned j = 2; j < m->cmd.size(); j++) {
	long osd = strtol(m->cmd[j].c_str(), 0, 10);
	if (!osdmap->exists(osd)) {
	  ss << "osd." << osd << " does not exist";
	} else if (osdmap->is_down(osd)) {
	  ss << "osd." << osd << " is already down";
	  err = 0;
	} else {
	  pending_inc->new_state[osd] = CEPH_OSD_UP;
	  if (any)
	    ss << ", osd." << osd;
	  else 
	    ss << "marked down osd." << osd;
	  any = true;
	}
      }
      if (any) {
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "out" && m->cmd.size() >= 3) {
      bool any = false;
      for (unsigned j = 2; j < m->cmd.size(); j++) {
	long osd = strtol(m->cmd[j].c_str(), 0, 10);
	if (!osdmap->exists(osd)) {
	  ss << "osd." << osd << " does not exist";
	} else if (osdmap->is_out(osd)) {
	  ss << "osd." << osd << " is already out";
	  err = 0;
	} else {
	  pending_inc->new_weight[osd] = CEPH_OSD_OUT;
	  if (any)
	    ss << ", osd." << osd;
	  else
	    ss << "marked out osd." << osd;
	  any = true;
	}
      }
      if (any) {
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } 
    }
    else if (m->cmd[1] == "in" && m->cmd.size() >= 3) {
      bool any = false;
      for (unsigned j = 2; j < m->cmd.size(); j++) {
	long osd = strtol(m->cmd[j].c_str(), 0, 10);
	if (osdmap->is_in(osd)) {
	  ss << "osd." << osd << " is already in";
	  err = 0;
	} else if (!osdmap->exists(osd)) {
	  ss << "osd." << osd << " does not exist";
	} else {
	  pending_inc->new_weight[osd] = CEPH_OSD_IN;
	  if (any)
	    ss << ", osd." << osd;
	  else
	    ss << "marked in osd." << osd;
	  any = true;
	}
      }
      if (any) {
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } 
    }
    else if (m->cmd[1] == "reweight" && m->cmd.size() == 4) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      float w = strtof(m->cmd[3].c_str(), 0);
      long ww = (int)((float)CEPH_OSD_IN*w);
      if (osdmap->exists(osd)) {
	pending_inc->new_weight[osd] = ww;
	ss << "reweighted osd." << osd << " to " << w << " (" << ios::hex << ww << ios::dec << ")";
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } 
    }
    else if (m->cmd[1] == "lost" && m->cmd.size() >= 3) {
      string err;
      int osd = strict_strtol(m->cmd[2].c_str(), 10, &err);
      if (!err.empty()) {
	ss << err;
      }
      else if ((m->cmd.size() < 4) || m->cmd[3] != "--yes-i-really-mean-it") {
	ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	      "--yes-i-really-mean-it if you really do.";
      }
      else if (!osdmap->exists(osd) || !osdmap->is_down(osd)) {
	ss << "osd." << osd << " is not down or doesn't exist";
      } else {
	epoch_t e = osdmap->get_info(osd).down_at;
	pending_inc->new_lost[osd] = e;
	ss << "marked osd lost in epoch " << e;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "create") {
      int i = -1;

      // optional uuid provided?
      uuid_d uuid;
      if (m->cmd.size() > 2 && uuid.parse(m->cmd[2].c_str())) {
	dout(10) << " osd create got uuid " << uuid << dendl;
	i = osdmap->identify_osd(uuid);
	if (i >= 0) {
	  // osd already exists
	  err = 0;
	  ss << i;
	  getline(ss, rs);
	  goto out;
	}
	i = pending_inc->identify_osd(uuid);
	if (i >= 0) {
	  // osd is about to exist
	  paxos->wait_for_commit(new C_RetryMessage(this, m));
	  return true;
	}
      }

      // allocate a new id
      for (i=0; i < osdmap->get_max_osd(); i++) {
	if (!osdmap->exists(i) &&
	    pending_inc->new_up_client.count(i) == 0 &&
	    (pending_inc->new_state.count(i) == 0 ||
	     (pending_inc->new_state[i] & CEPH_OSD_EXISTS) == 0))
	  goto done;
      }

      // raise max_osd
      if (pending_inc->new_max_osd < 0)
	pending_inc->new_max_osd = osdmap->get_max_osd() + 1;
      else
	pending_inc->new_max_osd++;
      i = pending_inc->new_max_osd - 1;

  done:
      dout(10) << " creating osd." << i << dendl;
      pending_inc->new_state[i] |= CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      if (!uuid.is_zero())
	pending_inc->new_uuid[i] = uuid;
      ss << i;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "rm" && m->cmd.size() >= 3) {
      bool any = false;
      for (unsigned j = 2; j < m->cmd.size(); j++) {
	long osd = strtol(m->cmd[j].c_str(), 0, 10);
	if (!osdmap->exists(osd)) {
	  ss << "osd." << osd << " does not exist";
	} else if (osdmap->is_up(osd)) {
	  ss << "osd." << osd << " is still up";
	} else {
	  pending_inc->new_state[osd] = osdmap->get_state(osd);
	  if (any)
	    ss << ", osd." << osd;
	  else 
	    ss << "marked dne osd." << osd;
	  any = true;
	}
      }
      if (any) {
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "blacklist" && m->cmd.size() >= 4) {
      entity_addr_t addr;
      if (!addr.parse(m->cmd[3].c_str(), 0))
	ss << "unable to parse address " << m->cmd[3];
      else if (m->cmd[2] == "add") {

	utime_t expires = ceph_clock_now(g_ceph_context);
	double d = 60*60;  // 1 hour default
	if (m->cmd.size() > 4)
	  d = atof(m->cmd[4].c_str());
	expires += d;

	pending_inc->new_blacklist[addr] = expires;
	ss << "blacklisting " << addr << " until " << expires << " (" << d << " sec)";
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } else if (m->cmd[2] == "rm") {
	if (osdmap->is_blacklisted(addr) || 
	    pending_inc->new_blacklist.count(addr)) {
	  if (osdmap->is_blacklisted(addr))
	    pending_inc->old_blacklist.push_back(addr);
	  else
	    pending_inc->new_blacklist.erase(addr);
	  ss << "un-blacklisting " << addr;
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
	ss << addr << " isn't blacklisted";
	err = -ENOENT;
	goto out;
      }
    }
    else if (m->cmd[1] == "pool" && m->cmd.size() >= 3) {
      if (m->cmd.size() >= 5 && m->cmd[2] == "mksnap") {
	int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = osdmap->get_pg_pool(pool);
	  pg_pool_t *pp = 0;
	  if (pending_inc->new_pools.count(pool))
	    pp = &pending_inc->new_pools[pool];
	  const string& snapname = m->cmd[4];
	  if (p->snap_exists(snapname.c_str()) ||
	      (pp && pp->snap_exists(snapname.c_str()))) {
	    ss << "pool " << m->cmd[3] << " snap " << snapname << " already exists";
	    err = -EEXIST;
	  } else {
	    if (!pp) {
	      pp = &pending_inc->new_pools[pool];
	      *pp = *p;
	    }
	    pp->add_snap(snapname.c_str(), ceph_clock_now(g_ceph_context));
	    pp->set_snap_epoch(pending_inc->epoch);
	    ss << "created pool " << m->cmd[3] << " snap " << snapname;
	    getline(ss, rs);
	    paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	    return true;
	  }
	}
      }
      else if (m->cmd.size() >= 5 && m->cmd[2] == "rmsnap") {
	int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = osdmap->get_pg_pool(pool);
	  pg_pool_t *pp = 0;
	  if (pending_inc->new_pools.count(pool))
	    pp = &pending_inc->new_pools[pool];
	  const string& snapname = m->cmd[4];
	  if (!p->snap_exists(snapname.c_str()) &&
	      (!pp || !pp->snap_exists(snapname.c_str()))) {
	    ss << "pool " << m->cmd[3] << " snap " << snapname << " does not exists";
	    err = -ENOENT;
	  } else {
	    if (!pp) {
	      pp = &pending_inc->new_pools[pool];
	      *pp = *p;
	    }
	    snapid_t sn = pp->snap_exists(snapname.c_str());
	    pp->remove_snap(sn);
	    pp->set_snap_epoch(pending_inc->epoch);
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
	int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
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
	int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else if (osdmap->lookup_pg_pool_name(m->cmd[4].c_str()) >= 0) {
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
	int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = osdmap->get_pg_pool(pool);
	  const char *start = m->cmd[5].c_str();
	  char *end = (char *)start;
	  unsigned n = strtol(start, &end, 10);
	  if (*end == '\0') {
	    if (m->cmd[4] == "size") {
	      if (pending_inc->new_pools.count(pool) == 0)
		pending_inc->new_pools[pool] = *p;
	      pending_inc->new_pools[pool].size = n;
	      pending_inc->new_pools[pool].last_change = pending_inc->epoch;
	      ss << "set pool " << pool << " size to " << n;
	      getline(ss, rs);
	      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	      return true;
	    } else if (m->cmd[4] == "crash_replay_interval") {
	      if (pending_inc->new_pools.count(pool) == 0)
		pending_inc->new_pools[pool] = *p;
	      pending_inc->new_pools[pool].crash_replay_interval = n;
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
		if (pending_inc->new_pools.count(pool) == 0)
		  pending_inc->new_pools[pool] = *p;
		pending_inc->new_pools[pool].set_pg_num(n);
		pending_inc->new_pools[pool].last_change = pending_inc->epoch;
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
		if (pending_inc->new_pools.count(pool) == 0)
		  pending_inc->new_pools[pool] = *p;
		pending_inc->new_pools[pool].set_pgp_num(n);
		pending_inc->new_pools[pool].last_change = pending_inc->epoch;
		ss << "set pool " << pool << " pgp_num to " << n;
		getline(ss, rs);
		paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
		return true;
	      }
	    } else if (m->cmd[4] == "crush_ruleset") {
	      if (osdmap->crush->rule_exists(n)) {
		if (pending_inc->new_pools.count(pool) == 0)
		  pending_inc->new_pools[pool] = *p;
		pending_inc->new_pools[pool].crush_ruleset = n;
		pending_inc->new_pools[pool].last_change = pending_inc->epoch;
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
	int64_t pool = osdmap->lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	  goto out;
	}

	const pg_pool_t *p = osdmap->get_pg_pool(pool);
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
    }
    else if ((m->cmd.size() > 1) &&
	     (m->cmd[1] == "reweight-by-utilization")) {
      int oload = 120;
      if (m->cmd.size() > 2) {
	oload = atoi(m->cmd[2].c_str());
      }
      string out_str;
      err = reweight_by_utilization(oload, out_str);
      if (err < 0) {
	ss << "FAILED reweight-by-utilization: " << out_str;
      }
      else if (err == 0) {
	ss << "no change: " << out_str;
      } else {
	ss << "SUCCESSFUL reweight-by-utilization: " << out_str;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "thrash") {
      thrash_map = atoi(m->cmd[2].c_str());
      ss << "will thrash map for " << thrash_map << " epochs";
      ret = thrash();
      err = 0;
    }
    else {
      ss << "unknown command " << m->cmd[1];
    }
  } else {
    ss << "no command?";
  }
out:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0)
    rs = cpp_strerror(err);
  mon->reply_command(m, err, rs, paxos->get_version());
  return ret;
}

bool OSDMonitor::preprocess_pool_op(MPoolOp *m) 
{
  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(m);

  if (!osdmap->get_pg_pool(m->pool)) {
    dout(10) << "attempt to delete non-existent pool id " << m->pool << dendl;
    _pool_op_reply(m, -ENOENT, osdmap->get_epoch());
    return true;
  }
  
  // check if the snap and snapname exists
  bool snap_exists = false;
  if (osdmap->get_pg_pool(m->pool)->snap_exists(m->name.c_str()))
    snap_exists = true;
  
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (snap_exists) {
      _pool_op_reply(m, -EEXIST, osdmap->get_epoch());
      return true;
    }
    return false; // continue processing
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    return false; // continue processing
  case POOL_OP_DELETE_SNAP:
    if (!snap_exists) {
      _pool_op_reply(m, -ENOENT, osdmap->get_epoch());
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

bool OSDMonitor::preprocess_pool_op_create(MPoolOp *m)
{
  MonSession *session = m->get_session();
  if (!session) {
    _pool_op_reply(m, -EPERM, osdmap->get_epoch());
    return true;
  }
  if ((m->auid && !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, m->auid)) &&
      !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W)) {
    dout(5) << "attempt to create new pool without sufficient auid privileges!"
	    << "message: " << *m  << std::endl
	    << "caps: " << session->caps << dendl;
    _pool_op_reply(m, -EPERM, osdmap->get_epoch());
    return true;
  }

  int64_t pool = osdmap.lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op_reply(m, -EEXIST, osdmap.get_epoch());
    return true;
  }

  return false;
}

bool OSDMonitor::prepare_pool_op(MPoolOp *m)
{
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
  if (pending_inc->new_pools.count(m->pool))
    pp = pending_inc->new_pools[m->pool];
  else
    pp = *osdmap.get_pg_pool(m->pool);

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
      dout(10) << "create snap in pool " << m->pool << " " << m->name << " seq " << pp.get_snap_epoch() << dendl;
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
    pending_inc->new_pools[m->pool] = pp;
  }

 out:
  paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, ret, pending_inc->epoch, blp));
  propose_pending();
  return false;
}

bool OSDMonitor::prepare_pool_op_create(MPoolOp *m)
{
  int err = prepare_new_pool(m);
  paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, err, pending_inc->epoch));
  return true;
}

int OSDMonitor::_prepare_remove_pool(uint64_t pool)
{
    dout(10) << "_prepare_remove_pool " << pool << dendl;
  if (pending_inc->old_pools.count(pool)) {
    dout(10) << "_prepare_remove_pool " << pool << " pending removal" << dendl;    
    return -ENOENT;  // already removed
  }
  pending_inc->old_pools.insert(pool);

  // remove any pg_temp mappings for this pool too
  for (map<pg_t,vector<int32_t> >::iterator p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p)
    if (p->first.pool() == pool) {
      dout(10) << "_prepare_remove_pool " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc->new_pg_temp[p->first].clear();
    }
  return 0;
}

int OSDMonitor::_prepare_rename_pool(uint64_t pool, string newname)
{
  dout(10) << "_prepare_rename_pool " << pool << dendl;
  if (pending_inc->old_pools.count(pool)) {
    dout(10) << "_prepare_rename_pool " << pool << " pending removal" << dendl;    
    return -ENOENT;
  }
  for (map<int64_t,string>::iterator p = pending_inc->new_pool_names.begin();
       p != pending_inc->new_pool_names.end();
       ++p) {
    if (p->second == newname) {
      return -EEXIST;
    }
  }

  pending_inc->new_pool_names[pool] = newname;
  return 0;
}

bool OSDMonitor::prepare_pool_op_delete(MPoolOp *m)
{
  int ret = _prepare_remove_pool(m->pool);
  paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, ret, pending_inc->epoch));
  return true;
}

bool OSDMonitor::prepare_pool_op_auid(MPoolOp *m)
{
  // check that current user can write to new auid
  MonSession *session = m->get_session();
  if (!session)
    goto fail;
  if (session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, m->auid)) {
    // check that current user can write to old auid
    int old_auid = osdmap.get_pg_pool(m->pool)->auid;
    if (session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, old_auid)) {
      // update pg_pool_t with new auid
      if (pending_inc->new_pools.count(m->pool) == 0)
	pending_inc->new_pools[m->pool] = *(osdmap.get_pg_pool(m->pool));
      pending_inc->new_pools[m->pool].auid = m->auid;
      paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, 0, pending_inc->epoch));
      return true;
    }
  }

 fail:
  // if it gets here it failed a permissions check
  _pool_op_reply(m, -EPERM, pending_inc->epoch);
  return true;
}

void OSDMonitor::_pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp)
{
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, paxos->get_version(), blp);
  mon->send_reply(m, reply);
  m->put();
}
