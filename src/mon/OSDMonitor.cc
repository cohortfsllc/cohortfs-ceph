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
#include "osd/OSDMap.h"

#include "MonitorDBStore.h"

#include "osd/PlaceSystem.h"

#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDScrub.h"
#include "messages/MRemoveSnaps.h"

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
#include "common/cmdparse.h"
#include "include/str_list.h"
#include "VolMonitor.h"

using namespace std::tr1;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, shared_ptr<OSDMap> osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap->get_epoch() << " ";
}

void OSDMonitor::create_initial()
{
  dout(10) << "create_initial for " << mon->monmap->fsid << dendl;

  auto_ptr<OSDMap> newmap(OSDMapPlaceSystem::getSystem().newOSDMap());

  bufferlist bl;

  get_mkfs(bl);
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

void OSDMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_version();
  if (version == osdmap->epoch)
    return;
  assert(version >= osdmap->epoch);

  dout(15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap->epoch << dendl;


  /* We no longer have stashed versions. Maybe we can do this by reading
   * from a full map? Maybe we should keep the last full map version on a key
   * as well (say, osdmap_full_version), and consider that the last_committed
   * always contains incrementals, and maybe a full version if
   * osdmap_full_version == last_committed
   *
   * This ^^^^ sounds about right. Do it. We should then change the
   * 'get_stashed_version()' to 'get_full_version(version_t ver)', which should
   * then be read iif
   *	(osdmap->epoch != osd_full_version)
   *	&& (osdmap->epoch <= osdmap_full_version)
   */
  version_t latest_full = get_version_latest_full();
  if ((latest_full > 0) && (latest_full > osdmap->epoch)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading latest full map e" << latest_full << dendl;
    osdmap->decode(latest_bl);
  }

  // walk through incrementals
  MonitorDBStore::Transaction t;
  while (version > osdmap->epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap->epoch+1, inc_bl);
    assert(err == 0);
    assert(inc_bl.length());

    dout(7) << "update_from_paxos  applying incremental " << osdmap->epoch+1 << dendl;
    auto_ptr<OSDMap::Incremental> inc(osdmap->newIncremental());
    err = osdmap->apply_incremental(*inc);
    assert(err == 0);

    // write out the full map for all past epochs
    bufferlist full_bl;
    osdmap->encode(full_bl);
    put_version_full(&t, osdmap->epoch, full_bl);

    // share
    dout(1) << *osdmap << dendl;

    if (osdmap->epoch == 1) {
      erase_mkfs(&t);
    }
  }
  if (!t.empty())
    mon->store->apply_transaction(t);

  for (int o = 0; o < osdmap->get_max_osd(); o++) {
    if (osdmap->is_down(o)) {
      // invalidate osd_epoch cache
      osd_epoch.erase(o);

      // populate down -> out map
      if (osdmap->is_in(o) &&
	  down_pending_out.count(o) == 0) {
	dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
	down_pending_out[o] = ceph_clock_now(g_ceph_context);
      }
    }
  }
  // blow away any osd_epoch items beyond max_osd
  map<int,epoch_t>::iterator p = osd_epoch.upper_bound(osdmap->get_max_osd());
  while (p != osd_epoch.end()) {
    osd_epoch.erase(p++);
  }

  send_to_waiting();
  check_subs();

  share_map_with_random_osd();
  update_logger();

  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();
}

void OSDMonitor::update_msgr_features()
{
  uint64_t mask;
  uint64_t features = osdmap->get_features(&mask);

  set<int> types;
  types.insert((int)entity_name_t::TYPE_OSD);
  types.insert((int)entity_name_t::TYPE_CLIENT);
  types.insert((int)entity_name_t::TYPE_MDS);
  types.insert((int)entity_name_t::TYPE_MON);
  for (set<int>::iterator q = types.begin(); q != types.end(); ++q) {
    if ((mon->messenger->get_policy(*q).features_required & mask) != features) {
      dout(0) << "crush map has features " << features << ", adjusting msgr requires" << dendl;
      Messenger::Policy p = mon->messenger->get_policy(*q);
      p.features_required = (p.features_required & ~mask) | features;
      mon->messenger->set_policy(*q, p);
    }
  }
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
    pending_inc->new_up_cluster[o] = entity_addr_t();
    pending_inc->new_hb_back_up[o] = entity_addr_t();
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

  if (mon->is_leader())
    mon->clog.info() << "osdmap " << *osdmap << "\n";

  if (!mon->is_leader()) {
    kick_all_failures();
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

  return 0;
}


void OSDMonitor::create_pending()
{
  pending_inc->fsid = mon->monmap->fsid;

  dout(10) << "create_pending e " << pending_inc->epoch << dendl;
}

/**
 * @note receiving a transaction in this function gives a fair amount of
 * freedom to the service implementation if it does need it. It shouldn't.
 */
void OSDMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  dout(10) << "encode_pending e " << pending_inc->epoch
	   << dendl;
  
  // finalize up pending_inc
  pending_inc->modified = ceph_clock_now(g_ceph_context);

  bufferlist bl;

  // tell me about it
  for (map<int32_t,uint8_t>::iterator i = pending_inc->new_state.begin();
       i != pending_inc->new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if (s & CEPH_OSD_UP)
      dout(2) << " osd." << i->first << " DOWN" << dendl;
    if (s & CEPH_OSD_EXISTS)
      dout(2) << " osd." << i->first << " DNE" << dendl;
  }
  for (map<int32_t,entity_addr_t>::iterator i = pending_inc->new_up_client.begin();
       i != pending_inc->new_up_client.end();
       ++i) {
    //FIXME: insert cluster addresses too
    dout(2) << " osd." << i->first << " UP " << i->second << dendl;
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc->new_weight.begin();
       i != pending_inc->new_weight.end();
       ++i) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd." << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd." << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd." << i->first << " WEIGHT " << hex << i->second << dec << dendl;
    }
  }

  // encode
  assert(get_version() + 1 == pending_inc->epoch);
  pending_inc->encode(bl, CEPH_FEATURES_ALL);

  /* put everything in the transaction */
  put_version(t, pending_inc->epoch, bl);
  put_last_committed(t, pending_inc->epoch);
}

void OSDMonitor::encode_full(MonitorDBStore::Transaction *t)
{
  dout(10) << __func__ << " osdmap e " << osdmap->epoch << dendl;
  assert(get_version() == osdmap->epoch);
 
  bufferlist osdmap_bl;
  osdmap->encode(osdmap_bl);
  put_version_full(t, osdmap->epoch, osdmap_bl);
  put_version_latest_full(t, osdmap->epoch);
}

void OSDMonitor::share_map_with_random_osd()
{
  if (osdmap->get_num_up_osds() == 0) {
    dout(10) << __func__ << " no up osds, don't share with anyone" << dendl;
    return;
  }

  MonSession *s = mon->session_map.get_random_osd_session();
  if (!s) {
    dout(10) << __func__ << " no up osd on our session map" << dendl;
    return;
  }

  dout(10) << "committed, telling random " << s->inst << " all about it" << dendl;
  // whatev, they'll request more if they need it
  MOSDMap *m = build_incremental(osdmap->get_epoch() - 1, osdmap->get_epoch());
  mon->messenger->send_message(m, s->inst);
}

bool OSDMonitor::service_should_trim()
{
  update_trim();
  return (get_trim_to() > 0);
}

// -------------

bool OSDMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m));

    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return preprocess_mark_me_down(static_cast<MOSDMarkMeDown*>(m));
  case MSG_OSD_FAILURE:
    return preprocess_failure(static_cast<MOSDFailure*>(m));
  case MSG_OSD_BOOT:
    return preprocess_boot(static_cast<MOSDBoot*>(m));
  case MSG_OSD_ALIVE:
    return preprocess_alive(static_cast<MOSDAlive*>(m));

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps(static_cast<MRemoveSnaps*>(m));

  default:
    // give concrete subclass a try
    return preprocess_query_sub(m);
  }
}


bool OSDMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return prepare_mark_me_down(static_cast<MOSDMarkMeDown*>(m));
  case MSG_OSD_FAILURE:
    return prepare_failure(static_cast<MOSDFailure*>(m));
  case MSG_OSD_BOOT:
    return prepare_boot(static_cast<MOSDBoot*>(m));
  case MSG_OSD_ALIVE:
    return prepare_alive(static_cast<MOSDAlive*>(m));

  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps(static_cast<MRemoveSnaps*>(m));

  default:
    return prepare_update_sub(m);
  }
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
    osdmap->adjust_osd_weights(osd_weight, *pending_inc.get());
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

bool OSDMonitor::check_source(PaxosServiceMessage *m, uuid_d fsid) {
  // check permissions
  MonSession *session = m->get_session();
  if (!session)
    return true;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    return true;
  }
  if (fsid != mon->monmap->fsid) {
    dout(0) << "check_source: on fsid " << fsid
	    << " != " << mon->monmap->fsid << dendl;
    return true;
  }
  return false;
}


bool OSDMonitor::preprocess_failure(MOSDFailure *m)
{
  // who is target_osd
  int badboy = m->get_target().name.num();

  // check permissions
  if (check_source(m, m->fsid))
    goto didit;

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

class C_AckMarkedDown : public Context {
  OSDMonitor *osdmon;
  MOSDMarkMeDown *m;
public:
  C_AckMarkedDown(
    OSDMonitor *osdmon,
    MOSDMarkMeDown *m)
    : osdmon(osdmon), m(m) {}

  void finish(int) {
    osdmon->mon->send_reply(
      m,
      new MOSDMarkMeDown(
	m->fsid,
	m->get_target(),
	m->get_epoch(),
	m->ack));
  }
  ~C_AckMarkedDown() {
    m->put();
  }
};

bool OSDMonitor::preprocess_mark_me_down(MOSDMarkMeDown *m)
{
  int requesting_down = m->get_target().name.num();
  int from = m->get_orig_source().num();

  // check permissions
  if (check_source(m, m->fsid))
    goto reply;

  // first, verify the reporting host is valid
  if (!m->get_orig_source().is_osd())
    goto reply;

  if (!osdmap->exists(from) ||
      osdmap->is_down(from) ||
      osdmap->get_addr(from) != m->get_target().addr) {
    dout(5) << "preprocess_mark_me_down from dead osd."
	    << from << ", ignoring" << dendl;
    send_incremental(m, m->get_epoch()+1);
    goto reply;
  }

  // no down might be set
  if (!can_mark_down(requesting_down))
    goto reply;

  dout(10) << "MOSDMarkMeDown for: " << m->get_target() << dendl;
  return false;

 reply:
  Context *c(new C_AckMarkedDown(this, m));
  c->complete(0);
  return true;
}

bool OSDMonitor::prepare_mark_me_down(MOSDMarkMeDown *m)
{
  int target_osd = m->get_target().name.num();

  assert(osdmap->is_up(target_osd));
  assert(osdmap->get_addr(target_osd) == m->get_target().addr);

  mon->clog.info() << "osd." << target_osd << " marked itself down\n";
  pending_inc->new_state[target_osd] = CEPH_OSD_UP;
  wait_for_finished_proposal(new C_AckMarkedDown(this, m));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap->test_flag(CEPH_OSDMAP_NODOWN)) {
    dout(5) << "can_mark_down NODOWN flag set, will not mark osd." << i << " down" << dendl;
    return false;
  }
  int up = osdmap->get_num_up_osds()
    - pending_inc->get_net_marked_down(&(*osdmap.get()));
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
  int in = osdmap->get_num_in_osds()
    - pending_inc->get_net_marked_out(&(*osdmap.get()));
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

void OSDMonitor::check_failures(utime_t now)
{
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    check_failure(now, p->first, p->second);
  }
}

bool OSDMonitor::check_failure(utime_t now, int target_osd, failure_info_t& fi)
{
  utime_t orig_grace(g_conf->osd_heartbeat_grace, 0);
  utime_t max_failed_since = fi.get_failed_since();
  utime_t failed_for = now - max_failed_since;

  utime_t grace = orig_grace;
  double my_grace = 0, peer_grace = 0;
  if (g_conf->mon_osd_adjust_heartbeat_grace) {
    double halflife = (double)g_conf->mon_osd_laggy_halflife;
    double decay_k = ::log(.5) / halflife;

    // scale grace period based on historical probability of 'lagginess'
    // (false positive failures due to slowness).
    const osd_xinfo_t& xi = osdmap->get_xinfo(target_osd);
    double decay = exp((double)failed_for * decay_k);
    dout(20) << " halflife " << halflife << " decay_k " << decay_k
	     << " failed_for " << failed_for << " decay " << decay << dendl;
    my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
    grace += my_grace;

    // consider the peers reporting a failure a proxy for a potential
    // 'subcluster' over the overall cluster that is similarly
    // laggy.  this is clearly not true in all cases, but will sometimes
    // help us localize the grace correction to a subset of the system
    // (say, a rack with a bad switch) that is unhappy.
    assert(fi.reporters.size());
    for (map<int,failure_reporter_t>::iterator p = fi.reporters.begin();
	 p != fi.reporters.end();
	 ++p) {
      const osd_xinfo_t& xi = osdmap->get_xinfo(p->first);
      utime_t elapsed = now - xi.down_stamp;
      double decay = exp((double)elapsed * decay_k);
      peer_grace += decay * (double)xi.laggy_interval * xi.laggy_probability;
    }
    peer_grace /= (double)fi.reporters.size();
    grace += peer_grace;
  }

  dout(10) << " osd." << target_osd << " has "
	   << fi.reporters.size() << " reporters and "
	   << fi.num_reports << " reports, "
	   << grace << " grace (" << orig_grace << " + " << my_grace << " + " << peer_grace << "), max_failed_since " << max_failed_since
	   << dendl;

  // already pending failure?
  if (pending_inc->new_state.count(target_osd) &&
      pending_inc->new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return true;
  }

  if (failed_for >= grace &&
      ((int)fi.reporters.size() >= g_conf->mon_osd_min_down_reporters) &&
      (fi.num_reports >= g_conf->mon_osd_min_down_reports)) {
    dout(1) << " we have enough reports/reporters to mark osd." << target_osd << " down" << dendl;
    pending_inc->new_state[target_osd] = CEPH_OSD_UP;

    mon->clog.info() << osdmap->get_inst(target_osd) << " failed ("
		     << fi.num_reports << " reports from " << (int)fi.reporters.size() << " peers after "
		     << failed_for << " >= grace " << grace << ")\n";
    return true;
  }
  return false;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  dout(1) << "prepare_failure " << m->get_target() << " from " << m->get_orig_source_inst()
          << " is reporting failure:" << m->if_osd_failed() << dendl;

  int target_osd = m->get_target().name.num();
  int reporter = m->get_orig_source().num();
  assert(osdmap->is_up(target_osd));
  assert(osdmap->get_addr(target_osd) == m->get_target().addr);

  // calculate failure time
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t failed_since = m->get_recv_stamp() - utime_t(m->failed_for ? m->failed_for : g_conf->osd_heartbeat_grace, 0);
  
  if (m->if_osd_failed()) {
    // add a report
    mon->clog.debug() << m->get_target() << " reported failed by "
		      << m->get_orig_source_inst() << "\n";
    failure_info_t& fi = failure_info[target_osd];
    MOSDFailure *old = fi.add_report(reporter, failed_since, m);
    if (old) {
      mon->no_reply(old);
      old->put();
    }

    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon->clog.debug() << m->get_target() << " failure report canceled by "
		      << m->get_orig_source_inst() << "\n";
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      list<MOSDFailure*> ls;
      fi.take_report_messages(ls);
      fi.cancel_report(reporter);
      while (!ls.empty()) {
	mon->no_reply(ls.front());
	ls.front()->put();
	ls.pop_front();
      }
      if (fi.reporters.empty()) {
	dout(10) << " removing last failure_info for osd." << target_osd << dendl;
	failure_info.erase(target_osd);
      } else {
	dout(10) << " failure_info for osd." << target_osd << " now "
		 << fi.reporters.size() << " reporters and "
		 << fi.num_reports << " reports" << dendl;
      }
    } else {
      dout(10) << " no failure_info for osd." << target_osd << dendl;
    }
    mon->no_reply(m);
    m->put();
  }

  return false;
}

void OSDMonitor::process_failures()
{
  map<int,failure_info_t>::iterator p = failure_info.begin();
  while (p != failure_info.end()) {
    if (osdmap->is_up(p->first)) {
      ++p;
    } else {
      dout(10) << "process_failures osd." << p->first << dendl;
      list<MOSDFailure*> ls;
      p->second.take_report_messages(ls);
      failure_info.erase(p++);

      while (!ls.empty()) {
	send_latest(ls.front(), ls.front()->get_epoch());
	ls.pop_front();
      }
    }
  }
}

void OSDMonitor::kick_all_failures()
{
  dout(10) << "kick_all_failures on " << failure_info.size() << " osds" << dendl;
  assert(!mon->is_leader());

  list<MOSDFailure*> ls;
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    p->second.take_report_messages(ls);
  }
  failure_info.clear();

  while (!ls.empty()) {
    dispatch(ls.front());
    ls.pop_front();
  }
}


// boot --

bool OSDMonitor::preprocess_boot(MOSDBoot *m)
{
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
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
	  << " cluster_addr " << m->cluster_addr
	  << " hb_back_addr " << m->hb_back_addr
	  << " hb_front_addr " << m->hb_front_addr
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
    wait_for_finished_proposal(new C_RetryMessage(this, m));
  } else if (pending_inc->new_up_client.count(from)) { //FIXME: should this be using new_up_client?
    // already prepared, just wait
    dout(7) << "prepare_boot already prepared, waiting on " << m->get_orig_source_addr() << dendl;
    wait_for_finished_proposal(new C_RetryMessage(this, m));
  } else {
    // mark new guy up.
    pending_inc->new_up_client[from] = m->get_orig_source_addr();
    if (!m->cluster_addr.is_blank_ip())
      pending_inc->new_up_cluster[from] = m->cluster_addr;
    pending_inc->new_hb_back_up[from] = m->hb_back_addr;
    if (!m->hb_front_addr.is_blank_ip())
      pending_inc->new_hb_front_up[from] = m->hb_front_addr;

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

    osd_xinfo_t xi = osdmap->get_xinfo(from);
    if (m->boot_epoch == 0) {
      xi.laggy_probability *= (1.0 - g_conf->mon_osd_laggy_weight);
      xi.laggy_interval *= (1.0 - g_conf->mon_osd_laggy_weight);
      dout(10) << " not laggy, new xi " << xi << dendl;
    } else {
      if (xi.down_stamp.sec()) {
	int interval = ceph_clock_now(g_ceph_context).sec() - xi.down_stamp.sec();
	xi.laggy_interval =
	  interval * g_conf->mon_osd_laggy_weight +
	  xi.laggy_interval * (1.0 - g_conf->mon_osd_laggy_weight);
      }
      xi.laggy_probability =
	g_conf->mon_osd_laggy_weight +
	xi.laggy_probability * (1.0 - g_conf->mon_osd_laggy_weight);
      dout(10) << " laggy, now xi " << xi << dendl;
    }
    pending_inc->new_xinfo[from] = xi;

    // wait
    wait_for_finished_proposal(new C_Booted(this, m));
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
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap->is_up(from) ||
      osdmap->get_inst(from) != m->get_orig_source_inst()) {
    dout(7) << "preprocess_alive ignoring alive message from down "
	    << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  if (osdmap->get_up_thru(from) >= m->want) {
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
  wait_for_finished_proposal(new C_ReplyMap(this, m, m->version));
  return true;
}

void OSDMonitor::_reply_map(PaxosServiceMessage *m, epoch_t e)
{
  dout(7) << "_reply_map " << e
	  << " from " << m->get_orig_source_inst()
	  << dendl;
  send_latest(m, e);
}

// ---

bool OSDMonitor::preprocess_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "preprocess_remove_snaps " << *m << dendl;

  // check privilege, ignore if failed
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_R | MON_CAP_W)) {
    dout(0) << "got preprocess_remove_snaps from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  if (!preprocess_remove_snaps_sub(m)) {
    return false;
  }

 ignore:
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
	++p;
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
  if (is_readable()) {
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
  MOSDMap *r = new MOSDMap(mon->monmap->fsid, &(*osdmap));
  r->oldest_map = get_first_committed();
  r->newest_map = osdmap->get_epoch();
  return r;
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to)
{
  dout(10) << "build_incremental [" << from << ".." << to << "]" << dendl;
  MOSDMap *m = new MOSDMap(mon->monmap->fsid);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap->get_epoch();

  for (epoch_t e = to; e >= from && e > 0; e--) {
    bufferlist bl;
    int err = get_version(e, bl);
    if (err == 0) {
      assert(bl.length());
      // if (get_version(e, bl) > 0) {
      dout(20) << "build_incremental    inc " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } else {
      assert(err == -ENOENT);
      assert(!bl.length());
      get_version("full", e, bl);
      if (bl.length() > 0) {
      //else if (get_version("full", e, bl) > 0) {
      dout(20) << "build_incremental   full " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->maps[e] = bl;
      } else {
	assert(0);  // we should have all maps.
      }
    }
  }
  return m;
}

void OSDMonitor::send_full(PaxosServiceMessage *m)
{
  dout(5) << "send_full to " << m->get_orig_source_inst() << dendl;
  mon->send_reply(m, build_latest_full());
}

/* TBH, I'm fairly certain these two functions could somehow be using a single
 * helper function to do the heavy lifting. As this is not our main focus right
 * now, I'm leaving it to the next near-future iteration over the services'
 * code. We should not forget it though.
 *
 * TODO: create a helper function and get rid of the duplicated code.
 */
void OSDMonitor::send_incremental(PaxosServiceMessage *req, epoch_t first)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap->get_epoch() << "]"
	  << " to " << req->get_orig_source_inst()
	  << dendl;

  int osd = -1;
  if (req->get_source().is_osd()) {
    osd = req->get_source().num();
    map<int,epoch_t>::iterator p = osd_epoch.find(osd);
    if (p != osd_epoch.end()) {
      dout(10) << " osd." << osd << " should have epoch " << p->second << dendl;
      first = p->second + 1;
      if (first > osdmap->get_epoch())
	return;
    }
  }

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version("full", first, bl);
    assert(err == 0);
    assert(bl.length());

    dout(20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap->get_fsid());
    m->oldest_map = first;
    m->newest_map = osdmap->get_epoch();
    m->maps[first] = bl;
    mon->send_reply(req, m);

    if (osd >= 0)
      osd_epoch[osd] = osdmap->get_epoch();
    return;
  }

  // send some maps.  it may not be all of them, but it will get them
  // started.
  epoch_t last = MIN(first + g_conf->osd_map_message_max, osdmap->get_epoch());
  MOSDMap *m = build_incremental(first, last);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap->get_epoch();
  mon->send_reply(req, m);

  if (osd >= 0)
    osd_epoch[osd] = last;
}

void OSDMonitor::send_incremental(epoch_t first, entity_inst_t& dest, bool onetime)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap->get_epoch() << "]"
	  << " to " << dest << dendl;

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version("full", first, bl);
    assert(err == 0);
    assert(bl.length());

    dout(20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap->get_fsid());
    m->oldest_map = first;
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

epoch_t OSDMonitor::blacklist(const entity_addr_t& a, utime_t until)
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
  if (!is_active()) return;

  dout(10) << *osdmap.get() << dendl;

  if (!mon->is_leader()) return;

  bool do_propose = false;
  utime_t now = ceph_clock_now(g_ceph_context);

  // mark osds down?
  check_failures(now);

  // mark down osds out?

  /* can_mark_out() checks if we can mark osds as being out. The -1 has no
   * influence at all. The decision is made based on the ratio of "in" osds,
   * and the function returns false if this ratio is lower that the minimum
   * ratio set by g_conf->mon_osd_min_in_ratio. So it's not really up to us.
   */
  if (can_mark_out(-1)) {
    set<int> down_cache;  // quick cache of down subtrees

    map<int,utime_t>::iterator i = down_pending_out.begin();
    while (i != down_pending_out.end()) {
      int o = i->first;
      utime_t down = now;
      down -= i->second;
      ++i;

      if (osdmap->is_down(o) &&
	  osdmap->is_in(o) &&
	  can_mark_out(o)) {
	utime_t orig_grace(g_conf->mon_osd_down_out_interval, 0);
	utime_t grace = orig_grace;
	double my_grace = 0.0;

	if (g_conf->mon_osd_adjust_down_out_interval) {
	  // scale grace period the same way we do the heartbeat grace.
	  const osd_xinfo_t& xi = osdmap->get_xinfo(o);
	  double halflife = (double)g_conf->mon_osd_laggy_halflife;
	  double decay_k = ::log(.5) / halflife;
	  double decay = exp((double)down * decay_k);
	  dout(20) << "osd." << o << " laggy halflife " << halflife << " decay_k " << decay_k
		   << " down for " << down << " decay " << decay << dendl;
	  my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
	  grace += my_grace;
	}

	if (g_conf->mon_osd_down_out_interval > 0 &&
	    down.sec() >= grace) {
	  dout(10) << "tick marking osd." << o << " OUT after " << down
		   << " sec (target " << grace << " = " << orig_grace << " + " << my_grace << ")" << dendl;
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
       ++p) {
    if (p->second < now) {
      dout(10) << "expiring blacklist item " << p->first << " expired " << p->second << " < now " << now << dendl;
      pending_inc->old_blacklist.push_back(p->first);
      do_propose = true;
    }
  }

  tick_sub(do_propose);
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
	mon->clog.info() << "osd." << i << " marked down after no pg stats for " << diff << "seconds\n";
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
       ++it) {
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

    // warn about flags
    if (osdmap->test_flag(CEPH_OSDMAP_PAUSERD |
			 CEPH_OSDMAP_PAUSEWR |
			 CEPH_OSDMAP_NOUP |
			 CEPH_OSDMAP_NODOWN |
			 CEPH_OSDMAP_NOIN |
			 CEPH_OSDMAP_NOOUT |
			 CEPH_OSDMAP_NOBACKFILL |
			 CEPH_OSDMAP_NORECOVER |
			 CEPH_OSDMAP_NOSCRUB |
			 CEPH_OSDMAP_NODEEP_SCRUB)) {
      ostringstream ss;
      ss << osdmap->get_flag_string() << " flag(s) set";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    get_health_sub(summary, detail);
  }
}

void OSDMonitor::dump_info(Formatter *f)
{
  f->open_object_section("osdmap");
  osdmap->dump(f);
  f->close_section();


  dump_info_sub(f);
}

bool OSDMonitor::preprocess_command(MMonCommand *m)
{
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_version());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("osd", MON_CAP_R) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  if (prefix == "osd stat") {
    osdmap->print_summary(ds);
    rdata.append(ds);
  }
  else if (prefix == "osd dump" ||
	   prefix == "osd tree" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap") {
    string val;

    epoch_t epoch = 0;
    int64_t epochnum;
    cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum, (int64_t)0);
    epoch = epochnum;

    OSDMap *p = osdmap.get();
    if (epoch) {
      bufferlist b;
      int err = get_version("full", epoch, b);
      if (err == -ENOENT) {
	r = -ENOENT;
	goto reply;
      }
      assert(err == 0);
      assert(b.length());
      p = newOSDMap(mon->volmon()->volmap);
      p->decode(b);
    }
    if (prefix == "osd dump") {
      stringstream ds;
      if (f) {
	f->open_object_section("osdmap");
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
      } else {
	p->print(ds);
      }
      rdata.append(ds);
      if (!f)
	ds << " ";
    } else if (prefix == "osd ls") {
      if (f) {
	f->open_array_section("osds");
	for (int i = 0; i < osdmap->get_max_osd(); i++) {
	  if (osdmap->exists(i)) {
	    f->dump_int("osd", i);
	  }
	}
	f->close_section();
	f->flush(ds);
      } else {
	bool first = true;
	for (int i = 0; i < osdmap->get_max_osd(); i++) {
	  if (osdmap->exists(i)) {
	    if (!first)
	      ds << "\n";
	    first = false;
	    ds << i;
	  }
	}
      }
      rdata.append(ds);
    } else if (prefix == "osd tree") {
      if (f) {
	f->open_object_section("tree");
	f.get();
	f->close_section();
	f->flush(ds);
      }
      rdata.append(ds);
    } else if (prefix == "osd getmap") {
      p->encode(rdata);
      ss << "got osdmap epoch " << p->get_epoch();
    }
    if (p != osdmap.get())
      delete p;
  } else if (prefix == "osd getmaxosd") {
    ds << "max_osd = " << osdmap->get_max_osd() << " in epoch " << osdmap->get_epoch();
    rdata.append(ds);
  } else if ((prefix == "osd scrub" ||
	      prefix == "osd deep-scrub" ||
	      prefix == "osd repair")) {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string> pvec;
    get_str_vec(prefix, pvec);

    if (whostr == "*") {
      ss << "osds ";
      int c = 0;
      for (int i = 0; i < osdmap->get_max_osd(); i++)
	if (osdmap->is_up(i)) {
	  ss << (c++ ? "," : "") << i;
	  mon->try_send_message(new MOSDScrub(osdmap->get_fsid(),
					      pvec.back() == "repair",
					      pvec.back() == "deep-scrub"),
				osdmap->get_inst(i));
	}
      r = 0;
      ss << " instructed to " << whostr;
    } else {
      long osd = parse_osd_id(whostr.c_str(), &ss);
      if (osd < 0) {
	r = -EINVAL;
      } else if (osdmap->is_up(osd)) {
	mon->try_send_message(new MOSDScrub(osdmap->get_fsid(),
					    pvec.back() == "repair",
					    pvec.back() == "deep-scrub"),
			      osdmap->get_inst(osd));
	ss << "osd." << osd << " instructed to " << pvec.back();
      } else {
	ss << "osd." << osd << " is not up";
	r = -EAGAIN;
      }
    }
  } else if (prefix == "osd blacklist ls") {
    for (hash_map<entity_addr_t,utime_t>::iterator p = osdmap->blacklist.begin();
	 p != osdmap->blacklist.end();
	 ++p) {
      stringstream ss;
      string s;
      ss << p->first << " " << p->second;
      getline(ss, s);
      s += "\n";
      rdata.append(s);
    }
    ss << "listed " << osdmap->blacklist.size() << " entries";
  } else if (!preprocess_command_sub(m, r, ss)) {
    return false;
  }

reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(m, r, rs, rdata, get_version());
  return true;
}

bool OSDMonitor::prepare_set_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc->new_flags < 0)
    pending_inc->new_flags = osdmap->get_flags();
  pending_inc->new_flags |= flag;
  ss << "set " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(), get_version()));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc->new_flags < 0)
    pending_inc->new_flags = osdmap->get_flags();
  pending_inc->new_flags &= ~flag;
  ss << "unset " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(), get_version()));
  return true;
}

int OSDMonitor::parse_osd_id(const char *s, stringstream *pss)
{
  // osd.NNN?
  if (strncmp(s, "osd.", 4) == 0) {
    s += 4;
  }

  // NNN?
  ostringstream ss;
  long id = parse_pos_long(s, &ss);
  if (id < 0) {
    *pss << ss.str();
    return id;
  }
  if (id > 0xffff) {
    *pss << "osd id " << id << " is too large";
    return -ERANGE;
  }
  return id;
}

void OSDMonitor::parse_loc_map(const vector<string>& args,  map<string,string> *ploc)
{
  for (unsigned i = 0; i < args.size(); ++i) {
    const char *s = args[i].c_str();
    const char *pos = strchr(s, '=');
    if (!pos)
      break;
    string key(s, 0, pos-s);
    string value(pos+1);
    if (value.length())
      (*ploc)[key] = value;
    else
      ploc->erase(key);
  }
}

bool OSDMonitor::prepare_command(MMonCommand *m)
{
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_version());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("osd", MON_CAP_W) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  int64_t id;
  string name;
  bool osdid_present = cmd_getval(g_ceph_context, cmdmap, "id", id);
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << id;
    name = oss.str();
  }

  if (prefix == "osd setmaxosd") {
    int64_t newmax;
    cmd_getval(g_ceph_context, cmdmap, "newmax", newmax);

    if (newmax > g_conf->mon_max_osd) {
      err = -ERANGE;
      ss << "cannot set max_osd to " << newmax << " which is > conf.mon_max_osd ("
	 << g_conf->mon_max_osd << ")";
      goto reply;
    }

    pending_inc->new_max_osd = newmax;
    ss << "set new max_osd = " << pending_inc->new_max_osd;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;

  } else if (prefix == "osd pause") {
    return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd unpause") {
    return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd set") {
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);
    if (key == "pause")
      return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_set_flag(m, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_set_flag(m, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_set_flag(m, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_set_flag(m, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_set_flag(m, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norecover")
      return prepare_set_flag(m, CEPH_OSDMAP_NORECOVER);

  } else if (prefix == "osd unset") {
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);
    if (key == "pause")
      return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_unset_flag(m, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norecover")
      return prepare_unset_flag(m, CEPH_OSDMAP_NORECOVER);

  } else if (prefix == "osd cluster_snap") {
    // ** DISABLE THIS FOR NOW **
    ss << "cluster snapshot currently disabled (broken implementation)";
    // ** DISABLE THIS FOR NOW **

  } else if (prefix == "osd down" ||
	     prefix == "osd out" ||
	     prefix == "osd in" ||
	     prefix == "osd rm") {

    bool any = false;

    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);
    for (unsigned j = 0; j < idvec.size(); j++) {
      long osd = parse_osd_id(idvec[j].c_str(), &ss);
      if (osd < 0) {
	ss << "invalid osd id" << osd;
	// XXX -EINVAL here?
	continue;
      } else if (!osdmap->exists(osd)) {
	ss << "osd." << osd << " does not exist. ";
	err = 0;
	continue;
      }
      if (prefix == "osd down") {
	if (osdmap->is_down(osd)) {
	  ss << "osd." << osd << " is already down. ";
	  err = 0;
	} else {
	  pending_inc->new_state[osd] = CEPH_OSD_UP;
	  ss << "marked down osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd out") {
	if (osdmap->is_out(osd)) {
	  ss << "osd." << osd << " is already out. ";
	  err = 0;
	} else {
	  pending_inc->new_weight[osd] = CEPH_OSD_OUT;
	  ss << "marked out osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd in") {
	if (osdmap->is_in(osd)) {
	  ss << "osd." << osd << " is already in. ";
	  err = 0;
	} else {
	  pending_inc->new_weight[osd] = CEPH_OSD_IN;
	  ss << "marked in osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd rm") {
	if (osdmap->is_up(osd)) {
	ss << "osd." << osd << " is still up; must be down before removal. ";
	} else {
	  pending_inc->new_state[osd] = osdmap->get_state(osd);
	  if (any)
	    ss << ", osd." << osd;
	  else
	    ss << "removed osd." << osd;
	  any = true;
	}
      }
    }
    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
  } else if (prefix == "osd reweight") {
    int64_t id;
    cmd_getval(g_ceph_context, cmdmap, "id", id);
    double w;
    cmd_getval(g_ceph_context, cmdmap, "weight", w);
    long ww = (int)((double)CEPH_OSD_IN*w);
    if (ww < 0L) {
      ss << "weight must be > 0";
      err = -EINVAL;
      goto reply;
    }
    if (osdmap->exists(id)) {
      pending_inc->new_weight[id] = ww;
      ss << "reweighted osd." << id << " to " << w << " (" << ios::hex << ww << ios::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }

  } else if (prefix == "osd lost") {
    int64_t id;
    cmd_getval(g_ceph_context, cmdmap, "id", id);
    string sure;
    if (!cmd_getval(g_ceph_context, cmdmap, "sure", sure) || sure != "--yes-i-really-mean-it") {
      ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	    "--yes-i-really-mean-it if you really do.";
    } else if (!osdmap->exists(id) || !osdmap->is_down(id)) {
      ss << "osd." << id << " is not down or doesn't exist";
    } else {
      epoch_t e = osdmap->get_info(id).down_at;
      pending_inc->new_lost[id] = e;
      ss << "marked osd lost in epoch " << e;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }

  } else if (prefix == "osd create") {
    int i = -1;

    // optional uuid provided?
    uuid_d uuid;
    string uuidstr;
    if (cmd_getval(g_ceph_context, cmdmap, "uuid", uuidstr)) {
      try {
	uuid = uuid_d::parse(uuidstr);
      } catch (const std::invalid_argument& ia) {
	err = -EINVAL;
	goto reply;
      }
      dout(10) << " osd create got uuid " << uuid << dendl;
      i = osdmap->identify_osd(uuid);
      if (i >= 0) {
	// osd already exists
	err = 0;
	ss << i;
	rdata.append(ss);
	goto reply;
      }
      i = pending_inc->identify_osd(uuid);
      if (i >= 0) {
	// osd is about to exist
	wait_for_finished_proposal(new C_RetryMessage(this, m));
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
    rdata.append(ss);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, rdata, get_version()));
    return true;

  } else if (prefix == "osd blacklist") {
    string addrstr;
    cmd_getval(g_ceph_context, cmdmap, "addr", addrstr);
    entity_addr_t addr;
    if (!addr.parse(addrstr.c_str(), 0))
      ss << "unable to parse address " << addrstr;
    else {
      string blacklistop;
      cmd_getval(g_ceph_context, cmdmap, "blacklistop", blacklistop);
      if (blacklistop == "add") {
	utime_t expires = ceph_clock_now(g_ceph_context);
	double d;
	// default one hour
	cmd_getval(g_ceph_context, cmdmap, "expire", d, double(60*60));
	expires += d;

	pending_inc->new_blacklist[addr] = expires;
	ss << "blacklisting " << addr << " until " << expires << " (" << d << " sec)";
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      } else if (blacklistop == "rm") {
	if (osdmap->is_blacklisted(addr) ||
	    pending_inc->new_blacklist.count(addr)) {
	  if (osdmap->is_blacklisted(addr))
	    pending_inc->old_blacklist.push_back(addr);
	  else
	    pending_inc->new_blacklist.erase(addr);
	  ss << "un-blacklisting " << addr;
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
	ss << addr << " isn't blacklisted";
	err = 0;
	goto reply;
      }
    }
  } else if (prefix == "osd reweight-by-utilization") {
    int64_t oload;
    cmd_getval(g_ceph_context, cmdmap, "oload", oload, int64_t(120));
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }

  } else if (prefix == "osd thrash") {
    int64_t num_epochs;
    cmd_getval(g_ceph_context, cmdmap, "num_epochs", num_epochs, int64_t(0));
    // thrash_map is a member var
    thrash_map = num_epochs;
    ss << "will thrash map for " << thrash_map << " epochs";
    ret = thrash();
    err = 0;
  } else if (prepare_command_sub(m, err, ss, rs)) {
    return true;
  } else {
    err = -EINVAL;
  }

reply:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0)
    rs = cpp_strerror(err);
  mon->reply_command(m, err, rs, rdata, get_version());
  return ret;
}
