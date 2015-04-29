// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <string>
#include <sstream>
#include <cassert>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/string_generator.hpp>

#include "boost/regex.hpp"

#include "OSDMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"

#include "MonitorDBStore.h"

#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MMonCommand.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/strtol.h"

#include "common/config.h"
#include "common/errno.h"

#include "include/compat.h"
#include "include/stringify.h"
#include "include/util.h"
#include "common/cmdparse.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "cohort/CohortVolume.h"
#include "cohort/ErasureCPlacer.h"
#include "cohort/StripedPlacer.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, OSDMap& osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap.get_epoch() << " ";
}

void OSDMonitor::create_initial()
{
  ldout(mon->cct, 10) << "create_initial for " << mon->monmap->fsid << dendl;

  OSDMap newmap;

  bufferlist bl;
  mon->store->get("mkfs", "osdmap", bl);

  if (bl.length()) {
    newmap.decode(bl);
    newmap.set_fsid(mon->monmap->fsid);
  } else {
    newmap.build_simple(mon->cct, 0, mon->monmap->fsid, 0);
  }
  newmap.set_epoch(1);
  newmap.created = newmap.modified = ceph::real_clock::now();

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap, mon->quorum_features);
}

void OSDMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == osdmap.epoch)
    return;
  assert(version >= osdmap.epoch);

  ldout(mon->cct, 15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap.epoch << dendl;


  /*
   * We will possibly have a stashed latest that *we* wrote, and we will
   * always be sure to have the oldest full map in the first..last range
   * due to encode_trim_extra(), which includes the oldest full map in the trim
   * transaction.
   *
   * encode_trim_extra() does not however write the full map's
   * version to 'full_latest'.	This is only done when we are building the
   * full maps from the incremental versions.  But don't panic!	 We make sure
   * that the following conditions find whichever full map version is newer.
   */
  version_t latest_full = get_version_latest_full();
  if (latest_full == 0 && get_first_committed() > 1)
    latest_full = get_first_committed();

  if (latest_full > 0) {
    // make sure we can really believe get_version_latest_full(); see
    // 76cd7ac1c2094b34ad36bea89b2246fa90eb2f6d
    bufferlist test;
    get_version_full(latest_full, test);
    if (test.length() == 0) {
      ldout(mon->cct, 10) << __func__ << " ignoring recorded latest_full as it is missing; fallback to search" << dendl;
      latest_full = 0;
    }
  }
  if (get_first_committed() > 1 &&
      latest_full < get_first_committed()) {
    /* a bug introduced in 7fb3804fb860dcd0340dd3f7c39eec4315f8e4b6 would lead
     * us to not update the on-disk latest_full key.  Upon trim, the actual
     * version would cease to exist but we would still point to it.  This
     * makes sure we get it pointing to a proper version.
     */
    version_t lc = get_last_committed();
    version_t fc = get_first_committed();

    ldout(mon->cct, 10) << __func__ << " looking for valid full map in interval"
	     << " [" << fc << ", " << lc << "]" << dendl;

    latest_full = 0;
    for (version_t v = lc; v >= fc; v--) {
      string full_key = "full_" + stringify(v);
      if (mon->store->exists(get_service_name(), full_key)) {
	ldout(mon->cct, 10) << __func__ << " found latest full map v " << v << dendl;
	latest_full = v;
	break;
      }
    }

    // if we trigger this, then there's something else going with the store
    // state, and we shouldn't want to work around it without knowing what
    // exactly happened.
    assert(latest_full > 0);
    MonitorDBStore::Transaction t;
    put_version_latest_full(&t, latest_full);
    mon->store->apply_transaction(t);
    ldout(mon->cct, 10) << __func__ << " updated the on-disk full map version to "
	     << latest_full << dendl;
  }

  if ((latest_full > 0) && (latest_full > osdmap.epoch)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    assert(latest_bl.length() != 0);
    ldout(mon->cct, 7) << __func__ << " loading latest full map e" << latest_full << dendl;
    osdmap.decode(latest_bl);
  }

  // walk through incrementals
  MonitorDBStore::Transaction *t = NULL;
  size_t tx_size = 0;
  while (version > osdmap.epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap.epoch+1, inc_bl);
    assert(err == 0);
    assert(inc_bl.length());

    ldout(mon->cct, 7) << "update_from_paxos  applying incremental " << osdmap.epoch+1 << dendl;
    OSDMap::Incremental inc(inc_bl);
    err = osdmap.apply_incremental(inc);
    assert(err == 0);

    if (t == NULL)
      t = new MonitorDBStore::Transaction;

    // Write out the full map for all past epochs.  Encode the full
    // map with the same features as the incremental.  If we don't
    // know, use the quorum features.  If we don't know those either,
    // encode with all features.
    uint64_t f = inc.encode_features;
    if (!f)
      f = mon->quorum_features;
    if (!f)
      f = -1;
    bufferlist full_bl;
    osdmap.encode(full_bl, f);
    tx_size += full_bl.length();

    put_version_full(t, osdmap.epoch, full_bl);
    put_version_latest_full(t, osdmap.epoch);

    // share
    ldout(mon->cct, 1) << osdmap << dendl;

    if (osdmap.epoch == 1) {
      t->erase("mkfs", "osdmap");
    }

    if (tx_size > mon->cct->_conf->mon_sync_max_payload_size*2) {
      mon->store->apply_transaction(*t);
      delete t;
      t = NULL;
      tx_size = 0;
    }
  }

  if (t != NULL) {
    mon->store->apply_transaction(*t);
    delete t;
  }

  for (int o = 0; o < osdmap.get_max_osd(); o++) {
    if (osdmap.is_down(o)) {
      // invalidate osd_epoch cache
      osd_epoch.erase(o);

      // populate down -> out map
      if (osdmap.is_in(o) &&
	  down_pending_out.count(o) == 0) {
	ldout(mon->cct, 10) << " adding osd." << o
			    << " to down_pending_out map" << dendl;
	down_pending_out[o] = ceph::real_clock::now();
      }
    }
  }
  // blow away any osd_epoch items beyond max_osd
  map<int,epoch_t>::iterator p = osd_epoch.upper_bound(osdmap.get_max_osd());
  while (p != osd_epoch.end()) {
    osd_epoch.erase(p++);
  }

  check_subs();

  share_map_with_random_osd();

  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();
}

void OSDMonitor::update_msgr_features()
{
  uint64_t mask;
  uint64_t features = osdmap.get_features(&mask);

  set<int> types;
  types.insert((int)entity_name_t::TYPE_OSD);
  types.insert((int)entity_name_t::TYPE_CLIENT);
  types.insert((int)entity_name_t::TYPE_MDS);
  types.insert((int)entity_name_t::TYPE_MON);
  for (set<int>::iterator q = types.begin(); q != types.end(); ++q) {
    if ((mon->messenger->get_policy(*q).features_required & mask) != features) {
      Messenger::Policy p = mon->messenger->get_policy(*q);
      p.features_required = (p.features_required & ~mask) | features;
      mon->messenger->set_policy(*q, p);
    }
  }
}

void OSDMonitor::on_active(Monitor::unique_lock& l)
{
  if (mon->is_leader())
    mon->clog.info() << "osdmap " << osdmap << "\n";

  if (!mon->is_leader()) {
    list<MOSDFailure*> ls;
    take_all_failures(ls);
    while (!ls.empty()) {
      dispatch(ls.front(), l);
      ls.pop_front();
    }
  }
}

void OSDMonitor::on_shutdown()
{
  ldout(mon->cct, 10) << __func__ << dendl;

  // discard failure info, waiters
  list<MOSDFailure*> ls;
  take_all_failures(ls);
  while (!ls.empty()) {
    ls.front()->put();
    ls.pop_front();
  }
}

void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;

  ldout(mon->cct, 10) << "create_pending e " << pending_inc.epoch << dendl;
}

/**
 * @note receiving a transaction in this function gives a fair amount of
 * freedom to the service implementation if it does need it. It shouldn't.
 */
void OSDMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  ldout(mon->cct, 10) << "encode_pending e " << pending_inc.epoch
	   << dendl;

  // finalize up pending_inc
  pending_inc.modified = ceph::real_clock::now();

  bufferlist bl;

  // tell me about it
  for (map<int32_t,uint8_t>::iterator i = pending_inc.new_state.begin();
       i != pending_inc.new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if (s & CEPH_OSD_UP)
      ldout(mon->cct, 2) << " osd." << i->first << " DOWN" << dendl;
    if (s & CEPH_OSD_EXISTS)
      ldout(mon->cct, 2) << " osd." << i->first << " DNE" << dendl;
  }
  for (map<int32_t,entity_addr_t>::iterator i = pending_inc.new_up_client.begin();
       i != pending_inc.new_up_client.end();
       ++i) {
    //FIXME: insert cluster addresses too
    ldout(mon->cct, 2) << " osd." << i->first << " UP " << i->second << dendl;
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc.new_weight.begin();
       i != pending_inc.new_weight.end();
       ++i) {
    if (i->second == CEPH_OSD_OUT) {
      ldout(mon->cct, 2) << " osd." << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      ldout(mon->cct, 2) << " osd." << i->first << " IN" << dendl;
    } else {
      ldout(mon->cct, 2) << " osd." << i->first << " WEIGHT " << hex << i->second << dec << dendl;
    }
  }

  // encode
  assert(get_last_committed() + 1 == pending_inc.epoch);
  ::encode(pending_inc, bl, mon->quorum_features);

  /* put everything in the transaction */
  put_version(t, pending_inc.epoch, bl);
  put_last_committed(t, pending_inc.epoch);

  // metadata, too!
  for (map<int,bufferlist>::iterator p = pending_metadata.begin();
       p != pending_metadata.end();
       ++p)
    t->put(OSD_METADATA_PREFIX, stringify(p->first), p->second);
  for (set<int>::iterator p = pending_metadata_rm.begin();
       p != pending_metadata_rm.end();
       ++p)
    t->erase(OSD_METADATA_PREFIX, stringify(*p));
  pending_metadata.clear();
  pending_metadata_rm.clear();
}

int OSDMonitor::dump_osd_metadata(int osd, Formatter *f, ostream *err)
{
  bufferlist bl;
  int r = mon->store->get(OSD_METADATA_PREFIX, stringify(osd), bl);
  if (r < 0)
    return r;
  map<string,string> m;
  try {
    bufferlist::iterator p = bl.begin();
    ::decode(m, p);
  }
  catch (buffer::error& e) {
    if (err)
      *err << "osd." << osd << " metadata is corrupt";
    return -EIO;
  }
  for (map<string,string>::iterator p = m.begin(); p != m.end(); ++p)
    f->dump_string(p->first.c_str(), p->second);
  return 0;
}

void OSDMonitor::share_map_with_random_osd()
{
  if (osdmap.get_num_up_osds() == 0) {
    ldout(mon->cct, 10) << __func__ << " no up osds, don't share with anyone" << dendl;
    return;
  }

  MonSession *s = mon->session_map.get_random_osd_session(&osdmap);
  if (!s) {
    ldout(mon->cct, 10) << __func__ << " no up osd on our session map" << dendl;
    return;
  }

  ldout(mon->cct, 10) << "committed, telling random " << s->inst << " all about it" << dendl;
  // whatev, they'll request more if they need it
  MOSDMap *m = build_incremental(osdmap.get_epoch() - 1, osdmap.get_epoch());
  mon->messenger->send_message(m, s->con);
}

version_t OSDMonitor::get_trim_to()
{
  epoch_t floor = 0;
  ldout(mon->cct, 10) << " min_last_epoch_clean " << dendl;
  if (mon->cct->_conf->mon_osd_force_trim_to > 0 &&
      mon->cct->_conf->mon_osd_force_trim_to < (int)get_last_committed()) {
    floor = mon->cct->_conf->mon_osd_force_trim_to;
    ldout(mon->cct, 10) << " explicit mon_osd_force_trim_to = " << floor << dendl;
  }
  unsigned min = mon->cct->_conf->mon_min_osdmap_epochs;
  if (floor + min > get_last_committed()) {
    if (min < get_last_committed())
      floor = get_last_committed() - min;
    else
      floor = 0;
  }
  if (floor > get_first_committed())
    return floor;
  return 0;
}

void OSDMonitor::encode_trim_extra(MonitorDBStore::Transaction *tx, version_t first)
{
  ldout(mon->cct, 10) << __func__ << " including full map for e " << first << dendl;
  bufferlist bl;
  get_version_full(first, bl);
  put_version_full(tx, first, bl);
}

// -------------

bool OSDMonitor::preprocess_query(PaxosServiceMessage *m,
				  unique_lock& l)
{
  ldout(mon->cct, 10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m), l);

    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return preprocess_mark_me_down(static_cast<MOSDMarkMeDown*>(m));
  case MSG_OSD_FAILURE:
    return preprocess_failure(static_cast<MOSDFailure*>(m));
  case MSG_OSD_BOOT:
    return preprocess_boot(static_cast<MOSDBoot*>(m), l);
  case MSG_OSD_ALIVE:
    return preprocess_alive(static_cast<MOSDAlive*>(m));

  default:
    assert(0);
    m->put();
    return true;
  }
}

bool OSDMonitor::prepare_update(PaxosServiceMessage *m, unique_lock& l)
{
  ldout(mon->cct, 7) << "prepare_update " << *m << " from "
		     << m->get_orig_source_inst() << dendl;

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
    return prepare_command(static_cast<MMonCommand*>(m), l);

  default:
    assert(0);
    m->put();
  }

  return false;
}

bool OSDMonitor::should_propose(ceph::timespan& delay)
{
  ldout(mon->cct, 10) << "should_propose" << dendl;

  // if full map, propose immediately! any subsequent changes will be
  // clobbered.
  if (pending_inc.fullmap.length())
    return true;

  // adjust osd weights?
  if (!osd_weight.empty() &&
      osd_weight.size() == (unsigned)osdmap.get_max_osd()) {
    ldout(mon->cct, 0) << " adjusting osd weights based on "
		       << osd_weight << dendl;
    osdmap.adjust_osd_weights(osd_weight, pending_inc);
    delay = 0ns;
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

bool OSDMonitor::check_source(PaxosServiceMessage *m,
			      const boost::uuids::uuid& fsid) {
  // check permissions
  MonSession *session = m->get_session();
  if (!session)
    return true;
  if (!session->is_capable("osd", MON_CAP_X)) {
    ldout(mon->cct, 0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    return true;
  }
  if (fsid != mon->monmap->fsid) {
    ldout(mon->cct, 0) << "check_source: on fsid " << fsid
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
    if (!osdmap.exists(from) ||
	osdmap.get_addr(from) != m->get_orig_source_inst().addr ||
	osdmap.is_down(from)) {
      ldout(mon->cct, 5) << "preprocess_failure from dead osd." << from << ", ignoring" << dendl;
      send_incremental(m, m->get_epoch()+1);
      goto didit;
    }
  }


  // weird?
  if (!osdmap.have_inst(badboy)) {
    ldout(mon->cct, 5) << "preprocess_failure dne(/dup?): " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }
  if (osdmap.get_inst(badboy) != m->get_target()) {
    ldout(mon->cct, 5) << "preprocess_failure wrong osd: report " << m->get_target() << " != map's " << osdmap.get_inst(badboy)
	    << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }

  // already reported?
  if (osdmap.is_down(badboy)) {
    ldout(mon->cct, 5) << "preprocess_failure dup: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }

  if (!can_mark_down(badboy)) {
    ldout(mon->cct, 5) << "preprocess_failure ignoring report of " << m->get_target() << " from " << m->get_orig_source_inst() << dendl;
    goto didit;
  }

  ldout(mon->cct, 10) << "preprocess_failure new: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
  return false;

 didit:
  m->put();
  return true;
}

class CB_AckMarkedDown {
  OSDMonitor *osdmon;
  MOSDMarkMeDown *m;
public:
  CB_AckMarkedDown(
    OSDMonitor *osdmon,
    MOSDMarkMeDown *m)
    : osdmon(osdmon), m(m) {}

  void operator()(int, Monitor::unique_lock&) {
    osdmon->mon->send_reply(
      m,
      new MOSDMarkMeDown(
	m->fsid,
	m->get_target(),
	m->get_epoch(),
	m->ack));
  }
  ~CB_AckMarkedDown() {
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

  if (!osdmap.exists(from) ||
      osdmap.is_down(from) ||
      osdmap.get_addr(from) != m->get_target().addr) {
    ldout(mon->cct, 5) << "preprocess_mark_me_down from dead osd."
	    << from << ", ignoring" << dendl;
    send_incremental(m, m->get_epoch()+1);
    goto reply;
  }

  // no down might be set
  if (!can_mark_down(requesting_down))
    goto reply;

  ldout(mon->cct, 10) << "MOSDMarkMeDown for: " << m->get_target() << dendl;
  return false;

 reply:
  mon->send_reply(m, new MOSDMarkMeDown(
		    m->fsid,
		    m->get_target(),
		    m->get_epoch(),
		    m->ack));

  m->put();
  return true;
}

bool OSDMonitor::prepare_mark_me_down(MOSDMarkMeDown *m)
{
  int target_osd = m->get_target().name.num();

  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);

  mon->clog.info() << "osd." << target_osd << " marked itself down\n";
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  wait_for_finished_proposal(CB_AckMarkedDown(this, m));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NODOWN)) {
    ldout(mon->cct, 5) << "can_mark_down NODOWN flag set, will not mark osd." << i << " down" << dendl;
    return false;
  }
  int up = osdmap.get_num_up_osds() - pending_inc.get_net_marked_down(&osdmap);
  float up_ratio = (float)up / (float)osdmap.get_num_osds();
  if (up_ratio < mon->cct->_conf->mon_osd_min_up_ratio) {
    ldout(mon->cct, 5) << "can_mark_down current up_ratio " << up_ratio << " < min "
	    << mon->cct->_conf->mon_osd_min_up_ratio
	    << ", will not mark osd." << i << " down" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::can_mark_up(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NOUP)) {
    ldout(mon->cct, 5) << "can_mark_up NOUP flag set, will not mark osd." << i << " up" << dendl;
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
  if (osdmap.test_flag(CEPH_OSDMAP_NOOUT)) {
    ldout(mon->cct, 5) << "can_mark_out NOOUT flag set, will not mark osds out" << dendl;
    return false;
  }
  int in = osdmap.get_num_in_osds() - pending_inc.get_net_marked_out(&osdmap);
  float in_ratio = (float)in / (float)osdmap.get_num_osds();
  if (in_ratio < mon->cct->_conf->mon_osd_min_in_ratio) {
    if (i >= 0)
      ldout(mon->cct, 5) << "can_mark_down current in_ratio " << in_ratio << " < min "
	      << mon->cct->_conf->mon_osd_min_in_ratio
	      << ", will not mark osd." << i << " out" << dendl;
    else
      ldout(mon->cct, 5) << "can_mark_down current in_ratio " << in_ratio << " < min "
	      << mon->cct->_conf->mon_osd_min_in_ratio
	      << ", will not mark osds out" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::can_mark_in(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NOIN)) {
    ldout(mon->cct, 5) << "can_mark_in NOIN flag set, will not mark osd." << i << " in" << dendl;
    return false;
  }
  return true;
}

void OSDMonitor::check_failures(ceph::real_time now)
{
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    check_failure(now, p->first, p->second);
  }
}

bool OSDMonitor::check_failure(ceph::real_time now, int target_osd,
			       failure_info_t& fi)
{
  ceph::timespan orig_grace(mon->cct->_conf->osd_heartbeat_grace);
  ceph::real_time max_failed_since = fi.get_failed_since();
  std::chrono::duration<double> failed_for(now - max_failed_since);

  ceph::timespan grace = orig_grace;
  ceph::timespan my_grace = 0ns, peer_grace = 0ns;
  if (mon->cct->_conf->mon_osd_adjust_heartbeat_grace) {
    std::chrono::duration<double> halflife(
      mon->cct->_conf->mon_osd_laggy_halflife);

    // scale grace period based on historical probability of 'lagginess'
    // (false positive failures due to slowness).
    const osd_xinfo_t& xi = osdmap.get_xinfo(target_osd);
    double decay = pow(.5, failed_for/halflife);
    my_grace = std::chrono::duration_cast<ceph::timespan>(
      decay * xi.laggy_interval * xi.laggy_probability);
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
      const osd_xinfo_t& xi = osdmap.get_xinfo(p->first);
      ceph::timespan elapsed = now - xi.down_stamp;
      double decay = pow(.5, elapsed / halflife);
      peer_grace += std::chrono::duration_cast<ceph::timespan>(
	decay * xi.laggy_interval * xi.laggy_probability);
    }
    peer_grace /= fi.reporters.size();
    grace += peer_grace;
  }

  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    ldout(mon->cct, 10) << " already pending failure" << dendl;
    return true;
  }

  if (failed_for >= grace &&
      ((int)fi.reporters.size() >=
       mon->cct->_conf->mon_osd_min_down_reporters) &&
      (fi.num_reports >= mon->cct->_conf->mon_osd_min_down_reports)) {
    ldout(mon->cct, 1) << " we have enough reports/reporters to mark osd."
		       << target_osd << " down" << dendl;
    pending_inc.new_state[target_osd] = CEPH_OSD_UP;

    mon->clog.info() << osdmap.get_inst(target_osd) << " failed ("
		     << fi.num_reports << " reports from "
		     << (int)fi.reporters.size() << " peers after "
		     << std::chrono::duration_cast<ceph::timespan>(failed_for)
		     << " >= grace " << grace << ")\n";
    return true;
  }
  return false;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  ldout(mon->cct, 1) << "prepare_failure " << m->get_target() << " from " << m->get_orig_source_inst()
	  << " is reporting failure:" << m->if_osd_failed() << dendl;

  int target_osd = m->get_target().name.num();
  int reporter = m->get_orig_source().num();
  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);

  // calculate failure time
  ceph::real_time now = ceph::real_clock::now();
  ceph::real_time failed_since
    = m->get_recv_stamp() - (m->failed_for ?
			     m->failed_for * 1s :
			     mon->cct->_conf->osd_heartbeat_grace);

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
	ldout(mon->cct, 10) << " removing last failure_info for osd." << target_osd << dendl;
	failure_info.erase(target_osd);
      } else {
	ldout(mon->cct, 10) << " failure_info for osd." << target_osd << " now "
		 << fi.reporters.size() << " reporters and "
		 << fi.num_reports << " reports" << dendl;
      }
    } else {
      ldout(mon->cct, 10) << " no failure_info for osd." << target_osd << dendl;
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
    if (osdmap.is_up(p->first)) {
      ++p;
    } else {
      ldout(mon->cct, 10) << "process_failures osd." << p->first << dendl;
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

void OSDMonitor::take_all_failures(list<MOSDFailure*>& ls)
{
  ldout(mon->cct, 10) << __func__ << " on " << failure_info.size() << " osds" << dendl;

  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    p->second.take_report_messages(ls);
  }
  failure_info.clear();
}


// boot --

bool OSDMonitor::preprocess_boot(MOSDBoot *m, unique_lock& l)
{
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    ldout(mon->cct, 0) << "got preprocess_boot message from entity with insufficient caps"
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->sb.cluster_fsid != mon->monmap->fsid) {
    ldout(mon->cct, 0) << "preprocess_boot on fsid " << m->sb.cluster_fsid
	    << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  if (m->get_orig_source_inst().addr.is_blank_ip()) {
    ldout(mon->cct, 0) << "preprocess_boot got blank addr for " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  assert(m->get_orig_source_inst().name.is_osd());

  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst()) {
    // yup.
    ldout(mon->cct, 7) << "preprocess_boot dup from " << m->get_orig_source_inst()
	    << " == " << osdmap.get_inst(from) << dendl;
    _booted(m, false, l);
    return true;
  }

  if (osdmap.exists(from) &&
      !osdmap.get_uuid(from).is_nil() &&
      osdmap.get_uuid(from) != m->sb.osd_fsid) {
    ldout(mon->cct, 7) << __func__ << " from " << m->get_orig_source_inst()
	    << " clashes with existing osd: different fsid"
	    << " (ours: " << osdmap.get_uuid(from)
	    << " ; theirs: " << m->sb.osd_fsid << ")" << dendl;
    goto ignore;
  }

  if (osdmap.exists(from) &&
      osdmap.get_info(from).up_from > m->version) {
    ldout(mon->cct, 7) << "prepare_boot msg from before last up_from, ignoring" << dendl;
    send_latest(m, m->sb.current_epoch+1);
    goto ignore;
  }

  // noup?
  if (!can_mark_up(from)) {
    ldout(mon->cct, 7) << "preprocess_boot ignoring boot from " << m->get_orig_source_inst() << dendl;
    send_latest(m, m->sb.current_epoch+1);
    return true;
  }

  ldout(mon->cct, 10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_boot(MOSDBoot *m)
{
  ldout(mon->cct, 7) << "prepare_boot from " << m->get_orig_source_inst() << " sb " << m->sb
	  << " cluster_addr " << m->cluster_addr
	  << " hb_back_addr " << m->hb_back_addr
	  << " hb_front_addr " << m->hb_front_addr
	  << dendl;

  assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();

  // does this osd exist?
  if (from >= osdmap.get_max_osd()) {
    ldout(mon->cct, 1) << "boot from osd." << from << " >= max_osd " << osdmap.get_max_osd() << dendl;
    m->put();
    return false;
  }

  int oldstate = osdmap.exists(from) ? osdmap.get_state(from) : CEPH_OSD_NEW;
  if (pending_inc.new_state.count(from))
    oldstate ^= pending_inc.new_state[from];

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    ldout(mon->cct, 7) << "prepare_boot was up, first marking down " << osdmap.get_inst(from) << dendl;
    // preprocess should have caught these;  if not, assert.
    assert(osdmap.get_inst(from) != m->get_orig_source_inst());
    assert(osdmap.get_uuid(from) == m->sb.osd_fsid);

    if (pending_inc.new_state.count(from) == 0 ||
	(pending_inc.new_state[from] & CEPH_OSD_UP) == 0) {
      // mark previous guy down
      pending_inc.new_state[from] = CEPH_OSD_UP;
    }
    wait_for_finished_proposal(CB_RetryMessage(this, m));
  } else if (pending_inc.new_up_client.count(from)) {
    //FIXME: should this be using new_up_client?
    // already prepared, just wait
    ldout(mon->cct, 7) << "prepare_boot already prepared, waiting on "
		       << m->get_orig_source_addr() << dendl;
    wait_for_finished_proposal(CB_RetryMessage(this, m));
  } else {
    // mark new guy up.
    pending_inc.new_up_client[from] = m->get_orig_source_addr();
    if (!m->cluster_addr.is_blank_ip())
      pending_inc.new_up_cluster[from] = m->cluster_addr;
    pending_inc.new_hb_back_up[from] = m->hb_back_addr;
    if (!m->hb_front_addr.is_blank_ip())
      pending_inc.new_hb_front_up[from] = m->hb_front_addr;

    // mark in?
    if ((mon->cct->_conf->mon_osd_auto_mark_auto_out_in && (oldstate & CEPH_OSD_AUTOOUT)) ||
	(mon->cct->_conf->mon_osd_auto_mark_new_in && (oldstate & CEPH_OSD_NEW)) ||
	(mon->cct->_conf->mon_osd_auto_mark_in)) {
      if (can_mark_in(from)) {
	pending_inc.new_weight[from] = CEPH_OSD_IN;
      } else {
	ldout(mon->cct, 7) << "prepare_boot NOIN set, will not mark in " << m->get_orig_source_addr() << dendl;
      }
    }

    down_pending_out.erase(from);  // if any

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // set uuid?
    ldout(mon->cct, 10) << " setting osd." << from << " uuid to " << m->sb.osd_fsid << dendl;
    if (!osdmap.exists(from) || osdmap.get_uuid(from) != m->sb.osd_fsid) {
      // preprocess should have caught this;  if not, assert.
      assert(!osdmap.exists(from) || osdmap.get_uuid(from).is_nil());
      pending_inc.new_uuid[from] = m->sb.osd_fsid;
    }

    // metadata
    bufferlist osd_metadata;
    ::encode(m->metadata, osd_metadata);
    pending_metadata[from] = osd_metadata;

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap.get_info(from);
    ldout(mon->cct, 10) << " old osd_info: " << info << dendl;

    osd_xinfo_t xi = osdmap.get_xinfo(from);
    if (m->boot_epoch == 0) {
      xi.laggy_probability *= (1.0 - mon->cct->_conf->mon_osd_laggy_weight);
      xi.laggy_interval *= (1.0 - mon->cct->_conf->mon_osd_laggy_weight);
      ldout(mon->cct, 10) << " not laggy, new xi " << xi << dendl;
    } else {
      if (xi.down_stamp > ceph::real_time::min()) {
	ceph::timespan interval = ceph::real_clock::now() - xi.down_stamp;
	// Ideally we should make all these parameters stop being doubles
	xi.laggy_interval
	  = std::chrono::duration_cast<
	    ceph::timespan>(interval *
			    mon->cct->_conf->mon_osd_laggy_weight +
			    xi.laggy_interval *
			    (1.0 - mon->cct->_conf->mon_osd_laggy_weight));
      }
      xi.laggy_probability =
	mon->cct->_conf->mon_osd_laggy_weight +
	xi.laggy_probability * (1.0 - mon->cct->_conf->mon_osd_laggy_weight);
      ldout(mon->cct, 10) << " laggy, now xi " << xi << dendl;
    }
    // set features shared by the osd
    xi.features = m->get_connection()->get_features();
    pending_inc.new_xinfo[from] = xi;

    // wait
    wait_for_finished_proposal(CB_Booted(this, m));
  }
  return true;
}

void OSDMonitor::_booted(MOSDBoot *m, bool logit, unique_lock& l)
{
  ldout(mon->cct, 7) << "_booted " << m->get_orig_source_inst()
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
    ldout(mon->cct, 0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      osdmap.get_inst(from) != m->get_orig_source_inst()) {
    ldout(mon->cct, 7) << "preprocess_alive ignoring alive message from down " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  if (osdmap.get_up_thru(from) >= m->want) {
    // yup.
    ldout(mon->cct, 7) << "preprocess_alive want up_thru " << m->want << " dup from " << m->get_orig_source_inst() << dendl;
    _reply_map(m, m->version);
    return true;
  }

  ldout(mon->cct, 10) << "preprocess_alive want up_thru " << m->want
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

  ldout(mon->cct, 7) << "prepare_alive want up_thru " << m->want << " have "
		     << m->version << " from " << m->get_orig_source_inst()
		     << dendl;
  // set to the latest map the OSD has
  pending_inc.new_up_thru[from] = m->version;
  wait_for_finished_proposal(CB_ReplyMap(this, m, m->version));
  return true;
}

void OSDMonitor::_reply_map(PaxosServiceMessage *m, epoch_t e)
{
  ldout(mon->cct, 7) << "_reply_map " << e
	  << " from " << m->get_orig_source_inst()
	  << dendl;
  send_latest(m, e);
}


// ---------------
// map helpers

void OSDMonitor::send_latest(PaxosServiceMessage *m, epoch_t start)
{
  ldout(mon->cct, 5) << "send_latest to " << m->get_orig_source_inst()
	  << " start " << start << dendl;
  if (start == 0)
    send_full(m);
  else
    send_incremental(m, start);
  m->put();
}


MOSDMap *OSDMonitor::build_latest_full()
{
  MOSDMap *r = new MOSDMap(mon->monmap->fsid, &osdmap);
  r->oldest_map = get_first_committed();
  r->newest_map = osdmap.get_epoch();
  return r;
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to)
{
  ldout(mon->cct, 10) << "build_incremental [" << from << ".." << to << "]" << dendl;
  MOSDMap *m = new MOSDMap(mon->monmap->fsid);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap.get_epoch();

  for (epoch_t e = to; e >= from && e > 0; e--) {
    bufferlist bl;
    int err = get_version(e, bl);
    if (err == 0) {
      assert(bl.length());
      // if (get_version(e, bl) > 0) {
      ldout(mon->cct, 20) << "build_incremental	inc " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } else {
      assert(err == -ENOENT);
      assert(!bl.length());
      get_version_full(e, bl);
      if (bl.length() > 0) {
      //else if (get_version("full", e, bl) > 0) {
      ldout(mon->cct, 20) << "build_incremental   full " << e << " "
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
  ldout(mon->cct, 5) << "send_full to " << m->get_orig_source_inst() << dendl;
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
  ldout(mon->cct, 5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << req->get_orig_source_inst()
	  << dendl;

  int osd = -1;
  if (req->get_source().is_osd()) {
    osd = req->get_source().num();
    map<int,epoch_t>::iterator p = osd_epoch.find(osd);
    if (p != osd_epoch.end()) {
      ldout(mon->cct, 10) << " osd." << osd << " should have epoch " << p->second << dendl;
      first = p->second + 1;
      if (first > osdmap.get_epoch())
	return;
    }
  }

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, bl);
    assert(err == 0);
    assert(bl.length());

    ldout(mon->cct, 20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap.get_fsid());
    m->oldest_map = first;
    m->newest_map = osdmap.get_epoch();
    m->maps[first] = bl;
    mon->send_reply(req, m);

    if (osd >= 0)
      osd_epoch[osd] = osdmap.get_epoch();
    return;
  }

  // send some maps.  it may not be all of them, but it will get them
  // started.
  epoch_t last = MIN(first + mon->cct->_conf->osd_map_message_max, osdmap.get_epoch());
  MOSDMap *m = build_incremental(first, last);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap.get_epoch();
  mon->send_reply(req, m);

  if (osd >= 0)
    osd_epoch[osd] = last;
}

void OSDMonitor::send_incremental(epoch_t first, ConnectionRef &con,
				  bool onetime)
{
  ldout(mon->cct, 5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << con << dendl;

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, bl);
    assert(err == 0);
    assert(bl.length());

    ldout(mon->cct, 20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap.get_fsid());
    m->oldest_map = first;
    m->newest_map = osdmap.get_epoch();
    m->maps[first] = bl;
    con->get_messenger()->send_message(m, con);
    first++;
  }

  while (first <= osdmap.get_epoch()) {
    epoch_t last = MIN(first + mon->cct->_conf->osd_map_message_max, osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last);
    con->get_messenger()->send_message(m, con);
    first = last + 1;
    if (onetime)
      break;
  }
}

void OSDMonitor::send_incremental(epoch_t first, entity_inst_t& dest, bool onetime)
{
  ldout(mon->cct, 5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << dest << dendl;

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, bl);
    assert(err == 0);
    assert(bl.length());

    ldout(mon->cct, 20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap.get_fsid());
    m->oldest_map = first;
    m->newest_map = osdmap.get_epoch();
    m->maps[first] = bl;
    mon->messenger->send_message(m, dest);
    first++;
  }

  while (first <= osdmap.get_epoch()) {
    epoch_t last = MIN(first + mon->cct->_conf->osd_map_message_max, osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last);
    mon->messenger->send_message(m, dest);
    first = last + 1;
    if (onetime)
      break;
  }
}

epoch_t OSDMonitor::blacklist(const entity_addr_t& a, ceph::real_time until)
{
  ldout(mon->cct, 10) << "blacklist " << a << " until " << until << dendl;
  pending_inc.new_blacklist[a] = until;
  return pending_inc.epoch;
}

void OSDMonitor::check_subs()
{
  ldout(mon->cct, 10) << __func__ << dendl;
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
  ldout(mon->cct, 10) << __func__ << " " << sub << " next " << sub->next
	   << (sub->onetime ? " (onetime)":" (ongoing)") << dendl;

  if (sub->next <= osdmap.get_epoch()) {
    Messenger *msgr = sub->session->con->get_messenger();
    if (sub->next >= 1)
      send_incremental(sub->next, sub->session->con, sub->incremental_onetime);
    else
      msgr->send_message(build_latest_full(), sub->session->con);
    if (sub->onetime)
      mon->session_map.remove_sub(sub);
    else
      sub->next = osdmap.get_epoch() + 1;
  }
}

// TICK

void OSDMonitor::tick(unique_lock& l)
{
  bool do_propose = false;
  if (!is_active()) return;

  ldout(mon->cct, 10) << osdmap << dendl;

  if (!mon->is_leader()) return;

  ceph::real_time now = ceph::real_clock::now();

  // mark osds down?
  check_failures(now);

  // mark down osds out?

  /* can_mark_out() checks if we can mark osds as being out. The -1 has no
   * influence at all. The decision is made based on the ratio of "in" osds,
   * and the function returns false if this ratio is lower that the minimum
   * ratio set by mon->cct->_conf->mon_osd_min_in_ratio. So it's not really up to us.
   */
  if (can_mark_out(-1)) {
    set<int> down_cache;  // quick cache of down subtrees

    auto i = down_pending_out.begin();
    while (i != down_pending_out.end()) {
      int o = i->first;
      std::chrono::duration<double> down(now - i->second);
      ++i;

      if (osdmap.is_down(o) &&
	  osdmap.is_in(o) &&
	  can_mark_out(o)) {
	ceph::timespan orig_grace
	  = mon->cct->_conf->mon_osd_down_out_interval;
	ceph::timespan grace = orig_grace;
	ceph::timespan my_grace = 0s;

	if (mon->cct->_conf->mon_osd_adjust_down_out_interval) {
	  // scale grace period the same way we do the heartbeat grace.
	  const osd_xinfo_t& xi = osdmap.get_xinfo(o);
	  std::chrono::duration<double>halflife(
	    mon->cct->_conf->mon_osd_laggy_halflife);
	  double decay = pow(.5, down / halflife);
	  my_grace = std::chrono::duration_cast<ceph::timespan>(
	    decay * xi.laggy_interval * xi.laggy_probability);
	  grace += my_grace;
	}

	if (mon->cct->_conf->mon_osd_down_out_interval > 0ns &&
	    down >= grace) {
	  ldout(mon->cct, 10)
	    << "tick marking osd." << o << " OUT after "
	    << std::chrono::duration_cast<ceph::timespan>(down)
	    << " sec (target " << grace << " = " << orig_grace << " + "
	    << my_grace << ")" << dendl;
	  pending_inc.new_weight[o] = CEPH_OSD_OUT;

	  // set the AUTOOUT bit.
	  if (pending_inc.new_state.count(o) == 0)
	    pending_inc.new_state[o] = 0;
	  pending_inc.new_state[o] |= CEPH_OSD_AUTOOUT;

	  do_propose = true;

	  mon->clog.info() << "osd." << o << " out (down for "
			   << std::chrono::duration_cast<ceph::timespan>(down)
			   << ")\n";
	} else
	  continue;
      }

      down_pending_out.erase(o);
    }
  } else {
    ldout(mon->cct, 10) << "tick NOOUT flag set, not checking down osds" << dendl;
  }

  // expire blacklisted items?
  for (auto& p : osdmap.blacklist) {
    if (p.second < now) {
      ldout(mon->cct, 10) << "expiring blacklist item " << p.first
			  << " expired " << p.second << " < now " << now
			  << dendl;
      pending_inc.old_blacklist.push_back(p.first);
      do_propose = true;
    }
  }

  if (pending_inc.new_flags != -1 &&
      (pending_inc.new_flags ^ osdmap.flags) & CEPH_OSDMAP_FULL) {
    ldout(mon->cct, 1) << "New setting for CEPH_OSDMAP_FULL -- doing propose" << dendl;
    do_propose = true;
  }

  if (do_propose)
    propose_pending(l);
}

void OSDMonitor::handle_osd_timeouts(
  const ceph::real_time &now, std::map<int,ceph::real_time> &last_osd_report,
  unique_lock &l)
{
  ceph::timespan timeo = mon->cct->_conf->mon_osd_report_timeout;
  int max_osd = osdmap.get_max_osd();
  bool new_down = false;

  for (int i=0; i < max_osd; ++i) {
    ldout(mon->cct, 30) << "handle_osd_timeouts: checking up on osd "
			<< i << dendl;
    if (!osdmap.exists(i))
      continue;
    if (!osdmap.is_up(i))
      continue;
    const std::map<int,ceph::real_time>::const_iterator t
      = last_osd_report.find(i);
    if (t == last_osd_report.end()) {
      // it wasn't in the map; start the timer.
      last_osd_report[i] = now;
    } else if (can_mark_down(i)) {
    ceph::timespan diff = now - t->second;
      if (diff > timeo) {
	mon->clog.info() << "osd." << i << " marked down after no pg stats "
			 << "for " << diff << "seconds\n";
	lderr(mon->cct) << "no osd stats from osd." << i << " since "
			<< t->second << ", " << diff
			<< " seconds ago.	marking down" << dendl;
	pending_inc.new_state[i] = CEPH_OSD_UP;
	new_down = true;
      }
    }
  }
  if (new_down) {
    propose_pending(l);
  }
}

void OSDMonitor::mark_all_down(unique_lock &l)
{
  assert(mon->is_leader());

  ldout(mon->cct, 7) << "mark_all_down" << dendl;

  set<int32_t> ls;
  osdmap.get_all_osds(ls);
  for (set<int32_t>::iterator it = ls.begin();
       it != ls.end();
       ++it) {
    if (osdmap.is_down(*it)) continue;
    pending_inc.new_state[*it] = CEPH_OSD_UP;
  }

  propose_pending(l);
}

void OSDMonitor::get_health(list<pair<health_status_t,string> >& summary,
			    list<pair<health_status_t,string> > *detail) const
{
  int num_osds = osdmap.get_num_osds();
  int num_up_osds = osdmap.get_num_up_osds();
  int num_in_osds = osdmap.get_num_in_osds();

  if (num_osds == 0) {
    summary.push_back(make_pair(HEALTH_ERR, "no osds"));
  } else {
    if (num_up_osds < num_in_osds) {
      ostringstream ss;
      ss << (num_in_osds - num_up_osds) << "/" << num_in_osds << " in osds are down";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));

      if (detail) {
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i) && !osdmap.is_up(i)) {
	    const osd_info_t& info = osdmap.get_info(i);
	    ostringstream ss;
	    ss << "osd." << i << " is down since epoch " << info.down_at
	       << ", last address " << osdmap.get_addr(i);
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
	}
      }
    }

    // warn about flags
    if (osdmap.test_flag(CEPH_OSDMAP_PAUSERD |
			 CEPH_OSDMAP_PAUSEWR |
			 CEPH_OSDMAP_NOUP |
			 CEPH_OSDMAP_NODOWN |
			 CEPH_OSDMAP_NOIN |
			 CEPH_OSDMAP_NOOUT)) {
      ostringstream ss;
      ss << osdmap.get_flag_string() << " flag(s) set";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    // Warn if 'mon_osd_down_out_interval' is set to zero.
    // Having this option set to zero on the leader acts much like the
    // 'noout' flag.  It's hard to figure out what's going wrong with clusters
    // without the 'noout' flag set but acting like that just the same, so
    // we report a HEALTH_WARN in case this option is set to zero.
    // This is an ugly hack to get the warning out, but until we find a way
    // to spread global options throughout the mon cluster and have all mons
    // using a base set of the same options, we need to work around this sort
    // of things.
    // There's also the obvious drawback that if this is set on a single
    // monitor on a 3-monitor cluster, this warning will only be shown every
    // third monitor connection.
    if (mon->cct->_conf->mon_warn_on_osd_down_out_interval_zero &&
	mon->cct->_conf->mon_osd_down_out_interval == 0ns) {
      ostringstream ss;
      ss << "mon." << mon->name << " has mon_osd_down_out_interval set to 0";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail) {
	ss << "; this has the same effect as the 'noout' flag";
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
  }
}

void OSDMonitor::dump_info(Formatter *f)
{
  f->open_object_section("osdmap");
  osdmap.dump(f);
  f->close_section();

  f->open_array_section("osd_metadata");
  for (int i=0; i<osdmap.get_max_osd(); ++i) {
    if (osdmap.exists(i)) {
      f->open_object_section("osd");
      f->dump_unsigned("id", i);
      dump_osd_metadata(i, f, NULL);
      f->close_section();
    }
  }
  f->close_section();

  f->dump_unsigned("osdmap_first_committed", get_first_committed());
  f->dump_unsigned("osdmap_last_committed", get_last_committed());
}

bool OSDMonitor::preprocess_command(MMonCommand *m, unique_lock &l)
{
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_last_committed(), l);
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", rdata,
		       get_last_committed(), l);
    return true;
  }

  string prefix;
  cmd_getval(mon->cct, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(mon->cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  if (prefix == "osd stat") {
    osdmap.print_summary(f.get(), ds);
    if (f)
      f->flush(rdata);
    else
      rdata.append(ds);
  }
  else if (prefix == "osd dump" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap") {
    string val;

    epoch_t epoch = 0;
    int64_t epochnum;
    cmd_getval(mon->cct, cmdmap, "epoch", epochnum, (int64_t)0);
    epoch = epochnum;

    OSDMap *p = &osdmap;
    if (epoch) {
      bufferlist b;
      int err = get_version_full(epoch, b);
      if (err == -ENOENT) {
	r = -ENOENT;
	ss << "there is no map for epoch " << epoch;
	goto reply;
      }
      assert(err == 0);
      assert(b.length());
      p = new OSDMap;
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
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i)) {
	    f->dump_int("osd", i);
	  }
	}
	f->close_section();
	f->flush(ds);
      } else {
	bool first = true;
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i)) {
	    if (!first)
	      ds << "\n";
	    first = false;
	    ds << i;
	  }
	}
      }
      rdata.append(ds);
    }  else if (prefix == "osd getmap") {
      p->encode(rdata, m->get_connection()->get_features());
      ss << "got osdmap epoch " << p->get_epoch();
    }
  } else if (prefix  == "osd find") {
    int64_t osd;
    if (!cmd_getval(mon->cct, cmdmap, "id", osd)) {
      ss << "unable to parse osd id value '"
	 << cmd_vartype_stringify(cmdmap["id"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    if (!osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(mon->cct, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));

    f->open_object_section("osd_location");
    f->dump_int("osd", osd);
    f->dump_stream("ip") << osdmap.get_addr(osd);
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd metadata") {
    int64_t osd;
    if (!cmd_getval(mon->cct, cmdmap, "id", osd)) {
      ss << "unable to parse osd id value '"
	 << cmd_vartype_stringify(cmdmap["id"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    if (!osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(mon->cct, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));
    f->open_object_section("osd_metadata");
    r = dump_osd_metadata(osd, f.get(), &ss);
    if (r < 0)
      goto reply;
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd blacklist ls") {
    if (f)
      f->open_array_section("blacklist");

    for (const auto& p : osdmap.blacklist) {
      if (f) {
	f->open_object_section("entry");
	f->dump_stream("addr") << p.first;
	f->dump_stream("until") << p.second;
	f->close_section();
      } else {
	stringstream ss;
	string s;
	ss << p.first << " " << p.second;
	getline(ss, s);
	s += "\n";
	rdata.append(s);
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    }
    ss << "listed " << osdmap.blacklist.size() << " entries";
  } else {
    // try prepare update
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(m, r, rs, rdata, get_last_committed(), l);
  return true;
}

int OSDMonitor::check_cluster_features(uint64_t features,
				       stringstream &ss)
{
  stringstream unsupported_ss;
  int unsupported_count = 0;
  if (!(mon->get_quorum_features() & features)) {
    unsupported_ss << "the monitor cluster";
    ++unsupported_count;
  }

  set<int32_t> up_osds;
  osdmap.get_up_osds(up_osds);
  for (set<int32_t>::iterator it = up_osds.begin();
       it != up_osds.end(); ++it) {
    const osd_xinfo_t &xi = osdmap.get_xinfo(*it);
    if ((xi.features & features) != features) {
      if (unsupported_count > 0)
	unsupported_ss << ", ";
      unsupported_ss << "osd." << *it;
      unsupported_count ++;
    }
  }

  if (unsupported_count > 0) {
    ss << "features " << features << " unsupported by: "
       << unsupported_ss.str();
    return -ENOTSUP;
  }

  // check pending osd state, too!
  for (map<int32_t,osd_xinfo_t>::const_iterator p =
	 pending_inc.new_xinfo.begin();
       p != pending_inc.new_xinfo.end(); ++p) {
    const osd_xinfo_t &xi = p->second;
    if ((xi.features & features) != features) {
      ldout(mon->cct, 10) << __func__ << " pending osd." << p->first
	       << " features are insufficient; retry" << dendl;
      return -EAGAIN;
    }
  }

  return 0;
}

bool OSDMonitor::prepare_set_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags |= flag;
  ss << "set " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(Monitor::CB_Command(mon, m, 0, ss.str(),
						 get_last_committed() + 1));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags &= ~flag;
  ss << "unset " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(Monitor::CB_Command(mon, m, 0, ss.str(),
						 get_last_committed() + 1));
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

bool OSDMonitor::prepare_command(MMonCommand *m, unique_lock& l)
{
  stringstream ss;
  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_last_committed(), l);
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", get_last_committed(), l);
    return true;
  }

  return prepare_command_impl(m, cmdmap, l);
}

bool OSDMonitor::prepare_command_impl(MMonCommand *m,
				      map<string,cmd_vartype> &cmdmap,
				      unique_lock& l)
{
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  string format;
  cmd_getval(mon->cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  string prefix;
  cmd_getval(mon->cct, cmdmap, "prefix", prefix);

  int64_t id;
  string name;
  bool osdid_present = cmd_getval(mon->cct, cmdmap, "id", id);
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << id;
    name = oss.str();
  }

  // Even if there's a pending state with changes that could affect
  // a command, considering that said state isn't yet committed, we
  // just don't care about those changes if the command currently being
  // handled acts as a no-op against the current committed state.
  // In a nutshell, we assume this command  happens *before*.
  //
  // Let me make this clearer:
  //
  //   - If we have only one client, and that client issues some
  //	 operation that would conflict with this operation  but is
  //	 still on the pending state, then we would be sure that said
  //	 operation wouldn't have returned yet, so the client wouldn't
  //	 issue this operation (unless the client didn't wait for the
  //	 operation to finish, and that would be the client's own fault).
  //
  //   - If we have more than one client, each client will observe
  //	 whatever is the state at the moment of the commit.  So, if we
  //	 have two clients, one issuing an unlink and another issuing a
  //	 link, and if the link happens while the unlink is still on the
  //	 pending state, from the link's point-of-view this is a no-op.
  //	 If different clients are issuing conflicting operations and
  //	 they care about that, then the clients should make sure they
  //	 enforce some kind of concurrency mechanism -- from our
  //	 perspective that's what Douglas Adams would call an SEP.
  //
  // This should be used as a general guideline for most commands handled
  // in this function.	Adapt as you see fit, but please bear in mind that
  // this is the expected behavior.


  if (prefix == "osd setmaxosd") {
    int64_t newmax;
    if (!cmd_getval(mon->cct, cmdmap, "newmax", newmax)) {
      ss << "unable to parse 'newmax' value '"
	 << cmd_vartype_stringify(cmdmap["newmax"]) << "'";
      err = -EINVAL;
      goto reply;
    }

    if (newmax > mon->cct->_conf->mon_max_osd) {
      err = -ERANGE;
      ss << "cannot set max_osd to " << newmax << " which is > conf.mon_max_osd ("
	 << mon->cct->_conf->mon_max_osd << ")";
      goto reply;
    }

    pending_inc.new_max_osd = newmax;
    ss << "set new max_osd = " << pending_inc.new_max_osd;
    getline(ss, rs);
    wait_for_finished_proposal(Monitor::CB_Command(mon, m, 0, rs,
						   get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pause") {
    return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd unpause") {
    return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd set") {
    string key;
    cmd_getval(mon->cct, cmdmap, "key", key);
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
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd unset") {
    string key;
    cmd_getval(mon->cct, cmdmap, "key", key);
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
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd down" ||
	     prefix == "osd out" ||
	     prefix == "osd in" ||
	     prefix == "osd rm") {

    bool any = false;

    vector<string> idvec;
    cmd_getval(mon->cct, cmdmap, "ids", idvec);
    for (unsigned j = 0; j < idvec.size(); j++) {
      long osd = parse_osd_id(idvec[j].c_str(), &ss);
      if (osd < 0) {
	ss << "invalid osd id" << osd;
	err = -EINVAL;
	continue;
      } else if (!osdmap.exists(osd)) {
	ss << "osd." << osd << " does not exist. ";
	continue;
      }
      if (prefix == "osd down") {
	if (osdmap.is_down(osd)) {
	  ss << "osd." << osd << " is already down. ";
	} else {
	  pending_inc.new_state[osd] = CEPH_OSD_UP;
	  ss << "marked down osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd out") {
	if (osdmap.is_out(osd)) {
	  ss << "osd." << osd << " is already out. ";
	} else {
	  pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	  ss << "marked out osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd in") {
	if (osdmap.is_in(osd)) {
	  ss << "osd." << osd << " is already in. ";
	} else {
	  pending_inc.new_weight[osd] = CEPH_OSD_IN;
	  ss << "marked in osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd rm") {
	if (osdmap.is_up(osd)) {
	  if (any)
	    ss << ", ";
	  ss << "osd." << osd << " is still up; must be down before removal. ";
	  err = -EBUSY;
	} else {
	  pending_inc.new_state[osd] = osdmap.get_state(osd);
	  pending_inc.new_uuid[osd] = boost::uuids::nil_uuid();
	  pending_metadata_rm.insert(osd);
	  if (any) {
	    ss << ", osd." << osd;
	  } else {
	    ss << "removed osd." << osd;
	  }
	  any = true;
	}
      }
    }
    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(Monitor::CB_Command(
				   mon, m, err, rs,
				   get_last_committed() + 1));
      return true;
    }
  }  else if (prefix == "osd reweight") {
    int64_t id;
    if (!cmd_getval(mon->cct, cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
	 << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(mon->cct, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap["weight"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    long ww = (int)((double)CEPH_OSD_IN*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply;
    }
    if (osdmap.exists(id)) {
      pending_inc.new_weight[id] = ww;
      ss << "reweighted osd." << id << " to " << w << " (" << std::ios::hex
	 << ww << std::ios::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(Monitor::CB_Command(
				   mon, m, 0, rs, get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "osd create") {
    int i = -1;

    // optional uuid provided?
    boost::uuids::uuid id;
    string uuidstr;
    if (cmd_getval(mon->cct, cmdmap, "uuid", uuidstr)) {
      boost::uuids::string_generator parse;
      try {
	id = parse(uuidstr);
      } catch (std::runtime_error& e) {
	return false;
	err = -EINVAL;
	goto reply;
      }

      ldout(mon->cct, 10) << " osd create got uuid " << id << dendl;
      i = osdmap.identify_osd(id);
      if (i >= 0) {
	// osd already exists
	err = 0;
	if (f) {
	  f->open_object_section("created_osd");
	  f->dump_int("osdid", i);
	  f->close_section();
	  f->flush(rdata);
	} else {
	  ss << i;
	  rdata.append(ss);
	}
	goto reply;
      }
      i = pending_inc.identify_osd(id);
      if (i >= 0) {
	// osd is about to exist
	wait_for_finished_proposal(
	  CB_RetryMessage(this, m));
	return true;
      }
    }

    // allocate a new id
    for (i=0; i < osdmap.get_max_osd(); i++) {
      if (!osdmap.exists(i) &&
	  pending_inc.new_up_client.count(i) == 0 &&
	  (pending_inc.new_state.count(i) == 0 ||
	   (pending_inc.new_state[i] & CEPH_OSD_EXISTS) == 0))
	goto done;
    }

    // raise max_osd
    if (pending_inc.new_max_osd < 0)
      pending_inc.new_max_osd = osdmap.get_max_osd() + 1;
    else
      pending_inc.new_max_osd++;
    i = pending_inc.new_max_osd - 1;

done:
    ldout(mon->cct, 10) << " creating osd." << i << dendl;
    pending_inc.new_state[i] |= CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    if (!id.is_nil())
      pending_inc.new_uuid[i] = id;
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", i);
      f->close_section();
      f->flush(rdata);
    } else {
      ss << i;
      rdata.append(ss);
    }
    wait_for_finished_proposal(Monitor::CB_Command(mon, m, 0, rs, rdata,
						   get_last_committed() + 1));
    return true;

  } else if (prefix == "osd blacklist") {
    string addrstr;
    cmd_getval(mon->cct, cmdmap, "addr", addrstr);
    entity_addr_t addr;
    if (!addr.parse(addrstr.c_str(), 0))
      ss << "unable to parse address " << addrstr;
    else {
      string blacklistop;
      cmd_getval(mon->cct, cmdmap, "blacklistop", blacklistop);
      if (blacklistop == "add") {
	ceph::real_time expires = ceph::real_clock::now();
	int64_t d;
	// default one hour
	cmd_getval(mon->cct, cmdmap, "expire", d, 3600L);
	expires += d * 1s;

	pending_inc.new_blacklist[addr] = expires;
	ss << "blacklisting " << addr << " until " << expires << " (" << d
	   << " sec)";
	getline(ss, rs);
	wait_for_finished_proposal(
	  Monitor::CB_Command(mon, m, 0, rs, get_last_committed() + 1));
	return true;
      } else if (blacklistop == "rm") {
	if (osdmap.is_blacklisted(addr) ||
	    pending_inc.new_blacklist.count(addr)) {
	  if (osdmap.is_blacklisted(addr))
	    pending_inc.old_blacklist.push_back(addr);
	  else
	    pending_inc.new_blacklist.erase(addr);
	  ss << "un-blacklisting " << addr;
	  getline(ss, rs);
	  wait_for_finished_proposal(
	    Monitor::CB_Command(mon, m, 0, rs, get_last_committed() + 1));
	  return true;
	}
	ss << addr << " isn't blacklisted";
	err = 0;
	goto reply;
      }
    }
  } else if (prefix == "osd placer create erasure") {
    /* Factor this out into its own function sometime. */
    string name;
    int64_t stripe_unit; // This is the /desired/ stripe unit. You
			 // are not guaranteed to get it, but it
			 // will probably be close.
    string plugin;
    string params;
    string place_text;
    string symbols;

    PlacerRef placer;

    cmd_getval(mon->cct, cmdmap, "placerName", name);
    cmd_getval(mon->cct, cmdmap, "stripeUnit", stripe_unit, 32768L);
    cmd_getval(mon->cct, cmdmap, "erasurePluginName", plugin,
	       string("jerasure"));
    cmd_getval(mon->cct, cmdmap, "erasureParams", params,
	       string("technique=reed_sol_van k=2 m=1"));
    cmd_getval(mon->cct, cmdmap, "placeCode", place_text, string());
    cmd_getval(mon->cct, cmdmap, "placeSymbols", symbols, string());

    if (!Placer::valid_name(name, ss)) {
      err = -EINVAL;
      goto reply;
    }
    placer = ErasureCPlacer::create(mon->cct, name, stripe_unit, plugin, params,
			       place_text, symbols, ss);
    if (!placer) {
      err = -EINVAL;
      goto reply;
    }
    ss << "ErasureCPlacer: " << placer << " created.";
    pending_inc.include_addition(placer);
    wait_for_finished_proposal(Monitor::CB_Command(
				 mon, m, 0, rs,
				 get_last_committed() + 1));
    return true;
  } else if (prefix == "osd placer create striped") {
    /* Factor this out into its own function sometime. */
    string name;
    int64_t stripe_unit;
    int64_t stripe_width;

    PlacerRef placer;

    cmd_getval(mon->cct, cmdmap, "placerName", name);
    cmd_getval(mon->cct, cmdmap, "stripeUnit", stripe_unit, 32768L);
    cmd_getval(mon->cct, cmdmap, "stripeWidth", stripe_width, 4L);

    if (!Placer::valid_name(name, ss)) {
      err = -EINVAL;
      goto reply;
    }
    placer = StripedPlacer::create(mon->cct, name, stripe_unit, stripe_width,
				   ss);
    if (!placer) {
      err = -EINVAL;
      goto reply;
    }
    ss << "StripedPlacer: " << placer << " created.";
    pending_inc.include_addition(placer);
    wait_for_finished_proposal(Monitor::CB_Command(
				 mon, m, 0, rs,
				 get_last_committed() + 1));
    return true;
  } else if (prefix == "osd placer remove") {
    string name;
    PlacerRef placer;

    cmd_getval(mon->cct, cmdmap, "placerName", name);
    if (!osdmap.find_by_name(name, placer)) {
      ss << "placer named " << name << " not found";
      err = -EINVAL;
      goto reply;
    }
    pending_inc.include_removal(placer);
    wait_for_finished_proposal(Monitor::CB_Command(
				 mon, m, 0, rs,
				 get_last_committed() + 1));
    return true;
  } else if (prefix == "osd placer list") {
    string pattern;
    stringstream ds;
    bool first = true;

    cmd_getval(mon->cct, cmdmap, "pattern", pattern, string("."));
    try {
      const boost::regex e(pattern);
      if (f)
	f->open_array_section("placers");
      for (auto pl = osdmap.placers.by_uuid.begin();
	   pl != osdmap.placers.by_uuid.end(); ++pl) {
	if (!regex_search(pl->second->name, e,
		boost::regex_constants::match_any))
	  continue;
	if (f) {
	  f->open_object_section("placer_info");
	  pl->second->dump(f.get());
	  f->close_section();
	} else {
	  if (!first) ds << "\n";
	  ds << *pl->second;
	  first = false;
	}
      }
      if (f) {
	f->close_section();
	f->flush(ds);
      }
      rdata.append(ds);
    } catch (boost::regex_error& e) {
      ss << e.what();
      err = -EINVAL;
      goto reply;
    }
  } else if (prefix == "osd volume create") {
    /* Factor this out into its own function sometime. */
    string volumeName;
    string placerName;

    PlacerRef placer;
    VolumeRef vol;

    cmd_getval(mon->cct, cmdmap, "volumeName", volumeName);
    cmd_getval(mon->cct, cmdmap, "placerName", placerName);

    /* Only one volume type for now, when we implement more I'll
       come back and complexify this. */

    if (!Placer::valid_name(placerName, ss)) {
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.find_by_name(placerName, placer)) {
      ss << "placer named " << placerName << " not found";
      err = -EINVAL;
      goto reply;
    }
    vol = CohortVolume::create(mon->cct, volumeName, placer, ss);
    if (!vol) {
      err = -EINVAL;
      goto reply;
    }
    ss << "volume: " << vol << " created.";
    pending_inc.include_addition(vol);
    wait_for_finished_proposal(Monitor::CB_Command(
				 mon, m, 0, rs,
				 get_last_committed() + 1));
    return true;
  } else if (prefix == "osd volume remove") {
    string name;
    VolumeRef vol;
    PlacerRef placer;

    cmd_getval(mon->cct, cmdmap, "volumeName", name);
    if (!osdmap.find_by_name(name, vol)) {
      ss << "volume named " << name << " not found";
      err = -EINVAL;
      goto reply;
    }
    pending_inc.include_removal(vol);
    wait_for_finished_proposal(Monitor::CB_Command(
				 mon, m, 0, rs,
				 get_last_committed() + 1));
    return true;
  } else if (prefix == "osd volume list") {
    string pattern;
    stringstream ds;
    bool first = true;

    cmd_getval(mon->cct, cmdmap, "pattern", pattern, string("."));
    try {
      const boost::regex e(pattern);
      if (f)
	f->open_array_section("volumes");
      for (auto v = osdmap.vols.by_uuid.begin(); v != osdmap.vols.by_uuid.end(); ++v) {
	if (!regex_search(v->second->name, e,
		boost::regex_constants::match_any))
	  continue;
	if (f) {
	  f->open_object_section("volume_info");
	  v->second->dump(f.get());
	  f->close_section();
	} else {
	  if (!first) ds << "\n";
	  ds << *v->second;
	  first = false;
	}
      }
      if (f) {
	f->close_section();
	f->flush(ds);
      }
      rdata.append(ds);
    } catch (boost::regex_error& e) {
      ss << e.what();
      err = -EINVAL;
      goto reply;
    }
  } else {
    err = -EINVAL;
  }

 reply:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0)
    rs = cpp_strerror(err);
  mon->reply_command(m, err, rs, rdata, get_last_committed(), l);
  return ret;
}
