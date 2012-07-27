// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 *
 */

#include "VolMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/config.h"

#include "messages/MMonCommand.h"


#define dout_subsys ceph_subsys_mon


/*
 * pure virtual functions defined in PaxosService
 */


void VolMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;
}


void VolMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();
  if (paxosv == volmap.version) {
    return;
  }
  assert(paxosv >= volmap.version);

  if (volmap.version != paxos->get_stashed_version()) {
    bufferlist latest;
    version_t v = paxos->get_stashed(latest);
    dout(7) << "update_from_paxos loading latest full volmap v" << v << dendl;
    try {
      VolMap tmp_volmap;
      bufferlist::iterator p = latest.begin();
      tmp_volmap.decode(p);
      volmap = tmp_volmap;
    } catch (const std::exception &e) {
      dout(0) << "update_from_paxos: error parsing update: "
	      << e.what() << dendl;
      assert(0 == "update_from_paxos: error parsing update");
      return;
    }
  }

  // walk through incrementals
  utime_t now(ceph_clock_now(g_ceph_context));
  while (paxosv > volmap.version) {
    bufferlist bl;
    const bool success = paxos->read(volmap.version+1, bl);
    assert(success);

    dout(7) << "update_from_paxos applying incremental " << volmap.version+1 << dendl;
    VolMap::Incremental inc;
    try {
      bufferlist::iterator p = bl.begin();
      inc.decode(p);
    } catch (const std::exception &e) {
      dout(0) << "update_from_paxos: error parsing "
	      << "incremental update: " << e.what() << dendl;
      assert(0 == "update_from_paxos: error parsing incremental update");
      return;
    }

    volmap.apply_incremental(inc);

    dout(10) << volmap << dendl;
  }

  assert(paxosv == volmap.version);

  // save latest
  bufferlist bl;
  volmap.encode(bl);
  paxos->stash_latest(paxosv, bl);

  // dump pgmap summaries?  (useful for debugging)
  if (0) {
    stringstream ds;
    volmap.dump(ds);
    bufferlist d;
    d.append(ds);
    mon->store->put_bl_sn(d, "volmap_dump", paxosv);
  }

  unsigned max = g_conf->mon_max_volmap_epochs;
  if (mon->is_leader() &&
      paxosv > max) {
    paxos->trim_to(paxosv - max);
  }

  // TODO : NEEDED?
  // send_vol_creates();

  // TODO : NEEDED?
  // update_logger();
}


void VolMonitor::create_pending()
{
  pending_volmap = volmap;
  pending_volmap.epoch++;

  pending_inc = VolMap::Incremental();
  pending_inc.version = volmap.version + 1;

  dout(10) << "create_pending v" << pending_inc.version << dendl;
}


void VolMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending v" << pending_inc.version << dendl;

  //print_map(pending_volmap);

  // apply to paxos
  assert(paxos->get_version() + 1 == pending_inc.version);
  pending_inc.encode(bl);
}


bool VolMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {

  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand *) m);

    /* ERIC REMOVE

  case MSG_MDS_BEACON:
    return preprocess_beacon((MMDSBeacon*)m);

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets((MMDSLoadTargets*)m);
    */

  default:
    assert(0);
    m->put();
    return true;
  }
}


bool VolMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(10) << "prepare_upate " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand *) m);

    /* ERIC REMOVE

  case MSG_MDS_BEACON:
    return preprocess_beacon((MMDSBeacon*)m);

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets((MMDSLoadTargets*)m);
    */

  default:
    assert(0);
    m->put();
    return true;
  }
}


/*
 * other member functions
 */

/*
 * Commands handled:
 *     list
 *     lookup <uuid>|<name>
 */
bool VolMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  stringstream ss;
  bufferlist rdata;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "list"  && m->cmd.size() == 2) {
      if (volmap.empty()) {
	ss << "volmap is empty" << std::endl;
      } else {
	ss << "volmap has " << volmap.size() << " entries" << std::endl;
	stringstream ds;
	for (map<string,VolMap::vol_info_t>::const_iterator i = volmap.begin();
	     i != volmap.end();
	     ++i) {
	  ds << i->second.uuid << " " << i->second.crush_map_entry
	     << " " << i->first << std::endl;
	}
	rdata.append(ds);
      }
      r = 0;
    } else if (m->cmd[1] == "lookup" && m->cmd.size() == 3) {
      ss << "got lookup command";
      r = -ENOSYS;
    }
  }

  string rs;
  getline(ss, rs);

  if (r != -1) {
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else {
    return false;
  }
}


/*
 * Commands handled:
 *     create <name> <crush_map_entry>
 *     add <uuid> <name> <crush_map_entry>
 *     remove <uuid> <name> <crush_map_entry>
 *     rename <uuid> <name>
 *     ? recrush <uuid> <crush_map_entry>
 */

bool VolMonitor::prepare_command(MMonCommand *m)
{
  int r = -1;
  stringstream ss;
  bufferlist rdata;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "create" && m->cmd.size() == 4) {
      const string name = m->cmd[2];
      const uint16_t crush_map_entry = (uint16_t) atoi(m->cmd[3].c_str());
      uuid_d uuid;

      r = pending_volmap.create_volume(name, crush_map_entry, uuid);
      if (r == 0) {
	ss << "volume " << uuid << " created with name \"" << name << "\"";
	pending_inc.include_addition(uuid, name, crush_map_entry);
      } else if (r == -EEXIST) {
	ss << "volume with name \"" << name << "\" already exists";
      } else {
	ss << "volume could not be created due to error code " << -r;
      }
    } else if (m->cmd[1] == "add" && m->cmd.size() == 5) {
      ss << "got add command";
      r = -ENOSYS;
    }
  }

  if (r == -1) {
    r = -EINVAL;
    ss << "unrecognized command";
  }
  string rs;
  getline(ss, rs);

  if (r != -1) {
    // success.. delay reply
    // paxos->wait_for_commit(new Monitor::C_Command(mon, m, r, rs, paxos->get_version()));
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else {
    return false;
  }
}
