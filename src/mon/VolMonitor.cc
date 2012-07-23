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

#include "common/strtol.h"
#include "common/ceph_argparse.h"
/* #include "common/debug.h" */
#include "common/config.h"

#include "messages/MMonCommand.h"


#define dout_subsys ceph_subsys_mon


/*
 * pure virtual functions defined in PaxosService
 */

void VolMonitor::create_initial()
{
}

void VolMonitor::update_from_paxos()
{
}

void VolMonitor::create_pending()
{
}

void VolMonitor::encode_pending(bufferlist &bl)
{
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
  return true;
}

/*
 * other member functions
 */

/*
 * create <name> <crush_map_entry>
 * add <uuid> <name> <crush_map_entry>
 * list
 * lookup <uuid>|<name>
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
    } else if (m->cmd[1] == "create" && m->cmd.size() == 4) {
      ss << "got create command";
      r = -ENOSYS;
    } else if (m->cmd[1] == "add" && m->cmd.size() == 5) {
      ss << "got add command";
      r = -ENOSYS;
    }
  }

  if (r == -EINVAL) {
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
