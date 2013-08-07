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


void VolMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_version();
  if (version == volmap->version)
    return;
  assert(version >= volmap->version);

  /* Obtain latest full pgmap version, if available and whose version is
   * greater than the current pgmap's version.
   */
  version_t latest_full = get_version_latest_full();
  if ((latest_full > 0) && (latest_full > volmap->version)) {
    bufferlist latest_bl;
    int err = get_version_full(latest_full, latest_bl);
    assert(err == 0);
    dout(7) << __func__ << " loading latest full volmap v"
	    << latest_full << dendl;
    try {
      VolMapRef tmp_volmap(new VolMap);
      bufferlist::iterator p = latest_bl.begin();
      tmp_volmap->decode(p);
      volmap = tmp_volmap;
    } catch (const std::exception& e) {
      dout(0) << __func__ << ": error parsing update: "
	      << e.what() << dendl;
      assert(0 == "update_from_paxos: error parsing update");
      return;
    }
  }

  // walk through incrementals
  while (version > volmap->version) {
    bufferlist bl;
    int err = get_version(volmap->version + 1, bl);
    assert(err == 0);
    assert(bl.length());

    dout(7) << "update_from_paxos  applying incremental "
	    << volmap->version + 1 << dendl;
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

    volmap->apply_incremental(g_ceph_context, inc);

    dout(10) << volmap << dendl;
  }

  assert(version == volmap->version);

  update_trim();
}


void VolMonitor::create_pending()
{
  pending_volmap.reset(new VolMap(*volmap));
  pending_volmap->epoch++;

  pending_inc = VolMap::Incremental();
  pending_inc.version = volmap->version + 1;

  dout(10) << "create_pending v" << pending_inc.version << dendl;
}

void VolMonitor::encode_pending(MonitorDBStore::Transaction* t)
{
  version_t version = pending_inc.version;
  dout(10) << __func__ << " v " << version << dendl;
  assert(get_version() + 1 == version);

  bufferlist bl;
  pending_inc.encode(bl, mon->get_quorum_features());

  put_version(t, version, bl);
  put_last_committed(t, version);
}

void VolMonitor::encode_full(MonitorDBStore::Transaction* t)
{
  dout(10) << __func__ << " volmap v " << volmap->version << dendl;
  assert(get_version() == volmap->version);

  bufferlist full_bl;
  volmap->encode(full_bl, mon->get_quorum_features());

  put_version_full(t, volmap->version, full_bl);
  put_version_latest_full(t, volmap->version);
}

void VolMonitor::update_trim()
{
  unsigned max = g_conf->mon_max_pgmap_epochs;
  version_t version = get_version();
  if (mon->is_leader() && (version > max))
    set_trim_to(version - max);
}

bool VolMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from "
	   << m->get_orig_source_inst() << dendl;

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

  // case OTHER TYPES OF COMMANDS?

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
    if (m->cmd[1] == "list" && m->cmd.size() == 2) {
      if (volmap->empty()) {
	ss << "volmap is empty" << std::endl;
      } else {
	ss << "volmap has " << volmap->size() << " entries" << std::endl;
	stringstream ds;
	for (map<string,VolMap::vol_info_t>::const_iterator i
	       = volmap->begin_n();
	     i != volmap->end_n();
	     ++i) {
	  ds << i->second << std::endl;
	}
	rdata.append(ds);
      }
      r = 0;
    } else if (m->cmd[1] == "lookup" && m->cmd.size() == 3) {
      const string& searchKey = m->cmd[2];
      const size_t maxResults = 100;
      const vector<VolMap::vol_info_t> results
	= volmap->search_vol_info(searchKey, maxResults);
      if (results.empty()) {
	ss << "no volmap entries match \"" << searchKey << "\"";
	r = -ENOENT;
      } else {
	if (results.size() == 1) {
	  ss << "matching volmap entry";
	} else if (results.size() == maxResults) {
	  ss << "maximum matching volmap entries; could be more";
	} else {
	  ss << "matching volmap entries";
	}
	stringstream ds;
	for (vector<VolMap::vol_info_t>::const_iterator i = results.begin();
	     i != results.end();
	     ++i) {
	  ds << *i << std::endl;
	}
	rdata.append(ds);
	r = 0;
      }
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
 *     create <name>
 *     remove <uuid> <name> # force user to type both to minimize odds of mistakes
 *     rename <uuid> <name>
 */

bool VolMonitor::prepare_command(MMonCommand *m)
{
  int r = -1;
  stringstream ss;
  bufferlist rdata;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "create" && m->cmd.size() == 4) {
      const string& name = m->cmd[2];
      uuid_d uuid;
      string error_message;

      if (!VolMap::is_valid_volume_name(name, error_message)) {
	ss << error_message;
	r = -EINVAL;
      } else {
	r = pending_volmap->create_volume(name, uuid);
	if (r == 0) {
	  ss << "volume " << uuid << " created with name \"" << name << "\"";
	  pending_inc.include_addition(uuid, name);
	} else if (r == -EEXIST) {
	  ss << "volume with name \"" << name << "\" already exists";
	} else {
	  ss << "volume could not be created due to error code " << -r;
	}
      }
    } else if (m->cmd[1] == "remove" && m->cmd.size() == 4) {
      const string& uuid_str = m->cmd[2];
      const string& name = m->cmd[3];
      string error_message;

      if (VolMap::is_valid_volume_name(name, error_message)) {
	uuid_d uuid;
	try {
	  uuid = uuid_d::parse(uuid_str);
	  r = pending_volmap->remove_volume(uuid, name);
	  if (r == 0) {
	    ss << "removed volume " << uuid << " \"" << name << "\"";
	    pending_inc.include_removal(uuid);
	  } else if (r == -ENOENT) {
	    ss << "no volume with provided uuid " << uuid << " exists";
	  } else if (r == -EINVAL) {
	    ss << "volume name \"" << name << "\" does not match volume with uuid " << uuid;
	  } else {
	    ss << "volume could not be removed due to error code " << -r;
	  }
	} catch (const std::invalid_argument& ia) {
	  ss << "provided volume uuid " << uuid << " is not a valid uuid";
	  r = -EINVAL;
	}
      } else {
	ss << error_message;
	r = -EINVAL;
      }
    } else if (m->cmd[1] == "rename" && m->cmd.size() == 4) {
      const string& volspec = m->cmd[2];
      const string& new_name = m->cmd[3];
      uuid_d uuid;
      const bool is_unique = pending_volmap->get_vol_uuid(volspec, uuid);
      if (is_unique) {
	VolMap::vol_info_t vinfo_out;
	r = pending_volmap->rename_volume(uuid, new_name, vinfo_out);
	if (r == 0) {
	  pending_inc.include_update(vinfo_out);
	  ss << "volume " << uuid << " renamed to " << new_name;
	} else if (r == -EINVAL) {
	  ss << "volume name is invalid";
	} else if (r == -EEXIST) {
	  ss << "volume with name \"" << new_name << "\" already exists";
	} else {
	  ss << "volume could not be renamed due to error code " << -r;
	}
      } else {
	ss << "provided volume specifier \"" << volspec << "\" does not specify a unique volume";
	r = -ENOENT;
      }
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
