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
 * Foundation.	See file COPYING.
 *
 */

#include <sstream>
#include <cassert>

#include "MDSMonitor.h"
#include "Monitor.h"
#include "MonitorDBStore.h"
#include "OSDMonitor.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMonCommand.h"

#include "messages/MGenericMessage.h"

#include "common/Timer.h"

#include "common/config.h"

#include "MonitorDBStore.h"
#include "common/cmdparse.h"
#include "include/str_list.h"

#include "mds/mdstypes.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mdsmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, MDSMap& mdsmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mds e" << mdsmap.get_epoch() << " ";
}



// my methods

void MDSMonitor::print_map(MDSMap &m, int dbl)
{
  ldout(mon->cct, dbl) << "print_map\n";
  m.print(*_dout);
  *_dout << dendl;
}


// service methods
void MDSMonitor::create_initial()
{
  ldout(mon->cct, 10) << "create_initial" << dendl;
}


void MDSMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == mdsmap.epoch)
    return;
  assert(version >= mdsmap.epoch);

  ldout(mon->cct, 10) << __func__ << " version " << version
	   << ", my e " << mdsmap.epoch << dendl;

  // read and decode
  mdsmap_bl.clear();
  int err = get_version(version, mdsmap_bl);
  assert(err == 0);

  assert(mdsmap_bl.length() > 0);
  ldout(mon->cct, 10) << __func__ << " got " << version << dendl;
  mdsmap.decode(mdsmap_bl);

  // new map
  ldout(mon->cct, 4) << "new map" << dendl;
  print_map(mdsmap, 0);

  check_subs();
}

void MDSMonitor::create_pending()
{
  pending_mdsmap = mdsmap;
  pending_mdsmap.epoch++;
  ldout(mon->cct, 10) << "create_pending e" << pending_mdsmap.epoch << dendl;
}

void MDSMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  ldout(mon->cct, 10) << "encode_pending e" << pending_mdsmap.epoch << dendl;

  pending_mdsmap.modified = ceph::real_clock::now();

  // print map iff 'debug mon = 30' or higher
  print_map(pending_mdsmap, 30);

  // apply to paxos
  assert(get_last_committed() + 1 == pending_mdsmap.epoch);
  bufferlist mdsmap_bl;
  pending_mdsmap.encode(mdsmap_bl, mon->get_quorum_features());

  /* put everything in the transaction */
  put_version(t, pending_mdsmap.epoch, mdsmap_bl);
  put_last_committed(t, pending_mdsmap.epoch);
}

bool MDSMonitor::preprocess_query(PaxosServiceMessage *m, unique_lock& l)
{
  ldout(mon->cct, 10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {

  case MSG_MDS_BEACON:
    return preprocess_beacon(static_cast<MMDSBeacon*>(m));

  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m), l);

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets(static_cast<MMDSLoadTargets*>(m));

  default:
    assert(0);
    m->put();
    return true;
  }
}

void MDSMonitor::_note_beacon(MMDSBeacon *m)
{
  uint64_t gid = m->get_global_id();
  version_t seq = m->get_seq();

  ldout(mon->cct, 15) << "_note_beacon " << *m << " noting time" << dendl;
  last_beacon[gid].stamp = ceph::mono_clock::now();
  last_beacon[gid].seq = seq;
}

bool MDSMonitor::preprocess_beacon(MMDSBeacon *m)
{
  int state = m->get_state();
  uint64_t gid = m->get_global_id();
  version_t seq = m->get_seq();
  MDSMap::mds_info_t info;

  // check privileges, ignore if fails
  MonSession *session = m->get_session();
  if (!session)
    goto out;
  if (!session->is_capable("mds", MON_CAP_X)) {
    ldout(mon->cct, 0) << "preprocess_beacon got MMDSBeacon from entity with insufficient privileges "
	    << session->caps << dendl;
    goto out;
  }

  if (m->get_fsid() != mon->monmap->fsid) {
    ldout(mon->cct, 0) << "preprocess_beacon on fsid " << m->get_fsid() << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  ldout(mon->cct, 12) << "preprocess_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << " " << m->get_compat()
	   << dendl;

  // make sure the address has a port
  if (m->get_orig_source_addr().get_port() == 0) {
    ldout(mon->cct, 1) << " ignoring boot message without a port" << dendl;
    goto out;
  }

  // fw to leader?
  if (!mon->is_leader())
    return false;

  if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
    ldout(mon->cct, 7) << " mdsmap DOWN flag set, ignoring mds " << m->get_source_inst() << " beacon" << dendl;
    goto out;
  }

  // booted, but not in map?
  if (pending_mdsmap.is_dne_gid(gid)) {
    if (state != MDSMap::STATE_BOOT) {
      ldout(mon->cct, 7) << "mds_beacon " << *m << " is not in mdsmap" << dendl;
      mon->send_reply(m, new MMDSMap(cct, mon->monmap->fsid, &mdsmap));
      goto out;
    } else {
      return false;  // not booted yet.
    }
  }
  info = pending_mdsmap.get_info_gid(gid);

  // old seq?
  if (info.state_seq > seq) {
    ldout(mon->cct, 7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
    goto out;
  }

  if (mdsmap.get_epoch() != m->get_last_epoch_seen()) {
    ldout(mon->cct, 10) << "mds_beacon " << *m
	     << " ignoring requested state, because mds hasn't seen latest map" << dendl;
    goto ignore;
  }

  if (info.laggy()) {
    _note_beacon(m);
    return false;  // no longer laggy, need to update map.
  }
  if (state == MDSMap::STATE_BOOT) {
    // ignore, already booted.
    goto out;
  }
  // is there a state change here?
  if (info.state != state) {

    _note_beacon(m);
    return false;  // need to update map
  }

 ignore:
  // note time and reply
  _note_beacon(m);
  mon->send_reply(m,
		  new MMDSBeacon(mon->monmap->fsid, m->get_global_id(), m->get_name(),
				 mdsmap.get_epoch(), state, seq));

  // done
 out:
  m->put();
  return true;
}

bool MDSMonitor::preprocess_offload_targets(MMDSLoadTargets* m)
{
  ldout(mon->cct, 10) << "preprocess_offload_targets " << *m << " from " << m->get_orig_source() << dendl;
  uint64_t gid;

  // check privileges, ignore message if fails
  MonSession *session = m->get_session();
  if (!session)
    goto done;
  if (!session->is_capable("mds", MON_CAP_X)) {
    ldout(mon->cct, 0) << "preprocess_offload_targets got MMDSLoadTargets from entity with insufficient caps "
	    << session->caps << dendl;
    goto done;
  }

  gid = m->global_id;
  if (mdsmap.mds_info.count(gid) &&
      m->targets == mdsmap.mds_info[gid].export_targets)
    goto done;

  return false;

 done:
  m->put();
  return true;
}


bool MDSMonitor::prepare_update(PaxosServiceMessage *m, unique_lock& l)
{
  ldout(mon->cct, 7) << "prepare_update " << *m << dendl;

  switch (m->get_type()) {

  case MSG_MDS_BEACON:
    return prepare_beacon(static_cast<MMDSBeacon*>(m), l);

  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m), l);

  case MSG_MDS_OFFLOAD_TARGETS:
    return prepare_offload_targets(static_cast<MMDSLoadTargets*>(m));

  default:
    assert(0);
    m->put();
  }

  return true;
}



bool MDSMonitor::prepare_beacon(MMDSBeacon *m, unique_lock& l)
{
  // -- this is an update --
  ldout(mon->cct, 12) << "prepare_beacon " << *m << " from " << m->get_orig_source_inst() << dendl;
  entity_addr_t addr = m->get_orig_source_inst().addr;
  uint64_t gid = m->get_global_id();
  int state = m->get_state();
  version_t seq = m->get_seq();

  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // zap previous instance of this name?
    if (mon->cct->_conf->mds_enforce_unique_name) {
      bool failed_mds = false;
      while (uint64_t existing = pending_mdsmap.find_mds_gid_by_name(m->get_name())) {
	if (!mon->osdmon()->is_writeable()) {
	  mon->osdmon()->wait_for_writeable(CB_RetryMessage(this, m));
	  return false;
	}
	fail_mds_gid(existing);
	failed_mds = true;
      }
      if (failed_mds) {
	assert(mon->osdmon()->is_writeable());
	request_proposal(mon->osdmon(), l);
      }
    }

    // add
    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];
    info.global_id = gid;
    info.name = m->get_name();
    info.addr = addr;
    info.state = MDSMap::STATE_BOOT;
    info.state_seq = seq;

    // initialize the beacon timer
    last_beacon[gid].stamp = ceph::mono_clock::now();
    last_beacon[gid].seq = seq;

  } else {
    // state change
    MDSMap::mds_info_t& info = pending_mdsmap.get_info_gid(gid);

    if (info.laggy()) {
      ldout(mon->cct, 10) << "prepare_beacon clearing laggy flag on " << addr << dendl;
      info.clear_laggy();
    }

    ldout(mon->cct, 10) << "prepare_beacon mds." << info.rank
	     << " " << ceph_mds_state_name(info.state)
	     << " -> " << ceph_mds_state_name(state)
	     << "  standby_for_rank=" << m->get_standby_for_rank()
	     << dendl;
    if (state == MDSMap::STATE_STOPPED) {
      pending_mdsmap.up.erase(info.rank);
      pending_mdsmap.stopped.insert(info.rank);
      pending_mdsmap.mds_info.erase(gid);  // last! info is a ref into this map
      last_beacon.erase(gid);
    } else {
      info.state = state;
      info.state_seq = seq;
    }
  }

  ldout(mon->cct, 7) << "prepare_beacon pending map now:" << dendl;
  print_map(pending_mdsmap);

  wait_for_finished_proposal(CB_Updated(this, m));

  return true;
}

bool MDSMonitor::prepare_offload_targets(MMDSLoadTargets *m)
{
  uint64_t gid = m->global_id;
  if (pending_mdsmap.mds_info.count(gid)) {
    ldout(mon->cct, 10) << "prepare_offload_targets " << gid << " " << m->targets << dendl;
    pending_mdsmap.mds_info[gid].export_targets = m->targets;
  } else {
    ldout(mon->cct, 10) << "prepare_offload_targets " << gid << " not in map" << dendl;
  }
  m->put();
  return true;
}

bool MDSMonitor::should_propose(ceph::timespan& delay)
{
  // delegate to PaxosService to assess whether we should propose
  return PaxosService::should_propose(delay);
}

void MDSMonitor::_updated(MMDSBeacon *m)
{
  ldout(mon->cct, 10) << "_updated " << m->get_orig_source() << " " << *m << dendl;
  mon->clog.info() << m->get_orig_source_inst() << " "
	  << ceph_mds_state_name(m->get_state()) << "\n";

  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    mon->send_reply(m, new MMDSMap(cct, mon->monmap->fsid, &mdsmap));
  }

  m->put();
}

void MDSMonitor::on_active(unique_lock& l)
{
  tick(l);

  if (mon->is_leader())
    mon->clog.info() << "mdsmap " << mdsmap << "\n";
}

void MDSMonitor::get_health(list<pair<health_status_t, string> >& summary,
			    list<pair<health_status_t, string> > *detail) const
{
  mdsmap.get_health(summary, detail);
}

void MDSMonitor::dump_info(Formatter *f)
{
  f->open_object_section("mdsmap");
  mdsmap.dump(f);
  f->close_section();

  f->dump_unsigned("mdsmap_first_committed", get_first_committed());
  f->dump_unsigned("mdsmap_last_committed", get_last_committed());
}

bool MDSMonitor::preprocess_command(MMonCommand *m, unique_lock& l)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_last_committed(), l);
    return true;
  }

  string prefix;
  cmd_getval(mon->cct, cmdmap, "prefix", prefix);
  string format;
  cmd_getval(mon->cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", rdata,
		       get_last_committed(), l);
    return true;
  }

  if (prefix == "mds stat") {
    if (f) {
      f->open_object_section("mds_stat");
      dump_info(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      ds << mdsmap;
    }
    r = 0;
  } else if (prefix == "mds dump") {
    int64_t epocharg;
    epoch_t epoch;

    MDSMap *p = &mdsmap;
    if (cmd_getval(mon->cct, cmdmap, "epoch", epocharg)) {
      epoch = epocharg;
      bufferlist b;
      int err = get_version(epoch, b);
      if (err == -ENOENT) {
	p = 0;
	r = -ENOENT;
      } else {
	assert(err == 0);
	assert(b.length());
	p = new MDSMap(cct);
	p->decode(b);
      }
    }
    if (p) {
      stringstream ds;
      if (f != NULL) {
	f->open_object_section("mdsmap");
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
	r = 0;
      } else {
	p->print(ds);
	r = 0;
      }
      if (r == 0) {
	rdata.append(ds);
	ss << "dumped mdsmap epoch " << p->get_epoch();
      }
      if (p != &mdsmap)
	delete p;
    }
  } else if (prefix == "mds getmap") {
    epoch_t e;
    int64_t epocharg;
    bufferlist b;
    if (cmd_getval(mon->cct, cmdmap, "epoch", epocharg)) {
      e = epocharg;
      int err = get_version(e, b);
      if (err == -ENOENT) {
	r = -ENOENT;
      } else {
	assert(r == 0);
	assert(b.length());
	MDSMap mm(cct);
	mm.decode(b);
	mm.encode(rdata, m->get_connection()->get_features());
	ss << "got mdsmap epoch " << mm.get_epoch();
      }
    } else {
      mdsmap.encode(rdata, m->get_connection()->get_features());
      ss << "got mdsmap epoch " << mdsmap.get_epoch();
    }
    r = 0;
  } else if (prefix == "mds tell") {
    string whostr;
    cmd_getval(mon->cct, cmdmap, "who", whostr);
    vector<string>args_vec;
    cmd_getval(mon->cct, cmdmap, "args", args_vec);

    if (whostr == "*") {
      r = -ENOENT;
      const map<uint64_t, MDSMap::mds_info_t> mds_info = mdsmap.get_mds_info();
      for (map<uint64_t, MDSMap::mds_info_t>::const_iterator i = mds_info.begin();
	   i != mds_info.end();
	   ++i) {
	m->cmd = args_vec;
	mon->send_command(i->second.get_inst(), m->cmd);
	r = 0;
      }
      if (r == -ENOENT) {
	ss << "no mds active";
      } else {
	ss << "ok";
      }
    } else {
      errno = 0;
      long who = strtol(whostr.c_str(), 0, 10);
      if (!errno && who >= 0) {
	if (mdsmap.is_up(who)) {
	  m->cmd = args_vec;
	  mon->send_command(mdsmap.get_inst(who), m->cmd);
	  r = 0;
	  ss << "ok";
	} else {
	  ss << "mds." << who << " not up";
	  r = -ENOENT;
	}
      } else ss << "specify mds number or *";
    }
  }

  if (r != -1) {
    rdata.append(ds);
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, get_last_committed(), l);
    return true;
  } else
    return false;
}

void MDSMonitor::fail_mds_gid(uint64_t gid)
{
  assert(pending_mdsmap.mds_info.count(gid));
  MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];
  ldout(mon->cct, 10) << "fail_mds_gid " << gid << " mds." << info.name
		      << " rank " << info.rank << dendl;

  ceph::real_time until = ceph::real_clock::now()
    + mon->cct->_conf->mds_blacklist_interval;

  pending_mdsmap.last_failure_osd_epoch
    = mon->osdmon()->blacklist(info.addr, until);

  pending_mdsmap.mds_info.erase(gid);
}

int MDSMonitor::fail_mds(std::ostream &ss, const std::string &arg,
			 unique_lock& l)
{
  std::string err;
  int w = strict_strtoll(arg.c_str(), 10, &err);
  if (!err.empty()) {
    // Try to interpret the arg as an MDS name
    const MDSMap::mds_info_t *mds_info = mdsmap.find_by_name(arg);
    if (!mds_info) {
      ss << "Can't find any MDS named '" << arg << "'";
      return -ENOENT;
    }
    w = mds_info->rank;
  }

  if (!mon->osdmon()->is_writeable()) {
    return -EAGAIN;
 }

  bool failed_mds_gid = false;
  if (pending_mdsmap.up.count(w)) {
    uint64_t gid = pending_mdsmap.up[w];
    if (pending_mdsmap.mds_info.count(gid)) {
      fail_mds_gid(gid);
      failed_mds_gid = true;
    }
    ss << "failed mds." << w;
  } else if (pending_mdsmap.mds_info.count(w)) {
    fail_mds_gid(w);
    failed_mds_gid = true;
    ss << "failed mds gid " << w;
  }

  if (failed_mds_gid) {
    assert(mon->osdmon()->is_writeable());
    request_proposal(mon->osdmon(), l);
  }
  return 0;
}

bool MDSMonitor::prepare_command(MMonCommand *m, unique_lock& l)
{
  int r = -EINVAL;
  stringstream ss;
  bufferlist rdata;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_last_committed(), l);
    return true;
  }

  string prefix;

  cmd_getval(mon->cct, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", rdata,
		       get_last_committed(), l);
    return true;
  }

  string whostr;
  cmd_getval(mon->cct, cmdmap, "who", whostr);
  if (prefix == "mds stop" ||
      prefix == "mds deactivate") {
    int who = parse_pos_long(whostr.c_str(), &ss);
    if (who < 0)
      goto out;
    if (!pending_mdsmap.is_active(who)) {
      r = -EEXIST;
      ss << "mds." << who << " not active ("
	 << ceph_mds_state_name(pending_mdsmap.get_state(who)) << ")";
    } else {
      r = 0;
      uint64_t gid = pending_mdsmap.up[who];
      ss << "telling mds." << who << " " << pending_mdsmap.mds_info[gid].addr << " to deactivate";
      pending_mdsmap.mds_info[gid].state = MDSMap::STATE_STOPPING;
    }

  } else if (prefix == "mds set") {
    string var;
    if (!cmd_getval(mon->cct, cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      goto out;
    }
    string val;
    string interr;
    int64_t n = 0;
    if (!cmd_getval(mon->cct, cmdmap, "val", val))
      goto out;
    // we got a string.	 see if it contains an int.
    n = strict_strtoll(val.c_str(), 10, &interr);
    if (0 /* var == "max_mds" */) {
    } else {
      ss << "unknown variable " << var;
      goto out;
    }
    r = 0;
  } else if (prefix == "mds setmap") {
    MDSMap map(cct);
    map.decode(m->get_data());
    epoch_t e = 0;
    int64_t epochnum;
    if (cmd_getval(mon->cct, cmdmap, "epoch", epochnum))
      e = epochnum;

    if (pending_mdsmap.epoch == e) {
      map.epoch = pending_mdsmap.epoch;	 // make sure epoch is correct
      pending_mdsmap = map;
      string rs = "set mds map";
      wait_for_finished_proposal(
	Monitor::CB_Command(mon, m, 0, rs, get_last_committed() + 1));
      return true;
    } else {
      ss << "next mdsmap epoch " << pending_mdsmap.epoch << " != " << e;
    }

  } else if (prefix == "mds set_state") {
    int64_t gid;
    if (!cmd_getval(mon->cct, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
	 << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      r = -EINVAL;
      goto out;
    }
    int64_t state;
    if (!cmd_getval(mon->cct, cmdmap, "state", state)) {
      ss << "error parsing 'state' string value '"
	 << cmd_vartype_stringify(cmdmap["state"]) << "'";
      r = -EINVAL;
      goto out;
    }
    if (!pending_mdsmap.is_dne_gid(gid)) {
      MDSMap::mds_info_t& info = pending_mdsmap.get_info_gid(gid);
      info.state = state;
      stringstream ss;
      ss << "set mds gid " << gid << " to state " << state << " "
	 << ceph_mds_state_name(state);
      string rs;
      getline(ss, rs);
      wait_for_finished_proposal(Monitor::CB_Command(
				   mon, m, 0, rs, get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "mds fail") {
    string who;
    cmd_getval(mon->cct, cmdmap, "who", who);
    r = fail_mds(ss, who, l);
    if (r < 0 && r == -EAGAIN) {
      mon->osdmon()->wait_for_writeable(CB_RetryMessage(this, m));
      return false; // don't propose yet; wait for message to be retried
    }

  } else if (prefix == "mds rm") {
    int64_t gid;
    if (!cmd_getval(mon->cct, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
	 << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      r = -EINVAL;
      goto out;
    }
    int state = pending_mdsmap.get_state_gid(gid);
    if (state == 0) {
      ss << "mds gid " << gid << " dne";
      r = 0;
    } else if (state > 0) {
      ss << "cannot remove active mds."
	 << pending_mdsmap.get_info_gid(gid).name
	 << " rank " << pending_mdsmap.get_info_gid(gid).rank;
      r = -EBUSY;
    } else {
      pending_mdsmap.mds_info.erase(gid);
      stringstream ss;
      ss << "removed mds gid " << gid;
      string rs;
      getline(ss, rs);
      wait_for_finished_proposal(Monitor::CB_Command(
				   mon, m, 0, rs, get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "mds rmfailed") {
    int64_t w;
    if (!cmd_getval(mon->cct, cmdmap, "who", w)) {
      ss << "error parsing 'who' value '"
	 << cmd_vartype_stringify(cmdmap["who"]) << "'";
      r = -EINVAL;
      goto out;
    }
    pending_mdsmap.failed.erase(w);
    stringstream ss;
    ss << "removed failed mds." << w;
    string rs;
    getline(ss, rs);
    wait_for_finished_proposal(Monitor::CB_Command(mon, m, 0, rs,
						   get_last_committed() + 1));
    return true;
  } else {
    ss << "unrecognized command";
  }
 out:
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(Monitor::CB_Command(mon, m, r, rs,
						   get_last_committed() + 1));
    return true;
  } else {
    // reply immediately
    mon->reply_command(m, r, rs, rdata, get_last_committed(), l);
    return false;
  }
}


void MDSMonitor::check_subs()
{
  string type = "mdsmap";
  if (mon->session_map.subs.count(type) == 0)
    return;
  xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void MDSMonitor::check_sub(Subscription *sub)
{
  if (sub->next <= mdsmap.get_epoch()) {
    mon->messenger->send_message(new MMDSMap(cct, mon->monmap->fsid, &mdsmap),
				 sub->session->con);
    if (sub->onetime)
      mon->session_map.remove_sub(sub);
    else
      sub->next = mdsmap.get_epoch() + 1;
  }
}

void MDSMonitor::tick(unique_lock& l)
{
  // make sure mds's are still alive
  // ...if i am an active leader
  if (!is_active()) return;

  ldout(mon->cct, 10) << mdsmap << dendl;

  bool do_propose = false;

  if (!mon->is_leader()) return;

  // check beacon timestamps
  ceph::mono_time cutoff = ceph::mono_clock::now() -
    mon->cct->_conf->mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (auto& p : pending_mdsmap.mds_info) {
    if (last_beacon.count(p.first) == 0) {
      const MDSMap::mds_info_t& info = p.second;
      ldout(mon->cct, 10) << " adding " << p.second.addr << " mds."
			  << info.rank
			  << " " << ceph_mds_state_name(info.state)
			  << " to last_beacon" << dendl;
      last_beacon[p.first].stamp = ceph::mono_clock::now();
      last_beacon[p.first].seq = 0;
    }
  }

  if (mon->osdmon()->is_writeable()) {

    bool propose_osdmap = false;

    map<uint64_t, beacon_info_t>::iterator p = last_beacon.begin();
    while (p != last_beacon.end()) {
      uint64_t gid = p->first;
      ceph::mono_time since = p->second.stamp;
//      uint64_t seq = p->second.seq;
      ++p;

      if (pending_mdsmap.mds_info.count(gid) == 0) {
	// clean it out
	last_beacon.erase(gid);
	continue;
      }

      if (since >= cutoff)
	continue;

      MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];

      ldout(mon->cct, 10) << "no beacon from " << gid << " " << info.addr << " mds." << info.rank
	       << " " << ceph_mds_state_name(info.state)
	       << " since " << since << dendl;

      // are we in?
      if (!info.laggy()) {
	ldout(mon->cct, 10) << " marking " << gid << " " << info.addr
			    << " mds." << info.rank
			    << " " << ceph_mds_state_name(info.state)
			    << " laggy" << dendl;
	info.laggy_since = ceph::real_clock::now();
	do_propose = true;
      }
      last_beacon.erase(gid);
    }

    if (propose_osdmap)
      request_proposal(mon->osdmon(), l);

  }


  if (do_propose)
    propose_pending(l);
}
