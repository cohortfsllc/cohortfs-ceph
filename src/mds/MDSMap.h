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


#ifndef CEPH_MDSMAP_H
#define CEPH_MDSMAP_H

#include <errno.h>

#include "common/ceph_context.h"
#include "common/code_environment.h"
#include "include/types.h"
#include "osd/OSDMap.h"
#include "vol/Volume.h"
#include "msg/Message.h"
#include "osdc/Objecter.h"

#include <set>
#include <map>
#include <string>
#include "common/config.h"

// #include "include/ceph_features.h"
#include "common/Formatter.h"

/*

 boot  --> active.

*/

class MDSMap {
public:
  // mds states
  // down, once existed, but no subtrees. empty log.
  static const int STATE_STOPPED = CEPH_MDS_STATE_STOPPED;
  // up, boot announcement.  destiny unknown.
  static const int STATE_BOOT = CEPH_MDS_STATE_BOOT;
  // up, idle.	waiting for assignment by monitor.
  static const int STATE_STARTING = CEPH_MDS_STATE_STARTING;
  // up, starting prior failed instance. scanning journal.
  static const int STATE_ACTIVE = CEPH_MDS_STATE_ACTIVE;
  // up, exporting metadata (-> standby or out)
  static const int STATE_STOPPING = CEPH_MDS_STATE_STOPPING;

  struct mds_info_t {
    uint64_t global_id;
    string name;
    int32_t rank;
    int32_t state;
    version_t state_seq;
    entity_addr_t addr;
    ceph::real_time laggy_since;
    set<int32_t> export_targets;

    mds_info_t() : global_id(0), state(STATE_BOOT),
		   state_seq(0) { }

    bool laggy() const { return !(laggy_since == ceph::real_time::min()); }
    void clear_laggy() { laggy_since = ceph::real_time::min(); }

    entity_inst_t get_inst() const {
      return entity_inst_t(entity_name_t::MDS(rank), addr);
    }

    void encode(bufferlist& bl, uint64_t features) const {
      if ((features & CEPH_FEATURE_MDSENC) == 0 ) encode_unversioned(bl);
      else encode_versioned(bl, features);
    }
    void decode(bufferlist::iterator& p);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<mds_info_t*>& ls);
  private:
    void encode_versioned(bufferlist& bl, uint64_t features) const;
    void encode_unversioned(bufferlist& bl) const;
  };


protected:
  // base map
  epoch_t epoch;
  uint32_t flags; // flags
  epoch_t last_failure; // mds epoch of last failure
  // osd epoch of last failure; any mds entering replay needs at least
  // this osdmap to ensure the blacklist propagates.
  epoch_t last_failure_osd_epoch;
  ceph::real_time created, modified;

  set<int32_t> failed, stopped; // which roles are failed or stopped
  map<int32_t,uint64_t> up; // who is in those roles
  map<uint64_t,mds_info_t> mds_info;

  bool inline_data_enabled;

public:
  friend class MDSMonitor;

  CephContext *cct;

public:
  MDSMap(CephContext* _cct)
    : epoch(0), flags(0), last_failure(0), last_failure_osd_epoch(0),
      cct(_cct) {
  }

  int get_flags() const { return flags; }
  int test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const ceph::real_time& get_created() const { return created; }
  void set_created(ceph::real_time ct) { modified = created = ct; }
  const ceph::real_time& get_modified() const { return modified; }
  void set_modified(ceph::real_time& mt) { modified = mt; }

  epoch_t get_last_failure() const { return last_failure; }
  epoch_t get_last_failure_osd_epoch() const { return last_failure_osd_epoch; }

  const map<uint64_t,mds_info_t>& get_mds_info() { return mds_info; }
  const mds_info_t& get_mds_info_gid(uint64_t gid) {
    assert(mds_info.count(gid));
    return mds_info[gid];
  }
  const mds_info_t& get_mds_info(int m) {
    assert(up.count(m) && mds_info.count(up[m]));
    return mds_info[up[m]];
  }
  uint64_t find_mds_gid_by_name(const string& s) {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == s) {
	return p->first;
      }
    }
    return 0;
  }

  // counts
  unsigned get_num_up_mds() {
    return up.size();
  }
  int get_num_failed_mds() {
    return failed.size();
  }
  unsigned get_num_mds(int state) const {
    unsigned n = 0;
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state == state) ++n;
    return n;
  }

  // sets
  void get_up_mds_set(set<int>& s) {
    for (map<int32_t,uint64_t>::const_iterator p = up.begin();
	 p != up.end();
	 ++p)
      s.insert(p->first);
  }
  void get_active_mds_set(set<int>& s) {
    get_mds_set(s, MDSMap::STATE_ACTIVE);
  }
  void get_failed_mds_set(set<int>& s) {
    s = failed;
  }
  int get_failed() {
    if (!failed.empty()) return *failed.begin();
    return -1;
  }
  void get_stopped_mds_set(set<int>& s) {
    s = stopped;
  }
  void get_mds_set(set<int>& s, int state) {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p)
      if (p->second.state == state)
	s.insert(p->second.rank);
  }

  int get_random_up_mds() {
    if (up.empty())
      return -1;
    map<int32_t,uint64_t>::iterator p = up.begin();
    for (int n = rand() % up.size(); n; n--)
      ++p;
    return p->first;
  }

  const mds_info_t* find_by_name(const string& name) const {
    for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin();
	 p != mds_info.end();
	 ++p) {
      if (p->second.name == name)
	return &p->second;
    }
    return NULL;
  }

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;

  // mds states
  bool is_down(int m) const { return up.count(m) == 0; }
  bool is_up(int m) const { return up.count(m); }

  bool is_failed(int m) const	{ return failed.count(m); }
  bool is_stopped(int m) const	  { return stopped.count(m); }

  bool is_dne(int m) const	{ return up.count(m) == 0; }
  bool is_dne_gid(uint64_t gid) const	  { return mds_info.count(gid) == 0; }

  int get_state(int m) const {
    map<int32_t,uint64_t>::const_iterator u = up.find(m);
    if (u == up.end())
      return 0;
    return get_state_gid(u->second);
  }
  int get_state_gid(uint64_t gid) const {
    map<uint64_t,mds_info_t>::const_iterator i = mds_info.find(gid);
    if (i == mds_info.end())
      return 0;
    return i->second.state;
  }

  mds_info_t& get_info(int m) { assert(up.count(m)); return mds_info[up[m]]; }
  mds_info_t& get_info_gid(uint64_t gid) { assert(mds_info.count(gid)); return mds_info[gid]; }

  bool is_starting(int m) const { return get_state(m) == STATE_STARTING; }
  bool is_active(int m) const  { return get_state(m) == STATE_ACTIVE; }
  bool is_stopping(int m) const { return get_state(m) == STATE_STOPPING; }
  bool is_active_or_stopping(int m) const {
    return is_active(m) || is_stopping(m);
  }

  bool is_followable(int m) const {
    return (is_active(m) ||
	    is_stopping(m));
  }

  bool is_laggy_gid(uint64_t gid) const {
    if (!mds_info.count(gid))
      return false;
    map<uint64_t,mds_info_t>::const_iterator p = mds_info.find(gid);
    return p->second.laggy();
  }


  // cluster states
  bool is_any_failed() {
    return failed.size();
  }
  bool is_stopped() {
    return up.empty();
  }

  // inst
  bool have_inst(int m) {
    return up.count(m);
  }
  const entity_inst_t get_inst(int m) {
    assert(up.count(m));
    return mds_info[up[m]].get_inst();
  }
  const entity_addr_t get_addr(int m) {
    assert(up.count(m));
    return mds_info[up[m]].addr;
  }
  bool get_inst(int m, entity_inst_t& inst) {
    if (up.count(m)) {
      inst = get_inst(m);
      return true;
    }
    return false;
  }

  int get_rank_gid(uint64_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].rank;
    return -1;
  }
  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }


  void print(ostream& out);
  void print_summary(Formatter *f, ostream *out);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<MDSMap*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(MDSMap::mds_info_t)
WRITE_CLASS_ENCODER_FEATURES(MDSMap)

inline ostream& operator<<(ostream& out, MDSMap& m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
