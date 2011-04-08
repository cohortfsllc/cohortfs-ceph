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
 * OSDCaps: Hold the capabilities associated with a single authenticated 
 * user key. These are specified by text strings of the form
 * "allow r" (which allows reading anything on the OSD)
 * "allow rwx auid foo[,bar,baz]" (which allows full access to listed auids)
 *  "allow rwx pool foo[,bar,baz]" (which allows full access to listed pools)
 * "allow *" (which allows full access to EVERYTHING)
 *
 * The OSD assumes that anyone with * caps is an admin and has full
 * message permissions. This means that only the monitor and the OSDs
 * should get *
 */

#ifndef CEPH_OSDCAPS_H
#define CEPH_OSDCAPS_H

#include "include/types.h"

#define OSD_POOL_CAP_R 0x01
#define OSD_POOL_CAP_W 0x02
#define OSD_POOL_CAP_X 0x04

#define OSD_POOL_CAP_ALL (OSD_POOL_CAP_R | OSD_POOL_CAP_W | OSD_POOL_CAP_X)

typedef __u8 rwx_t;

static inline ostream& operator<<(ostream& out, rwx_t p) {
  if (p & OSD_POOL_CAP_R)
    out << "r";
  if (p & OSD_POOL_CAP_W)
    out << "w";
  if (p & OSD_POOL_CAP_X)
    out << "x";
  return out;
}


struct OSDCap {
  rwx_t allow;
  rwx_t deny;
  OSDCap() : allow(0), deny(0) {}
};

static inline ostream& operator<<(ostream& out, const OSDCap& pc) {
  return out << "(allow " << pc.allow << ", deny " << pc.deny << ")";
}

struct CapMap {
  virtual ~CapMap();
  virtual OSDCap& get_cap(string& name) = 0;
};

struct PoolsMap : public CapMap {
  map<string, OSDCap> pools_map;

  OSDCap& get_cap(string& name) { return pools_map[name]; }

  void dump();
  void apply_caps(string& name, int& cap);
};

struct AuidMap : public CapMap {
  map<uint64_t, OSDCap> auid_map;

  OSDCap& get_cap(string& name) {
    uint64_t num = strtoll(name.c_str(), NULL, 10);
    return auid_map[num];
  }

  void apply_caps(uint64_t uid, int& cap);
};

struct OSDCaps {
  PoolsMap pools_map;
  AuidMap auid_map;
  rwx_t default_allow;
  rwx_t default_deny;
  bool allow_all;
  int peer_type;
  uint64_t auid;

  bool get_next_token(string s, size_t& pos, string& token);
  bool is_rwx(string& token, rwx_t& cap_val);
  
  OSDCaps() : default_allow(0), default_deny(0), allow_all(false),
	      auid(CEPH_AUTH_UID_DEFAULT) {}
  bool parse(bufferlist::iterator& iter);
  int get_pool_cap(string& pool_name, uint64_t uid = CEPH_AUTH_UID_DEFAULT);
  bool is_mon() { return CEPH_ENTITY_TYPE_MON == peer_type; }
  bool is_osd() { return CEPH_ENTITY_TYPE_OSD == peer_type; }
  bool is_mds() { return CEPH_ENTITY_TYPE_MDS == peer_type; }
  void set_allow_all(bool allow) { allow_all = allow; }
  void set_peer_type (int pt) { peer_type = pt; }
  void set_auid(uint64_t uid) { auid = uid; }
};

static inline ostream& operator<<(ostream& out, const OSDCaps& c) {
  return out << "osdcaps(pools=" << c.pools_map.pools_map << " default allow=" << c.default_allow << " default_deny=" << c.default_deny << ")";
}

#endif
