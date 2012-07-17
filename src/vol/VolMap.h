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


#ifndef CEPH_VOLMAP_H
#define CEPH_VOLMAP_H

#include <errno.h>

// #include "include/types.h"
// #include "common/Clock.h"
#include "msg/Message.h"

#include <set>
#include <map>
#include <string>
using namespace std;

#include "common/config.h"

// #include "include/CompatSet.h"
#include "common/Formatter.h"

class CephContext;


class VolMap {

public:

  struct vol_info_t {
    uuid_d uuid;
    string name;
    uint16_t crush_map_entry;

    vol_info_t()
      : uuid(), name(""), crush_map_entry(0)
    {
      // empty
    }

    vol_info_t(uuid_d puuid,
	       string pname,
	       uint16_t pcrush_map_entry)
      : uuid(puuid), name(pname), crush_map_entry(pcrush_map_entry)
    {
      // empty
    }
    
    void encode(bufferlist& bl) const {
      __u8 v = 1;
      ::encode(v, bl);
      ::encode(uuid, bl);
      ::encode(name, bl);
      ::encode(crush_map_entry, bl);
    }

    void decode(bufferlist::iterator& bl) {
      __u8 v;
      ::decode(v, bl);
      ::decode(uuid, bl);
      ::decode(name, bl);
      ::decode(crush_map_entry, bl);
    }

    void dump(Formatter *f) const;
  }; // vol_info_t


protected:
  // base map
  epoch_t epoch;
  map<uuid_d,vol_info_t> vol_info_by_uuid;
  map<string,vol_info_t> vol_info_by_name;

public:

  friend class VolMonitor;

public:
  VolMap() 
    : epoch(0) { }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  int create_volume(string name, uint16_t crush_map_entry);
  int add_volume(uuid_d uuid, string name, uint16_t crush_map_entry);
  int remove_volume(uuid_d uuid);
  int remove_volume(string name);

  const vol_info_t& get_vol_info_uuid(const uuid_d& uuid) {
    assert(vol_info_by_uuid.count(uuid));
    return vol_info_by_uuid[uuid];
  }

  const vol_info_t& get_vol_info_name(const string& name) {
    assert(vol_info_by_name.count(name));
    return vol_info_by_name[name];
  }

  void encode(bufferlist& bl) const {
    __u16 v = 1;
    ::encode(v, bl);
    ::encode(epoch, bl);
    ::encode(vol_info_by_uuid, bl);
  }

  void decode(bufferlist::iterator& p) {
    __u16 v;
    ::decode(v, p);
    ::decode(epoch, p);
    ::decode(vol_info_by_uuid, p);
  }

  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  void print(ostream& out);
  void print_summary(ostream& out);

  void dump(Formatter *f) const;
}; // class VolMap

WRITE_CLASS_ENCODER(VolMap::vol_info_t)
WRITE_CLASS_ENCODER(VolMap)

inline ostream& operator<<(ostream& out, VolMap& m) {
  m.print_summary(out);
  return out;
}

#endif // CEPH_VOLMAP_H
