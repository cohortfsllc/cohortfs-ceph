// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com> All rights
 * reserved.
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

#include <string>
#include <map>
#include <vector>

#include "common/config.h"

// #include "include/CompatSet.h"
#include "common/Formatter.h"


using namespace std;


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

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void decode(bufferlist& bl);
    void dump(Formatter *f) const;
  }; // vol_info_t


  class Incremental {

  public:

    struct inc_add {
      uint16_t sequence;
      vol_info_t vol_info;

      void encode(bufferlist& bl, uint64_t features = -1) const;
      void decode(bufferlist::iterator& bl);
      void decode(bufferlist& bl);
    };
    typedef inc_add inc_update;

    struct inc_remove {
      uint16_t sequence;
      uuid_d uuid;

      void encode(bufferlist& bl, uint64_t features = -1) const;
      void decode(bufferlist::iterator& bl);
      void decode(bufferlist& bl);
    };

    version_t version;
    uint16_t next_sequence;
    vector<inc_add> additions;
    vector<inc_remove> removals;
    vector<inc_update> updates;

  public:

    void include_addition(const vol_info_t &vol_info) {
      inc_add increment;
      increment.sequence = next_sequence++;
      increment.vol_info = vol_info;
      additions.push_back(increment);
    }

    void include_addition(const uuid_d& uuid,
			  const string& name,
			  const uint16_t crush_map_entry) {
      include_addition(vol_info_t(uuid, name, crush_map_entry));
    }

    void include_removal(const uuid_d &uuid) {
      inc_remove increment;
      increment.sequence = next_sequence++;
      increment.uuid = uuid;
      removals.push_back(increment);
    }

    void include_update(const vol_info_t &vol_info) {
      inc_update increment;
      increment.sequence = next_sequence++;
      increment.vol_info = vol_info;
      updates.push_back(increment);
    }

    void include_update(const uuid_d& uuid,
			const string& name,
			const uint16_t crush_map_entry) {
      include_update(vol_info_t(uuid, name, crush_map_entry));
    }

    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::iterator& bl);
    void decode(bufferlist& bl);
  };


protected:

  // base map
  epoch_t epoch;
  version_t version;
  map<uuid_d,vol_info_t> vol_info_by_uuid;
  map<string,vol_info_t> vol_info_by_name;

public:

  const static string EMPTY_STRING;
  const static size_t DEFAULT_MAX_SEARCH_RESULTS = 128;

  friend class VolMonitor;

public:
  VolMap()
    : epoch(0) { }

  static bool is_valid_volume_name(const string& name, string& error);
  static bool is_valid_crush_map_entry(uint16_t crush_map_entry,
				       string& error);

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  int create_volume(const string& name, uint16_t crush_map_entry, uuid_d &out);
  int add_volume(uuid_d uuid, string name, uint16_t crush_map_entry);
  int remove_volume(uuid_d uuid, const string& name_verifier = EMPTY_STRING);
  int rename_volume(uuid_d uuid, const string& name,
		    vol_info_t& out_vinfo);
  int recrush_volume(uuid_d uuid, uint16_t crush_map_entry,
		     vol_info_t& out_vinfo);
  int update_volume(uuid_d uuid, string name, uint16_t crush_map_entry,
		    vol_info_t& out_vinfo);

  void apply_incremental(CephContext *cct, const VolMap::Incremental& inc);

  const vol_info_t& get_vol_info_uuid(const uuid_d& uuid) {
    assert(vol_info_by_uuid.count(uuid));
    return vol_info_by_uuid[uuid];
  }

  const vol_info_t& get_vol_info_name(const string& name) {
    assert(vol_info_by_name.count(name));
    return vol_info_by_name[name];
  }

  bool get_vol_info_uuid(const uuid_d& uuid, vol_info_t& vol_info) const {
    map<uuid_d,vol_info_t>::const_iterator i = vol_info_by_uuid.find(uuid);
    if (i == vol_info_by_uuid.end()) {
      return false;
    } else {
      vol_info = i->second;
      return true;
    }
  }

  bool get_vol_info_name(const string& name, vol_info_t& vol_info) const {
    map<string,vol_info_t>::const_iterator i = vol_info_by_name.find(name);
    if (i == vol_info_by_name.end()) {
      return false;
    } else {
      vol_info = i->second;
      return true;
    }
  }

  /*
   * Will search the entries by both name and uuid returning a vector of up to max entries.
   */
  vector<vol_info_t> search_vol_info(const string& name, size_t max = DEFAULT_MAX_SEARCH_RESULTS) const;

  /*
   * Will search for a unique volume specified by volspec (either by
   * uuid or name) and will set the uuid and return true. If the
   * specification is not unique, false is returned.
   */
  bool get_vol_uuid(const string& volspec, uuid_d& uuid_out) const;

  map<string,vol_info_t>::const_iterator begin() const {
    return vol_info_by_name.begin();
  }

  map<string,vol_info_t>::const_iterator end() const {
    return vol_info_by_name.end();
  }

  bool empty() const {
    return vol_info_by_uuid.empty();
  }

  size_t size() const {
    return vol_info_by_uuid.size();
  }

  void encode(bufferlist& bl, uint64_t features = -1) const;
  void decode(bufferlist::iterator& p);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  void print(ostream& out) const;
  void print_summary(ostream& out) const;

  void dump(Formatter *f) const;
  void dump(ostream& ss) const;
}; // class VolMap


WRITE_CLASS_ENCODER(VolMap::vol_info_t);
WRITE_CLASS_ENCODER_FEATURES(VolMap::Incremental);
WRITE_CLASS_ENCODER(VolMap::Incremental::inc_add);
WRITE_CLASS_ENCODER(VolMap::Incremental::inc_remove);
WRITE_CLASS_ENCODER_FEATURES(VolMap);


inline ostream& operator<<(ostream& out, const VolMap& m) {
  m.print_summary(out);
  return out;
}

inline ostream& operator<<(ostream& out, const VolMap::vol_info_t& vol_info) {
#if 0
  out << "vol u:" << vol_info.uuid << " cm: " << vol_info.crush_map_entry
      << " n:" << vol_info.name;
#else
  out << vol_info.uuid
      << " " << setw(4) << right << vol_info.crush_map_entry 
      << " " << vol_info.name;
#endif
  return out;
}


#endif // CEPH_VOLMAP_H
