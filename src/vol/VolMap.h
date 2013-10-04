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


#ifndef VOL_VOLMAP_H
#define VOL_VOLMAP_H

#include <errno.h>
#include "msg/Message.h"
#include <string>
#include <map>
#include <vector>

#include "Volume.h"
#include "common/config.h"
#include "common/Formatter.h"

using namespace std;

class VolMonitor;
class CephContext;
class VolMap;

class VolMap {
public:
  class Incremental {
  public:
    struct inc_add {
      uint16_t sequence;
      VolumeRef vol;

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

    void include_addition(VolumeRef vol) {
      inc_add increment;
      increment.sequence = next_sequence++;
      increment.vol = vol;
      additions.push_back(increment);
    }

    void include_removal(const uuid_d &uuid) {
      inc_remove increment;
      increment.sequence = next_sequence++;
      increment.uuid = uuid;
      removals.push_back(increment);
    }

    void include_update(VolumeRef vol) {
      inc_update increment;
      increment.sequence = next_sequence++;
      increment.vol = vol;
      updates.push_back(increment);
    }

    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::iterator& bl);
    void decode(bufferlist& bl);
  };

protected:

  // base map
  epoch_t epoch;
  version_t version;
  map<uuid_d,VolumeRef> vol_by_uuid;
  map<string,VolumeRef> vol_by_name;

public:

  const static string EMPTY_STRING;
  const static size_t DEFAULT_MAX_SEARCH_RESULTS = 128;

  friend VolMonitor;

public:
  VolMap()
    : epoch(0) { }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  int create_volume(VolumeRef volume, uuid_d& out);
  int add_volume(VolumeRef volume);
  int remove_volume(uuid_d uuid, const string& name_verifier = EMPTY_STRING);
  int rename_volume(VolumeRef v, const string& name);
  int rename_volume(uuid_d uuid, const string& name);
  int update_volume(uuid_d uuid, VolumeRef volume);

  void apply_incremental(CephContext *cct, const VolMap::Incremental& inc);

  map<uuid_d,VolumeRef>::const_iterator find(const uuid_d& uuid) const {
    return vol_by_uuid.find(uuid);
  }

  VolumeRef get(const uuid_d& uuid) const {
    map<uuid_d,VolumeRef>::const_iterator i = vol_by_uuid.find(uuid);
    if (i == vol_by_uuid.end())
      return VolumeRef();
    else
      return i->second;
  }

  map<string,VolumeRef>::const_iterator find(const string& name) const {
    return vol_by_name.find(name);
  }

  map<uuid_d,VolumeRef>::const_iterator begin_u() const {
    return vol_by_uuid.begin();
  }

  map<string,VolumeRef>::const_iterator begin_n() const {
    return vol_by_name.begin();
  }

  map<uuid_d,VolumeRef>::const_iterator end_u() const {
    return vol_by_uuid.end();
  }

  map<string,VolumeRef>::const_iterator end_n() const {
    return vol_by_name.end();
  }

  /*
   * Will search the entries by both name and uuid returning a vector
   * of up to max entries.
   */
  vector<VolumeCRef> search_vol(const string& name,
				size_t max = DEFAULT_MAX_SEARCH_RESULTS) const;

  /*
   * Will search for a unique volume specified by volspec (either by
   * uuid or name) and will set the uuid and return true. If the
   * specification is not unique, false is returned.
   */
  bool get_vol_uuid(const string& volspec, uuid_d& uuid_out) const;

  bool empty() const {
    return vol_by_uuid.empty();
  }

  size_t size() const {
    assert(vol_by_name.size() == vol_by_uuid.size());
    return vol_by_uuid.size();
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

typedef std::tr1::shared_ptr<VolMap> VolMapRef;

WRITE_CLASS_ENCODER(Volume);
WRITE_CLASS_ENCODER_FEATURES(VolMap::Incremental);
WRITE_CLASS_ENCODER(VolMap::Incremental::inc_add);
WRITE_CLASS_ENCODER(VolMap::Incremental::inc_remove);
WRITE_CLASS_ENCODER_FEATURES(VolMap);


inline ostream& operator<<(ostream& out, const VolMap& m) {
  m.print_summary(out);
  return out;
}

inline ostream& operator<<(ostream& out, const Volume& volume) {
  out << volume.uuid
      << " " << setw(4) << right << " " << volume.name;
  return out;
}

#endif // VOL_VOLMAP_H
