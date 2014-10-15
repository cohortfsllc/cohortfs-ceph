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


#ifndef CEPH_OSDMAP_H
#define CEPH_OSDMAP_H

/*
 * describe properties of the OSD cluster.
 *   disks, disk groups, total # osds,
 *
 */
#include "common/config.h"
#include "include/types.h"
#include "osd_types.h"
#include "msg/Message.h"
#include "common/Mutex.h"
#include "common/Clock.h"
#include "vol/Volume.h"

#include "include/ceph_features.h"

#include "include/interval_set.h"

#include <vector>
#include <list>
#include <set>
#include <map>
#include <unordered_set>

/*
 * we track up to two intervals during which the osd was alive and
 * healthy.  the most recent is [up_from,up_thru), where up_thru is
 * the last epoch the osd is known to have _started_.  i.e., a lower
 * bound on the actual osd death.  down_at (if it is > up_from) is an
 * upper bound on the actual osd death.
 */
struct osd_info_t {
  epoch_t up_from;   // epoch osd marked up
  epoch_t up_thru;   // lower bound on actual osd death (if > up_from)
  epoch_t down_at;   // upper bound on actual osd death (if > up_from)

  osd_info_t() : up_from(0), up_thru(0), down_at(0) {}

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<osd_info_t*>& o);
};
WRITE_CLASS_ENCODER(osd_info_t)

template <typename T>
typename StrmRet<T>::type& operator<<(T& out, const osd_info_t& info)
{
  out << "up_from " << info.up_from
      << " up_thru " << info.up_thru
      << " down_at " << info.down_at;
    return out;
}

struct osd_xinfo_t {
  utime_t down_stamp; ///< timestamp when we were last marked down
  float laggy_probability; ///< encoded as uint32_t: 0 = definitely
			   ///	not laggy, 0xffffffff definitely laggy
  uint32_t laggy_interval; ///< average interval between being marked
			   ///	laggy and recovering
  uint64_t features; ///< features supported by this osd we should know about

  osd_xinfo_t() : laggy_probability(0), laggy_interval(0),
		  features(0) {}

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<osd_xinfo_t*>& o);
};
WRITE_CLASS_ENCODER(osd_xinfo_t)

template <typename T>
typename StrmRet<T>::type& operator<<(T& out, const osd_xinfo_t& xi)
{
  return out << "down_stamp " << xi.down_stamp
	     << " laggy_probability " << xi.laggy_probability
	     << " laggy_interval " << xi.laggy_interval;
}


/** OSDMap
 */
class OSDMap {

public:
  class Incremental {
  public:
    /// feature bits we were encoded with.  the subsequent OSDMap
    /// encoding should match.
    uint64_t encode_features;
    uuid_d fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    utime_t modified;
    int32_t new_flags;

    // full (rare)
    bufferlist fullmap;	 // in leiu of below.

    // incremental
    int32_t new_max_osd;
    map<int32_t,entity_addr_t> new_up_client;
    map<int32_t,entity_addr_t> new_up_cluster;
    map<int32_t,uint8_t> new_state; // XORed onto previous state.
    map<int32_t,uint32_t> new_weight;
    map<int32_t,uint32_t> new_primary_affinity;
    map<int32_t,epoch_t> new_up_thru;
    map<int32_t,uuid_d> new_uuid;
    map<int32_t,osd_xinfo_t> new_xinfo;

    map<entity_addr_t,utime_t> new_blacklist;
    vector<entity_addr_t> old_blacklist;
    map<int32_t, entity_addr_t> new_hb_back_up;
    map<int32_t, entity_addr_t> new_hb_front_up;

    int get_net_marked_out(const OSDMap *previous) const;
    int get_net_marked_down(const OSDMap *previous) const;
    int identify_osd(uuid_d u) const;

    void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Incremental*>& o);

    Incremental(epoch_t e=0) :
      encode_features(0),
      epoch(e), new_flags(-1), new_max_osd(-1) {
      memset(&fsid, 0, sizeof(fsid));
    }
    Incremental(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    Incremental(bufferlist::iterator &p) {
      decode(p);
    }

    struct vol_inc_add {
      uint16_t sequence;
      VolumeRef vol;

      void encode(bufferlist& bl, uint64_t features = -1) const;
      void decode(bufferlist::iterator& bl);
      void decode(bufferlist& bl);
    };

    struct vol_inc_remove {
      uint16_t sequence;
      uuid_d uuid;

      void encode(bufferlist& bl, uint64_t features = -1) const;
      void decode(bufferlist::iterator& bl);
      void decode(bufferlist& bl);
    };

    version_t vol_version;
    uint16_t vol_next_sequence;
    vector<vol_inc_add> vol_additions;
    vector<vol_inc_remove> vol_removals;

    void include_addition(VolumeRef vol) {
      vol_inc_add increment;
      increment.sequence = vol_next_sequence++;
      increment.vol = vol;
      vol_additions.push_back(increment);
    }

    void include_removal(const uuid_d &uuid) {
      vol_inc_remove increment;
      increment.sequence = vol_next_sequence++;
      increment.uuid = uuid;
      vol_removals.push_back(increment);
    }
  };

private:
  uuid_d fsid;
  epoch_t epoch; // what epoch of the osd cluster descriptor is this
  utime_t created, modified; // epoch start time

  uint32_t flags;

  int num_osd;	       // not saved
  int32_t max_osd;
  vector<uint8_t> osd_state;

  struct {
    map<uuid_d,VolumeRef> by_uuid;
    map<string,VolumeRef> by_name;
  } vols;

  struct addrs_s {
    vector<std::shared_ptr<entity_addr_t> > client_addr;
    vector<std::shared_ptr<entity_addr_t> > cluster_addr;
    vector<std::shared_ptr<entity_addr_t> > hb_back_addr;
    vector<std::shared_ptr<entity_addr_t> > hb_front_addr;
    entity_addr_t blank;
  };
  std::shared_ptr<addrs_s> osd_addrs;

  vector<uint32_t>   osd_weight;   // 16.16 fixed point, 0x10000 = "in", 0 = "out"
  vector<osd_info_t> osd_info;

  std::shared_ptr< vector<uuid_d> > osd_uuid;
  vector<osd_xinfo_t> osd_xinfo;

  std::unordered_map<entity_addr_t,utime_t> blacklist;

  bool new_blacklist_entries;

 public:

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0),
	     flags(0),
	     num_osd(0), max_osd(0),
	     osd_addrs(new addrs_s),
	     osd_uuid(new vector<uuid_d>),
	     new_blacklist_entries(false) {
    memset(&fsid, 0, sizeof(fsid));
  }

  // no copying
  /* oh, how i long for c++11...
private:
  OSDMap(const OSDMap& other) = default;
  const OSDMap& operator=(const OSDMap& other) = default;
public:
  */

  void deepish_copy_from(const OSDMap& o) {
    *this = o;
    osd_uuid.reset(new vector<uuid_d>(*o.osd_uuid));

    // NOTE: this still references shared entity_addr_t's.
    osd_addrs.reset(new addrs_s(*o.osd_addrs));
  }

  // map info
  const uuid_d& get_fsid() const { return fsid; }
  void set_fsid(uuid_d& f) { fsid = f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  void set_epoch(epoch_t e);

  /* stamps etc */
  const utime_t& get_created() const { return created; }
  const utime_t& get_modified() const { return modified; }

  bool is_blacklisted(const entity_addr_t& a) const;
  void get_blacklist(list<pair<entity_addr_t,utime_t > > *bl) const;

  /***** cluster state *****/
  /* osds */
  int get_max_osd() const { return max_osd; }
  void set_max_osd(int m);

  unsigned get_num_osds() const {
    return num_osd;
  }
  int calc_num_osds();

  void get_all_osds(set<int32_t>& ls) const;
  void get_up_osds(set<int32_t>& ls) const;
  unsigned get_num_up_osds() const;
  unsigned get_num_in_osds() const;

  int get_flags() const { return flags; }
  int test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  static void calc_state_set(int state, set<string>& st);

  int get_state(int o) const {
    assert(o < max_osd);
    return osd_state[o];
  }
  int get_state(int o, set<string>& st) const {
    assert(o < max_osd);
    unsigned t = osd_state[o];
    calc_state_set(t, st);
    return osd_state[o];
  }
  void set_state(int o, unsigned s) {
    assert(o < max_osd);
    osd_state[o] = s;
  }
  void set_weightf(int o, float w) {
    set_weight(o, (int)((float)CEPH_OSD_IN * w));
  }
  void set_weight(int o, unsigned w) {
    assert(o < max_osd);
    osd_weight[o] = w;
    if (w)
      osd_state[o] |= CEPH_OSD_EXISTS;
  }
  unsigned get_weight(int o) const {
    assert(o < max_osd);
    return osd_weight[o];
  }
  float get_weightf(int o) const {
    return (float)get_weight(o) / (float)CEPH_OSD_IN;
  }
  void adjust_osd_weights(const map<int,double>& weights, Incremental& inc) const;

  bool exists(int osd) const {
    //assert(osd >= 0);
    return osd >= 0 && osd < max_osd && (osd_state[osd] & CEPH_OSD_EXISTS);
  }

  bool is_up(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_UP);
  }

  bool is_down(int osd) const {
    return !is_up(osd);
  }

  bool is_out(int osd) const {
    return !exists(osd) || get_weight(osd) == CEPH_OSD_OUT;
  }

  bool is_in(int osd) const {
    return !is_out(osd);
  }

int identify_osd(const entity_addr_t& addr) const;
  int identify_osd(const uuid_d& u) const;

  bool have_addr(const entity_addr_t& addr) const {
    return identify_osd(addr) >= 0;
  }
  bool find_osd_on_ip(const entity_addr_t& ip) const;
  bool have_inst(int osd) const {
    return exists(osd) && is_up(osd);
  }
  const entity_addr_t &get_addr(int osd) const {
    assert(exists(osd));
    return osd_addrs->client_addr[osd] ? *osd_addrs->client_addr[osd] : osd_addrs->blank;
  }
  const entity_addr_t &get_cluster_addr(int osd) const {
    assert(exists(osd));
    if (!osd_addrs->cluster_addr[osd] || *osd_addrs->cluster_addr[osd] == entity_addr_t())
      return get_addr(osd);
    return *osd_addrs->cluster_addr[osd];
  }
  const entity_addr_t &get_hb_back_addr(int osd) const {
    assert(exists(osd));
    return osd_addrs->hb_back_addr[osd] ? *osd_addrs->hb_back_addr[osd] : osd_addrs->blank;
  }
  const entity_addr_t &get_hb_front_addr(int osd) const {
    assert(exists(osd));
    return osd_addrs->hb_front_addr[osd] ? *osd_addrs->hb_front_addr[osd] : osd_addrs->blank;
  }
  entity_inst_t get_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_addr(osd));
  }
  entity_inst_t get_cluster_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_cluster_addr(osd));
  }
  entity_inst_t get_hb_back_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_hb_back_addr(osd));
  }
  entity_inst_t get_hb_front_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_hb_front_addr(osd));
  }

  const uuid_d& get_uuid(int osd) const {
    assert(exists(osd));
    return (*osd_uuid)[osd];
  }

  const epoch_t& get_up_from(int osd) const {
    assert(exists(osd));
    return osd_info[osd].up_from;
  }
  const epoch_t& get_up_thru(int osd) const {
    assert(exists(osd));
    return osd_info[osd].up_thru;
  }
  const epoch_t& get_down_at(int osd) const {
    assert(exists(osd));
    return osd_info[osd].down_at;
  }
  const osd_info_t& get_info(int osd) const {
    assert(osd < max_osd);
    return osd_info[osd];
  }

  const osd_xinfo_t& get_xinfo(int osd) const {
    assert(osd < max_osd);
    return osd_xinfo[osd];
  }

  int get_any_up_osd() const {
    for (int i=0; i<max_osd; i++)
      if (is_up(i))
	return i;
    return -1;
  }

  int get_next_up_osd_after(int n) const {
    for (int i = n + 1; i != n; ++i) {
      if (i >= get_max_osd())
	i = 0;
      if (i == n)
	break;
      if (is_up(i))
	return i;
    }
    return -1;
  }

  int get_previous_up_osd_before(int n) const {
    for (int i = n - 1; i != n; --i) {
      if (i < 0)
	i = get_max_osd() - 1;
      if (i == n)
	break;
      if (is_up(i))
	return i;
    }
    return -1;
  }

  /**
   * get feature bits required by the current structure
   *
   * @param mask [out] set of all possible map-related features we could set
   * @return feature bits used by this map
   */
  uint64_t get_features(uint64_t *mask) const;

  /**
   * get intersection of features supported by up osds
   */
  uint64_t get_up_osd_features() const;

  int apply_incremental(const Incremental &inc);

  /// try to re-use/reference addrs in oldmap from newmap
  static void dedup(const OSDMap *oldmap, OSDMap *newmap);

  // serialize, unserialize
private:
  void post_decode();
public:
  void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
  void decode(bufferlist& bl);
  void decode(bufferlist::iterator& bl);

public:

  /*
   * handy helpers to build simple maps...
   */
  /**
   * Build an OSD map suitable for basic usage. If **num_osd** is >= 0
   * it will be initialized with the specified number of OSDs in a
   * single host. If **num_osd** is < 0 the layout of the OSD map will
   * be built by reading the content of the configuration file.
   *
   * @param cct [in] in core ceph context
   * @param e [in] initial epoch
   * @param fsid [in] id of the cluster
   * @param num_osd [in] number of OSDs if >= 0 or read from conf if < 0
   * @return **0** on success, negative errno on error.
   */
  int build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
		   int num_osd);
private:
  void print_osd_line(int cur, ostream *out, Formatter *f) const;
public:
  void print(ostream& out) const;
  void print_summary(Formatter *f, ostream& out) const;
  template <typename T>
  void print_oneline_summary(T& out) const;

  string get_flag_string() const;
  static string get_flag_string(unsigned flags);
  void dump_json(ostream& out) const;
  void dump(Formatter *f) const;
  static void generate_test_instances(list<OSDMap*>& o);
  bool check_new_blacklist_entries() const { return new_blacklist_entries; }

  int create_volume(VolumeRef volume, uuid_d& out);
  int add_volume(VolumeRef volume);
  int remove_volume(uuid_d uuid);

  bool vol_exists(const uuid_d& uuid) {
    map<uuid_d,VolumeRef>::iterator v = vols.by_uuid.find(uuid);
    if (v == vols.by_uuid.end()) {
      return false;
    } else {
      return true;
    }
  }

  bool find_by_uuid(const uuid_d& uuid, VolumeRef& vol) {
    map<uuid_d,VolumeRef>::iterator v = vols.by_uuid.find(uuid);
    if (v == vols.by_uuid.end()) {
      return false;
    } else {
      vol = v->second;
      return true;
    }
  }
  bool find_by_name(const string& name, VolumeRef& vol) {
    map<string,VolumeRef>::iterator v = vols.by_name.find(name);
    if (v == vols.by_name.end()) {
      return false;
    } else {
      vol = v->second;
      return true;
    }
  }

  bool volmap_empty(void) {
    return vols.by_uuid.empty();
  }
};
WRITE_CLASS_ENCODER_FEATURES(OSDMap)
WRITE_CLASS_ENCODER_FEATURES(OSDMap::Incremental)
WRITE_CLASS_ENCODER(OSDMap::Incremental::vol_inc_add)
WRITE_CLASS_ENCODER(OSDMap::Incremental::vol_inc_remove)

typedef std::shared_ptr<const OSDMap> OSDMapRef;

template <typename T>
inline typename StrmRet<T>::type& operator<<(T& out, const OSDMap& m) {
  m.print_oneline_summary(out);
  return out;
}

#endif
