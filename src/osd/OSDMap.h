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
// #include "pg/pg_types.h"
// #include "pg/OSDMapPGBridge.h"
#include "msg/Message.h"
#include "common/Mutex.h"
#include "common/Clock.h"

#include "include/ceph_features.h"

#include "include/interval_set.h"

#include <vector>
#include <list>
#include <set>
#include <map>
#include <tr1/memory>

#include "vol/VolMap.h"


using namespace std;


class Objecter;


#include <ext/hash_set>
using __gnu_cxx::hash_set;

/*
 * we track up to two intervals during which the osd was alive and
 * healthy.  the most recent is [up_from,up_thru), where up_thru is
 * the last epoch the osd is known to have _started_.  i.e., a lower
 * bound on the actual osd death.  down_at (if it is > up_from) is an
 * upper bound on the actual osd death.
 *
 * the second is the last_clean interval [first,last].  in that case,
 * the last interval is the last epoch known to have been either
 * _finished_, or during which the osd cleanly shut down.  when
 * possible, we push this forward to the epoch the osd was eventually
 * marked down.
 *
 * the lost_at is used to allow build_prior to proceed without waiting
 * for an osd to recover.  In certain cases, progress may be blocked 
 * because an osd is down that may contain updates (i.e., a pg may have
 * gone rw during an interval).  If the osd can't be brought online, we
 * can force things to proceed knowing that we _might_ be losing some
 * acked writes.  If the osd comes back to life later, that's fine to,
 * but those writes will still be lost (the divergent objects will be
 * thrown out).
 */
struct osd_info_t {
  epoch_t last_clean_begin;  // last interval that ended with a clean osd shutdown
  epoch_t last_clean_end;
  epoch_t up_from;   // epoch osd marked up
  epoch_t up_thru;   // lower bound on actual osd death (if > up_from)
  epoch_t down_at;   // upper bound on actual osd death (if > up_from)
  epoch_t lost_at;   // last epoch we decided data was "lost"
  
  osd_info_t() : last_clean_begin(0), last_clean_end(0),
		 up_from(0), up_thru(0), down_at(0), lost_at(0) {}

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<osd_info_t*>& o);
};
WRITE_CLASS_ENCODER(osd_info_t)

ostream& operator<<(ostream& out, const osd_info_t& info);


struct osd_xinfo_t {
  utime_t down_stamp;      ///< timestamp when we were last marked down
  float laggy_probability; ///< encoded as __u32: 0 = definitely not laggy, 0xffffffff definitely laggy
  __u32 laggy_interval;    ///< average interval between being marked laggy and recovering

  osd_xinfo_t() : laggy_probability(0), laggy_interval(0) {}

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<osd_xinfo_t*>& o);
};
WRITE_CLASS_ENCODER(osd_xinfo_t)

ostream& operator<<(ostream& out, const osd_xinfo_t& xi);

class Monitor;

class OSDMap;

typedef std::tr1::shared_ptr<OSDMap> OSDMapRef;
typedef std::tr1::shared_ptr<const OSDMap> OSDMapConstRef;


/** OSDMap
 */
class OSDMap {

  VolMapRef volmap;

public:
  static void dedup(const OSDMap *o, OSDMap *n);
  class Incremental {
  public:
    uuid_d fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    utime_t modified;
    int32_t new_flags;

    // full (rare)
    bufferlist fullmap;  // in leiu of below.

    // incremental
    int32_t new_max_osd;
    map<int32_t,entity_addr_t> new_up_client;
    map<int32_t,entity_addr_t> new_up_cluster;
    map<int32_t,uint8_t> new_state;             // XORed onto previous state.
    map<int32_t,uint32_t> new_weight;
    map<int32_t,epoch_t> new_up_thru;
    map<int32_t,pair<epoch_t,epoch_t> > new_last_clean_interval;
    map<int32_t,epoch_t> new_lost;
    map<int32_t,uuid_d> new_uuid;
    map<int32_t,osd_xinfo_t> new_xinfo;

    map<entity_addr_t,utime_t> new_blacklist;
    vector<entity_addr_t> old_blacklist;
    map<int32_t, entity_addr_t> new_hb_back_up;
    map<int32_t, entity_addr_t> new_hb_front_up;

    string cluster_snapshot;

    int get_net_marked_out(const OSDMap *previous) const;
    int get_net_marked_down(const OSDMap *previous) const;
    int identify_osd(uuid_d u) const;

    void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
    void decode(bufferlist::iterator &p);
    void decode(bufferlist& bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    void dump(Formatter *f) const;

    Incremental(epoch_t e=0) :
      epoch(e),
      new_flags(-1),
      new_max_osd(-1)
    {
      memset(&fsid, 0, sizeof(fsid));
    }
    Incremental(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    Incremental(bufferlist::iterator &p) {
      decode(p);
    }
    virtual ~Incremental() {}

    virtual OSDMap* newOSDMap(VolMapRef v) const = 0;
  }; // class OSDMap::Incremental

  virtual void thrash(Monitor* mon, OSDMap::Incremental& pending_inc_orig) = 0;

protected:
  virtual void populate_simple(CephContext *cct) = 0;
  virtual void populate_simple_from_conf(CephContext *cct) = 0;
protected:
  uuid_d fsid;
  epoch_t epoch;        // what epoch of the osd cluster descriptor is this
  utime_t created, modified; // epoch start time

  uint32_t flags;

  int num_osd;         // not saved
  int32_t max_osd;
  vector<uint8_t> osd_state;

  struct addrs_s {
    vector<std::tr1::shared_ptr<entity_addr_t> > client_addr;
    vector<std::tr1::shared_ptr<entity_addr_t> > cluster_addr;
    vector<std::tr1::shared_ptr<entity_addr_t> > hb_back_addr;
    vector<std::tr1::shared_ptr<entity_addr_t> > hb_front_addr;
    entity_addr_t blank;
  };
  std::tr1::shared_ptr<addrs_s> osd_addrs;

  vector<__u32>   osd_weight;   // 16.16 fixed point, 0x10000 = "in", 0 = "out"
  vector<osd_info_t> osd_info;

  std::tr1::shared_ptr< vector<uuid_d> > osd_uuid;
  vector<osd_xinfo_t> osd_xinfo;

  hash_map<entity_addr_t,utime_t> blacklist;

  epoch_t cluster_snapshot_epoch;
  string cluster_snapshot;
  bool new_blacklist_entries;

 public:
  friend class OSDMonitor;
  friend class CohortOSDMonitor;
  friend class PGMonitor;
  friend class MDS;

 public:
  OSDMap(VolMapRef v) :
    volmap(v),
    epoch(0),
    flags(0),
    num_osd(0), max_osd(0),
    osd_addrs(new addrs_s),
    osd_uuid(new vector<uuid_d>),
    cluster_snapshot_epoch(0),
    new_blacklist_entries(false) {
    memset(&fsid, 0, sizeof(fsid));
  }
  virtual ~OSDMap() { }

  virtual Incremental* newIncremental() const = 0;
  static OSDMapRef build_simple(CephContext *cct, epoch_t e,
				uuid_d &fsid, int num_osd);
  static OSDMapRef build_simple_from_conf(CephContext *cct, epoch_t e,
					  uuid_d &fsid);
  virtual int get_oid_osd(const Objecter* objecter,
			  const object_t& oid,
			  const ceph_file_layout* layout) = 0;

  virtual int get_file_stripe_address(vector<ObjectExtent>& extents,
				      vector<entity_addr_t>& address) = 0;

  // map info
  const uuid_d& get_fsid() const { return fsid; }
  void set_fsid(uuid_d& f) { fsid = f; }

  virtual void set_epoch(epoch_t e);
  void inc_epoch() { epoch++; }
  epoch_t get_epoch() const { return epoch; }

  /* stamps etc */
  const utime_t& get_created() const { return created; }
  const utime_t& get_modified() const { return modified; }

  bool is_blacklisted(const entity_addr_t& a) const;
  void get_blacklist(list<pair<entity_addr_t,utime_t > > *bl) const;

  string get_cluster_snapshot() const {
    if (cluster_snapshot_epoch == epoch)
      return cluster_snapshot;
    return string();
  }

  /***** cluster state *****/
  /* osds */
  int get_max_osd() const { return max_osd; }
  void set_max_osd(int m);

  int get_num_osds() const {
    return num_osd;
  }
  int calc_num_osds();

  void get_all_osds(set<int32_t>& ls) const;
  int get_num_up_osds() const;
  int get_num_in_osds() const;
  void _remove_nonexistent_osds(vector<int>& osds) const;

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
  const vector<__u32>& get_weights() const {
    return osd_weight;
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
    return !exists(osd) || !is_up(osd);
  }

  bool is_out(int osd) const {
    return !exists(osd) || get_weight(osd) == CEPH_OSD_OUT;
  }

  bool is_in(int osd) const {
    return exists(osd) && !is_out(osd);
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
  virtual uint64_t get_features(uint64_t *mask) const = 0;
  int apply_incremental(const Incremental &inc);

  virtual int apply_incremental_subclass(const Incremental& inc) = 0;

  virtual void encode(bufferlist& bl,
		      uint64_t features=CEPH_FEATURES_ALL) const = 0;
  virtual void decode(bufferlist::iterator& p) = 0;
  void decode(bufferlist& bl);

protected:
  void encodeOSDMap(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
  void decodeOSDMap(bufferlist& bl, __u16 v);
  void decodeOSDMap(bufferlist::iterator& p, __u16 v);

public:

private:
  void print_osd_line(int cur, ostream *out, Formatter *f) const;
public:
  void print(ostream& out) const;
  void print_summary(ostream& out) const;

  string get_flag_string() const;

  static string get_flag_string(unsigned flags);
  virtual void dump_json(ostream& out) const;
  virtual void dump(Formatter *f) const = 0;
  static void generate_test_instances(list<OSDMap*>& o);
  bool check_new_blacklist_entries() const { return new_blacklist_entries; }
};
WRITE_CLASS_ENCODER_FEATURES(OSDMap)
WRITE_CLASS_ENCODER_FEATURES(OSDMap::Incremental)

inline ostream& operator<<(ostream& out, const OSDMap& m) {
  m.print_summary(out);
  return out;
}


#endif // CEPH_OSDMAP_H
