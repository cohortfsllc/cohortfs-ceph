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

#ifndef CEPH_OSD_TYPES_H
#define CEPH_OSD_TYPES_H

#include <sstream>
#include <stdio.h>
#include <memory>

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "include/interval_set.h"
#include "common/snap_types.h"
#include "common/Formatter.h"
#include "os/hobject.h"


#define CEPH_OSD_ONDISK_MAGIC "ceph osd volume v026"

#define CEPH_OSD_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "initial feature set(~v.18)")
#define CEPH_OSD_FEATURE_INCOMPAT_PGINFO CompatSet::Feature(2, "pginfo object")
#define CEPH_OSD_FEATURE_INCOMPAT_OLOC CompatSet::Feature(3, "object locator")
#define CEPH_OSD_FEATURE_INCOMPAT_LEC  CompatSet::Feature(4, "last_epoch_clean")
#define CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES  CompatSet::Feature(5, "categories")
#define CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL  CompatSet::Feature(6, "hobjectpool")
#define CEPH_OSD_FEATURE_INCOMPAT_BIGINFO CompatSet::Feature(7, "biginfo")

#define OSD_SUPERBLOCK_POBJECT hobject_t(sobject_t(object_t("osd_superblock"), 0))


// placement seed (a hash value)
typedef uint32_t ps_t;

typedef hobject_t collection_list_handle_t;


/**
 * osd request identifier
 *
 * caller name + incarnation# + tid to unique identify this request.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  tid_t         tid;
  int32_t       inc;  // incarnation

  osd_reqid_t()
    : tid(0), inc(0) {}
  osd_reqid_t(const entity_name_t& a, int i, tid_t t)
    : name(a), tid(t), inc(i) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<osd_reqid_t*>& o);
};
WRITE_CLASS_ENCODER(osd_reqid_t)

inline ostream& operator<<(ostream& out, const osd_reqid_t& r) {
  return out << r.name << "." << r.inc << ":" << r.tid;
}

inline bool operator==(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name == r.name) && (l.inc == r.inc) && (l.tid == r.tid);
}
inline bool operator!=(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name != r.name) || (l.inc != r.inc) || (l.tid != r.tid);
}
inline bool operator<(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) || 
    (l.name == r.name && l.inc == r.inc && l.tid < r.tid);
}
inline bool operator<=(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) ||
    (l.name == r.name && l.inc == r.inc && l.tid <= r.tid);
}
inline bool operator>(const osd_reqid_t& l, const osd_reqid_t& r) { return !(l <= r); }
inline bool operator>=(const osd_reqid_t& l, const osd_reqid_t& r) { return !(l < r); }

namespace __gnu_cxx {
  template<> struct hash<osd_reqid_t> {
    size_t operator()(const osd_reqid_t &r) const { 
      static hash<uint64_t> H;
      return H(r.name.num() ^ r.tid ^ r.inc);
    }
  };
}


// a locator constrains the placement of an object.  mainly, which pool
// does it go in.
struct object_locator_t {
  int64_t pool;
  string key;

  explicit object_locator_t()
    : pool(-1) {}
  explicit object_locator_t(int64_t po)
    : pool(po) {}
  explicit object_locator_t(int64_t po, string s)
    : pool(po), key(s) {}

  int get_pool() const {
    return pool;
  }

  void clear() {
    pool = -1;
    key = "";
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_locator_t*>& o);
};
WRITE_CLASS_ENCODER(object_locator_t)

inline bool operator==(const object_locator_t& l, const object_locator_t& r) {
  return l.pool == r.pool && l.key == r.key;
}
inline bool operator!=(const object_locator_t& l, const object_locator_t& r) {
  return !(l == r);
}

inline ostream& operator<<(ostream& out, const object_locator_t& loc)
{
  out << "@" << loc.pool;
  if (loc.key.length())
    out << ":" << loc.key;
  return out;
}


// compound rados version type
class eversion_t {
public:
  version_t version;
  epoch_t epoch;
  __u32 __pad;
  eversion_t() : version(0), epoch(0), __pad(0) {}
  eversion_t(epoch_t e, version_t v) : version(v), epoch(e), __pad(0) {}

  eversion_t(const ceph_eversion& ce) : 
    version(ce.version),
    epoch(ce.epoch),
    __pad(0) { }

  eversion_t(bufferlist& bl) : __pad(0) { decode(bl); }

  static eversion_t max() {
    eversion_t max;
    max.version -= 1;
    max.epoch -= 1;
    return max;
  }

  operator ceph_eversion() {
    ceph_eversion c;
    c.epoch = epoch;
    c.version = version;
    return c;
  }

  void inc(epoch_t e) {
    if (epoch < e)
      epoch = e;
    version++;
  }

  void encode(bufferlist &bl) const {
    ::encode(version, bl);
    ::encode(epoch, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(version, bl);
    ::decode(epoch, bl);
  }
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
};
WRITE_CLASS_ENCODER(eversion_t)

inline bool operator==(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) && (l.version == r.version);
}
inline bool operator!=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch != r.epoch) || (l.version != r.version);
}
inline bool operator<(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version < r.version):(l.epoch < r.epoch);
}
inline bool operator<=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version <= r.version):(l.epoch <= r.epoch);
}
inline bool operator>(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version > r.version):(l.epoch > r.epoch);
}
inline bool operator>=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version >= r.version):(l.epoch >= r.epoch);
}
inline ostream& operator<<(ostream& out, const eversion_t e) {
  return out << e.epoch << "'" << e.version;
}



/** osd_stat
 * aggregate stats for an osd
 */
struct osd_stat_t {
  int64_t kb, kb_used, kb_avail;
  vector<int> hb_in, hb_out;
  int32_t snap_trim_queue_len, num_snap_trimming;

  osd_stat_t() : kb(0), kb_used(0), kb_avail(0),
		 snap_trim_queue_len(0), num_snap_trimming(0) {}

  void add(const osd_stat_t& o) {
    kb += o.kb;
    kb_used += o.kb_used;
    kb_avail += o.kb_avail;
    snap_trim_queue_len += o.snap_trim_queue_len;
    num_snap_trimming += o.num_snap_trimming;
  }
  void sub(const osd_stat_t& o) {
    kb -= o.kb;
    kb_used -= o.kb_used;
    kb_avail -= o.kb_avail;
    snap_trim_queue_len -= o.snap_trim_queue_len;
    num_snap_trimming -= o.num_snap_trimming;
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(std::list<osd_stat_t*>& o);
};
WRITE_CLASS_ENCODER(osd_stat_t)

inline bool operator==(const osd_stat_t& l, const osd_stat_t& r) {
  return l.kb == r.kb &&
    l.kb_used == r.kb_used &&
    l.kb_avail == r.kb_avail &&
    l.snap_trim_queue_len == r.snap_trim_queue_len &&
    l.num_snap_trimming == r.num_snap_trimming &&
    l.hb_in == r.hb_in &&
    l.hb_out == r.hb_out;
}
inline bool operator!=(const osd_stat_t& l, const osd_stat_t& r) {
  return !(l == r);
}
inline ostream& operator<<(ostream& out, const osd_stat_t& s) {
  return out << "osd_stat(" << kb_t(s.kb_used) << " used, "
	     << kb_t(s.kb_avail) << " avail, "
	     << kb_t(s.kb) << " total, "
	     << "peers " << s.hb_in << "/" << s.hb_out << ")";
}


/**
 * a summation of object stats
 *
 * This is just a container for object stats; we don't know what for.
 */
struct object_stat_sum_t {
  int64_t num_bytes;    // in bytes
  int64_t num_objects;
  int64_t num_object_clones;
  int64_t num_object_copies;  // num_objects * num_replicas
  int64_t num_objects_missing_on_primary;
  int64_t num_objects_degraded;
  int64_t num_objects_unfound;
  int64_t num_rd, num_rd_kb;
  int64_t num_wr, num_wr_kb;

  object_stat_sum_t()
    : num_bytes(0),
      num_objects(0), num_object_clones(0), num_object_copies(0),
      num_objects_missing_on_primary(0), num_objects_degraded(0), num_objects_unfound(0),
      num_rd(0), num_rd_kb(0), num_wr(0), num_wr_kb(0)
  {}

  void clear() {
    memset(this, 0, sizeof(*this));
  }

  void calc_copies(int nrep) {
    num_object_copies = nrep * num_objects;
  }

  bool is_zero() const {
    object_stat_sum_t zero;
    return memcmp(this, &zero, sizeof(zero)) == 0;
  }

  void add(const object_stat_sum_t& o);
  void sub(const object_stat_sum_t& o);

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<object_stat_sum_t*>& o);
};
WRITE_CLASS_ENCODER(object_stat_sum_t)

/**
 * a collection of object stat sums
 *
 * This is a collection of stat sums over different categories.
 */
struct object_stat_collection_t {
  object_stat_sum_t sum;
  map<string,object_stat_sum_t> cat_sum;

  void calc_copies(int nrep) {
    sum.calc_copies(nrep);
    for (map<string,object_stat_sum_t>::iterator p = cat_sum.begin(); p != cat_sum.end(); ++p)
      p->second.calc_copies(nrep);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<object_stat_collection_t*>& o);

  bool is_zero() const {
    return (cat_sum.empty() && sum.is_zero());
  }

  void clear() {
    sum.clear();
    cat_sum.clear();
  }

  void add(const object_stat_sum_t& o, const string& cat) {
    sum.add(o);
    if (cat.length())
      cat_sum[cat].add(o);
  }

  void add(const object_stat_collection_t& o) {
    sum.add(o.sum);
    for (map<string,object_stat_sum_t>::const_iterator p = o.cat_sum.begin();
	 p != o.cat_sum.end();
	 ++p)
      cat_sum[p->first].add(p->second);
  }
  void sub(const object_stat_collection_t& o) {
    sum.sub(o.sum);
    for (map<string,object_stat_sum_t>::const_iterator p = o.cat_sum.begin();
	 p != o.cat_sum.end();
	 ++p) {
      object_stat_sum_t& s = cat_sum[p->first];
      s.sub(p->second);
      if (s.is_zero())
	cat_sum.erase(p->first);
    }
  }
};
WRITE_CLASS_ENCODER(object_stat_collection_t)


// -----------------------------------------

struct osd_peer_stat_t {
  utime_t stamp;

  osd_peer_stat_t() { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<osd_peer_stat_t*>& o);
};
WRITE_CLASS_ENCODER(osd_peer_stat_t)

ostream& operator<<(ostream& out, const osd_peer_stat_t &stat);


// -----------------------------------------

class ObjectExtent {
 public:
  object_t    oid;       // object id
  uint64_t    offset;    // in object
  uint64_t    length;    // in object

  object_locator_t oloc;   // object locator (pool etc)

  map<uint64_t, uint64_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : offset(0), length(0) {}
  ObjectExtent(object_t o, uint64_t off=0, uint64_t l=0) : oid(o), offset(off), length(l) { }
};

inline ostream& operator<<(ostream& out, const ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " in " << ex.oloc
             << " " << ex.offset << "~" << ex.length
             << ")";
}





// ---------------------------------------

class OSDSuperblock {
public:
  uuid_d cluster_fsid, osd_fsid;
  int32_t whoami;    // my role in this fs.
  epoch_t current_epoch;             // most recent epoch
  epoch_t oldest_map, newest_map;    // oldest/newest maps we have.
  double weight;

  CompatSet compat_features;

  // last interval over which i mounted and was then active
  epoch_t mounted;     // last epoch i mounted
  epoch_t clean_thru;  // epoch i was active and clean thru

  OSDSuperblock() : 
    whoami(-1), 
    current_epoch(0), oldest_map(0), newest_map(0), weight(0),
    mounted(0), clean_thru(0) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<OSDSuperblock*>& o);
};
WRITE_CLASS_ENCODER(OSDSuperblock)

inline ostream& operator<<(ostream& out, const OSDSuperblock& sb)
{
  return out << "sb(" << sb.cluster_fsid
             << " osd." << sb.whoami
	     << " " << sb.osd_fsid
             << " e" << sb.current_epoch
             << " [" << sb.oldest_map << "," << sb.newest_map << "]"
	     << " lci=[" << sb.mounted << "," << sb.clean_thru << "]"
             << ")";
}


// -------

WRITE_CLASS_ENCODER(interval_set<uint64_t>)





/*
 * attached to object head.  describes most recent snap context, and
 * set of existing clones.
 */
struct SnapSet {
  snapid_t seq;
  bool head_exists;
  vector<snapid_t> snaps;    // ascending
  vector<snapid_t> clones;   // ascending
  map<snapid_t, interval_set<uint64_t> > clone_overlap;  // overlap w/ next newest
  map<snapid_t, uint64_t> clone_size;

  SnapSet() : head_exists(false) {}
  SnapSet(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
    
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapSet*>& o);  
};
WRITE_CLASS_ENCODER(SnapSet)

ostream& operator<<(ostream& out, const SnapSet& cs);



#define OI_ATTR "_"
#define SS_ATTR "snapset"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;

  watch_info_t(uint64_t c=0, uint32_t t=0) : cookie(c), timeout_seconds(t) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<watch_info_t*>& o);
};
WRITE_CLASS_ENCODER(watch_info_t)

static inline bool operator==(const watch_info_t& l, const watch_info_t& r) {
  return l.cookie == r.cookie && l.timeout_seconds == r.timeout_seconds;
}

static inline ostream& operator<<(ostream& out, const watch_info_t& w) {
  return out << "watch(cookie " << w.cookie << " " << w.timeout_seconds << "s)";
}

struct notify_info_t {
  uint64_t cookie;
  uint32_t timeout;
  bufferlist bl;
};

static inline ostream& operator<<(ostream& out, const notify_info_t& n) {
  return out << "notify(cookie " << n.cookie << " " << n.timeout << "s)";
}



struct object_info_t {
  hobject_t soid;
  object_locator_t oloc;
  string category;

  eversion_t version, prior_version;
  eversion_t user_version;
  osd_reqid_t last_reqid;

  uint64_t size;
  utime_t mtime;
  bool lost;

  osd_reqid_t wrlock_by;   // [head]
  vector<snapid_t> snaps;  // [clone]

  uint64_t truncate_seq, truncate_size;


  map<entity_name_t, watch_info_t> watchers;
  bool uses_tmap;

  void copy_user_bits(const object_info_t& other);

  static ps_t legacy_object_locator_to_ps(const object_t &oid, 
					  const object_locator_t &loc);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_info_t*>& o);

  explicit object_info_t()
    : size(0), lost(false),
      truncate_seq(0), truncate_size(0), uses_tmap(false)
  {}

  object_info_t(const hobject_t& s, const object_locator_t& o)
    : soid(s), oloc(o), size(0),
      lost(false), truncate_seq(0), truncate_size(0), uses_tmap(false) {}

  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)


ostream& operator<<(ostream& out, const object_info_t& oi);



// Object recovery
struct ObjectRecoveryInfo {
  hobject_t soid;
  eversion_t version;
  uint64_t size;
  object_info_t oi;
  SnapSet ss;
  interval_set<uint64_t> copy_subset;
  map<hobject_t, interval_set<uint64_t> > clone_subset;

  ObjectRecoveryInfo() : size(0) { }

  static void generate_test_instances(list<ObjectRecoveryInfo*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ObjectRecoveryInfo)
ostream& operator<<(ostream& out, const ObjectRecoveryInfo &inf);

struct ObjectRecoveryProgress {
  bool first;
  uint64_t data_recovered_to;
  bool data_complete;
  string omap_recovered_to;
  bool omap_complete;

  ObjectRecoveryProgress()
    : first(true),
      data_recovered_to(0),
      data_complete(false), omap_complete(false) { }

  bool is_complete(const ObjectRecoveryInfo& info) const {
    return (data_recovered_to >= (
      info.copy_subset.empty() ?
      0 : info.copy_subset.range_end())) &&
      omap_complete;
  }

  static void generate_test_instances(list<ObjectRecoveryProgress*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ObjectRecoveryProgress)
ostream& operator<<(ostream& out, const ObjectRecoveryProgress &prog);


/*
 * summarize pg contents for purposes of a scrub
 */
struct ScrubMap {
  struct object {
    uint64_t size;
    bool negative;
    map<string,bufferptr> attrs;

    object(): size(0), negative(false) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<object*>& o);
  };
  WRITE_CLASS_ENCODER(object)

  map<hobject_t,object> objects;
  map<string,bufferptr> attrs;
  bufferlist logbl;
  eversion_t valid_through;
  eversion_t incr_since;

  void merge_incr(const ScrubMap &l);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl, int64_t pool=-1);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ScrubMap*>& o);
};
WRITE_CLASS_ENCODER(ScrubMap::object)
WRITE_CLASS_ENCODER(ScrubMap)


struct OSDOp {
  ceph_osd_op op;
  sobject_t soid;

  bufferlist indata, outdata;
  int32_t rval;

  OSDOp() : rval(0) {
    memset(&op, 0, sizeof(ceph_osd_op));
  }

  /**
   * split a bufferlist into constituent indata nembers of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  static void split_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& in);

  /**
   * merge indata nembers of a vector of OSDOp into a single bufferlist
   *
   * Notably this also encodes certain other OSDOp data into the data
   * buffer, including the sobject_t soid.
   *
   * @param ops [in] vector of OSDOps
   * @param in  [out] combined data buffer
   */
  static void merge_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& out);

  /**
   * split a bufferlist into constituent outdata members of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  static void split_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& in);

  /**
   * merge outdata members of a vector of OSDOps into a single bufferlist
   *
   * @param ops [in] vector of OSDOps
   * @param in  [out] combined data buffer
   */
  static void merge_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& out);
};

ostream& operator<<(ostream& out, const OSDOp& op);


#endif // CEPH_OSD_TYPES_H
