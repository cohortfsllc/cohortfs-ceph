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
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO CompatSet::Feature(8, "leveldbinfo")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG CompatSet::Feature(9, "leveldblog")
#define CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER CompatSet::Feature(10, "snapmapper")

#define OSD_SUPERBLOCK_POBJECT hobject_t(sobject_t(object_t(0, "osd_superblock"), 0))


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

// Internal OSD op flags - set by the OSD based on the op types
enum {
  CEPH_OSD_RMW_FLAG_READ        = (1 << 1),
  CEPH_OSD_RMW_FLAG_WRITE       = (1 << 2),
  CEPH_OSD_RMW_FLAG_CLASS_READ  = (1 << 3),
  CEPH_OSD_RMW_FLAG_CLASS_WRITE = (1 << 4),
  CEPH_OSD_RMW_FLAG_PGOP        = (1 << 5),
};

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

  string get_key_name() const;

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
  int64_t num_scrub_errors;	// total deep and shallow scrub errors
  int64_t num_shallow_scrub_errors;
  int64_t num_deep_scrub_errors;
  int64_t num_objects_recovered;
  int64_t num_bytes_recovered;
  int64_t num_keys_recovered;

  object_stat_sum_t()
    : num_bytes(0),
      num_objects(0), num_object_clones(0), num_object_copies(0),
      num_objects_missing_on_primary(0), num_objects_degraded(0), num_objects_unfound(0),
      num_rd(0), num_rd_kb(0), num_wr(0), num_wr_kb(0),
      num_scrub_errors(0), num_shallow_scrub_errors(0),
      num_deep_scrub_errors(0),
      num_objects_recovered(0),
      num_bytes_recovered(0),
      num_keys_recovered(0)
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
  uint64_t    objectno;
  uint64_t    offset;    // in object
  uint64_t    length;    // in object
  uint64_t    truncate_size;	// in object

  vector<pair<uint64_t,uint64_t> >  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : objectno(0), offset(0), length(0), truncate_size(0) {}
  ObjectExtent(object_t o, uint64_t ono, uint64_t off, uint64_t l, uint64_t ts) :
    oid(o), objectno(ono), offset(off), length(l), truncate_size(ts) { }
};

inline ostream& operator<<(ostream& out, const ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " (" << ex.objectno << ") in " 
             << " " << ex.offset << "~" << ex.length
	     << " -> " << ex.buffer_extents
             << ")";
}





// ---------------------------------------

class OSDSuperblock {
public:
  uuid_d cluster_fsid, osd_fsid;
  int whoami;    // my role in this fs.
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
  vector<snapid_t> snaps;    // descending
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
  entity_addr_t addr;

  watch_info_t() : cookie(0), timeout_seconds(0) { }
  watch_info_t(uint64_t c, uint32_t t, entity_addr_t a) : cookie(c), timeout_seconds(t), addr(a) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<watch_info_t*>& o);
};
WRITE_CLASS_ENCODER(watch_info_t)

static inline bool operator==(const watch_info_t& l, const watch_info_t& r) {
  return l.cookie == r.cookie && l.timeout_seconds == r.timeout_seconds
	    && l.addr == r.addr;
}

static inline ostream& operator<<(ostream& out, const watch_info_t& w) {
  return out << "watch(cookie " << w.cookie << " " << w.timeout_seconds << "s"
    << " " << w.addr << ")";
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


  map<pair<uint64_t, entity_name_t>, watch_info_t> watchers;
  bool uses_tmap;

  void copy_user_bits(const object_info_t& other);

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

  object_info_t(const hobject_t& s)
    : soid(s), size(0),
      lost(false), truncate_seq(0), truncate_size(0), uses_tmap(false) {}

  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)

struct ObjectState {
  object_info_t oi;
  bool exists;

  ObjectState(const object_info_t &oi_, bool exists_)
    : oi(oi_), exists(exists_) {}
};


struct SnapSetContext {
  object_t oid;
  int ref;
  bool registered;
  SnapSet snapset;

  SnapSetContext(const object_t& o) : oid(o), ref(0), registered(false) { }
};


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

struct watch_item_t {
  entity_name_t name;
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_item_t() : cookie(0), timeout_seconds(0) { }
  watch_item_t(entity_name_t name, uint64_t cookie, uint32_t timeout,
     entity_addr_t addr)
    : name(name), cookie(cookie), timeout_seconds(timeout),
    addr(addr) { }

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(name, bl);
    ::encode(cookie, bl);
    ::encode(timeout_seconds, bl);
    ::encode(addr, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(2, bl);
    ::decode(name, bl);
    ::decode(cookie, bl);
    ::decode(timeout_seconds, bl);
    if (struct_v >= 2) {
      ::decode(addr, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(watch_item_t)

struct obj_watch_item_t {
  hobject_t obj;
  watch_item_t wi;
};

/**
 * obj list watch response format
 *
 */
struct obj_list_watch_response_t {
  list<watch_item_t> entries;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_array_section("entries");
    for (list<watch_item_t>::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("watch");
      f->dump_stream("watcher") << p->name;
      f->dump_int("cookie", p->cookie);
      f->dump_int("timeout", p->timeout_seconds);
      f->open_object_section("addr");
      p->addr.dump(f);
      f->close_section();
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<obj_list_watch_response_t*>& o) {
    entity_addr_t ea;
    o.push_back(new obj_list_watch_response_t);
    o.push_back(new obj_list_watch_response_t);
    ea.set_nonce(1000);
    ea.set_family(AF_INET);
    ea.set_in4_quad(0, 127);
    ea.set_in4_quad(1, 0);
    ea.set_in4_quad(2, 0);
    ea.set_in4_quad(3, 1);
    ea.set_port(1024);
    o.back()->entries.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 1), 10, 30, ea));
    ea.set_nonce(1001);
    ea.set_in4_quad(3, 2);
    ea.set_port(1025);
    o.back()->entries.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 2), 20, 60, ea));
  }
};

WRITE_CLASS_ENCODER(obj_list_watch_response_t)

struct clone_info {
  snapid_t cloneid;
  vector<snapid_t> snaps;  // ascending
  vector< pair<uint64_t,uint64_t> > overlap;
  uint64_t size;

  clone_info() : cloneid(CEPH_NOSNAP), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(cloneid, bl);
    ::encode(snaps, bl);
    ::encode(overlap, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(cloneid, bl);
    ::decode(snaps, bl);
    ::decode(overlap, bl);
    ::decode(size, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    if (cloneid == CEPH_NOSNAP)
      f->dump_string("cloneid", "HEAD");
    else
      f->dump_unsigned("cloneid", cloneid.val);
    f->open_array_section("snapshots");
    for (vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
      f->open_object_section("snap");
      f->dump_unsigned("id", p->val);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("overlaps");
    for (vector< pair<uint64_t,uint64_t> >::const_iterator q = overlap.begin();
          q != overlap.end(); ++q) {
      f->open_object_section("overlap");
      f->dump_unsigned("offset", q->first);
      f->dump_unsigned("length", q->second);
      f->close_section();
    }
    f->close_section();
    f->dump_unsigned("size", size);
  }
  static void generate_test_instances(list<clone_info*>& o) {
    o.push_back(new clone_info);
    o.push_back(new clone_info);
    o.back()->cloneid = 1;
    o.back()->snaps.push_back(1);
    o.back()->overlap.push_back(pair<uint64_t,uint64_t>(0,4096));
    o.back()->overlap.push_back(pair<uint64_t,uint64_t>(8192,4096));
    o.back()->size = 16384;
    o.push_back(new clone_info);
    o.back()->cloneid = CEPH_NOSNAP;
    o.back()->size = 32768;
  }
};
WRITE_CLASS_ENCODER(clone_info)

/**
 * obj list snaps response format
 *
 */
struct obj_list_snap_response_t {
  vector<clone_info> clones;   // ascending
  snapid_t seq;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(clones, bl);
    ::encode(seq, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(clones, bl);
    if (struct_v >= 2)
      ::decode(seq, bl);
    else
      seq = CEPH_NOSNAP;
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_array_section("clones");
    for (vector<clone_info>::const_iterator p = clones.begin(); p != clones.end(); ++p) {
      f->open_object_section("clone");
      p->dump(f);
      f->close_section();
    }
    f->dump_unsigned("seq", seq);
    f->close_section();
  }
  static void generate_test_instances(list<obj_list_snap_response_t*>& o) {
    o.push_back(new obj_list_snap_response_t);
    o.push_back(new obj_list_snap_response_t);
    clone_info cl;
    cl.cloneid = 1;
    cl.snaps.push_back(1);
    cl.overlap.push_back(pair<uint64_t,uint64_t>(0,4096));
    cl.overlap.push_back(pair<uint64_t,uint64_t>(8192,4096));
    cl.size = 16384;
    o.back()->clones.push_back(cl);
    cl.cloneid = CEPH_NOSNAP;
    cl.snaps.clear();
    cl.overlap.clear();
    cl.size = 32768;
    o.back()->clones.push_back(cl);
    o.back()->seq = 123;
  }
};

WRITE_CLASS_ENCODER(obj_list_snap_response_t)

class coll_t {
public:
  const static coll_t META_COLL;

  coll_t()
    : str("meta")
  { }

  explicit coll_t(const std::string &str_)
    : str(str_)
  { }

  const std::string& to_str() const {
    return str;
  }

  const char* c_str() const {
    return str.c_str();
  }

  int operator<(const coll_t &rhs) const {
    return str < rhs.str;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  inline bool operator==(const coll_t& rhs) const {
    return str == rhs.str;
  }
  inline bool operator!=(const coll_t& rhs) const {
    return str != rhs.str;
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<coll_t*>& o);

private:
  std::string str;
};

WRITE_CLASS_ENCODER(coll_t)

inline ostream& operator<<(ostream& out, const coll_t& c) {
  out << c.to_str();
  return out;
}

namespace __gnu_cxx {
  template<> struct hash<coll_t> {
    size_t operator()(const coll_t &c) const {
      size_t h = 0;
      string str(c.to_str());
      std::string::const_iterator end(str.end());
      for (std::string::const_iterator s = str.begin(); s != end; ++s) {
	h += *s;
	h += (h << 10);
	h ^= (h >> 6);
      }
      h += (h << 3);
      h ^= (h >> 11);
      h += (h << 15);
      return h;
    }
  };
}
#endif
