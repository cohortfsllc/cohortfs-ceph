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

#ifndef CEPH_OSD_TYPES_H
#define CEPH_OSD_TYPES_H

#include <sstream>
#include <cstdio>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/optional.hpp>

#include "include/rados/rados_types.hpp"

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/ceph_time.h"
#include "include/CompatSet.h"
#include "common/histogram.h"
#include "include/interval_set.h"
#include "common/Formatter.h"
#include "common/oid.h"
#include "Watch.h"
#include "OpRequest.h"
#include "include/cmp.h"

#define CEPH_OSD_ONDISK_MAGIC "ceph osd volume v026"

#define CEPH_OSD_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "initial feature set(~v.18)")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO CompatSet::Feature(8, "leveldbinfo")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG CompatSet::Feature(9, "leveldblog")


typedef oid_t collection_list_handle_t;

/// convert a single CPEH_OSD_FLAG_* to a string
const char *ceph_osd_flag_name(unsigned flag);

/// convert CEPH_OSD_FLAG_* op flags to a string
string ceph_osd_flag_string(unsigned flags);

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

namespace std {
  template<> struct hash<osd_reqid_t> {
    size_t operator()(const osd_reqid_t &r) const {
      static hash<uint64_t> H;
      return H(r.name.num() ^ r.tid ^ r.inc);
    }
  };
}


// Internal OSD op flags - set by the OSD based on the op types
enum {
  CEPH_OSD_RMW_FLAG_READ	= (1 << 1),
  CEPH_OSD_RMW_FLAG_WRITE	= (1 << 2),
  CEPH_OSD_RMW_FLAG_CLASS_READ	= (1 << 3),
  CEPH_OSD_RMW_FLAG_CLASS_WRITE = (1 << 4),
  CEPH_OSD_RMW_FLAG_CACHE	= (1 << 6),
};


// pg stuff

// object namespaces
#define CEPH_METADATA_NS       1
#define CEPH_DATA_NS	       2
#define CEPH_CAS_NS	       3
#define CEPH_OSDMETADATA_NS 0xff

#define OSD_SUPERBLOCK_POBJECT oid_t("osd_superblock")

// ----------------------

class coll_t {
public:
  const static coll_t META_COLL;

  coll_t()
    : str("meta")
  { }

  explicit coll_t(const std::string &str_)
    : str(str_)
  { }

  explicit coll_t(const boost::uuids::uuid& volume)
    : str(to_string(volume))
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

  bool is_vol(boost::uuids::uuid& volume) const;
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

inline ostream& operator<<(ostream& out, const ceph_object_layout &ol)
{
  int su = ol.ol_stripe_unit;
  if (su)
    out << ".su=" << su;
  return out;
}

// compound rados version type
class eversion_t {
public:
  version_t version;
  epoch_t epoch;
  uint32_t __pad;
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

/**
 * objectstore_perf_stat_t
 *
 * current perf information about the osd
 */
struct objectstore_perf_stat_t {
  // cur_op_latency is in ms since double add/sub are not associative
  uint32_t filestore_commit_latency;
  uint32_t filestore_apply_latency;

  objectstore_perf_stat_t() :
    filestore_commit_latency(0), filestore_apply_latency(0) {}

  void add(const objectstore_perf_stat_t &o) {
    filestore_commit_latency += o.filestore_commit_latency;
    filestore_apply_latency += o.filestore_apply_latency;
  }

  void sub(const objectstore_perf_stat_t &o) {
    filestore_commit_latency -= o.filestore_commit_latency;
    filestore_apply_latency -= o.filestore_apply_latency;
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(std::list<objectstore_perf_stat_t*>& o);
};
WRITE_CLASS_ENCODER(objectstore_perf_stat_t)

/** osd_stat
 * aggregate stats for an osd
 */
struct osd_stat_t {
  int64_t kb, kb_used, kb_avail;
  vector<int> hb_in, hb_out;

  pow2_hist_t op_queue_age_hist;

  objectstore_perf_stat_t fs_perf_stat;

  osd_stat_t() : kb(0), kb_used(0), kb_avail(0) {}

  void add(const osd_stat_t& o) {
    kb += o.kb;
    kb_used += o.kb_used;
    kb_avail += o.kb_avail;
    op_queue_age_hist.add(o.op_queue_age_hist);
    fs_perf_stat.add(o.fs_perf_stat);
  }

  void sub(const osd_stat_t& o) {
    kb -= o.kb;
    kb_used -= o.kb_used;
    kb_avail -= o.kb_avail;
    op_queue_age_hist.sub(o.op_queue_age_hist);
    fs_perf_stat.sub(o.fs_perf_stat);
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
	     << "peers " << s.hb_in << "/" << s.hb_out
	     << " op hist " << s.op_queue_age_hist.h
	     << ")";
}

/**
 * a summation of object stats
 *
 * This is just a container for object stats; we don't know what for.
 */
struct object_stat_sum_t {
  int64_t num_bytes;	// in bytes
  int64_t num_objects;
  int64_t num_rd, num_rd_kb;
  int64_t num_wr, num_wr_kb;
  int64_t num_objects_omap;

  object_stat_sum_t()
    : num_bytes(0), num_objects(0), num_rd(0), num_rd_kb(0),
      num_wr(0), num_wr_kb(0), num_objects_omap(0)
  {}

  void clear() {
    memset(this, 0, sizeof(*this));
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

/**
 * vol_info_t - summary of volume statistics.
 *
 * Probably not needed still.
 */
struct vol_info_t {
  boost::uuids::uuid volume;
  uint64_t hk;
  eversion_t last_update;    // last object version applied to store.
  epoch_t last_epoch_started;// last epoch at which this volume
			     // started on this osd
  version_t last_user_version; // last user object version applied to store

  vol_info_t(const boost::uuids::uuid& volume)
    : volume(volume),
      last_epoch_started(0), last_user_version(0)
  {
    boost::hash<boost::uuids::uuid> hash;
    hk = hash(volume);
  }

  vol_info_t()
    : vol_info_t(boost::uuids::uuid{{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}})
  {}

  bool is_empty() const { return last_update.version == 0; }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<vol_info_t*>& o);
};
WRITE_CLASS_ENCODER(vol_info_t)

inline ostream& operator<<(ostream& out, const vol_info_t& vi)
{
  out << vi.volume << "(";
  if (vi.is_empty())
    out << " empty";
  else
    out << " v " << vi.last_update;
  out << " local-les=" << vi.last_epoch_started << ")";
  return out;
}

class ObjectModDesc {
  bool can_local_rollback;
  bool stashed;
public:
  class Visitor {
  public:
    virtual void append(uint64_t old_offset) {}
    virtual void setattrs(map<string, boost::optional<bufferlist> > &attrs) {}
    virtual void rmobject(version_t old_version) {}
    virtual void create() {}
    virtual ~Visitor() {}
  };

  void visit(Visitor *visitor) const;
  mutable bufferlist bl;
  enum ModID {
    APPEND = 1,
    SETATTRS = 2,
    DELETE = 3,
    CREATE = 4
  };

  ObjectModDesc() : can_local_rollback(true), stashed(false) {}
  void claim(ObjectModDesc &other) {
    bl.clear();
    bl.claim(other.bl);
    can_local_rollback = other.can_local_rollback;
    stashed = other.stashed;
  }

  void claim_append(ObjectModDesc &other) {
    if (!can_local_rollback || stashed)
      return;
    bl.claim_append(other.bl);
    stashed = other.stashed;
  }

  void swap(ObjectModDesc &other) {
    bl.swap(other.bl);

    bool temp = other.can_local_rollback;
    other.can_local_rollback = can_local_rollback;
    can_local_rollback = temp;

    temp = other.stashed;
    other.stashed = stashed;
    stashed = temp;
  }

  void append_id(ModID id) {
    uint8_t _id(id);
    ::encode(_id, bl);
  }

  void append(uint64_t old_size) {
    if (!can_local_rollback || stashed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(APPEND);
    ::encode(old_size, bl);
    ENCODE_FINISH(bl);
  }

  void setattrs(map<string, boost::optional<bufferlist> > &old_attrs) {
    if (!can_local_rollback || stashed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(SETATTRS);
    ::encode(old_attrs, bl);
    ENCODE_FINISH(bl);
  }

  bool rmobject(version_t deletion_version) {
    if (!can_local_rollback || stashed)
      return false;
    ENCODE_START(1, 1, bl);
    append_id(DELETE);
    ::encode(deletion_version, bl);
    ENCODE_FINISH(bl);
    stashed = true;
    return true;
  }

  void create() {
    if (!can_local_rollback || stashed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(CREATE);
    ENCODE_FINISH(bl);
  }

  bool empty() const {
    return can_local_rollback && (bl.length() == 0);
  }

  /**
   * Create fresh copy of bl bytes to avoid keeping large buffers around
   * in the case that bl contains ptrs which point into a much larger
   * message buffer
   */
  void trim_bl() {
    if (bl.length() > 0)
      bl.rebuild();
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectModDesc*>& o);
};
WRITE_CLASS_ENCODER(ObjectModDesc)


// -----------------------------------------

struct osd_peer_stat_t {
  ceph::real_time stamp;

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
  oid_t oid; // object id
  uint64_t offset; // in object
  uint64_t length; // in object
  uint64_t truncate_size; // in object

  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  vector<pair<uint64_t,uint64_t> >  buffer_extents;

  ObjectExtent() : offset(0), length(0), truncate_size(0) {}
  ObjectExtent(oid_t o, uint64_t off,
	       uint64_t l, uint64_t ts) :
    oid(o), offset(off), length(l), truncate_size(ts) { }
};

inline ostream& operator<<(ostream& out, const ObjectExtent &ex)
{
  return out << "extent( in "
	     << " " << ex.offset << "~" << ex.length
	     << " -> " << ex.buffer_extents
	     << ")";
}

// ---------------------------------------

class OSDSuperblock {
public:
  boost::uuids::uuid cluster_fsid, osd_fsid;
  int32_t whoami;    // my role in this fs.
  epoch_t current_epoch;	     // most recent epoch
  epoch_t oldest_map, newest_map;    // oldest/newest maps we have.
  double weight;

  CompatSet compat_features;

  // last interval over which i mounted and was then active
  epoch_t mounted;     // last epoch i mounted
  epoch_t last_map_marked_full; // last epoch osdmap was marked full

  OSDSuperblock() :
    whoami(-1),
    current_epoch(0), oldest_map(0), newest_map(0), weight(0),
    mounted(0), last_map_marked_full(0) {
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
	     << " lci=[" << sb.mounted << "," << "]"
	     << ")";
}


// -------

WRITE_CLASS_ENCODER(interval_set<uint64_t>)


#define OI_ATTR "_"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_info_t() : cookie(0), timeout_seconds(0) { }
  watch_info_t(uint64_t c, uint32_t t, const entity_addr_t& a)
    : cookie(c), timeout_seconds(t), addr(a) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<watch_info_t*>& o);
};
WRITE_CLASS_ENCODER(watch_info_t)

static inline bool operator==(const watch_info_t& l,
			      const watch_info_t& r) {
  return l.cookie == r.cookie &&
    l.timeout_seconds == r.timeout_seconds && l.addr == r.addr;
}

static inline ostream& operator<<(ostream& out,
				  const watch_info_t& w) {
  return out << "watch(cookie " << w.cookie << " "
	     << w.timeout_seconds << "s"
	     << " " << w.addr << ")";
}

struct notify_info_t {
  uint64_t cookie;
  uint32_t timeout;
  bufferlist bl;
};

static inline ostream& operator<<(ostream& out,
				  const notify_info_t& n) {
  return out << "notify(cookie " << n.cookie << " "
	     << n.timeout << "s)";
}

struct object_info_t {
  hoid_t oid;

  eversion_t version, prior_version;
  version_t user_version;
  osd_reqid_t last_reqid;

  uint64_t size;
  ceph::real_time mtime;

  // note: these are currently encoded into a total 16 bits; see
  // encode()/decode() for the weirdness.
  typedef enum {
    FLAG_OMAP	  = 1 << 3  // has (or may have) some/any omap data
  } flag_t;

  flag_t flags;

  static string get_flag_string(flag_t flags) {
    string s;
    if (flags & FLAG_OMAP)
      s += "|omap";
    if (s.length())
      return s.substr(1);
    return s;
  }

  string get_flag_string() const {
    return get_flag_string(flags);
  }

  osd_reqid_t wrlock_by;   // [head]

  uint64_t truncate_seq, truncate_size;
  uint64_t total_real_length;

  map<pair<uint64_t, entity_name_t>, watch_info_t> watchers;

  void copy_user_bits(const object_info_t& other);

  bool test_flag(flag_t f) const {
    return (flags & f) == f;
  }

  void set_flag(flag_t f) {
    flags = (flag_t)(flags | f);
  }

  void clear_flag(flag_t f) {
    flags = (flag_t)(flags & ~f);
  }

  bool is_omap() const {
    return test_flag(FLAG_OMAP);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_info_t*>& o);

  explicit object_info_t()
    : user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0), total_real_length(0)
  {}

  object_info_t(const hoid_t& _oid)
    : oid(_oid),
      user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0), total_real_length(0) {}

  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)

struct ObjectState {
  object_info_t oi;
  void* oh; /* ObjectHandle */
  bool exists; /* < the stored object exists (i.e., we will remember
		  the object_info_t) */

  ObjectState(const hoid_t& _oid, void* _oh)
    : oi(_oid), oh(_oh), exists(false) {}

  ObjectState(const object_info_t &_oi, void* _oh, bool _exists)
    : oi(_oi), oh(_oh), exists(_exists) {}
};

/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */
struct ObjectContext {
  ObjectState obs;

private:
  std::mutex lock; /* XXX */

  void on_last_ref(void* oh); // returns Object ref

public:
  std::atomic<uint64_t> nref;
  std::condition_variable cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;

  /* any entity in obs.oi.watchers MUST be in either watchers or
   * unconnected_watchers. */
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  void get() { nref.fetch_add(1, std::memory_order_relaxed); }

  void put() {
    if (nref.fetch_sub(1, std::memory_order_release) == 1) {
      std::atomic_thread_fence(std::memory_order_acquire);
      on_last_ref(obs.oh);
    }
  }

  struct RWState {
    enum State {
      RWNONE,
      RWREAD,
      RWWRITE
    };

    static const char *get_state_name(State s) {
      switch (s) {
      case RWNONE: return "none";
      case RWREAD: return "read";
      case RWWRITE: return "write";
      default: return "???";
      }
    }

    const char *get_state_name() const {
      return get_state_name(state);
    }

    State state;		 ///< rw state
    uint64_t count;		 ///< number of readers or writers
    OpRequest::Queue waiters;	 ///< ops waiting on state change

    RWState()
      : state(RWNONE), count(0)
    {}

    bool get_read(OpRequest* op) {
      if (get_read_lock()) {
	return true;
      } // else
      assert(! op->q_hook.is_linked());
      op->get(); // waiters queue ref
      waiters.push_back(*op);
      return false;
    }

    /// this function adjusts the counts if necessary
    bool get_read_lock() {
      // don't starve anybody!
      if (!waiters.empty()) {
	return false;
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWREAD;
	// fall through
      case RWREAD:
	count++;
	return true;
      case RWWRITE:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }

    bool get_write(OpRequest* op) {
      if (get_write_lock()) {
	return true;
      } // else
      /* XXX code had if (op) check */
      op->get();
      waiters.push_back(*op);
      return false;
    }

    bool get_write_lock() {
      // don't starve anybody!
      if (!waiters.empty()) {
	return false;
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWWRITE;
	// fall through
      case RWWRITE:
	count++;
	return true;
      case RWREAD:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }

    /// same as get_write_lock, but ignore starvation
    bool take_write_lock() {
      if (state == RWWRITE) {
	count++;
	return true;
      }
      return get_write_lock();
    }

    void dec(OpRequest::Queue& to_requeue) {
      assert(count > 0);
      count--;
      if (count == 0) {
	OpRequest::Queue::iterator i1 = to_requeue.end();
	to_requeue.splice(i1, waiters);
	state = RWNONE;
      }
    }

    void put_read(OpRequest::Queue& to_requeue) {
      assert(state == RWREAD);
      dec(to_requeue);
    }

    void put_write(OpRequest::Queue& to_requeue) {
      assert(state == RWWRITE);
      dec(to_requeue);
    }

    bool empty() const { return state == RWNONE; }
  } rwstate;
  cohort::SpinLock rwstate_lock;

  bool get_read(OpRequest* op) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    return rwstate.get_read(op);
  }

  bool get_write(OpRequest* op) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    return rwstate.get_write(op);
  }

  void put_read(OpRequest::Queue& to_wake) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    rwstate.put_read(to_wake);
  }

  void put_write(OpRequest::Queue& to_wake) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    rwstate.put_write(to_wake);
  }

  ObjectContext(const hoid_t& _oid, void* oh)
    : obs(_oid, oh), nref(0),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0)
  {}

  ~ObjectContext() {
    assert(rwstate.empty());
  }

  /* do simple synchronous mutual exclusion, for now. no waitqueues
   * or anything fancy. */
  void ondisk_write_lock() {
    std::unique_lock<std::mutex> l(lock);
    writers_waiting++;
    cond.wait(l, [&](){ return !(readers_waiting || readers); });
    writers_waiting--;
    unstable_writes++;
    l.unlock();
  }

  void ondisk_write_unlock() {
    std::lock_guard<std::mutex> l(lock);
    assert(unstable_writes > 0);
    unstable_writes--;
    if (!unstable_writes && readers_waiting)
      cond.notify_all();
  }

  void ondisk_read_lock() {
    std::unique_lock<std::mutex> l(lock);
    readers_waiting++;
    cond.wait(l, [&](){ return !unstable_writes; });
    readers_waiting--;
    readers++;
  }

  void ondisk_read_unlock() {
    std::lock_guard<std::mutex> l(lock);
    assert(readers > 0);
    readers--;
    if (!readers && writers_waiting)
      cond.notify_all();
  }

  // attr cache
  map<string, bufferlist> attr_cache;

  void fill_in_setattrs(const set<string> &changing,
			ObjectModDesc *mod) {
    map<string, boost::optional<bufferlist> > to_set;
    for (set<string>::const_iterator i = changing.begin();
	 i != changing.end();
	 ++i) {
      map<string, bufferlist>::iterator iter = attr_cache.find(*i);
      if (iter != attr_cache.end()) {
	to_set[*i] = iter->second;
      } else {
	to_set[*i];
      }
    }
    mod->setattrs(to_set);
  }
}; /* ObjectContext */


typedef boost::intrusive_ptr<ObjectContext> ObjectContextRef;

inline void intrusive_ptr_add_ref(ObjectContext *obc) { obc->get(); }
inline void intrusive_ptr_release(ObjectContext *obc) { obc->put(); }

inline ostream& operator<<(ostream& out, const ObjectState& obs)
{
  out << obs.oi.oid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline ostream& operator<<(ostream& out,
			   const ObjectContext::RWState& rw)
{
  return out << "rwstate(" << rw.get_state_name()
	     << " n=" << rw.count
	     << " w=" << rw.waiters.size()
	     << ")";
}

inline ostream& operator<<(ostream& out, const ObjectContext& obc)
{
  return out << "obc(" << obc.obs << " " << obc.rwstate << ")";
}

ostream& operator<<(ostream& out, const object_info_t& oi);

struct OSDOp {
  ceph_osd_op op;
  oid_t oid;

  bufferlist indata, outdata;
  int32_t rval;

  Context* ctx;
  bufferlist* out_bl;
  int* out_rval;

  OSDOp(int the_op = 0) : rval(0), ctx(nullptr), out_bl(nullptr),
			  out_rval(nullptr) {
    memset(&op, 0, sizeof(ceph_osd_op));
    op.op = the_op;
  }

  /**
   * split a bufferlist into constituent indata nembers of a vector
   * of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in	[in] combined data buffer
   */
  static void split_osd_op_vector_in_data(vector<OSDOp>& ops,
					  bufferlist& in);

  /**
   * merge indata nembers of a vector of OSDOp into a single bufferlist
   *
   * Notably this also encodes certain other OSDOp data into the data
   * buffer, including the object_t oid.
   *
   * @param ops [in] vector of OSDOps
   * @param in	[out] combined data buffer
   */
  static void merge_osd_op_vector_in_data(vector<OSDOp>& ops,
					  bufferlist& out);

  /**
   * split a bufferlist into constituent outdata members of a vector
   * of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in	[in] combined data buffer
   */
  static void split_osd_op_vector_out_data(vector<OSDOp>& ops,
					   bufferlist& in);

  /**
   * merge outdata members of a vector of OSDOps into a single
   * bufferlist
   *
   * @param ops [in] vector of OSDOps
   * @param in	[out] combined data buffer
   */
  static void merge_osd_op_vector_out_data(vector<OSDOp>& ops,
					   bufferlist& out);
};

ostream& operator<<(ostream& out, const OSDOp& op);

struct watch_item_t {
  entity_name_t name;
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_item_t() : cookie(0), timeout_seconds(0) { }
  watch_item_t(entity_name_t name, uint64_t cookie, uint32_t timeout,
     const entity_addr_t& addr)
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
  hoid_t oid;
  watch_item_t wi;
};

/**
 * oid list watch response format
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

#endif
