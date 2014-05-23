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
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_TYPES_H
#define CEPH_OSD_TYPES_H

#include <sstream>
#include <stdio.h>
#include <memory>
#include <boost/scoped_ptr.hpp>
#include <boost/optional.hpp>

#include "include/rados/rados_types.hpp"

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "common/histogram.h"
#include "include/interval_set.h"
#include "common/Formatter.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "Watch.h"
#include "OpRequest.h"
#include "include/cmp.h"

#define CEPH_OSD_ONDISK_MAGIC "ceph osd volume v026"

#define CEPH_OSD_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "initial feature set(~v.18)")
#define CEPH_OSD_FEATURE_INCOMPAT_PGINFO CompatSet::Feature(2, "pginfo object")
#define CEPH_OSD_FEATURE_INCOMPAT_OLOC CompatSet::Feature(3, "object locator")
#define CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES  CompatSet::Feature(5, "categories")
#define CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL  CompatSet::Feature(6, "hobjectpool")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO CompatSet::Feature(8, "leveldbinfo")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG CompatSet::Feature(9, "leveldblog")


typedef hobject_t collection_list_handle_t;

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

CEPH_HASH_NAMESPACE_START
  template<> struct hash<osd_reqid_t> {
    size_t operator()(const osd_reqid_t &r) const { 
      static hash<uint64_t> H;
      return H(r.name.num() ^ r.tid ^ r.inc);
    }
  };
CEPH_HASH_NAMESPACE_END


// -----

// a locator constrains the placement of an object.  mainly, which pool
// does it go in.
struct object_locator_t {
  // You specify either the hash or the key -- not both
  int64_t pool;     ///< pool id
  string key;       ///< key string (if non-empty)
  string nspace;    ///< namespace
  int64_t hash;     ///< hash position (if >= 0)

  explicit object_locator_t()
    : pool(-1), hash(-1) {}
  explicit object_locator_t(int64_t po)
    : pool(po), hash(-1)  {}
  explicit object_locator_t(int64_t po, int64_t ps)
    : pool(po), hash(ps)  {}
  explicit object_locator_t(int64_t po, string ns)
    : pool(po), nspace(ns), hash(-1) {}
  explicit object_locator_t(int64_t po, string ns, int64_t ps)
    : pool(po), nspace(ns), hash(ps) {}
  explicit object_locator_t(int64_t po, string ns, string s)
    : pool(po), key(s), nspace(ns), hash(-1) {}
  explicit object_locator_t(const hobject_t& soid)
    : pool(soid.pool), key(soid.get_key()), nspace(soid.nspace), hash(-1) {}

  int64_t get_pool() const {
    return pool;
  }

  void clear() {
    pool = -1;
    key = "";
    nspace = "";
    hash = -1;
  }

  bool empty() const {
    return pool == -1;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_locator_t*>& o);
};
WRITE_CLASS_ENCODER(object_locator_t)

inline bool operator==(const object_locator_t& l, const object_locator_t& r) {
  return l.pool == r.pool && l.key == r.key && l.nspace == r.nspace && l.hash == r.hash;
}
inline bool operator!=(const object_locator_t& l, const object_locator_t& r) {
  return !(l == r);
}

inline ostream& operator<<(ostream& out, const object_locator_t& loc)
{
  out << "@" << loc.pool;
  if (loc.nspace.length())
    out << ";" << loc.nspace;
  if (loc.key.length())
    out << ":" << loc.key;
  return out;
}

struct request_redirect_t {
private:
  object_locator_t redirect_locator; ///< this is authoritative
  string redirect_object; ///< If non-empty, the request goes to this object name
  bufferlist osd_instructions; ///< a bufferlist for the OSDs, passed but not interpreted by clients

  friend ostream& operator<<(ostream& out, const request_redirect_t& redir);
public:

  request_redirect_t() {}
  explicit request_redirect_t(const object_locator_t& orig, int64_t rpool) :
      redirect_locator(orig) { redirect_locator.pool = rpool; }
  explicit request_redirect_t(const object_locator_t& rloc) :
      redirect_locator(rloc) {}
  explicit request_redirect_t(const object_locator_t& orig,
                              const string& robj) :
      redirect_locator(orig), redirect_object(robj) {}

  void set_instructions(const bufferlist& bl) { osd_instructions = bl; }
  const bufferlist& get_instructions() { return osd_instructions; }

  bool empty() const { return redirect_locator.empty() &&
			      redirect_object.empty(); }

  void combine_with_locator(object_locator_t& orig, string& obj) const {
    orig = redirect_locator;
    if (!redirect_object.empty())
      obj = redirect_object;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<request_redirect_t*>& o);
};
WRITE_CLASS_ENCODER(request_redirect_t)

inline ostream& operator<<(ostream& out, const request_redirect_t& redir) {
  out << "object " << redir.redirect_object << ", locator{" << redir.redirect_locator << "}";
  return out;
}

// Internal OSD op flags - set by the OSD based on the op types
enum {
  CEPH_OSD_RMW_FLAG_READ        = (1 << 1),
  CEPH_OSD_RMW_FLAG_WRITE       = (1 << 2),
  CEPH_OSD_RMW_FLAG_CLASS_READ  = (1 << 3),
  CEPH_OSD_RMW_FLAG_CLASS_WRITE = (1 << 4),
  CEPH_OSD_RMW_FLAG_PGOP        = (1 << 5),
  CEPH_OSD_RMW_FLAG_CACHE       = (1 << 6),
};


// pg stuff

// object namespaces
#define CEPH_METADATA_NS       1
#define CEPH_DATA_NS           2
#define CEPH_CAS_NS            3
#define CEPH_OSDMETADATA_NS 0xff

#define OSD_SUPERBLOCK_POBJECT hobject_t(object_t("osd_superblock"))

// placement seed (a hash value)
typedef uint32_t ps_t;

// old (v1) pg_t encoding (wrap old struct ceph_pg)
struct old_pg_t {
  ceph_pg v;
  void encode(bufferlist& bl) const {
    ::encode_raw(v, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode_raw(v, bl);
  }
};
WRITE_CLASS_ENCODER(old_pg_t)

// placement group id
struct pg_t {
  uint64_t m_pool;
  uint32_t m_seed;
  int32_t m_preferred;

  pg_t() : m_pool(0), m_seed(0), m_preferred(-1) {}
  pg_t(ps_t seed, uint64_t pool, int pref=-1) {
    m_seed = seed;
    m_pool = pool;
    m_preferred = pref;
  }

  pg_t(const ceph_pg& cpg) {
    m_pool = cpg.pool;
    m_seed = cpg.ps;
    m_preferred = (int16_t)cpg.preferred;
  }
  old_pg_t get_old_pg() const {
    old_pg_t o;
    assert(m_pool < 0xffffffffull);
    o.v.pool = m_pool;
    o.v.ps = m_seed;
    o.v.preferred = (int16_t)m_preferred;
    return o;
  }
  pg_t(const old_pg_t& opg) {
    *this = opg.v;
  }

  ps_t ps() const {
    return m_seed;
  }
  uint64_t pool() const {
    return m_pool;
  }
  int32_t preferred() const {
    return m_preferred;
  }

  void set_ps(ps_t p) {
    m_seed = p;
  }
  void set_pool(uint64_t p) {
    m_pool = p;
  }
  void set_preferred(int32_t osd) {
    m_preferred = osd;
  }

  pg_t get_parent() const;
  pg_t get_ancestor(unsigned old_pg_num) const;

  int print(char *o, int maxlen) const;
  bool parse(const char *s);

  bool is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *pchildren) const;

  /**
   * Returns b such that for all object o:
   *   ~((~0)<<b) & o.hash) == 0 iff o is in the pg for *this
   */
  unsigned get_split_bits(unsigned pg_num) const;

  void encode(bufferlist& bl) const {
    uint8_t v = 1;
    ::encode(v, bl);
    ::encode(m_pool, bl);
    ::encode(m_seed, bl);
    ::encode(m_preferred, bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint8_t v;
    ::decode(v, bl);
    ::decode(m_pool, bl);
    ::decode(m_seed, bl);
    ::decode(m_preferred, bl);
  }
  void decode_old(bufferlist::iterator& bl) {
    old_pg_t opg;
    ::decode(opg, bl);
    *this = opg;
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_t*>& o);
};
WRITE_CLASS_ENCODER(pg_t)

inline bool operator<(const pg_t& l, const pg_t& r) {
  return l.pool() < r.pool() ||
    (l.pool() == r.pool() && (l.preferred() < r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() < r.ps()))));
}
inline bool operator<=(const pg_t& l, const pg_t& r) {
  return l.pool() < r.pool() ||
    (l.pool() == r.pool() && (l.preferred() < r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() <= r.ps()))));
}
inline bool operator==(const pg_t& l, const pg_t& r) {
  return l.pool() == r.pool() &&
    l.preferred() == r.preferred() &&
    l.ps() == r.ps();
}
inline bool operator!=(const pg_t& l, const pg_t& r) {
  return l.pool() != r.pool() ||
    l.preferred() != r.preferred() ||
    l.ps() != r.ps();
}
inline bool operator>(const pg_t& l, const pg_t& r) {
  return l.pool() > r.pool() ||
    (l.pool() == r.pool() && (l.preferred() > r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() > r.ps()))));
}
inline bool operator>=(const pg_t& l, const pg_t& r) {
  return l.pool() > r.pool() ||
    (l.pool() == r.pool() && (l.preferred() > r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() >= r.ps()))));
}

ostream& operator<<(ostream& out, const pg_t &pg);

CEPH_HASH_NAMESPACE_START
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static hash<uint32_t> H;
      return H((x.pool() & 0xffffffff) ^ (x.pool() >> 32) ^ x.ps() ^ x.preferred());
    }
  };
CEPH_HASH_NAMESPACE_END

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

  explicit coll_t(pg_t pgid)
    : str(pg_to_str(pgid))
  { }

  static coll_t make_temp_coll(pg_t pgid) {
    return coll_t(pg_to_tmp_str(pgid));
  }

  static coll_t make_removal_coll(uint64_t seq, pg_t pgid) {
    return coll_t(seq_to_removal_str(seq, pgid));
  }

  const std::string& to_str() const {
    return str;
  }

  const char* c_str() const {
    return str.c_str();
  }

  int operator<(const coll_t &rhs) const {
    return str < rhs.str;
  }

  bool is_pg_prefix(pg_t& pgid) const;
  bool is_pg(pg_t& pgid) const;
  bool is_temp(pg_t& pgid) const;
  bool is_removal(uint64_t *seq, pg_t *pgid) const;
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
  static std::string pg_to_str(pg_t p) {
    std::ostringstream oss;
    oss << p;
    return oss.str();
  }
  static std::string pg_to_tmp_str(pg_t p) {
    std::ostringstream oss;
    oss << p << "_TEMP";
    return oss.str();
  }
  static std::string seq_to_removal_str(uint64_t seq, pg_t pgid) {
    std::ostringstream oss;
    oss << "FORREMOVAL_" << seq << "_" << pgid;
    return oss.str();
  }

  std::string str;
};

WRITE_CLASS_ENCODER(coll_t)

inline ostream& operator<<(ostream& out, const coll_t& c) {
  out << c.to_str();
  return out;
}

CEPH_HASH_NAMESPACE_START
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
CEPH_HASH_NAMESPACE_END

inline ostream& operator<<(ostream& out, const ceph_object_layout &ol)
{
  out << pg_t(ol.ol_pgid);
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


/*
 * pg states
 */
#define PG_STATE_CREATING     (1<<0)  // creating
#define PG_STATE_ACTIVE       (1<<1)  // i am active.  (primary: replicas too)
#define PG_STATE_DOWN         (1<<4)  // a needed replica is down, PG offline
#define PG_STATE_REMAPPED     (1<<18) // pg is explicitly remapped to different OSDs than CRUSH

std::string pg_state_string(int state);

/*
 * pg_pool
 */
struct pg_pool_t {
  enum {
    TYPE_REPLICATED = 1,     // replication
  };
  static const char *get_type_name(int t) {
    switch (t) {
    case TYPE_REPLICATED: return "replicated";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    return get_type_name(type);
  }
  static const char* get_default_type() {
    return "replicated";
  }

  enum {
    FLAG_HASHPSPOOL = 1, // hash pg seed and pool together (instead of adding)
    FLAG_FULL       = 2 // pool is full
  };

  static const char *get_flag_name(int f) {
    switch (f) {
    case FLAG_HASHPSPOOL: return "hashpspool";
    case FLAG_FULL: return "full";
    default: return "???";
    }
  }
  static string get_flags_string(uint64_t f) {
    string s;
    for (unsigned n=0; f && n<64; ++n) {
      if (f & (1ull << n)) {
	if (s.length())
	  s += ",";
	s += get_flag_name(1ull << n);
      }
    }
    return s;
  }
  string get_flags_string() const {
    return get_flags_string(flags);
  }

  uint64_t flags; ///< FLAG_*
  uint8_t type; ///< TYPE_*
  uint8_t size, min_size; ///< number of osds in each pg
  uint8_t crush_ruleset; ///< crush placement rule set
  uint8_t object_hash; ///< hash mapping object name to ps
private:
  uint32_t pg_num, pgp_num;    ///< number of pgs


public:
  map<string,string> properties;  ///< OBSOLETE
  epoch_t last_change;      ///< most recent epoch changed
  uint64_t auid; ///< who owns the pg
  uint32_t crash_replay_interval; ///< seconds to allow clients to replay ACKed but unCOMMITted requests

  uint64_t quota_max_bytes; ///< maximum number of bytes for this pool
  uint64_t quota_max_objects; ///< maximum number of objects for this pool

  unsigned pg_num_mask, pgp_num_mask;


  uint32_t cache_target_dirty_ratio_micro; ///< cache: fraction of target to leave dirty
  uint32_t cache_target_full_ratio_micro;  ///< cache: fraction of target to fill before we evict in earnest

  uint32_t cache_min_flush_age;  ///< minimum age (seconds) before we can flush
  uint32_t cache_min_evict_age;  ///< minimum age (seconds) before we can evict

  pg_pool_t()
    : flags(0), type(0), size(0), min_size(0),
      crush_ruleset(0), object_hash(0),
      pg_num(0), pgp_num(0),
      last_change(0),
      auid(0),
      crash_replay_interval(0),
      quota_max_bytes(0), quota_max_objects(0),
      pg_num_mask(0), pgp_num_mask(0)
  { }

  void dump(Formatter *f) const;

  uint64_t get_flags() const { return flags; }

  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
  unsigned get_min_size() const { return min_size; }
  int get_crush_ruleset() const { return crush_ruleset; }
  int get_object_hash() const { return object_hash; }
  const char *get_object_hash_name() const {
    return ceph_str_hash_name(get_object_hash());
  }
  epoch_t get_last_change() const { return last_change; }
  uint64_t get_auid() const { return auid; }

  bool is_replicated()   const { return get_type() == TYPE_REPLICATED; }

  bool can_shift_osds() const {
    switch (get_type()) {
    case TYPE_REPLICATED:
      return true;
    default:
      assert(0 == "unhandled pool type");
    }
  }

  unsigned get_pg_num() const { return pg_num; }
  unsigned get_pgp_num() const { return pgp_num; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }

  // if pg_num is not a multiple of two, pgs are not equally sized.
  // return, for a given pg, the fraction (denominator) of the total
  // pool size that it represents.
  unsigned get_pg_num_divisor(pg_t pgid) const;

  void set_pg_num(int p) {
    pg_num = p;
    calc_pg_masks();
  }
  void set_pgp_num(int p) {
    pgp_num = p;
    calc_pg_masks();
  }

  void set_quota_max_bytes(uint64_t m) {
    quota_max_bytes = m;
  }
  uint64_t get_quota_max_bytes() {
    return quota_max_bytes;
  }

  void set_quota_max_objects(uint64_t m) {
    quota_max_objects = m;
  }
  uint64_t get_quota_max_objects() {
    return quota_max_objects;
  }

  static int calc_bits_of(int t);
  void calc_pg_masks();

  /// hash a object name+namespace key to a hash position
  uint32_t hash_key(const string& key, const string& ns) const;

  /// round a hash position down to a pg num
  uint32_t raw_hash_to_pg(uint32_t v) const;

  /*
   * map a raw pg (with full precision ps) into an actual pg, for storage
   */
  pg_t raw_pg_to_pg(pg_t pg) const;

  /*
   * map raw pg (full precision ps) into a placement seed.  include
   * pool id in that value so that different pools don't use the same
   * seeds.
   */
  ps_t raw_pg_to_pps(pg_t pg) const;

  /// choose a random hash position within a pg
  uint32_t get_random_pg_position(pg_t pgid, uint32_t seed) const;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);

  static void generate_test_instances(list<pg_pool_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_pool_t)

ostream& operator<<(ostream& out, const pg_pool_t& p);


/**
 * a summation of object stats
 *
 * This is just a container for object stats; we don't know what for.
 */
struct object_stat_sum_t {
  int64_t num_bytes;    // in bytes
  int64_t num_objects;
  int64_t num_rd, num_rd_kb;
  int64_t num_wr, num_wr_kb;
  int64_t num_objects_omap;

  object_stat_sum_t()
    : num_bytes(0), num_objects(0), num_rd(0), num_rd_kb(0),
      num_wr(0), num_wr_kb(0), num_objects_omap(0)
  {}

  void floor(int64_t f) {
#define FLOOR(x) if (x < f) x = f
    FLOOR(num_bytes);
    FLOOR(num_objects);
    FLOOR(num_rd);
    FLOOR(num_rd_kb);
    FLOOR(num_wr);
    FLOOR(num_wr_kb);
    FLOOR(num_objects_omap);
#undef FLOOR
  }

  void split(vector<object_stat_sum_t> &out) const {
#define SPLIT(PARAM)				\
    for (unsigned i = 0; i < out.size(); ++i) { \
      out[i].PARAM = PARAM / out.size();	\
      if (i < (PARAM % out.size())) {		\
      out[i].PARAM++;				\
      }						\
    }						\

    SPLIT(num_bytes);
    SPLIT(num_objects);
    SPLIT(num_rd);
    SPLIT(num_rd_kb);
    SPLIT(num_wr);
    SPLIT(num_wr_kb);
    SPLIT(num_objects_omap);
#undef SPLIT
  }

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

  void floor(int64_t f) {
    sum.floor(f);
    for (map<string,object_stat_sum_t>::iterator p = cat_sum.begin(); p != cat_sum.end(); ++p)
      p->second.floor(f);
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

/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  eversion_t version;
  version_t reported_seq;  // sequence number
  epoch_t reported_epoch;  // epoch of this report
  uint32_t state;
  utime_t last_fresh;   // last reported
  utime_t last_change;  // new state != previous state
  utime_t last_active;  // state & PG_STATE_ACTIVE
  utime_t last_unstale; // (state & PG_STATE_STALE) == 0

  epoch_t created;
  pg_t parent;
  uint32_t parent_split_bits;

  object_stat_collection_t stats;

  utime_t last_became_active;

  int osd;

  pg_stat_t()
    : reported_seq(0),
      reported_epoch(0),
      state(0),
      created(0),
      parent_split_bits(0),
      osd(-1)
  { }

  pair<epoch_t, version_t> get_version_pair() const {
    return make_pair(reported_epoch, reported_seq);
  }

  void floor(int64_t f) {
    stats.floor(f);
  }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
  }

  void dump(Formatter *f) const;
  void dump_brief(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pg_stat_t*>& o);
};
WRITE_CLASS_ENCODER(pg_stat_t)

/*
 * summation over an entire pool
 */
struct pool_stat_t {
  object_stat_collection_t stats;

  void floor(int64_t f) {
    stats.floor(f);
  }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
  }

  bool is_zero() const {
    return stats.is_zero();
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pool_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pool_stat_t)


// -----------------------------------------

// -----------------------------------------

/**
 * pg_history_t - information about recent pg peering/mapping history
 *
 * This is aggressively shared between OSDs to bound the amount of past
 * history they need to worry about.
 */
struct pg_history_t {
  epoch_t epoch_created;       // epoch in which PG was created
  epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)
  epoch_t last_epoch_split;    // as parent

  epoch_t same_up_since;       // same acting set since
  epoch_t same_interval_since;   // same acting AND up set since
  epoch_t same_primary_since;  // same primary at least back through this epoch.

  pg_history_t()
    : epoch_created(0),
      last_epoch_started(0), last_epoch_split(0),
      same_up_since(0), same_interval_since(0), same_primary_since(0) {}

  bool merge(const pg_history_t &other) {
    // Here, we only update the fields which cannot be calculated from the OSDmap.
    bool modified = false;
    if (epoch_created < other.epoch_created) {
      epoch_created = other.epoch_created;
      modified = true;
    }
    if (last_epoch_started < other.last_epoch_started) {
      last_epoch_started = other.last_epoch_started;
      modified = true;
    }
    if (last_epoch_split < other.last_epoch_split) {
      last_epoch_split = other.last_epoch_split;
      modified = true;
    }
    return modified;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_history_t*>& o);
};
WRITE_CLASS_ENCODER(pg_history_t)

inline ostream& operator<<(ostream& out, const pg_history_t& h) {
  return out << "ec=" << h.epoch_created
	     << " les/c " << h.last_epoch_started 
	     << " " << h.same_up_since << "/" << h.same_interval_since << "/" << h.same_primary_since;
}


/**
 * pg_info_t - summary of PG statistics.
 */
struct pg_info_t {
  pg_t pgid;
  eversion_t last_update;    // last object version applied to store.
  epoch_t last_epoch_started;// last epoch at which this pg started on this osd

  version_t last_user_version; // last user object version applied to store

  pg_stat_t stats;

  pg_history_t history;

  pg_info_t()
    : last_epoch_started(0), last_user_version(0)
  { }
  pg_info_t(pg_t p)
    : pgid(p),
      last_epoch_started(0), last_user_version(0)
  { }

  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_info_t*>& o);
};
WRITE_CLASS_ENCODER(pg_info_t)

inline ostream& operator<<(ostream& out, const pg_info_t& pgi) 
{
  out << pgi.pgid << "(";
  if (pgi.dne())
    out << " DNE";
  if (pgi.is_empty())
    out << " empty";
  else
    out << " v " << pgi.last_update;
  out << " local-les=" << pgi.last_epoch_started;
  out << " n=" << pgi.stats.stats.sum.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

struct pg_notify_t {
  epoch_t query_epoch;
  epoch_t epoch_sent;
  pg_info_t info;
  pg_notify_t() :
    query_epoch(0), epoch_sent(0) {}
  pg_notify_t(
    epoch_t query_epoch,
    epoch_t epoch_sent,
    const pg_info_t &info)
    : query_epoch(query_epoch),
      epoch_sent(epoch_sent),
      info(info) { }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_notify_t*> &o);
};
WRITE_CLASS_ENCODER(pg_notify_t)
ostream &operator<<(ostream &lhs, const pg_notify_t &notify);


/**
 * pg_query_t - used to ask a peer for information about a pg.
 */
struct pg_query_t {
  enum {
    INFO = 0
  };
  const char *get_type_name() const {
    switch (type) {
    case INFO: return "info";
    default: return "???";
    }
  }

  int32_t type;
  eversion_t since;
  pg_history_t history;
  epoch_t epoch_sent;

  pg_query_t() : type(-1), epoch_sent(0) { }
  pg_query_t(
    int t,
    const pg_history_t& h,
    epoch_t epoch_sent)
    : type(t),
      history(h),
      epoch_sent(epoch_sent) {
  }
  pg_query_t(
    int t,
    eversion_t s,
    const pg_history_t& h,
    epoch_t epoch_sent)
    : type(t), since(s), history(h),
      epoch_sent(epoch_sent) {
  }

  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_query_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_query_t)

inline ostream& operator<<(ostream& out, const pg_query_t& q) {
  out << "query(" << q.get_type_name() << " " << q.since;
  out << ")";
  return out;
}

class PGBackend;
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


/**
 * pg list objects response format
 *
 */
struct pg_ls_response_t {
  collection_list_handle_t handle;
  list<pair<object_t, string> > entries;

  void encode(bufferlist& bl) const {
    uint8_t v = 1;
    ::encode(v, bl);
    ::encode(handle, bl);
    ::encode(entries, bl);
  }
  void decode(bufferlist::iterator& bl) {
    uint8_t v;
    ::decode(v, bl);
    assert(v == 1);
    ::decode(handle, bl);
    ::decode(entries, bl);
  }
  void dump(Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (list<pair<object_t, string> >::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_stream("object") << p->first;
      f->dump_string("key", p->second);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<pg_ls_response_t*>& o) {
    o.push_back(new pg_ls_response_t);
    o.push_back(new pg_ls_response_t);
    o.back()->handle = hobject_t(object_t("hi"), "key", 2, -1, "");
    o.back()->entries.push_back(make_pair(object_t("one"), string()));
    o.back()->entries.push_back(make_pair(object_t("two"), string("twokey")));
  }
};

WRITE_CLASS_ENCODER(pg_ls_response_t)

/**
 * pg creation info
 */
struct pg_create_t {
  epoch_t created;   // epoch pg created
  pg_t parent;       // split from parent (if != pg_t())
  int32_t split_bits;

  pg_create_t()
    : created(0), split_bits(0) {}
  pg_create_t(unsigned c, pg_t p, int s)
    : created(c), parent(p), split_bits(s) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_create_t*>& o);
};
WRITE_CLASS_ENCODER(pg_create_t)

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

  object_locator_t oloc;   // object locator (pool etc)

  vector<pair<uint64_t,uint64_t> >  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : objectno(0), offset(0), length(0), truncate_size(0) {}
  ObjectExtent(object_t o, uint64_t ono, uint64_t off, uint64_t l, uint64_t ts) :
    oid(o), objectno(ono), offset(off), length(l), truncate_size(ts) { }
};

inline ostream& operator<<(ostream& out, const ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " (" << ex.objectno << ") in " << ex.oloc
             << " " << ex.offset << "~" << ex.length
	     << " -> " << ex.buffer_extents
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
  watch_info_t(uint64_t c, uint32_t t, const entity_addr_t& a) : cookie(c), timeout_seconds(t), addr(a) {}

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
  version_t user_version;
  osd_reqid_t last_reqid;

  uint64_t size;
  utime_t mtime;

  // note: these are currently encoded into a total 16 bits; see
  // encode()/decode() for the weirdness.
  typedef enum {
    FLAG_OMAP     = 1 << 3  // has (or may have) some/any omap data
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

  map<pair<uint64_t, entity_name_t>, watch_info_t> watchers;

  void copy_user_bits(const object_info_t& other);

  static ps_t legacy_object_locator_to_ps(const object_t &oid, 
					  const object_locator_t &loc);

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
      truncate_seq(0), truncate_size(0)
  {}

  object_info_t(const hobject_t& s)
    : soid(s),
      user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0) {}

  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)

struct ObjectState {
  object_info_t oi;
  bool exists;         ///< the stored object exists (i.e., we will remember the object_info_t)

  ObjectState() : exists(false) {}

  ObjectState(const object_info_t &oi_, bool exists_)
    : oi(oi_), exists(exists_) {}
};


/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */

struct ObjectContext;

typedef ceph::shared_ptr<ObjectContext> ObjectContextRef;

struct ObjectContext {
  ObjectState obs;

  Context *destructor_callback;

private:
  Mutex lock;
public:
  Cond cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

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

    State state;                 ///< rw state
    uint64_t count;              ///< number of readers or writers
    list<OpRequestRef> waiters;  ///< ops waiting on state change

    RWState()
      : state(RWNONE), count(0)
    {}
    bool get_read(OpRequestRef op) {
      if (get_read_lock()) {
	return true;
      } // else
      waiters.push_back(op);
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

    bool get_write(OpRequestRef op) {
      if (get_write_lock()) {
	return true;
      } // else
      if (op)
	waiters.push_back(op);
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
    void dec(list<OpRequestRef> *requeue) {
      assert(count > 0);
      assert(requeue);
      count--;
      if (count == 0) {
	state = RWNONE;
	requeue->splice(requeue->end(), waiters);
      }
    }
    void put_read(list<OpRequestRef> *requeue) {
      assert(state == RWREAD);
      dec(requeue);
    }
    void put_write(list<OpRequestRef> *requeue) {
      assert(state == RWWRITE);
      dec(requeue);
    }
    bool empty() const { return state == RWNONE; }
  } rwstate;

  bool get_read(OpRequestRef op) {
    return rwstate.get_read(op);
  }
  bool get_write(OpRequestRef op) {
    return rwstate.get_write(op);
  }
  void put_read(list<OpRequestRef> *to_wake) {
    rwstate.put_read(to_wake);
  }
  void put_write(list<OpRequestRef> *to_wake,
		 bool *requeue_recovery) {
    rwstate.put_write(to_wake);
  }

  ObjectContext()
    : destructor_callback(0),
      lock("ReplicatedPG::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0) {}

  ~ObjectContext() {
    assert(rwstate.empty());
    if (destructor_callback)
      destructor_callback->complete(0);
  }

  // do simple synchronous mutual exclusion, for now.  now waitqueues or anything fancy.
  void ondisk_write_lock() {
    lock.Lock();
    writers_waiting++;
    while (readers_waiting || readers)
      cond.Wait(lock);
    writers_waiting--;
    unstable_writes++;
    lock.Unlock();
  }
  void ondisk_write_unlock() {
    lock.Lock();
    assert(unstable_writes > 0);
    unstable_writes--;
    if (!unstable_writes && readers_waiting)
      cond.Signal();
    lock.Unlock();
  }
  void ondisk_read_lock() {
    lock.Lock();
    readers_waiting++;
    while (unstable_writes)
      cond.Wait(lock);
    readers_waiting--;
    readers++;
    lock.Unlock();
  }
  void ondisk_read_unlock() {
    lock.Lock();
    assert(readers > 0);
    readers--;
    if (!readers && writers_waiting)
      cond.Signal();
    lock.Unlock();
  }

  // attr cache
  map<string, bufferlist> attr_cache;

  void fill_in_setattrs(const set<string> &changing, ObjectModDesc *mod) {
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
};

inline ostream& operator<<(ostream& out, const ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline ostream& operator<<(ostream& out, const ObjectContext::RWState& rw)
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
  object_t oid;

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
   * buffer, including the object_t oid.
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
  vector< pair<uint64_t,uint64_t> > overlap;
  uint64_t size;

  clone_info() : size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(overlap, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(overlap, bl);
    ::decode(size, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
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
    o.back()->overlap.push_back(pair<uint64_t,uint64_t>(0,4096));
    o.back()->overlap.push_back(pair<uint64_t,uint64_t>(8192,4096));
    o.back()->size = 16384;
    o.push_back(new clone_info);
    o.back()->size = 32768;
  }
};
WRITE_CLASS_ENCODER(clone_info)

#endif
