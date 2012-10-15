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

#ifndef CEPH_PG_TYPES_H
#define CEPH_PG_TYPES_H


// object namespaces
#define CEPH_METADATA_NS       1
#define CEPH_DATA_NS           2
#define CEPH_CAS_NS            3
#define CEPH_OSDMETADATA_NS 0xff

// poolsets
enum {
  CEPH_DATA_RULE,
  CEPH_METADATA_RULE,
  CEPH_RBD_RULE,
};

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
  pg_t(ps_t seed, uint64_t pool, int pref) {
    m_seed = seed;
    m_pool = pool;
    m_preferred = pref;
  }

  pg_t(const ceph_pg& cpg) {
    m_pool = cpg.pool;
    m_seed = cpg.ps;
    m_preferred = (__s16)cpg.preferred;
  }
  old_pg_t get_old_pg() const {
    old_pg_t o;
    assert(m_pool < 0xffffffffull);
    o.v.pool = m_pool;
    o.v.ps = m_seed;
    o.v.preferred = (__s16)m_preferred;
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

  int print(char *o, int maxlen) const;
  bool parse(const char *s);

  bool is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *pchildren) const;

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(m_pool, bl);
    ::encode(m_seed, bl);
    ::encode(m_preferred, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
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

// -----


namespace __gnu_cxx {
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static hash<uint32_t> H;
      return H((x.pool() & 0xffffffff) ^ (x.pool() >> 32) ^ x.ps() ^ x.preferred());
    }
  };
}


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

  explicit coll_t(pg_t pgid, snapid_t snap = CEPH_NOSNAP)
    : str(pg_and_snap_to_str(pgid, snap))
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

  bool is_pg(pg_t& pgid, snapid_t& snap) const;
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
  static std::string pg_and_snap_to_str(pg_t p, snapid_t s) {
    std::ostringstream oss;
    oss << p << "_" << s;
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


/*
 * pg states
 */
#define PG_STATE_CREATING     (1<<0)  // creating
#define PG_STATE_ACTIVE       (1<<1)  // i am active.  (primary: replicas too)
#define PG_STATE_CLEAN        (1<<2)  // peers are complete, clean of stray replicas.
#define PG_STATE_DOWN         (1<<4)  // a needed replica is down, PG offline
#define PG_STATE_REPLAY       (1<<5)  // crashed, waiting for replay
#define PG_STATE_STRAY        (1<<6)  // i must notify the primary i exist.
#define PG_STATE_SPLITTING    (1<<7)  // i am splitting
#define PG_STATE_SCRUBBING    (1<<8)  // scrubbing
#define PG_STATE_SCRUBQ       (1<<9)  // queued for scrub
#define PG_STATE_DEGRADED     (1<<10) // pg membership not complete
#define PG_STATE_INCONSISTENT (1<<11) // pg replicas are inconsistent (but shouldn't be)
#define PG_STATE_PEERING      (1<<12) // pg is (re)peering
#define PG_STATE_REPAIR       (1<<13) // pg should repair on next scrub
#define PG_STATE_RECOVERING   (1<<14) // pg is recovering/migrating objects
#define PG_STATE_BACKFILL     (1<<15) // [active] backfilling pg content
#define PG_STATE_INCOMPLETE   (1<<16) // incomplete content, peering failed.
#define PG_STATE_STALE        (1<<17) // our state for this pg is stale, unknown.
#define PG_STATE_REMAPPED     (1<<18) // pg is explicitly remapped to different OSDs than CRUSH

std::string pg_state_string(int state);


/*
 * pool_snap_info_t
 *
 * attributes for a single pool snapshot.  
 */
struct pool_snap_info_t {
  snapid_t snapid;
  utime_t stamp;
  string name;

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<pool_snap_info_t*>& o);
};
WRITE_CLASS_ENCODER(pool_snap_info_t)

inline ostream& operator<<(ostream& out, const pool_snap_info_t& si) {
  return out << si.snapid << '(' << si.name << ' ' << si.stamp << ')';
}


/*
 * pg_pool
 */
struct pg_pool_t {
  enum {
    TYPE_REP = 1,     // replication
    TYPE_RAID4 = 2,   // raid4 (never implemented)
  };

  static const char *get_type_name(int t) {
    switch (t) {
    case TYPE_REP: return "rep";
    case TYPE_RAID4: return "raid4";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    return get_type_name(type);
  }

  uint64_t flags;           /// FLAG_* 
  __u8 type;                /// TYPE_*
  __u8 size;                /// number of osds in each pg
  __u8 crush_ruleset;       /// crush placement rule set
  __u8 object_hash;         /// hash mapping object name to ps
private:
  __u32 pg_num, pgp_num;    /// number of pgs
public:
  epoch_t last_change;      /// most recent epoch changed, exclusing snapshot changes
  snapid_t snap_seq;        /// seq for per-pool snapshot
  epoch_t snap_epoch;       /// osdmap epoch of last snap
  uint64_t auid;            /// who owns the pg
  __u32 crash_replay_interval; /// seconds to allow clients to replay ACKed but unCOMMITted requests

  /*
   * Pool snaps (global to this pool).  These define a SnapContext for
   * the pool, unless the client manually specifies an alternate
   * context.
   */
  map<snapid_t, pool_snap_info_t> snaps;
  /*
   * Alternatively, if we are definining non-pool snaps (e.g. via the
   * Ceph MDS), we must track @removed_snaps (since @snaps is not
   * used).  Snaps and removed_snaps are to be used exclusive of each
   * other!
   */
  interval_set<snapid_t> removed_snaps;

  int pg_num_mask, pgp_num_mask;

  pg_pool_t()
    : flags(0), type(0), size(0), crush_ruleset(0), object_hash(0),
      pg_num(0), pgp_num(0),
      last_change(0),
      snap_seq(0), snap_epoch(0),
      auid(0),
      crash_replay_interval(0),
      pg_num_mask(0), pgp_num_mask(0) { }

  void dump(Formatter *f) const;

  uint64_t get_flags() const { return flags; }
  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
  int get_crush_ruleset() const { return crush_ruleset; }
  int get_object_hash() const { return object_hash; }
  const char *get_object_hash_name() const {
    return ceph_str_hash_name(get_object_hash());
  }
  epoch_t get_last_change() const { return last_change; }
  epoch_t get_snap_epoch() const { return snap_epoch; }
  snapid_t get_snap_seq() const { return snap_seq; }
  uint64_t get_auid() const { return auid; }
  unsigned get_crash_replay_interval() const { return crash_replay_interval; }

  void set_snap_seq(snapid_t s) { snap_seq = s; }
  void set_snap_epoch(epoch_t e) { snap_epoch = e; }

  bool is_rep()   const { return get_type() == TYPE_REP; }
  bool is_raid4() const { return get_type() == TYPE_RAID4; }

  unsigned get_pg_num() const { return pg_num; }
  unsigned get_pgp_num() const { return pgp_num; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }

  void set_pg_num(int p) {
    pg_num = p;
    calc_pg_masks();
  }
  void set_pgp_num(int p) {
    pgp_num = p;
    calc_pg_masks();
  }

  static int calc_bits_of(int t);
  void calc_pg_masks();

  /*
   * we have two snap modes:
   *  - pool global snaps
   *    - snap existence/non-existence defined by snaps[] and snap_seq
   *  - user managed snaps
   *    - removal governed by removed_snaps
   *
   * we know which mode we're using based on whether removed_snaps is empty.
   * If nothing has been created, both functions report false.
   */
  bool is_pool_snaps_mode() const;
  bool is_unmanaged_snaps_mode() const;
  bool is_removed_snap(snapid_t s) const;

  /*
   * build set of known-removed sets from either pool snaps or
   * explicit removed_snaps set.
   */
  void build_removed_snaps(interval_set<snapid_t>& rs) const;
  snapid_t snap_exists(const char *s) const;
  void add_snap(const char *n, utime_t stamp);
  void add_unmanaged_snap(uint64_t& snapid);
  void remove_snap(snapid_t s);
  void remove_unmanaged_snap(snapid_t s);

  SnapContext get_snap_context() const;

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

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);

  static void generate_test_instances(list<pg_pool_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_pool_t)

ostream& operator<<(ostream& out, const pg_pool_t& p);


/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  eversion_t version;
  eversion_t reported;
  __u32 state;
  utime_t last_fresh;   // last reported
  utime_t last_change;  // new state != previous state
  utime_t last_active;  // state & PG_STATE_ACTIVE
  utime_t last_clean;   // state & PG_STATE_CLEAN
  utime_t last_unstale; // (state & PG_STATE_STALE) == 0

  eversion_t log_start;         // (log_start,version]
  eversion_t ondisk_log_start;  // there may be more on disk

  epoch_t created;
  epoch_t last_epoch_clean;
  pg_t parent;
  __u32 parent_split_bits;

  eversion_t last_scrub;
  utime_t last_scrub_stamp;

  object_stat_collection_t stats;

  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size

  vector<int> up, acting;
  epoch_t mapping_epoch;

  pg_stat_t()
    : state(0),
      created(0), last_epoch_clean(0),
      parent_split_bits(0), 
      log_size(0), ondisk_log_size(0),
      mapping_epoch(0)
  { }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
  }

  void dump(Formatter *f) const;
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
  uint64_t log_size;
  uint64_t ondisk_log_size;    // >= active_log_size

  pool_stat_t() : log_size(0), ondisk_log_size(0)
  { }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
  }

  bool is_zero() const {
    return (stats.is_zero() &&
	    log_size == 0 &&
	    ondisk_log_size == 0);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pool_stat_t*>& o);
};
WRITE_CLASS_ENCODER(pool_stat_t)


/**
 * pg_history_t - information about recent pg peering/mapping history
 *
 * This is aggressively shared between OSDs to bound the amount of past
 * history they need to worry about.
 */
struct pg_history_t {
  epoch_t epoch_created;       // epoch in which PG was created
  epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)
  epoch_t last_epoch_clean;    // lower bound on last epoch the PG was completely clean.
  epoch_t last_epoch_split;    // as parent
  
  epoch_t same_up_since;       // same acting set since
  epoch_t same_interval_since;   // same acting AND up set since
  epoch_t same_primary_since;  // same primary at least back through this epoch.

  eversion_t last_scrub;
  utime_t last_scrub_stamp;

  pg_history_t()
    : epoch_created(0),
      last_epoch_started(0), last_epoch_clean(0), last_epoch_split(0),
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
    if (last_epoch_clean < other.last_epoch_clean) {
      last_epoch_clean = other.last_epoch_clean;
      modified = true;
    }
    if (last_epoch_split < other.last_epoch_started) {
      last_epoch_split = other.last_epoch_started; 
      modified = true;
    }
    if (other.last_scrub > last_scrub) {
      last_scrub = other.last_scrub;
      modified = true;
    }
    if (other.last_scrub_stamp > last_scrub_stamp) {
      last_scrub_stamp = other.last_scrub_stamp;
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
	     << " les/c " << h.last_epoch_started << "/" << h.last_epoch_clean
	     << " " << h.same_up_since << "/" << h.same_interval_since << "/" << h.same_primary_since;
}


/**
 * pg_info_t - summary of PG statistics.
 *
 * some notes: 
 *  - last_complete implies we have all objects that existed as of that
 *    stamp, OR a newer object, OR have already applied a later delete.
 *  - if last_complete >= log.bottom, then we know pg contents thru log.head.
 *    otherwise, we have no idea what the pg is supposed to contain.
 */
struct pg_info_t {
  pg_t pgid;
  eversion_t last_update;    // last object version applied to store.
  eversion_t last_complete;  // last version pg was complete through.
  
  eversion_t log_tail;     // oldest log entry.

  hobject_t last_backfill;   // objects >= this and < last_complete may be missing

  interval_set<snapid_t> purged_snaps;

  pg_stat_t stats;

  pg_history_t history;

  pg_info_t()
    : last_backfill(hobject_t::get_max())
  { }
  pg_info_t(pg_t p)
    : pgid(p),
      last_backfill(hobject_t::get_max())
  { }
  
  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  bool is_incomplete() const { return last_backfill != hobject_t::get_max(); }

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
  else {
    out << " v " << pgi.last_update;
    if (pgi.last_complete != pgi.last_update)
      out << " lc " << pgi.last_complete;
    out << " (" << pgi.log_tail << "," << pgi.last_update << "]";
    if (pgi.is_incomplete())
      out << " lb " << pgi.last_backfill;
  }
  //out << " c " << pgi.epoch_created;
  out << " n=" << pgi.stats.stats.sum.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

struct pg_notify_t {
  epoch_t query_epoch;
  epoch_t epoch_sent;
  pg_info_t info;
  pg_notify_t() : query_epoch(0), epoch_sent(0) {}
  pg_notify_t(epoch_t query_epoch,
	      epoch_t epoch_sent,
	      const pg_info_t &info)
    : query_epoch(query_epoch),
      epoch_sent(epoch_sent),
      info(info) {}
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_notify_t*> &o);
};
WRITE_CLASS_ENCODER(pg_notify_t)
ostream &operator<<(ostream &lhs, const pg_notify_t notify);


/**
 * pg_interval_t - information about a past interval
 */
class OSDMap;
struct pg_interval_t {
  vector<int> up, acting;
  epoch_t first, last;
  bool maybe_went_rw;

  pg_interval_t() : first(0), last(0), maybe_went_rw(false) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_interval_t*>& o);

  /**
   * Integrates a new map into *past_intervals, returns true
   * if an interval was closed out.
   */
  static bool check_new_interval(
    const vector<int> &old_acting,              ///< [in] acting as of lastmap
    const vector<int> &new_acting,              ///< [in] acting as of osdmap
    const vector<int> &old_up,                  ///< [in] up as of lastmap
    const vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    std::tr1::shared_ptr<const OSDMap> osdmap,  ///< [in] current map
    std::tr1::shared_ptr<const OSDMap> lastmap, ///< [in] last map
    map<epoch_t, pg_interval_t> *past_intervals,///< [out] intervals
    ostream *out = 0                            ///< [out] debug ostream
    );
};
WRITE_CLASS_ENCODER(pg_interval_t)

ostream& operator<<(ostream& out, const pg_interval_t& i);

typedef map<epoch_t, pg_interval_t> pg_interval_map_t;


/** 
 * pg_query_t - used to ask a peer for information about a pg.
 *
 * note: if version=0, type=LOG, then we just provide our full log.
 */
struct pg_query_t {
  enum {
    INFO = 0,
    LOG = 1,
    MISSING = 4,
    FULLLOG = 5,
  };
  const char *get_type_name() const {
    switch (type) {
    case INFO: return "info";
    case LOG: return "log";
    case MISSING: return "missing";
    case FULLLOG: return "fulllog";
    default: return "???";
    }
  }

  __s32 type;
  eversion_t since;
  pg_history_t history;
  epoch_t epoch_sent;

  pg_query_t() : type(-1) {}
  pg_query_t(int t, const pg_history_t& h,
	     epoch_t epoch_sent)
    : type(t), history(h),
      epoch_sent(epoch_sent) {
    assert(t != LOG);
  }
  pg_query_t(int t, eversion_t s, const pg_history_t& h,
	     epoch_t epoch_sent)
    : type(t), since(s), history(h),
      epoch_sent(epoch_sent) {
    assert(t == LOG);
  }
  
  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_query_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_query_t)

inline ostream& operator<<(ostream& out, const pg_query_t& q) {
  out << "query(" << q.get_type_name() << " " << q.since;
  if (q.type == pg_query_t::LOG)
    out << " " << q.history;
  out << ")";
  return out;
}


/**
 * pg_log_entry_t - single entry/event in pg log
 *
 */
struct pg_log_entry_t {
  enum {
    MODIFY = 1,
    CLONE = 2,
    DELETE = 3,
    BACKLOG = 4,  // event invented by generate_backlog [deprecated]
    LOST_REVERT = 5, // lost new version, revert to an older version.
    LOST_DELETE = 6, // lost new version, revert to no object (deleted).
    LOST_MARK = 7,   // lost new version, now EIO
  };
  static const char *get_op_name(int op) {
    switch (op) {
    case MODIFY:
      return "modify  ";
    case CLONE:
      return "clone   ";
    case DELETE:
      return "delete  ";
    case BACKLOG:
      return "backlog ";
    case LOST_REVERT:
      return "l_revert";
    case LOST_DELETE:
      return "l_delete";
    case LOST_MARK:
      return "l_mark  ";
    default:
      return "unknown ";
    }
  }
  const char *get_op_name() const {
    return get_op_name(op);
  }

  __s32      op;
  hobject_t  soid;
  eversion_t version, prior_version;
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  utime_t     mtime;  // this is the _user_ mtime, mind you
  bufferlist snaps;   // only for clone entries
  bool invalid_hash; // only when decoding sobject_t based entries
  bool invalid_pool; // only when decoding pool-less hobject based entries

  uint64_t offset;   // [soft state] my offset on disk
      
  pg_log_entry_t()
    : op(0), invalid_hash(false), offset(0) {}
  pg_log_entry_t(int _op, const hobject_t& _soid, 
		 const eversion_t& v, const eversion_t& pv,
		 const osd_reqid_t& rid, const utime_t& mt)
    : op(_op), soid(_soid), version(v),
      prior_version(pv),
      reqid(rid), mtime(mt), invalid_hash(false), invalid_pool(false),
      offset(0) {}
      
  bool is_clone() const { return op == CLONE; }
  bool is_modify() const { return op == MODIFY; }
  bool is_backlog() const { return op == BACKLOG; }
  bool is_lost_revert() const { return op == LOST_REVERT; }
  bool is_lost_delete() const { return op == LOST_DELETE; }
  bool is_lost_mark() const { return op == LOST_MARK; }

  bool is_update() const {
    return is_clone() || is_modify() || is_backlog() || is_lost_revert() || is_lost_mark();
  }
  bool is_delete() const {
    return op == DELETE || op == LOST_DELETE;
  }
      
  bool reqid_is_indexed() const {
    return reqid != osd_reqid_t() && (op == MODIFY || op == DELETE);
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_log_entry_t*>& o);

};
WRITE_CLASS_ENCODER(pg_log_entry_t)

ostream& operator<<(ostream& out, const pg_log_entry_t& e);



/**
 * pg_log_t - incremental log of recent pg changes.
 *
 *  serves as a recovery queue for recent changes.
 */
struct pg_log_t {
  /*
   *   head - newest entry (update|delete)
   *   tail - entry previous to oldest (update|delete) for which we have
   *          complete negative information.  
   * i.e. we can infer pg contents for any store whose last_update >= tail.
   */
  eversion_t head;    // newest entry
  eversion_t tail;    // version prior to oldest

  list<pg_log_entry_t> log;  // the actual log.
  
  pg_log_t() {}

  void clear() {
    eversion_t z;
    head = tail = z;
    log.clear();
  }

  bool empty() const {
    return log.empty();
  }

  bool null() const {
    return head.version == 0 && head.epoch == 0;
  }

  size_t approx_size() const {
    return head.version - tail.version;
  }

  list<pg_log_entry_t>::iterator find_entry(eversion_t v) {
    int fromhead = head.version - v.version;
    int fromtail = v.version - tail.version;
    list<pg_log_entry_t>::iterator p;
    if (fromhead < fromtail) {
      p = log.end();
      p--;
      while (p->version > v)
	p--;
      return p;
    } else {
      p = log.begin();
      while (p->version < v)
	p++;
      return p;
    }      
  }

  /**
   * copy entries from the tail of another pg_log_t
   *
   * @param other pg_log_t to copy from
   * @param from copy entries after this version
   */
  void copy_after(const pg_log_t &other, eversion_t from);

  /**
   * copy a range of entries from another pg_log_t
   *
   * @param other pg_log_t to copy from
   * @param from copy entries after this version
   * @parem to up to and including this version
   */
  void copy_range(const pg_log_t &other, eversion_t from, eversion_t to);

  /**
   * copy up to N entries
   *
   * @param o source log
   * @param max max number of entreis to copy
   */
  void copy_up_to(const pg_log_t &other, int max);

  ostream& print(ostream& out) const;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_log_t*>& o);
};
WRITE_CLASS_ENCODER(pg_log_t)

inline ostream& operator<<(ostream& out, const pg_log_t& log) 
{
  out << "log(" << log.tail << "," << log.head << "]";
  return out;
}


/**
 * pg_missing_t - summary of missing objects.
 *
 *  kept in memory, as a supplement to pg_log_t
 *  also used to pass missing info in messages.
 */
struct pg_missing_t {
  struct item {
    eversion_t need, have;
    item() {}
    item(eversion_t n) : need(n) {}  // have no old version
    item(eversion_t n, eversion_t h) : need(n), have(h) {}

    void encode(bufferlist& bl) const {
      ::encode(need, bl);
      ::encode(have, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(need, bl);
      ::decode(have, bl);
    }
    void dump(Formatter *f) const {
      f->dump_stream("need") << need;
      f->dump_stream("have") << have;
    }
    static void generate_test_instances(list<item*>& o) {
      o.push_back(new item);
      o.push_back(new item);
      o.back()->need = eversion_t(1, 2);
      o.back()->have = eversion_t(1, 1);
    }
  }; 
  WRITE_CLASS_ENCODER(item)

  map<hobject_t, item> missing;         // oid -> (need v, have v)
  map<version_t, hobject_t> rmissing;  // v -> oid

  unsigned int num_missing() const;
  bool have_missing() const;
  void swap(pg_missing_t& o);
  bool is_missing(const hobject_t& oid) const;
  bool is_missing(const hobject_t& oid, eversion_t v) const;
  eversion_t have_old(const hobject_t& oid) const;
  void add_next_event(const pg_log_entry_t& e);
  void revise_need(hobject_t oid, eversion_t need);
  void revise_have(hobject_t oid, eversion_t have);
  void add(const hobject_t& oid, eversion_t need, eversion_t have);
  void rm(const hobject_t& oid, eversion_t v);
  void rm(const std::map<hobject_t, pg_missing_t::item>::iterator &m);
  void got(const hobject_t& oid, eversion_t v);
  void got(const std::map<hobject_t, pg_missing_t::item>::iterator &m);

  void clear() {
    missing.clear();
    rmissing.clear();
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_missing_t*>& o);
};
WRITE_CLASS_ENCODER(pg_missing_t::item)
WRITE_CLASS_ENCODER(pg_missing_t)

ostream& operator<<(ostream& out, const pg_missing_t::item& i);
ostream& operator<<(ostream& out, const pg_missing_t& missing);

/**
 * pg list objects response format
 *
 */
struct pg_ls_response_t {
  collection_list_handle_t handle; 
  list<pair<object_t, string> > entries;

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(handle, bl);
    ::encode(entries, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
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
    o.back()->handle = hobject_t(object_t("hi"), "key", 1, 2, -1);
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
  __s32 split_bits;

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


inline ostream& operator<<(ostream& out, const ceph_object_layout &ol)
{
  out << pg_t(ol.ol_pgid);
  int su = ol.ol_stripe_unit;
  if (su)
    out << ".su=" << su;
  return out;
}


#endif // CEPH_PG_TYPES_H
