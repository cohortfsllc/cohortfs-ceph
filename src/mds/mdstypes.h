// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MDSTYPES_H
#define CEPH_MDSTYPES_H

#include <cassert>
#include <math.h>
#include <ostream>
#include <set>
#include <map>
#include "common/config.h"
#include "common/DecayCounter.h"
#include "include/Context.h"

#include "include/frag.h"
#include "include/xlist.h"
#include "include/interval_set.h"
#include "inode_backtrace.h"
#include <boost/pool/pool.hpp>

#define CEPH_FS_ONDISK_MAGIC "ceph fs volume v011"


#define MDS_REF_SET	 // define me for improved debug output, sanity checking
//#define MDS_AUTHPIN_SET  // define me for debugging auth pin leaks
//#define MDS_VERIFY_FRAGSTAT	 // do do (slow) sanity checking on frags

#define MDS_PORT_CACHE	 0x200
#define MDS_PORT_LOCKER	 0x300
#define MDS_PORT_MIGRATOR 0x400

// FIXME: this should not be hardcoded
#define MDS_DATA_POOL		0
#define MDS_METADATA_POOL	1

#define MAX_MDS			  0x100
#define NUM_STRAY		  10

#define MDS_INO_ROOT		  1

// No longer created but recognised in existing filesystems
// so that we don't try to fragment it.
#define MDS_INO_CEPH		  2

#define MDS_INO_MDSDIR_OFFSET	  (1*MAX_MDS)
#define MDS_INO_LOG_OFFSET	  (2*MAX_MDS)
#define MDS_INO_STRAY_OFFSET	  (6*MAX_MDS)

#define MDS_INO_SYSTEM_BASE	  ((6*MAX_MDS) + (MAX_MDS * NUM_STRAY))

#define MDS_INO_STRAY(x,i)  (MDS_INO_STRAY_OFFSET+((((unsigned)(x))*NUM_STRAY)+((unsigned)(i))))
#define MDS_INO_MDSDIR(x) (MDS_INO_MDSDIR_OFFSET+((unsigned)x))

#define MDS_INO_IS_STRAY(i)  ((i) >= MDS_INO_STRAY_OFFSET  && (i) < (MDS_INO_STRAY_OFFSET+(MAX_MDS*NUM_STRAY)))
#define MDS_INO_IS_MDSDIR(i) ((i) >= MDS_INO_MDSDIR_OFFSET && (i) < (MDS_INO_MDSDIR_OFFSET+MAX_MDS))
#define MDS_INO_IS_BASE(i)   (MDS_INO_ROOT == (i) || MDS_INO_IS_MDSDIR(i))
#define MDS_INO_STRAY_OWNER(i) (signed (((unsigned (i)) - MDS_INO_STRAY_OFFSET) / NUM_STRAY))
#define MDS_INO_STRAY_INDEX(i) (((unsigned (i)) - MDS_INO_STRAY_OFFSET) % NUM_STRAY)

#define MDS_TRAVERSE_FORWARD	   1
#define MDS_TRAVERSE_DISCOVER	   2	// skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3	// succeeds on (foreign?) null, xlocked dentries.


extern long g_num_ino, g_num_dir, g_num_dn, g_num_cap;
extern long g_num_inoa, g_num_dira, g_num_dna, g_num_capa;
extern long g_num_inos, g_num_dirs, g_num_dns, g_num_caps;


// CAPS

inline string gcap_string(int cap)
{
  string s;
  if (cap & CEPH_CAP_GSHARED) s += "s";
  if (cap & CEPH_CAP_GEXCL) s += "x";
  if (cap & CEPH_CAP_GCACHE) s += "c";
  if (cap & CEPH_CAP_GRD) s += "r";
  if (cap & CEPH_CAP_GWR) s += "w";
  if (cap & CEPH_CAP_GBUFFER) s += "b";
  if (cap & CEPH_CAP_GWREXTEND) s += "a";
  if (cap & CEPH_CAP_GLAZYIO) s += "l";
  return s;
}
inline string ccap_string(int cap)
{
  string s;
  if (cap & CEPH_CAP_PIN) s += "p";

  int a = (cap >> CEPH_CAP_SAUTH) & 3;
  if (a) s += 'A' + gcap_string(a);

  a = (cap >> CEPH_CAP_SLINK) & 3;
  if (a) s += 'L' + gcap_string(a);

  a = (cap >> CEPH_CAP_SXATTR) & 3;
  if (a) s += 'X' + gcap_string(a);

  a = cap >> CEPH_CAP_SFILE;
  if (a) s += 'F' + gcap_string(a);

  if (s.length() == 0)
    s = "-";
  return s;
}


struct scatter_info_t {
  version_t version;

  scatter_info_t() : version(0) {}
};

struct frag_info_t : public scatter_info_t {
  // this frag
  ceph::real_time mtime;
  int64_t nfiles;	 // files
  int64_t nsubdirs;	 // subdirs

  frag_info_t() : nfiles(0), nsubdirs(0) {}

  int64_t size() const { return nfiles + nsubdirs; }

  void zero() {
    *this = frag_info_t();
  }

  // *this += cur - acc;
  void add_delta(const frag_info_t &cur, frag_info_t &acc, bool& touched_mtime) {
    if (!(cur.mtime == acc.mtime)) {
      mtime = cur.mtime;
      touched_mtime = true;
    }
    nfiles += cur.nfiles - acc.nfiles;
    nsubdirs += cur.nsubdirs - acc.nsubdirs;
  }

  void add(const frag_info_t& other) {
    if (other.mtime > mtime)
      mtime = other.mtime;
    nfiles += other.nfiles;
    nsubdirs += other.nsubdirs;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<frag_info_t*>& ls);
};
WRITE_CLASS_ENCODER(frag_info_t)

inline bool operator==(const frag_info_t &l, const frag_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}

ostream& operator<<(ostream &out, const frag_info_t &f);


struct nest_info_t : public scatter_info_t {
  // this frag + children
  ceph::real_time rctime;
  int64_t rbytes;
  int64_t rfiles;
  int64_t rsubdirs;
  int64_t rsize() const { return rfiles + rsubdirs; }

  int64_t ranchors;  // for dirstat, includes inode's anchored flag.

  nest_info_t() : rbytes(0), rfiles(0), rsubdirs(0),
		  ranchors(0) {}

  void zero() {
    *this = nest_info_t();
  }

  void sub(const nest_info_t &other) {
    add(other, -1);
  }
  void add(const nest_info_t &other, int fac=1) {
    if (other.rctime > rctime)
      rctime = other.rctime;
    rbytes += fac*other.rbytes;
    rfiles += fac*other.rfiles;
    rsubdirs += fac*other.rsubdirs;
    ranchors += fac*other.ranchors;
  }

  // *this += cur - acc;
  void add_delta(const nest_info_t &cur, nest_info_t &acc) {
    if (cur.rctime > rctime)
      rctime = cur.rctime;
    rbytes += cur.rbytes - acc.rbytes;
    rfiles += cur.rfiles - acc.rfiles;
    rsubdirs += cur.rsubdirs - acc.rsubdirs;
    ranchors += cur.ranchors - acc.ranchors;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<nest_info_t*>& ls);
};
WRITE_CLASS_ENCODER(nest_info_t)

inline bool operator==(const nest_info_t &l, const nest_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}

ostream& operator<<(ostream &out, const nest_info_t &n);


struct vinodeno_t {
  inodeno_t ino;
  vinodeno_t() {}
  explicit vinodeno_t(inodeno_t i) : ino(i) {}

  void encode(bufferlist& bl) const {
    ::encode(ino, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(ino, p);
  }
};
WRITE_CLASS_ENCODER(vinodeno_t)

inline bool operator==(const vinodeno_t &l, const vinodeno_t &r) {
  return l.ino == r.ino;
}
inline bool operator!=(const vinodeno_t &l, const vinodeno_t &r) {
  return !(l == r);
}
inline bool operator<(const vinodeno_t &l, const vinodeno_t &r) {
  return l.ino < r.ino;
}

namespace std {
  template<> struct hash<vinodeno_t> {
    size_t operator()(const vinodeno_t &vino) const {
      hash<inodeno_t> H;
      return H(vino.ino);
    }
  };
}




inline ostream& operator<<(ostream &out, const vinodeno_t &vino) {
  out << vino.ino;
  return out;
}


/*
 * client_writeable_range_t
 */
struct client_writeable_range_t {
  struct byte_range_t {
    uint64_t first, last;    // interval client can write to
    byte_range_t() : first(0), last(0) {}
  };

  byte_range_t range;

  client_writeable_range_t() {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<client_writeable_range_t*>& ls);
};

inline void decode(client_writeable_range_t::byte_range_t& range, bufferlist::iterator& bl) {
  ::decode(range.first, bl);
  ::decode(range.last, bl);
}

WRITE_CLASS_ENCODER(client_writeable_range_t)

ostream& operator<<(ostream& out, const client_writeable_range_t& r);

inline bool operator==(const client_writeable_range_t& l,
		       const client_writeable_range_t& r) {
  return l.range.first == r.range.first && l.range.last == r.range.last;
}


/*
 * inode_t
 */
struct inode_t {
  // base (immutable)
  inodeno_t ino;
  uint32_t  rdev;    // if special file

  // affected by any inode change...
  ceph::real_time ctime;   // inode change time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t	     uid;
  gid_t	     gid;

  // nlink
  int32_t    nlink;
  bool	     anchored;		// auth only?

  // file (data access)
  boost::uuids::uuid volume;
  vector <boost::uuids::uuid> old_volumes;
  uint64_t   size;	  // on directory, # dentries
  uint64_t   max_size_ever; // max size the file has ever been
  uint32_t   truncate_seq;
  uint64_t   truncate_size, truncate_from;
  uint32_t   truncate_pending;
  ceph::real_time mtime;   // file data modify time.
  ceph::real_time atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())
  bufferlist inline_data;
  version_t  inline_version;

  map<client_t,client_writeable_range_t> client_ranges;	 // client(s) can write to these ranges

  // dirfrag, recursive accountin
  frag_info_t dirstat;	       // protected by my filelock
  nest_info_t rstat;	       // protected by my nestlock
  nest_info_t accounted_rstat; // protected by parent's nestlock

  // special stuff
  version_t version;	       // auth only
  version_t file_data_version; // auth only
  version_t xattr_version;

  version_t backtrace_version;

  inode_t() : ino(0), rdev(0),
	      mode(0), uid(0), gid(0),
	      nlink(0), anchored(false),
	      size(0), max_size_ever(0),
	      truncate_seq(0), truncate_size(0), truncate_from(0),
	      truncate_pending(0),
	      time_warp_seq(0),
	      inline_version(1),
	      version(0), file_data_version(0), xattr_version(0), backtrace_version(0) {
  }

  // file type
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()	    const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool is_truncating() const { return (truncate_pending > 0); }
  void truncate(uint64_t old_size, uint64_t new_size) {
    assert(new_size < old_size);
    if (old_size > max_size_ever)
      max_size_ever = old_size;
    truncate_from = old_size;
    size = new_size;
    rstat.rbytes = new_size;
    truncate_size = size;
    truncate_seq++;
    truncate_pending++;
  }

  uint64_t get_layout_size_increment() {
// XXX how to calculate?  is this necessary?  &c.  mdw 20150212
#if 0
    return object_size * stripe_count;
#else
    return 4096;
#endif
  }

  bool is_dirty_rstat() const { return !(rstat == accounted_rstat); }

  uint64_t get_max_size() const {
    uint64_t max = 0;
      for (map<client_t,client_writeable_range_t>::const_iterator p = client_ranges.begin();
	   p != client_ranges.end();
	   ++p)
	if (p->second.range.last > max)
	  max = p->second.range.last;
      return max;
  }
  void set_max_size(uint64_t new_max) {
    if (new_max == 0) {
      client_ranges.clear();
    } else {
      for (map<client_t,client_writeable_range_t>::iterator p = client_ranges.begin();
	   p != client_ranges.end();
	   ++p)
	p->second.range.last = new_max;
    }
  }

  bool is_backtrace_updated() {
    return backtrace_version == version;
  }
  void update_backtrace(version_t pv=0) {
    backtrace_version = pv ? pv : version;
  }

  void add_old_volume(const boost::uuids::uuid& uuid) {
    backtrace_version = version;
    old_volumes.push_back(uuid);
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<inode_t*>& ls);
};
WRITE_CLASS_ENCODER(inode_t)

/*
 * like an inode, but for a dir frag
 */
struct fnode_t {
  version_t version;
  frag_info_t fragstat, accounted_fragstat;
  nest_info_t rstat, accounted_rstat;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<fnode_t*>& ls);
  fnode_t() : version(0) {};
};
WRITE_CLASS_ENCODER(fnode_t)


/*
 * session_info_t
 */

struct session_info_t {
  entity_inst_t inst;
  map<ceph_tid_t,inodeno_t> completed_requests;
  interval_set<inodeno_t> prealloc_inos;   // preallocated, ready to use.
  interval_set<inodeno_t> used_inos;	   // journaling use

  client_t get_client() const { return client_t(inst.name.num()); }

  void clear_meta() {
    prealloc_inos.clear();
    used_inos.clear();
    completed_requests.clear();
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<session_info_t*>& ls);
};
WRITE_CLASS_ENCODER(session_info_t)


/*
 * mds_table_pending_t
 *
 * mds's requesting any pending ops.  child needs to encode the corresponding
 * pending mutation state in the table.
 */
struct mds_table_pending_t {
  uint64_t reqid;
  int32_t mds;
  version_t tid;
  mds_table_pending_t() : reqid(0), mds(0), tid(0) {}
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<mds_table_pending_t*>& ls);
};
WRITE_CLASS_ENCODER(mds_table_pending_t)


// =========
// requests

struct metareqid_t {
  entity_name_t name;
  uint64_t tid;
  metareqid_t() : tid(0) {}
  metareqid_t(entity_name_t n, ceph_tid_t t) : name(n), tid(t) {}
  void encode(bufferlist& bl) const {
    ::encode(name, bl);
    ::encode(tid, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode(name, p);
    ::decode(tid, p);
  }
};
WRITE_CLASS_ENCODER(metareqid_t)

inline ostream& operator<<(ostream& out, const metareqid_t& r) {
  return out << r.name << ":" << r.tid;
}

inline bool operator==(const metareqid_t& l, const metareqid_t& r) {
  return (l.name == r.name) && (l.tid == r.tid);
}
inline bool operator!=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name != r.name) || (l.tid != r.tid);
}
inline bool operator<(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) ||
    (l.name == r.name && l.tid < r.tid);
}
inline bool operator<=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) ||
    (l.name == r.name && l.tid <= r.tid);
}
inline bool operator>(const metareqid_t& l, const metareqid_t& r) { return !(l <= r); }
inline bool operator>=(const metareqid_t& l, const metareqid_t& r) { return !(l < r); }

namespace std {
  template<> struct hash<metareqid_t> {
    size_t operator()(const metareqid_t &r) const {
      hash<uint64_t> H;
      return H(r.name.num()) ^ H(r.name.type()) ^ H(r.tid);
    }
  };
}


// cap info for client reconnect
struct cap_reconnect_t {
  string path;
  mutable ceph_mds_cap_reconnect capinfo;
  bufferlist flockbl;

  cap_reconnect_t() {
    memset(&capinfo, 0, sizeof(capinfo));
  }
  cap_reconnect_t(uint64_t cap_id, inodeno_t pino, const string& p, int w,
		  int i) :
    path(p) {
    capinfo.cap_id = cap_id;
    capinfo.wanted = w;
    capinfo.issued = i;
    capinfo.pathbase = pino;
    capinfo.flock_len = 0;
  }
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void encode_old(bufferlist& bl) const;
  void decode_old(bufferlist::iterator& bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<cap_reconnect_t*>& ls);
};
WRITE_CLASS_ENCODER(cap_reconnect_t)


// compat for pre-FLOCK feature
struct old_ceph_mds_cap_reconnect {
  uint64_t cap_id;
  uint32_t wanted;
  uint32_t issued;
  uint64_t old_size;
  ceph_timespec old_mtime, old_atime;
  uint64_t pathbase; /* base ino for our path to this ino */
};
WRITE_RAW_ENCODER(old_ceph_mds_cap_reconnect)

struct old_cap_reconnect_t {
  string path;
  old_ceph_mds_cap_reconnect capinfo;

  const old_cap_reconnect_t& operator=(const cap_reconnect_t& n) {
    path = n.path;
    capinfo.cap_id = n.capinfo.cap_id;
    capinfo.wanted = n.capinfo.wanted;
    capinfo.issued = n.capinfo.issued;
    capinfo.pathbase = n.capinfo.pathbase;
    return *this;
  }
  operator cap_reconnect_t() {
    cap_reconnect_t n;
    n.path = path;
    n.capinfo.cap_id = capinfo.cap_id;
    n.capinfo.wanted = capinfo.wanted;
    n.capinfo.issued = capinfo.issued;
    n.capinfo.pathbase = capinfo.pathbase;
    return n;
  }

  void encode(bufferlist& bl) const {
    ::encode(path, bl);
    ::encode(capinfo, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(path, bl);
    ::decode(capinfo, bl);
  }
};
WRITE_CLASS_ENCODER(old_cap_reconnect_t)


// ================================================================
// dir frag

struct dirfrag_t {
  inodeno_t ino;
  frag_t    frag;

  dirfrag_t() : ino(0) { }
  dirfrag_t(inodeno_t i, frag_t f) : ino(i), frag(f) { }

  void encode(bufferlist& bl) const {
    ::encode(ino, bl);
    ::encode(frag, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(ino, bl);
    ::decode(frag, bl);
  }
};
WRITE_CLASS_ENCODER(dirfrag_t)


inline ostream& operator<<(ostream& out, const dirfrag_t df) {
  out << df.ino;
  if (!df.frag.is_root()) out << "." << df.frag;
  return out;
}
inline bool operator<(dirfrag_t l, dirfrag_t r) {
  if (l.ino < r.ino) return true;
  if (l.ino == r.ino && l.frag < r.frag) return true;
  return false;
}
inline bool operator==(dirfrag_t l, dirfrag_t r) {
  return l.ino == r.ino && l.frag == r.frag;
}

namespace std {
  template<> struct hash<dirfrag_t> {
    size_t operator()(const dirfrag_t &df) const {
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(df.ino) ^ I(df.frag);
    }
  };
}



// ================================================================

#define META_POP_IRD	 0
#define META_POP_IWR	 1
#define META_POP_READDIR 2
#define META_POP_FETCH	 3
#define META_POP_STORE	 4
#define META_NPOP	 5

class inode_load_vec_t {
  static const int NUM = 2;
  std::vector < DecayCounter > vec;
public:
  inode_load_vec_t(const ceph::real_time& now)
     : vec(NUM, DecayCounter(now))
  {}
  // for dencoder infrastructure
  inode_load_vec_t() :
    vec(NUM, DecayCounter())
  {}
  DecayCounter &get(int t) {
    assert(t < NUM);
    return vec[t];
  }
 void zero(ceph::real_time now) {
    for (int i=0; i<NUM; i++)
      vec[i].reset(now);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f);
  static void generate_test_instances(list<inode_load_vec_t*>& ls);
};
inline void encode(const inode_load_vec_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(inode_load_vec_t &c, bufferlist::iterator &p) {
  c.decode(p);
}

class dirfrag_load_vec_t {
public:
  static const int NUM = 5;
  std::vector < DecayCounter > vec;
  dirfrag_load_vec_t(const ceph::real_time &now)
     : vec(NUM, DecayCounter(now))
  { }
  // for dencoder infrastructure
  dirfrag_load_vec_t()
    : vec(NUM, DecayCounter())
  {}
  void encode(bufferlist &bl) const {
    ENCODE_START(2, 2, bl);
    for (int i=0; i<NUM; i++)
      ::encode(vec[i], bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
    for (int i=0; i<NUM; i++)
      ::decode(vec[i], p);
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<dirfrag_load_vec_t*>& ls);

  DecayCounter &get(int t) {
    assert(t < NUM);
    return vec[t];
  }
  void adjust(ceph::real_time now, const DecayRate& rate, double d) {
    for (int i=0; i<NUM; i++)
      vec[i].adjust(now, rate, d);
  }
  void zero(ceph::real_time now) {
    for (int i=0; i<NUM; i++)
      vec[i].reset(now);
  }
  double meta_load(ceph::real_time now, const DecayRate& rate) {
    return
      1*vec[META_POP_IRD].get(now, rate) +
      2*vec[META_POP_IWR].get(now, rate) +
      1*vec[META_POP_READDIR].get(now, rate) +
      2*vec[META_POP_FETCH].get(now, rate) +
      4*vec[META_POP_STORE].get(now, rate);
  }
  double meta_load() {
    return
      1*vec[META_POP_IRD].get_last() +
      2*vec[META_POP_IWR].get_last() +
      1*vec[META_POP_READDIR].get_last() +
      2*vec[META_POP_FETCH].get_last() +
      4*vec[META_POP_STORE].get_last();
  }

  void add(ceph::real_time now, DecayRate& rate, dirfrag_load_vec_t& r) {
    for (int i = 0; i < dirfrag_load_vec_t::NUM; i++)
      vec[i].adjust(r.vec[i].get(now, rate));
  }
  void sub(ceph::real_time now, DecayRate& rate, dirfrag_load_vec_t& r) {
    for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].adjust(-r.vec[i].get(now, rate));
  }
  void scale(double f) {
    for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
      vec[i].scale(f);
  }
};

inline void encode(const dirfrag_load_vec_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(dirfrag_load_vec_t& c, bufferlist::iterator &p) {
  c.decode(p);
}

inline ostream& operator<<(ostream& out, dirfrag_load_vec_t& dl)
{
  // ugliness!
  return out << "[" << dl.vec[0].get_last() << ","
	     << dl.vec[1].get_last() << "]";
}






/* mds_load_t
 * mds load
 */

struct mds_load_t {
  int mds_bal_mode;
  dirfrag_load_vec_t auth;
  dirfrag_load_vec_t all;

  double req_rate;
  double cache_hit_rate;
  double queue_len;

  double cpu_load_avg;

  mds_load_t(int _mds_bal_mode, const ceph::real_time &t) :
    mds_bal_mode(_mds_bal_mode), auth(t), all(t), req_rate(0),
    cache_hit_rate(0), queue_len(0), cpu_load_avg(0)
  {}
  mds_load_t(const ceph::real_time &t) :
    mds_bal_mode(-1), auth(t), all(t), req_rate(0),
    cache_hit_rate(0), queue_len(0), cpu_load_avg(0)
  {}
  // mostly for the dencoder infrastructure
  mds_load_t() :
    mds_bal_mode(-1), auth(), all(),
    req_rate(0), cache_hit_rate(0), queue_len(0), cpu_load_avg(0)
  {}

  double mds_load();  // defiend in MDBalancer.cc
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<mds_load_t*>& ls);
};
inline void encode(const mds_load_t &c, bufferlist &bl) { c.encode(bl); }
inline void decode(mds_load_t &c, bufferlist::iterator &p) {
  c.decode(p);
}

inline ostream& operator<<( ostream& out, mds_load_t& load )
{
  return out << "mdsload<" << load.auth << "/" << load.all
	     << ", req " << load.req_rate
	     << ", hr " << load.cache_hit_rate
	     << ", qlen " << load.queue_len
	     << ", cpu " << load.cpu_load_avg
	     << ">";
}

class load_spread_t {
public:
  static const int MAX = 4;
  int last[MAX];
  int p, n;
  DecayCounter count;

public:
  load_spread_t(CephContext* cct) : p(0), n(0),
				    count(ceph::real_clock::now())
  {
    for (int i=0; i<MAX; i++)
      last[i] = -1;
  }

  double hit(ceph::real_time now, const DecayRate& rate, int who) {
    for (int i=0; i<n; i++)
      if (last[i] == who)
	return count.get_last();

    // we're new(ish)
    last[p++] = who;
    if (n < MAX) n++;
    if (n == 1) return 0.0;

    if (p == MAX) p = 0;

    return count.hit(now, rate);
  }
  double get(ceph::real_time now, const DecayRate& rate) {
    return count.get(now, rate);
  }
};



// ================================================================

//#define MDS_PIN_REPLICATED	 1
//#define MDS_STATE_AUTH     (1<<0)

class MLock;
class SimpleLock;

class MDSCacheObject;

// -- authority delegation --
// directory authority types
//  >= 0 is the auth mds
#define CDIR_AUTH_PARENT   -1	// default
#define CDIR_AUTH_UNKNOWN  -2
#define CDIR_AUTH_DEFAULT   pair<int,int>(-1, -2)
#define CDIR_AUTH_UNDEF	    pair<int,int>(-2, -2)
//#define CDIR_AUTH_ROOTINODE pair<int,int>( 0, -2)


/*
 * for metadata leases to clients
 */
struct ClientLease {
  client_t client;
  MDSCacheObject *parent;

  ceph_seq_t seq;
  ceph::real_time ttl;
  xlist<ClientLease*>::item item_session_lease; // per-session list
  xlist<ClientLease*>::item item_lease;		// global list

  ClientLease(client_t c, MDSCacheObject *p) :
    client(c), parent(p), seq(0),
    item_session_lease(this),
    item_lease(this) { }
};


// print hack
struct mdsco_db_line_prefix {
  MDSCacheObject *object;
  mdsco_db_line_prefix(MDSCacheObject *o) : object(o) {}
};
ostream& operator<<(ostream& out, mdsco_db_line_prefix o);

// printer
ostream& operator<<(ostream& out, MDSCacheObject &o);

class MDSCacheObjectInfo {
public:
  inodeno_t ino;
  dirfrag_t dirfrag;
  string dname;

  MDSCacheObjectInfo() : ino(0) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<MDSCacheObjectInfo*>& ls);
};

inline bool operator==(const MDSCacheObjectInfo& l,
		       const MDSCacheObjectInfo& r) {
  if (l.ino || r.ino)
    return l.ino == r.ino;
  else
    return l.dirfrag == r.dirfrag && l.dname == r.dname;
}

WRITE_CLASS_ENCODER(MDSCacheObjectInfo)


class MDSCacheObject {
 public:
  // -- pins --
  const static int PIN_REPLICATED =  1000;
  const static int PIN_DIRTY	  =  1001;
  const static int PIN_LOCK	  = -1002;
  const static int PIN_REQUEST	  = -1003;
  const static int PIN_WAITER	  =  1004;
  const static int PIN_DIRTYSCATTERED = -1005;
  static const int PIN_AUTHPIN	  =  1006;
  static const int PIN_PTRWAITER  = -1007;
  const static int PIN_TEMPEXPORTING = 1008;  // temp pin between encode_ and finish_export
  static const int PIN_CLIENTLEASE = 1009;

  const char *generic_pin_name(int p) {
    switch (p) {
    case PIN_REPLICATED: return "replicated";
    case PIN_DIRTY: return "dirty";
    case PIN_LOCK: return "lock";
    case PIN_REQUEST: return "request";
    case PIN_WAITER: return "waiter";
    case PIN_DIRTYSCATTERED: return "dirtyscattered";
    case PIN_AUTHPIN: return "authpin";
    case PIN_PTRWAITER: return "ptrwaiter";
    case PIN_TEMPEXPORTING: return "tempexporting";
    case PIN_CLIENTLEASE: return "clientlease";
    default: assert(0); return 0;
    }
  }

  // -- state --
  const static int STATE_AUTH	   = (1<<30);
  const static int STATE_DIRTY	   = (1<<29);
  const static int STATE_NOTIFYREF = (1<<28); // notify dropping ref drop through _put()
  const static int STATE_REJOINING = (1<<27);  // replica has not joined w/ primary copy
  const static int STATE_REJOINUNDEF = (1<<26);	 // contents undefined.


  // -- wait --
  const static uint64_t WAIT_SINGLEAUTH	 = (1ull<<60);
  const static uint64_t WAIT_UNFREEZE	 = (1ull<<59); // pka AUTHPINNABLE


  // ============================================
  // cons
 public:
  MDSCacheObject() :
    state(0),
    ref(0),
    replica_nonce(0) {}
  virtual ~MDSCacheObject() {}

  // printing
  virtual void print(ostream& out) = 0;
  virtual ostream& print_db_line_prefix(ostream& out) {
    return out << "mdscacheobject(" << this << ") ";
  }

  // --------------------------------------------
  // state
 protected:
  uint32_t state;     // state bits

 public:
  unsigned get_state() const { return state; }
  unsigned state_test(unsigned mask) const { return (state & mask); }
  void state_clear(unsigned mask) { state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }
  void state_reset(unsigned s) { state = s; }

  bool is_auth() const { return state_test(STATE_AUTH); }
  bool is_dirty() const { return state_test(STATE_DIRTY); }
  bool is_clean() const { return !is_dirty(); }
  bool is_rejoining() const { return state_test(STATE_REJOINING); }

  // --------------------------------------------
  // authority
  virtual pair<int,int> authority() = 0;
  bool is_ambiguous_auth() {
    return authority().second != CDIR_AUTH_UNKNOWN;
  }

  // --------------------------------------------
  // pins
protected:
  int32_t      ref;	  // reference count
#ifdef MDS_REF_SET
  map<int,int> ref_map;
#endif

 public:
  int get_num_ref(int by = -1) {
#ifdef MDS_REF_SET
    if (by >= 0) {
      if (ref_map.find(by) == ref_map.end())
	return 0;
      return ref_map[by];
    }
#endif
    return ref;
  }
#ifdef MDS_REF_SET
  int get_pin_totals() {
    int total = 0;
    for(map<int,int>::iterator i = ref_map.begin(); i != ref_map.end(); ++i) {
      total += i->second;
    }
    return total;
  }
#endif
  virtual const char *pin_name(int by) = 0;
  //bool is_pinned_by(int by) { return ref_set.count(by); }
  //multiset<int>& get_ref_set() { return ref_set; }

  virtual void last_put() {}
  virtual void bad_put(int by) {
#ifdef MDS_REF_SET
    assert(ref_map[by] > 0);
#endif
    assert(ref > 0);
  }
  virtual void _put() {}
  void put(int by) {
#ifdef MDS_REF_SET
    if (ref == 0 || ref_map[by] == 0) {
#else
    if (ref == 0) {
#endif
      bad_put(by);
    } else {
      ref--;
#ifdef MDS_REF_SET
      ref_map[by]--;
      assert(ref == get_pin_totals());
#endif
      if (ref == 0)
	last_put();
      if (state_test(STATE_NOTIFYREF))
	_put();
    }
  }

  virtual void first_get() {}
  virtual void bad_get(int by) {
#ifdef MDS_REF_SET
    assert(by < 0 || ref_map[by] == 0);
#endif
    assert(0);
  }
  void get(int by) {
    if (ref == 0)
      first_get();
    ref++;
#ifdef MDS_REF_SET
    if (ref_map.find(by) == ref_map.end())
      ref_map[by] = 0;
    ref_map[by]++;
    assert(ref == get_pin_totals());
#endif
  }

  void print_pin_set(ostream& out) {
#ifdef MDS_REF_SET
    map<int, int>::iterator it = ref_map.begin();
    while (it != ref_map.end()) {
      out << " " << pin_name(it->first) << "=" << it->second;
      ++it;
    }
#else
    out << " nref=" << ref;
#endif
  }


  // --------------------------------------------
  // auth pins
  virtual bool can_auth_pin() = 0;
  virtual void auth_pin(void *who) = 0;
  virtual void auth_unpin(void *who) = 0;
  virtual bool is_frozen() = 0;
  virtual bool is_freezing() = 0;
  virtual bool is_freezing_or_frozen() {
    return is_frozen() || is_freezing();
  }


  // --------------------------------------------
  // replication (across mds cluster)
 protected:
  unsigned		replica_nonce; // [replica] defined on replica
  map<int,unsigned>	replica_map;   // [auth] mds -> nonce

 public:
  bool is_replicated() { return !replica_map.empty(); }
  bool is_replica(int mds) { return replica_map.count(mds); }
  int num_replicas() { return replica_map.size(); }
  unsigned add_replica(int mds) {
    if (replica_map.count(mds))
      return ++replica_map[mds];  // inc nonce
    if (replica_map.empty())
      get(PIN_REPLICATED);
    return replica_map[mds] = 1;
  }
  void add_replica(int mds, unsigned nonce) {
    if (replica_map.empty())
      get(PIN_REPLICATED);
    replica_map[mds] = nonce;
  }
  unsigned get_replica_nonce(int mds) {
    assert(replica_map.count(mds));
    return replica_map[mds];
  }
  void remove_replica(int mds) {
    assert(replica_map.count(mds));
    replica_map.erase(mds);
    if (replica_map.empty())
      put(PIN_REPLICATED);
  }
  void clear_replica_map() {
    if (!replica_map.empty())
      put(PIN_REPLICATED);
    replica_map.clear();
  }
  map<int,unsigned>::iterator replicas_begin() { return replica_map.begin(); }
  map<int,unsigned>::iterator replicas_end() { return replica_map.end(); }
  const map<int,unsigned>& get_replicas() { return replica_map; }
  void list_replicas(set<int>& ls) {
    for (map<int,unsigned>::const_iterator p = replica_map.begin();
	 p != replica_map.end();
	 ++p)
      ls.insert(p->first);
  }

  unsigned get_replica_nonce() { return replica_nonce; }
  void set_replica_nonce(unsigned n) { replica_nonce = n; }


  // ---------------------------------------------
  // waiting
 protected:
  multimap<uint64_t, Context*>	waiting;

 public:
  bool is_waiter_for(uint64_t mask, uint64_t min=0) {
    if (!min) {
      min = mask;
      while (min & (min-1))  // if more than one bit is set
	min &= min-1;	     //	 clear LSB
    }
    for (multimap<uint64_t,Context*>::iterator p = waiting.lower_bound(min);
	 p != waiting.end();
	 ++p) {
      if (p->first & mask) return true;
      if (p->first > mask) return false;
    }
    return false;
  }
  virtual void add_waiter(uint64_t mask, Context *c) {
    if (waiting.empty())
      get(PIN_WAITER);
    waiting.insert(pair<uint64_t,Context*>(mask, c));
//    pdout(10,g_conf->debug_mds) << (mdsco_db_line_prefix(this))
//			       << "add_waiter " << hex << mask << dec << " " << c
//			       << " on " << *this
//			       << dendl;

  }
  virtual void take_waiting(uint64_t mask, Context::List& vs) {
    if (waiting.empty()) return;
    multimap<uint64_t,Context*>::iterator it = waiting.begin();
    while (it != waiting.end()) {
      if (it->first & mask) {
	vs.push_back(*it->second);
//	pdout(10,g_conf->debug_mds) << (mdsco_db_line_prefix(this))
//				   << "take_waiting mask " << hex << mask << dec << " took " << it->second
//				   << " tag " << hex << it->first << dec
//				   << " on " << *this
//				   << dendl;
	waiting.erase(it++);
      } else {
//	pdout(10,g_conf->debug_mds) << "take_waiting mask " << hex << mask << dec << " SKIPPING " << it->second
//				   << " tag " << hex << it->first << dec
//				   << " on " << *this
//				   << dendl;
	++it;
      }
    }
    if (waiting.empty())
      put(PIN_WAITER);
  }

  void finish_waiting(uint64_t mask, int result = 0) {
    Context::List finished;
    take_waiting(mask, finished);
    finish_contexts(finished, result);
  }


  // ---------------------------------------------
  // locking
  // noop unless overloaded.
  virtual SimpleLock* get_lock(int type) { assert(0); return 0; }
  virtual void set_object_info(MDSCacheObjectInfo &info) { assert(0); }
  virtual void encode_lock_state(int type, bufferlist& bl) { assert(0); }
  virtual void decode_lock_state(int type, bufferlist& bl) { assert(0); }
  virtual void finish_lock_waiters(int type, uint64_t mask, int r=0) { assert(0); }
  virtual void add_lock_waiter(int type, uint64_t mask, Context *c) { assert(0); }
  virtual bool is_lock_waiting(int type, uint64_t mask) { assert(0); return false; }

  virtual void clear_dirty_scattered(int type) { assert(0); }

  // ---------------------------------------------
  // ordering
  virtual bool is_lt(const MDSCacheObject *r) const = 0;
  struct ptr_lt {
    bool operator()(const MDSCacheObject* l, const MDSCacheObject* r) const {
      return l->is_lt(r);
    }
  };

};

inline ostream& operator<<(ostream& out, MDSCacheObject &o) {
  o.print(out);
  return out;
}

inline ostream& operator<<(ostream& out, const MDSCacheObjectInfo &info) {
  if (info.ino) return out << info.ino;
  if (info.dname.length()) return out << info.dirfrag << "/" << info.dname;
  return out << info.dirfrag;
}

inline ostream& operator<<(ostream& out, mdsco_db_line_prefix o) {
  o.object->print_db_line_prefix(out);
  return out;
}





#endif
