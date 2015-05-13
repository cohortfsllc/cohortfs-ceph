// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_INODE_H
#define CEPH_CLIENT_INODE_H

#include <cassert>
#include "include/ceph_hash.h"
#include "include/types.h"
#include "include/xlist.h"
#include "include/filepath.h"

#include "vol/Volume.h"
#include "mds/mdstypes.h" // hrm

struct MetaSession;
class Dentry;
class Dir;
class Inode;

struct Cap {
  MetaSession *session;
  Inode *inode;
  xlist<Cap*>::item cap_item;

  uint64_t cap_id;
  unsigned issued;
  unsigned implemented;
  unsigned wanted;   // as known to mds.
  uint64_t seq, issue_seq;
  uint32_t mseq;  // migration seq
  uint32_t gen;

  Cap() : session(NULL), inode(NULL), cap_item(this), cap_id(0), issued(0),
	       implemented(0), wanted(0), seq(0), issue_seq(0), mseq(0), gen(0) {}

  void dump(Formatter *f) const;
};

// inode flags
#define I_COMPLETE 1

class Inode {
 public:
  CephContext *cct;

  // -- the actual inode --
  inodeno_t ino;
  uint32_t rdev;    // if special file

  // affected by any inode change...
  ceph::real_time ctime;   // inode change time

  // perm (namespace permissions)
  uint32_t mode;
  uid_t uid;
  gid_t gid;

  // nlink
  int32_t nlink;

  // file (data access)
  AVolRef volume;
  uint64_t size; // on directory, # dentries
  uint32_t truncate_seq;
  uint64_t truncate_size;
  ceph::real_time mtime;   // file data modify time.
  ceph::real_time atime;   // file data access time.
  // count of (potential) mtime/atime timewarps (i.e., utimes())
  uint32_t time_warp_seq;

  uint64_t max_size; // max size we can write to

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat;

  // special stuff
  version_t version; // auth only
  version_t xattr_version;

  // inline data
  version_t  inline_version;
  bufferlist inline_data;

  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  uint32_t hash_dentry_name(const string &dn) {
    int which = 0;	// was: dir_layout.dl_dir_hash
    if (!which)
      which = CEPH_STR_HASH_LINUX;
    return ceph_str_hash(which, dn.data(), dn.length());
  }

  unsigned flags;

  // about the dir (if this is one!)
  set<int>  dir_contacts;
  bool      dir_hashed, dir_replicated;

  // per-mds caps
  map<int,Cap*> caps; // mds -> Cap
  Cap *auth_cap;
  unsigned dirty_caps, flushing_caps;
  uint64_t flushing_cap_seq;
  uint16_t flushing_cap_tid[CEPH_CAP_BITS];
  int shared_gen, cache_gen;
  ceph::mono_time hold_caps_until;
  xlist<Inode*>::item cap_item, flushing_cap_item;
  ceph_tid_t last_flush_tid;

  map<int,int> open_by_mode;
  map<int,int> cap_refs;

  uint64_t     reported_size, wanted_max_size, requested_max_size;

  int _ref; // ref count. 1 for each dentry, fh that links to me.
  int ll_ref; // separate ref count for ll client
  Dir *dir; // if i'm a dir.
  set<Dentry*> dn_set; // if i'm linked to a dentry.
  string symlink;  // symlink content, if it's a symlink
  fragtree_t dirfragtree;
  map<string,bufferptr> xattrs;
  map<frag_t,int> fragmap; // known frag -> mds mappings

  list<std::condition_variable*> waitfor_caps;
  list<std::condition_variable*> waitfor_commit;

  Dentry *get_first_parent() {
    assert(!dn_set.empty());
    return *dn_set.begin();
  }

  void make_long_path(filepath& p);
  void make_relative_path(filepath& p);

  void get() {
    _ref++;
    lsubdout(cct, mds, 15) << "inode.get on " << this << " " <<  ino
			   << " now " << _ref << dendl;
  }
  /// private method to put a reference; see Client::put_inode()
  int _put(int n=1) {
    _ref -= n;
    lsubdout(cct, mds, 15) << "inode.put on " << this << " " << ino
			   << " now " << _ref << dendl;
    assert(_ref >= 0);
    return _ref;
  }

  int get_num_ref() {
    return _ref;
  }

  void ll_get() {
    ll_ref++;
  }
  void ll_put(int n=1) {
    assert(ll_ref >= n);
    ll_ref -= n;
  }

  Inode(CephContext *cct_, vinodeno_t vino, AVolRef vol_)
    : cct(cct_), ino(vino.ino), rdev(0), mode(0), uid(0), gid(0), nlink(0),
      volume(vol_),
      size(0), truncate_seq(1), truncate_size(-1), time_warp_seq(0),
      max_size(0), version(0), xattr_version(0), inline_version(0),
      flags(0), dir_hashed(false), dir_replicated(false), auth_cap(NULL),
      dirty_caps(0), flushing_caps(0), flushing_cap_seq(0), shared_gen(0),
      cache_gen(0), cap_item(this), flushing_cap_item(this), last_flush_tid(0),
      reported_size(0), wanted_max_size(0), requested_max_size(0),
      _ref(0), ll_ref(0), dir(0), dn_set()
  {
    memset(&flushing_cap_tid, 0, sizeof(uint16_t)*CEPH_CAP_BITS);
  }
  ~Inode() { }

  vinodeno_t vino() { return vinodeno_t(ino); }

  struct Compare {
    bool operator() (Inode* const &left, Inode* const &right) {
      return left->ino.val < right->ino.val;
    }
  };

  bool check_mode(uid_t uid, gid_t gid, gid_t *sgids, int sgid_count,
		  uint32_t flags);

  // CAPS --------
  void get_open_ref(int mode);
  bool put_open_ref(int mode);

  void get_cap_ref(int cap);
  int put_cap_ref(int cap);
  bool is_any_caps();
  bool cap_is_valid(Cap* cap);
  int caps_issued(int *implemented = 0);
  void touch_cap(Cap *cap);
  void try_touch_cap(int mds);
  bool caps_issued_mask(unsigned mask);
  int caps_used();
  int caps_file_wanted();
  int caps_wanted();
  int caps_dirty();

  bool have_valid_size();
  Dir *open_dir();

  void dump(Formatter *f) const;
};

ostream& operator<<(ostream &out, Inode &in);

#endif
