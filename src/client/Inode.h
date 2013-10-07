// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_INODE_H
#define CEPH_CLIENT_INODE_H

#include "include/types.h"
#include "include/xlist.h"
#include "include/filepath.h"

#include "mds/mdstypes.h" // hrm

#include "osdc/ObjectCacher.h"
#include "include/assert.h"
#include "Capability.h"

class MetaSession;
class Dentry;
class DirStripe;
class SnapRealm;
class Inode;
class InodeCache;

struct CapSnap {
  //snapid_t follows;  // map key
  Inode *in;
  SnapContext context;
  int issued, dirty;

  uint64_t size;
  utime_t ctime, mtime, atime;
  version_t time_warp_seq;
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;
  map<string,bufferptr> xattrs;
  version_t xattr_version;

  bool writing, dirty_data;
  uint64_t flush_tid;
  xlist<CapSnap*>::item flushing_item;

  CapSnap(Inode *i)
    : in(i), issued(0), dirty(0), 
      size(0), time_warp_seq(0), mode(0), uid(0), gid(0), xattr_version(0),
      writing(false), dirty_data(false), flush_tid(0),
      flushing_item(this)
  {}

  void dump(Formatter *f) const;
};


class Inode : public CapObject {
 private:
  InodeCache *cache;
 public:
  uint32_t   rdev;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;  

  // file (data access)
  ceph_dir_layout dir_layout;
  ceph_file_layout layout;
  uint64_t   size;        // on directory, # dentries
  uint32_t   truncate_seq;
  uint64_t   truncate_size;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())

  uint64_t max_size;  // max size we can write to

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat;
 
  // special stuff
  version_t version;           // auth only
  version_t xattr_version;

  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  bool has_dir_layout() const {
    for (unsigned c = 0; c < sizeof(layout); c++)
      if (*((const char *)&layout + c))
	return true;
    return false;
  }

  // about the dir (if this is one!)
  set<int>  dir_contacts;
  bool      dir_hashed, dir_replicated;

  Inode *snapdir_parent;  // only if we are a snapdir inode
  map<snapid_t,CapSnap*> cap_snaps;   // pending flush to mds

  //int open_by_mode[CEPH_FILE_MODE_NUM];
  map<int,int> open_by_mode;

  ObjectCacher::ObjectSet oset;

  uint64_t     reported_size, wanted_max_size, requested_max_size;

  int       _ref;      // ref count. 1 for each dentry, fh that links to me.
  int       ll_ref;   // separate ref count for ll client
  set<Dentry*> dn_set;      // if i'm linked to a dentry.
  string    symlink;  // symlink content, if it's a symlink
  map<string,bufferptr> xattrs;

  vector<int> stripe_auth;
  vector<DirStripe*> stripes; // if i'm a dir.

  list<Cond*>       waitfor_commit;

  Dentry *get_first_parent() {
    assert(!dn_set.empty());
    return *dn_set.begin();
  }

  void make_long_path(filepath& p);
  void make_nosnap_relative_path(filepath& p);

  void get() { 
    _ref++; 
    lsubdout(cct, mds, 15) << "inode.get on " << this << " " <<  ino << '.' << snapid
		   << " now " << _ref << dendl;
  }
  int put(int n=1) { 
    _ref -= n; 
    lsubdout(cct, mds, 15) << "inode.put on " << this << " " << ino << '.' << snapid
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

  Inode(CephContext *cct, InodeCache *cache, vinodeno_t vino,
        ceph_file_layout *newlayout)
    : CapObject(cct, vino), cache(cache),
      rdev(0), mode(0), uid(0), gid(0), nlink(0),
      size(0), truncate_seq(1), truncate_size(-1),
      time_warp_seq(0), max_size(0), version(0), xattr_version(0),
      dir_hashed(false), dir_replicated(false),
      snapdir_parent(0), oset((void *)this, newlayout->fl_pg_pool, ino),
      reported_size(0), wanted_max_size(0), requested_max_size(0),
      _ref(0), ll_ref(0)
  {
    memset(&dir_layout, 0, sizeof(dir_layout));
    memset(&layout, 0, sizeof(layout));
  }
  ~Inode() { }

  vinodeno_t vino() { return vinodeno_t(ino, snapid); }

  struct Compare {
    bool operator() (Inode* const & left, Inode* const & right) {
      if (left->ino.val < right->ino.val) {
	return (left->snapid.val < right->snapid.val);
      }
      return false;
    }
  };

  bool check_mode(uid_t uid, gid_t gid, gid_t *sgids, int sgid_count, uint32_t flags);

  // CAPS --------
  void get_open_ref(int mode);
  bool put_open_ref(int mode);

  virtual unsigned caps_wanted() const;
  virtual void read_client_caps(const Cap *cap, MClientCaps *m);
  virtual void write_client_caps(const Cap *cap, MClientCaps *m, unsigned mask);
  virtual bool on_caps_revoked(unsigned revoked);
  virtual bool check_cap(const Cap *cap, unsigned retain, bool unmounting) const;

  void update_file_bits(uint64_t truncate_seq, uint64_t truncate_size,
                        uint64_t size, uint64_t time_warp_seq,
                        utime_t ctime, utime_t mtime, utime_t atime,
                        unsigned issued);

  bool have_valid_size();
  stripeid_t pick_stripe(const string &dname);
  DirStripe *open_stripe(stripeid_t stripeid);

  virtual void print(ostream &out);
  void dump(Formatter *f) const;
};

ostream& operator<<(ostream &out, Inode &in);

#endif
