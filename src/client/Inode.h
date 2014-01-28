// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_INODE_H
#define CEPH_CLIENT_INODE_H

#include "include/types.h"
#include "include/xlist.h"
#include "include/filepath.h"
#include "include/lru.h"

#include "mds/mdstypes.h" // hrm

#include "osdc/ObjectCacher.h"
#include "include/assert.h"
#include "Capability.h"

struct MetaSession;
class Dentry;
class DirStripe;
class DirRegMap;
class MDSRegMap;
struct SnapRealm;
class Inode;
class InodeCache;

typedef hash_map<vinodeno_t, Inode*> inode_hashmap;

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


class Inode : public CapObject, public LRUObject {
 private:
  InodeCache *cache;
  vector<int> stripe_auth;
 public:
  const inode_hashmap &inodes; // inode map; needed for DirStripe::unlink()
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
  DirRegMap *registrations; // directory placement callback registrations

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

  vector<DirStripe*> stripes; // if i'm a dir.

  list<Cond*>       waitfor_commit;

  Dentry *get_first_parent() {
    assert(!dn_set.empty());
    return *dn_set.begin();
  }

  void make_long_path(filepath& p);
  void make_nosnap_relative_path(filepath& p);

  // reference counting
  void get() { 
    assert(_ref >= 0);
    if (++_ref == 2)
      lru_pin();
    lsubdout(cct, client, 15) << "inode.get on " << this << " " <<  vino()
                           << " now " << _ref << dendl;
  }
  void put() {
    if (--_ref == 1)
      lru_unpin();
    lsubdout(cct, client, 15) << "inode.put on " << this << " " << vino()
                           << " now " << _ref << dendl;
    if (_ref == 0)
      delete this;
  }

  int get_num_ref() const {
    return _ref;
  }

  void ll_get() {
    ll_ref++;
  }
  void ll_put(int n=1) {
    assert(ll_ref >= n);
    ll_ref -= n;
  }

  // clean up an inode reference on destruction
  class Deref {
    Inode *in;
   public:
    Deref(Inode *in) : in(in) {}
    ~Deref() { in->put(); }
  };

  // maintain a set of referenced inodes with a destructor to drop their refs
  class DerefSet {
    set<Inode*> inodes;
   public:
    ~DerefSet() { drop_refs(); }
    void insert(Inode *in) { inodes.insert(in); }
    void drop_refs() {
      for (set<Inode*>::iterator i = inodes.begin(); i != inodes.end(); ++i)
        (*i)->put();
      inodes.clear();
    }
  };

  Inode(CephContext *cct, InodeCache *cache, const inode_hashmap &inodes,
        vinodeno_t vino, ceph_file_layout *newlayout)
    : CapObject(cct, vino), cache(cache), inodes(inodes),
      rdev(0), mode(0), uid(0), gid(0), nlink(0),
      size(0), truncate_seq(1), truncate_size(-1),
      time_warp_seq(0), max_size(0), version(0), xattr_version(0),
      dir_hashed(false), dir_replicated(false), registrations(NULL),
      snapdir_parent(0), oset((void *)this, newlayout->fl_pg_pool, ino),
      reported_size(0), wanted_max_size(0), requested_max_size(0),
      _ref(0), ll_ref(0)
  {
    memset(&dir_layout, 0, sizeof(dir_layout));
    memset(&layout, 0, sizeof(layout));
  }
  ~Inode();

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
  __u64 hash_dentry_name(const string &dname) const;
  stripeid_t pick_stripe(__u64 dnhash) const;
  stripeid_t pick_stripe(const string &dname) const;
  DirStripe *open_stripe(stripeid_t stripeid);
  void close_stripe(DirStripe *stripe);


  // callback registrations
  bool add_dir_registration(MDSRegMap *mdsregs, uint32_t regid,
			    void *placement, void *recall, void *user);
  void remove_dir_registration(uint32_t regid);

  void set_stripe_auth(const vector<int> &stripes);
  size_t get_stripe_count() const { return stripe_auth.size(); }
  int get_stripe_auth(stripeid_t stripe) const { return stripe_auth[stripe]; }


  virtual void print(ostream &out);
  void dump(Formatter *f) const;
};

ostream& operator<<(ostream &out, Inode &in);

#endif
