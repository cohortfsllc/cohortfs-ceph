#ifndef CEPH_CLIENT_DENTRY_H
#define CEPH_CLIENT_DENTRY_H

#include "mds/mdstypes.h"

class DirStripe;
class Inode;

class Dentry {
 private:
  CephContext *cct;
  int     ref;                       // 1 if there's a dir beneath me.

 public:
  string  name;                      // sort of lame
  //const char *name;
  DirStripe *stripe;
  vinodeno_t vino;
  uint64_t offset;
  int lease_mds;
  utime_t lease_ttl;
  uint64_t lease_gen;
  ceph_seq_t lease_seq;
  int cap_shared_gen;
  
  void get() { 
    assert(ref >= 0);
    ref++;
    lsubdout(cct, client, 15) << "dentry.get on " << this << " " << name << " now " << ref << dendl;
  }
  void put() { 
    assert(ref > 0);
    ref--;
    lsubdout(cct, client, 15) << "dentry.put on " << this << " " << name << " now " << ref << dendl;
    if (ref == 0)
      delete this;
  }
  int get_num_ref() const { return ref; }

  void dump(Formatter *f) const;

  bool is_null() const { return vino.ino == 0; }
 
  Dentry(CephContext *cct)
      : cct(cct), ref(0), stripe(0), vino(0, CEPH_NOSNAP), offset(0),
        lease_mds(-1), lease_gen(0), lease_seq(0), cap_shared_gen(0) {}
private:
  ~Dentry() {
    assert(ref == 0);
  }
};





#endif
