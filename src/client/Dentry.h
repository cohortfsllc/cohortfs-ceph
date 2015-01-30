// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef CEPH_CLIENT_DENTRY_H
#define CEPH_CLIENT_DENTRY_H

#include "include/lru.h"

class Dir;
class Inode;

class Dentry : public LRUObject {
public:
  string name; // sort of lame
  Dir *dir;
  Inode *inode;
  int ref; // 1 if there's a dir beneath me.
  uint64_t offset;
  int lease_mds;
  ceph::mono_time lease_ttl;
  uint64_t lease_gen;
  ceph_seq_t lease_seq;
  int cap_shared_gen;

  /*
   * ref==1 -> cached, unused
   * ref >1 -> pinned in lru
   */
  void get() {
    assert(ref > 0);
    if (++ref == 2)
      lru_pin();
  }
  void put() {
    assert(ref > 0);
    if (--ref == 1)
      lru_unpin();
    if (ref == 0)
      delete this;
  }

  void dump(Formatter *f) const;

  Dentry() : dir(0), inode(0), ref(1), offset(0), lease_mds(-1),
	     lease_gen(0), lease_seq(0), cap_shared_gen(0) { }
private:
  ~Dentry() {
    assert(ref == 0);
  }
};





#endif
