// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_CACHE_H
#define COHORT_MDS_CACHE_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

class Storage;

class Inode;
typedef boost::intrusive_ptr<Inode> InodeRef;

class Cache {
 private:
  const mcas::gc_global &gc;
  mcas::skiplist<Inode> inodes;
  Storage *storage;
  std::atomic<_inodeno_t> next_ino;
  InodeRef root;

 public:
  Cache(const mcas::gc_global &gc, Storage *storage,
        int highwater, int lowwater)
    : gc(gc),
      inodes(gc, inode_cmp, "inodes", highwater, lowwater),
      storage(storage),
      next_ino(0)
  {}

  InodeRef create(const identity &who, int type);
  InodeRef get(_inodeno_t ino);

 private:
  // skiplist sort function
  static int inode_cmp(const void *lhs, const void *rhs);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_CACHE_H
