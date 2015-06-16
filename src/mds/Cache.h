// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_CACHE_H
#define COHORT_MDS_CACHE_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

class Inode;
typedef boost::intrusive_ptr<Inode> InodeRef;

class Storage;
class Volume;

class Cache {
 private:
  const Volume *volume;
  const mcas::gc_global &gc;
  mcas::skiplist<Inode> inodes;
  Storage *storage;
  std::atomic<libmds_ino_t> next_ino;
  InodeRef root;

 public:
  Cache(const Volume *volume, const mcas::gc_global &gc,
        Storage *storage, int highwater, int lowwater);

  const Volume* get_volume() const { return volume; }

  InodeRef create(const identity &who, int type);
  InodeRef get(libmds_ino_t ino);

 private:
  // skiplist sort function
  static int inode_cmp(const void *lhs, const void *rhs);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_CACHE_H
