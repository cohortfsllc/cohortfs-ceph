// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_CACHE_H
#define COHORT_MDS_CACHE_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

class Dentry;
typedef boost::intrusive_ptr<Dentry> DentryRef;
class Inode;
typedef boost::intrusive_ptr<Inode> InodeRef;

class Storage;
class Volume;

class Cache {
 private:
  const Volume *volume;
  const mcas::gc_global &gc;
  mcas::skiplist<Inode> inodes;
  mcas::skiplist<Dentry> dentries;
  Storage *storage;
  std::atomic<ino_t> next_ino;
  InodeRef root;

 public:
  Cache(const Volume *volume, const mcas::gc_global &gc,
        const mcas::obj_cache &inode_cache,
        const mcas::obj_cache &dentry_cache,
        Storage *storage, int highwater, int lowwater);

  const Volume* get_volume() const { return volume; }

  InodeRef create(const mcas::gc_guard &guard, const identity &who, int type);
  InodeRef get(const mcas::gc_guard &guard, ino_t ino);

  DentryRef lookup(const mcas::gc_guard &guard, const Inode *parent,
                   const std::string &name, bool return_nonexistent = false);
  DentryRef lookup(const mcas::gc_guard &guard, ino_t parent,
                   const std::string &name, bool return_nonexistent = false);

 private:
  // skiplist sort functions
  static int inode_cmp(const void *lhs, const void *rhs);
  static int dentry_cmp(const void *lhs, const void *rhs);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_CACHE_H
