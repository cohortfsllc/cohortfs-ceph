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
class Dir;
typedef boost::intrusive_ptr<Dir> DirRef;
class Inode;
typedef boost::intrusive_ptr<Inode> InodeRef;

class Storage;
class Volume;

class Cache {
 private:
  const Volume *volume;
  const mcas::gc_global &gc;
  mcas::skiplist<Inode> inodes;
  mcas::skiplist<Dir> dirs;
  mcas::skiplist<Dentry> dentries;
  Storage *storage;
  std::atomic<ino_t> next_ino;
  uint32_t dir_stripes;
  InodeRef root;

 public:
  Cache(const Volume *volume, const mcas::gc_global &gc,
        const mcas::obj_cache &inode_cache,
        const mcas::obj_cache &dir_cache,
        const mcas::obj_cache &dentry_cache,
        Storage *storage, int highwater, int lowwater);

  const Volume* get_volume() const { return volume; }

  InodeRef create_inode(const mcas::gc_guard &guard, const identity &who,
                        int type, uint32_t stripes = 0);
  InodeRef get_inode(const mcas::gc_guard &guard, ino_t ino);

  DirRef get_dir(const mcas::gc_guard &guard, ino_t ino, uint32_t stripe);

  DentryRef lookup(const mcas::gc_guard &guard, const Dir *parent,
                   const std::string &name, bool return_nonexistent = false);
  DentryRef lookup(const mcas::gc_guard &guard, ino_t parent,
                   uint32_t stripe, const std::string &name,
                   bool return_nonexistent = false);
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_CACHE_H
