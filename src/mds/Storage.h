/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_STORAGE_H
#define COHORT_MDS_STORAGE_H

#include <boost/uuid/uuid.hpp>
#include "common/mcas_skiplist.h"
#include "DirStorage.h"
#include "InodeStorage.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

class Storage {
 private:
  const mcas::gc_global &gc;
  mcas::skiplist<InodeStorage> inodes;
  mcas::skiplist<DirStorage> dirs;
 public:
  Storage(const mcas::gc_global &gc, const mcas::obj_cache &inode_cache,
          const mcas::obj_cache &dir_cache)
    : gc(gc),
      inodes(gc, inode_cache, InodeStorage::cmp),
      dirs(gc, dir_cache, DirStorage::cmp)
  {}

  InodeStorageRef get_inode(const mcas::gc_guard &guard,
                            const boost::uuids::uuid &volume, ino_t ino)
  {
    auto inode = inodes.get(guard, InodeStorage(volume, ino));
    if (inode && inode->attr.nlinks == 0) {
      // if our get() raced with destroy(), drop our ref immediately because
      // the node will be freed at the end of our gc_guard
      inode.reset();
    }
    return inode;
  }

  InodeStorageRef get_or_create_inode(const mcas::gc_guard &guard,
                                      const boost::uuids::uuid &volume,
                                      ino_t ino, const identity_t &who,
                                      int mode, uint32_t stripes = 0)
  {
    auto inode = inodes.get_or_create(guard, InodeStorage(volume, ino,
                                                          who, mode, stripes));
    if (inode->attr.nlinks == 0) {
      // if our get() raced with destroy(), drop our ref immediately because
      // the node will be freed at the end of our gc_guard
      inode.reset();
    }
    return inode;
  }

  void destroy(const mcas::gc_guard &guard, InodeStorageRef &&inode)
  {
    inodes.destroy(guard, std::move(inode));
  }

  DirStorageRef get_dir(const mcas::gc_guard &guard,
                        const boost::uuids::uuid &volume,
                        ino_t ino, uint32_t stripe)
  {
    auto dir = dirs.get(guard, DirStorage(volume, ino, stripe));
    if (dir && dir->unlinked) {
      // if our get() raced with destroy(), drop our ref immediately because
      // the node will be freed at the end of our gc_guard
      dir.reset();
    }
    return dir;
  }

  DirStorageRef get_or_create_dir(const mcas::gc_guard &guard,
                                  const boost::uuids::uuid &volume,
                                  ino_t ino, uint32_t stripe)
  {
    auto dir = dirs.get_or_create(guard, DirStorage(volume, ino, stripe));
    if (dir->unlinked) {
      // if our get() raced with destroy(), drop our ref immediately because
      // the node will be freed at the end of our gc_guard
      dir.reset();
    }
    return dir;
  }

  void destroy(const mcas::gc_guard &guard, DirStorageRef &&dir)
  {
    dirs.destroy(guard, std::move(dir));
  }
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_STORAGE_H
