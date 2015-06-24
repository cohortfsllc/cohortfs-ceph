/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_STORAGE_H
#define COHORT_MDS_STORAGE_H

#include <unordered_map>
#include <boost/uuid/uuid.hpp>
#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

struct InodeStorage : public mcas::skiplist_object {
  boost::uuids::uuid volume;
  libmds_ino_t inodeno;
  ObjAttr attr;

  struct Dir {
    Dir() : gen(0) {}
    std::unordered_map<std::string, libmds_ino_t> entries;
    uint64_t gen; // for readdir verf
  } dir;

  // search template for lookup
  InodeStorage(const boost::uuids::uuid &volume, libmds_ino_t ino)
    : volume(volume), inodeno(ino) {}

  // search template for create
  InodeStorage(const boost::uuids::uuid &volume, libmds_ino_t ino,
               const identity &who, int type)
    : volume(volume), inodeno(ino)
  {
    attr.filesize = 0;
    attr.mode = 0777;
    attr.user = who.uid;
    attr.group = who.gid;
    attr.atime = attr.mtime = attr.ctime = ceph::real_clock::now();
    attr.nlinks = 1;
    attr.type = type;
    attr.rawdev = 0;
  }

  // move constructor for cache inserts
  InodeStorage(InodeStorage &&o)
    : inodeno(0)
  {
    std::swap(volume, o.volume);
    std::swap(inodeno, o.inodeno);
    std::swap(attr, o.attr);
    std::swap(dir.entries, o.dir.entries);
    std::swap(dir.gen, o.dir.gen);
  }

  static int cmp(const void *lhs, const void *rhs)
  {
    const InodeStorage *l = static_cast<const InodeStorage*>(lhs);
    const InodeStorage *r = static_cast<const InodeStorage*>(rhs);
    if (l->inodeno > r->inodeno)
      return 1;
    if (l->inodeno < r->inodeno)
      return -1;
    if (l->volume > r->volume)
      return 1;
    if (l->volume < r->volume)
      return -1;
    return 0;
  }
};
typedef boost::intrusive_ptr<InodeStorage> InodeStorageRef;
inline void intrusive_ptr_add_ref(InodeStorage *p) { p->get(); }
inline void intrusive_ptr_release(InodeStorage *p) { p->put(); }

class Storage {
 private:
  const mcas::gc_global &gc;
  mcas::skiplist<InodeStorage> skiplist;
 public:
  Storage(const mcas::gc_global &gc, const mcas::obj_cache &cache)
    : gc(gc),
      skiplist(gc, cache, InodeStorage::cmp)
  {}

  InodeStorageRef get(const mcas::gc_guard &guard,
                      const boost::uuids::uuid &volume, libmds_ino_t ino)
  {
    auto inode = skiplist.get(guard, InodeStorage(volume, ino));
    if (inode->attr.nlinks == 0) {
      // if our get() raced with destroy(), drop our ref immediately because
      // the node will be freed at the end of our gc_guard
      inode.reset();
    }
    return inode;
  }

  InodeStorageRef get_or_create(const mcas::gc_guard &guard,
                                const boost::uuids::uuid &volume,
                                libmds_ino_t ino, const identity &who, int type)
  {
    auto inode = skiplist.get_or_create(guard, InodeStorage(volume, ino,
                                                            who, type));
    if (inode->attr.nlinks == 0) {
      // if our get() raced with destroy(), drop our ref immediately because
      // the node will be freed at the end of our gc_guard
      inode.reset();
    }
    return inode;
  }

  void destroy(const mcas::gc_guard &guard, InodeStorageRef &&inode)
  {
    skiplist.destroy(guard, std::move(inode));
  }
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_STORAGE_H
