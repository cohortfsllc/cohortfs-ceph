/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_STORAGE_H
#define COHORT_MDS_STORAGE_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

struct InodeStorage : public mcas::skiplist<InodeStorage>::object {
  _inodeno_t inodeno;
  ObjAttr attr;

  struct Dir {
    Dir() : gen(0) {}
    std::map<std::string, _inodeno_t> entries;
    uint64_t gen; // for readdir verf
  } dir;

  // search template for lookup
  InodeStorage(_inodeno_t ino)
    : inodeno(ino) {}

  // search template for create
  InodeStorage(_inodeno_t ino, const identity &who, int type)
    : inodeno(ino)
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
    std::swap(inodeno, o.inodeno);
    std::swap(attr, o.attr);
    std::swap(dir.entries, o.dir.entries);
    std::swap(dir.gen, o.dir.gen);
  }

  static int cmp(const void *lhs, const void *rhs)
  {
    const InodeStorage *l = static_cast<const InodeStorage*>(lhs);
    const InodeStorage *r = static_cast<const InodeStorage*>(rhs);
    if (l->inodeno == r->inodeno)
      return 0;
    if (l->inodeno > r->inodeno)
      return 1;
    return -1;
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
  Storage(const mcas::gc_global &gc)
    : gc(gc),
      skiplist(gc, InodeStorage::cmp, "inode_store")
  {}

  InodeStorageRef get(_inodeno_t ino)
  {
    return skiplist.get(InodeStorage(ino));
  }

  InodeStorageRef get_or_create(_inodeno_t ino, const identity &who, int type)
  {
    return skiplist.get_or_create(InodeStorage(ino, who, type));
  }

  void destroy(InodeStorageRef &&inode)
  {
    skiplist.destroy(std::move(inode));
  }
};

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_STORAGE_H
