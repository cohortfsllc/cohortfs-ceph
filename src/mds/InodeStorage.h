/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_INODE_STORAGE_H
#define COHORT_MDS_INODE_STORAGE_H

#include <boost/uuid/uuid.hpp>
#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

struct InodeStorage : public mcas::skiplist_object {
  boost::uuids::uuid volume;
  ino_t ino;
  ObjAttr attr;
  uint32_t stripes;

  // search template for lookup
  InodeStorage(const boost::uuids::uuid &volume, ino_t ino)
    : volume(volume), ino(ino) {}

  // search template for create
  InodeStorage(const boost::uuids::uuid &volume, ino_t ino,
               const identity_t &who, int mode, uint32_t stripes = 0)
    : volume(volume), ino(ino), stripes(stripes)
  {
    attr.filesize = 0;
    attr.mode = mode & ~S_IFMT;
    attr.user = who.uid;
    attr.group = who.gid;
    attr.atime = attr.mtime = attr.ctime = ceph::real_clock::now();
    attr.nlinks = 1;
    attr.type = mode & S_IFMT;
    attr.rawdev = 0;
  }

  // move constructor for cache inserts
  InodeStorage(InodeStorage &&o)
    : ino(0), stripes(0)
  {
    std::swap(volume, o.volume);
    std::swap(ino, o.ino);
    std::swap(attr, o.attr);
    std::swap(stripes, o.stripes);
  }

  static int cmp(const void *lhs, const void *rhs)
  {
    const InodeStorage *l = static_cast<const InodeStorage*>(lhs);
    const InodeStorage *r = static_cast<const InodeStorage*>(rhs);
    if (l->ino > r->ino) return 1;
    if (l->ino < r->ino) return -1;
    if (l->volume > r->volume) return 1;
    if (l->volume < r->volume) return -1;
    return 0;
  }
};
typedef boost::intrusive_ptr<InodeStorage> InodeStorageRef;
inline void intrusive_ptr_add_ref(InodeStorage *p) { p->get(); }
inline void intrusive_ptr_release(InodeStorage *p) { p->put(); }

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_INODE_STORAGE_H
