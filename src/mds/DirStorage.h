/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_DIR_STORAGE_H
#define COHORT_MDS_DIR_STORAGE_H

#include <map>
#include <boost/uuid/uuid.hpp>
#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

struct DirStorage : public mcas::skiplist_object {
  boost::uuids::uuid volume;
  ino_t ino;
  uint32_t stripe;
  std::map<std::string, ino_t> entries;
  uint64_t gen; // for readdir verf
  bool unlinked;

  // search template
  DirStorage(const boost::uuids::uuid &volume, ino_t ino, uint32_t stripe)
    : volume(volume), ino(ino), stripe(stripe), gen(0), unlinked(false) {}

  // move constructor for cache inserts
  DirStorage(DirStorage &&o)
    : ino(0), stripe(0), gen(0), unlinked(true)
  {
    std::swap(volume, o.volume);
    std::swap(ino, o.ino);
    std::swap(stripe, o.stripe);
    std::swap(entries, o.entries);
    std::swap(gen, o.gen);
    std::swap(unlinked, o.unlinked);
  }

  static int cmp(const void *lhs, const void *rhs)
  {
    const DirStorage *l = static_cast<const DirStorage*>(lhs);
    const DirStorage *r = static_cast<const DirStorage*>(rhs);
    if (l->ino > r->ino) return 1;
    if (l->ino < r->ino) return -1;
    if (l->stripe > r->stripe) return 1;
    if (l->stripe < r->stripe) return -1;
    if (l->volume > r->volume) return 1;
    if (l->volume < r->volume) return -1;
    return 0;
  }
};
typedef boost::intrusive_ptr<DirStorage> DirStorageRef;
inline void intrusive_ptr_add_ref(DirStorage *p) { p->get(); }
inline void intrusive_ptr_release(DirStorage *p) { p->put(); }

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_DIR_STORAGE_H
