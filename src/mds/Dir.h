/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_DIR_H
#define COHORT_MDS_DIR_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

class Cache;
class Storage;
struct DirStorage;
typedef boost::intrusive_ptr<DirStorage> DirStorageRef;

class Dir : public mcas::skiplist_object {
 public:
  enum State {
    /// directory hasn't been fetched from storage
    STATE_EMPTY,
    /// directory was fetched from storage and contains valid data
    STATE_VALID,
    /// directory was not found on storage and is cached as a negative entry
    STATE_NONEXISTENT,
  };

  mutable std::mutex mutex;

  typedef boost::intrusive_ptr<Dir> Ref;

 private:
  Cache *cache;
  ino_t inodeno;
  uint32_t stripe;
  State state;
  DirStorageRef dir;

 public:
  // search template for lookup
  Dir(Cache *cache, ino_t ino, uint32_t stripe)
    : cache(cache), inodeno(ino), stripe(stripe), state(STATE_EMPTY) {}

  // search template for create
  Dir(Cache *cache, ino_t ino, uint32_t stripe, DirStorageRef& dir)
    : cache(cache), inodeno(ino), stripe(stripe),
      state(STATE_VALID), dir(dir) {}

  // move constructor for cache inserts
  Dir(Dir &&o)
    : cache(nullptr), inodeno(0), stripe(0), state(STATE_EMPTY)
  {
    std::swap(cache, o.cache);
    std::swap(inodeno, o.inodeno);
    std::swap(stripe, o.stripe);
    std::swap(state, o.state);
    std::swap(dir, o.dir);
  }

  ino_t ino() const { return inodeno; }
  uint32_t get_stripe() const { return stripe; }

  uint32_t get_state() const  { return state; }
  bool is_empty() const       { return state == STATE_EMPTY; }
  bool is_valid() const       { return state == STATE_VALID; }
  bool is_nonexistent() const { return state == STATE_NONEXISTENT; }

  bool is_dir_notempty() const;

  int lookup(const std::string &name, ino_t *ino) const;
  int readdir(uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user) const;
  int link(const std::string &name, ino_t ino);
  int unlink(const std::string &name);

  // storage
  bool fetch(const mcas::gc_guard &guard, Storage *storage);
  bool destroy(const mcas::gc_guard &guard, Storage *storage);

  static int cmp(const void *lhs, const void *rhs)
  {
    const Dir *l = static_cast<const Dir*>(lhs);
    const Dir *r = static_cast<const Dir*>(rhs);
    if (l->inodeno < r->inodeno) return -1;
    if (l->inodeno > r->inodeno) return 1;
    if (l->stripe < r->stripe) return -1;
    if (l->stripe > r->stripe) return 1;
    return 0;
  }
};

typedef Dir::Ref DirRef;
inline void intrusive_ptr_add_ref(Dir *p) { p->get(); }
inline void intrusive_ptr_release(Dir *p) { p->put(); }

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_DIR_H
