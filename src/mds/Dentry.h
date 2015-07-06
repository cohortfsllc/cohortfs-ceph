/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_DENTRY_H
#define COHORT_MDS_DENTRY_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"

namespace cohort {
namespace mds {

class Inode;
typedef boost::intrusive_ptr<Inode> InodeRef;

class Cache;

class Dentry : public mcas::skiplist_object {
 public:
  typedef boost::intrusive_ptr<Dentry> Ref;
  mutable std::mutex mutex;

 private:
  enum State {
    /// dentry hasn't been fetched from storage
    STATE_EMPTY,
    /// dentry was fetched from storage and contains valid data
    STATE_VALID,
    /// dentry was not found on storage and is cached as a negative entry
    STATE_NONEXISTENT,
  };

  ino_t parent;
  std::string name;
  ino_t inodeno;
  State state;

 public:
  // search template
  // TODO: avoid std::string alloc/copy
  Dentry(ino_t parent, const std::string &name)
    : parent(parent), name(name), inodeno(0), state(STATE_EMPTY) {}

  // move constructor for cache inserts
  Dentry(Dentry &&o)
    : parent(0), inodeno(0), state(STATE_EMPTY)
  {
    std::swap(parent, o.parent);
    std::swap(name, o.name);
    std::swap(inodeno, o.inodeno);
  }

  ino_t get_parent() const { return parent; }
  const std::string& get_name() const { return name; }
  ino_t ino() const { return inodeno; }

  bool is_empty() const       { return state == STATE_EMPTY; }
  bool is_valid() const       { return state == STATE_VALID; }
  bool is_nonexistent() const { return state == STATE_NONEXISTENT; }

  void link(ino_t ino) {
    inodeno = ino;
    state = STATE_VALID;
  }
  void unlink() {
    state = STATE_NONEXISTENT;
  }

  static int cmp(const void *lhs, const void *rhs)
  {
    const Dentry *l = static_cast<const Dentry*>(lhs);
    const Dentry *r = static_cast<const Dentry*>(rhs);
    if (l->parent < r->parent) return -1;
    if (l->parent > r->parent) return 1;
    return l->name.compare(r->name);
  }
};

typedef Dentry::Ref DentryRef;
inline void intrusive_ptr_add_ref(Dentry *p) { p->get(); }
inline void intrusive_ptr_release(Dentry *p) { p->put(); }

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_DENTRY_H
