/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_INODE_H
#define COHORT_MDS_INODE_H

#include <mutex>

#include "common/cohort_function.h"
#include "common/mcas_skiplist.h"
#include "include/types.h" // inodeno_t
#include "mds_types.h"

namespace cohort {
namespace mds {

class Inode : public mcas::skiplist<Inode>::object {
 public:
  /// inode was created, and hasn't been written to storage
  static constexpr uint32_t STATE_NEW =         0x01;
  /// inode was not found on storage and is cached as a negative entry
  static constexpr uint32_t STATE_NONEXISTENT = 0x02;
  /// inode was destroyed because numlinks went to 0
  static constexpr uint32_t STATE_DESTROYED =   0x04;

  typedef boost::intrusive_ptr<Inode> Ref;
 private:
  _inodeno_t inodeno;
  uint32_t state;
  mutable std::mutex mtx;
  ObjAttr attr;

  struct Dir {
    Dir() : gen(0) {}
    mutable std::mutex mtx;
    std::map<std::string, Ref> entries;
    uint64_t gen; // for readdir verf
  } dir;

 public:
  // search template for lookup
  Inode(_inodeno_t ino)
    : inodeno(ino), state(STATE_NONEXISTENT) {}

  // search template for create
  Inode(_inodeno_t ino, const identity &who, int type);

  // move constructor for cache inserts
  Inode(Inode &&o) : inodeno(0) {
    std::swap(inodeno, o.inodeno);
    std::swap(state, o.state);
    std::swap(attr, o.attr);
    std::swap(dir.entries, o.dir.entries);
    std::swap(dir.gen, o.dir.gen);
  }

  _inodeno_t ino() const { return inodeno; }

  uint32_t get_state() const { return state; }
  bool is_new() const         { return state & STATE_NEW; }
  bool is_nonexistent() const { return state & STATE_NONEXISTENT; }
  bool is_destroyed() const   { return state & STATE_DESTROYED; }

  void set_destroyed() { state = STATE_DESTROYED; }

  // attr.type is immutable, so we don't need to lock these
  bool is_reg() const { return S_ISREG(attr.type); }
  bool is_dir() const { return S_ISDIR(attr.type); }

  int adjust_nlinks(int n) { return attr.nlinks += n; }

  int getattr(int mask, ObjAttr &attrs) const;
  int setattr(int mask, const ObjAttr &attrs);

  int lookup(const std::string &name, Ref *inode) const;
  int readdir(uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user) const;
  int link(const std::string &name, const Ref& inode);
  int unlink(const std::string &name, Ref *inode);
};

typedef Inode::Ref InodeRef;
inline void intrusive_ptr_add_ref(Inode *p) { p->get(); }
inline void intrusive_ptr_release(Inode *p) { p->put(); }

} // namespace mds
} // namespace cohort

#endif /* COHORT_MDS_INODE_H */
