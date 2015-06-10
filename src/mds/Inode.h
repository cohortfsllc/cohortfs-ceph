/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_INODE_H
#define COHORT_MDS_INODE_H

#include "common/mcas_skiplist.h"
#include "mds_types.h"
#include "Storage.h"

namespace cohort {
namespace mds {

class Cache;
class Storage;
struct InodeStorage;
typedef boost::intrusive_ptr<InodeStorage> InodeStorageRef;

class Inode : public mcas::skiplist<Inode>::object {
 public:
  enum State {
    /// inode hasn't been fetched from storage
    STATE_EMPTY,
    /// inode was fetched from storage and contains valid data
    STATE_VALID,
    /// inode was not found on storage and is cached as a negative entry
    STATE_NONEXISTENT,
  };

  typedef boost::intrusive_ptr<Inode> Ref;
 private:
  mutable std::mutex mutex;
  mutable std::mutex dir_mutex;
  _inodeno_t inodeno;
  State state;
  InodeStorageRef inode;

 public:
  // search template for lookup
  Inode(_inodeno_t ino)
    : inodeno(ino), state(STATE_EMPTY) {}

  // search template for create
  Inode(_inodeno_t ino, InodeStorageRef& inode)
    : inodeno(ino), state(STATE_VALID), inode(inode) {}

  // move constructor for cache inserts
  Inode(Inode &&o)
    : inodeno(0)
  {
    std::swap(inodeno, o.inodeno);
    std::swap(state, o.state);
    std::swap(inode, o.inode);
  }

  _inodeno_t ino() const { return inodeno; }

  uint32_t get_state() const  { return state; }
  bool is_empty() const       { return state & STATE_EMPTY; }
  bool is_valid() const       { return state & STATE_VALID; }
  bool is_nonexistent() const { return state & STATE_NONEXISTENT; }

  // attr.type is immutable, so we don't need to lock these
  bool is_reg() const { return S_ISREG(inode->attr.type); }
  bool is_dir() const { return S_ISDIR(inode->attr.type); }

  int adjust_nlinks(int n) { return inode->attr.nlinks += n; }

  int getattr(int mask, ObjAttr &attrs) const;
  int setattr(int mask, const ObjAttr &attrs);

  int lookup(const std::string &name, _inodeno_t *ino) const;
  int readdir(uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user) const;
  int link(const std::string &name, _inodeno_t ino);
  int unlink(const std::string &name, Cache *cache, Ref *unlinked);

  // storage
  bool fetch(Storage *storage);
  bool destroy(Storage *storage);
};

typedef Inode::Ref InodeRef;
inline void intrusive_ptr_add_ref(Inode *p) { p->get(); }
inline void intrusive_ptr_release(Inode *p) { p->put(); }

} // namespace mds
} // namespace cohort

#endif // COHORT_MDS_INODE_H
