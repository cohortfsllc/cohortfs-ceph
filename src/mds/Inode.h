/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_INODE_H
#define COHORT_MDS_INODE_H

#include <mutex>

#include "common/cohort_function.h"
#include "include/types.h" // inodeno_t
#include "mds_types.h"

namespace cohort {
namespace mds {

class Inode {
 public:
  const _inodeno_t ino;
 private:
  mutable std::mutex mtx;
  ObjAttr attr;

  struct Dir {
    Dir() : gen(0) {}
    mutable std::mutex mtx;
    std::map<std::string, Inode*> entries;
    uint64_t gen; // for readdir verf
  } dir;
 public:
  Inode(_inodeno_t ino, const identity &who, int type);

  // attr.type is immutable, so we don't need to lock these
  bool is_reg() const { return S_ISREG(attr.type); }
  bool is_dir() const { return S_ISDIR(attr.type); }

  int adjust_nlinks(int n) { return attr.nlinks += n; }

  int getattr(int mask, ObjAttr &attrs) const;
  int setattr(int mask, const ObjAttr &attrs);

  int lookup(const std::string &name, Inode **obj) const;
  int readdir(uint64_t pos, uint64_t gen,
              libmds_readdir_fn cb, void *user) const;
  int link(const std::string &name, Inode *obj);
  int unlink(const std::string &name, Inode **obj);
};

} // namespace mds
} // namespace cohort

#endif /* COHORT_MDS_INODE_H */
