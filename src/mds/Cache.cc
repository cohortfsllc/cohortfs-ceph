// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Cache.h"
#include "Inode.h"
#include "Storage.h"

using namespace cohort::mds;

Cache::Cache(const mcas::gc_global &gc, Storage *storage,
             int highwater, int lowwater)
  : gc(gc),
    inodes(gc, inode_cmp, "inodes", highwater, lowwater),
    storage(storage),
    next_ino(1)
{
}

InodeRef Cache::create(const identity &who, int type)
{
  const auto ino = next_ino++;
  auto data = storage->get_or_create(ino, who, type);
  return inodes.get_or_create(Inode(ino, data));
}

InodeRef Cache::get(_inodeno_t ino)
{
  auto inode = inodes.get_or_create(Inode(ino));
  if (!inode->fetch(storage))
    return nullptr;
  return inode;
}

int Cache::inode_cmp(const void *lhs, const void *rhs)
{
  const Inode *l = static_cast<const Inode*>(lhs);
  const Inode *r = static_cast<const Inode*>(rhs);
  if (l->ino() == r->ino())
    return 0;
  if (l->ino() > r->ino())
    return 1;
  return -1;
}
