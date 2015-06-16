// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Cache.h"
#include "Inode.h"
#include "Storage.h"
#include "Volume.h"

using namespace cohort::mds;

Cache::Cache(const Volume *volume, const mcas::gc_global &gc,
             const mcas::obj_cache &cache, Storage *storage,
             int highwater, int lowwater)
  : volume(volume),
    gc(gc),
    inodes(gc, cache, inode_cmp, highwater, lowwater),
    storage(storage),
    next_ino(1)
{
}

InodeRef Cache::create(const identity &who, int type)
{
  const auto ino = next_ino++;
  auto data = storage->get_or_create(volume->get_uuid(), ino, who, type);
  return inodes.get_or_create(Inode(this, ino, data));
}

InodeRef Cache::get(libmds_ino_t ino)
{
  auto inode = inodes.get_or_create(Inode(this, ino));
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
