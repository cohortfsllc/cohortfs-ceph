// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Cache.h"
#include "Dentry.h"
#include "Inode.h"
#include "Storage.h"
#include "Volume.h"

using namespace cohort::mds;

Cache::Cache(const Volume *volume, const mcas::gc_global &gc,
             const mcas::obj_cache &inode_cache,
             const mcas::obj_cache &dentry_cache,
             Storage *storage, int highwater, int lowwater)
  : volume(volume),
    gc(gc),
    inodes(gc, inode_cache, inode_cmp, highwater, lowwater),
    dentries(gc, dentry_cache, dentry_cmp, highwater, lowwater),
    storage(storage),
    next_ino(1)
{
}

InodeRef Cache::create(const mcas::gc_guard &guard,
                       const identity &who, int type)
{
  const auto ino = next_ino++;
  auto data = storage->get_or_create(guard, volume->get_uuid(), ino, who, type);
  return inodes.get_or_create(guard, Inode(this, ino, data));
}

InodeRef Cache::get(const mcas::gc_guard &guard, libmds_ino_t ino)
{
  auto inode = inodes.get_or_create(guard, Inode(this, ino));
  if (!inode->fetch(guard, storage))
    return nullptr;
  return inode;
}

DentryRef Cache::lookup(const mcas::gc_guard &guard, libmds_ino_t parent,
                        const std::string &name)
{
  auto dn = dentries.get_or_create(guard, Dentry(parent, name));
  if (!dn->fetch(guard, this, storage))
    return nullptr;
  return dn;
}

DentryRef Cache::unlink(const mcas::gc_guard &guard, libmds_ino_t parent,
                        const std::string &name)
{
  return dentries.get_or_create(guard, Dentry(parent, name, true));
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

int Cache::dentry_cmp(const void *lhs, const void *rhs)
{
  const Dentry *l = static_cast<const Dentry*>(lhs);
  const Dentry *r = static_cast<const Dentry*>(rhs);
  if (l->get_parent() < r->get_parent())
    return -1;
  if (l->get_parent() > r->get_parent())
    return 1;
  return l->get_name().compare(r->get_name());
}
