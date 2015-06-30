// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Cache.h"
#include "Dentry.h"
#include "Dir.h"
#include "Inode.h"
#include "Storage.h"
#include "Volume.h"

using namespace cohort::mds;

Cache::Cache(const Volume *volume, const mcas::gc_global &gc,
             const mcas::obj_cache &inode_cache,
             const mcas::obj_cache &dir_cache,
             const mcas::obj_cache &dentry_cache,
             Storage *storage, int highwater, int lowwater)
  : volume(volume),
    gc(gc),
    inodes(gc, inode_cache, Inode::cmp, highwater, lowwater),
    dirs(gc, dir_cache, Dir::cmp, highwater, lowwater),
    dentries(gc, dentry_cache, Dentry::cmp, highwater, lowwater),
    storage(storage),
    next_ino(1)
{
}

InodeRef Cache::create_inode(const mcas::gc_guard &guard,
                             const identity &who, int type, uint32_t stripes)
{
  const auto ino = next_ino++;
  auto data = storage->get_or_create_inode(guard, volume->get_uuid(),
                                           ino, who, type, stripes);
  return inodes.get_or_create(guard, Inode(this, ino, data));
}

InodeRef Cache::get_inode(const mcas::gc_guard &guard, ino_t ino)
{
  auto inode = inodes.get_or_create(guard, Inode(this, ino));
  if (!inode->fetch(guard, storage))
    return nullptr;
  return inode;
}

DirRef Cache::get_dir(const mcas::gc_guard &guard, ino_t ino, uint32_t stripe)
{
  auto dir = dirs.get_or_create(guard, Dir(this, ino, stripe));
  if (!dir->fetch(guard, storage))
    return nullptr;
  return dir;
}

DentryRef Cache::lookup(const mcas::gc_guard &guard, const Dir *parent,
                        const std::string &name, bool return_nonexistent)
{
  auto dn = dentries.get_or_create(guard, Dentry(parent->ino(), name));
  // TODO: lock dentry
  if (dn->is_empty()) {
    // TODO: lock parent inode
    ino_t ino;
    int r = parent->lookup(name, &ino);
    if (r == 0)
      dn->link(ino);
    else
      dn->unlink();
  }
  if (dn->is_nonexistent() && !return_nonexistent)
    return nullptr;
  return dn;
}

DentryRef Cache::lookup(const mcas::gc_guard &guard, ino_t parent,
                        uint32_t stripe, const std::string &name,
                        bool return_nonexistent)
{
  auto dn = dentries.get_or_create(guard, Dentry(parent, name));
  // TODO: lock dentry
  if (dn->is_empty()) {
    // TODO: drop lock over directory fetch
    auto dir = get_dir(guard, parent, stripe);
    // TODO: lock parent directory
    ino_t ino;
    int r = dir->lookup(name, &ino);
    if (r == 0)
      dn->link(ino);
    else
      dn->unlink();
  }
  if (dn->is_nonexistent() && !return_nonexistent)
    return nullptr;
  return dn;
}
