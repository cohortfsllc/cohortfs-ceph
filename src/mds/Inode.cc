/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "Inode.h"
#include "Cache.h"
#include "Storage.h"
#include "Volume.h"

using namespace cohort::mds;

int Inode::getattr(ObjAttr &attrs) const
{
  attrs.filesize = inode->attr.filesize;
  attrs.mode = inode->attr.mode;
  attrs.group = inode->attr.group;
  attrs.user = inode->attr.user;
  attrs.atime = inode->attr.atime;
  attrs.mtime = inode->attr.mtime;
  attrs.ctime = inode->attr.ctime;
  attrs.nlinks = inode->attr.nlinks;
  attrs.rawdev = inode->attr.rawdev;
  return 0;
}

int Inode::setattr(int mask, const ObjAttr &attrs)
{
  if (mask & LIBMDS_ATTR_SIZE)
    inode->attr.filesize = attrs.filesize;
  if (mask & LIBMDS_ATTR_MODE) // preserve file type
    inode->attr.mode = (inode->attr.mode & S_IFMT) | (attrs.mode & ~S_IFMT);
  if (mask & LIBMDS_ATTR_GROUP)
    inode->attr.group = attrs.group;
  if (mask & LIBMDS_ATTR_OWNER)
    inode->attr.user = attrs.user;
  if (mask & LIBMDS_ATTR_ATIME)
    inode->attr.atime = attrs.atime;
  if (mask & LIBMDS_ATTR_MTIME)
    inode->attr.mtime = attrs.mtime;
  if (mask & LIBMDS_ATTR_CTIME)
    inode->attr.ctime = attrs.ctime;
  return 0;
}

bool Inode::fetch(const mcas::gc_guard &guard, Storage *storage)
{
  std::lock_guard<std::mutex> lock(mutex);
  switch (state) {
    case STATE_EMPTY:
      inode = storage->get_inode(guard, cache->get_volume()->get_uuid(),
                                 inodeno);
      if (inode) {
        state = STATE_VALID;
        return true;
      }
      state = STATE_NONEXISTENT;
      return false;
    case STATE_VALID:
      return true;
    default:
      return false;
  }
}

bool Inode::destroy(const mcas::gc_guard &guard, Storage *storage)
{
  if (!is_valid())
    return false;
  storage->destroy(guard, std::move(inode));
  state = STATE_NONEXISTENT;
  return true;
}
