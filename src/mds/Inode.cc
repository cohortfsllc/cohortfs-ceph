/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "Inode.h"
#include "Cache.h"
#include "Storage.h"
#include "Volume.h"

using namespace cohort::mds;

static void copy_attrs(int mask, ObjAttr &to, const ObjAttr &from)
{
  if (mask & ATTR_SIZE) to.filesize = from.filesize;
  if (mask & ATTR_MODE) to.mode = from.mode;
  if (mask & ATTR_GROUP) to.group = from.group;
  if (mask & ATTR_OWNER) to.user = from.user;
  if (mask & ATTR_ATIME) to.atime = from.atime;
  if (mask & ATTR_MTIME) to.mtime = from.mtime;
  if (mask & ATTR_CTIME) to.ctime = from.ctime;
  if (mask & ATTR_NLINKS) to.nlinks = from.nlinks;
  if (mask & ATTR_TYPE) to.type = from.type;
  if (mask & ATTR_RAWDEV) to.rawdev = from.rawdev;
}

int Inode::getattr(int mask, ObjAttr &attrs) const
{
  copy_attrs(mask, attrs, inode->attr);
  return 0;
}

int Inode::setattr(int mask, const ObjAttr &attrs)
{
  if (mask & ATTR_TYPE) // can't change type with setattr
    return -EINVAL;

  copy_attrs(mask, inode->attr, attrs);
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
