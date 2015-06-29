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

bool Inode::is_dir_notempty() const
{
  if (!is_dir())
    return false;
  return !inode->dir.entries.empty();
}

int Inode::lookup(const std::string &name, ino_t *ino) const
{
  if (!is_dir())
    return -ENOTDIR;
  auto i = inode->dir.entries.find(name);
  if (i == inode->dir.entries.end())
    return -ENOENT;
  *ino = i->second;
  return 0;
}

int Inode::readdir(uint64_t pos, uint64_t gen,
                   libmds_readdir_fn cb, void *user) const
{
  if (!is_dir())
    return -ENOTDIR;

  if (pos > 0 && gen != inode->dir.gen)
    return -ESTALE;

  if (pos >= inode->dir.entries.size())
    return -EOF;

  // advance to the requested position
  auto i = inode->dir.entries.begin();
  for (uint64_t j = 0; j < pos; j++)
    ++i;

  // pass entries to the callback function until it returns an error
  for (; i != inode->dir.entries.end(); ++i) {
    int r = cb(i->first.c_str(), i->second, ++pos, inode->dir.gen, user);
    if (r)
      break;
  }
  return 0;
}

int Inode::link(const std::string &name, ino_t ino)
{
  if (!is_dir())
    return -ENOTDIR;

  auto i = inode->dir.entries.insert(std::make_pair(name, ino));
  if (!i.second)
    return -EEXIST;

  inode->dir.gen++;
  return 0;
}

int Inode::unlink(const std::string &name)
{
  if (!is_dir())
    return -ENOTDIR;

  auto i = inode->dir.entries.find(name);
  if (i == inode->dir.entries.end())
    return -ENOENT;

  inode->dir.entries.erase(i);
  inode->dir.gen++;
  return 0;
}

bool Inode::fetch(const mcas::gc_guard &guard, Storage *storage)
{
  std::lock_guard<std::mutex> lock(mutex);
  switch (state) {
    case STATE_EMPTY:
      inode = storage->get(guard, cache->get_volume()->get_uuid(), inodeno);
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
