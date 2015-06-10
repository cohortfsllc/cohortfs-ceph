/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "Inode.h"
#include "Cache.h"
#include "Storage.h"

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
  std::lock_guard<std::mutex> lock(mutex);
  copy_attrs(mask, attrs, inode->attr);
  return 0;
}

int Inode::setattr(int mask, const ObjAttr &attrs)
{
  if (mask & ATTR_TYPE) // can't change type with setattr
    return -EINVAL;

  std::lock_guard<std::mutex> lock(mutex);
  copy_attrs(mask, inode->attr, attrs);
  return 0;
}

int Inode::lookup(const std::string &name, _inodeno_t *ino) const
{
  if (!is_dir())
    return -ENOTDIR;

  std::lock_guard<std::mutex> lock(dir_mutex);
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

  std::lock_guard<std::mutex> lock(dir_mutex);
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

int Inode::link(const std::string &name, _inodeno_t ino)
{
  if (!is_dir())
    return -ENOTDIR;

  std::lock_guard<std::mutex> lock(dir_mutex);
  auto i = inode->dir.entries.insert(std::make_pair(name, ino));
  if (!i.second)
    return -EEXIST;
  inode->dir.gen++;
  return 0;
}

int Inode::unlink(const std::string &name, Cache *cache, Ref *unlinked)
{
  if (!is_dir())
    return -ENOTDIR;

  std::lock_guard<std::mutex> lock(dir_mutex);
  // find the directory entry
  auto i = inode->dir.entries.find(name);
  if (i == inode->dir.entries.end())
    return -ENOENT;

  // fetch the child inode
  auto child = cache->get(i->second); // XXX: fetch under dir mutex
  assert(child);

  // child must be empty
  std::lock_guard<std::mutex> child_lock(child->dir_mutex);
  if (child->is_valid() && !child->inode->dir.entries.empty())
    return -ENOTEMPTY;

  *unlinked = child;
  inode->dir.entries.erase(i);
  inode->dir.gen++;
  return 0;
}

bool Inode::fetch(Storage *storage)
{
  std::lock_guard<std::mutex> lock(mutex);
  switch (state) {
    case STATE_EMPTY:
      assert(is_empty());
      inode = storage->get(inodeno);
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

bool Inode::destroy(Storage *storage)
{
  std::lock_guard<std::mutex> lock(mutex);
  if (!is_valid())
    return false;
  storage->destroy(std::move(inode));
  state = STATE_NONEXISTENT;
  return true;
}
