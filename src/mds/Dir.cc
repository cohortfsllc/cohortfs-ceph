/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#include "Dir.h"
#include "Cache.h"
#include "Storage.h"
#include "Volume.h"

using namespace cohort::mds;

bool Dir::is_dir_notempty() const
{
  return !dir->entries.empty();
}

int Dir::lookup(const std::string &name, ino_t *ino) const
{
  auto i = dir->entries.find(name);
  if (i == dir->entries.end())
    return -ENOENT;
  *ino = i->second;
  return 0;
}

int Dir::readdir(uint64_t pos, uint64_t gen,
                 libmds_readdir_fn cb, void *user) const
{
  uint32_t p = pos & 0xffffffff; // mask off the stripe index

  if (p > 0 && gen != dir->gen)
    return -ESTALE;

  if (p >= dir->entries.size())
    return -EOF;

  // advance to the requested position
  auto i = dir->entries.begin();
  for (uint64_t j = 0; j < p; j++)
    ++i;

  // pass entries to the callback function until it returns an error
  int r = -EOF;
  for (; i != dir->entries.end(); ++i) {
    if (i == dir->entries.rbegin().base())
      pos = static_cast<uint64_t>(stripe + 1) << 32;
    else
      pos++;
    r = cb(i->first.c_str(), i->second, pos, dir->gen, user);
    if (r)
      break;
  }
  return r;
}

int Dir::link(const std::string &name, ino_t ino)
{
  auto i = dir->entries.insert(std::make_pair(name, ino));
  if (!i.second)
    return -EEXIST;

  dir->gen++;
  return 0;
}

int Dir::unlink(const std::string &name)
{
  auto i = dir->entries.find(name);
  if (i == dir->entries.end())
    return -ENOENT;

  dir->entries.erase(i);
  dir->gen++;
  return 0;
}

bool Dir::fetch(const mcas::gc_guard &guard, Storage *storage)
{
  std::lock_guard<std::mutex> lock(mutex);
  switch (state) {
    case STATE_EMPTY:
      dir = storage->get_dir(guard, cache->get_volume()->get_uuid(),
                             inodeno, stripe);
      if (dir) {
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

bool Dir::destroy(const mcas::gc_guard &guard, Storage *storage)
{
  if (!is_valid())
    return false;
  storage->destroy(guard, std::move(dir));
  state = STATE_NONEXISTENT;
  return true;
}
