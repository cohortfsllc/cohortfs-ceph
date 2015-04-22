// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "FragTreeIndex.h"
#include "common/debug.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_filestore

#define INDEX_FILENAME ".index"
#define SIZES_FILENAME ".sizes"

using namespace cohort;

FragTreeIndex::FragTreeIndex(CephContext *cct, int split_threshold,
                             int split_bits)
  : cct(cct),
    split_threshold(split_threshold),
    split_bits(split_bits),
    rootfd(-1)
{
}

FragTreeIndex::~FragTreeIndex()
{
#if 1 // don't blow up on test failures
  if (rootfd != -1)
    ::close(rootfd);
#else
  assert(rootfd == -1); // must not be mounted
#endif
}

int FragTreeIndex::init(const std::string &path)
{
  // must not be mounted
  if (rootfd != -1)
    return -EINVAL;

  // open root fd
  int dirfd = ::open(path.c_str(), O_RDONLY, 0644);
  if (dirfd == -1)
    return -errno;

  // TODO: verify that we opened a directory
  // TODO: verify that the directory is empty

  // write empty index to disk
  std::lock_guard<std::shared_timed_mutex> lock(index_mutex);
  committed.tree.clear();
  committed.splits.clear();
  committed.merges.clear();

  int r = write_index(dirfd);
  ::close(dirfd);
  return r;
}

int FragTreeIndex::mount(const std::string &path, bool async_recovery)
{
  // must not be mounted
  if (rootfd != -1)
    return -EINVAL;

  // open root fd
  rootfd = ::open(path.c_str(), O_RDONLY, 0644);
  if (rootfd == -1)
    return -errno;

  // sizes lock deferred because a later declaration would cross goto
  std::unique_lock<std::mutex> sizes_lock(sizes_mutex, std::defer_lock);

  // read index from disk
  std::unique_lock<std::shared_timed_mutex> index_wrlock(index_mutex);
  int r = read_index(rootfd);
  index_wrlock.unlock();
  if (r)
    goto out_close;

  // read sizes from disk
  sizes_lock.lock();
  r = read_sizes(rootfd);
  if (r == -ENOENT) {
    // fresh index or not unmounted cleanly
    std::shared_lock<std::shared_timed_mutex> index_rdlock(index_mutex);
    r = count_sizes(rootfd);
  }
  sizes_lock.unlock();
  if (r)
    goto out_close;

  if (async_recovery) {
    // restart unfinished migrations
    restart_migrations(true);
  }
  // else, caller is expected to call restart_migrations()
  return 0;

out_close:
  ::close(rootfd);
  rootfd = -1;
  return r;
}

void FragTreeIndex::restart_migrations(bool async)
{
  for (auto i = committed.splits.begin(); i != committed.splits.end(); ) {
    const frag_t &frag = i->first;
    const int bits = i->second;
    // if recovery is synchronous, finish_split() will erase i from
    // committed.splits, so we increment i here before it's invalidated
    ++i;

    frag_path path;
    // use 'committed.tree' for splits, because it still has frag as a leaf
    int r = path.build(committed.tree, frag.value());
    assert(r == 0);
    assert(path.frag == frag);

    auto fn = [=]() {
      frag_size_map size_updates;
      do_split(path, bits, size_updates);
      finish_split(frag, size_updates);
    };
    if (async) {
      auto t = migration_threads.insert(std::make_pair(frag, std::thread(fn)));
      assert(t.second); // must not have a thread running for this frag
    } else {
      fn();
    }
  }

  for (auto i = committed.merges.begin(); i != committed.merges.end(); ) {
    const frag_t &frag = i->first;
    const int bits = i->second;
    ++i;

    frag_path path;
    // use uncommitted 'tree' for merges, because it now has frag as a leaf
    int r = path.build(tree, frag.value());
    assert(r == 0);
    assert(path.frag == frag);

    auto fn = [=]() {
      do_merge(path, bits);
      finish_merge(frag);
    };
    if (async) {
      auto t = migration_threads.insert(std::make_pair(frag, std::thread(fn)));
      assert(t.second); // must not have a thread running for this frag
    } else {
      fn();
    }
  }
}

int FragTreeIndex::unmount()
{
  // must be mounted
  if (rootfd == -1)
    return -EINVAL;

  // TODO: stop migration threads (and apply any size updates from splits!)

  // write sizes to disk
  std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
  int r = write_sizes(rootfd);
  sizes_lock.unlock();

  ::close(rootfd);
  rootfd = -1;
  return r;
}

int FragTreeIndex::lookup(const std::string &name, uint64_t hash)
{
  struct stat st;
  return stat(name, hash, &st);
}

int FragTreeIndex::stat(const std::string &name, uint64_t hash, struct stat *st)
{
  int r = -ENOENT;
  struct frag_path path, orig;
  {
    // build paths under index rdlock
    std::shared_lock<std::shared_timed_mutex> lock(index_mutex);
    r = path.build(tree, hash);
    if (r) return r;
    r = orig.build(committed.tree, hash);
    if (r) return r;
  }

  // if a migration is in progress, check the original location first
  if (orig.frag != path.frag) {
    r = orig.append(name.c_str(), name.size());
    if (r) return r;

    r = ::fstatat(rootfd, orig.path, st, 0);
    if (r == 0)
      return r;
    r = errno;
    if (r != ENOENT) {
      derr << "fstatat failed for original path " << orig.path << ": "
          << cpp_strerror(r) << dendl;
      return -r;
    }
    // on ENOENT, fall back to expected location
  }

  // check its expected location
  r = path.append(name.c_str(), name.size());
  if (r) return r;

  r = ::fstatat(rootfd, path.path, st, 0);
  if (r < 0) {
    r = -errno;
    derr << "fstatat failed for path " << path.path << ": "
        << cpp_strerror(-r) << dendl;
  }
  return r;
}

int FragTreeIndex::open(const std::string &name, uint64_t hash,
                        bool create, int *fd)
{
  int r = -ENOENT;
  struct frag_path path, orig;
  {
    // build paths under index rdlock
    std::shared_lock<std::shared_timed_mutex> lock(index_mutex);
    r = path.build(tree, hash);
    if (r) return r;
    r = orig.build(committed.tree, hash);
    if (r) return r;
  }

  // if a migration is in progress, check the original location first
  if (orig.frag != path.frag) {
    r = orig.append(name.c_str(), name.size());
    if (r) return r;

    r = ::openat(rootfd, orig.path, O_RDWR);
    if (r >= 0) {
      *fd = r;
      return 0;
    }
    r = errno;
    if (r != ENOENT) {
      derr << "open " << orig.path << " failed: " << cpp_strerror(r) << dendl;
      return -r;
    }
    // on ENOENT, fall back to expected location
  }

  // check its expected location
  r = path.append(name.c_str(), name.size());
  if (r) return r;

  do {
    r = ::openat(rootfd, path.path, O_RDWR);
    if (r >= 0) {
      *fd = r;
      return 0;
    }
    r = errno;
    if (r == ENOENT && create) {
      // do an exclusive create to keep 'sizes' consistent
      r = ::openat(rootfd, path.path, O_CREAT | O_EXCL | O_RDWR, 0644);
      if (r >= 0) {
        *fd = r;
        // increase the directory size
        increment_size(path.frag);
        return 0;
      }
      r = errno;
    }
  } while (r == EEXIST); // retry if exclusive create failed

  derr << "open " << path.path << " failed: " << cpp_strerror(r) << dendl;
  return -r;
}

int FragTreeIndex::unlink(const std::string &name, uint64_t hash)
{
  int r = -ENOENT;
  struct frag_path path, orig;
  {
    // build paths under index rdlock
    std::shared_lock<std::shared_timed_mutex> lock(index_mutex);
    r = path.build(tree, hash);
    if (r) return r;
    r = orig.build(committed.tree, hash);
    if (r) return r;
  }

  // if a migration is in progress, check the original location first
  if (orig.frag != path.frag) {
    r = orig.append(name.c_str(), name.size());
    if (r) return r;

    r = ::unlinkat(rootfd, orig.path, 0);
    if (r == 0) {
      // note that we're decrementing path.frag, not orig.frag!
      decrement_size(path.frag);
      return r;
    }
    r = errno;
    if (r != ENOENT) {
      derr << "unlink failed for original path " << orig.path << ": "
          << cpp_strerror(r) << dendl;
      return -r;
    }
    // on ENOENT, fall back to expected location
  }

  // check its expected location
  r = path.append(name.c_str(), name.size());
  if (r) return r;

  r = ::unlinkat(rootfd, path.path, 0);
  if (r == 0) {
    decrement_size(path.frag);
    return r;
  }

  r = errno;
  derr << "unlink failed for path " << path.path << ": "
      << cpp_strerror(r) << dendl;
  return -r;
}


// IndexRecord
int FragTreeIndex::read_index(int dirfd)
{
  //assert(index_lock.is_wlocked());

  // open file
  int fd = ::openat(dirfd, INDEX_FILENAME, O_RDONLY);
  if (fd < 0) {
    int r = errno;
    derr << "read_index failed to open " INDEX_FILENAME ": "
        << cpp_strerror(r) << dendl;
    return -r;
  }

  // stat for size
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = errno;
    derr << "read_index failed to stat " INDEX_FILENAME ": "
        << cpp_strerror(r) << dendl;
    ::close(fd);
    return -r;
  }

  // read into a bufferlist
  bufferlist bl;
  ssize_t len = bl.read_fd(fd, st.st_size);
  ::close(fd);
  if (len < 0) {
    derr << "read_index failed to read " INDEX_FILENAME ": "
        << cpp_strerror(-len) << dendl;
    return len;
  }

  // decode the index record
  bufferlist::iterator p = bl.begin();
  ::decode(committed, p);

  // apply pending operations to the tree
  tree = committed.tree;
  for (auto i : committed.splits)
    tree.split(i.first, i.second, false);
  for (auto i : committed.merges)
    tree.merge(i.first, i.second, false);
  return 0;
}

int FragTreeIndex::write_index(int dirfd)
{
  //assert(index_lock.is_locked());

  // encode the index record
  bufferlist bl;
  ::encode(committed, bl);

  // open file
  int fd = ::openat(dirfd, ".index", O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    int r = errno;
    derr << "write_index failed to open " INDEX_FILENAME ": "
        << cpp_strerror(r) << dendl;
    return -r;
  }

  // write the file
  int r = bl.write_fd(fd);
  ::close(fd);
  if (r < 0) {
    derr << "write_index failed to write " INDEX_FILENAME ": "
        << cpp_strerror(-r) << dendl;
    return r;
  }
  return r;
}

int FragTreeIndex::read_sizes(int dirfd)
{
  //assert(sizes_lock.is_locked());

  // open file
  int fd = ::openat(dirfd, SIZES_FILENAME, O_RDONLY);
  if (fd < 0) {
    int r = errno;
    derr << "read_sizes failed to open " SIZES_FILENAME ": "
        << cpp_strerror(r) << dendl;
    return -r;
  }

  // stat for size
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = errno;
    derr << "read_sizes failed to stat " SIZES_FILENAME ": "
        << cpp_strerror(r) << dendl;
    ::close(fd);
    return -r;
  }

  // read into a bufferlist
  bufferlist bl;
  ssize_t len = bl.read_fd(fd, st.st_size);
  ::close(fd);
  if (len < 0) {
    derr << "read_sizes failed to read " SIZES_FILENAME ": "
        << cpp_strerror(-len) << dendl;
    return len;
  }

  // decode the size record
  bufferlist::iterator p = bl.begin();
  ::decode(sizes, p);

  // unlink the file, because we don't keep it consistent while mounted
  r = ::unlinkat(dirfd, SIZES_FILENAME, 0);
  if (r < 0) {
    r = errno;
    derr << "read_sizes failed to unlink " SIZES_FILENAME ": "
        << cpp_strerror(r) << dendl;
    return -r;
  }
  return 0;
}

int FragTreeIndex::write_sizes(int dirfd)
{
  //assert(sizes_lock.is_locked());

  // encode the index record
  bufferlist bl;
  ::encode(sizes, bl);

  // create file
  int fd = ::openat(dirfd, SIZES_FILENAME, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    int r = errno;
    derr << "write_sizes failed to create " SIZES_FILENAME ": "
        << cpp_strerror(r) << dendl;
    return -r;
  }

  // write the file
  int r = bl.write_fd(fd);
  if (r)
    derr << "write_sizes failed to write " SIZES_FILENAME ": "
        << cpp_strerror(-r) << dendl;

  ::close(fd);
  return r;
}

namespace {

// closedir() does not appear to clear the readdir state associated with
// a file descriptor.  so if we dup() the fd and do another fdopendir(),
// we have to follow it with a rewinddir() to clear it manually
DIR* fdopendir_rewind(int fd)
{
  DIR *dir = ::fdopendir(fd);
  if (dir)
    ::rewinddir(dir);
  return dir;
}

int parse_frag_value(const char *name)
{
  int value;
  std::stringstream ss; // TODO: faster with sscanf()
  ss << name;
  ss >> std::hex >> value;
  if (ss.fail() || !ss.eof()) // parse failed or incomplete
    return -1;
  return value;
}

} // anonymous namespace

int FragTreeIndex::count_sizes(int dirfd)
{
  //assert(sizes_lock.is_locked());
  //assert(index_lock.is_locked());

  int r = 0;
  sizes.clear();

  // fdopendir() takes control of the fd and closes it on closedir(). the
  // caller expects its dirfd to remain open, so we dup() it and use that
  int dirfd2 = ::dup(dirfd);
  if (dirfd2 == -1) {
    r = errno;
    derr << "count_sizes failed to dup root dir fd: "
        << cpp_strerror(r) << dendl;
    return -r;
  }
  DIR *dir = fdopendir_rewind(dirfd2);
  if (dir == NULL) {
    r = errno;
    derr << "count_sizes failed to open root dir: "
        << cpp_strerror(r) << dendl;
    return -r;
  }

  // use a stack to recursively count directory entries
  struct dir_entry { int fd; DIR *dir; frag_t frag; };
  std::vector<dir_entry> stack;

  stack.push_back(dir_entry {dirfd, dir, frag_t()});
  while (r == 0 && !stack.empty()) {
    const dir_entry &entry = stack.back();
    // TODO: handle cases where entry.frag is being migrated!
    // if it's splitting, increment the subdir corresponding to object hash
    // if it's merging, increment the parent frag
    const int bits = tree.get_split(entry.frag);
    int count = 0;

    struct dirent *dn;
    while ((dn = ::readdir(entry.dir)) != NULL) {
      // skip hidden files/directories
      if (dn->d_name[0] == '.')
        continue;

      if (dn->d_type == DT_DIR) {
        // decode the frag number from the directory name
        int value = parse_frag_value(dn->d_name);
        if (value < 0 || value >= (1 >> bits)) // doesn't match fragtree
          continue;

        // since there is no opendirat(), we need to use openat()
        // and pass the resulting fd to fdopendir(). note that the
        // corresponding closedir() also closes this fd
        int fd = ::openat(entry.fd, dn->d_name, O_RDONLY);
        if (fd < 0) {
          r = -errno;
          derr << "count_sizes failed to open " << dn->d_name << ": "
              << cpp_strerror(-r) << dendl;
          break;
        }
        dir = fdopendir_rewind(fd);
        if (dir == NULL) {
          r = -errno;
          derr << "count_sizes failed to opendir " << dn->d_name << ": "
              << cpp_strerror(-r) << dendl;
          ::close(fd);
          break;
        }

        frag_t frag = entry.frag.make_child(value, bits);
        stack.push_back(dir_entry {fd, dir, frag});
        break;
      }

      if (dn->d_type == DT_REG)
        count++;
    }

    sizes[entry.frag] += count;

    ::closedir(entry.dir);
    stack.pop_back();
  }

  // closedir any open directories
  while (!stack.empty()) {
    struct dir_entry &entry = stack.back();
    ::closedir(entry.dir);
    stack.pop_back();
  }
  return r;
}

void FragTreeIndex::increment_size(frag_t frag, int n)
{
  std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
  const int count = sizes[frag] += n;
  sizes_lock.unlock();

  // split if necessary
  if (count >= split_threshold)
    split(frag, split_bits);
}

void FragTreeIndex::decrement_size(frag_t frag, int n)
{
  std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
  auto i = sizes.find(frag);
  assert(i != sizes.end());
  i->second -= n;
  sizes_lock.unlock();

  // TODO: sum sizes for all siblings and merge if necessary
}

namespace {

// create subdirectories for a splitting fragment
int split_mkdirs(CephContext *cct, int rootfd, const fragtree_t &tree,
                 frag_t frag, int bits)
{
  struct frag_path path;
  int r = path.build(tree, frag.value());
  if (r)
    return r;
  assert(path.frag == frag);

  // TODO: consider opening directory at frag, and using mkdirat() from there

  const int remaining = sizeof(path.path) - path.len;
  const int nway = 1 << bits;
  for (int i = 0; i < nway; i++) {
    r = snprintf(path.path + path.len, remaining, "%x", i);
    if (r < 0)
      return r;
    if (r >= remaining)
      return -ENAMETOOLONG;

    r = ::mkdirat(rootfd, path.path, 0755);
    if (r) {
      r = errno;
      derr << "split failed to mkdir " << path.path << ": "
          << cpp_strerror(r) << dendl;
      return -r;
    }
  }
  return 0;
}

} // anonymous namespace

int FragTreeIndex::split(frag_t frag, int bits, bool async)
{
  if (rootfd == -1) // must be mounted
    return -EINVAL;

  std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

  // don't split if a merge or parent split is in progress, because our
  // directory doesn't have all of its entries yet
  auto pending_merge = committed.merges.find(frag);
  if (pending_merge != committed.merges.end())
    return -EINPROGRESS;
  if (!frag.is_root()) {
    auto pending_split = committed.splits.find(frag.parent());
    if (pending_split != committed.splits.end())
      return -EINPROGRESS;
  }

  // add a split
  auto i = committed.splits.insert(make_pair(frag, bits));
  if (!i.second)
    return -EINPROGRESS;

  // create the subdirectories
  int r = split_mkdirs(cct, rootfd, tree, frag, bits);
  if (r) {
    committed.splits.erase(i.first); // erase the split
    return r;
  }

  // write the index (still under lock)
  r = write_index(rootfd);
  if (r) {
    committed.splits.erase(i.first); // erase the split
    return r;
  }

  // split the tree, but don't touch committed.tree until finish_split()
  tree.split(frag, bits, false);

  if (async) {
    frag_path path;
    r = path.build(tree, frag.value());
    assert(r == 0);
    assert(path.frag == frag);

    // spawn and register a migration thread to run do_split()
    auto fn = [=]() {
      frag_size_map size_updates;
      do_split(path, bits, size_updates);
      finish_split(frag, size_updates);
    };
    auto t = migration_threads.insert(std::make_pair(frag, std::thread(fn)));
    assert(t.second); // must not have a thread running for this frag
  }
  // else, caller is expected to do_split() and finish_split()
  return 0;
}

namespace {

bool parse_hash_value(const char *name, uint64_t *value)
{
  std::stringstream ss; // TODO: faster with sscanf()
  if (strlen(name) < 16)
    return -1;
  ss.write(name, 16);
  ss >> std::hex >> *value;
  return ss.eof() && !ss.fail();
}

} // anonymous namespace

void FragTreeIndex::do_split(frag_path path, int bits,
                             frag_size_map &size_updates)
{
  int dirfd;
  if (path.len)
    dirfd = ::openat(rootfd, path.path, O_RDONLY);
  else
    dirfd = ::dup(rootfd);

  if (dirfd < 0) {
    derr << "do_split failed to open " << path.path << ": "
        << cpp_strerror(errno) << dendl;
    assert(dirfd >= 0);
  }

  // initialize size_updates
  const int nway = 1 << bits;
  for (int i = 0; i < nway; i++)
    size_updates.insert(std::make_pair(path.frag.make_child(i, bits), 0));

  DIR *dir = fdopendir_rewind(dirfd);
  assert(dir);

  struct dirent *dn;
  while ((dn = ::readdir(dir)) != NULL) {
    // skip directories
    if (dn->d_type != DT_REG)
      continue;
    // skip hidden files
    if (dn->d_name[0] == '.')
      continue;

    // decode the hash value
    uint64_t hash;
    if (!parse_hash_value(dn->d_name, &hash)) {
      derr << "do_split failed to get hash value from " << dn->d_name << dendl;
      continue;
    }

    // find the corresponding child frag
    frag_t frag;
    int i;
    for (i = 0; i < nway; i++) {
      frag = path.frag.make_child(i, bits);
      if (frag.contains(hash))
        break;
    }
    if (i == nway) {
      derr << "do_split found no child frag containing value "
          << hash << " for " << dn->d_name << dendl;
      continue;
    }

    frag_path dest = {};
    int r = dest.append(i, bits);
    assert(r == 0);
    r = dest.append(dn->d_name, strlen(dn->d_name));
    assert(r == 0);

    r = ::renameat(dirfd, dn->d_name, dirfd, dest.path);
    if (r < 0)
      derr << "do_split failed to rename " << dn->d_name
          << " to " << dest.path << ": " << cpp_strerror(errno) << dendl;
    else {
      dout(0) << "do_split renamed " << dn->d_name
          << " to " << dest.path << dendl;
      size_updates[frag]++;
    }
  }

  ::closedir(dir);
}

void FragTreeIndex::finish_split(frag_t frag, const frag_size_map &size_updates)
{
  {
    std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

    auto i = committed.splits.find(frag);
    assert(i != committed.splits.end());

    auto t = migration_threads.find(frag);
    if (t != migration_threads.end()) {
      // we're probably running in this thread, so detach before destroying it
      t->second.detach();
      migration_threads.erase(t);
    }

    // remove the split and apply to the committed tree
    committed.tree.split(frag, i->second, false);
    committed.splits.erase(i);

    // write the index (still under lock)
    int r = write_index(rootfd);
    assert(r == 0);
  }

  // apply the size updates
  std::lock_guard<std::mutex> sizes_lock(sizes_mutex);
  for (auto &i : size_updates)
    sizes[i.first] += i.second;
  sizes.erase(frag); // XXX: does this need to happen in split()?
}

int FragTreeIndex::merge(frag_t frag, bool async)
{
  if (rootfd == -1) // must be mounted
    return -EINVAL;

  std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

  const int bits = tree.get_split(frag);
  if (bits == 0)
    return -ENOENT;

  // don't merge if we're splitting or if any of our child fragments are
  // merging, because our children don't have all of their entries yet
  auto pending_split = committed.splits.find(frag);
  if (pending_split != committed.splits.end())
    return -EINPROGRESS;

  const int nway = 1 << bits;
  for (int i = 0; i < nway; ++i) {
    auto pending_merge = committed.merges.find(frag.make_child(i, bits));
    if (pending_merge != committed.merges.end())
      return -EINPROGRESS;
  }

  auto m = committed.merges.insert(std::make_pair(frag, bits));
  if (!m.second) // merge is already in progress
    return -EEXIST;

  // write the index (still under lock)
  int r = write_index(rootfd);
  if (r) {
    committed.merges.erase(m.first); // erase the merge
    return r;
  }

  tree.merge(frag, bits, false);

  std::lock_guard<std::mutex> sizes_lock(sizes_mutex);
  // move child directory sizes into parent
  int sum = 0;
  for (int i = 0; i < nway; ++i) {
    auto s = sizes.find(frag.make_child(i, bits));
    if (s != sizes.end()) {
      sum += s->second;
      sizes.erase(s);
    }
  }
  auto s = sizes.insert(std::make_pair(frag, sum));
  assert(s.second); // frag was not a leaf, it shouldn't have had a size

  if (async) {
    frag_path path;
    r = path.build(tree, frag.value());
    assert(r == 0);
    assert(path.frag == frag);

    // spawn and register a migration thread to run do_split()
    auto fn = [=]() {
      do_merge(path, bits);
      finish_merge(frag);
    };
    auto t = migration_threads.insert(std::make_pair(frag, std::thread(fn)));
    assert(t.second); // must not have a thread running for this frag
  }
  // else, caller is expected to do_merge() and finish_merge()
  return 0;
}

void FragTreeIndex::do_merge(frag_path path, int bits)
{
  int parentfd;
  if (path.len)
    parentfd = ::openat(rootfd, path.path, O_RDONLY);
  else
    parentfd = rootfd;

  if (parentfd < 0) {
    derr << "do_merge failed to open " << path.path << ": "
        << cpp_strerror(errno) << dendl;
    assert(parentfd >= 0);
  }

  // TODO: merge each subdirectory in a separate thread?
  const int nway = 1 << bits;
  for (int i = 0; i < nway; i++) {
    frag_path src = {path.frag};
    int r = src.append(i, bits);
    assert(r == 0);

    int dirfd = ::openat(parentfd, src.path, O_RDONLY);
    if (dirfd < 0) {
      derr << "do_merge failed to open " << src.path << " under "
          << path.path << ": " << cpp_strerror(errno) << dendl;
      continue;
    }

    DIR *dir = fdopendir_rewind(dirfd);
    assert(dir);

    struct dirent *dn;
    while ((dn = ::readdir(dir)) != NULL) {
      // skip directories
      if (dn->d_type != DT_REG)
        continue;
      // skip hidden files
      if (dn->d_name[0] == '.')
        continue;
      // TODO: require filenames to match naming format?

      r = ::renameat(dirfd, dn->d_name, parentfd, dn->d_name);
      if (r < 0)
        derr << "do_merge failed to rename " << src.path << dn->d_name
            << " to parent: " << cpp_strerror(errno) << dendl;
      else
        dout(0) << "do_merge renamed " << src.path << dn->d_name
            << " to parent" << dendl;
    }

    ::closedir(dir);
  }
}

void FragTreeIndex::finish_merge(frag_t frag)
{
  std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

  auto i = committed.merges.find(frag);
  assert(i != committed.merges.end());

  auto t = migration_threads.find(frag);
  if (t != migration_threads.end()) {
    // we're probably running in this thread, so detach before destroying it
    t->second.detach();
    migration_threads.erase(t);
  }

  // remove the merge and apply to the committed tree
  committed.tree.merge(frag, i->second, false);
  committed.merges.erase(i);

  // write the index (still under lock)
  int r = write_index(rootfd);
  assert(r == 0);
}


// format a path to the fragment located at 'hash' in the tree
int frag_path::build(const fragtree_t &tree, uint64_t hash)
{
  frag = frag_t();
  len = 0;

  for (;;) {
    assert(frag.contains(hash));
    int bits = tree.get_split(frag);
    if (bits == 0)
      break;

    // pick appropriate child fragment.
    const int nway = 1 << bits;
    int i;
    for (i = 0; i < nway; i++)
      if (frag.make_child(i, bits).contains(hash))
        break;
    assert(i < nway);

    int r = append(i, bits);
    if (r)
      return r;
  }
  return 0;
}

int frag_path::append(int frag_index, int bits)
{
  const int remaining = sizeof(path) - len;
  int r = snprintf(path + len, remaining, "%x/", frag_index);
  if (r < 0)
    return r;
  if (r >= remaining)
    return -ENAMETOOLONG;
  len += r;
  frag = frag.make_child(frag_index, bits);
  return 0;
}

int frag_path::append(const char *name, size_t name_len)
{
  if (len + name_len >= sizeof(path))
    return -ENAMETOOLONG;
  std::copy(name, name + name_len, path + len);
  path[len += name_len] = 0;
  return 0;
}
