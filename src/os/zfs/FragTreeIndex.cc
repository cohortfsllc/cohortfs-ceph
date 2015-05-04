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
#include "os/chain_xattr.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "index "

#define INDEX_FILENAME ".index"
#define SIZES_FILENAME ".sizes"
#define OBJECT_NAME_XATTR "user.full_object_name"

namespace cohort_zfs {

  using namespace cohort;

  FragTreeIndex::FragTreeIndex(CephContext* cct,
			       lzfw_vfs_t *zhfs,
			       uint32_t initial_split)
    : cct(cct),
      zhfs(zhfs),
      root(nullptr),
      cred{0,0},
      initial_split(initial_split),
      migration_threads(cct,
			cct->_conf->fragtreeindex_migration_threads,
			ThreadPool::FLAG_DROP_JOBS_ON_SHUTDOWN)
  {
  }

  FragTreeIndex::~FragTreeIndex()
  {
#if 1 // don't blow up on test failures
    if (root) {
      lzfw_closedir(zhfs, &cred, root);
      root = nullptr;
    }
#else
    assert(root == nullptr); // must not be mounted
#endif
  }

  namespace {

    int check_directory_empty(lzfw_vfs_t* zhfs, creden_t* cred,
			      lzfw_vnode_t* d_vnode)
    {
      off_t off = 0;
      lzfw_entry_t dirents[4];

      int r = lzfw_readdir(zhfs, cred, d_vnode, dirents, 4, &off);
      if (!!r)
	return -r;

      int ix;
      for (ix = 0; ix < 4; ++ix) {
	lzfw_entry_t* dn = &dirents[ix];
	if (dn->psz_filename[0] == '\0')
	  break;
      }

      if (ix > 3)
	return -ENOTEMPTY;

      return 0;
    }

  } // anonymous namespace

  int FragTreeIndex::init(const std::string& path)
  {
    // must not be mounted
    if (root)
      return -EINVAL;

    // open the real root
    int r = open_root(path);
    if (!!r)
      return r;

    // verify that the directory is empty
    r = check_directory_empty(zhfs, &cred, root);
    if (!!r)
      return r;

    // create an index with the initial number of subdirectories
    if (initial_split > 0) {
      frag_path path = {};
      frag_size_map size_updates;
      int r = split(path.frag, initial_split, false);
      if (r == 0) {
	do_split(path, initial_split, size_updates);
	finish_split(path.frag, size_updates);
      }
    } else {
      // write an empty index
      std::shared_lock<std::shared_timed_mutex>
	index_lock(index_mutex);
      r = write_index(root);
    }
    lzfw_closedir(zhfs, &cred, root);
    root = nullptr;
    return r;
  }

  int FragTreeIndex::destroy(const std::string& path)
  {
    int r = 0;

    if (!root) {
      r = open_root(path);
      if (!!r) {
	derr << "destroy failed to open unmounted collection at "
	     << path << ": " << cpp_strerror(r) << dendl;
	return r;
      }
    } else {
      // stop migration threads
      migration_threads.shutdown();
    }

    // unlink metadata files
    lzfw_unlinkat(zhfs, &cred, root, INDEX_FILENAME, 0);
    lzfw_unlinkat(zhfs, &cred, root, SIZES_FILENAME, 0);

    // use a stack to recursively unlink directories
    struct dir_entry {
      lzfw_vnode_t* d_vnode;
      std::string name;
    };
    std::vector<dir_entry> stack;

    /* base step: push root */
    stack.push_back(dir_entry {root, ""});

    while (r == 0 && !stack.empty()) {
      dir_entry& entry = stack.back();
      off_t d_off = 0;
      lzfw_entry_t dirents[32];
      bool done = false;

      do {
	r = lzfw_readdir(zhfs, &cred, entry.d_vnode, dirents, 32,
			 &d_off);
	if (!!r)
	  abort();

	for (int ix = 0; ix < 32; ++ix) {
	  lzfw_entry_t* dn = &dirents[ix];
	  if (dn->psz_filename[0] == '\0') {
	    // indicates no more dirents
	    if (entry.d_vnode != root)
	      (void) lzfw_close(zhfs, &cred, entry.d_vnode, O_RDONLY);
	    std::string name;
	    std::swap(name, entry.name);
	    stack.pop_back();
	    if (!stack.empty()) {
	      // unlink directory from parent
	      const dir_entry& parent = stack.back();
	      r = lzfw_unlinkat(zhfs, &cred, parent.d_vnode,
				name.c_str(), 0);
	      if (!!r) {
		derr << "destroy failed to rmdir " << name << ": "
		     << cpp_strerror(-r) << dendl;
	      } else {
		dout(0) << "destroy unlinked directory " << name
			<< dendl;
	      }
	    }
	    done = true;
	    goto next_entry;
	  }
	  if (dn->type == DT_REG) {
	    r = lzfw_unlinkat(zhfs, &cred, entry.d_vnode,
			      dn->psz_filename, 0);
	    if (!!r) {
	      derr << "destroy failed to unlink " << dn->psz_filename
		   << ": " << cpp_strerror(-r) << dendl;
	      break;
	    }
	    dout(0) << "destroy unlinked file " << dn->psz_filename
		    << dendl;
	    continue;
	  }
	  if (dn->type == DT_DIR && dn->psz_filename[0] != '.') {
	    lzfw_vnode_t* d_vnode;
	    r = lzfw_openat(zhfs, &cred, entry.d_vnode,
			    dn->psz_filename, O_RDONLY, 0, &d_vnode);
	    if (!!r) {
	      derr << "destroy failed to open " << dn->psz_filename
	      << ": " << cpp_strerror(-r) << dendl;
	      break;
	    }
	    stack.push_back(dir_entry {d_vnode, dn->psz_filename});
	    break;
	  }
	} /* while readdir */
      } while (!done); /* while readdir segs */
    next_entry:
      ;
    } /* while stack */

    // closedir any open directories
    while (!stack.empty()) {
      struct dir_entry& entry = stack.back();
      r = lzfw_closedir(zhfs, &cred, entry.d_vnode);
      stack.pop_back();
    }

    if (r == 0) { // XXX which r?
      // unlink the collection's root directory
      r = lzfw_rmdir(zhfs, &cred, root_ino, path.c_str());
      if (!!r) {
	derr << "destroy failed to rmdir collection " << path << ": "
	     << cpp_strerror(-r) << dendl;
      }
    }
    return r;
  } /* destroy */

  int FragTreeIndex::open_root(const std::string& path)
  {
    // open real root fd
    inogen_t fs_root;
    int r = lzfw_getroot(zhfs, &fs_root);

    if (!!r)
      return -EINVAL;

    /* XXX assume that path is a name in fs_root */
    int type;
    r = lzfw_lookup(zhfs, &cred, fs_root, path.c_str(), &root_ino,
	&type);
    if (!!r)
      return -EINVAL;

    r = lzfw_opendir(zhfs, &cred, root_ino, &root);
    if (!!r)
      return -EINVAL;

    /* verify that we opened a directory (XXX do this above) */
    struct stat st;
    r = lzfw_stat(zhfs, &cred, root, &st);
    if ((!!r) || (!S_ISDIR(st.st_mode)))
      return -ENOTDIR;

    return 0;
  }

  int FragTreeIndex::close_root() {
    int r = lzfw_closedir(zhfs, &cred, root);
    root = nullptr;
    return r;
  }

  int FragTreeIndex::mount(const std::string& path,
			   bool async_recovery)
  {
    // must not be mounted
    if (root)
      return -EINVAL;

    int r = open_root(path);
    if (!!r)
      return r;

    // read index from disk
    std::unique_lock<std::shared_timed_mutex>
      index_wrlock(index_mutex);
    r = read_index(root);
    index_wrlock.unlock();
    if (!!r) {
      (void) close_root();
      return r;
    }

    // read sizes from disk
    std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
    r = read_sizes(root);
    if (r == -ENOENT) {
      // fresh index or not unmounted cleanly
      std::shared_lock<std::shared_timed_mutex>
	index_rdlock(index_mutex);
      r = count_sizes(root);
    }
    sizes_lock.unlock();
    if (!!r) {
      (void) close_root();
      return r;
    }

    if (async_recovery) {
      // restart unfinished migrations
      restart_migrations(true);
    }
    // else, caller is expected to call restart_migrations()
    return 0;
  }

  void FragTreeIndex::restart_migrations(bool async)
  {
    for (auto i = committed.splits.begin();
	 i != committed.splits.end(); ) {
      const frag_t& frag = i->first;
      const int bits = i->second;
      /* if recovery is synchronous, finish_split() will erase i from
       * committed.splits, so we increment i here before it's
       * invalidated */
      ++i;

      frag_path path;
      /* use 'committed.tree' for splits, because it still has frag
       * as a leaf */
      int r = path.build(committed.tree, frag.value());
      assert(r == 0);
      assert(path.frag == frag);

      auto fn = [=]() {
	frag_size_map size_updates;
	do_split(path, bits, size_updates);
	finish_split(frag, size_updates);
      };
      if (async)
	migration_threads.submit(fn);
      else
	fn();
    }

    for (auto i = committed.merges.begin();
	 i != committed.merges.end(); ) {
      const frag_t& frag = i->first;
      const int bits = i->second;
      ++i;

      frag_path path;
      /*  use uncommitted 'tree' for merges, because it now has frag
       * as a leaf */
      int r = path.build(tree, frag.value());
      assert(r == 0);
      assert(path.frag == frag);

      auto fn = [=]() {
	do_merge(path, bits);
	finish_merge(frag);
      };
      if (async)
	migration_threads.submit(fn);
      else
	fn();
    }
  }

  int FragTreeIndex::unmount()
  {
    // must be mounted
    if (!root)
      return -EINVAL;

    // stop migration threads
    migration_threads.shutdown();

    // write sizes to disk
    std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
    int r = write_sizes(root);
    sizes_lock.unlock();

    (void) close_root();
    return r;
  }

  namespace
  {

#define STR(s) #s
#define XSTR(s) STR(s)
#define HASH_LEN 16 // hex digits for 64-bit value

    std::string format_name(const hoid_t& oid)
    {
      // allocate a string of the required length
      const size_t oid_len = std::min(oid.oid.name.size(),
				      size_t(NAME_MAX - HASH_LEN));
      std::string name(HASH_LEN + oid_len, 0);

      // fill in the hash prefix
#define HASH_FMT ("%0" XSTR(HASH_LEN) "lX")
      int count = snprintf(&name[0], name.size(), HASH_FMT, oid.hk);
      assert(count == HASH_LEN);

      // append the object name, truncating if necessary
      std::copy(oid.oid.name.begin(), oid.oid.name.begin() + oid_len,
		name.begin() + count);

      return name;
    }

  } // anonymous namespace

  int FragTreeIndex::lookup(const hoid_t& oid)
  {
    struct stat st; // ignored
    const std::string name = format_name(oid);
    return _stat(name, oid.hk, &st);
  }

  int FragTreeIndex::stat(const hoid_t& oid, struct stat* st)
  {
    const std::string name = format_name(oid);
    return _stat(name, oid.hk, st);
  }

  int FragTreeIndex::_stat(const std::string& name, uint64_t hash,
			   struct stat* st)
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

    lzfw_vnode_t* vnode;

    // if a migration is in progress, check the original location first
    if (orig.frag != path.frag) {
      r = orig.append(name.c_str(), name.size());
      if (r) return r;

      r = lzfw_openat(zhfs, &cred, root, orig.path, O_RDONLY, 0,
		      &vnode);
      if (r == 0) {
	r = lzfw_stat(zhfs, &cred, vnode, st);
	(void) lzfw_close(zhfs, &cred, vnode, O_RDONLY /* XXX ew */);
	if (!r)
	  return 0;
	return -r;
      }
      if (r != ENOENT)
	return -r;
      // on ENOENT, fall back to expected location
    }

    // check its expected location
    r = path.append(name.c_str(), name.size());
    if (r) return r;

    r = lzfw_openat(zhfs, &cred, root, path.path, O_RDONLY, 0, &vnode);
    if (r == 0) {
      r = lzfw_stat(zhfs, &cred, vnode, st);
      (void) lzfw_close(zhfs, &cred, vnode, O_RDONLY /* XXX ew */);
      if (!r)
	return 0;
      return -r;
    }
    return -r;
  }

  int FragTreeIndex::open(const hoid_t& oid, bool create,
			  lzfw_vnode_t** vnode)
  {
    int r = -ENOENT;
    struct frag_path path, orig;
    {
      // build paths under index rdlock
      std::shared_lock<std::shared_timed_mutex> lock(index_mutex);
      r = path.build(tree, oid.hk);
      if (r) return r;
      r = orig.build(committed.tree, oid.hk);
      if (r) return r;
    }

    const std::string name = format_name(oid);

    // if a migration is in progress, check the original location first
    if (orig.frag != path.frag) {
      r = orig.append(name.c_str(), name.size());
      if (r) return r;

      r = lzfw_openat(zhfs, &cred, root, orig.path, O_RDWR, 0, vnode);
      if (!r)
	return 0;
      if (r != ENOENT)
	return -r;
      // on ENOENT, fall back to expected location
    }

    // check its expected location
    r = path.append(name.c_str(), name.size());
    if (r) return r;

    do {
      r = lzfw_openat(zhfs, &cred, root, path.path, O_RDWR, 0, vnode);
      if (!r)
	return 0;
      if (r == ENOENT && create) {
	// do an exclusive create to keep 'sizes' consistent
	r = lzfw_openat(zhfs, &cred, root, path.path,
			O_CREAT|O_EXCL|O_RDWR, 0644, vnode);
	if (!!r) {
	  // increase the directory size
	  increment_size(path.frag);

	  // set xattr for full object name if we had to truncate
	  if (oid.oid.name.size() > NAME_MAX - HASH_LEN) {
#warning need to replace chain_fsetxattr
#if 0
	    r = chain_fsetxattr(*fd, OBJECT_NAME_XATTR,
				oid.oid.name.c_str(),
				oid.oid.name.size());
	    if (r < 0) {
	      /* XXX was r = -errno */
	      derr << "open failed to write xattr "
		OBJECT_NAME_XATTR << ": "
		   << cpp_strerror(-r) << dendl;
	    }
#endif
	    /* error here means collection_list() will show truncated
	     * name */
	  }
	  return 0;
	}
      }
    } while (r == EEXIST); // retry if exclusive create failed

    return -r;
  }

  int FragTreeIndex::unlink(const hoid_t& oid)
  {
    int r = -ENOENT;
    struct frag_path path, orig;
    frag_t parent;
    {
      // build paths under index rdlock
      std::shared_lock<std::shared_timed_mutex> lock(index_mutex);
      r = path.build(tree, oid.hk);
      if (r) return r;
      r = orig.build(committed.tree, oid.hk);
      if (r) return r;
      parent = tree.get_branch_above(path.frag); //for decrement_size()
    }

    const std::string name = format_name(oid);

    // if a migration is in progress, check the original location first
    if (orig.frag != path.frag) {
      r = orig.append(name.c_str(), name.size());
      if (r) return r;

      r = lzfw_unlinkat(zhfs, &cred, root, orig.path, 0);
      if (r == 0) {
	// note that we're decrementing path.frag, not orig.frag!
	decrement_size(path.frag, parent);
	return r;
      }
      if (r != ENOENT)
	return -r;
      // on ENOENT, fall back to expected location
    }

    // check its expected location
    r = path.append(name.c_str(), name.size());
    if (r) return r;

    r = lzfw_unlinkat(zhfs, &cred, root, path.path, 0);
    if (r == 0) {
      decrement_size(path.frag, parent);
      return r;
    }

    return -r;
  }

  // IndexRecord
  int FragTreeIndex::read_index(lzfw_vnode_t* vnode)
  {
    lzfw_vnode_t* vnode2;
    //assert(index_lock.is_wlocked());

    // open file
    int r = lzfw_openat(zhfs, &cred, vnode, INDEX_FILENAME, O_RDONLY,
			0, &vnode2);
    if (!!r) {
      derr << "read_index failed to open " INDEX_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      return -r;
    }

    // stat for size
    struct stat st;
    r = lzfw_stat(zhfs, &cred, vnode2, &st);
    if (!!r) {
      derr << "read_index failed to stat " INDEX_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      (void) lzfw_close(zhfs, &cred, vnode2, O_RDONLY /* XXX ew */);
      return -r;
    }

    // read into a bufferlist
    bufferlist bl;
    ssize_t len;
    /* XXXX need something to read into a bufferlist */
#warning need replacement for bl.read_fd
#if 0
    len = bl.read_fd(fd, st.st_size);
#endif
    (void) lzfw_close(zhfs, &cred, vnode2, O_RDONLY /* XXX ew */);
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

  int FragTreeIndex::write_index(lzfw_vnode_t* d_vnode)
  {
    //assert(index_lock.is_locked());

    // encode the index record
    bufferlist bl;
    ::encode(committed, bl);

    // open file
    lzfw_vnode_t* vnode;
    int r = lzfw_openat(zhfs, &cred, d_vnode, ".index",
			O_CREAT|O_TRUNC|O_WRONLY, 0644, &vnode);
    if (!!r) {
      derr << "write_index failed to open " INDEX_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      return -r;
    }

    // write the file
#warning need replacement for bl.write_fd
#if 0
    r = bl.write_fd(fd);
#endif
    (void) lzfw_close(zhfs, &cred, vnode, O_RDONLY /* XXX ew */);
    if (!!r) {
      derr << "write_index failed to write " INDEX_FILENAME ": "
	   << cpp_strerror(-r) << dendl;
      return r;
    }
    return r;
  }

  int FragTreeIndex::read_sizes(lzfw_vnode_t* d_vnode)
  {
    //assert(sizes_lock.is_locked());

    // open file
    lzfw_vnode_t* vnode;
    int r = lzfw_openat(zhfs, &cred, d_vnode, SIZES_FILENAME, O_RDONLY,
			0, &vnode);
    if (!!r) {
      derr << "read_sizes failed to open " SIZES_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      return -r;
    }

    // stat for size
    struct stat st;
    r = lzfw_stat(zhfs, &cred, vnode, &st);
    if (!!r) {
      derr << "read_sizes failed to stat " SIZES_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      (void) lzfw_close(zhfs, &cred, vnode, O_RDONLY /* XXX ew */);
      return -r;
    }

    // read into a bufferlist
    bufferlist bl;
    ssize_t len;
#warning need replacement for bl.read_fd
#if 0
    len = bl.read_fd(fd, st.st_size);
#endif
    (void) lzfw_close(zhfs, &cred, vnode, O_RDONLY /* XXX ew */);
    if (len < 0) {
      derr << "read_sizes failed to read " SIZES_FILENAME ": "
	   << cpp_strerror(-len) << dendl;
      return len;
    }

    // decode the size record
    bufferlist::iterator p = bl.begin();
    ::decode(sizes, p);

    /* unlink the file, because we don't keep it consistent while
     * mounted */
    r = lzfw_unlinkat(zhfs, &cred, d_vnode, SIZES_FILENAME, 0);
    if (!!r) {
      derr << "read_sizes failed to unlink " SIZES_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      return -r;
    }
    return 0;
  }

  int FragTreeIndex::write_sizes(lzfw_vnode_t* d_vnode)
  {
    //assert(sizes_lock.is_locked());

    // encode the index record
    bufferlist bl;
    ::encode(sizes, bl);

    // create file
    lzfw_vnode_t* vnode;
    int r = lzfw_openat(zhfs, &cred, d_vnode, SIZES_FILENAME,
			O_CREAT|O_TRUNC|O_WRONLY, 0644, &vnode);
    if (!!r) {
      derr << "write_sizes failed to create " SIZES_FILENAME ": "
	   << cpp_strerror(r) << dendl;
      return -r;
    }

    // write the file
#warning need replacement for bl.write_fd
#if 0
    r = bl.write_fd(fd);
#endif
    if (r) {
      derr << "write_sizes failed to write " SIZES_FILENAME ": "
	   << cpp_strerror(-r) << dendl;
    }
    (void) lzfw_close(zhfs, &cred, vnode, O_RDONLY /* XXX ew */);
    return r;
  }

  namespace {

    bool parse_hex_value(const char* name, uint64_t* value)
    {
      char* end;
      auto result = strtoull(name, &end, 16);
      if (result == ULLONG_MAX && errno) // overflow
	return false;
      if (result == 0 && end == name) // empty
	return false;
      if (*end) // garbage after the value
	return false;

      *value = result;
      return true;
    }

    bool parse_hash_prefix(const char* name, uint64_t* value)
    {
      /* copy into a temporary buffer to make sure strtoull() doesn't
       * try to parse more than the 16 bytes at the beginning */
      char buf[17];
      strncpy(buf, name, sizeof(buf));
      buf[16] = 0;

      return parse_hex_value(buf, value);
    }

  } // anonymous namespace

  int FragTreeIndex::count_sizes(lzfw_vnode_t* d_vnode)
  {
    //assert(sizes_lock.is_locked());
    //assert(index_lock.is_locked());

    int r = 0;
    sizes.clear();

    // use a stack to recursively count directory entries
    struct dir_entry {
      lzfw_vnode_t* d_vnode;
      frag_t frag;
      bool merge;
    };
    std::vector<dir_entry> stack;

    stack.push_back(dir_entry {d_vnode, frag_t(), false});
    while (r == 0 && !stack.empty()) {
      const dir_entry& entry = stack.back();

      // if it's merging, have subdirs increment the current frag
      bool merging = committed.merges.count(entry.frag);

      uint64_t bits =
	(merging ? committed.tree : tree).get_split(entry.frag);
      uint64_t nway = 1 << bits;

      int count = 0;
      off_t d_off = 0;
      lzfw_entry_t dirents[32];
      bool done = false;

      do {
	r = lzfw_readdir(zhfs, &cred, d_vnode, dirents, 32, &d_off);
	if (!!r)
	  abort();

	for (int ix = 0; ix < 32; ++ix) {
	  lzfw_entry_t* dn = &dirents[ix];

	  // indicates no more dirents
	  if (dn->psz_filename[0] == '\0') {
	    if (entry.d_vnode != root)
	      (void) lzfw_close(zhfs, &cred, entry.d_vnode, O_RDONLY);
	    stack.pop_back();
	    done = true;
	    goto next_entry;
	  }

	  // skip hidden files/directories
	  if (dn->psz_filename[0] == '.')
	    continue;

	  if (dn->type == DT_DIR) {
	    // decode the frag number from the directory name
	    uint64_t value;
	    if (!parse_hex_value(dn->psz_filename, &value))
	      continue;
	    if (value >= nway) // doesn't match fragtree
	      continue;

	    /* since there is no opendirat(), we need to use openat()
	     * and pass the resulting fd to fdopendir(). note that the
	     * corresponding closedir() also closes this fd */
	    lzfw_vnode_t* d_vnode;
	    r = lzfw_openat(zhfs, &cred, entry.d_vnode,
			    dn->psz_filename, O_RDONLY, 0, &d_vnode);
	    if (!!r) {
	      derr << "count_sizes failed to open " << dn->psz_filename
		   << ": " << cpp_strerror(-r) << dendl;
	      break;
	    }
	    frag_t frag = entry.frag.make_child(value, bits);
	    stack.push_back(dir_entry {d_vnode, frag, merging});
	    break;
	  } /* DT_DIR */

	  if (dn->type == DT_REG)
	    ++count;

	  if (count) {
	    /* if parent is merging, apply size updates to the parent
	     * frag */
	    if (entry.merge)
	      sizes[tree.get_branch_above(entry.frag)] += count;
	    else
	      sizes[entry.frag] += count;
	  }
	} /* while readdir */
      } while (!done); /* while readdir segs */
    next_entry:
      ;
    } /* while stack */

    // close any open directories
    while (!stack.empty()) {
      struct dir_entry& entry = stack.back();
      r = lzfw_closedir(zhfs, &cred, entry.d_vnode);
      if (!!r) {
	derr << "error closing directory " << entry.frag
	     << cpp_strerror(r) << dendl;
      }
      stack.pop_back();
    }
    return r;
  } /* count_sizes */

  void FragTreeIndex::increment_size(frag_t frag)
  {
    std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
    const int count = ++sizes[frag];
    sizes_lock.unlock();

    // split if necessary
    if (count >= cct->_conf->fragtreeindex_split_threshold)
      split(frag, cct->_conf->fragtreeindex_split_bits);
  }

  void FragTreeIndex::decrement_size(frag_t frag, frag_t parent)
  {
    std::unique_lock<std::mutex> sizes_lock(sizes_mutex);
    auto i = sizes.find(frag);
    assert(i != sizes.end());
    i->second--;

    if (parent.bits() <= initial_split) {
      // no further merging allowed
      return;
    }

    const int bits = parent.bits() - frag.bits();
    const uint64_t nway = 1 << bits;

    // sum sizes for all siblings and merge into parent if necessary
    int sum = 0;
    for (uint64_t i = 0; i < nway; i++)
      sum += sizes[parent.make_child(i, bits)];

    sizes_lock.unlock();

    if (sum < cct->_conf->fragtreeindex_merge_threshold)
      merge(parent);
  }

  // create subdirectories for a splitting fragment
  int FragTreeIndex::split_mkdirs(CephContext* cct,
				  lzfw_vnode_t* d_vnode,
				  const fragtree_t& tree,
				  frag_t frag, int bits)
  {
    struct frag_path path;
    int r = path.build(tree, frag.value());
    if (r)
      return r;
    assert(path.frag == frag);

    /* TODO: consider opening directory at frag, and using
     * mkdirat() from there */
    const int remaining = sizeof(path.path) - path.len;
    const uint64_t nway = 1 << bits;
    for (uint64_t i = 0; i < nway; i++) {
      r = snprintf(path.path + path.len, remaining, "%lx", i);
      if (r < 0)
	return r;
      if (r >= remaining)
	return -ENAMETOOLONG;

      inogen_t ino;
      r = lzfw_mkdirat(zhfs, &cred, d_vnode, path.path, 0755, &ino);
      if (!!r) {
	derr << "split failed to mkdir " << path.path << ": "
	     << cpp_strerror(r) << dendl;
	return -r;
      }
    }
    return 0;
  } /* split_mkdirs */

  int FragTreeIndex::split(frag_t frag, int bits, bool async)
  {
    if (!root) // must be mounted
      return -EINVAL;

    std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

    /* don't split if a merge or parent split is in progress, because
     * our directory doesn't have all of its entries yet */
    auto pending_merge = committed.merges.find(frag);
    if (pending_merge != committed.merges.end())
      return -EINPROGRESS;
    if (!frag.is_root()) {
      frag_t parent = tree.get_branch_above(frag);
      auto pending_split = committed.splits.find(parent);
      if (pending_split != committed.splits.end())
	return -EINPROGRESS;
    }

    // add a split
    auto i = committed.splits.insert(make_pair(frag, bits));
    if (!i.second)
      return -EINPROGRESS;

    // create the subdirectories
    int r = split_mkdirs(cct, root, tree, frag, bits);
    if (r) {
      committed.splits.erase(i.first); // erase the split
      return r;
    }

    // write the index (still under lock)
    r = write_index(root);
    if (r) {
      committed.splits.erase(i.first); // erase the split
      return r;
    }

    /* split the tree, but don't touch committed.tree until
     * finish_split() */
    tree.split(frag, bits, false);

    if (async) {
      frag_path path;
      r = path.build(committed.tree, frag.value());
      assert(r == 0);
      assert(path.frag == frag);

      // spawn and register a migration thread to run do_split()
      auto fn = [=]() {
	frag_size_map size_updates;
	do_split(path, bits, size_updates);
	finish_split(frag, size_updates);
      };
      migration_threads.submit(fn);
    }
    // else, caller is expected to do_split() and finish_split()
    return 0;
  }

  void FragTreeIndex::do_split(frag_path path, int bits,
			       frag_size_map& size_updates)
  {
    lzfw_vnode_t* d_vnode;
    int r = 0;

    if (path.len)
      r = lzfw_openat(zhfs, &cred, root, path.path, O_RDONLY, 0,
		      &d_vnode);
    else
      d_vnode = root;

    if (!!r || !d_vnode) {
      derr << "do_split failed to open " << path.path << ": "
	   << cpp_strerror(r) << dendl;
      assert(false);
    }

    // initialize size_updates
    const uint64_t nway = 1 << bits;
    for (uint64_t i = 0; i < nway; i++)
      size_updates.insert(
		   std::make_pair(path.frag.make_child(i, bits), 0));

    off_t d_off = 0;
    lzfw_entry_t dirents[32];
    bool done = false;

    do {
      r = lzfw_readdir(zhfs, &cred, d_vnode, dirents, 32, &d_off);
      if (!!r)
	abort();

      for (int ix = 0; ix < 32; ++ix) {
	lzfw_entry_t* dn = &dirents[ix];

	// indicates no more dirents
	if (dn->psz_filename[0] == '\0') {
	  done = true;
	  goto last_entry;
	}
	// skip directories
	if (dn->type != DT_REG)
	  continue;
	// skip hidden files
	if (dn->psz_filename[0] == '.')
	  continue;

	// decode the hash value
	uint64_t hash;
	if (!parse_hash_prefix(dn->psz_filename, &hash)) {
	  derr << "do_split failed to get hash value from "
	       << dn->psz_filename << dendl;
	  continue;
	}

	// find the corresponding child frag
	frag_t frag;
	uint64_t i;
	for (i = 0; i < nway; i++) {
	  frag = path.frag.make_child(i, bits);
	  if (frag.contains(hash))
	    break;
	}
	if (i == nway) {
	  derr << "do_split found no child frag containing value "
	       << hash << " for " << dn->psz_filename << dendl;
	  continue;
	}

	frag_path dest = {};
	int r = dest.append(i, bits);
	assert(r == 0);
	r = dest.append(dn->psz_filename, strlen(dn->psz_filename));
	assert(r == 0);

	r = lzfw_renameat(zhfs, &cred,
			  d_vnode, dn->psz_filename,
			  d_vnode, dest.path);
	if (!!r)
	  derr << "do_split failed to rename " << dn->psz_filename
	       << " to " << dest.path << ": " << cpp_strerror(r)
	       << dendl;
	else {
	  dout(20) << "do_split renamed " << dn->psz_filename
		   << " to " << dest.path << dendl;
	  size_updates[frag]++;
	}
      } /* while readdir */
    } while (!done); /* while readdir segs */

  last_entry:
    // close d_vnode iff we opened it
    if (d_vnode != root) {
      r = lzfw_close(zhfs, &cred, d_vnode, O_RDONLY);
      if (!!r)
	abort();
    }
  } /* do_split */

  void FragTreeIndex::finish_split(frag_t frag,
				   const frag_size_map& size_updates)
  {
    {
      std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

      auto i = committed.splits.find(frag);
      assert(i != committed.splits.end());

      // remove the split and apply to the committed tree
      committed.tree.split(frag, i->second, false);
      committed.splits.erase(i);

      // write the index (still under lock)
      int r = write_index(root);
      assert(r == 0);
    }

    // apply the size updates
    std::lock_guard<std::mutex> sizes_lock(sizes_mutex);
    for (auto& i : size_updates)
      sizes[i.first] += i.second;
    sizes.erase(frag);
  }

  int FragTreeIndex::merge(frag_t frag, bool async)
  {
    if (!root) // must be mounted
      return -EINVAL;

    std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

    const int bits = tree.get_split(frag);
    if (bits == 0)
      return -ENOENT;

    /* don't merge if we're splitting or if any of our child fragments
     * are merging, because our children don't have all of their
     * entries yet */
    auto pending_split = committed.splits.find(frag);
    if (pending_split != committed.splits.end())
      return -EINPROGRESS;

    const int nway = 1 << bits;
    for (int i = 0; i < nway; ++i) {
      auto pending_merge =
	committed.merges.find(frag.make_child(i, bits));
      if (pending_merge != committed.merges.end())
	return -EINPROGRESS;
    }

    auto m = committed.merges.insert(std::make_pair(frag, bits));
    if (!m.second) // merge is already in progress
      return -EEXIST;

    // write the index (still under lock)
    int r = write_index(root);
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
    assert(s.second); /* frag was not a leaf, it shouldn't have had
		       * a size */

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
      migration_threads.submit(fn);
    }
    // else, caller is expected to do_merge() and finish_merge()
    return 0;
  }

  void FragTreeIndex::do_merge(frag_path path, int bits)
  {
    lzfw_vnode_t* parent;
    int r = 0;

    if (path.len)
      r = lzfw_openat(zhfs, &cred, root, path.path, O_RDONLY, 0,
		      &parent);
    else
      parent = root;

    if (!!r || !parent) {
      derr << "do_merge failed to open " << path.path << ": "
	   << cpp_strerror(r) << dendl;
      assert(false);
    }

    // TODO: merge each subdirectory in a separate thread?
    const int nway = 1 << bits;
    for (int i = 0; i < nway; i++) {
      frag_path src = {path.frag};
      int r = src.append(i, bits);
      assert(r == 0);

      lzfw_vnode_t* d_vnode;
      r = lzfw_openat(zhfs, &cred, parent, src.path, O_RDONLY,
		      0, &d_vnode);
      if (!!r) {
	derr << "do_merge failed to open " << src.path << " under "
	     << path.path << ": " << cpp_strerror(r) << dendl;
	continue;
      }

      off_t d_off = 0;
      lzfw_entry_t dirents[32];
      bool done = false;

      do {
	r = lzfw_readdir(zhfs, &cred, d_vnode, dirents, 32, &d_off);
	if (!!r)
	  abort();

	for (int ix = 0; ix < 32; ++ix) {
	  lzfw_entry_t* dn = &dirents[ix];

	  // indicates no more dirents
	  if (dn->psz_filename[0] == '\0') {
	    done = true;
	    break;
	  }
	  // skip directories
	  if (dn->type != DT_REG)
	    continue;
	  // skip hidden files
	  if (dn->psz_filename[0] == '.')
	    continue;

	  // TODO: require filenames to match naming format?
	  r = lzfw_renameat(zhfs, &cred,
			    d_vnode, dn->psz_filename,
			    parent, dn->psz_filename);
	  if (!!r)
	    derr << "do_merge failed to rename " << src.path
		 << dn->psz_filename  << " to parent: "
		 << cpp_strerror(r) << dendl;
	  else
	    dout(20) << "do_merge renamed " << src.path <<
	      dn->psz_filename << " to parent" << dendl;
	  
	} /* while readdir */
      } while (!done); /* while readdir segs */

      // close d_vnode, which we unconditionally open
      r = lzfw_close(zhfs, &cred, d_vnode, O_RDONLY);
      if (!!r)
	abort();
    } /* i */

    // close parent iff we opened it
    if (parent != root) {
      r = lzfw_close(zhfs, &cred, parent, O_RDONLY);
      if (!!r)
	abort();
    }
  } /* do_merge */

  void FragTreeIndex::finish_merge(frag_t frag)
  {
    std::lock_guard<std::shared_timed_mutex> index_lock(index_mutex);

    auto i = committed.merges.find(frag);
    assert(i != committed.merges.end());

    // remove the merge and apply to the committed tree
    committed.tree.merge(frag, i->second, false);
    committed.merges.erase(i);

    // write the index (still under lock)
    int r = write_index(root);
    assert(r == 0);
  }

  // format a path to the fragment located at 'hash' in the tree
  int frag_path::build(const fragtree_t& tree, uint64_t hash)
  {
    frag = frag_t();
    len = 0;

    for (;;) {
      assert(frag.contains(hash));
      int bits = tree.get_split(frag);
      if (bits == 0)
	break;

      // pick appropriate child fragment.
      const uint64_t nway = 1 << bits;
      uint64_t i;
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

  int frag_path::append(const char* name, size_t name_len)
  {
    if (len + name_len >= sizeof(path))
      return -ENAMETOOLONG;
    std::copy(name, name + name_len, path + len);
    path[len += name_len] = 0;
    return 0;
  }

} /* namespace cohort_zfs */
