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

#ifndef CEPH_FRAGTREEINDEX_H
#define CEPH_FRAGTREEINDEX_H

#include <thread>
#include <shared_mutex>

#include "include/frag.h"
#include "common/oid.h"
#include "common/ThreadPool.h"
#include <libzfswrap.h>

namespace cohort_zfs { // temporarily isolate ZFS variant

  /* helper for constructing paths that traverse to a given hash */
  struct frag_path {
    frag_t frag;
    char path[260];
    size_t len;
    
    int build(const fragtree_t& tree, uint64_t hash);
    int append(int frag_index, int bits);
    int append(const char* name, size_t name_len);
  };

  class FragTreeIndex {
  public:
    /* index record, serialized to disk before starting
     * splits/merges */
    struct IndexRecord {
      fragtree_t tree;
      std::map<frag_t, int> splits;
      std::map<frag_t, int> merges;

      void encode(bufferlist& bl) const {
	::encode(tree, bl);
	::encode(splits, bl);
	::encode(merges, bl);
      }
      void decode(bufferlist::iterator& p) {
	::decode(tree, p);
	::decode(splits, p);
	::decode(merges, p);
      }
    }; /* IndexRecord */

  protected: // allow unit tests to access some internals
    CephContext* cct;

    /* open libzfswrap vfs handle */
    lzfw_vfs_t *zhfs;

    /* root collection directory vnode */
    inogen_t root_ino;
    lzfw_vnode_t* root;

    /* fake Unix credential */
    creden_t cred;

    /* don't allow merging below the initial split */
    const uint32_t initial_split;

    IndexRecord committed; /* last committed index record */
    /* current index, including uncommitted splits/merges */
    fragtree_t tree;
    /* controls read/write access to 'committed' and 'tree' */
    std::shared_timed_mutex index_mutex;

    typedef std::map<frag_t, int> frag_size_map;
    frag_size_map sizes; /* cache of all directory sizes */
    std::mutex sizes_mutex; /* controls access to 'sizes' */

    /* thread pool for migration operations */
    cohort::ThreadPool migration_threads;

    int read_index(lzfw_vnode_t* vno);
    int write_index(lzfw_vnode_t* vno);

    int read_sizes(lzfw_vnode_t* vno);
    int write_sizes(lzfw_vnode_t* vno);
    int count_sizes(lzfw_vnode_t* vno);

    void increment_size(frag_t frag);
    void decrement_size(frag_t frag, frag_t parent);

    int split(frag_t frag, int bits, bool async=true);
    int merge(frag_t frag, bool async=true);

    void do_split(frag_path path, int bits,
		  frag_size_map& size_updates);
    void do_merge(frag_path path, int bits);

    void finish_split(frag_t frag, const frag_size_map& size_updates);
    void finish_merge(frag_t frag);

    void restart_migrations(bool async=true);

    int _stat(const std::string& name, uint64_t hash, struct stat* st);

  private: /* copy/assignment disabled */
    FragTreeIndex(const FragTreeIndex& other) = delete;
    const FragTreeIndex& operator=(const FragTreeIndex& other)
    = delete;

  public:
    FragTreeIndex(CephContext* cct, lzfw_vfs_t *zhfs, uint32_t initial_split);
    ~FragTreeIndex();

    /* initialize a fresh collection index at the given path */
    int init(const std::string& path);

    /* destroy a collection index, unlinking all intermediate
     * directories.  will fail if any other files are present.
     * can be called while mounted or unmounted. results in an
     * unmounted collection */
    int destroy(const std::string& path);

    /* mount a previously initialized collection index at the
     * given path */
    int mount(const std::string& path, bool async_recovery=true);

    /* open root */
    int open_root(const std::string& path);

    /* close root */
    int close_root();

    /* unmount a mounted collection index */
    int unmount();

    /* return the vnode for the root directory of a
     * mounted collection index, or nullptr if not mounted */
    lzfw_vnode_t* get_root() const { return root; }

    /* check for the existence of an object */
    int lookup(const hoid_t& oid);

    /* fetch an object's file attributes */
    int stat(const hoid_t& oid, struct stat* st);

    /* open an object, or create if requested */
    int open(const hoid_t& oid, bool create, lzfw_vnode_t** vno);

    /* unlink an object from the index */
    int unlink(const hoid_t& oid);
  };

} // namespace cohort_zfs

WRITE_CLASS_ENCODER(cohort_zfs::FragTreeIndex::IndexRecord);

#endif // CEPH_FRAGTREEINDEX_H
