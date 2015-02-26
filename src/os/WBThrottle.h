// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef WBTHROTTLE_H
#define WBTHROTTLE_H

#include <map>
#include <boost/tuple/tuple.hpp>
#include "include/buffer.h"
#include "common/Formatter.h"
#include "common/oid.h"
#include "include/interval_set.h"
#include "FDCache.h"
#include "common/Thread.h"
#include "common/ceph_context.h"

/**
 * WBThrottle
 *
 * Tracks, throttles, and flushes outstanding IO
 */
class WBThrottle : Thread, public md_config_obs_t {
  oid_t clearing;

  /* *_limits.first is the start_flusher limit and
   * *_limits.second is the hard limit
   */

  /// Limits on unflushed bytes
  pair<uint64_t, uint64_t> size_limits;

  /// Limits on unflushed ios
  pair<uint64_t, uint64_t> io_limits;

  /// Limits on unflushed objects
  pair<uint64_t, uint64_t> fd_limits;

  uint64_t cur_ios;  /// Currently unflushed IOs
  uint64_t cur_size; /// Currently unflushed bytes

  /**
   * PendingWB tracks the ios pending on an object.
   */
  class PendingWB {
  public:
    bool nocache;
    uint64_t size;
    uint64_t ios;
    PendingWB() : nocache(true), size(0), ios(0) {}
    void add(bool _nocache, uint64_t _size, uint64_t _ios) {
      if (!_nocache)
	nocache = false; // only nocache if all writes are nocache
      size += _size;
      ios += _ios;
    }
  };

  CephContext *cct;
  bool stopping;
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  std::condition_variable cond;


  /**
   * Flush objects in lru order
   */
  list<oid_t> lru;
  map<oid_t, list<oid_t>::iterator> rev_lru;
  void remove_object(const oid_t &oid) {
    // We must hold lock here.
    auto iter = rev_lru.find(oid);
    if (iter == rev_lru.end())
      return;

    lru.erase(iter->second);
    rev_lru.erase(iter);
  }
  oid_t pop_object() {
    assert(!lru.empty());
    oid_t oid(lru.front());
    lru.pop_front();
    rev_lru.erase(oid);
    return oid;
  }
  void insert_object(const oid_t &oid) {
    assert(rev_lru.find(oid) == rev_lru.end());
    lru.push_back(oid);
    rev_lru.insert(make_pair(oid, --lru.end()));
  }

  map<oid_t, pair<PendingWB, FDRef> > pending_wbs;

  /// get next flush to perform
  bool get_next_should_flush(unique_lock& l,
    boost::tuple<oid_t, FDRef, PendingWB> *next ///< [out] next to flush
    ); ///< @return false if we are shutting down
public:
  enum FS {
    BTRFS,
    XFS
  };

private:
  FS fs;

  void set_from_conf();
public:
  WBThrottle(CephContext *cct);
  ~WBThrottle();

  void start();
  void stop();
  /// Set fs as XFS or BTRFS
  void set_fs(FS new_fs) {
    lock_guard l(lock);
    fs = new_fs;
    set_from_conf();
  }

  /// Queue wb on oid, fd taking throttle (does not block)
  void queue_wb(
    FDRef fd,		   ///< [in] FDRef to obj
    const oid_t &oid, ///< [in] object
    uint64_t offset,	   ///< [in] offset written
    uint64_t len,	   ///< [in] length written
    bool nocache	   ///< [in] try to clear out of cache after write
    );

  /// Clear all wb (probably due to sync)
  void clear();

  /// Clear object
  void clear_object(const oid_t &oid);

  /// Block until there is throttle available
  void throttle();

  /// md_config_obs_t
  const char** get_tracked_conf_keys() const;
  void handle_conf_change(const md_config_t *conf,
			  const std::set<std::string> &changed);

  /// Thread
  void *entry();
};

#endif
