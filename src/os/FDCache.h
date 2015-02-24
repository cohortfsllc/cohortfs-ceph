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
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_FDCACHE_H
#define CEPH_FDCACHE_H

#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>

#include "common/oid.h"
#include "common/shared_cache.hpp"
#include "include/compat.h"
#include "common/ceph_context.h"
#include "common/config.h"

/**
 * FD Cache
 */
class FDCache : public md_config_obs_t {
public:
  /**
   * FD
   *
   * Wrapper for an fd.  Destructor closes the fd.
   */
  class FD {
  public:
    const int fd;
    FD(int _fd) : fd(_fd) {
      assert(_fd >= 0);
    }
    int operator*() const {
      return fd;
    }
    ~FD() {
      VOID_TEMP_FAILURE_RETRY(::close(fd));
    }
  };

private:
  SharedLRU<oid, FD> registry;
  CephContext *cct;

public:
  FDCache(CephContext *cct) : cct(cct) {
    assert(cct);
    cct->_conf->add_observer(this);
    registry.set_size(cct->_conf->filestore_fd_cache_size);
  }
  ~FDCache() {
    cct->_conf->remove_observer(this);
  }
  typedef std::shared_ptr<FD> FDRef;

  FDRef lookup(const oid &hoid) {
    return registry.lookup(hoid);
  }

  FDRef add(const oid &hoid, int fd) {
    return registry.add(hoid, new FD(fd));
  }

  /// clear cached fd for hoid, subsequent lookups will get an empty FD
  void clear(const oid &hoid) {
    registry.clear(hoid);
    assert(!registry.lookup(hoid));
  }

  /// md_config_obs_t
  const char** get_tracked_conf_keys() const {
    static const char* KEYS[] = {
      "filestore_fd_cache_size",
      NULL
    };
    return KEYS;
  }
  void handle_conf_change(const md_config_t *conf,
			  const std::set<std::string> &changed) {
    if (changed.count("filestore_fd_cache_size")) {
      registry.set_size(conf->filestore_fd_cache_size);
    }
  }

};
typedef FDCache::FDRef FDRef;

#endif
