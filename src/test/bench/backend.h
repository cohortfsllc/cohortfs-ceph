// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef BACKENDH
#define BACKENDH

#include <functional>
#include <mutex>
#include "include/buffer.h"

class Backend {
public:
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  virtual void write(
    const std::string &oid_t,
    uint64_t offset,
    const ceph::bufferlist &bl,
    std::function<void(int)>&& on_applied,
    std::function<void(int)>&& on_commit) = 0;

  virtual void read(
    const std::string &oid_t,
    uint64_t offset,
    uint64_t length,
    ceph::bufferlist *bl,
    std::function<void(int)>&& on_complete) = 0;
  virtual ~Backend() {}
};

#endif
