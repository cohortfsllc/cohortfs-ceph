// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef BACKENDH
#define BACKENDH

#include "include/buffer.h"
#include "include/Context.h"

class Backend {
public:
  virtual void write(
    const std::string &oid,
    uint64_t offset,
    const ceph::bufferlist &bl,
    Context *on_applied,
    Context *on_commit) = 0;

  virtual void read(
    const std::string &oid,
    uint64_t offset,
    uint64_t length,
    ceph::bufferlist *bl,
    Context *on_complete) = 0;
  virtual ~Backend() {}
};

#endif
