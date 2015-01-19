// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef RADOSBACKENDH
#define RADOSBACKENDH

#include "backend.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"

using namespace ceph;

class RadosBackend : public Backend {
  librados::IoCtx *ioctx;
public:
  RadosBackend(
    librados::IoCtx *ioctx)
    : ioctx(ioctx) {}
  void write(
    const std::string &oid_t,
    uint64_t offset,
    const ceph::bufferlist &bl,
    OSDC::op_callback&& on_applied,
    OSDC::op_callback&& on_commit);

  void read(
    const std::string &oid_t,
    uint64_t offset,
    uint64_t length,
    bufferlist *bl,
    OSDC::op_callback&& on_complete);
};

#endif
