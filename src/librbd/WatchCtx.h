// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_WATCHCTX_H
#define CEPH_LIBRBD_WATCHCTX_H

#include <mutex>
#include "include/buffer.h"
#include "include/rados/librados.hpp"

class ImageCtx;

namespace librbd {

  class WatchCtx : public librados::WatchCtx {
    ImageCtx *ictx;
    bool valid;
    std::mutex lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    typedef std::unique_lock<std::mutex> unique_lock;
  public:
    uint64_t cookie;
    WatchCtx(ImageCtx *ctx) : ictx(ctx),
			      valid(true),
			      cookie(0) {}
    virtual ~WatchCtx() {}
    void invalidate();
    virtual void notify(uint8_t opcode, uint64_t ver, ceph::bufferlist& bl);
  };
}

#endif
