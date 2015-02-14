// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

/*
 * NB This version IGNORES rbd_cache and behaves as if
 *  it were always false.  This is because that feature depends
 *  on objectcacher, which needs to be rewritten to be useful
 *  with cohort volumes.  Adam promised he'll do just that.
 *  Sometime.  Soon.  Just not yet.  -mdw 20150105.
 */

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"

#include "librbd/internal.h"
#include "librbd/WatchCtx.h"

#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageCtx: "

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using librados::IoCtx;

namespace librbd {
  ImageCtx::ImageCtx(const string &image_name, IoCtx& p, bool ro)
    : cct((CephContext*)p.cct()),
      read_only(ro),
      flush_encountered(false),
      exclusive_locked(false),
      name(image_name),
      wctx(NULL),
      refresh_seq(0),
      last_refresh(0),
      size(0)
  {
    io_ctx.dup(p);

    memset(&header, 0, sizeof(header));
  }

  ImageCtx::~ImageCtx() {
  }

  int ImageCtx::init() {
    int r;

    r = check_exists(io_ctx, name, NULL);
    if (r < 0) {
      lderr(cct) << "error finding header: " << cpp_strerror(r) << dendl;
      return r;
    }
    header_obj = header_name(name);
    image_obj = image_name(name);
    return 0;
  }

  uint64_t ImageCtx::get_current_size() const
  {
    return size;
  }

  uint64_t ImageCtx::get_image_size() const
  {
    return size;
  }

  int ImageCtx::register_watch() {
#if 0
    assert(!wctx);
    wctx = new WatchCtx(this);
    return io_ctx.watch(header_obj, 0, &(wctx->cookie), wctx);
#endif
    return 0;
  }

  void ImageCtx::unregister_watch() {
#if 0
    assert(wctx);
    wctx->invalidate();
    io_ctx.unwatch(header_obj, wctx->cookie);
    delete wctx;
    wctx = NULL;
#endif
  }
}
