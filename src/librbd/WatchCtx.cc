// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"

#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "librbd/WatchCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::WatchCtx: "

namespace librbd {

  void WatchCtx::invalidate()
  {
    lock_guard l(lock);
    valid = false;
  }

  void WatchCtx::notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
  {
    lock_guard l(lock);
    ldout(ictx->cct, 1) <<  " got notification opcode=" << (int)opcode
			<< " ver=" << ver << " cookie=" << cookie << dendl;
    if (valid) {
      ImageCtx::lock_guard lictx(ictx->refresh_lock);
      ++ictx->refresh_seq;
    }
  }
}
