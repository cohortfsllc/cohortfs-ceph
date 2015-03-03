// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef COHORT_OPQUEUE_H
#define COHORT_OPQUEUE_H

#include <functional>
#include <queue>
#include <thread>

#include <boost/intrusive/list.hpp>
#include <mutex>
#include <condition_variable>
#include "include/ceph_time.h"
#include "common/ThreadPool.h"
#include "common/likely.h"
#include "osd/OpRequest.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

class CephContext;

namespace cohort {

  namespace bi = boost::intrusive;

  template <typename LK, uint8_t N=3>
  class OpQueue {
  public:

    /* public flag values */
    static constexpr uint32_t FLAG_NONE = 0x0000;

    static constexpr uint8_t BAND_BASE = 0;
    static constexpr uint8_t BAND_HIGH = 1;

    struct Band
    {
      LK producer_lk;
      OpRequest::Queue producer_q;
      CACHE_PAD(0);
      LK consumer_lk;
      OpRequest::Queue consumer_q;
    };

    struct Lane {
      Band band[2]; // band 1 is baseline, 0 is urgent
      CACHE_PAD(0);
      Lane() {}
    };

    Lane qlane[N];
 };

} // namespace cohort

#endif // COHORT_OPQUEUE_H
