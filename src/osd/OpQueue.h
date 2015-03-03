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
#include <array>

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

    typedef std::unique_lock<LK> unique_lock;

    /* public flag values */
    static constexpr uint32_t FLAG_NONE = 0x0000;
    static constexpr uint32_t FLAG_LOCK = 0x0001;

    struct Lane;

    struct Worker {
      std::thread thread;
      Lane& lane;
      bi::list_member_hook<> worker_hook;

      typedef bi::list<Worker, bi::member_hook<Worker,
					       bi::list_member_hook<>,
					       &Worker::worker_hook>> Queue;

      Worker(Lane& _l, int timeout_s)
	: lane(_l) {
      }
    };

    typedef typename Worker::Queue WorkerQueue;

    enum class Bands : std::uint8_t
    {
      BASE = 0,
      HIGH
    };

    struct Band
    {
      LK producer_lk;
      OpRequest::Queue producer_q;
      CACHE_PAD(0);
      LK consumer_lk;
      OpRequest::Queue consumer_q;
    };

    struct Lane {

      static constexpr uint32_t FLAG_NONE = 0x0000;
      static constexpr uint32_t FLAG_SHUTDOWN = 0x0001;

      Band band[2]; // band 0 is baseline, 1 is urgent
      std::mutex mtx;
      std::condition_variable cv;
      WorkerQueue workers;
      uint32_t flags;
      CACHE_PAD(0);

      Lane() {}

      void run(Worker& wk) {

	std::unique_lock<std::mutex> lk(mtx, std::defer_lock);

	std::array<enum Bands, 3> cycle = {
	  Bands::HIGH, Bands::BASE, Bands::HIGH
	};

      restart:
	for (;;) {
	  int n_reqs = 0;
	  for (int ix = 0; ix < cycle.size(); ++ix) {
	    Band& band = band[int(cycle[ix])];
	    unique_lock lk(band.producer_lk);
	    if (! band.producer_q.empty()) {
	      OpRequest& op = band.producer_q.back();
	      band.producer_q.pop_back();
	      ++n_reqs;
	      // XXXX do it	      
	    }
	  }

	  /* try again if we did any work */
	  if (n_reqs)
	    continue;
	  
	  ceph::mono_time timeout =
	    ceph::mono_clock::now() + std::chrono::seconds(120);
	  lk.lock();
	  workers.push_back(wk);
	  std::cv_status r = cv.wait_until(lk, timeout);
	  workers.erase(workers.s_iterator_to(wk));
	  lk.unlock();
	  if (r != std::cv_status::timeout) {
	    /* signalled! */
	    goto restart;
	  }
	} /* for (inf) */

	/* thread exit */

      } /* run */

    };

    Lane qlane[N];

    Lane& lane_of_scalar(uint64_t k) {
      return qlane[(k % N)];
    }

    void enqueue(uint64_t k, OpRequest& op, enum Bands b, uint32_t flags) {
      Lane& lane = lane_of_scalar(k);
      Band& band = lane.band[int(b)];
      unique_lock lk(band.producer_lk, std::defer_lock);
      if (flags & FLAG_LOCK)
	lk.lock();
      band.producer_q.push_back(op);
    }

    void shutdown() {
      for (int ix = 0; ix < N; ++ix) {
	Lane& lane = qlane[ix];
	std::unique_lock<std::mutex> lk(lane.mtx);
	lane.flags |= Lane::FLAG_SHUTDOWN;
	while (! lane.workers.empty()) {
	  lane.cv.notify_all();
	  ceph::mono_time timeout = ceph::mono_clock::now() +
	    std::chrono::seconds(1);
	  lane.cv.wait_until(lk, timeout);
	}
      }
    } /* shutdown */
 };

} // namespace cohort

#endif // COHORT_OPQUEUE_H
