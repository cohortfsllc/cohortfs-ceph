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
class OSD;

namespace cohort {

  namespace bi = boost::intrusive;

  template <typename LK, uint8_t N=3>
  class OpQueue {
  public:

    typedef std::unique_lock<LK> unique_lock;

    typedef void (*op_func) (OSD*, OpRequest*);

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
    }; /* Worker */

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

      Band bands[2]; // band 0 is baseline, 1 is urgent
      std::mutex mtx;
      std::condition_variable cv;
      WorkerQueue workers;
      op_func dequeue_op_func;
      OpQueue* op_q;
      OSD* osd;
      uint32_t start_thresh;
      uint32_t n_active;
      uint32_t flags;
      CACHE_PAD(0);

      Lane(OpQueue* _q=NULL, uint32_t _start_thresh = 10)
	: op_q(_q), start_thresh(_start_thresh)
      {}

      void spawn_worker() {
	/* ASSERT mtx LOCKED */
	Worker* worker = new Worker(*this, 120);
	workers.push_back(*worker);
	auto fn = [this, worker]() {
	  this->run(worker);
	};
	worker->thread = std::thread(fn);
      }

      void run(Worker* worker) {

	std::unique_lock<std::mutex> lane_lk(mtx, std::defer_lock);

	std::array<enum Bands, 3> cycle = {
	  Bands::HIGH, Bands::BASE, Bands::HIGH
	};

      restart:
	for (;;) {
	  int n_reqs = 0;
	  uint32_t size, size_max = 0;
	  for (unsigned int ix = 0; ix < cycle.size(); ++ix) {
	    Band& b = bands[int(cycle[ix])];
	    unique_lock lk(b.producer_lk);
	    size = b.producer_q.size();
	    if (size) {
	      OpRequest& op = b.producer_q.back();
	      b.producer_q.pop_back();
	      if (size > size_max)
		size_max = size; 
	      ++n_reqs;
	      ++n_active;
	      /* dequeue op */
	      dequeue_op_func(osd, &op);
	      --n_active;
	    }
	  }

	  /* try again if we did any work */
	  if (n_reqs) {
	    if (size_max > start_thresh) {
	      lane_lk.lock();
	      spawn_worker();
	      lane_lk.unlock();
	    }
	    continue;
	  }
	  
	  ceph::mono_time timeout =
	    ceph::mono_clock::now() + std::chrono::seconds(120);
	  lane_lk.lock();
	  workers.push_back(*worker);
	  std::cv_status r = cv.wait_until(lane_lk, timeout);
	  workers.erase(workers.s_iterator_to(*worker));
	  lane_lk.unlock();
	  if (r != std::cv_status::timeout) {
	    /* signalled! */
	    goto restart;
	  }
	} /* for (inf) */

	delete worker;
	/* thread exit */

      } /* run */

    }; /* Lane */

    Lane qlane[N];

    uint8_t thread_lowat;
    uint8_t thread_hiwat;

    OpQueue(OSD* osd, op_func func, uint8_t lowat=1, uint8_t hiwat=2)
      : thread_lowat(lowat),
	thread_hiwat(hiwat) {
      for (int ix = 0; ix < N; ++ix) {
	Lane& lane = qlane[ix];
	lane.op_q = this;
	lane.dequeue_op_func = func;
      }
      /* XXXX start stuff */
    }

    Lane& lane_of_scalar(uint64_t k) {
      return qlane[(k % N)];
    }

    void enqueue(uint64_t k, OpRequest& op, enum Bands b, uint32_t flags) {
      Lane& lane = lane_of_scalar(k);
      Band& band = lane.bands[int(b)];
      unique_lock lk(band.producer_lk, std::defer_lock);
      band.producer_q.push_back(op);
      if (! lane.n_active) {
	/* maybe signal a worker */
	lk.unlock();
	std::unique_lock<std::mutex> lk(lane.mtx);
	if (lane.workers.size())
	  lane.cv.notify_one();
	else
	  lane.spawn_worker();
      }
    } /* enqueue */

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
  }; /* OpQueue */

} // namespace cohort

#endif // COHORT_OPQUEUE_H
