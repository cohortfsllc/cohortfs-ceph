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

#include <thread>
#include <array>
#include <vector>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <boost/intrusive/list.hpp>
#include "include/mpmc-bounded-queue.hpp"
#include "include/ceph_time.h"
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

  /* Disruptor OpQueue */
  class OpQueue {
  public:

    static constexpr uint32_t FLAG_NONE = 0x0000;
    static constexpr uint32_t FLAG_SHUTDOWN = 0x0001;

    typedef void (*op_func) (OSD*, OpRequest*);
    typedef void (*thr_exit_func) (OSD*);

    enum class Bands : std::uint8_t
    {
      BASE = 0,
      HIGH
    };

    enum class Pos : std::uint8_t
    {
      BACK = 0,
      FRONT
    };

    typedef mpmc_bounded_queue_t<OpRequest*> mpmc_q;

    struct Worker {
      std::thread thread;
      bool leader;
      Worker(bool _leader) : leader(_leader) {}
    }; /* Worker */

  private:
    struct Lane;

    OSD* osd;
    op_func dequeue_op_func;
    thr_exit_func exit_func;
    int n_lanes;
    Lane* qlane;
    ceph::timespan worker_timeout;
    uint32_t thrd_lowat;
    uint32_t thrd_hiwat;
    uint32_t flags;

    friend class Band;
    friend class Lane;
    friend class Worker;

    struct Band {

      static constexpr uint32_t FLAG_NONE = 0x0000;
      static constexpr uint32_t FLAG_LEADER = 0x0001;

      enum Bands band;
      OpQueue* op_queue;
      std::mutex* lane_mtx;
      std::mutex mtx;
      std::condition_variable cv;
      std::atomic<uint32_t> n_workers;
      std::thread graveyard; // where exiting workers go to die
      mpmc_q queue;
      CACHE_PAD(0);

      Band() : n_workers(0), queue(1024) {};

      void spawn_worker(uint32_t flags) {
	if (n_workers < op_queue->thrd_hiwat) {
	  Worker* worker = new Worker(flags);
	  ++n_workers;
	  auto fn = [this, worker]() {
	    this->run(worker);
	  };
	  worker->thread = std::thread(fn);
	}
      }

      void run(Worker* worker) {
	uint32_t backoff = 0;
	while (worker->leader ||
	       (backoff < 200)) {
	  /* shutting down? */
	  if (op_queue->flags & FLAG_SHUTDOWN)
	    goto worker_exit;
	  OpRequest* op = nullptr;
	  queue.dequeue(op);
	  if (op) {
	    op_queue->dequeue_op_func(op_queue->osd, op);
	    /* leaders can spawn new workers */
	    if (worker->leader &&
		(! backoff) &&
		(n_workers < op_queue->thrd_hiwat))
	      spawn_worker(FLAG_NONE);
	    backoff = 0;
	    continue;
	  }
	  if (++backoff < 10)
	    continue;
	  if (backoff < 100) {
	    sched_yield();
	    continue;
	  }
	  std::unique_lock<std::mutex> lk(mtx);
	  cv.wait_for(lk, op_queue->worker_timeout);
	} /* while (backoff < 1000) */

      worker_exit:
	std::unique_lock<std::mutex> lane_lk(*lane_mtx);
        graveyard.swap(worker->thread);
        lane_lk.unlock();

	/* allow OSDVol to clean up */
	op_queue->exit_func(op_queue->osd);

        if (worker->thread.joinable()) // join previous thread
          worker->thread.join();
	--n_workers;
	delete worker;
	/* thread exit */
      } /* run */
    }; /* Band */

    struct Lane {
      std::mutex mtx;
      Band band[2];
    }; /* Lane */

  public:
    OpQueue(OSD* osd, op_func opf, thr_exit_func ef, uint16_t lanes,
	       uint8_t thrd_lowat = 1, uint8_t thrd_hiwat = 2,
	       std::chrono::milliseconds worker_timeout = 50ms)
      : osd(osd),
	dequeue_op_func(opf),
	exit_func(ef),
	n_lanes(lanes),
	worker_timeout(worker_timeout),
	thrd_lowat(thrd_lowat),
	thrd_hiwat(thrd_hiwat),
	flags(FLAG_NONE)
    {
      assert(n_lanes > 0);
      qlane = new Lane[n_lanes];
      for (int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = qlane[ix];
	for (int bix = 0; bix < 2; ++bix) {
	  Band& band = lane.band[bix];
	  band.op_queue = this;
	  band.lane_mtx = &lane.mtx;
	  band.spawn_worker(Band::FLAG_LEADER);
	}
      }
    }

    ~OpQueue() { delete[] qlane; }

    Lane& choose_lane() {
      // use rdtsc to choose the lane
      unsigned lo, hi;
      asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
      uint64_t k = static_cast<uint64_t>(hi) << 32 | lo;
      return qlane[(k % n_lanes)];
    }

    bool enqueue(OpRequest& op, enum Bands b) {
      /* don't accept work if shutting down */
      if (flags & FLAG_SHUTDOWN)
	return false;

      Lane& lane = choose_lane();
      Band& band = lane.band[int(b)];

      /* we don't have a queue front anymore, tough
       * nuggies, requeuers */
      band.queue.enqueue(&op);
      return true;
    } /* enqueue */

    void shutdown() {
      std::mutex mtx;
      std::condition_variable cv;
      std::unique_lock<std::mutex> lk(mtx);
      flags |= FLAG_SHUTDOWN;
      for (int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = qlane[ix];
	for (int bix = 0; bix < 2; ++bix) {
	  Band& band = lane.band[bix];
	  if (band.graveyard.joinable())
	    band.graveyard.join();
	  while (band.n_workers.load() > 0) {
	    cv.wait_for(lk, worker_timeout);
	  }
	}
      }
    } /* shutdown */

  }; /* Disruptor_OpQueue */

} // namespace cohort

#endif // COHORT_OPQUEUE_H
