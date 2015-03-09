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
#include <mutex>
#include <condition_variable>
#include <boost/intrusive/list.hpp>
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

  template <typename LK>
  class OpQueue {
  public:
    typedef void (*op_func) (OSD*, OpRequest*);
    typedef std::unique_lock<LK> unique_lock;
    typedef bi::link_mode<bi::safe_link> link_mode; // for debugging

    /* public flag values */
    static constexpr uint32_t FLAG_NONE = 0x0000;
    static constexpr uint32_t FLAG_LOCK = 0x0001;

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

  private:
    struct Lane;

    struct Worker {
      std::thread thread;
      Lane& lane;
      bi::list_member_hook<link_mode> worker_hook;

      typedef bi::list<Worker,
		       bi::member_hook<Worker,
				       bi::list_member_hook<link_mode>,
				       &Worker::worker_hook>,
		       bi::constant_time_size<true>> Queue;

      Worker(Lane& _l)
	: lane(_l) {
      }
    }; /* Worker */

    typedef typename Worker::Queue WorkerQueue;

    struct Band
    {
      LK producer_lk;
      OpRequest::Queue producer_q;
      CACHE_PAD(0);
    };

    struct Lane {

      static constexpr uint32_t FLAG_NONE = 0x0000;
      static constexpr uint32_t FLAG_SHUTDOWN = 0x0001;
      static constexpr uint32_t FLAG_LOCKED = 0x0002;

      Band bands[2]; // band 0 is baseline, 1 is urgent
      std::mutex mtx;
      std::condition_variable cv;
      WorkerQueue workers;
      op_func dequeue_op_func;
      OpQueue* op_q;
      OSD* osd;
      std::chrono::milliseconds worker_timeout;
      uint32_t start_thresh;
      uint32_t n_active;
      uint32_t thrd_lowat;
      uint32_t thrd_hiwat;
      uint32_t flags;
      std::thread graveyard; // where exiting workers go to die
      CACHE_PAD(0);

      Lane(OpQueue* _q=NULL)
	: op_q(_q)
      {}

      void spawn_worker(uint32_t flags) {
	std::unique_lock<std::mutex> lane_lk(mtx, std::defer_lock);
	if (! (flags & Lane::FLAG_LOCKED))
	  lane_lk.lock();
	if (workers.size() <= thrd_hiwat) {
	  Worker* worker = new Worker(*this);
	  workers.push_back(*worker);
	  auto fn = [this, worker]() {
	    this->run(worker);
	  };
	  worker->thread = std::thread(fn);
	}
      }

      void run(Worker* worker) {

	std::unique_lock<std::mutex> lane_lk(mtx, std::defer_lock);

	std::array<enum Bands, 3> cycle = {
	  Bands::HIGH, Bands::BASE, Bands::HIGH
	};

	for (;;) {
	  int n_reqs = 0;
	  uint32_t size, size_max = 0;

	  for (unsigned int ix = 0; ix < cycle.size(); ++ix) {
	    Band& b = bands[int(cycle[ix])];
	    unique_lock lk(b.producer_lk);
	    size = b.producer_q.size();
	    if (size) {
	      OpRequest& op = b.producer_q.back();
	      b.producer_q.pop_back(); /* dequeued */
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
	      spawn_worker(FLAG_NONE);
	    }
	    continue;
	  }

	  /* n_reqs == 0 */
	  ceph::mono_time timeout =
	    ceph::mono_clock::now() + worker_timeout;
	  lane_lk.lock();
	  std::cv_status r = cv.wait_until(lane_lk, timeout);
	  /* conditionally exit if wait timed out */
	  if (r == std::cv_status::timeout) {
	    /* cond trim workers */
	    if (workers.size() > thrd_lowat) {
	      workers.erase(workers.s_iterator_to(*worker));
	      break;
	    }
	  }

	  /* unconditionally exit if shutdown() called */
	  if (flags & FLAG_SHUTDOWN) {
	    workers.erase(workers.s_iterator_to(*worker));
	    break;
	  }

	  /* at thrd_lowat or signalled */
	  lane_lk.unlock();
	} /* for (inf) */

        if (graveyard.joinable()) // join previous thread
          graveyard.join();
        graveyard.swap(worker->thread);
	delete worker;
	/* thread exit */
      } /* run */

    }; /* Lane */

    int n_lanes;
    Lane* qlane;

  public:
    OpQueue(OSD* osd, op_func func, uint16_t lanes,
	    uint8_t thrd_lowat = 1, uint8_t thrd_hiwat = 2,
	    uint32_t start_thresh = 10,
	    std::chrono::milliseconds worker_timeout = 200ms)
      : n_lanes(lanes)
    {
      assert(n_lanes > 0);
      qlane = new Lane[n_lanes];
      for (int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = qlane[ix];
	lane.op_q = this;
	lane.osd = osd;
	lane.dequeue_op_func = func;
	lane.thrd_lowat = thrd_lowat;
	lane.thrd_hiwat = thrd_hiwat;
	lane.start_thresh = start_thresh;
	lane.worker_timeout = worker_timeout;
      }
    }

    ~OpQueue() { delete[] qlane; }

    Lane& lane_of_scalar(uint64_t k) {
      return qlane[(k % n_lanes)];
    }

    bool enqueue(uint64_t k, OpRequest& op, enum Bands b, enum Pos p) {
      Lane& lane = lane_of_scalar(k);
      Band& band = lane.bands[int(b)];
      unique_lock producer_lock(band.producer_lk);
      /* don't accept work if shutting down */
      if (lane.flags & Lane::FLAG_SHUTDOWN)
	return false;
      switch (p) {
      case Pos::BACK:
	band.producer_q.push_back(op);
	break;
      default:
	band.producer_q.push_front(op);
      };
      if (! lane.n_active) {
	/* maybe signal a worker */
	producer_lock.unlock();
	std::unique_lock<std::mutex> lane_lock(lane.mtx);
	if (! lane.n_active) {
	  /* no workers active */
	  if (lane.workers.size())
	    lane.cv.notify_one();
	  else
	    lane.spawn_worker(Lane::FLAG_LOCKED); /* ignore hiwat */
	}
      }
      return true;
    } /* enqueue */

    void shutdown() {
      for (int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = qlane[ix];
	std::unique_lock<std::mutex> lane_lk(lane.mtx);
	lane.flags |= Lane::FLAG_SHUTDOWN;
	while (! lane.workers.empty()) {
	  lane.cv.notify_all();
	  ceph::mono_time timeout = ceph::mono_clock::now() +
	    std::chrono::seconds(1);
	  lane.cv.wait_until(lane_lk, timeout);
	}
        if (lane.graveyard.joinable())
          lane.graveyard.join();
      }
    } /* shutdown */
  }; /* OpQueue */

} // namespace cohort

#endif // COHORT_OPQUEUE_H
