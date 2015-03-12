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
    typedef void (*thr_exit_func) (OSD*);

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
      bi::list_member_hook<link_mode> idle_hook;
      std::mutex mtx;
      std::condition_variable cv;
      OpRequest* mailbox;

      typedef bi::list<Worker,
		       bi::member_hook<Worker,
				       bi::list_member_hook<link_mode>,
				       &Worker::worker_hook>,
		       bi::constant_time_size<true>> Queue;

      typedef bi::list<Worker,
		       bi::member_hook<Worker,
				       bi::list_member_hook<link_mode>,
				       &Worker::worker_hook>,
		       bi::constant_time_size<true>> IdleQueue;

      Worker(Lane& _l)
	: lane(_l), mailbox(nullptr) {
      }
    }; /* Worker */

    typedef typename Worker::Queue WorkerQueue;
    typedef typename Worker::IdleQueue IdleQueue;

    struct Band
    {
      OpRequest::Queue q;
      CACHE_PAD(0);
    };

    struct Lane {

      static constexpr uint32_t FLAG_NONE = 0x0000;
      static constexpr uint32_t FLAG_SHUTDOWN = 0x0001;
      static constexpr uint32_t FLAG_LOCKED = 0x0002;

      Band bands[2]; // band 0 is baseline, 1 is urgent
      LK mtx;
      WorkerQueue workers;
      WorkerQueue idle;
      op_func dequeue_op_func;
      thr_exit_func exit_func;
      OpQueue* op_q;
      OSD* osd;
      ceph::timespan worker_timeout;
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
	unique_lock lane_lk(mtx, std::defer_lock);
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

	/* lane lock */
	unique_lock lane_lk(mtx, std::defer_lock);

	/* worker sleep lock */
	std::unique_lock<std::mutex>
	  wk_lk(worker->mtx, std::defer_lock);

	std::array<enum Bands, 3> cycle = {
	  Bands::HIGH, Bands::BASE, Bands::HIGH
	};

	for (;;) {
	  int n_reqs = 0;
	  uint32_t size, size_max = 0;
	  lane_lk.lock();
	  for (unsigned int ix = 0; ix < cycle.size(); ++ix) {
	    Band& band = bands[int(cycle[ix])];
	    size = band.q.size();
	    if (size) {
	      OpRequest& op = band.q.back();
	      band.q.pop_back(); /* dequeued */
	      if (size > size_max)
		size_max = size;
	      ++n_reqs;
	      ++n_active;
              lane_lk.unlock();
	      /* dequeue op */
	      dequeue_op_func(osd, &op);
              lane_lk.lock();
              --n_active;
	      /* try again if we did any work */
	      if (n_reqs) {
		if (size_max > start_thresh) {
		  spawn_worker(Lane::FLAG_LOCKED);
		}
		continue;
	      }
	    }
	  } /* for bands */

	  /* n_reqs == 0 */
	  idle.push_back(*worker);
	  lane_lk.unlock();
	  ceph::mono_time timeout =
	    ceph::mono_clock::now() + worker_timeout;
	  wk_lk.lock();
	  std::cv_status r = worker->cv.wait_until(wk_lk, timeout);
	  wk_lk.unlock();
	  lane_lk.lock();
	  /* remove from idle */
	  idle.erase(idle.s_iterator_to(*worker));
	  /* conditionally exit if wait timed out */
	  if (r == std::cv_status::timeout) {
	    /* cond trim workers */
	    if (workers.size() > thrd_lowat) {
	      workers.erase(workers.s_iterator_to(*worker));
	      break;
	    }
	  } else {
	    /* signalled */
	    if (flags & FLAG_SHUTDOWN) {
	      workers.erase(workers.s_iterator_to(*worker));
	      break;
	    }
	    /* !shutting down */
	    if (likely(!! worker->mailbox)) {
	      OpRequest* op = nullptr;
	      std::swap(worker->mailbox, op);
	      ++n_active;
              lane_lk.unlock();
	      dequeue_op_func(osd, op);
	      lane_lk.lock();
	      --n_active;
	    }
	  }
	  /* above thrd_lowat */
	  lane_lk.unlock();
	} /* for (inf) */

	/* allow OSDVol to clean up */
	exit_func(osd);

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
    OpQueue(OSD* osd, op_func opf, thr_exit_func ef, uint16_t lanes,
	    uint8_t thrd_lowat = 1, uint8_t thrd_hiwat = 2,
	    uint32_t start_thresh = 10,
	    std::chrono::milliseconds worker_timeout = 200ms)
      : n_lanes(lanes)
    {
      assert(n_lanes > 0);
      qlane = new Lane[n_lanes];
      for (int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = qlane[ix];
	lane.flags = Lane::FLAG_NONE;
	lane.op_q = this;
	lane.osd = osd;
	lane.n_active = 0;
	lane.dequeue_op_func = opf;
	lane.exit_func = ef;
	lane.thrd_lowat = thrd_lowat;
	lane.thrd_hiwat = thrd_hiwat;
	lane.start_thresh = start_thresh;
	lane.worker_timeout = worker_timeout;
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

    bool enqueue(OpRequest& op, enum Bands b, enum Pos p) {
      Lane& lane = choose_lane();
      Band& band = lane.bands[int(b)];
      unique_lock lane_lk(lane.mtx);
      /* don't accept work if shutting down */
      if (lane.flags & Lane::FLAG_SHUTDOWN)
	return false;
      switch (p) {
      case Pos::BACK:
	band.q.push_back(op);
	break;
      default:
	band.q.push_front(op);
      };
      /* if workers idle, hand off */
      if (lane.idle.size()) {
	Worker& worker = lane.idle.back();
	lane.idle.pop_back();
	lane_lk.unlock();
	std::unique_lock<std::mutex> wk_lock(worker.mtx);
	worker.mailbox = &op;
	worker.cv.notify_one();
	return true;
      }
      /* ensure at least one worker */
      if (! lane.workers.size()) {
	lane.spawn_worker(Lane::FLAG_LOCKED); /* ignore hiwat */
      }
      return true;
    } /* enqueue */

    void shutdown() {
      std::mutex mtx;
      std::condition_variable cv;
      std::unique_lock<std::mutex> shutdown_lk(mtx);
      for (int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = qlane[ix];
	unique_lock lane_lk(lane.mtx);
	lane.flags |= Lane::FLAG_SHUTDOWN;
	while (! lane.workers.empty()) {
	  typename WorkerQueue::iterator i;
	  for (i = lane.workers.begin();
	       i != lane.workers.end(); ++i) {
	    Worker& worker = *i;
	    std::unique_lock<std::mutex> worker_lk(worker.mtx);
	    worker.cv.notify_one();
	    worker_lk.unlock();
	  }
	  lane_lk.unlock();
	  cv.wait_for(shutdown_lk, 100ms);
	  lane_lk.lock();
	}
        if (lane.graveyard.joinable())
          lane.graveyard.join();
      }
    } /* shutdown */
  }; /* OpQueue */

} // namespace cohort

#endif // COHORT_OPQUEUE_H
