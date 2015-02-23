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

#ifndef COHORT_THREADPOOL_H
#define COHORT_THREADPOOL_H

#include <functional>
#include <queue>
#include <thread>

#include <boost/intrusive/list.hpp>
#include <mutex>
#include <condition_variable>
#include "include/ceph_time.h"
#include "common/likely.h"

class CephContext;

namespace cohort {

namespace bi = boost::intrusive;

class ThreadPool {
 public:

  typedef std::unique_lock<std::mutex> unique_lock;

ThreadPool(CephContext *cct, uint32_t max_threads = 0,
	     struct timespec idle_timeout = {120, 0})
    : cct(cct),
      max_threads(max_threads),
      idle_timeout(idle_timeout),
      flags(FLAG_NONE)
  {}

  /// submit a job, specified by a function and its arguments
  template<typename F, typename ...Args>
  int submit(F&& f, Args&&... args);

  /// shut down worker threads and wait for them to finish
  void shutdown();

 private:
  typedef std::function<void()> Job;

  enum {
    FLAG_NONE = 0x0,
    FLAG_SHUTDOWN = 0x1,
  };

  struct Worker {
    std::thread thread;
    Job job;
    std::mutex mutex;
    std::condition_variable cond;

    bi::list_member_hook<> pool_hook;
    bi::list_member_hook<> idle_hook;

    typedef bi::list<Worker, bi::member_hook<Worker, bi::list_member_hook<>,
                                            &Worker::pool_hook>> PoolQueue;
    typedef bi::list<Worker, bi::member_hook<Worker, bi::list_member_hook<>,
                                            &Worker::idle_hook>> IdleQueue;
  };

  CephContext *const cct;
  const uint32_t max_threads;
  const struct timespec idle_timeout;
  uint32_t flags;

  std::mutex mutex;
  std::condition_variable cond;
  std::queue<Job> jobs;
  Worker::PoolQueue workers;
  Worker::IdleQueue idle;
  std::thread graveyard;

  /// spawn a new thread to handle a job
  int spawn(Job job);

  /// dispatch a job to an idle thread
  int dispatch(Job job);

  /// wait for the next queued job, or an empty function on shutdown
  Job wait(Worker &worker);

  /// worker thread entry function
  void worker_entry(Worker *worker);

  /// dispose of a thread safely on exit
  void dispose(std::thread &&thread);
};


template<typename F, typename ...Args>
int ThreadPool::submit(F&& f, Args&&... args)
{
  auto job = std::bind(f, args...);

  unique_lock lock(mutex);

  // queue is draining
  if (unlikely(flags & FLAG_SHUTDOWN))
    return -1;

  // idle thread(s) available
  if (!idle.empty())
    return dispatch(job);

  // need a thread
  if (max_threads == 0 || workers.size() < max_threads)
    return spawn(job);

  // add to the job queue
  jobs.push(job);
  return 0;
}

} // namespace cohort

#endif // COHORT_THREADPOOL_H
