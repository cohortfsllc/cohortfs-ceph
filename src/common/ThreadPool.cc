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

#include "ThreadPool.h"

using namespace cohort;

int ThreadPool::spawn(Job job)
{
  Worker *worker = new Worker();
  worker->job = job;

  // add the worker to the list
  workers.push_back(*worker);

  // spawn a thread to call worker_entry()
  auto fn = [this, worker]() {
    worker_entry(worker);
  };
  worker->thread = std::thread(fn);
  return 0;
}

int ThreadPool::dispatch(Job job)
{
  // dispatch to the first idle worker
  auto worker = idle.begin();
  {
    std::lock_guard<std::mutex> lock(worker->mutex);
    assert(!worker->job);
    worker->job = job;
    worker->cond.notify_one();
  }
  idle.erase(worker);
  return 0;
}

ThreadPool::Job ThreadPool::wait(Worker &worker)
{
  Job job;

  std::unique_lock<std::mutex> pool_lock(mutex);
  if (flags & FLAG_SHUTDOWN)
    return job;

  // take a job from the queue
  if (!jobs.empty()) {
    job.swap(jobs.front());
    jobs.pop();
    return job;
  }

  // enter the idle state
  idle.push_back(worker);

  std::unique_lock<std::mutex> worker_lock(worker.mutex);
  assert(!worker.job);
  pool_lock.unlock();

  // wait for a signal from dispatch()
  std::cv_status r;
  if (flags & FLAG_SHUTDOWN)
    r = std::cv_status::timeout;
  else
    r = worker.cond.wait_for(worker_lock, idle_timeout);

  if (r == std::cv_status::timeout) {
    worker_lock.unlock();
    pool_lock.lock();
    worker_lock.lock();

    if (worker.job) // signal raced with locks
      job.swap(worker.job);
    else
      idle.erase(idle.iterator_to(worker));
    return job;
  }

  // signaled
  job.swap(worker.job);
  return job;
}

void ThreadPool::worker_entry(Worker *worker)
{
  Job job;
  assert(worker->job); // from spawn()
  job.swap(worker->job);

  do {
    job();
    job = wait(*worker);
  } while (job);

  std::lock_guard<std::mutex> lock(mutex);
  workers.erase(workers.iterator_to(*worker));
  cond.notify_one();

  dispose(std::move(worker->thread));
  delete worker;
}

void ThreadPool::dispose(std::thread &&thread)
{
  if (graveyard.joinable()) // join previous thread
    graveyard.join();

  // put our thread in the graveyard so we can exit before its destruction
  graveyard.swap(thread);
}

void ThreadPool::shutdown()
{
  std::unique_lock<std::mutex> pool_lock(mutex);
  flags |= FLAG_SHUTDOWN;

  // signal idle threads to shut down
  for (auto &worker : idle) {
    std::lock_guard<std::mutex> worker_lock(worker.mutex);
    worker.cond.notify_one();
  }

  // finish (or drop) jobs in queue
  while (!jobs.empty()) {
    if ((flags & FLAG_DROP_JOBS_ON_SHUTDOWN) == 0)
      jobs.front()();
    jobs.pop();
  }

  // join workers
  ceph::timespan wait = 1s;
  while (!workers.empty()) {
    cond.wait_for(pool_lock, wait);
    wait = 5s;
  }

  if (graveyard.joinable())
    graveyard.join();
}
