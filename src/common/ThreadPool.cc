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
  // dispatch to the first idle thread
  auto i = idle.begin();

  i->mutex.lock();
  assert(!i->job);
  i->job = job;
  i->cond.notify_one();
  i->mutex.unlock();

  idle.erase(i);
  return 0;
}

ThreadPool::Job ThreadPool::wait(Worker &worker)
{
  Job job;
  unique_lock lk(mutex);

  if (flags & FLAG_SHUTDOWN) {
    return job;
  }

  // take a job from the queue
  if (!jobs.empty()) {
    job.swap(jobs.front());
    jobs.pop();
    return job;
  }

  // enter the idle state
  idle.push_back(worker);

  unique_lock worker_lk(worker.mutex);
  assert(!worker.job);
  worker.job = Job();
  lk.unlock();

  // wait for a signal from dispatch()
  std::cv_status r;
  if (flags & FLAG_SHUTDOWN)
    r = std::cv_status::timeout;
  else {
    ceph::mono_time timeout = ceph::mono_clock::now() +
      ceph::span_from_double(idle_timeout.tv_sec); /* XXX */
    r = cond.wait_until(worker_lk, timeout);
  }

  if (r == std::cv_status::timeout) {
    worker_lk.unlock();
    unique_lock pool_lock(mutex);
    worker_lk.lock();

    if (worker.job) // signal raced with locks
      job.swap(worker.job);
    else
      idle.erase(idle.iterator_to(worker));
    return job;
  }

  // signaled
  job.swap(worker.job);
  worker.mutex.unlock();
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

  unique_lock lock(mutex);
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
  unique_lock lk(mutex);
  flags |= FLAG_SHUTDOWN;

  // signal idle threads to shut down
  for (auto i = idle.begin(); i != idle.end(); ++i)
    i->cond.notify_all(); // nb., we don't hold i->mutex

  // finish jobs in queue
  while (!jobs.empty()) {
    jobs.front()();
    jobs.pop();
  }

  // join workers
  ceph::mono_time timeout = ceph::mono_clock::now() + ceph::span_from_double(1);
  while (!workers.empty()) {
    cond.wait_until(lk, timeout);
    timeout = ceph::mono_clock::now() + ceph::span_from_double(5);
  }

  if (graveyard.joinable())
    graveyard.join();
}
