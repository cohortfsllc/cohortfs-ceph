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
  assert(mutex.is_locked());

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
  assert(mutex.is_locked());

  // dispatch to the first idle worker
  auto worker = idle.begin();

  worker->mutex.Lock();
  assert(!worker->job);
  worker->job = job;
  worker->cond.SignalOne();
  worker->mutex.Unlock();

  idle.erase(worker);
  return 0;
}

ThreadPool::Job ThreadPool::wait(Worker &worker)
{
  Job job;

  mutex.Lock();
  if (flags & FLAG_SHUTDOWN) {
    mutex.Unlock();
    return job;
  }

  // take a job from the queue
  if (!jobs.empty()) {
    job.swap(jobs.front());
    jobs.pop();
    mutex.Unlock();
    return job;
  }

  // enter the idle state
  idle.push_back(worker);

  worker.mutex.Lock();
  assert(!worker.job);
  mutex.Unlock();

  // wait for a signal from dispatch()
  int r;
  if (flags & FLAG_SHUTDOWN)
    r = ETIMEDOUT;
  else
    r = worker.cond.WaitInterval(cct, worker.mutex, idle_timeout);

  if (r == ETIMEDOUT) {
    worker.mutex.Unlock();
    Mutex::Locker pool_lock(mutex);
    Mutex::Locker worker_lock(worker.mutex);

    if (worker.job) // signal raced with locks
      job.swap(worker.job);
    else
      idle.erase(idle.iterator_to(worker));
    return job;
  }

  // signaled
  job.swap(worker.job);
  worker.mutex.Unlock();
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

  Mutex::Locker lock(mutex);
  workers.erase(workers.iterator_to(*worker));
  cond.SignalOne();

  dispose(std::move(worker->thread));
  delete worker;
}

void ThreadPool::dispose(std::thread &&thread)
{
  assert(mutex.is_locked());

  if (graveyard.joinable()) // join previous thread
    graveyard.join();

  // put our thread in the graveyard so we can exit before its destruction
  graveyard.swap(thread);
}

void ThreadPool::shutdown()
{
  Mutex::Locker lock(mutex);
  flags |= FLAG_SHUTDOWN;

  // signal idle threads to shut down
  for (auto i = idle.begin(); i != idle.end(); ++i) {
    Mutex::Locker worker_lock(i->mutex);
    i->cond.SignalOne();
  }

  // finish (or drop) jobs in queue
  while (!jobs.empty()) {
    if ((flags & FLAG_DROP_JOBS_ON_SHUTDOWN) == 0)
      jobs.front()();
    jobs.pop();
  }

  // join workers
  utime_t wait(1, 0);
  while (!workers.empty()) {
    cond.WaitInterval(cct, mutex, wait);
    wait = utime_t(5, 0);
  }

  if (graveyard.joinable())
    graveyard.join();
}
