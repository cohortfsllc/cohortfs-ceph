// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "common/ThreadPool.h"
#include <future>
#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

CephContext *cct;

// test submit arguments
namespace {
int submit_a = 0;
int submit_b = 0;
int submit_c = 0;
int submit_d = 0;
void submit_job0() { submit_c = 3; }
void submit_job1(int &x) { x = 4; }
}
TEST(ThreadPool, Submit)
{
  cohort::ThreadPool pool(cct, 1);
  // submit with single lambda function
  ASSERT_EQ(0, pool.submit([&]() { submit_a = 1; }));
  // submit with lambda function and its argument
  ASSERT_EQ(0, pool.submit([](int &x) { x = 2; }, std::ref(submit_b)));
  // submit with standalone function
  ASSERT_EQ(0, pool.submit(submit_job0));
  // submit with standalone function and its argument
  ASSERT_EQ(0, pool.submit(submit_job1, std::ref(submit_d)));
  pool.shutdown();
  ASSERT_EQ(1, submit_a);
  ASSERT_EQ(2, submit_b);
  ASSERT_EQ(3, submit_c);
  ASSERT_EQ(4, submit_d);
}

// test that new threads are spawned when none are idle
TEST(ThreadPool, Spawn)
{
  pthread_t a, b, c;

  std::mutex mutex;
  std::condition_variable cond;
  bool block = true;

  auto fn = [&](pthread_t &tid) {
    std::unique_lock<std::mutex> lock(mutex);
    while (block)
      cond.wait(lock);
    tid = pthread_self();
  };

  // queue up multiple jobs, spawning a new worker for each
  cohort::ThreadPool pool(cct, 0);
  ASSERT_EQ(0, pool.submit(fn, std::ref(a)));
  ASSERT_EQ(0, pool.submit(fn, std::ref(b)));
  ASSERT_EQ(0, pool.submit(fn, std::ref(c)));

  // unblock the jobs
  {
    std::lock_guard<std::mutex> lock(mutex);
    block = false;
    cond.notify_all();
  }
  pool.shutdown();

  // make sure each tid is different
  ASSERT_TRUE(a != b);
  ASSERT_TRUE(b != c);
  ASSERT_TRUE(c != a);
  ASSERT_TRUE(a != pthread_self());
  ASSERT_TRUE(b != pthread_self());
  ASSERT_TRUE(c != pthread_self());
}

// test that a single worker will take more jobs from the queue
TEST(ThreadPool, Wait)
{
  pthread_t a, b, c;

  std::mutex mutex;
  std::condition_variable cond_block, cond_done;
  bool block = true;
  int jobs_done = 0;

  auto fn = [&](pthread_t &tid) {
    std::unique_lock<std::mutex> lock(mutex);
    while (block)
      cond_block.wait(lock);

    tid = pthread_self();
    ++jobs_done;
    cond_done.notify_all();
  };

  // queue up multiple jobs for a single worker
  cohort::ThreadPool pool(cct, 1);
  ASSERT_EQ(0, pool.submit(fn, std::ref(a)));
  ASSERT_EQ(0, pool.submit(fn, std::ref(b)));
  ASSERT_EQ(0, pool.submit(fn, std::ref(c)));

  // unblock the jobs
  {
    std::lock_guard<std::mutex> lock(mutex);
    block = false;
    cond_block.notify_all();
  }
  // wait for them to complete (so shutdown() thread doesn't handle any)
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (jobs_done < 3)
      cond_done.wait(lock);
  }
  pool.shutdown();

  // make sure all tids are the same
  ASSERT_EQ(a, b);
  ASSERT_EQ(b, c);
}

// test that submit will dispatch jobs to idle workers
TEST(ThreadPool, Dispatch)
{
  std::mutex mutex;
  std::condition_variable cond;
  bool block = true;

  auto fn = [&]() {
    std::lock_guard<std::mutex> lock(mutex);
    block = false;
    cond.notify_all();
  };

  // submit a job for a single worker
  cohort::ThreadPool pool(cct, 1);
  ASSERT_EQ(0, pool.submit(fn));

  // wait for the job to complete (worker returns to idle state)
  {
    std::unique_lock<std::mutex> lock(mutex);
    while (block)
      cond.wait(lock);
  }
  // submit another job
  ASSERT_EQ(0, pool.submit([](){}));
  pool.shutdown();
}

// test that threads exit after idle timeout
TEST(ThreadPool, IdleTimeout)
{
  pthread_t a, b;

  cohort::ThreadPool pool(cct, 1, cohort::ThreadPool::FLAG_NONE, 5ms);

  ASSERT_EQ(0, pool.submit([&a]() { a = pthread_self(); }));

  // sleep to make sure thread times out
  std::this_thread::sleep_for(50ms);

  ASSERT_EQ(0, pool.submit([&b]() { b = pthread_self(); }));
  pool.shutdown();

  ASSERT_TRUE(a != b);
}

// test the FLAG_DROP_JOBS_ON_SHUTDOWN flag
TEST(ThreadPool, DropJobsOnShutdown)
{
  const uint32_t flags = cohort::ThreadPool::FLAG_DROP_JOBS_ON_SHUTDOWN;

  std::mutex mutex;
  std::condition_variable cond;
  bool block = true;
  int jobs_done = 0;

  auto fn = [&]() {
    std::unique_lock<std::mutex> lock(mutex);
    while (block)
      cond.wait(lock);
    ++jobs_done;
  };

  // queue up multiple jobs for a single worker
  cohort::ThreadPool pool(cct, 1, flags);
  ASSERT_EQ(0, pool.submit(fn));
  ASSERT_EQ(0, pool.submit(fn));
  ASSERT_EQ(0, pool.submit(fn));

  // start shutdown asynchronously
  auto shutdown = std::async(std::launch::async, [&]() { pool.shutdown(); });

  // give it time to block waiting for the worker to exit
  std::this_thread::sleep_for(10ms);

  // unblock the jobs
  {
    std::lock_guard<std::mutex> lock(mutex);
    block = false;
    cond.notify_all();
  }
  // wait for shutdown to complete
  shutdown.wait();

  ASSERT_EQ(1, jobs_done); // must have dropped jobs 2 and 3
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
