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
  Mutex mutex;
  Cond cond;
  bool block = true;

  auto fn = [&]() {
    Mutex::Locker lock(mutex);
    while (block)
      cond.Wait(mutex);
  };

  // queue up multiple jobs, spawning a new worker for each
  cohort::ThreadPool pool(cct, 0);
  ASSERT_EQ(0, pool.submit(fn));
  ASSERT_EQ(0, pool.submit(fn));
  ASSERT_EQ(0, pool.submit(fn));

  // unblock the jobs
  mutex.Lock();
  block = false;
  cond.Signal();
  mutex.Unlock();

  pool.shutdown();
}

// test that a single worker will take more jobs from the queue
TEST(ThreadPool, Wait)
{
  Mutex mutex;
  Cond cond;
  bool block = true;

  auto fn = [&]() {
    Mutex::Locker lock(mutex);
    while (block)
      cond.Wait(mutex);
  };

  // queue up multiple jobs for a single worker
  cohort::ThreadPool pool(cct, 1);
  ASSERT_EQ(0, pool.submit(fn));
  ASSERT_EQ(0, pool.submit(fn));
  ASSERT_EQ(0, pool.submit(fn));

  // unblock the jobs
  mutex.Lock();
  block = false;
  cond.Signal();
  mutex.Unlock();

  pool.shutdown();
}

// test that submit will dispatch jobs to idle workers
TEST(ThreadPool, Dispatch)
{
  Mutex mutex;
  Cond cond;
  bool block = true;

  auto fn = [&]() {
    Mutex::Locker lock(mutex);
    block = false;
    cond.Signal();
  };

  // submit a job for a single worker
  cohort::ThreadPool pool(cct, 1);
  ASSERT_EQ(0, pool.submit(fn));

  // wait for the job to complete (worker returns to idle state)
  mutex.Lock();
  while (block)
    cond.Wait(mutex);
  mutex.Unlock();

  // submit another job
  ASSERT_EQ(0, pool.submit([](){}));
  pool.shutdown();
}

// test that threads exit after idle timeout
TEST(ThreadPool, IdleTimeout)
{
  pthread_t a, b;

  const utime_t timeout(0, 20000000ul); // 20ms
  cohort::ThreadPool pool(cct, 1, timeout);

  ASSERT_EQ(0, pool.submit([&a]() { a = pthread_self(); }));

  // sleep to make sure thread times out
  const utime_t wait(0, 50000000ul); // 50ms
  wait.sleep();

  ASSERT_EQ(0, pool.submit([&b]() { b = pthread_self(); }));
  pool.shutdown();

  ASSERT_TRUE(a != b);
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
