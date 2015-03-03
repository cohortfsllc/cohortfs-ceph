// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "global/global_init.h"

#include <iostream>

using std::cout;
using std::cerr;

/*
 * TestTimers
 *
 * Tests the timer classes
 */
#define MAX_TEST_EVENTS 5

typedef std::lock_guard<std::mutex> lock_guard;
typedef std::unique_lock<std::mutex> unique_lock;

static CephContext* cct;

namespace
{
  int arr[MAX_TEST_EVENTS];
  int array_idx;
  uint64_t test_ids[MAX_TEST_EVENTS];

  std::mutex array_lock;
}

void test_fun(int num)
{
  unique_lock al(array_lock);
  cout << "TestContext " << num << std::endl;
  arr[array_idx++] = num;
  al.unlock();
}

void strict_test_fun(int num)
{
  unique_lock al(array_lock);
  cout << "StrictOrderTestContext " << num << std::endl;
  arr[num] = num;
  al.unlock();
}

static void print_status(const char *str, int ret)
{
  cout << str << ": ";
  cout << ((ret == 0) ? "SUCCESS" : "FAILURE");
  cout << std::endl;
}

template <typename T>
static int basic_timer_test(T &timer)
{
  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_ids, 0, sizeof(test_ids));

  cout << __PRETTY_FUNCTION__ << std::endl;

  for (int i = 0; i < MAX_TEST_EVENTS; ++i) {
    test_ids[i] = timer.add_event(i * 2s, test_fun, i);
  }

  bool done = false;
  do {
    std::this_thread::sleep_for(1s);
    unique_lock al(array_lock);
    done = (array_idx == MAX_TEST_EVENTS);
    al.unlock();
  } while (!done);

  for (int i = 0; i < MAX_TEST_EVENTS; ++i) {
    if (arr[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << arr[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int test_out_of_order_insertion(cohort::Timer<ceph::mono_clock> &timer)
{
  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_ids, 0, sizeof(test_ids));

  cout << __PRETTY_FUNCTION__ << std::endl;

  test_ids[0] = timer.add_event(100s, strict_test_fun, 0);
  test_ids[1] = timer.add_event(2s, strict_test_fun, 1);

  int secs = 0;
  for (; secs < 100 ; ++secs) {
    std::this_thread::sleep_for(1s);
    unique_lock al(array_lock);
    int a = arr[1];
    al.unlock();
    if (a == 1)
      break;
  }

  if (secs == 100) {
    ret = 1;
    cout << "error: expected array[" << 1 << "] = " << 1
	 << "; got " << arr[1] << " instead." << std::endl;
 }

  return ret;
}

static int timer_cancel_all_test(cohort::Timer<ceph::mono_clock> &timer)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_ids, 0, sizeof(test_ids));

  for (int i = 0; i < MAX_TEST_EVENTS; ++i) {
    test_ids[i] = timer.add_event(1 * 4s, test_fun, i);
  }

  std::this_thread::sleep_for(10s);

  timer.cancel_all_events();

  for (int i = 0; i < array_idx; ++i) {
    if (arr[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << arr[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int timer_cancellation_test(cohort::Timer<ceph::mono_clock> &timer)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_ids, 0, sizeof(test_ids));

  for (int i = 0; i < MAX_TEST_EVENTS; ++i) {
    test_ids[i] = timer.add_event(i * 4s, strict_test_fun, i);
  }

  // cancel the even-numbered events
  for (int i = 0; i < MAX_TEST_EVENTS; i += 2) {
    timer.cancel_event(test_ids[i]);
  }

  std::this_thread::sleep_for(20s);

  timer.cancel_all_events();

  for (int i = 1; i < array_idx; i += 2) {
    if (arr[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << arr[i] << " instead." << std::endl;
    }
  }

  return ret;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  int ret;
  cohort::Timer<ceph::mono_clock> timer;

  ret = basic_timer_test(timer);
  if (ret)
    goto done;

  ret = timer_cancel_all_test(timer);
  if (ret)
    goto done;

  ret = timer_cancellation_test(timer);
  if (ret)
    goto done;

  ret = test_out_of_order_insertion(timer);
  if (ret)
    goto done;


done:
  print_status(argv[0], ret);
  return ret;
}
