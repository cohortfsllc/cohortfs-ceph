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
#define MAX_TEST_CONTEXTS 5

typedef std::lock_guard<std::mutex> lock_guard;
typedef std::unique_lock<std::mutex> unique_lock;

class TestContext;
static CephContext* cct;

namespace
{
  int arr[MAX_TEST_CONTEXTS];
  int array_idx;
  TestContext* test_contexts[MAX_TEST_CONTEXTS];

  std::mutex array_lock;
}

class TestContext : public Context
{
public:
  TestContext(int num_)
    : num(num_)
  {
  }

  virtual void finish(int r)
  {
    unique_lock al(array_lock);
    cout << "TestContext " << num << std::endl;
    arr[array_idx++] = num;
    al.unlock();
  }

  virtual ~TestContext()
  {
  }

protected:
  int num;
};

class StrictOrderTestContext : public TestContext
{
public:
  StrictOrderTestContext (int num_)
    : TestContext(num_)
  {
  }

  virtual void finish(int r)
  {
    unique_lock al(array_lock);
    cout << "StrictOrderTestContext " << num << std::endl;
    arr[num] = num;
    al.unlock();
  }

  virtual ~StrictOrderTestContext()
  {
  }
};

static void print_status(const char *str, int ret)
{
  cout << str << ": ";
  cout << ((ret == 0) ? "SUCCESS" : "FAILURE");
  cout << std::endl;
}

template <typename T>
static int basic_timer_test(T &timer, std::mutex *lock)
{
  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  cout << __PRETTY_FUNCTION__ << std::endl;

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }


  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    unique_lock l;
    if (lock)
      l = unique_lock(*lock);
    ceph::mono_time t = ceph::mono_clock::now() + i * 2s;
    timer.add_event_at(t, test_contexts[i]);
    if (lock)
      l.unlock();
  }

  bool done = false;
  do {
    sleep(1);
    unique_lock al(array_lock);
    done = (array_idx == MAX_TEST_CONTEXTS);
    al.unlock();
  } while (!done);

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    if (arr[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << arr[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int test_out_of_order_insertion(SafeTimer<ceph::mono_clock> &timer,
				       std::mutex *lock)
{
  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  cout << __PRETTY_FUNCTION__ << std::endl;

  test_contexts[0] = new StrictOrderTestContext(0);
  test_contexts[1] = new StrictOrderTestContext(1);

  {
    auto t = ceph::mono_clock::now() + 100s;
    unique_lock l(*lock);
    timer.add_event_at(t, test_contexts[0]);
    l.unlock();
  }

  {
    auto t = ceph::mono_clock::now() + 2s;
    unique_lock l(*lock);
    timer.add_event_at(t, test_contexts[1]);
    l.unlock();
  }

  int secs = 0;
  for (; secs < 100 ; ++secs) {
    sleep(1);
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

static int safe_timer_cancel_all_test(SafeTimer<ceph::mono_clock> &safe_timer,
				      std::mutex& safe_timer_lock)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }

  unique_lock stl(safe_timer_lock);
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    auto t = ceph::mono_clock::now() + i * 4s;
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  stl.unlock();

  sleep(10);

  stl.lock();
  safe_timer.cancel_all_events();
  stl.unlock();

  for (int i = 0; i < array_idx; ++i) {
    if (arr[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << arr[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int safe_timer_cancellation_test(
  SafeTimer<ceph::mono_clock> &safe_timer, std::mutex& safe_timer_lock)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new StrictOrderTestContext(i);
  }

  unique_lock stl(safe_timer_lock);
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    auto t = ceph::mono_clock::now() + i * 4s;
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  stl.unlock();

  // cancel the even-numbered events
  for (int i = 0; i < MAX_TEST_CONTEXTS; i += 2) {
    stl.lock();
    safe_timer.cancel_event(test_contexts[i]);
    stl.unlock();
  }

  sleep(20);

  stl.lock();
  safe_timer.cancel_all_events();
  stl.unlock();

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

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      0);
  common_init_finish(cct);

  int ret;
  std::mutex safe_timer_lock;
  SafeTimer<ceph::mono_clock> safe_timer(safe_timer_lock);

  ret = basic_timer_test<SafeTimer<
			   ceph::mono_clock> >(safe_timer, &safe_timer_lock);
  if (ret)
    goto done;

  ret = safe_timer_cancel_all_test(safe_timer, safe_timer_lock);
  if (ret)
    goto done;

  ret = safe_timer_cancellation_test(safe_timer, safe_timer_lock);
  if (ret)
    goto done;

  ret = test_out_of_order_insertion(safe_timer, &safe_timer_lock);
  if (ret)
    goto done;

done:
  print_status(argv[0], ret);
  return ret;
}
