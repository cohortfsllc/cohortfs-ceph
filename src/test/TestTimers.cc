#include "common/ceph_argparse.h"
#include "common/Mutex.h"
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

class TestContext;
static CephContext* cct;

namespace
{
  int arr[MAX_TEST_CONTEXTS];
  int array_idx;
  TestContext* test_contexts[MAX_TEST_CONTEXTS];

  Mutex array_lock;
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
    array_lock.Lock();
    cout << "TestContext " << num << std::endl;
    arr[array_idx++] = num;
    array_lock.Unlock();
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
    array_lock.Lock();
    cout << "StrictOrderTestContext " << num << std::endl;
    arr[num] = num;
    array_lock.Unlock();
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
static int basic_timer_test(T &timer, Mutex *lock)
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
    if (lock)
      lock->Lock();
    utime_t inc(2 * i, 0);
    utime_t t = ceph_clock_now(cct) + inc;
    timer.add_event_at(t, test_contexts[i]);
    if (lock)
      lock->Unlock();
  }

  bool done = false;
  do {
    sleep(1);
    array_lock.Lock();
    done = (array_idx == MAX_TEST_CONTEXTS);
    array_lock.Unlock();
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

static int test_out_of_order_insertion(SafeTimer &timer, Mutex *lock)
{
  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  cout << __PRETTY_FUNCTION__ << std::endl;

  test_contexts[0] = new StrictOrderTestContext(0);
  test_contexts[1] = new StrictOrderTestContext(1);

  {
    utime_t inc(100, 0);
    utime_t t = ceph_clock_now(cct) + inc;
    lock->Lock();
    timer.add_event_at(t, test_contexts[0]);
    lock->Unlock();
  }

  {
    utime_t inc(2, 0);
    utime_t t = ceph_clock_now(cct) + inc;
    lock->Lock();
    timer.add_event_at(t, test_contexts[1]);
    lock->Unlock();
  }

  int secs = 0;
  for (; secs < 100 ; ++secs) {
    sleep(1);
    array_lock.Lock();
    int a = arr[1];
    array_lock.Unlock();
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

static int safe_timer_cancel_all_test(SafeTimer &safe_timer, Mutex& safe_timer_lock)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new TestContext(i);
  }

  safe_timer_lock.Lock();
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    utime_t inc(4 * i, 0);
    utime_t t = ceph_clock_now(cct) + inc;
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  safe_timer_lock.Unlock();

  sleep(10);

  safe_timer_lock.Lock();
  safe_timer.cancel_all_events();
  safe_timer_lock.Unlock();

  for (int i = 0; i < array_idx; ++i) {
    if (arr[i] != i) {
      ret = 1;
      cout << "error: expected array[" << i << "] = " << i
	   << "; got " << arr[i] << " instead." << std::endl;
    }
  }

  return ret;
}

static int safe_timer_cancellation_test(SafeTimer &safe_timer, Mutex& safe_timer_lock)
{
  cout << __PRETTY_FUNCTION__ << std::endl;

  int ret = 0;
  memset(&arr, 0, sizeof(arr));
  array_idx = 0;
  memset(&test_contexts, 0, sizeof(test_contexts));

  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    test_contexts[i] = new StrictOrderTestContext(i);
  }

  safe_timer_lock.Lock();
  for (int i = 0; i < MAX_TEST_CONTEXTS; ++i) {
    utime_t inc(4 * i, 0);
    utime_t t = ceph_clock_now(cct) + inc;
    safe_timer.add_event_at(t, test_contexts[i]);
  }
  safe_timer_lock.Unlock();

  // cancel the even-numbered events
  for (int i = 0; i < MAX_TEST_CONTEXTS; i += 2) {
    safe_timer_lock.Lock();
    safe_timer.cancel_event(test_contexts[i]);
    safe_timer_lock.Unlock();
  }

  sleep(20);

  safe_timer_lock.Lock();
  safe_timer.cancel_all_events();
  safe_timer_lock.Unlock();

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

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  int ret;
  Mutex safe_timer_lock;
  SafeTimer safe_timer(cct, safe_timer_lock);

  ret = basic_timer_test <SafeTimer>(safe_timer, &safe_timer_lock);
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
