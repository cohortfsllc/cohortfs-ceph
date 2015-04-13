#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <errno.h>
#include <semaphore.h>
#include "gtest/gtest.h"

using namespace librados;

typedef RadosTest LibRadosWatchNotify;
typedef RadosTestParamPP LibRadosWatchNotifyPP;

static sem_t sem;

static void watch_notify_test_cb(uint8_t opcode, uint64_t ver, void *arg)
{
  sem_post(&sem);
}

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
    {
      sem_post(&sem);
    }
};

TEST_F(LibRadosWatchNotify, WatchNotifyTest) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t handle;
  ASSERT_EQ(0,
      rados_watch(ioctx, "foo", 0, &handle, watch_notify_test_cb, NULL));
  ASSERT_EQ(0, rados_notify(ioctx, "foo", NULL, 0));
  TestAlarm alarm;
  sem_wait(&sem);
  rados_unwatch(ioctx, "foo", handle);
  sem_destroy(&sem);
}

TEST_P(LibRadosWatchNotifyPP, WatchNotifyTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  uint64_t handle;
  WatchNotifyTestCtx ctx;
  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  std::list<obj_watch_t> watches;
  ASSERT_EQ(0, ioctx.list_watchers("foo", &watches));
  ASSERT_EQ(watches.size(), 1u);
  bufferlist bl2;
  ASSERT_EQ(0, ioctx.notify("foo", bl2));
  TestAlarm alarm;
  sem_wait(&sem);
  ioctx.unwatch("foo", handle);
  sem_destroy(&sem);
}
TEST_P(LibRadosWatchNotifyPP, WatchNotifyTimeoutTestPP) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  ioctx.set_notify_timeout(1s);
  uint64_t handle;
  WatchNotifyTestCtx ctx;

  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
  sem_destroy(&sem);
}

INSTANTIATE_TEST_CASE_P(LibRadosWatchNotifyPPTests, LibRadosWatchNotifyPP,
			::testing::Values("", "cache"));
