#include "common/mcas_skiplist.h"
#include "gtest/gtest.h"

using namespace cohort::mcas;

struct test_object : public skiplist_object {
  int key;
  test_object(int key) : key(key) {}
  test_object(test_object &&rhs) : key(0) { std::swap(key, rhs.key); }

  static int cmp(const void *lhs, const void *rhs) {
    const test_object *l = static_cast<const test_object*>(lhs);
    const test_object *r = static_cast<const test_object*>(rhs);
    if (l->key == r->key)
      return 0;
    if (l->key > r->key)
      return 1;
    return -1;
  }
};
void intrusive_ptr_add_ref(test_object *p) { p->get(); }
void intrusive_ptr_release(test_object *p) { p->put(); }

gc_global gc;

TEST(Skiplist, Get)
{
  obj_cache cache(gc, sizeof(test_object), "test");
  skiplist<test_object> skip(gc, cache, test_object::cmp);
  skip_stats stats;

  gc_guard guard(gc);
  auto obj = skip.get(guard, test_object(5));
  ASSERT_EQ(nullptr, obj);
  skip.get_stats(&stats);
  ASSERT_EQ(1, stats.gets);
  ASSERT_EQ(1, stats.gets_miss);

  obj = skip.get_or_create(guard, test_object(5));
  ASSERT_TRUE(obj != nullptr);
  skip.get_stats(&stats);
  ASSERT_EQ(2, stats.gets);
  ASSERT_EQ(1, stats.gets_created);
  ASSERT_EQ(0, stats.gets_existing);
  ASSERT_EQ(0, stats.puts);

  obj.reset();
  skip.get_stats(&stats);
  ASSERT_EQ(1, stats.puts);
  ASSERT_EQ(1, stats.puts_last);

  obj = skip.get(guard, test_object(5));
  skip.get_stats(&stats);
  ASSERT_EQ(3, stats.gets);
  ASSERT_EQ(1, stats.gets_existing);

  obj.reset();
  skip.get_stats(&stats);
  ASSERT_EQ(2, stats.puts);
  ASSERT_EQ(2, stats.puts_last);
}

TEST(Skiplist, Reap)
{
  obj_cache cache(gc, sizeof(test_object), "test");
  skiplist<test_object> skip(gc, cache, test_object::cmp, 0, 1);

  gc_guard guard(gc);
  // hold a reference to the first object
  auto obj = skip.get_or_create(guard, test_object(1));
  // drop the reference on the rest
  skip.get_or_create(guard, test_object(2));
  skip.get_or_create(guard, test_object(3));
  skip.get_or_create(guard, test_object(4));

  // let the reaper thread run; it shouldn't sleep
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  skip_stats stats;
  skip.get_stats(&stats);
  ASSERT_EQ(3, stats.reaped);
  ASSERT_EQ(1, stats.size);
}