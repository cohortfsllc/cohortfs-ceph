#include "common/mcas_skiplist.h"
#include "gtest/gtest.h"

using namespace cohort::mcas;

struct test_object : public skiplist<test_object>::object {
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

TEST(Skiplist, Get)
{
  char name[] = "test";
  gc_global gc;
  skiplist<test_object> skip(gc, test_object::cmp, name);
  test_object* obj = skip.get(test_object(5));
  ASSERT_TRUE(obj != nullptr);
}
