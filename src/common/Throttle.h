// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_THROTTLE_H
#define CEPH_THROTTLE_H

#include <atomic>
#include <list>
#include <mutex>
#include <condition_variable>
#include "include/Context.h"

class CephContext;

class Throttle {
  CephContext *cct;
  std::string name;
  std::atomic<int64_t> count;
  std::atomic<int64_t> max;
  std::mutex lock;
  std::list<std::condition_variable*> cond;

public:
  Throttle(CephContext *cct, std::string n, int64_t m = 0);
  ~Throttle();

private:
  void _reset_max(int64_t m);
  bool _should_wait(int64_t c) {
    int64_t m = max;
    int64_t cur = count;
    return
      m &&
      ((c <= m && cur + c > m) || // normally stay under max
       (c >= m && cur > m));     // except for large c
  }

  bool _wait(int64_t c, std::unique_lock<std::mutex>& l);

public:
  int64_t get_current() {
    return count;
  }

  int64_t get_max() { return max; }

  bool wait(int64_t m = 0);

  int64_t take(int64_t c = 1);
  bool get(int64_t c = 1, int64_t m = 0);

  /**
   * Returns true if it successfully got the requested amount,
   * or false if it would block.
   */
  bool get_or_fail(int64_t c = 1);
  int64_t put(int64_t c = 1);
};


/**
 * @class SimpleThrottle
 * This is a simple way to bound the number of concurrent operations.
 *
 * It tracks the first error encountered, and makes it available
 * when all requests are complete. wait_for_ret() should be called
 * before the instance is destroyed.
 *
 * Re-using the same instance isn't safe if you want to check each set
 * of operations for errors, since the return value is not reset.
 */
class SimpleThrottle {
public:
  SimpleThrottle(uint64_t max, bool ignore_enoent);
  ~SimpleThrottle();
  void start_op();
  void end_op(int r);
  int wait_for_ret();
private:
  std::mutex m_lock;
  std::condition_variable m_cond;
  uint64_t m_max;
  uint64_t m_current;
  int m_ret;
  bool m_ignore_enoent;
};

class C_SimpleThrottle : public Context {
public:
  C_SimpleThrottle(SimpleThrottle *throttle) : m_throttle(throttle) {
    m_throttle->start_op();
  }
  virtual void finish(int r) {
    m_throttle->end_op(r);
  }
private:
  SimpleThrottle *m_throttle;
};

#endif
