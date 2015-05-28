// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <errno.h>

#include "common/Throttle.h"
#include "common/dout.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_throttle

#undef dout_prefix
#define dout_prefix *_dout << "throttle(" << (void*)this << ") "

Throttle::Throttle(CephContext *cct, int64_t m)
  : cct(cct), count(0), max(m)
{
  assert(m >= 0);
}

Throttle::~Throttle()
{
  while (!cond.empty()) {
    std::condition_variable *cv = cond.front();
    delete cv;
    cond.pop_front();
  }
}

void Throttle::_reset_max(int64_t m)
{
  // We are locked from our caller.
  if (!cond.empty())
    cond.front()->notify_one();
  max = m;
}

bool Throttle::_wait(int64_t c, std::unique_lock<std::mutex>& l)
{
  ceph::mono_time start;
  bool waited = false;
  if (_should_wait(c) || !cond.empty()) { // always wait behind other waiters.
    std::condition_variable *cv = new std::condition_variable;
    cond.push_back(cv);
    do {
      if (!waited) {
	ldout(cct, 2) << "_wait waiting..." << dendl;
      }
      waited = true;
      cv->wait(l);
    } while (_should_wait(c) || cv != cond.front());

    if (waited) {
      ldout(cct, 3) << "_wait finished waiting" << dendl;
    }

    delete cv;
    cond.pop_front();

    // wake up the next guy
    if (!cond.empty())
      cond.front()->notify_one();
  }
  return waited;
}

bool Throttle::wait(int64_t m)
{
  if (0 == max) {
    return false;
  }

  std::unique_lock<std::mutex> l(lock);
  if (m) {
    assert(m > 0);
    _reset_max(m);
  }
  ldout(cct, 10) << "wait" << dendl;
  return _wait(0, l);
}

int64_t Throttle::take(int64_t c)
{
  if (0 == max) {
    return 0;
  }
  assert(c >= 0);
  ldout(cct, 10) << "take " << c << dendl;
  {
    std::lock_guard<std::mutex> l(lock);
    count += c;
  }
  return count;
}

bool Throttle::get(int64_t c, int64_t m)
{
  if (0 == max) {
    return false;
  }

  assert(c >= 0);
  ldout(cct, 10) << "get " << c << " (" << count << " -> " << (count + c)
		 << ")" << dendl;
  bool waited = false;
  {
    std::unique_lock<std::mutex> l(lock);
    if (m) {
      assert(m > 0);
      _reset_max(m);
    }
    waited = _wait(c, l);
    count += c;
  }
  return waited;
}

/* Returns true if it successfully got the requested amount,
 * or false if it would block.
 */
bool Throttle::get_or_fail(int64_t c)
{
  if (0 == max) {
    return true;
  }

  assert (c >= 0);
  std::lock_guard<std::mutex> l(lock);
  if (_should_wait(c) || !cond.empty()) {
    ldout(cct, 10) << "get_or_fail " << c << " failed" << dendl;
    return false;
  } else {
    ldout(cct, 10) << "get_or_fail " << c << " success (" << count << " -> "
		   << (count + c) << ")" << dendl;
    count += c;
    return true;
  }
}

int64_t Throttle::put(int64_t c)
{
  if (0 == max) {
    return 0;
  }

  assert(c >= 0);
  ldout(cct, 10) << "put " << c << " (" << count << " -> "
		 << (count - c) << ")" << dendl;
  std::lock_guard<std::mutex> l(lock);
  if (c) {
    if (!cond.empty())
      cond.front()->notify_one();
    assert(count >= c); //if count goes negative, we failed somewhere!
    count -= c;
  }
  return count;
}

SimpleThrottle::SimpleThrottle(uint64_t max, bool ignore_enoent)
  : m_max(max),
    m_current(0),
    m_ret(0),
    m_ignore_enoent(ignore_enoent)
{
}

SimpleThrottle::~SimpleThrottle()
{
  std::lock_guard<std::mutex> l(m_lock);
  assert(m_current == 0);
}

void SimpleThrottle::start_op()
{
  std::unique_lock<std::mutex> l(m_lock);
  m_cond.wait(l, [&](){ return !(m_max == m_current); });
  ++m_current;
}

void SimpleThrottle::end_op(int r)
{
  std::lock_guard<std::mutex> l(m_lock);
  --m_current;
  if (r < 0 && !m_ret && !(r == -ENOENT && m_ignore_enoent))
    m_ret = r;
  m_cond.notify_all();
}

int SimpleThrottle::wait_for_ret()
{
  std::unique_lock<std::mutex> l(m_lock);
  m_cond.wait(l, [&](){ return !(m_current > 0); });
  return m_ret;
}
