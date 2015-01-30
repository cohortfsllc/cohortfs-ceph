// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_TIMER_H
#define CEPH_TIMER_H

#include <condition_variable>
#include <map>
#include <mutex>
#include <sstream>
#include <signal.h>
#include <sys/time.h>
#include <math.h>
#include <include/Context.h>

#include "include/ceph_time.h"
#include "common/Thread.h"

class CephContext;

template <class TC>
class SafeTimerThread;

template <class TC>
class SafeTimer
{
  typedef std::multimap <typename TC::time_point, Context *> scheduled_map_t;
  typedef std::map < Context*,
		     typename scheduled_map_t::iterator > event_lookup_map_t;

  // This class isn't supposed to be copied
  SafeTimer(const SafeTimer &rhs);
  SafeTimer& operator=(const SafeTimer &rhs);

  std::mutex& lock;
  std::condition_variable cond;
  bool safe_callbacks;

  friend class SafeTimerThread<TC>;
  SafeTimerThread<TC> *thread;

  void timer_thread() {
    std::unique_lock<std::mutex> l(lock);
    while (!stopping) {
      typename TC::time_point now = TC::now();

      while (!schedule.empty()) {
	auto p = schedule.begin();

	// is the future now?
	if (p->first > now)
	  break;

	Context *callback = p->second;
	events.erase(callback);
	schedule.erase(p);

	if (!safe_callbacks)
	  l.unlock();
	callback->complete(0);
	if (!safe_callbacks)
	  l.lock();
      }

      if (schedule.empty())
	cond.wait(l);
      else
	cond.wait_until(l, schedule.begin()->first);
    }
  }

  std::multimap<typename TC::time_point, Context*> schedule;
  std::map<Context*,
	   typename std::multimap<typename TC::time_point,
				  Context*>::iterator> events;
  bool stopping;

public:
  /* Safe callbacks determines whether callbacks are called with the lock
   * held.
   *
   * safe_callbacks = true (default option) guarantees that a cancelled
   * event's callback will never be called.
   *
   * Under some circumstances, holding the lock can cause lock cycles.
   * If you are able to relax requirements on cancelled callbacks, then
   * setting safe_callbacks = false eliminates the lock cycle issue.
   * */
  SafeTimer(std::mutex& l, bool safe_callbacks=true)
    : lock(l),
      safe_callbacks(safe_callbacks),
      thread(NULL),
      stopping(false) { }

  ~SafeTimer() {
    assert(thread == NULL);
  }


  /* Call with the event_lock UNLOCKED.
   *
   * Cancel all events and stop the timer thread.
   *
   * If there are any events that still have to run, they will need to take
   * the event_lock first. */
  void init() {
    thread = new SafeTimerThread<TC>(this);
    thread->create();
  }

  void shutdown(std::unique_lock<std::mutex>& l) {
    if (thread) {
      cancel_all_events();
      stopping = true;
      cond.notify_all();
      l.unlock();
      thread->join();
      l.lock();
      delete thread;
      thread = NULL;
    }
  }

  /* Schedule an event in the future
   * Call with the event_lock LOCKED */
  void add_event_after(typename TC::duration duration, Context *callback) {
    typename TC::time_point when = TC::now();
    when += duration;
    add_event_at(when, callback);
  }

  void add_event_at(typename TC::time_point when, Context *callback) {
    typename scheduled_map_t::value_type s_val(when, callback);
    auto i = schedule.insert(s_val);

    typename event_lookup_map_t::value_type e_val(callback, i);
    auto rval = events.insert(e_val);

    /* If you hit this, you tried to insert the same Context* twice. */
    assert(rval.second);

    /* If the event we have just inserted comes before everything
     * else, we need to adjust our timeout. */
    if (i == schedule.begin())
      cond.notify_all();
  }

  /* Cancel an event.
   * Call with the event_lock LOCKED
   *
   * Returns true if the callback was cancelled.
   * Returns false if you never addded the callback in the first place.
   */
  bool cancel_event(Context *callback) {
    auto p = events.find(callback);
    if (p == events.end()) {
      return false;
    }

    delete p->first;

    schedule.erase(p->second);
    events.erase(p);
    return true;
  }

  /* Cancel all events.
   * Call with the event_lock LOCKED
   *
   * When this function returns, all events have been cancelled, and there are no
   * more in progress.
   */
  void cancel_all_events() {
    while (!events.empty()) {
      auto p = events.begin();
      delete p->first;
      schedule.erase(p->second);
      events.erase(p);
    }
  }
};

template <typename TC>
class SafeTimerThread : public Thread {
  SafeTimer<TC> *parent;
public:
  SafeTimerThread(SafeTimer<TC> *s) : parent(s) {}
  void *entry() {
    parent->timer_thread();
    return NULL;
  }
};

#endif
