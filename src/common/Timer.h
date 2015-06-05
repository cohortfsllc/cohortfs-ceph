// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMMON_TIMER_H
#define COMMON_TIMER_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include <boost/intrusive/set.hpp>
#include <boost/pool/object_pool.hpp>

#include "include/ceph_time.h"

namespace cohort {

  /// Newly constructed timer should be suspended at point of
  /// construction.

  struct construct_suspended_t { };
  constexpr construct_suspended_t construct_suspended { };

  template <class TC>
  class Timer
  {
    typedef boost::intrusive::set_member_hook<
      boost::intrusive::link_mode< boost::intrusive::normal_link> > sh;

    struct event {
      typename TC::time_point t;
      uint64_t id;
      std::function<void()> f;

      sh schedule_link;
      sh event_link;

      event() : t(TC::time_point::min()), id(0) {}
      event(uint64_t _id) : t(TC::time_point::min()), id(_id) {}
      event(typename TC::time_point _t, uint64_t _id,
	    std::function<void()>&& _f) : t(_t), id(_id), f(_f) {}
      event(typename TC::time_point _t, uint64_t _id,
	    const std::function<void()>& _f) : t(_t), id(_id), f(_f) {}
      bool operator <(const event& e) {
	return t == e.t ? id < e.id : t < e.t;
      }
    };
    struct SchedCompare {
      bool operator()(const event& e1, const event& e2) const {
	return e1.t == e2.t ? e1.id < e2.id : e1.t < e2.t;
      }
    };
    struct EventCompare {
      bool operator()(const event& e1, const event& e2) const {
	return e1.id < e2.id;
      }
    };

    boost::object_pool<event> event_pool;

    boost::intrusive::set<
      event,
      boost::intrusive::member_hook<event, sh, &event::schedule_link>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::compare<SchedCompare> > schedule;

    boost::intrusive::set<
      event,
      boost::intrusive::member_hook<event, sh, &event::event_link>,
      boost::intrusive::constant_time_size<false>,
      boost::intrusive::compare<EventCompare> > events;

    std::mutex lock;
    std::condition_variable cond;

    event* running;
    uint64_t next_id;

    std::thread thread;

    void timer_thread() {
      std::unique_lock<std::mutex> l(lock);
      while (!suspended) {
	typename TC::time_point now = TC::now();

	while (!schedule.empty()) {
	  auto p = schedule.begin();
	  // Should we wait for the future?
	  if (p->t > now)
	    break;

	  event& e = *p;
	  schedule.erase(e);
	  events.erase(e);

	  // Since we have only one thread it is impossible to have more
	  // than one running event
	  running = &e;

	  l.unlock();
	  e.f();
	  l.lock();

	  if (running) {
	    running = nullptr;
	    event_pool.destroy(&e);
	  } // Otherwise the event requeued itself
	}

	if (schedule.empty())
	  cond.wait(l);
	else
	  cond.wait_until(l, schedule.begin()->t);
      }
    }

    bool suspended;

  public:
    // Create a running timer ready to execute submitted tasks.
    Timer() : running(nullptr), next_id(0), suspended(false) {
      thread = std::thread(&Timer::timer_thread, this);
    }

    // Create a suspended timer, jobs will be executed in order when
    // it is resumed.
    Timer(construct_suspended_t) : running(nullptr), suspended(true) { }

    Timer(const Timer &) = delete;
    Timer& operator=(const Timer &) = delete;

    ~Timer() {
      if (!suspended) {
	suspend();
      }
      cancel_all_events();
    }

    // Suspend operation of the timer (and let its thread die). Must
    // have been previously running.
    void suspend() {
      if (suspended)
	throw std::make_error_condition(std::errc::operation_not_permitted);

      suspended = true;
      cond.notify_one();
      thread.join();
    }


    // Resume operation of the timer. (Must have been previously
    // suspended.)
    void resume() {
      if (!suspended)
	throw std::make_error_condition(std::errc::operation_not_permitted);

      suspended = false;
      assert(!thread.joinable());
      thread = std::thread(&Timer::timer_thread, this);
    }

    // Schedule an event in the relative future
    template<typename Callable, typename... Args>
    uint64_t add_event(typename TC::duration duration,
		       Callable&& f, Args&&... args) {
      typename TC::time_point when = TC::now();
      when += duration;
      return add_event(when,
		       std::forward<Callable>(f),
		       std::forward<Args>(args)...);
    }

    // Schedule an event in the absolute future
    template<typename Callable, typename... Args>
    uint64_t add_event(typename TC::time_point when,
		       Callable&& f, Args&&... args) {
      std::lock_guard<std::mutex> l(lock);
      event& e = *(event_pool.construct(
		     when, ++next_id,
		     std::forward<std::function<void()> >(
		       std::bind(std::forward<Callable>(f),
				 std::forward<Args>(args)...))));
      auto i = schedule.insert(e);
      events.insert(e);

      /* If the event we have just inserted comes before everything
       * else, we need to adjust our timeout. */
      if (i.first == schedule.begin())
	cond.notify_one();

      // Previously each event was a context, identified by a pointer,
      // and each context to be called only once. Since you can queue
      // the same function pointer, member function, lambda, or functor
      // up multiple times, identifying things by function fdr the
      // purposes of cancellation is no longer suitable. Thus:
      return e.id;
    }

    // Cancel an event. If the event has already come and gone (or you
    // never submitted it) you will receive false. Otherwise you will
    // receive true and it is guaranteed the event will not execute.
    bool cancel_event(const uint64_t id) {
      std::lock_guard<std::mutex> l(lock);
      event dummy(id);
      auto p = events.find(dummy);
      if (p == events.end()) {
	return false;
      }

      event& e = *p;
      events.erase(e);
      schedule.erase(e);
      event_pool.destroy(&e);

      return true;
    }

    // Reschedules a currently running event in the relative
    // future. Must be called only from an event executed by this
    // timer. if you have a function that can be called either from
    // this timer or some other way, it is your responsibility to make
    // sure it can tell the difference only does not call
    // reschedule_me in the non-timer case.
    //
    // Returns an event id. If you had an event_id from the first
    // scheduling, replace it with this return value.
    uint64_t reschedule_me(typename TC::duration duration) {
      return reschedule_me(TC::now() + duration);
    }

    // Reschedules a currently running event in the absolute
    // future. Must be called only from an event executed by this
    // timer. if you have a function that can be called either from
    // this timer or some other way, it is your responsibility to make
    // sure it can tell the difference only does not call
    // reschedule_me in the non-timer case.
    //
    // Returns an event id. If you had an event_id from the first
    // scheduling, replace it with this return value.
    uint64_t reschedule_me(typename TC::time_point when) {
      if (std::this_thread::get_id() != thread.get_id())
	throw std::make_error_condition(std::errc::operation_not_permitted);
      std::lock_guard<std::mutex> l(lock);
      running->t = when;
      uint64_t id = ++next_id;
      running->id = id;
      schedule.insert(*running);
      events.insert(*running);

      // Hacky, but keeps us from being deleted
      running = nullptr;

      // Same function, but you get a new ID.
      return id;
    }

    // Remove all events from the queue.
    void cancel_all_events() {
      std::lock_guard<std::mutex> l(lock);
      while (!events.empty()) {
	auto p = events.begin();
	event& e = *p;
	schedule.erase(e);
	events.erase(e);
	event_pool.destroy(&e);
      }
    }
  };
}; // cohort

#endif
