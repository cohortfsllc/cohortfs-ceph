// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef COHORT_WQE_H
#define COHORT_WQE_H

#include <stdint.h>
#include <boost/intrusive/list.hpp>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include "common/likely.h"

namespace cohort {

  namespace bi = boost::intrusive;

  typedef bi::link_mode<bi::safe_link> link_mode; // for debugging

  struct WaitEntry {
    std::mutex mtx;
    std::condition_variable cv;
  };

  template <typename T>
  class WaitQueueEntry {
  public:
    uint32_t flags;
    WaitEntry lwe; /* left */
    bi::list_member_hook<link_mode> q_hook;
    T data;

    typedef bi::list<WaitQueueEntry,
		     bi::member_hook<WaitQueueEntry,
				     bi::list_member_hook<link_mode>,
				     &WaitQueueEntry::q_hook>,
		     bi::constant_time_size<true>> Queue;

    WaitQueueEntry(T _data)
      : data(_data)
    {}

    const T& get() { return data; }

  }; /* WaitQueueEntry */

  template <typename T>
  class WaitQueue {
  public:
    typedef WaitQueueEntry<T> Entry;
    typedef typename WaitQueueEntry<T>::Queue Queue;
    typedef typename Queue::iterator iterator;

    uint32_t flags;
    uint32_t waiters;
    WaitEntry we;
    Queue queue;

    static constexpr uint32_t LFLAG_NONE = 0x0000;
    static constexpr uint32_t LFLAG_WAIT_SYNC = 0x0001;
    static constexpr uint32_t LFLAG_SYNC_DONE = 0x0002;

    static constexpr uint32_t FLAG_SIGNAL = 0x0001;
    static constexpr uint32_t FLAG_LOCKED = 0x0002;

    typedef std::unique_lock<std::mutex> unique_lock;

    WaitQueue() : flags(LFLAG_NONE)
    {}

    void wait_on(Entry& e) {
      /* set up unshared wait entry */
      unique_lock lk(e.lwe.mtx);
      e.flags = LFLAG_WAIT_SYNC;
      /* enqueue on shared waitq */
      we.mtx.lock();
      ++waiters;
      queue.push_back(e);
      we.mtx.unlock(); /* release interlock */
      e.lwe.cv.wait(lk);
    } /* wait_on */

    void dequeue(Entry& e, uint32_t flags) {
      iterator it = Queue::s_iterator_to(e);
      if (! (flags & FLAG_LOCKED))
	we.mtx.lock();
      --waiters;
      if (flags & FLAG_SIGNAL) {
	unique_lock lk(e.lwe.mtx);
	e.flags &= ~LFLAG_WAIT_SYNC;
	e.flags |= LFLAG_SYNC_DONE;
	e.lwe.cv.notify_one();
      }
      if (! (flags & FLAG_LOCKED))
	we.mtx.unlock();
    } /* dequeue */
  };

} /* namespace cohort */

#endif /* COHORT_WQE_H */
