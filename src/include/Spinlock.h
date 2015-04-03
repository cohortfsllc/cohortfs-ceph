// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 * @author Sage Weil <sage@inktank.com>
 */

#ifndef CEPH_SPINLOCK_H
#define CEPH_SPINLOCK_H

#include <pthread.h>

class Spinlock {
  mutable pthread_spinlock_t _lock;

public:
  Spinlock() {
    pthread_spin_init(&_lock, PTHREAD_PROCESS_PRIVATE);
  }
  ~Spinlock() {
    pthread_spin_destroy(&_lock);
  }

  // don't allow copying.
  void operator=(Spinlock& s) = delete;
  Spinlock(const Spinlock& s) = delete;

  /// acquire spinlock
  void lock() const {
    pthread_spin_lock(&_lock);
  }
  /// release spinlock
  void unlock() const {
    pthread_spin_unlock(&_lock);
  }

  class Locker {
    const Spinlock& spinlock;
  public:
    Locker(const Spinlock& s) : spinlock(s) {
      spinlock.lock();
    }
    ~Locker() {
      spinlock.unlock();
    }
  };
};

namespace cohort {

  class SpinLock
  {
    std::atomic_flag flag;

  public:
    inline SpinLock() : flag(0 /* ATOMIC_FLAG_INIT */)
    {}

    inline void lock()
    {
      while(flag.test_and_set(std::memory_order_acquire))
	;
    }
    inline bool try_lock()
    {
      return !flag.test_and_set(std::memory_order_acquire);
    }
    inline void unlock()
    {
      flag.clear(std::memory_order_release);
    }
  };

} /* cohort */

#endif
