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

#ifndef CEPH_MUTEX_H
#define CEPH_MUTEX_H

#include <iostream>
#include <cassert>
#include <pthread.h>

enum {
  l_mutex_first = 999082,
  l_mutex_wait,
  l_mutex_last
};

class Mutex {
private:
  const char *name;
  bool recursive;

  pthread_mutex_t _m;
  int nlock;
  pthread_t locked_by;

  // don't allow copying.
  void operator=(Mutex &M);
  Mutex(const Mutex &M);

public:

  Mutex(bool r = false) : recursive(r), nlock(0), locked_by(0)
    {
      if (recursive) {
	// Mutexes of type PTHREAD_MUTEX_RECURSIVE do all the same checks as
	// mutexes of type PTHREAD_MUTEX_ERRORCHECK.
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&_m,&attr);
	pthread_mutexattr_destroy(&attr);
      } else {
	// If the mutex type is PTHREAD_MUTEX_NORMAL, deadlock detection
	// shall not be provided. Attempting to relock the mutex causes
	// deadlock. If a thread attempts to unlock a mutex that  it  has not
	// locked or a mutex which is unlocked, undefined behavior results.
	pthread_mutex_init(&_m, NULL);
      }
    }
  ~Mutex()
    {
      assert(nlock == 0);
      int r = pthread_mutex_destroy(&_m);
#if 0
      /* the original ceph code ignored failures from pthread_mutex_destroy()
       * There's an inherent problem destroying a mutex that's in use by
       * by another thread, and the ceph code can do this.  My test case:
       * interrupting "ceph" python script trying to connect to a dead ceph-mon.
       * This results in thread #2 calling rados_shutdown() while thread #1
       * is stuck waiting inside of rados_connect().  -mdw 20140912
       */
      assert(r == 0);
#else
      if (r != 0) {
	std::cerr << "Mutex::~Mutex: pthread_mutex_destroy returned " << r << std::endl;
      }
#endif
    }
  bool is_locked() const {
    return (nlock > 0);
  }
  bool is_locked_by_me() const {
    return nlock > 0 && locked_by == pthread_self();
  }

  bool TryLock() {
    int r = pthread_mutex_trylock(&_m);
    if (r == 0)
      _post_lock();
    return r == 0;
  }

  void Lock()
    {
      int r = pthread_mutex_lock(&_m);
      assert(r == 0);
      _post_lock();
    }

  void _post_lock() {
    if (!recursive) {
      assert(nlock == 0);
      locked_by = pthread_self();
    };
    nlock++;
  }

  void _pre_unlock() {
    assert(nlock > 0);
    --nlock;
    if (!recursive) {
      assert(locked_by == pthread_self());
      locked_by = 0;
      assert(nlock == 0);
    }
  }
  void Unlock() {
    _pre_unlock();
    int r = pthread_mutex_unlock(&_m);
    assert(r == 0);
  }

  friend class Cond;


public:
  class Locker {
    Mutex &mutex;

  public:
    Locker(Mutex& m) : mutex(m) {
      mutex.Lock();
    }
    ~Locker() {
      mutex.Unlock();
    }
  };
};

#endif
