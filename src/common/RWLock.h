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



#ifndef CEPH_RWLock_Posix__H
#define CEPH_RWLock_Posix__H

#include <pthread.h>

class RWLock
{
  mutable pthread_rwlock_t L;

public:
  RWLock() {
    pthread_rwlock_init(&L, NULL);
  }

  virtual ~RWLock() {
    pthread_rwlock_unlock(&L);
    pthread_rwlock_destroy(&L);
  }

  void unlock() const {
    pthread_rwlock_unlock(&L);
  }

  // read
  void get_read() const {
    pthread_rwlock_rdlock(&L);
  }
  bool try_get_read() const {
    if (pthread_rwlock_tryrdlock(&L) == 0) {
      return true;
    }
    return false;
  }
  void put_read() const {
    unlock();
  }

  // write
  void get_write() {
    pthread_rwlock_wrlock(&L);
  }
  bool try_get_write() {
    if (pthread_rwlock_trywrlock(&L) == 0) {
      return true;
    }
    return false;
  }
  void put_write() {
    unlock();
  }

public:
  class RLocker {
    const RWLock &m_lock;

  public:
    RLocker(const RWLock& lock) : m_lock(lock) {
      m_lock.get_read();
    }
    ~RLocker() {
      m_lock.put_read();
    }
  };

  class WLocker {
    RWLock &m_lock;

  public:
    WLocker(RWLock& lock) : m_lock(lock) {
      m_lock.get_write();
    }
    ~WLocker() {
      m_lock.put_write();
    }
  };
};

#endif // !_Mutex_Posix_
