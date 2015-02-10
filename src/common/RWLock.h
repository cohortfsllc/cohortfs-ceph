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

#include <atomic>
#include <cassert>
#include <pthread.h>

class RWLock
{
  mutable pthread_rwlock_t L;
  mutable std::atomic<uint32_t> nrlock, nwlock;

public:
  RWLock(const RWLock& other) = delete;
  const RWLock& operator=(const RWLock& other) = delete;

  RWLock() : nrlock(0), nwlock(0) {
    pthread_rwlock_init(&L, NULL);
  }

  bool is_locked() const {
    return (nrlock > 0) || (nwlock > 0);
  }

  bool is_wlocked() const {
    return (nwlock > 0);
  }
  virtual ~RWLock() {
    pthread_rwlock_unlock(&L);
    pthread_rwlock_destroy(&L);
  }

  void unlock() const {
    if (nwlock > 0) {
      --nwlock;
    } else {
      assert(nrlock > 0);
      --nrlock;
    }
    int r = pthread_rwlock_unlock(&L);
    assert(r == 0);
  }

  // read
  void get_read() const {
    int r = pthread_rwlock_rdlock(&L);
    assert(r == 0);
    ++nrlock;
  }
  bool try_get_read() const {
    if (pthread_rwlock_tryrdlock(&L) == 0) {
      ++nrlock;
      return true;
    }
    return false;
  }
  void put_read() const {
    unlock();
  }

  // write
  void get_write() {
    int r = pthread_rwlock_wrlock(&L);
    assert(r == 0);
    ++nwlock;

  }
  bool try_get_write() {
    if (pthread_rwlock_trywrlock(&L) == 0) {
      ++nwlock;
      return true;
    }
    return false;
  }
  void put_write() {
    unlock();
  }

  void get(bool for_write) {
    if (for_write) {
      get_write();
    } else {
      get_read();
    }
  }

public:
  class RLocker {
    const RWLock &m_lock;

    bool locked;

  public:
    RLocker(const RWLock& lock) : m_lock(lock) {
      m_lock.get_read();
      locked = true;
    }
    void unlock() {
      assert(locked);
      m_lock.unlock();
      locked = false;
    }
    ~RLocker() {
      if (locked) {
	m_lock.unlock();
      }
    }
  };

  class WLocker {
    RWLock &m_lock;

    bool locked;

  public:
    WLocker(RWLock& lock) : m_lock(lock) {
      m_lock.get_write();
      locked = true;
    }
    void unlock() {
      assert(locked);
      m_lock.unlock();
      locked = false;
    }
    ~WLocker() {
      if (locked) {
	m_lock.unlock();
      }
    }
  };

  class Context {
    RWLock& lock;

  public:
    enum LockState {
      Untaken = 0,
      TakenForRead = 1,
      TakenForWrite = 2,
    };

  private:
    LockState state;
    bool do_release;

  public:
    Context(RWLock& l) : lock(l), do_release(false) {}
    Context(RWLock& l, LockState s) : lock(l), state(s), do_release(false) {}
    Context(RWLock& l, LockState s, bool r) : lock(l), state(s), do_release(r) {}
    ~Context() {
      if (do_release && state != Untaken) {
	lock.unlock();
      }
    }

    void get_write() {
      assert(state == Untaken);

      lock.get_write();
      state = TakenForWrite;
    }

    void get_read() {
      assert(state == Untaken);

      lock.get_read();
      state = TakenForRead;
    }

    void unlock() {
      assert(state != Untaken);
      lock.unlock();
      state = Untaken;
    }

    void promote() {
      assert(state == TakenForRead);
      unlock();
      get_write();
    }

    LockState get_state() { return state; }
    void set_state(LockState s) {
      state = s;
    }

    bool is_locked() {
      return (state != Untaken);
    }

    bool is_rlocked() {
      return (state == TakenForRead);
    }

    bool is_wlocked() {
      return (state == TakenForWrite);
    }
  };
};

#endif /* CEPH_RWLock_Posix__H */
