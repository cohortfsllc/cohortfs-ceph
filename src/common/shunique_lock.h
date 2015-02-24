// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMMON_SHUNIQUE_LOCK_H
#define COMMON_SHUNIQUE_LOCK_H

#include <mutex>
#include <shared_mutex>
#include <system_error>

namespace ceph {

  // This is a 'lock' class in the style of shared_lock and
  // unique_lock. Like shared_timed_mutex it implements both Lockable
  // and SharedLockable.

  // My rationale is thus: one of the advantages of unique_lock is
  // that I can pass a thread of execution's control of a lock around
  // as a parameter. So that methods further down the call stack can
  // unlock it, do something, relock it, and have the lock state
  // be known by the caller afterward, explicitly. The shared_lock
  // class offers a similar advantage to shared_lock, but each class
  // if one or the other. In Objecter we have calls that in most cases
  // need /a/ lock on the shared mutex, and whether it's shared or
  // exclusive doesn't matter. In some circumstances they may drop the
  // shared lock and reacquire an exclusive one. Right now this is
  // handled by passing both a shared and unique lock down the call
  // stack. This is vexacious and shameful.

  // Wanting to avoid heaping shame and vexation upon myself, I threw
  // this class together to fix that.

  // This class makes no attempt to support atomic upgrade or
  // downgrade. I don't want either. Matt has convinced me that if you
  // think you want them you've usually made a mistake somewhere. It
  // is exactly and only a reification of the state held on a shared
  // mutex.

  /// Acquire unique ownership of the mutex.
  struct acquire_unique_t { };

  /// Acquire shared ownership of the mutex.
  struct acquire_shared_t { };

  constexpr acquire_unique_t acquire_unique { };
  constexpr acquire_shared_t acquire_shared { };

  template<typename _Mutex>
  class shunique_lock
  {
  public:
    typedef _Mutex mutex_type;
    typedef std::unique_lock<_Mutex> unique_lock_type;
    typedef std::shared_lock<_Mutex> shared_lock_type;

    shunique_lock() noexcept : m(nullptr), o(ownership::none) { }

    // We do not provide a default locking/try_locking constructor
    // that takes only the mutex, since it is not clear whether to
    // take it shared or unique. We explicitly require the use of
    // lock_deferred to prevent Nasty Surprises.

    shunique_lock(mutex_type& _m, std::defer_lock_t) noexcept
      : m(&_m), o(ownership::none) { }

    shunique_lock(mutex_type& _m, acquire_unique_t)
      : m(&_m), o(ownership::none) {
      lock();
    }

    shunique_lock(mutex_type& _m, acquire_shared_t)
      : m(&_m), o(ownership::none) {
      lock_shared();
    }

    shunique_lock(mutex_type& _m, acquire_unique_t, std::try_to_lock_t)
      : m(&_m), o(ownership::none) {
      try_lock();
    }

    shunique_lock(mutex_type& _m, acquire_shared_t, std::try_to_lock_t)
      : m(&_m), o(ownership::none) {
      try_lock_shared();
    }

    shunique_lock(mutex_type& _m, acquire_unique_t, std::adopt_lock_t)
      : m(&_m), o(ownership::unique) {
      // You'd better actually have a lock, or I will find you and I
      // will hunt you down.
    }

    shunique_lock(mutex_type& _m, acquire_shared_t, std::adopt_lock_t)
      : m(&_m), o(ownership::shared) {
    }

    template<typename _Clock, typename _Duration>
    shunique_lock(mutex_type& _m, acquire_unique_t,
		  const std::chrono::time_point<_Clock, _Duration>& atime)
      : m(&_m), o(ownership::none) {
      try_lock_until(atime);
    }

    template<typename _Clock, typename _Duration>
    shunique_lock(mutex_type& _m, acquire_shared_t,
		  const std::chrono::time_point<_Clock, _Duration>& atime)
      : m(&_m), o(ownership::none) {
      try_lock_shared_until(atime);
    }

    template<typename _Rep, typename _Period>
    shunique_lock(mutex_type& _m, acquire_unique_t,
		  const std::chrono::duration<_Rep, _Period>& dur)
      : m(&_m), o(ownership::none) {
      try_lock_for(dur);
    }

    template<typename _Rep, typename _Period>
    shunique_lock(mutex_type& _m, acquire_shared_t,
		  const std::chrono::duration<_Rep, _Period>& dur)
      : m(&_m), o(ownership::none) {
      try_shared_lock_for(dur);
    }

    ~shunique_lock() {
      switch (o) {
      case ownership::none:
	return;
	break;
      case ownership::unique:
	m->unlock();
	break;
      case ownership::shared:
	m->unlock_shared();
	break;
      }
    }

    shunique_lock(shunique_lock const&) = delete;
    shunique_lock& operator=(shunique_lock const&) = delete;

    shunique_lock(shunique_lock&& l) noexcept : shunique_lock() {
      swap(l);
    }

    shunique_lock(unique_lock_type&& l) noexcept {
      if (l.owns()) {
	o = ownership::unique;
      }
      m = l.release();
    }

    shunique_lock(shared_lock_type&& l) noexcept {
      if (l.owns()) {
	o = ownership::shared;
      }
      m = l.release();
    }

    shunique_lock& operator=(shunique_lock&& l) noexcept {
      shunique_lock(std::move(l)).swap(*this);
      return *this;
    }

    shunique_lock& operator=(unique_lock_type&& l) noexcept {
      shunique_lock(std::move(l)).swap(*this);
      return *this;
    }

    shunique_lock& operator=(shared_lock_type&& l) noexcept {
      shunique_lock(std::move(l)).swap(*this);
      return *this;
    }

    void lock() {
      lockable();
      m->lock();
      o = ownership::unique;
    }

    void lock_shared() {
      lockable();
      m->lock_shared();
      o = ownership::shared;
    }

    bool try_lock() {
      lockable();
      if (m->try_lock()) {
	o = ownership::unique;
	return true;
      }
      return false;
    }

    bool try_lock_shared() {
      lockable();
      if (m->try_lock_shared()) {
	o = ownership::shared;
	return true;
      }
      return false;
    }

    template<typename _Rep, typename _Period>
    bool try_lock_for(const std::chrono::duration<_Rep, _Period>& dur) {
      lockable();
      if (m->try_lock_for(dur)) {
	o = ownership::unique;
	return true;
      }
      return false;
    }

    template<typename _Rep, typename _Period>
    bool try_lock_shared_for(const std::chrono::duration<_Rep, _Period>& dur) {
      lockable();
      if (m->try_lock_shared_for(dur)) {
	o = ownership::shared;
	return true;
      }
      return false;
    }

    template<typename _Clock, typename _Duration>
    bool try_lock_until(const std::chrono::time_point<_Clock,
			_Duration>& time) {
      lockable();
      if (m->try_lock_until(time)) {
	o = ownership::unique;
	return true;
      }
      return false;
    }

    template<typename _Clock, typename _Duration>
    bool try_lock_shared_until(const std::chrono::time_point<_Clock,
			       _Duration>& time) {
      lockable();
      if (m->try_lock_shared_until(time)) {
	o = ownership::shared;
	return true;
      }
      return false;
    }

    // Onloy have a single unlock method, otherwise we're just
    // building schmuck bait.

    void unlock() {
      switch (o) {
      case ownership::none:
	throw std::system_error((int)std::errc::resource_deadlock_would_occur,
				std::generic_category());
	break;

      case ownership::unique:
	m->unlock();
	break;

      case ownership::shared:
	m->unlock_shared();
	break;
      }
      o = ownership::none;
    }

    // Setters

    void swap(shunique_lock& u) noexcept {
      std::swap(m, u.m);
      std::swap(o, u.o);
    }

    mutex_type* release() noexcept {
      o = ownership::none;
      return std::exchange(m, nullptr);
    }

    unique_lock_type release_to_unique_lock() {
      if (o == ownership::unique) {
	o = ownership::none;
	return unique_lock_type(*std::exchange(m, nullptr), std::adopt_lock);
      } else if (o == ownership::none) {
	return unique_lock_type(*std::exchange(m, nullptr), std::defer_lock);
      } else if (m == nullptr) {
	return unique_lock_type();
      }
      throw std::system_error((int)std::errc::operation_not_permitted,
			      std::generic_category());
      return unique_lock_type();
    }

    shared_lock_type release_to_shared_lock() {
      if (o == ownership::shared) {
	o = ownership::none;
	return shared_lock_type(*std::exchange(m, nullptr), std::adopt_lock);
      } else if (o == ownership::none) {
	return shared_lock_type(*std::exchange(m, nullptr), std::defer_lock);
      } else if (m == nullptr) {
	return shared_lock_type();
      }
      throw std::system_error((int)std::errc::operation_not_permitted,
			      std::generic_category());
      return shared_lock_type();
    }

    // Getters

    // Note that this returns true if the lock UNIQUE, it will return
    // false for shared
    bool owns_lock() const noexcept {
      return o == ownership::unique;
    }

    bool owns_lock_shared() const noexcept {
      return o == ownership::shared;
    }

    // If you want tomake sure you have a lock of some sort on the
    // mutex, just treat as a bool.
    explicit operator bool() const noexcept {
      return o != ownership::none;
    }

    mutex_type* mutex() const noexcept {
      return m;
    }

  private:
    void lockable() const {
      if (m == nullptr)
	throw std::system_error((int)std::errc::operation_not_permitted,
				std::generic_category());
      if (o != ownership::none)
	throw std::system_error((int)std::errc::resource_deadlock_would_occur,
				std::generic_category());
    }

    mutex_type*	m;
    enum struct ownership : uint8_t {
      none, unique, shared
    };
    ownership o;
  };
};

#endif // COMMON_SHUNIQUE_LOCK_H
