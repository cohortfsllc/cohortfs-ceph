// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
//===---------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===---------------------------------------------------------------------===//

#ifndef MIN_ALLOCATOR_H
#define MIN_ALLOCATOR_H

#include <cstddef>
#include <memory>

template<class T>
class min_allocator;

template<class T>
class min_allocator {
public:
  typedef T value_type;
  typedef std::true_type propagate_on_container_move_assignment;
  template<typename U>
  struct rebind {
    typedef min_allocator<U> other;
  };

  min_allocator() = default;
  min_allocator(const min_allocator&) = default;
  min_allocator(min_allocator&&) = default;
  template <class U>
  min_allocator(const min_allocator<U>&) noexcept {}
  template <class U>
  min_allocator(min_allocator<U>&&) noexcept {}

  T* allocate(std::ptrdiff_t n) {
    return static_cast<T*>(::operator new(n*sizeof(T)));
  }

  void deallocate(T* p, std::ptrdiff_t) {
    return ::operator delete(p);
  }

  friend bool operator==(min_allocator, min_allocator) {
    return true;
  }
  friend bool operator!=(min_allocator x, min_allocator y) {
    return false;
  }
};

#endif  // MIN_ALLOCATOR_H
