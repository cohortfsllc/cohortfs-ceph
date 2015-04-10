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

#ifndef COHORT_FREELIST_H
#define COHORT_FREELIST_H

#include "include/mpmc-bounded-queue.hpp"

namespace cohort {

template <typename T>
struct DefaultConstruct {
  T* alloc() const { return new T; }
  void free(T* entry) { delete entry; }
};

// uses new char[] to allocate memory, but doesn't call constructors or
// destructors. for use with overloaded new/delete operators
template <typename T>
struct CharArrayAlloc {
  T* alloc() const { return reinterpret_cast<T*>(new char[sizeof(T)]); }
  void free(T* entry) { delete[] reinterpret_cast<char*>(entry); }
};

template <typename T, typename Alloc>
class FreeList {
 private:
  mpmc_bounded_queue_t<T*> queue;
  Alloc &allocator;

 public:
  FreeList(size_t max_size, Alloc &allocator, bool preallocate = true)
    : queue(max_size), allocator(allocator) {
    if (preallocate)
      for (size_t i = 0; i < max_size; i++)
        queue.enqueue(allocator.alloc());
  }

  ~FreeList() {
    // free all remaining entries
    T *entry;
    while (queue.dequeue(entry))
      allocator.free(entry);
  }

  T* alloc() {
    T *entry;
    if (queue.dequeue(entry))
      return entry;
    // queue is empty, allocate it
    return allocator.alloc();
  }

  void free(T *entry) {
    if (!queue.enqueue(entry)) {
      // queue is full, delete it
      allocator.free(entry);
    }
  }
};

} // namespace cohort

#endif // COHORT_FREELIST_H
