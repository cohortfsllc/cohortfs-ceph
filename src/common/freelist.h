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
class FreeList {
 private:
  mpmc_bounded_queue_t<T*> queue;

 public:
  FreeList(size_t max_size, bool preallocate = true) : queue(max_size) {
    if (preallocate)
      for (size_t i = 0; i < max_size; i++)
        queue.enqueue(reinterpret_cast<T*>(new char[sizeof(T)]));
  }

  ~FreeList() {
    // free all remaining entries
    T *entry;
    while (queue.dequeue(entry))
      delete[] reinterpret_cast<char*>(entry);
  }

  T* alloc() {
    T *entry;
    if (queue.dequeue(entry))
      return entry;
    // queue is empty, allocate it
    return reinterpret_cast<T*>(new char[sizeof(T)]);
  }

  void free(T *entry) {
    if (!queue.enqueue(entry)) {
      // queue is full, delete it
      delete[] reinterpret_cast<char*>(entry);
    }
  }
};

} // namespace cohort

#endif // COHORT_FREELIST_H
