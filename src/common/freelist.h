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

#include <sys/mman.h>
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

// Simple huge page allocator with no bookeeping
#define HUGE_PAGE_SIZE (2 * 1024 * 1024)

// Might use it someday
#define ALIGN_HUGE(x) \
  (((x) + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE * HUGE_PAGE_SIZE)

class HugePageQ
{
private:
  int page_cnt;
  void* base;
  mpmc_bounded_queue_t<void*> queue;
  size_t aligned_size;
  bool huge;

public:
  HugePageQ(int page_cnt, int q_size)
    : page_cnt(page_cnt), base(nullptr), queue(q_size), huge(true) {

    assert(page_cnt > 0);
    assert(q_size > page_cnt);

    aligned_size = page_cnt * HUGE_PAGE_SIZE;
    base = mmap(NULL, aligned_size, PROT_READ | PROT_WRITE,
		MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE |
		MAP_HUGETLB, -1, 0);

    if (base == MAP_FAILED) {
      huge = false;
      base = malloc(aligned_size);
      if (! base)
	abort(); // aieee!!
    }

    int ix;
    void *addr;
    for (ix = 0; ix < page_cnt;) {
      addr = static_cast<char*>(base)+ix;
      if (queue.enqueue(addr))
	++ix;
    }
  }

  bool valid() { return !!base; };

  bool is_huge() { return huge; }

  ~HugePageQ() {
    // free all remaining entries
    void* entry;
    while (queue.dequeue(entry))
      ; // do nothing

    // return the region
    if (huge)
      munmap(base, aligned_size);
    else
      free(base);
  }

  void* alloc() {
    void* entry;
    for (;;)
      if (queue.dequeue(entry))
	return entry;
  }

  void free(void* entry) {
    for (;;)
      if (queue.enqueue(entry))
	return;
  }
};

} // namespace cohort

#endif // COHORT_FREELIST_H
