// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */


#ifndef CEPH_PARTITIONEDPAGESET_H
#define CEPH_PARTITIONEDPAGESET_H

#include "PageSet.h"


template<size_t PageSize>
class PartitionedPageSet {
 private:
  std::vector<PageSet<PageSize>> partitions;
  size_t pages_per_stripe;

  size_t count_pages(uint64_t offset, uint64_t len) const {
    // count the overlapping pages
    size_t count = 0;
    if (offset % PageSize) {
      count++;
      size_t rem = PageSize - offset % PageSize;
      len = len <= rem ? 0 : len - rem;
    }
    count += len / PageSize;
    if (len % PageSize)
      count++;
    return count;
  }

  // copy disabled
  PartitionedPageSet(const PartitionedPageSet&) = delete;
  const PartitionedPageSet& operator=(const PartitionedPageSet&) = delete;

 public:
  PartitionedPageSet(size_t count, size_t pages_per_stripe)
    : partitions(count), pages_per_stripe(pages_per_stripe)
  {}

  typedef typename PageSet<PageSize>::page_vector page_vector;

  // allocate all pages that intersect the range [offset,length)
  void alloc_range(uint64_t offset, size_t length, page_vector &range) {
    const size_t page_count = count_pages(offset, length);
    range.resize(page_count);

    typename page_vector::iterator p = range.begin();

    const size_t stripe_unit = PageSize * pages_per_stripe;
    auto part = partitions.begin() + (offset / stripe_unit) % partitions.size();

    while (length) {
      // number of pages left in this stripe
      size_t count = pages_per_stripe - (offset / PageSize) % pages_per_stripe;
      // ending offset of this stripe
      uint64_t stripe_end = (offset & ~(PageSize-1)) + PageSize * count;
      // bytes remaining in this stripe
      uint64_t stripe_len = stripe_end - offset;

      if (stripe_len > length) {
        count -= ((offset % PageSize) + (stripe_len - length)) / PageSize;
        stripe_len = length;
      }

      part->alloc_range(offset, stripe_len, p, p + count);

      offset += stripe_len;
      length -= stripe_len;

      if (++part == partitions.end()) // next partition
        part = partitions.begin();
      p += count; // advance position in output vector
    }
    assert(p == range.end());
  }

  // return all allocated pages that intersect the range [offset,length)
  void get_range(uint64_t offset, size_t length, page_vector &range) {
    const size_t stripe_unit = PageSize * pages_per_stripe;
    auto part = partitions.begin() + (offset / stripe_unit) % partitions.size();

    while (length) {
      // number of pages left in this stripe
      size_t count = pages_per_stripe - (offset / PageSize) % pages_per_stripe;
      // ending offset of this stripe
      uint64_t stripe_end = (offset & ~(PageSize-1)) + PageSize * count;
      // bytes remaining in this stripe
      uint64_t stripe_len = stripe_end - offset;

      if (stripe_len > length)
        stripe_len = length;

      part->get_range(offset, stripe_len, range);

      offset += stripe_len;
      length -= stripe_len;

      if (++part == partitions.end()) // next partition
        part = partitions.begin();
    }
  }

  void free_pages_after(uint64_t offset) {
    for (auto &part : partitions)
      part.free_pages_after(offset);
  }

  void encode(bufferlist &bl) const {
    uint32_t n = partitions.size();
    ::encode(n, bl);
    ::encode(pages_per_stripe, bl);
    for (const auto &part : partitions)
      part.encode(bl);
  }
  void decode(bufferlist::iterator &p) {
    uint32_t n;
    ::decode(n, p);
    ::decode(pages_per_stripe, p);
    partitions.resize(n);
    for (auto &part : partitions)
      part.decode(p);
  }
};

#endif
