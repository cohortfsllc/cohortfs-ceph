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


#ifndef CEPH_PAGESET_H
#define CEPH_PAGESET_H

#include <algorithm>
#include <cassert>
#include <boost/intrusive/avl_set.hpp>
#include <boost/pool/pool.hpp>

#include "common/RefCountedObj.h"


template<size_t PageSize>
struct Page : public RefCountedObject {
  Page(uint64_t offset = 0) : offset(offset) {}
  ~Page() { assert(!hook.is_linked()); }

  char data[PageSize];
  boost::intrusive::avl_set_member_hook<> hook;
  uint64_t offset;

  // key-value comparison functor
  struct Less {
    bool operator()(uint64_t offset, const Page<PageSize> &page) const {
      return offset < page.offset;
    }
    bool operator()(const Page<PageSize> &page, uint64_t offset) const {
      return page.offset < offset;
    }
    bool operator()(const Page<PageSize> &lhs, const Page<PageSize> &rhs) const {
      return lhs.offset < rhs.offset;
    }
  };
#if 0
  // pool allocation
  static boost::pool<>& get_pool() {
    static boost::pool<> pool(PageSize);
    return pool;
  }
  static void *operator new(size_t num_bytes) {
    void *n = get_pool().malloc();
    if (!n)
      throw std::bad_alloc();
    return n;
  }
  void operator delete(void *p) {
    get_pool().free(p);
  }
#endif
  void encode(bufferlist &bl) const {
    bl.append(buffer::copy(data, PageSize));
    ::encode(offset, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode_array_nohead(data, PageSize, p);
    ::decode(offset, p);
  }

private: // copy disabled
  Page(const Page<PageSize>&) {}
  const Page<PageSize>& operator=(const Page<PageSize>&) { return *this; }
};

template<size_t PageSize>
class PageSet {
public:
  typedef Page<PageSize> page_type;

  // alloc_range() and get_range() return page pointers in a vector
  typedef std::vector<page_type*> page_vector;

private:
  // store pages in a boost intrusive avl_set
  typedef typename page_type::Less page_cmp;
  typedef boost::intrusive::member_hook<page_type,
	  boost::intrusive::avl_set_member_hook<>,
	  &page_type::hook> member_option;
  typedef boost::intrusive::avl_set<page_type,
	  boost::intrusive::compare<page_cmp>, member_option> page_set;

  typedef typename page_set::iterator iterator;

  page_set pages;

  typedef cohort::SpinLock lock_type;
  lock_type lock;

  void free_pages(iterator cur, iterator end) {
    while (cur != end) {
      page_type *page = &*cur;
      cur = pages.erase(cur);
      page->put();
    }
  }

  // disable copy
  PageSet(const PageSet&) = delete;
  const PageSet& operator=(const PageSet&) = delete;

public:
  PageSet() {}
  PageSet(PageSet &&rhs) : pages(std::move(rhs.pages)) {}
  ~PageSet() {
    free_pages(pages.begin(), pages.end());
  }

  bool empty() const { return pages.empty(); }
  size_t size() const { return pages.size(); }

  // allocate all pages that intersect the range [offset,length)
  void alloc_range(uint64_t offset, size_t length,
                   typename page_vector::iterator range_begin,
                   typename page_vector::iterator range_end) {
    // loop in reverse so we can provide hints to avl_set::insert_check()
    //	and get O(1) insertions after the first
    uint64_t position = offset + length - 1;

    typename page_vector::reverse_iterator out(range_end);

    std::lock_guard<lock_type> lk(lock);
    iterator cur = pages.end();
    while (length) {
      const uint64_t page_offset = position & ~(PageSize-1);

      typename page_set::insert_commit_data commit;
      auto insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
      if (insert.second) {
	page_type *page = new page_type(page_offset);
	cur = pages.insert_commit(*page, commit);

	/* XXX	Dont zero-fill pages AOT, rather find holes and expand
	 * them when read.  Just avoiding the fills isn't enough, but it
	 * increased throughput by 100MB/s.   And it's enough for simple
	 * benchmarks that only read after write.  */
#if 1
	// zero end of page past offset + length
	if (offset + length < page->offset + PageSize)
	  std::fill(page->data + offset + length - page->offset,
	      page->data + PageSize, 0);
	// zero front of page between page_offset and offset
	if (offset > page->offset)
	  std::fill(page->data, page->data + offset - page->offset, 0);
#else
	memset(page->data, 0, PageSize);
#endif

      } else { // exists
	cur = insert.first;
      }
      // add to output vector and take a corresponding ref
      cur->get();
      *out = &*cur;
      ++out;

      int c = std::min(length, (position & (PageSize-1)) + 1);
      position -= c;
      length -= c;
    }
    // make sure we sized the vector correctly
    assert(out == typename page_vector::reverse_iterator(range_begin));
  }

  // return all allocated pages that intersect the range [offset,length)
  void get_range(uint64_t offset, size_t length, page_vector &range) {
    auto cur = pages.lower_bound(offset & ~(PageSize-1), page_cmp());
    while (cur != pages.end() && cur->offset < offset + length) {
      cur->get();
      range.push_back(&*cur++);
    }
  }

  void free_pages_after(uint64_t offset) {
    std::lock_guard<lock_type> lk(lock);
    auto cur = pages.lower_bound(offset & ~(PageSize-1), page_cmp());
    if (cur == pages.end())
      return;
    if (cur->offset < offset)
      cur++;
    free_pages(cur, pages.end());
  }

  void encode(bufferlist &bl) const {
    unsigned count = pages.size();
    ::encode(count, bl);
    for (auto p = pages.rbegin(); p != pages.rend(); ++p)
      p->encode(bl);
  }
  void decode(bufferlist::iterator &p) {
    assert(empty());
    unsigned count;
    ::decode(count, p);
    auto cur = pages.end();
    for (unsigned i = 0; i < count; i++) {
      page_type *page = new page_type;
      page->decode(p);
      cur = pages.insert_before(cur, *page);
    }
  }
};

#endif
