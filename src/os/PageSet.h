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
#include <atomic>
#include <cassert>
#include <mutex>
#include <vector>
#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive_ptr.hpp>

#include "include/encoding.h"
#include "include/Spinlock.h"


template<size_t PageSize>
struct Page {
  typedef Page<PageSize> page_type;

  Page(uint64_t offset = 0) : offset(offset), nrefs(1) {}

  char data[PageSize];
  boost::intrusive::avl_set_member_hook<> hook;
  uint64_t offset;

  // avoid RefCountedObject because it has a virtual destructor
  std::atomic<uint16_t> nrefs;
  void get() { ++nrefs; }
  void put() { if (--nrefs == 0) delete this; }

  typedef boost::intrusive_ptr<Page> Ref;
  friend void intrusive_ptr_add_ref(Page<PageSize> *p) { p->get(); }
  friend void intrusive_ptr_release(Page<PageSize> *p) { p->put(); }

  // key-value comparison functor for avl
  struct Less {
    bool operator()(uint64_t offset, const page_type &page) const {
      return offset < page.offset;
    }
    bool operator()(const page_type &page, uint64_t offset) const {
      return page.offset < offset;
    }
    bool operator()(const page_type &lhs, const page_type &rhs) const {
      return lhs.offset < rhs.offset;
    }
  };
  void encode(bufferlist &bl) const {
    bl.append(buffer::copy(data, PageSize));
    ::encode(offset, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode_array_nohead(data, PageSize, p);
    ::decode(offset, p);
  }

private: // copy disabled
  Page(const page_type&) = delete;
  const page_type& operator=(const page_type&) = delete;
};

template<size_t PageSize>
class PageSet {
public:
  typedef Page<PageSize> page_type;

  // alloc_range() and get_range() return page refs in a vector
  typedef std::vector<typename page_type::Ref> page_vector;

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

  typedef Spinlock lock_type;
  lock_type mutex;

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

    std::lock_guard<lock_type> lock(mutex);
    iterator cur = pages.end();
    while (length) {
      const uint64_t page_offset = position & ~(PageSize-1);

      typename page_set::insert_commit_data commit;
      auto insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
      if (insert.second) {
	page_type *page = new page_type(page_offset);
	cur = pages.insert_commit(*page, commit);

        // assume that the caller will write to the range [offset,length),
        //  so we only need to zero memory outside of this range

	// zero end of page past offset + length
	if (offset + length < page->offset + PageSize)
	  std::fill(page->data + offset + length - page->offset,
	      page->data + PageSize, 0);
	// zero front of page between page_offset and offset
	if (offset > page->offset)
	  std::fill(page->data, page->data + offset - page->offset, 0);
      } else { // exists
	cur = insert.first;
      }
      // add a reference to output vector
      out->reset(&*cur);
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
    while (cur != pages.end() && cur->offset < offset + length)
      range.push_back(&*cur++);
  }

  void free_pages_after(uint64_t offset) {
    std::lock_guard<lock_type> lock(mutex);
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

#endif // CEPH_PAGESET_H
