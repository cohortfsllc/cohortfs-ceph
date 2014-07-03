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
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_PAGESET_H
#define CEPH_PAGESET_H

#include <algorithm>
#include <boost/intrusive/avl_set.hpp>
#include <boost/pool/pool.hpp>

#include "common/RefCountedObj.h"
#include "include/assert.h"


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

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]


template<size_t PageSize>
class PageSet {
public:
  typedef Page<PageSize> page_type;

  Spinlock sp;

  // store pages in a boost intrusive avl_set
  typedef typename page_type::Less page_cmp;
  typedef boost::intrusive::member_hook<page_type,
	  boost::intrusive::avl_set_member_hook<>,
	  &page_type::hook> member_option;
  typedef boost::intrusive::avl_set<page_type,
	  boost::intrusive::compare<page_cmp>, member_option> page_set;

  typedef typename page_set::iterator iterator;
  typedef typename page_set::const_iterator const_iterator;
  typedef typename page_set::reverse_iterator reverse_iterator;
  typedef typename page_set::const_reverse_iterator const_reverse_iterator;

private:
  page_set pages;
  CACHE_PAD(0); // coerce lanes into separate cache lines

  void free_pages(iterator cur, iterator end) {
    while (cur != end) {
      page_type *page = &*cur;
      cur = pages.erase(cur);
      page->put();
    }
  }

public:
  PageSet() {}
  ~PageSet() {
    free_pages(pages.begin(), pages.end());
  }

  bool empty() const { return pages.empty(); }
  size_t size() const { return pages.size(); }

  iterator begin() { return pages.begin(); }
  const_iterator begin() const { return pages.begin(); }
  iterator end() { return pages.end(); }
  const_iterator end() const { return pages.end(); }

  // allocate all pages that intersect the range [offset,length)
  pair<iterator, iterator> alloc_range(uint64_t offset, size_t length) {
    std::pair<iterator, bool> insert;
    iterator cur = pages.end();
    iterator last = pages.end(); // track last page in range

    // loop in reverse so we can provide hints to avl_set::insert_check()
    //  and get O(1) insertions after the first
    uint64_t position = offset + length;
    if ((position & ~(PageSize-1)) == position)
      position--;

    while (length) {
      const uint64_t page_offset = position & ~(PageSize-1);

      typename page_set::insert_commit_data commit;
      insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
      if (insert.second) {
	page_type *page = new page_type(page_offset);
	cur = pages.insert_commit(*page, commit);

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
      if (last == pages.end())
	last = cur;

      position -= PageSize;
      length -= std::min(length, PageSize);
    }
    return make_pair(cur, last);
  }

  // allocate all pages that intersect the range [offset,length)
  pair<iterator, iterator> alloc_range_ex(uint64_t offset, size_t length,
					  unsigned int soff,
					  unsigned int npart) {
    std::pair<iterator, bool> insert;
    iterator cur = pages.end();
    iterator last = pages.end(); // track last page in range

    // loop in reverse so we can provide hints to avl_set::insert_check()
    //  and get O(1) insertions after the first
    uint64_t position = offset + length;
    if ((position & ~(PageSize-1)) == position)
      position--;

    // adjust for stride
    while ((position % npart) != soff) {
      position -= PageSize;
    }

    while (length) {
      const uint64_t page_offset = position & ~(PageSize-1);

      typename page_set::insert_commit_data commit;
      insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
      if (insert.second) {
	page_type *page = new page_type(page_offset);
	cur = pages.insert_commit(*page, commit);

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
      if (last == pages.end())
	last = cur;

      position -= (PageSize * npart);
      length -= std::min(length, PageSize);
    }
    return make_pair(cur, last);
  }

  void alloc_range(uint64_t offset, size_t length, vector<page_type*> &arr) {
    pair<iterator, iterator> range = alloc_range(offset, length);
    do {
      arr.push_back(&*range.first);
      range.first->get();
      ++range.first;
    } while (range.first != range.second);
  }

  void alloc_range_ex(uint64_t offset, size_t length,
		      vector<page_type*> &arr, unsigned int soff,
		      unsigned int npart) {
    pair<iterator, iterator> range =
      alloc_range_ex(offset, length, soff, npart);

    int ix = 0;
    do {
      arr[(ix+soff)] = &*range.first;
      range.first->get();
      ++range.first;
      ++ix;
    } while (range.first != range.second);
  }

  iterator first_page_containing(uint64_t offset, size_t length) {
    iterator cur = pages.lower_bound(offset & ~(PageSize-1), page_cmp());
    if (cur == pages.end() || cur->offset >= offset + length)
      return pages.end();
    return cur;
  }

  void free_pages_after(uint64_t offset) {
    iterator cur = pages.lower_bound(offset & ~(PageSize-1), page_cmp());
    if (cur == pages.end())
      return;
    if (cur->offset < offset)
      cur++;
    free_pages(cur, pages.end());
  }

  void encode(bufferlist &bl) const {
    unsigned count = pages.size();
    ::encode(count, bl);
    for (const_reverse_iterator p = pages.rbegin(); p != pages.rend(); ++p)
      p->encode(bl);
  }
  void decode(bufferlist::iterator &p) {
    assert(empty());
    unsigned count;
    ::decode(count, p);
    iterator cur = pages.end();
    for (unsigned i = 0; i < count; i++) {
      page_type *page = new page_type;
      page->decode(p);
      cur = pages.insert_before(cur, *page);
    }
  }
};

template<size_t PageSize>
class PageSetX {
public:
  typedef Page<PageSize> page_type;
  typedef PageSet<PageSize> pageset_type;

private:
  unsigned int npart;
  pageset_type *t;

public:
  PageSetX(unsigned int _npart) : npart(_npart) {
    t = static_cast<pageset_type*>(malloc(npart*sizeof(pageset_type)));
    for (int ix = 0; ix < npart; ++ix)  {
      pageset_type& pgset = t[ix];
      new (&pgset) pageset_type(); // placement new
    }
  }

  ~PageSetX() {
    for (int ix = 0; ix < npart; ++ix)  {
      pageset_type& pgset = t[ix];
      pgset.~pageset_type();
    }
    free(t);
  }

  class PageVec {
  private:
    unsigned int npart;
    std::vector<page_type*> vec;
  public:
    PageVec(const PageSetX& pgset) : npart(pgset.npart) {};
    std::vector<page_type*>& get_vec() {
      return vec;
    }
    void clear() {
      vec.clear();
    }
    void resize(unsigned int n) {
      vec.resize(n);
    }
  };

  void alloc_range(uint64_t offset, size_t length, PageVec& pgvec) {

    uint64_t page_offset = offset & ~(PageSize-1);
    uint64_t len = length + (offset-page_offset);
    unsigned int n = len/PageSize;

    pgvec.resize(n);
    for( int ix = 0; ix < npart; ++ix) {
      pageset_type& pgset = t[ix];
      pgset.sp.lock();
      pgset.alloc_range_ex(offset, length, pgvec.get_vec(), ix, npart);
      pgset.sp.unlock();
    }
  }
};

#endif
