// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BUFFER_LIST_H
#define CEPH_BUFFER_LIST_H

#include <utility>

#include "buffer_int.h"
#include "buffer_ptr.h"

#include <include/intarith.h>

namespace ceph {

  /* re-open buffer namespace */
  namespace buffer {

    /*
     * list
     */
    typedef bi::list< ptr,
		      bi::member_hook<
			ptr, bi::list_member_hook<>, &ptr::bi_hook1 >
		      > PtrList;

    class list {
    private:
      PtrList _bi_buffers;
      unsigned _len;
      ptr append_buffer; // where i put small appends.

    public:
      class iterator {
	list *bl;
	PtrList* bi_ls;  // meh.. just here to avoid an extra pointer deref
	unsigned off;  // in bl
	PtrList::iterator bi_p;
	unsigned p_off; // in *p

      public:
	// constructor.  position.
	iterator() :
	  bl(0), bi_ls(0), off(0), p_off(0) {}

	iterator(list *l, unsigned o=0) :
	  bl(l), bi_ls(&bl->_bi_buffers), off(0),
	  bi_p(bi_ls->begin()), p_off(0) {
	  advance(o);
	}

	iterator(list *l, unsigned o, std::list<ptr>::iterator ip,
		 unsigned po) :
	  bl(l), off(o), p_off(po) { }

	iterator(list *l, unsigned o, PtrList::iterator ip,
		 unsigned po) :
	  bl(l), bi_ls(&bl->_bi_buffers), off(o),
	  bi_p(ip), p_off(po) { }

	iterator(const iterator& other) : bl(other.bl),
					  bi_ls(other.bi_ls),
					  off(other.off),
					  bi_p(other.bi_p),
					  p_off(other.p_off) {}

	iterator& operator=(const iterator& other) {
	  if (this != &other) {
	    bl = other.bl;
	    bi_ls = other.bi_ls;
	    off = other.off;
	    bi_p = other.bi_p;
	    p_off = other.p_off;
	  }
	  return *this;
	}

	/*  get current iterator offset in buffer::list */
	unsigned get_off() { return off; }

	/* get nbytes remaining from iterator pos to the end of the
	   buffer::list */
	unsigned get_remaining() { return bl->length() - off; }

	/*  true if iterator is at the end of the buffer::list */
	bool end() {
	  return bi_p == bi_ls->end();
	  //return off == bl->length();
	}

	void advance(int o) {
	  /* cout << this << " advance " << o << " from " << off
	     << " (p_off " << p_off << " in " << p->length() << ")"
	     << std::endl;
	  */
	  if (o > 0) {
	    p_off += o;
	    while (p_off > 0) {
	      if (bi_p == bi_ls->end())
		throw end_of_buffer();
	      if (p_off >= bi_p->length()) {
		// skip this buffer
		p_off -= bi_p->length();
		++bi_p;
	      } else {
		// somewhere in this buffer!
		break;
	      }
	    }
	    off += o;
	    return;
	  }
	  while (o < 0) {
	    if (p_off) {
	      unsigned d = -o;
	      if (d > p_off)
		d = p_off;
	      p_off -= d;
	      off -= d;
	      o += d;
	    } else if (off > 0) {
	      assert(bi_p != bi_ls->begin());
	      --bi_p;
	      p_off = bi_p->length();
	    } else {
	      throw end_of_buffer();
	    }
	  }
	}

	void seek(unsigned o) {
	  //cout << this << " seek " << o << std::endl;
	  bi_p = bi_ls->begin();
	  off = p_off = 0;
	  advance(o);
	}

	char operator*() {
	  if (bi_p == bi_ls->end())
	    throw end_of_buffer();
	  return (*bi_p)[p_off];
	}

	buffer::list::iterator& operator++() {
	  if (bi_p == bi_ls->end())
	    throw end_of_buffer();
	  advance(1);
	  return *this;
	}

	buffer::ptr get_current_ptr() {
	  if (bi_p == bi_ls->end())
	    throw end_of_buffer();
	  return ptr(*bi_p, p_off, bi_p->length() - p_off);
	}

	// copy data out.
	// note that these all _append_ to dest!
	void copy(unsigned len, char *dest) {
	  if (bi_p == bi_ls->end()) seek(off);
	  while (len > 0) {
	    if (bi_p == bi_ls->end())
	      throw end_of_buffer();
	    assert(bi_p->length() > 0);

	    unsigned howmuch = bi_p->length() - p_off;
	    if (len < howmuch) howmuch = len;
	    bi_p->copy_out(p_off, howmuch, dest);
	    dest += howmuch;

	    len -= howmuch;
	    advance(howmuch);
	  }
	}

	void copy(unsigned len, ptr &dest) {
	  dest = create(len);
	  copy(len, dest.c_str());
	}

	void copy(unsigned len, list &dest) {
	  if (bi_p == bi_ls->end())
	    seek(off);
	  while (len > 0) {
	    if (bi_p == bi_ls->end())
	      throw end_of_buffer();

	    unsigned howmuch = bi_p->length() - p_off;
	    if (len < howmuch)
	      howmuch = len;
	    dest.append(*bi_p, p_off, howmuch);

	    len -= howmuch;
	    advance(howmuch);
	  }
	}

	void copy(unsigned len, std::string &dest) {
	  if (bi_p == bi_ls->end())
	    seek(off);
	  while (len > 0) {
	    if (bi_p == bi_ls->end())
	      throw end_of_buffer();

	    unsigned howmuch = bi_p->length() - p_off;
	    const char *c_str = bi_p->c_str();
	    if (len < howmuch)
	      howmuch = len;
	    dest.append(c_str + p_off, howmuch);

	    len -= howmuch;
	    advance(howmuch);
	  }
	}

	void copy_all(list &dest) {
	  if (bi_p == bi_ls->end())
	    seek(off);
	  while (1) {
	    if (bi_p == bi_ls->end())
	      return;
	    assert(bi_p->length() > 0);

	    unsigned howmuch = bi_p->length() - p_off;
	    const char *c_str = bi_p->c_str();
	    dest.append(c_str + p_off, howmuch);

	    advance(howmuch);
	  }
	}

	// copy data in
	void copy_in(unsigned len, const char *src) {
	  // copy
	  if (bi_p == bi_ls->end())
	    seek(off);
	  while (len > 0) {
	    if (bi_p == bi_ls->end())
	      throw end_of_buffer();

	    unsigned howmuch = bi_p->length() - p_off;
	    if (len < howmuch)
	      howmuch = len;
	    bi_p->copy_in(p_off, howmuch, src);

	    src += howmuch;
	    len -= howmuch;
	    advance(howmuch);
	  }
	}

	void copy_in(unsigned len, const list& otherl) {
	  if (bi_p == bi_ls->end())
	    seek(off);
	  unsigned left = len;
	  for (PtrList::const_iterator i = otherl._bi_buffers.begin();
	       i != otherl._bi_buffers.end();
	       ++i) {
	    unsigned l = (*i).length();
	    if (left < l)
	      l = left;
	    copy_in(l, i->c_str());
	    left -= l;
	    if (left == 0)
	      break;
	  }
	}
      };

    private:
      mutable iterator last_p;
      int zero_copy_to_fd(int fd) const;

    public:
      list() : _len(0), last_p(this) {}
      list(unsigned prealloc) : _len(0), last_p(this) {
	append_buffer = buffer::create(prealloc);
	append_buffer.set_length(0);   // unused, so far.
      }
      ~list() {}

      // XXX Casey review
      list(const list& other) : _len(other._len),
				last_p(this) {
	//PtrList blah(other._bi_buffers); // cant do this
	PtrList::const_iterator it;
	for (it = other._bi_buffers.begin(); it != _bi_buffers.end();
	     ++it) {
	  // XXX is this correct, or do we need to push_back
	  // ptr bp(it->get_raw()?  hoping it's the same thing...
	  // I make use of that form later, need to get
	  // correct or consistent
	  _bi_buffers.push_back(const_cast<ptr&>(*it));
	}
      }

      list& operator= (const list& other) {
	if (this != &other) {
	  _bi_buffers.clear();
	  // _buffers = other._buffers;
	  PtrList::const_iterator it;
	  for (it = other._bi_buffers.begin(); it != _bi_buffers.end();
	       ++it) {
	    // XXX viz^^
	    _bi_buffers.push_back(const_cast<ptr&>(*it));
	  }
	  _len = other._len;
	}
	return *this;
      }

      // XXXX
      //const std::list<ptr>& buffers() const { return _buffers; }
      const PtrList& buffers() const { return _bi_buffers; }
      const PtrList& bi_buffers() const { return _bi_buffers; }

      void swap(list& other)
	{
	  std::swap(_len, other._len);
	  _bi_buffers.swap(other._bi_buffers);
	  append_buffer.swap(other.append_buffer);
	  //last_p.swap(other.last_p);
	  last_p = begin();
	  other.last_p = other.begin();
	}

      unsigned length() const {
#if 0
	// DEBUG: verify _len
	unsigned len = 0;
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end();
	     ++it) {
	  len += (*it).length();
	}
	assert(len == _len);
#endif
	return _len;
      }

      bool contents_equal(ceph::buffer::list& other)
	{
	  if (length() != other.length())
	    return false;

	  // buffer-wise comparison
	  if (true) {
	    PtrList::const_iterator a = _bi_buffers.begin();
	    PtrList::const_iterator b = other._bi_buffers.begin();
	    unsigned aoff = 0, boff = 0;
	    while (a != _bi_buffers.end()) {
	      unsigned len = a->length() - aoff;
	      if (len > b->length() - boff)
		len = b->length() - boff;
	      if (memcmp(a->c_str() + aoff, b->c_str() + boff, len) != 0)
		return false;
	      aoff += len;
	      if (aoff == a->length()) {
		aoff = 0;
		++a;
	      }
	      boff += len;
	      if (boff == b->length()) {
		boff = 0;
		++b;
	      }
	    }
	    assert(b == other._bi_buffers.end());
	    return true;
	  }

	  // byte-wise comparison
	  if (false) {
	    bufferlist::iterator me = begin();
	    bufferlist::iterator him = other.begin();
	    while (!me.end()) {
	      if (*me != *him)
		return false;
	      ++me;
	      ++him;
	    }
	    return true;
	  }
	}

      bool can_zero_copy() const {
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end();
	     ++it)
	  if (!it->can_zero_copy())
	    return false;
	return true;
      }

      // std++11 lambdas ftw (next 3)
      bool is_page_aligned() const {
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end();
	     ++it)
	  if (!it->is_page_aligned())
	    return false;
	return true;
      }

      bool is_n_page_sized() const {
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end();
	     ++it)
	  if (!it->is_n_page_sized())
	    return false;
	return true;
      }

      bool is_zero() const {
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end();
	     ++it) {
	  if (!it->is_zero()) {
	    return false;
	  }
	}
	return true;
      }

      // modifiers
      void clear() {
	_bi_buffers.clear();
	_len = 0;
	last_p = begin();
      }

      void push_front(ptr& bp) {
	if (bp.length() == 0)
	  return;
	_bi_buffers.push_front(bp);
	_len += bp.length();
      }

      void push_front(raw *r) {
	ptr bp(r);
	push_front(bp);
      }

      void push_back(const ptr& bp) {
	if (bp.length() == 0)
	  return;
	_bi_buffers.push_back(const_cast<ptr&>(bp));
	_len += bp.length();
      }

      void push_back(raw *r) {
	ptr bp(r);
	push_back(bp);
      }

      // std++11 lambdas ftw
      void zero()
	{
	  for (PtrList::iterator it = _bi_buffers.begin();
	       it != _bi_buffers.end();
	       ++it)
	    it->zero();
	}

      void zero(unsigned o, unsigned l)
	{
	  assert(o+l <= _len);
	  unsigned p = 0;
	  for (PtrList::iterator it = _bi_buffers.begin();
	       it != _bi_buffers.end();
	       ++it) {
	    if (p + it->length() > o) {
	      if (p >= o && p+it->length() <= o+l)
		it->zero(); // all
	      else if (p >= o)
		it->zero(0, o+l-p); // head
	      else
		it->zero(o-p, it->length()-(o-p)); // tail
	    }
	    p += it->length();
	    if (o+l <= p)
	      break;  // done
	  }
	}

      bool is_contiguous()
	{
	  return &(*_bi_buffers.begin()) == &(*_bi_buffers.rbegin());
	}

      void rebuild()
	{
	  ptr nb;
	  if ((_len & ~CEPH_PAGE_MASK) == 0)
	    nb = buffer::create_page_aligned(_len);
	  else
	    nb = buffer::create(_len);
	  rebuild(nb);
	}

      void rebuild(ptr& nb)
	{
	  unsigned pos = 0;
	  for (PtrList::iterator it = _bi_buffers.begin();
	       it != _bi_buffers.end();
	       ++it) {
	    nb.copy_in(pos, it->length(), it->c_str());
	    pos += it->length();
	  }
	  _bi_buffers.clear();
	  _bi_buffers.push_back(nb);
	}

      void rebuild_page_aligned()
	{
	  PtrList::iterator p = _bi_buffers.begin();
	  while (p != _bi_buffers.end()) {
	    // keep anything that's already page sized+aligned
	    if (p->is_page_aligned() && p->is_n_page_sized()) {
	      /*cout << " segment " << (void*)p->c_str()
		<< " offset " << ((unsigned long)p->c_str() & ~CEPH_PAGE_MASK)
		<< " length " << p->length()
		<< " " << (p->length() & ~CEPH_PAGE_MASK) << " ok"
		<< std::endl;
	      */
	      ++p;
	      continue;
	    }

	    /* consolidate unaligned items, until we get something that is
	     * sized+aligned */
	    list unaligned;
	    unsigned offset = 0;
	    do {
	      /*cout << " segment " << (void*)p->c_str()
		<< " offset " << ((unsigned long)p->c_str() & ~CEPH_PAGE_MASK)
		<< " length " << p->length() << " "
		<< (p->length() & ~CEPH_PAGE_MASK)
		<< " overall offset " << offset << " "
		<< (offset & ~CEPH_PAGE_MASK)
		<< " not ok" << std::endl;
	      */
	      offset += p->length();
	      unaligned.push_back(*p);
	      _bi_buffers.erase(p++);
	    } while (p != _bi_buffers.end() &&
		     (!p->is_page_aligned() ||
		      !p->is_n_page_sized() ||
		      (offset & ~CEPH_PAGE_MASK)));
	    ptr nb(buffer::create_page_aligned(unaligned._len));
	    unaligned.rebuild(nb);
	    _bi_buffers.insert(p, unaligned._bi_buffers.front());
	  }
	}

      // sort-of-like-assignment-op
      void claim(list& bl)
	{
	  // free my buffers
	  clear();
	  claim_append(bl);
	}

      void claim_append(list& bl)
	{
	  // steal the other guy's buffers
	  _len += bl._len;
	  _bi_buffers.splice(_bi_buffers.end(), bl._bi_buffers );
	  bl._len = 0;
	  bl.last_p = bl.begin();
	}

      void claim_prepend(list& bl)
	{
	  // steal the other guy's buffers
	  _len += bl._len;
	  _bi_buffers.splice(_bi_buffers.begin(), bl._bi_buffers);
	  bl._len = 0;
	  bl.last_p = bl.begin();
	}

      iterator begin() {
	return iterator(this, 0);
      }

      iterator end() {
	return iterator(this, _len, _bi_buffers.end(), 0);
      }

      // crope lookalikes.
      // *** WARNING: this are horribly inefficient for large bufferlists. ***
      void copy(unsigned off, unsigned len, char *dest) const {
	if (off + len > length())
	  throw end_of_buffer();
	if (last_p.get_off() != off)
	  last_p.seek(off);
	last_p.copy(len, dest);
      }

      void copy(unsigned off, unsigned len, list &dest) const {
	if (off + len > length())
	  throw end_of_buffer();
	if (last_p.get_off() != off)
	  last_p.seek(off);
	last_p.copy(len, dest);
      }

      void copy(unsigned off, unsigned len, std::string& dest) const {
	if (last_p.get_off() != off)
	  last_p.seek(off);
	return last_p.copy(len, dest);
      }

      void copy_in(unsigned off, unsigned len, const char *src) {
	if (off + len > length())
	  throw end_of_buffer();

	if (last_p.get_off() != off)
	  last_p.seek(off);
	last_p.copy_in(len, src);
      }

      void copy_in(unsigned off, unsigned len, const list& src) {
	if (last_p.get_off() != off)
	  last_p.seek(off);
	last_p.copy_in(len, src);
      }

      void append(char c) {
	// put what we can into the existing append_buffer.
	unsigned gap = append_buffer.unused_tail_length();
	if (!gap) {
	  // make a new append_buffer!
	  unsigned alen = CEPH_PAGE_SIZE;
	  append_buffer = create_page_aligned(alen);
	  append_buffer.set_length(0);   // unused, so far.
	}
	append_buffer.append(c);
	/* add segment to the list */
	append(append_buffer, append_buffer.end() - 1, 1);
      }

      void append(const char *data, unsigned len) {
	while (len > 0) {
	  // put what we can into the existing append_buffer.
	  unsigned gap = append_buffer.unused_tail_length();
	  if (gap > 0) {
	    if (gap > len) gap = len;
	    /* cout << "append first char is " << data[0]
	       << ", last char is " << data[len-1] << std::endl;  */
	    append_buffer.append(data, gap);
	    append(append_buffer, append_buffer.end() - gap, gap);
	    len -= gap;
	    data += gap;
	  }
	  if (len == 0)
	    break;  // done!

	  // make a new append_buffer!
	  unsigned alen = CEPH_PAGE_SIZE * (((len-1) / CEPH_PAGE_SIZE) + 1);
	  append_buffer = create_page_aligned(alen);
	  append_buffer.set_length(0);   // unused, so far.
	}
      }

      void append(const std::string& s) {
	append(s.data(), s.length());
      }

      void append(const ptr& bp) {
	if (bp.length())
	  push_back(bp);
      }

      void append(const ptr& bp, unsigned off, unsigned len) {
	assert(len+off <= bp.length());
	if (!_bi_buffers.empty()) {
	  ptr &l = _bi_buffers.back();
	  if (l.get_raw() == bp.get_raw() &&
	      l.end() == bp.start() + off) {
	    // yay contiguous with tail bp!
	    l.set_length(l.length()+len);
	    _len += len;
	    return;
	  }
	}
	// add new item to list
	ptr tempbp(bp, off, len);
	push_back(tempbp);
      }

      // XXX intrusive sharing exemplar
      void append(const list& bl) {
	_len += bl._len;
	for (PtrList::const_iterator p = bl._bi_buffers.begin();
	     p != bl._bi_buffers.end();
	     ++p) {
	  ptr bp(p->get_raw());
	  _bi_buffers.push_back(const_cast<ptr&>(bp));
	}
      }

      void append(std::istream& in) {
	while (!in.eof()) {
	  std::string s;
	  getline(in, s);
	  append(s.c_str(), s.length());
	  if (s.length())
	    append("\n", 1);
	}
      }

      void append_zero(unsigned len) {
	ptr bp(len);
	bp.zero();
	append(bp);
      }

      /*
       * get a char
       */
      const char& operator[](unsigned n) const {
	if (n >= _len)
	  throw end_of_buffer();

	for (PtrList::const_iterator p = _bi_buffers.begin();
	     p != _bi_buffers.end();
	     ++p) {
	  if (n >= p->length()) {
	    n -= p->length();
	    continue;
	  }
	  return (*p)[n];
	}
	assert(0);
      }

      /*
       * return a contiguous ptr to whole bufferlist contents.
       */
      char *c_str() {
	if (_bi_buffers.empty())
	  return 0; // no buffers

	PtrList::const_iterator iter = _bi_buffers.begin();
	++iter;

	if (iter != _bi_buffers.end())
	  rebuild();
	return _bi_buffers.front().c_str();  // good, we're already contiguous.
      }

      void substr_of(const list& other, unsigned off, unsigned len) {
	if (off + len > other.length())
	  throw end_of_buffer();

	clear();

	// skip off
	PtrList::const_iterator curbuf = other._bi_buffers.begin();
	while (off > 0 &&
	       off >= curbuf->length()) {
	  // skip this buffer
	  //cout << "skipping over " << *curbuf << std::endl;
	  off -= curbuf->length();
	  ++curbuf;
	}
	assert(len == 0 || curbuf != other._bi_buffers.end());

	while (len > 0) {
	  // partial?
	  if (off + len < curbuf->length()) {
	    //cout << "copying partial of " << *curbuf << std::endl;
	    ptr bp(curbuf->get_raw(), off, len);
	    _bi_buffers.push_back(bp);
	    _len += len;
	    break;
	  }

	  // through end
	  //cout << "copying end (all?) of " << *curbuf << std::endl;
	  unsigned howmuch = curbuf->length() - off;
	  ptr bp(curbuf->get_raw(), off, howmuch);
	  _bi_buffers.push_back(bp);
	  _len += howmuch;
	  len -= howmuch;
	  off = 0;
	  ++curbuf;
	}
      }

      // XXXX:  TODO:  finish; dont understand splice semantics!

      // funky modifer
      void splice(unsigned off, unsigned len, list *claim_by=0
		  /*, bufferlist& replace_with */) { // fixme?
	if (len == 0)
	  return;

	if (off >= length())
	  throw end_of_buffer();

	assert(len > 0);
	/* cout << "splice off " << off << " len " << len << " ... mylen = "
	   << length() << std::endl;
	*/

	// skip off
	PtrList::iterator curbuf = _bi_buffers.begin();
	while (off > 0) {
	  assert(curbuf != _bi_buffers.end());
	  if (off >= curbuf->length()) {
	    // skip this buffer
	    /* cout << "off = " << off << " skipping over " << *curbuf
	       << std::endl; */
	    off -= curbuf->length();
	    ++curbuf;
	  } else {
	    // somewhere in this buffer!
	    /* cout << "off = " << off << " somewhere in " << *curbuf
	       << std::endl; */
	    break;
	  }
	}

	if (off) {
	  // add a reference to the front bit
	  //  insert it before curbuf (which we'll hose)
	  /* cout << "keeping front " << off << " of " << *curbuf
	     << std::endl; */
	  ptr bp(curbuf->get_raw(), 0, off);
	  _bi_buffers.insert(curbuf, bp);
	  _len += off;
	}

	while (len > 0) {
	  // partial?
	  if (off + len < curbuf->length()) {
	    /* cout << "keeping end of " << *curbuf << ", losing first "
	       << off+len << std::endl; */
	    if (claim_by)
	      claim_by->append(*curbuf, off, len);
	    /* ignore beginning big */
	    curbuf->set_offset(off+len + curbuf->offset());
	    curbuf->set_length(curbuf->length() - (len+off));
	    _len -= off+len;
	    //cout << " now " << *curbuf << std::endl;
	    break;
	  }

	  // hose through the end
	  unsigned howmuch = curbuf->length() - off;
	  /* cout << "discarding " << howmuch << " of " << *curbuf
	     << std::endl; */
	  if (claim_by)
	    claim_by->append(*curbuf, off, howmuch);
	  _len -= curbuf->length();
	  _bi_buffers.erase(curbuf++);
	  len -= howmuch;
	  off = 0;
	}

	// splice in *replace (implement me later?)
	last_p = begin();  // just in case we were in the removed region.
      };

      void write(int off, int len, std::ostream& out) const {
	list s;
	s.substr_of(*this, off, len);
	for (PtrList::const_iterator it = s._bi_buffers.begin();
	     it != s._bi_buffers.end();
	     ++it)
	  if (it->length())
	    out.write(it->c_str(), it->length());
	/*iterator p(this, off);
	  while (len > 0 && !p.end()) {
	  int l = p.left_in_this_buf();
	  if (l > len)
	  l = len;
	  out.write(p.c_str(), l);
	  len -= l;
	  }*/
      }

      void encode_base64(buffer::list& o) {
	ptr bp(length() * 4 / 3 + 3);
	int l = ceph_armor(bp.c_str(), bp.c_str() + bp.length(),
			   c_str(), c_str() + length());
	bp.set_length(l);
	o.push_back(bp);
      }

      void decode_base64(buffer::list& e) {
	ptr bp(4 + ((e.length() * 3) / 4));
	int l = ceph_unarmor(bp.c_str(), bp.c_str() + bp.length(),
			     e.c_str(), e.c_str() + e.length());
	if (l < 0) {
	  std::ostringstream oss;
	  oss << "decode_base64: decoding failed:\n";
	  hexdump(oss);
	  throw buffer::malformed_input(oss.str().c_str());
	}
	assert(l <= (int)bp.length());
	bp.set_length(l);
	push_back(bp);
      }

      void hexdump(std::ostream &out) const {
	std::ios_base::fmtflags original_flags = out.flags();

	out.setf(std::ios::right);
	out.fill('0');

	unsigned per = 16;

	for (unsigned o=0; o<length(); o += per) {
	  out << std::hex << std::setw(4) << o << " :";

	  unsigned i;
	  for (i=0; i<per && o+i<length(); i++) {
	    out << " " << std::setw(2)
		<< ((unsigned)(*this)[o+i] & 0xff);
	  }
	  for (; i<per; i++)
	    out << "   ";

	  out << " : ";
	  for (i=0; i<per && o+i<length(); i++) {
	    char c = (*this)[o+i];
	    if (isupper(c) || islower(c) || isdigit(c) || c == ' ' ||
		ispunct(c))
	      out << c;
	    else
	      out << '.';
	  }
	  out << std::dec << std::endl;
	}

	out.flags(original_flags);
      }

      /* XXX POSIX */
      int read_file(const char *fn, std::string *error) {
	int fd = TEMP_FAILURE_RETRY(::open(fn, O_RDONLY));
	if (fd < 0) {
	  int err = errno;
	  std::ostringstream oss;
	  oss << "can't open " << fn << ": " << cpp_strerror(err);
	  *error = oss.str();
	  return -err;
	}

	struct stat st;
	memset(&st, 0, sizeof(st));
	::fstat(fd, &st);

	ssize_t ret = read_fd(fd, st.st_size);
	if (ret < 0) {
	  std::ostringstream oss;
	  oss << "bufferlist::read_file(" << fn << "): read error:"
	      << cpp_strerror(ret);
	  *error = oss.str();
	  VOID_TEMP_FAILURE_RETRY(::close(fd));
	  return ret;
	}
	else if (ret != st.st_size) {
	  // Premature EOF.
	  // Perhaps the file changed between stat() and read()?
	  std::ostringstream oss;
	  oss << "bufferlist::read_file(" << fn
	      << "): warning: got premature EOF.";
	  *error = oss.str();
	  // not actually an error, but weird
	}
	VOID_TEMP_FAILURE_RETRY(::close(fd));
	return 0;
      }

      ssize_t read_fd(int fd, size_t len) {
	// try zero copy first
	if (false && read_fd_zero_copy(fd, len) == 0) {
	  // TODO fix callers to not require correct read size, which is not
	  // available for raw_pipe until we actually inspect the data
	  return 0;
	}
	int s = ROUND_UP_TO(len, CEPH_PAGE_SIZE);
	ptr bp = buffer::create_page_aligned(s);
	ssize_t ret = safe_read(fd, (void*)bp.c_str(), len);
	if (ret >= 0) {
	  bp.set_length(ret);
	  append(bp);
	}
	return ret;
      }

      int read_fd_zero_copy(int fd, size_t len) {
#ifdef CEPH_HAVE_SPLICE
	try {
	  bufferptr bp = buffer::create_zero_copy(len, fd, NULL);
	  append(bp);
	} catch (buffer::error_code e) {
	  return e.code;
	} catch (buffer::malformed_input) {
	  return -EIO;
	}
	return 0;
#else
	return -ENOTSUP;
#endif
      }

      int write_file(const char *fn, int mode=644) {
	int fd = TEMP_FAILURE_RETRY(::open(fn, O_WRONLY|O_CREAT|O_TRUNC,
					   mode));
	if (fd < 0) {
	  int err = errno;
	  std::cerr << "bufferlist::write_file(" << fn
		    << "): failed to open file: "
		    << cpp_strerror(err) << std::endl;
	  return -err;
	}
	int ret = write_fd(fd);
	if (ret) {
	  std::cerr << "bufferlist::write_fd(" << fn << "): write_fd error: "
		    << cpp_strerror(ret) << std::endl;
	  VOID_TEMP_FAILURE_RETRY(::close(fd));
	  return ret;
	}
	if (TEMP_FAILURE_RETRY(::close(fd))) {
	  int err = errno;
	  std::cerr << "bufferlist::write_file(" << fn << "): close error: "
		    << cpp_strerror(err) << std::endl;
	  return -err;
	}
	return 0;
      }

      int write_fd(int fd) const {
	if (can_zero_copy())
	  return write_fd_zero_copy(fd);

	// use writev!
	iovec iov[IOV_MAX];
	int iovlen = 0;
	ssize_t bytes = 0;

	PtrList::const_iterator p = _bi_buffers.begin();
	while (p != _bi_buffers.end()) {
	  if (p->length() > 0) {
	    iov[iovlen].iov_base = (void *)p->c_str();
	    iov[iovlen].iov_len = p->length();
	    bytes += p->length();
	    iovlen++;
	  }
	  ++p;

	  if (iovlen == IOV_MAX-1 ||
	      p == _bi_buffers.end()) {
	    iovec *start = iov;
	    int num = iovlen;
	    ssize_t wrote;
	  retry:
	    wrote = ::writev(fd, start, num);
	    if (wrote < 0) {
	      int err = errno;
	      if (err == EINTR)
		goto retry;
	      return -err;
	    }
	    if (wrote < bytes) {
	      // partial write, recover!
	      while ((size_t)wrote >= start[0].iov_len) {
		wrote -= start[0].iov_len;
		bytes -= start[0].iov_len;
		start++;
		num--;
	      }
	      if (wrote > 0) {
		start[0].iov_len -= wrote;
		start[0].iov_base = (char *)start[0].iov_base + wrote;
		bytes -= wrote;
	      }
	      goto retry;
	    }
	    iovlen = 0;
	    bytes = 0;
	  }
	}
	return 0;
      }

      int write_fd_zero_copy(int fd) const {
	if (!can_zero_copy())
	  return -ENOTSUP;
	/* pass offset to each call to avoid races updating the fd seek
	 * position, since the I/O may be non-blocking
	 */
	int64_t offset = ::lseek(fd, 0, SEEK_CUR);
	int64_t *off_p = &offset;
	if (offset < 0 && offset != ESPIPE)
	  return (int) offset;
	if (offset == ESPIPE)
	  off_p = NULL;
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end(); ++it) {
	  int r = it->zero_copy_to_fd(fd, off_p);
	  if (r < 0)
	    return r;
	  if (off_p)
	    offset += it->length();
	}
	return 0;
      }

      __u32 crc32c(__u32 crc) const {
	for (PtrList::const_iterator it = _bi_buffers.begin();
	     it != _bi_buffers.end(); ++it) {
	  if (it->length()) {
	    raw *r = it->get_raw();
	    pair<size_t, size_t> ofs(it->offset(), it->offset() +
				     it->length());
	    pair<uint32_t, uint32_t> ccrc;
	    if (r->get_crc(ofs, &ccrc)) {
	      if (ccrc.first == crc) {
		// got it already
		crc = ccrc.second;
		if (buffer_track_crc)
		  buffer_cached_crc.inc();
	      } else {
		/* If we have cached crc32c(buf, v) for initial value v,
		 * we can convert this to a different initial value v' by:
		 * crc32c(buf, v') = crc32c(buf, v) ^ adjustment
		 * where adjustment = crc32c(0*len(buf), v ^ v')
		 *
		 * http://crcutil.googlecode.com/files/crc-doc.1.0.pdf
		 * note, u for our crc32c implementation is 0
		 */
		crc = ccrc.second ^ ceph_crc32c(ccrc.first ^ crc, NULL,
						it->length());
		if (buffer_track_crc)
		  buffer_cached_crc_adjusted.inc();
	      }
	    } else {
	      uint32_t base = crc;
	      crc = ceph_crc32c(crc, (unsigned char*)it->c_str(), it->length());
	      r->set_crc(ofs, std::make_pair(base, crc));
	    }
	  }
	}
	return crc;
      }

      // -- buffer::list::iterator --
      /* iterator operator=(const iterator& other) {
	if (this != &other) {
	  bl = other.bl;
	  ls = other.ls;
	  off = other.off;
	  p = other.p;
	  p_off = other.p_off;
	}
	return *this;
	} */

      // -- buffer::list --

    }; /* class buffer::list */

    inline bool operator>(bufferlist& l, bufferlist& r) {
      for (unsigned p = 0; ; p++) {
	if (l.length() > p && r.length() == p) return true;
	if (l.length() == p) return false;
	if (l[p] > r[p]) return true;
	if (l[p] < r[p]) return false;
      }
    }

    inline bool operator>=(bufferlist& l, bufferlist& r) {
      for (unsigned p = 0; ; p++) {
	if (l.length() > p && r.length() == p) return true;
	if (r.length() == p && l.length() == p) return true;
	if (l.length() == p && r.length() > p) return false;
	if (l[p] > r[p]) return true;
	if (l[p] < r[p]) return false;
      }
    }

    inline std::ostream& operator<<(std::ostream& out,
				    const buffer::list& bl) {
      out << "buffer::list(len=" << bl.length() << "," << std::endl;

      PtrList::const_iterator it = bl.bi_buffers().begin();
      while (it != bl.bi_buffers().end()) {
	out << "\t" << *it;
	if (++it == bl.bi_buffers().end()) break;
	out << "," << std::endl;
      }
      out << std::endl << ")";
      return out;
    }

    inline bool operator==(bufferlist &l, bufferlist &r) {
      if (l.length() != r.length())
	return false;
      for (unsigned p = 0; p < l.length(); p++) {
	if (l[p] != r[p])
	  return false;
      }
      return true;
    }

    inline bool operator<(bufferlist& l, bufferlist& r) {
      return r > l;
    }

    inline bool operator<=(bufferlist& l, bufferlist& r) {
      return r >= l;
    }

  } /* namespace buffer */

} /* namespace ceph */

#endif /* CEPH_BUFFER_LIST_H */
