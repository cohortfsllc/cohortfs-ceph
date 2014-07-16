// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef __CEPH_OS_HOBJECT_H
#define __CEPH_OS_HOBJECT_H

#include <string.h>
#include "include/types.h"
#include "include/object.h"
#include "include/cmp.h"
#include "include/assert.h"   // spirit clobbers it!


namespace ceph {
  class Formatter;
}

enum stripetype_t {
  ENTIRETY, // Stripeno is not meaningful in this case
  DATA,
  ECC,
  TERMINUS
};
WRITE_RAW_ENCODER(stripetype_t)
/* This identifies a stripe of an object. hobject_t is a stupid name
   for it. Either change it, which will be a pain, or find some
   retronym to justify it. */

struct hobject_t {
  object_t oid;
  stripetype_t stripetype;
  // I don't know whether it will make more sense to have the stripe
  // numbers go back to zero when we switch from code to data, but
  // whatever it is, it should be consistent.
  uint32_t stripeno;

  bool append_c_str(char *orig, char sep, size_t len,
		    char *(*appender)(char *dest, const char* src,
				      size_t len) = NULL) const;

  void append_str(
    string &orig, char sep,
    void (*appender)(string &dest, const string &src) = NULL) const;
  string to_str() const;

  hobject_t() : stripetype(ENTIRETY) {}

  hobject_t(object_t oid,
	    stripetype_t stripetype = ENTIRETY,
	    uint32_t stripeno = 0) :
    oid(oid), stripetype(stripetype), stripeno(stripeno) {
    assert(!(stripetype == ENTIRETY) || (stripeno == 0));
  }

  hobject_t datastripe(uint32_t _stripeno) {
    return hobject_t(oid, DATA, _stripeno);
  }

  void swap(hobject_t &o) {
    hobject_t temp(o);
    o = (*this);
    (*this) = temp;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<hobject_t*>& o);
  friend bool operator<(const hobject_t&, const hobject_t&);
  friend bool operator>(const hobject_t&, const hobject_t&);
  friend bool operator<=(const hobject_t&, const hobject_t&);
  friend bool operator>=(const hobject_t&, const hobject_t&);
  friend bool operator==(const hobject_t&, const hobject_t&);
  friend bool operator!=(const hobject_t&, const hobject_t&);
};
WRITE_CLASS_ENCODER(hobject_t)

CEPH_HASH_NAMESPACE_START
template<> struct hash<hobject_t> {
  size_t operator()(const hobject_t& r) const {
    static hash<object_t> H;
    return H(r.oid);
  }
};
CEPH_HASH_NAMESPACE_END

ostream& operator<<(ostream& out, const hobject_t& o);

WRITE_EQ_OPERATORS_3(hobject_t, oid, stripetype, stripeno)
WRITE_CMP_OPERATORS_3(hobject_t, oid, stripetype, stripeno)
#endif
