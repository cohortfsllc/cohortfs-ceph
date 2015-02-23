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

#ifndef COMMON_OBJECT_H
#define COMMON_OBJECT_H

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include "include/cmp.h"
#include "include/encoding.h"

/// Maximum supported object name length for Ceph, in bytes.
static constexpr size_t MAX_CEPH_OBJECT_NAME_LEN = 4096;

enum struct chunktype : uint32_t {
  entirety, // Stride is not meaningful in this case
  data,
  ecc,
  terminus
};
WRITE_RAW_ENCODER(chunktype)

struct oid {
  std::string name;
  chunktype type;
  uint32_t stride;

  oid() : type(chunktype::entirety) {}
  oid(const std::string& s,
      chunktype type = chunktype::entirety,
      uint32_t stride = 0) :
    name(s), type(type), stride(stride) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(std::string&& s,
      chunktype type = chunktype::entirety,
      uint32_t stride = 0) :
    name(s), type(type), stride(stride) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(const char* s,
      chunktype type = chunktype::entirety,
      uint32_t stride = 0) :
    name(s), type(type), stride(stride) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(const oid& o) {
    name = o.name;
    type = o.type;
    stride = o.stride;
  }
  oid(const oid& o,
      chunktype type,
      uint32_t stride) : oid(o) {
    type = o.type;
    stride = o.stride;
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(const oid& o,
      chunktype type) : oid(o) {
    type = o.type;
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(const oid& o,
      uint32_t stride) : oid(o) {
    stride = o.stride;
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(oid&& o) : oid() {
    std::swap(name, o.name);
    type = o.type;
    stride = o.stride;
  }
  oid(oid&& o, chunktype type, uint32_t stride) : oid(o) {
    type = o.type;
    stride = o.stride;
  }
  oid(oid&& o, chunktype type) : oid(o) {
    type = o.type;
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(oid&& o, uint32_t stride) : oid(o) {
    stride = o.stride;
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid(const char *in, char sep,
      bool (*appender)(std::string &dest, const char *begin,
		       const char *bound), const char *end = NULL);
  oid(const std::string &in, char sep,
      bool (*appender)(std::string &dest, const char *begin,
		       const char *bound))
    : oid(in.c_str(), sep, appender) { }
  ~oid() { }

  oid& operator=(const oid& o) {
    name = o.name;
    type = o.type;
    stride = o.stride;
    return *this;
  }
  oid& operator=(oid&& o) {
    swap(o);
    return *this;
  }

  void swap(oid& o) {
    std::swap(name, o.name);
    std::swap(type, o.type);
    std::swap(stride, o.stride);
  }

  bool append_c_str(char *orig, char sep, size_t len,
		    char *(*appender)(char *dest, const char* src,
				      size_t len) = NULL) const;
  void append_str(
    std::string &orig, char sep,
    void (*appender)(std::string &dest, const std::string &src) = NULL) const;
  std::string to_str() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<oid*>& o);
  friend bool operator<(const oid&, const oid&);
  friend bool operator>(const oid&, const oid&);
  friend bool operator<=(const oid&, const oid&);
  friend bool operator>=(const oid&, const oid&);
  friend bool operator==(const oid&, const oid&);
  friend bool operator!=(const oid&, const oid&);
};
WRITE_CLASS_ENCODER(oid)

std::ostream& operator<<(std::ostream& out, const oid& o);

WRITE_EQ_OPERATORS_3(oid, name, type, stride)
WRITE_CMP_OPERATORS_3(oid, name, type, stride)

namespace std {
  template<> struct hash<oid> {
    size_t operator()(const oid& o) const {
      return (std::hash<std::string>()(o.name) ^
	      std::hash<uint32_t>()((uint32_t)o.type) ^
	      std::hash<uint32_t>()(o.stride));
    }
  };
}


// Should probably be in the MDS definitions. Also we don't have block
// numbers any more.
static inline oid file_oid(uint64_t i = 0, uint64_t b = 0) {
  char buf[33];
  sprintf(buf, "%" PRIx64 ".%08" PRIx64, i, b);
  return oid(buf);
};

#endif
