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
#include "include/ceph_hash.h"

namespace ceph {
  class Formatter;
};

/// Maximum supported object name length for Ceph, in bytes.
static constexpr size_t MAX_CEPH_OBJECT_NAME_LEN = 4096;

enum struct chunktype : uint32_t {
  entirety, // Stride is not meaningful in this case
  data,
  ecc,
  terminus
};
WRITE_RAW_ENCODER(chunktype)

struct oid_t {
  std::string name;
  chunktype type;
  uint32_t stride;

  oid_t() : type(chunktype::entirety) {}
  oid_t(const std::string& n,
      chunktype t = chunktype::entirety,
      uint32_t s = 0) :
    name(n), type(t), stride(s) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(std::string&& n,
      chunktype type = chunktype::entirety,
      uint32_t stride = 0) :
    name(n), type(type), stride(stride) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(const char* n,
      chunktype t = chunktype::entirety,
      uint32_t s = 0) :
    name(n), type(t), stride(s) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(const oid_t& o) {
    name = o.name;
    type = o.type;
    stride = o.stride;
  }
  oid_t(const oid_t& o,
      chunktype t,
      uint32_t s) : name(o.name), type(t), stride(s) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(const oid_t& o,
      chunktype t) : name(o.name), type(t), stride(o.stride) {
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(const oid_t& o,
      uint32_t s) : name(o.name), type(o.type), stride(s) {
    stride = o.stride;
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(oid_t&& o) : oid_t() {
    std::swap(name, o.name);
    type = o.type;
    stride = o.stride;
  }
  oid_t(oid_t&& o, chunktype t, uint32_t s) : type(t), stride(s) {
    std::swap(name, o.name);
  }
  oid_t(oid_t&& o, chunktype t) : type(t), stride(o.stride) {
    std::swap(name, o.name);
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(oid_t&& o, uint32_t s) : type(o.type), stride(s) {
    std::swap(name, o.name);
    assert(!(type == chunktype::entirety) || (stride == 0));
  }
  oid_t(const char *in, char sep,
      bool (*appender)(std::string &dest, const char *begin,
		       const char *bound), const char *end = NULL);
  oid_t(const std::string &in, char sep,
      bool (*appender)(std::string &dest, const char *begin,
		       const char *bound))
    : oid_t(in.c_str(), sep, appender) { }
  ~oid_t() { }

  oid_t& operator=(const oid_t& o) {
    name = o.name;
    type = o.type;
    stride = o.stride;
    return *this;
  }
  oid_t& operator=(oid_t&& o) {
    swap(o);
    return *this;
  }

  void swap(oid_t& o) {
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
  static void generate_test_instances(std::list<oid_t*>& o);
  friend bool operator<(const oid_t&, const oid_t&);
  friend bool operator>(const oid_t&, const oid_t&);
  friend bool operator<=(const oid_t&, const oid_t&);
  friend bool operator>=(const oid_t&, const oid_t&);
  friend bool operator==(const oid_t&, const oid_t&);
  friend bool operator!=(const oid_t&, const oid_t&);
};
WRITE_CLASS_ENCODER(oid_t)

std::ostream& operator<<(std::ostream& out, const oid_t& o);

WRITE_EQ_OPERATORS_3(oid_t, name, type, stride)
WRITE_CMP_OPERATORS_3(oid_t, name, type, stride)

namespace std {
  template<> struct hash<oid_t> {
    size_t operator()(const oid_t& o) const {
      XXH64_state_t hs;
      XXH64_reset(&hs, 667);
      (void) XXH64_update(&hs, o.name.c_str(), o.name.size());
      (void) XXH64_update(&hs, (void*) &(o.type), 4);
      (void) XXH64_update(&hs, (void*) &(o.stride), 4);
      return XXH64_digest(&hs);
    }
  };
}

/* hoid */
struct hoid_t {
  oid_t oid;
  uint64_t hk;

  hoid_t(const hoid_t& hoid) : oid(hoid.oid) {
    hash();
  }

  hoid_t(const oid_t& _oid) : oid(_oid) {
    hash();
  }

  hoid_t() : hoid_t(oid_t()) {}

  hoid_t(hoid_t&& hoid) : oid(hoid.oid), hk(hoid.hk) {}

  void hash() {
    XXH64_state_t hs;
    XXH64_reset(&hs, 667);
    (void) XXH64_update(&hs, oid.name.c_str(), oid.name.size());
    (void) XXH64_update(&hs, (void*) &(oid.type), 4);
    (void) XXH64_update(&hs, (void*) &(oid.stride), 4);
    hk = XXH64_digest(&hs);
  }

  hoid_t& operator=(const hoid_t& o) {
    oid = o.oid;
    hk = o.hk;
    return *this;
  }

  hoid_t& operator=(hoid_t&& o) {
    swap(o);
    return *this;
  }

  void swap(hoid_t& o) {
    std::swap(oid, o.oid);
    std::swap(hk, o.hk);
  }

  std::string to_str() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<hoid_t*>& o);
  friend bool operator<(const hoid_t&, const hoid_t&);
  friend bool operator>(const hoid_t&, const hoid_t&);
  friend bool operator<=(const hoid_t&, const hoid_t&);
  friend bool operator>=(const hoid_t&, const hoid_t&);
  friend bool operator==(const hoid_t&, const hoid_t&);
  friend bool operator!=(const hoid_t&, const hoid_t&);
};
WRITE_CLASS_ENCODER(hoid_t)

inline bool operator<(const hoid_t& lhs, const hoid_t& rhs)
{
  return ((lhs.hk < rhs.hk) ||
	  ((lhs.hk == rhs.hk) && (lhs.oid < rhs.oid)));
}

inline bool operator>(const hoid_t& lhs, const hoid_t& rhs)
{
  return (rhs < lhs);
}

inline bool operator==(const hoid_t& lhs, const hoid_t& rhs)
{
  return (lhs.oid == rhs.oid);
}

std::ostream& operator<<(std::ostream& out, const hoid_t& o);

namespace std {
  template<> struct hash<hoid_t> {
    size_t operator()(const hoid_t& o) const {
      return o.hk;
    }
  };
}

// Should probably be in the MDS definitions. Also we don't have block
// numbers any more.
static inline oid_t file_oid(uint64_t i = 0, uint64_t b = 0) {
  char buf[33];
  sprintf(buf, "%" PRIx64 ".%08" PRIx64, i, b);
  return oid_t(buf);
};

#endif
