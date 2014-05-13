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

#ifndef CEPH_OBJECT_H
#define CEPH_OBJECT_H

#include <stdint.h>
#include <stdio.h>

#include <iostream>
#include <iomanip>
using namespace std;

#include "include/unordered_map.h"
#include "include/hash_namespace.h"

#include "hash.h"
#include "encoding.h"
#include "ceph_hash.h"
#include "cmp.h"

/// Maximum supported object name length for Ceph, in bytes.
#define MAX_CEPH_OBJECT_NAME_LEN 4096

struct object_t {
  string name;

  object_t() {}
  object_t(const char *s) : name(s) {}
  object_t(const string& s) : name(s) {}

  void swap(object_t& o) {
    name.swap(o.name);
  }
  void clear() {
    name.clear();
  }

  void encode(bufferlist &bl) const {
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(name, bl);
  }
};
WRITE_CLASS_ENCODER(object_t)

inline bool operator==(const object_t& l, const object_t& r) {
  return l.name == r.name;
}
inline bool operator!=(const object_t& l, const object_t& r) {
  return l.name != r.name;
}
inline bool operator>(const object_t& l, const object_t& r) {
  return l.name > r.name;
}
inline bool operator<(const object_t& l, const object_t& r) {
  return l.name < r.name;
}
inline bool operator>=(const object_t& l, const object_t& r) {
  return l.name >= r.name;
}
inline bool operator<=(const object_t& l, const object_t& r) {
  return l.name <= r.name;
}
inline ostream& operator<<(ostream& out, const object_t& o) {
  return out << o.name;
}

CEPH_HASH_NAMESPACE_START
  template<> struct hash<object_t> {
    size_t operator()(const object_t& r) const {
      //static hash<string> H;
      //return H(r.name);
      return ceph_str_hash_linux(r.name.c_str(), r.name.length());
    }
  };
CEPH_HASH_NAMESPACE_END


struct file_object_t {
  uint64_t ino, bno;
  mutable char buf[33];

  file_object_t(uint64_t i=0, uint64_t b=0) : ino(i), bno(b) {
    buf[0] = 0;
  }

  const char *c_str() const {
    if (!buf[0])
      sprintf(buf, "%"PRIx64".%08"PRIx64, ino, bno);
    return buf;
  }

  operator object_t() {
    return object_t(c_str());
  }
};

#endif
