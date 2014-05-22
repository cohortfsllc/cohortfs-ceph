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

#include "json_spirit/json_spirit_value.h"
#include "include/assert.h"   // spirit clobbers it!

typedef uint64_t filestore_hobject_key_t;

namespace ceph {
  class Formatter;
}

struct hobject_t {
  object_t oid;
  uint32_t hash;
private:
  bool max;
  static const int64_t POOL_IS_TEMP = -1;
public:
  int64_t pool;
  string nspace;

private:
  string key;

public:
  const string &get_key() const {
    return key;
  }

  string to_str() const;

  static bool match_hash(uint32_t to_check, uint32_t bits, uint32_t match) {
    return (match & ~((~0)<<bits)) == (to_check & ~((~0)<<bits));
  }
  bool match(uint32_t bits, uint32_t match) const {
    return match_hash(hash, bits, match);
  }

  static hobject_t make_temp(const string &name) {
    hobject_t ret(object_t(name), "", 0, POOL_IS_TEMP, "");
    return ret;
  }
  bool is_temp() const {
    return pool == POOL_IS_TEMP;
  }

  hobject_t() : hash(0), max(false), pool(-1) {}

  hobject_t(object_t oid, const string& key, uint64_t hash,
	    int64_t pool, string nspace) :
    oid(oid), hash(hash), max(false),
    pool(pool), nspace(nspace),
    key(oid.name == key ? string() : key) {}

  /// @return min hobject_t ret s.t. ret.hash == this->hash
  hobject_t get_boundary() const {
    if (is_max())
      return *this;
    hobject_t ret;
    ret.hash = hash;
    return ret;
  }

  /// @return head version of this hobject_t
  hobject_t get_head() const {
    hobject_t ret(*this);
    return ret;
  }

  /* Do not use when a particular hash function is needed */
  explicit hobject_t(const object_t &o) :
    oid(o), max(false), pool(-1) {
    hash = CEPH_HASH_NAMESPACE::hash<object_t>()(o);
  }

  // maximum sorted value.
  static hobject_t get_max() {
    hobject_t h;
    h.max = true;
    return h;
  }
  bool is_max() const {
    return max;
  }
  bool is_min() const {
    // this needs to match how it's constructed
    return hash == 0 &&
	   !max &&
	   pool == -1;
  }

  static uint32_t _reverse_nibbles(uint32_t retval) {
    // reverse nibbles
    retval = ((retval & 0x0f0f0f0f) << 4) | ((retval & 0xf0f0f0f0) >> 4);
    retval = ((retval & 0x00ff00ff) << 8) | ((retval & 0xff00ff00) >> 8);
    retval = ((retval & 0x0000ffff) << 16) | ((retval & 0xffff0000) >> 16);
    return retval;
  }

  /**
   * Returns set S of strings such that for any object
   * h where h.match(bits, mask), there is some string
   * s \in S such that s is a prefix of h.to_str().
   * Furthermore, for any s \in S, s is a prefix of
   * h.str() implies that h.match(bits, mask).
   */
  static set<string> get_prefixes(
    uint32_t bits,
    uint32_t mask,
    int64_t pool);

  filestore_hobject_key_t get_filestore_key_u32() const {
    assert(!max);
    return _reverse_nibbles(hash);
  }
  filestore_hobject_key_t get_filestore_key() const {
    if (max)
      return 0x100000000ull;
    else
      return get_filestore_key_u32();
  }

  const string& get_effective_key() const {
    if (key.length())
      return key;
    return oid.name;
  }

  void swap(hobject_t &o) {
    hobject_t temp(o);
    o = (*this);
    (*this) = temp;
  }

  const string &get_namespace() const {
    return nspace;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(json_spirit::Value& v);
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
    size_t operator()(const hobject_t &r) const {
      static hash<object_t> H;
      return H(r.oid);
    }
  };
CEPH_HASH_NAMESPACE_END

ostream& operator<<(ostream& out, const hobject_t& o);

WRITE_EQ_OPERATORS_6(hobject_t, oid, get_key(), hash, max, pool, nspace)
// sort hobject_t's by <max, get_filestore_key(hash), key, oid>
WRITE_CMP_OPERATORS_6(hobject_t,
		      max,
		      get_filestore_key(),
		      nspace,
		      pool,
		      get_effective_key(),
		      oid)
#endif
