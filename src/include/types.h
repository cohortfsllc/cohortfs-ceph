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
 * Foundation.	See file COPYING.
 *
 */
#ifndef CEPH_TYPES_H
#define CEPH_TYPES_H

// this is needed for ceph_fs to compile in userland
#include "byteorder.h"

#include <set>
#include <map>
#include <unordered_map>
#include <string>
#include <vector>
#include <list>
#include <deque>
#include <sstream>
#include <netinet/in.h>
#include <fcntl.h>
#include <string.h>
#include <inttypes.h>
#include "include/hash.h"
#include "include/cmp.h"

using std::set;
using std::map;
using std::unordered_map;
using std::multimap;
using std::pair;
using std::make_pair;
using std::string;
using std::vector;
using std::list;
using std::deque;
using std::stringstream;
using std::ostringstream;
using std::istringstream;

#include "ceph_fs.h"
#include "ceph_frag.h"
#include "rbd_types.h"

extern "C" {
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "statlite.h"
}

#include <string>
#include <list>
#include <set>
#include <map>
#include <vector>
#include <iostream>
#include <iomanip>

#include <unordered_map>
#include <boost/optional.hpp>

#include "intarith.h"

#include "acconfig.h"

#include "assert.h"

// DARWIN compatibility
#ifdef DARWIN
typedef long long loff_t;
typedef long long off64_t;
#define O_DIRECT 00040000
#endif

// FreeBSD compatibility
#ifdef __FreeBSD__
typedef off_t loff_t;
typedef off_t off64_t;
#endif

// -- io helpers --

template <typename T>
inline std::ostream &operator << (std::ostream &out, boost::optional<T> const &maybe) {
  if (maybe)
    out << *maybe;
  return out;
}

template<class A, class B>
inline std::ostream& operator<<(std::ostream& out, const std::pair<A,B>& v) {
  return out << v.first << "," << v.second;
}

template<class A>
inline std::ostream& operator<<(std::ostream& out, const std::vector<A>& v) {
  out << "[";
  for (typename std::vector<A>::const_iterator p = v.begin(); p != v.end(); ++p) {
    if (p != v.begin()) out << ",";
    out << *p;
  }
  out << "]";
  return out;
}
template<class A>
inline std::ostream& operator<<(std::ostream& out, const std::deque<A>& v) {
  out << "<";
  for (typename std::deque<A>::const_iterator p = v.begin(); p != v.end();
       ++p) {
    if (p != v.begin()) out << ",";
    out << *p;
  }
  out << ">";
  return out;
}

template<class A>
inline std::ostream& operator<<(std::ostream& out, const std::list<A>& ilist) {
  for (typename std::list<A>::const_iterator it = ilist.begin();
       it != ilist.end();
       ++it) {
    if (it != ilist.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A>
inline std::ostream& operator<<(std::ostream& out, const std::set<A>& iset) {
  for (typename std::set<A>::const_iterator it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A>
inline std::ostream& operator<<(std::ostream& out, const std::multiset<A>& iset) {
  for (typename std::multiset<A>::const_iterator it = iset.begin();
       it != iset.end();
       ++it) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A,class B>
inline std::ostream& operator<<(std::ostream& out, const std::map<A,B>& m)
{
  out << "{";
  for (typename std::map<A,B>::const_iterator it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}

template<class A,class B>
inline std::ostream& operator<<(std::ostream& out, const std::multimap<A,B>& m)
{
  out << "{{";
  for (typename std::multimap<A,B>::const_iterator it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}}";
  return out;
}




/*
 * comparators for stl containers
 */
// for std::unordered_map:
//   std::unordered_map<const char*, long, hash<const char*>, eqstr> vals;
struct eqstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) == 0;
  }
};

// for set, map
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};



#include "encoding.h"

WRITE_RAW_ENCODER(ceph_fsid)
WRITE_RAW_ENCODER(ceph_mds_session_head)
WRITE_RAW_ENCODER(ceph_mds_request_head)
WRITE_RAW_ENCODER(ceph_mds_request_release)
WRITE_RAW_ENCODER(ceph_filelock)
WRITE_RAW_ENCODER(ceph_mds_caps)
WRITE_RAW_ENCODER(ceph_mds_cap_peer)
WRITE_RAW_ENCODER(ceph_mds_cap_release)
WRITE_RAW_ENCODER(ceph_mds_cap_item)
WRITE_RAW_ENCODER(ceph_mds_lease)
WRITE_RAW_ENCODER(ceph_mds_reply_head)
WRITE_RAW_ENCODER(ceph_mds_reply_inode)
WRITE_RAW_ENCODER(ceph_mds_cap_reconnect)
WRITE_RAW_ENCODER(ceph_frag_tree_split)
WRITE_RAW_ENCODER(ceph_osd_reply_head)
WRITE_RAW_ENCODER(ceph_osd_op)
WRITE_RAW_ENCODER(ceph_msg_header)
WRITE_RAW_ENCODER(ceph_msg_footer)
WRITE_RAW_ENCODER(ceph_msg_footer_old)
WRITE_RAW_ENCODER(ceph_mon_subscribe_item)

WRITE_RAW_ENCODER(ceph_mon_statfs)
WRITE_RAW_ENCODER(ceph_mon_statfs_reply)

// ----------------------
// some basic types

// NOTE: these must match ceph_fs.h typedefs
typedef uint64_t ceph_tid_t; // transaction id
typedef uint64_t version_t;
typedef uint32_t epoch_t;	// map epoch  (32bits -> 13 epochs/second for 10 years)

// --------------------------------------
// identify individual mount clients by 64bit value

struct client_t {
  int64_t v;

  client_t(int64_t _v = -2) : v(_v) {}

  void encode(bufferlist& bl) const {
    ::encode(v, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(v, bl);
  }
};
WRITE_CLASS_ENCODER(client_t)

static inline bool operator==(const client_t& l, const client_t& r) { return l.v == r.v; }
static inline bool operator!=(const client_t& l, const client_t& r) { return l.v != r.v; }
static inline bool operator<(const client_t& l, const client_t& r) { return l.v < r.v; }
static inline bool operator<=(const client_t& l, const client_t& r) { return l.v <= r.v; }
static inline bool operator>(const client_t& l, const client_t& r) { return l.v > r.v; }
static inline bool operator>=(const client_t& l, const client_t& r) { return l.v >= r.v; }

static inline bool operator>=(const client_t& l, int64_t o) { return l.v >= o; }
static inline bool operator<(const client_t& l, int64_t o) { return l.v < o; }

inline std::ostream& operator<<(std::ostream& out, const client_t& c) {
  return out << c.v;
}



// --------------------------------------
// ino

typedef uint64_t _inodeno_t;

struct inodeno_t {
  _inodeno_t val;
  inodeno_t() : val(0) {}
  inodeno_t(_inodeno_t v) : val(v) {}
  inodeno_t operator+=(inodeno_t o) { val += o.val; return *this; }
  operator _inodeno_t() const { return val; }

  void encode(bufferlist& bl) const {
    ::encode(val, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(val, p);
  }
} __attribute__ ((__may_alias__));
WRITE_CLASS_ENCODER(inodeno_t)

inline std::ostream& operator<<(std::ostream& out, inodeno_t ino) {
  return out << std::hex << ino.val << std::dec;
}

namespace std {
  template<> struct hash< inodeno_t >
  {
    size_t operator()( const inodeno_t& x ) const
    {
      static rjhash<uint64_t> H;
      return H(x.val);
    }
  };
}


// file modes

static inline bool file_mode_is_readonly(int mode) {
  return (mode & CEPH_FILE_MODE_WR) == 0;
}


// dentries
#define MAX_DENTRY_LEN 255

// --
namespace ceph {
  class Formatter;
}


// --

struct prettybyte_t {
  uint64_t v;
  prettybyte_t(uint64_t _v) : v(_v) {}
};

inline std::ostream& operator<<(std::ostream& out, const prettybyte_t& b)
{
  uint64_t bump_after = 100;
  if (b.v > bump_after << 60)
    return out << (b.v >> 60) << " EB";
  if (b.v > bump_after << 50)
    return out << (b.v >> 50) << " PB";
  if (b.v > bump_after << 40)
    return out << (b.v >> 40) << " TB";
  if (b.v > bump_after << 30)
    return out << (b.v >> 30) << " GB";
  if (b.v > bump_after << 20)
    return out << (b.v >> 20) << " MB";
  if (b.v > bump_after << 10)
    return out << (b.v >> 10) << " kB";
  return out << b.v << " bytes";
}

struct si_t {
  uint64_t v;
  si_t(uint64_t _v) : v(_v) {}
};

inline std::ostream& operator<<(std::ostream& out, const si_t& b)
{
  uint64_t bump_after = 100;
  if (b.v > bump_after << 60)
    return out << (b.v >> 60) << "E";
  if (b.v > bump_after << 50)
    return out << (b.v >> 50) << "P";
  if (b.v > bump_after << 40)
    return out << (b.v >> 40) << "T";
  if (b.v > bump_after << 30)
    return out << (b.v >> 30) << "G";
  if (b.v > bump_after << 20)
    return out << (b.v >> 20) << "M";
  if (b.v > bump_after << 10)
    return out << (b.v >> 10) << "k";
  return out << b.v;
}

struct pretty_si_t {
  uint64_t v;
  pretty_si_t(uint64_t _v) : v(_v) {}
};

inline std::ostream& operator<<(std::ostream& out, const pretty_si_t& b)
{
  uint64_t bump_after = 100;
  if (b.v > bump_after << 60)
    return out << (b.v >> 60) << " E";
  if (b.v > bump_after << 50)
    return out << (b.v >> 50) << " P";
  if (b.v > bump_after << 40)
    return out << (b.v >> 40) << " T";
  if (b.v > bump_after << 30)
    return out << (b.v >> 30) << " G";
  if (b.v > bump_after << 20)
    return out << (b.v >> 20) << " M";
  if (b.v > bump_after << 10)
    return out << (b.v >> 10) << " k";
  return out << b.v << " ";
}

struct kb_t {
  uint64_t v;
  kb_t(uint64_t _v) : v(_v) {}
};

inline std::ostream& operator<<(std::ostream& out, const kb_t& kb)
{
  uint64_t bump_after = 100;
  if (kb.v > bump_after << 40)
    return out << (kb.v >> 40) << " PB";
  if (kb.v > bump_after << 30)
    return out << (kb.v >> 30) << " TB";
  if (kb.v > bump_after << 20)
    return out << (kb.v >> 20) << " GB";
  if (kb.v > bump_after << 10)
    return out << (kb.v >> 10) << " MB";
  return out << kb.v << " kB";
}

inline std::ostream& operator<<(std::ostream& out, const ceph_mon_subscribe_item& i)
{
  return out << i.start
	     << ((i.flags & CEPH_SUBSCRIBE_ONETIME) ? "" : "+");
}

enum health_status_t {
  HEALTH_ERR = 0,
  HEALTH_WARN = 1,
  HEALTH_OK = 2,
};

#ifdef __cplusplus
inline std::ostream& operator<<(std::ostream &oss, health_status_t status) {
  switch (status) {
    case HEALTH_ERR:
      oss << "HEALTH_ERR";
      break;
    case HEALTH_WARN:
      oss << "HEALTH_WARN";
      break;
    case HEALTH_OK:
      oss << "HEALTH_OK";
      break;
  }
  return oss;
};
#endif

#endif
