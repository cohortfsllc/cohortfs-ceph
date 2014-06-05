// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _CEPH_UUID_H
#define _CEPH_UUID_H

/*
 * Thin C++ wrapper around libuuid.
 */

#include "encoding.h"
#include <ostream>
#include <string>

extern "C" {
#include <uuid/uuid.h>
#include <unistd.h>
}

struct uuid_d {
  uuid_t uuid;

  uuid_d() {
    memset(&uuid, 0, sizeof(uuid));
  }

  bool is_zero() const {
    return uuid_is_null(uuid);
  }

  void generate_random() {
    uuid_generate(uuid);
  }

  bool parse(const char *s) {
    return uuid_parse(s, uuid) == 0;
  }

  bool parse(const std::string &s) {
    return uuid_parse(s.c_str(), uuid) == 0;
  }
  void print(char *s) {
    return uuid_unparse(uuid, s);
  }

  std::string to_str() {
    char b[uuid_d::char_rep_buf_size];
    uuid_unparse(uuid, b);
    return std::string(b);
  }

  void encode(bufferlist& bl) const {
    ::encode_raw(uuid, bl);
  }
  void decode(bufferlist::iterator& p) const {
    ::decode_raw(uuid, p);
  }

  static const int char_rep_buf_size = 37;
};
WRITE_CLASS_ENCODER(uuid_d)

inline std::ostream& operator<<(std::ostream& out, const uuid_d& u) {
  char b[uuid_d::char_rep_buf_size];
  uuid_unparse(u.uuid, b);
  return out << b;
}

inline bool operator==(const uuid_d& l, const uuid_d& r) {
  return uuid_compare(l.uuid, r.uuid) == 0;
}
inline bool operator!=(const uuid_d& l, const uuid_d& r) {
  return uuid_compare(l.uuid, r.uuid) != 0;
}
inline bool operator<(const uuid_d& l, const uuid_d& r) {
  return uuid_compare(l.uuid, r.uuid) < 0;
}
inline bool operator<=(const uuid_d& l, const uuid_d& r) {
  return uuid_compare(l.uuid, r.uuid) <= 0;
}
inline bool operator>(const uuid_d& l, const uuid_d& r) {
  return uuid_compare(l.uuid, r.uuid) > 0;
}
inline bool operator>=(const uuid_d& l, const uuid_d& r) {
  return uuid_compare(l.uuid, r.uuid) >= 0;
}

#endif
