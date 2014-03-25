#ifndef _CEPH_UUID_H
#define _CEPH_UUID_H

/*
 * Thin C++ wrapper around libuuid.
 */

#include "encoding.h"
#include <ostream>
#include <string>
#include <stdexcept>

extern "C" {
#include <uuid/uuid.h>
#include <unistd.h>
}

struct uuid_d {
  static const int char_rep_buf_size = 37;

  uuid_t uuid;

  uuid_d() {
    uuid_clear(uuid);
  }

  uuid_d(uint64_t x, uint64_t y) {
    memcpy(uuid, &x, sizeof(uint64_t));
    memcpy(uuid + sizeof(uint64_t), &y, sizeof(uint64_t));
  }

  uuid_d(const uuid_d &u) {
    uuid_copy(uuid, u.uuid);
  }

  void clear() {
    uuid_clear(uuid);
  }

  bool is_zero() const {
    return uuid_is_null(uuid);
  }

  static uuid_d generate_random() {
    uuid_d u;
    uuid_generate(u.uuid);
    return u;
  }

  static uuid_d parse(const char *s) {
    uuid_d u;
    int i = uuid_parse(s, u.uuid);
    if (i == 0) {
      return u;
    } else {
      return uuid_d(-1, -1);
    }
  }

  void print(char *s) const {
    return uuid_unparse(uuid, s);
  }

  // version of above functions using strings
  static uuid_d parse(const std::string& s) {
    uuid_d u = parse(s.c_str());
    return u;
  }

  // version of above functions using strings
  static uuid_d swallow(const uuid_t u) {
    uuid_d v;
    uuid_copy(v.uuid, u);
    return v;
  }
  void print(std::string& s) const {
    char buff[char_rep_buf_size];
    print(buff);
    s = buff;
  }

  void encode(bufferlist& bl) const {
    ::encode_raw(uuid, bl);
  }
  void decode(bufferlist::iterator& p) const {
    ::decode_raw(uuid, p);
  }

  // allows uuid_d datatype to be used as key to std::map
  bool operator<(const uuid_d& r) const {
      return uuid_compare(this->uuid, r.uuid) < 0;
  }

  bool operator<=(const uuid_d& r) const {
      return uuid_compare(this->uuid, r.uuid) <= 0;
  }

  bool operator>(const uuid_d& r) const {
      return uuid_compare(this->uuid, r.uuid) > 0;
  }

  bool operator>=(const uuid_d& r) const {
      return uuid_compare(this->uuid, r.uuid) >= 0;
  }

  operator std::string() const {
      char buf[char_rep_buf_size];
      print(buf);
      return std::string(buf);
  }
}; // uuid_d

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

#endif
