// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_LTTNGSTREAM
#define CEPH_LOG_LTTNGSTREAM

#include <inttypes.h>

#include "LttngLog.h"


// lttng log stream
class lttng_stream {
private:
  // TODO: add variables for pid, message_id for use in emit_*() functions

  // integer types for log_integer tracepoint
  enum int_type {
    TYPE_BOOL,
    TYPE_U8,
    TYPE_S8,
    TYPE_U16,
    TYPE_S16,
    TYPE_U32,
    TYPE_S32,
    TYPE_U64,
    TYPE_S64,
    TYPE_FLOAT,
    TYPE_DOUBLE,
    TYPE_DOUBLE64,
    TYPE_PTR,
  };

  // tracepoint emission functions
  void emit_header()
  {
    // TODO: emit header tracepoint with values from lttng_stream() constructor
  }
  void emit_integer(uint64_t val, enum int_type type)
  {
  }
  void emit_string(const char *val)
  {
  }
  void emit_footer()
  {
  }

public:
  lttng_stream() // TODO: add any arguments for the header message
  {
    emit_header();
  }
  ~lttng_stream()
  {
    emit_footer();
  }

  // output operators
  lttng_stream& operator<<(bool val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_BOOL);
    return *this;
  }
  lttng_stream& operator<<(char val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S8);
    return *this;
  }
  lttng_stream& operator<<(unsigned char val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U8);
    return *this;
  }
  lttng_stream& operator<<(short val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S16);
    return *this;
  }
  lttng_stream& operator<<(unsigned short val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U16);
    return *this;
  }
  lttng_stream& operator<<(int val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S32);
    return *this;
  }
  lttng_stream& operator<<(unsigned int val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U32);
    return *this;
  }
  lttng_stream& operator<<(long val) {
    const enum int_type type = sizeof(val) == 4 ? TYPE_S32 : TYPE_S64;
    emit_integer(static_cast<uint64_t>(val), type);
    return *this;
  }
  lttng_stream& operator<<(unsigned long val) {
    const enum int_type type = sizeof(val) == 4 ? TYPE_U32 : TYPE_U64;
    emit_integer(static_cast<uint64_t>(val), type);
    return *this;
  }
  lttng_stream& operator<<(long long val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_S64);
    return *this;
  }
  lttng_stream& operator<<(unsigned long long val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_U64);
    return *this;
  }
  lttng_stream& operator<<(float val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_FLOAT);
    return *this;
  }
  lttng_stream& operator<<(double val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_DOUBLE);
    return *this;
  }
  lttng_stream& operator<<(long double val) {
    emit_integer(static_cast<uint64_t>(val), TYPE_DOUBLE64);
    return *this;
  }
  lttng_stream& operator<<(void* val) {
    emit_integer(reinterpret_cast<uint64_t>(val), TYPE_PTR);
    return *this;
  }
  lttng_stream& operator<<(const char* val) {
    emit_string(val);
    return *this;
  }
  lttng_stream& operator<<(const unsigned char* val) {
    emit_string(reinterpret_cast<const char*>(val));
    return *this;
  }

  // for std::flush, endl, hex, dec, etc
  typedef std::ostream& (*ostream_manip)(std::ostream&);
  lttng_stream& operator<<(ostream_manip manip) {
    // XXX: figure out how to support endl, hex, dec, etc
    return *this;
  }
};

#endif // CEPH_LOG_LTTNGSTREAM
