// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef COMMON_ZIPKIN_TRACE_H
#define COMMON_ZIPKIN_TRACE_H

#ifdef HAVE_LTTNG

#include <ztracer.hpp>

#else // !HAVE_LTTNG

// add stubs for noop Trace and Endpoint
struct blkin_trace_info {};
namespace ZTracer
{
static inline int ztrace_init() { return 0; }

class Endpoint {
 public:
  Endpoint(const char *ip, int port, const char *name) {}

  void copy_ip(const string &newip) {}
  void copy_name(const string &newname) {}
  void set_port(int p) {}
};

class Trace {
 public:
  Trace() {}
  Trace(const char *name, const Endpoint *ep, const Trace *parent = NULL) {}
  Trace(const char *name, const Endpoint *ep,
        const blkin_trace_info *i, bool child=false) {}
  Trace(const Trace &rhs) {}

  bool valid() const { return false; }
  operator bool() const { return false; }

  int init(const char *name, const Endpoint *ep, const Trace *parent = NULL) {
    return 0;
  }
  int init(const char *name, const Endpoint *ep,
           const blkin_trace_info *i, bool child=false) {
    return 0;
  }

  void copy_name(const string &newname) {}

  const blkin_trace_info* get_info() const { return NULL; }
  void set_info(const blkin_trace_info *i) {}

  void keyval(const char *key, const char *val) {}
  void keyval(const char *key, int64_t val) {}
  void keyval(const char *key, const char *val, const Endpoint *ep) {}
  void keyval(const char *key, int64_t val, const Endpoint *ep) {}

  void event(const char *event) {}
  void event(const char *event, const Endpoint *ep) {}
};
} // namespace ZTrace

#endif // !HAVE_LTTNG

#endif // COMMON_ZIPKIN_TRACE_H
