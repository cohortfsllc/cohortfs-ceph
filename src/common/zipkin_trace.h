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
  Endpoint(const char *name) {}
  Endpoint(const char *ip, int port, const char *name) {}

  void copy_ip(const string &newip) {}
  void copy_name(const string &newname) {}
  void copy_address_from(const Endpoint *endpoint) {}
  void share_address_from(const Endpoint *endpoint) {}
  void set_port(int p) {}
};

class Trace {
 public:
  Trace() {}
  Trace(const char *name, const Endpoint *ep, const Trace *parent = NULL) {}
  Trace(const char *name, const Endpoint *ep,
        const blkin_trace_info *i, bool child=false) {}

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

  void keyval(const char *key, const char *val) const {}
  void keyval(const char *key, int64_t val) const {}
  void keyval(const char *key, const char *val, const Endpoint *ep) const {}
  void keyval(const char *key, int64_t val, const Endpoint *ep) const {}

  void event(const char *event) const {}
  void event(const char *event, const Endpoint *ep) const {}
};
} // namespace ZTrace

#endif // !HAVE_LTTNG

#endif // COMMON_ZIPKIN_TRACE_H
