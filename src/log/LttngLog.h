// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_LTTNGLOG_H
#define CEPH_LOG_LTTNGLOG_H

#define TRACEPOINT_CREATE_PROBES
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(
       ceph,
       log,
       TP_ARGS(short, prio, short, subsys, const char *, message),
       TP_FIELDS(
             ctf_integer(short, prio, prio)
             ctf_integer(short, subsys, subsys)
             ctf_string(msg, message)
       )
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log,
       TRACE_INFO)

#endif
