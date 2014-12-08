// vim: ts=8 sw=2 smarttab

//#ifndef CEPH_LOG_LTTNGLOG_H
//#define CEPH_LOG_LTTNGLOG_H

#define TRACEPOINT_PROVIDER ceph
#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "log/LttngLog.h"

#if !defined(CEPH_LOG_LTTNGLOG_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define CEPH_LOG_LTTNGLOG_H

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

#include <lttng/tracepoint-event.h>
