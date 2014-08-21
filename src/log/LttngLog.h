// vim: ts=8 sw=2 smarttab

//#ifndef CEPH_LOG_LTTNGLOG_H
//#define CEPH_LOG_LTTNGLOG_H

#define TRACEPOINT_PROVIDER ceph
#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "/home/ali/ceph-local/src/log/LttngLog.h"

#if !defined(CEPH_LOG_LTTNGLOG_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define CEPH_LOG_LTTNGLOG_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(
       ceph,
       log_header,
       TP_ARGS(int, entity_type, const char *,  entity_name, int, pid, int, message_id, short, prio, short, subsys),
       TP_FIELDS(
	     ctf_integer(int, entity_type, entity_type)
	     ctf_string(entity_name,  entity_name)
	     ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
             ctf_integer(short, subsys, subsys)
             ctf_integer(short, prio, prio)
       )
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_header,
       TRACE_INFO)

TRACEPOINT_EVENT(
       ceph,
       log_message,
       TP_ARGS(int, pid, int, message_id, const char *, message),
       TP_FIELDS(
             ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
             ctf_string(msg, message)
       )
)

TRACEPOINT_LOGLEVEL(
       nothing,
       log_message,
       TRACE_INFO)


#endif

#include <lttng/tracepoint-event.h>
