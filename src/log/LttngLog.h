// vim: ts=8 sw=2 smarttab

//#ifndef CEPH_LOG_LTTNGLOG_H
//#define CEPH_LOG_LTTNGLOG_H

#define TRACEPOINT_PROVIDER ceph
#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "log/LttngLog.h"

#if !defined(CEPH_LOG_LTTNGLOG_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define CEPH_LOG_LTTNGLOG_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(ceph,
      log_header,
       TP_ARGS(int, entity_type, const char *, entity_name, long, thread, int, pid, int, message_id, short, prio, short, subsys),
       TP_FIELDS(
	     ctf_integer(int, entity_type, entity_type)
	     ctf_string(entity_name, entity_name)
	     ctf_integer(long, thread, thread)
	     ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
             ctf_integer(short, prio, prio)
             ctf_integer(short, subsys, subsys)
       )
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_header,
       TRACE_INFO)

TRACEPOINT_EVENT(ceph,
       log_integer,
       TP_ARGS(uint64_t, value, char, type, int, pid, int, message_id),
       TP_FIELDS(
	     ctf_integer(uint64_t, value, value)
	     ctf_integer(char, type, type)
	     ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
	)	
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_integer,
       TRACE_INFO)

TRACEPOINT_EVENT(ceph,
       log_string,
       TP_ARGS(const char *, string, int, pid, int, message_id),
       TP_FIELDS(
	     ctf_string(string, string)
	     ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
	)
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_string,
       TRACE_INFO)

TRACEPOINT_EVENT(ceph,
       log_manip,
       TP_ARGS(int, value, char, type, int, pid, int, message_id),
       TP_FIELDS(
	     ctf_integer(int, value, value)
	     ctf_integer(char, type, type)
	     ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
	)	
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_manip,
       TRACE_INFO)

TRACEPOINT_EVENT(
       ceph,
       log_footer,
       TP_ARGS(int, pid, int, message_id),
       TP_FIELDS(
             ctf_integer(int, pid, pid)
             ctf_integer(int, message_id, message_id)
       )
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_footer,
       TRACE_INFO)

TRACEPOINT_EVENT(
       ceph,
       log_blob,
       TP_ARGS(const char *, blob, size_t, len),
       TP_FIELDS(
             ctf_sequence(char, blob_name, blob, size_t, len)
       )
)

TRACEPOINT_LOGLEVEL(
       ceph,
       log_blob,
       TRACE_INFO)

#endif

#include <lttng/tracepoint-event.h>
