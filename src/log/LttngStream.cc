// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#define TRACEPOINT_CREATE_PROBES
#define TRACEPOINT_DEFINE

#include "LttngLog.h"
#include "LttngStream.h"
//#include "Log.h"

void lttng_stream::emit_header(int entity_type, const char *entity_name,
	 long thread, short subsys, short prio)
{
	// TODO: emit header tracepoint with values from lttng_stream() constructor
	tracepoint(ceph, log_header, entity_type, entity_name, thread, pid, message_id, prio, subsys);
}

void lttng_stream::emit_integer(uint64_t val, enum int_type type)
{
	tracepoint(ceph, log_integer, val, type, pid, message_id);
}

void lttng_stream::emit_string(const char *val)
{
	tracepoint(ceph, log_string, val, pid, message_id);
}

void lttng_stream::emit_manip(enum manip_type type, int val)
{
}

void lttng_stream::emit_footer()
{
	tracepoint(ceph, log_footer, pid, message_id);
}
