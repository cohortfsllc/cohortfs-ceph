// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LttngStream.h"
#include "LttngLog.h"
#include "Log.h"

void lttng_stream::emit_header(int entity_type, const char *entity_name,
	 long thread, int pid, int message_id, short subsys, short prio)
{
	// TODO: emit header tracepoint with values from lttng_stream() constructor
	tracepoint(ceph, log_header, entity_type, entity_name, thread, pid, message_id, prio, subsys);
}

void lttng_stream::emit_integer(uint64_t val, enum int_type type)
{
}

void lttng_stream::emit_string(const char *val)
{
}

void lttng_stream::emit_footer()
{
}
