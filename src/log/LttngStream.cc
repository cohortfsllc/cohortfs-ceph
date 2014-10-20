// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#define TRACEPOINT_CREATE_PROBES
#define TRACEPOINT_DEFINE

#include "LttngLog.h"
#include "LttngStream.h"
#include <atomic>
//#include "Log.h"

namespace {
pid_t lttng_getpid() {
        static const pid_t pid = getpid();
        return pid;
}
uint64_t lttng_next_message_id() {
        static std::atomic<uint64_t> id(0);
        return id++;
}
}

lttng_stream::lttng_endl lttng_stream::endl;

lttng_stream::lttng_stream(int entity_type, const char *entity_name,
	 short subsys, short prio)
	: pid(lttng_getpid()),
	message_id(lttng_next_message_id()),
	thread(pthread_self())

  {
	emit_header(entity_type, entity_name, thread, subsys, prio);
  }

void lttng_stream::emit_header(int entity_type, const char *entity_name,
	 long thread, short subsys, short prio)
{
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
	tracepoint(ceph, log_manip, val, type, pid, message_id);
}

void lttng_stream::emit_footer()
{
	tracepoint(ceph, log_footer, pid, message_id);
}

void lttng_stream::emit_blob(const char *blob, size_t len)
{
	tracepoint(ceph, log_blob, blob, len, pid, message_id);
}
