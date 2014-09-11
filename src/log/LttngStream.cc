// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LttngStream.h"
#include "LttngLog.h"


void lttng_stream::emit_header()
{
  // TODO: emit header tracepoint with values from lttng_stream() constructor
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
