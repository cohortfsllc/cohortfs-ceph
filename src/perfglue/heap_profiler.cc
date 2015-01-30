// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include "heap_profiler.h"
#include "common/environment.h"
#include "common/LogClient.h"
#include "common/debug.h"

bool ceph_using_tcmalloc()
{
  return true;
}

void ceph_heap_profiler_init(CephContext* cct)
{
  // Two other interesting environment variables to set are:
  // HEAP_PROFILE_ALLOCATION_INTERVAL, HEAP_PROFILE_INUSE_INTERVAL
  if (get_env_bool("CEPH_HEAP_PROFILER_INIT")) {
    ceph_heap_profiler_start(cct);
  }
}

void ceph_heap_profiler_stats(char *buf, int length)
{
  MallocExtension::instance()->GetStats(buf, length);
}

void ceph_heap_release_free_memory()
{
  MallocExtension::instance()->ReleaseFreeMemory();
}

bool ceph_heap_profiler_running()
{
  return IsHeapProfilerRunning();
}

static void get_profile_name(CephContext* cct, char *profile_name,
			     int profile_name_len)
{
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s", cct->_conf->log_file.c_str());
  char *last_slash = rindex(path, '/');

  if (last_slash == NULL) {
    snprintf(profile_name, profile_name_len, "./%s.profile",
	     cct->_conf->name.to_cstr());
  }
  else {
    last_slash[1] = '\0';
    snprintf(profile_name, profile_name_len, "%s/%s.profile",
	     path, cct->_conf->name.to_cstr());
  }
}

void ceph_heap_profiler_start(CephContext* cct)
{
  char profile_name[PATH_MAX];
  get_profile_name(cct, profile_name, sizeof(profile_name));
  generic_dout(0) << "turning on heap profiler with prefix "
		  << profile_name << dendl;
  HeapProfilerStart(profile_name);
}

void ceph_heap_profiler_stop()
{
  HeapProfilerStop();
}

void ceph_heap_profiler_dump(const char *reason)
{
  HeapProfilerDump(reason);
}

void ceph_heap_profiler_handle_command(CephContext* cct,
				       const std::vector<std::string>& cmd,
				       ostream& out)
{
  if (cmd.size() == 1 && cmd[0] == "dump") {
    if (!ceph_heap_profiler_running()) {
      out << "heap profiler not running; can't dump";
      return;
    }
    char *heap_stats = new char[1024];
    ceph_heap_profiler_stats(heap_stats, 1024);
    out << cct->_conf->name << "dumping heap profile now.\n"
	<< heap_stats;
    ceph_heap_profiler_dump("admin request");
  } else if (cmd.size() == 1 && cmd[0] == "start_profiler") {
    ceph_heap_profiler_start(cct);
    out << cct->_conf->name << " started profiler";
  } else if (cmd.size() == 1 && cmd[0] == "stop_profiler") {
    ceph_heap_profiler_stop();
    out << cct->_conf->name << " stopped profiler";
  } else if (cmd.size() == 1 && cmd[0] == "release") {
    ceph_heap_release_free_memory();
    out << cct->_conf->name << " releasing free RAM back to system.";
  } else if (cmd.size() == 1 && cmd[0] == "stats") {
    char *heap_stats = new char[1024];
    ceph_heap_profiler_stats(heap_stats, 1024);
    out << cct->_conf->name << "tcmalloc heap stats:"
	<< heap_stats;
  } else {
    out << "unknown command " << cmd;
  }
}
