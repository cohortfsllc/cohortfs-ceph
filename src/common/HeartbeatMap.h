// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_HEARTBEATMAP_H
#define CEPH_HEARTBEATMAP_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <list>
#include <time.h>
#include <pthread.h>

#include "include/ceph_time.h"


class CephContext;

namespace ceph {

/*
 * HeartbeatMap -
 *
 * Maintain a set of handles for internal subsystems to periodically
 * check in with a health check and timeout.  Each user can register
 * and get a handle they can use to set or reset a timeout.
 *
 * A simple is_healthy() method checks for any users who are not within
 * their grace period for a heartbeat.
 */

struct heartbeat_handle_d {
  std::string name;
  // We really want atomic timepoints, look into that later
  std::atomic<ceph_timerep> timeout;
  std::atomic<ceph_timerep> suicide_timeout;
  timespan grace, suicide_grace;
  std::list<heartbeat_handle_d*>::iterator list_item;

  heartbeat_handle_d(const std::string& n)
    : name(n), timeout(0), suicide_timeout(0),
      grace(timespan(0)), suicide_grace(timespan(0))
  { }
};

class HeartbeatMap {
 public:
  // register/unregister
  heartbeat_handle_d *add_worker(std::string name);
  void remove_worker(heartbeat_handle_d *h);

  // reset the timeout so that it expects another touch within grace amount of time
  void reset_timeout(heartbeat_handle_d *h,
		     timespan grace, timespan suicide_grace);
  // clear the timeout so that it's not checked on
  void clear_timeout(heartbeat_handle_d *h);

  // return false if any of the timeouts are currently expired.
  bool is_healthy();

  // touch cct->_conf->heartbeat_file if is_healthy()
  void check_touch_file();

  HeartbeatMap(CephContext *cct);
  ~HeartbeatMap();

 private:
  CephContext *m_cct;
  std::shared_timed_mutex m_rwlock;
  typedef std::shared_lock<std::shared_timed_mutex> shared_lock;
  typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
  ceph::mono_time m_inject_unhealthy_until;
  std::list<heartbeat_handle_d*> m_workers;

  bool _check(heartbeat_handle_d *h, const char *who,
	      ceph::mono_time now);
};

}

using ceph::heartbeat_handle_d;

#endif
