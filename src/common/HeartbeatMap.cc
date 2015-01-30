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

#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "include/ceph_time.h"
#include "HeartbeatMap.h"
#include "ceph_context.h"
#include "common/errno.h"

#include "debug.h"
#define dout_subsys ceph_subsys_heartbeatmap
#undef dout_prefix
#define dout_prefix *_dout << "heartbeat_map "

namespace ceph {

HeartbeatMap::HeartbeatMap(CephContext *cct)
  : m_cct(cct),
    m_inject_unhealthy_until(ceph::mono_time::min())
{
}

HeartbeatMap::~HeartbeatMap()
{
  assert(m_workers.empty());
}

heartbeat_handle_d *HeartbeatMap::add_worker(string name)
{
  unique_lock l(m_rwlock);
  ldout(m_cct, 10) << "add_worker '" << name << "'" << dendl;
  heartbeat_handle_d *h = new heartbeat_handle_d(name);
  m_workers.push_front(h);
  h->list_item = m_workers.begin();
  l.unlock();
  return h;
}

void HeartbeatMap::remove_worker(heartbeat_handle_d *h)
{
  unique_lock l(m_rwlock);
  ldout(m_cct, 10) << "remove_worker '" << h->name << "'" << dendl;
  m_workers.erase(h->list_item);
  l.unlock();
  delete h;
}

bool HeartbeatMap::_check(heartbeat_handle_d *h, const char *who,
			  ceph::mono_time now)
{
  bool healthy = true;
  ceph::mono_time was;

  was = mono_time(timespan(h->timeout));
  if (was > was.min() && was < now) {
    healthy = false;
  }
  was = mono_time(timespan(h->suicide_timeout));
  if (was > was.min() && was < now) {
    assert(0 == "hit suicide timeout");
  }
  return healthy;
}

void HeartbeatMap::reset_timeout(heartbeat_handle_d *h,
				 timespan grace,
				 timespan suicide_grace)
{
  mono_time now = mono_clock::now();
  _check(h, "reset_timeout", now);

  h->timeout = (now + grace).time_since_epoch().count();
  h->grace = grace;

  if (suicide_grace > ceph::timespan(0))
    h->suicide_timeout = (now + suicide_grace).time_since_epoch().count();
  else
    h->suicide_timeout = 0;
  h->suicide_grace = suicide_grace;
}

void HeartbeatMap::clear_timeout(heartbeat_handle_d *h)
{
  ldout(m_cct, 20) << "clear_timeout '" << h->name << "'" << dendl;
  mono_time now = mono_clock::now();
  _check(h, "clear_timeout", now);
  h->timeout = 0;
  h->suicide_timeout = 0;
}

bool HeartbeatMap::is_healthy()
{
  shared_lock l(m_rwlock);
  ceph::mono_time now = ceph::mono_clock::now();
  if (m_cct->_conf->heartbeat_inject_failure) {
    ldout(m_cct, 0) << "is_healthy injecting failure for next "
		    << m_cct->_conf->heartbeat_inject_failure << " seconds"
		    << dendl;
    m_inject_unhealthy_until = now + ceph::span_from_double(
      m_cct->_conf->heartbeat_inject_failure);
    m_cct->_conf->set_val("heartbeat_inject_failure", "0");
  }

  bool healthy = true;
  if (now < m_inject_unhealthy_until) {
    healthy = false;
  }

  for (list<heartbeat_handle_d*>::iterator p = m_workers.begin();
       p != m_workers.end();
       ++p) {
    heartbeat_handle_d *h = *p;
    if (!_check(h, "is_healthy", now)) {
      healthy = false;
    }
  }
  l.unlock();
  ldout(m_cct, 20) << "is_healthy = " << (healthy ? "healthy" : "NOT HEALTHY") << dendl;
  return healthy;
}

void HeartbeatMap::check_touch_file()
{
  if (is_healthy()) {
    string path = m_cct->_conf->heartbeat_file;
    if (path.length()) {
      int fd = ::open(path.c_str(), O_WRONLY|O_CREAT, 0644);
      if (fd >= 0) {
	::utimes(path.c_str(), NULL);
	::close(fd);
      } else {
	ldout(m_cct, 0) << "unable to touch " << path << ": "
			<< cpp_strerror(errno) << dendl;
      }
    }
  }
}

}
