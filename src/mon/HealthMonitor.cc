// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <sstream>
#include <cassert>
#include <stdlib.h>
#include <limits.h>

#include <boost/intrusive_ptr.hpp>

#include "mon/Monitor.h"
#include "mon/QuorumService.h"
#include "mon/HealthService.h"
#include "mon/HealthMonitor.h"
#include "mon/DataHealthService.h"

#include "messages/MMonHealth.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon,
			const HealthMonitor *hmon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ")." << hmon->get_name()
		<< "(" << hmon->get_epoch() << ") ";
}

void HealthMonitor::init()
{
  ldout(mon->cct, 10) << __func__ << dendl;
  assert(services.empty());
  services[HealthService::SERVICE_HEALTH_DATA] = new DataHealthService(mon);

  for (map<int,HealthService*>::iterator it = services.begin();
       it != services.end();
       ++it) {
    it->second->init();
  }
}

bool HealthMonitor::service_dispatch(Message *m, unique_lock& l)
{
  assert(m->get_type() == MSG_MON_HEALTH);
  MMonHealth *hm = (MMonHealth*)m;
  int service_type = hm->get_service_type();
  if (services.count(service_type) == 0) {
    ldout(mon->cct, 1) << __func__ << " service type " << service_type
	    << " not registered -- drop message!" << dendl;
    m->put();
    return false;
  }
  return services[service_type]->service_dispatch(hm, l);
}

void HealthMonitor::service_shutdown()
{
  ldout(mon->cct, 0) << "HealthMonitor::service_shutdown "
		     << services.size() << " services" << dendl;
  for (map<int,HealthService*>::iterator it = services.begin();
      it != services.end();
       ++it) {
    it->second->shutdown();
    delete it->second;
  }
  services.clear();
}

health_status_t HealthMonitor::get_health(Formatter *f,
					  list<pair<health_status_t,string> > *detail)
{
  health_status_t overall = HEALTH_OK;
  if (f) {
    f->open_object_section("health");
    f->open_array_section("health_services");
  }

  for (map<int,HealthService*>::iterator it = services.begin();
       it != services.end();
       ++it) {
    health_status_t h = it->second->get_health(f, detail);
    if (overall > h)
      overall = h;
  }

  if (f) {
    f->close_section(); // health_services
    f->close_section(); // health
  }

  return overall;
}

