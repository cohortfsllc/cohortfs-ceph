// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "mdstypes.h"

#include "MDBalancer.h"
#include "MDS.h"

#include "msg/Messenger.h"

#include <fstream>
#include <iostream>

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".bal "


/* This function DOES put the passed message before returning */
int MDBalancer::proc_message(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_HEARTBEAT:
    m->put();
    break;

  default:
    dout(1) << " balancer unknown message " << m->get_type() << dendl;
    assert(0);
    m->put();
    break;
  }

  return 0;
}


double mds_load_t::mds_load()
{
  switch(g_conf->mds_bal_mode) {
  case 0:
    return
      .8 * auth.meta_load() +
      .2 * all.meta_load() +
      req_rate +
      10.0 * queue_len;

  case 1:
    return req_rate + 10.0*queue_len;

  case 2:
    return cpu_load_avg;

  }
  assert(0);
  return 0;
}

mds_load_t MDBalancer::get_load(utime_t now)
{
  mds_load_t load(now);

  load.req_rate = mds->get_req_rate();
  load.queue_len = mds->messenger->get_dispatch_queue_len();

  ifstream cpu("/proc/loadavg");
  if (cpu.is_open())
    cpu >> load.cpu_load_avg;

  dout(15) << "get_load " << load << dendl;
  return load;
}

