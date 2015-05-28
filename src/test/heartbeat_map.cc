// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/HeartbeatMap.h"
#include "common/ceph_context.h"
#include "test/unit.h"
#include "common/config.h"

using namespace ceph;

CephContext *cct;

TEST(HeartbeatMap, Healthy) {
  HeartbeatMap hm(cct);
  heartbeat_handle_d *h = hm.add_worker("one");

  hm.reset_timeout(h, 9s, 18s);
  bool healthy = hm.is_healthy();
  ASSERT_EQ(healthy, true);

  hm.remove_worker(h);
}

TEST(HeartbeatMap, Unhealth) {
  HeartbeatMap hm(cct);
  heartbeat_handle_d *h = hm.add_worker("one");

  hm.reset_timeout(h, 1s, 3s);
  sleep(2);
  bool healthy = hm.is_healthy();
  ASSERT_EQ(healthy, false);

  hm.remove_worker(h);
}
