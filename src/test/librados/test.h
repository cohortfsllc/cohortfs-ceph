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

#ifndef CEPH_TEST_RADOS_API_TEST_H
#define CEPH_TEST_RADOS_API_TEST_H


#include <string>
#include <unistd.h>

std::string get_temp_volume_name();

std::string create_one_volume(const std::string &volume_name, rados_t *cluster);
std::string create_one_volume_pp(const std::string &volume_name,
			    librados::Rados &cluster);
std::string connect_cluster(rados_t *cluster);
std::string connect_cluster_pp(librados::Rados &cluster);
int destroy_one_volume(const std::string &volume_name, rados_t *cluster);
int destroy_one_volume_pp(const std::string &volume_name, librados::Rados &cluster);

class TestAlarm
{
public:
  TestAlarm() {
    alarm(360);
  }
  ~TestAlarm() {
    alarm(0);
  }
};

#endif
