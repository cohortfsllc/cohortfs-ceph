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
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_UNIT_TEST_H
#define CEPH_UNIT_TEST_H

#include "include/types.h" // FIXME: ordering shouldn't be important, but right
			   // now, this include has to come before the others.

#include "common/code_environment.h"
#include "global/global_init.h"
#include "include/msgr.h" // for CEPH_ENTITY_TYPE_CLIENT
#include "gtest/gtest.h"

#include <vector>

/*
 * You only need to include this file if you are testing Ceph internal code. If
 * you are testing library code, the library init() interfaces will handle
 * initialization for you.
 */
int main(int argc, char **argv) {
  std::vector<const char*> args;
  CephContext *cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif
