// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <sys/types.h>
#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/freelist.h"
#include "global/global_init.h"

extern "C" {
#include <libzfswrap.h>
}

namespace {

  std::vector<void*> v{100};
  cohort::HugePageQ hpq(100, 1024);

} /* namespace */

TEST(HUGEPQ, INIT)
{
  ASSERT_TRUE(hpq.valid() && hpq.is_huge());
}

TEST(HUGEPQ, ALLOC)
{
  for (int ix = 0; ix < 100; ++ix) {
    void* addr = hpq.alloc();
    ASSERT_TRUE(!!addr);
    v.push_back(addr);
  }
}

TEST(HUGEPQ, FREE)
{
  for (auto& it : v) {
    hpq.free(it);
  }
  v.clear();
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  CephContext *cct =
    global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		CODE_ENVIRONMENT_UTILITY, 0);

  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
