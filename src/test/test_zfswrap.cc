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

#include <iostream>
#include "gtest/gtest.h"
#include <boost/filesystem.hpp>
#include <boost/filesystem/exception.hpp>
#include <boost/format.hpp>
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

extern "C" {
#include <libzfswrap.h>
}

namespace bf = boost::filesystem;

namespace {

  bool create_vdev1 = true;
  bf::path vdevs("/opt/zpools");
  lzfw_handle_t* zhd;
  

} /* namespace */

TEST(ZFSWRAP, INIT)
{
  zhd = lzfw_init();
  ASSERT_NE(zhd, nullptr);
}

TEST(ZFSWRAP, ZDEV1)
{
  // create file-based vdev and pool
  if (!create_vdev1)
    return;

  ASSERT_EQ(is_directory(vdevs), true);  
  bf::path vdev_path(vdevs);
  vdev_path /= "zd1";
  
  std::string cmd = "dd if=/dev/zero of=";
  cmd += vdev_path.c_str();
  cmd += " bs=1M count=500";

  std::cout << cmd.c_str() << std::endl;

  system(cmd.c_str());
}

/* TODO: finish */

TEST(ZFSWRAP, SHUTDOWN)
{
  // XXX unmount everything mounted
  lzfw_exit(zhd);
  zhd = nullptr;
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
