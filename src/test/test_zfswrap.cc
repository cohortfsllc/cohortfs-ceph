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
  bool destroy_vdev1 = true;

  bf::path vdevs("/opt/zpools");
  bf::path vdev1(vdevs);
  lzfw_handle_t* zhd;
  

} /* namespace */

TEST(ZFSWRAP, INIT)
{
  zhd = lzfw_init();
  ASSERT_NE(zhd, nullptr);
}

TEST(ZFSWRAP, ZDEV1)
{
  // create file-based vdev backing file
  if (!create_vdev1)
    return;

  ASSERT_EQ(is_directory(vdevs), true);  
  vdev1 /= "zd1";
  
  std::string cmd = "dd if=/dev/zero of=";
  cmd += vdev1.c_str();
  cmd += " bs=1M count=500";

  std::cout << cmd.c_str() << std::endl;

  system(cmd.c_str());
}

TEST(ZFSWRAP, ZPOOL1)
{
  // create a default zpool on vdev1
  int err;
  const char* v[1]; // an array of devices
  const char* lzw_err;

  v[0] = vdev1.c_str();
  err = lzfw_zpool_create(zhd, "zp1", "" /* type */, v, 1,
			  &lzw_err);
  ASSERT_EQ(err, 0);
}

TEST(ZFSWRAP, ZPLIST)
{
  int err;
  const char* lzw_err;

  err = lzfw_zpool_list(zhd, NULL, &lzw_err);
  ASSERT_EQ(err, 0);
}

TEST(ZFSWRAP, ZFS1)
{
  // create a filesystem on zp1
  int err;
  const char* lzw_err;
  const int ZFS_TYPE_FILESYSTEM = 0x1;

  // XXX spa code edits strings in-place--can't pass const!
  char* fs = strdup("zp1/zf1");
  err = lzfw_dataset_create(zhd, fs, ZFS_TYPE_FILESYSTEM,
			    &lzw_err);
  ASSERT_EQ(err, 0);
  free(fs);
}

TEST(ZFSWRAP, ZFSDESTROY1)
{
  int err;
  const char* lzw_err;

  char* fs = strdup("zp1/zf1");
  err = lzfw_dataset_destroy(zhd, fs, &lzw_err);
  ASSERT_EQ(err, 0);
  free(fs);
}

/* TODO: finish */

TEST(ZFSWRAP, ZPDESTROY1)
{
  if (!destroy_vdev1)
    return;

  int err;
  const char* lzw_err;

  err = lzfw_zpool_destroy(zhd, "zp1", true /* force */, &lzw_err);
  ASSERT_EQ(err, 0);
}

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
