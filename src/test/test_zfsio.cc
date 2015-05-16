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
#include <sys/uio.h>
#include <iostream>
#include <vector>
#include <random>
#include "xxHash-r39/xxhash.h"
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

  bool create = false; // will pre-destroy
  bool destroy = false;

  bf::path vdevs("/opt/zpools");
  bf::path vdev1(vdevs);

  lzfw_handle_t* zhd; /* zfswrap handle */
  lzfw_vfs_t* zhfs; /* dataset handle */
  lzfw_vnode_t* root_vnode = nullptr;
  inogen_t root_ino = {0, 0};
  creden_t cred = {0, 0};

  struct ZFSObject
  {
    std::string leaf_name;
    inogen_t ino;
    lzfw_vnode_t* vnode;

  ZFSObject(std::string n) : leaf_name(std::move(n)), ino{0, 0},
      vnode(nullptr)
    {}
  };

  std::vector<ZFSObject> zfs1_objs;

  std::uniform_int_distribution<uint8_t> uint_dist;
  std::mt19937 rng;

  struct ZPage
  {
    char data[65536];
    uint64_t cksum;
  }; /* ZPage */
  
  struct ZPageSet
  {
    std::vector<ZPage*> pages;
    struct iovec* iovs;

    ZPageSet(int n) {
      pages.reserve(n);
      iovs = (struct iovec*) calloc(n, sizeof(struct iovec));
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = new ZPage();
	for (int data_ix = 0; data_ix < 65536; ++data_ix) {
	  p->data[data_ix] = uint_dist(rng);
	} // data_ix
	p->cksum = XXH64(p->data, 65536, 8675309);
	pages[page_ix] = p;
	// and iovs
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = 65536;
      } // page_ix
    }

    ~ZPageSet() {
      for (int ix = 0; ix < pages.size(); ++ix)
	delete pages[ix];
      free(iovs);
    }
  }; /* ZPageSet */

} /* namespace */

TEST(ZFSIO, INIT)
{
  int err;
  const char* lzw_err;

  zhd = lzfw_init();
  ASSERT_NE(zhd, nullptr);

  vdev1 /= "zd2";

  if (create || !is_regular_file(vdev1)) {
    { // "pre-destroy" accounting info
      (void) lzfw_zpool_destroy(zhd, "zp2", true /* force */, &lzw_err);
      remove(vdev1);
    }
    { // create backing
      std::string cmd = "dd if=/dev/zero of=";
      cmd += vdev1.c_str();
      cmd += " bs=1M count=500";
      std::cout << cmd.c_str() << std::endl;
      system(cmd.c_str());
    }
    { // create pool
      const char* v[1]; // an array of devices
      v[0] = vdev1.c_str();
      err = lzfw_zpool_create(zhd, "zp2", "" /* type */, v, 1,
			      &lzw_err);
      ASSERT_EQ(err, 0);
    }
    { // create dataset
      const int ZFS_TYPE_FILESYSTEM = 0x1;
      // XXX spa code edits strings in-place--can't pass const!
      char* fs = strdup("zp2/zf2");
      err = lzfw_dataset_create(zhd, fs, ZFS_TYPE_FILESYSTEM,
				&lzw_err);
      ASSERT_EQ(err, 0);
      free(fs);
    }

    // seed rng
    rng.seed(1337);
  } /* create */

  // mount fs
  zhfs = lzfw_mount("zp2", "/zf2", "" /* XXX "mount options" */);
  ASSERT_NE(zhfs, nullptr);

  // get root ino
  err = lzfw_getroot(zhfs, &root_ino);
  ASSERT_EQ(err, 0);

  // open root vnode
  err = lzfw_opendir(zhfs, &cred, root_ino, &root_vnode);
  ASSERT_EQ(err, 0);
  ASSERT_NE(root_vnode, nullptr);
}

/* TODO: finish */
TEST(ZFSIO, IOV1)
{
  ZPageSet zp_set(16); // 1M random data in 16 64K pages
}

TEST(ZFSIO, SHUTDOWN)
{
  int err;
  const char* lzw_err;

  // close root vnode
  err = lzfw_closedir(zhfs, &cred, root_vnode);
  root_vnode = nullptr;
  ASSERT_EQ(err, 0);

  // release fs
  err = lzfw_umount(zhfs, true /* force */);
  zhfs = nullptr;
  ASSERT_EQ(err, 0);

  // cond destroy everything
  if (destroy) {
    { // destroy fs
      char* fs = strdup("zp2/zf2");
      err = lzfw_dataset_destroy(zhd, fs, &lzw_err);
      ASSERT_EQ(err, 0);
      free(fs);
    }
    { // destroy pool
      err = lzfw_zpool_destroy(zhd, "zp1", true /* force */, &lzw_err);
      ASSERT_EQ(err, 0);
      remove(vdev1);
    }
  }

  // release library
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

  string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i))
      break;
    if (ceph_argparse_flag(args, &i, "--create", (char*)NULL)) {
      create = true;
      continue;
    }
    if (ceph_argparse_flag(args, &i, "--destroy", (char*)NULL)) {
      destroy = true;
      continue;
    }
  } /* while(args) */
  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
