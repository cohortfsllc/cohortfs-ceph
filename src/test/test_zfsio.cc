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
#include <map>
#include <random>
#include "xxHash-r39/xxhash.h"
#include "gtest/gtest.h"
#include <boost/filesystem.hpp>
#include <boost/filesystem/exception.hpp>
#include <boost/format.hpp>
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "osd/osd_types.h"
#include "os/zfs/ZFSHelper.h"

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
  creden_t acred = {0, 0};

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
  std::vector<ZFSObject> zfs2_objs;

  static constexpr int n_objs_c2 = 10000;

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
	pages.emplace_back(p);
	// and iovs
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = 65536;
      } // page_ix
    }

    int size() { return pages.size(); }

    struct iovec* get_iovs() { return iovs; }

    bool operator==(const ZPageSet& rhs) {
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p1 = pages[page_ix];
	ZPage* p2 = rhs.pages[page_ix];
	if (p1->cksum != p2->cksum)
	  return false;
      }
      return true;
    }

    void cksum() {
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = pages[page_ix];
	p->cksum = XXH64(p->data, 65536, 8675309);
      }
    }

    void reset_iovs() { // VOP_READ and VOP_WRITE update
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = pages[page_ix];
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = 65536;
      }
    }

    ~ZPageSet() {
      for (int ix = 0; ix < pages.size(); ++ix)
	delete pages[ix];
      free(iovs);
    }
  }; /* ZPageSet */

  static int cb_zpool_f(zpool_handle_t* zhp, void* arg)
  {
    std::vector<std::string>& pools =
      *(static_cast<vector<std::string>*>(arg));
    /* ZFS handle structure is now visible! */
    pools.push_back(std::string(zhp->zpool_name));
    return 0;
  } /* cb_zpool_f */

  /* from ZFStore list_collections */
  static int cb_props_f(zfs_handle_t* zhp, void* arg)
  {
    std::pair<zprop_list_t*, vector<coll_t>&>& lc_args =
      *(static_cast<std::pair<zprop_list_t*, vector<coll_t>&>*>(arg));

    char property[ZFS_MAXPROPLEN];
    nvlist_t *userprops = zfs_get_user_props(zhp);
    nvlist_t *propval;
    char *propstr;

    zprop_list_t* zprop_list = lc_args.first;
    vector<coll_t>& vc = lc_args.second;

    for (; zprop_list != nullptr; zprop_list = zprop_list->pl_next) {
      if (zprop_list->pl_prop != ZPROP_INVAL) {
	if(zfs_prop_get(zhp,
			static_cast<zfs_prop_t>(zprop_list->pl_prop),
			property, sizeof (property), NULL, NULL, 0,
			B_FALSE) != 0)
	  propstr = const_cast<char*>("-");
	else
	  propstr = property;
	vc.push_back(coll_t(propstr)); /* XXXX */
      } else {
	if(nvlist_lookup_nvlist(userprops,
				zprop_list->pl_user_prop,
				&propval) != 0)
	  propstr = const_cast<char*>("-");
	else
	  assert(nvlist_lookup_string(propval,
				      ZPROP_VALUE, &propstr) == 0);
	vc.push_back(coll_t(propstr)); /* XXXX */
      }
    }
    return 0;
  } /* cb_props_f */

  /* do the same thing, but use the ZFS dataset handle */
  static int cb_dslist_f(zfs_handle_t* zhp, void* arg)
  {
    std::vector<coll_t>& vc = *(static_cast<vector<coll_t>*>(arg));
    /* ZFS handle structure is now visible! */
    vc.push_back(coll_t(zhp->zfs_name));
    return 0;
  } /* cb_dslist_f */

  ZFSObject c2_o(std::string("creates2"));

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

  // validate zpool
  std::vector<std::string> pools;
  err = libzfs_zpool_iter(zhd, cb_zpool_f, &pools, &lzw_err);
  ASSERT_TRUE(pools[0] == std::string("zp2"));

  // mount fs
  zhfs = lzfw_mount("zp2", "/zf2", "" /* XXX "mount options" */);
  ASSERT_NE(zhfs, nullptr);

  // get root ino
  err = lzfw_getroot(zhfs, &root_ino);
  ASSERT_EQ(err, 0);

  // open root vnode
  err = lzfw_opendir(zhfs, &acred, root_ino, &root_vnode);
  ASSERT_EQ(err, 0);
  ASSERT_NE(root_vnode, nullptr);
}

TEST(ZFSIO, CREATEF1)
{
  int err, ix;
  zfs1_objs.reserve(100);
  for (ix = 0; ix < 100; ++ix) {
    unsigned o_flags;
    std::string n{"f" + std::to_string(ix)};
    zfs1_objs.emplace_back(ZFSObject(n));
    ZFSObject& o = zfs1_objs[ix];
    /* create and open */
    err = lzfw_openat(zhfs, &acred, root_vnode, o.leaf_name.c_str(),
		      O_RDWR|O_CREAT, 644, &o_flags, &o.vnode);
    ASSERT_EQ(err, 0);
  }
}

TEST(ZFSIO, WRITEV1)
{
  ssize_t err, ix;
  const int iovcnt = 16;
  ZPageSet zp_set1{iovcnt}; // 1M random data in 16 64K pages
  struct iovec *iov1 = zp_set1.get_iovs();

  for (ix = 0; ix < 10; ++ix) {
    ZFSObject& o = zfs1_objs[ix];
    err = lzfw_pwritev(zhfs, &acred, o.vnode, iov1, iovcnt,
		       0 /* offset */);
    /* VOP_WRITE updates iov_len for all iovs touched */
    zp_set1.reset_iovs();
    ASSERT_EQ(err, iovcnt*65536);
  }

  for (ix = 0; ix < 10; ++ix) {
    ZFSObject& o = zfs1_objs[ix];
    ZPageSet zp_set2{iovcnt}; // 1M random data in 16 64K pages
    struct iovec *iov2 = zp_set2.get_iovs();

    /* VOP_READ requires (and updates) iov_len for all iovs */
    err = lzfw_preadv(zhfs, &acred, o.vnode, iov2, iovcnt,
		      0 /* offset */);
    ASSERT_EQ(err, iovcnt*65536);
    zp_set2.cksum();
    ASSERT_TRUE(zp_set1 == zp_set2);
  }
}

TEST(ZFSIO, CLOSE1)
{
  int err, ix;
  for (ix = 0; ix < 100; ++ix) {
    ZFSObject& o = zfs1_objs[ix];
    err = lzfw_close(zhfs, &acred, o.vnode, O_RDWR|O_CREAT);
    ASSERT_EQ(err, 0);
  }
}

TEST(ZFSIO, POOLDEV_SYNTAX)
{
  using namespace cohort_zfs;

  int sz;
  zp_desc_map zpm;

  // parse the default zpool device map rep
  std::string zfstore_zpool_devices = "default,zp_default,/opt/zpools/zp_default";
  sz = parse_zp_desc(zfstore_zpool_devices, zpm);
  print_zp_desc_map(zpm);

  // parse a map rep with multiple pool descriptions
  zfstore_zpool_devices =
    "default,zp_one,/opt/zpools/zp_one;default,zp_two,/opt/zpools/zp_two";
  sz = parse_zp_desc(zfstore_zpool_devices, zpm);
  print_zp_desc_map(zpm);
}

TEST(ZFSIO, LS_DATASETS)
{
  std::vector<coll_t> vc;
  const char* lzw_err;

  int r = lzfw_datasets_iter((libzfs_handle_t*) zhd, "zp2",
			     cb_dslist_f, &vc, &lzw_err);
  if (!!r)
    std::cout << "libzfs_zfs_iter failed: " << lzw_err << std::endl;
  else {
    /* enumerate */
    bool have_zp2 = false;
    for (auto& it : vc) {
      if (it == coll_t("zp2/zf2"))
	have_zp2 = true;
    }
    ASSERT_TRUE(have_zp2);
  }
}

TEST(ZFSIO, DATASET_PROPS)
{
  std::vector<coll_t> vc;
    int r;
  const char* lzw_err;
  zprop_list_t* zprop_list = nullptr;
  static char zprops[] = "name";
  r = zprop_get_list(static_cast<libzfs_handle_t*>(zhd),
		     zprops, &zprop_list,
		     static_cast<zfs_type_t>(ZFS_TYPE_DATASET));
  ASSERT_EQ(r, 0);

  std::pair<zprop_list_t*, std::vector<coll_t>&>
    lc_args{zprop_list, vc};

  r = lzfw_datasets_iter((libzfs_handle_t*) zhd, "zp2",
			 cb_props_f, &lc_args, &lzw_err);
  if (!!r)
    std::cout << "libzfs_zfs_iter failed: " << lzw_err << std::endl;
  else {
    /* enumerate */
    bool have_zp2 = false;
    for (auto& it : vc) {
      if (it == coll_t("zp2/zf2"))
	have_zp2 = true;
    }
    ASSERT_TRUE(have_zp2);
  }
}

TEST(ZFSIO, CREATE2)
{
  int err, ix;
  int cnt = n_objs_c2;

  if (!create)
    return;

  /* first, create a new container as a child of the root */
  err = lzfw_mkdirat(zhfs, &acred, root_vnode, c2_o.leaf_name.c_str(),
		     777 /* mode */, &c2_o.ino);
  ASSERT_EQ(err, 0);

  /* open it */
  err = lzfw_opendir(zhfs, &acred, c2_o.ino, &c2_o.vnode);
  ASSERT_EQ(err, 0);

  zfs2_objs.reserve(2*cnt);
  /* files */
  for (ix = 0; ix < cnt; ++ix) {
    std::string n{"f" + std::to_string(ix)};
    zfs1_objs.emplace_back(ZFSObject(n));
    ZFSObject& o = zfs1_objs[ix];
    /* create */
    err = lzfw_createat(zhfs, &acred, c2_o.vnode, o.leaf_name.c_str(), 644,
			&o.ino);
    ASSERT_EQ(err, 0);
  }
  /* directories */
  int dirmax = 2*cnt;
  for (ix = cnt; ix < dirmax; ++ix) {
    std::string n{"d" + std::to_string(ix)};
    zfs1_objs.emplace_back(ZFSObject(n));
    ZFSObject& o = zfs1_objs[ix];
    err = lzfw_mkdirat(zhfs, &acred, c2_o.vnode, o.leaf_name.c_str(),
		       777 /* mode */, &o.ino);
    ASSERT_EQ(err, 0);
  }
}

int c2_dir_iter(vnode_t *vno, dir_iter_cb_context_t *cb_ctx,
		void *arg)
{
  std::vector<string>& names = *(static_cast<std::vector<string>*>(arg));
  /* XXX do something */
  if (cb_ctx->dirent) {
    names.push_back(std::string(cb_ctx->dirent->d_name));
  }
  return 0;
}

TEST(ZFSIO, DIRITER1)
{
  std::vector<string> names;
  int r;

  if (!create) {
    /* open c2 container */
    unsigned int o_flags;
    /* XXX works for directories, but... */
    r = lzfw_openat(zhfs, &acred, root_vnode, c2_o.leaf_name.c_str(),
		    O_RDWR, 644, &o_flags, &c2_o.vnode);

    ASSERT_EQ(r, 0);
  }

  off_t cookie = 0;
  r = lzfw_dir_iter(zhfs, &acred, c2_o.vnode, c2_dir_iter, &names,
		    &cookie, LZFW_DI_FLAG_GETATTR);
  /* validate names */
  std::cout << "names size: " << names.size() << std::endl;
  ASSERT_EQ(r, 0);
}

TEST(ZFSIO, SHUTDOWN)
{
  int err;
  const char* lzw_err;

  // close root vnode
  err = lzfw_closedir(zhfs, &acred, root_vnode);
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
      err = lzfw_zpool_destroy(zhd, "zp2", true /* force */, &lzw_err);
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
