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

/*
 * Test Ioctx::operate
 */

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/config.h"
#include "global/global_init.h"
#include "include/rados/librados.hpp"
#include "include/types.h"

#include <errno.h>
#include <iostream>
#include <string>

using std::cerr;
using std::string;

static CephContext* cct;

using namespace librados;

static void usage(void)
{
  cerr << "--oid_t	   set object id to 'operate' on" << std::endl;
  cerr << "--volume	   set volume to 'operate' on" << std::endl;
}

int main(int argc, const char **argv)
{
  int ret = 0;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  string val;
  string oid_t("ceph_test_object");
  string volume_name("test_volume");
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--oid_t", "-o", (char*)NULL)) {
      oid_t = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--volume", "-p", (char*)NULL)) {
      volume_name = val;
    }
    else {
      cerr << "unknown command line option: " << *i << std::endl;
      cerr << std::endl;
      usage();
      return 2;
    }
  }

  Rados rados;
  if (rados.init_with_context(cct) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     return 1;
  }
  if (rados.conf_read_file(NULL) < 0) {
     cerr << "failed to read rados configuration file!" << std::endl;
     return 1;
  }
  if (rados.connect() < 0) {
     cerr << "couldn't connect to cluster!" << std::endl;
     return 1;
  }

  IoCtx ioctx;
  if (rados.lookup_volume(volume_name).is_nil()) {
    ret = rados.volume_create(volume_name.c_str());
    if (ret) {
       cerr << "failed to create volume named '" << volume_name
	    << "': error " << ret << std::endl;
       return 1;
    }
  }
  ret = rados.ioctx_create(volume_name.c_str(), ioctx);
  if (ret) {
     cerr << "failed to create ioctx for volume '" << volume_name
	  << "': error " << ret << std::endl;
     return 1;
  }
  librados::ObjectWriteOperation o(ioctx);
  librados::ObjectWriteOperation op(ioctx);
  op.create(true);
  ret = ioctx.operate(oid_t, &op);
  if (ret) {
     cerr << "ioctx.operate failed: ret = " << ret << std::endl;
     return 1;
  }

  return 0;
}
