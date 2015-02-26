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

#include "cross_process_sem.h"
#include "include/rados/librados.h"
#include "st_rados_create_volume.h"
#include "st_rados_delete_volume.h"
#include "st_rados_list_objects.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <sstream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>

using std::ostringstream;
using std::string;
using std::vector;

static int g_num_objects = 50;

/*
 * rados_delete_volumes_parallel
 *
 * This tests creation and deletion races.
 *
 * EXPECT:	      * can delete a volume while another user is using it
 *		      * operations on volumes return error codes after the volumes
 *			are deleted
 *
 * DO NOT EXPECT      * hangs, crashes
 */

const char *get_id_str()
{
  return "main";
}

int main(int argc, const char **argv)
{
  const char *num_objects = getenv("NUM_OBJECTS");
  std::string volume = "foo";
  if (num_objects) {
    g_num_objects = atoi(num_objects);
    if (g_num_objects == 0)
      return 100;
  }

  CrossProcessSem *volume_setup_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &volume_setup_sem));
  CrossProcessSem *delete_volume_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &delete_volume_sem));

  // first test: create a volume, then delete that volume
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, 50, ".oid");
    StRadosDeleteVolume r2(argc, argv, volume_setup_sem, NULL, volume);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test1: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  // second test: create a volume, the list objects in that volume while it's
  // being deleted.
  RETURN1_IF_NONZERO(volume_setup_sem->reinit(0));
  RETURN1_IF_NONZERO(delete_volume_sem->reinit(0));
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, g_num_objects, ".oid");
    StRadosDeleteVolume r2(argc, argv, delete_volume_sem, NULL, volume);
    StRadosListObjects r3(argc, argv, volume, true, g_num_objects / 2,
			  volume_setup_sem, NULL, delete_volume_sem);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test2: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  printf("******* SUCCESS **********\n");
  return EXIT_SUCCESS;
}
