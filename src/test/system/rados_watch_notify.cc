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
#include "st_rados_delete_objs.h"
#include "st_rados_watch.h"
#include "st_rados_notify.h"
#include "systest_runnable.h"
#include "systest_settings.h"
#include "include/stringify.h"

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
#include <sys/types.h>
#include <unistd.h>

using std::ostringstream;
using std::string;
using std::vector;

/*
 * rados_watch_notify
 *
 * This tests watch/notify with volume and object deletion.
 *
 * EXPECT:	      * notifies to a deleted object or volume are not received
 *		      * notifies to existing objects are received
 *
 * DO NOT EXPECT      * hangs, crashes
 */

const char *get_id_str()
{
  return "main";
}

int main(int argc, const char **argv)
{
  std::string volume = "foo." + stringify(getpid());
  CrossProcessSem *setup_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &setup_sem));
  CrossProcessSem *watch_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &watch_sem));
  CrossProcessSem *notify_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &notify_sem));

  // create a volume and an object, watch the object, notify.
  {
    StRadosCreateVolume r1(argc, argv, NULL, setup_sem, NULL, volume, 1, ".obj");
    StRadosWatch r2(argc, argv, setup_sem, watch_sem, notify_sem,
		    1, 0, volume, "0.obj");
    StRadosNotify r3(argc, argv, setup_sem, watch_sem, notify_sem,
		     0, volume, "0.obj");
    vector<SysTestRunnable*> vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test1: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  RETURN1_IF_NONZERO(setup_sem->reinit(0));
  RETURN1_IF_NONZERO(watch_sem->reinit(0));
  RETURN1_IF_NONZERO(notify_sem->reinit(0));

  // create a volume and an object, watch a non-existent object,
  // notify non-existent object.watch
  volume += ".";
  {
    StRadosCreateVolume r1(argc, argv, NULL, setup_sem, NULL, volume, 0, ".obj");
    StRadosWatch r2(argc, argv, setup_sem, watch_sem, notify_sem,
		    0, -ENOENT, volume, "0.obj");
    StRadosNotify r3(argc, argv, setup_sem, watch_sem, notify_sem,
		     -ENOENT, volume, "0.obj");
    vector<SysTestRunnable*> vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test2: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  RETURN1_IF_NONZERO(setup_sem->reinit(0));
  RETURN1_IF_NONZERO(watch_sem->reinit(0));
  RETURN1_IF_NONZERO(notify_sem->reinit(0));

  CrossProcessSem *finished_notifies_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &finished_notifies_sem));
  CrossProcessSem *deleted_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &deleted_sem));
  CrossProcessSem *second_volume_sem = NULL;
  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &second_volume_sem));

  // create a volume and an object, watch the object, notify,
  // then delete the volume.
  // Create a new volume and write to it to make the osd get the updated map,
  // then try notifying on the deleted volume.
  volume += ".";
  {
    StRadosCreateVolume r1(argc, argv, NULL, setup_sem, NULL, volume, 1, ".obj");
    StRadosWatch r2(argc, argv, setup_sem, watch_sem, finished_notifies_sem,
		    1, 0, volume, "0.obj");
    StRadosNotify r3(argc, argv, setup_sem, watch_sem, notify_sem,
		     0, volume, "0.obj");
    StRadosDeleteVolume r4(argc, argv, notify_sem, deleted_sem, volume);
    StRadosCreateVolume r5(argc, argv, deleted_sem, second_volume_sem, NULL,
			 "bar", 1, ".obj");
    StRadosNotify r6(argc, argv, second_volume_sem, NULL, finished_notifies_sem,
		     0, "bar", "0.obj");
    StRadosDeleteVolume r7(argc, argv, finished_notifies_sem, NULL, "bar");
    vector<SysTestRunnable*> vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    vec.push_back(&r4);
    vec.push_back(&r5);
    vec.push_back(&r6);
    vec.push_back(&r7);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test3: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  RETURN1_IF_NONZERO(setup_sem->reinit(0));
  RETURN1_IF_NONZERO(watch_sem->reinit(0));
  RETURN1_IF_NONZERO(notify_sem->reinit(0));
  RETURN1_IF_NONZERO(finished_notifies_sem->reinit(0));
  RETURN1_IF_NONZERO(deleted_sem->reinit(0));

  // create a volume and an object, watch the object, notify,
  // then delete the object, notify
  if (false) {
    // this test is currently broken, pending the resolution of bug #2339
  volume += ".";
  {
    StRadosCreateVolume r1(argc, argv, NULL, setup_sem, NULL, volume, 1, ".obj");
    StRadosWatch r2(argc, argv, setup_sem, watch_sem, finished_notifies_sem,
		    1, 0, volume, "0.obj");
    StRadosNotify r3(argc, argv, setup_sem, watch_sem, notify_sem,
		     0, volume, "0.obj");
    StRadosDeleteObjs r4(argc, argv, notify_sem, deleted_sem, 1, volume, ".obj");
    StRadosNotify r5(argc, argv, setup_sem, deleted_sem, finished_notifies_sem,
		     -ENOENT, volume, "0.obj");

    vector<SysTestRunnable*> vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    vec.push_back(&r4);
    vec.push_back(&r5);
    std::string error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("test4: got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }
  }

  printf("******* SUCCESS **********\n");
  return EXIT_SUCCESS;
}
