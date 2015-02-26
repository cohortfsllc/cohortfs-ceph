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
#include "include/stringify.h"
#include "st_rados_create_volume.h"
#include "st_rados_list_objects.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>
#include <map>
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

#if 0

using std::ostringstream;
using std::string;
using std::vector;

static int g_num_objects = 50;

static CrossProcessSem *volume_setup_sem = NULL;
static CrossProcessSem *modify_sem = NULL;

class RadosDeleteObjectsR : public SysTestRunnable
{
public:
  RadosDeleteObjectsR(int argc, const char **argv,
		      const std::string &volume_name)
    : SysTestRunnable(argc, argv), m_volume_name(volume_name)
  {
  }

  ~RadosDeleteObjectsR()
  {
  }

  int run(void)
  {
    rados_t cl;
    RETURN1_IF_NONZERO(rados_create(&cl, NULL));
    rados_conf_parse_argv(cl, m_argc, m_argv);
    RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
    rados_conf_parse_env(cl, NULL);
    std::string log_name = SysTestSettings::inst().get_log_name(get_id_str());
    if (!log_name.empty())
      rados_conf_set(cl, "log_file", log_name.c_str());
    RETURN1_IF_NONZERO(rados_connect(cl));
    volume_setup_sem->wait();
    volume_setup_sem->post();

    rados_ioctx_t io_ctx;
    rados_volume_create(cl, m_volume_name.c_str());
    RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_volume_name.c_str(), &io_ctx));

    std::map <int, std::string> to_delete;
    for (int i = 0; i < g_num_objects; ++i) {
      char oid_t[128];
      snprintf(oid_t, sizeof(oid_t), "%d.oid", i);
      to_delete[i] = oid_t;
    }

    int removed = 0;
    while (true) {
      if (to_delete.empty())
	break;
      int r = rand() % to_delete.size();
      std::map <int, std::string>::iterator d = to_delete.begin();
      for (int i = 0; i < r; ++i)
	++d;
      if (d == to_delete.end()) {
	return -EDOM;
      }
      std::string oid_t(d->second);
      to_delete.erase(d);
      int ret = rados_remove(io_ctx, oid_t.c_str());
      if (ret != 0) {
	printf("%s: rados_remove(%s) failed with error %d\n",
	       get_id_str(), oid_t.c_str(), ret);
	return ret;
      }
      ++removed;
      if ((removed % 25) == 0) {
	printf("%s: removed %d objects...\n", get_id_str(), removed);
      }
      if (removed == g_num_objects / 2) {
	printf("%s: removed half of the objects\n", get_id_str());
	modify_sem->post();
      }
    }

    printf("%s: removed %d objects\n", get_id_str(), removed);

    rados_ioctx_destroy(io_ctx);
    rados_shutdown(cl);

    return 0;
  }
private:
  std::string m_volume_name;
};

class RadosAddObjectsR : public SysTestRunnable
{
public:
  RadosAddObjectsR(int argc, const char **argv,
		   const std::string &volume_name,
		   const std::string &suffix)
    : SysTestRunnable(argc, argv),
      m_volume_name(volume_name),
      m_suffix(suffix)
  {
  }

  ~RadosAddObjectsR()
  {
  }

  int run(void)
  {
    rados_t cl;
    RETURN1_IF_NONZERO(rados_create(&cl, NULL));
    rados_conf_parse_argv(cl, m_argc, m_argv);
    RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
    rados_conf_parse_env(cl, NULL);
    std::string log_name = SysTestSettings::inst().get_log_name(get_id_str());
    if (!log_name.empty())
      rados_conf_set(cl, "log_file", log_name.c_str());
    RETURN1_IF_NONZERO(rados_connect(cl));
    volume_setup_sem->wait();
    volume_setup_sem->post();

    rados_ioctx_t io_ctx;
    rados_volume_create(cl, m_volume_name.c_str());
    RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_volume_name.c_str(), &io_ctx));

    std::map <int, std::string> to_add;
    for (int i = 0; i < g_num_objects; ++i) {
      char oid_t[128];
      snprintf(oid_t, sizeof(oid_t), "%d%s", i, m_suffix.c_str());
      to_add[i] = oid_t;
    }

    int added = 0;
    while (true) {
      if (to_add.empty())
	break;
      int r = rand() % to_add.size();
      std::map <int, std::string>::iterator d = to_add.begin();
      for (int i = 0; i < r; ++i)
	++d;
      if (d == to_add.end()) {
	return -EDOM;
      }
      std::string oid_t(d->second);
      to_add.erase(d);

      std::string buf(StRadosCreateVolume::get_random_buf(256));
      int ret = rados_write(io_ctx, oid_t.c_str(), buf.c_str(), buf.size(), 0);
      if (ret != 0) {
	printf("%s: rados_write(%s) failed with error %d\n",
	       get_id_str(), oid_t.c_str(), ret);
	return ret;
      }
      ++added;
      if ((added % 25) == 0) {
	printf("%s: added %d objects...\n", get_id_str(), added);
      }
      if (added == g_num_objects / 2) {
	printf("%s: added half of the objects\n", get_id_str());
	modify_sem->post();
      }
    }

    printf("%s: added %d objects\n", get_id_str(), added);

    rados_ioctx_destroy(io_ctx);
    rados_shutdown(cl);

    return 0;
  }
private:
  std::string m_volume_name;
  std::string m_suffix;
};

const char *get_id_str()
{
  return "main";
}

#endif

int main(int argc, const char **argv)
{
#if 0
  const char *num_objects = getenv("NUM_OBJECTS");
  std::string volume = "foo." + stringify(getpid());
  if (num_objects) {
    g_num_objects = atoi(num_objects);
    if (g_num_objects == 0)
      return 100;
  }

  RETURN1_IF_NONZERO(CrossProcessSem::create(0, &volume_setup_sem));
  RETURN1_IF_NONZERO(CrossProcessSem::create(1, &modify_sem));

  std::string error;

  // Test 1... list objects
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, g_num_objects, ".oid");
    StRadosListObjects r2(argc, argv, volume, false, g_num_objects,
			  volume_setup_sem, modify_sem, NULL);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  // Test 2... list objects while they're being deleted
  RETURN1_IF_NONZERO(volume_setup_sem->reinit(0));
  RETURN1_IF_NONZERO(modify_sem->reinit(0));
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, g_num_objects, ".oid");
    StRadosListObjects r2(argc, argv, volume, false, g_num_objects / 2,
			  volume_setup_sem, modify_sem, NULL);
    RadosDeleteObjectsR r3(argc, argv, volume);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  // Test 3... list objects while others are being added
  RETURN1_IF_NONZERO(volume_setup_sem->reinit(0));
  RETURN1_IF_NONZERO(modify_sem->reinit(0));
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, g_num_objects, ".oid");
    StRadosListObjects r2(argc, argv, volume, false, g_num_objects / 2,
			  volume_setup_sem, modify_sem, NULL);
    RadosAddObjectsR r3(argc, argv, volume, ".obj2");
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  // Test 4... list objects while others are being added and deleted
  RETURN1_IF_NONZERO(volume_setup_sem->reinit(0));
  RETURN1_IF_NONZERO(modify_sem->reinit(0));
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, g_num_objects, ".oid");
    StRadosListObjects r2(argc, argv, volume, false, g_num_objects / 2,
			  volume_setup_sem, modify_sem, NULL);
    RadosAddObjectsR r3(argc, argv, volume, ".obj2");
    RadosAddObjectsR r4(argc, argv, volume, ".obj3");
    RadosDeleteObjectsR r5(argc, argv, volume);
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    vec.push_back(&r4);
    vec.push_back(&r5);
    error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  // Test 5... list objects while they are being modified
  RETURN1_IF_NONZERO(volume_setup_sem->reinit(0));
  RETURN1_IF_NONZERO(modify_sem->reinit(0));
  {
    StRadosCreateVolume r1(argc, argv, NULL, volume_setup_sem, NULL,
			 volume, g_num_objects, ".oid");
    StRadosListObjects r2(argc, argv, volume, false, g_num_objects / 2,
			  volume_setup_sem, modify_sem, NULL);
    // AddObjects with the same 'suffix' as used in StRadosCreateVolume
    RadosAddObjectsR r3(argc, argv, volume, ".oid");
    vector < SysTestRunnable* > vec;
    vec.push_back(&r1);
    vec.push_back(&r2);
    vec.push_back(&r3);
    error = SysTestRunnable::run_until_finished(vec);
    if (!error.empty()) {
      printf("got error: %s\n", error.c_str());
      return EXIT_FAILURE;
    }
  }

  rados_t cl;
  rados_create(&cl, NULL);
  rados_conf_parse_argv(cl, argc, argv);
  rados_conf_parse_argv(cl, argc, argv);
  rados_conf_read_file(cl, NULL);
  rados_conf_parse_env(cl, NULL);
  rados_connect(cl);
  rados_volume_delete(cl, volume.c_str());

  printf("******* SUCCESS **********\n");
#endif
  return EXIT_SUCCESS;
}

