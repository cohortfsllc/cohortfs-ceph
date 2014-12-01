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

#ifndef TEST_SYSTEM_ST_RADOS_CREATE_VOLUME_H
#define TEST_SYSTEM_ST_RADOS_CREATE_VOLUME_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_create_volume
 *
 * Waits, then posts to setup_sem.
 * Creates a volume and populates it with some objects.
 * Then, calls volume_setup_sem->post()
 */
class StRadosCreateVolume : public SysTestRunnable
{
public:
  static std::string get_random_buf(int sz);
  StRadosCreateVolume(int argc, const char **argv,
		    CrossProcessSem *setup_sem,
		    CrossProcessSem *volume_setup_sem,
		    CrossProcessSem *close_create_volume_sem,
		    const std::string &volume_name,
		    int num_objects,
		    const std::string &suffix);
  ~StRadosCreateVolume();
  virtual int run();
private:
  CrossProcessSem *m_setup_sem;
  CrossProcessSem *m_volume_setup_sem;
  CrossProcessSem *m_close_create_volume;
  std::string m_volume_name;
  int m_num_objects;
  std::string m_suffix;
};

#endif
