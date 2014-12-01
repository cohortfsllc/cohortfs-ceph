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

#ifndef TEST_SYSTEM_ST_RADOS_DELETE_VOLUME_H
#define TEST_SYSTEM_ST_RADOS_DELETE_VOLUME_H

#include "systest_runnable.h"

class CrossProcessSem;

/*
 * st_rados_delete_volume
 *
 * Waits on volume_setup_sem, posts to it,
 * deletes a volume, and posts to delete_volume_sem.
 */
class StRadosDeleteVolume : public SysTestRunnable
{
public:
  StRadosDeleteVolume(int argc, const char **argv,
		    CrossProcessSem *volume_setup_sem,
		    CrossProcessSem *delete_volume_sem,
		    const std::string &volume_name);
  ~StRadosDeleteVolume();
  virtual int run();
private:
  CrossProcessSem *m_volume_setup_sem;
  CrossProcessSem *m_delete_volume_sem;
  std::string m_volume_name;
};

#endif
