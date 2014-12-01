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
#include "st_rados_delete_volume.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>

StRadosDeleteVolume::StRadosDeleteVolume(int argc, const char **argv,
				     CrossProcessSem *volume_setup_sem,
				     CrossProcessSem *delete_volume_sem,
				     const std::string &volume_name)
    : SysTestRunnable(argc, argv),
      m_volume_setup_sem(volume_setup_sem),
      m_delete_volume_sem(delete_volume_sem),
      m_volume_name(volume_name)
{
}

StRadosDeleteVolume::~StRadosDeleteVolume()
{
}

int StRadosDeleteVolume::run()
{
  rados_t cl;
  RETURN1_IF_NONZERO(rados_create(&cl, NULL));
  rados_conf_parse_argv(cl, m_argc, m_argv);
  RETURN1_IF_NONZERO(rados_conf_read_file(cl, NULL));
  rados_conf_parse_env(cl, NULL);
  RETURN1_IF_NONZERO(rados_connect(cl));
  m_volume_setup_sem->wait();
  m_volume_setup_sem->post();

  rados_ioctx_t io_ctx;
  rados_volume_create(cl, m_volume_name.c_str());
  RETURN1_IF_NONZERO(rados_ioctx_create(cl, m_volume_name.c_str(), &io_ctx));
  rados_ioctx_destroy(io_ctx);
  printf("%s: deleting volume %s\n", get_id_str(), m_volume_name.c_str());
  RETURN1_IF_NONZERO(rados_volume_delete(cl, m_volume_name.c_str()));
  if (m_delete_volume_sem)
    m_delete_volume_sem->post();
  rados_shutdown(cl);
  return 0;
}
