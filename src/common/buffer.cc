// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "armor.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/simple_spin.h"
#include "common/strtol.h"
#include "common/Mutex.h"
#include "include/types.h"
#include "include/compat.h"
#include "common/likely.h"
#if defined(HAVE_XIO)
#include "msg/XioMsg.h"
#endif

using std::cerr;

namespace ceph {

  /* re-open buffer namespace */
  namespace buffer {


    std::atomic<uint32_t>  buffer_max_pipe_size;
    int update_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
      char buf[32];
      int r;
      std::string err;
      struct stat stat_result;
      if (::stat("/proc/sys/fs/pipe-max-size", &stat_result) == -1)
	return -errno;
      r = safe_read_file("/proc/sys/fs/", "pipe-max-size",
			 buf, sizeof(buf) - 1);
      if (r < 0)
	return r;
      buf[r] = '\0';
      size_t size = strict_strtol(buf, 10, &err);
      if (!err.empty())
	return -EIO;
      buffer_max_pipe_size.set(size);
#endif
      return 0;
    }

    size_t get_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
      size_t size = buffer_max_pipe_size.read();
      if (size)
	return size;
      if (update_max_pipe_size() == 0)
	return buffer_max_pipe_size.read();
#endif
      // this is the max size hardcoded in linux before 2.6.35
      return 65536;
    }

  } /* namespace buffer */
} /* namespace ceph */

