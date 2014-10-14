// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BUFFER_XIO_H
#define CEPH_BUFFER_XIO_H

#if defined(HAVE_XIO)
#include <atomic>
#include "msg/XioPool.h"

extern struct xio_mempool *xio_msgr_reg_mpool;

#include "buffer_ptr.h" // includes buffer_raw.h

namespace ceph {

  /* re-open buffer namespace */
  namespace buffer {
    // pool-allocated raw objects must release their memory in operator delete
    inline void raw::operator delete(void *p) {
      raw *r = static_cast<raw*>(p);
      switch (r->get_type()) {
      case type_xio:
      case type_xio_reg:
	xio_mempool_free(&r->xio.mp);
	break;
      default:
	break;
      }
    }

    inline struct xio_mempool_obj* raw::get_xio_mp() const {
      switch(get_type()) {
      case raw::type_xio:
      case raw::type_xio_reg:
	return const_cast<xio_mempool_obj*>(&(xio.mp));
	break;
      default:
	break;
      }
      return NULL;
    }

    inline raw* raw::create_reg(struct xio_iovec_ex *iov) {
      struct xio_mempool_obj mp;
      xpool_alloc(xio_msgr_reg_mpool, iov->iov_len+sizeof(raw), &mp);
      if (! mp.addr)
	abort();
      iov->iov_base = mp.addr;
      iov->mr = mp.mr;
      // placement construct it
      buffer::raw* r =
	reinterpret_cast<buffer::raw*>((char*) mp.addr + iov->iov_len);
      new (r) raw(type_xio_reg, iov->iov_len, static_cast<char*>(mp.addr));
      r->xio.mp = mp;
      return r;
    }
  } /* namespace buffer */
} /* namespace ceph */

#endif /* HAVE_XIO */
#endif /* CEPH_BUFFER_XIO_H */
