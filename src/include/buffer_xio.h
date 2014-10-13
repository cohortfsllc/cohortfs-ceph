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

#include "buffer_raw.h"
#include "buffer_ptr.h" // includes buffer_raw.h

#include <atomic>
#include "msg/XioPool.h"

extern struct xio_mempool *xio_msgr_reg_mpool;

namespace ceph {

  /* re-open buffer namespace  */
  namespace buffer {

    class xio_mempool : public raw {
    public:
      struct xio_mempool_obj mp_this;
      xio_mempool(struct xio_mempool_obj& _mp, unsigned l) :
	raw(type_xio_reg, l, (char*)_mp.addr), mp_this(_mp)
	{}

      bool is_volatile() {
	/* data points to registered memory, which, though safe to hold, is a
	 * finite pool resource */
	return true;
      }

      static void operator delete(void *p) {
	xio_mempool *xm = static_cast<xio_mempool*>(p);
	xpool_free(xm->len + sizeof(xio_mempool), &xm->mp_this);
      }

      raw* clone_empty() {
	return new buffer::raw_char(len);
      }
    };

    static inline raw* ptr_to_raw(void *addr) {
      return reinterpret_cast<raw*>(addr);
    }

    static inline struct xio_mempool_obj* get_xio_mp(const buffer::ptr& bp) {
      buffer::xio_mempool *mb =
	dynamic_cast<buffer::xio_mempool*>(bp.get_raw());
      if (mb) {
	return &(mb->mp_this);
      }
      return NULL;
    }

    static inline unsigned int sizeof_reg() {
      return sizeof(xio_mempool);
    }

    static inline buffer::raw* create_reg(struct xio_iovec_ex *iov) {
      struct xio_mempool_obj mp;
      xpool_alloc(xio_msgr_reg_mpool, iov->iov_len+sizeof(xio_mempool), &mp);
      if (! mp.addr)
	abort();
      iov->iov_base = mp.addr;
      iov->mr = mp.mr;
      // placement construct it
      buffer::raw* bp =
	reinterpret_cast<buffer::raw*>((char*) mp.addr + iov->iov_len);
      new (bp) xio_mempool(mp, iov->iov_len);
      return bp;
    }

  } /* namespace buffer */
} /* namespace ceph */

#endif /* HAVE_XIO */
#endif /* CEPH_BUFFER_XIO_H */
