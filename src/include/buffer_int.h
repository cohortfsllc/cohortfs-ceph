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
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_BUFFER_INT_H
#define CEPH_BUFFER_INT_H

#if defined(__linux__) || defined(__FreeBSD__)
#include <stdlib.h>
#endif

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 600
#endif

#include <stdio.h>

#if defined(__linux__)	// For malloc(2).
#include <malloc.h>
#endif

#include <inttypes.h>
#include <stdint.h>
#include <string.h>

#ifndef __CYGWIN__
# include <sys/mman.h>
#endif

#include <iostream>
#include <istream>
#include <iomanip>
#include <list>
#include <string>
#include <exception>

#include "common/errno.h"
#include "common/likely.h"
#include "page.h"
#include "crc32c.h"

#if defined(HAVE_XIO)
extern "C" {
#include "libxio.h"
}

class XioCompletionHook;

#endif /* HAVE_XIO */

namespace ceph {

  namespace buffer {

    /*
     * exceptions
     */
    struct error : public std::exception{
      const char *what() const throw () {
	return "buffer::exception";
      }
    };

    struct bad_alloc : public error {
      const char *what() const throw () {
	return "buffer::bad_alloc";
      }
    };

    struct end_of_buffer : public error {
      const char *what() const throw () {
	return "buffer::end_of_buffer";
      }
    };

    struct malformed_input : public error {
      explicit malformed_input(const char *w) {
	snprintf(buf, sizeof(buf), "buffer::malformed_input: %s", w);
      }
      const char *what() const throw () {
	return buf;
      }
    private:
      char buf[256];
    };

    struct error_code : public malformed_input {
      error_code(int error) :
	malformed_input(cpp_strerror(error).c_str()), code(error) {}
      int code;
    };

    /// total bytes allocated
    int get_total_alloc();

    /// enable/disable alloc tracking
    void track_alloc(bool b);

    /// count of cached crc hits (matching input)
    int get_cached_crc();
    /// count of cached crc hits (mismatching input, required adjustment)
    int get_cached_crc_adjusted();
    /// enable/disable tracking of cached crcs
    void track_cached_crc(bool b);

    /// count of calls to buffer::ptr::c_str()
    int get_c_str_accesses();
    /// enable/disable tracking of buffer::ptr::c_str() calls
    void track_c_str(bool b);

    /* hack for memory utilization debugging. */
    void inc_total_alloc(unsigned len);
    void dec_total_alloc(unsigned len);

    /*
     * an abstract raw buffer.  with a reference count.
     */
    class raw;
    class raw_crc;
    class raw_malloc;
    class raw_static;
    class raw_mmap_pages;
    class raw_posix_aligned;
    class raw_hack_aligned;
    class raw_char;
    class raw_pipe;
    class xio_mempool;
    class xio_msg_buffer;

    /*
     * named constructors
     */
    raw* copy(const char *c, unsigned len);
    raw* create(unsigned len);
    raw* claim_char(unsigned len, char *buf);
    raw* create_malloc(unsigned len);
    raw* claim_malloc(unsigned len, char *buf);
    raw* create_static(unsigned len, char *buf);
    raw* create_page_aligned(unsigned len);
    raw* create_zero_copy(unsigned len, int fd, int64_t *offset);

#if defined(HAVE_XIO)
    raw* create_msg(unsigned len, char *buf, XioCompletionHook *m_hook);
#endif

    /*
     * nested utility classes
     */
    class ptr;
    class list;
    class hash;

    inline std::ostream& operator<<(std::ostream& out, buffer::error& e) {
      return out << e.what();
    }

  } /* namespace buffer */

  typedef buffer::ptr bufferptr;
  typedef buffer::list bufferlist;
  typedef buffer::hash bufferhash;

} /* namespace ceph */

/* allow these to be used anywhere */
using ceph::bufferptr;
using ceph:: bufferlist;
using ceph:: bufferhash;

#endif /* CEPH_BUFFER_INT_H */
