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

#ifndef CEPH_BUFFER_RAW_H
#define CEPH_BUFFER_RAW_H

#include <map>
#include <atomic>

#include <errno.h>
#include <fstream>
#include <sstream>
#include <sys/uio.h>
#include <limits.h>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
}

#include "common/armor.h"
#include "common/environment.h"
#include "common/safe_io.h"
#include "common/simple_spin.h"
#include "common/strtol.h"
#include "common/Mutex.h"
#include "include/compat.h"

#include "buffer_int.h"

#ifdef HAVE_XIO
class XioCompletionHook;
#endif

namespace ceph {

  using std::pair;
  using std::map;

  /* re-open buffer namespace  */
  namespace buffer {

    class raw {
    public:
      char *data;
      unsigned len;
      std::atomic<uint32_t> nref;

      mutable Mutex crc_lock;
      map<pair<size_t, size_t>, pair<uint32_t, uint32_t> > crc_map;

      raw(unsigned l)
	: data(NULL), len(l), nref(0),
	  crc_lock("buffer::raw::crc_lock")
	{ }
      raw(char *c, unsigned l)
	: data(c), len(l), nref(0),
	  crc_lock("buffer::raw::crc_lock")
	{ }
      virtual ~raw() {};

      // no copying.
      raw(const raw &other);
      const raw& operator=(const raw &other);

      virtual char *get_data() {
	return data;
      }

      virtual raw* clone_empty() = 0;

      raw *clone() {
	raw *c = clone_empty();
	memcpy(c->data, data, len);
	return c;
      }

      virtual bool can_zero_copy() const {
	return false;
      }

      virtual int zero_copy_to_fd(int fd, loff_t *offset) {
	return -ENOTSUP;
      }

      virtual bool is_page_aligned() {
	return ((long)data & ~CEPH_PAGE_MASK) == 0;
      }

      bool is_n_page_sized() {
	return (len & ~CEPH_PAGE_MASK) == 0;
      }

      virtual bool is_volatile() {
      /* true if the raw memory may be unsafe (or preferable not to) hold
       * for long periods due to, e.g., special registration */
      return false;
     }

      virtual bool get_crc(const pair<size_t, size_t> &fromto,
			   pair<uint32_t, uint32_t> *crc) const {
	Mutex::Locker l(crc_lock);
	map<pair<size_t, size_t>, pair<uint32_t, uint32_t> >::const_iterator i
	  = crc_map.find(fromto);
	if (i == crc_map.end())
	  return false;
	*crc = i->second;
	return true;
      }

      virtual void set_crc(const pair<size_t, size_t> &fromto,
			   const pair<uint32_t, uint32_t> &crc) {
	Mutex::Locker l(crc_lock);
	crc_map[fromto] = crc;
      }

      virtual void invalidate_crc() {
	Mutex::Locker l(crc_lock);
	crc_map.clear();
      }

      static raw* create(unsigned len);
      static raw* claim_char(unsigned len, char *buf);
      static raw* create_malloc(unsigned len);
      static raw* claim_malloc(unsigned len, char *buf);
      static raw* create_static(unsigned len, char *buf);
      static raw* create_page_aligned(unsigned len);
      static raw* create_zero_copy(unsigned len, int fd, int64_t *offset);
#ifdef HAVE_XIO
      static raw* create_xio_msg(unsigned len, char *buf,
				 XioCompletionHook *hook);
#endif
    };

    class raw_malloc : public raw {
    public:
      raw_malloc(unsigned l) : raw(l) {
	if (len) {
	  data = (char *)malloc(len);
	  if (!data)
	    throw bad_alloc();
	} else {
	  data = 0;
	}
      }

      raw_malloc(unsigned l, char *b) : raw(b, l)
	{}

      ~raw_malloc() {
	free(data);
      }

      raw* clone_empty() {
	return new raw_malloc(len);
      }
    };

#ifndef __CYGWIN__
    class raw_mmap_pages : public raw {
    public:
      raw_mmap_pages(unsigned l) : raw(l) {
	data = (char*)::mmap(NULL, len, PROT_READ|PROT_WRITE,
			     MAP_PRIVATE|MAP_ANON, -1, 0);
	if (!data)
	  throw bad_alloc();
      }

      ~raw_mmap_pages() {
	::munmap(data, len);
      }

      raw* clone_empty() {
	return new raw_mmap_pages(len);
      }
    };

    class raw_posix_aligned : public raw {
    public:
      raw_posix_aligned(unsigned l) : raw(l) {
#ifdef DARWIN
	data = (char *) valloc (len);
#else
	data = 0;
	int r = ::posix_memalign((void**)(void*)&data, CEPH_PAGE_SIZE, len);
	if (r)
	  throw bad_alloc();
#endif /* DARWIN */
	if (!data)
	  throw bad_alloc();
      }

      ~raw_posix_aligned() {
	::free((void*)data);
      }

      raw* clone_empty() {
	return new raw_posix_aligned(len);
      }
    };
#endif

#ifdef __CYGWIN__
    class raw_hack_aligned : public raw {
      char *realdata;
    public:
      raw_hack_aligned(unsigned l) : raw(l) {
	realdata = new char[len+CEPH_PAGE_SIZE-1];
	unsigned off = ((unsigned)realdata) & ~CEPH_PAGE_MASK;
	if (off)
	  data = realdata + CEPH_PAGE_SIZE - off;
	else
	  data = realdata;
	//cout << "hack aligned " << (unsigned)data
	//<< " in raw " << (unsigned)realdata
	//<< " off " << off << std::endl;
	assert(((unsigned)data & (CEPH_PAGE_SIZE-1)) == 0);
      }
      ~raw_hack_aligned() {
	delete[] realdata;
      }
      raw* clone_empty() {
	return new raw_hack_aligned(len);
      }
    };
#endif

#ifdef CEPH_HAVE_SPLICE
    class raw_pipe : public raw {
    public:
      raw_pipe(unsigned len) : raw(len), source_consumed(false) {
	size_t max = get_max_pipe_size();
	if (len > max) {
	  throw malformed_input("length larger than max pipe size");
	}
	pipefds[0] = -1;
	pipefds[1] = -1;

	int r;
	if (::pipe(pipefds) == -1) {
	  r = -errno;
	  throw error_code(r);
	}

	r = set_nonblocking(pipefds);
	if (r < 0) {
	  throw error_code(r);
	}

	r = set_pipe_size(pipefds, len);
	if (r < 0) {
	  // continue, since the pipe should become large enough as needed
	}
      }

      ~raw_pipe() {
	if (data)
	  delete data;
	close_pipe(pipefds);
      }

      bool can_zero_copy() const {
	return true;
      }

      bool is_page_aligned() {
	return false;
      }

      int set_source(int fd, loff_t *off) {
	int flags = SPLICE_F_NONBLOCK;
	ssize_t r = safe_splice(fd, off, pipefds[1], NULL, len, flags);
	if (r < 0) {
	  return r;
	}
	// update length with actual amount read
	len = r;
	return 0;
      }

      int zero_copy_to_fd(int fd, loff_t *offset) {
	assert(!source_consumed);
	int flags = SPLICE_F_NONBLOCK;
	ssize_t r = safe_splice_exact(pipefds[0], NULL, fd, offset, len, flags);
	if (r < 0) {
	  return r;
	}
	source_consumed = true;
	return 0;
      }

      raw* clone_empty() {
	// cloning doesn't make sense for pipe-based buffers,
	// and is only used by unit tests for other types of buffers
	return NULL;
      }

      char *get_data() {
	if (data)
	  return data;
	return copy_pipe(pipefds);
      }

    private:
      int set_pipe_size(int *fds, long length) {
#ifdef CEPH_HAVE_SETPIPE_SZ
	if (::fcntl(fds[1], F_SETPIPE_SZ, length) == -1) {
	  int r = -errno;
	  if (r == -EPERM) {
	    // pipe limit must have changed - EPERM means we requested
	    // more than the maximum size as an unprivileged user
	    update_max_pipe_size();
	    throw malformed_input("length larger than new max pipe size");
	  }
	  return r;
	}
#endif
	return 0;
      }

      int set_nonblocking(int *fds) {
	if (::fcntl(fds[0], F_SETFL, O_NONBLOCK) == -1)
	  return -errno;
	if (::fcntl(fds[1], F_SETFL, O_NONBLOCK) == -1)
	  return -errno;
	return 0;
      }

      void close_pipe(int *fds) {
	if (fds[0] >= 0)
	  VOID_TEMP_FAILURE_RETRY(::close(fds[0]));
	if (fds[1] >= 0)
	  VOID_TEMP_FAILURE_RETRY(::close(fds[1]));
      }

      char *copy_pipe(int *fds) {
	/* preserve original pipe contents by copying into a temporary
	 * pipe before reading.
	 */
	int tmpfd[2];
	int r;

	assert(!source_consumed);
	assert(fds[0] >= 0);

	if (::pipe(tmpfd) == -1) {
	  r = -errno;
	  throw error_code(r);
	}
	r = set_nonblocking(tmpfd);
	if (r < 0) {
	  throw error_code(r);
	}
	r = set_pipe_size(tmpfd, len);
	if (r < 0) {
	}
	int flags = SPLICE_F_NONBLOCK;
	if (::tee(fds[0], tmpfd[1], len, flags) == -1) {
	  r = errno;
	  close_pipe(tmpfd);
	  throw error_code(r);
	}
	data = (char *)malloc(len);
	if (!data) {
	  close_pipe(tmpfd);
	  throw bad_alloc();
	}
	r = safe_read(tmpfd[0], data, len);
	if (r < (ssize_t)len) {
	  free(data);
	  data = NULL;
	  close_pipe(tmpfd);
	  throw error_code(r);
	}
	close_pipe(tmpfd);
	return data;
      }
      bool source_consumed;
      int pipefds[2];
    };
#endif // CEPH_HAVE_SPLICE

    /*
     * primitive buffer types
     */
    class raw_char : public raw {
    public:
      raw_char(unsigned l) : raw(l) {
	if (len)
	  data = new char[len];
	else
	  data = 0;
      }
      
      raw_char(unsigned l, char *b) : raw(b, l)
	{}
      
      ~raw_char() {
	delete[] data;
      }
      
      raw* clone_empty() {
	return new raw_char(len);
      }
    };

    class raw_static : public raw {
    public:
      raw_static(const char *d, unsigned l) : raw((char*)d, l) { }
      ~raw_static() {}
      raw* clone_empty() {
	return new raw_char(len);
      }
    };

    inline raw* copy(const char *c, unsigned len) {
      raw* r = new raw_char(len);
      memcpy(r->data, c, len);
      return r;
    }

    inline raw* raw::create(unsigned len) {
      return new raw_char(len);
    }

    inline raw* raw::claim_char(unsigned len, char *buf) {
      return new raw_char(len, buf);
    }

    inline raw* raw::create_malloc(unsigned len) {
      return new raw_malloc(len);
    }

    inline raw* raw::claim_malloc(unsigned len, char *buf) {
      return new raw_malloc(len, buf);
    }

    inline raw* raw::create_static(unsigned len, char *buf) {
      return new raw_static(buf, len);
    }

    inline raw* raw::create_page_aligned(unsigned len) {
#ifndef __CYGWIN__
      //return new raw_mmap_pages(len);
      return new raw_posix_aligned(len);
#else
      return new raw_hack_aligned(len);
#endif
    }

    inline raw* raw::create_zero_copy(unsigned len, int fd, int64_t *offset) {
#ifdef CEPH_HAVE_SPLICE
      raw_pipe* buf = new raw_pipe(len);
      int r = buf->set_source(fd, (loff_t*)offset);
      if (r < 0) {
	delete buf;
	throw error_code(r);
      }
      return buf;
#else
      throw error_code(-ENOTSUP);
#endif
    }

    inline std::ostream& operator<<(std::ostream& out, const raw &r) {
      return out << "buffer::raw(" << (void*)r.data << " len " << r.len
		 << " nref " << r.nref.load() << ")";
    }

  } /* namespace buffer */

} /* namespace ceph */

#endif /* CEPH_BUFFER_RAW_H */
