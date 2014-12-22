// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef __LIBRBD_HPP
#define __LIBRBD_HPP

#include <stdbool.h>
#include <string>
#include <list>
#include <map>
#include <vector>
#include "../rados/buffer.h"
#include "../rados/librados.hpp"
#include "librbd.h"

namespace librbd {

  using librados::IoCtx;

  class Image;
  typedef void *image_ctx_t;
  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

#if 0
  typedef struct {
    std::string client;
    std::string cookie;
    std::string address;
  } locker_t;
#endif

  typedef rbd_image_info_t image_info_t;

  class ProgressContext
  {
  public:
    virtual ~ProgressContext();
    virtual int update_progress(uint64_t offset, uint64_t total) = 0;
  };

class RBD
{
public:
  RBD();
  ~RBD();

  struct AioCompletion {
    void *pc;
    AioCompletion(void *cb_arg, callback_t complete_cb);
    bool is_complete();
    int wait_for_complete();
    ssize_t get_return_value();
    void release();
  };

  void version(int *major, int *minor, int *extra);

  int open(IoCtx& io_ctx, Image& image, const char *name);
  // see librbd.h
  int open_read_only(IoCtx& io_ctx, Image& image, const char *name);
#if 0
  int list(IoCtx& io_ctx, std::vector<std::string>& names);
#endif
  int create(IoCtx& io_ctx, const char *name, uint64_t size);
  int remove(IoCtx& io_ctx, const char *name);
  int remove_with_progress(IoCtx& io_ctx, const char *name, ProgressContext& pctx);
  int rename(IoCtx& src_io_ctx, const char *srcname, const char *destname);

private:
  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);
};

class Image
{
public:
  Image();
  ~Image();

  int resize(uint64_t size);
  int resize_with_progress(uint64_t size, ProgressContext& pctx);
  int stat(image_info_t &info, size_t infosize);
  int size(uint64_t *size);
  int overlap(uint64_t *overlap);
  int copy(IoCtx& dest_io_ctx, const char *destname);
  int copy2(Image& dest);
  int copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
			 ProgressContext &prog_ctx);
  int copy_with_progress2(Image& dest, ProgressContext &prog_ctx);

  /* striping */
  uint64_t get_stripe_unit() const;
  uint64_t get_stripe_count() const;

  /* advisory locking (see librbd.h for details) */
#if 0
  int list_lockers(std::list<locker_t> *lockers,
		   bool *exclusive, std::string *tag);
  int lock_exclusive(const std::string& cookie);
  int lock_shared(const std::string& cookie, const std::string& tag);
  int unlock(const std::string& cookie);
  int break_lock(const std::string& client, const std::string& cookie);
#endif

  /* I/O */
  ssize_t read(uint64_t ofs, size_t len, ceph::bufferlist& bl);
  int64_t read_iterate(uint64_t ofs, size_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
  int read_iterate2(uint64_t ofs, uint64_t len,
		    int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
  ssize_t write(uint64_t ofs, size_t len, ceph::bufferlist& bl);
  int discard(uint64_t ofs, uint64_t len);

  int aio_write(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);

  /**
   * read async from image
   *
   * The target bufferlist is populated with references to buffers
   * that contain the data for the given extent of the image.
   *
   * NOTE: If caching is enabled, the bufferlist will directly
   * reference buffers in the cache to avoid an unnecessary data copy.
   * As a result, if the user intends to modify the buffer contents
   * directly, they should make a copy first (unconditionally, or when
   * the reference count on ther underlying buffer is more than 1).
   *
   * @param off offset in image
   * @param len length of read
   * @param bl bufferlist to read into
   * @param c aio completion to notify when read is complete
   */
  int aio_read(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  int aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c);

  int flush();
  /**
   * Start a flush if caching is enabled. Get a callback when
   * the currently pending writes are on disk.
   *
   * @param image the image to flush writes to
   * @param c what to call when flushing is complete
   * @returns 0 on success, negative error code on failure
   */
  int aio_flush(RBD::AioCompletion *c);

private:
  friend class RBD;

  Image(const Image& rhs);
  const Image& operator=(const Image& rhs);

  image_ctx_t ctx;
};

}

#endif
