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
#include <errno.h>

#include "common/dout.h"
#include "common/errno.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "osdc/ObjectCacher.h"

#include "librbd/AioCompletion.h"
#include "cls/lock/cls_lock_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/LibrbdWriteback.h"

#include <algorithm>
#include <string>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

using std::string;
using std::vector;

using ceph::bufferlist;
using librados::IoCtx;

namespace librbd {
  ProgressContext::~ProgressContext()
  {
  }

  class CProgressContext : public ProgressContext
  {
  public:
    CProgressContext(librbd_progress_fn_t fn, void *data)
      : m_fn(fn), m_data(data)
    {
    }
    int update_progress(uint64_t offset, uint64_t src_size)
    {
      return m_fn(offset, src_size, m_data);
    }
  private:
    librbd_progress_fn_t m_fn;
    void *m_data;
  };

  /*
    RBD
  */
  RBD::RBD()
  {
  }

  RBD::~RBD()
  {
  }

  void RBD::version(int *major, int *minor, int *extra)
  {
    rbd_version(major, minor, extra);
  }

  int RBD::open(IoCtx& io_ctx, Image& image, const char *name)
  {
    ImageCtx *ictx = new ImageCtx(name, io_ctx, false);

    int r = librbd::open_image(ictx);
    if (r < 0)
      return r;

    image.ctx = (image_ctx_t) ictx;
    return 0;
  }

  int RBD::open_read_only(IoCtx& io_ctx, Image& image, const char *name)
  {
    ImageCtx *ictx = new ImageCtx(name, io_ctx, true);

    int r = librbd::open_image(ictx);
    if (r < 0)
      return r;

    image.ctx = (image_ctx_t) ictx;
    return 0;
  }

  int RBD::create(IoCtx& io_ctx, const char *name, uint64_t size)
  {
    return librbd::create(io_ctx, name, size);
  }

  int RBD::remove(IoCtx& io_ctx, const char *name)
  {
    librbd::NoOpProgressContext prog_ctx;
    int r = librbd::remove(io_ctx, name, prog_ctx);
    return r;
  }

  int RBD::remove_with_progress(IoCtx& io_ctx, const char *name,
				ProgressContext& pctx)
  {
    int r = librbd::remove(io_ctx, name, pctx);
    return r;
  }

#if 0
  int RBD::list(IoCtx& io_ctx, vector<string>& names)
  {
    int r = librbd::list(io_ctx, names);
    return r;
  }
  #endif

  int RBD::rename(IoCtx& src_io_ctx, const char *srcname, const char *destname)
  {
    int r = librbd::rename(src_io_ctx, srcname, destname);
    return r;
  }

  RBD::AioCompletion::AioCompletion(void *cb_arg, callback_t complete_cb)
  {
    librbd::AioCompletion *c = librbd::aio_create_completion(cb_arg,
							     complete_cb);
    pc = (void *)c;
    c->rbd_comp = this;
  }

  bool RBD::AioCompletion::is_complete()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    return c->is_complete();
  }

  int RBD::AioCompletion::wait_for_complete()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    return c->wait_for_complete();
  }

  ssize_t RBD::AioCompletion::get_return_value()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    return c->get_return_value();
  }

  void RBD::AioCompletion::release()
  {
    librbd::AioCompletion *c = (librbd::AioCompletion *)pc;
    c->release();
    delete this;
  }

  /*
    Image
  */

  Image::Image() : ctx(NULL)
  {
  }

  Image::~Image()
  {
    if (ctx) {
      ImageCtx *ictx = (ImageCtx *)ctx;
      close_image(ictx);
    }
  }

  int Image::resize(uint64_t size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::resize(ictx, size, prog_ctx);
  }

  int Image::resize_with_progress(uint64_t size, librbd::ProgressContext& pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::resize(ictx, size, pctx);
  }

  int Image::stat(image_info_t& info, size_t infosize)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::info(ictx, info, infosize);
  }

  int Image::size(uint64_t *size)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::get_size(ictx, size);
  }

  int Image::copy(IoCtx& dest_io_ctx, const char *destname)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
  }

  int Image::copy2(Image& dest)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    librbd::NoOpProgressContext prog_ctx;
    return librbd::copy(srcctx, destctx, prog_ctx);
  }

  int Image::copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
				librbd::ProgressContext &pctx)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::copy(ictx, dest_io_ctx, destname, pctx);
  }

  int Image::copy_with_progress2(Image& dest, librbd::ProgressContext &pctx)
  {
    ImageCtx *srcctx = (ImageCtx *)ctx;
    ImageCtx *destctx = (ImageCtx *)dest.ctx;
    return librbd::copy(srcctx, destctx, pctx);
  }

#if 0
  int Image::list_lockers(std::list<librbd::locker_t> *lockers,
			  bool *exclusive, string *tag)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::list_lockers(ictx, lockers, exclusive, tag);
  }

  int Image::lock_exclusive(const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::lock(ictx, true, cookie, "");
  }

  int Image::lock_shared(const string& cookie, const std::string& tag)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::lock(ictx, false, cookie, tag);
  }

  int Image::unlock(const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::unlock(ictx, cookie);
  }

  int Image::break_lock(const string& client, const string& cookie)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::break_lock(ictx, client, cookie);
  }
#endif

  ssize_t Image::read(uint64_t ofs, size_t len, bufferlist& bl)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    bufferptr ptr(len);
    bl.push_back(ptr);
    return librbd::read(ictx, ofs, len, bl.c_str());
  }

  int64_t Image::read_iterate(uint64_t ofs, size_t len,
			      int (*cb)(uint64_t, size_t, const char *, void *),
			      void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::read_iterate(ictx, ofs, len, cb, arg);
  }

  int Image::read_iterate2(uint64_t ofs, uint64_t len,
			      int (*cb)(uint64_t, size_t, const char *, void *),
			      void *arg)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
    if (r > 0)
      r = 0;
    return (int)r;
  }

  ssize_t Image::write(uint64_t ofs, size_t len, bufferlist& bl)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    if (bl.length() < len)
      return -EINVAL;
    return librbd::write(ictx, ofs, len, bl.c_str());
  }

  int Image::discard(uint64_t ofs, uint64_t len)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::discard(ictx, ofs, len);
  }

  int Image::aio_write(uint64_t off, size_t len, bufferlist& bl,
		       RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    if (bl.length() < len)
      return -EINVAL;
    return librbd::aio_write(ictx, off, len, bl.c_str(),
			     (librbd::AioCompletion *)c->pc);
  }

  int Image::aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::aio_discard(ictx, off, len, (librbd::AioCompletion *)c->pc);
  }

  int Image::aio_read(uint64_t off, size_t len, bufferlist& bl,
		      RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    ldout(ictx->cct, 10) << "Image::aio_read() buf=" << (void *)bl.c_str() << "~"
			 << (void *)(bl.c_str() + len - 1) << dendl;
    return librbd::aio_read(ictx, off, len, NULL, &bl, (librbd::AioCompletion *)c->pc);
  }

  int Image::flush()
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::flush(ictx);
  }

  int Image::aio_flush(RBD::AioCompletion *c)
  {
    ImageCtx *ictx = (ImageCtx *)ctx;
    return librbd::aio_flush(ictx, (librbd::AioCompletion *)c->pc);
  }

} // namespace librbd

extern "C" void rbd_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRBD_VER_MAJOR;
  if (minor)
    *minor = LIBRBD_VER_MINOR;
  if (extra)
    *extra = LIBRBD_VER_EXTRA;
}

#if 0
/* images */
extern "C" int rbd_list(rados_ioctx_t p, char *names, size_t *size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  vector<string> cpp_names;
  int r = librbd::list(io_ctx, cpp_names);
  if (r == -ENOENT)
    return 0;

  if (r < 0)
    return r;

  size_t expected_size = 0;

  for (size_t i = 0; i < cpp_names.size(); i++) {
    expected_size += cpp_names[i].size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    return -ERANGE;
  }

  for (int i = 0; i < (int)cpp_names.size(); i++) {
    strcpy(names, cpp_names[i].c_str());
    names += strlen(names) + 1;
  }
  return (int)expected_size;
}
#endif

extern "C" int rbd_create(rados_ioctx_t p, const char *name, uint64_t size)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  return librbd::create(io_ctx, name, size);
}

extern "C" int rbd_remove(rados_ioctx_t p, const char *name)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::NoOpProgressContext prog_ctx;
  return librbd::remove(io_ctx, name, prog_ctx);
}

extern "C" int rbd_remove_with_progress(rados_ioctx_t p, const char *name,
					librbd_progress_fn_t cb, void *cbdata)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::remove(io_ctx, name, prog_ctx);
}

extern "C" int rbd_copy(rbd_image_t image, rados_ioctx_t dest_p,
			const char *destname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  librbd::NoOpProgressContext prog_ctx;
  return librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
}

extern "C" int rbd_copy2(rbd_image_t srcp, rbd_image_t destp)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  librbd::NoOpProgressContext prog_ctx;
  return librbd::copy(src, dest, prog_ctx);
}

extern "C" int rbd_copy_with_progress(rbd_image_t image, rados_ioctx_t dest_p,
				      const char *destname,
				      librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librados::IoCtx dest_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(dest_p, dest_io_ctx);
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(ictx, dest_io_ctx, destname, prog_ctx);
  return ret;
}

extern "C" int rbd_copy_with_progress2(rbd_image_t srcp, rbd_image_t destp,
				      librbd_progress_fn_t fn, void *data)
{
  librbd::ImageCtx *src = (librbd::ImageCtx *)srcp;
  librbd::ImageCtx *dest = (librbd::ImageCtx *)destp;
  librbd::CProgressContext prog_ctx(fn, data);
  int ret = librbd::copy(src, dest, prog_ctx);
  return ret;
}

extern "C" int rbd_rename(rados_ioctx_t src_p, const char *srcname,
			  const char *destname)
{
  librados::IoCtx src_io_ctx;
  librados::IoCtx::from_rados_ioctx_t(src_p, src_io_ctx);
  return librbd::rename(src_io_ctx, srcname, destname);
}

extern "C" int rbd_open(rados_ioctx_t p, const char *name, rbd_image_t *image)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, io_ctx, false);
  int r = librbd::open_image(ictx);
  if (r >= 0)
    *image = (rbd_image_t)ictx;
  return r;
}

extern "C" int rbd_open_read_only(rados_ioctx_t p, const char *name,
				  rbd_image_t *image)
{
  librados::IoCtx io_ctx;
  librados::IoCtx::from_rados_ioctx_t(p, io_ctx);
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, io_ctx, true);
  int r = librbd::open_image(ictx);
  if (r >= 0)
    *image = (rbd_image_t)ictx;
  return r;
}

extern "C" int rbd_close(rbd_image_t image)
{
  librbd::ImageCtx *ctx = (librbd::ImageCtx *)image;
  librbd::close_image(ctx);
  return 0;
}

extern "C" int rbd_resize(rbd_image_t image, uint64_t size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::NoOpProgressContext prog_ctx;
  return librbd::resize(ictx, size, prog_ctx);
}

extern "C" int rbd_resize_with_progress(rbd_image_t image, uint64_t size,
					librbd_progress_fn_t cb, void *cbdata)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::CProgressContext prog_ctx(cb, cbdata);
  return librbd::resize(ictx, size, prog_ctx);
}

extern "C" int rbd_stat(rbd_image_t image, rbd_image_info_t *info,
			size_t infosize)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::info(ictx, *info, infosize);
}

extern "C" int rbd_get_size(rbd_image_t image, uint64_t *size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::get_size(ictx, size);
}

#if 0
extern "C" ssize_t rbd_list_lockers(rbd_image_t image, int *exclusive,
				    char *tag, size_t *tag_len,
				    char *clients, size_t *clients_len,
				    char *cookies, size_t *cookies_len,
				    char *addrs, size_t *addrs_len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  std::list<librbd::locker_t> lockers;
  bool exclusive_bool;
  string tag_str;

  int r = list_lockers(ictx, &lockers, &exclusive_bool, &tag_str);
  if (r < 0)
    return r;

  ldout(ictx->cct, 20) << "list_lockers r = " << r << " lockers.size() = " << lockers.size() << dendl;

  *exclusive = (int)exclusive_bool;
  size_t clients_total = 0;
  size_t cookies_total = 0;
  size_t addrs_total = 0;
  for (list<librbd::locker_t>::const_iterator it = lockers.begin();
       it != lockers.end(); ++it) {
    clients_total += it->client.length() + 1;
    cookies_total += it->cookie.length() + 1;
    addrs_total += it->address.length() + 1;
  }

  bool too_short = ((clients_total > *clients_len) ||
		    (cookies_total > *cookies_len) ||
		    (addrs_total > *addrs_len) ||
		    (tag_str.length() + 1 > *tag_len));
  *clients_len = clients_total;
  *cookies_len = cookies_total;
  *addrs_len = addrs_total;
  *tag_len = tag_str.length() + 1;
  if (too_short)
    return -ERANGE;

  strcpy(tag, tag_str.c_str());
  char *clients_p = clients;
  char *cookies_p = cookies;
  char *addrs_p = addrs;
  for (list<librbd::locker_t>::const_iterator it = lockers.begin();
       it != lockers.end(); ++it) {
    strcpy(clients_p, it->client.c_str());
    clients_p += it->client.length() + 1;
    strcpy(cookies_p, it->cookie.c_str());
    cookies_p += it->cookie.length() + 1;
    strcpy(addrs_p, it->address.c_str());
    addrs_p += it->address.length() + 1;
  }

  return lockers.size();
}

extern "C" int rbd_lock_exclusive(rbd_image_t image, const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::lock(ictx, true, cookie ? cookie : "", "");
}

extern "C" int rbd_lock_shared(rbd_image_t image, const char *cookie,
			       const char *tag)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::lock(ictx, false, cookie ? cookie : "", tag ? tag : "");
}

extern "C" int rbd_unlock(rbd_image_t image, const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::unlock(ictx, cookie ? cookie : "");
}

extern "C" int rbd_break_lock(rbd_image_t image, const char *client,
			      const char *cookie)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::break_lock(ictx, client, cookie ? cookie : "");
}
#endif

/* I/O */
extern "C" ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len,
			    char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::read(ictx, ofs, len, buf);
}

extern "C" int64_t rbd_read_iterate(rbd_image_t image, uint64_t ofs, size_t len,
				    int (*cb)(uint64_t, size_t, const char *, void *),
				    void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::read_iterate(ictx, ofs, len, cb, arg);
}

extern "C" int rbd_read_iterate2(rbd_image_t image, uint64_t ofs, uint64_t len,
				 int (*cb)(uint64_t, size_t, const char *, void *),
				 void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  int64_t r = librbd::read_iterate(ictx, ofs, len, cb, arg);
  if (r > 0)
    r = 0;
  return (int)r;
}

extern "C" ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len,
			     const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::write(ictx, ofs, len, buf);
}

extern "C" int rbd_discard(rbd_image_t image, uint64_t ofs, uint64_t len)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::discard(ictx, ofs, len);
}

extern "C" int rbd_aio_create_completion(void *cb_arg,
					 rbd_callback_t complete_cb,
					 rbd_completion_t *c)
{
  librbd::RBD::AioCompletion *rbd_comp =
    new librbd::RBD::AioCompletion(cb_arg, complete_cb);
  *c = (rbd_completion_t) rbd_comp;
  return 0;
}

extern "C" int rbd_aio_write(rbd_image_t image, uint64_t off, size_t len,
			     const char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_write(ictx, off, len, buf,
			   (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_discard(rbd_image_t image, uint64_t off, uint64_t len,
			       rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_discard(ictx, off, len, (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_read(rbd_image_t image, uint64_t off, size_t len,
			    char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_read(ictx, off, len, buf, NULL,
			  (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_flush(rbd_image_t image)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return librbd::flush(ictx);
}

extern "C" int rbd_aio_flush(rbd_image_t image, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return librbd::aio_flush(ictx, (librbd::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_is_complete(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->is_complete();
}

extern "C" int rbd_aio_wait_for_complete(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->wait_for_complete();
}

extern "C" ssize_t rbd_aio_get_return_value(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->get_return_value();
}

extern "C" void rbd_aio_release(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  comp->release();
}
