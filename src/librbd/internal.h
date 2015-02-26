// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_INTERNAL_H
#define CEPH_LIBRBD_INTERNAL_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"

enum {
  l_librbd_first = 26000,

  l_librbd_rd,		     // read ops
  l_librbd_rd_bytes,	     // bytes read
  l_librbd_rd_latency,	     // average latency
  l_librbd_wr,
  l_librbd_wr_bytes,
  l_librbd_wr_latency,
  l_librbd_discard,
  l_librbd_discard_bytes,
  l_librbd_discard_latency,
  l_librbd_flush,

  l_librbd_aio_rd,		 // read ops
  l_librbd_aio_rd_bytes,	 // bytes read
  l_librbd_aio_rd_latency,
  l_librbd_aio_wr,
  l_librbd_aio_wr_bytes,
  l_librbd_aio_wr_latency,
  l_librbd_aio_discard,
  l_librbd_aio_discard_bytes,
  l_librbd_aio_discard_latency,
  l_librbd_aio_flush,
  l_librbd_aio_flush_latency,

  l_librbd_notify,
  l_librbd_resize,

  l_librbd_last,
};

namespace librbd {

  struct AioCompletion;
  struct ImageCtx;

  class NoOpProgressContext : public ProgressContext
  {
  public:
    NoOpProgressContext()
    {
    }
    int update_progress(uint64_t offset, uint64_t src_size)
    {
      return 0;
    }
  };

  const std::string header_name(const std::string &name);
  const std::string image_name(const std::string &name);

  int check_exists(librados::IoCtx &io_ctx, const std::string &name,
		   uint64_t *size = NULL);

#if 0
  int list(librados::IoCtx& io_ctx, std::vector<std::string>& names);
#endif
  int create(librados::IoCtx& io_ctx, const char *imgname, uint64_t size);
  int rename(librados::IoCtx& io_ctx, const char *srcname, const char *dstname);
  int info(ImageCtx *ictx, image_info_t& info, size_t image_size);
  int get_size(ImageCtx *ictx, uint64_t *size);

  int remove(librados::IoCtx& io_ctx, const char *imgname,
	     ProgressContext& prog_ctx);
  int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx);
  int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx);
  int ictx_check(ImageCtx *ictx);
  int ictx_refresh(ImageCtx *ictx);
  int copy(ImageCtx *ictx, IoCtx& dest_md_ctx, const char *destname,
	   ProgressContext &prog_ctx);
  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx);

  int open_image(ImageCtx *ictx);
  void close_image(ImageCtx *ictx);

  /* cooperative locking */
#if 0
  int list_lockers(ImageCtx *ictx,
		   std::list<locker_t> *locks,
		   bool *exclusive,
		   std::string *tag);
  int lock(ImageCtx *ictx, bool exclusive, const std::string& cookie,
	   const std::string& tag);
  int lock_shared(ImageCtx *ictx, const std::string& cookie,
		  const std::string& tag);
  int unlock(ImageCtx *ictx, const std::string& cookie);
  int break_lock(ImageCtx *ictx, const std::string& client,
		 const std::string& cookie);
#endif

  void trim_image(ImageCtx *ictx, uint64_t newsize, ProgressContext& prog_ctx);

  int read_header_bl(librados::IoCtx& io_ctx, const std::string& md_oid,
		     ceph::bufferlist& header);
  int notify_change(librados::IoCtx& io_ctx, const std::string& oid_t,
		    ImageCtx *ictx);
  int read_header(librados::IoCtx& io_ctx, const std::string& md_oid,
		  struct rbd_obj_header_ondisk *header);
  int write_header(librados::IoCtx& io_ctx, const std::string& md_oid,
		   ceph::bufferlist& header);
#if 0
  int tmap_set(librados::IoCtx& io_ctx, const std::string& imgname);
  int tmap_rm(librados::IoCtx& io_ctx, const std::string& imgname);
#endif
  void image_info(const ImageCtx *ictx, image_info_t& info, size_t info_size);
  std::string get_block_oid(const std::string &object_prefix, uint64_t num,
			    bool old_format);
  int clip_io(ImageCtx *ictx, uint64_t off, uint64_t *len);
  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk, uint64_t size);

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, uint64_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg);
  ssize_t read(ImageCtx *ictx, uint64_t off, size_t len, char *buf);
  ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf);
  int discard(ImageCtx *ictx, uint64_t off, uint64_t len);
  int aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
		AioCompletion *c);
  int aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c);
  int aio_read(ImageCtx *ictx, uint64_t off, size_t len,
	       char *buf, bufferlist *pbl, AioCompletion *c);
  int aio_flush(ImageCtx *ictx, AioCompletion *c);
  int flush(ImageCtx *ictx);
  int _flush(ImageCtx *ictx);

  ssize_t handle_sparse_read(CephContext *cct,
			     ceph::bufferlist data_bl,
			     uint64_t block_ofs,
			     const std::map<uint64_t, uint64_t> &data_map,
			     uint64_t buf_ofs,
			     size_t buf_len,
			     char *dest_buf);

  AioCompletion *aio_create_completion();
  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete);
  AioCompletion *aio_create_completion_internal(void *cb_arg,
						callback_t cb_complete);

  // raw callbacks
  int simple_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg);
  void rados_req_cb(rados_completion_t cb, void *arg);
  void rados_ctx_cb(rados_completion_t cb, void *arg);
  void rbd_req_cb(completion_t cb, void *arg);
  void rbd_ctx_cb(completion_t cb, void *arg);
}

#endif
