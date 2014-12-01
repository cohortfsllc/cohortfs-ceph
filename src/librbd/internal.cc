// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <limits.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "cls/lock/cls_lock_client.h"
#include "include/stringify.h"

#include "librbd/AioCompletion.h"
#include "librbd/AioRequest.h"
#include "librbd/ImageCtx.h"

#include "librbd/internal.h"
#include "include/util.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

#define rbd_howmany(x, y)  (((x) + (y) - 1) / (y))

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;
// list binds to list() here, so std::list is explicitly used below

using ceph::bufferlist;
using librados::IoCtx;
using librados::Rados;

namespace librbd {
  const string header_name(const string &image_name)
  {
    return image_name + RBD_SUFFIX;
  }

  const string image_name(const string &name)
  {
    return "rb." + name + ".image";
  }

  int check_exists(IoCtx &io_ctx, const string &name, uint64_t *size)
  {
    int r = io_ctx.stat(header_name(name), size, NULL);
    return r;
  }

  void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
		       uint64_t size)
  {
    memset(&ondisk, 0, sizeof(ondisk));

    memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
    memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE,
	   sizeof(RBD_HEADER_SIGNATURE));
    memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

    ondisk.image_size = size;
  }

  void image_info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    ictx->md_lock.get_read();
    info.size = ictx->get_image_size();
    ictx->md_lock.put_read();
  }

  void trim_image(ImageCtx *ictx, uint64_t newsize, ProgressContext& prog_ctx)
  {
    CephContext *cct = (CephContext *)ictx->io_ctx.cct();
    uint64_t size = ictx->get_current_size();

    if (newsize < size) {
      SimpleThrottle throttle(cct->_conf->rbd_concurrent_management_ops, true);
      Context *req_comp = new C_SimpleThrottle(&throttle);
      librados::AioCompletion *rados_completion =
	librados::Rados::aio_create_completion(req_comp, NULL, rados_ctx_cb);
      librados::ObjectWriteOperation op(ictx->io_ctx);
      op.truncate(newsize);
      ictx->io_ctx.aio_operate(ictx->image_oid, rados_completion,
			       &op, 0);
      rados_completion->release();
      int r = throttle.wait_for_ret();
      if (r < 0) {
	lderr(cct) << "warning: failed to remove some object(s): "
		   << cpp_strerror(r) << dendl;
      }
    }
  }

  int read_header_bl(IoCtx& io_ctx, const string& header_oid,
		     bufferlist& header)
  {
    int r;
    uint64_t off = 0;
#define READ_SIZE 4096
    do {
      bufferlist bl;
      r = io_ctx.read(header_oid, bl, READ_SIZE, off);
      if (r < 0)
	return r;
      header.claim_append(bl);
      off += r;
    } while (r == READ_SIZE);

    if (memcmp(RBD_HEADER_TEXT, header.c_str(), sizeof(RBD_HEADER_TEXT))) {
      CephContext *cct = (CephContext *)io_ctx.cct();
      lderr(cct) << "unrecognized header format" << dendl;
      return -ENXIO;
    }

    return 0;
  }

  int notify_change(IoCtx& io_ctx, const string& oid, ImageCtx *ictx)
  {
    if (ictx) {
      ictx->refresh_lock.Lock();
      ldout(ictx->cct, 20) << "notify_change refresh_seq = " << ictx->refresh_seq
			   << " last_refresh = " << ictx->last_refresh << dendl;
      ++ictx->refresh_seq;
      ictx->refresh_lock.Unlock();
    }

    bufferlist bl;
    io_ctx.notify(oid, bl);
    return 0;
  }

  int read_header(IoCtx& io_ctx, const string& header_oid,
		  struct rbd_obj_header_ondisk *header)
  {
    bufferlist header_bl;
    int r = read_header_bl(io_ctx, header_oid, header_bl);
    if (r < 0)
      return r;
    if (header_bl.length() < (int)sizeof(*header))
      return -EIO;
    memcpy(header, header_bl.c_str(), sizeof(*header));

    return 0;
  }

  int write_header(IoCtx& io_ctx, const string& header_oid, bufferlist& header)
  {
    bufferlist bl;
    int r = io_ctx.write(header_oid, header, header.length(), 0);

    notify_change(io_ctx, header_oid, NULL);

    return r;
  }

  int create(IoCtx& io_ctx, const char *imgname, uint64_t size)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();

    ldout(cct, 2) << "creating rbd image..." << dendl;
    struct rbd_obj_header_ondisk header;
    init_rbd_header(header, size);

    bufferlist bl;
    bl.append((const char *)&header, sizeof(header));

    string header_oid = header_name(imgname);
    int r = io_ctx.write(header_oid, bl, bl.length(), 0);
    if (r < 0) {
      lderr(cct) << "Error writing image header: " << cpp_strerror(r)
		 << dendl;
      return r;
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }


  int rename(IoCtx& io_ctx, const char *srcname, const char *dstname)
  {
    CephContext *cct = (CephContext *)io_ctx.cct();
    ldout(cct, 20) << "rename " << &io_ctx << " " << srcname << " -> "
		   << dstname << dendl;

    uint64_t src_size;
    int r = check_exists(io_ctx, srcname, &src_size);
    if (r < 0) {
      lderr(cct) << "error finding source object: " << cpp_strerror(r) << dendl;
      return r;
    }

    string src_oid = header_name(srcname);
    string dst_oid = header_name(dstname);

    bufferlist databl;
    map<string, bufferlist> omap_values;
    r = io_ctx.read(src_oid, databl, src_size, 0);
    if (r < 0) {
      lderr(cct) << "error reading source object: " << src_oid << ": "
		 << cpp_strerror(r) << dendl;
      return r;
    }

    int MAX_READ = 1024;
    string last_read = "";
    do {
      map<string, bufferlist> outbl;
      r = io_ctx.omap_get_vals(src_oid, last_read, MAX_READ, outbl);
      if (r < 0) {
	lderr(cct) << "error reading source object omap values: "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      omap_values.insert(outbl.begin(), outbl.end());
      if (!outbl.empty())
	last_read = outbl.rbegin()->first;
    } while (r == MAX_READ);

    r = check_exists(io_ctx, dstname);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "error checking for existing image called "
		 << dstname << ":" << cpp_strerror(r) << dendl;
      return r;
    }
    if (r == 0) {
      lderr(cct) << "rbd image " << dstname << " already exists" << dendl;
      return -EEXIST;
    }

    librados::ObjectWriteOperation op(io_ctx);
    op.create(true);
    op.write_full(databl);
    if (!omap_values.empty())
      op.omap_set(omap_values);
    r = io_ctx.operate(dst_oid, &op);
    if (r < 0) {
      lderr(cct) << "error writing destination object: " << dst_oid << ": "
		 << cpp_strerror(r) << dendl;
      return r;
    }

    r = io_ctx.remove(src_oid);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "warning: couldn't remove old source object ("
		 << src_oid << ")" << dendl;
    }

    notify_change(io_ctx, header_name(srcname), NULL);

    return 0;
  }


  int info(ImageCtx *ictx, image_info_t& info, size_t infosize)
  {
    ldout(ictx->cct, 20) << "info " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    image_info(ictx, info, infosize);
    return 0;
  }

  int get_size(ImageCtx *ictx, uint64_t *size)
  {
    int r = ictx_check(ictx);
    if (r < 0)
      return r;
    RWLock::RLocker l(ictx->md_lock);
    *size = ictx->get_image_size();
    return 0;
  }

  int remove(IoCtx& io_ctx, const char *imgname, ProgressContext& prog_ctx)
  {
    CephContext *cct((CephContext *)io_ctx.cct());
    ldout(cct, 20) << "remove " << &io_ctx << " " << imgname << dendl;

    ImageCtx *ictx = new ImageCtx(imgname, io_ctx, false);
    int r = open_image(ictx);
    if (r < 0) {
      ldout(cct, 2) << "error opening image: " << cpp_strerror(-r) << dendl;
    } else {
      string header_oid = ictx->header_oid;

      std::list<obj_watch_t> watchers;
      r = io_ctx.list_watchers(header_oid, &watchers);
      if (r < 0) {
	lderr(cct) << "error listing watchers" << dendl;
	close_image(ictx);
	return r;
      }
      if (watchers.size() > 1) {
	lderr(cct) << "image has watchers - not removing" << dendl;
	close_image(ictx);
	return -EBUSY;
      }
      assert(watchers.size() == 1);

      ictx->md_lock.get_read();
      trim_image(ictx, 0, prog_ctx);
      ictx->md_lock.put_read();

      close_image(ictx);

      ldout(cct, 2) << "removing header..." << dendl;
      r = io_ctx.remove(header_oid);
      if (r < 0 && r != -ENOENT) {
	lderr(cct) << "error removing header: " << cpp_strerror(-r) << dendl;
	return r;
      }
    }

    ldout(cct, 2) << "done." << dendl;
    return 0;
  }

  int resize_helper(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
  {
    CephContext *cct = ictx->cct;

    if (size == ictx->size) {
      ldout(cct, 2) << "no change in size (" << ictx->size << " -> " << size
		    << ")" << dendl;
      return 0;
    }

    if (size > ictx->size) {
      ldout(cct, 2) << "expanding image " << ictx->size << " -> " << size
		    << dendl;
      // TODO: make ictx->set_size
    } else {
      ldout(cct, 2) << "shrinking image " << ictx->size << " -> " << size
		    << dendl;
      trim_image(ictx, size, prog_ctx);
    }
    ictx->size = size;

    int r;
    // rewrite header
    bufferlist bl;
    ictx->header.image_size = size;
    bl.append((const char *)&(ictx->header), sizeof(ictx->header));
    r = ictx->io_ctx.write(ictx->header_oid, bl, bl.length(), 0);

    // TODO: remove this useless check
    if (r == -ERANGE)
      lderr(cct) << "operation might have conflicted with another client!"
		 << dendl;
    if (r < 0) {
      lderr(cct) << "error writing header: " << cpp_strerror(-r) << dendl;
      return r;
    } else {
      notify_change(ictx->io_ctx, ictx->header_oid, ictx);
    }

    return 0;
  }

  int resize(ImageCtx *ictx, uint64_t size, ProgressContext& prog_ctx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "resize " << ictx << " " << ictx->size << " -> "
		   << size << dendl;

    if (ictx->read_only)
      return -EROFS;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    RWLock::WLocker l(ictx->md_lock);
    if (size < ictx->size && ictx->object_cacher) {
      // need to invalidate since we're deleting objects, and
      // ObjectCacher doesn't track non-existent objects
      ictx->invalidate_cache();
    }
    resize_helper(ictx, size, prog_ctx);

    ldout(cct, 2) << "done." << dendl;

    return 0;
  }

  int ictx_check(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "ictx_check " << ictx << dendl;
    ictx->refresh_lock.Lock();
    bool needs_refresh = ictx->last_refresh != ictx->refresh_seq;
    ictx->refresh_lock.Unlock();

    if (needs_refresh) {
      RWLock::WLocker l(ictx->md_lock);

      int r = ictx_refresh(ictx);
      if (r < 0) {
	lderr(cct) << "Error re-reading rbd header: " << cpp_strerror(-r)
		   << dendl;
	return r;
      }
    }
    return 0;
  }

  int ictx_refresh(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    bufferlist bl, bl2;

    ldout(cct, 20) << "ictx_refresh " << ictx << dendl;

    ictx->refresh_lock.Lock();
    int refresh_seq = ictx->refresh_seq;
    ictx->refresh_lock.Unlock();

    {
      int r;
      ictx->lockers.clear();
      r = read_header(ictx->io_ctx, ictx->header_oid, &ictx->header);
      if (r < 0) {
	lderr(cct) << "Error reading header: " << cpp_strerror(r) << dendl;
	return r;
      }
      ClsLockType lock_type = LOCK_NONE;
      r = rados::cls::lock::get_lock_info(&ictx->io_ctx, ictx->header_oid,
					  RBD_LOCK_NAME, &ictx->lockers,
					  &lock_type, &ictx->lock_tag);

      // If EOPNOTSUPP, treat image as if there are no locks (we can't
      // query them).

      // Ugly: OSDs prior to eed28daaf8927339c2ecae1b1b06c1b63678ab03
      // return EIO when the class isn't present; should be EOPNOTSUPP.
      // Treat EIO or EOPNOTSUPP the same for now, as LOCK_NONE.  Blech.

      if (r < 0 && ((r != -EOPNOTSUPP) && (r != -EIO))) {
	lderr(cct) << "Error getting lock info: " << cpp_strerror(r)
		   << dendl;
	return r;
      }
      ictx->exclusive_locked = (lock_type == LOCK_EXCLUSIVE);
      ictx->size = ictx->header.image_size;
    }

    ictx->refresh_lock.Lock();
    ictx->last_refresh = refresh_seq;
    ictx->refresh_lock.Unlock();

    return 0;
  }

  struct CopyProgressCtx {
    CopyProgressCtx(ProgressContext &p)
      : destictx(NULL), src_size(0), prog_ctx(p)
    { }

    ImageCtx *destictx;
    uint64_t src_size;
    ProgressContext &prog_ctx;
  };

  int do_copy_extent(uint64_t offset, size_t len, const char *buf, void *data)
  {
    CopyProgressCtx *cp = reinterpret_cast<CopyProgressCtx*>(data);
    cp->prog_ctx.update_progress(offset, cp->src_size);
    int ret = 0;
    if (buf) {
      ret = write(cp->destictx, offset, len, buf);
    }
    return ret;
  }

  int copy(ImageCtx *src, IoCtx& dest_md_ctx, const char *destname,
	   ProgressContext &prog_ctx)
  {
    CephContext *cct = (CephContext *)dest_md_ctx.cct();
    ldout(cct, 20) << "copy " << src->name
		   << " -> " << destname << dendl;

    src->md_lock.get_read();
    uint64_t src_size = src->get_image_size();
    src->md_lock.put_read();

    int r = create(dest_md_ctx, destname, src_size);
    if (r < 0) {
      lderr(cct) << "header creation failed" << dendl;
      return r;
    }

    ImageCtx *dest = new librbd::ImageCtx(destname, dest_md_ctx, false);
    r = open_image(dest);
    if (r < 0) {
      lderr(cct) << "failed to read newly created header" << dendl;
      return r;
    }

    r = copy(src, dest, prog_ctx);
    close_image(dest);
    return r;
  }

  class C_CopyWrite : public Context {
  public:
    C_CopyWrite(SimpleThrottle *throttle, bufferlist *bl)
      : m_throttle(throttle), m_bl(bl) {}
    virtual void finish(int r) {
      delete m_bl;
      m_throttle->end_op(r);
    }
  private:
    SimpleThrottle *m_throttle;
    bufferlist *m_bl;
  };

  class C_CopyRead : public Context {
  public:
    C_CopyRead(SimpleThrottle *throttle, ImageCtx *dest, uint64_t offset,
	       bufferlist *bl)
      : m_throttle(throttle), m_dest(dest), m_offset(offset), m_bl(bl) {
      m_throttle->start_op();
    }
    virtual void finish(int r) {
      if (r < 0) {
	lderr(m_dest->cct) << "error reading from source image at offset "
			   << m_offset << ": " << cpp_strerror(r) << dendl;
	delete m_bl;
	m_throttle->end_op(r);
	return;
      }
      assert(m_bl->length() == (size_t)r);

      if (m_bl->is_zero()) {
	delete m_bl;
	m_throttle->end_op(r);
	return;
      }

      Context *ctx = new C_CopyWrite(m_throttle, m_bl);
      AioCompletion *comp = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_write(m_dest, m_offset, m_bl->length(), m_bl->c_str(), comp);
      if (r < 0) {
	ctx->complete(r);
	comp->release();
	lderr(m_dest->cct) << "error writing to destination image at offset "
			   << m_offset << ": " << cpp_strerror(r) << dendl;
      }
    }
  private:
    SimpleThrottle *m_throttle;
    ImageCtx *m_dest;
    uint64_t m_offset;
    bufferlist *m_bl;
  };

  int copy(ImageCtx *src, ImageCtx *dest, ProgressContext &prog_ctx)
  {
    src->md_lock.get_read();
    uint64_t src_size = src->get_image_size();
    src->md_lock.put_read();

    dest->md_lock.get_read();
    uint64_t dest_size = dest->get_image_size();
    dest->md_lock.put_read();

    CephContext *cct = src->cct;
    if (dest_size < src_size) {
      lderr(cct) << " src size " << src_size << " >= dest size "
		 << dest_size << dendl;
      return -EINVAL;
    }
    int r;
    SimpleThrottle throttle(cct->_conf->rbd_concurrent_management_ops, false);
    uint64_t len = src->io_ctx.op_size();
    for (uint64_t offset = 0; offset < src_size; offset += len) {
      bufferlist *bl = new bufferlist();
      Context *ctx = new C_CopyRead(&throttle, dest, offset, bl);
      AioCompletion *comp = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_read(src, offset, len, NULL, bl, comp);
      if (r < 0) {
	ctx->complete(r);
	comp->release();
	throttle.wait_for_ret();
	lderr(cct) << "could not read from source image from "
		   << offset << " to " << offset + len << ": "
		   << cpp_strerror(r) << dendl;
	return r;
      }
      prog_ctx.update_progress(offset, src_size);
    }

    r = throttle.wait_for_ret();
    if (r >= 0)
      prog_ctx.update_progress(src_size, src_size);
    return r;
  }

  int open_image(ImageCtx *ictx)
  {
    ldout(ictx->cct, 20) << "open_image: ictx = " << ictx
			 << " name = '" << ictx->name
			 << dendl;
    int r = ictx->init();
    if (r < 0)
      return r;

    if (!ictx->read_only) {
      r = ictx->register_watch();
      if (r < 0) {
	lderr(ictx->cct) << "error registering a watch: " << cpp_strerror(r)
			 << dendl;
	goto err_close;
      }
    }

    ictx->md_lock.get_write();
    r = ictx_refresh(ictx);
    ictx->md_lock.put_write();
    if (r < 0)
      goto err_close;

    return 0;

  err_close:
    close_image(ictx);
    return r;
  }

  void close_image(ImageCtx *ictx)
  {
    ldout(ictx->cct, 20) << "close_image " << ictx << dendl;
    if (ictx->object_cacher)
      ictx->shutdown_cache(); // implicitly flushes
    else
      flush(ictx);

    if (ictx->wctx)
      ictx->unregister_watch();

    delete ictx;
  }

  int list_lockers(ImageCtx *ictx,
		   std::list<locker_t> *lockers,
		   bool *exclusive,
		   string *tag)
  {
    ldout(ictx->cct, 20) << "list_locks on image " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    RWLock::RLocker locker(ictx->md_lock);
    if (exclusive)
      *exclusive = ictx->exclusive_locked;
    if (tag)
      *tag = ictx->lock_tag;
    if (lockers) {
      lockers->clear();
      map<rados::cls::lock::locker_id_t,
	  rados::cls::lock::locker_info_t>::const_iterator it;
      for (it = ictx->lockers.begin(); it != ictx->lockers.end(); ++it) {
	locker_t locker;
	locker.client = stringify(it->first.locker);
	locker.cookie = it->first.cookie;
	locker.address = stringify(it->second.addr);
	lockers->push_back(locker);
      }
    }

    return 0;
  }

  int lock(ImageCtx *ictx, bool exclusive, const string& cookie,
	   const string& tag)
  {
    ldout(ictx->cct, 20) << "lock image " << ictx << " exclusive=" << exclusive
			 << " cookie='" << cookie << "' tag='" << tag << "'"
			 << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    /**
     * If we wanted we could do something more intelligent, like local
     * checks that we think we will succeed. But for now, let's not
     * duplicate that code.
     */
    RWLock::RLocker locker(ictx->md_lock);
    r = rados::cls::lock::lock(&ictx->io_ctx, ictx->header_oid, RBD_LOCK_NAME,
			       exclusive ? LOCK_EXCLUSIVE : LOCK_SHARED,
			       cookie, tag, "", utime_t(), 0);
    if (r < 0)
      return r;
    notify_change(ictx->io_ctx, ictx->header_oid, ictx);
    return 0;
  }

  int unlock(ImageCtx *ictx, const string& cookie)
  {
    ldout(ictx->cct, 20) << "unlock image " << ictx
			 << " cookie='" << cookie << "'" << dendl;


    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    RWLock::RLocker locker(ictx->md_lock);
    r = rados::cls::lock::unlock(&ictx->io_ctx, ictx->header_oid,
				 RBD_LOCK_NAME, cookie);
    if (r < 0)
      return r;
    notify_change(ictx->io_ctx, ictx->header_oid, ictx);
    return 0;
  }

  int break_lock(ImageCtx *ictx, const string& client,
		 const string& cookie)
  {
    ldout(ictx->cct, 20) << "break_lock image " << ictx << " client='" << client
			 << "' cookie='" << cookie << "'" << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    entity_name_t lock_client;
    if (!lock_client.parse(client)) {
      lderr(ictx->cct) << "Unable to parse client '" << client
		       << "'" << dendl;
      return -EINVAL;
    }
    RWLock::RLocker locker(ictx->md_lock);
    r = rados::cls::lock::break_lock(&ictx->io_ctx, ictx->header_oid,
				     RBD_LOCK_NAME, cookie, lock_client);
    if (r < 0)
      return r;
    notify_change(ictx->io_ctx, ictx->header_oid, ictx);
    return 0;
  }

  void rbd_ctx_cb(completion_t cb, void *arg)
  {
    Context *ctx = reinterpret_cast<Context *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    ctx->complete(comp->get_return_value());
    comp->release();
  }

  int64_t read_iterate(ImageCtx *ictx, uint64_t off, uint64_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *),
		       void *arg)
  {
    utime_t start_time, elapsed;

    ldout(ictx->cct, 20) << "read_iterate " << ictx << " off = " << off
			 << " len = " << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    uint64_t mylen = len;
    r = clip_io(ictx, off, &mylen);
    if (r < 0)
      return r;

    int64_t total_read = 0;
    uint64_t left = mylen;

    start_time = ceph_clock_now(ictx->cct);
    while (left > 0) {
      uint64_t read_len = ictx->io_ctx.op_size();

      bufferlist bl;

      Mutex mylock;
      Cond cond;
      bool done;
      int ret;

      Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
      AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
      r = aio_read(ictx, off, read_len, NULL, &bl, c);
      if (r < 0) {
	c->release();
	delete ctx;
	return r;
      }

      mylock.Lock();
      while (!done)
	cond.Wait(mylock);
      mylock.Unlock();

      if (ret < 0)
	return ret;

      r = cb(total_read, ret, bl.c_str(), arg);
      if (r < 0)
	return r;

      total_read += ret;
      left -= ret;
      off += ret;
    }

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    return total_read;
  }

  int simple_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg)
  {
    char *dest_buf = (char *)arg;
    if (buf)
      memcpy(dest_buf + ofs, buf, len);
    else
      memset(dest_buf + ofs, 0, len);

    return 0;
  }

  ssize_t read(ImageCtx *ictx, uint64_t ofs, size_t len, char *buf)
  {
    Mutex mylock;
    Cond cond;
    bool done;
    int ret;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    int r = aio_read(ictx, ofs, len, buf, NULL, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    return ret;
  }

  ssize_t write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf)
  {
    utime_t start_time, elapsed;
    ldout(ictx->cct, 20) << "write " << ictx << " off = " << off << " len = "
			 << len << dendl;

    start_time = ceph_clock_now(ictx->cct);
    Mutex mylock;
    Cond cond;
    bool done;
    int ret;

    uint64_t mylen = len;
    int r = clip_io(ictx, off, &mylen);
    if (r < 0)
      return r;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    r = aio_write(ictx, off, mylen, buf, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    if (ret < 0)
      return ret;

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    return mylen;
  }

  int discard(ImageCtx *ictx, uint64_t off, uint64_t len)
  {
    utime_t start_time, elapsed;
    ldout(ictx->cct, 20) << "discard " << ictx << " off = " << off << " len = "
			 << len << dendl;

    start_time = ceph_clock_now(ictx->cct);
    Mutex mylock;
    Cond cond;
    bool done;
    int ret;

    Context *ctx = new C_SafeCond(&mylock, &cond, &done, &ret);
    AioCompletion *c = aio_create_completion_internal(ctx, rbd_ctx_cb);
    int r = aio_discard(ictx, off, len, c);
    if (r < 0) {
      c->release();
      delete ctx;
      return r;
    }

    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();

    if (ret < 0)
      return ret;

    elapsed = ceph_clock_now(ictx->cct) - start_time;
    return len;
  }

  ssize_t handle_sparse_read(CephContext *cct,
			     bufferlist data_bl,
			     uint64_t block_ofs,
			     const map<uint64_t, uint64_t> &data_map,
			     uint64_t buf_ofs,   // offset into buffer
			     size_t buf_len,     // length in buffer (not size of buffer!)
			     char *dest_buf)
  {
    uint64_t bl_ofs = 0;
    size_t buf_left = buf_len;

    for (map<uint64_t, uint64_t>::const_iterator iter = data_map.begin();
	 iter != data_map.end();
	 ++iter) {
      uint64_t extent_ofs = iter->first;
      size_t extent_len = iter->second;

      ldout(cct, 10) << "extent_ofs=" << extent_ofs
		     << " extent_len=" << extent_len << dendl;
      ldout(cct, 10) << "block_ofs=" << block_ofs << dendl;

      /* a hole? */
      if (extent_ofs > block_ofs) {
	uint64_t gap = extent_ofs - block_ofs;
	ldout(cct, 10) << "<1>zeroing " << buf_ofs << "~" << gap << dendl;
	memset(dest_buf + buf_ofs, 0, gap);

	buf_ofs += gap;
	buf_left -= gap;
	block_ofs = extent_ofs;
      } else if (extent_ofs < block_ofs) {
	assert(0 == "osd returned data prior to what we asked for");
	return -EIO;
      }

      if (bl_ofs + extent_len > (buf_ofs + buf_left)) {
	assert(0 == "osd returned more data than we asked for");
	return -EIO;
      }

      /* data */
      ldout(cct, 10) << "<2>copying " << buf_ofs << "~" << extent_len
		     << " from ofs=" << bl_ofs << dendl;
      memcpy(dest_buf + buf_ofs, data_bl.c_str() + bl_ofs, extent_len);

      bl_ofs += extent_len;
      buf_ofs += extent_len;
      assert(buf_left >= extent_len);
      buf_left -= extent_len;
      block_ofs += extent_len;
    }

    /* last hole */
    if (buf_left > 0) {
      ldout(cct, 10) << "<3>zeroing " << buf_ofs << "~" << buf_left << dendl;
      memset(dest_buf + buf_ofs, 0, buf_left);
    }

    return buf_len;
  }

  void rados_req_cb(rados_completion_t c, void *arg)
  {
    AioRequest *req = reinterpret_cast<AioRequest *>(arg);
    req->complete(rados_aio_get_return_value(c));
  }

  void rados_ctx_cb(rados_completion_t c, void *arg)
  {
    Context *comp = reinterpret_cast<Context *>(arg);
    comp->complete(rados_aio_get_return_value(c));
  }

  // validate extent against image size; clip to image size if necessary
  int clip_io(ImageCtx *ictx, uint64_t off, uint64_t *len)
  {
    ictx->md_lock.get_read();
    uint64_t image_size = ictx->get_image_size();
    ictx->md_lock.put_read();

    // special-case "len == 0" requests: always valid
    if (*len == 0)
      return 0;

    // can't start past end
    if (off >= image_size)
      return -EINVAL;

    // clip requests that extend past end to just end
    if ((off + *len) > image_size)
      *len = (size_t)(image_size - off);

    return 0;
  }

  int aio_flush(ImageCtx *ictx, AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_flush " << ictx << " completion " << c <<  dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    ictx->user_flushed();

    c->get();
    c->add_request();
    c->init_time(ictx, AIO_TYPE_FLUSH);
    C_AioWrite *req_comp = new C_AioWrite(cct, c);
    if (ictx->object_cacher) {
      ictx->flush_cache_aio(req_comp);
    } else {
      librados::AioCompletion *rados_completion =
	librados::Rados::aio_create_completion(req_comp, NULL, rados_ctx_cb);
      ictx->io_ctx.aio_flush_async(rados_completion);
      rados_completion->release();
    }
    c->finish_adding_requests(cct);
    c->put();

    return 0;
  }

  int flush(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "flush " << ictx << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    ictx->user_flushed();
    r = _flush(ictx);
    return r;
  }

  int _flush(ImageCtx *ictx)
  {
    CephContext *cct = ictx->cct;
    int r;
    // flush any outstanding writes
    if (ictx->object_cacher) {
      r = ictx->flush_cache();
    } else {
      r = ictx->io_ctx.aio_flush();
    }

    if (r)
      lderr(cct) << "_flush " << ictx << " r = " << r << dendl;

    return r;
  }

  int aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
		AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_write " << ictx << " off = " << off << " len = "
		   << len << " buf = " << (void*)buf << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    uint64_t mylen = len;
    r = clip_io(ictx, off, &mylen);
    if (r < 0)
      return r;

    if (ictx->read_only)
      return -EROFS;

    // map
    c->get();
    c->init_time(ictx, AIO_TYPE_WRITE);
    bufferlist bl;
    bl.append(buf, len);
    C_AioWrite *req_comp = new C_AioWrite(cct, c);
    if (ictx->object_cacher) {
      c->add_request();
      ictx->write_to_cache(ictx->image_oid, bl,
			   len, off, req_comp);
    } else {
      AioWrite *req = new AioWrite(ictx, ictx->image_oid, off,
				   bl, req_comp);
      c->add_request();
      r = req->send();
      if (r < 0)
	goto done;
    }

  done:
    c->finish_adding_requests(ictx->cct);
    c->put();

    return r;
  }

  int aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c)
  {
    CephContext *cct = ictx->cct;
    ldout(cct, 20) << "aio_discard " << ictx << " off = " << off << " len = "
		   << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    r = clip_io(ictx, off, &len);
    if (r < 0)
      return r;

    if (ictx->read_only)
      return -EROFS;

    c->get();
    c->init_time(ictx, AIO_TYPE_DISCARD);
    C_AioWrite *req_comp = new C_AioWrite(cct, c);
    AbstractWrite *req;
    c->add_request();

    if (off + len <= ictx->size) {
      req = new AioTruncate(ictx, ictx->image_oid, off, req_comp);
    } else {
      req = new AioZero(ictx, ictx->image_oid, off, len, req_comp);
    }

    r = req->send();
    if (r < 0)
      goto done;
    r = 0;
  done:
    if (ictx->object_cacher) {
      Mutex::Locker l(ictx->cache_lock);
      ObjectExtent x(ictx->image_oid, off, len, 0);
      vector<ObjectExtent> xs;
      xs.push_back(x);
      ictx->object_cacher->discard_set(ictx->object_set, xs);
    }

    c->finish_adding_requests(ictx->cct);
    c->put();

    /* FIXME: cleanup all the allocated stuff */
    return r;
  }

  void rbd_req_cb(completion_t cb, void *arg)
  {
    AioRequest *req = reinterpret_cast<AioRequest *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    req->complete(comp->get_return_value());
  }

  int aio_read(ImageCtx *ictx, uint64_t off, size_t len,
	       char *buf, bufferlist *bl,
	       AioCompletion *c)
  {
    ldout(ictx->cct, 20) << "aio_read " << ictx << " completion " << c
			 << " " << off << "~" << len << dendl;

    int r = ictx_check(ictx);
    if (r < 0)
      return r;

    r = clip_io(ictx, off, &len);
    if (r < 0)
      return r;

    int64_t ret;

    c->read_buf = buf;
    c->read_buf_len = len;
    c->read_bl = bl;

    c->get();
    c->init_time(ictx, AIO_TYPE_READ);
    C_AioRead *req_comp = new C_AioRead(ictx->cct, c);
    AioRead *req = new AioRead(ictx, ictx->image_oid,
			       off, len, req_comp);
    req_comp->set_req(req);
    c->add_request();

    if (ictx->object_cacher) {
      C_CacheRead *cache_comp = new C_CacheRead(req);
      ictx->aio_read_from_cache(ictx->image_oid, &req->data(),
				len, off, cache_comp);
    } else {
      r = req->send();
      if (r < 0 && r == -ENOENT)
	r = 0;
      if (r < 0) {
	ret = r;
	goto done;
      }
    }

    ret = len;
  done:
    c->finish_adding_requests(ictx->cct);
    c->put();

    return ret;
  }

  AioCompletion *aio_create_completion() {
    AioCompletion *c = new AioCompletion();
    return c;
  }

  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete) {
    AioCompletion *c = new AioCompletion();
    c->set_complete_cb(cb_arg, cb_complete);
    return c;
  }

  AioCompletion *aio_create_completion_internal(void *cb_arg,
						callback_t cb_complete) {
    AioCompletion *c = aio_create_completion(cb_arg, cb_complete);
    c->rbd_comp = c;
    return c;
  }
}
