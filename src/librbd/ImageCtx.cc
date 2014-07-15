// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"

#include "librbd/internal.h"
#include "librbd/WatchCtx.h"

#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageCtx: "

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using librados::IoCtx;

namespace librbd {
  ImageCtx::ImageCtx(const string &image_name, IoCtx& p, bool ro)
    : cct((CephContext*)p.cct()),
      perfcounter(NULL),
      read_only(ro),
      flush_encountered(false),
      exclusive_locked(false),
      name(image_name),
      wctx(NULL),
      refresh_seq(0),
      last_refresh(0),
      md_lock("librbd::ImageCtx::md_lock"),
      cache_lock("librbd::ImageCtx::cache_lock"),
      refresh_lock("librbd::ImageCtx::refresh_lock"),
      size(0),
      object_cacher(NULL), writeback_handler(NULL), object_set(NULL)
  {
    md_ctx.dup(p);
    data_ctx.dup(p);

    memset(&header, 0, sizeof(header));

    string pname = string("librbd-") + string("-") +
      data_ctx.get_volume_name() + string("/") + name;
    perf_start(pname);

    if (cct->_conf->rbd_cache) {
      Mutex::Locker l(cache_lock);
      ldout(cct, 20) << "enabling caching..." << dendl;
      writeback_handler = new LibrbdWriteback(this, cache_lock);

      uint64_t init_max_dirty = cct->_conf->rbd_cache_max_dirty;
      if (cct->_conf->rbd_cache_writethrough_until_flush)
	init_max_dirty = 0;
      ldout(cct, 20) << "Initial cache settings:"
		     << " size=" << cct->_conf->rbd_cache_size
		     << " num_objects=" << 10
		     << " max_dirty=" << init_max_dirty
		     << " target_dirty=" << cct->_conf->rbd_cache_target_dirty
		     << " max_dirty_age="
		     << cct->_conf->rbd_cache_max_dirty_age << dendl;

      object_cacher = new ObjectCacher(cct, pname, *writeback_handler, cache_lock,
				       NULL, NULL,
				       cct->_conf->rbd_cache_size,
				       10,  /* reset this in init */
				       init_max_dirty,
				       cct->_conf->rbd_cache_target_dirty,
				       cct->_conf->rbd_cache_max_dirty_age,
				       cct->_conf->rbd_cache_block_writes_upfront);
      object_set = new ObjectCacher::ObjectSet(NULL, data_ctx.get_volume(), 0);
      object_set->return_enoent = true;
      object_cacher->start();
    }
  }

  ImageCtx::~ImageCtx() {
    perf_stop();
    if (object_cacher) {
      delete object_cacher;
      object_cacher = NULL;
    }
    if (writeback_handler) {
      delete writeback_handler;
      writeback_handler = NULL;
    }
    if (object_set) {
      delete object_set;
      object_set = NULL;
    }
  }

  int ImageCtx::init() {
    int r;

    r = check_exists(md_ctx, name, NULL);
    if (r < 0) {
      lderr(cct) << "error finding header: " << cpp_strerror(r) << dendl;
      return r;
    }
    header_oid = header_name(name);
    image_oid = image_name(name);
    return 0;
  }

  void ImageCtx::perf_start(string name) {
    PerfCountersBuilder plb(cct, name, l_librbd_first, l_librbd_last);

    plb.add_u64_counter(l_librbd_rd, "rd");
    plb.add_u64_counter(l_librbd_rd_bytes, "rd_bytes");
    plb.add_time_avg(l_librbd_rd_latency, "rd_latency");
    plb.add_u64_counter(l_librbd_wr, "wr");
    plb.add_u64_counter(l_librbd_wr_bytes, "wr_bytes");
    plb.add_time_avg(l_librbd_wr_latency, "wr_latency");
    plb.add_u64_counter(l_librbd_discard, "discard");
    plb.add_u64_counter(l_librbd_discard_bytes, "discard_bytes");
    plb.add_time_avg(l_librbd_discard_latency, "discard_latency");
    plb.add_u64_counter(l_librbd_flush, "flush");
    plb.add_u64_counter(l_librbd_aio_rd, "aio_rd");
    plb.add_u64_counter(l_librbd_aio_rd_bytes, "aio_rd_bytes");
    plb.add_time_avg(l_librbd_aio_rd_latency, "aio_rd_latency");
    plb.add_u64_counter(l_librbd_aio_wr, "aio_wr");
    plb.add_u64_counter(l_librbd_aio_wr_bytes, "aio_wr_bytes");
    plb.add_time_avg(l_librbd_aio_wr_latency, "aio_wr_latency");
    plb.add_u64_counter(l_librbd_aio_discard, "aio_discard");
    plb.add_u64_counter(l_librbd_aio_discard_bytes, "aio_discard_bytes");
    plb.add_time_avg(l_librbd_aio_discard_latency, "aio_discard_latency");
    plb.add_u64_counter(l_librbd_aio_flush, "aio_flush");
    plb.add_time_avg(l_librbd_aio_flush_latency, "aio_flush_latency");
    plb.add_u64_counter(l_librbd_notify, "notify");
    plb.add_u64_counter(l_librbd_resize, "resize");

    perfcounter = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perfcounter);
  }

  void ImageCtx::perf_stop() {
    assert(perfcounter);
    cct->get_perfcounters_collection()->remove(perfcounter);
    delete perfcounter;
  }

  uint64_t ImageCtx::get_current_size() const
  {
    return size;
  }

  uint64_t ImageCtx::get_image_size() const
  {
    return size;
  }

  void ImageCtx::aio_read_from_cache(object_t o, bufferlist *bl, size_t len,
				     uint64_t off, Context *onfinish) {
    ObjectCacher::OSDRead *rd = object_cacher->prepare_read(bl, 0);
    ObjectExtent extent(o, off, len, 0);
    extent.buffer_extents.push_back(make_pair(0, len));
    rd->extents.push_back(extent);
    cache_lock.Lock();
    int r = object_cacher->readx(rd, object_set, onfinish);
    cache_lock.Unlock();
    if (r != 0)
      onfinish->complete(r);
  }

  void ImageCtx::write_to_cache(object_t o, bufferlist& bl, size_t len,
				uint64_t off, Context *onfinish) {
    ObjectCacher::OSDWrite *wr
      = object_cacher->prepare_write(bl, utime_t(), 0);
    ObjectExtent extent(o, off, len, 0);
    extent.buffer_extents.push_back(make_pair(0, len));
    wr->extents.push_back(extent);
    {
      Mutex::Locker l(cache_lock);
      object_cacher->writex(wr, object_set, cache_lock, onfinish);
    }
  }

  int ImageCtx::read_from_cache(object_t o, bufferlist *bl, size_t len,
				uint64_t off) {
    int r;
    Mutex mylock("librbd::ImageCtx::read_from_cache");
    Cond cond;
    bool done;
    Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
    aio_read_from_cache(o, bl, len, off, onfinish);
    mylock.Lock();
    while (!done)
      cond.Wait(mylock);
    mylock.Unlock();
    return r;
  }

  void ImageCtx::user_flushed() {
    if (object_cacher && cct->_conf->rbd_cache_writethrough_until_flush) {
      md_lock.get_read();
      bool flushed_before = flush_encountered;
      md_lock.put_read();

      uint64_t max_dirty = cct->_conf->rbd_cache_max_dirty;
      if (!flushed_before && max_dirty > 0) {
	md_lock.get_write();
	flush_encountered = true;
	md_lock.put_write();

	ldout(cct, 10) << "saw first user flush, enabling writeback" << dendl;
	Mutex::Locker l(cache_lock);
	object_cacher->set_max_dirty(max_dirty);
      }
    }
  }

  void ImageCtx::flush_cache_aio(Context *onfinish) {
    cache_lock.Lock();
    object_cacher->flush_set(object_set, onfinish);
    cache_lock.Unlock();
  }

  int ImageCtx::flush_cache() {
    int r = 0;
    Mutex mylock("librbd::ImageCtx::flush_cache");
    Cond cond;
    bool done;
    Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
    flush_cache_aio(onfinish);
    mylock.Lock();
    while (!done) {
      ldout(cct, 20) << "waiting for cache to be flushed" << dendl;
      cond.Wait(mylock);
    }
    mylock.Unlock();
    ldout(cct, 20) << "finished flushing cache" << dendl;
    return r;
  }

  void ImageCtx::shutdown_cache() {
    md_lock.get_write();
    invalidate_cache();
    md_lock.put_write();
    object_cacher->stop();
  }

  void ImageCtx::invalidate_cache() {
    if (!object_cacher)
      return;
    cache_lock.Lock();
    object_cacher->release_set(object_set);
    cache_lock.Unlock();
    int r = flush_cache();
    if (r)
      lderr(cct) << "flush_cache returned " << r << dendl;
    cache_lock.Lock();
    bool unclean = object_cacher->release_set(object_set);
    cache_lock.Unlock();
    if (unclean)
      lderr(cct) << "could not release all objects from cache" << dendl;
  }

  void ImageCtx::clear_nonexistence_cache() {
    if (!object_cacher)
      return;
    cache_lock.Lock();
    object_cacher->clear_nonexistence(object_set);
    cache_lock.Unlock();
  }

  int ImageCtx::register_watch() {
    assert(!wctx);
    wctx = new WatchCtx(this);
    return md_ctx.watch(header_oid, 0, &(wctx->cookie), wctx);
  }

  void ImageCtx::unregister_watch() {
    assert(wctx);
    wctx->invalidate();
    md_ctx.unwatch(header_oid, wctx->cookie);
    delete wctx;
    wctx = NULL;
  }
}
