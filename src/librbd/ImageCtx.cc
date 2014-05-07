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
  ImageCtx::ImageCtx(const string &image_name, const string &image_id,
		     IoCtx& p, bool ro)
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
      extra_read_flags(0),
      old_format(true),
      order(0), size(0), features(0),
      format_string(NULL),
      id(image_id),
      stripe_unit(0), stripe_count(0),
      object_cacher(NULL), writeback_handler(NULL), object_set(NULL)
  {
    md_ctx.dup(p);
    data_ctx.dup(p);

    memset(&header, 0, sizeof(header));
    memset(&layout, 0, sizeof(layout));

    string pname = string("librbd-") + id + string("-") +
      data_ctx.get_pool_name() + string("/") + name;
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
      object_set = new ObjectCacher::ObjectSet(NULL, data_ctx.get_id(), 0);
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
    delete[] format_string;
  }

  int ImageCtx::init() {
    int r;
    if (id.length()) {
      old_format = false;
    } else {
      r = detect_format(md_ctx, name, &old_format, NULL);
      if (r < 0) {
	lderr(cct) << "error finding header: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    if (!old_format) {
      if (!id.length()) {
	r = cls_client::get_id(&md_ctx, id_obj_name(name), &id);
	if (r < 0) {
	  lderr(cct) << "error reading image id: " << cpp_strerror(r)
		     << dendl;
	  return r;
	}
      }

      header_oid = header_name(id);
      r = cls_client::get_immutable_metadata(&md_ctx, header_oid,
					     &object_prefix, &order);
      if (r < 0) {
	lderr(cct) << "error reading immutable metadata: "
		   << cpp_strerror(r) << dendl;
	return r;
      }

      r = cls_client::get_stripe_unit_count(&md_ctx, header_oid,
					    &stripe_unit, &stripe_count);
      if (r < 0 && r != -ENOEXEC && r != -EINVAL) {
	lderr(cct) << "error reading striping metadata: "
		   << cpp_strerror(r) << dendl;
	return r;
      }

      init_layout();
    } else {
      header_oid = old_header_name(name);
    }
    return 0;
  }
  
  void ImageCtx::init_layout()
  {
    if (stripe_unit == 0 || stripe_count == 0) {
      stripe_unit = 1ull << order;
      stripe_count = 1;
    }

    memset(&layout, 0, sizeof(layout));
    layout.fl_stripe_unit = stripe_unit;
    layout.fl_stripe_count = stripe_count;
    layout.fl_object_size = 1ull << order;
    layout.fl_pg_pool = data_ctx.get_id();  // FIXME: pool id overflow?

    delete[] format_string;
    size_t len = object_prefix.length() + 16;
    format_string = new char[len];
    if (old_format) {
      snprintf(format_string, len, "%s.%%012llx", object_prefix.c_str());
    } else {
      snprintf(format_string, len, "%s.%%016llx", object_prefix.c_str());
    }

    // size object cache appropriately
    if (object_cacher) {
      uint64_t obj = cct->_conf->rbd_cache_size / (1ull << order);
      ldout(cct, 10) << " cache bytes " << cct->_conf->rbd_cache_size << " order " << (int)order
		     << " -> about " << obj << " objects" << dendl;
      object_cacher->set_max_objects(obj * 4 + 10);
    }

    ldout(cct, 10) << "init_layout stripe_unit " << stripe_unit
		   << " stripe_count " << stripe_count
		   << " object_size " << layout.fl_object_size
		   << " prefix " << object_prefix
		   << " format " << format_string
		   << dendl;
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

  void ImageCtx::set_read_flag(unsigned flag) {
    extra_read_flags |= flag;
  }

  int ImageCtx::get_read_flags() {
    return librados::OPERATION_NOFLAG | extra_read_flags;
  }

  uint64_t ImageCtx::get_current_size() const
  {
    return size;
  }

  uint64_t ImageCtx::get_object_size() const
  {
    return 1ull << order;
  }

  string ImageCtx::get_object_name(uint64_t num) const {
    char buf[object_prefix.length() + 32];
    snprintf(buf, sizeof(buf), format_string, num);
    return string(buf);
  }

  uint64_t ImageCtx::get_stripe_unit() const
  {
    return stripe_unit;
  }

  uint64_t ImageCtx::get_stripe_count() const
  {
    return stripe_count;
  }

  uint64_t ImageCtx::get_stripe_period() const
  {
    return stripe_count * (1ull << order);
  }

  uint64_t ImageCtx::get_num_objects() const
  {
    uint64_t period = get_stripe_period();
    uint64_t num_periods = (size + period - 1) / period;
    return num_periods * stripe_count;
  }

  uint64_t ImageCtx::get_image_size() const
  {
    return size;
  }

  int ImageCtx::get_features(uint64_t *out_features) const
  {
    *out_features = features;
    return 0;
  }

  void ImageCtx::aio_read_from_cache(object_t o, bufferlist *bl, size_t len,
				     uint64_t off, Context *onfinish) {
    ObjectCacher::OSDRead *rd = object_cacher->prepare_read(CEPH_NOSNAP, bl, 0);
    ObjectExtent extent(o, 0 /* a lie */, off, len, 0);
    extent.oloc.pool = data_ctx.get_id();
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
      = object_cacher->prepare_write(::SnapContext(), bl, utime_t(), 0);
    ObjectExtent extent(o, 0, off, len, 0);
    extent.oloc.pool = data_ctx.get_id();
    // XXX: nspace is always default, io_ctx_impl field private
    //extent.oloc.nspace = data_ctx.io_ctx_impl->oloc.nspace;
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
