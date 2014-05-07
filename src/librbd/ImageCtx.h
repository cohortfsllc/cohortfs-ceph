// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include "include/int_types.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "include/types.h"
#include "osdc/ObjectCacher.h"

#include "cls/rbd/cls_rbd_client.h"
#include "librbd/LibrbdWriteback.h"

class CephContext;
class PerfCounters;

namespace librbd {

  class WatchCtx;

  struct ImageCtx {
    CephContext *cct;
    PerfCounters *perfcounter;
    struct rbd_obj_header_ondisk header;
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    bool flush_encountered;

    std::map<rados::cls::lock::locker_id_t,
	     rados::cls::lock::locker_info_t> lockers;
    bool exclusive_locked;
    std::string lock_tag;

    std::string name;
    IoCtx data_ctx, md_ctx;
    WatchCtx *wctx;
    int refresh_seq;    ///< sequence for refresh requests
    int last_refresh;   ///< last completed refresh

    /**
     * Lock ordering:
     * md_lock, cache_lock, refresh_lock
     */
    RWLock md_lock; // protects access to the mutable image metadata that
                   // isn't guarded by other locks below
                   // (size, features, image locks, etc)
    Mutex cache_lock; // used as client_lock for the ObjectCacher
    Mutex refresh_lock; // protects refresh_seq and last_refresh

    unsigned extra_read_flags;

    bool old_format;
    uint8_t order;
    uint64_t size;
    uint64_t features;
    std::string object_prefix;
    char *format_string;
    std::string header_oid;
    std::string id; // only used for new-format images
    uint64_t stripe_unit, stripe_count;

    ceph_file_layout layout;

    ObjectCacher *object_cacher;
    LibrbdWriteback *writeback_handler;
    ObjectCacher::ObjectSet *object_set;

    /**
     * Either image_name or image_id must be set.
     * If id is not known, pass the empty std::string,
     * and init() will look it up.
     */
    ImageCtx(const std::string &image_name, const std::string &image_id,
	     IoCtx& p, bool read_only);
    ~ImageCtx();
    int init();
    void init_layout();
    void perf_start(std::string name);
    void perf_stop();
    void set_read_flag(unsigned flag);
    int get_read_flags();

    uint64_t get_current_size() const;
    uint64_t get_object_size() const;
    string get_object_name(uint64_t num) const;
    uint64_t get_num_objects() const;
    uint64_t get_image_size() const;
    uint64_t get_stripe_unit() const;
    uint64_t get_stripe_count() const;
    uint64_t get_stripe_period() const;

    int get_features(uint64_t *out_features) const;
    void aio_read_from_cache(object_t o, bufferlist *bl, size_t len,
			     uint64_t off, Context *onfinish);
    void write_to_cache(object_t o, bufferlist& bl, size_t len, uint64_t off,
			Context *onfinish);
    int read_from_cache(object_t o, bufferlist *bl, size_t len, uint64_t off);
    void user_flushed();
    void flush_cache_aio(Context *onfinish);
    int flush_cache();
    void shutdown_cache();
    void invalidate_cache();
    void clear_nonexistence_cache();
    int register_watch();
    void unregister_watch();
  };
}

#endif
