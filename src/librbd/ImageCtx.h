// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

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

#include "cls/lock/cls_lock_client.h"
#include "librbd/LibrbdWriteback.h"

class CephContext;

namespace librbd {

  class WatchCtx;

  struct ImageCtx {
    CephContext *cct;
    struct rbd_obj_header_ondisk header;
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    bool flush_encountered;

    std::map<rados::cls::lock::locker_id_t,
	     rados::cls::lock::locker_info_t> lockers;
    bool exclusive_locked;
    std::string lock_tag;

    std::string name;
    IoCtx io_ctx;
    WatchCtx *wctx;
    int refresh_seq;    ///< sequence for refresh requests
    int last_refresh;   ///< last completed refresh

    /**
     * Lock ordering:
     * md_lock, cache_lock, refresh_lock
     */
    RWLock md_lock; // protects access to the mutable image metadata
		    // that isn't guarded by other locks below (size,
		    // image locks, etc)
    Mutex cache_lock; // used as client_lock for the ObjectCacher
    Mutex refresh_lock; // protects refresh_seq and last_refresh

    uint64_t size;
    std::string header_obj;
    std::string image_obj;

#if 0
//FIXME!  ObjectCacher *object_cacher;
#endif
    LibrbdWriteback *writeback_handler;
    ObjectCacher::ObjectSet *object_set;

    ImageCtx(const std::string &image_name, IoCtx& p, bool read_only);
    ~ImageCtx();
    int init();

    uint64_t get_current_size() const;
    uint64_t get_image_size() const;

#if 0
//FIXME! needs OSDRead&etc... -- Dan? Adam?  -mdw 20150105
    void aio_read_from_cache(oid o, bufferlist *bl, size_t len,
			     uint64_t off, Context *onfinish);
    void write_to_cache(oid o, bufferlist& bl, size_t len, uint64_t off,
			Context *onfinish);
    int read_from_cache(oid o, bufferlist *bl, size_t len, uint64_t off);
#endif
    void user_flushed();
#if 0
    void flush_cache_aio(Context *onfinish);
//FIXME!    int flush_cache();
#endif
    void shutdown_cache();
    void invalidate_cache();
    void clear_nonexistence_cache();
    int register_watch();
    void unregister_watch();
  };
}

#endif
