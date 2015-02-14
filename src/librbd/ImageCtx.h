// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include <mutex>
#include <shared_mutex>
#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd_types.h"
#include "include/types.h"

#include "cls/lock/cls_lock_client.h"

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
     * md_lock, refresh_lock
     */
    std::shared_timed_mutex md_lock; // protects access to the mutable
				     // image metadata that isn't
				     // guarded by other locks below
				     // (size, image locks, etc)
    typedef std::shared_lock<std::shared_timed_mutex> shared_md_lock;
    typedef std::unique_lock<std::shared_timed_mutex> unique_md_lock;
    std::mutex refresh_lock; // protects refresh_seq and last_refresh
    typedef std::lock_guard<std::mutex> lock_guard;
    typedef std::unique_lock<std::mutex> unique_lock;


    uint64_t size;
    std::string header_obj;
    std::string image_obj;

    ImageCtx(const std::string &image_name, IoCtx& p, bool read_only);
    ~ImageCtx();
    int init();

    uint64_t get_current_size() const;
    uint64_t get_image_size() const;

    int register_watch();
    void unregister_watch();
  };
}

#endif
