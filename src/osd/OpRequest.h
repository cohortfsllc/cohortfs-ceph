// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef OPREQUEST_H_
#define OPREQUEST_H_

#include <stdint.h>

#include <boost/intrusive/list.hpp>
#include "include/ceph_time.h"
#include "common/freelist.h"
#include "messages/MOSDOp.h"
#include "OpContext.h"

namespace bi = boost::intrusive;


class OpRequest : public MOSDOp {
 private:
  static cohort::FreeList<OpRequest> free_list;
 public:
  static void *operator new(size_t num_bytes) {
    return free_list.alloc();
  }
  void operator delete(void *p) {
    return free_list.free(static_cast<OpRequest*>(p));
  }

public:
  typedef boost::intrusive_ptr<OpRequest> Ref;

  /* is queuable */
  typedef bi::link_mode<bi::safe_link> link_mode; // for debugging
  bi::list_member_hook<link_mode> q_hook;

  typedef bi::list<OpRequest,
		   bi::member_hook<
		     OpRequest, bi::list_member_hook<link_mode>,
		     &OpRequest::q_hook>, bi::constant_time_size<true>
		   > Queue;

  OpContext context;

  // rmw flags
  int rmw_flags;

  bool check_rmw(int flag) {
    return rmw_flags & flag;
  }

  OpRequest() : rmw_flags(0), hit_flag_points(0), latest_flag_point(0) {}
  OpRequest(int inc, long tid, const oid_t& obj,
            const boost::uuids::uuid& volume,
            epoch_t epoch, int flags)
    : MOSDOp(inc, tid, obj, volume, epoch, flags),
      rmw_flags(0), hit_flag_points(0), latest_flag_point(0)
  {}

  bool may_read() { return need_read_cap() || need_class_read_cap(); }
  bool may_write() { return need_write_cap() || need_class_write_cap(); }
  bool may_cache() { return check_rmw(CEPH_OSD_RMW_FLAG_CACHE); }

  bool need_read_cap() {
    return check_rmw(CEPH_OSD_RMW_FLAG_READ);
  }

  bool need_write_cap() {
    return check_rmw(CEPH_OSD_RMW_FLAG_WRITE);
  }

  bool need_class_read_cap() {
    return check_rmw(CEPH_OSD_RMW_FLAG_CLASS_READ);
  }

  bool need_class_write_cap() { 
    return check_rmw(CEPH_OSD_RMW_FLAG_CLASS_WRITE);
  }

  void set_read() { rmw_flags |= CEPH_OSD_RMW_FLAG_READ; }
  void set_write() { rmw_flags |= CEPH_OSD_RMW_FLAG_WRITE; }
  void set_class_read() { rmw_flags |= CEPH_OSD_RMW_FLAG_CLASS_READ; }
  void set_class_write() { rmw_flags |= CEPH_OSD_RMW_FLAG_CLASS_WRITE; }
  void set_cache() { rmw_flags |= CEPH_OSD_RMW_FLAG_CACHE; }

private:
  ~OpRequest() {}
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  ceph::real_time dequeued_time;
  static const uint8_t flag_queued_for_vol = 1 << 0;
  static const uint8_t flag_reached_vol = 1 << 1;
  static const uint8_t flag_delayed = 1 << 2;
  static const uint8_t flag_started = 1 << 3;
  static const uint8_t flag_sub_op_sent = 1 << 4;
  static const uint8_t flag_commit_sent = 1 << 5;

public:
  ZTracer::Trace trace; // zipkin trace

  bool been_queued_for_vol() { return hit_flag_points & flag_queued_for_vol; }
  bool been_reached_vol() { return hit_flag_points & flag_reached_vol; }
  bool been_delayed() { return hit_flag_points & flag_delayed; }
  bool been_started() { return hit_flag_points & flag_started; }
  bool been_sub_op_sent() { return hit_flag_points & flag_sub_op_sent; }
  bool been_commit_sent() { return hit_flag_points & flag_commit_sent; }
  bool currently_queued_for_vol() {
    return latest_flag_point & flag_queued_for_vol; }
  bool currently_reached_vol() { return latest_flag_point & flag_reached_vol; }
  bool currently_delayed() { return latest_flag_point & flag_delayed; }
  bool currently_started() { return latest_flag_point & flag_started; }
  bool currently_sub_op_sent() { return latest_flag_point & flag_sub_op_sent; }
  bool currently_commit_sent() { return latest_flag_point & flag_commit_sent; }

  const char *state_string() const {
    switch(latest_flag_point) {
    case flag_queued_for_vol: return "queued for volume";
    case flag_reached_vol: return "reached volume";
    case flag_delayed: return "delayed";
    case flag_started: return "started";
    case flag_sub_op_sent: return "waiting for sub ops";
    case flag_commit_sent: return "commit sent; apply or cleanup";
    default: break;
    }
    return "no flag points reached";
  }

  void mark_queued_for_vol() {
    hit_flag_points |= flag_queued_for_vol;
    latest_flag_point = flag_queued_for_vol;
  }

  void mark_reached_vol() {
    hit_flag_points |= flag_reached_vol;
    latest_flag_point = flag_reached_vol;
  }

  void mark_delayed(string s) {
    hit_flag_points |= flag_delayed;
    latest_flag_point = flag_delayed;
  }

  void mark_started() {
    hit_flag_points |= flag_started;
    latest_flag_point = flag_started;
  }

  void mark_sub_op_sent(string s) {
    hit_flag_points |= flag_sub_op_sent;
    latest_flag_point = flag_sub_op_sent;
  }

  void mark_commit_sent() {
    hit_flag_points |= flag_commit_sent;
    latest_flag_point = flag_commit_sent;
  }

  ceph::real_time get_dequeued_time() const {
    return dequeued_time;
  }

  void set_dequeued_time(ceph::real_time deq_time) {
    dequeued_time = deq_time;
  }
};

inline void intrusive_ptr_add_ref(OpRequest* op) { op->get(); }
inline void intrusive_ptr_release(OpRequest* op) { op->put(); }

typedef OpRequest::Ref OpRequestRef;

#endif /* OPREQUEST_H_ */
