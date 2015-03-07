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

#include <sstream>
#include <stdint.h>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <include/ceph_time.h>
#include "msg/Message.h"

namespace bi = boost::intrusive;

/**
 * osd request identifier
 *
 * caller name + incarnation# + tid to unique identify this request.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  ceph_tid_t tid;
  int32_t       inc;  // incarnation

  osd_reqid_t()
    : tid(0), inc(0) {}
  osd_reqid_t(const entity_name_t& a, int i, ceph_tid_t t)
    : name(a), tid(t), inc(i) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<osd_reqid_t*>& o);
};
WRITE_CLASS_ENCODER(osd_reqid_t)

/**
 * The OpRequest takes in a Message* and takes over a single reference
 * to it, which it puts() when destroyed.
 */
class OpRequest {
public:
  typedef boost::intrusive_ptr<OpRequest> Ref;
  std::atomic<uint64_t> nref;

  /* is queuable */
  typedef bi::link_mode<bi::safe_link> link_mode; // for debugging
  bi::list_member_hook<link_mode> q_hook;

  typedef bi::list<OpRequest,
		   bi::member_hook<
		     OpRequest, bi::list_member_hook<link_mode>,
		     &OpRequest::q_hook>, bi::constant_time_size<true>
		   > Queue;

  // rmw flags
  int rmw_flags;

  static Ref create_request(Message *ref);

  void get() { ++nref; }

  void put() {
    if (--nref == 0)
      delete this;
  }

  // XXX why not inline?
  bool check_rmw(int flag);
  bool may_read();
  bool may_write();
  bool may_cache();
  bool need_read_cap();
  bool need_write_cap();
  bool need_class_read_cap();
  bool need_class_write_cap();
  void set_read();
  void set_write();
  void set_cache();
  void set_class_read();
  void set_class_write();

  Message *get_req() const { return request; }

  uint64_t get_k() const {
    return request->get_recv_stamp().time_since_epoch().count();
  }

private:
  Message *request;
  osd_reqid_t reqid;
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  ceph::real_time dequeued_time;
  static const uint8_t flag_queued_for_vol = 1 << 0;
  static const uint8_t flag_reached_vol = 1 << 1;
  static const uint8_t flag_delayed = 1 << 2;
  static const uint8_t flag_started = 1 << 3;
  static const uint8_t flag_sub_op_sent = 1 << 4;
  static const uint8_t flag_commit_sent = 1 << 5;

  OpRequest(Message *req);

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

  osd_reqid_t get_reqid() const {
    return reqid;
  }
  
  ~OpRequest() { request->put(); }
  
};

inline void intrusive_ptr_add_ref(OpRequest* op) { op->get(); }
inline void intrusive_ptr_release(OpRequest* op) { op->put(); }

typedef OpRequest::Ref OpRequestRef;

#endif /* OPREQUEST_H_ */
