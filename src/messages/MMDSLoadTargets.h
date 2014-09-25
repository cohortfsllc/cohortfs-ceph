// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMDSLoadTargets_H
#define CEPH_MMDSLoadTargets_H

#include "msg/Message.h"
#include "messages/PaxosServiceMessage.h"
#include "include/types.h"

#include <map>
using std::map;

class MMDSLoadTargets : public PaxosServiceMessage {
 public:
  uint64_t global_id;
  set<int32_t> targets;

  MMDSLoadTargets() : PaxosServiceMessage(MSG_MDS_OFFLOAD_TARGETS, 0) {}

  MMDSLoadTargets(uint64_t g, set<int32_t>& mds_targets) :
    PaxosServiceMessage(MSG_MDS_OFFLOAD_TARGETS, 0),
    global_id(g), targets(mds_targets) {}
private:
  ~MMDSLoadTargets() {}
  template <typename T>
  void _print(T& o) const {
    o << "mds_load_targets(" << global_id << " " << targets << ")";
  }

public:
  const char* get_type_name() const { return "mds_load_targets"; }

  void print(ostream& out) const { _print(out); }
  void print(lttng_stream& out) const { _print(out); }  

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(global_id, p);
    ::decode(targets, p);
  }

  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(global_id, payload);
    ::encode(targets, payload);
  }
};

#endif
