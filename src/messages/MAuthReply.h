// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_MAUTHREPLY_H
#define CEPH_MAUTHREPLY_H

#include "msg/Message.h"
#include "common/errno.h"

struct MAuthReply : public Message {
  uint32_t protocol;
  int32_t result;
  uint64_t global_id;      // if zero, meaningless
  string result_msg;
  bufferlist result_bl;

  MAuthReply() : Message(CEPH_MSG_AUTH_REPLY), protocol(0), result(0), global_id(0) {}
  MAuthReply(uint32_t p, bufferlist *bl = NULL, int r = 0, uint64_t gid=0, const char *msg = "") :
    Message(CEPH_MSG_AUTH_REPLY),
    protocol(p), result(r), global_id(gid),
    result_msg(msg) {
    if (bl)
      result_bl = *bl;
  }
private:
  ~MAuthReply() {}

  template <typename T>
  void _print(T& o) const {
    o << "auth_reply(proto " << protocol << " " << result << " " << cpp_strerror(result);
    if (result_msg.length())
      o << ": " << result_msg;
    o << ")";
  }

public:
  const char *get_type_name() const { return "auth_reply"; }

  void print(ostream& out) const { _print(out); }
  void print(lttng_stream& out) const { _print(out); }  

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(protocol, p);
    ::decode(result, p);
    ::decode(global_id, p);
    ::decode(result_bl, p);
    ::decode(result_msg, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(protocol, payload);
    ::encode(result, payload);
    ::encode(global_id, payload);
    ::encode(result_bl, payload);
    ::encode(result_msg, payload);
  }
};

#endif
