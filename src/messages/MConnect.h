// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_CONNECT_H
#define CEPH_MSG_CONNECT_H

#include "msg/Message.h"
#include "msg/msg_types.h"

/*
 * A family of messages designed to formalize Ceph transport session
 * negotiation for XioMessenger, but potentially re-usable.
 */
class MConnect : public Message {
public:
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  std::string banner;   /* ceph banner */
  uint64_t features;     /* supported feature bits */
  __u32 host_type;    /* CEPH_ENTITY_TYPE_* */
  __u32 global_seq;   /* count connections initiated by this host */
  __u32 connect_seq;  /* count connections initiated in this session */
  __u32 protocol_version;
  __u32 authorizer_protocol;
  __u32 authorizer_len;

  entity_addr_t addr;
  entity_name_t name;
  uint64_t last_in_seq;
  uint64_t last_out_seq;
  __u32 reply_code; // Ceph Messenger tag (e.g., CEPH_MSGR_TAG_FEATURES)
  __u32 flags;          /* CEPH_MSG_CONNECT_* */

  MConnect()
    : Message(MSG_CONNECT, HEAD_VERSION, COMPAT_VERSION)
    {}

  ~MConnect() {}

  void encode_payload(uint64_t _features) {
    ::encode(CEPH_BANNER, payload);
    ::encode(features, payload);
    ::encode(host_type, payload);
    ::encode(global_seq, payload);
    ::encode(connect_seq, payload);
    ::encode(protocol_version, payload);
    ::encode(authorizer_protocol, payload);
    ::encode(authorizer_len, payload);
    ::encode(addr, payload);
    ::encode(name, payload);
    ::encode(last_in_seq, payload);
    ::encode(last_out_seq, payload);
    ::encode(reply_code, payload);
    ::encode(flags, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(banner, p);
    ::decode(features, p);
    ::decode(host_type, p);
    ::decode(global_seq, p);
    ::decode(connect_seq, p);
    ::decode(protocol_version, p);
    ::decode(authorizer_protocol, p);
    ::decode(authorizer_len, p);
    ::decode(addr, p);
    ::decode(name, p);
    ::decode(last_in_seq, p);
    ::decode(last_out_seq, p);
    ::decode(reply_code, p);
    ::decode(flags, p);
  }

  const char *get_type_name() const { return "MConnect"; }

  void print(ostream& out) const {
    out << get_type_name() << " ";
  }
}; /* MConnect */


class MConnectReply : public Message {
public:

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  std::string banner;   /* ceph banner */
  uint64_t features;     /* supported feature bits */
  __u32 host_type;    /* CEPH_ENTITY_TYPE_* */
  __u32 global_seq;   /* count connections initiated by this host */
  __u32 connect_seq;  /* count connections initiated in this session */
  __u32 protocol_version;
  __u32 authorizer_protocol;
  __u32 authorizer_len;

  entity_addr_t addr;
  entity_name_t name;
  uint64_t last_in_seq;
  uint64_t last_out_seq;
  __u32 reply_code; // Ceph Messenger tag (e.g., CEPH_MSGR_TAG_FEATURES)
  __u32 flags;          /* CEPH_MSG_CONNECT_* */

  MConnectReply()
    : Message(MSG_CONNECT_REPLY, HEAD_VERSION, COMPAT_VERSION)
    {}

  ~MConnectReply() {}

  void encode_payload(uint64_t _features) {
    ::encode(CEPH_BANNER, payload);
    ::encode(features, payload);
    ::encode(host_type, payload);
    ::encode(global_seq, payload);
    ::encode(connect_seq, payload);
    ::encode(protocol_version, payload);
    ::encode(authorizer_protocol, payload);
    ::encode(authorizer_len, payload);
    ::encode(addr, payload);
    ::encode(name, payload);
    ::encode(last_in_seq, payload);
    ::encode(last_out_seq, payload);
    ::encode(reply_code, payload);
    ::encode(flags, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(banner, p);
    ::decode(features, p);
    ::decode(host_type, p);
    ::decode(global_seq, p);
    ::decode(connect_seq, p);
    ::decode(protocol_version, p);
    ::decode(authorizer_protocol, p);
    ::decode(authorizer_len, p);
    ::decode(addr, p);
    ::decode(name, p);
    ::decode(last_in_seq, p);
    ::decode(last_out_seq, p);
    ::decode(reply_code, p);
    ::decode(flags, p);
  }

  const char *get_type_name() const { return "MConnect"; }

  void print(ostream& out) const {
    out << get_type_name() << " ";
  }
}; /* MConnectReply */

#endif /* CEPH_MSG_CONNECT_H */
