// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 * Client requests often need to get forwarded from some monitor
 * to the leader. This class encapsulates the original message
 * along with the client's caps so the leader can do proper permissions
 * checking.
 */

#ifndef CEPH_MFORWARD_H
#define CEPH_MFORWARD_H

#include "msg/Message.h"
#include "mon/MonCaps.h"
#include "include/encoding.h"

struct MForward : public Message {
  uint64_t tid;
  PaxosServiceMessage *msg;
  entity_inst_t client;
  MonCaps client_caps;
  
  MForward() : Message(MSG_FORWARD), tid(0), msg(NULL) {}
  //the message needs to have caps filled in!
  MForward(uint64_t t, PaxosServiceMessage *m) :
    Message(MSG_FORWARD), tid(t), msg(m) {
    client = m->get_source_inst();
    client_caps = m->get_session()->caps;
  }
  MForward(uint64_t t, PaxosServiceMessage *m, MonCaps caps) :
    Message(MSG_FORWARD), tid(t), msg(m), client_caps(caps) {
    client = m->get_source_inst();
  }
private:
  ~MForward() {
    if (msg) msg->put();
  }

public:
  void encode_payload() {
    ::encode(tid, payload);
    ::encode(client, payload);
    ::encode(client_caps, payload);
    encode_message(msg, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(tid, p);
    ::decode(client, p);
    ::decode(client_caps, p);
    msg = (PaxosServiceMessage *)decode_message(p);
  }

  const char *get_type_name() { return "forward"; }
  void print(ostream& o) {
    if (msg)
      o << "forward(" << *msg << ") to leader";
    else o << "forward(??? ) to leader";
  }
};
  
#endif
