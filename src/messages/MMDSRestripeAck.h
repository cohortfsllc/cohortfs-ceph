// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MDSRESTRIPEACK_H
#define CEPH_MDSRESTRIPEACK_H

#include "msg/Message.h"

struct MMDSRestripeAck : public Message {
  stripeid_t stripeid; // inode container stripeid

  MMDSRestripeAck() : Message(MSG_MDS_RESTRIPEACK) {}
  MMDSRestripeAck(stripeid_t stripeid)
      : Message(MSG_MDS_RESTRIPEACK), stripeid(stripeid) {}

  const char *get_type_name() const { return "restripe_ack"; }
  void print(ostream &out) const {
    out << get_type_name() << "(" << stripeid << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(stripeid, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(stripeid, p);
  }
};

#endif
