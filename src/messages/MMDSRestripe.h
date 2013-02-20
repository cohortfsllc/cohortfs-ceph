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

#ifndef CEPH_MDSRESTRIPE_H
#define CEPH_MDSRESTRIPE_H

#include "msg/Message.h"

struct MMDSRestripe : public Message {
  bufferlist container; // container inode replica
  stripeid_t stripeid; // target inode container stripeid

  MMDSRestripe() : Message(MSG_MDS_RESTRIPE) {}
  MMDSRestripe(stripeid_t stripeid)
      : Message(MSG_MDS_RESTRIPE), stripeid(stripeid) {}

  const char *get_type_name() const { return "restripe"; }
  void print(ostream &out) const {
    out << get_type_name() << "(" << stripeid << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(container, payload);
    ::encode(stripeid, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(container, p);
    ::decode(stripeid, p);
  }
};

#endif
