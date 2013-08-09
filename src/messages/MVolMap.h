// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */

#ifndef CEPH_MVOLMAP_H
#define CEPH_MVOLMAP_H

#include "include/ceph_features.h"
#include "msg/Message.h"
#include "vol/VolMap.h"

class MVolMap : public Message {
public:
  uuid_d fsid;
  epoch_t epoch;
  bufferlist encoded;

  version_t get_epoch() const { return epoch; }
  bufferlist& get_encoded() { return encoded; }

  MVolMap() :
    Message(CEPH_MSG_VOL_MAP) {}
  MVolMap(const uuid_d &f, VolMapRef vm) :
    Message(CEPH_MSG_VOL_MAP),
    fsid(f) {
    epoch = vm->get_epoch();
    vm->encode(encoded);
  }
private:
  ~MVolMap() {}

public:
  const char *get_type_name() const { return "volmap"; }
  void print(ostream& out) const {
    out << "volmap(e " << epoch << ")";
  }

  // marshalling
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(epoch, p);
    ::decode(encoded, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
    ::encode(epoch, payload);
    ::encode(encoded, payload);
  }
};

#endif
