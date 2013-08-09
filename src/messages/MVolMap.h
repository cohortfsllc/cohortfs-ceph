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
  VolMapRef volmap;

  MVolMap(VolMapRef v) :
    Message(CEPH_MSG_VOL_MAP),
    volmap(v) {}
private:
  ~MVolMap() {}

public:
  const char *get_type_name() const {
    return "vol_map";
  }

  void encode_payload(uint64_t features) {
    volmap->encode(payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    volmap->decode(p);
  }
};

#endif
