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
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MEXPORTDIRNOTIFYACK_H
#define CEPH_MEXPORTDIRNOTIFYACK_H

#include "msg/Message.h"
#include <string>

class MExportDirNotifyAck : public Message {
  dirstripe_t dirstripe;

 public:
  dirstripe_t get_dirstripe() { return dirstripe; }
  
  MExportDirNotifyAck() {}
  MExportDirNotifyAck(dirstripe_t ds) :
    Message(MSG_MDS_EXPORTDIRNOTIFYACK), dirstripe(ds) {}
private:
  ~MExportDirNotifyAck() {}

public:
  const char *get_type_name() const { return "ExNotA"; }
  void print(ostream& o) const {
    o << "export_notify_ack(" << dirstripe << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(dirstripe, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirstripe, p);
  }
  
};

#endif
