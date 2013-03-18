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


#ifndef CEPH_MEXPORTDIR_H
#define CEPH_MEXPORTDIR_H

#include "msg/Message.h"


class MExportDir : public Message {
 public:  
  dirstripe_t dirstripe;
  bufferlist export_data;
  vector<dirstripe_t> bounds;
  bufferlist client_map;

  MExportDir() : Message(MSG_MDS_EXPORTDIR) {}
  MExportDir(dirstripe_t ds) : 
    Message(MSG_MDS_EXPORTDIR),
    dirstripe(ds) {
  }
private:
  ~MExportDir() {}

public:
  const char *get_type_name() const { return "Ex"; }
  void print(ostream& o) const {
    o << "export(" << dirstripe << ")";
  }

  void add_export(dirstripe_t ds) { 
    bounds.push_back(ds); 
  }

  void encode_payload(uint64_t features) {
    ::encode(dirstripe, payload);
    ::encode(bounds, payload);
    ::encode(export_data, payload);
    ::encode(client_map, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirstripe, p);
    ::decode(bounds, p);
    ::decode(export_data, p);
    ::decode(client_map, p);
  }

};

#endif
