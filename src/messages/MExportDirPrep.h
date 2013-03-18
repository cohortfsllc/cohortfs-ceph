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


#ifndef CEPH_MEXPORTDIRPREP_H
#define CEPH_MEXPORTDIRPREP_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirPrep : public Message {
  dirstripe_t dirstripe;
 public:
  bufferlist base;
  list<dirstripe_t> bounds;
  list<bufferlist> traces;
private:
  set<__s32> bystanders;
  bool b_did_assim;

public:
  dirstripe_t get_dirstripe() { return dirstripe; }
  list<dirstripe_t>& get_bounds() { return bounds; }
  set<__s32> &get_bystanders() { return bystanders; }

  bool did_assim() { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

  MExportDirPrep() {
    b_did_assim = false;
  }
  MExportDirPrep(dirstripe_t ds) : 
    Message(MSG_MDS_EXPORTDIRPREP),
    dirstripe(ds),
    b_did_assim(false) { }
private:
  ~MExportDirPrep() {}

public:
  const char *get_type_name() const { return "ExP"; }
  void print(ostream& o) const {
    o << "export_prep(" << dirstripe << ")";
  }

  void add_bound(dirstripe_t ds) {
    bounds.push_back(ds);
  }
  void add_trace(bufferlist& bl) {
    traces.push_back(bl);
  }
  void add_bystander(int who) {
    bystanders.insert(who);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirstripe, p);
    ::decode(base, p);
    ::decode(bounds, p);
    ::decode(traces, p);
    ::decode(bystanders, p);
  }

  void encode_payload(uint64_t features) {
    ::encode(dirstripe, payload);
    ::encode(base, payload);
    ::encode(bounds, payload);
    ::encode(traces, payload);
    ::encode(bystanders, payload);
  }
};

#endif
