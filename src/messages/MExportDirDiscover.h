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

#ifndef CEPH_MEXPORTDIRDISCOVER_H
#define CEPH_MEXPORTDIRDISCOVER_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirDiscover : public Message {
  int32_t from;
  dirstripe_t dirstripe;
  filepath path;

 public:
  int get_source_mds() { return from; }
  inodeno_t get_ino() { return dirstripe.ino; }
  dirstripe_t get_dirstripe() { return dirstripe; }
  filepath& get_path() { return path; }

  bool started;

  MExportDirDiscover() :     
    Message(MSG_MDS_EXPORTDIRDISCOVER),
    started(false) { }
  MExportDirDiscover(int f, filepath& p, dirstripe_t ds) : 
    Message(MSG_MDS_EXPORTDIRDISCOVER),
    from(f), 
    dirstripe(ds),
    path(p),
    started(false)
  { }
private:
  ~MExportDirDiscover() {}

public:
  const char *get_type_name() const { return "ExDis"; }
  void print(ostream& o) const {
    o << "export_discover(" << dirstripe << " " << path << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(dirstripe, p);
    ::decode(path, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(from, payload);
    ::encode(dirstripe, payload);
    ::encode(path, payload);
  }
};

#endif
