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


#ifndef CEPH_MDIRUPDATE_H
#define CEPH_MDIRUPDATE_H

#include "msg/Message.h"

class MDirUpdate : public Message {
  int32_t from_mds;
  dirstripe_t dirstripe;
  filepath path;
  bool replicate;
  int32_t discover;

 public:
  int get_source_mds() const { return from_mds; }
  dirstripe_t get_dirstripe() const { return dirstripe; }
  bool should_replicate() const { return replicate; }
  bool should_discover() const { return discover > 0; }
  const filepath& get_path() const { return path; }

  void tried_discover() {
    if (discover) discover--;
  }

  MDirUpdate() : Message(MSG_MDS_DIRUPDATE) {}
  MDirUpdate(int from, dirstripe_t dirstripe, filepath& path,
             bool replicate = false, bool discover = false) :
      Message(MSG_MDS_DIRUPDATE),
      from_mds(from),
      dirstripe(dirstripe),
      path(path),
      replicate(replicate),
      discover(discover ? 5 : 0) {}
private:
  ~MDirUpdate() {}

public:
  const char *get_type_name() const { return "dir_update"; }
  void print(ostream& out) const {
    out << "dir_update(" << dirstripe << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from_mds, p);
    ::decode(dirstripe, p);
    ::decode(replicate, p);
    ::decode(discover, p);
    ::decode(path, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(from_mds, payload);
    ::encode(dirstripe, payload);
    ::encode(replicate, payload);
    ::encode(discover, payload);
    ::encode(path, payload);
  }
};

#endif
