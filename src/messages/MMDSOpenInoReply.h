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

#ifndef CEPH_MDSOPENINOREPLY_H
#define CEPH_MDSOPENINOREPLY_H

#include "msg/Message.h"

struct MMDSOpenInoReply : public Message {
  inodeno_t ino;
  vector<inode_backpointer_t> ancestors;
  int32_t hint;
  int32_t error;

  MMDSOpenInoReply() : Message(MSG_MDS_OPENINOREPLY) {}
  MMDSOpenInoReply(ceph_tid_t t, inodeno_t i, int h=-1, int e=0) :
    Message(MSG_MDS_OPENINOREPLY), ino(i), hint(h), error(e) {
    header.tid = t;
  }

  const char *get_type_name() const { return "openinoreply"; }
private:
  template <typename T>
  void _print(T &out) const {
    out << "openinoreply(" << header.tid << " "
	<< ino << " " << hint << " " << ancestors << ")";
  }
public:
  void print(ostream& out) const { _print(out); }
  void print(lttng_stream& out) const { _print(out); }  

  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
    ::encode(ancestors, payload);
    ::encode(hint, payload);
    ::encode(error, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(ancestors, p);
    ::decode(hint, p);
    ::decode(error, p);
  }
};

#endif
