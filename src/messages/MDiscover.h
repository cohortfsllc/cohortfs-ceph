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


#ifndef CEPH_MDISCOVER_H
#define CEPH_MDISCOVER_H

#include "msg/Message.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;


class MDiscover : public Message {
 public:
  dirfrag_t       base; // base ino/stripe/frag
  snapid_t        snapid;
  string          name; // dentry wanted, if any
  pair<__u8,__u8> want; // <from, to>
  bool            xlock;


  MDiscover() : Message(MSG_MDS_DISCOVER) {}
  MDiscover(dirfrag_t base, snapid_t snapid, const string &name,
            __u8 want_from, __u8 want_to, bool xlock)
      : Message(MSG_MDS_DISCOVER),
        base(base),
        snapid(snapid),
        name(name),
        want(want_from, want_to),
        xlock(xlock) {}
private:
  ~MDiscover() {}

public:
  const char *get_type_name() const { return "Dis"; }
  void print(ostream &out) const {
    out << "discover(" << header.tid << " " << base << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base, p);
    ::decode(snapid, p);
    ::decode(name, p);
    ::decode(want, p);
    ::decode(xlock, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(base, payload);
    ::encode(snapid, payload);
    ::encode(name, payload);
    ::encode(want, payload);
    ::encode(xlock, payload);
  }
};

#endif
