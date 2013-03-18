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
  dirfrag_t       base;
  snapid_t        snapid;
  filepath        want;   // ... [/]need/this/stuff
  inodeno_t       want_ino;

  bool want_base_stripe;
  bool want_xlocked;

 public:
  dirfrag_t get_base_dirfrag() const { return base; }
  inodeno_t get_base_ino() const { return base.stripe.ino; }
  stripeid_t get_base_stripe() const { return base.stripe.stripeid; }
  frag_t get_base_frag() const { return base.frag; }
  snapid_t get_snapid() const { return snapid; }

  const filepath& get_want() const { return want; }
  inodeno_t get_want_ino() const { return want_ino; }
  const string& get_dentry(int n) const { return want[n]; }

  bool wants_base_stripe() const { return want_base_stripe; }
  bool wants_xlocked() const { return want_xlocked; }
 
  void set_base_frag(frag_t f) { base.frag = f; }

  MDiscover() : Message(MSG_MDS_DISCOVER) { }
  MDiscover(dirfrag_t base,
            snapid_t snapid,
            const filepath& want,
            inodeno_t want_ino,
            bool want_base_stripe = true,
            bool want_xlocked = false) :
      Message(MSG_MDS_DISCOVER),
      base(base),
      snapid(snapid),
      want(want),
      want_ino(want_ino),
      want_base_stripe(want_base_stripe),
      want_xlocked(want_xlocked) {}
private:
  ~MDiscover() {}

public:
  const char *get_type_name() const { return "Dis"; }
  void print(ostream &out) const {
    out << "discover(" << header.tid << " " << base << " " << want;
    if (want_ino)
      out << want_ino;
    out << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base, p);
    ::decode(snapid, p);
    ::decode(want, p);
    ::decode(want_ino, p);
    ::decode(want_base_stripe, p);
    ::decode(want_xlocked, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(base, payload);
    ::encode(snapid, payload);
    ::encode(want, payload);
    ::encode(want_ino, payload);
    ::encode(want_base_stripe, payload);
    ::encode(want_xlocked, payload);
  }

};

#endif
