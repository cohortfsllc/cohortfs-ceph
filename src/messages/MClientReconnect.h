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

#ifndef CEPH_MCLIENTRECONNECT_H
#define CEPH_MCLIENTRECONNECT_H

#include "msg/Message.h"
#include "mds/mdstypes.h"
#include "include/ceph_features.h"


class MClientReconnect : public Message {

  const static int HEAD_VERSION = 3;

public:
  map<inodeno_t, cap_reconnect_t>  caps;   // only head inodes

  MClientReconnect() : Message(CEPH_MSG_CLIENT_RECONNECT, HEAD_VERSION) { }
private:
  ~MClientReconnect() {}

  template <typename T>
  void _print(T& out) const {
    out << "client_reconnect("
	<< caps.size() << " caps)";
  }

public:
  const char *get_type_name() const { return "client_reconnect"; }

  void print(ostream& out) const { _print(out); }
  void print(lttng_stream& out) const { _print(out); }  

  void add_cap(inodeno_t ino, uint64_t cap_id, inodeno_t pathbase, const string& path,
	       int wanted, int issued) {
    caps[ino] = cap_reconnect_t(cap_id, pathbase, path, wanted, issued);
  }

  void encode_payload(uint64_t features) {
    data.clear();
    if (features & CEPH_FEATURE_MDSENC) {
      ::encode(caps, data);
    } else if (features & CEPH_FEATURE_FLOCK) {
      // encode with old cap_reconnect_t encoding
      uint32_t n = caps.size();
      ::encode(n, data);
      for (map<inodeno_t,cap_reconnect_t>::iterator p = caps.begin(); p != caps.end(); ++p) {
	::encode(p->first, data);
	p->second.encode_old(data);
      }
      header.version = 2;
    } else {
      // compat crap
      header.version = 1;
      map<inodeno_t, old_cap_reconnect_t> ocaps;
      for (map<inodeno_t,cap_reconnect_t>::iterator p = caps.begin(); p != caps.end(); p++)
	ocaps[p->first] = p->second;
      ::encode(ocaps, data);
    }
  }
  void decode_payload() {
    bufferlist::iterator p = data.begin();
    if (header.version >= 3) {
      // new protocol
      ::decode(caps, p);
    } else if (header.version == 2) {
      uint32_t n;
      ::decode(n, p);
      inodeno_t ino;
      while (n--) {
	::decode(ino, p);
	caps[ino].decode_old(p);
      }
    } else {
      // compat crap
      map<inodeno_t, old_cap_reconnect_t> ocaps;
      ::decode(ocaps, p);
      for (map<inodeno_t,old_cap_reconnect_t>::iterator q = ocaps.begin(); q != ocaps.end(); q++)
	caps[q->first] = q->second;
    }
  }

};


#endif
