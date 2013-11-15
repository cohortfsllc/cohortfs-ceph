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
  map<dirstripe_t, cap_reconnect_t> caps;
  vector<ceph_mds_snaprealm_reconnect> realms;

  MClientReconnect() : Message(CEPH_MSG_CLIENT_RECONNECT, HEAD_VERSION) { }
private:
  ~MClientReconnect() {}

public:
  const char *get_type_name() const { return "client_reconnect"; }
  void print(ostream& out) const {
    out << "client_reconnect(" << caps.size() << " caps)";
  }

  void add_cap(inodeno_t ino, uint64_t cap_id, int wanted, int issued,
	       inodeno_t sr) {
    dirstripe_t ds(ino, CEPH_CAP_OBJECT_INODE);
    caps[ds] = cap_reconnect_t(cap_id, wanted, issued, sr);
  }
  void add_cap(dirstripe_t ds, uint64_t cap_id, int wanted, int issued,
	       inodeno_t sr) {
    caps[ds] = cap_reconnect_t(cap_id, wanted, issued, sr);
  }
  void add_snaprealm(inodeno_t ino, snapid_t seq, inodeno_t parent) {
    ceph_mds_snaprealm_reconnect r;
    r.ino = ino;
    r.seq = seq;
    r.parent = parent;
    realms.push_back(r);
  }

  void encode_payload(uint64_t features) {
    data.clear();
    if (features & CEPH_FEATURE_MDSENC) {
      ::encode(caps, data);
    } else if (features & CEPH_FEATURE_FLOCK) {
      // encode with old cap_reconnect_t encoding
      __u32 n = caps.size();
      ::encode(n, data);
      for (map<dirstripe_t,cap_reconnect_t>::iterator p = caps.begin(); p != caps.end(); ++p) {
	::encode(p->first, data);
	p->second.encode(data);
      }
      header.version = 2;
    } else {
      // compat crap
      header.version = 1;
      map<dirstripe_t, old_cap_reconnect_t> ocaps;
      for (map<dirstripe_t,cap_reconnect_t>::iterator p = caps.begin(); p != caps.end(); ++p)
	ocaps[p->first] = p->second;
      ::encode(ocaps, data);
    }
    ::encode_nohead(realms, data);
  }
  void decode_payload() {
    bufferlist::iterator p = data.begin();
    if (header.version >= 3) {
      // new protocol
      ::decode(caps, p);
    } else if (header.version == 2) {
      __u32 n;
      ::decode(n, p);
      dirstripe_t ds;
      while (n--) {
	::decode(ds, p);
	caps[ds].decode(p);
      }
    } else {
      // compat crap
      map<dirstripe_t, old_cap_reconnect_t> ocaps;
      ::decode(ocaps, p);
      for (map<dirstripe_t,old_cap_reconnect_t>::iterator q = ocaps.begin(); q != ocaps.end(); ++q)
	caps[q->first] = q->second;
    }
    while (!p.end()) {
      realms.push_back(ceph_mds_snaprealm_reconnect());
      ::decode(realms.back(), p);
    }
  }

};


#endif
