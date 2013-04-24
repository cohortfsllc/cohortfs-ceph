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

#ifndef CEPH_MCACHEEXPIRE_H
#define CEPH_MCACHEEXPIRE_H

#include "mds/mdstypes.h"

class MCacheExpire : public Message {
 private:
  __s32 from;

 public:
  map<vinodeno_t, __s32> inodes;
  map<dirstripe_t, __s32> stripes;
  map<dirfrag_t, __s32> dirs;
  map<dirfrag_t, map<pair<string,snapid_t>,__s32> > dentries;

  int get_from() { return from; }

  MCacheExpire(int f = -1) : Message(MSG_MDS_CACHEEXPIRE), from(f) {}
 private:
  ~MCacheExpire() {}

 public:
  virtual const char *get_type_name() const { return "cache_expire"; }

  void add_inode(vinodeno_t vino, int nonce) {
    inodes[vino] = nonce;
  }
  void add_stripe(dirstripe_t ds, int nonce) {
    stripes[ds] = nonce;
  }
  void add_dir(dirfrag_t df, int nonce) {
    dirs[df] = nonce;
  }
  void add_dentry(dirfrag_t df, const string& dn, snapid_t last, int nonce) {
    dentries[df][make_pair(dn,last)] = nonce;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(inodes, p);
    ::decode(stripes, p);
    ::decode(dirs, p);
    ::decode(dentries, p);
  }
    
  void encode_payload(uint64_t features) {
    ::encode(from, payload);
    ::encode(inodes, payload);
    ::encode(stripes, payload);
    ::encode(dirs, payload);
    ::encode(dentries, payload);
  }
};

#endif
