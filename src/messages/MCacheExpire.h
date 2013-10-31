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
  __s32 from;

public:
  /*
    group things by realm (auth delgation root), since that's how auth is determined.
    that makes it less work to process when exports are in progress.
  */
  struct realm {
    map<vinodeno_t, __s32> inodes;
    map<dirstripe_t, __s32> stripes;
    map<dirfrag_t, __s32> dirs;
    map<dirfrag_t, map<pair<string,snapid_t>,__s32> > dentries;

    void encode(bufferlist &bl) const {
      ::encode(inodes, bl);
      ::encode(stripes, bl);
      ::encode(dirs, bl);
      ::encode(dentries, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(inodes, bl);
      ::decode(stripes, bl);
      ::decode(dirs, bl);
      ::decode(dentries, bl);
    }
  };
  WRITE_CLASS_ENCODER(realm)

  map<dirstripe_t, realm> realms;

  int get_from() { return from; }

  MCacheExpire() : Message(MSG_MDS_CACHEEXPIRE), from(-1) {}
  MCacheExpire(int f) : 
    Message(MSG_MDS_CACHEEXPIRE),
    from(f) { }
private:
  ~MCacheExpire() {}

public:
  virtual const char *get_type_name() const { return "cache_expire";}
  
  void add_inode(dirstripe_t r, vinodeno_t vino, int nonce) {
    realms[r].inodes[vino] = nonce;
  }
  void add_stripe(dirstripe_t r, dirstripe_t ds, int nonce) {
    realms[r].stripes[ds] = nonce;
  }
  void add_dir(dirstripe_t r, dirfrag_t df, int nonce) {
    realms[r].dirs[df] = nonce;
  }
  void add_dentry(dirstripe_t r, dirfrag_t df, const string& dn, snapid_t last, int nonce) {
    realms[r].dentries[df][make_pair(dn,last)] = nonce;
  }

  void add_realm(dirstripe_t ds, realm& r) {
    realms[ds] = r;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(realms, p);
  }
    
  void encode_payload(uint64_t features) {
    ::encode(from, payload);
    ::encode(realms, payload);
  }
};

WRITE_CLASS_ENCODER(MCacheExpire::realm)

#endif
