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


#ifndef CEPH_MDISCOVERREPLY_H
#define CEPH_MDISCOVERREPLY_H

#include "msg/Message.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;



/**
 * MDiscoverReply - return new replicas (of inodes, dirs, dentries)
 *
 * we group returned items by (dir, dentry, inode).  each
 * item in each set shares an index (it's "depth").
 *
 * we can start and end with any type.
 *   no_base_dir    = true if the first group has an inode but no dir
 *   no_base_dentry = true if the first group has an inode but no dentry
 * they are false if there is no returned data, ie the first group is empty.
 *
 * we also return errors:
 *   error_flag_dn(string) - the specified dentry dne
 *   error_flag_dir        - the last item wasn't a dir, so we couldn't continue.
 *
 * and sometimes,
 *   auth_hint             - where we think the stripe auth is
 *
 * depth() gives us the number of depth units/indices for which we have 
 * information.  this INCLUDES those for which we have errors but no data.
 *
 * see MDCache::handle_discover, handle_discover_reply.
 *
 *
 * so basically, we get
 *
 *   dir den ino   i
 *            x    0
 *    x   x   x    1
 * or
 *        x   x    0
 *    x   x   x    1
 * or
 *    x   x   x    0
 *    x   x   x    1
 * ...and trail off however we want.    
 * 
 * 
 */

class MDiscoverReply : public Message {

  static const int HEAD_VERSION = 2;

 public:
  // info about original request
  dirfrag_t base;
  bool xlock;
  snapid_t snapid;
  pair<__u8,__u8> want;

  // and the response
  static const int FLAG_ERR_DN =        (1<<0);
  static const int FLAG_ERR_INO =       (1<<1);
  static const int FLAG_ERR_PLACEMENT = (1<<2);
  static const int FLAG_ERR_STRIPE =    (1<<3);
  static const int FLAG_ERR_DIR =       (1<<4);
  __u8 flags;

  string error_dentry;   // dentry that was not found (to trigger waiters on asker)

  __s32 auth_hint;

  pair<__u8,__u8> contains;
  bufferlist trace;

  // cons
  MDiscoverReply() : Message(MSG_MDS_DISCOVERREPLY, HEAD_VERSION) { }
  MDiscoverReply(const MDiscover *dis)
      : Message(MSG_MDS_DISCOVERREPLY, HEAD_VERSION),
        base(dis->base),
        xlock(dis->xlock),
        snapid(dis->snapid),
        want(dis->want),
        flags(0),
        auth_hint(CDIR_AUTH_UNKNOWN) {
    header.tid = dis->get_tid();
  }
private:
  ~MDiscoverReply() {}

public:
  const char *get_type_name() const { return "discover_reply"; }
  void print(ostream& out) const {
    out << "discover_reply(" << header.tid << " " << base << ")";
  }

  bool is_empty() const {
    return trace.length() == 0 && flags == 0
        && auth_hint == CDIR_AUTH_UNKNOWN;
  }

  bool is_flag_error_dn() const { return flags & FLAG_ERR_DN; }
  bool is_flag_error_ino() const { return flags & FLAG_ERR_INO; }
  bool is_flag_error_placement() const { return flags & FLAG_ERR_PLACEMENT; }
  bool is_flag_error_stripe() const { return flags & FLAG_ERR_STRIPE; }
  bool is_flag_error_dir() const { return flags & FLAG_ERR_DIR; }

  void set_flag_error_dn(const string& dn) {
    flags &= FLAG_ERR_DN;
    error_dentry = dn;
  }
  void set_flag_error_ino() { flags &= FLAG_ERR_INO; }
  void set_flag_error_placement() { flags &= FLAG_ERR_PLACEMENT; }
  void set_flag_error_stripe() { flags &= FLAG_ERR_STRIPE; }
  void set_flag_error_dir() { flags &= FLAG_ERR_DIR; }

  void set_auth_hint(int a) {
    auth_hint = a;
  }

  // ...
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base, p);
    ::decode(xlock, p);
    ::decode(snapid, p);
    ::decode(want, p);
    ::decode(flags, p);
    ::decode(error_dentry, p);
    ::decode(auth_hint, p);
    ::decode(contains, p);
    ::decode(trace, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(base, payload);
    ::encode(xlock, payload);
    ::encode(snapid, payload);
    ::encode(want, payload);
    ::encode(flags, payload);
    ::encode(error_dentry, payload);
    ::encode(auth_hint, payload);
    ::encode(contains, payload);
    ::encode(trace, payload);
  }

};

#endif
