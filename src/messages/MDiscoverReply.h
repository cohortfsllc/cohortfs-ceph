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

  // info about original request
  dirfrag_t base;
  bool wanted_base_stripe;
  bool wanted_xlocked;
  inodeno_t wanted_ino;
  snapid_t wanted_snapid;

  // and the response
  bool flag_error_dn;
  bool flag_error_ino;
  bool flag_error_stripe;
  bool flag_error_dir;
  string error_dentry;   // dentry that was not found (to trigger waiters on asker)
  bool unsolicited;

  __s32 auth_hint;

 public:
  __u8 starts_with;
  bufferlist trace;

  enum { STRIPE, DIR, DENTRY, INODE };

  // accessors
  dirfrag_t get_base_dirfrag() const { return base; }
  inodeno_t get_base_ino() const { return base.stripe.ino; }
  stripeid_t get_base_stripe() const { return base.stripe.stripeid; }
  frag_t get_base_frag() const { return base.frag; }
  bool get_wanted_base_stripe() const { return wanted_base_stripe; }
  bool get_wanted_xlocked() const { return wanted_xlocked; }
  inodeno_t get_wanted_ino() const { return wanted_ino; }
  snapid_t get_wanted_snapid() const { return wanted_snapid; }

  bool is_flag_error_dn() const { return flag_error_dn; }
  bool is_flag_error_ino() const { return flag_error_ino; }
  bool is_flag_error_stripe() const { return flag_error_stripe; }
  bool is_flag_error_dir() const { return flag_error_dir; }
  const string& get_error_dentry() const { return error_dentry; }

  int get_starts_with() const { return starts_with; }

  int get_auth_hint() const { return auth_hint; }

  bool is_unsolicited() const { return unsolicited; }
  void mark_unsolicited() { unsolicited = true; }

  void set_base_frag(frag_t df) { base.frag = df; }

  // cons
  MDiscoverReply() : Message(MSG_MDS_DISCOVERREPLY, HEAD_VERSION) { }
  MDiscoverReply(const MDiscover *dis) :
    Message(MSG_MDS_DISCOVERREPLY, HEAD_VERSION),
    base(dis->get_base_dirfrag()),
    wanted_base_stripe(dis->wants_base_stripe()),
    wanted_xlocked(dis->wants_xlocked()),
    wanted_ino(dis->get_want_ino()),
    wanted_snapid(dis->get_snapid()),
    flag_error_dn(false),
    flag_error_ino(false),
    flag_error_stripe(false),
    flag_error_dir(false),
    auth_hint(CDIR_AUTH_UNKNOWN) {
    header.tid = dis->get_tid();
  }
  MDiscoverReply(dirfrag_t df) :
    Message(MSG_MDS_DISCOVERREPLY, HEAD_VERSION),
    base(df),
    wanted_base_stripe(false),
    wanted_xlocked(false),
    wanted_ino(inodeno_t()),
    wanted_snapid(CEPH_NOSNAP),
    flag_error_dn(false),
    flag_error_ino(false),
    flag_error_stripe(false),
    flag_error_dir(false),
    auth_hint(CDIR_AUTH_UNKNOWN) {
    header.tid = 0;
  }
private:
  ~MDiscoverReply() {}

public:
  const char *get_type_name() const { return "discover_reply"; }
  void print(ostream& out) const {
    out << "discover_reply(" << header.tid << " " << base << ")";
  }
  
  // builders
  bool is_empty() {
    return trace.length() == 0 &&
      !flag_error_dn &&
      !flag_error_ino &&
      !flag_error_stripe &&
      !flag_error_dir &&
      auth_hint == CDIR_AUTH_UNKNOWN;
  }

  //  void set_flag_forward() { flag_forward = true; }
  void set_flag_error_dn(const string& dn) {
    flag_error_dn = true;
    error_dentry = dn;
  }
  void set_flag_error_ino() {
    flag_error_ino = true;
  }
  void set_flag_error_stripe() {
    flag_error_stripe = true;
  }
  void set_flag_error_dir() {
    flag_error_dir = true;
  }
  void set_auth_hint(int a) {
    auth_hint = a;
  }
  void set_error_dentry(const string& dn) {
    error_dentry = dn;
  }


  // ...
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base, p);
    ::decode(wanted_base_stripe, p);
    ::decode(wanted_xlocked, p);
    ::decode(wanted_snapid, p);
    ::decode(flag_error_dn, p);
    ::decode(flag_error_ino, p);
    ::decode(flag_error_stripe, p);
    ::decode(flag_error_dir, p);
    ::decode(error_dentry, p);
    ::decode(auth_hint, p);
    ::decode(unsolicited, p);

    ::decode(starts_with, p);
    ::decode(trace, p);
    if (header.version >= 2)
      ::decode(wanted_ino, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(base, payload);
    ::encode(wanted_base_stripe, payload);
    ::encode(wanted_xlocked, payload);
    ::encode(wanted_snapid, payload);
    ::encode(flag_error_dn, payload);
    ::encode(flag_error_ino, payload);
    ::encode(flag_error_stripe, payload);
    ::encode(flag_error_dir, payload);
    ::encode(error_dentry, payload);
    ::encode(auth_hint, payload);
    ::encode(unsolicited, payload);

    ::encode(starts_with, payload);
    ::encode(trace, payload);
    ::encode(wanted_ino, payload);
  }

};

#endif
