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


#ifndef CEPH_MMDSSLAVEREQUEST_H
#define CEPH_MMDSSLAVEREQUEST_H

#include "msg/Message.h"
#include "mds/mdstypes.h"

class MMDSSlaveRequest : public Message {
 public:
  static const int OP_XLOCK =       1;
  static const int OP_XLOCKACK =   -1;
  static const int OP_UNXLOCK =     2;
  static const int OP_AUTHPIN =     3;
  static const int OP_AUTHPINACK = -3;

  static const int OP_LINKPREP =     4;
  static const int OP_UNLINKPREP =   5;
  static const int OP_LINKPREPACK = -4;

  static const int OP_MKDIR =        6;
  static const int OP_MKDIRACK =    -6;

  static const int OP_RENAMEPREP =     7;
  static const int OP_RENAMEPREPACK = -7;

  static const int OP_WRLOCK = 8;
  static const int OP_WRLOCKACK = -8;
  static const int OP_UNWRLOCK = 9;

  static const int OP_RMDIRPREP = 10;
  static const int OP_RMDIRPREPACK = -10;

  static const int OP_DROPLOCKS	= 11;

  static const int OP_FINISH = 17;  
  static const int OP_COMMITTED = -18;  

  static const int OP_ABORT =  20;  // used for recovery only
  //static const int OP_COMMIT = 21;  // used for recovery only


  const static char *get_opname(int o) {
    switch (o) { 
    case OP_XLOCK: return "xlock";
    case OP_XLOCKACK: return "xlock_ack";
    case OP_UNXLOCK: return "unxlock";
    case OP_AUTHPIN: return "authpin";
    case OP_AUTHPINACK: return "authpin_ack";

    case OP_LINKPREP: return "link_prep";
    case OP_LINKPREPACK: return "link_prep_ack";
    case OP_UNLINKPREP: return "unlink_prep";

    case OP_MKDIR: return "mkdir";
    case OP_MKDIRACK: return "mkdir_ack";

    case OP_RENAMEPREP: return "rename_prep";
    case OP_RENAMEPREPACK: return "rename_prep_ack";

    case OP_FINISH: return "finish"; // commit
    case OP_COMMITTED: return "committed";

    case OP_WRLOCK: return "wrlock";
    case OP_WRLOCKACK: return "wrlock_ack";
    case OP_UNWRLOCK: return "unwrlock";

    case OP_RMDIRPREP: return "rmdir_prep";
    case OP_RMDIRPREPACK: return "rmdir_prep_ack";

    case OP_DROPLOCKS: return "drop_locks";

    case OP_ABORT: return "abort";
      //case OP_COMMIT: return "commit";

    default: assert(0); return 0;
    }
  }

 private:
  metareqid_t reqid;
  __u32 attempt;
  __s16 op;

  // for locking
  __u16 lock_type;  // lock object type
  MDSCacheObjectInfo object_info;
  
  // for authpins
  vector<MDSCacheObjectInfo> authpins;

 public:
  filepath path;

  // for rename prep
  struct {
    inoparent_t dn;
    inodeno_t ino;
    int d_type;
  } src, dest;
  utime_t now;

  // mkdir
  bufferlist srci_replica;
  vector<stripeid_t> stripes;

  bufferlist stray;  // stray dir + dentry

public:
  metareqid_t get_reqid() { return reqid; }
  __u32 get_attempt() const { return attempt; }
  int get_op() { return op; }
  bool is_reply() { return op < 0; }

  int get_lock_type() { return lock_type; }
  MDSCacheObjectInfo &get_object_info() { return object_info; }
  MDSCacheObjectInfo &get_authpin_freeze() { return object_info; }

  vector<MDSCacheObjectInfo>& get_authpins() { return authpins; }

  void set_lock_type(int t) { lock_type = t; }


  // ----
  MMDSSlaveRequest() : Message(MSG_MDS_SLAVE_REQUEST) { }
  MMDSSlaveRequest(metareqid_t ri, __u32 att, int o) : 
    Message(MSG_MDS_SLAVE_REQUEST),
    reqid(ri), attempt(att), op(o) { }
private:
  ~MMDSSlaveRequest() {}

public:
  void encode_payload(uint64_t features) {
    ::encode(reqid, payload);
    ::encode(attempt, payload);
    ::encode(op, payload);
    ::encode(lock_type, payload);
    ::encode(object_info, payload);
    ::encode(authpins, payload);
    ::encode(path, payload);
    ::encode(src.dn, payload);
    ::encode(src.ino, payload);
    ::encode(src.d_type, payload);
    ::encode(dest.dn, payload);
    ::encode(dest.ino, payload);
    ::encode(dest.d_type, payload);
    ::encode(now, payload);
    ::encode(srci_replica, payload);
    ::encode(stripes, payload);
    ::encode(stray, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(reqid, p);
    ::decode(attempt, p);
    ::decode(op, p);
    ::decode(lock_type, p);
    ::decode(object_info, p);
    ::decode(authpins, p);
    ::decode(path, p);
    ::decode(src.dn, p);
    ::decode(src.ino, p);
    ::decode(src.d_type, p);
    ::decode(dest.dn, p);
    ::decode(dest.ino, p);
    ::decode(dest.d_type, p);
    ::decode(now, p);
    ::decode(srci_replica, p);
    ::decode(stripes, p);
    ::decode(stray, p);
  }

  const char *get_type_name() const { return "slave_request"; }
  void print(ostream& out) const {
    out << "slave_request(" << reqid
	<< "." << attempt
	<< " " << get_opname(op) 
	<< ")";
  }  
	
};

#endif
