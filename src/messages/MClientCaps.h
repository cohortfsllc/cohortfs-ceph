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

#ifndef CEPH_MCLIENTCAPS_H
#define CEPH_MCLIENTCAPS_H

#include "msg/Message.h"


class MClientCaps : public Message {
 public:
  struct ceph_mds_caps head;
  bufferlist snapbl;
  bufferlist xattrbl;
  bufferlist flockbl;

  int      get_caps() { return head.caps; }
  int      get_wanted() { return head.wanted; }
  int      get_dirty() { return head.dirty; }
  ceph_seq_t get_seq() { return head.seq; }
  ceph_seq_t get_issue_seq() { return head.issue_seq; }
  ceph_seq_t get_mseq() { return head.migrate_seq; }

  inodeno_t get_ino() { return inodeno_t(head.ino); }
  inodeno_t get_realm() { return inodeno_t(head.realm); }
  uint64_t get_cap_id() { return head.cap_id; }

  uint64_t get_size() { return head.size;  }
  uint64_t get_max_size() { return head.max_size;  }
  __u32 get_truncate_seq() { return head.truncate_seq; }
  uint64_t get_truncate_size() { return head.truncate_size; }
  utime_t get_ctime() { return utime_t(head.ctime); }
  utime_t get_mtime() { return utime_t(head.mtime); }
  utime_t get_atime() { return utime_t(head.atime); }
  __u32 get_time_warp_seq() { return head.time_warp_seq; }

  ceph_file_layout& get_layout() { return head.layout; }

  int       get_migrate_seq() { return head.migrate_seq; }
  int       get_op() { return head.op; }

  uint64_t get_client_tid() { return get_tid(); }
  void set_client_tid(uint64_t s) { set_tid(s); }

  snapid_t get_snap_follows() { return snapid_t(head.snap_follows); }
  void set_snap_follows(snapid_t s) { head.snap_follows = s; }

  void set_caps(int c) { head.caps = c; }
  void set_wanted(int w) { head.wanted = w; }

  void set_max_size(uint64_t ms) { head.max_size = ms; }

  void set_migrate_seq(unsigned m) { head.migrate_seq = m; }
  void set_op(int o) { head.op = o; }

  void set_size(loff_t s) { head.size = s; }
  void set_mtime(const utime_t &t) { t.encode_timeval(&head.mtime); }
  void set_atime(const utime_t &t) { t.encode_timeval(&head.atime); }

  MClientCaps() {}
  MClientCaps(int op,
	      inodeno_t ino,
	      inodeno_t realm,
	      uint64_t id,
	      long seq,
	      int caps,
	      int wanted,
	      int dirty,
	      int mseq) :
    Message(CEPH_MSG_CLIENT_CAPS) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.realm = realm;
    head.cap_id = id;
    head.seq = seq;
    head.caps = caps;
    head.wanted = wanted;
    head.dirty = dirty;
    head.migrate_seq = mseq;
  }
  MClientCaps(int op,
	      inodeno_t ino, inodeno_t realm,
	      uint64_t id, int mseq) :
    Message(CEPH_MSG_CLIENT_CAPS) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.realm = realm;
    head.cap_id = id;
    head.migrate_seq = mseq;
  }
private:
  ~MClientCaps() {}

public:
  const char *get_type_name() { return "Cfcap";}
  void print(ostream& out) {
    out << "client_caps(" << ceph_cap_op_name(head.op)
	<< " ino " << inodeno_t(head.ino)
	<< " " << head.cap_id
	<< " seq " << head.seq;
    if (get_tid())
      out << " tid " << get_tid();
    out << " caps=" << ccap_string(head.caps)
	<< " dirty=" << ccap_string(head.dirty)
	<< " wanted=" << ccap_string(head.wanted);
    out << " follows " << snapid_t(head.snap_follows);
    if (head.migrate_seq)
      out << " mseq " << head.migrate_seq;

    out << " size " << head.size << "/" << head.max_size;
    if (head.truncate_seq)
      out << " ts " << head.truncate_seq;
    out << " mtime " << utime_t(head.mtime);
    if (head.time_warp_seq)
      out << " tws " << head.time_warp_seq;

    if (head.xattr_version)
      out << " xattrs(v=" << head.xattr_version << " l=" << xattrbl.length() << ")";

    out << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode_nohead(head.snap_trace_len, snapbl, p);

    assert(middle.length() == head.xattr_len);
    if (head.xattr_len)
      xattrbl = middle;

    // conditionally decode flock metadata
    if (header.version >= 2)
      ::decode(flockbl, p);
  }
  void encode_payload() {
    head.snap_trace_len = snapbl.length();
    head.xattr_len = xattrbl.length();
    ::encode(head, payload);
    ::encode_nohead(snapbl, payload);

    middle = xattrbl;

    // conditionally include flock metadata
    if (connection->has_feature(CEPH_FEATURE_FLOCK)) {
      header.version = 2;
      ::encode(flockbl, payload);
    }
  }
};

#endif
