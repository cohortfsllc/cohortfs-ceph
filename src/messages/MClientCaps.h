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
#include "include/ceph_features.h"


class MClientCaps : public Message {

  static const int HEAD_VERSION = 4;   // added flock metadata, inline data
  static const int COMPAT_VERSION = 1;

 public:
  struct ceph_mds_caps head;
  struct ceph_mds_cap_peer peer;
  bufferlist xattrbl;
  bufferlist flockbl;
  version_t  inline_version;
  bufferlist inline_data;

  int      get_caps() { return head.caps; }
  int      get_wanted() { return head.wanted; }
  int      get_dirty() { return head.dirty; }
  ceph_seq_t get_seq() { return head.seq; }
  ceph_seq_t get_issue_seq() { return head.issue_seq; }
  ceph_seq_t get_mseq() { return head.migrate_seq; }

  inodeno_t get_ino() { return inodeno_t(head.ino); }
  uint64_t get_cap_id() { return head.cap_id; }

  uint64_t get_size() { return head.size;  }
  uint64_t get_max_size() { return head.max_size;  }
  uint32_t get_truncate_seq() { return head.truncate_seq; }
  uint64_t get_truncate_size() { return head.truncate_size; }
  utime_t get_ctime() { return utime_t(head.ctime); }
  utime_t get_mtime() { return utime_t(head.mtime); }
  utime_t get_atime() { return utime_t(head.atime); }
  uint32_t get_time_warp_seq() { return head.time_warp_seq; }

  ceph_file_layout& get_layout() { return head.layout; }

  int       get_migrate_seq() { return head.migrate_seq; }
  int       get_op() { return head.op; }

  uint64_t get_client_tid() { return get_tid(); }
  void set_client_tid(uint64_t s) { set_tid(s); }

  void set_caps(int c) { head.caps = c; }
  void set_wanted(int w) { head.wanted = w; }

  void set_max_size(uint64_t ms) { head.max_size = ms; }

  void set_migrate_seq(unsigned m) { head.migrate_seq = m; }
  void set_op(int o) { head.op = o; }

  void set_size(loff_t s) { head.size = s; }
  void set_mtime(const utime_t &t) { t.encode_timeval(&head.mtime); }
  void set_atime(const utime_t &t) { t.encode_timeval(&head.atime); }

  void set_cap_peer(uint64_t id, ceph_seq_t seq, ceph_seq_t mseq, int mds, int flags) {
    peer.cap_id = id;
    peer.seq = seq;
    peer.mseq = mseq;
    peer.mds = mds;
    peer.flags = flags;
  }

  MClientCaps()
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION) {
    inline_version = 0;
  }
  MClientCaps(int op,
	      inodeno_t ino,
	      uint64_t id,
	      long seq,
	      int caps,
	      int wanted,
	      int dirty,
	      int mseq)
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.cap_id = id;
    head.seq = seq;
    head.caps = caps;
    head.wanted = wanted;
    head.dirty = dirty;
    head.migrate_seq = mseq;
    peer.cap_id = 0;
    inline_version = 0;
  }
  MClientCaps(int op,
	      inodeno_t ino,
	      uint64_t id, int mseq)
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.cap_id = id;
    head.migrate_seq = mseq;
    peer.cap_id = 0;
    inline_version = 0;
  }
private:
  ~MClientCaps() {}

public:
  const char *get_type_name() const { return "Cfcap";}
  void print(ostream& out) const {
    out << "client_caps(" << ceph_cap_op_name(head.op)
	<< " ino " << inodeno_t(head.ino)
	<< " " << head.cap_id
	<< " seq " << head.seq;
    if (get_tid())
      out << " tid " << get_tid();
    out << " caps=" << ccap_string(head.caps)
	<< " dirty=" << ccap_string(head.dirty)
	<< " wanted=" << ccap_string(head.wanted);
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

    assert(middle.length() == head.xattr_len);
    if (head.xattr_len)
      xattrbl = middle;

    // conditionally decode flock metadata
    if (header.version >= 2)
      ::decode(flockbl, p);

    if (header.version >= 3) {
      if (head.op == CEPH_CAP_OP_IMPORT)
	::decode(peer, p);
      else if (head.op == CEPH_CAP_OP_EXPORT)
	memcpy(&peer, &head.peer, sizeof(peer));
    }

    if (header.version >= 4) {
      ::decode(inline_version, p);
      ::decode(inline_data, p);
    } else {
      inline_version = CEPH_INLINE_NONE;
    }
  }
  void encode_payload(uint64_t features) {
    head.xattr_len = xattrbl.length();

    // record peer in unused fields of cap export message
    if (head.op == CEPH_CAP_OP_EXPORT)
      memcpy(&head.peer, &peer, sizeof(peer));

    ::encode(head, payload);

    middle = xattrbl;

    // conditionally include flock metadata
    if (features & CEPH_FEATURE_FLOCK) {
      ::encode(flockbl, payload);
    } else {
      header.version = 1;
      return;
    }

    if (head.op == CEPH_CAP_OP_IMPORT)
      ::encode(peer, payload);

    if (features & CEPH_FEATURE_MDS_INLINE_DATA) {
      ::encode(inline_version, payload);
      ::encode(inline_data, payload);
    } else {
      header.version = 3;
      return;
    }
  }
};

#endif
