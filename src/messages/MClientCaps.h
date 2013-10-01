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

  static const int HEAD_VERSION = 2;   // added flock metadata
  static const int COMPAT_VERSION = 1;

 public:
  struct ceph_mds_caps head;
  struct ceph_mds_caps_inode inode;
  struct ceph_mds_caps_stripe stripe;
  bufferlist snapbl;
  bufferlist xattrbl;
  bufferlist flockbl;

  int get_caps() const { return head.caps; }
  int get_wanted() const { return head.wanted; }
  int get_dirty() const { return head.dirty; }
  ceph_seq_t get_seq() const { return head.seq; }
  ceph_seq_t get_issue_seq() const { return head.issue_seq; }
  ceph_seq_t get_mseq() const { return head.migrate_seq; }

  inodeno_t get_ino() const { return inodeno_t(head.ino); }
  inodeno_t get_realm() const { return inodeno_t(head.realm); }
  uint64_t get_cap_id() const { return head.cap_id; }

  int get_migrate_seq() const { return head.migrate_seq; }
  int get_op() const { return head.op; }

  uint64_t get_client_tid() const { return get_tid(); }
  void set_client_tid(uint64_t s) { set_tid(s); }

  snapid_t get_snap_follows() const { return snapid_t(head.snap_follows); }
  void set_snap_follows(snapid_t s) { head.snap_follows = s; }

  void set_caps(int c) { head.caps = c; }
  void set_wanted(int w) { head.wanted = w; }

  void set_migrate_seq(unsigned m) { head.migrate_seq = m; }
  void set_op(int o) { head.op = o; }

  bool is_inode() const { return head.stripeid == CEPH_CAP_OBJECT_INODE; }
  bool is_stripe() const { return head.stripeid != CEPH_CAP_OBJECT_INODE; }

  // inode

  uint64_t get_size() const { return inode.size; }
  uint64_t get_max_size() const { return inode.max_size;  }
  __u32 get_truncate_seq() const { return inode.truncate_seq; }
  uint64_t get_truncate_size() const { return inode.truncate_size; }
  utime_t get_inode_ctime() const { return utime_t(inode.ctime); }
  utime_t get_inode_mtime() const { return utime_t(inode.mtime); }
  utime_t get_inode_atime() const { return utime_t(inode.atime); }
  __u32 get_time_warp_seq() const { return inode.time_warp_seq; }

  ceph_file_layout& get_layout() { return inode.layout; }

  void set_size(loff_t s) { inode.size = s; }
  void set_max_size(uint64_t ms) { inode.max_size = ms; }
  void set_inode_mtime(const utime_t &t) { t.encode_timeval(&inode.mtime); }
  void set_inode_atime(const utime_t &t) { t.encode_timeval(&inode.atime); }

  // stripe

  stripeid_t get_stripeid() const { return stripeid_t(head.stripeid); }
  utime_t get_stripe_mtime() const { return utime_t(stripe.mtime); }
  uint64_t get_stripe_nfiles() const { return stripe.nfiles; }
  uint64_t get_stripe_nsubdirs() const { return stripe.nsubdirs; }


  MClientCaps()
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION) {}
  MClientCaps(int op, inodeno_t realm, uint64_t id, long seq,
	      int caps, int wanted, int dirty, int mseq)
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.realm = realm;
    head.stripeid = CEPH_CAP_OBJECT_INODE;
    head.cap_id = id;
    head.seq = seq;
    head.caps = caps;
    head.wanted = wanted;
    head.dirty = dirty;
    head.migrate_seq = mseq;
  }
  MClientCaps(int op, inodeno_t realm, uint64_t id, int mseq)
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.realm = realm;
    head.stripeid = CEPH_CAP_OBJECT_INODE;
    head.cap_id = id;
    head.migrate_seq = mseq;
  }
private:
  ~MClientCaps() {}

public:
  const char *get_type_name() const { return "Cfcap";}
  void print(ostream& out) const {
    out << "client_caps(" << ceph_cap_op_name(head.op);
    if (is_inode())
      out << " ino " << inodeno_t(head.ino);
    else
      out << " stripe " << inodeno_t(head.ino) << ':' << head.stripeid;
    out << " " << head.cap_id
	<< " seq " << head.seq;
    if (get_tid())
      out << " tid " << get_tid();
    out << " caps=" << ccap_string(head.caps)
	<< " dirty=" << ccap_string(head.dirty)
	<< " wanted=" << ccap_string(head.wanted);
    out << " follows " << snapid_t(head.snap_follows);
    if (head.migrate_seq)
      out << " mseq " << head.migrate_seq;

    if (is_inode()) { // inode
      out << " size " << inode.size << "/" << inode.max_size;
      if (inode.truncate_seq)
        out << " ts " << inode.truncate_seq;
      out << " mtime " << utime_t(inode.mtime);
      if (inode.time_warp_seq)
        out << " tws " << inode.time_warp_seq;
      if (inode.xattr_version)
        out << " xattrs(v=" << inode.xattr_version << " l=" << xattrbl.length() << ")";
    } else { // stripe
      out << " files " << stripe.nfiles
          << " subdirs " << stripe.nsubdirs
          << " mtime " << utime_t(stripe.mtime);
    }
    out << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    if (is_inode()) {
      ::decode(inode, p);
      assert(middle.length() == inode.xattr_len);
      if (inode.xattr_len)
        xattrbl = middle;
    } else
      ::decode(stripe, p);
    ::decode_nohead(head.snap_trace_len, snapbl, p);

    // conditionally decode flock metadata
    if (header.version >= 2)
      ::decode(flockbl, p);
  }
  void encode_payload(uint64_t features) {
    head.snap_trace_len = snapbl.length();
    ::encode(head, payload);
    if (is_inode()) {
      inode.xattr_len = xattrbl.length();
      ::encode(inode, payload);
    } else
      ::encode(stripe, payload);
    ::encode_nohead(snapbl, payload);

    middle = xattrbl;

    // conditionally include flock metadata
    if (features & CEPH_FEATURE_FLOCK) {
      ::encode(flockbl, payload);
    } else {
      header.version = 1;  // old
    }
  }
};

#endif
