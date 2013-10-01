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


#ifndef CEPH_MMDSCAPS_H
#define CEPH_MMDSCAPS_H

class MMDSCaps : public Message {
  MDSCacheObjectInfo info;
  __u32 caps;

 public:
  const MDSCacheObjectInfo& get_info() const { return info; }
  int get_caps() const { return caps; }

  bool is_inode() const {
    return get_dirstripe().stripeid == CEPH_CAP_OBJECT_INODE;
  }
  inodeno_t get_ino() const { return info.ino; }
  dirstripe_t get_dirstripe() const { return info.dirfrag.stripe; }

  MMDSCaps() : Message(MSG_MDS_MDSCAPS) {}
  MMDSCaps(MDSCacheObjectInfo &info, int caps)
      : Message(MSG_MDS_MDSCAPS), info(info), caps(caps) {}
 private:
  ~MMDSCaps() {}

 public:
  const char *get_type_name() const { return "mds_caps"; }
  void print(ostream& out) const {
    if (is_inode())
      out << "mds_inode_caps(" << get_ino() << " " << ccap_string(caps) << ")";
    else
      out << "mds_stripe_caps(" << get_dirstripe()
          << " " << ccap_string(caps) << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(info, payload);
    ::encode(caps, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(info, p);
    ::decode(caps, p);
  }
};

#endif
