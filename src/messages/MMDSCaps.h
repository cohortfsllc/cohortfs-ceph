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

  MMDSCaps() : Message(MSG_MDS_MDSCAPS) {}
  MMDSCaps(MDSCacheObjectInfo &info, int caps)
      : Message(MSG_MDS_MDSCAPS), info(info), caps(caps) {}
 private:
  ~MMDSCaps() {}

 public:
  const char *get_type_name() const { return "mds_caps"; }
  void print(ostream& out) const {
    out << get_type_name() << '(' << info << ' ' << ccap_string(caps) << ')';
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
