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

#ifndef CEPH_MEXPORTDIRNOTIFY_H
#define CEPH_MEXPORTDIRNOTIFY_H

#include "msg/Message.h"
#include <string>
using namespace std;

class MExportDirNotify : public Message {
  dirfrag_t base;
  bool ack;
  pair<int32_t,int32_t> old_auth, new_auth;
  // bounds; these dirs are _not_ included (tho the dirfragdes are)
  list<dirfrag_t> bounds;

 public:
  dirfrag_t get_dirfrag() { return base; }
  pair<int32_t,int32_t> get_old_auth() { return old_auth; }
  pair<int32_t,int32_t> get_new_auth() { return new_auth; }
  bool wants_ack() { return ack; }
  list<dirfrag_t>& get_bounds() { return bounds; }

  MExportDirNotify() {}
  MExportDirNotify(dirfrag_t i, uint64_t tid, bool a, pair<int32_t,int32_t> oa,
		   pair<int32_t,int32_t> na) :
    Message(MSG_MDS_EXPORTDIRNOTIFY),
    base(i), ack(a), old_auth(oa), new_auth(na) {
    set_tid(tid);
  }
private:
  ~MExportDirNotify() {}

  template <typename T>
  void _print(T& o) const {
    o << "export_notify(" << base;
    o << " " << old_auth << " -> " << new_auth;
    if (ack)
      o << " ack)";
    else
      o << " no ack)";
  }

public:
  const char *get_type_name() const { return "ExNot"; }

  void print(ostream& out) const { _print(out); }
  void print(lttng_stream& out) const { _print(out); }  

  void copy_bounds(list<dirfrag_t>& ex) {
    this->bounds = ex;
  }
  void copy_bounds(set<dirfrag_t>& ex) {
    for (set<dirfrag_t>::iterator i = ex.begin();
	 i != ex.end(); ++i)
      bounds.push_back(*i);
  }

  void encode_payload(uint64_t features) {
    ::encode(base, payload);
    ::encode(ack, payload);
    ::encode(old_auth, payload);
    ::encode(new_auth, payload);
    ::encode(bounds, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(base, p);
    ::decode(ack, p);
    ::decode(old_auth, p);
    ::decode(new_auth, p);
    ::decode(bounds, p);
  }
};

#endif
