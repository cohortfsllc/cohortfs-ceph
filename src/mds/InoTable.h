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


#ifndef CEPH_INOTABLE_H
#define CEPH_INOTABLE_H

#include "MDSTable.h"
#include "include/interval_set.h"

class MDS;

class InoTable : public MDSTable {
  interval_set<inodeno_t> free;   // unused ids
  interval_set<inodeno_t> projected_free;

 public:
  InoTable(MDS *m) : MDSTable(m, "inotable", true) { }

  inodeno_t project_alloc_id(inodeno_t id=0);
  void apply_alloc_id(inodeno_t id);

  void project_alloc_ids(interval_set<inodeno_t>& inos, int want);
  void apply_alloc_ids(interval_set<inodeno_t>& inos);

  void project_release_ids(interval_set<inodeno_t>& inos);
  void apply_release_ids(interval_set<inodeno_t>& inos);

  void replay_alloc_id(inodeno_t ino);
  void replay_alloc_ids(interval_set<inodeno_t>& inos);
  void replay_release_ids(interval_set<inodeno_t>& inos);
  void replay_reset();

  void reset_state();
  void encode_state(bufferlist& bl) {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(free, bl);
  }
  void decode_state(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(free, bl);
    projected_free = free;
  }

  void skip_inos(inodeno_t i);
};

#endif
