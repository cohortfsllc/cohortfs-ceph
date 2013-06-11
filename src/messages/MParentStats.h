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


#ifndef CEPH_MPARENTSTATS_H
#define CEPH_MPARENTSTATS_H

#include "msg/Message.h"


class MParentStats : public Message {
 public:
  // batched inode/stripe parent stat updates
  typedef pair<frag_info_t, nest_info_t> stat_update_t;
  typedef map<inodeno_t, stat_update_t> inode_map;
  inode_map inodes;

  typedef map<dirstripe_t, stat_update_t> stripe_map;
  stripe_map stripes;


  void add_inode(inodeno_t ino, const frag_info_t &dirstat,
                 const nest_info_t &rstat)
  {
    const stat_update_t update(dirstat, rstat);
    pair<inode_map::iterator, bool> result =
        inodes.insert(make_pair(ino, update));
    if (result.second)
      return;

    // add to existing stats
    stat_update_t &existing = result.first->second;
    if (existing.first.version < update.first.version)
      existing.first = update.first;
    if (existing.second.version < update.second.version)
      existing.second = update.second;
  }

  void add_stripe(dirstripe_t ds, const frag_info_t &fragstat,
                  const nest_info_t &rstat)
  {
    const stat_update_t update(fragstat, rstat);
    pair<stripe_map::iterator, bool> result =
        stripes.insert(make_pair(ds, update));
    if (result.second)
      return;

    // add to existing stats
    stat_update_t &existing = result.first->second;
    if (existing.first.version < update.first.version)
      existing.first = update.first;
    if (existing.second.version < update.second.version)
      existing.second = update.second;
  }

  MParentStats() : Message(MSG_MDS_PARENTSTATS) {}
 private:
  ~MParentStats() {}

 public:
  const char *get_type_name() const { return "parent_stats"; }
  void print(ostream& out) const {
    out << get_type_name()
        << "(inodes=" << inodes.size() << " stripes=" << stripes.size() << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(inodes, p);
    ::decode(stripes, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(inodes, payload);
    ::encode(stripes, payload);
  }
};

#endif
