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
  typedef map<inodeno_t, inode_stat_update_t> inode_map;
  inode_map inodes;

  typedef vector<stripe_stat_update_t> stripe_update_vec;
  typedef map<dirstripe_t, stripe_update_vec> stripe_map;
  stripe_map stripes;


  void add_inode(inodeno_t ino, const inode_stat_update_t &update)
  {
    pair<inode_map::iterator, bool> result =
        inodes.insert(make_pair(ino, update));
    if (result.second)
      return;

    // add to existing stats
    inode_stat_update_t &existing = result.first->second;
    existing.frag.add(update.frag);
    existing.nest.add(update.nest);
  }

  void add_stripe(dirstripe_t ds, const stripe_stat_update_t &update)
  {
    // search existing updates for a matching ino
    stripe_update_vec &existing = stripes[ds];
    for (stripe_update_vec::iterator i = existing.begin();
         i != existing.end(); ++i) {
      if (i->ino == update.ino) {
        // add to existing stats
        i->frag.add(update.frag);
        i->nest.add(update.nest);
        return;
      }
    }
    existing.push_back(update);
  }


  // batched inode/stripe ack messages
  typedef map<inodeno_t, nest_info_t> inode_ack_map;
  inode_ack_map inode_acks;

  typedef pair<frag_info_t, nest_info_t> stat_ack_t;
  typedef map<dirstripe_t, stat_ack_t> stripe_ack_map;
  stripe_ack_map stripe_acks;

  void add_inode_ack(inodeno_t ino, const nest_info_t &rstat)
  {
    pair<inode_ack_map::iterator, bool> result =
        inode_acks.insert(make_pair(ino, rstat));
    if (result.second)
      return;

    // update existing ack if newer
    nest_info_t &existing = result.first->second;
    if (existing.version < rstat.version)
      existing = rstat;
  }

  void add_stripe_ack(dirstripe_t ds,
                      const frag_info_t &fragstat,
                      const nest_info_t &rstat)
  {
    const stat_ack_t ack(fragstat, rstat);

    pair<stripe_ack_map::iterator, bool> result =
        stripe_acks.insert(make_pair(ds, ack));
    if (result.second)
      return;

    // update existing ack if newer
    stat_ack_t &existing = result.first->second;
    if (existing.first.version < fragstat.version)
      existing.first = fragstat;
    if (existing.second.version < rstat.version)
      existing.second = rstat;
  }

  MParentStats() : Message(MSG_MDS_PARENTSTATS) {}
 private:
  ~MParentStats() {}

 public:
  const char *get_type_name() const { return "parent_stats"; }
  void print(ostream& out) const {
    out << get_type_name()
        << "(inodes=" << inodes.size() << "/" << inode_acks.size()
        << " stripes=" << stripes.size() << "/" << stripe_acks.size() << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(inodes, p);
    ::decode(inode_acks, p);
    ::decode(stripes, p);
    ::decode(stripe_acks, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(inodes, payload);
    ::encode(inode_acks, payload);
    ::encode(stripes, payload);
    ::encode(stripe_acks, payload);
  }
};

#endif
