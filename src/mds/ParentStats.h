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

#ifndef CEPH_PARENTSTATS_H
#define CEPH_PARENTSTATS_H

#include "mdstypes.h"

class CInode;
class CStripe;
class EMetaBlob;
class MDS;
class MParentStats;
class Mutation;

class ParentStats {
 private:
  MDS *mds;

  // queued async update messages
  typedef map<int, MParentStats*> dirty_stats_map;
  dirty_stats_map dirty_stats;

  // find/create stats for the given mds
  MParentStats* stats_for_mds(int who);


  // timer to send out pending remote parent stat requests
  Context *tick_event;
  void tick();

  friend class C_PS_Tick;


  // open the parent object or forward stats to auth mds
  CStripe* open_parent_stripe(inode_t *pi, const frag_info_t &fragstat,
                              const nest_info_t &rstat);
  CInode* open_parent_inode(CStripe *stripe, const Mutation *mut,
                            const frag_info_t &dirstat,
                            const nest_info_t &rstat);

  // set of projected stripes/inodes
  class Projected {
   private:
    set<CStripe*> stripes;
    set<CInode*> inodes;
   public:
    fnode_t* get(CStripe *stripe, Mutation *mut);
    inode_t* get(CInode *in, Mutation *mut);
    bool journal(EMetaBlob *blob);
  };

  // stripe fragstat/rstat
  bool update_stripe_stats(CStripe *stripe, Projected &projected,
                           Mutation *mut, EMetaBlob *blob,
                           frag_info_t &fragstat, nest_info_t &rstat);
  // stripe rstat
  bool update_stripe_nest(CStripe *stripe, Projected &projected,
                          Mutation *mut, EMetaBlob *blob,
                          nest_info_t &rstat);
  // inode dirstat/rstat
  bool update_inode_stats(CInode *in, Projected &projected,
                         Mutation *mut, EMetaBlob *blob,
                         frag_info_t &dirstat, nest_info_t &rstat);
  // inode rstat
  bool update_inode_nest(CInode *in, Projected &projected,
                         Mutation *mut, EMetaBlob *blob,
                         nest_info_t &rstat);

  // recursive versions, terminated by non-recursive loop in update_rstats()
  void update_stripe(CStripe *stripe, Projected &projected,
                     Mutation *mut, EMetaBlob *blob,
                     frag_info_t &fragstat, nest_info_t &rstat);

  void update_inode(CInode *in, Projected &projected,
                    Mutation *mut, EMetaBlob *blob,
                    frag_info_t &dirstat, nest_info_t &rstat);

  // stripe rstat -> inode rstat -> ...
  void update_rstats(CInode *in, Projected &projected,
                     Mutation *mut, EMetaBlob *blob,
                     nest_info_t rstat);

  friend class C_PS_StripeFrag;
  friend class C_PS_StripeNest;
  friend class C_PS_InodeAmbig;
  friend class C_PS_InodeFrag;
  friend class C_PS_InodeNest;

 public:
  ParentStats(MDS *mds) : mds(mds), tick_event(NULL) {}

  // update parent stats for the given object
  void update(CInode *in, Mutation *mut, EMetaBlob *blob, frag_info_t fragstat);

  // handle remote requests to update parent stats
  void handle(MParentStats *m);
};

#endif

