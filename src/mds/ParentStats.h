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

#include "include/types.h"
#include "include/elist.h"

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

  elist<CInode*> unaccounted_inodes;
  elist<CStripe*> unaccounted_stripes;

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
  CStripe* open_parent_stripe(CInode *in, const stripe_stat_update_t &supdate);
  CInode* open_parent_inode(CStripe *stripe, const Mutation *mut,
                            const inode_stat_update_t &iupdate);

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
                           const stripe_stat_update_t &supdate,
                           inode_stat_update_t &iupdate);
  // stripe rstat
  bool update_stripe_nest(CStripe *stripe, Projected &projected,
                          Mutation *mut, EMetaBlob *blob,
                          const stripe_stat_update_t &supdate,
                          inode_stat_update_t &iupdate);
  // inode dirstat/rstat
  bool update_inode_stats(CInode *in, Projected &projected,
                         Mutation *mut, EMetaBlob *blob,
                         const inode_stat_update_t &iupdate,
                         stripe_stat_update_t &supdate);
  // inode rstat
  bool update_inode_nest(CInode *in, Projected &projected,
                         Mutation *mut, EMetaBlob *blob,
                         const inode_stat_update_t &iupdate,
                         stripe_stat_update_t &supdate);

  // recursive versions, terminated by non-recursive loop in update_rstats()
  void update_stripe(CStripe *stripe, Projected &projected,
                     Mutation *mut, EMetaBlob *blob,
                     const stripe_stat_update_t &supdate);

  void update_inode(CInode *in, Projected &projected,
                    Mutation *mut, EMetaBlob *blob,
                    const inode_stat_update_t &iupdate);

  // stripe rstat -> inode rstat -> ...
  void update_rstats(CInode *in, Projected &projected,
                     Mutation *mut, EMetaBlob *blob,
                     stripe_stat_update_t &supdate);

  friend class C_PS_StripeFrag;
  friend class C_PS_StripeNest;
  friend class C_PS_InodeAmbig;
  friend class C_PS_InodeFrag;
  friend class C_PS_InodeNest;

  // update accounted stats, or send an ack to the auth mds
  void update_accounted(dirstripe_t ds, int who,
                        Projected &projected, Mutation *mut,
                        const frag_info_t &accounted_fragstat,
                        const nest_info_t &accounted_rstat);
  void update_accounted(inodeno_t ino, Projected &projected, Mutation *mut,
                        const nest_info_t &accounted_rstat);

  void account_stripe(CStripe *stripe, const frag_info_t &fragstat,
                      const nest_info_t &rstat);
  void account_inode(CInode *in, const nest_info_t &rstat);

 public:
  ParentStats(MDS *mds);

  // update parent stats for the given object
  void update(CInode *in, Mutation *mut, EMetaBlob *blob,
              const frag_info_t &fragstat,
              const nest_info_t &rstat);

  // handle remote requests to update parent stats
  void handle(MParentStats *m);

  // collect the set of unaccounted parent stats during replay
  void replay_unaccounted(CStripe *stripe);
  void replay_unaccounted(CInode *in);

  // start to propagate unaccounted parent stats from replay
  void propagate_unaccounted();
};

#endif

