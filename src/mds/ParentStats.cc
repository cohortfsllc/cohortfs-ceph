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

#include "ParentStats.h"
#include "CStripe.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDS.h"
#include "Mutation.h"
#include "Server.h"

#include "events/EUpdate.h"
#include "messages/MParentStats.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".parentstats "

#define PS_TICK_DELAY_SEC 0.5 // TODO: make configurable


// callbacks
class C_PS_Tick : public Context {
 private:
  ParentStats *ps;
 public:
  C_PS_Tick(ParentStats *ps) : ps(ps) {}
  void finish(int r) { ps->tick(); }
};

class C_PS_Finish : public Context {
 private:
  MDS *mds;
  Mutation *mut;
 public:
  C_PS_Finish(MDS *mds, Mutation *mut) : mds(mds), mut(mut) {}
  ~C_PS_Finish() { delete mut; }
  void finish(int r) {
    mut->ls = mds->mdlog->get_current_segment();
    mut->apply();
    mds->locker->drop_locks(mut);
    mut->cleanup();
  }
};

class C_PS_StripeFrag : public Context {
 private:
  MDS *mds;
  CStripe *stripe;
  Mutation *mut;
  const stripe_stat_update_t supdate;
 public:
  C_PS_StripeFrag(MDS *mds, CStripe *stripe, Mutation *mut,
                  const stripe_stat_update_t &supdate)
      : mds(mds), stripe(stripe), mut(mut), supdate(supdate) {}
  ~C_PS_StripeFrag() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the linklock again
    if (!mut->wrlocks.count(&stripe->linklock) &&
        !mds->locker->wrlock_start(&stripe->linklock, mut, &gather)) {
      gather.set_finisher(new C_PS_StripeFrag(mds, stripe, mut, supdate));
      gather.activate();
      dout(10) << "still waiting on " << stripe->nestlock
          << " for " << stripe->dirstripe() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_stripe(stripe, projected, mut, &le->metablob, supdate);
      mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
    }
    mut = NULL;
  }
};

class C_PS_StripeNest : public Context {
 private:
  MDS *mds;
  CStripe *stripe;
  Mutation *mut;
  stripe_stat_update_t supdate;
 public:
  C_PS_StripeNest(MDS *mds, CStripe *stripe, Mutation *mut,
                  const stripe_stat_update_t &update)
      : mds(mds), stripe(stripe), mut(mut), supdate(update)
  {
    // frag info was already handled
    supdate.frag.delta.zero();
    supdate.frag.stat.zero();
  }
  ~C_PS_StripeNest() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the nestlock again
    if (!mut->wrlocks.count(&stripe->nestlock) &&
        !mds->locker->wrlock_start(&stripe->nestlock, mut, &gather)) {
      gather.set_finisher(new C_PS_StripeNest(mds, stripe, mut, supdate));
      gather.activate();
      dout(10) << "still waiting on " << stripe->nestlock
          << " for " << stripe->dirstripe() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_stripe(stripe, projected, mut, &le->metablob, supdate);
      mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
    }
    mut = NULL;
  }
};

class C_PS_InodeAmbig : public Context {
 private:
  MDS *mds;
  CInode *in;
  Mutation *mut;
  const inode_stat_update_t iupdate;
 public:
  C_PS_InodeAmbig(MDS *mds, CInode *in, Mutation *mut,
                  const inode_stat_update_t &iupdate)
      : mds(mds), in(in), mut(mut), iupdate(iupdate) {}
  ~C_PS_InodeAmbig() { delete mut; }
  void finish(int r) {
    assert(!in->is_ambiguous_auth());
    ParentStats &ps = mds->mdcache->parentstats;
    if (!in->is_auth()) {
      int who = in->authority().first;
      dout(10) << "forwarding to parent auth mds." << who << dendl;
      ps.stats_for_mds(who)->add_inode(in->ino(), iupdate);
    } else {
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_inode(in, projected, mut, &le->metablob, iupdate);
      mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
      mut = NULL;
    }
  }
};

class C_PS_InodeFrag : public Context {
 private:
  MDS *mds;
  CInode *in;
  Mutation *mut;
  const inode_stat_update_t iupdate;
 public:
  C_PS_InodeFrag(MDS *mds, CInode *in, Mutation *mut,
                 const inode_stat_update_t &iupdate)
      : mds(mds), in(in), mut(mut), iupdate(iupdate) {}
  ~C_PS_InodeFrag() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the filelock again
    if (!mut->wrlocks.count(&in->filelock) &&
        !mds->locker->wrlock_start(&in->filelock, mut, &gather)) {
      gather.set_finisher(new C_PS_InodeFrag(mds, in, mut, iupdate));
      gather.activate();
      dout(10) << "still waiting on " << in->filelock
          << " for " << in->ino() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_inode(in, projected, mut, &le->metablob, iupdate);
      mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
    }
    mut = NULL;
  }
};

class C_PS_InodeNest : public Context {
 private:
  MDS *mds;
  CInode *in;
  Mutation *mut;
  inode_stat_update_t iupdate;
 public:
  C_PS_InodeNest(MDS *mds, CInode *in, Mutation *mut,
                 const inode_stat_update_t &update)
      : mds(mds), in(in), mut(mut), iupdate(update)
  {
    // frag info was already handled
    iupdate.frag.delta.zero();
    iupdate.frag.stat.zero();
  }
  ~C_PS_InodeNest() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the nestlock again
    if (!mut->wrlocks.count(&in->nestlock) &&
        !mds->locker->wrlock_start(&in->nestlock, mut, &gather)) {
      gather.set_finisher(new C_PS_InodeNest(mds, in, mut, iupdate));
      gather.activate();
      dout(10) << "still waiting on " << in->nestlock
          << " for " << in->ino() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_inode(in, projected, mut, &le->metablob, iupdate);
      mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
    }
    mut = NULL;
  }
};


// Projected helpers
fnode_t* ParentStats::Projected::get(CStripe *stripe, Mutation *mut)
{
  if (!stripes.insert(stripe).second) // already projected
    return stripe->get_projected_fnode();

  mut->auth_pin(stripe);
  mut->add_projected_fnode(stripe);

  return stripe->project_fnode();
}

inode_t* ParentStats::Projected::get(CInode *in, Mutation *mut)
{
  if (!inodes.insert(in).second) // already projected
    return in->get_projected_inode();

  mut->auth_pin(in);
  mut->add_projected_inode(in);

  return in->project_inode();
}

bool ParentStats::Projected::journal(EMetaBlob *blob)
{
  for (set<CStripe*>::iterator p = stripes.begin(); p != stripes.end(); ++p)
    blob->add_stripe(*p, true);
  for (set<CInode*>::iterator p = inodes.begin(); p != inodes.end(); ++p)
    blob->add_inode(*p, true);
  return stripes.size() || inodes.size();
}


// ParentStats

ParentStats::ParentStats(MDS *mds)
  : mds(mds),
    unaccounted_inodes(member_offset(CInode, item_dirty_rstat)),
    unaccounted_stripes(member_offset(CStripe, item_dirty_rstat)),
    tick_event(NULL)
{
}

MParentStats* ParentStats::stats_for_mds(int who)
{
  // find/create stats for the given mds
  pair<dirty_stats_map::iterator, bool> result =
      dirty_stats.insert(pair<int, MParentStats*>(who, NULL));
  if (result.second)
    result.first->second = new MParentStats;

  if (!tick_event) {
    // start a timer to send stats in bulk
    tick_event = new C_PS_Tick(this);
    mds->timer.add_event_after(PS_TICK_DELAY_SEC, tick_event);
  }
  return result.first->second;
}

// send out pending remote parent stats
void ParentStats::tick()
{
  tick_event = NULL;
  assert(!dirty_stats.empty());

  for (dirty_stats_map::iterator i = dirty_stats.begin(); i != dirty_stats.end(); ++i) {
    assert(i->first != mds->get_nodeid()); // must be remote
    dout(10) << "sending " << *i->second << " to mds." << i->first << dendl;
    mds->send_message_mds(i->second, i->first);
  }
  dirty_stats.clear();
}

CStripe* ParentStats::open_parent_stripe(const inoparent_t &parent,
                                         const stripe_stat_update_t &update)
{
  assert(update.frag.delta.version || update.nest.delta.version);

  if (parent.who != mds->get_nodeid()) {
    dout(10) << "forwarding to auth mds." << parent.who
        << " for parent " << parent << dendl;
    // forward to stripe mds
    stats_for_mds(parent.who)->add_stripe(parent.stripe, update);
    return NULL;
  }

  CStripe *stripe = mds->mdcache->get_dirstripe(parent.stripe);
  assert(stripe); // TODO: fetch stripe and continue
  return stripe;
}

CInode* ParentStats::open_parent_inode(CStripe *stripe,
                                       const Mutation *mut,
                                       const inode_stat_update_t &update)
{
  assert(update.frag.delta.version || update.nest.delta.version);

  CInode *in = stripe->get_inode();
  assert(in);

  if (in->is_ambiguous_auth()) {
    // retry on single auth
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    in->add_waiter(CInode::WAIT_SINGLEAUTH,
                   new C_PS_InodeAmbig(mds, in, newmut, update));
    dout(10) << "waiting on single auth for " << *in << dendl;
    return NULL;
  }
  if (!in->is_auth()) {
    // forward to auth mds
    int who = in->authority().first;
    dout(10) << "forwarding to auth mds." << who
        << " for parent inode " << in->ino() << dendl;
    stats_for_mds(who)->add_inode(in->ino(), update);
    return NULL;
  }

  return in;
}


// stripe.fragstat and stripe.rstat
bool ParentStats::update_stripe_stats(CStripe *stripe, Projected &projected,
                                      Mutation *mut, EMetaBlob *blob,
                                      const stripe_stat_update_t &supdate,
                                      inode_stat_update_t &iupdate)
{
  const frag_delta_t &frag = supdate.frag;
  if (!frag.delta.version) {
    dout(10) << "update_stripe_frag already accounted" << dendl;
    return update_stripe_nest(stripe, projected, mut, blob, supdate, iupdate);
  }

  // requires stripe.linklock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->wrlocks.count(&stripe->linklock) &&
      !mds->locker->wrlock_start(&stripe->linklock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_StripeFrag(mds, stripe, newmut, supdate));
    gather.activate();
    dout(10) << "update_stripe_frag waiting on " << stripe->linklock << dendl;
    return false;
  }

  // inode.dirstat -> stripe.fragstat
  fnode_t *pf = projected.get(stripe, mut);
  assert(pf->fragstat.version >= pf->accounted_fragstat.version);
  pf->fragstat.add(frag.delta);
  pf->fragstat.version++;

  // pin until accounted
  if (!stripe->state_test(CStripe::STATE_DIRTYFRAGSTAT)) {
    stripe->state_set(CStripe::STATE_DIRTYFRAGSTAT);
    stripe->get(CStripe::PIN_DIRTYFRAGSTAT);
  }

  iupdate.frag.delta = frag.delta;
  iupdate.frag.stat = pf->fragstat;

  dout(10) << "update_stripe_frag " << stripe->dirstripe()
      << " " << iupdate.frag << dendl;

  return update_stripe_nest(stripe, projected, mut, blob, supdate, iupdate);
}

// stripe.rstat
bool ParentStats::update_stripe_nest(CStripe *stripe, Projected &projected,
                                     Mutation *mut, EMetaBlob *blob,
                                     const stripe_stat_update_t &supdate,
                                     inode_stat_update_t &iupdate)
{
  const nest_delta_t &nest = supdate.nest;
  if (!nest.delta.version) {
    dout(10) << "update_stripe_nest already accounted" << dendl;
    return false;
  }

  // requires stripe.nestlock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->wrlocks.count(&stripe->nestlock) &&
      !mds->locker->wrlock_start(&stripe->nestlock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_StripeNest(mds, stripe, newmut, supdate));
    gather.activate();
    dout(10) << "update_stripe_nest waiting on " << stripe->nestlock << dendl;
    return false;
  }

  // inode.rstat -> stripe.rstat
  fnode_t *pf = projected.get(stripe, mut);
  assert(pf->rstat.version >= pf->accounted_rstat.version);
  pf->rstat.add(nest.delta);
  pf->rstat.version++;

  // pin until accounted
  if (!stripe->state_test(CStripe::STATE_DIRTYRSTAT)) {
    stripe->state_set(CStripe::STATE_DIRTYRSTAT);
    stripe->get(CStripe::PIN_DIRTYRSTAT);
  }

  iupdate.stripeid = stripe->get_stripeid();
  iupdate.nest.delta = nest.delta;
  iupdate.nest.stat = pf->rstat;

  dout(10) << "update_stripe_nest " << stripe->dirstripe()
     << " " << iupdate.nest << dendl;

  // ack for inode.accounted_rstat
  update_accounted(supdate.ino, projected, mut, nest.stat);
  return true;
}

// inode.dirstat and inode.rstat
bool ParentStats::update_inode_stats(CInode *in, Projected &projected,
                                     Mutation *mut, EMetaBlob *blob,
                                     const inode_stat_update_t &iupdate,
                                     stripe_stat_update_t &supdate)
{
  const frag_delta_t &frag = iupdate.frag;
  if (!frag.delta.version) {
    dout(10) << "update_inode_frag already accounted" << dendl;
    return update_inode_nest(in, projected, mut, blob, iupdate, supdate);
  }

  // requires inode.filelock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->wrlocks.count(&in->filelock) &&
      !mds->locker->wrlock_start(&in->filelock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_InodeFrag(mds, in, newmut, iupdate));
    gather.activate();
    dout(10) << "update_inode_frag waiting on " << in->filelock << dendl;
    return false;
  }

  inode_t *pi = projected.get(in, mut);
  pi->dirstat.add(frag.delta);
  pi->dirstat.version++;

  // frag info does not propagate recursively
  supdate.ino = pi->ino;
  supdate.frag.delta.zero();
  supdate.frag.stat.zero();

  dout(10) << "update_inode_frag " << pi->ino << " " << pi->dirstat << dendl;

  // ack for stripe.accounted_fragstat
  dirstripe_t ds(in->ino(), iupdate.stripeid);
  int who = in->get_stripe_auth(ds.stripeid);
  update_accounted(ds, who, projected, mut, frag.stat, nest_info_t());

  return update_inode_nest(in, projected, mut, blob, iupdate, supdate);
}

// inode.rstat
bool ParentStats::update_inode_nest(CInode *in, Projected &projected,
                                    Mutation *mut, EMetaBlob *blob,
                                    const inode_stat_update_t &iupdate,
                                    stripe_stat_update_t &supdate)
{
  const nest_delta_t &nest = iupdate.nest;
  if (!nest.delta.version) {
    dout(10) << "update_inode_nest already accounted" << dendl;
    return false;
  }

  // requires inode.nestlock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->wrlocks.count(&in->nestlock) &&
      !mds->locker->wrlock_start(&in->nestlock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_InodeNest(mds, in, newmut, iupdate));
    gather.activate();
    dout(10) << "update_inode_nest waiting on " << in->nestlock << dendl;
    return false;
  }

  inode_t *pi = projected.get(in, mut);
  assert(pi->rstat.version >= pi->accounted_rstat.version);
  pi->rstat.add(nest.delta);
  pi->rstat.version++;

  // pin until accounted
  if (!in->state_test(CInode::STATE_DIRTYRSTAT)) {
    in->state_set(CInode::STATE_DIRTYRSTAT);
    in->get(CInode::PIN_DIRTYRSTAT);
  }

  supdate.ino = pi->ino;
  supdate.nest.delta = nest.delta;
  supdate.nest.stat = pi->rstat;

  dout(10) << "update_inode_nest " << pi->ino << " " << supdate.nest << dendl;

  // ack for stripe.accounted_rstat
  dirstripe_t ds(in->ino(), iupdate.stripeid);
  int who = in->get_stripe_auth(ds.stripeid);
  update_accounted(ds, who, projected, mut, frag_info_t(), nest.stat);
  return true;
}


// update accounted stats
void ParentStats::update_accounted(dirstripe_t ds, int who,
                                   Projected &projected, Mutation *mut,
                                   const frag_info_t &fragstat,
                                   const nest_info_t &rstat)
{
  if (who == mds->get_nodeid()) {
    CStripe *stripe = mds->mdcache->get_dirstripe(ds);
    if (!stripe) // must be accounted already if it isn't pinned
      return;

    projected.get(stripe, mut);
    account_stripe(stripe, fragstat, rstat);
  } else {
    // queue ack for remote mds
    stats_for_mds(who)->add_stripe_ack(ds, fragstat, rstat);
    dout(10) << "forwarding stripe ack for " << ds << " to mds." << who
        << ": " << fragstat << " " << rstat << dendl;
  }
}

void ParentStats::update_accounted(inodeno_t ino, Projected &projected,
                                   Mutation *mut, const nest_info_t &rstat)
{
  // check cache, or use placement algorithm to locate inode
  CInode *in = mds->mdcache->get_inode(ino);
  InodeContainer *container = mds->mdcache->get_container();
  int who = in ? in->authority().first :
      container->get_inode()->get_stripe_auth(container->place(ino));
  if (who == mds->get_nodeid()) {
    if (!in) // must be accounted already if it isn't pinned
      return;

    projected.get(in, mut);
    account_inode(in, rstat);
  } else {
    // queue ack for remote mds
    stats_for_mds(who)->add_inode_ack(ino, rstat);
    dout(10) << "forwarding inode ack for " << ino << " to mds." << who
        << ": " << rstat << dendl;
  }
}

void ParentStats::account_stripe(CStripe *stripe, const frag_info_t &fragstat,
                                 const nest_info_t &rstat)
{
  fnode_t *pf = stripe->get_projected_fnode();

  assert(pf->fragstat.version >= fragstat.version);
  if (pf->accounted_fragstat.version < fragstat.version) {
    pf->accounted_fragstat = fragstat;
    dout(10) << "fragstat accounted " << pf->accounted_fragstat
        << " for stripe " << stripe->dirstripe() << dendl;

    if (stripe->state_test(CStripe::STATE_DIRTYFRAGSTAT) &&
        pf->accounted_fragstat.version == pf->fragstat.version) {
      stripe->state_clear(CStripe::STATE_DIRTYFRAGSTAT);
      stripe->put(CStripe::PIN_DIRTYFRAGSTAT);
      dout(15) << "fragstat fully accounted for "
          << stripe->dirstripe() << dendl;
    }
  }

  assert(pf->rstat.version >= rstat.version);
  if (pf->accounted_rstat.version < rstat.version) {
    pf->accounted_rstat = rstat;
    dout(10) << "rstat accounted " << pf->accounted_rstat
        << " for stripe " << stripe->dirstripe() << dendl;

    if (stripe->state_test(CStripe::STATE_DIRTYRSTAT) &&
        pf->accounted_rstat.version == pf->rstat.version) {
      stripe->state_clear(CStripe::STATE_DIRTYRSTAT);
      stripe->put(CStripe::PIN_DIRTYRSTAT);
      dout(15) << "rstat fully accounted for "
          << stripe->dirstripe() << dendl;
    }
  }
}

void ParentStats::account_inode(CInode *in, const nest_info_t &rstat)
{
  inode_t *pi = in->get_projected_inode();

  assert(pi->rstat.version >= rstat.version);
  if (pi->accounted_rstat.version < rstat.version) {
    pi->accounted_rstat = rstat;
    dout(10) << "rstat accounted " << pi->accounted_rstat
        << " for inode " << pi->ino << dendl;

    if (in->state_test(CInode::STATE_DIRTYRSTAT) &&
        pi->accounted_rstat.version == pi->rstat.version) {
      in->state_clear(CInode::STATE_DIRTYRSTAT);
      in->put(CInode::PIN_DIRTYRSTAT);
      dout(15) << "rstat fully accounted for " << pi->ino << dendl;
    }
  }
}

// recursive versions
// stripe stats -> inode stats
void ParentStats::update_stripe(CStripe *stripe, Projected &projected,
                                Mutation *mut, EMetaBlob *blob,
                                const stripe_stat_update_t &supdate)
{
  inode_stat_update_t iupdate;
  if (!update_stripe_stats(stripe, projected, mut, blob, supdate, iupdate))
    return;

  CInode *in = open_parent_inode(stripe, mut, iupdate);
  if (in)
    update_inode(in, projected, mut, blob, iupdate);
}

// inode stats -> rstats
void ParentStats::update_inode(CInode *in, Projected &projected,
                               Mutation *mut, EMetaBlob *blob,
                               const inode_stat_update_t &iupdate)
{
  stripe_stat_update_t supdate;
  if (!update_inode_stats(in, projected, mut, blob, iupdate, supdate))
    return;

  update_rstats(in, projected, mut, blob, supdate);
}

// inode.rstat -> stripe.rstat -> ...
void ParentStats::update_rstats(CInode *in, Projected &projected,
                                Mutation *mut, EMetaBlob *blob,
                                stripe_stat_update_t &supdate)
{
  const nest_delta_t &nest = supdate.nest;
  if (!nest.delta.version) {
    dout(10) << "update_rstats already accounted" << dendl;
    return;
  }

  dout(10) << "update_rstats " << supdate << dendl;

  const inode_t *pi = in->get_projected_inode();
  if (pi->parents.empty()) {
    dout(10) << "at base inode " << pi->ino << dendl;
    account_inode(in, supdate.nest.stat);
    return;
  }

  CStripe *stripe = open_parent_stripe(pi->parents.front(), supdate);
  while (stripe) {
    inode_stat_update_t iupdate;
    if (!update_stripe_nest(stripe, projected, mut, blob, supdate, iupdate))
      break;

    in = open_parent_inode(stripe, mut, iupdate);
    if (!in)
      break;

    supdate.ino = in->ino();
    supdate.frag.delta.zero();
    supdate.frag.stat.zero();
    supdate.nest.delta.zero();
    supdate.nest.stat.zero();

    if (!update_inode_nest(in, projected, mut, blob, iupdate, supdate))
      break;

    pi = projected.get(in, mut);
    if (pi->parents.empty()) {
      dout(10) << "at base inode " << pi->ino << dendl;
      account_inode(in, supdate.nest.stat);
      return;
    }
    // continue with next parent stripe
    stripe = open_parent_stripe(pi->parents.front(), supdate);
  }
}

void ParentStats::update(const inode_t *pi, const inoparent_t &parent,
                         Mutation *mut, EMetaBlob *blob,
                         const frag_info_t &fragstat,
                         const nest_info_t &rstat)
{
  Projected projected; // projected stripes and inodes to journal

  stripe_stat_update_t update;
  update.ino = pi->ino;
  update.frag.delta = fragstat;
  update.nest.delta = rstat;

  if (!update.frag.delta.version && !update.nest.delta.version)
    return;

  dout(10) << "update " << parent << " " << update << dendl;

  // find parent stripe, or forward request
  CStripe *stripe;
  if (!parent) {
    // pass empty inoparent to default to first parent
    if (pi->parents.empty()) {
      dout(10) << "at base inode " << pi->ino << dendl;
      return;
    }
    stripe = open_parent_stripe(pi->parents.front(), update);
  } else
    stripe = open_parent_stripe(parent, update);

  // update stripe.fragstat and recursive stats
  if (stripe)
    update_stripe(stripe, projected, mut, blob, update);

  // write updated stripes and inodes to the journal
  projected.journal(blob);
}


void ParentStats::add(const inode_t *pi, const inoparent_t &parent,
                      Mutation *mut, EMetaBlob *blob)
{
  assert(parent);
  Projected projected; // projected stripes and inodes to journal

  stripe_stat_update_t update;
  update.ino = pi->ino;

  if (pi->is_dir())
    update.frag.delta.nsubdirs = 1;
  else
    update.frag.delta.nfiles = 1;
  update.frag.delta.mtime = mut->now;
  update.frag.delta.version = 1;

  update.nest.delta.add(pi->rstat);
  // rbytes only for first parent
  if (!pi->parents.empty() && parent != pi->parents.front())
    update.nest.delta.rbytes = 0;
  update.nest.delta.version = 1;

  dout(10) << "add " << parent << " " << update << dendl;

  // update stripe.fragstat and recursive stats
  CStripe *stripe = open_parent_stripe(parent, update);
  if (stripe)
    update_stripe(stripe, projected, mut, blob, update);

  // write updated stripes and inodes to the journal
  projected.journal(blob);
}

void ParentStats::sub(const inode_t *pi, const inoparent_t &parent,
                      Mutation *mut, EMetaBlob *blob)
{
  assert(parent);
  assert(!pi->parents.empty());
  Projected projected; // projected stripes and inodes to journal

  stripe_stat_update_t update;
  update.ino = pi->ino;

  if (pi->is_dir())
    update.frag.delta.nsubdirs = -1;
  else
    update.frag.delta.nfiles = -1;
  update.frag.delta.mtime = mut->now;
  update.frag.delta.version = 1;

  update.nest.delta.sub(pi->rstat);
  // rbytes only for first parent
  if (parent != pi->parents.front()) {
    update.nest.delta.rbytes = 0;
  } else if (pi->parents.size() > 1 && update.nest.delta.rbytes) {
    // move rbytes to next parent
    list<inoparent_t>::const_iterator next = ++pi->parents.begin();

    stripe_stat_update_t sizeupdate;
    sizeupdate.ino = pi->ino;
    sizeupdate.nest.delta.rbytes = -update.nest.delta.rbytes;
    sizeupdate.nest.delta.version = 1;

    dout(10) << "sub moving " << sizeupdate.nest.delta
        << " to next parent " << *next << dendl;
    CStripe *stripe = open_parent_stripe(*next, sizeupdate);
    if (stripe)
      update_stripe(stripe, projected, mut, blob, sizeupdate);
  }
  update.nest.delta.version = 1;

  dout(10) << "sub " << parent << " " << update << dendl;

  // update stripe.fragstat and recursive stats
  CStripe *stripe = open_parent_stripe(parent, update);
  if (stripe)
    update_stripe(stripe, projected, mut, blob, update);

  // write updated stripes and inodes to the journal
  projected.journal(blob);
}


void ParentStats::handle(MParentStats *m)
{
  int from = m->get_source().num();
  dout(7) << "handle " << *m << " from mds." << from << dendl;

  Projected projected; // projected stripes and inodes to journal

  Mutation *mut = new Mutation();
  EUpdate *le = new EUpdate(mds->mdlog, "remote_parent_stats");

  // account for acks first
  typedef MParentStats::inode_ack_map::iterator inode_ack_iter;
  for (inode_ack_iter i = m->inode_acks.begin(); i != m->inode_acks.end(); ++i) {
    dout(15) << "ack " << i->first << ": " << i->second << dendl;

    update_accounted(i->first, projected, mut, i->second);
  }

  typedef MParentStats::stripe_ack_map::iterator stripe_ack_iter;
  for (stripe_ack_iter s = m->stripe_acks.begin(); s != m->stripe_acks.end(); ++s) {
    dout(15) << "ack " << s->first << ": " << s->second << dendl;

    update_accounted(s->first, mds->get_nodeid(), projected, mut,
                     s->second.first, s->second.second);
  }

  // propagate inode and stripe updates
  typedef MParentStats::inode_map::iterator inode_iter;
  for (inode_iter i = m->inodes.begin(); i != m->inodes.end(); ++i) {
    dout(15) << "ino " << i->first << ": " << i->second << dendl;

    CInode *in = mds->mdcache->get_inode(i->first);
    assert(in); // TODO: fetch from disk
    assert(in->is_auth());

    update_inode(in, projected, mut, &le->metablob, i->second);
  }

  typedef MParentStats::stripe_map::iterator stripe_iter;
  for (stripe_iter s = m->stripes.begin(); s != m->stripes.end(); ++s) {
    dout(15) << "stripe " << s->first << ": " << s->second << dendl;

    CStripe *stripe = mds->mdcache->get_dirstripe(s->first);
    assert(stripe); // TODO: fetch from disk
    assert(stripe->is_auth());

    typedef MParentStats::stripe_update_vec::iterator stripe_update_iter;
    for (stripe_update_iter i = s->second.begin(); i != s->second.end(); ++i)
      update_stripe(stripe, projected, mut, &le->metablob, *i);
  }

  if (!projected.journal(&le->metablob)) {
    // no changes to journal
    mds->locker->drop_locks(mut);
    delete mut;
    delete le;
  } else {
    mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
  }

  m->put();
}


// replay/rejoin
void ParentStats::replay_unaccounted(CStripe *stripe)
{
  fnode_t *pf = stripe->get_projected_fnode();

  // get pins for dirty parent stats
  if (pf->accounted_fragstat.version != pf->fragstat.version &&
      !stripe->state_test(CStripe::STATE_DIRTYFRAGSTAT)) {
    stripe->state_set(CStripe::STATE_DIRTYFRAGSTAT);
    stripe->get(CStripe::PIN_DIRTYFRAGSTAT);
  }
  if (pf->accounted_rstat.version != pf->rstat.version &&
      !stripe->state_test(CStripe::STATE_DIRTYRSTAT)) {
    stripe->state_set(CStripe::STATE_DIRTYRSTAT);
    stripe->get(CStripe::PIN_DIRTYRSTAT);
  }
  unaccounted_stripes.push_back(&stripe->item_dirty_rstat);
}

void ParentStats::replay_unaccounted(CInode *in)
{
  inode_t *pi = in->get_projected_inode();

  // get pin for dirty rstat
  if (pi->accounted_rstat.version != pi->rstat.version &&
      !in->state_test(CInode::STATE_DIRTYRSTAT)) {
    in->state_set(CInode::STATE_DIRTYRSTAT);
    in->get(CInode::PIN_DIRTYRSTAT);
  }
  unaccounted_inodes.push_back(&in->item_dirty_rstat);
}

void ParentStats::propagate_unaccounted()
{
  dout(10) << "propagate_unaccounted" << dendl;
  assert(mds->mdcache->is_open());

  Projected projected; // projected stripes and inodes to journal

  Mutation *mut = new Mutation();
  EUpdate *le = new EUpdate(mds->mdlog, "replay_parent_stats");

  for (elist<CStripe*>::iterator s = unaccounted_stripes.begin(); !s.end(); ++s) {
    CStripe *stripe = *s;
    stripe->item_dirty_rstat.remove_myself();

    fnode_t *pf = projected.get(stripe, mut);

    inode_stat_update_t update;
    update.stripeid = stripe->get_stripeid();

    // get fragstat delta, or drop dirty fragstat pin
    if (pf->fragstat.version != pf->accounted_fragstat.version) {
      bool mtime = false; // ignored
      update.frag.delta.add_delta(pf->fragstat, pf->accounted_fragstat, mtime);
      update.frag.delta.version = 1;
      update.frag.stat = pf->fragstat;
    } else if (stripe->state_test(CStripe::STATE_DIRTYFRAGSTAT)) {
      stripe->state_clear(CStripe::STATE_DIRTYFRAGSTAT);
      stripe->put(CStripe::PIN_DIRTYFRAGSTAT);
    }
    // get rstat delta, or drop dirty rstat pin
    if (pf->rstat.version != pf->accounted_rstat.version) {
      update.nest.delta.add_delta(pf->rstat, pf->accounted_rstat);
      update.nest.delta.version = 1;
      update.nest.stat = pf->rstat;
    } else if (stripe->state_test(CStripe::STATE_DIRTYRSTAT)) {
      stripe->state_clear(CStripe::STATE_DIRTYRSTAT);
      stripe->put(CStripe::PIN_DIRTYRSTAT);
    }

    if (update.frag.delta.version == 0 && update.nest.delta.version == 0)
      continue;

    CInode *in = open_parent_inode(stripe, mut, update);
    if (in)
      update_inode(in, projected, mut, &le->metablob, update);
  }
  assert(unaccounted_stripes.empty());

  for (elist<CInode*>::iterator i = unaccounted_inodes.begin(); !i.end(); ++i) {
    CInode *in = *i;
    in->item_dirty_rstat.remove_myself();

    inode_t *pi = projected.get(in, mut);

    stripe_stat_update_t update;
    update.ino = pi->ino;

    // get rstat delta, or drop dirty rstat pin
    if (pi->rstat.version != pi->accounted_rstat.version) {
      update.nest.delta.add_delta(pi->rstat, pi->accounted_rstat);
      update.nest.delta.version = 1;
      update.nest.stat = pi->rstat;
    } else if (in->state_test(CInode::STATE_DIRTYRSTAT)) {
      in->state_clear(CInode::STATE_DIRTYRSTAT);
      in->put(CInode::PIN_DIRTYRSTAT);
    }

    if (update.nest.delta.version == 0)
      continue;

    if (pi->parents.empty()) {
      dout(10) << "at base inode " << pi->ino << dendl;
      account_inode(in, update.nest.stat);
      return;
    }
    CStripe *stripe = open_parent_stripe(pi->parents.front(), update);
    if (stripe)
      update_stripe(stripe, projected, mut, &le->metablob, update);
  }
  assert(unaccounted_inodes.empty());

  if (!projected.journal(&le->metablob)) {
    // no changes to journal
    mds->locker->drop_locks(mut);
    delete mut;
    delete le;
  } else {
    mds->mdlog->start_submit_entry(le, new C_PS_Finish(mds, mut));
  }
}

