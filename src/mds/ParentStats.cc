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
  frag_info_t fragstat;
  nest_info_t rstat;
 public:
  C_PS_StripeFrag(MDS *mds, CStripe *stripe, Mutation *mut,
                  const frag_info_t &fragstat, const nest_info_t &rstat)
      : mds(mds), stripe(stripe), mut(mut), fragstat(fragstat), rstat(rstat) {}
  ~C_PS_StripeFrag() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the linklock again
    if (!mut->xlocks.count(&stripe->linklock) &&
        !mds->locker->xlock_start(&stripe->linklock, mut, &gather)) {
      gather.set_finisher(new C_PS_StripeFrag(mds, stripe, mut, fragstat, rstat));
      gather.activate();
      dout(10) << "still waiting on " << stripe->nestlock
          << " for " << stripe->dirstripe() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_stripe(stripe, projected, mut, &le->metablob, fragstat, rstat);
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
  nest_info_t rstat;
 public:
  C_PS_StripeNest(MDS *mds, CStripe *stripe, Mutation *mut,
                  const nest_info_t &rstat)
      : mds(mds), stripe(stripe), mut(mut), rstat(rstat) {}
  ~C_PS_StripeNest() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the nestlock again
    if (!mut->xlocks.count(&stripe->nestlock) &&
        !mds->locker->xlock_start(&stripe->nestlock, mut, &gather)) {
      gather.set_finisher(new C_PS_StripeNest(mds, stripe, mut, rstat));
      gather.activate();
      dout(10) << "still waiting on " << stripe->nestlock
          << " for " << stripe->dirstripe() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      frag_info_t empty;
      ps.update_stripe(stripe, projected, mut, &le->metablob, empty, rstat);
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
  frag_info_t dirstat;
  nest_info_t rstat;
 public:
  C_PS_InodeAmbig(MDS *mds, CInode *in, Mutation *mut,
                  const frag_info_t &dirstat, const nest_info_t &rstat)
      : mds(mds), in(in), mut(mut), dirstat(dirstat), rstat(rstat) {}
  ~C_PS_InodeAmbig() { delete mut; }
  void finish(int r) {
    assert(!in->is_ambiguous_auth());
    ParentStats &ps = mds->mdcache->parentstats;
    if (!in->is_auth()) {
      int who = in->authority().first;
      dout(10) << "forwarding to parent auth mds." << who << dendl;
      ps.stats_for_mds(who)->add_inode(in->ino(), dirstat, rstat);
    } else {
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_inode(in, projected, mut, &le->metablob, dirstat, rstat);
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
  frag_info_t dirstat;
  nest_info_t rstat;
 public:
  C_PS_InodeFrag(MDS *mds, CInode *in, Mutation *mut,
                  const frag_info_t &dirstat, const nest_info_t &rstat)
      : mds(mds), in(in), mut(mut), dirstat(dirstat), rstat(rstat) {}
  ~C_PS_InodeFrag() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the filelock again
    if (!mut->wrlocks.count(&in->filelock) &&
        !mds->locker->wrlock_start(&in->filelock, mut, &gather)) {
      gather.set_finisher(new C_PS_InodeFrag(mds, in, mut, dirstat, rstat));
      gather.activate();
      dout(10) << "still waiting on " << in->filelock
          << " for " << in->ino() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      ps.update_inode(in, projected, mut, &le->metablob, dirstat, rstat);
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
  nest_info_t rstat;
 public:
  C_PS_InodeNest(MDS *mds, CInode *in, Mutation *mut, const nest_info_t &rstat)
      : mds(mds), in(in), mut(mut), rstat(rstat) {}
  ~C_PS_InodeNest() { delete mut; }
  void finish(int r) {
    ParentStats &ps = mds->mdcache->parentstats;
    C_GatherBuilder gather(g_ceph_context);
    // attempt to take the nestlock again
    if (!mut->wrlocks.count(&in->nestlock) &&
        !mds->locker->wrlock_start(&in->nestlock, mut, &gather)) {
      gather.set_finisher(new C_PS_InodeNest(mds, in, mut, rstat));
      gather.activate();
      dout(10) << "still waiting on " << in->nestlock
          << " for " << in->ino() << dendl;
    } else {
      // only after we get the lock, create a log entry and do the update
      ParentStats::Projected projected;
      EUpdate *le = new EUpdate(mds->mdlog, "parent_stats");
      frag_info_t empty;
      ps.update_inode(in, projected, mut, &le->metablob, empty, rstat);
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

CStripe* ParentStats::open_parent_stripe(inode_t *pi,
                                         const frag_info_t &fragstat,
                                         const nest_info_t &rstat)
{
  // find first parent
  if (pi->parents.empty()) {
    dout(10) << "open_parent_stripe at base inode " << pi->ino << dendl;
    return NULL;
  }

  const inoparent_t &parent = pi->parents.front();
  if (parent.who != mds->get_nodeid()) {
    dout(10) << "forwarding to auth mds." << parent.who
        << " for parent " << parent.stripe << ":" << parent.name << dendl;
    // forward to stripe mds
    stats_for_mds(parent.who)->add_stripe(parent.stripe, fragstat, rstat);
    return NULL;
  }

  CStripe *stripe = mds->mdcache->get_dirstripe(parent.stripe);
  assert(stripe); // TODO: fetch stripe and continue
  return stripe;
}

CInode* ParentStats::open_parent_inode(CStripe *stripe, const Mutation *mut,
                                       const frag_info_t &dirstat,
                                       const nest_info_t &rstat)
{
  CInode *in = stripe->get_inode();
  assert(in);

  if (in->is_ambiguous_auth()) {
    // retry on single auth
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    in->add_waiter(CInode::WAIT_SINGLEAUTH,
                   new C_PS_InodeAmbig(mds, in, newmut, dirstat, rstat));
    dout(10) << "waiting on single auth for " << *in << dendl;
    return NULL;
  }
  if (!in->is_auth()) {
    // forward to auth mds
    int who = in->authority().first;
    dout(10) << "forwarding to auth mds." << who
        << " for parent inode " << in->ino() << dendl;
    stats_for_mds(who)->add_inode(in->ino(), dirstat, rstat);
    return NULL;
  }

  return in;
}


// stripe.fragstat and stripe.rstat
bool ParentStats::update_stripe_stats(CStripe *stripe, Projected &projected,
                                      Mutation *mut, EMetaBlob *blob,
                                      frag_info_t &fragstat, nest_info_t &rstat)
{
  if (!fragstat.size() && fragstat.mtime.is_zero()) {
    dout(10) << "update_stripe_frag empty fragstat" << dendl;
    return update_stripe_nest(stripe, projected, mut, blob, rstat);
  }

  // requires stripe.linklock from caller
  assert(mut->xlocks.count(&stripe->linklock));

  // inode.dirstat -> stripe.fragstat
  fnode_t *pf = projected.get(stripe, mut);
  assert(pf->fragstat.version >= pf->accounted_fragstat.version);
  pf->fragstat.add(fragstat);
  pf->fragstat.version++;

  dout(10) << "update_stripe_frag " << stripe->dirstripe()
      << " " << pf->fragstat << dendl;

  return update_stripe_nest(stripe, projected, mut, blob, rstat);
}

// stripe.rstat
bool ParentStats::update_stripe_nest(CStripe *stripe, Projected &projected,
                                     Mutation *mut, EMetaBlob *blob,
                                     nest_info_t &rstat)
{
  if (!rstat.rsize() && rstat.rctime.is_zero()) {
    dout(10) << "update_stripe_nest empty rstat" << dendl;
    return false;
  }

  // requires stripe.nestlock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->xlocks.count(&stripe->nestlock) &&
      !mds->locker->xlock_start(&stripe->nestlock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_StripeNest(mds, stripe, newmut, rstat));
    gather.activate();
    dout(10) << "update_stripe_nest waiting on " << stripe->nestlock << dendl;
    return false;
  }

  // inode.rstat -> stripe.rstat
  fnode_t *pf = projected.get(stripe, mut);
  assert(pf->rstat.version >= pf->accounted_rstat.version);
  pf->rstat.add(rstat);
  pf->rstat.version++;

  dout(10) << "update_stripe_nest " << stripe->dirstripe()
     << " " << pf->rstat << dendl;
  return true;
}

// inode.dirstat and inode.rstat
bool ParentStats::update_inode_stats(CInode *in, Projected &projected,
                                     Mutation *mut, EMetaBlob *blob,
                                     frag_info_t &dirstat, nest_info_t &rstat)
{
  if (!dirstat.size() && dirstat.mtime.is_zero()) {
    dout(10) << "update_inode_frag empty dirstat" << dendl;
    return update_inode_nest(in, projected, mut, blob, rstat);
  }

  // requires inode.filelock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->wrlocks.count(&in->filelock) &&
      !mds->locker->wrlock_start(&in->filelock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_InodeFrag(mds, in, newmut, dirstat, rstat));
    gather.activate();
    dout(10) << "update_inode_frag waiting on " << in->filelock << dendl;
    return false;
  }

  inode_t *pi = projected.get(in, mut);
  pi->dirstat.add(dirstat);
  pi->dirstat.version++;

  // frag info does not propagate recursively
  dirstat.zero();

  dout(10) << "update_inode_frag " << pi->ino << " " << pi->dirstat << dendl;

  return update_inode_nest(in, projected, mut, blob, rstat);
}

// inode.rstat
bool ParentStats::update_inode_nest(CInode *in, Projected &projected,
                                    Mutation *mut, EMetaBlob *blob,
                                    nest_info_t &rstat)
{
  if (!rstat.rsize() && rstat.rctime.is_zero()) {
    dout(10) << "update_inode_nest empty rstat" << dendl;
    return false;
  }

  // requires inode.nestlock
  C_GatherBuilder gather(g_ceph_context);
  if (!mut->wrlocks.count(&in->nestlock) &&
      !mds->locker->wrlock_start(&in->nestlock, mut, &gather)) {
    Mutation *newmut = new Mutation(mut->reqid, mut->attempt);
    gather.set_finisher(new C_PS_InodeNest(mds, in, newmut, rstat));
    gather.activate();
    dout(10) << "update_inode_nest waiting on " << in->nestlock << dendl;
    return false;
  }

  inode_t *pi = projected.get(in, mut);
  assert(pi->rstat.version >= pi->accounted_rstat.version);
  pi->rstat.add(rstat);
  pi->rstat.version++;

  dout(10) << "update_inode_nest " << pi->ino << " " << pi->rstat << dendl;
  return true;
}


// recursive versions
// stripe stats -> inode stats
void ParentStats::update_stripe(CStripe *stripe, Projected &projected,
                                Mutation *mut, EMetaBlob *blob,
                                frag_info_t &fragstat, nest_info_t &rstat)
{
  if (!update_stripe_stats(stripe, projected, mut, blob, fragstat, rstat))
    return;

  CInode *in = open_parent_inode(stripe, mut, fragstat, rstat);
  if (in)
    update_inode(in, projected, mut, blob, fragstat, rstat);
}

// inode stats -> rstats
void ParentStats::update_inode(CInode *in, Projected &projected,
                               Mutation *mut, EMetaBlob *blob,
                               frag_info_t &dirstat, nest_info_t &rstat)
{
  if (!update_inode_stats(in, projected, mut, blob, dirstat, rstat))
    return;

  update_rstats(in, projected, mut, blob, rstat);
}

// inode.rstat -> stripe.rstat -> ...
void ParentStats::update_rstats(CInode *in, Projected &projected,
                                Mutation *mut, EMetaBlob *blob,
                                nest_info_t rstat)
{
  if (!rstat.rsize() && rstat.rctime.is_zero()) {
    dout(10) << "update_rstats empty rstat" << dendl;
    return;
  }

  dout(10) << "update_rstats " << rstat << dendl;
  frag_info_t empty;

  CStripe *stripe = open_parent_stripe(in->get_projected_inode(), empty, rstat);
  while (stripe) {
    if (!update_stripe_nest(stripe, projected, mut, blob, rstat))
      break;

    in = open_parent_inode(stripe, mut, empty, rstat);
    if (!in)
      break;

    if (!update_inode_nest(in, projected, mut, blob, rstat))
      break;

    // continue with next parent stripe
    stripe = open_parent_stripe(in->get_projected_inode(), empty, rstat);
  }
}

void ParentStats::update(CInode *in, Mutation *mut, EMetaBlob *blob,
                         frag_info_t fragstat)
{
  Projected projected; // projected stripes and inodes to journal

  inode_t *pi = in->get_projected_inode();

  nest_info_t rstat;
  rstat.add_delta(pi->rstat, pi->accounted_rstat);

  dout(10) << "update " << in->ino() << " " << fragstat << dendl;

  // find first parent, or forward request
  CStripe *stripe = open_parent_stripe(pi, fragstat, rstat);

  // update stripe.fragstat and recursive stats
  if (stripe)
    update_stripe(stripe, projected, mut, blob, fragstat, rstat);

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

  // propagate inode and stripe updates
  typedef MParentStats::inode_map::iterator inode_iter;
  for (inode_iter i = m->inodes.begin(); i != m->inodes.end(); ++i) {
    dout(15) << "ino " << i->first << ": " << i->second << dendl;

    CInode *in = mds->mdcache->get_inode(i->first);
    assert(in); // TODO: fetch from disk
    assert(in->is_auth());

    update_inode(in, projected, mut, &le->metablob,
                 i->second.first, i->second.second);
  }

  typedef MParentStats::stripe_map::iterator stripe_iter;
  for (stripe_iter s = m->stripes.begin(); s != m->stripes.end(); ++s) {
    dout(15) << "stripe " << s->first << ": " << s->second << dendl;

    CStripe *stripe = mds->mdcache->get_dirstripe(s->first);
    assert(stripe); // TODO: fetch from disk
    assert(stripe->is_auth());

    update_stripe(stripe, projected, mut, &le->metablob,
                  s->second.first, s->second.second);
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

