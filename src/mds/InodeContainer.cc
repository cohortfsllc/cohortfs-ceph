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


#include "InodeContainer.h"

#include "include/Context.h"

#include "MDCache.h"
#include "MDS.h"
#include "MDLog.h"
#include "Mutation.h"

#include "messages/MMDSRestripe.h"
#include "messages/MMDSRestripeAck.h"

#include "events/EUpdate.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".container ";
}


CInode* InodeContainer::create()
{
  int auth = mdcache->mds->mdsmap->get_root();
  return mdcache->create_system_inode(MDS_INO_CONTAINER, auth, S_IFDIR);
}

void InodeContainer::open(Context *c)
{
  MDS *mds = mdcache->mds;
  if (mds->whoami == mds->mdsmap->get_root()) {
    dout(7) << "open fetching" << dendl;
    create()->fetch(c);
  } else {
    dout(7) << "open discovering" << dendl;
    mdcache->discover_ino(MDS_INO_CONTAINER, c, mds->mdsmap->get_root());
  }
}

CDentry* InodeContainer::xlock_dentry(MDRequest *mdr, inodeno_t ino,
                                      set<SimpleLock*> &xlocks)
{
  MDS *mds = mdcache->mds;

  // format inode number as dentry name
  char dname[20];
  snprintf(dname, sizeof(dname), "%llx", (unsigned long long)ino.val);

  dout(12) << "xlock_dentry " << ino << dendl;

  CDirPlacement *placement = in->get_placement();
  if (!stripe) {
    // find our stripeid and open it
    const vector<int> &stripe_auth = placement->get_stripe_auth();
    vector<int>::const_iterator i = find(stripe_auth.begin(),
                                         stripe_auth.end(),
                                         mds->get_nodeid());
    assert(i != stripe_auth.end());

    stripeid_t stripeid = i - stripe_auth.begin();
    stripe = placement->get_stripe(stripeid);
  }
  assert(stripe);
  // pick the dirfrag
  frag_t fg = stripe->pick_dirfrag(dname);
  CDirFrag *dir = stripe->get_or_open_dirfrag(fg);
  assert(dir);

  if (dir->is_freezing_or_frozen()) {
    dout(7) << "waiting on frozen inode container " << *dir << dendl;
    dir->add_waiter(CDirFrag::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    mdr->drop_local_auth_pins();
    return NULL;
  }

  if (!dir->is_complete()) {
    dout(7) << "waiting on incomplete inode container " << *dir << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return NULL;
  }

  // find/create the dentry
  CDentry *dn = dir->lookup(dname);
  if (dn == NULL)
    dn = dir->add_null_dentry(dname);
  else
    assert(dn->get_linkage()->is_null()); // must not exist

  xlocks.insert(&dn->lock);
  return dn;
}


stripeid_t InodeContainer::place(inodeno_t ino) const
{
  static const uint64_t SHIFT = 40; // see InoTable
  assert(in);
  return ((ino >> SHIFT) - 1) % in->get_placement()->get_stripe_count();
}


// ====================================================================
// restriping

class C_IC_RestripeFinish : public Context {
 private:
  InodeContainer *container;
 public:
  C_IC_RestripeFinish(InodeContainer *container) : container(container) {}
  void finish(int r) {
    assert(r == 0);
    container->restripe_finish();
  }
};

void InodeContainer::restripe(const set<int> &nodes, bool replay)
{
  MDS *mds = mdcache->mds;
  assert(mds->is_restripe());
  assert(mds->get_nodeid() == mds->mdsmap->get_root());
  assert(in);

  dout(7) << "restripe over nodes " << nodes << ": " << *in << dendl;

  // update stripe_auth
  const vector<int> stripe_auth(nodes.begin(), nodes.end());
  // stripe 0 must always be root
  assert(stripe_auth[0] == mds->mdsmap->get_root());

  EUpdate *le = NULL;
  if (replay) // no journal for replay
    assert(in->get_placement()->get_stripe_auth() == stripe_auth);
  else {
    in->set_stripe_auth(stripe_auth);

    le = new EUpdate(mds->mdlog, "restripe");
    le->metablob.add_inode(in, false);
  }

  C_GatherBuilder gather(g_ceph_context, new C_IC_RestripeFinish(this));

  CDirPlacement *placement = in->get_placement();
  for (size_t i = 0; i < stripe_auth.size(); i++) {
    int to = stripe_auth[i];
    if (to == mds->get_nodeid()) {
      stripe = placement->get_stripe(i);
      if (stripe == NULL) {
        stripe = placement->add_stripe(new CDirStripe(placement, i, to));
        if (le)
          le->metablob.add_stripe(stripe, false);
        else
          stripe->fetch(gather.new_sub());
      }
    } else {
      assert(mds->mdsmap->is_restripe(to));
      // alert the mds that it's auth for the given stripe
      MMDSRestripe *m = new MMDSRestripe(i, replay);
      // include a replica with the updated stripe_auth
      mdcache->replicate_inode(in, to, m->container);
      mdcache->replicate_placement(placement, to, m->container);

      dout(7) << "restripe sending " << *m << " to mds." << to << dendl;
      mds->send_message_mds(m, to);

      // waiting on ack
      assert(pending_restripe_ack.count(to) == 0);
      pending_restripe_ack.insert(to);
    }
  }

  if (pending_restripe_ack.size())
    pending_restripe_finish = gather.new_sub();

  if (le)
    mds->mdlog->start_submit_entry(le, gather.new_sub());

  assert(gather.has_subs());
  gather.activate();
}

void InodeContainer::handle_restripe(MMDSRestripe *m)
{
  MDS *mds = mdcache->mds;
  int from = m->get_source().num();

  dout(7) << "handle_restripe " << *m << " from mds." << from << dendl;

  assert(mds->get_nodeid() != mds->mdsmap->get_root());
  assert(from == mds->mdsmap->get_root());

  // decode inode container replica with new stripe_auth
  bufferlist::iterator p = m->container.begin();

  std::list<Context*> unused;
  in = mdcache->add_replica_inode(p, NULL, from, unused);
  assert(in);
  CDirPlacement *placement = mdcache->add_replica_placement(p, in, from, unused);
  assert(placement);

  // open my inode container stripe and claim auth
  stripe = placement->get_stripe(m->stripeid);
  if (!stripe) {
    stripe = placement->add_stripe(
        new CDirStripe(placement, m->stripeid, mds->get_nodeid()));
    stripe->set_stripe_auth(mds->get_nodeid());
  }

  C_GatherBuilder gather(g_ceph_context);

  if (!stripe->is_open()) {
    if (m->replay)
      stripe->fetch(gather.new_sub());
    else
      stripe->mark_open();
  }

  dout(10) << "handle_restripe opened " << *stripe << " in " << *in << dendl;

  if (!m->replay) {
    EUpdate *le = new EUpdate(mds->mdlog, "restripe");
    le->metablob.add_inode(in, false);
    le->metablob.add_stripe(stripe, false, false);
    // TODO: journal placement object
    mds->mdlog->start_submit_entry(le, gather.new_sub());
  }

  if (gather.has_subs()) {
    gather.set_finisher(new C_IC_RestripeFinish(this));
    gather.activate();
  } else
    restripe_finish();

  m->put();
}

void InodeContainer::handle_restripe_ack(MMDSRestripeAck *m)
{
  MDS *mds = mdcache->mds;
  int from = m->get_source().num();

  dout(7) << "handle_restripe_ack " << *m << " from mds." << from << dendl;

  assert(mds->get_nodeid() == mds->mdsmap->get_root());
  assert(pending_restripe_ack.count(from));
  pending_restripe_ack.erase(from);

  if (pending_restripe_ack.empty()) {
    pending_restripe_finish->complete(0);
    pending_restripe_finish = NULL;
  }

  m->put();
}

void InodeContainer::restripe_finish()
{
  MDS *mds = mdcache->mds;
  int root = mds->mdsmap->get_root();

  dout(7) << "restripe_finish" << dendl;

  if (mds->get_nodeid() != root) {
    // reply to root mds with an ack
    MMDSRestripeAck *ack = new MMDSRestripeAck(stripe->get_stripeid());
    dout(10) << "handle_restripe sending " << *ack << " to mds." << root << dendl;
    mds->send_message_mds(ack, root);
  }

  mds->restripe_done();
}

