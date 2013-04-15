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
#include "Mutation.h"

#include "messages/MMDSRestripe.h"
#include "messages/MMDSRestripeAck.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".container ";
}


CInode* InodeContainer::create()
{
  return mdcache->create_system_inode(MDS_INO_CONTAINER, S_IFDIR);
}

void InodeContainer::open(Context *c)
{
  MDS *mds = mdcache->mds;
  if (mds->whoami == mds->mdsmap->get_root()) {
    dout(7) << "open fetching" << dendl;
    create()->fetch(c);
  } else {
    dout(7) << "open discovering" << dendl;
    mdcache->discover_base_ino(MDS_INO_CONTAINER, c, mds->mdsmap->get_root());
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

  // pick the dirfrag
  assert(stripe);
  frag_t fg = stripe->pick_dirfrag(dname);
  CDir *dir = stripe->get_or_open_dirfrag(fg);
  assert(dir);

  if (dir->is_freezing_or_frozen()) {
    dout(7) << "waiting on frozen inode container " << *dir << dendl;
    dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    mdr->drop_local_auth_pins();
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


// ====================================================================
// restriping

void InodeContainer::restripe(const set<int> &nodes)
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
  in->set_stripe_auth(stripe_auth);

  for (size_t i = 0; i < stripe_auth.size(); i++) {
    int to = stripe_auth[i];
    if (to == mds->get_nodeid()) {
      stripe = in->get_stripe(i);
      if (stripe == NULL) {
        stripe = in->add_stripe(new CStripe(in, i, true));
        mdcache->adjust_subtree_auth(stripe, mds->get_nodeid());
      }
    } else {
      // alert the mds that it's auth for the given stripe
      MMDSRestripe *m = new MMDSRestripe(i);
      // include a replica with the updated stripe_auth
      mdcache->replicate_inode(in, to, m->container);

      dout(7) << "restripe sending " << *m << " to mds." << to << dendl;
      mds->send_message_mds(m, to);

      // waiting on ack
      assert(pending_restripe_ack.count(to) == 0);
      pending_restripe_ack.insert(to);
    }
  }
  assert(!pending_restripe_ack.empty());
}

void InodeContainer::handle_restripe(MMDSRestripe *m)
{
  MDS *mds = mdcache->mds;
  int from = m->get_source().num();

  dout(7) << "handle_restripe " << *m << " from mds." << from << dendl;

  // decode inode container replica with new stripe_auth
  bufferlist::iterator p = m->container.begin();

  std::list<Context*> unused;
  in = mdcache->add_replica_inode(p, NULL, unused);
  assert(in);
  assert(!in->is_auth());

  // open my inode container fragment and claim auth
  stripe = in->add_stripe(new CStripe(in, m->stripeid, true));
  mdcache->adjust_subtree_auth(stripe, mds->get_nodeid());

  dout(10) << "handle_restripe opened " << *stripe << " in " << *in << dendl;

  MMDSRestripeAck *ack = new MMDSRestripeAck(m->stripeid);
  dout(10) << "handle_restripe sending " << *ack << " to mds." << from << dendl;
  mds->send_message_mds(ack, from);
  mds->restripe_done();

  m->put();
}

void InodeContainer::handle_restripe_ack(MMDSRestripeAck *m)
{
  MDS *mds = mdcache->mds;
  int from = m->get_source().num();

  dout(7) << "handle_restripe_ack " << *m << " from mds." << from << dendl;

  assert(pending_restripe_ack.count(from));
  pending_restripe_ack.erase(from);

  if (pending_restripe_ack.empty())
    mds->restripe_done();

  m->put();
}

