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

#include "events/EUpdate.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".container ";
}


CInode* InodeContainer::create()
{
  MDS *mds = mdcache->mds;
  int auth = mds->mdsmap->get_root();
  CInode *in = mdcache->create_system_inode(MDS_INO_CONTAINER, auth, S_IFDIR);

  // XXX: don't know how many nodes in the cluster at this point
  vector<int> stripe_auth(3);
  for (int i = 0; i < 3; ++i)
    stripe_auth[i] = i;

  in->set_stripe_auth(stripe_auth);
  dout(7) << "created " << *in << " with " << *in->get_placement() << dendl;
  return in;
}

struct C_IC_ReOpen : public Context {
  InodeContainer *container;
  Context *c;
  C_IC_ReOpen(InodeContainer *container, Context *c)
      : container(container), c(c) {}
  void finish(int r) {
    container->open(c);
  }
};

bool InodeContainer::is_open()
{
  return in && in->get_placement();
}

void InodeContainer::open(Context *c)
{
  MDS *mds = mdcache->mds;
  int root = mds->mdsmap->get_root();
  if (mds->get_nodeid() == root) {
    dout(7) << "open fetching" << dendl;
    create()->fetch(c);
  } else if (!in) {
    dout(7) << "open discovering inode" << dendl;
    mdcache->discover_ino(MDS_INO_CONTAINER, new C_IC_ReOpen(this, c), root);
  } else if (!in->get_placement()) {
    dout(7) << "open discovering placement" << dendl;
    mdcache->discover_dir_placement(MDS_INO_CONTAINER, c, root);
  } else {
    dout(7) << "open has placement " << *in->get_placement() << dendl;
    c->complete(0);
  }
}

CDentry* InodeContainer::xlock_dentry(MDRequest *mdr, inodeno_t ino,
                                      set<SimpleLock*> &xlocks)
{
  MDS *mds = mdcache->mds;

  stripeid_t stripeid = place(ino);
  CDirPlacement *placement = in->get_placement();
  CDirStripe *stripe = placement->get_or_open_stripe(stripeid);
  assert(stripe);
  dout(12) << "xlock_dentry " << ino << " in " << *stripe << dendl;
  assert(stripe->is_auth());

  if (!stripe->is_open()) {
    stripe->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return NULL;
  }

  // format inode number as dentry name
  char dname[20];
  snprintf(dname, sizeof(dname), "%llx", (unsigned long long)ino.val);

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
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), string(dname));
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

