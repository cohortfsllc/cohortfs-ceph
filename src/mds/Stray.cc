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

#include "Stray.h"

#include "CDentry.h"
#include "CInode.h"
#include "CStripe.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDS.h"

#include "osdc/Filer.h"
#include "events/EUpdate.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".stray "


Stray::Stray(MDS *mds)
  : mds(mds),
    inodes(member_offset(CInode, item_stray)),
    stripes(member_offset(CStripe, item_stray))
{
}

void Stray::add(CInode *in)
{
  inodes.push_back(&in->item_stray);
}

void Stray::add(CStripe *stripe)
{
  stripes.push_back(&stripe->item_stray);
}

void Stray::scan()
{
  dout(10) << "scan" << dendl;

  for (elist<CStripe*>::iterator s = stripes.begin(); !s.end(); ++s) {
    CStripe *stripe = *s;
    stripe->item_stray.remove_myself();
    eval(stripe);
  }
  assert(stripes.empty());

  for (elist<CInode*>::iterator i = inodes.begin(); !i.end(); ++i) {
    CInode *in = *i;
    in->item_stray.remove_myself();
    eval(in);
  }
  assert(inodes.empty());
}


void Stray::eval(CInode *in)
{
  dout(10) << "eval " << *in << dendl;

  if (in->is_base()) {
    dout(20) << " is base" << dendl;
    return;
  }
  if (in->inode.nlink) {
    dout(20) << " links=" << in->inode.nlink
        << " " << in->inode.parents << dendl;
    return;
  }

  CDentry *dn = in->get_projected_parent_dn();
  if (dn->is_replicated()) {
    dout(20) << " replicated" << dendl;
    return;
  }
  if (dn->is_any_leases() || in->is_any_caps()) {
    dout(20) << " caps | leases" << dendl;
    return; // wait
  }
  list<CStripe*> stripes;
  in->get_stripes(stripes);
  if (!stripes.empty()) {
    dout(20) << " open stripes" << dendl;
    for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s)
      eval(*s);
    return; // wait for stripes to close/trim
  }
  if (dn->state_test(CDentry::STATE_PURGING)) {
    dout(20) << " already purging" << dendl;
    return; // already purging
  }
  if (in->state_test(CInode::STATE_NEEDSRECOVER) ||
      in->state_test(CInode::STATE_RECOVERING)) {
    dout(20) << " pending recovery" << dendl;
    return; // don't mess with file size probing
  }
  if (in->get_num_ref() > (int)in->is_dirty()) {
    dout(20) << " too many inode refs" << dendl;
    return;
  }
  if (dn->get_num_ref() > (int)dn->is_dirty() + !!in->get_num_ref()) {
    dout(20) << " too many dn refs" << dendl;
    return;
  }

  purge(in);
}

void Stray::eval(CStripe *stripe)
{
  dout(10) << "eval " << *stripe << dendl;

  if (!stripe->state_test(CStripe::STATE_UNLINKED)) { // rmdir rollback
    dout(20) << " linked" << dendl;
    return;
  }
  if (stripe->state_test(CStripe::STATE_PURGING)) {
    dout(20) << " already purging" << dendl;
    return;
  }
  list<CDir*> dirs;
  stripe->get_dirfrags(dirs);
  for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d) {
    CDir *dir = *d;
    // allow pins that are dropped by CStripe::close_dirfrag()
    int allowed = dir->is_dirty() + dir->state_test(CDir::STATE_STICKY);
    if (dir->get_num_ref() > allowed) {
      dout(20) << " open dirfrag " << *dir << dendl;
      return;
    }
  }
  // allow pins that are dropped by CInode::close_stripe()
  int allowed = stripe->is_dirty() + dirs.size() +
      !!stripe->state_test(CStripe::STATE_DIRTYFRAGSTAT) +
      !!stripe->state_test(CStripe::STATE_DIRTYRSTAT);
  if (stripe->get_num_ref() > allowed) {
    dout(20) << " too many refs: " << stripe->get_num_ref()
       << " > " << allowed << dendl;
    return;
  }

  purge(stripe);
}


class C_StrayPurged : public Context {
 private:
  Stray *stray;
  CInode *in;
  CStripe *stripe;
 public:
  C_StrayPurged(Stray *stray, CInode *in)
      : stray(stray), in(in), stripe(NULL) {}
  C_StrayPurged(Stray *stray, CStripe *stripe)
      : stray(stray), in(NULL), stripe(stripe) {}

  void finish(int r) {
    if (in)
      stray->purged(in);
    else
      stray->purged(stripe);
  }
};

void Stray::purge(CInode *in)
{
  CDentry *dn = in->get_projected_parent_dn();
  dn->get(CDentry::PIN_PURGING);
  dn->state_set(CDentry::STATE_PURGING);
  in->state_set(CInode::STATE_PURGING);

  dout(10) << "purge " << *dn << " " << *in << dendl;

  if (!in->is_file()) {
    // no file data to remove
    purged(in);
    return;
  }

  ceph_file_layout &layout = in->inode.layout;
  uint64_t period = (uint64_t)layout.fl_object_size *
      (uint64_t)layout.fl_stripe_count;
  uint64_t cur_max_size = in->inode.get_max_size();
  uint64_t to = MAX(in->inode.size, cur_max_size);
  if (!period || !to) {
    purged(in);
    return;
  }

  SnapRealm *realm = mds->mdcache->get_snaprealm();
  SnapContext nullsnap;
  const SnapContext *snapc;
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnap;
    assert(in->last == CEPH_NOSNAP);
  }

  uint64_t num = (to + period - 1) / period;
  dout(10) << "purge_range 0~" << to << " objects 0~" << num
      << " snapc " << snapc << " on " << *in << dendl;
  mds->filer->purge_range(in->inode.ino, &layout, *snapc, 0, num,
                          ceph_clock_now(g_ceph_context), 0,
                          new C_StrayPurged(this, in));
}

class C_AssertRemoved : public Context {
 private:
  MDS *mds;
  dirstripe_t ds;
  frag_t fg;
 public:
  C_AssertRemoved(MDS *mds, dirstripe_t ds)
      : mds(mds), ds(ds), fg(-1u) {}
  C_AssertRemoved(MDS *mds, dirstripe_t ds, frag_t fg)
      : mds(mds), ds(ds), fg(fg) {}
  void finish(int r) {
    if (fg == -1u)
      dout(0) << "stripe " << ds << " removal got " << r << dendl;
    else
      dout(0) << "dir " << ds << ":" << fg << " removal got " << r << dendl;
    assert(r == 0);
  }
};

void Stray::purge(CStripe *stripe)
{
  dout(10) << "purge " << *stripe << dendl;

  stripe->state_set(CStripe::STATE_PURGING);

  C_GatherBuilder gather(g_ceph_context, new C_StrayPurged(this, stripe));

  const object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  const SnapContext nullsnap;
  utime_t now = ceph_clock_now(g_ceph_context);
  dirstripe_t ds = stripe->dirstripe();

  // delete the stripe object
  if (stripe->item_new.empty()) {
    dout(10) << "removing stripe " << ds << dendl;
    mds->objecter->remove(file_object_t(ds.ino, (uint64_t)ds.stripeid << 32),
                          oloc, nullsnap, now, 0, gather.new_sub(),
                          new C_AssertRemoved(mds, ds));
  }

  // delete each dirfrag object
  list<frag_t> frags;
  stripe->get_fragtree().get_leaves(frags);
  for (list<frag_t>::iterator fg = frags.begin(); fg != frags.end(); ++fg) {
    CDir *dir = stripe->get_dirfrag(*fg);
    if (dir && !dir->item_new.empty())
      continue;
    dout(10) << "removing dir " << ds << "." << *fg << dendl;
    mds->objecter->remove(file_object_t(ds.ino, *fg), oloc,
                          nullsnap, now, 0, gather.new_sub(),
                          new C_AssertRemoved(mds, ds, *fg));
  }

  if (!gather.has_subs()) {
    purged(stripe);
    return;
  }

  gather.set_finisher(new C_StrayPurged(this, stripe));
  gather.activate();
}


class C_StrayLogged : public Context {
 private:
  Stray *stray;
  CDentry *dn;
  CInode *in;
  CStripe *stripe;
 public:
  C_StrayLogged(Stray *stray, CDentry *dn, CInode *in)
      : stray(stray), dn(dn), in(in), stripe(NULL) {}
  C_StrayLogged(Stray *stray, CStripe *stripe)
      : stray(stray), dn(NULL), in(NULL), stripe(stripe) {}
  void finish(int r) {
    if (stripe)
      stray->logged(stripe);
    else
      stray->logged(dn, in);
  }
};

void Stray::purged(CInode *in)
{
  CDentry *dn = in->get_projected_parent_dn();
  dout(10) << "purged " << *dn << " " << *in << dendl;

  EUpdate *le = new EUpdate(mds->mdlog, "purge inode");

  dn->push_projected_linkage();

  le->metablob.add_dentry(dn, true);
  le->metablob.add_destroyed_inode(in->ino());

  mds->mdlog->start_submit_entry(le, new C_StrayLogged(this, dn, in));
}

void Stray::purged(CStripe *stripe)
{
  dout(10) << "purged " << *stripe << dendl;

  EUpdate *le = new EUpdate(mds->mdlog, "purge stripe");

  le->metablob.add_destroyed_stripe(stripe->dirstripe());

  mds->mdlog->start_submit_entry(le, new C_StrayLogged(this, stripe));
}


void Stray::logged(CDentry *dn, CInode *in)
{
  dout(10) << "logged " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  assert(!in->state_test(CInode::STATE_RECOVERING));

  // unlink
  CDir *dir = dn->get_dir();
  assert(dn->get_projected_linkage()->is_null());
  dir->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(mds->mdlog->get_current_segment());

  // drop inode
  if (in->is_dirty())
    in->mark_clean();
  mds->mdcache->remove_inode(in);

  // drop dentry?
  if (dn->is_new()) {
    dout(20) << " dn is new, removing" << dendl;
    dn->mark_clean();
    dir->remove_dentry(dn);
  } else
    mds->mdcache->touch_dentry_bottom(dn); // drop dn as quickly as possible.
}

void Stray::logged(CStripe *stripe)
{
  dout(10) << "logged " << *stripe << dendl;

  stripe->clear_dirty_parent_stats();

  CInode *in = stripe->get_inode();
  in->close_stripe(stripe);

  eval(in);
}

