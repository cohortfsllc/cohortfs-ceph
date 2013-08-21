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


#include "include/types.h"

#include "CDirFrag.h"
#include "CDentry.h"
#include "CInode.h"
#include "Mutation.h"

#include "MDSMap.h"
#include "MDS.h"
#include "MDCache.h"
#include "Locker.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "include/bloom_filter.hpp"
#include "include/Context.h"
#include "common/Clock.h"

#include "osdc/Objecter.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << cache->mds->get_nodeid() << ".cache.frag(" << this->dirfrag() << ") "



// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

boost::pool<> CDirFrag::pool(sizeof(CDirFrag));


ostream& operator<<(ostream& out, CDirFrag& dir)
{
  string path;
  dir.get_inode()->make_path_string_projected(path);
  out << "[dir " << dir.dirfrag() << " " << path << "/"
      << " [" << dir.first << ",head]";
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    out << " v=" << dir.get_version();
    out << " cv=" << dir.get_committing_version();
    out << "/" << dir.get_committed_version();
  } else {
    pair<int,int> a = dir.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << dir.get_replica_nonce();
  }

  if (dir.get_auth_pins())
    out << " ap=" << dir.get_auth_pins();

  out << " state=" << dir.get_state();
  if (dir.state_test(CDirFrag::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDirFrag::STATE_FROZEN)) out << "|frozen";
  if (dir.state_test(CDirFrag::STATE_FREEZING)) out << "|freezing";

  out << " hs=" << dir.get_num_head_items() << "+" << dir.get_num_head_null();
  out << ",ss=" << dir.get_num_snap_items() << "+" << dir.get_num_snap_null();
  if (dir.get_num_dirty())
    out << " dirty=" << dir.get_num_dirty();
  
  if (dir.get_num_ref()) {
    out << " |";
    dir.print_pin_set(out);
  }

  out << " " << &dir;
  return out << "]";
}


void CDirFrag::print(ostream& out) 
{
  out << *this;
}




ostream& CDirFrag::print_db_line_prefix(ostream& out) 
{
  return out << ceph_clock_now(g_ceph_context) << " mds." << cache->mds->get_nodeid() << ".cache.frag(" << this->dirfrag() << ") ";
}



// -------------------------------------------------------------------
// CDirFrag

CDirFrag::CDirFrag(CDirStripe *stripe, frag_t frag, MDCache *mdcache, bool auth)
  : cache(mdcache),
    stripe(stripe),
    frag(frag),
    first(2),
    item_dirty(this),
    item_new(this),
    version(0),
    num_head_items(0),
    num_head_null(0),
    num_snap_items(0),
    num_snap_null(0),
    num_dirty(0),
    committing_version(0),
    committed_version(0),
    auth_pins(0),
    request_pins(0),
    bloom(NULL)
{
  g_num_dir++;
  g_num_dira++;

  if (auth) 
    state_set(STATE_AUTH);
}

CDentry* CDirFrag::lookup(const string &name, snapid_t snap)
{ 
  dout(20) << "lookup (" << snap << ", '" << name << "')" << dendl;
  map_t::iterator iter = items.lower_bound(dentry_key_t(snap, &name));
  if (iter == items.end())
    return 0;
  if (iter->second->name == name &&
      iter->second->first <= snap) {
    dout(20) << "  hit -> " << iter->first << dendl;
    return iter->second;
  }
  dout(20) << "  miss -> " << iter->first << dendl;
  return 0;
}





/***
 * linking fun
 */

CDentry* CDirFrag::add_null_dentry(const string& dname,
			       snapid_t first, snapid_t last)
{
  // foreign
  assert(lookup_exact_snap(dname, last) == 0);
   
  // create dentry
  CDentry* dn = new CDentry(dname, get_inode()->hash_dentry_name(dname), first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = version;
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;
  if (last == CEPH_NOSNAP)
    num_head_null++;
  else
    num_snap_null++;

  if (state_test(CDirFrag::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << "add_null_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  assert(get_num_any() == items.size());
  return dn;
}


CDentry* CDirFrag::add_primary_dentry(const string& dname, CInode *in,
				  snapid_t first, snapid_t last) 
{
  // primary
  assert(lookup_exact_snap(dname, last) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, get_inode()->hash_dentry_name(dname), first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = version;
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;

  dn->get_linkage()->inode = in;
  in->set_primary_parent(dn);

  link_inode_work(dn, in);

  if (dn->last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;
  
  if (state_test(CDirFrag::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << "add_primary_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  assert(get_num_any() == items.size());
  return dn;
}

CDentry* CDirFrag::add_remote_dentry(const string& dname, inodeno_t ino, unsigned char d_type,
				 snapid_t first, snapid_t last) 
{
  // foreign
  assert(lookup_exact_snap(dname, last) == 0);

  // create dentry
  CDentry* dn = new CDentry(dname, get_inode()->hash_dentry_name(dname), ino, d_type, first, last);
  if (is_auth()) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = version;
  
  // add to dir
  assert(items.count(dn->key()) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->key()] = dn;
  if (last == CEPH_NOSNAP)
    num_head_items++;
  else
    num_snap_items++;

  if (state_test(CDirFrag::STATE_DNPINNEDFRAG)) {
    dn->get(CDentry::PIN_FRAGMENTING);
    dn->state_set(CDentry::STATE_FRAGMENTING);
  }    

  dout(12) << "add_remote_dentry " << *dn << dendl;

  // pin?
  if (get_num_any() == 1)
    get(PIN_CHILD);
  
  assert(get_num_any() == items.size());
  return dn;
}



void CDirFrag::remove_dentry(CDentry *dn) 
{
  dout(12) << "remove_dentry " << *dn << dendl;

  // there should be no client leases at this point!
  assert(dn->client_lease_map.empty());

  if (state_test(CDirFrag::STATE_DNPINNEDFRAG)) {
    dn->put(CDentry::PIN_FRAGMENTING);
    dn->state_clear(CDentry::STATE_FRAGMENTING);
  }    

  if (dn->get_linkage()->is_null()) {
    if (dn->last == CEPH_NOSNAP)
      num_head_null--;
    else
      num_snap_null--;
  } else {
    if (dn->last == CEPH_NOSNAP)
      num_head_items--;
    else
      num_snap_items--;
  }

  if (!dn->get_linkage()->is_null())
    // detach inode and dentry
    unlink_inode_work(dn);
  
  // remove from list
  assert(items.count(dn->key()) == 1);
  items.erase(dn->key());

  // clean?
  if (dn->is_dirty())
    dn->mark_clean();

  cache->lru.lru_remove(dn);
  delete dn;

  // unpin?
  if (get_num_any() == 0)
    put(PIN_CHILD);
  assert(get_num_any() == items.size());
}

void CDirFrag::link_remote_inode(CDentry *dn, CInode *in)
{
  link_remote_inode(dn, in->ino(), IFTODT(in->get_projected_inode()->mode));
}

void CDirFrag::link_remote_inode(CDentry *dn, inodeno_t ino, unsigned char d_type)
{
  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  assert(dn->get_linkage()->is_null());

  dn->get_linkage()->set_remote(ino, d_type);

  if (dn->last == CEPH_NOSNAP) {
    num_head_items++;
    num_head_null--;
  } else {
    num_snap_items++;
    num_snap_null--;
  }
  assert(get_num_any() == items.size());
}

void CDirFrag::link_primary_inode(CDentry *dn, CInode *in)
{
  dout(12) << "link_primary_inode " << *dn << " " << *in << dendl;
  assert(dn->get_linkage()->is_null());

  dn->get_linkage()->inode = in;
  in->set_primary_parent(dn);

  link_inode_work(dn, in);
  
  if (dn->last == CEPH_NOSNAP) {
    num_head_items++;
    num_head_null--;
  } else {
    num_snap_items++;
    num_snap_null--;
  }

  assert(get_num_any() == items.size());
}

void CDirFrag::link_inode_work(CDentry *dn, CInode *in)
{
  assert(dn->get_linkage()->get_inode() == in);
  assert(in->get_parent_dn() == dn);

  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);
}

void CDirFrag::unlink_inode(CDentry *dn)
{
  if (dn->get_linkage()->is_remote()) {
    dout(12) << "unlink_inode " << *dn << dendl;
  } else {
    dout(12) << "unlink_inode " << *dn << " " << *dn->get_linkage()->get_inode() << dendl;
  }

  unlink_inode_work(dn);

  if (dn->last == CEPH_NOSNAP) {
    num_head_items--;
    num_head_null++;
  } else {
    num_snap_items--;
    num_snap_null++;
  }
  assert(get_num_any() == items.size());
}


void CDirFrag::try_remove_unlinked_dn(CDentry *dn)
{
  assert(dn->dir == this);
  assert(dn->get_linkage()->is_null());
  
  // no pins (besides dirty)?
  if (dn->get_num_ref() != dn->is_dirty()) 
    return;

  // was the dn new?
  if (dn->is_new()) {
    dout(10) << "try_remove_unlinked_dn " << *dn << " in " << *this << dendl;
    if (dn->is_dirty())
      dn->mark_clean();
    remove_dentry(dn);

    // NOTE: we may not have any more dirty dentries, but the fnode
    // still changed, so the directory must remain dirty.
  }
}


void CDirFrag::unlink_inode_work(CDentry *dn)
{
  CInode *in = dn->get_linkage()->get_inode();

  if (dn->get_linkage()->is_remote()) {
    // remote
    if (in) 
      dn->unlink_remote(dn->get_linkage());

    dn->get_linkage()->set_remote(0, 0);
  } else {
    // primary
    assert(dn->get_linkage()->is_primary());
 
    // unpin dentry?
    if (in->get_num_ref())
      dn->put(CDentry::PIN_INODEPIN);

    // detach inode
    in->remove_primary_parent(dn);
    dn->get_linkage()->inode = 0;
  }
}

void CDirFrag::add_to_bloom(CDentry *dn)
{
  if (!bloom) {
    /* not create bloom filter for incomplete dir that was added by log replay */
    if (!is_complete())
      return;
    bloom = new bloom_filter(100, 0.05, 0);
  }
  /* This size and false positive probability is completely random.*/
  bloom->insert(dn->name.c_str(), dn->name.size());
}

bool CDirFrag::is_in_bloom(const string& name)
{
  if (!bloom)
    return false;
  return bloom->contains(name.c_str(), name.size());
}

void CDirFrag::remove_bloom()
{
  delete bloom;
  bloom = NULL;
}

void CDirFrag::remove_null_dentries() {
  dout(12) << "remove_null_dentries " << *this << dendl;

  CDirFrag::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    p++;
    if (dn->get_linkage()->is_null() && !dn->is_projected())
      remove_dentry(dn);
  }

  assert(num_snap_null == 0);
  assert(num_head_null == 0);
  assert(get_num_any() == items.size());
}


bool CDirFrag::try_trim_snap_dentry(CDentry *dn, const set<snapid_t>& snaps)
{
  assert(dn->last != CEPH_NOSNAP);
  set<snapid_t>::const_iterator p = snaps.lower_bound(dn->first);
  CDentry::linkage_t *dnl= dn->get_linkage();
  CInode *in = 0;
  if (dnl->is_primary())
    in = dnl->get_inode();
  if ((p == snaps.end() || *p > dn->last) &&
      (dn->get_num_ref() == dn->is_dirty()) &&
      (!in || in->get_num_ref() == in->is_dirty())) {
    dout(10) << " purging snapped " << *dn << dendl;
    if (in && in->is_dirty())
      in->mark_clean();
    remove_dentry(dn);
    if (in) {
      dout(10) << " purging snapped " << *in << dendl;
      cache->remove_inode(in);
    }
    return true;
  }
  return false;
}


void CDirFrag::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << "purge_stale_snap_data " << snaps << dendl;

  CDirFrag::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    p++;

    if (dn->last == CEPH_NOSNAP)
      continue;

    try_trim_snap_dentry(dn, snaps);
  }
}


/**
 * steal_dentry -- semi-violently move a dentry from one CDirFrag to another
 * (*) violently, in that nitems, most pins, etc. are not correctly maintained 
 * on the old CDirFrag corpse; must call finish_old_fragment() when finished.
 */
void CDirFrag::steal_dentry(CDentry *dn)
{
  dout(15) << "steal_dentry " << *dn << dendl;

  items[dn->key()] = dn;

  dn->dir->items.erase(dn->key());
  if (dn->dir->items.empty())
    dn->dir->put(PIN_CHILD);

  if (get_num_any() == 0)
    get(PIN_CHILD);
  if (dn->get_linkage()->is_null()) {
    if (dn->last == CEPH_NOSNAP)
      num_head_null++;
    else
      num_snap_null++;
  } else {
    if (dn->last == CEPH_NOSNAP)
      num_head_items++;
    else
      num_snap_items++;

    // no change to stripe rstats
    assert(dn->dir->get_stripe() == get_stripe());
  }

  if (dn->is_dirty())
    num_dirty++;

  dn->dir = this;
}

void CDirFrag::prepare_old_fragment(bool replay)
{
  // auth_pin old fragment for duration so that any auth_pinning
  // during the dentry migration doesn't trigger side effects
  if (!replay && is_auth())
    auth_pin(this);
}

void CDirFrag::prepare_new_fragment(bool replay)
{
  if (!replay && is_auth())
    _freeze_dir();
}

void CDirFrag::finish_old_fragment(list<Context*>& waiters, bool replay)
{
  // take waiters _before_ unfreeze...
  if (!replay) {
    take_waiting(WAIT_ANY_MASK, waiters);
    if (is_auth()) {
      auth_unpin(this);  // pinned in prepare_old_fragment
      assert(is_frozen_dir());
      unfreeze_dir();
    }
  }

  assert(auth_pins == 0);

  num_head_items = num_head_null = 0;
  num_snap_items = num_snap_null = 0;

  // this mirrors init_fragment_pins()
  if (is_auth()) 
    clear_replica_map();
  if (is_dirty())
    mark_clean();

  if (auth_pins > 0)
    put(PIN_AUTHPIN);

  assert(get_num_ref() == (state_test(STATE_STICKY) ? 1:0));
}

void CDirFrag::init_fragment_pins()
{
  if (!replica_map.empty())
    get(PIN_REPLICATED);
  if (state_test(STATE_DIRTY))
    get(PIN_DIRTY);
}

void CDirFrag::split(int bits, list<CDirFrag*>& subs, list<Context*>& waiters, bool replay)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_sp);

  assert(replay || is_complete() || !is_auth());

  list<frag_t> frags;
  frag.split(bits, frags);

  vector<CDirFrag*> subfrags(1 << bits);
  
  prepare_old_fragment(replay);

  // create subfrag dirs
  int n = 0;
  for (list<frag_t>::iterator p = frags.begin(); p != frags.end(); ++p) {
    CDirFrag *f = new CDirFrag(stripe, *p, cache, is_auth());
    f->state_set(state & MASK_STATE_FRAGMENT_KEPT);
    f->replica_map = replica_map;
    f->init_fragment_pins();
    f->set_version(get_version());

    dout(10) << " subfrag " << *p << " " << *f << dendl;
    subfrags[n++] = f;
    subs.push_back(f);
    stripe->add_dirfrag(f);

    f->prepare_new_fragment(replay);
  }
  
  // repartition dentries
  while (!items.empty()) {
    CDirFrag::map_t::iterator p = items.begin();
    
    CDentry *dn = p->second;
    frag_t subfrag = stripe->pick_dirfrag(dn->name);
    int n = (subfrag.value() & (subfrag.mask() ^ frag.mask())) >> subfrag.mask_shift();
    dout(15) << " subfrag " << subfrag << " n=" << n << " for " << p->first << dendl;
    CDirFrag *f = subfrags[n];
    f->steal_dentry(dn);
  }

  finish_old_fragment(waiters, replay);
}

void CDirFrag::merge(list<CDirFrag*>& subs, list<Context*>& waiters, bool replay)
{
  dout(10) << "merge " << subs << dendl;

  prepare_new_fragment(replay);

  for (list<CDirFrag*>::iterator p = subs.begin(); p != subs.end(); p++) {
    CDirFrag *dir = *p;
    dout(10) << " subfrag " << dir->get_frag() << " " << *dir << dendl;
    assert(!dir->is_auth() || dir->is_complete() || replay);
    
    dir->prepare_old_fragment(replay);

    // steal dentries
    while (!dir->items.empty()) 
      steal_dentry(dir->items.begin()->second);
    
    // merge replica map
    for (map<int,int>::iterator p = dir->replica_map.begin();
	 p != dir->replica_map.end();
	 ++p) {
      int cur = replica_map[p->first];
      if (p->second > cur)
	replica_map[p->first] = p->second;
    }

    // merge version
    if (dir->get_version() > get_version())
      set_version(dir->get_version());

    // merge state
    state_set(dir->get_state() & MASK_STATE_FRAGMENT_KEPT);

    dir->finish_old_fragment(waiters, replay);
    stripe->close_dirfrag(dir->get_frag());
  }

  init_fragment_pins();
}





/****************************************
 * WAITING
 */

void CDirFrag::add_dentry_waiter(const string& dname, snapid_t snapid, Context *c) 
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[string_snap_t(dname, snapid)].push_back(c);
  dout(10) << "add_dentry_waiter dentry " << dname
	   << " snap " << snapid
	   << " " << c << " on " << *this << dendl;
}

void CDirFrag::take_dentry_waiting(const string& dname, snapid_t first, snapid_t last,
			       list<Context*>& ls)
{
  if (waiting_on_dentry.empty())
    return;
  
  string_snap_t lb(dname, first);
  string_snap_t ub(dname, last);
  map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.lower_bound(lb);
  while (p != waiting_on_dentry.end() &&
	 !(ub < p->first)) {
    dout(10) << "take_dentry_waiting dentry " << dname
	     << " [" << first << "," << last << "] found waiter on snap "
	     << p->first.snapid
	     << " on " << *this << dendl;
    ls.splice(ls.end(), p->second);
    waiting_on_dentry.erase(p++);
  }

  if (waiting_on_dentry.empty())
    put(PIN_DNWAITER);
}

void CDirFrag::add_ino_waiter(inodeno_t ino, Context *c) 
{
  if (waiting_on_ino.empty())
    get(PIN_INOWAITER);
  waiting_on_ino[ino].push_back(c);
  dout(10) << "add_ino_waiter ino " << ino << " " << c << " on " << *this << dendl;
}

void CDirFrag::take_ino_waiting(inodeno_t ino, list<Context*>& ls)
{
  if (waiting_on_ino.empty()) return;
  if (waiting_on_ino.count(ino) == 0) return;
  dout(10) << "take_ino_waiting ino " << ino
	   << " x " << waiting_on_ino[ino].size() 
	   << " on " << *this << dendl;
  ls.splice(ls.end(), waiting_on_ino[ino]);
  waiting_on_ino.erase(ino);
  if (waiting_on_ino.empty())
    put(PIN_INOWAITER);
}

void CDirFrag::take_sub_waiting(list<Context*>& ls)
{
  dout(10) << "take_sub_waiting" << dendl;
  if (!waiting_on_dentry.empty()) {
    for (map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.begin(); 
	 p != waiting_on_dentry.end();
	 ++p) 
      ls.splice(ls.end(), p->second);
    waiting_on_dentry.clear();
    put(PIN_DNWAITER);
  }
  for (map<inodeno_t, list<Context*> >::iterator p = waiting_on_ino.begin(); 
       p != waiting_on_ino.end();
       ++p) 
    ls.splice(ls.end(), p->second);
  waiting_on_ino.clear();
}



void CDirFrag::add_waiter(uint64_t tag, Context *c) 
{
  // hierarchical?

  // at free root?
  if (tag & WAIT_ATFREEZEROOT) {
    if (!is_freezing_dir() && !is_frozen_dir()) {
      // try parent
      dout(10) << "add_waiter " << std::hex << tag << std::dec << " " << c << " should be ATFREEZEROOT, " << *this << " is not root, trying parent" << dendl;
      stripe->add_waiter(tag, c);
      return;
    }
  }
  
  // at subtree root?
  if (tag & WAIT_ATSUBTREEROOT) {
    dout(10) << "add_waiter " << std::hex << tag << std::dec << " " << c << " should be ATSUBTREEROOT, " << *this << " is not root, trying parent" << dendl;
    stripe->add_waiter(tag, c);
    return;
  }

  MDSCacheObject::add_waiter(tag, c);
}



/* NOTE: this checks dentry waiters too */
void CDirFrag::take_waiting(uint64_t mask, list<Context*>& ls)
{
  if ((mask & WAIT_DENTRY) && waiting_on_dentry.size()) {
    // take all dentry waiters
    while (!waiting_on_dentry.empty()) {
      map<string_snap_t, list<Context*> >::iterator p = waiting_on_dentry.begin(); 
      dout(10) << "take_waiting dentry " << p->first.name
	       << " snap " << p->first.snapid << " on " << *this << dendl;
      ls.splice(ls.end(), p->second);
      waiting_on_dentry.erase(p);
    }
    put(PIN_DNWAITER);
  }
  
  // waiting
  MDSCacheObject::take_waiting(mask, ls);
}


void CDirFrag::finish_waiting(uint64_t mask, int result) 
{
  dout(11) << "finish_waiting mask " << hex << mask << dec << " result " << result << " on " << *this << dendl;

  list<Context*> finished;
  take_waiting(mask, finished);
  if (result < 0)
    finish_contexts(g_ceph_context, finished, result);
  else
    cache->mds->queue_waiters(finished);
}



// dirty/clean

void CDirFrag::mark_dirty(LogSegment *ls)
{
  version++;
  dout(10) << "mark_dirty v" << version << dendl;
  _mark_dirty(ls);
}

void CDirFrag::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    dout(10) << "mark_dirty (was clean) " << *this << " version " << get_version() << dendl;
    _set_dirty_flag();
    assert(ls);
  } else {
    dout(10) << "mark_dirty (already dirty) " << *this << " version " << get_version() << dendl;
  }
  if (ls) {
    ls->dirty_dirfrags.push_back(&item_dirty);

    // if i've never committed, i need to be before _any_ mention of me is trimmed from the journal.
    if (committed_version == 0 && !item_new.is_on_list())
      ls->new_dirfrags.push_back(&item_new);
  }
}

void CDirFrag::mark_new(LogSegment *ls)
{
  ls->new_dirfrags.push_back(&item_new);
}

void CDirFrag::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << get_version() << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    item_dirty.remove_myself();
    item_new.remove_myself();
  }
}


struct C_Dir_Dirty : public Context {
  CDirFrag *dir;
  LogSegment *ls;
  C_Dir_Dirty(CDirFrag *d, LogSegment *l) : dir(d), ls(l) {}
  void finish(int r) {
    dir->mark_dirty(ls);
  }
};

void CDirFrag::log_mark_dirty()
{
  MDLog *mdlog = cache->mds->mdlog;
  mdlog->flush();
  mdlog->wait_for_safe(new C_Dir_Dirty(this, mdlog->get_current_segment()));
}

void CDirFrag::mark_complete() {
  state_set(STATE_COMPLETE);
  remove_bloom();
}

void CDirFrag::first_get()
{
  stripe->get(CDirStripe::PIN_DIRFRAG);
}

void CDirFrag::last_put()
{
  stripe->put(CDirStripe::PIN_DIRFRAG);
}



/******************************************************************************
 * FETCH and COMMIT
 */

// -----------------------
// FETCH

class C_Dir_Fetch : public Context {
 protected:
  CDirFrag *dir;
  string want_dn;
 public:
  bufferlist bl;

  C_Dir_Fetch(CDirFrag *d, const string& w) : dir(d), want_dn(w) { }
  void finish(int result) {
    dir->_fetched(bl, want_dn);
  }
};

void CDirFrag::fetch(Context *c, bool ignore_authpinnability)
{
  string want;
  return fetch(c, want, ignore_authpinnability);
}

void CDirFrag::fetch(Context *c, const string& want_dn, bool ignore_authpinnability)
{
  dout(10) << "fetch on " << *this << dendl;
  
  assert(is_auth());
  assert(!is_complete());

  if (!can_auth_pin() && !ignore_authpinnability) {
    if (c) {
      dout(7) << "fetch waiting for authpinnable" << dendl;
      add_waiter(WAIT_UNFREEZE, c);
    } else
      dout(7) << "fetch not authpinnable and no context" << dendl;
    return;
  }

  if (c) add_waiter(WAIT_COMPLETE, c);
  
  // already fetching?
  if (state_test(CDirFrag::STATE_FETCHING)) {
    dout(7) << "already fetching; waiting" << dendl;
    return;
  }

  auth_pin(this);
  state_set(CDirFrag::STATE_FETCHING);

  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_f);

  // start by reading the first hunk of it
  C_Dir_Fetch *fin = new C_Dir_Fetch(this, want_dn);
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());
  ObjectOperation rd;
  rd.tmap_get(&fin->bl, NULL);
  cache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, NULL, 0, fin);
}

void CDirFrag::_fetched(bufferlist &bl, const string& want_dn)
{
  LogClient &clog = cache->mds->clog;
  dout(10) << "_fetched " << bl.length() 
	   << " bytes for " << *this
	   << " want_dn=" << want_dn
	   << dendl;
  
  assert(is_auth());
  assert(!is_frozen());

  // empty?!?
  if (bl.length() == 0) {
    dout(0) << "_fetched missing object for " << *this << dendl;
    clog.error() << "dir " << ino() << "." << dirfrag()
	  << " object missing on disk; some files may be lost\n";

    log_mark_dirty();

    // mark complete, !fetching
    state_set(STATE_COMPLETE);
    state_clear(STATE_FETCHING);
    auth_unpin(this);
    
    // kick waiters
    finish_waiting(WAIT_COMPLETE, 0);
    return;
  }

  // decode trivialmap.
  int len = bl.length();
  bufferlist::iterator p = bl.begin();
  
  bufferlist header;
  ::decode(header, p);
  bufferlist::iterator hp = header.begin();
  version_t got_version;
  ::decode(got_version, hp);
  ::decode(snap_purged_thru, hp);

  __u32 n;
  ::decode(n, p);

  dout(10) << "_fetched version " << got_version
	   << ", " << len << " bytes, " << n << " keys"
	   << dendl;
  

  // take the loaded fnode?
  // only if we are a fresh CDirFrag* with no prior state.
  if (get_version() == 0) {
    assert(!state_test(STATE_COMMITTING));
    committing_version = committed_version = version = got_version;

    if (state_test(STATE_REJOINUNDEF)) {
      assert(cache->mds->is_rejoin());
      state_clear(STATE_REJOINUNDEF);
      cache->opened_undef_dirfrag(this);
    }
  }

  // purge stale snaps?
  const set<snapid_t> *snaps = 0;
  SnapRealm *realm = cache->get_snaprealm();
  if (snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
    snap_purged_thru = realm->get_last_destroyed();
  }
  bool purged_any = false;


  //int num_new_inodes_loaded = 0;
  loff_t baseoff = p.get_off();
  for (unsigned i=0; i<n; i++) {
    loff_t dn_offset = p.get_off() - baseoff;

    // dname
    string dname;
    snapid_t first, last;
    dentry_key_t::decode_helper(p, dname, last);
    
    bufferlist dndata;
    ::decode(dndata, p);
    bufferlist::iterator q = dndata.begin();
    ::decode(first, q);

    // marker
    char type;
    ::decode(type, q);

    dout(24) << "_fetched pos " << dn_offset << " marker '" << type << "' dname '" << dname
	     << " [" << first << "," << last << "]"
	     << dendl;

    bool stale = false;
    if (snaps && last != CEPH_NOSNAP) {
      set<snapid_t>::const_iterator p = snaps->lower_bound(first);
      if (p == snaps->end() || *p > last) {
	dout(10) << " skipping stale dentry on [" << first << "," << last << "]" << dendl;
	stale = true;
	purged_any = true;
      }
    }
    
    /*
     * look for existing dentry for _last_ snap, because unlink +
     * create may leave a "hole" (epochs during which the dentry
     * doesn't exist) but for which no explicit negative dentry is in
     * the cache.
     */
    CDentry *dn = 0;
    if (!stale)
      dn = lookup(dname, last);

    if (type == 'L') {
      // hard link
      inodeno_t ino;
      unsigned char d_type;
      ::decode(ino, q);
      ::decode(d_type, q);

      if (stale)
	continue;

      if (dn) {
        if (dn->get_linkage()->get_inode() == 0) {
          dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
        } else {
          dout(12) << "_fetched  had dentry " << *dn << dendl;
        }
      } else {
	// (remote) link
	dn = add_remote_dentry(dname, ino, d_type, first, last);
	
	// link to inode?
	CInode *in = cache->get_inode(ino);   // we may or may not have it.
	if (in) {
	  dn->link_remote(dn->get_linkage(), in);
	  dout(12) << "_fetched  got remote link " << ino << " which we have " << *in << dendl;
	} else {
	  dout(12) << "_fetched  got remote link " << ino << " (dont' have it)" << dendl;
	}
      }
    } 
    else if (type == 'I') {
      // inode
      
      // parse out inode
      inode_t inode;
      string symlink;
      vector<int> stripe_auth;
      map<string, bufferptr> xattrs;
      map<snapid_t,old_inode_t> old_inodes;
      ::decode(inode, q);
      if (inode.is_symlink())
        ::decode(symlink, q);
      if (inode.is_dir())
        ::decode(stripe_auth, q);
      ::decode(xattrs, q);
      ::decode(old_inodes, q);
      
      if (stale)
	continue;

      if (dn) {
        if (dn->get_linkage()->get_inode() == 0) {
          dout(12) << "_fetched  had NEG dentry " << *dn << dendl;
        } else {
          dout(12) << "_fetched  had dentry " << *dn << dendl;
        }
      } else {
	// add inode
	CInode *in = cache->get_inode(inode.ino, last);
	if (in) {
	  dout(0) << "_fetched  badness: got (but i already had) " << *in
		  << " mode " << in->inode.mode
		  << " mtime " << in->inode.mtime << dendl;
	  string dirpath, inopath;
	  get_inode()->make_path_string(dirpath);
	  in->make_path_string(inopath);
	  clog.error() << "loaded dup inode " << inode.ino
	    << " [" << first << "," << last << "] v" << inode.version
	    << " at " << dirpath << "/" << dname
	    << ", but inode " << in->vino() << " v" << in->inode.version
	    << " already exists at " << inopath << "\n";
	  continue;
	} else {
	  // inode
	  in = new CInode(cache, cache->mds->get_nodeid(), first, last);
	  in->inode = inode;
	  
	  // symlink?
	  if (in->is_symlink()) 
	    in->symlink = symlink;
          if (in->is_dir())
            in->set_stripe_auth(stripe_auth);
	  
	  in->xattrs.swap(xattrs);
	  in->old_inodes.swap(old_inodes);
	  if (snaps)
	    in->purge_stale_snap_data(*snaps);

	  // add 
	  cache->add_inode( in );
	
	  // link
	  dn = add_primary_dentry(dname, in, first, last);
	  dout(12) << "_fetched  got " << *dn << " " << *in << dendl;

	  //in->hack_accessed = false;
	  //in->hack_load_stamp = ceph_clock_now(g_ceph_context);
	  //num_new_inodes_loaded++;
	}
      }
    } else {
      dout(1) << "corrupt directory, i got tag char '" << type << "' val " << (int)(type)
	      << " at offset " << p.get_off() << dendl;
      assert(0);
    }
    
    if (dn && want_dn.length() && want_dn == dname) {
      dout(10) << " touching wanted dn " << *dn << dendl;
      cache->touch_dentry(dn);
    }

    /** clean underwater item?
     * Underwater item is something that is dirty in our cache from
     * journal replay, but was previously flushed to disk before the
     * mds failed.
     *
     * We only do this is committed_version == 0. that implies either
     * - this is a fetch after from a clean/empty CDirFrag is created
     *   (and has no effect, since the dn won't exist); or
     * - this is a fetch after _recovery_, which is what we're worried 
     *   about.  Items that are marked dirty from the journal should be
     *   marked clean if they appear on disk.
     */
    if (committed_version == 0 &&     
	dn &&
	dn->get_version() <= got_version &&
	dn->is_dirty()) {
      dout(10) << "_fetched  had underwater dentry " << *dn << ", marking clean" << dendl;
      dn->mark_clean();

      if (dn->get_linkage()->get_inode()) {
	assert(dn->get_linkage()->get_inode()->get_version() <= got_version);
	dout(10) << "_fetched  had underwater inode " << *dn->get_linkage()->get_inode() << ", marking clean" << dendl;
	dn->get_linkage()->get_inode()->mark_clean();
      }
    }
  }
  if (!p.end()) {
    clog.warn() << "dir " << dirfrag() << " has "
	<< bl.length() - p.get_off() << " extra bytes\n";
  }

  //cache->mds->logger->inc("newin", num_new_inodes_loaded);
  //hack_num_accessed = 0;

  if (purged_any)
    log_mark_dirty();

  // mark complete, !fetching
  state_set(STATE_COMPLETE);
  state_clear(STATE_FETCHING);
  auth_unpin(this);

  // kick waiters
  finish_waiting(WAIT_COMPLETE, 0);
}



// -----------------------
// COMMIT

/**
 * commit
 *
 * @param want - min version i want committed
 * @param c - callback for completion
 */
void CDirFrag::commit(version_t want, Context *c, bool ignore_authpinnability)
{
  dout(10) << "commit want " << want << " on " << *this << dendl;
  if (want == 0) want = get_version();

  // preconditions
  assert(want <= get_version() || get_version() == 0);    // can't commit the future
  assert(want > committed_version); // the caller is stupid
  assert(is_auth());
  assert(ignore_authpinnability || can_auth_pin());

  // note: queue up a noop if necessary, so that we always
  // get an auth_pin.
  if (!c)
    c = new C_NoopContext;

  // auth_pin on first waiter
  if (waiting_for_commit.empty())
    auth_pin(this);
  waiting_for_commit[want].push_back(c);
  
  // ok.
  _commit(want);
}


class C_Dir_RetryCommit : public Context {
  CDirFrag *dir;
  version_t want;
public:
  C_Dir_RetryCommit(CDirFrag *d, version_t v) : 
    dir(d), want(v) { }
  void finish(int r) {
    dir->_commit(want);
  }
};

class C_Dir_Committed : public Context {
  CDirFrag *dir;
  version_t version, last_renamed_version;
public:
  C_Dir_Committed(CDirFrag *d, version_t v, version_t lrv) : dir(d), version(v), last_renamed_version(lrv) { }
  void finish(int r) {
    dir->_committed(version, last_renamed_version);
  }
};

/**
 * Try and write out the full directory to disk.
 *
 * If the bufferlist we're using exceeds max_write_size, bail out
 * and switch to _commit_partial -- it can safely break itself into
 * multiple non-atomic writes.
 */
CDirFrag::map_t::iterator CDirFrag::_commit_full(ObjectOperation& m, const set<snapid_t> *snaps,
                               unsigned max_write_size)
{
  dout(10) << "_commit_full" << dendl;

  // encode
  bufferlist bl;
  __u32 n = 0;

  bufferlist header;
  ::encode(version, header);
  ::encode(snap_purged_thru, header);
  max_write_size -= header.length();

  map_t::iterator p = items.begin();
  while (p != items.end() && bl.length() < max_write_size) {
    CDentry *dn = p->second;
    p++;
    
    if (dn->linkage.is_null()) 
      continue;  // skip negative entries

    if (snaps && dn->last != CEPH_NOSNAP &&
	try_trim_snap_dentry(dn, *snaps))
      continue;
    
    n++;

    _encode_dentry(dn, bl, snaps);
  }

  if (p != items.end()) {
    assert(bl.length() > max_write_size);
    return _commit_partial(m, snaps, max_write_size);
  }

  // encode final trivialmap
  bufferlist finalbl;
  ::encode(header, finalbl);
  assert(num_head_items + num_head_null + num_snap_items + num_snap_null == items.size());
  assert(n == (num_head_items + num_snap_items));
  ::encode(n, finalbl);
  finalbl.claim_append(bl);

  // write out the full blob
  m.tmap_put(finalbl);
  return p;
}

/**
 * Flush out the modified dentries in this dir. Keep the bufferlist
 * below max_write_size; if we exceed that size then return the last
 * dentry that got committed into the bufferlist. (Note that the
 * bufferlist might be larger than requested by the size of that
 * last dentry as encoded.)
 *
 * If we're passed a last_committed_dn, skip to the next dentry after that.
 * Also, don't encode the header again -- we don't want to update it
 * on-disk until all the updates have made it through, so keep the header
 * in only the first changeset -- our caller is responsible for making sure
 * that changeset doesn't go through until after all the others do, if it's
 * necessary.
 */
CDirFrag::map_t::iterator CDirFrag::_commit_partial(ObjectOperation& m,
                                  const set<snapid_t> *snaps,
                                  unsigned max_write_size,
                                  map_t::iterator last_committed_dn)
{
  dout(10) << "_commit_partial" << dendl;
  bufferlist finalbl;

  // header
  if (last_committed_dn == map_t::iterator()) {
    bufferlist header;
    ::encode(version, header);
    ::encode(snap_purged_thru, header);
    finalbl.append(CEPH_OSD_TMAP_HDR);
    ::encode(header, finalbl);
  }

  // updated dentries
  map_t::iterator p = items.begin();
  if(last_committed_dn != map_t::iterator())
    p = last_committed_dn;

  while (p != items.end() && finalbl.length() < max_write_size) {
    CDentry *dn = p->second;
    ++p;
    
    if (snaps && dn->last != CEPH_NOSNAP &&
	try_trim_snap_dentry(dn, *snaps))
      continue;

    if (!dn->is_dirty())
      continue;  // skip clean dentries

    if (dn->get_linkage()->is_null()) {
      dout(10) << " rm " << dn->name << " " << *dn << dendl;
      finalbl.append(CEPH_OSD_TMAP_RMSLOPPY);
      dn->key().encode(finalbl);
    } else {
      dout(10) << " set " << dn->name << " " << *dn << dendl;
      finalbl.append(CEPH_OSD_TMAP_SET);
      _encode_dentry(dn, finalbl, snaps);
    }
  }

  // update the trivialmap at the osd
  m.tmap_update(finalbl);
  return p;
}

void CDirFrag::_encode_dentry(CDentry *dn, bufferlist& bl,
			  const set<snapid_t> *snaps)
{
  // clear dentry NEW flag, if any.  we can no longer silently drop it.
  dn->clear_new();

  dn->key().encode(bl);

  ceph_le32 plen = init_le32(0);
  unsigned plen_off = bl.length();
  ::encode(plen, bl);

  ::encode(dn->first, bl);

  // primary or remote?
  if (dn->linkage.is_remote()) {
    inodeno_t ino = dn->linkage.get_remote_ino();
    unsigned char d_type = dn->linkage.get_remote_d_type();
    dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' remote ino " << ino << dendl;
    
    // marker, name, ino
    bl.append('L');         // remote link
    ::encode(ino, bl);
    ::encode(d_type, bl);
  } else {
    // primary link
    CInode *in = dn->linkage.get_inode();
    assert(in);
    
    dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' inode " << *in << dendl;
    
    // marker, name, inode, [symlink string]
    bl.append('I');         // inode
    ::encode(in->inode, bl);
    
    if (in->is_symlink()) {
      // include symlink destination!
      dout(18) << "    including symlink ptr " << in->symlink << dendl;
      ::encode(in->symlink, bl);
    }
    if (in->is_dir())
      ::encode(in->get_stripe_auth(), bl);

    ::encode(in->xattrs, bl);

    if (in->is_multiversion() && snaps)
      in->purge_stale_snap_data(*snaps);
    ::encode(in->old_inodes, bl);
  }
  
  plen = bl.length() - plen_off - sizeof(__u32);

  ceph_le32 eplen;
  eplen = plen;
  bl.copy_in(plen_off, sizeof(eplen), (char*)&eplen);
}


void CDirFrag::_commit(version_t want)
{
  dout(10) << "_commit want " << want << " on " << *this << dendl;

  // we can't commit things in the future.
  // (even the projected future.)
  assert(want <= get_version() || get_version() == 0);

  // check pre+postconditions.
  assert(is_auth());

  // already committed?
  if (committed_version >= want) {
    dout(10) << "already committed " << committed_version << " >= " << want << dendl;
    return;
  }
  // already committing >= want?
  if (committing_version >= want) {
    dout(10) << "already committing " << committing_version << " >= " << want << dendl;
    assert(state_test(STATE_COMMITTING));
    return;
  }

  // alrady committed an older version?
  if (committing_version > committed_version) {
    dout(10) << "already committing older " << committing_version << ", waiting for that to finish" << dendl;
    return;
  }
  
  // complete first?  (only if we're not using TMAPUP osd op)
  if (!g_conf->mds_use_tmap && !is_complete()) {
    dout(7) << "commit not complete, fetching first" << dendl;
    if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_ffc);
    fetch(new C_Dir_RetryCommit(this, want));
    return;
  }
  
  // commit.
  committing_version = get_version();

  // mark committing (if not already)
  if (!state_test(STATE_COMMITTING)) {
    dout(10) << "marking committing" << dendl;
    state_set(STATE_COMMITTING);
  }
  
  if (cache->mds->logger) cache->mds->logger->inc(l_mds_dir_c);

  // snap purge?
  SnapRealm *realm = cache->get_snaprealm();
  const set<snapid_t> *snaps = 0;
  if (snap_purged_thru < realm->get_last_destroyed()) {
    snaps = &realm->get_snaps();
    dout(10) << " snap_purged_thru " << snap_purged_thru
	     << " < " << realm->get_last_destroyed()
	     << ", snap purge based on " << *snaps << dendl;
  }

  ObjectOperation m;
  map_t::iterator committed_dn;
  unsigned max_write_size = cache->max_dir_commit_size;

  // update parent pointer while we're here.
  //  NOTE: the pointer is ONLY required to be valid for the first frag.  we put the xattr
  //        on other frags too because it can't hurt, but it won't necessarily be up to date
  //        in that case!!
  max_write_size -= get_inode()->encode_parent_mutation(m);

  if (is_complete() &&
      (num_dirty > (num_head_items*g_conf->mds_dir_commit_ratio))) {
    snap_purged_thru = realm->get_last_destroyed();
    committed_dn = _commit_full(m, snaps, max_write_size);
  } else {
    committed_dn = _commit_partial(m, snaps, max_write_size);
  }

  SnapContext snapc;
  object_t oid = get_ondisk_object();
  object_locator_t oloc(cache->mds->mdsmap->get_metadata_pool());

  m.priority = CEPH_MSG_PRIO_LOW;  // set priority lower than journal!

  if (committed_dn == items.end())
    cache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0, NULL,
                                 new C_Dir_Committed(this, get_version(),
                                       get_inode()->inode.last_renamed_version));
  else { // send in a different Context
    C_GatherBuilder gather(g_ceph_context, 
	    new C_Dir_Committed(this, get_version(),
		      get_inode()->inode.last_renamed_version));
    while (committed_dn != items.end()) {
      ObjectOperation n = ObjectOperation();
      committed_dn = _commit_partial(n, snaps, max_write_size, committed_dn);
      cache->mds->objecter->mutate(oid, oloc, n, snapc, ceph_clock_now(g_ceph_context), 0, NULL,
                                  gather.new_sub());
    }
    /*
     * save the original object for last -- it contains the new header,
     * which will be committed on-disk. If we were to send it off before
     * the other commits, but die before sending them all, we'd think
     * that the on-disk state was fully committed even though it wasn't!
     * However, since the messages are strictly ordered between the MDS and
     * the OSD, and since messages to a given PG are strictly ordered, if
     * we simply send the message containing the header off last, we cannot
     * get our header into an incorrect state.
     */
    cache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0, NULL,
                                gather.new_sub());
    gather.activate();
  }
}


/**
 * _committed
 *
 * @param v version i just committed
 */
void CDirFrag::_committed(version_t v, version_t lrv)
{
  dout(10) << "_committed v " << v << " (last renamed " << lrv << ") on " << *this << dendl;
  assert(is_auth());

  CInode *inode = get_inode();

  // did we update the parent pointer too?
  if (get_frag() == frag_t() &&     // only counts on first frag
      inode->state_test(CInode::STATE_DIRTYPARENT) &&
      lrv == inode->inode.last_renamed_version) {
    inode->item_renamed_file.remove_myself();
    inode->state_clear(CInode::STATE_DIRTYPARENT);
    inode->put(CInode::PIN_DIRTYPARENT);
    dout(10) << "_committed  stored parent pointer, removed from renamed_files list " << *inode << dendl;
  }
  
  // take note.
  assert(v > committed_version);
  assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version) 
    state_clear(CDirFrag::STATE_COMMITTING);

  // _any_ commit, even if we've been redirtied, means we're no longer new.
  item_new.remove_myself();
  
  // dir clean?
  if (committed_version == get_version()) 
    mark_clean();

  // dentries clean?
  for (map_t::iterator it = items.begin();
       it != items.end(); ) {
    CDentry *dn = it->second;
    it++;
    
    // inode?
    if (dn->linkage.is_primary()) {
      CInode *in = dn->linkage.get_inode();
      assert(in);
      assert(in->is_auth());
      
      if (committed_version >= in->get_version()) {
	if (in->is_dirty()) {
	  dout(15) << " dir " << committed_version << " >= inode " << in->get_version() << " now clean " << *in << dendl;
	  in->mark_clean();
	}
      } else {
	dout(15) << " dir " << committed_version << " < inode " << in->get_version() << " still dirty " << *in << dendl;
	assert(in->is_dirty() || in->last < CEPH_NOSNAP);  // special case for cow snap items (not predirtied)
      }
    }

    // dentry
    if (committed_version >= dn->get_version()) {
      if (dn->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << dendl;
	dn->mark_clean();
      } 
    } else {
      dout(15) << " dir " << committed_version << " < dn " << dn->get_version() << " still dirty " << *dn << dendl;
    }
  }

  // finishers?
  bool were_waiters = !waiting_for_commit.empty();
  
  map<version_t, list<Context*> >::iterator p = waiting_for_commit.begin();
  while (p != waiting_for_commit.end()) {
    map<version_t, list<Context*> >::iterator n = p;
    n++;
    if (p->first > committed_version) {
      dout(10) << " there are waiters for " << p->first << ", committing again" << dendl;
      _commit(p->first);
      break;
    }
    cache->mds->queue_waiters(p->second);
    waiting_for_commit.erase(p);
    p = n;
  } 

  // unpin if we kicked the last waiter.
  if (were_waiters &&
      waiting_for_commit.empty())
    auth_unpin(this);
}





// IMPORT/EXPORT

void CDirFrag::encode_export(bufferlist& bl)
{
  ::encode(first, bl);
  ::encode(version, bl);
  ::encode(snap_purged_thru, bl);
  ::encode(committed_version, bl);
  ::encode(state, bl);
  ::encode(replica_map, bl);

  get(PIN_TEMPEXPORTING);
}

void CDirFrag::finish_export(utime_t now)
{
  put(PIN_TEMPEXPORTING);
}

void CDirFrag::decode_import(bufferlist::iterator& blp, utime_t now)
{
  ::decode(first, blp);
  ::decode(version, blp);
  ::decode(snap_purged_thru, blp);
  ::decode(committed_version, blp);
  committing_version = committed_version;

  unsigned s;
  ::decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) get(PIN_DIRTY);

  ::decode(replica_map, blp);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined
}


/** contains(x)
 * true if we are x, or an ancestor of x
 */
bool CDirFrag::contains(CDirFrag *x)
{
  while (1) {
    if (x == this)
      return true;
    x = x->get_inode()->get_projected_parent_dir();
    if (x == 0)
      return false;    
  }
}


/*****************************************
 * AUTH PINS and FREEZING
 *
 * the basic plan is that auth_pins only exist in auth regions, and they
 * prevent a freeze (and subsequent auth change).  
 *
 * however, we also need to prevent a parent from freezing if a child is frozen.
 * for that reason, the parent inode of a frozen directory is auth_pinned.
 *
 * the oddity is when the frozen directory is a subtree root.  if that's the case,
 * the parent inode isn't frozen.  which means that when subtree authority is adjusted
 * at the bounds, inodes for any frozen bound directories need to get auth_pins at that
 * time.
 *
 */

void CDirFrag::auth_pin(void *by) 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this
	   << " count now " << auth_pins << dendl;
}

void CDirFrag::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin by " << by << " on " << *this
	   << " count now " << auth_pins << dendl;
  assert(auth_pins >= 0);

  maybe_finish_freeze();  // pending freeze?
}


/*****************************************************************************
 * FREEZING
 */

void CDirFrag::maybe_finish_freeze()
{
  if (auth_pins != 1)
    return;

  // we can freeze the _dir_ even with nested pins...
  if (state_test(STATE_FREEZING)) {
    _freeze_dir();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
  }
}

bool CDirFrag::freeze_dir()
{
  assert(!is_frozen());
  assert(!is_freezing());
  
  auth_pin(this);
  if (is_freezeable_dir(true)) {
    _freeze_dir();
    auth_unpin(this);
    return true;
  } else {
    state_set(STATE_FREEZING);
    dout(10) << "freeze_dir + wait " << *this << dendl;
    return false;
  } 
}

void CDirFrag::_freeze_dir()
{
  dout(10) << "_freeze_dir " << *this << dendl;
  //assert(is_freezeable_dir(true));
  // not always true during split because the original fragment may have frozen a while
  // ago and we're just now getting around to breaking it up.

  state_clear(STATE_FREEZING);
  state_set(STATE_FROZEN);
  get(PIN_FROZEN);
}


void CDirFrag::unfreeze_dir()
{
  dout(10) << "unfreeze_dir " << *this << dendl;

  if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);

    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // still freezing. stop.
    assert(state_test(STATE_FREEZING));
    state_clear(STATE_FREEZING);
    auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
  }
}

