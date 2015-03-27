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
 * Foundation.	See file COPYING.
 *
 */


#include <cassert>
#include "include/types.h"

#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "Mutation.h"

#include "MDSMap.h"
#include "MDS.h"
#include "MDCache.h"
#include "Locker.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "include/Context.h"
#include "osdc/Objecter.h"

#include "common/config.h"
#include "common/MultiCallback.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << cache->mds->get_nodeid() << ".cache.dir(" << this->dirfrag() << ") "



// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };

boost::pool<> CDir::pool(sizeof(CDir));


ostream& operator<<(ostream& out, CDir& dir)
{
  string path;
  dir.get_inode()->make_path_string_projected(path);
  out << "[dir " << dir.dirfrag() << " " << path << "/";
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    if (dir.is_projected())
      out << " pv=" << dir.get_projected_version();
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

  if (dir.is_rep()) out << " REP";

  if (dir.get_dir_auth() != CDIR_AUTH_DEFAULT) {
    if (dir.get_dir_auth().second == CDIR_AUTH_UNKNOWN)
      out << " dir_auth=" << dir.get_dir_auth().first;
    else
      out << " dir_auth=" << dir.get_dir_auth();
  }

  if (dir.get_cum_auth_pins())
    out << " ap=" << dir.get_auth_pins()
	<< "+" << dir.get_dir_auth_pins()
	<< "+" << dir.get_nested_auth_pins();
  if (dir.get_nested_anchors())
    out << " na=" << dir.get_nested_anchors();

  out << " state=" << dir.get_state();
  if (dir.state_test(CDir::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDir::STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDir::STATE_FROZENTREE)) out << "|frozentree";
  //if (dir.state_test(CDir::STATE_FROZENTREELEAF)) out << "|frozentreeleaf";
  if (dir.state_test(CDir::STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDir::STATE_FREEZINGDIR)) out << "|freezingdir";
  if (dir.state_test(CDir::STATE_EXPORTBOUND)) out << "|exportbound";
  if (dir.state_test(CDir::STATE_IMPORTBOUND)) out << "|importbound";

  // fragstat
  out << " " << dir.fnode.fragstat;
  if (!(dir.fnode.fragstat == dir.fnode.accounted_fragstat))
    out << "/" << dir.fnode.accounted_fragstat;
  if (dir.cct->_conf->mds_debug_scatterstat && dir.is_projected()) {
    fnode_t *pf = dir.get_projected_fnode();
    out << "->" << pf->fragstat;
    if (!(pf->fragstat == pf->accounted_fragstat))
      out << "/" << pf->accounted_fragstat;
  }

  // rstat
  out << " " << dir.fnode.rstat;
  if (!(dir.fnode.rstat == dir.fnode.accounted_rstat))
    out << "/" << dir.fnode.accounted_rstat;
  if (dir.cct->_conf->mds_debug_scatterstat && dir.is_projected()) {
    fnode_t *pf = dir.get_projected_fnode();
    out << "->" << pf->rstat;
    if (!(pf->rstat == pf->accounted_rstat))
      out << "/" << pf->accounted_rstat;
 }

  out << " hs=" << dir.get_num_head_items() << "+" << dir.get_num_head_null();
  if (dir.get_num_dirty())
    out << " dirty=" << dir.get_num_dirty();

  if (dir.get_num_ref()) {
    out << " |";
    dir.print_pin_set(out);
  }

  out << " " << &dir;
  return out << "]";
}


void CDir::print(ostream& out)
{
  out << *this;
}




ostream& CDir::print_db_line_prefix(ostream& out)
{
  return out << ceph::real_clock::now() << " mds." << cache->mds->get_nodeid()
	     << ".cache.dir(" << this->dirfrag() << ") ";
}



// -------------------------------------------------------------------
// CDir

CDir::CDir(CephContext* cct, CInode *in, frag_t fg, MDCache *mdcache,
	   bool auth) :
  cct(in->cct), dirty_rstat_inodes(member_offset(CInode, dirty_rstat_item)),
  item_dirty(this), item_new(this),
  pop_me(ceph::real_clock::now()), pop_nested(ceph::real_clock::now()),
  pop_auth_subtree(ceph::real_clock::now()),
  pop_auth_subtree_nested(ceph::real_clock::now()), pop_spread(cct)

{
  g_num_dir++;
  g_num_dira++;

  inode = in;
  frag = fg;
  this->cache = mdcache;

  num_head_items = num_head_null = 0;
  num_dirty = 0;

  num_dentries_nested = 0;
  num_dentries_auth_subtree = 0;
  num_dentries_auth_subtree_nested = 0;

  state = STATE_INITIAL;

  memset(&fnode, 0, sizeof(fnode));
  projected_version = 0;

  committing_version = 0;
  committed_version = 0;

  // dir_auth
  dir_auth = CDIR_AUTH_DEFAULT;

  // auth
  assert(in->is_dir());
  if (auth)
    state |= STATE_AUTH;

  auth_pins = 0;
  nested_auth_pins = 0;
  dir_auth_pins = 0;
  request_pins = 0;

  nested_anchors = 0;

  dir_rep = REP_NONE;
}

/**
 * Check the recursive statistics on size for consistency.
 * If mds_debug_scatterstat is enabled, assert for correctness,
 * otherwise just print out the mismatch and continue.
 */
bool CDir::check_rstats()
{
  if (!cct->_conf->mds_debug_scatterstat)
    return true;

  dout(25) << "check_rstats on " << this << dendl;
  if (!is_complete() || !is_auth() || is_frozen()) {
    dout(10) << "check_rstats bailing out -- incomplete or non-auth or "
	     << "frozen dir!" << dendl;
    return true;
  }

  // fragstat
  if(!(get_num_head_items()==
      (fnode.fragstat.nfiles + fnode.fragstat.nsubdirs))) {
    dout(1) << "mismatch between head items and fnode.fragstat! "
	    << "printing dentries" << dendl;
    dout(1) << "get_num_head_items() = " << get_num_head_items()
	    << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
	    << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
    for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
      //if (i->second->get_linkage()->is_primary())
      dout(1) << *(i->second) << dendl;
    }
    assert(get_num_head_items() == (fnode.fragstat.nfiles
				    + fnode.fragstat.nsubdirs));
  } else {
    dout(20) << "get_num_head_items() = " << get_num_head_items()
	     << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
	     << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs
	     << dendl;
  }

  // rstat
  nest_info_t sub_info;
  for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
    if (i->second->get_linkage()->is_primary()) {
      sub_info.add(i->second->get_linkage()->inode->inode.accounted_rstat);
    }
  }

  if ((!(sub_info.rbytes == fnode.rstat.rbytes)) ||
      (!(sub_info.rfiles == fnode.rstat.rfiles)) ||
      (!(sub_info.rsubdirs == fnode.rstat.rsubdirs))) {
    dout(1) << "mismatch between child accounted_rstats and my rstats!" << dendl;
    dout(1) << "total of child dentrys: " << sub_info << dendl;
    dout(1) << "my rstats:		" << fnode.rstat << dendl;
    for (map_t::iterator i = items.begin(); i != items.end(); ++i) {
      if (i->second->get_linkage()->is_primary()) {
	dout(1) << *(i->second) << " "
		<< i->second->get_linkage()->inode->inode.accounted_rstat
		<< dendl;
      }
    }
  } else {
    dout(25) << "total of child dentrys: " << sub_info << dendl;
    dout(25) << "my rstats:		 " << fnode.rstat << dendl;
  }

  assert(sub_info.rbytes == fnode.rstat.rbytes);
  assert(sub_info.rfiles == fnode.rstat.rfiles);
  assert(sub_info.rsubdirs == fnode.rstat.rsubdirs);
  dout(10) << "check_rstats complete on " << this << dendl;
  return true;
}


CDentry *CDir::lookup(const string& name)
{
  dout(20) << "lookup (" << name << "')" << dendl;
  map_t::iterator iter = items.find(name);
  if (iter == items.end())
    return 0;
  dout(20) << "	 hit -> " << iter->first << dendl;
  return iter->second;
}

/***
 * linking fun
 */

CDentry* CDir::add_null_dentry(const string& dname)
{
  // foreign
  assert(lookup(dname) == 0);

  // create dentry
  CDentry* dn = new CDentry(cct, dname, inode->hash_dentry_name(dname));
  if (is_auth())
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
   dn->version = get_projected_version();

   // add to dir
   assert(items.count(dn->name) == 0);

   items[dn->name] = dn;
   num_head_null++;

   if (state_test(CDir::STATE_DNPINNEDFRAG)) {
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


 CDentry* CDir::add_primary_dentry(const string& dname, CInode *in)
 {
   // primary
   assert(lookup(dname) == 0);

   // create dentry
   CDentry* dn = new CDentry(cct, dname, inode->hash_dentry_name(dname));
   if (is_auth())
     dn->state_set(CDentry::STATE_AUTH);
   cache->lru.lru_insert_mid(dn);

   dn->dir = this;
   dn->version = get_projected_version();

   // add to dir
   assert(items.count(dn->name) == 0);

  items[dn->name] = dn;

  dn->get_linkage()->inode = in;
  in->set_primary_parent(dn);

  link_inode_work(dn, in);

  num_head_items++;

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
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

CDentry* CDir::add_remote_dentry(const string& dname, inodeno_t ino,
				 unsigned char d_type)
{
  // foreign
  assert(lookup(dname) == 0);

  // create dentry
  CDentry* dn = new CDentry(cct, dname, inode->hash_dentry_name(dname), ino,
			    d_type);
  if (is_auth())
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = get_projected_version();

  // add to dir
  assert(items.count(dn->name) == 0);
  //assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  num_head_items++;

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
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



void CDir::remove_dentry(CDentry *dn)
{
  dout(12) << "remove_dentry " << *dn << dendl;

  // there should be no client leases at this point!
  assert(dn->client_lease_map.empty());

  if (state_test(CDir::STATE_DNPINNEDFRAG)) {
    dn->put(CDentry::PIN_FRAGMENTING);
    dn->state_clear(CDentry::STATE_FRAGMENTING);
  }

  if (dn->get_linkage()->is_null()) {
    num_head_null--;
  } else {
    num_head_items--;
  }

  if (!dn->get_linkage()->is_null())
    // detach inode and dentry
    unlink_inode_work(dn);

  // remove from list
  assert(items.count(dn->name) == 1);
  items.erase(dn->name);

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

void CDir::link_remote_inode(CDentry *dn, CInode *in)
{
  link_remote_inode(dn, in->ino(), IFTODT(in->get_projected_inode()->mode));
}

void CDir::link_remote_inode(CDentry *dn, inodeno_t ino, unsigned char d_type)
{
  dout(12) << "link_remote_inode " << *dn << " remote " << ino << dendl;
  assert(dn->get_linkage()->is_null());

  dn->get_linkage()->set_remote(ino, d_type);

  num_head_items++;
  num_head_null--;
  assert(get_num_any() == items.size());
}

void CDir::link_primary_inode(CDentry *dn, CInode *in)
{
  dout(12) << "link_primary_inode " << *dn << " " << *in << dendl;
  assert(dn->get_linkage()->is_null());

  dn->get_linkage()->inode = in;
  in->set_primary_parent(dn);

  link_inode_work(dn, in);

  num_head_items++;
  num_head_null--;

  assert(get_num_any() == items.size());
}

void CDir::link_inode_work(CDentry *dn, CInode *in)
{
  assert(dn->get_linkage()->get_inode() == in);
  assert(in->get_parent_dn() == dn);

  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);

  // adjust auth pin count
  if (in->auth_pins + in->nested_auth_pins)
    dn->adjust_nested_auth_pins(in->auth_pins + in->nested_auth_pins, in->auth_pins, NULL);

  if (in->inode.anchored + in->nested_anchors)
    dn->adjust_nested_anchors(in->nested_anchors + in->inode.anchored);

}

void CDir::unlink_inode(CDentry *dn)
{
  if (dn->get_linkage()->is_remote()) {
    dout(12) << "unlink_inode " << *dn << dendl;
  } else {
    dout(12) << "unlink_inode " << *dn << " " << *dn->get_linkage()->get_inode() << dendl;
  }

  unlink_inode_work(dn);

  num_head_items--;
  num_head_null++;

  assert(get_num_any() == items.size());
}


void CDir::try_remove_unlinked_dn(CDentry *dn)
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


void CDir::unlink_inode_work( CDentry *dn )
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

    // unlink auth_pin count
    if (in->auth_pins + in->nested_auth_pins)
      dn->adjust_nested_auth_pins(0 - (in->auth_pins + in->nested_auth_pins), 0 - in->auth_pins, NULL);

    if (in->inode.anchored + in->nested_anchors)
      dn->adjust_nested_anchors(0 - (in->nested_anchors + in->inode.anchored));

    // detach inode
    in->remove_primary_parent(dn);
    dn->get_linkage()->inode = 0;
  }
}

void CDir::remove_null_dentries() {
  dout(12) << "remove_null_dentries " << *this << dendl;

  CDir::map_t::iterator p = items.begin();
  while (p != items.end()) {
    CDentry *dn = p->second;
    ++p;
    if (dn->get_linkage()->is_null() && !dn->is_projected())
      remove_dentry(dn);
  }

  assert(num_head_null == 0);
  assert(get_num_any() == items.size());
}

void CDir::touch_dentries_bottom() {
  dout(12) << "touch_dentries_bottom " << *this << dendl;

  for (CDir::map_t::iterator p = items.begin();
       p != items.end();
       ++p)
    inode->mdcache->touch_dentry_bottom(p->second);
}

/**
 * steal_dentry -- semi-violently move a dentry from one CDir to another
 * (*) violently, in that nitems, most pins, etc. are not correctly maintained
 * on the old CDir corpse; must call finish_old_fragment() when finished.
 */
void CDir::steal_dentry(CDentry *dn)
{
  dout(15) << "steal_dentry " << *dn << dendl;

  items[dn->name] = dn;

  dn->dir->items.erase(dn->name);
  if (dn->dir->items.empty())
    dn->dir->put(PIN_CHILD);

  if (get_num_any() == 0)
    get(PIN_CHILD);
  if (dn->get_linkage()->is_null()) {
    num_head_null++;
  } else {
    num_head_items++;

    if (dn->get_linkage()->is_primary()) {
      CInode *in = dn->get_linkage()->get_inode();
      inode_t *pi = in->get_projected_inode();
      if (dn->get_linkage()->get_inode()->is_dir())
	fnode.fragstat.nsubdirs++;
      else
	fnode.fragstat.nfiles++;
      fnode.rstat.rbytes += pi->accounted_rstat.rbytes;
      fnode.rstat.rfiles += pi->accounted_rstat.rfiles;
      fnode.rstat.rsubdirs += pi->accounted_rstat.rsubdirs;
      fnode.rstat.ranchors += pi->accounted_rstat.ranchors;
      if (pi->accounted_rstat.rctime > fnode.rstat.rctime)
	fnode.rstat.rctime = pi->accounted_rstat.rctime;

      // move dirty inode rstat to new dirfrag
      if (in->is_dirty_rstat())
	dirty_rstat_inodes.push_back(&in->dirty_rstat_item);
    } else if (dn->get_linkage()->is_remote()) {
      if (dn->get_linkage()->get_remote_d_type() == DT_DIR)
	fnode.fragstat.nsubdirs++;
      else
	fnode.fragstat.nfiles++;
    }
  }

  if (dn->auth_pins || dn->nested_auth_pins) {
    // use the helpers here to maintain the auth_pin invariants on the dir inode
    int ap = dn->get_num_auth_pins() + dn->get_num_nested_auth_pins();
    int dap = dn->get_num_dir_auth_pins();
    assert(dap <= ap);
    adjust_nested_auth_pins(ap, dap, NULL);
    dn->dir->adjust_nested_auth_pins(-ap, -dap, NULL);
  }

  nested_anchors += dn->nested_anchors;
  if (dn->is_dirty())
    num_dirty++;

  dn->dir = this;
}

void CDir::prepare_old_fragment(bool replay)
{
  // auth_pin old fragment for duration so that any auth_pinning
  // during the dentry migration doesn't trigger side effects
  if (!replay && is_auth())
    auth_pin(this);
}

void CDir::prepare_new_fragment(bool replay)
{
  if (!replay && is_auth()) {
    _freeze_dir();
    mark_complete();
  }
}

void CDir::finish_old_fragment(std::vector<Context*>& waiters,
			       bool replay)
{
  // take waiters _before_ unfreeze...
  if (!replay) {
    take_waiting(WAIT_ANY_MASK, waiters);
    if (is_auth()) {
      auth_unpin(this);	 // pinned in prepare_old_fragment
      assert(is_frozen_dir());
      unfreeze_dir();
    }
  }

  assert(nested_auth_pins == 0);
  assert(dir_auth_pins == 0);
  assert(auth_pins == 0);

  num_head_items = num_head_null = 0;

  // this mirrors init_fragment_pins()
  if (is_auth())
    clear_replica_map();
  if (is_dirty())
    mark_clean();
  if (state_test(STATE_IMPORTBOUND))
    put(PIN_IMPORTBOUND);
  if (state_test(STATE_EXPORTBOUND))
    put(PIN_EXPORTBOUND);
  if (is_subtree_root())
    put(PIN_SUBTREE);

  if (auth_pins > 0)
    put(PIN_AUTHPIN);

  assert(get_num_ref() == (state_test(STATE_STICKY) ? 1:0));
}

void CDir::init_fragment_pins()
{
  if (!replica_map.empty())
    get(PIN_REPLICATED);
  if (state_test(STATE_DIRTY))
    get(PIN_DIRTY);
  if (state_test(STATE_EXPORTBOUND))
    get(PIN_EXPORTBOUND);
  if (state_test(STATE_IMPORTBOUND))
    get(PIN_IMPORTBOUND);
  if (is_subtree_root())
    get(PIN_SUBTREE);
}

void CDir::split(int bits, list<CDir*>& subs,
		 std::vector<Context*>& waiters, bool replay)
{
  dout(10) << "split by " << bits << " bits on " << *this << dendl;

  assert(replay || is_complete() || !is_auth());

  list<frag_t> frags;
  frag.split(bits, frags);

  vector<CDir*> subfrags(1 << bits);

  double fac = 1.0 / (double)(1 << bits);  // for scaling load vecs

  dout(15) << "		  rstat " << fnode.rstat << dendl;
  dout(15) << " accounted_rstat " << fnode.accounted_rstat << dendl;
  nest_info_t rstatdiff;
  rstatdiff.add_delta(fnode.accounted_rstat, fnode.rstat);
  dout(15) << "		  fragstat " << fnode.fragstat << dendl;
  dout(15) << " accounted_fragstat " << fnode.accounted_fragstat << dendl;
  frag_info_t fragstatdiff;
  bool touched_mtime;
  fragstatdiff.add_delta(fnode.accounted_fragstat, fnode.fragstat, touched_mtime);
  dout(10) << " rstatdiff " << rstatdiff << " fragstatdiff " << fragstatdiff << dendl;

  prepare_old_fragment(replay);

  // create subfrag dirs
  int n = 0;
  for (list<frag_t>::iterator p = frags.begin(); p != frags.end(); ++p) {
    CDir *f = new CDir(cct, inode, *p, cache, is_auth());
    f->state_set(state & (MASK_STATE_FRAGMENT_KEPT | STATE_COMPLETE));
    f->replica_map = replica_map;
    f->dir_auth = dir_auth;
    f->init_fragment_pins();
    f->set_version(get_version());

    f->pop_me = pop_me;
    f->pop_me.scale(fac);

    // FIXME; this is an approximation
    f->pop_nested = pop_nested;
    f->pop_nested.scale(fac);
    f->pop_auth_subtree = pop_auth_subtree;
    f->pop_auth_subtree.scale(fac);
    f->pop_auth_subtree_nested = pop_auth_subtree_nested;
    f->pop_auth_subtree_nested.scale(fac);

    dout(10) << " subfrag " << *p << " " << *f << dendl;
    subfrags[n++] = f;
    subs.push_back(f);
    inode->add_dirfrag(f);

    f->set_dir_auth(get_dir_auth());
    f->prepare_new_fragment(replay);
  }

  // repartition dentries
  while (!items.empty()) {
    CDir::map_t::iterator p = items.begin();

    CDentry *dn = p->second;
    frag_t subfrag = inode->pick_dirfrag(dn->name);
    int n = (subfrag.value() & (subfrag.mask() ^ frag.mask())) >> subfrag.mask_shift();
    dout(15) << " subfrag " << subfrag << " n=" << n << " for " << p->first << dendl;
    CDir *f = subfrags[n];
    f->steal_dentry(dn);
  }

  // FIXME: handle dirty old rstat

  // fix up new frag fragstats
  for (int i=0; i<n; i++) {
    CDir *f = subfrags[i];
    f->fnode.rstat.version = fnode.rstat.version;
    f->fnode.accounted_rstat = f->fnode.rstat;
    f->fnode.fragstat.version = fnode.fragstat.version;
    f->fnode.accounted_fragstat = f->fnode.fragstat;
    dout(10) << " rstat " << f->fnode.rstat << " fragstat " << f->fnode.fragstat
	     << " on " << *f << dendl;
  }

  // give any outstanding frag stat differential to first frag
  dout(10) << " giving rstatdiff " << rstatdiff << " fragstatdiff" << fragstatdiff
	   << " to " << *subfrags[0] << dendl;
  subfrags[0]->fnode.accounted_rstat.add(rstatdiff);
  subfrags[0]->fnode.accounted_fragstat.add(fragstatdiff);

  finish_old_fragment(waiters, replay);
}

void CDir::merge(list<CDir*>& subs, std::vector<Context*>& waiters,
		 bool replay)
{
  dout(10) << "merge " << subs << dendl;

  set_dir_auth(subs.front()->get_dir_auth());
  prepare_new_fragment(replay);

  nest_info_t rstatdiff;
  frag_info_t fragstatdiff;
  bool touched_mtime;
  version_t rstat_version = inode->get_projected_inode()->rstat.version;
  version_t dirstat_version = inode->get_projected_inode()->dirstat.version;

  for (list<CDir*>::iterator p = subs.begin(); p != subs.end(); ++p) {
    CDir *dir = *p;
    dout(10) << " subfrag " << dir->get_frag() << " " << *dir << dendl;
    assert(!dir->is_auth() || dir->is_complete() || replay);

    if (dir->fnode.accounted_rstat.version == rstat_version)
      rstatdiff.add_delta(dir->fnode.accounted_rstat, dir->fnode.rstat);
    if (dir->fnode.accounted_fragstat.version == dirstat_version)
      fragstatdiff.add_delta(dir->fnode.accounted_fragstat, dir->fnode.fragstat,
			     touched_mtime);

    dir->prepare_old_fragment(replay);

    // steal dentries
    while (!dir->items.empty())
      steal_dentry(dir->items.begin()->second);

    // merge replica map
    for (map<int,unsigned>::iterator p = dir->replicas_begin();
	 p != dir->replica_map.end();
	 ++p) {
      unsigned cur = replica_map[p->first];
      if (p->second > cur)
	replica_map[p->first] = p->second;
    }

    // merge version
    if (dir->get_version() > get_version())
      set_version(dir->get_version());

    // merge state
    state_set(dir->get_state() & MASK_STATE_FRAGMENT_KEPT);
    dir_auth = dir->dir_auth;

    dir->finish_old_fragment(waiters, replay);
    inode->close_dirfrag(dir->get_frag());
  }

  if (is_auth() && !replay)
    mark_complete();

  // FIXME: merge dirty old rstat
  fnode.rstat.version = rstat_version;
  fnode.accounted_rstat = fnode.rstat;
  fnode.accounted_rstat.add(rstatdiff);

  fnode.fragstat.version = dirstat_version;
  fnode.accounted_fragstat = fnode.fragstat;
  fnode.accounted_fragstat.add(fragstatdiff);

  init_fragment_pins();
}




void CDir::resync_accounted_fragstat()
{
  fnode_t *pf = get_projected_fnode();
  inode_t *pi = inode->get_projected_inode();

  if (pf->accounted_fragstat.version != pi->dirstat.version) {
    pf->fragstat.version = pi->dirstat.version;
    dout(10) << "resync_accounted_fragstat " << pf->accounted_fragstat << " -> " << pf->fragstat << dendl;
    pf->accounted_fragstat = pf->fragstat;
  }
}

/*
 * resync rstat and accounted_rstat with inode
 */
void CDir::resync_accounted_rstat()
{
  fnode_t *pf = get_projected_fnode();
  inode_t *pi = inode->get_projected_inode();

  if (pf->accounted_rstat.version != pi->rstat.version) {
    pf->rstat.version = pi->rstat.version;
    dout(10) << "resync_accounted_rstat " << pf->accounted_rstat << " -> " << pf->rstat << dendl;
    pf->accounted_rstat = pf->rstat;
  }
}

void CDir::assimilate_dirty_rstat_inodes()
{
  dout(10) << "assimilate_dirty_rstat_inodes" << dendl;
  for (elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
       !p.end(); ++p) {
    CInode *in = *p;
    assert(in->is_auth());
    if (in->is_frozen())
      continue;

    inode_t *pi = in->project_inode();
    pi->version = in->pre_dirty();
  }
  state_set(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes done" << dendl;
}

void CDir::assimilate_dirty_rstat_inodes_finish(MutationRef& mut, EMetaBlob *blob)
{
  if (!state_test(STATE_ASSIMRSTAT))
    return;
  state_clear(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes_finish" << dendl;
  elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
  while (!p.end()) {
    CInode *in = *p;
    ++p;

    if (in->is_frozen())
      continue;

    CDentry *dn = in->get_projected_parent_dn();

    mut->auth_pin(in);
    mut->add_projected_inode(in);

    in->clear_dirty_rstat();
    blob->add_primary_dentry(dn, in, true);
  }

  if (!dirty_rstat_inodes.empty())
    inode->mdcache->mds->locker->mark_updated_scatterlock(&inode->nestlock);
}




/****************************************
 * WAITING
 */

void CDir::add_dentry_waiter(const string& dname, Context *c)
{
  if (waiting_on_dentry.empty())
    get(PIN_DNWAITER);
  waiting_on_dentry[dname].push_back(c);
  dout(10) << "add_dentry_waiter dentry " << dname
	   << " " << c << " on " << *this << dendl;
}

void CDir::take_dentry_waiting(const string& dname,
			       std::vector<Context*>& vs)
{
  if (waiting_on_dentry.empty())
    return;

  map<string, std::vector<Context*> >::iterator p =
    waiting_on_dentry.find(dname);
  if (p != waiting_on_dentry.end()) {
    dout(10) << "take_dentry_waiting dentry " << dname
	     << " found waiter "
	     << " on " << *this << dendl;
    move_left(vs, p->second);
    waiting_on_dentry.erase(p++);
  }

  if (waiting_on_dentry.empty())
    put(PIN_DNWAITER);
}

void CDir::add_ino_waiter(inodeno_t ino, Context *c)
{
  if (waiting_on_ino.empty())
    get(PIN_INOWAITER);
  waiting_on_ino[ino].push_back(c);
  dout(10) << "add_ino_waiter ino " << ino << " " << c << " on " << *this
	   << dendl;
}

void CDir::take_ino_waiting(inodeno_t ino, std::vector<Context*>& vs)
{
  if (waiting_on_ino.empty()) return;
  if (waiting_on_ino.count(ino) == 0) return;
  dout(10) << "take_ino_waiting ino " << ino
	   << " x " << waiting_on_ino[ino].size()
	   << " on " << *this << dendl;
  move_left(vs, waiting_on_ino[ino]);
  waiting_on_ino.erase(ino);
  if (waiting_on_ino.empty())
    put(PIN_INOWAITER);
}

void CDir::take_sub_waiting(std::vector<Context*>& vs)
{
  dout(10) << "take_sub_waiting" << dendl;
  if (!waiting_on_dentry.empty()) {
    for (map<string, std::vector<Context*> >::iterator p
	   = waiting_on_dentry.begin();
	 p != waiting_on_dentry.end();
	 ++p)
      move_left(vs, p->second);
    waiting_on_dentry.clear();
    put(PIN_DNWAITER);
  }
  if (!waiting_on_ino.empty()) {
    for (map<inodeno_t, std::vector<Context*> >::iterator p =
	   waiting_on_ino.begin();
	 p != waiting_on_ino.end();
	 ++p)
      move_left(vs, p->second);
    waiting_on_ino.clear();
    put(PIN_INOWAITER);
  }
}

void CDir::add_waiter(uint64_t tag, Context *c)
{
  // hierarchical?

  // at free root?
  if (tag & WAIT_ATFREEZEROOT) {
    if (!(is_freezing_tree_root() || is_frozen_tree_root() ||
	  is_freezing_dir() || is_frozen_dir())) {
      // try parent
      dout(10) << "add_waiter " << std::hex << tag << std::dec
	       << " " << c
	       << " should be ATFREEZEROOT, " << *this
	       << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }

  // at subtree root?
  if (tag & WAIT_ATSUBTREEROOT) {
    if (!is_subtree_root()) {
      // try parent
      dout(10) << "add_waiter " << std::hex << tag << std::dec << " " << c
	       << " should be ATSUBTREEROOT, " << *this
	       << " is not root, trying parent" << dendl;
      inode->parent->dir->add_waiter(tag, c);
      return;
    }
  }

  MDSCacheObject::add_waiter(tag, c);
}

/* NOTE: this checks dentry waiters too */
void CDir::take_waiting(uint64_t mask, std::vector<Context*>& vs)
{
  if ((mask & WAIT_DENTRY) && !waiting_on_dentry.empty()) {
    // take all dentry waiters
    while (!waiting_on_dentry.empty()) {
      map<string, std::vector<Context*> >::iterator p =
	waiting_on_dentry.begin();
      dout(10) << "take_waiting dentry " << p->first
	       << " on " << *this << dendl;
      move_left(vs, p->second);
      waiting_on_dentry.erase(p);
    }
    put(PIN_DNWAITER);
  }

  // waiting
  MDSCacheObject::take_waiting(mask, vs);
}

void CDir::finish_waiting(uint64_t mask, int result)
{
  dout(11) << "finish_waiting mask " << hex << mask << dec
	   << " result " << result << " on " << *this << dendl;

  std::vector<Context*> finished;
  take_waiting(mask, finished);
  if (result < 0)
    finish_contexts(finished, result);
  else
    cache->mds->queue_waiters(finished);
}

// dirty/clean

fnode_t *CDir::project_fnode()
{
  assert(get_version() != 0);
  fnode_t *p = new fnode_t;
  *p = *get_projected_fnode();
  projected_fnode.push_back(p);
  dout(10) << "project_fnode " << p << dendl;
  return p;
}

void CDir::pop_and_dirty_projected_fnode(LogSegment *ls)
{
  assert(!projected_fnode.empty());
  dout(15) << "pop_and_dirty_projected_fnode " << projected_fnode.front()
	   << " v" << projected_fnode.front()->version << dendl;
  fnode = *projected_fnode.front();
  _mark_dirty(ls);
  delete projected_fnode.front();
  projected_fnode.pop_front();
}

version_t CDir::pre_dirty(version_t min)
{
  if (min > projected_version)
    projected_version = min;
  ++projected_version;
  dout(10) << "pre_dirty " << projected_version << dendl;
  return projected_version;
}

void CDir::mark_dirty(version_t pv, LogSegment *ls)
{
  assert(get_version() < pv);
  assert(pv <= projected_version);
  fnode.version = pv;
  _mark_dirty(ls);
}

void CDir::_mark_dirty(LogSegment *ls)
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

void CDir::mark_new(LogSegment *ls)
{
  ls->new_dirfrags.push_back(&item_new);
}

void CDir::mark_clean()
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
  CDir *dir;
  version_t pv;
  LogSegment *ls;
  C_Dir_Dirty(CDir *d, version_t p, LogSegment *l) : dir(d), pv(p), ls(l) {}
  void finish(int r) {
    dir->mark_dirty(pv, ls);
  }
};

void CDir::log_mark_dirty()
{
  MDLog *mdlog = inode->mdcache->mds->mdlog;
  version_t pv = pre_dirty();
  mdlog->flush();
  mdlog->wait_for_safe(new C_Dir_Dirty(this, pv, mdlog->get_current_segment()));
}

void CDir::mark_complete() {
  state_set(STATE_COMPLETE);
}

void CDir::first_get()
{
  inode->get(CInode::PIN_DIRFRAG);
}

void CDir::last_put()
{
  inode->put(CInode::PIN_DIRFRAG);
}



/******************************************************************************
 * FETCH and COMMIT
 */

// -----------------------
// FETCH
void CDir::fetch(Context *c, bool ignore_authpinnability)
{
  string want;
  return fetch(c, want, ignore_authpinnability);
}

void CDir::fetch(Context *c, const string& want_dn, bool ignore_authpinnability)
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
  if (state_test(CDir::STATE_FETCHING)) {
    dout(7) << "already fetching; waiting" << dendl;
    return;
  }

  auth_pin(this);
  state_set(CDir::STATE_FETCHING);

  _omap_fetch(want_dn);
}

class CB_Dir_OMAP_Fetched {
 protected:
  CDir *dir;
  string want_dn;
 public:
  bufferlist hdrbl;
  map<string, bufferlist> omap;
  int ret1, ret2;

  CB_Dir_OMAP_Fetched(CDir *d, const string& w) : dir(d), want_dn(w) { }
  void operator()(int r) {
    if (r >= 0) r = ret1;
    if (r >= 0) r = ret2;
    dir->_omap_fetched(hdrbl, omap, want_dn, r);
  }
};

void CDir::_omap_fetch(const string& want_dn)
{
  oid_t oid = get_ondisk_object();
  VolumeRef volume(cache->mds->get_metadata_volume());
  CB_Dir_OMAP_Fetched fin(this, want_dn);
  if (!volume) {
    dout(0) << "Unable to attach volume " << volume << dendl;
    return;
  }
  std::unique_ptr<ObjOp> rd = volume->op();
  if (!rd) {
    dout(0) << "Unable to make operation for volume " << volume << dendl;
    return;
  }
  rd->omap_get_header(&fin.hdrbl, &fin.ret1);
  rd->omap_get_vals("", "", (uint64_t)-1, fin.omap, &fin.ret2);
  cache->mds->objecter->read(oid, volume, rd, NULL, 0,
			     std::ref(fin));
}

void CDir::_omap_fetched(bufferlist& hdrbl, map<string, bufferlist>& omap,
			 const string& want_dn, int r)
{
  LogClient &clog = cache->mds->clog;
  dout(10) << "_fetched header " << hdrbl.length() << " bytes "
	   << omap.size() << " keys for " << *this
	   << " want_dn=" << want_dn << dendl;

  assert(r == 0 || r == -ENOENT || r == -ENODATA);
  assert(is_auth());
  assert(!is_frozen());

  if (hdrbl.length() == 0) {

    dout(0) << "_fetched missing object for " << *this << dendl;
    clog.error() << "dir " << dirfrag() << " object missing on disk; some files may be lost\n";

    log_mark_dirty();

    // mark complete, !fetching
    mark_complete();
    state_clear(STATE_FETCHING);
    auth_unpin(this);

    // kick waiters
    finish_waiting(WAIT_COMPLETE, 0);
    return;
  }

  fnode_t got_fnode;
  {
    bufferlist::iterator p = hdrbl.begin();
    ::decode(got_fnode, p);
    if (!p.end()) {
      clog.warn() << "header buffer of dir " << dirfrag() << " has "
		  << hdrbl.length() - p.get_off() << " extra bytes\n";
    }
  }

  dout(10) << "_fetched version " << got_fnode.version << dendl;

  // take the loaded fnode?
  // only if we are a fresh CDir* with no prior state.
  if (get_version() == 0) {
    assert(!is_projected());
    assert(!state_test(STATE_COMMITTING));
    fnode = got_fnode;
    projected_version = committing_version = committed_version = got_fnode.version;

    if (state_test(STATE_REJOINUNDEF)) {
      assert(cache->mds->is_rejoin());
      state_clear(STATE_REJOINUNDEF);
      cache->opened_undef_dirfrag(this);
    }
  }

  list<CInode*> undef_inodes;

  // mark complete, !fetching
  mark_complete();
  state_clear(STATE_FETCHING);

  // open & force frags
  while (!undef_inodes.empty()) {
    CInode *in = undef_inodes.front();
    undef_inodes.pop_front();
    in->state_clear(CInode::STATE_REJOINUNDEF);
    cache->opened_undef_inode(in);
  }

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
void CDir::commit(version_t want, Context *c, bool ignore_authpinnability, int op_prio)
{
  dout(10) << "commit want " << want << " on " << *this << dendl;
  if (want == 0) want = get_version();

  // preconditions
  assert(want <= get_version() || get_version() == 0);	  // can't commit the future
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
  _commit(want, op_prio);
}

class Dir_Committed : public cohort::SimpleMultiCallback<int> {
  friend class cohort::MultiCallback;
  CDir *dir;
  version_t version;
  Dir_Committed(CDir *d, version_t v) : dir(d), version(v) { } ;
public:
  void work(int r) {
    assert(r == 0);
  }
  void finish() {
    dir->_committed(version);
  }
};

/**
 * Flush out the modified dentries in this dir. Keep the bufferlist
 * below max_write_size;
 */
void CDir::_omap_commit(int op_prio)
{
  dout(10) << "_omap_commit" << dendl;

  unsigned max_write_size = cache->max_dir_commit_size;
  unsigned write_size = 0;

  if (op_prio < 0)
    op_prio = CEPH_MSG_PRIO_DEFAULT;

  set<string> to_remove;
  map<string, bufferlist> to_set;

  auto& committed = cohort::MultiCallback::create<Dir_Committed>(
    this, get_version());

  oid_t oid = get_ondisk_object();
  VolumeRef volume(cache->mds->get_metadata_volume());
  if (!volume) {
    dout(0) << "Unable to attach volume " << volume << dendl;
    return;
  }

  for (map_t::iterator p = items.begin();
      p != items.end(); ) {
    CDentry *dn = p->second;
    ++p;

    if (!dn->is_dirty() &&
	(!dn->state_test(CDentry::STATE_FRAGMENTING) || dn->get_linkage()->is_null()))
      continue;	 // skip clean dentries

    if (dn->get_linkage()->is_null()) {
      dout(10) << " rm " << dn->name << " " << *dn << dendl;
      write_size += dn->name.length();
      to_remove.insert(dn->name);
    } else {
      dout(10) << " set " << dn->name << " " << *dn << dendl;
      bufferlist dnbl;
      _encode_dentry(dn, dnbl);
      write_size += dn->name.length() + dnbl.length();
      to_set[dn->name].swap(dnbl);
    }

    if (write_size >= max_write_size) {
      std::unique_ptr<ObjOp> op(volume->op());
      if (!op) {
	dout(0) << "Unable to make operation for volume " << volume << dendl;
	return;
      }
      op->priority = op_prio;

      if (!to_set.empty())
	op->omap_set(to_set);
      if (!to_remove.empty())
	op->omap_rm_keys(to_remove);

      cache->mds->objecter->mutate(oid, volume, op,
				   ceph::real_clock::now(), 0, NULL,
				   committed.add());

      write_size = 0;
      to_set.clear();
      to_remove.clear();
    }
  }

  std::unique_ptr<ObjOp> op(volume->op());
  if (!op) {
    dout(0) << "Unable to make operation for volume " << volume << dendl;
    return;
  }
  op->priority = op_prio;

  /*
   * save the header at the last moment.. If we were to send it off
   * before other updates, but die before sending them all, we'd think
   * that the on-disk state was fully committed even though it wasn't!
   * However, since the messages are strictly ordered between the MDS
   * and the OSD, and since messages to a given PG are strictly
   * ordered, if we simply send the message containing the header off
   * last, we cannot get our header into an incorrect state.
   */
  bufferlist header;
  ::encode(fnode, header);
  op->omap_set_header(header);

  if (!to_set.empty())
    op->omap_set(to_set);
  if (!to_remove.empty())
    op->omap_rm_keys(to_remove);

  cache->mds->objecter->mutate(oid, volume, op, ceph::real_clock::now(),
			       0, NULL, committed.add());

  committed.activate();
}

void CDir::_encode_dentry(CDentry *dn, bufferlist& bl)
{
  // clear dentry NEW flag, if any.  we can no longer silently drop it.
  dn->clear_new();

  // primary or remote?
  if (dn->linkage.is_remote()) {
    inodeno_t ino = dn->linkage.get_remote_ino();
    unsigned char d_type = dn->linkage.get_remote_d_type();
    dout(14) << " pos " << bl.length() << " dn '" << dn->name
	     << "' remote ino " << ino << dendl;

    // marker, name, ino
    bl.append('L'); // remote link
    ::encode(ino, bl);
    ::encode(d_type, bl);
  } else {
    // primary link
    CInode *in = dn->linkage.get_inode();
    assert(in);

    dout(14) << " pos " << bl.length() << " dn '" << dn->name << "' inode " << *in << dendl;

    // marker, name, inode, [symlink string]
    bl.append('I'); // inode
    ::encode(in->inode, bl);

    if (in->is_symlink()) {
      // include symlink destination!
      dout(18) << "    including symlink ptr " << in->symlink << dendl;
      ::encode(in->symlink, bl);
    }

    ::encode(in->dirfragtree, bl);
    ::encode(in->xattrs, bl);
  }
}

void CDir::_commit(version_t want, int op_prio)
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

  // commit.
  committing_version = get_version();

  // mark committing (if not already)
  if (!state_test(STATE_COMMITTING)) {
    dout(10) << "marking committing" << dendl;
    state_set(STATE_COMMITTING);
  }

  _omap_commit(op_prio);
}


/**
 * _committed
 *
 * @param v version i just committed
 */
void CDir::_committed(version_t v)
{
  dout(10) << "_committed v " << v << " on " << *this << dendl;
  assert(is_auth());

  bool stray = inode->is_stray();

  // take note.
  assert(v > committed_version);
  assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version)
    state_clear(CDir::STATE_COMMITTING);

  // _any_ commit, even if we've been redirtied, means we're no longer new.
  item_new.remove_myself();

  // dir clean?
  if (committed_version == get_version())
    mark_clean();

  // dentries clean?
  for (map_t::iterator it = items.begin();
       it != items.end(); ) {
    CDentry *dn = it->second;
    ++it;

    // inode?
    if (dn->linkage.is_primary()) {
      CInode *in = dn->linkage.get_inode();
      assert(in);
      assert(in->is_auth());

      if (committed_version >= in->get_version()) {
	if (in->is_dirty()) {
	  dout(15) << " dir " << committed_version << " >= inode "
		   << in->get_version() << " now clean " << *in << dendl;
	  in->mark_clean();
	}
      }
    }

    // dentry
    if (committed_version >= dn->get_version()) {
      if (dn->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= dn "
		 << dn->get_version() << " now clean " << *dn << dendl;
	dn->mark_clean();

	// drop clean null stray dentries immediately
	if (stray &&
	    dn->get_num_ref() == 0 &&
	    !dn->is_projected() &&
	    dn->get_linkage()->is_null())
	  remove_dentry(dn);
      }
    } else {
      dout(15) << " dir " << committed_version << " < dn "
	       << dn->get_version() << " still dirty " << *dn << dendl;
    }
  }

  // finishers?
  bool were_waiters = !waiting_for_commit.empty();

  map<version_t, std::vector<Context*> >::iterator p =
    waiting_for_commit.begin();
  while (p != waiting_for_commit.end()) {
    map<version_t, std::vector<Context*> >::iterator n = p;
    ++n;
    if (p->first > committed_version) {
      dout(10) << " there are waiters for " << p->first
	       << ", committing again" << dendl;
      _commit(p->first, -1);
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

void CDir::encode_export(bufferlist& bl)
{
  assert(!is_projected());
  ::encode(fnode, bl);
  ::encode(committed_version, bl);

  ::encode(state, bl);
  ::encode(dir_rep, bl);

  ::encode(pop_me, bl);
  ::encode(pop_auth_subtree, bl);

  ::encode(dir_rep_by, bl);
  ::encode(replica_map, bl);

  get(PIN_TEMPEXPORTING);
}

void CDir::finish_export(ceph::real_time now)
{
  state &= MASK_STATE_EXPORT_KEPT;
  pop_auth_subtree_nested.sub(now, cache->decayrate, pop_auth_subtree);
  pop_me.zero(now);
  pop_auth_subtree.zero(now);
  put(PIN_TEMPEXPORTING);
}

void CDir::decode_import(bufferlist::iterator& blp,
			 ceph::real_time now,
			 LogSegment *ls)
{
  ::decode(fnode, blp);
  projected_version = fnode.version;
  ::decode(committed_version, blp);
  committing_version = committed_version;

  unsigned s;
  ::decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) {
    get(PIN_DIRTY);
    _mark_dirty(ls);
  }

  ::decode(dir_rep, blp);

  ::decode(pop_me, blp);
  ::decode(pop_auth_subtree, blp);
  pop_auth_subtree_nested.add(now, cache->decayrate, pop_auth_subtree);

  ::decode(dir_rep_by, blp);
  ::decode(replica_map, blp);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined

  // did we import some dirty scatterlock data?
  if (!(fnode.fragstat == fnode.accounted_fragstat)) {
    cache->mds->locker->mark_updated_scatterlock(&inode->filelock);
    ls->dirty_dirfrag_dir.push_back(&inode->item_dirty_dirfrag_dir);
  }
  if (is_dirty_dft()) {
    if (inode->dirfragtreelock.get_state() != LOCK_MIX &&
	inode->dirfragtreelock.is_stable()) {
      // clear stale dirtydft
      state_clear(STATE_DIRTYDFT);
    } else {
      cache->mds->locker->mark_updated_scatterlock(&inode->dirfragtreelock);
      ls->dirty_dirfrag_dirfragtree.push_back(&inode->item_dirty_dirfrag_dirfragtree);
    }
  }
}




/********************************
 * AUTHORITY
 */

/*
 * if dir_auth.first == parent, auth is same as inode.
 * unless .second != unknown, in which case that sticks.
 */
pair<int,int> CDir::authority()
{
  if (is_subtree_root())
    return dir_auth;
  else
    return inode->authority();
}

/** is_subtree_root()
 * true if this is an auth delegation point.
 * that is, dir_auth != default (parent,unknown)
 *
 * some key observations:
 *  if i am auth:
 *    - any region bound will be an export, or frozen.
 *
 * note that this DOES heed dir_auth.pending
 */
/*
bool CDir::is_subtree_root()
{
  if (dir_auth == CDIR_AUTH_DEFAULT) {
    //dout(10) << "is_subtree_root false " << dir_auth << " != " << CDIR_AUTH_DEFAULT
    //<< " on " << ino() << dendl;
    return false;
  } else {
    //dout(10) << "is_subtree_root true " << dir_auth << " != " << CDIR_AUTH_DEFAULT
    //<< " on " << ino() << dendl;
    return true;
  }
}
*/

/** contains(x)
 * true if we are x, or an ancestor of x
 */
bool CDir::contains(CDir *x)
{
  while (1) {
    if (x == this)
      return true;
    x = x->get_inode()->get_projected_parent_dir();
    if (x == 0)
      return false;
  }
}



/** set_dir_auth
 */
void CDir::set_dir_auth(pair<int,int> a)
{
  dout(10) << "setting dir_auth=" << a
	   << " from " << dir_auth
	   << " on " << *this << dendl;

  bool was_subtree = is_subtree_root();
  bool was_ambiguous = dir_auth.second >= 0;

  // set it.
  dir_auth = a;

  // new subtree root?
  if (!was_subtree && is_subtree_root()) {
    dout(10) << " new subtree root, adjusting auth_pins" << dendl;

    // adjust nested auth pins
    if (get_cum_auth_pins())
      inode->adjust_nested_auth_pins(-1, NULL);

    // unpin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_unpin(this);
  }
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << dendl;

    // adjust nested auth pins
    if (get_cum_auth_pins())
      inode->adjust_nested_auth_pins(1, NULL);

    // pin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_pin(this);
  }

  // newly single auth?
  if (was_ambiguous && dir_auth.second == CDIR_AUTH_UNKNOWN) {
    std::vector<Context*> vs;
    take_waiting(WAIT_SINGLEAUTH, vs);
    cache->mds->queue_waiters(vs);
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

void CDir::auth_pin(void *by)
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by
	   << " on " << *this
	   << " count now " << auth_pins << " + " << nested_auth_pins << dendl;

  // nest pins?
  if (!is_subtree_root() &&
      get_cum_auth_pins() == 1)
    inode->adjust_nested_auth_pins(1, by);
}

void CDir::auth_unpin(void *by)
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin by " << by
	   << " on " << *this
	   << " count now " << auth_pins << " + " << nested_auth_pins << dendl;
  assert(auth_pins >= 0);

  int newcum = get_cum_auth_pins();

  maybe_finish_freeze();  // pending freeze?

  // nest?
  if (!is_subtree_root() &&
      newcum == 0)
    inode->adjust_nested_auth_pins(-1, by);
}

void CDir::adjust_nested_auth_pins(int inc, int dirinc, void *by)
{
  assert(inc);
  nested_auth_pins += inc;
  dir_auth_pins += dirinc;

  dout(15) << "adjust_nested_auth_pins " << inc << "/" << dirinc << " on " << *this
	   << " by " << by << " count now "
	   << auth_pins << " + " << nested_auth_pins << dendl;
  assert(nested_auth_pins >= 0);
  assert(dir_auth_pins >= 0);

  int newcum = get_cum_auth_pins();

  maybe_finish_freeze();  // pending freeze?

  // nest?
  if (!is_subtree_root()) {
    if (newcum == 0)
      inode->adjust_nested_auth_pins(-1, by);
    else if (newcum == inc)
      inode->adjust_nested_auth_pins(1, by);
  }
}

void CDir::adjust_nested_anchors(int by)
{
  assert(by);
  nested_anchors += by;
  dout(20) << "adjust_nested_anchors by " << by << " -> " << nested_anchors << dendl;
  assert(nested_anchors >= 0);
  inode->adjust_nested_anchors(by);
}

#ifdef MDS_VERIFY_FRAGSTAT
void CDir::verify_fragstat()
{
  assert(is_complete());
  if (inode->is_stray())
    return;

  frag_info_t c;
  memset(&c, 0, sizeof(c));

  for (map_t::iterator it = items.begin();
       it != items.end();
       ++it) {
    CDentry *dn = it->second;
    if (dn->is_null())
      continue;

    dout(10) << " " << *dn << dendl;
    if (dn->is_primary())
      dout(10) << "	" << *dn->inode << dendl;

    if (dn->is_primary()) {
      if (dn->inode->is_dir())
	c.nsubdirs++;
      else
	c.nfiles++;
    }
    if (dn->is_remote()) {
      if (dn->get_remote_d_type() == DT_DIR)
	c.nsubdirs++;
      else
	c.nfiles++;
    }
  }

  if (c.nsubdirs != fnode.fragstat.nsubdirs ||
      c.nfiles != fnode.fragstat.nfiles) {
    dout(0) << "verify_fragstat failed " << fnode.fragstat << " on " << *this << dendl;
    dout(0) << "	       i count " << c << dendl;
    assert(0);
  } else {
    dout(0) << "verify_fragstat ok " << fnode.fragstat << " on " << *this << dendl;
  }
}
#endif

/*****************************************************************************
 * FREEZING
 */

// FREEZE TREE

bool CDir::freeze_tree()
{
  assert(!is_frozen());
  assert(!is_freezing());

  auth_pin(this);
  if (is_freezeable(true)) {
    _freeze_tree();
    auth_unpin(this);
    return true;
  } else {
    state_set(STATE_FREEZINGTREE);
    dout(10) << "freeze_tree waiting " << *this << dendl;
    return false;
  }
}

void CDir::_freeze_tree()
{
  dout(10) << "_freeze_tree " << *this << dendl;
  assert(is_freezeable(true));

  // twiddle state
  state_clear(STATE_FREEZINGTREE);   // actually, this may get set again by next context?
  state_set(STATE_FROZENTREE);
  get(PIN_FROZEN);

  // auth_pin inode for duration of freeze, if we are not a subtree root.
  if (is_auth() && !is_subtree_root())
    inode->auth_pin(this);
}

void CDir::unfreeze_tree()
{
  dout(10) << "unfreeze_tree " << *this << dendl;

  if (state_test(STATE_FROZENTREE)) {
    // frozen.	unfreeze.
    state_clear(STATE_FROZENTREE);
    put(PIN_FROZEN);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    if (is_auth() && !is_subtree_root())
      inode->auth_unpin(this);

    // waiters?
    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // freezing.  stop it.
    assert(state_test(STATE_FREEZINGTREE));
    state_clear(STATE_FREEZINGTREE);
    auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
  }
}

bool CDir::is_freezing_tree()
{
  CDir *dir = this;
  while (1) {
    if (dir->is_freezing_tree_root()) return true;
    if (dir->is_subtree_root()) return false;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      return false; // root on replica
  }
}

bool CDir::is_frozen_tree()
{
  CDir *dir = this;
  while (1) {
    if (dir->is_frozen_tree_root()) return true;
    if (dir->is_subtree_root()) return false;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      return false;  // root on replica
  }
}

CDir *CDir::get_frozen_tree_root()
{
  assert(is_frozen());
  CDir *dir = this;
  while (1) {
    if (dir->is_frozen_tree_root())
      return dir;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      assert(0);
  }
}

struct C_Dir_AuthUnpin : public Context {
  CDir *dir;
  C_Dir_AuthUnpin(CDir *d) : dir(d) {}
  void finish(int r) {
    dir->auth_unpin(dir->get_inode());
  }
};

void CDir::maybe_finish_freeze()
{
  if (auth_pins != 1 || dir_auth_pins != 0)
    return;

  // we can freeze the _dir_ even with nested pins...
  if (state_test(STATE_FREEZINGDIR)) {
    _freeze_dir();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
  }

  if (nested_auth_pins != 0)
    return;

  if (state_test(STATE_FREEZINGTREE)) {
    if (!is_subtree_root() && inode->is_frozen()) {
      dout(10) << "maybe_finish_freeze !subtree root and frozen inode, waiting for unfreeze on " << inode << dendl;
      // retake an auth_pin...
      auth_pin(inode);
      // and release it when the parent inode unfreezes
      inode->add_waiter(WAIT_UNFREEZE, new C_Dir_AuthUnpin(this));
      return;
    }

    _freeze_tree();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
  }
}



// FREEZE DIR

bool CDir::freeze_dir()
{
  assert(!is_frozen());
  assert(!is_freezing());

  auth_pin(this);
  if (is_freezeable_dir(true)) {
    _freeze_dir();
    auth_unpin(this);
    return true;
  } else {
    state_set(STATE_FREEZINGDIR);
    dout(10) << "freeze_dir + wait " << *this << dendl;
    return false;
  }
}

void CDir::_freeze_dir()
{
  dout(10) << "_freeze_dir " << *this << dendl;
  //assert(is_freezeable_dir(true));
  // not always true during split because the original fragment may have frozen a while
  // ago and we're just now getting around to breaking it up.

  state_clear(STATE_FREEZINGDIR);
  state_set(STATE_FROZENDIR);
  get(PIN_FROZEN);

  if (is_auth() && !is_subtree_root())
    inode->auth_pin(this);  // auth_pin for duration of freeze
}


void CDir::unfreeze_dir()
{
  dout(10) << "unfreeze_dir " << *this << dendl;

  if (state_test(STATE_FROZENDIR)) {
    state_clear(STATE_FROZENDIR);
    put(PIN_FROZEN);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    if (is_auth() && !is_subtree_root())
      inode->auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
  } else {
    finish_waiting(WAIT_FROZEN, -1);

    // still freezing. stop.
    assert(state_test(STATE_FREEZINGDIR));
    state_clear(STATE_FREEZINGDIR);
    auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
  }
}
