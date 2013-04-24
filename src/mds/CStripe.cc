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

#include <inttypes.h>
#include <string>
#include <stdio.h>

#include "CStripe.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Locker.h"
#include "Mutation.h"

#include "events/EUpdate.h"

#include "osdc/Objecter.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "messages/MLock.h"
#include "messages/MClientCaps.h"

#include "common/config.h"
#include "global/global_context.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.stripe(" << dirstripe() << ") "


boost::pool<> CStripe::pool(sizeof(CStripe));

LockType CStripe::dirfragtreelock_type(CEPH_LOCK_SDFT);

ostream& CStripe::print_db_line_prefix(ostream& out)
{
  return out << ceph_clock_now(g_ceph_context) << " mds." << mdcache->mds->get_nodeid() << ".cache.stripe(" << dirstripe() << ") ";
}

ostream& operator<<(ostream& out, CStripe &s)
{
  string path;
  s.get_inode()->make_path_string_projected(path);
  out << "[stripe " << s.dirstripe() << " " << path << "/";

  if (s.is_auth()) {
    out << " auth";
    if (s.is_replicated()) 
      out << s.get_replicas();
  } else {
    pair<int,int> a = s.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << s.get_replica_nonce();
  }

  out << " v" << s.get_version();
  if (s.get_projected_version() > s.get_version())
    out << " pv" << s.get_projected_version();

  if (s.is_auth_pinned())
    out << " ap=" << s.get_num_auth_pins()
        << "+" << s.get_num_nested_auth_pins();

  if (!s.get_fragtree().empty())
    out << " " << s.get_fragtree();
  if (!s.dirfragtreelock.is_sync_and_unlocked())
    out << " " << s.dirfragtreelock;

  // fragstat
  out << " " << s.fnode.fragstat;
  if (!s.is_fragstat_accounted())
    out << "/" << s.fnode.accounted_fragstat;
  if (g_conf->mds_debug_scatterstat && s.is_projected()) {
    fnode_t *pf = s.get_projected_fnode();
    out << "->" << pf->fragstat;
    if (!(pf->fragstat == pf->accounted_fragstat))
      out << "/" << pf->accounted_fragstat;
  }

  // rstat
  out << " " << s.fnode.rstat;
  if (!s.is_rstat_accounted())
    out << "/" << s.fnode.accounted_rstat;
  if (g_conf->mds_debug_scatterstat && s.is_projected())
  {
    fnode_t *pf = s.get_projected_fnode();
    out << "->" << pf->rstat;
    if (!(pf->rstat == pf->accounted_rstat))
      out << "/" << pf->accounted_rstat;
  }

  if (s.state_test(CStripe::STATE_OPEN)) out << " OPEN";
  if (s.state_test(CStripe::STATE_DIRTY)) out << " DIRTY";
  if (s.state_test(CStripe::STATE_FREEZING)) out << " FREEZING";
  if (s.state_test(CStripe::STATE_FROZEN)) out << " FROZEN";
  if (s.state_test(CStripe::STATE_ASSIMRSTAT)) out << " ASSIMRSTAT";
  if (s.state_test(CStripe::STATE_COMMITTING)) out << " COMMITTING";
  if (s.state_test(CStripe::STATE_IMPORTBOUND)) out << " IMPORTBOUND";
  if (s.state_test(CStripe::STATE_EXPORTBOUND)) out << " EXPORTBOUND";
  if (s.state_test(CStripe::STATE_EXPORTING)) out << " EXPORTING";
  if (s.state_test(CStripe::STATE_IMPORTING)) out << " IMPORTING";

  if (s.is_rep()) out << " REP";

  if (s.get_num_ref()) {
    out << " |";
    s.print_pin_set(out);
  }

  out << " " << &s;
  out << "]";
  return out;
}

void CStripe::print(ostream& out)
{
  out << *this;
}


CStripe::CStripe(CInode *in, stripeid_t stripeid, int auth)
  : mdcache(in->mdcache),
    inode(in),
    stripeid(stripeid),
    stripe_auth(auth, CDIR_AUTH_UNKNOWN),
    auth_pins(0),
    nested_auth_pins(0),
    replicate(false),
    pop_me(ceph_clock_now(g_ceph_context)),
    pop_nested(ceph_clock_now(g_ceph_context)),
    pop_auth_subtree(ceph_clock_now(g_ceph_context)),
    pop_auth_subtree_nested(ceph_clock_now(g_ceph_context)),
    first(2),
    projected_version(0),
    committing_version(0),
    committed_version(0),
    dirty_rstat_inodes(member_offset(CInode, dirty_rstat_item)),
    item_dirty(this),
    item_new(this),
    stickydir_ref(0),
    dirfragtreelock(this, &dirfragtreelock_type)
{
  memset(&fnode, 0, sizeof(fnode));
  if (auth == mdcache->mds->get_nodeid())
    state_set(STATE_AUTH);
}


unsigned CStripe::get_num_head_items()
{
  unsigned count = 0;
  for (map<frag_t, CDir*>::iterator i = dirfrags.begin(); i != dirfrags.end(); ++i)
    count += i->second->get_num_head_items();
  return count;
}

unsigned CStripe::get_num_any()
{
  unsigned count = 0;
  for (map<frag_t, CDir*>::iterator i = dirfrags.begin(); i != dirfrags.end(); ++i)
    count += i->second->get_num_any();
  return count;
}

// fragstat/rstat
fnode_t *CStripe::project_fnode()
{
  fnode_t *p = new fnode_t;
  *p = *get_projected_fnode();
  projected_fnode.push_back(p);
  dout(10) << "project_fnode " << p << dendl;
  return p;
}

void CStripe::pop_and_dirty_projected_fnode(LogSegment *ls)
{
  assert(!projected_fnode.empty());
  dout(15) << "pop_and_dirty_projected_fnode " << projected_fnode.front()
      << " v" << projected_fnode.front()->version << dendl;
  fnode = *projected_fnode.front();
  delete projected_fnode.front();
  projected_fnode.pop_front();

  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
  }
}

void CStripe::resync_accounted_fragstat()
{
  fnode_t *pf = get_projected_fnode();
  inode_t *pi = get_inode()->get_projected_inode();

  if (pf->accounted_fragstat.version != pi->dirstat.version) {
    pf->fragstat.version = pi->dirstat.version;
    dout(10) << "resync_accounted_fragstat " << pf->accounted_fragstat << " -> " << pf->fragstat << dendl;
    pf->accounted_fragstat = pf->fragstat;
  }
}

/*
 * resync rstat and accounted_rstat with inode
 */
void CStripe::resync_accounted_rstat()
{
  fnode_t *pf = get_projected_fnode();
  inode_t *pi = get_inode()->get_projected_inode();
  
  if (pf->accounted_rstat.version != pi->rstat.version) {
    pf->rstat.version = pi->rstat.version;
    dout(10) << "resync_accounted_rstat " << pf->accounted_rstat << " -> " << pf->rstat << dendl;
    pf->accounted_rstat = pf->rstat;
    dirty_old_rstat.clear();
  }
}

void CStripe::assimilate_dirty_rstat_inodes()
{
  dout(10) << "assimilate_dirty_rstat_inodes" << dendl;
  for (elist<CInode*>::iterator p = dirty_rstat_inodes.begin_use_current();
       !p.end(); ++p) {
    CInode *in = *p;
    if (in->is_frozen())
      continue;

    inode_t *pi = in->project_inode();
    pi->version = in->pre_dirty();

    mdcache->project_rstat_inode_to_frag(in, this, 0, 0);
  }
  state_set(STATE_ASSIMRSTAT);
  dout(10) << "assimilate_dirty_rstat_inodes done" << dendl;
}

void CStripe::assimilate_dirty_rstat_inodes_finish(Mutation *mut, EMetaBlob *blob)
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
    blob->add_primary_dentry(dn, true, in);
  }

  if (!dirty_rstat_inodes.empty())
    mdcache->mds->locker->mark_updated_scatterlock(&get_inode()->nestlock);
}


// dirfrags

frag_t CStripe::pick_dirfrag(__u32 dnhash)
{
  return dirfragtree[dnhash];
}

frag_t CStripe::pick_dirfrag(const string& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = get_inode()->hash_dentry_name(dn);
  return dirfragtree[h];
}

bool CStripe::get_dirfrags_under(frag_t fg, list<CDir*>& ls)
{
  bool all = true;
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (fg.contains(p->first))
      ls.push_back(p->second);
    else
      all = false;
  }
  return all;
}

void CStripe::verify_dirfrags() const
{
  bool bad = false;
  for (map<frag_t,CDir*>::const_iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (!dirfragtree.is_leaf(p->first)) {
      dout(0) << "have open dirfrag " << p->first << " but not leaf in " << dirfragtree
	      << ": " << *p->second << dendl;
      bad = true;
    }
  }
  assert(!bad);
}

void CStripe::force_dirfrags()
{
  bool bad = false;
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (!dirfragtree.is_leaf(p->first)) {
      dout(0) << "have open dirfrag " << p->first << " but not leaf in " << dirfragtree
	      << ": " << *p->second << dendl;
      bad = true;
    }
  }

  if (bad) {
    list<frag_t> leaves;
    dirfragtree.get_leaves(leaves);
    for (list<frag_t>::iterator p = leaves.begin(); p != leaves.end(); ++p)
      mdcache->get_force_dirfrag(dirfrag_t(dirstripe(), *p));
  }

  verify_dirfrags();
}

CDir *CStripe::get_approx_dirfrag(frag_t fg)
{
  CDir *dir = get_dirfrag(fg);
  if (dir) return dir;

  // find a child?
  list<CDir*> ls;
  get_dirfrags_under(fg, ls);
  if (!ls.empty()) 
    return ls.front();

  // try parents?
  while (1) {
    fg = fg.parent();
    dir = get_dirfrag(fg);
    if (dir) return dir;
  }
  return NULL;
}	

void CStripe::get_dirfrags(list<CDir*>& ls) 
{
  // all dirfrags
  for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    ls.push_back(p->second);
}

CDir *CStripe::get_or_open_dirfrag(frag_t fg)
{
  // have it?
  CDir *dir = get_dirfrag(fg);
  if (!dir) // create it.
    dir = add_dirfrag(new CDir(this, fg, mdcache, is_auth()));
  return dir;
}

CDir *CStripe::add_dirfrag(CDir *dir)
{
  assert(dirfrags.count(dir->get_frag()) == 0);
  dirfrags[dir->get_frag()] = dir;

  if (stickydir_ref > 0) {
    dir->state_set(CDir::STATE_STICKY);
    dir->get(CDir::PIN_STICKY);
  }

  return dir;
}

void CStripe::close_dirfrag(frag_t fg)
{
  dout(14) << "close_dirfrag " << fg << dendl;
  assert(dirfrags.count(fg));
  
  CDir *dir = dirfrags[fg];
  dir->remove_null_dentries();
  
  // clear dirty flag
  if (dir->is_dirty())
    dir->mark_clean();
  
  if (stickydir_ref > 0) {
    dir->state_clear(CDir::STATE_STICKY);
    dir->put(CDir::PIN_STICKY);
  }
  
  // dump any remaining dentries, for debugging purposes
  for (CDir::map_t::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) 
    dout(14) << "close_dirfrag LEFTOVER dn " << *p->second << dendl;

  assert(dir->get_num_ref() == 0);
  delete dir;
  dirfrags.erase(fg);
}

void CStripe::close_dirfrags()
{
  while (!dirfrags.empty()) 
    close_dirfrag(dirfrags.begin()->first);
}


void CStripe::get_stickydirs()
{
  if (stickydir_ref == 0) {
    get(PIN_STICKYDIRS);
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_set(CDir::STATE_STICKY);
      p->second->get(CDir::PIN_STICKY);
    }
  }
  stickydir_ref++;
}

void CStripe::put_stickydirs()
{
  assert(stickydir_ref > 0);
  stickydir_ref--;
  if (stickydir_ref == 0) {
    put(PIN_STICKYDIRS);
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_clear(CDir::STATE_STICKY);
      p->second->put(CDir::PIN_STICKY);
    }
  }
}


// locks

void CStripe::set_object_info(MDSCacheObjectInfo &info)
{
  info.dirfrag.stripe = dirstripe();
}

void CStripe::encode_lock_state(int type, bufferlist& bl)
{
  assert(type == CEPH_LOCK_SDFT);
  if (is_auth()) // encode the raw tree for replicas
    ::encode(dirfragtree, bl);
}

void CStripe::decode_lock_state(int type, bufferlist& bl)
{
  assert(type == CEPH_LOCK_SDFT);
  bufferlist::iterator p = bl.begin();
  if (!is_auth()) // take the new tree
    ::decode(dirfragtree, p);
}


// pins

void CStripe::first_get()
{
  inode->get(CInode::PIN_STRIPE);
}

void CStripe::last_put()
{
  inode->put(CInode::PIN_STRIPE);
}

CStripe *CStripe::get_parent_stripe()
{
  return inode ? inode->get_parent_stripe() : NULL;
}

CStripe *CStripe::get_projected_parent_stripe()
{
  return inode ? inode->get_projected_parent_stripe() : NULL;
}

bool CStripe::contains(CStripe *stripe)
{
  while (stripe) {
    if (stripe == this)
      return true;
    stripe = stripe->get_parent_stripe();
  }
  return false;
}


version_t CStripe::pre_dirty(version_t min)
{
  if (projected_version > min)
    projected_version = min;
  ++projected_version;
  dout(10) << "pre_dirty " << projected_version << dendl;
  return projected_version;
}

void CStripe::mark_dirty(version_t pv, LogSegment *ls)
{
  dout(10) << "mark_dirty " << *this << dendl;

  assert(get_version() < pv);
  assert(pv <= projected_version);
  fnode.version = pv;
  _mark_dirty(ls);
}

void CStripe::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
    dout(10) << "_mark_dirty (was clean) " << *this << dendl;
    assert(ls);
  }

  if (ls) {
    // join segment's dirty list
    ls->dirty_stripes.push_back(&item_dirty);
    // join segment's new list if never committed
    if (committed_version == 0 && !item_new.is_on_list())
      ls->new_stripes.push_back(&item_new);
  }
}

void CStripe::mark_clean()
{
  dout(10) << "mark_clean " << *this << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    item_dirty.remove_myself();
    item_new.remove_myself();
  }
}

void CStripe::mark_new(LogSegment *ls)
{
  dout(10) << "mark_new " << *this << dendl;

  ls->new_stripes.push_back(&item_new);
}


// --------------
// stripe storage

struct C_Stripe_Committed : public Context {
  CStripe *stripe;
  Context *fin;
  C_Stripe_Committed(CStripe *stripe, Context *fin)
      : stripe(stripe), fin(fin) {}
  void finish(int r) {
    stripe->_committed();
    fin->complete(r);
  }
};

void CStripe::commit(Context *fin)
{
  dout(10) << "commit " << *this << dendl;

  assert(is_auth());
  assert(!state_test(STATE_COMMITTING));

  state_set(STATE_COMMITTING);
  committing_version = get_version();

  // encode
  bufferlist bl;
  string magic = CEPH_FS_ONDISK_MAGIC;
  ::encode(magic, bl);
  encode_store(bl);

  // write it.
  SnapContext snapc;
  ObjectOperation m;
  m.write_full(bl);

  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());
  utime_t now = ceph_clock_now(g_ceph_context);

  mdcache->mds->objecter->mutate(oid, oloc, m, snapc, now, 0, NULL,
				 new C_Stripe_Committed(this, fin));
}

void CStripe::_committed()
{
  assert(state_test(STATE_COMMITTING));
  state_clear(STATE_COMMITTING);
  committed_version = committing_version;
  mark_clean();
  dout(10) << "_committed " << *this << dendl;
}

struct C_Stripe_Fetched : public Context {
  CStripe *stripe;
  bufferlist bl;
  Context *fin;
  C_Stripe_Fetched(CStripe *stripe, Context *fin)
      : stripe(stripe), fin(fin) {}
  void finish(int r) {
    assert(r == 0);
    r = stripe->_fetched(bl);
    fin->complete(r);
  }
};

void CStripe::fetch(Context *fin)
{
  dout(10) << "fetch" << dendl;

  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  C_Stripe_Fetched *c = new C_Stripe_Fetched(this, fin);
  mdcache->mds->objecter->read(oid, oloc, 0, 0, CEPH_NOSNAP, &c->bl, 0, c);
}

int CStripe::_fetched(bufferlist& bl)
{
  dout(10) << "_fetched got " << bl.length() << dendl;

  bufferlist::iterator p = bl.begin();
  string magic;
  ::decode(magic, p);
  if (magic != CEPH_FS_ONDISK_MAGIC) {
    dout(0) << "on disk magic '" << magic << "' != my magic '"
        << CEPH_FS_ONDISK_MAGIC << "'" << dendl;
    return -EINVAL;
  }

  version_t v = get_version();
  decode_store(p);
  if (v == 0)
    committing_version = committed_version = projected_version = fnode.version;
  state_set(STATE_OPEN);
  dout(10) << "_fetched " << *this << dendl;
  return 0;
}


// replication

void CStripe::encode_export(bufferlist& bl)
{
  assert(!is_projected());
  ::encode(first, bl);
  ::encode(fnode, bl);
  ::encode(committed_version, bl);

  ::encode(state, bl);
  ::encode(replicate, bl);

  ::encode(pop_me, bl);
  ::encode(pop_auth_subtree, bl);

  ::encode(replica_map, bl);

  get(PIN_TEMPEXPORTING);
}

void CStripe::decode_import(bufferlist::iterator& blp, utime_t now)
{
  ::decode(first, blp);
  ::decode(fnode, blp);
  projected_version = fnode.version;
  ::decode(committed_version, blp);
  committing_version = committed_version;

  unsigned s;
  ::decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) get(PIN_DIRTY);

  ::decode(replicate, blp);

  ::decode(pop_me, now, blp);
  ::decode(pop_auth_subtree, now, blp);
  pop_auth_subtree_nested.add(now, mdcache->decayrate, pop_auth_subtree);

  ::decode(replica_map, blp);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined
}

void CStripe::finish_export(utime_t now)
{
  pop_auth_subtree_nested.sub(now, mdcache->decayrate, pop_auth_subtree);
  pop_me.zero(now);
  pop_auth_subtree.zero(now);

  put(PIN_TEMPEXPORTING);
}

void CStripe::abort_export()
{
  put(PIN_TEMPEXPORTING);
}


// freezing

bool CStripe::is_freezing()
{
  if (is_freezing_root())
    return true;
  if (is_subtree_root())
    return false;
  CStripe *parent = get_parent_stripe();
  return parent ? parent->is_freezing() : false;
}

bool CStripe::is_frozen()
{
  if (is_frozen_root())
    return true;
  if (is_subtree_root())
    return false;
  CStripe *parent = get_parent_stripe();
  return parent ? parent->is_frozen() : false;
}

bool CStripe::freeze()
{
  auth_pin(this); // auth pin while freezing
  if (auth_pins > 1 || nested_auth_pins > 0) {
    state_set(STATE_FREEZING);
    dout(10) << "freeze waiting " << *this << dendl;
    return false;
  }

  _freeze();
  auth_unpin(this);
  return true;
}

void CStripe::_freeze()
{
  state_clear(STATE_FREEZING);
  state_set(STATE_FROZEN);
  get(PIN_FROZEN);

  if (is_auth() && !is_subtree_root())
    inode->auth_pin(this);

  dout(10) << "stripe frozen " << *this << dendl;
}

void CStripe::unfreeze()
{
  if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);

    if (is_auth() && !is_subtree_root())
      inode->auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
    dout(10) << "unfreeze " << *this << dendl;
  } else {
    assert(state_test(STATE_FREEZING));
    finish_waiting(WAIT_FROZEN, -1);

    state_clear(STATE_FREEZING);
    auth_unpin(this);

    finish_waiting(WAIT_UNFREEZE);
    dout(10) << "unfreeze canceled freezing " << *this << dendl;
  }
}

void CStripe::maybe_finish_freeze()
{
  if (is_freezing_root() && auth_pins == 1 && nested_auth_pins == 0) {
    _freeze();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
  }  
}

void CStripe::add_waiter(uint64_t tag, Context *c)
{
  dout(10) << "add_waiter tag " << std::hex << tag << std::dec << " " << c
	   << " !ambig " << !is_ambiguous_stripe_auth()
	   << " !frozen " << !is_frozen()
	   << " !freezing " << !is_freezing()
	   << dendl;
  // wait on the stripe?
  //  make sure its not the stripe that is explicitly ambiguous|freezing|frozen
  if (((tag & WAIT_SINGLEAUTH) && !is_ambiguous_stripe_auth()) ||
      ((tag & WAIT_UNFREEZE) && !is_freezing_root() && !is_frozen_root())) {
    dout(15) << "passing waiter up tree" << dendl;
    inode->add_waiter(tag, c);
    return;
  }
  dout(15) << "taking waiter here" << dendl;
  MDSCacheObject::add_waiter(tag, c);
}

// auth_pins

void CStripe::auth_pin(void *by)
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;

  if (get_cum_auth_pins() == 1 && !is_subtree_root())
    inode->adjust_nested_auth_pins(1, this);
}

void CStripe::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif

  if (auth_pins == 0)
    put(PIN_AUTHPIN);
  
  dout(10) << "auth_unpin by " << by << " on " << *this
	   << " now " << auth_pins << "+" << nested_auth_pins
	   << dendl;
  
  assert(auth_pins >= 0);

  int newcum = get_cum_auth_pins();

  maybe_finish_freeze();

  if (newcum == 0 && !is_subtree_root())
    inode->adjust_nested_auth_pins(-1, by);
}

void CStripe::adjust_nested_auth_pins(int a, void *by)
{
  assert(a);
  nested_auth_pins += a;
  dout(15) << "adjust_nested_auth_pins by " << by
	   << " change " << a << " yields "
	   << auth_pins << "+" << nested_auth_pins << dendl;
  assert(nested_auth_pins >= 0);

  if (g_conf->mds_debug_auth_pins) {
    // audit
    int s = 0;
    for (map<frag_t,CDir*>::iterator p = dirfrags.begin();
         p != dirfrags.end();
         ++p)
      if (p->second->get_cum_auth_pins())
	s++;
    assert(s == nested_auth_pins);
  }

  int newcum = get_cum_auth_pins();

  maybe_finish_freeze();

  if (!is_subtree_root()) {
    if (newcum == 0)
      inode->adjust_nested_auth_pins(-1, by);
    else if (newcum == a)
      inode->adjust_nested_auth_pins(1, by);
  }
}


void CStripe::set_stripe_auth(const pair<int, int> &a)
{
  dout(10) << "setting stripe_auth=" << a
      << " from " << stripe_auth << " on " << *this << dendl;

  bool was_subtree = is_subtree_root();
  bool was_ambiguous = is_ambiguous_stripe_auth();

  // set it.
  stripe_auth = a;

  // new subtree root?
  if (!was_subtree && is_subtree_root()) {
    dout(10) << " new subtree root, adjusting auth_pins" << dendl;

    // adjust nested auth pins
    if (get_cum_auth_pins())
      inode->adjust_nested_auth_pins(-1, NULL);

    // unpin parent of frozen tree?
    if (inode->is_auth() && is_frozen_root())
      inode->auth_unpin(this);
  }
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << dendl;

    // adjust nested auth pins
    if (get_cum_auth_pins())
      inode->adjust_nested_auth_pins(1, NULL);

    // pin parent of frozen tree?
    if (inode->is_auth() && is_frozen_root())
      inode->auth_pin(this);
  }

  // newly single auth?
  if (was_ambiguous && !is_ambiguous_stripe_auth())
    finish_waiting(WAIT_SINGLEAUTH, 0);
}


//#ifdef MDS_VERIFY_FRAGSTAT
void CStripe::verify_fragstat()
{
  if (get_inode()->is_stray())
    return;

  // accumulate frag stats
  frag_info_t c;
  memset(&c, 0, sizeof(c));

  list<frag_t> frags;
  dirfragtree.get_leaves(frags);

  for (list<frag_t>::iterator f = frags.begin(); f != frags.end(); ++f) {
    CDir *dir = get_dirfrag(*f);
    assert(dir && dir->is_complete() && !dir->is_frozen());

    for (CDir::map_t::iterator it = dir->items.begin();
         it != dir->items.end();
         it++) {
      CDentry *dn = it->second;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_null())
        continue;

      dout(10) << " " << *dn << dendl;
      if (dnl->is_primary()) {
        dout(10) << "     " << *dnl->inode << dendl;
        if (dnl->inode->is_dir())
          c.nsubdirs++;
        else
          c.nfiles++;
      }
      if (dnl->is_remote()) {
        if (dnl->get_remote_d_type() == DT_DIR)
          c.nsubdirs++;
        else
          c.nfiles++;
      }
    }
  }

  if (c.nsubdirs != fnode.fragstat.nsubdirs ||
      c.nfiles != fnode.fragstat.nfiles) {
    dout(0) << "verify_fragstat failed " << fnode.fragstat << " on " << *this << dendl;
    dout(0) << "               i count " << c << dendl;
    assert(0);
  } else {
    dout(0) << "verify_fragstat ok " << fnode.fragstat << " on " << *this << dendl;
  }
}
//#endif

/**
 * Check the recursive statistics on size for consistency.
 * If mds_debug_scatterstat is enabled, assert for correctness,
 * otherwise just print out the mismatch and continue.
 */
bool CStripe::check_rstats()
{
  dout(25) << "check_rstats on " << *this << dendl;

  unsigned num_head_items = 0;
  nest_info_t sub_info;

  list<frag_t> frags;
  dirfragtree.get_leaves(frags);

  // accumulate dir stats
  for (list<frag_t>::iterator f = frags.begin(); f != frags.end(); ++f) {
    CDir *dir = get_dirfrag(*f);
    if (!dir || !dir->is_complete() || dir->is_frozen()) {
      dout(10) << "check_rstats bailing out -- incomplete or non-auth or frozen dir!" << dendl;
      return true;
    }

    num_head_items += dir->get_num_head_items();

    for (CDir::map_t::iterator i = dir->items.begin(); i != dir->items.end(); ++i)
      if (i->second->get_linkage()->is_primary() &&
          i->second->last == CEPH_NOSNAP)
        sub_info.add(i->second->get_linkage()->inode->inode.accounted_rstat);
  }

  // fragstat
  if (num_head_items != (fnode.fragstat.nfiles + fnode.fragstat.nsubdirs)) {
    dout(1) << "mismatch between head items and fnode.fragstat!" << dendl;
    dout(1) << "get_num_head_items() = " << num_head_items
             << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
    assert(!g_conf->mds_debug_scatterstat ||
           (num_head_items == (fnode.fragstat.nfiles + fnode.fragstat.nsubdirs)));
  } else {
    dout(20) << "get_num_head_items() = " << num_head_items
             << "; fnode.fragstat.nfiles=" << fnode.fragstat.nfiles
             << " fnode.fragstat.nsubdirs=" << fnode.fragstat.nsubdirs << dendl;
  }

  // rstat
  if (sub_info.rbytes != fnode.rstat.rbytes ||
      sub_info.rfiles != fnode.rstat.rfiles ||
      sub_info.rsubdirs != fnode.rstat.rsubdirs) {
    dout(1) << "mismatch between child accounted_rstats and my rstats!" << dendl;
    dout(1) << "total of child dentrys: " << sub_info << dendl;
    dout(1) << "my rstats:              " << fnode.rstat << dendl;
  } else {
    dout(25) << "total of child dentrys: " << sub_info << dendl;
    dout(25) << "my rstats:              " << fnode.rstat << dendl;
  }

  assert(!g_conf->mds_debug_scatterstat || sub_info.rbytes == fnode.rstat.rbytes);
  assert(!g_conf->mds_debug_scatterstat || sub_info.rfiles == fnode.rstat.rfiles);
  assert(!g_conf->mds_debug_scatterstat || sub_info.rsubdirs == fnode.rstat.rsubdirs);
  dout(10) << "check_rstats complete on " << *this << dendl;
  return true;
}

