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

#include "CDirStripe.h"
#include "CDirFrag.h"
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


boost::pool<> CDirStripe::pool(sizeof(CDirStripe));

LockType CDirStripe::dirfragtreelock_type(CEPH_LOCK_SDFT);
LockType CDirStripe::linklock_type(CEPH_LOCK_SLINK);
LockType CDirStripe::nestlock_type(CEPH_LOCK_SNEST);

ostream& CDirStripe::print_db_line_prefix(ostream& out)
{
  return out << ceph_clock_now(g_ceph_context) << " mds." << mdcache->mds->get_nodeid() << ".cache.stripe(" << dirstripe() << ") ";
}

void CDirStripe::print(ostream& out)
{
  string path;
  get_inode()->make_path_string_projected(path);
  out << "[stripe " << dirstripe() << " " << path << "/";

  if (is_auth()) {
    out << " auth";
    if (is_replicated()) 
      out << get_replicas();
  } else {
    pair<int,int> a = authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << get_replica_nonce();
  }

  out << " v" << get_version();
  if (get_projected_version() > get_version())
    out << " pv" << get_projected_version();

  if (is_auth_pinned())
    out << " ap=" << get_num_auth_pins();

  if (!get_fragtree().empty())
    out << " " << get_fragtree();
  if (!dirfragtreelock.is_sync_and_unlocked())
    out << " " << dirfragtreelock;
  if (!linklock.is_sync_and_unlocked())
    out << " " << linklock;
  if (!nestlock.is_sync_and_unlocked())
    out << " " << nestlock;

  // fragstat
  out << " " << fnode.fragstat;
  if (fnode.fragstat.version != fnode.accounted_fragstat.version)
    out << "/" << fnode.accounted_fragstat;
  if (g_conf->mds_debug_scatterstat && is_projected()) {
    fnode_t *pf = get_projected_fnode();
    out << "->" << pf->fragstat;
    if (!(pf->fragstat == pf->accounted_fragstat))
      out << "/" << pf->accounted_fragstat;
  }

  // rstat
  out << " " << fnode.rstat;
  if (fnode.rstat.version != fnode.accounted_rstat.version)
    out << "/" << fnode.accounted_rstat;
  if (g_conf->mds_debug_scatterstat && is_projected())
  {
    fnode_t *pf = get_projected_fnode();
    out << "->" << pf->rstat;
    if (!(pf->rstat == pf->accounted_rstat))
      out << "/" << pf->accounted_rstat;
  }

  CapObject::print(out);

  if (state_test(CDirStripe::STATE_OPEN)) out << " OPEN";
  if (state_test(CDirStripe::STATE_DIRTY)) out << " DIRTY";
  if (state_test(CDirStripe::STATE_FREEZING)) out << " FREEZING";
  if (state_test(CDirStripe::STATE_FROZEN)) out << " FROZEN";
  if (state_test(CDirStripe::STATE_COMMITTING)) out << " COMMITTING";
  if (state_test(CDirStripe::STATE_UNLINKED)) out << " UNLINKED";
  if (state_test(CDirStripe::STATE_PURGING)) out << " PURGING";

  if (is_rep()) out << " REP";

  if (get_num_ref()) {
    out << " |";
    print_pin_set(out);
  }

  out << " " << this;
  out << "]";
}

ostream& operator<<(ostream& out, CDirStripe &s)
{
  s.print(out);
  return out;
}


CDirStripe::CDirStripe(CDirPlacement *placement, stripeid_t stripeid, int auth)
  : CapObject(placement->mdcache, placement->get_inode()->first,
              placement->get_inode()->last),
    placement(placement),
    ds(placement->ino(), stripeid),
    stripe_auth(auth, CDIR_AUTH_UNKNOWN),
    auth_pins(0),
    replicate(false),
    committing_version(0),
    committed_version(0),
    item_dirty(this),
    item_new(this),
    item_stray(this),
    item_dirty_rstat(this),
    stickydir_ref(0),
    dirfragtreelock(this, &dirfragtreelock_type),
    linklock(this, &linklock_type),
    nestlock(this, &nestlock_type)
{
  memset(&fnode, 0, sizeof(fnode));
  if (auth == mdcache->mds->get_nodeid())
    state_set(STATE_AUTH);
}

CInode* CDirStripe::get_inode()
{
  return placement->get_inode();
}

unsigned CDirStripe::get_num_head_items()
{
  unsigned count = 0;
  for (map<frag_t, CDirFrag*>::iterator i = dirfrags.begin(); i != dirfrags.end(); ++i)
    count += i->second->get_num_head_items();
  return count;
}

unsigned CDirStripe::get_num_any()
{
  unsigned count = 0;
  for (map<frag_t, CDirFrag*>::iterator i = dirfrags.begin(); i != dirfrags.end(); ++i)
    count += i->second->get_num_any();
  return count;
}

// fragstat/rstat
fnode_t *CDirStripe::project_fnode()
{
  fnode_t *p = new fnode_t;
  *p = *get_projected_fnode();
  projected_fnode.push_back(p);
  p->version = get_projected_version();
  dout(10) << "project_fnode " << p << dendl;
  return p;
}

void CDirStripe::pop_and_dirty_projected_fnode(LogSegment *ls)
{
  assert(!projected_fnode.empty());
  dout(15) << "pop_and_dirty_projected_fnode " << projected_fnode.front()
      << " v" << projected_fnode.front()->version << dendl;
  fnode = *projected_fnode.front();
  delete projected_fnode.front();
  projected_fnode.pop_front();

  _mark_dirty(ls);
}


// dirfrags

frag_t CDirStripe::pick_dirfrag(__u32 dnhash)
{
  return dirfragtree[dnhash];
}

frag_t CDirStripe::pick_dirfrag(const string& dn)
{
  if (dirfragtree.empty())
    return frag_t();          // avoid the string hash if we can.

  __u32 h = get_placement()->hash_dentry_name(dn);
  return dirfragtree[h];
}

bool CDirStripe::get_dirfrags_under(frag_t fg, list<CDirFrag*>& ls)
{
  bool all = true;
  for (map<frag_t,CDirFrag*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (fg.contains(p->first))
      ls.push_back(p->second);
    else
      all = false;
  }
  return all;
}

void CDirStripe::verify_dirfrags() const
{
  bool bad = false;
  for (map<frag_t,CDirFrag*>::const_iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
    if (!dirfragtree.is_leaf(p->first)) {
      dout(0) << "have open dirfrag " << p->first << " but not leaf in " << dirfragtree
	      << ": " << *p->second << dendl;
      bad = true;
    }
  }
  assert(!bad);
}

void CDirStripe::force_dirfrags()
{
  bool bad = false;
  for (map<frag_t,CDirFrag*>::iterator p = dirfrags.begin(); p != dirfrags.end(); ++p) {
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

CDirFrag *CDirStripe::get_approx_dirfrag(frag_t fg)
{
  CDirFrag *dir = get_dirfrag(fg);
  if (dir) return dir;

  // find a child?
  list<CDirFrag*> ls;
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

void CDirStripe::get_dirfrags(list<CDirFrag*>& ls) 
{
  // all dirfrags
  for (map<frag_t,CDirFrag*>::iterator p = dirfrags.begin();
       p != dirfrags.end();
       ++p)
    ls.push_back(p->second);
}

CDirFrag *CDirStripe::get_or_open_dirfrag(frag_t fg)
{
  // have it?
  CDirFrag *dir = get_dirfrag(fg);
  if (!dir) // create it.
    dir = add_dirfrag(new CDirFrag(this, fg, mdcache, is_auth()));
  return dir;
}

CDirFrag *CDirStripe::add_dirfrag(CDirFrag *dir)
{
  assert(dirfrags.count(dir->get_frag()) == 0);
  dirfrags[dir->get_frag()] = dir;

  if (stickydir_ref > 0) {
    dir->state_set(CDirFrag::STATE_STICKY);
    dir->get(CDirFrag::PIN_STICKY);
  }

  return dir;
}

void CDirStripe::close_dirfrag(frag_t fg)
{
  dout(14) << "close_dirfrag " << fg << dendl;
  assert(dirfrags.count(fg));
  
  CDirFrag *dir = dirfrags[fg];
  dir->remove_null_dentries();
  
  // clear dirty flag
  if (dir->is_dirty())
    dir->mark_clean();
  
  if (stickydir_ref > 0) {
    dir->state_clear(CDirFrag::STATE_STICKY);
    dir->put(CDirFrag::PIN_STICKY);
  }
  
  // dump any remaining dentries, for debugging purposes
  for (CDirFrag::map_t::iterator p = dir->items.begin();
       p != dir->items.end();
       ++p) 
    dout(14) << "close_dirfrag LEFTOVER dn " << *p->second << dendl;

  assert(dir->get_num_ref() == 0);
  delete dir;
  dirfrags.erase(fg);
}

void CDirStripe::close_dirfrags()
{
  while (!dirfrags.empty()) 
    close_dirfrag(dirfrags.begin()->first);
}


void CDirStripe::get_stickydirs()
{
  if (stickydir_ref == 0) {
    get(PIN_STICKYDIRS);
    for (map<frag_t,CDirFrag*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_set(CDirFrag::STATE_STICKY);
      p->second->get(CDirFrag::PIN_STICKY);
    }
  }
  stickydir_ref++;
}

void CDirStripe::put_stickydirs()
{
  assert(stickydir_ref > 0);
  stickydir_ref--;
  if (stickydir_ref == 0) {
    put(PIN_STICKYDIRS);
    for (map<frag_t,CDirFrag*>::iterator p = dirfrags.begin();
	 p != dirfrags.end();
	 ++p) {
      p->second->state_clear(CDirFrag::STATE_STICKY);
      p->second->put(CDirFrag::PIN_STICKY);
    }
  }
}


// locks

void CDirStripe::set_object_info(MDSCacheObjectInfo &info)
{
  info.ino = ds.ino;
  info.dirfrag.stripe = ds;
}

void CDirStripe::encode_lock_state(int type, bufferlist& bl)
{
  if (!is_auth())
   return;

  switch (type) {
  case CEPH_LOCK_SDFT:
    ::encode(dirfragtree, bl);
    break;
  case CEPH_LOCK_SLINK:
    ::encode(fnode.fragstat, bl);
    break;
  case CEPH_LOCK_SNEST:
    ::encode(fnode.rstat, bl);
    break;
  }
}

void CDirStripe::decode_lock_state(int type, bufferlist& bl)
{
  if (is_auth())
    return;

  bufferlist::iterator p = bl.begin();
  switch (type) {
  case CEPH_LOCK_SDFT:
    ::decode(dirfragtree, p);
    break;
  case CEPH_LOCK_SLINK:
    ::decode(fnode.fragstat, p);
    break;
  case CEPH_LOCK_SNEST:
    ::decode(fnode.rstat, p);
    break;
  }
}


// caps
int CDirStripe::get_caps_liked()
{
  return CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;
}

int CDirStripe::get_caps_allowed_ever()
{
  return (CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED) &
      CapObject::get_caps_allowed_ever();
}

int CDirStripe::encode_stripestat(bufferlist &bl, Session *session,
                                  SnapRealm *dir_realm, snapid_t snapid,
                                  unsigned max_bytes)
{
  if (max_bytes && sizeof(ceph_mds_reply_stripe) > max_bytes)
    return -ENOSPC;

  struct ceph_mds_reply_stripe e;
  e.ino = ds.ino;
  e.stripeid = ds.stripeid;
  e.snapid = snapid;

  // "fake" a version that is old (stable) version, +1 if projected.
  e.version = (fnode.version * 2) + is_projected();

  client_t client = session->get_client();
  bool plink = linklock.is_xlocked_by_client(client) || get_loner() == client;
  fnode_t *f = plink ? get_projected_fnode() : &fnode;

  e.nfiles = f->fragstat.nfiles;
  e.nsubdirs = f->fragstat.nsubdirs;
  f->fragstat.mtime.encode_timeval(&e.mtime);

  e.rbytes = f->rstat.rbytes;
  e.rfiles = f->rstat.rfiles;
  e.rsubdirs = f->rstat.rsubdirs;
  f->rstat.rctime.encode_timeval(&e.rctime);

  // caps
  if (snapid != CEPH_NOSNAP) {
    e.cap.caps = is_auth() ? get_caps_allowed_by_type(CAP_ANY) : CEPH_CAP_PIN;
    if (last == CEPH_NOSNAP || is_any_caps())
      e.cap.caps = e.cap.caps & get_caps_allowed_for_client(client);
    e.cap.seq = 0;
    e.cap.mseq = 0;
    e.cap.realm = 0;
  } else {
    Capability *cap = get_client_cap(client);
    if (!cap) {
      cap = add_client_cap(client, session, containing_realm);
      if (is_auth()) {
        if (choose_ideal_loner() >= 0)
          try_set_loner();
        else if (get_wanted_loner() < 0)
          try_drop_loner();
      }
    }

    int likes = get_caps_liked();
    int allowed = get_caps_allowed_for_client(client);
    int issue = (cap->wanted() | likes) & allowed;
    cap->issue_norevoke(issue);
    issue = cap->pending();
    cap->set_last_issue();
    cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
    e.cap.caps = issue;
    e.cap.wanted = cap->wanted();
    e.cap.cap_id = cap->get_cap_id();
    e.cap.seq = cap->get_last_seq();
    dout(10) << "encode_stripestat issueing " << ccap_string(issue) << " seq " << cap->get_last_seq() << dendl;
    e.cap.mseq = cap->get_mseq();
    e.cap.realm = MDS_INO_ROOT;
  }
  e.cap.flags = is_auth() ? CEPH_CAP_FLAG_AUTH:0;
  dout(10) << "encode_stripestat caps " << ccap_string(e.cap.caps)
      << " seq " << e.cap.seq << " mseq " << e.cap.mseq << dendl;
  return 1;
}

void CDirStripe::encode_cap_message(MClientCaps *m, Capability *cap)
{
  m->head.ino = ds.ino;
  m->head.stripeid = ds.stripeid;

  client_t client = cap->get_client();
  bool plink = linklock.is_xlocked_by_client(client) ||
      (cap->issued() & CEPH_CAP_LINK_EXCL);
  fnode_t *f = plink ? get_projected_fnode() : &fnode;

  m->stripe.nfiles = f->fragstat.nfiles;
  m->stripe.nsubdirs = f->fragstat.nsubdirs;
  f->fragstat.mtime.encode_timeval(&m->stripe.mtime);

  m->stripe.rbytes = f->rstat.rbytes;
  m->stripe.rfiles = f->rstat.rfiles;
  m->stripe.rsubdirs = f->rstat.rsubdirs;
  f->rstat.rctime.encode_timeval(&m->stripe.rctime);
}

// pins

void CDirStripe::first_get()
{
  placement->get(CDirPlacement::PIN_STRIPE);
}

void CDirStripe::last_put()
{
  placement->put(CDirPlacement::PIN_STRIPE);
}


void CDirStripe::mark_dirty(LogSegment *ls)
{
  fnode.version++;
  dout(10) << "mark_dirty " << *this << dendl;
  _mark_dirty(ls);
}

void CDirStripe::_mark_dirty(LogSegment *ls)
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
      mark_new(ls);
  }
}

void CDirStripe::mark_clean()
{
  dout(10) << "mark_clean " << *this << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);

    item_dirty.remove_myself();
    item_new.remove_myself();
  }
}

void CDirStripe::mark_new(LogSegment *ls)
{
  dout(10) << "mark_new " << *this << dendl;

  ls->new_stripes.push_back(&item_new);
}

void CDirStripe::clear_dirty_parent_stats()
{
  if (state_test(CDirStripe::STATE_DIRTYFRAGSTAT)) {
    state_clear(CDirStripe::STATE_DIRTYFRAGSTAT);
    put(CDirStripe::PIN_DIRTYFRAGSTAT);
  }
  if (state_test(CDirStripe::STATE_DIRTYRSTAT)) {
    state_clear(CDirStripe::STATE_DIRTYRSTAT);
    put(CDirStripe::PIN_DIRTYRSTAT);
  }
  item_dirty_rstat.remove_myself();
}


// --------------
// stripe storage

struct C_Stripe_Committed : public Context {
  CDirStripe *stripe;
  Context *fin;
  C_Stripe_Committed(CDirStripe *stripe, Context *fin)
      : stripe(stripe), fin(fin) {}
  void finish(int r) {
    stripe->_committed();
    fin->complete(r);
  }
};

void CDirStripe::commit(Context *fin)
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

void CDirStripe::_committed()
{
  assert(state_test(STATE_COMMITTING));
  state_clear(STATE_COMMITTING);
  committed_version = committing_version;
  mark_clean();
  dout(10) << "_committed " << *this << dendl;
}

struct C_Stripe_Fetched : public Context {
  CDirStripe *stripe;
  bufferlist bl;
  C_Stripe_Fetched(CDirStripe *stripe) : stripe(stripe) {}
  void finish(int r) {
    stripe->_fetched(bl);
  }
};

void CDirStripe::fetch(Context *fin)
{
  assert(is_auth());
  if (fin)
    fetch_waiters.push_back(fin);

  if (state_test(STATE_FETCHING)) {
    dout(10) << "already fetching " << *this << dendl;
    return;
  }
  state_set(STATE_FETCHING);

  dout(10) << "fetching " << *this << dendl;

  object_t oid = get_ondisk_object();
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  C_Stripe_Fetched *c = new C_Stripe_Fetched(this);
  mdcache->mds->objecter->read(oid, oloc, 0, 0, CEPH_NOSNAP, &c->bl, 0, c);
}

int CDirStripe::_fetched(bufferlist& bl)
{
  state_clear(STATE_FETCHING);
  mdcache->mds->queue_waiters(fetch_waiters);

  if (bl.length() == 0) {
    LogSegment *ls = mdcache->mds->mdlog->get_current_segment();
    ls->new_stripes.push_back(&item_new);
    state_set(STATE_OPEN);
    dout(10) << "stripe not found, marking new/open " << *this << dendl;
    return 0;
  }

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
    committing_version = committed_version = fnode.version;
  state_set(STATE_OPEN);
  dout(10) << "_fetched " << *this << dendl;
  return 0;
}


// replication

void CDirStripe::encode_export(bufferlist& bl)
{
  assert(!is_projected());
  ::encode(first, bl);
  ::encode(fnode, bl);
  ::encode(committed_version, bl);

  ::encode(state, bl);
  ::encode(replicate, bl);

  ::encode(replica_map, bl);

  get(PIN_TEMPEXPORTING);
}

void CDirStripe::decode_import(bufferlist::iterator& blp, utime_t now)
{
  ::decode(first, blp);
  ::decode(fnode, blp);
  ::decode(committed_version, blp);
  committing_version = committed_version;

  unsigned s;
  ::decode(s, blp);
  state &= MASK_STATE_IMPORT_KEPT;
  state |= (s & MASK_STATE_EXPORTED);
  if (is_dirty()) get(PIN_DIRTY);

  ::decode(replicate, blp);

  ::decode(replica_map, blp);
  if (!replica_map.empty()) get(PIN_REPLICATED);

  replica_nonce = 0;  // no longer defined
}

void CDirStripe::finish_export(utime_t now)
{
  put(PIN_TEMPEXPORTING);
}

void CDirStripe::abort_export()
{
  put(PIN_TEMPEXPORTING);
}


// freezing

bool CDirStripe::freeze()
{
  auth_pin(this); // auth pin while freezing
  if (auth_pins > 1) {
    state_set(STATE_FREEZING);
    dout(10) << "freeze waiting " << *this << dendl;
    return false;
  }

  _freeze();
  auth_unpin(this);
  return true;
}

void CDirStripe::_freeze()
{
  state_clear(STATE_FREEZING);
  state_set(STATE_FROZEN);
  get(PIN_FROZEN);

  dout(10) << "stripe frozen " << *this << dendl;
}

void CDirStripe::unfreeze()
{
  if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);

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

void CDirStripe::maybe_finish_freeze()
{
  if (is_freezing() && auth_pins == 1) {
    _freeze();
    auth_unpin(this);
    finish_waiting(WAIT_FROZEN);
  }
}

void CDirStripe::add_waiter(uint64_t tag, Context *c)
{
  dout(10) << "add_waiter tag " << std::hex << tag << std::dec << " " << c
	   << " !ambig " << !is_ambiguous_stripe_auth()
	   << " !frozen " << !is_frozen()
	   << " !freezing " << !is_freezing()
	   << dendl;
  // wait on the stripe?
  //  make sure its not the stripe that is explicitly ambiguous|freezing|frozen
  if (((tag & WAIT_SINGLEAUTH) && !is_ambiguous_stripe_auth()) ||
      ((tag & WAIT_UNFREEZE) && !is_freezing() && !is_frozen())) {
    dout(15) << "passing waiter up tree" << dendl;
    placement->add_waiter(tag, c);
    return;
  }
  dout(15) << "taking waiter here" << dendl;
  MDSCacheObject::add_waiter(tag, c);
}

// auth_pins

void CDirStripe::auth_pin(void *by)
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this
	   << " now " << auth_pins << dendl;
}

void CDirStripe::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif

  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(10) << "auth_unpin by " << by << " on " << *this
	   << " now " << auth_pins << dendl;

  assert(auth_pins >= 0);

  maybe_finish_freeze();
}


void CDirStripe::set_stripe_auth(const pair<int, int> &a)
{
  dout(10) << "setting stripe_auth=" << a
      << " from " << stripe_auth << " on " << *this << dendl;

  bool was_ambiguous = is_ambiguous_stripe_auth();

  // set it.
  stripe_auth = a;

  // newly single auth?
  if (was_ambiguous && !is_ambiguous_stripe_auth())
    finish_waiting(WAIT_SINGLEAUTH, 0);
}

