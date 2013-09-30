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

#include "CapObject.h"
#include "Capability.h"
#include "MDCache.h"
#include "MDS.h"
#include "SessionMap.h"
#include "SimpleLock.h"
#include "snap.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.cap "


void CapObject::print(ostream& out)
{
  if (!get_client_caps().empty()) {
    out << " caps={";
    for (map<client_t,Capability*>::iterator it = get_client_caps().begin();
         it != get_client_caps().end();
         it++) {
      if (it != get_client_caps().begin()) out << ",";
      out << it->first << "="
	  << ccap_string(it->second->pending());
      if (it->second->issued() != it->second->pending())
	out << "/" << ccap_string(it->second->issued());
      out << "/" << ccap_string(it->second->wanted())
	  << "@" << it->second->get_last_sent();
    }
    out << "}";
    if (get_loner() >= 0 || get_wanted_loner() >= 0) {
      out << ",l=" << get_loner();
      if (get_loner() != get_wanted_loner())
	out << "(" << get_wanted_loner() << ")";
    }
  }
  if (!get_mds_caps_wanted().empty()) {
    out << " mcw={";
    for (map<int,int>::iterator p = get_mds_caps_wanted().begin();
	 p != get_mds_caps_wanted().end(); ++p) {
      if (p != get_mds_caps_wanted().begin())
	out << ',';
      out << p->first << '=' << ccap_string(p->second);
    }
    out << '}';
  }
}


ostream& operator<<(ostream& out, CapObject& o)
{
  o.print(out);
  return out;
}


CapObject::CapObject(MDCache *mdcache, snapid_t first, snapid_t last)
  : mdcache(mdcache),
    replica_caps_wanted(0),
    first(first),
    last(last),
    containing_realm(NULL),
    cap_update_mask(0),
    loner_cap(-1),
    want_loner_cap(-1)
{
}


client_t CapObject::calc_ideal_loner()
{
  if (!mds_caps_wanted.empty())
    return -1;
  
  int n = 0;
  client_t loner = -1;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       it++) 
    if (!it->second->is_stale() &&
        (it->second->wanted() & (CEPH_CAP_ANY_WR|CEPH_CAP_FILE_WR|CEPH_CAP_FILE_RD))) {
      if (n)
	return -1;
      n++;
      loner = it->first;
    }
  return loner;
}

client_t CapObject::choose_ideal_loner()
{
  want_loner_cap = calc_ideal_loner();
  return want_loner_cap;
}

bool CapObject::try_set_loner()
{
  assert(want_loner_cap >= 0);
  if (loner_cap >= 0 && loner_cap != want_loner_cap)
    return false;
  set_loner_cap(want_loner_cap);
  return true;
}

void CapObject::set_loner_cap(client_t l)
{
  loner_cap = l;
  for (vector<SimpleLock*>::iterator i = cap_locks.begin(); i != cap_locks.end(); ++i)
    (*i)->set_excl_client(loner_cap);
}

bool CapObject::try_drop_loner()
{
  if (loner_cap < 0)
    return true;

  int other_allowed = get_caps_allowed_by_type(CAP_ANY);
  Capability *cap = get_client_cap(loner_cap);
  if (!cap || (cap->issued() & ~other_allowed) == 0) {
    set_loner_cap(-1);
    return true;
  }
  return false;
}

int CapObject::count_nonstale_caps() const
{
  int n = 0;
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       it++)
    if (!it->second->is_stale())
      n++;
  return n;
}

bool CapObject::multiple_nonstale_caps() const
{
  int n = 0;
  for (map<client_t,Capability*>::const_iterator it = client_caps.begin();
       it != client_caps.end();
       it++)
    if (!it->second->is_stale()) {
      if (n)
        return true;
      n++;
    }
  return false;
}

// choose new lock state during recovery, based on issued caps
void CapObject::choose_lock_state(SimpleLock *lock, int allissued)
{
  int shift = lock->get_cap_shift();
  int issued = (allissued >> shift) & lock->get_cap_mask();
  if (is_auth()) {
    if (lock->is_xlocked()) {
      // do nothing here
    } else {
      if (issued & CEPH_CAP_GEXCL)
        lock->set_state(LOCK_EXCL);
      else if (issued & CEPH_CAP_GWR)
	lock->set_state(LOCK_MIX);
      else if (lock->is_dirty()) {
	if (is_replicated())
	  lock->set_state(LOCK_MIX);
	else
	  lock->set_state(LOCK_LOCK);
      } else
	lock->set_state(LOCK_SYNC);
    }
  } else {
    // our states have already been chosen during rejoin.
    if (lock->is_xlocked())
      assert(lock->get_state() == LOCK_LOCK);
  }
}

void CapObject::choose_lock_states()
{
  int issued = get_caps_issued();
  if (is_auth() && (issued & (CEPH_CAP_ANY_EXCL|CEPH_CAP_ANY_WR)) &&
      choose_ideal_loner() >= 0)
    try_set_loner();
  for (vector<SimpleLock*>::iterator i = cap_locks.begin(); i != cap_locks.end(); ++i)
    choose_lock_state(*i, issued);
}

void CapObject::update_cap_lru()
{
  int target = g_conf->mds_cap_update_lru_target;

  // can we free up space in the blacklist?
  while (cap_blacklist.size() && shared_cap_lru.size() < target) {
    Capability *cap = cap_blacklist.front();
    cap_blacklist.remove(&cap->item_parent_lru);
  }

  // limit the number of clients that need callbacks
  if (cap_update_mask && target) {
    // drop no more than half of the caps until we reach the target
    if (target < shared_cap_lru.size() / 2)
      target = shared_cap_lru.size() / 2;
    while (shared_cap_lru.size() > target) {
      // move to blacklist
      Capability *cap = shared_cap_lru.front();
      cap_blacklist.push_back(&cap->item_parent_lru);
    }
  }
}

bool CapObject::is_cap_blacklisted(Capability *cap) const
{
  return cap->item_parent_lru.get_list() == &cap_blacklist;
}

int CapObject::get_client_cap_pending(client_t client) const
{
  map<client_t,Capability*>::const_iterator i = client_caps.find(client);
  return i != client_caps.end() ? i->second->pending() : 0;
}

Capability* CapObject::add_client_cap(client_t client, Session *session, SnapRealm *conrealm)
{
  if (client_caps.empty()) {
    get(PIN_CAPS);
    if (conrealm)
      containing_realm = conrealm;
    else
      containing_realm = mdcache->get_snaprealm();
    dout(10) << "add_client_cap first cap, joining realm " << *containing_realm << dendl;
  }

  mdcache->num_caps++;

  Capability *cap = new Capability(this, ++mdcache->last_cap_id, client);
  assert(client_caps.count(client) == 0);
  client_caps[client] = cap;
  if (session)
    session->add_cap(cap);
  
  cap->client_follows = first-1;
  
  containing_realm->add_cap(client, cap);
  
  return cap;
}

void CapObject::remove_client_cap(client_t client)
{
  assert(client_caps.count(client) == 1);
  Capability *cap = client_caps[client];
  
  cap->item_session_caps.remove_myself();
  containing_realm->remove_cap(client, cap);
  
  if (client == loner_cap)
    loner_cap = -1;

  cap->item_parent_lru.remove_myself();
  delete cap;
  client_caps.erase(client);
  if (client_caps.empty()) {
    dout(10) << "remove_client_cap last cap, leaving realm " << *containing_realm << dendl;
    put(PIN_CAPS);
    containing_realm = NULL;
  }
  mdcache->num_caps--;
}

void CapObject::move_to_realm(SnapRealm *realm)
{
  dout(10) << "move_to_realm joining realm " << *realm
	   << ", leaving realm " << *containing_realm << dendl;
  for (map<client_t,Capability*>::iterator q = client_caps.begin();
       q != client_caps.end();
       q++) {
    containing_realm->remove_cap(q->first, q->second);
    realm->add_cap(q->first, q->second);
  }
  containing_realm = realm;
}

Capability *CapObject::reconnect_cap(client_t client, ceph_mds_cap_reconnect& icr, Session *session)
{
  Capability *cap = get_client_cap(client);
  if (cap) {
    // FIXME?
    cap->merge(icr.wanted, icr.issued);
  } else {
    cap = add_client_cap(client, session);
    cap->set_wanted(icr.wanted);
    cap->issue_norevoke(icr.issued);
    cap->reset_seq();
  }
  cap->set_cap_id(icr.cap_id);
  cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
  return cap;
}

void CapObject::clear_client_caps_after_export()
{
  while (!client_caps.empty())
    remove_client_cap(client_caps.begin()->first);
  loner_cap = -1;
  want_loner_cap = -1;
  mds_caps_wanted.clear();
}

void CapObject::export_client_caps(map<client_t,CapExport>& cl)
{
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       it++) {
    cl[it->first] = it->second->make_export();
  }
}

// caps allowed
int CapObject::get_caps_allowed_ever()
{
  int allowed = CEPH_CAP_PIN;
  for (vector<SimpleLock*>::iterator i = cap_locks.begin(); i != cap_locks.end(); ++i) {
    int shift = (*i)->get_cap_shift();
    if (shift)
      allowed |= (*i)->gcaps_allowed_ever() << shift;
  }
  return allowed;
}

int CapObject::get_caps_allowed_by_type(int type)
{
  int allowed = CEPH_CAP_PIN;
  for (vector<SimpleLock*>::iterator i = cap_locks.begin(); i != cap_locks.end(); ++i) {
    int shift = (*i)->get_cap_shift();
    if (shift)
      allowed |= (*i)->gcaps_allowed(type) << shift;
  }
  return allowed;
}

int CapObject::get_caps_careful()
{
  int careful = 0;
  for (vector<SimpleLock*>::iterator i = cap_locks.begin(); i != cap_locks.end(); ++i) {
    int shift = (*i)->get_cap_shift();
    if (shift)
      careful |= (*i)->gcaps_careful() << shift;
  }
  return careful;
}

int CapObject::get_xlocker_mask(client_t client)
{
  int mask = 0;
  for (vector<SimpleLock*>::iterator i = cap_locks.begin(); i != cap_locks.end(); ++i) {
    int shift = (*i)->get_cap_shift();
    if (shift)
      mask |= (*i)->gcaps_xlocker_mask(client) << shift;
  }
  return mask;
}

int CapObject::get_caps_allowed_for_client(client_t client)
{
  int allowed;
  if (client == get_loner()) {
    // as the loner, we get the loner_caps AND any xlocker_caps for things we have xlocked
    allowed =
      get_caps_allowed_by_type(CAP_LONER) |
      (get_caps_allowed_by_type(CAP_XLOCKER) & get_xlocker_mask(client));
  } else {
    allowed = get_caps_allowed_by_type(CAP_ANY);
  }
  return allowed;
}

// caps issued, wanted
int CapObject::get_caps_issued(int *ploner, int *pother, int *pxlocker,
			    int shift, int mask)
{
  int c = 0;
  int loner = 0, other = 0, xlocker = 0;
  if (!is_auth())
    loner_cap = -1;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       it++) {
    int i = it->second->issued();
    c |= i;
    if (it->first == loner_cap)
      loner |= i;
    else
      other |= i;
    xlocker |= get_xlocker_mask(it->first) & i;
  }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask;
  if (pxlocker) *pxlocker = (xlocker >> shift) & mask;
  return (c >> shift) & mask;
}

bool CapObject::is_any_caps_wanted()
{
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       it++)
    if (it->second->wanted())
      return true;
  return false;
}

int CapObject::get_caps_wanted(int *ploner, int *pother, int shift, int mask)
{
  int w = 0;
  int loner = 0, other = 0;
  for (map<client_t,Capability*>::iterator it = client_caps.begin();
       it != client_caps.end();
       it++) {
    if (!it->second->is_stale()) {
      int t = it->second->wanted();
      w |= t;
      if (it->first == loner_cap)
	loner |= t;
      else
	other |= t;	
    }
    //cout << " get_caps_wanted client " << it->first << " " << cap_string(it->second.wanted()) << endl;
  }
  if (is_auth())
    for (map<int,int>::iterator it = mds_caps_wanted.begin();
	 it != mds_caps_wanted.end();
	 it++) {
      w |= it->second;
      other |= it->second;
      //cout << " get_caps_wanted mds " << it->first << " " << cap_string(it->second) << endl;
    }
  if (ploner) *ploner = (loner >> shift) & mask;
  if (pother) *pother = (other >> shift) & mask;
  return (w >> shift) & mask;
}

bool CapObject::issued_caps_need_gather(SimpleLock *lock)
{
  int loner_issued, other_issued, xlocker_issued;
  get_caps_issued(&loner_issued, &other_issued, &xlocker_issued,
		  lock->get_cap_shift(), lock->get_cap_mask());
  if ((loner_issued & ~lock->gcaps_allowed(CAP_LONER)) ||
      (other_issued & ~lock->gcaps_allowed(CAP_ANY)) ||
      (xlocker_issued & ~lock->gcaps_allowed(CAP_XLOCKER)))
    return true;
  return false;
}

