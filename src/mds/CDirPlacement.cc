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

#include "CDirPlacement.h"
#include "CDirStripe.h"
#include "CInode.h"

#include "MDS.h"
#include "MDCache.h"

#include "include/Context.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.dir(" << get_ino() << ") "



boost::pool<> CDirPlacement::pool(sizeof(CDirPlacement));


ostream& operator<<(ostream& out, CDirPlacement& dir)
{
  string path;
  out << "[dir " << dir.get_ino();
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    out << " v=" << dir.get_version();
  } else {
    pair<int,int> a = dir.authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << dir.get_replica_nonce();
  }

  out << " stripes=" << dir.get_stripe_auth();

  out << " state=" << dir.get_state();
  if (dir.state_test(CDirPlacement::STATE_FROZEN)) out << "|frozen";

  if (dir.get_num_ref()) {
    out << " |";
    dir.print_pin_set(out);
  }

  out << " " << &dir;
  return out << "]";
}

void CDirPlacement::print(ostream& out) 
{
  out << *this;
}

ostream& CDirPlacement::print_db_line_prefix(ostream& out) 
{
  return out << ceph_clock_now(g_ceph_context) << " mds." << mdcache->mds->get_nodeid() << ".cache.dir(" << get_ino() << ") ";
}


// constructor

CDirPlacement::CDirPlacement(MDCache *mdcache, inodeno_t ino, int inode_auth,
                             const vector<int> &stripe_auth)
  : mdcache(mdcache),
    ino(ino),
    inode_auth(inode_auth, CDIR_AUTH_UNKNOWN),
    version(0),
    stripe_auth(stripe_auth),
    auth_pins(0)
{
  if (inode_auth == mdcache->mds->get_nodeid())
    state_set(STATE_AUTH);

  memset(&layout, 0, sizeof(layout));
  layout.dl_dir_hash = g_conf->mds_default_dir_hash;
}

__u32 CDirPlacement::hash_dentry_name(const string &dn)
{ 
  int which = layout.dl_dir_hash; 
  if (!which) 
    which = CEPH_STR_HASH_LINUX;
  return ceph_str_hash(which, dn.data(), dn.length());
}

stripeid_t CDirPlacement::pick_stripe(__u32 dnhash)
{
  return stripeid_t(dnhash % stripe_auth.size());
} 

stripeid_t CDirPlacement::pick_stripe(const string &dname)
{
  if (stripe_auth.size() == 1)
    return 0;
  __u32 dnhash = hash_dentry_name(dname);
  return stripeid_t(dnhash % stripe_auth.size());
}

// stripes

void CDirPlacement::get_stripes(list<CDirStripe*> &ls)
{
  for (stripe_map::iterator p = stripes.begin(); p != stripes.end(); ++p)
    ls.push_back(p->second);
}

CDirStripe* CDirPlacement::get_stripe(stripeid_t stripeid)
{
  assert(stripeid < stripe_auth.size());
  stripe_map::iterator i = stripes.find(stripeid);
  return i == stripes.end() ? NULL : i->second;
}

CDirStripe* CDirPlacement::get_or_open_stripe(stripeid_t stripeid)
{
  // have it?
  CDirStripe *stripe = get_stripe(stripeid);
  if (!stripe) {
    // create it.
    int auth = get_stripe_auth(stripeid);
    stripe = new CDirStripe(this, stripeid, auth);
    add_stripe(stripe);
  }
  return stripe;
}

CDirStripe* CDirPlacement::add_stripe(CDirStripe *stripe)
{
  assert(stripes.count(stripe->get_stripeid()) == 0);
  stripes[stripe->get_stripeid()] = stripe;
  return stripe;
}

void CDirPlacement::close_stripe(CDirStripe *stripe)
{
  dout(14) << "close_stripe " << *stripe << dendl;

  stripe_map::iterator s = stripes.find(stripe->get_stripeid());
  assert(s != stripes.end());

  stripe->close_dirfrags();

  // clear dirty flag
  if (stripe->is_dirty())
    stripe->mark_clean();

  stripe->item_stray.remove_myself();
  stripe->item_new.remove_myself();

  assert(stripe->get_num_ref() == 0);
  stripes.erase(s);
  delete stripe;
}

void CDirPlacement::close_stripes()
{
  while (!stripes.empty())
    close_stripe(stripes.begin()->second);
}


// waiters

void CDirPlacement::add_stripe_waiter(stripeid_t stripeid, Context *c)
{
  if (waiting_on_stripe.empty())
    get(PIN_STRIPEWAITER);

  waiting_on_stripe[stripeid].push_back(c);
}

void CDirPlacement::take_stripe_waiting(stripeid_t stripeid, list<Context*>& ls)
{
  stripe_waiter_map::iterator i = waiting_on_stripe.find(stripeid);
  if (i == waiting_on_stripe.end())
    return;

  ls.splice(ls.end(), i->second);
  waiting_on_stripe.erase(i);

  if (waiting_on_stripe.empty())
    put(PIN_STRIPEWAITER);
}


// pins

void CDirPlacement::auth_pin(void *by) 
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

void CDirPlacement::auth_unpin(void *by) 
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
}

