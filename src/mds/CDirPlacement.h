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



#ifndef CEPH_CDIRPLACEMENT_H
#define CEPH_CDIRPLACEMENT_H

#include "include/types.h"
#include "mdstypes.h"
#include "common/config.h"

#include "SimpleLock.h"

#include <list>
#include <map>
#include <vector>
using std::list;
using std::map;
using std::vector;

class CInode;
class CDirStripe;
class MDCache;

ostream& operator<<(ostream& out, class CDir& dir);
class CDirPlacement : public MDSCacheObject {
  /*
   * This class uses a boost::pool to handle allocation. This is *not*
   * thread-safe, so don't do allocations from multiple threads!
   *
   * Alternatively, switch the pool to use a boost::singleton_pool.
   */
 private:
  static boost::pool<> pool;
 public:
  static void *operator new(size_t num_bytes) { 
    void *n = pool.malloc();
    if (!n)
      throw std::bad_alloc();
    return n;
  }
  void operator delete(void *p) {
    pool.free(p);
  }

 public:
  // -- pins --
  static const int PIN_INODE =          1;
  static const int PIN_STRIPE =        -2;
  static const int PIN_STRIPEWAITER =   3;
  const char *pin_name(int p) {
    switch (p) {
      case PIN_INODE: return "inode";
      case PIN_STRIPE: return "stripe";
      case PIN_STRIPEWAITER: return "stripewaiter";
      default: return generic_pin_name(p);
    }
  }

  // -- state --
  static const unsigned STATE_FREEZING = (1<<0);
  static const unsigned STATE_FROZEN =   (1<<1);

  // -- wait masks --
  static const uint64_t WAIT_STRIPE =    (1<<0); // wait for item to be in cache
  static const uint64_t WAIT_FROZEN =    (1<<1); // auth pins removed

 public:
  MDCache *mdcache;

  CDirPlacement(MDCache *mdcache, inodeno_t ino, int inode_auth);
  virtual ~CDirPlacement() {}

  bool is_lt(const MDSCacheObject *r) const {
    return get_ino() < ((const CDirPlacement*)r)->get_ino();
  }

  bool is_freezing() { return state_test(STATE_FREEZING); }
  bool is_frozen() { return state_test(STATE_FROZEN); }

 private:
  inodeno_t ino;
  pair<int,int> inode_auth;
  version_t version;

 public:
  inodeno_t get_ino() const { return ino; }
  void set_inode_auth(int auth) { inode_auth.first = auth; }
  void set_inode_auth(const pair<int,int> &auth) { inode_auth = auth; }
  pair<int,int> authority() { return inode_auth; }
  void set_version(version_t v) { version = v; }
  version_t get_version() const { return version; }

  // stripe authority
 private:
  vector<int> stripe_auth; // auth mds for each stripe

 public:
  size_t get_stripe_count() const { return stripe_auth.size(); }

  const vector<int>& get_stripe_auth() const { return stripe_auth; }
  int get_stripe_auth(stripeid_t stripeid) const {
    assert(stripeid < stripe_auth.size());
    return stripe_auth[stripeid];
  }

  void set_stripe_auth(const vector<int> &auth) {
    // don't erase any open stripes
    assert(stripes.lower_bound(auth.size()) == stripes.end());
    stripe_auth = auth;
  }
  void set_stripe_auth(stripeid_t stripeid, int auth) {
    assert(stripeid < stripe_auth.size());
    stripe_auth[stripeid] = auth;
  }

  // stripe placement
 private:
  ceph_dir_layout layout;

 public:
  const ceph_dir_layout& get_layout() const { return layout; }
  void set_layout(const ceph_dir_layout &lo) { layout = lo; }

  __u32 hash_dentry_name(const string &dn);
  stripeid_t pick_stripe(__u32 hash);
  stripeid_t pick_stripe(const string &dn);


  // stripe object cache
 private:
  typedef map<stripeid_t, CDirStripe*> stripe_map;
  stripe_map stripes;

 public:
  void get_stripes(list<CDirStripe*> &stripes);

  CDirStripe* get_stripe(stripeid_t stripeid);
  CDirStripe* get_or_open_stripe(stripeid_t stripeid);
  CDirStripe* add_stripe(CDirStripe *stripe);
  void close_stripe(CDirStripe *stripe);
  void close_stripes();


  // permissions
 private:
  unsigned mode; // inode mode
  unsigned gid; // gid for S_ISGID

 public:
  void set_mode(unsigned m) { mode = m; }
  unsigned get_mode() const { return mode; }
  bool is_sticky_gid() const { return mode & S_ISGID; }

  void set_gid(unsigned g) { gid = g; }
  unsigned get_gid() const { return gid; }


  // ref counting
 private:
  int auth_pins;
#ifdef MDS_AUTHPIN_SET
  multiset<void*> auth_pin_set;
#endif

 public:
  bool can_auth_pin() { return is_auth() && !is_freezing_or_frozen(); }
  void auth_pin(void *who);
  void auth_unpin(void *who);


  // waiters
 private:
  typedef map<stripeid_t, list<Context*> > stripe_waiter_map;
  stripe_waiter_map waiting_on_stripe;

 public:
  bool is_waiting_for_stripe(stripeid_t stripeid) {
    return waiting_on_stripe.count(stripeid);
  }
  void add_stripe_waiter(stripeid_t stripeid, Context *c);
  void take_stripe_waiting(stripeid_t stripeid, list<Context*>& ls);


  // locks
 private:
  static LockType authlock_type;
  SimpleLock authlock; // protects mode, gid

 public:
  SimpleLock* get_lock(int type) {
    assert(type == CEPH_LOCK_DAUTH);
    return &authlock;
  }
  void encode_lock_state(int type, bufferlist& bl);
  void decode_lock_state(int type, bufferlist& bl);


  // replication
  void encode_replica(int who, bufferlist& bl) {
    __u32 nonce = add_replica(who);
    ::encode(nonce, bl);
    ::encode(stripe_auth, bl);
    ::encode(version, bl);
    ::encode(mode, bl);
    ::encode(gid, bl);
    authlock.encode_state_for_replica(bl);
  }
  void decode_replica(bufferlist::iterator& p, bool isnew) {
    __u32 nonce;
    ::decode(nonce, p);
    replica_nonce = nonce;
    ::decode(stripe_auth, p);
    ::decode(version, p);
    ::decode(mode, p);
    ::decode(gid, p);
    authlock.decode_state(p, isnew);
  }


  ostream& print_db_line_prefix(ostream& out);
  void print(ostream& out);
};

#endif
