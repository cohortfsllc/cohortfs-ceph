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

#ifndef CEPH_CSTRIPE_H
#define CEPH_CSTRIPE_H

#include "include/types.h"
#include "include/buffer.h"
#include "mdstypes.h"
#include "common/config.h"

#include <iostream>

#include <map>

#include "CInode.h"


class CDentry;
class CDir;
class Context;
class MDCache;

ostream& operator<<(ostream& out, class CStripe& stripe);
class CStripe : public MDSCacheObject {
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
  static const int PIN_DIRFRAG            = -1;
  static const int PIN_STICKYDIRS         = 2;
  static const int PIN_FROZEN             = 3;

  const char *pin_name(int p) {
    switch (p) {
      case PIN_DIRFRAG: return "dirfrag";
      case PIN_STICKYDIRS: return "stickydirs";
      case PIN_FROZEN: return "frozen";
      default: return generic_pin_name(p);
    }
  }

  // -- state --
  static const unsigned STATE_OPEN          = (1<<0); // has been loaded from disk
  static const unsigned STATE_FROZEN        = (1<<1);
  static const unsigned STATE_FREEZING      = (1<<2);
  static const unsigned STATE_ASSIMRSTAT    = (1<<3); // assimilating inode->stripe rstats
  static const unsigned STATE_COMMITTING    = (1<<4);

  // these state bits are preserved by an import/export
  static const unsigned MASK_STATE_EXPORTED =
      (STATE_OPEN
       |STATE_DIRTY);
  static const unsigned MASK_STATE_IMPORT_KEPT =
      (STATE_OPEN
       |STATE_FROZEN);
  static const unsigned MASK_STATE_EXPORT_KEPT =
      (STATE_FROZEN);

  // -- wait masks --
  static const uint64_t WAIT_DIR          = (1<<0);  // wait for item to be in cache
  static const uint64_t WAIT_FROZEN       = (1<<1);  // auth pins removed

  static const uint64_t WAIT_ANY_MASK  = (0xffffffff);

  static const int EXPORT_NONCE = 1;

 private:
  MDCache *mdcache;

  CInode *inode; // my inode
  stripeid_t stripeid; // stripe index

  pair<int,int> stripe_auth;

  int auth_pins;

  // cache control  (defined for authority; hints for replicas)
  bool replicate; // was CDir::dir_rep

  friend class MDBalancer;
  friend class MDCache;
  friend class Migrator;

 public:
  CStripe(CInode *in, stripeid_t stripe, int auth);

  bool is_lt(const MDSCacheObject *r) const {
    return dirstripe() < ((const CStripe*)r)->dirstripe();
  }


  // -- accessors --
  CInode* get_inode() { return inode; }
  dirstripe_t dirstripe() const { return dirstripe_t(inode->ino(), stripeid); }
  stripeid_t get_stripeid() const { return stripeid; }

  CStripe* get_parent_stripe();
  CStripe* get_projected_parent_stripe();

  bool contains(CStripe *stripe);

  void first_get();
  void last_put();

  unsigned get_num_head_items();
  unsigned get_num_any();

  // -- fragstat/rstat --
 public:
  fnode_t fnode;
  snapid_t first;
  map<snapid_t,old_rstat_t> dirty_old_rstat;  // [value.first,key]

 private:
  version_t projected_version, committing_version, committed_version;
  list<fnode_t*> projected_fnode;

 public:
  elist<CInode*> dirty_rstat_inodes;
  elist<CStripe*>::item item_dirty, item_new;

  version_t get_version() { return fnode.version; }
  void set_version(version_t v) {
    assert(projected_fnode.empty());
    projected_version = fnode.version = v;
  }
  version_t get_projected_version() { return projected_version; }

  fnode_t *get_projected_fnode() {
    if (projected_fnode.empty())
      return &fnode;
    else
      return projected_fnode.back();
  }
  fnode_t *project_fnode();

  void pop_and_dirty_projected_fnode(LogSegment *ls);
  bool is_projected() { return !projected_fnode.empty(); }

  bool is_rstat_accounted() const {
    return fnode.rstat == fnode.accounted_rstat;
  }
  bool is_fragstat_accounted() const {
    return fnode.fragstat == fnode.accounted_fragstat;
  }

  void resync_accounted_fragstat();
  void resync_accounted_rstat();
  void assimilate_dirty_rstat_inodes();
  void assimilate_dirty_rstat_inodes_finish(Mutation *mut, EMetaBlob *blob);
  bool check_rstats();
  void verify_fragstat();

  version_t pre_dirty(version_t min=0);
  void mark_dirty(version_t pv, LogSegment *ls);
  void _mark_dirty(LogSegment *ls);
  void log_mark_dirty();
  void mark_clean();

  void mark_new(LogSegment *ls);

  // -- dirfrags --
 private:
  fragtree_t dirfragtree;
  map<frag_t, CDir*> dirfrags; // cached dir fragments under this stripe
  int stickydir_ref;

 public:
  void set_fragtree(const fragtree_t &dft) { dirfragtree = dft; }

  fragtree_t& get_fragtree() { return dirfragtree; }
  const fragtree_t& get_fragtree() const { return dirfragtree; }

  bool has_dirfrags() const { return !dirfrags.empty(); }

  frag_t pick_dirfrag(__u32 hash);
  frag_t pick_dirfrag(const string &dn);

  CDir* get_dirfrag(frag_t fg) {
    if (dirfrags.count(fg)) {
      //assert(g_conf->debug_mds < 2 || dirfragtree.is_leaf(fg)); // performance hack FIXME
      return dirfrags[fg];
    } else
      return 0;
  }
  bool get_dirfrags_under(frag_t fg, list<CDir*>& ls);
  CDir* get_approx_dirfrag(frag_t fg);
  void get_dirfrags(list<CDir*>& ls);

  CDir *get_or_open_dirfrag(frag_t fg);
  CDir *add_dirfrag(CDir *dir);
  void close_dirfrag(frag_t fg);
  void close_dirfrags();

  void force_dirfrags();
  void verify_dirfrags() const;

  void get_stickydirs();
  void put_stickydirs();

  // -- authority --
  pair<int,int> authority() { return stripe_auth; }
  pair<int,int> get_stripe_auth() { return stripe_auth; }
  void set_stripe_auth(const pair<int,int> &a);
  void set_stripe_auth(int a) {
    set_stripe_auth(make_pair(a, CDIR_AUTH_UNKNOWN));
  }
  bool is_ambiguous_stripe_auth() {
    return stripe_auth.second != CDIR_AUTH_UNKNOWN;
  }
  bool is_full_stripe_auth() {
    return is_auth() && !is_ambiguous_stripe_auth();
  }
  bool is_full_stripe_nonauth() {
    return !is_auth() && !is_ambiguous_stripe_auth();
  }

  // -- locks --
  static LockType dirfragtreelock_type;
  static LockType linklock_type;
  static LockType nestlock_type;

  SimpleLock dirfragtreelock; // protects dirfragtree
  SimpleLock linklock; // protects fnode.fragstat
  SimpleLock nestlock; // protects fnode.rstat

  SimpleLock* get_lock(int type) {
    switch (type) {
    case CEPH_LOCK_SDFT: return &dirfragtreelock;
    case CEPH_LOCK_SLINK: return &linklock;
    case CEPH_LOCK_SNEST: return &nestlock;
    }
    return 0;
  }

  void set_object_info(MDSCacheObjectInfo &info);
  void encode_lock_state(int type, bufferlist& bl);
  void decode_lock_state(int type, bufferlist& bl);

  // -- auth pins --
 private:
#ifdef MDS_AUTHPIN_SET
    multiset<void*> auth_pin_set;
#endif
 public:
  bool is_auth_pinned() const { return auth_pins; }
  bool can_auth_pin() { return is_auth() && !is_freezing_or_frozen(); }
  void auth_pin(void *by);
  void auth_unpin(void *by);

  int get_num_auth_pins() const { return auth_pins; }

  // -- freezing --
 private:
  void maybe_finish_freeze();
  void _freeze();

 public:
  bool is_freezing() { return state & STATE_FREEZING; }
  bool is_frozen() { return state & STATE_FROZEN; }

  bool freeze();
  void unfreeze();


  // -- waiters --
  void add_waiter(uint64_t mask, Context *c);


  // -- fetch / commit --
 private:
  object_t get_ondisk_object() {
    uint64_t bno = get_stripeid();
    return file_object_t(get_inode()->ino(), bno << 32);
  }

  int _fetched(bufferlist &bl);
  void _committed();

  friend struct C_Stripe_Fetched;
  friend struct C_Stripe_Committed;

  void encode_store(bufferlist& bl) {
    ::encode(dirfragtree, bl);
  }
  void decode_store(bufferlist::iterator& p) {
    ::decode(dirfragtree, p);
  }

 public:
  void fetch(Context *c);
  void commit(Context *c);

  bool is_open() const { return state_test(STATE_OPEN); }
  void mark_open() { state_set(STATE_OPEN); }


  // -- replication --
  bool is_rep() const { return replicate; }
  void encode_replica(int who, bufferlist& bl) {
    __u32 nonce = add_replica(who);
    ::encode(nonce, bl);
    ::encode(dirfragtree, bl);
  }
  void decode_replica(bufferlist::iterator& p) {
    __u32 nonce;
    ::decode(nonce, p);
    replica_nonce = nonce;
    ::decode(dirfragtree, p);
    state_set(STATE_OPEN);
  }


  // -- migration --
  void encode_export(bufferlist& bl);
  void decode_import(bufferlist::iterator& blp, utime_t now);

  void finish_export(utime_t now);
  void abort_export();


  ostream& print_db_line_prefix(ostream& out);
  void print(ostream& out);
};

#endif
