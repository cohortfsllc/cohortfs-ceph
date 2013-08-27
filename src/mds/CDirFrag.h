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



#ifndef CEPH_CDIRFRAG_H
#define CEPH_CDIRFRAG_H

#include "include/types.h"
#include "include/buffer.h"
#include "mdstypes.h"
#include "common/config.h"
#include "common/DecayCounter.h"

#include <iostream>

#include <list>
#include <set>
#include <map>
#include <string>
using namespace std;


#include "CDirStripe.h"

class CDentry;
class CInode;
class MDCache;
class MDCluster;
class Context;
class bloom_filter;

class ObjectOperation;

ostream& operator<<(ostream& out, class CDirFrag& dir);
class CDirFrag : public MDSCacheObject {
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
  static const int PIN_DNWAITER =     1;
  static const int PIN_INOWAITER =    2;
  static const int PIN_CHILD =        3;
  static const int PIN_FROZEN =       4;
  static const int PIN_STICKY =       5;
  const char *pin_name(int p) {
    switch (p) {
    case PIN_DNWAITER: return "dnwaiter";
    case PIN_INOWAITER: return "inowaiter";
    case PIN_CHILD: return "child";
    case PIN_FROZEN: return "frozen";
    case PIN_STICKY: return "sticky";
    default: return generic_pin_name(p);
    }
  }

  // -- state --
  static const unsigned STATE_COMPLETE =      (1<<0);   // the complete contents are in cache
  static const unsigned STATE_FROZEN =        (1<<1);
  static const unsigned STATE_FREEZING =      (1<<2);
  static const unsigned STATE_COMMITTING =    (1<<3);   // mid-commit
  static const unsigned STATE_FETCHING =      (1<<4);   // currenting fetching
  static const unsigned STATE_FRAGMENTING =   (1<<5);
  static const unsigned STATE_STICKY =        (1<<6);  // sticky pin due to stripe stickydirs
  static const unsigned STATE_DNPINNEDFRAG =  (1<<7);  // dir is refragmenting

  // common states
  static const unsigned STATE_CLEAN =  0;
  static const unsigned STATE_INITIAL = 0;

  // these state bits are preserved by an import/export
  // ...except if the directory is hashed, in which case none of them are!
  static const unsigned MASK_STATE_EXPORTED =
      (STATE_COMPLETE|STATE_DIRTY);
  static const unsigned MASK_STATE_IMPORT_KEPT =
      (STATE_STICKY);
  static const unsigned MASK_STATE_EXPORT_KEPT =
      (STATE_FROZEN|STATE_STICKY);
  static const unsigned MASK_STATE_FRAGMENT_KEPT =
      (STATE_DIRTY|STATE_COMPLETE);

  // -- rep spec --
  static const int REP_NONE =     0;
  static const int REP_ALL =      1;
  static const int REP_LIST =     2;


  static const int NONCE_EXPORT  = 1;


  // -- wait masks --
  static const uint64_t WAIT_DENTRY       = (1<<0);  // wait for item to be in cache
  static const uint64_t WAIT_COMPLETE     = (1<<1);  // wait for complete dir contents
  static const uint64_t WAIT_FROZEN       = (1<<2);  // auth pins removed

  static const int WAIT_DNLOCK_OFFSET = 4;

  static const uint64_t WAIT_ANY_MASK  = (0xffffffff);
  static const uint64_t WAIT_ATFREEZEROOT = (WAIT_UNFREEZE);
  static const uint64_t WAIT_ATSUBTREEROOT = (WAIT_SINGLEAUTH);




 public:
  // context
  MDCache *cache;

  CDirStripe *stripe; // my stripe
  frag_t frag; // my frag
  snapid_t first;

  bool is_lt(const MDSCacheObject *r) const {
    return dirfrag() < ((const CDirFrag*)r)->dirfrag();
  }

  elist<CDirFrag*>::item item_dirty, item_new;

 private:
  version_t version;
  snapid_t snap_purged_thru;

 public:
  void set_version(version_t v) { version = v; }
  version_t get_version() const { return version; }

  void _mark_dirty(LogSegment *ls);
  void _set_dirty_flag() {
    if (!state_test(STATE_DIRTY)) {
      state_set(STATE_DIRTY);
      get(PIN_DIRTY);
    }
  }
  void mark_dirty(LogSegment *ls);
  void log_mark_dirty();
  void mark_clean();

  void mark_new(LogSegment *ls);

public:
  typedef map<dentry_key_t, CDentry*> map_t;

  // contents of this directory
  map_t items;       // non-null AND null
protected:
  unsigned num_head_items;
  unsigned num_head_null;
  unsigned num_snap_items;
  unsigned num_snap_null;

  int num_dirty;

  // state
  version_t committing_version;
  version_t committed_version;


  // lock nesting, freeze
  int auth_pins;
#ifdef MDS_AUTHPIN_SET
  multiset<void*> auth_pin_set;
#endif
  int request_pins;


  // friends
  friend class CInode;
  friend class CDirStripe;
  friend class MDCache;
  friend class MDiscover;
  friend class MDBalancer;
  friend class Server;

  friend class CDirFragDiscover;
  friend class CDirFragExport;

  bloom_filter *bloom;
  /* If you set up the bloom filter, you must keep it accurate!
   * It's deleted when you mark_complete() and is deliberately not serialized.*/

 public:
  CDirFrag(CDirStripe *stripe, frag_t frag, MDCache *mdcache, bool auth);
  ~CDirFrag() {
    g_num_dir--;
    g_num_dirs++;
  }



  // -- accessors --
  inodeno_t ino() const { return stripe->ino(); } // deprecate me?
  frag_t get_frag() const { return frag; }
  dirfrag_t dirfrag() const { return dirfrag_t(stripe->dirstripe(), frag); }

  CDirStripe *get_stripe() { return stripe; }
  CDirPlacement *get_placement() { return stripe->get_placement(); }
  CInode* get_inode() { return stripe->get_inode(); }

  map_t::iterator begin() { return items.begin(); }
  map_t::iterator end() { return items.end(); }

  unsigned get_num_head_items() { return num_head_items; }
  unsigned get_num_head_null() { return num_head_null; }
  unsigned get_num_snap_items() { return num_snap_items; }
  unsigned get_num_snap_null() { return num_snap_null; }
  unsigned get_num_any() { return num_head_items + num_head_null + num_snap_items + num_snap_null; }

  void inc_num_dirty() { num_dirty++; }
  void dec_num_dirty() { 
    assert(num_dirty > 0);
    num_dirty--; 
  }
  int get_num_dirty() {
    return num_dirty;
  }


  // -- dentries and inodes --
 public:
  CDentry* lookup_exact_snap(const string& dname, snapid_t last) {
    map_t::iterator p = items.find(dentry_key_t(last, &dname));
    if (p == items.end())
      return NULL;
    return p->second;
  }
  CDentry* lookup(const string& n, snapid_t snap=CEPH_NOSNAP);

  CDentry* add_null_dentry(const string& dname, 
			   snapid_t first=2, snapid_t last=CEPH_NOSNAP);
  CDentry* add_primary_dentry(const string& dname, CInode *in, 
			      snapid_t first=2, snapid_t last=CEPH_NOSNAP);
  CDentry* add_remote_dentry(const string& dname, inodeno_t ino, unsigned char d_type, 
			     snapid_t first=2, snapid_t last=CEPH_NOSNAP);
  void remove_dentry( CDentry *dn );         // delete dentry
  void link_remote_inode( CDentry *dn, inodeno_t ino, unsigned char d_type);
  void link_remote_inode( CDentry *dn, CInode *in );
  void link_primary_inode( CDentry *dn, CInode *in );
  void unlink_inode( CDentry *dn );
  void try_remove_unlinked_dn(CDentry *dn);

  void add_to_bloom(CDentry *dn);
  bool is_in_bloom(const string& name);
  bool has_bloom() { return (bloom ? true : false); }
  void remove_bloom();
private:
  void link_inode_work( CDentry *dn, CInode *in );
  void unlink_inode_work( CDentry *dn );
  void remove_null_dentries();
  void purge_stale_snap_data(const set<snapid_t>& snaps);
public:
  bool try_trim_snap_dentry(CDentry *dn, const set<snapid_t>& snaps);


public:
  void split(int bits, list<CDirFrag*>& subs, list<Context*>& waiters, bool replay);
  void merge(list<CDirFrag*>& subs, list<Context*>& waiters, bool replay);

  bool should_split() {
    return (int)get_num_head_items() > g_conf->mds_bal_split_size;
  }
  bool should_merge() {
    return (int)get_num_head_items() < g_conf->mds_bal_merge_size;
  }

private:
  void prepare_new_fragment(bool replay);
  void prepare_old_fragment(bool replay);
  void steal_dentry(CDentry *dn);  // from another dir.  used by merge/split.
  void finish_old_fragment(list<Context*>& waiters, bool replay);
  void init_fragment_pins();


  // -- authority --
 public:
  pair<int,int> authority() { return stripe->authority(); }

  bool contains(CDirFrag *x);  // true if we are x or an ancestor of x 


  void encode_replica(int who, bufferlist& bl) {
    __u32 nonce = add_replica(who);
    ::encode(nonce, bl);
    ::encode(first, bl);
    ::encode(version, bl);
  }
  void decode_replica(bufferlist::iterator& p) {
    __u32 nonce;
    ::decode(nonce, p);
    replica_nonce = nonce;
    ::decode(first, p);
    ::decode(version, p);
  }



  // -- state --
  bool is_complete() { return state & STATE_COMPLETE; }

 
  // -- fetch --
  object_t get_ondisk_object() { 
    return file_object_t(ino(), frag);
  }
  void fetch(Context *c, bool ignore_authpinnability=false);
  void fetch(Context *c, const string& want_dn, bool ignore_authpinnability=false);
  void _fetched(bufferlist &bl, const string& want_dn);

  // -- commit --
  map<version_t, list<Context*> > waiting_for_commit;

  void commit_to(version_t want);
  void commit(version_t want, Context *c, bool ignore_authpinnability=false);
  void _commit(version_t want);
  map_t::iterator _commit_full(ObjectOperation& m, const set<snapid_t> *snaps,
                           unsigned max_write_size=-1);
  map_t::iterator _commit_partial(ObjectOperation& m, const set<snapid_t> *snaps,
                       unsigned max_write_size=-1,
                       map_t::iterator last_committed_dn=map_t::iterator());
  void _encode_dentry(CDentry *dn, bufferlist& bl, const set<snapid_t> *snaps);
  void _committed(version_t v, version_t last_renamed_version);
  void wait_for_commit(Context *c, version_t v=0);

  // -- dirtyness --
  version_t get_committing_version() { return committing_version; }
  version_t get_committed_version() { return committed_version; }
  void set_committed_version(version_t v) { committed_version = v; }

  void mark_complete();


  // -- reference counting --
  void first_get();
  void last_put();

  void request_pin_get() {
    if (request_pins == 0) get(PIN_REQUEST);
    request_pins++;
  }
  void request_pin_put() {
    request_pins--;
    if (request_pins == 0) put(PIN_REQUEST);
  }

    
  // -- waiters --
protected:
  map< string_snap_t, list<Context*> > waiting_on_dentry;
  map< inodeno_t, list<Context*> > waiting_on_ino;

public:
  bool is_waiting_for_dentry(const char *dname, snapid_t snap) {
    return waiting_on_dentry.count(string_snap_t(dname, snap));
  }
  void add_dentry_waiter(const string& dentry, snapid_t snap, Context *c);
  void take_dentry_waiting(const string& dentry, snapid_t first, snapid_t last, list<Context*>& ls);

  bool is_waiting_for_ino(inodeno_t ino) {
    return waiting_on_ino.count(ino);
  }
  void add_ino_waiter(inodeno_t ino, Context *c);
  void take_ino_waiting(inodeno_t ino, list<Context*>& ls);

  void take_sub_waiting(list<Context*>& ls);  // dentry or ino

  void add_waiter(uint64_t mask, Context *c);
  void take_waiting(uint64_t mask, list<Context*>& ls);  // may include dentry waiters
  void finish_waiting(uint64_t mask, int result = 0);    // ditto
  

  // -- import/export --
  void encode_export(bufferlist& bl);
  void finish_export(utime_t now);
  void abort_export() { 
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(bufferlist::iterator& blp, utime_t now);


  // -- auth pins --
  bool can_auth_pin() { return is_auth() && !is_freezing_or_frozen(); }
  int get_auth_pins() { return auth_pins; }
  void auth_pin(void *who);
  void auth_unpin(void *who);

  void verify_fragstat();

  // -- freezing --
  bool freeze_dir();
  void _freeze_dir();
  void unfreeze_dir();

  void maybe_finish_freeze();

  bool is_freezing_dir() { return state & STATE_FREEZING; }
  bool is_frozen_dir() { return state & STATE_FROZEN; }

  bool is_freezing() { return is_freezing_dir() || stripe->is_freezing(); }
  bool is_frozen() { return is_frozen_dir() || stripe->is_frozen(); }
#if 0
  bool is_freezeable(bool freezing=false) {
    // no nested auth pins.
    if ((auth_pins-freezing) > 0 || nested_auth_pins > 0) 
      return false;

    // inode must not be frozen.
    if (!is_subtree_root() && get_inode()->is_frozen())
      return false;

    return true;
  }
#endif
  bool is_freezeable_dir(bool freezing=false) {
    if ((auth_pins-freezing) > 0)
      return false;

    // XXX: conditions for freezing
#if 0
    // if not subtree root, inode must not be frozen (tree--frozen_dir is okay).
    if (!is_subtree_root() && get_inode()->is_frozen() && !get_inode()->is_frozen_dir())
      return false;
#endif
    return true;
  }


  ostream& print_db_line_prefix(ostream& out);
  void print(ostream& out);
};

#endif
