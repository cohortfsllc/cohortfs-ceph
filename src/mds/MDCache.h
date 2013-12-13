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



#ifndef CEPH_MDCACHE_H
#define CEPH_MDCACHE_H

#include "include/types.h"
#include "include/filepath.h"
#include "include/elist.h"

#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "InodeContainer.h"
#include "ParentStats.h"
#include "include/Context.h"
#include "events/EMetaBlob.h"

#include "messages/MClientRequest.h"
#include "messages/MMDSSlaveRequest.h"

class PerfCounters;

class MDS;
class Session;
class Migrator;

class Message;
class Session;

class MMDSResolve;
class MMDSResolveAck;
class MMDSCacheRejoin;
class MMDSCacheRejoinAck;
class MJoin;
class MJoinAck;
class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryLink;
class MDentryUnlink;
class MLock;
struct MMDSFindIno;
struct MMDSFindInoReply;
struct MMDSOpenIno;
struct MMDSOpenInoReply;
class MMDSRestripe;
class MMDSRestripeAck;

class Message;
class MClientRequest;
class MMDSSlaveRequest;
struct MClientSnap;

class MMDSFragmentNotify;

class ESubtreeMap;

struct Mutation;
struct MDRequest;
struct MDSlaveUpdate;


// flags for predirty_journal_parents()
static const int PREDIRTY_PRIMARY = 1; // primary dn, adjust nested accounting
static const int PREDIRTY_DIR = 2;     // update parent dir mtime/size
static const int PREDIRTY_SHALLOW = 4; // only go to immediate parent (for easier rollback)

class MDCache {
 public:
  // my master
  MDS *mds;

  // -- my cache --
  LRU lru;   // dentry lru for expiring items from cache
 protected:
  typedef hash_map<vinodeno_t,CInode*> inode_map;
  inode_map inodes;                        // map of inodes by ino
  CInode *root;                            // root inode
  CInode *myin;                            // .ceph/mds%d dir
  InodeContainer container;                // inode container dir
  SnapRealm snaprealm;

  set<CInode*> base_inodes;

public:
  ParentStats parentstats;

  SnapRealm* get_snaprealm() { return &snaprealm; }

  DecayRate decayrate;

  int num_inodes_with_caps;
  int num_caps;

  unsigned max_dir_commit_size;

  ceph_file_layout default_file_layout;
  ceph_file_layout default_log_layout;

  // -- client leases --
public:
  static const int client_lease_pools = 3;
  float client_lease_durations[client_lease_pools];
protected:
  xlist<ClientLease*> client_leases[client_lease_pools];
public:
  void touch_client_lease(ClientLease *r, int pool, utime_t ttl) {
    client_leases[pool].push_back(&r->item_lease);
    r->ttl = ttl;
  }

  // -- client caps --
  uint64_t              last_cap_id;
  


  // -- discover --
  struct discover_info_t {
    tid_t tid;
    int mds;
    dirfrag_t base;
    snapid_t snap;
    filepath want_path;
    inodeno_t want_ino;
    bool want_base_stripe;
    bool want_xlocked;

    discover_info_t() : tid(0), mds(-1), snap(CEPH_NOSNAP), want_base_stripe(false), want_xlocked(false) {}
  };

  map<tid_t, discover_info_t> discovers;
  tid_t discover_last_tid;

  void _send_discover(discover_info_t& dis);
  discover_info_t& _create_discover(int mds) {
    tid_t t = ++discover_last_tid;
    discover_info_t& d = discovers[t];
    d.tid = t;
    d.mds = mds;
    return d;
  }

  // waiters
  map<int, map<inodeno_t, list<Context*> > > waiting_for_base_ino;

  void discover_base_ino(inodeno_t want_ino, Context *onfinish, int from=-1);
  void discover_dir_stripe(CInode *base, stripeid_t stripeid, Context *onfinish, int from=-1);
  void discover_dir_frag(CStripe *stripe, frag_t approx_fg, Context *onfinish,
			 int from=-1);
  void discover_path(CInode *base, snapid_t snap, const filepath &want_path,
                     Context *onfinish, bool want_xlocked=false, int from=-1);
  void discover_path(CStripe *base, snapid_t snap, const filepath &want_path,
                     Context *onfinish, bool want_xlocked=false);
  void discover_path(CDir *base, snapid_t snap, const filepath &want_path,
                     Context *onfinish, bool want_xlocked=false);
  void discover_ino(CDir *base, inodeno_t want_ino, Context *onfinish,
		    bool want_xlocked=false);

  void kick_discovers(int who);  // after a failure.


public:
  int get_num_inodes() { return inodes.size(); }
  int get_num_dentries() { return lru.lru_get_size(); }

protected:
  // delayed cache expire
  map<CStripe*, map<int, MCacheExpire*> > delayed_expire; // subtree root -> expire msg


  // -- requests --
protected:
  hash_map<metareqid_t, MDRequest*> active_requests; 

public:
  int get_num_client_requests();

  MDRequest* request_start(MClientRequest *req);
  MDRequest* request_start_slave(metareqid_t rid, __u32 attempt, int by);
  MDRequest* request_start_internal(int op);
  bool have_request(metareqid_t rid) {
    return active_requests.count(rid);
  }
  MDRequest* request_get(metareqid_t rid);
  void request_pin_ref(MDRequest *r, CInode *ref, vector<CDentry*>& trace);
  void request_finish(MDRequest *mdr);
  void request_forward(MDRequest *mdr, int mds, int port=0);
  void dispatch_request(MDRequest *mdr);
  void request_drop_foreign_locks(MDRequest *mdr);
  void request_drop_non_rdlocks(MDRequest *r);
  void request_drop_locks(MDRequest *r);
  void request_cleanup(MDRequest *r);
  
  void request_kill(MDRequest *r);  // called when session closes

  // journal/snap helpers
  CInode *pick_inode_snap(CInode *in, snapid_t follows);
  CInode *cow_inode(CInode *in, snapid_t last);
  void journal_cow_dentry(Mutation *mut, EMetaBlob *metablob, CDentry *dn, snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0, CDentry::linkage_t *dnl=0);
  void journal_cow_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP,
			  CInode **pcow_inode=0);
  void journal_dirty_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows=CEPH_NOSNAP);

  void project_rstat_inode_to_frag(CInode *cur, CStripe *parent, snapid_t first, int linkunlink);
  void _project_rstat_inode_to_frag(inode_t& inode, snapid_t ofirst, snapid_t last,
				    CStripe *parent, int linkunlink=0);
  void project_rstat_frag_to_inode(const nest_info_t& rstat,
                                   const nest_info_t& accounted_rstat,
				   snapid_t ofirst, snapid_t last,
				   CInode *pin, bool cow_head);
  void predirty_journal_parents(Mutation *mut, EMetaBlob *blob,
				CInode *in, CDir *parent,
				int flags, int linkunlink=0,
				snapid_t follows=CEPH_NOSNAP);

  // slaves
  void add_uncommitted_master(metareqid_t reqid, LogSegment *ls, set<int> &slaves, bool safe=false) {
    uncommitted_masters[reqid].ls = ls;
    uncommitted_masters[reqid].slaves = slaves;
    uncommitted_masters[reqid].safe = safe;
  }
  void wait_for_uncommitted_master(metareqid_t reqid, Context *c) {
    uncommitted_masters[reqid].waiters.push_back(c);
  }
  void log_master_commit(metareqid_t reqid);
  void logged_master_update(metareqid_t reqid);
  void _logged_master_commit(metareqid_t reqid);
  void committed_master_slave(metareqid_t r, int from);
  void finish_committed_masters();

  void _logged_slave_commit(int from, metareqid_t reqid);

  // -- recovery --
protected:
  set<int> recovery_set;

public:
  void set_recovery_set(set<int>& s);
  void handle_mds_failure(int who);
  void handle_mds_recovery(int who);

protected:
  // [resolve]
  map<int, map<metareqid_t, MDSlaveUpdate*> > uncommitted_slave_updates;  // slave: for replay.
  map<CStripe*, int> uncommitted_slave_rename_oldstripe;  // slave: preserve the non-auth dir until seeing commit.
  map<CInode*, int> uncommitted_slave_unlink;  // slave: preserve the unlinked inode until seeing commit.

  // track master requests whose slaves haven't acknowledged commit
  struct umaster {
    set<int> slaves;
    LogSegment *ls;
    list<Context*> waiters;
    bool safe;
    bool committing;
    bool recovering;
    umaster() : committing(false), recovering(false) {}
  };
  map<metareqid_t, umaster>                 uncommitted_masters;         // master: req -> slave set

  set<metareqid_t>		pending_masters;
  map<int, set<metareqid_t> >	ambiguous_slave_updates;

  friend class ESlaveUpdate;
  friend class ECommitted;

  bool resolves_pending;
  set<int> resolve_gather;	// nodes i need resolves from
  set<int> resolve_ack_gather;	// nodes i need a resolve_ack from
  map<metareqid_t, int> need_resolve_rollback;  // rollbacks i'm writing to the journal
  map<int, MMDSResolve*> delayed_resolve;
  
  void handle_resolve(MMDSResolve *m);
  void handle_resolve_ack(MMDSResolveAck *m);
  void process_delayed_resolve();
  void discard_delayed_resolve(int who);
  void maybe_resolve_finish();
  void recalc_auth_bits() {}
  void trim_unlinked_inodes();
  void add_uncommitted_slave_update(metareqid_t reqid, int master, MDSlaveUpdate*);
  void finish_uncommitted_slave_update(metareqid_t reqid, int master);
  MDSlaveUpdate* get_uncommitted_slave_update(metareqid_t reqid, int master);
public:
  void remove_inode_recursive(CInode *in);

  bool is_ambiguous_slave_update(metareqid_t reqid, int master) {
    return ambiguous_slave_updates.count(master) &&
	   ambiguous_slave_updates[master].count(reqid);
  }
  void add_ambiguous_slave_update(metareqid_t reqid, int master) {
    ambiguous_slave_updates[master].insert(reqid);
  }
  void remove_ambiguous_slave_update(metareqid_t reqid, int master) {
    assert(ambiguous_slave_updates[master].count(reqid));
    ambiguous_slave_updates[master].erase(reqid);
    if (ambiguous_slave_updates[master].empty())
      ambiguous_slave_updates.erase(master);
  }

  void add_rollback(metareqid_t reqid, int master) {
    need_resolve_rollback[reqid] = master;
  }
  void finish_rollback(metareqid_t reqid);

  void resolve_start();
  void send_resolves();
  void send_slave_resolves();
  void send_subtree_resolves();
  void maybe_send_pending_resolves() {
    if (resolves_pending)
      send_subtree_resolves();
  }

  void clean_open_file_lists();

protected:
  // [rejoin]
  bool rejoins_pending;
  set<int> rejoin_gather;      // nodes from whom i need a rejoin
  set<int> rejoin_sent;        // nodes i sent a rejoin to
  set<int> rejoin_ack_gather;  // nodes from whom i need a rejoin ack

  map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> > cap_exports; // ino -> client -> capex
  map<inodeno_t,int> cap_export_targets; // ino -> auth mds

  map<inodeno_t,map<client_t,map<int,ceph_mds_cap_reconnect> > > cap_imports;  // ino -> client -> frommds -> capex
  map<inodeno_t,filepath> cap_import_paths;
  set<inodeno_t> cap_imports_missing;
  int cap_imports_num_opening;
  
  set<CInode*> rejoin_undef_inodes;
  set<CInode*> rejoin_potential_updated_scatterlocks;
  set<CDir*>   rejoin_undef_dirfrags;
  map<int, set<CInode*> > rejoin_unlinked_inodes;

  vector<CInode*> rejoin_recover_q, rejoin_check_q;
  list<Context*> rejoin_waiters;

  void rejoin_walk(CStripe *stripe, MMDSCacheRejoin *rejoin);
  void handle_cache_rejoin(MMDSCacheRejoin *m);
  void handle_cache_rejoin_weak(MMDSCacheRejoin *m);
  bool rejoin_fetch_dirfrags(MMDSCacheRejoin *m);
  void handle_cache_rejoin_strong(MMDSCacheRejoin *m);
  void rejoin_scour_survivor_replicas(int from, MMDSCacheRejoin *ack,
				      set<SimpleLock *>& gather_locks,
				      set<vinodeno_t>& acked_inodes);
  void handle_cache_rejoin_ack(MMDSCacheRejoin *m);
  void handle_cache_rejoin_purge(MMDSCacheRejoin *m);
  void handle_cache_rejoin_missing(MMDSCacheRejoin *m);
  void handle_cache_rejoin_full(MMDSCacheRejoin *m);
  void rejoin_send_acks();
  void rejoin_trim_undef_inodes();
  void maybe_send_pending_rejoins() {
    if (rejoins_pending)
      rejoin_send_rejoins();
  }
public:
  void rejoin_start();
  void rejoin_gather_finish();
  void rejoin_send_rejoins();
  void rejoin_export_caps(inodeno_t ino, client_t client, ceph_mds_cap_reconnect& capinfo,
			  int target=-1) {
    cap_exports[ino][client] = capinfo;
    cap_export_targets[ino] = target;
  }
  void rejoin_recovered_caps(inodeno_t ino, client_t client, cap_reconnect_t& icr, 
			     int frommds=-1) {
    cap_imports[ino][client][frommds] = icr.capinfo;
    cap_import_paths[ino] = filepath(icr.path, (uint64_t)icr.capinfo.pathbase);
  }
  ceph_mds_cap_reconnect *get_replay_cap_reconnect(inodeno_t ino, client_t client) {
    if (cap_imports.count(ino) &&
	cap_imports[ino].count(client) &&
	cap_imports[ino][client].count(-1)) {
      return &cap_imports[ino][client][-1];
    }
    return NULL;
  }
  void remove_replay_cap_reconnect(inodeno_t ino, client_t client) {
    assert(cap_imports[ino].size() == 1);
    assert(cap_imports[ino][client].size() == 1);
    cap_imports.erase(ino);
  }

  // [reconnect/rejoin caps]
  friend class C_MDC_RejoinOpenInoFinish;
  void rejoin_open_ino_finish(inodeno_t ino, int ret);
  bool process_imported_caps();
  void choose_lock_states();
  void rejoin_import_cap(CInode *in, client_t client, ceph_mds_cap_reconnect& icr, int frommds);
  void finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq);
  void try_reconnect_cap(CInode *in, Session *session);
  void export_remaining_imported_caps();

  // cap imports.  delayed snap parent opens.
  //  realm inode -> client -> cap inodes needing to split to this realm
  map<CInode*,map<client_t, set<inodeno_t> > > missing_snap_parents; 
  map<client_t,set<CInode*> > delayed_imported_caps;

  void do_cap_import(Session *session, CInode *in, Capability *cap);
  void do_delayed_cap_imports();
  void check_realm_past_parents(SnapRealm *realm);
  void open_snap_parents();

  bool open_undef_inodes_dirfrags();
  void opened_undef_dirfrag(CDir *dir) {
    rejoin_undef_dirfrags.erase(dir);
  }
  void opened_undef_inode(CInode *in) {
    rejoin_undef_inodes.erase(in);
  }

  void reissue_all_caps();
  

  friend class Locker;
  friend class Migrator;
  friend class MDBalancer;


  // file size recovery
  set<CInode*> file_recover_queue;
  set<CInode*> file_recovering;

  void queue_file_recover(CInode *in);
  void unqueue_file_recover(CInode *in);
  void _queued_file_recover_cow(CInode *in, Mutation *mut);
  void _queue_file_recover(CInode *in);
  void identify_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q);
  void start_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q);

  void do_file_recover();
  void _recovered(CInode *in, int r, uint64_t size, utime_t mtime);

  void purge_prealloc_ino(inodeno_t ino, Context *fin);



public:

  // subsystems
  Migrator *migrator;

 public:
  MDCache(MDS *m);
  ~MDCache();
  
  // debug
  void log_stat();

  // root inode
  CInode *get_root() { return root; }
  CInode *get_myin() { return myin; }

  // cache
  void set_cache_size(size_t max) { lru.lru_set_max(max); }
  size_t get_cache_size() { return lru.lru_get_size(); }

  // trimming
  bool trim(int max = -1);   // trim cache
  bool trim_dentry(CDentry *dn, map<int, MCacheExpire*>& expiremap);
  void trim_dirfrag(CDir *dir, map<int, MCacheExpire*>& expiremap);
  void trim_stripe(CStripe *stripe, map<int, MCacheExpire*>& expiremap);
  void trim_inode(CDentry *dn, CInode *in, map<int, MCacheExpire*>& expiremap);
  void send_expire_messages(map<int, MCacheExpire*>& expiremap);
  void trim_non_auth();      // trim out trimmable non-auth items
  bool trim_non_auth_subtree(CStripe *stripe);
  void try_trim_non_auth_subtree(CStripe *stripe);

  void trim_client_leases();
  void check_memory_usage();

  // shutdown
  void shutdown_start();
  void shutdown_check();
  bool shutdown_pass();
  bool shutdown();                    // clear cache (ie at shutodwn)

  bool did_shutdown_log_cap;

  // inode_map
  bool have_inode(vinodeno_t vino) {
    return inodes.count(vino) ? true:false;
  }
  bool have_inode(inodeno_t ino, snapid_t snap=CEPH_NOSNAP) {
    return have_inode(vinodeno_t(ino, snap));
  }
  CInode* get_inode(vinodeno_t vino) {
    inode_map::iterator i = inodes.find(vino);
    return i == inodes.end() ? NULL : i->second;
  }
  CInode* get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }

  CStripe *get_dirstripe(dirstripe_t ds) {
    CInode *in = get_inode(ds.ino);
    if (!in)
      return NULL;
    return in->get_stripe(ds.stripeid);
  }

  CDir* get_dirfrag(dirfrag_t df) {
    CStripe *stripe = get_dirstripe(df.stripe);
    if (!stripe)
      return NULL;
    return stripe->get_dirfrag(df.frag);
  }
  CDir* get_force_dirfrag(dirfrag_t df) {
    CStripe *stripe = get_dirstripe(df.stripe);
    if (!stripe)
      return NULL;
    CDir *dir = force_dir_fragment(stripe, df.frag);
    if (!dir)
      dir = stripe->get_dirfrag(df.frag);
    return dir;
  }

  MDSCacheObject *get_object(MDSCacheObjectInfo &info);

  

 public:
  void add_inode(CInode *in);

  void remove_inode(CInode *in);
 protected:
  void touch_inode(CInode *in) {
    if (in->get_parent_dn())
      touch_dentry(in->get_projected_parent_dn());
  }
public:
  void touch_dentry(CDentry *dn) {
    // touch ancestors
    if (dn->get_dir()->get_inode()->get_projected_parent_dn())
      touch_dentry(dn->get_dir()->get_inode()->get_projected_parent_dn());
    
    // touch me
    if (dn->is_auth())
      lru.lru_touch(dn);
    else
      lru.lru_midtouch(dn);
  }
  void touch_dentry_bottom(CDentry *dn) {
    lru.lru_bottouch(dn);
    if (dn->get_projected_linkage()->is_primary()) {
      CInode *in = dn->get_projected_linkage()->get_inode();
      list<CStripe*> stripes;
      in->get_stripes(stripes);
      list<CDir*> dirs;
      for (list<CStripe*>::iterator p = stripes.begin(); p != stripes.end(); ++p)
        (*p)->get_dirfrags(dirs);
      for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); ++p)
        (*p)->touch_dentries_bottom();
    }
  }
protected:

  void inode_remove_replica(CInode *in, int rep, set<SimpleLock *>& gather_locks);
  void dentry_remove_replica(CDentry *dn, int rep, set<SimpleLock *>& gather_locks);

  void rename_file(CDentry *srcdn, CDentry *destdn);

 public:
  // truncate
  void truncate_inode(CInode *in, LogSegment *ls);
  void _truncate_inode(CInode *in, LogSegment *ls);
  void truncate_inode_finish(CInode *in, LogSegment *ls);
  void truncate_inode_logged(CInode *in, Mutation *mut);

  void add_recovered_truncate(CInode *in, LogSegment *ls);
  void remove_recovered_truncate(CInode *in, LogSegment *ls);
  void start_recovered_truncates();


private:
  bool opening_root, open;
  list<Context*> waiting_for_open;

public:
  void init_layouts();
  CInode *create_system_inode(inodeno_t ino, int auth, int mode);
  CInode *create_root_inode();

  void create_empty_hierarchy(C_Gather *gather);
  void create_mydir_hierarchy(C_Gather *gather);

  bool is_open() { return open; }
  void wait_for_open(Context *c) {
    waiting_for_open.push_back(c);
  }

  void open_root_inode(Context *c);
  void open_root();
  void open_mydir_inode(Context *c);
  void populate_mydir();

  InodeContainer* get_container() { return &container; }

  void _create_system_file(CDir *dir, const char *name, CInode *in, Context *fin);
  void _create_system_file_finish(Mutation *mut, CDentry *dn, Context *fin);

  void open_foreign_mdsdir(inodeno_t ino, Context *c);


  Context *_get_waiter(MDRequest *mdr, Message *req, Context *fin);

  /**
   * Find the given dentry (and whether it exists or not), its ancestors,
   * and get them all into memory and usable on this MDS. This function
   * makes a best-effort attempt to load everything; if it needs to
   * go away and do something then it will put the request on a waitlist.
   * It prefers the mdr, then the req, then the fin. (At least one of these
   * must be non-null.)
   *
   * At least one of the params mdr, req, and fin must be non-null.
   *
   * @param mdr The MDRequest associated with the path. Can be null.
   * @param req The Message associated with the path. Can be null.
   * @param fin The Context associated with the path. Can be null.
   * @param path The path to traverse to.
   * @param pdnvec Data return parameter -- on success, contains a
   * vector of dentries. On failure, is either empty or contains the
   * full trace of traversable dentries.
   * @param pin Data return parameter -- if successful, points to the inode
   * associated with filepath. If unsuccessful, is null.
   * @param onfail Specifies different lookup failure behaviors. If set to
   * MDS_TRAVERSE_DISCOVERXLOCK, path_traverse will succeed on null
   * dentries (instead of returning -ENOENT). If set to
   * MDS_TRAVERSE_FORWARD, it will forward the request to the auth
   * MDS if that becomes appropriate (ie, if it doesn't know the contents
   * of a directory). If set to MDS_TRAVERSE_DISCOVER, it
   * will attempt to look up the path from a different MDS (and bring them
   * into its cache as replicas).
   *
   * @returns 0 on success, 1 on "not done yet", 2 on "forwarding", -errno otherwise.
   * If it returns 1, the requester associated with this call has been placed
   * on the appropriate waitlist, and it should unwind itself and back out.
   * If it returns 2 the request has been forwarded, and again the requester
   * should unwind itself and back out.
   */
  int path_traverse(MDRequest *mdr, Message *req, Context *fin, const filepath& path,
		    vector<CDentry*> *pdnvec, CInode **pin, int onfail);
  bool path_is_mine(filepath& path);
  bool path_is_mine(string& p) {
    filepath path(p, 1);
    return path_is_mine(path);
  }

  CInode *cache_traverse(const filepath& path);

  void open_remote_dirstripe(CInode *diri, stripeid_t stripeid, Context *fin);
  void open_remote_dirfrag(CStripe *stripe, frag_t fg, Context *fin);
  CInode *get_dentry_inode(CDentry *dn, MDRequest *mdr, bool projected=false);
  void open_remote_ino(inodeno_t ino, Context *fin, bool want_xlocked=false);

  bool parallel_fetch(map<inodeno_t,filepath>& pathmap, set<inodeno_t>& missing);
  bool parallel_fetch_traverse_dir(inodeno_t ino, filepath& path, 
				   set<CDir*>& fetch_queue, set<inodeno_t>& missing,
				   C_GatherBuilder &gather_bld);

  void open_remote_dentry(CDentry *dn, bool projected, Context *fin,
			  bool want_xlocked=false);
  void _open_remote_dentry_finish(CDentry *dn, inodeno_t ino, Context *fin,
				  bool want_xlocked, int mode, int r);

  void make_trace(vector<CDentry*>& trace, CInode *in);

protected:
  struct open_ino_info_t {
    vector<inode_backpointer_t> ancestors;
    set<int> checked;
    int checking;
    int auth_hint;
    bool check_peers;
    bool fetch_backtrace;
    bool discover;
    bool want_replica;
    bool want_xlocked;
    version_t tid;
    int64_t pool;
    list<Context*> waiters;
    open_ino_info_t() : checking(-1), auth_hint(-1),
      check_peers(true), fetch_backtrace(true), discover(false) {}
  };
  tid_t open_ino_last_tid;
  map<inodeno_t,open_ino_info_t> opening_inodes;

  void _open_ino_backtrace_fetched(inodeno_t ino, bufferlist& bl, int err);
  void _open_ino_parent_opened(inodeno_t ino, int ret);
  void _open_ino_traverse_dir(inodeno_t ino, open_ino_info_t& info, int err);
  void _open_ino_fetch_dir(inodeno_t ino, MMDSOpenIno *m, CDir *dir);
  Context* _open_ino_get_waiter(inodeno_t ino, MMDSOpenIno *m);
  int open_ino_traverse_dir(inodeno_t ino, MMDSOpenIno *m,
			    vector<inode_backpointer_t>& ancestors,
			    bool discover, bool want_xlocked, int *hint);
  void open_ino_finish(inodeno_t ino, open_ino_info_t& info, int err);
  void do_open_ino(inodeno_t ino, open_ino_info_t& info, int err);
  void do_open_ino_peer(inodeno_t ino, open_ino_info_t& info);
  void handle_open_ino(MMDSOpenIno *m);
  void handle_open_ino_reply(MMDSOpenInoReply *m);
  friend class C_MDC_OpenInoBacktraceFetched;
  friend struct C_MDC_OpenInoTraverseDir;
  friend struct C_MDC_OpenInoParentOpened;

public:
  void kick_open_ino_peers(int who);
  void open_ino(inodeno_t ino, int64_t pool, Context *fin,
		bool want_replica=true, bool want_xlocked=false);
  
  // -- find_ino_peer --
  struct find_ino_peer_info_t {
    inodeno_t ino;
    tid_t tid;
    Context *fin;
    int hint;
    int checking;
    set<int> checked;

    find_ino_peer_info_t() : tid(0), fin(NULL), hint(-1), checking(-1) {}
  };

  map<tid_t, find_ino_peer_info_t> find_ino_peer;
  tid_t find_ino_peer_last_tid;

  void find_ino_peers(inodeno_t ino, Context *c, int hint=-1);
  void _do_find_ino_peer(find_ino_peer_info_t& fip);
  void handle_find_ino(MMDSFindIno *m);
  void handle_find_ino_reply(MMDSFindInoReply *m);
  void kick_find_ino_peers(int who);

  // -- find_ino_dir --
  struct find_ino_dir_info_t {
    inodeno_t ino;
    Context *fin;
  };

  void find_ino_dir(inodeno_t ino, Context *c);
  void _find_ino_dir(inodeno_t ino, Context *c, bufferlist& bl, int r);

protected:
  void fetch_backtrace(inodeno_t ino, int64_t pool, bufferlist& bl, Context *fin);
  friend class C_MDC_FetchedBacktrace;

  // -- stray --
 public:
  void scan_stray_dir() {}
  void maybe_eval_stray(CInode *in) {}

  // == messages ==
 public:
  void dispatch(Message *m);

 protected:
  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);
  friend class C_MDC_Join;

public:
  void replicate_stripe(CStripe *stripe, int to, bufferlist& bl) {
    dirstripe_t ds = stripe->dirstripe();
    ::encode(ds, bl);
    stripe->encode_replica(to, bl);
  }
  void replicate_dir(CDir *dir, int to, bufferlist& bl) {
    dirfrag_t df = dir->dirfrag();
    ::encode(df, bl);
    dir->encode_replica(to, bl);
  }
  void replicate_dentry(CDentry *dn, int to, bufferlist& bl) {
    ::encode(dn->name, bl);
    ::encode(dn->last, bl);
    dn->encode_replica(to, bl);
  }
  void replicate_inode(CInode *in, int to, bufferlist& bl) {
    ::encode(in->inode.ino, bl);  // bleh, minor assymetry here
    ::encode(in->last, bl);
    in->encode_replica(to, bl);
  }

  CStripe* add_replica_stripe(bufferlist::iterator& p, CInode *diri,
                              int from, list<Context*>& finished);
  CStripe* forge_replica_stripe(CInode *diri, stripeid_t stripe, int from);
  CDir* add_replica_dir(bufferlist::iterator& p, CStripe *stripe, list<Context*>& finished);
  CDir* forge_replica_dir(CStripe *stripe, frag_t fg, int from);
  CDentry *add_replica_dentry(bufferlist::iterator& p, CDir *dir, list<Context*>& finished);
  CInode *add_replica_inode(bufferlist::iterator& p, CDentry *dn,
                            int from, list<Context*>& finished);

  void replicate_stray(CDentry *straydn, int who, bufferlist& bl);
  CDentry *add_replica_stray(bufferlist &bl, int from);

  // -- namespace --
public:
  void send_dentry_link(CDentry *dn, const vector<int> *skip = NULL);
  void send_dentry_unlink(CDentry *dn, MDRequest *mdr);
protected:
  void handle_dentry_link(MDentryLink *m);
  void handle_dentry_unlink(MDentryUnlink *m);


  // -- fragmenting --
private:
  struct ufragment {
    int bits;
    bool committed;
    LogSegment *ls;
    list<Context*> waiters;
    map<frag_t, version_t> old_frags;
    ufragment() : bits(0), committed(false), ls(NULL) {}
  };
  map<dirfrag_t, ufragment> uncommitted_fragments;

  struct fragment_info_t {
    dirfrag_t dirfrag;
    int bits;
    list<CDir*> dirs;
    list<CDir*> resultfrags;
  };
  map<metareqid_t, fragment_info_t> fragment_requests;

  void adjust_dir_fragments(CStripe *stripe, frag_t basefrag, int bits,
			    list<CDir*>& frags, list<Context*>& waiters, bool replay);
  void adjust_dir_fragments(CStripe *stripe,
			    list<CDir*>& srcfrags,
			    frag_t basefrag, int bits,
			    list<CDir*>& resultfrags, 
			    list<Context*>& waiters,
			    bool replay);
  CDir *force_dir_fragment(CStripe *stripe, frag_t fg);

  bool can_fragment(CStripe *stripe, list<CDir*>& dirs);

  void fragment_freeze_dirs(list<CDir*>& dirs, C_GatherBuilder &gather);
  void fragment_mark_and_complete(list<CDir*>& dirs);
  void fragment_frozen(list<CDir*>& dirs, frag_t basefrag, int bits);
  void fragment_unmark_unfreeze_dirs(list<CDir*>& dirs);
  void dispatch_fragment_dir(MDRequest *mdr);
  void _fragment_logged(MDRequest *mdr);
  void _fragment_stored(MDRequest *mdr);
  void _fragment_committed(dirfrag_t f, list<CDir*>& resultfrags);
  void _fragment_finish(dirfrag_t f, list<CDir*>& resultfrags);

  friend class EFragment;
  friend class C_MDC_FragmentFrozen;
  friend class C_MDC_FragmentMarking;
  friend class C_MDC_FragmentPrep;
  friend class C_MDC_FragmentStore;
  friend class C_MDC_FragmentCommit;
  friend class C_MDC_FragmentFinish;

  void handle_fragment_notify(MMDSFragmentNotify *m);

  void add_uncommitted_fragment(dirfrag_t basedirfrag, int bits,
                                map<frag_t, version_t>& old_frags,
                                LogSegment *ls);
  void finish_uncommitted_fragment(dirfrag_t basedirfrag, int op);
  void rollback_uncommitted_fragment(dirfrag_t basedirfrag,
                                     map<frag_t, version_t>& old_frags);
public:
  void wait_for_uncommitted_fragment(dirfrag_t dirfrag, Context *c) {
    assert(uncommitted_fragments.count(dirfrag));
    uncommitted_fragments[dirfrag].waiters.push_back(c);
  }
  void split_dir(CDir *dir, int byn);
  void merge_dir(CStripe *stripe, frag_t fg);
  void rollback_uncommitted_fragments();

  // -- updates --
  //int send_inode_updates(CInode *in);
  //void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CStripe *stripe, bool bcast=false);
  void handle_dir_update(MDirUpdate *m);

  // -- cache expiration --
  void handle_cache_expire(MCacheExpire *m);
  void process_delayed_expire(CStripe *stripe);
  void discard_delayed_expire(CStripe *stripe);


  // == crap fns ==
 public:
  void show_cache();
  void dump_cache(const char *fn=0);

  CInode *hack_pick_random_inode() {
    assert(!inodes.empty());
    int n = rand() % inodes.size();
    inode_map::iterator p = inodes.begin();
    while (n--) ++p;
    return p->second;
  }

};

class C_MDS_RetryRequest : public Context {
  MDCache *cache;
  MDRequest *mdr;
 public:
  C_MDS_RetryRequest(MDCache *c, MDRequest *r);
  virtual void finish(int r);
};

#endif
