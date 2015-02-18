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

#ifndef CEPH_MDS_LOCKER_H
#define CEPH_MDS_LOCKER_H

#include "include/types.h"

#include <map>
#include <list>
#include <set>
using std::map;
using std::list;
using std::set;

class MDS;
class Session;
class CDir;
class CInode;
class CDentry;
class EMetaBlob;

class Message;

class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryUnlink;
class MLock;

class MClientRequest;

class Anchor;
class Capability;
class LogSegment;

class SimpleLock;
class ScatterLock;
class LocalLock;
class MDCache;

#include "SimpleLock.h"

class Locker {
private:
  MDS *mds;
  MDCache *mdcache;

 public:
  Locker(MDS *m, MDCache *c) : mds(m), mdcache(c) {}

  SimpleLock *get_lock(int lock_type, MDSCacheObjectInfo &info);

  void dispatch(Message *m);
  void handle_lock(MLock *m);


  void nudge_log(SimpleLock *lock);

protected:
  void send_lock_message(SimpleLock *lock, int msg);
  void send_lock_message(SimpleLock *lock, int msg, const bufferlist &data);

  // -- locks --
  void _drop_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue);
  void _drop_non_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue);
public:

  bool acquire_locks(MDRequestRef& mdr,
		     set<SimpleLock*> &rdlocks,
		     set<SimpleLock*> &wrlocks,
		     set<SimpleLock*> &xlocks,
		     map<SimpleLock*,int> *remote_wrlocks=NULL,
		     CInode *auth_pin_freeze=NULL,
		     bool auth_pin_nonblock=false);

  void cancel_locking(MutationImpl *mut, set<CInode*> *pneed_issue);
  void drop_locks(MutationImpl *mut, set<CInode*> *pneed_issue=0);
  void set_xlocks_done(MutationImpl *mut, bool skip_dentry=false);
  void drop_non_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue=0);
  void drop_rdlocks(MutationImpl *mut, set<CInode*> *pneed_issue=0);

  void eval_gather(SimpleLock *lock, bool first=false,
		   bool *need_issue=0,
		   std::vector<Context*> *pfinishers=0);
  void eval(SimpleLock *lock, bool *need_issue);
  void eval_any(SimpleLock *lock, bool *need_issue,
		std::vector<Context*> *pfinishers=0,
		bool first=false) {
    if (!lock->is_stable())
      eval_gather(lock, first, need_issue, pfinishers);
    else if (lock->get_parent()->is_auth())
      eval(lock, need_issue);
  }

  class C_EvalScatterGathers : public Context {
    Locker *locker;
    CInode *in;
  public:
    C_EvalScatterGathers(Locker *l, CInode *i) : locker(l), in(i) {
      in->get(CInode::PIN_PTRWAITER);
    }
    void finish(int r) {
      in->put(CInode::PIN_PTRWAITER);
      locker->eval_scatter_gathers(in);
    }
  };
  void eval_scatter_gathers(CInode *in);

  void eval_cap_gather(CInode *in, set<CInode*> *issue_set=0);

  bool eval(CInode *in, int mask, bool caps_imported=false);
  void try_eval(MDSCacheObject *p, int mask);
  void try_eval(SimpleLock *lock, bool *pneed_issue);

  bool _rdlock_kick(SimpleLock *lock, bool as_anon);
  bool rdlock_try(SimpleLock *lock, client_t client, Context *c);
  bool rdlock_start(SimpleLock *lock, MDRequestRef& mut, bool as_anon=false);
  void rdlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue);
  bool can_rdlock_set(set<SimpleLock*>& locks);
  bool rdlock_try_set(set<SimpleLock*>& locks);
  void rdlock_take_set(set<SimpleLock*>& locks, MutationRef& mut);

  void wrlock_force(SimpleLock *lock, MutationRef& mut);
  bool wrlock_start(SimpleLock *lock, MDRequestRef& mut, bool nowait=false);
  void wrlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue);

  void remote_wrlock_start(SimpleLock *lock, int target, MDRequestRef& mut);
  void remote_wrlock_finish(SimpleLock *lock, int target, MutationImpl *mut);

  bool xlock_start(SimpleLock *lock, MDRequestRef& mut);
  void _finish_xlock(SimpleLock *lock, client_t xlocker, bool *pneed_issue);
  void xlock_finish(SimpleLock *lock, MutationImpl *mut, bool *pneed_issue);

  void xlock_export(SimpleLock *lock, MutationImpl *mut);
  void xlock_import(SimpleLock *lock);


  // simple
public:
  void try_simple_eval(SimpleLock *lock);
  bool simple_rdlock_try(SimpleLock *lock, Context *con);
protected:
  void simple_eval(SimpleLock *lock, bool *need_issue);
  void handle_simple_lock(SimpleLock *lock, MLock *m);

public:
  bool simple_sync(SimpleLock *lock, bool *need_issue=0);
protected:
  void simple_lock(SimpleLock *lock, bool *need_issue=0);
  void simple_excl(SimpleLock *lock, bool *need_issue=0);
  void simple_xlock(SimpleLock *lock);


  // scatter
public:
  void scatter_eval(ScatterLock *lock, bool *need_issue);	 // public for MDCache::adjust_subtree_auth()

  void scatter_tick();
  void scatter_nudge(ScatterLock *lock, Context *c, bool forcelockchange=false);

protected:
  void handle_scatter_lock(ScatterLock *lock, MLock *m);
  void _scatter_replica_lock(ScatterLock *lock, int auth);
  bool scatter_scatter_fastpath(ScatterLock *lock);
  void scatter_scatter(ScatterLock *lock, bool nowait=false);
  void scatter_tempsync(ScatterLock *lock, bool *need_issue=0);

  void scatter_writebehind(ScatterLock *lock);
  class C_Locker_ScatterWB : public Context {
    Locker *locker;
    ScatterLock *lock;
    MutationRef mut;
  public:
    C_Locker_ScatterWB(Locker *l, ScatterLock *sl, MutationRef& m) :
      locker(l), lock(sl), mut(m) {}
    void finish(int r) {
      locker->scatter_writebehind_finish(lock, mut);
    }
  };
  void scatter_writebehind_finish(ScatterLock *lock, MutationRef& mut);

  xlist<ScatterLock*> updated_scatterlocks;
public:
  void mark_updated_scatterlock(ScatterLock *lock);


  void handle_reqrdlock(SimpleLock *lock, MLock *m);



  // caps

  // when to defer processing client cap release or writeback due to being
  // frozen.  the condition must be consistent across handle_client_caps and
  // process_request_cap_release to preserve ordering.
  bool should_defer_client_cap_frozen(CInode *in);

  void process_request_cap_release(MDRequestRef& mdr, client_t client, const ceph_mds_request_release& r,
				   const string &dname);

  void kick_cap_releases(MDRequestRef& mdr);
  void kick_issue_caps(CInode *in, client_t client, ceph_seq_t seq);

  void remove_client_cap(CInode *in, client_t client);

 protected:
  void adjust_cap_wanted(Capability *cap, int wanted, int issue_seq);
  void handle_client_caps(class MClientCaps *m);
  void _update_cap_fields(CInode *in, int dirty, MClientCaps *m, inode_t *pi);
  bool _do_cap_update(CInode *in, Capability *cap, int dirty, MClientCaps *m,
		      MClientCaps *ack=0);
  void handle_client_cap_release(class MClientCapRelease *m);
  void _do_cap_release(client_t client, inodeno_t ino, uint64_t cap_id, ceph_seq_t mseq, ceph_seq_t seq);


  // local
public:
  void local_wrlock_grab(LocalLock *lock, MutationRef& mut);
protected:
  bool local_wrlock_start(LocalLock *lock, MDRequestRef& mut);
  void local_wrlock_finish(LocalLock *lock, MutationImpl *mut);
  bool local_xlock_start(LocalLock *lock, MDRequestRef& mut);
  void local_xlock_finish(LocalLock *lock, MutationImpl *mut);


  // file
public:
  void file_eval(ScatterLock *lock, bool *need_issue);
protected:
  void handle_file_lock(ScatterLock *lock, MLock *m);
  void scatter_mix(ScatterLock *lock, bool *need_issue=0);
  void file_excl(ScatterLock *lock, bool *need_issue=0);
  void file_xsyn(SimpleLock *lock, bool *need_issue=0);

public:
  void file_recover(ScatterLock *lock);

private:
  xlist<ScatterLock*> updated_filelocks;
public:
  void mark_updated_Filelock(ScatterLock *lock);

  // -- file i/o --
 public:
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, Session *session,
			     bool is_replay);
  bool issue_caps(CInode *in, Capability *only_cap=0);
  void issue_caps_set(set<CInode*>& inset);
  void issue_truncate(CInode *in);
  void revoke_stale_caps(Session *session);
  void resume_stale_caps(Session *session);
  void remove_stale_leases(Session *session);

public:
  void request_inode_file_caps(CInode *in);
protected:
  void handle_inode_file_caps(class MInodeFileCaps *m);

  void file_update_finish(CInode *in, MutationRef& mut, bool share, client_t client, Capability *cap,
			  MClientCaps *ack);
public:
  void calc_new_client_ranges(CInode *in, uint64_t size, map<client_t, client_writeable_range_t>& new_ranges);
  bool check_inode_max_size(CInode *in, bool force_wrlock=false,
			    bool update_size=false, uint64_t newsize=0,
			    bool update_max=false, uint64_t newmax=0,
			    utime_t mtime=utime_t());
  void share_inode_max_size(CInode *in, Capability *only_cap=0);

private:
  friend class C_MDL_CheckMaxSize;
  friend class C_MDL_RequestInodeFileCaps;
  friend struct C_Locker_FileUpdate_finish;
  friend class C_Locker_RetryCapRelease;


  // -- client leases --
public:
  void handle_client_lease(struct MClientLease *m);

  void issue_client_lease(CDentry *dn, client_t client, bufferlist &bl, utime_t now, Session *session);
  void revoke_client_leases(SimpleLock *lock);
};


#endif
