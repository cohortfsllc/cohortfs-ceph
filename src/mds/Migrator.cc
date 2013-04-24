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

#include "MDS.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Migrator.h"
#include "Locker.h"
#include "Server.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"

#include "include/filepath.h"

#include "events/EString.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/ESessions.h"

#include "msg/Messenger.h"

#include "messages/MClientCaps.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"


/*
 * this is what the dir->dir_auth values look like
 *
 *   dir_auth  authbits  
 * export
 *   me         me      - before
 *   me, me     me      - still me, but preparing for export
 *   me, them   me      - send MExportDir (peer is preparing)
 *   them, me   me      - journaled EExport
 *   them       them    - done
 *
 * import:
 *   them       them    - before
 *   me, them   me      - journaled EImportStart
 *   me         me      - done
 *
 * which implies:
 *  - auth bit is set if i am listed as first _or_ second dir_auth.
 */

#include "common/config.h"


#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) (l <= cct->_conf->debug_mds || l <= cct->_conf->debug_mds_migrator)
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".migrator "

/* This function DOES put the passed message before returning*/
void Migrator::dispatch(Message *m)
{
  m->put();
}
#if 0
  switch (m->get_type()) {
    // import
  case MSG_MDS_EXPORTDIRDISCOVER:
    handle_export_discover((MExportDirDiscover*)m);
    break;
  case MSG_MDS_EXPORTDIRPREP:
    handle_export_prep((MExportDirPrep*)m);
    break;
  case MSG_MDS_EXPORTDIR:
    handle_export_dir((MExportDir*)m);
    break;
  case MSG_MDS_EXPORTDIRFINISH:
    handle_export_finish((MExportDirFinish*)m);
    break;
  case MSG_MDS_EXPORTDIRCANCEL:
    handle_export_cancel((MExportDirCancel*)m);
    break;

    // export 
  case MSG_MDS_EXPORTDIRDISCOVERACK:
    handle_export_discover_ack((MExportDirDiscoverAck*)m);
    break;
  case MSG_MDS_EXPORTDIRPREPACK:
    handle_export_prep_ack((MExportDirPrepAck*)m);
    break;
  case MSG_MDS_EXPORTDIRACK:
    handle_export_ack((MExportDirAck*)m);
    break;
  case MSG_MDS_EXPORTDIRNOTIFYACK:
    handle_export_notify_ack((MExportDirNotifyAck*)m);
    break;    

    // export 3rd party (dir_auth adjustments)
  case MSG_MDS_EXPORTDIRNOTIFY:
    handle_export_notify((MExportDirNotify*)m);
    break;

    // caps
  case MSG_MDS_EXPORTCAPS:
    handle_export_caps((MExportCaps*)m);
    break;
  case MSG_MDS_EXPORTCAPSACK:
    handle_export_caps_ack((MExportCapsAck*)m);
    break;

  default:
    assert(0);
  }
}

class C_MDC_EmptyImport : public Context {
  Migrator *mig;
  CStripe *stripe;
public:
  C_MDC_EmptyImport(Migrator *mig, CStripe *stripe)
      : mig(mig), stripe(stripe) {}
  void finish(int r) {
    mig->export_empty_import(stripe);
  }
};


void Migrator::export_empty_import(CStripe *stripe)
{
  dout(7) << "export_empty_import " << *stripe << dendl;
  assert(stripe->is_subtree_root());

  if (stripe->get_inode()->is_auth()) {
    dout(7) << " inode is auth" << dendl;
    return;
  }
  if (!stripe->is_auth()) {
    dout(7) << " not auth" << dendl;
    return;
  }
  if (stripe->is_freezing_or_frozen()) {
    dout(7) << " freezing or frozen" << dendl;
    return;
  }
  if (stripe->get_num_head_items() > 0) {
    dout(7) << " not actually empty" << dendl;
    return;
  }
  if (stripe->get_inode()->is_root()) {
    dout(7) << " root" << dendl;
    return;
  }
 
  int dest = stripe->get_inode()->authority().first;
  //if (mds->is_shutting_down()) dest = 0;  // this is more efficient.
 
  dout(7) << " really empty, exporting to " << dest << dendl;
  assert (dest != mds->get_nodeid());
 
  dout(7) << "exporting to mds." << dest 
           << " empty import " << *stripe << dendl;
  export_dir(stripe, dest);
}




// ==========================================================
// mds failure handling

void Migrator::handle_mds_failure_or_stop(int who)
{
  dout(5) << "handle_mds_failure_or_stop mds." << who << dendl;

  // check my exports

  // first add an extra auth_pin on any freezes, so that canceling a
  // nested freeze doesn't complete one further up the hierarchy and
  // confuse the shit out of us.  we'll remove it after canceling the
  // freeze.  this way no freeze completions run before we want them
  // to.
  list<CStripe*> pinned_dirs;
  for (map<CStripe*,int>::iterator p = export_state.begin();
       p != export_state.end();
       ++p) {
    if (p->second == EXPORT_FREEZING) {
      CStripe *stripe = p->first;
      dout(10) << "adding temp auth_pin on freezing " << *stripe << dendl;
      stripe->auth_pin(this);
      pinned_dirs.push_back(stripe);
    }
  }

  map<CStripe*,int>::iterator p = export_state.begin();
  while (p != export_state.end()) {
    map<CStripe*,int>::iterator next = p;
    next++;
    CStripe *stripe = p->first;

    // abort exports:
    //  - that are going to the failed node
    //  - that aren't frozen yet (to avoid auth_pin deadlock)
    //  - they havne't prepped yet (they may need to discover bounds to do that)
    if (export_peer[stripe] == who ||
	p->second == EXPORT_DISCOVERING ||
	p->second == EXPORT_FREEZING ||
	p->second == EXPORT_PREPPING) {
      // the guy i'm exporting to failed, or we're just freezing.
      dout(10) << "cleaning up export state (" << p->second << ")" << get_export_statename(p->second)
	       << " of " << *stripe << dendl;
      
      switch (p->second) {
      case EXPORT_DISCOVERING:
	dout(10) << "export state=discovering : canceling freeze and removing auth_pin" << dendl;
	stripe->unfreeze();  // cancel the freeze
	stripe->auth_unpin(this);
	export_state.erase(stripe); // clean up
	export_unlock(stripe);
	export_locks.erase(stripe);
	stripe->state_clear(CStripe::STATE_EXPORTING);
	if (export_peer[stripe] != who) // tell them.
	  mds->send_message_mds(new MExportDirCancel(stripe->dirstripe()), export_peer[stripe]);
	break;
	
      case EXPORT_FREEZING:
	dout(10) << "export state=freezing : canceling freeze" << dendl;
	stripe->unfreeze();  // cancel the freeze
	export_state.erase(stripe); // clean up
	stripe->state_clear(CStripe::STATE_EXPORTING);
	if (export_peer[stripe] != who) // tell them.
	  mds->send_message_mds(new MExportDirCancel(stripe->dirstripe()), export_peer[stripe]);
	break;

	// NOTE: state order reversal, warning comes after loggingstart+prepping
      case EXPORT_WARNING:
	dout(10) << "export state=warning : unpinning bounds, unfreezing, notifying" << dendl;
	// fall-thru

      case EXPORT_PREPPING:
	if (p->second != EXPORT_WARNING) 
	  dout(10) << "export state=loggingstart|prepping : unpinning bounds, unfreezing" << dendl;
	{
	  // unpin bounds
	  set<CStripe*> bounds;
	  cache->get_subtree_bounds(stripe, bounds);
	  for (set<CStripe*>::iterator p = bounds.begin();
	       p != bounds.end();
	       ++p) {
	    CStripe *bd = *p;
	    bd->put(CStripe::PIN_EXPORTBOUND);
	    bd->state_clear(CStripe::STATE_EXPORTBOUND);
	  }
	}
	stripe->unfreeze();
	export_state.erase(stripe); // clean up
	cache->adjust_subtree_auth(stripe, mds->get_nodeid());
	cache->try_subtree_merge(stripe);  // NOTE: this may journal subtree_map as side effect
	export_unlock(stripe);
	export_locks.erase(stripe);
	stripe->state_clear(CStripe::STATE_EXPORTING);
	if (export_peer[stripe] != who) // tell them.
	  mds->send_message_mds(new MExportDirCancel(stripe->dirstripe()), export_peer[stripe]);
	break;
	
      case EXPORT_EXPORTING:
	dout(10) << "export state=exporting : reversing, and unfreezing" << dendl;
	export_reverse(stripe);
	export_state.erase(stripe); // clean up
	export_locks.erase(stripe);
	stripe->state_clear(CStripe::STATE_EXPORTING);
	break;

      case EXPORT_LOGGINGFINISH:
      case EXPORT_NOTIFYING:
	dout(10) << "export state=loggingfinish|notifying : ignoring dest failure, we were successful." << dendl;
	// leave export_state, don't clean up now.
	break;

      default:
	assert(0);
      }

      // finish clean-up?
      if (export_state.count(stripe) == 0) {
	export_peer.erase(stripe);
	export_warning_ack_waiting.erase(stripe);
	export_notify_ack_waiting.erase(stripe);

	// wake up any waiters
	mds->queue_waiters(export_finish_waiters[stripe]);
	export_finish_waiters.erase(stripe);

	// send pending import_maps?  (these need to go out when all exports have finished.)
	cache->maybe_send_pending_resolves();

	cache->show_subtrees();

	maybe_do_queued_export();
      }
    } else {
      // bystander failed.
      if (export_warning_ack_waiting.count(stripe) &&
	  export_warning_ack_waiting[stripe].count(who)) {
	export_warning_ack_waiting[stripe].erase(who);
	export_notify_ack_waiting[stripe].erase(who);   // they won't get a notify either.
	if (p->second == EXPORT_WARNING) {
	  // exporter waiting for warning acks, let's fake theirs.
	  dout(10) << "faking export_warning_ack from mds." << who
		   << " on " << *stripe << " to mds." << export_peer[stripe] 
		   << dendl;
	  if (export_warning_ack_waiting[stripe].empty()) 
	    export_go(stripe);
	}
      }
      if (export_notify_ack_waiting.count(stripe) &&
	  export_notify_ack_waiting[stripe].count(who)) {
	export_notify_ack_waiting[stripe].erase(who);
	if (p->second == EXPORT_NOTIFYING) {
	  // exporter is waiting for notify acks, fake it
	  dout(10) << "faking export_notify_ack from mds." << who
		   << " on " << *stripe << " to mds." << export_peer[stripe] 
		   << dendl;
	  if (export_notify_ack_waiting[stripe].empty()) 
	    export_finish(stripe);
	}
      }
    }
    
    // next!
    p = next;
  }


  // check my imports
  map<dirstripe_t,int>::iterator q = import_state.begin();
  while (q != import_state.end()) {
    map<dirstripe_t,int>::iterator next = q;
    next++;
    dirstripe_t ds = q->first;
    CInode *diri = mds->mdcache->get_inode(ds.ino);
    CStripe *stripe = mds->mdcache->get_dirstripe(ds);

    if (import_peer[ds] == who) {
      if (stripe)
	dout(10) << "cleaning up import state (" << q->second << ")"
            << get_import_statename(q->second) << " of " << *stripe << dendl;
      else
	dout(10) << "cleaning up import state (" << q->second << ")"
            << get_import_statename(q->second) << " of " << ds << dendl;

      switch (q->second) {
      case IMPORT_DISCOVERING:
	dout(10) << "import state=discovering : clearing state" << dendl;
	import_reverse_discovering(ds);
	break;

      case IMPORT_DISCOVERED:
	assert(diri);
	dout(10) << "import state=discovered : unpinning inode " << *diri << dendl;
	import_reverse_discovered(ds, diri);
	break;

      case IMPORT_PREPPING:
	assert(stripe);
	dout(10) << "import state=prepping : unpinning base+bounds " << *stripe << dendl;
	import_reverse_prepping(stripe);
	break;

      case IMPORT_PREPPED:
	assert(stripe);
	dout(10) << "import state=prepped : unpinning base+bounds, unfreezing " << *stripe << dendl;
	{
	  set<CStripe*> bounds;
	  cache->get_subtree_bounds(stripe, bounds);
	  import_remove_pins(stripe, bounds);
	  
	  // adjust auth back to me
	  cache->adjust_subtree_auth(stripe, import_peer[ds]);
	  cache->try_subtree_merge(stripe);   // NOTE: may journal subtree_map as side-effect

	  // bystanders?
	  if (import_bystanders[stripe].empty()) {
	    import_reverse_unfreeze(stripe);
	  } else {
	    // notify them; wait in aborting state
	    import_notify_abort(stripe, bounds);
	    import_state[ds] = IMPORT_ABORTING;
	    assert(g_conf->mds_kill_import_at != 10);
	  }
	}
	break;

      case IMPORT_LOGGINGSTART:
	dout(10) << "import state=loggingstart : reversing import on " << *stripe << dendl;
	import_reverse(stripe);
	break;

      case IMPORT_ACKING:
	// hrm.  make this an ambiguous import, and wait for exporter recovery to disambiguate
	dout(10) << "import state=acking : noting ambiguous import " << *stripe << dendl;
	{
	  set<CStripe*> bounds;
	  cache->get_subtree_bounds(stripe, bounds);
	  cache->add_ambiguous_import(stripe, bounds);
	}
	break;
	
      case IMPORT_ABORTING:
	dout(10) << "import state=aborting : ignoring repeat failure " << *stripe << dendl;
	break;
      }
    } else {
      if (q->second == IMPORT_ABORTING &&
	  import_bystanders[stripe].count(who)) {
	dout(10) << "faking export_notify_ack from mds." << who
		 << " on aborting import " << *stripe
                 << " from mds." << import_peer[ds] << dendl;
	import_bystanders[stripe].erase(who);
	if (import_bystanders[stripe].empty()) {
	  import_bystanders.erase(stripe);
	  import_reverse_unfreeze(stripe);
	}
      }
    }

    // next!
    q = next;
  }

  while (!pinned_dirs.empty()) {
    CStripe *stripe = pinned_dirs.front();
    dout(10) << "removing temp auth_pin on " << *stripe << dendl;
    stripe->auth_unpin(this);
    pinned_dirs.pop_front();
  }  
}



void Migrator::show_importing()
{  
  dout(10) << "show_importing" << dendl;
  for (map<dirstripe_t,int>::iterator p = import_state.begin();
       p != import_state.end();
       p++) {
    CStripe *stripe = mds->mdcache->get_dirstripe(p->first);
    if (stripe) {
      dout(10) << " importing from " << import_peer[p->first]
	       << ": (" << p->second << ") " << get_import_statename(p->second) 
	       << " " << p->first
	       << " " << *stripe
	       << dendl;
    } else {
      dout(10) << " importing from " << import_peer[p->first]
	       << ": (" << p->second << ") " << get_import_statename(p->second) 
	       << " " << p->first 
	       << dendl;
    }
  }
}

void Migrator::show_exporting() 
{
  dout(10) << "show_exporting" << dendl;
  for (map<CStripe*,int>::iterator p = export_state.begin();
       p != export_state.end();
       p++) 
    dout(10) << " exporting to " << export_peer[p->first]
	     << ": (" << p->second << ") " << get_export_statename(p->second) 
	     << " " << p->first->dirstripe()
	     << " " << *p->first
	     << dendl;
}



void Migrator::audit()
{
  if (!g_conf->subsys.should_gather(ceph_subsys_mds, 5))
    return;  // hrm.

  // import_state
  show_importing();
  for (map<dirstripe_t,int>::iterator p = import_state.begin();
       p != import_state.end();
       p++) {
    if (p->second == IMPORT_DISCOVERING) 
      continue;
    if (p->second == IMPORT_DISCOVERED) {
      CInode *in = cache->get_inode(p->first.ino);
      assert(in);
      continue;
    }
    CStripe *stripe = cache->get_dirstripe(p->first);
    assert(stripe);
    if (p->second == IMPORT_PREPPING) 
      continue;
    if (p->second == IMPORT_ABORTING) {
      assert(!stripe->is_ambiguous_stripe_auth());
      assert(stripe->get_stripe_auth().first != mds->get_nodeid());
      continue;
    }
    assert(stripe->is_ambiguous_stripe_auth());
    assert(stripe->authority().first  == mds->get_nodeid() ||
	   stripe->authority().second == mds->get_nodeid());
  }

  // export_state
  show_exporting();
  for (map<CStripe*,int>::iterator p = export_state.begin();
       p != export_state.end();
       p++) {
    CStripe *stripe = p->first;
    if (p->second == EXPORT_DISCOVERING ||
	p->second == EXPORT_FREEZING) continue;
    assert(stripe->is_ambiguous_stripe_auth());
    assert(stripe->authority().first  == mds->get_nodeid() ||
	   stripe->authority().second == mds->get_nodeid());
  }

  // ambiguous+me subtrees should be importing|exporting

  // write me
}





// ==========================================================
// EXPORT

void Migrator::export_dir_nicely(CStripe *stripe, int dest)
{
  // enqueue
  dout(7) << "export_dir_nicely " << *stripe << " to " << dest << dendl;
  export_queue.push_back(make_pair(stripe->dirstripe(), dest));

  maybe_do_queued_export();
}

void Migrator::maybe_do_queued_export()
{
  while (!export_queue.empty() &&
         export_state.size() <= 4) {
    dirstripe_t ds = export_queue.front().first;
    int dest = export_queue.front().second;
    export_queue.pop_front();

    CStripe *stripe = mds->mdcache->get_dirstripe(ds);
    if (!stripe) continue;
    if (!stripe->is_auth()) continue;

    dout(0) << "nicely exporting to mds." << dest << " " << *stripe << dendl;

    export_dir(stripe, dest);
  }
}




class C_MDC_ExportFreeze : public Context {
  Migrator *mig;
  CStripe *ex;   // dir i'm exporting

public:
  C_MDC_ExportFreeze(Migrator *m, CStripe *e) : mig(m), ex(e) {}
  virtual void finish(int r) {
    if (r >= 0)
      mig->export_frozen(ex);
  }
};


void Migrator::get_export_lock_set(CStripe *stripe, set<SimpleLock*>& locks)
{
  // path
  vector<CDentry*> trace;
  cache->make_trace(trace, stripe->get_inode());
  for (vector<CDentry*>::iterator it = trace.begin();
       it != trace.end();
       it++)
    locks.insert(&(*it)->lock);

  // bound dftlocks:
  // NOTE: We need to take an rdlock on bounding dirfrags during
  //  migration for a rather irritating reason: when we export the
  //  bound inode, we need to send scatterlock state for the dirfrags
  //  as well, so that the new auth also gets the correct info.  If we
  //  race with a refragment, this info is useless, as we can't
  //  redivvy it up.  And it's needed for the scatterlocks to work
  //  properly: when the auth is in a sync/lock state it keeps each
  //  dirfrag's portion in the local (auth OR replica) dirfrag.
  set<CStripe*> wouldbe_bounds;
  cache->get_wouldbe_subtree_bounds(stripe, wouldbe_bounds);
  for (set<CStripe*>::iterator p = wouldbe_bounds.begin(); p != wouldbe_bounds.end(); ++p)
    locks.insert(&(*p)->dirfragtreelock);
}


/** export_dir(dir, dest)
 * public method to initiate an export.
 * will fail if the directory is freezing, frozen, unpinnable, or root. 
 */
void Migrator::export_dir(CStripe *stripe, int dest)
{
  dout(7) << "export_dir " << *stripe << " to " << dest << dendl;
  assert(stripe->is_auth());
  assert(dest != mds->get_nodeid());
   
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "cluster degraded, no exports for now" << dendl;
    return;
  }
  if (stripe->get_inode()->is_system()) {
    dout(7) << "i won't export system dirs (root, mdsdirs, stray, /.ceph, etc.)" << dendl;
    //assert(0);
    return;
  }

  if (!stripe->get_inode()->is_base() &&
      stripe->get_parent_stripe()->get_inode()->is_stray() &&
      stripe->get_parent_stripe()->get_parent_stripe()->get_inode()->ino() != MDS_INO_MDSDIR(dest)) {
    dout(7) << "i won't export anything in stray" << dendl;
    return;
  }

  if (stripe->is_freezing_or_frozen()) {
    dout(7) << " can't export, freezing|frozen.  wait for other exports to finish first." << dendl;
    return;
  }
  if (stripe->state_test(CStripe::STATE_EXPORTING)) {
    dout(7) << "already exporting" << dendl;
    return;
  }
  
  // locks?
  set<SimpleLock*> locks;
  get_export_lock_set(stripe, locks);
  if (!mds->locker->rdlock_try_set(locks)) {
    dout(7) << "export_dir can't rdlock needed locks, failing." << dendl;
    return;
  }
  mds->locker->rdlock_take_set(locks);
  export_locks[stripe].swap(locks);

  // ok.
  assert(export_state.count(stripe) == 0);
  export_state[stripe] = EXPORT_DISCOVERING;
  export_peer[stripe] = dest;

  stripe->state_set(CStripe::STATE_EXPORTING);
  assert(g_conf->mds_kill_export_at != 1);

  // send ExportDirDiscover (ask target)
  filepath path;
  stripe->get_inode()->make_path(path);
  mds->send_message_mds(new MExportDirDiscover(mds->get_nodeid(), path,
                                               stripe->dirstripe()), dest);
  assert(g_conf->mds_kill_export_at != 2);

  // start the freeze, but hold it up with an auth_pin.
  stripe->auth_pin(this);
  stripe->freeze();
  assert(stripe->is_freezing());
  stripe->add_waiter(CStripe::WAIT_FROZEN, new C_MDC_ExportFreeze(this, stripe));
}


/*
 * called on receipt of MExportDirDiscoverAck
 * the importer now has the directory's _inode_ in memory, and pinned.
 *
 * This function DOES put the passed message before returning
 */
void Migrator::handle_export_discover_ack(MExportDirDiscoverAck *m)
{
  CStripe *stripe = cache->get_dirstripe(m->get_dirstripe());
  assert(stripe);
  
  dout(7) << "export_discover_ack from " << m->get_source()
	  << " on " << *stripe << dendl;

  if (export_state.count(stripe) == 0 ||
      export_state[stripe] != EXPORT_DISCOVERING ||
      export_peer[stripe] != m->get_source().num()) {
    dout(7) << "must have aborted" << dendl;
  } else {
    // release locks to avoid deadlock
    export_unlock(stripe);
    export_locks.erase(stripe);
    // freeze the subtree
    export_state[stripe] = EXPORT_FREEZING;
    stripe->auth_unpin(this);
    assert(g_conf->mds_kill_export_at != 3);
  }
  
  m->put();  // done
}

void Migrator::export_frozen(CStripe *stripe)
{
  dout(7) << "export_frozen on " << *stripe << dendl;
  assert(stripe->is_frozen());
  assert(stripe->get_cum_auth_pins() == 0);

  int dest = export_peer[stripe];
  CInode *diri = stripe->get_inode();

  // ok, try to grab all my locks.
  set<SimpleLock*> locks;
  get_export_lock_set(stripe, locks);
  if (!mds->locker->can_rdlock_set(locks)) {
    dout(7) << "export_dir couldn't rdlock all needed locks, failing. " 
	    << *diri << dendl;

    // .. unwind ..
    export_peer.erase(stripe);
    export_state.erase(stripe);
    stripe->unfreeze();
    stripe->state_clear(CStripe::STATE_EXPORTING);

    mds->queue_waiters(export_finish_waiters[stripe]);
    export_finish_waiters.erase(stripe);

    mds->send_message_mds(new MExportDirCancel(stripe->dirstripe()), dest);
    return;
  }
  mds->locker->rdlock_take_set(locks);
  export_locks[stripe].swap(locks);
  
  cache->show_subtrees();

  // note the bounds.
  //  force it into a subtree by listing auth as <me,me>.
  cache->adjust_subtree_auth(stripe, mds->get_nodeid(), mds->get_nodeid());
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);

  // generate prep message, log entry.
  MExportDirPrep *prep = new MExportDirPrep(stripe->dirstripe());

  // include list of bystanders
  for (map<int,int>::iterator p = stripe->replicas_begin();
       p != stripe->replicas_end();
       p++) {
    if (p->first != dest) {
      dout(10) << "bystander mds." << p->first << dendl;
      prep->add_bystander(p->first);
    }
  }

  // include base stripe
  cache->replicate_stripe(stripe, dest, prep->base);
 
  /*
   * include spanning tree for all nested exports.
   * these need to be on the destination _before_ the final export so that
   * dir_auth updates on any nested exports are properly absorbed.
   * this includes inodes and dirfrags included in the subtree, but
   * only the inodes at the bounds.
   *
   * each trace is: ds ('-' | ('s' stripe dir | ('f' dir | 'd' df)) dentry inode (stripe dir dentry inode)*)
   */
  set<inodeno_t> inodes_added;
  set<dirfrag_t> dirfrags_added;
  set<dirstripe_t> dirstripes_added;

  // check bounds
  for (set<CStripe*>::iterator it = bounds.begin(); it != bounds.end(); it++) {
    CStripe *bound = *it;

    // pin it.
    bound->get(CStripe::PIN_EXPORTBOUND);
    bound->state_set(CStripe::STATE_EXPORTBOUND);

    dout(7) << "  export bound " << *bound << dendl;
    prep->add_bound(bound->dirstripe());

    // trace to bound
    bufferlist tracebl;
    CStripe *cur = bound;

    char start = '-';
    while (1) {
      CInode *in = cur->get_inode();
      
      // don't repeat inodes
      if (inodes_added.count(in->ino()))
	break;
      inodes_added.insert(in->ino());

      // prepend dentry + inode
      assert(in->is_auth());
      bufferlist bl;
      cache->replicate_dentry(in->get_parent_dn(), dest, bl);
      dout(7) << "  added " << *in->get_parent_dn() << dendl;
      cache->replicate_inode(in, dest, bl);
      dout(7) << "  added " << *in << dendl;
      bl.claim_append(tracebl);
      tracebl.claim(bl);

      CDir *dir = in->get_parent_dir();

      // don't repeat dirfrags
      if (dirfrags_added.count(dir->dirfrag())) {
	start = 'd';  // start with dentry
        // prepend dirfrag
        ::encode(dir->dirfrag(), bl);
        bl.claim_append(tracebl);
        tracebl.claim(bl);
	break;
      }
      dirfrags_added.insert(dir->dirfrag());

      // prepend dir
      cache->replicate_dir(dir, dest, bl);
      dout(7) << "  added " << *dir << dendl;
      bl.claim_append(tracebl);
      tracebl.claim(bl);

      cur = dir->get_stripe();

      // don't repeat stripes
      if (dirstripes_added.count(cur->dirstripe()) || cur == stripe) {
        start = 'f';  // start with dirfrag
        break;
      }
      dirstripes_added.insert(cur->dirstripe());

      // prepend stripe
      cache->replicate_stripe(cur, dest, bl);
      dout(7) << "  added " << *cur << dendl;
      bl.claim_append(tracebl);
      tracebl.claim(bl);

      start = 's'; // start with stripe
    }
    bufferlist final;
    dirstripe_t ds = cur->dirstripe();
    ::encode(ds, final);
    ::encode(start, final);
    final.claim_append(tracebl);
    prep->add_trace(final);
  }

  // send.
  export_state[stripe] = EXPORT_PREPPING;
  mds->send_message_mds(prep, dest);
  assert (g_conf->mds_kill_export_at != 4);
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_prep_ack(MExportDirPrepAck *m)
{
  CStripe *stripe = cache->get_dirstripe(m->get_dirstripe());
  assert(stripe);

  dout(7) << "export_prep_ack " << *stripe << dendl;

  if (export_state.count(stripe) == 0 ||
      export_state[stripe] != EXPORT_PREPPING) {
    // export must have aborted.  
    dout(7) << "export must have aborted" << dendl;
    m->put();
    return;
  }

  assert (g_conf->mds_kill_export_at != 5);
  // send warnings
  int dest = export_peer[stripe];
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);

  assert(export_peer.count(stripe));
  assert(export_warning_ack_waiting.count(stripe) == 0);
  assert(export_notify_ack_waiting.count(stripe) == 0);

  for (map<int,int>::iterator p = stripe->replicas_begin();
       p != stripe->replicas_end();
       ++p) {
    if (p->first == dest) continue;
    if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(p->first))
      continue;  // only if active
    export_warning_ack_waiting[stripe].insert(p->first);
    export_notify_ack_waiting[stripe].insert(p->first);  // we'll eventually get a notifyack, too!

    MExportDirNotify *notify = new MExportDirNotify(stripe->dirstripe(), true,
						    make_pair(mds->get_nodeid(),CDIR_AUTH_UNKNOWN),
						    make_pair(mds->get_nodeid(),export_peer[stripe]));
    for (set<CStripe*>::iterator i = bounds.begin(); i != bounds.end(); i++)
      notify->get_bounds().push_back((*i)->dirstripe());
    mds->send_message_mds(notify, p->first);
    
  }
  export_state[stripe] = EXPORT_WARNING;

  assert(g_conf->mds_kill_export_at != 6);
  // nobody to warn?
  if (export_warning_ack_waiting.count(stripe) == 0) 
    export_go(stripe);  // start export.
    
  // done.
  m->put();
}


class C_M_ExportGo : public Context {
  Migrator *migrator;
  CStripe *stripe;
public:
  C_M_ExportGo(Migrator *m, CStripe *s) : migrator(m), stripe(s) {}
  void finish(int r) {
    migrator->export_go_synced(stripe);
  }
};

void Migrator::export_go(CStripe *stripe)
{
  assert(export_peer.count(stripe));
  int dest = export_peer[stripe];
  dout(7) << "export_go " << *stripe << " to " << dest << dendl;

  // first sync log to flush out e.g. any cap imports
  mds->mdlog->wait_for_safe(new C_M_ExportGo(this, stripe));
  mds->mdlog->flush();
}

void Migrator::export_go_synced(CStripe *stripe)
{  
  if (export_state.count(stripe) == 0 ||
      export_state[stripe] != EXPORT_WARNING) {
    // export must have aborted.  
    dout(7) << "export must have aborted on " << *stripe << dendl;
    return;
  }

  assert(export_peer.count(stripe));
  int dest = export_peer[stripe];
  dout(7) << "export_go_synced " << *stripe << " to " << dest << dendl;

  cache->show_subtrees();
  
  export_warning_ack_waiting.erase(stripe);
  export_state[stripe] = EXPORT_EXPORTING;
  assert(g_conf->mds_kill_export_at != 7);

  assert(stripe->get_cum_auth_pins() == 0);

  // set ambiguous auth
  cache->adjust_subtree_auth(stripe, mds->get_nodeid(), dest);

  // take away the popularity we're sending.
  utime_t now = ceph_clock_now(g_ceph_context);
  mds->balancer->subtract_export(stripe, now);
 
  // fill export message with cache data
  MExportDir *req = new MExportDir(stripe->dirstripe());
  map<client_t,entity_inst_t> exported_client_map;
  int num_exported_inodes = encode_export_stripe(req->export_data, stripe,
                                                 exported_client_map, now);
  ::encode(exported_client_map, req->client_map);

  // add bounds to message
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);
  for (set<CStripe*>::iterator p = bounds.begin(); p != bounds.end(); ++p)
    req->add_export((*p)->dirstripe());

  // send
  dout(10) << "sending " << *req << " to mds." << dest << dendl;
  mds->send_message_mds(req, dest);
  assert(g_conf->mds_kill_export_at != 8);

  // stats
  if (mds->logger) mds->logger->inc(l_mds_ex);
  if (mds->logger) mds->logger->inc(l_mds_iexp, num_exported_inodes);

  cache->show_subtrees();
}

#endif

/** encode_export_inode
 * update our local state for this inode to export.
 * encode relevant state to be sent over the wire.
 * used by: encode_export_dir, file_rename (if foreign)
 *
 * FIXME: the separation between CInode.encode_export and these methods 
 * is pretty arbitrary and dumb.
 */
void Migrator::encode_export_inode(CInode *in, bufferlist& enc_state, 
				   map<client_t,entity_inst_t>& exported_client_map)
{
  dout(7) << "encode_export_inode " << *in << dendl;
  assert(!in->is_replica(mds->get_nodeid()));

  // relax locks?
  if (!in->is_replicated()) {
    in->replicate_relax_locks();
    dout(20) << " did replicate_relax_locks, now " << *in << dendl;
  }

  ::encode(in->inode.ino, enc_state);
  ::encode(in->last, enc_state);
  in->encode_export(enc_state);

  // caps 
  encode_export_inode_caps(in, enc_state, exported_client_map);
}

void Migrator::encode_export_inode_caps(CInode *in, bufferlist& bl, 
					map<client_t,entity_inst_t>& exported_client_map)
{
  dout(20) << "encode_export_inode_caps " << *in << dendl;

  // encode caps
  client_cap_export_map cap_map;
  in->export_client_caps(cap_map);
  ::encode(cap_map, bl);

  in->state_set(CInode::STATE_EXPORTINGCAPS);
  in->get(CInode::PIN_EXPORTINGCAPS);

  // make note of clients named by exported capabilities
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) 
    exported_client_map[it->first] = mds->sessionmap.get_inst(entity_name_t::CLIENT(it->first.v));
}

void Migrator::finish_export_inode_caps(CInode *in)
{
  dout(20) << "finish_export_inode_caps " << *in << dendl;

  in->state_clear(CInode::STATE_EXPORTINGCAPS);
  in->put(CInode::PIN_EXPORTINGCAPS);

  // tell (all) clients about migrating caps.. 
  for (map<client_t, Capability*>::iterator it = in->client_caps.begin();
       it != in->client_caps.end();
       ++it) {
    Capability *cap = it->second;
    dout(7) << "finish_export_inode telling client." << it->first
	    << " exported caps on " << *in << dendl;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_EXPORT,
				     in->ino(),
				     in->find_snaprealm()->inode->ino(),
				     cap->get_cap_id(), cap->get_last_seq(), 
				     cap->pending(), cap->wanted(), 0,
				     cap->get_mseq());
    mds->send_message_client_counted(m, it->first);
  }
  in->clear_client_caps_after_export();
  mds->locker->eval(in, CEPH_CAP_LOCKS);
}

void Migrator::finish_export_inode(CInode *in, utime_t now, list<Context*>& finished)
{
  dout(12) << "finish_export_inode " << *in << dendl;

  in->finish_export(now);

  finish_export_inode_caps(in);

  // clean
  if (in->is_dirty())
    in->mark_clean();
  
  // clear/unpin cached_by (we're no longer the authority)
  in->clear_replica_map();
  
  // twiddle lock states for auth -> replica transition
  in->authlock.export_twiddle();
  in->linklock.export_twiddle();
  in->filelock.export_twiddle();
  in->nestlock.export_twiddle();
  in->xattrlock.export_twiddle();
  in->snaplock.export_twiddle();
  in->flocklock.export_twiddle();
  in->policylock.export_twiddle();
  
  // mark auth
  assert(in->is_auth());
  in->state_clear(CInode::STATE_AUTH);
  in->replica_nonce = CInode::EXPORT_NONCE;
  
  in->clear_dirty_rstat();

  in->item_open_file.remove_myself();

  // waiters
  in->take_waiting(CInode::WAIT_ANY_MASK, finished);
  
  // *** other state too?

  // move to end of LRU so we drop out of cache quickly!
  if (in->get_parent_dn()) 
    cache->lru.lru_bottouch(in->get_parent_dn());

}

int Migrator::encode_export_dir(bufferlist& exportbl,
				CDir *dir,
				map<client_t,entity_inst_t>& exported_client_map,
				utime_t now)
{
  int num_exported = 0;

  dout(7) << "encode_export_dir " << *dir << " " << dir->get_num_head_items() << " head items" << dendl;
  
  assert(dir->get_projected_version() == dir->get_version());

#ifdef MDS_VERIFY_FRAGSTAT
  if (dir->is_complete())
    dir->verify_fragstat();
#endif

  // dir 
  dirfrag_t df = dir->dirfrag();
  ::encode(df, exportbl);
  dir->encode_export(exportbl);
  
  __u32 nden = dir->items.size();
  ::encode(nden, exportbl);
  
  // dentries
  list<CStripe*> subdirs;
  CDir::map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); ++it) {
    CDentry *dn = it->second;
    CInode *in = dn->get_linkage()->get_inode();
    
    if (!dn->is_replicated())
      dn->lock.replicate_relax();

    num_exported++;
    
    // -- dentry
    dout(7) << "encode_export_dir exporting " << *dn << dendl;
    
    // dn name
    ::encode(dn->name, exportbl);
    ::encode(dn->last, exportbl);
    
    // state
    dn->encode_export(exportbl);
    
    // points to...
    
    // null dentry?
    if (dn->get_linkage()->is_null()) {
      exportbl.append("N", 1);  // null dentry
      continue;
    }
    
    if (dn->get_linkage()->is_remote()) {
      // remote link
      exportbl.append("L", 1);  // remote link
      
      inodeno_t ino = dn->get_linkage()->get_remote_ino();
      unsigned char d_type = dn->get_linkage()->get_remote_d_type();
      ::encode(ino, exportbl);
      ::encode(d_type, exportbl);
      continue;
    }
    
    // primary link
    // -- inode
    exportbl.append("I", 1);    // inode dentry
    
    encode_export_inode(in, exportbl, exported_client_map);  // encode, and (update state for) export
    
    // directory?
    in->get_nested_stripes(subdirs);
    // XXX: ok to get_nested_stripes() instead of testing each for STATE_EXPORTBOUND?
  }

  // subdirs
  for (list<CStripe*>::iterator it = subdirs.begin(); it != subdirs.end(); ++it)
    num_exported += encode_export_stripe(exportbl, *it, exported_client_map, now);

  return num_exported;
}

void Migrator::finish_export_dir(CDir *dir, list<Context*>& finished, utime_t now)
{
  dout(10) << "finish_export_dir " << *dir << dendl;

  // release open_by 
  dir->clear_replica_map();

  // mark
  assert(dir->is_auth());
  dir->state_clear(CDir::STATE_AUTH);
  dir->remove_bloom();
  dir->replica_nonce = CDir::NONCE_EXPORT;

  if (dir->is_dirty())
    dir->mark_clean();
  
  // discard most dir state
  dir->state &= CDir::MASK_STATE_EXPORT_KEPT;  // i only retain a few things.

  // suck up all waiters
  dir->take_waiting(CDir::WAIT_ANY_MASK, finished);    // all dir waiters
  
  // pop
  dir->finish_export(now);

  // dentries
  list<CStripe*> subdirs;
  CDir::map_t::iterator it;
  for (it = dir->begin(); it != dir->end(); ++it) {
    CDentry *dn = it->second;
    CInode *in = dn->get_linkage()->get_inode();

    // dentry
    dn->finish_export();

    // inode?
    if (dn->get_linkage()->is_primary()) {
      finish_export_inode(in, now, finished);

      // subdirs?
      in->get_nested_stripes(subdirs);
    }
  }

  // subdirs
  for (list<CStripe*>::iterator it = subdirs.begin(); it != subdirs.end(); ++it)
    finish_export_stripe(*it, finished, now);
}

int Migrator::encode_export_stripe(bufferlist& bl, CStripe *stripe,
                                   map<client_t,entity_inst_t>& exported_client_map,
                                   utime_t now)
{
  int num_exported = 0;

  dout(10) << "encode_export_stripe " << *stripe << dendl;

  // encode stripe
  dirstripe_t ds = stripe->dirstripe();
  ::encode(ds, bl);
  stripe->encode_export(bl);

  list<CDir*> dirs;
  stripe->get_dirfrags(dirs);
  __u32 count = dirs.size();
  ::encode(count, bl);

  for (list<CDir*>::iterator i = dirs.begin(); i != dirs.end(); ++i)
    num_exported += encode_export_dir(bl, *i, exported_client_map, now);

  return num_exported;
}

void Migrator::finish_export_stripe(CStripe *stripe, list<Context*>& finished,
                                    utime_t now)
{
  dout(10) << "finish_export_stripe " << *stripe << dendl;

  // clear auth
  assert(stripe->is_auth());
  stripe->state_clear(CStripe::STATE_AUTH);
  stripe->clear_replica_map();
  stripe->replica_nonce = CStripe::EXPORT_NONCE;
  stripe->dirfragtreelock.export_twiddle();

  // clear dirty
  if (stripe->is_dirty())
    stripe->mark_clean();

  stripe->state &= CStripe::MASK_STATE_EXPORT_KEPT;
  stripe->take_waiting(CStripe::WAIT_ANY_MASK, finished);

  stripe->finish_export(now);

  list<CDir*> dirs;
  stripe->get_dirfrags(dirs);
  for (list<CDir*>::iterator i = dirs.begin(); i != dirs.end(); ++i)
    finish_export_dir(*i, finished, now);
}

#if 0

class C_MDS_ExportFinishLogged : public Context {
  Migrator *migrator;
  CStripe *stripe;
public:
  C_MDS_ExportFinishLogged(Migrator *m, CStripe *s) : migrator(m), stripe(s) {}
  void finish(int r) {
    migrator->export_logged_finish(stripe);
  }
};


/*
 * i should get an export_ack from the export target.
 *
 * This function DOES put the passed message before returning
 */
void Migrator::handle_export_ack(MExportDirAck *m)
{
  CStripe *stripe = cache->get_dirstripe(m->get_dirstripe());
  assert(stripe);
  assert(stripe->is_frozen_root());  // i'm exporting!

  // yay!
  dout(7) << "handle_export_ack " << *stripe << dendl;

  export_warning_ack_waiting.erase(stripe);
  
  export_state[stripe] = EXPORT_LOGGINGFINISH;
  assert (g_conf->mds_kill_export_at != 9);
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);

  // list us second, them first.
  // this keeps authority().first in sync with subtree auth state in the journal.
  int target = export_peer[stripe];
  cache->adjust_subtree_auth(stripe, target, mds->get_nodeid());

  // log completion. 
  //  include export bounds, to ensure they're in the journal.
  EExport *le = new EExport(mds->mdlog, stripe->dirstripe());
  mds->mdlog->start_entry(le);

  le->metablob.add_stripe_context(stripe, EMetaBlob::TO_ROOT);
  for (set<CStripe*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
    CStripe *bound = *p;
    le->get_bounds().insert(bound->dirstripe());
    le->metablob.add_stripe_context(bound);
  }

  // log export completion, then finish (unfreeze, trigger finish context, etc.)
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(new C_MDS_ExportFinishLogged(this, stripe));
  mds->mdlog->flush();
  assert (g_conf->mds_kill_export_at != 10);
  
  m->put();
}





/*
 * this happens if hte dest failes after i send teh export data but before it is acked
 * that is, we don't know they safely received and logged it, so we reverse our changes
 * and go on.
 */
void Migrator::export_reverse(CStripe *stripe)
{
  dout(7) << "export_reverse " << *stripe << dendl;
  
  assert(export_state[stripe] == EXPORT_EXPORTING);
  
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);

  // adjust auth, with possible subtree merge.
  cache->adjust_subtree_auth(stripe, mds->get_nodeid());
  cache->try_subtree_merge(stripe);  // NOTE: may journal subtree_map as side-effect

  // remove exporting pins
  list<CStripe*> stripes(1, stripe);
  while (!stripes.empty()) {
    CStripe *s = stripes.front(); 
    stripes.pop_front();
    s->abort_export();

    list<CDir*> dirs;
    s->get_dirfrags(dirs);
    for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d) {
      CDir *dir = *d;
      dir->abort_export();

      for (CDir::map_t::iterator p = dir->items.begin(); p != dir->items.end(); ++p) {
        p->second->abort_export();
        if (!p->second->get_linkage()->is_primary())
          continue;
        CInode *in = p->second->get_linkage()->get_inode();
        in->abort_export();
        if (in->is_dir())
          in->get_nested_stripes(stripes);
      }
    }
  }
  
  // unpin bounds
  for (set<CStripe*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
    CStripe *bd = *p;
    bd->put(CStripe::PIN_EXPORTBOUND);
    bd->state_clear(CStripe::STATE_EXPORTBOUND);
  }

  // process delayed expires
  cache->process_delayed_expire(stripe);
  
  // some clean up
  export_warning_ack_waiting.erase(stripe);
  export_notify_ack_waiting.erase(stripe);

  // unfreeze
  stripe->unfreeze();

  export_unlock(stripe);

  cache->show_cache();
}


/*
 * once i get the ack, and logged the EExportFinish(true),
 * send notifies (if any), otherwise go straight to finish.
 * 
 */
void Migrator::export_logged_finish(CStripe *stripe)
{
  dout(7) << "export_logged_finish " << *stripe << dendl;

  // send notifies
  int dest = export_peer[stripe];

  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);

  for (set<int>::iterator p = export_notify_ack_waiting[stripe].begin();
       p != export_notify_ack_waiting[stripe].end();
       ++p) {
    MExportDirNotify *notify;
    if (mds->mdsmap->is_clientreplay_or_active_or_stopping(export_peer[stripe])) 
      // dest is still alive.
      notify = new MExportDirNotify(stripe->dirstripe(), true,
				    make_pair(mds->get_nodeid(), dest),
				    make_pair(dest, CDIR_AUTH_UNKNOWN));
    else 
      // dest is dead.  bystanders will think i am only auth, as per mdcache->handle_mds_failure()
      notify = new MExportDirNotify(stripe->dirstripe(), true,
				    make_pair(mds->get_nodeid(), CDIR_AUTH_UNKNOWN),
				    make_pair(dest, CDIR_AUTH_UNKNOWN));

    for (set<CStripe*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
      notify->get_bounds().push_back((*i)->dirstripe());

    mds->send_message_mds(notify, *p);
  }

  // wait for notifyacks
  export_state[stripe] = EXPORT_NOTIFYING;
  assert (g_conf->mds_kill_export_at != 11);

  // no notifies to wait for?
  if (export_notify_ack_waiting[stripe].empty())
    export_finish(stripe);  // skip notify/notify_ack stage.
}

/*
 * warning:
 *  i'll get an ack from each bystander.
 *  when i get them all, do the export.
 * notify:
 *  i'll get an ack from each bystander.
 *  when i get them all, unfreeze and send the finish.
 *
 * This function DOES put the passed message before returning
 */
void Migrator::handle_export_notify_ack(MExportDirNotifyAck *m)
{
  CStripe *stripe = cache->get_dirstripe(m->get_dirstripe());
  assert(stripe);
  int from = m->get_source().num();

  if (export_state.count(stripe) && export_state[stripe] == EXPORT_WARNING) {
    // exporting. process warning.
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": exporting, processing warning on "
	    << *stripe << dendl;
    assert(export_warning_ack_waiting.count(stripe));
    export_warning_ack_waiting[stripe].erase(from);
    
    if (export_warning_ack_waiting[stripe].empty()) 
      export_go(stripe);     // start export.
  } 
  else if (export_state.count(stripe) && export_state[stripe] == EXPORT_NOTIFYING) {
    // exporting. process notify.
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": exporting, processing notify on "
	    << *stripe << dendl;
    assert(export_notify_ack_waiting.count(stripe));
    export_notify_ack_waiting[stripe].erase(from);
    
    if (export_notify_ack_waiting[stripe].empty())
      export_finish(stripe);
  }
  else if (import_state.count(stripe->dirstripe()) && import_state[stripe->dirstripe()] == IMPORT_ABORTING) {
    // reversing import
    dout(7) << "handle_export_notify_ack from " << m->get_source()
	    << ": aborting import on "
	    << *stripe << dendl;
    assert(import_bystanders[stripe].count(from));
    import_bystanders[stripe].erase(from);
    if (import_bystanders[stripe].empty()) {
      import_bystanders.erase(stripe);
      import_reverse_unfreeze(stripe);
    }
  }

  m->put();
}

void Migrator::export_unlock(CStripe *stripe)
{
  dout(10) << "export_unlock " << *stripe << dendl;

  mds->locker->rdlock_finish_set(export_locks[stripe]);

  list<Context*> ls;
  mds->queue_waiters(ls);
}

void Migrator::export_finish(CStripe *stripe)
{
  dout(5) << "export_finish " << *stripe << dendl;

  assert (g_conf->mds_kill_export_at != 12);
  if (export_state.count(stripe) == 0) {
    dout(7) << "target must have failed, not sending final commit message.  export succeeded anyway." << dendl;
    return;
  }

  // send finish/commit to new auth
  if (mds->mdsmap->is_clientreplay_or_active_or_stopping(export_peer[stripe])) {
    mds->send_message_mds(new MExportDirFinish(stripe->dirstripe()), export_peer[stripe]);
  } else {
    dout(7) << "not sending MExportDirFinish, dest has failed" << dendl;
  }
  assert(g_conf->mds_kill_export_at != 13);
  
  // finish export (adjust local cache state)
  C_Contexts *fin = new C_Contexts(g_ceph_context);
  finish_export_stripe(stripe, fin->contexts, ceph_clock_now(g_ceph_context));
  stripe->add_waiter(CStripe::WAIT_UNFREEZE, fin);

  // unfreeze
  dout(7) << "export_finish unfreezing" << dendl;
  stripe->unfreeze();
  
  // unpin bounds
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);
  for (set<CStripe*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
    CStripe *bd = *p;
    bd->put(CStripe::PIN_EXPORTBOUND);
    bd->state_clear(CStripe::STATE_EXPORTBOUND);
  }

  // adjust auth, with possible subtree merge.
  //  (we do this _after_ removing EXPORTBOUND pins, to allow merges)
  cache->adjust_subtree_auth(stripe, export_peer[stripe]);
  cache->try_subtree_merge(stripe);  // NOTE: may journal subtree_map as sideeffect

  // unpin path
  export_unlock(stripe);

  // discard delayed expires
  cache->discard_delayed_expire(stripe);

  // remove from exporting list, clean up state
  stripe->state_clear(CStripe::STATE_EXPORTING);
  export_state.erase(stripe);
  export_locks.erase(stripe);
  export_peer.erase(stripe);
  export_notify_ack_waiting.erase(stripe);

  // queue finishers
  mds->queue_waiters(export_finish_waiters[stripe]);
  export_finish_waiters.erase(stripe);

  cache->show_subtrees();
  audit();

  // send pending import_maps?
  mds->mdcache->maybe_send_pending_resolves();
  
  maybe_do_queued_export();
}








// ==========================================================
// IMPORT

void Migrator::handle_export_discover(MExportDirDiscover *m)
{
  int from = m->get_source_mds();
  assert(from != mds->get_nodeid());

  dout(7) << "handle_export_discover on " << m->get_path() << dendl;

  if (!mds->mdcache->is_open()) {
    dout(5) << " waiting for root" << dendl;
    mds->mdcache->wait_for_open(new C_MDS_RetryMessage(mds, m));
    return;
  }

  // note import state
  dirstripe_t ds = m->get_dirstripe();
  
  // only start discovering on this message once.
  if (!m->started) {
    m->started = true;
    import_state[ds] = IMPORT_DISCOVERING;
    import_peer[ds] = from;
  }

  // am i retrying after ancient path_traverse results?
  if (import_state.count(ds) == 0 ||
      import_state[ds] != IMPORT_DISCOVERING) {
    dout(7) << "hmm import_state is off, i must be obsolete lookup" << dendl;
    m->put();
    return;
  }

  assert (g_conf->mds_kill_import_at != 1);

  // do we have it?
  CInode *in = cache->get_inode(ds.ino);
  if (!in) {
    // must discover it!
    filepath fpath(m->get_path());
    vector<CDentry*> trace;
    int r = cache->path_traverse(NULL, m, NULL, fpath, &trace, NULL, MDS_TRAVERSE_DISCOVER);
    if (r > 0) return;
    if (r < 0) {
      dout(7) << "handle_export_discover_2 failed to discover or not dir " << m->get_path() << ", NAK" << dendl;
      assert(0);    // this shouldn't happen if the auth pins his path properly!!!! 
    }

    assert(0); // this shouldn't happen; the get_inode above would have succeeded.
  }

  // yay
  dout(7) << "handle_export_discover have " << ds << " inode " << *in << dendl;
  
  import_state[ds] = IMPORT_DISCOVERED;

  // pin inode in the cache (for now)
  assert(in->is_dir());
  in->get(CInode::PIN_IMPORTING);

  // reply
  dout(7) << " sending export_discover_ack on " << *in << dendl;
  mds->send_message_mds(new MExportDirDiscoverAck(ds), import_peer[ds]);
  m->put();
  assert (g_conf->mds_kill_import_at != 2);
}

void Migrator::import_reverse_discovering(dirstripe_t ds)
{
  import_state.erase(ds);
  import_peer.erase(ds);
}

void Migrator::import_reverse_discovered(dirstripe_t ds, CInode *diri)
{
  // unpin base
  diri->put(CInode::PIN_IMPORTING);
  import_state.erase(ds);
  import_peer.erase(ds);
}

void Migrator::import_reverse_prepping(CStripe *stripe)
{
  set<CStripe*> bounds;
  cache->map_dirstripe_set(import_bound_ls[stripe], bounds);
  import_remove_pins(stripe, bounds);
  import_reverse_final(stripe);
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_cancel(MExportDirCancel *m)
{
  dout(7) << "handle_export_cancel on " << m->get_dirstripe() << dendl;
  dirstripe_t ds = m->get_dirstripe();
  if (import_state[ds] == IMPORT_DISCOVERING) {
    import_reverse_discovering(ds);
  } else if (import_state[ds] == IMPORT_DISCOVERED) {
    CInode *in = cache->get_inode(ds.ino);
    assert(in);
    import_reverse_discovered(ds, in);
  } else if (import_state[ds] == IMPORT_PREPPING ||
	     import_state[ds] == IMPORT_PREPPED) {
    CStripe *stripe = mds->mdcache->get_dirstripe(ds);
    assert(stripe);
    import_reverse_prepping(stripe);
  } else {
    assert(0 == "got export_cancel in weird state");
  }
  m->put();
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_prep(MExportDirPrep *m)
{
  int oldauth = m->get_source().num();
  assert(oldauth != mds->get_nodeid());

  dirstripe_t ds = m->get_dirstripe();

  // make sure we didn't abort
  if (import_state.count(ds) == 0 ||
      (import_state[ds] != IMPORT_DISCOVERED &&
       import_state[ds] != IMPORT_PREPPING) ||
      import_peer[ds] != oldauth) {
    dout(10) << "handle_export_prep import has aborted, dropping" << dendl;
    m->put();
    return;
  }

  CInode *diri = cache->get_inode(ds.ino);
  assert(diri);
  
  list<Context*> finished;

  // assimilate root dir.
  CStripe *stripe;

  if (!m->did_assim()) {
    bufferlist::iterator p = m->base.begin();
    stripe = cache->add_replica_stripe(p, diri, oldauth, finished);
    dout(7) << "handle_export_prep on " << *stripe << " (first pass)" << dendl;
  } else {
    stripe = cache->get_dirstripe(ds);
    assert(stripe);
    dout(7) << "handle_export_prep on " << *stripe << " (subsequent pass)" << dendl;
  }
  assert(stripe->is_auth() == false);

  cache->show_subtrees();

  // build import bound map
  typedef set<stripeid_t> stripeset_t;
  map<inodeno_t, stripeset_t> import_bound_stripeset;
  for (list<dirstripe_t>::iterator p = m->get_bounds().begin();
       p != m->get_bounds().end();
       ++p) {
    dout(10) << " bound " << *p << dendl;
    import_bound_stripeset[p->ino].insert(p->stripeid);
  }

  // assimilate contents?
  if (!m->did_assim()) {
    dout(7) << "doing assim on " << *stripe << dendl;
    m->mark_assim();  // only do this the first time!

    // move pin to dir
    diri->put(CInode::PIN_IMPORTING);
    stripe->get(CStripe::PIN_IMPORTING);  
    stripe->state_set(CStripe::STATE_IMPORTING);

    // change import state
    import_state[ds] = IMPORT_PREPPING;
    assert(g_conf->mds_kill_import_at != 3);
    import_bound_ls[stripe] = m->get_bounds();

    // bystander list
    import_bystanders[stripe] = m->get_bystanders();
    dout(7) << "bystanders are " << import_bystanders[stripe] << dendl;

    // assimilate traces to exports
    // each trace is: ds ('-' | ('s' stripe dir | ('f' dir | 'd' df)) dentry inode (stripe dir dentry inode)*)
    for (list<bufferlist>::iterator p = m->traces.begin();
	 p != m->traces.end();
	 p++) {
      bufferlist::iterator q = p->begin();
      dirstripe_t ds;
      ::decode(ds, q);
      char start;
      ::decode(start, q);
      dout(10) << " trace from " << ds << " start " << start << " len " << p->length() << dendl;

      CDir *cur = 0;
      if (start == 'd') {
        dirfrag_t df;
        ::decode(df, q);
	cur = cache->get_dirfrag(df);
	assert(cur);
	dout(10) << "  had " << *cur << dendl;
      } else if (start == 'f') {
        CStripe *stripe = cache->get_dirstripe(ds);
        assert(stripe);
	dout(10) << "  had " << *stripe << dendl;
	cur = cache->add_replica_dir(q, stripe, finished);
 	dout(10) << "  added " << *cur << dendl;
      } else if (start == 's') {
        CStripe *stripe = cache->get_dirstripe(ds);
        assert(stripe);
	dout(10) << "  had " << *stripe << dendl;
        cur = cache->add_replica_dir(q, stripe, finished);
 	dout(10) << "  added " << *cur << dendl;
      } else if (start == '-') {
	// nothing
      } else
	assert(0 == "unrecognized start char");
      while (start != '-') {
	CDentry *dn = cache->add_replica_dentry(q, cur, finished);
	dout(10) << "  added " << *dn << dendl;
	CInode *in = cache->add_replica_inode(q, dn, finished);
	dout(10) << "  added " << *in << dendl;
	if (q.end())
	  break;
        CStripe *stripe = cache->add_replica_stripe(q, in, oldauth, finished);
	dout(10) << "  added " << *stripe << dendl;
	cur = cache->add_replica_dir(q, stripe, finished);
	dout(10) << "  added " << *cur << dendl;
      }
    }

    // make bound sticky
    for (map<inodeno_t,stripeset_t>::iterator p = import_bound_stripeset.begin();
	 p != import_bound_stripeset.end();
	 ++p) {
      CInode *in = cache->get_inode(p->first);
      assert(in);
      in->get_stickystripes();
      dout(7) << " set stickystripes on bound inode " << *in << dendl;
    }

  } else {
    dout(7) << " not doing assim on " << *stripe << dendl;
  }

  if (!finished.empty())
    mds->queue_waiters(finished);


  // open all bounds
  set<CStripe*> import_bounds;
  for (map<inodeno_t,stripeset_t>::iterator p = import_bound_stripeset.begin();
       p != import_bound_stripeset.end();
       ++p) {
    CInode *in = cache->get_inode(p->first);
    assert(in);

    // map fragset into a frag_t list, based on the inode fragtree
    for (stripeset_t::iterator q = p->second.begin(); q != p->second.end(); ++q) {
      CStripe *bound = cache->get_dirstripe(dirstripe_t(p->first, *q));
      if (!bound) {
	dout(7) << "  opening bounding stripe " << *q << " on " << *in << dendl;
	cache->open_remote_dirstripe(in, *q, new C_MDS_RetryMessage(mds, m));
	return;
      }

      if (!bound->state_test(CStripe::STATE_IMPORTBOUND)) {
	dout(7) << "  pinning import bound " << *bound << dendl;
	bound->get(CStripe::PIN_IMPORTBOUND);
	bound->state_set(CStripe::STATE_IMPORTBOUND);
      } else {
	dout(7) << "  already pinned import bound " << *bound << dendl;
      }
      import_bounds.insert(bound);
    }
  }

  dout(7) << " all ready, noting auth and freezing import region" << dendl;

  // note that i am an ambiguous auth for this subtree.
  // specify bounds, since the exporter explicitly defines the region.
  cache->adjust_bounded_subtree_auth(stripe, import_bounds,
				     make_pair(oldauth, mds->get_nodeid()));
  cache->verify_subtree_bounds(stripe, import_bounds);

  // freeze.
  stripe->freeze();

  // ok!
  dout(7) << " sending export_prep_ack on " << *stripe << dendl;
  mds->send_message(new MExportDirPrepAck(stripe->dirstripe()), m->get_connection());

  // note new state
  import_state[ds] = IMPORT_PREPPED;
  assert(g_conf->mds_kill_import_at != 4);
  // done
  m->put();
}




class C_MDS_ImportDirLoggedStart : public Context {
  Migrator *migrator;
  CStripe *stripe;
  int from;
public:
  map<client_t,entity_inst_t> imported_client_map;
  map<client_t,uint64_t> sseqmap;

  C_MDS_ImportDirLoggedStart(Migrator *m, CStripe *s, int f) :
    migrator(m), stripe(s), from(f) {}
  void finish(int r) {
    migrator->import_logged_start(stripe, from, imported_client_map, sseqmap);
  }
};

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_dir(MExportDir *m)
{
  assert (g_conf->mds_kill_import_at != 5);
  CStripe *stripe = cache->get_dirstripe(m->dirstripe);
  assert(stripe);
  
  utime_t now = ceph_clock_now(g_ceph_context);
  int oldauth = m->get_source().num();
  dout(7) << "handle_export_dir importing " << *stripe << " from " << oldauth << dendl;
  assert(stripe->is_auth() == false);

  cache->show_subtrees();

  C_MDS_ImportDirLoggedStart *onlogged = new C_MDS_ImportDirLoggedStart(this, stripe, m->get_source().num());

  // start the journal entry
  EImportStart *le = new EImportStart(mds->mdlog, stripe->dirstripe(), m->bounds);
  mds->mdlog->start_entry(le);

  le->metablob.add_stripe_context(stripe);

  // adjust auth (list us _first_)
  cache->adjust_subtree_auth(stripe, mds->get_nodeid(), oldauth);

  // new client sessions, open these after we journal
  // include imported sessions in EImportStart
  bufferlist::iterator cmp = m->client_map.begin();
  ::decode(onlogged->imported_client_map, cmp);
  assert(cmp.end());
  le->cmapv = mds->server->prepare_force_open_sessions(onlogged->imported_client_map, onlogged->sseqmap);
  le->client_map.claim(m->client_map);

  bufferlist::iterator blp = m->export_data.begin();
  int num_imported_inodes = 0;
  while (!blp.end()) {
    num_imported_inodes += 
      decode_import_stripe(blp, oldauth, stripe, le,
                           mds->mdlog->get_current_segment(),
                           import_caps[stripe],
                           import_updated_scatterlocks[stripe], now);
  }
  dout(10) << " " << m->bounds.size() << " imported bounds" << dendl;
  
  // include bounds in EImportStart
  set<CStripe*> import_bounds;
  cache->get_subtree_bounds(stripe, import_bounds);
  for (set<CStripe*>::iterator it = import_bounds.begin();
       it != import_bounds.end();
       it++) 
    le->metablob.add_stripe(*it, false);  // note that parent metadata is already in the event

  // adjust popularity
  mds->balancer->add_import(stripe, now);

  dout(7) << "handle_export_dir did " << *stripe << dendl;

  // note state
  import_state[m->dirstripe] = IMPORT_LOGGINGSTART;
  assert (g_conf->mds_kill_import_at != 6);

  // log it
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(onlogged);
  mds->mdlog->flush();

  // some stats
  if (mds->logger) {
    mds->logger->inc(l_mds_im);
    mds->logger->inc(l_mds_iim, num_imported_inodes);
  }

  m->put();
}


/*
 * this is an import helper
 *  called by import_finish, and import_reverse and friends.
 */
void Migrator::import_remove_pins(CStripe *stripe, set<CStripe*>& bounds)
{
  // root
  stripe->put(CStripe::PIN_IMPORTING);
  stripe->state_clear(CStripe::STATE_IMPORTING);

  // bounding inodes
  set<inodeno_t> did;
  for (list<dirstripe_t>::iterator p = import_bound_ls[stripe].begin();
       p != import_bound_ls[stripe].end();
       p++) {
    if (did.count(p->ino))
      continue;
    did.insert(p->ino);
    CInode *in = cache->get_inode(p->ino);
    in->put_stickystripes();
  }

  if (import_state[stripe->dirstripe()] >= IMPORT_PREPPED) {
    // bounding dirfrags
    for (set<CStripe*>::iterator it = bounds.begin();
	 it != bounds.end();
	 it++) {
      CStripe *bd = *it;
      bd->put(CStripe::PIN_IMPORTBOUND);
      bd->state_clear(CStripe::STATE_IMPORTBOUND);
    }
  }
}


/*
 * note: this does teh full work of reversing and import and cleaning up
 *  state.  
 * called by both handle_mds_failure and by handle_resolve (if we are
 *  a survivor coping with an exporter failure+recovery).
 */
void Migrator::import_reverse(CStripe *stripe)
{
  dout(7) << "import_reverse " << *stripe << dendl;

  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);

  // remove pins
  import_remove_pins(stripe, bounds);

  // update auth, with possible subtree merge.
  assert(stripe->is_subtree_root());
  if (mds->is_resolve())
    cache->trim_non_auth_subtree(stripe);
  cache->adjust_subtree_auth(stripe, import_peer[stripe->dirstripe()]);

  // adjust auth bits.
  list<CStripe*> q(1, stripe);
  while (!q.empty()) {
    CStripe *cur = q.front();
    q.pop_front();

    // stripe
    assert(cur->is_auth());
    cur->state_clear(CStripe::STATE_AUTH);
    cur->clear_replica_map();
    cur->dirfragtreelock.clear_gather();
    if (cur->is_dirty())
      cur->mark_clean();

    list<CDir*> dirs;
    cur->get_dirfrags(dirs);
    for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d) {
      CDir *dir = *d;

      // dir
      assert(dir->is_auth());
      dir->state_clear(CDir::STATE_AUTH);
      dir->remove_bloom();
      dir->clear_replica_map();
      if (dir->is_dirty())
        dir->mark_clean();

      CDir::map_t::iterator it;
      for (it = dir->begin(); it != dir->end(); it++) {
        CDentry *dn = it->second;

        // dentry
        dn->state_clear(CDentry::STATE_AUTH);
        dn->clear_replica_map();
        if (dn->is_dirty()) 
	dn->mark_clean();

        // inode?
        if (dn->get_linkage()->is_primary()) {
          CInode *in = dn->get_linkage()->get_inode();
          in->state_clear(CDentry::STATE_AUTH);
          in->clear_replica_map();
          if (in->is_dirty()) 
            in->mark_clean();
          in->authlock.clear_gather();
          in->linklock.clear_gather();
          in->filelock.clear_gather();

          // non-bounding dir?
          in->get_nested_stripes(q);
        }
      }
    }
  }

  // reexport caps
  for (inode_cap_export_map::iterator p = import_caps[stripe].begin();
       p != import_caps[stripe].end();
       ++p) {
    CInode *in = p->first;
    dout(20) << " reexporting caps on " << *in << dendl;
    /*
     * bleh.. just export all caps for this inode.  the auth mds
     * will pick them up during recovery.
     */
    bufferlist bl; // throw this away
    map<client_t,entity_inst_t> exported_client_map;  // throw this away too
    encode_export_inode_caps(in, bl, exported_client_map);
    finish_export_inode_caps(in);
  }
	 
  // log our failure
  mds->mdlog->start_submit_entry(new EImportFinish(stripe->dirstripe(), false));

  cache->try_subtree_merge(stripe);  // NOTE: this may journal subtree map as side effect

  // bystanders?
  if (import_bystanders[stripe].empty()) {
    dout(7) << "no bystanders, finishing reverse now" << dendl;
    import_reverse_unfreeze(stripe);
  } else {
    // notify them; wait in aborting state
    dout(7) << "notifying bystanders of abort" << dendl;
    import_notify_abort(stripe, bounds);
    import_state[stripe->dirstripe()] = IMPORT_ABORTING;
    assert (g_conf->mds_kill_import_at != 10);
  }
}

void Migrator::import_notify_abort(CStripe *stripe, set<CStripe*>& bounds)
{
  dout(7) << "import_notify_abort " << *stripe << dendl;
  
  for (set<int>::iterator p = import_bystanders[stripe].begin();
       p != import_bystanders[stripe].end();
       ++p) {
    // NOTE: the bystander will think i am _only_ auth, because they will have seen
    // the exporter's failure and updated the subtree auth.  see mdcache->handle_mds_failure().
    MExportDirNotify *notify = 
      new MExportDirNotify(stripe->dirstripe(), true,
			   make_pair(mds->get_nodeid(), CDIR_AUTH_UNKNOWN),
			   make_pair(import_peer[stripe->dirstripe()], CDIR_AUTH_UNKNOWN));
    for (set<CStripe*>::iterator i = bounds.begin(); i != bounds.end(); i++)
      notify->get_bounds().push_back((*i)->dirstripe());
    mds->send_message_mds(notify, *p);
  }
}

void Migrator::import_reverse_unfreeze(CStripe *stripe)
{
  dout(7) << "import_reverse_unfreeze " << *stripe << dendl;
  stripe->unfreeze();
  list<Context*> ls;
  mds->queue_waiters(ls);
  cache->discard_delayed_expire(stripe);
  import_reverse_final(stripe);
}

void Migrator::import_reverse_final(CStripe *stripe) 
{
  dout(7) << "import_reverse_final " << *stripe << dendl;

  // clean up
  import_state.erase(stripe->dirstripe());
  import_peer.erase(stripe->dirstripe());
  import_bystanders.erase(stripe);
  import_bound_ls.erase(stripe);
  import_updated_scatterlocks.erase(stripe);
  import_caps.erase(stripe);

  // send pending import_maps?
  mds->mdcache->maybe_send_pending_resolves();

  cache->show_subtrees();
  //audit();  // this fails, bc we munge up the subtree map during handle_import_map (resolve phase)
}




void Migrator::import_logged_start(CStripe *stripe, int from,
				   map<client_t,entity_inst_t>& imported_client_map,
				   map<client_t,uint64_t>& sseqmap)
{
  dirstripe_t ds = stripe->dirstripe();

  if (import_state.count(ds) == 0 ||
      import_state[ds] != IMPORT_LOGGINGSTART) {
    dout(7) << "import " << ds << " must have aborted" << dendl;
    return;
  }

  dout(7) << "import_logged " << *stripe << dendl;

  // note state
  import_state[ds] = IMPORT_ACKING;

  assert (g_conf->mds_kill_import_at != 7);

  // force open client sessions and finish cap import
  mds->server->finish_force_open_sessions(imported_client_map, sseqmap);
  
  for (inode_cap_export_map::iterator p = import_caps[stripe].begin();
       p != import_caps[stripe].end();
       ++p) {
    finish_import_inode_caps(p->first, from, p->second);
  }
  
  // send notify's etc.
  dout(7) << "sending ack for " << *stripe << " to old auth mds." << from << dendl;

  // test surviving observer of a failed migration that did not complete
  //assert(dir->replica_map.size() < 2 || mds->whoami != 0);

  mds->send_message_mds(new MExportDirAck(ds), from);
  assert (g_conf->mds_kill_import_at != 8);

  cache->show_subtrees();
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_finish(MExportDirFinish *m)
{
  CStripe *stripe = cache->get_dirstripe(m->get_dirstripe());
  assert(stripe);
  dout(7) << "handle_export_finish on " << *stripe << dendl;
  import_finish(stripe);
  m->put();
}

void Migrator::import_finish(CStripe *stripe) 
{
  dout(7) << "import_finish on " << *stripe << dendl;

  // log finish
  assert(g_conf->mds_kill_import_at != 9);

  // clear updated scatterlocks
  /*
  for (list<ScatterLock*>::iterator p = import_updated_scatterlocks[dir].begin();
       p != import_updated_scatterlocks[dir].end();
       ++p) 
    (*p)->clear_updated();
  */

  // remove pins
  set<CStripe*> bounds;
  cache->get_subtree_bounds(stripe, bounds);
  import_remove_pins(stripe, bounds);

  inode_cap_export_map cap_imports;
  import_caps[stripe].swap(cap_imports);

  dirstripe_t ds = stripe->dirstripe();

  // clear import state (we're done!)
  import_state.erase(ds);
  import_peer.erase(ds);
  import_bystanders.erase(stripe);
  import_bound_ls.erase(stripe);
  import_caps.erase(stripe);
  import_updated_scatterlocks.erase(stripe);

  mds->mdlog->start_submit_entry(new EImportFinish(ds, true));

  // adjust auth, with possible subtree merge.
  cache->adjust_subtree_auth(stripe, mds->get_nodeid());
  cache->try_subtree_merge(stripe);   // NOTE: this may journal subtree_map as sideffect

  // process delayed expires
  cache->process_delayed_expire(stripe);

  // ok now unfreeze (and thus kick waiters)
  stripe->unfreeze();
  cache->show_subtrees();
  //audit();  // this fails, bc we munge up the subtree map during handle_import_map (resolve phase)

  list<Context*> ls;
  mds->queue_waiters(ls);

  // re-eval imported caps
  for (inode_cap_export_map::iterator p = cap_imports.begin();
       p != cap_imports.end();
       p++)
    if (p->first->is_auth())
      mds->locker->eval(p->first, CEPH_CAP_LOCKS, true);

  // send pending import_maps?
  mds->mdcache->maybe_send_pending_resolves();

  // did i just import mydir?
  if (ds.ino == MDS_INO_MDSDIR(mds->whoami))
    cache->populate_mydir();

  // is it empty?
  if (stripe->get_num_head_items() == 0 &&
      !stripe->get_inode()->is_auth()) {
    // reexport!
    export_empty_import(stripe);
  }
}

#endif

void Migrator::decode_import_inode(CDentry *dn, bufferlist::iterator& blp, int oldauth,
				   LogSegment *ls, uint64_t log_offset,
                                   inode_cap_export_map& cap_imports,
				   list<ScatterLock*>& updated_scatterlocks)
{  
  dout(15) << "decode_import_inode on " << *dn << dendl;

  inodeno_t ino;
  snapid_t last;
  ::decode(ino, blp);
  ::decode(last, blp);

  bool added = false;
  CInode *in = cache->get_inode(ino, last);
  if (!in) {
    in = new CInode(mds->mdcache, true, 1, last);
    added = true;
  } else {
    in->state_set(CInode::STATE_AUTH);
  }

  // state after link  -- or not!  -sage
  in->decode_import(blp, ls);  // cap imports are noted for later action

  // note that we are journaled at this log offset
  in->last_journaled = log_offset;

  // caps
  decode_import_inode_caps(in, blp, cap_imports);

  // link before state  -- or not!  -sage
  if (dn->get_linkage()->get_inode() != in) {
    assert(!dn->get_linkage()->get_inode());
    dn->get_dir()->link_primary_inode(dn, in);
  }
 
  // add inode?
  if (added) {
    cache->add_inode(in);
    dout(10) << "added " << *in << dendl;
  } else {
    dout(10) << "  had " << *in << dendl;
  }

  if (in->inode.is_dirty_rstat())
    in->mark_dirty_rstat();
  
  // clear if dirtyscattered, since we're going to journal this
  //  but not until we _actually_ finish the import...
  if (in->filelock.is_dirty()) {
    updated_scatterlocks.push_back(&in->filelock);
    mds->locker->mark_updated_scatterlock(&in->filelock);
  }

  // adjust replica list
  //assert(!in->is_replica(oldauth));  // not true on failed export
  in->add_replica(oldauth, CInode::EXPORT_NONCE);
  if (in->is_replica(mds->get_nodeid()))
    in->remove_replica(mds->get_nodeid());
  
}

void Migrator::decode_import_inode_caps(CInode *in,
					bufferlist::iterator &blp,
                                        inode_cap_export_map& cap_imports)
{
  client_cap_export_map cap_map;
  ::decode(cap_map, blp);
  if (!cap_map.empty()) {
    cap_imports[in].swap(cap_map);
    in->get(CInode::PIN_IMPORTINGCAPS);
  }
}

void Migrator::finish_import_inode_caps(CInode *in, int from, 
                                        client_cap_export_map &cap_map)
{
  assert(!cap_map.empty());
  
  for (client_cap_export_map::iterator it = cap_map.begin();
       it != cap_map.end();
       it++) {
    dout(10) << "finish_import_inode_caps for client." << it->first << " on " << *in << dendl;
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(it->first.v));
    assert(session);

    Capability *cap = in->get_client_cap(it->first);
    if (!cap) {
      cap = in->add_client_cap(it->first, session);
    }
    cap->merge(it->second);

    mds->mdcache->do_cap_import(session, in, cap);
  }

  in->put(CInode::PIN_IMPORTINGCAPS);
}

int Migrator::decode_import_dir(bufferlist::iterator& blp, int oldauth,
				CStripe *import_root, EImportStart *le,
				LogSegment *ls, inode_cap_export_map& cap_imports,
				list<ScatterLock*>& updated_scatterlocks, utime_t now)
{
  // set up dir
  dirfrag_t df;
  ::decode(df, blp);

  CInode *in = cache->get_inode(df.stripe.ino);
  assert(in);
  CStripe *stripe = in->get_or_open_stripe(df.stripe.stripeid);
  assert(stripe);
  CDir *dir = stripe->get_or_open_dirfrag(df.frag);
  assert(dir);
  
  dout(7) << "decode_import_dir " << *dir << dendl;

  // assimilate state
  dir->decode_import(blp, now);

  // mark  (may already be marked from get_or_open_dir() above)
  if (!dir->is_auth())
    dir->state_set(CDir::STATE_AUTH);

  // adjust replica list
  //assert(!dir->is_replica(oldauth));    // not true on failed export
  dir->add_replica(oldauth);
  if (dir->is_replica(mds->get_nodeid()))
    dir->remove_replica(mds->get_nodeid());

  // add to journal entry
  if (le) 
    le->metablob.add_import_dir(dir);

  int num_imported = 0;

  // take all waiters on this dir
  // NOTE: a pass of imported data is guaranteed to get all of my waiters because
  // a replica's presense in my cache implies/forces it's presense in authority's.
  list<Context*> waiters;
  
  dir->take_waiting(CDir::WAIT_ANY_MASK, waiters);
  for (list<Context*>::iterator it = waiters.begin();
       it != waiters.end();
       it++) 
    import_root->add_waiter(CStripe::WAIT_UNFREEZE, *it);  // UNFREEZE will get kicked both on success or failure
  
  dout(15) << "doing contents" << dendl;
  
  // contents
  __u32 nden;
  ::decode(nden, blp);
  
  for (; nden>0; nden--) {
    num_imported++;
    
    // dentry
    string dname;
    snapid_t last;
    ::decode(dname, blp);
    ::decode(last, blp);
    
    CDentry *dn = dir->lookup_exact_snap(dname, last);
    if (!dn)
      dn = dir->add_null_dentry(dname, 1, last);
    
    dn->decode_import(blp, ls);

    dn->add_replica(oldauth, CDentry::EXPORT_NONCE);
    if (dn->is_replica(mds->get_nodeid()))
      dn->remove_replica(mds->get_nodeid());

    dout(15) << "decode_import_dir got " << *dn << dendl;
    
    // points to...
    char icode;
    ::decode(icode, blp);
    
    if (icode == 'N') {
      // null dentry
      assert(dn->get_linkage()->is_null());  
      
      // fall thru
    }
    else if (icode == 'L') {
      // remote link
      inodeno_t ino;
      unsigned char d_type;
      ::decode(ino, blp);
      ::decode(d_type, blp);
      if (dn->get_linkage()->is_remote()) {
	assert(dn->get_linkage()->get_remote_ino() == ino);
      } else {
	dir->link_remote_inode(dn, ino, d_type);
      }
    }
    else if (icode == 'I') {
      // inode
      decode_import_inode(dn, blp, oldauth, ls, le->get_start_off(), cap_imports, updated_scatterlocks);
    }
    
    // add dentry to journal entry
    if (le)
      le->metablob.add_dentry(dn, dn->is_dirty());
  }
  
#ifdef MDS_VERIFY_FRAGSTAT
  if (dir->is_complete())
    dir->verify_fragstat();
#endif

  dout(7) << "decode_import_dir done " << *dir << dendl;
  return num_imported;
}


int Migrator::decode_import_stripe(bufferlist::iterator& blp, int oldauth,
                                   CStripe *import_root, EImportStart *le,
                                   LogSegment *ls, inode_cap_export_map& cap_imports,
                                   list<ScatterLock*>& updated_scatterlocks, utime_t now)
{
  int num_imported = 0;

  // decode dirstripe and frag count
  dirstripe_t ds;
  ::decode(ds, blp);

  CInode *in = cache->get_inode(ds.ino);
  assert(in);
  CStripe *stripe = in->get_or_open_stripe(ds.stripeid);
  assert(stripe);

  dout(7) << "decode_import_stripe " << *stripe << dendl;

  stripe->decode_import(blp, now);

  stripe->state_set(CDir::STATE_AUTH);
  stripe->add_replica(oldauth);
  if (stripe->is_replica(mds->get_nodeid()))
    stripe->remove_replica(mds->get_nodeid());

  __u32 count;
  ::decode(count, blp);

  while (count--)
    num_imported += decode_import_dir(blp, oldauth, import_root, le, ls,
                                      cap_imports, updated_scatterlocks, now);

  return num_imported;
}

#if 0

// authority bystander

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_notify(MExportDirNotify *m)
{
  dirstripe_t ds = m->get_dirstripe();
  CStripe *stripe = cache->get_dirstripe(ds);

  int from = m->get_source().num();
  pair<int,int> old_auth = m->get_old_auth();
  pair<int,int> new_auth = m->get_new_auth();
  
  if (!stripe) {
    dout(7) << "handle_export_notify " << old_auth << " -> " << new_auth
	    << " on missing dir " << ds << dendl;
  } else if (stripe->authority() != old_auth) {
    dout(7) << "handle_export_notify old_auth was " << stripe->authority() 
	    << " != " << old_auth << " -> " << new_auth
	    << " on " << *stripe << dendl;
  } else {
    dout(7) << "handle_export_notify " << old_auth << " -> " << new_auth
	    << " on " << *stripe << dendl;
    // adjust auth
    set<CStripe*> have;
    cache->map_dirstripe_set(m->get_bounds(), have);
    cache->adjust_bounded_subtree_auth(stripe, have, new_auth);

    // induce a merge?
    cache->try_subtree_merge(stripe);

    // if inode is auth, update stripe_auth array
    if (stripe->get_inode()->is_auth())
      stripe->get_inode()->set_stripe_auth(ds.stripeid, new_auth.second);
  }
  
  // send ack
  if (m->wants_ack()) {
    mds->send_message_mds(new MExportDirNotifyAck(ds), from);
  } else {
    // aborted.  no ack.
    dout(7) << "handle_export_notify no ack requested" << dendl;
  }
  
  m->put();
}








/** cap exports **/



void Migrator::export_caps(CInode *in)
{
  int dest = in->authority().first;
  dout(7) << "export_caps to mds." << dest << " " << *in << dendl;

  assert(in->is_any_caps());
  assert(!in->is_auth());
  assert(!in->is_ambiguous_auth());
  assert(!in->state_test(CInode::STATE_EXPORTINGCAPS));

  MExportCaps *ex = new MExportCaps;
  ex->ino = in->ino();

  encode_export_inode_caps(in, ex->cap_bl, ex->client_map);

  mds->send_message_mds(ex, dest);
}

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_caps_ack(MExportCapsAck *ack)
{
  CInode *in = cache->get_inode(ack->ino);
  assert(in);
  dout(10) << "handle_export_caps_ack " << *ack << " from " << ack->get_source() 
	   << " on " << *in
	   << dendl;
  
  finish_export_inode_caps(in);
  ack->put();
}


class C_M_LoggedImportCaps : public Context {
  Migrator *migrator;
  CInode *in;
  int from;
public:
  Migrator::inode_cap_export_map cap_imports;
  map<client_t,entity_inst_t> client_map;
  map<client_t,uint64_t> sseqmap;

  C_M_LoggedImportCaps(Migrator *m, CInode *i, int f) : migrator(m), in(i), from(f) {}
  void finish(int r) {
    migrator->logged_import_caps(in, from, cap_imports, client_map, sseqmap);
  }  
};

/* This function DOES put the passed message before returning*/
void Migrator::handle_export_caps(MExportCaps *ex)
{
  dout(10) << "handle_export_caps " << *ex << " from " << ex->get_source() << dendl;
  CInode *in = cache->get_inode(ex->ino);
  
  assert(in->is_auth());
  /*
   * note: i may be frozen, but i won't have been encoded for export (yet)!
   *  see export_go() vs export_go_synced().
   */

  C_M_LoggedImportCaps *finish = new C_M_LoggedImportCaps(this, in, ex->get_source().num());
  finish->client_map = ex->client_map;

  // decode new caps
  bufferlist::iterator blp = ex->cap_bl.begin();
  decode_import_inode_caps(in, blp, finish->cap_imports);
  assert(!finish->cap_imports.empty());   // thus, inode is pinned.

  // journal open client sessions
  version_t pv = mds->server->prepare_force_open_sessions(finish->client_map, finish->sseqmap);
  
  ESessions *le = new ESessions(pv, ex->client_map);
  mds->mdlog->start_entry(le);
  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(finish);
  mds->mdlog->flush();

  ex->put();
}


void Migrator::logged_import_caps(CInode *in, int from,
				  inode_cap_export_map& cap_imports,
				  map<client_t,entity_inst_t>& client_map,
				  map<client_t,uint64_t>& sseqmap)
{
  dout(10) << "logged_import_caps on " << *in << dendl;

  // force open client sessions and finish cap import
  mds->server->finish_force_open_sessions(client_map, sseqmap);

  assert(cap_imports.count(in));
  finish_import_inode_caps(in, from, cap_imports[in]);  
  mds->locker->eval(in, CEPH_CAP_LOCKS, true);

  mds->send_message_mds(new MExportCapsAck(in->ino()), from);
}



#endif
