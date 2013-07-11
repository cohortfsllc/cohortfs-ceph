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

#include <errno.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <map>

#include "MDCache.h"
#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "Migrator.h"

#include "AnchorClient.h"
#include "SnapClient.h"

#include "MDSMap.h"

#include "CInode.h"
#include "CDir.h"

#include "Mutation.h"

#include "include/ceph_fs.h"
#include "include/filepath.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "common/perf_counters.h"
#include "common/MemoryModel.h"
#include "osdc/Journaler.h"
#include "osdc/Filer.h"

#include "events/ESubtreeMap.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EString.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"
#include "events/ECommitted.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSCacheRejoin.h"

#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MDirUpdate.h"
#include "messages/MCacheExpire.h"

#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"
#include "messages/MDentryLink.h"
#include "messages/MDentryUnlink.h"

#include "messages/MMDSFindIno.h"
#include "messages/MMDSFindInoReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MMDSFragmentNotify.h"


#include "InoTable.h"

#include "common/Timer.h"

using namespace std;

extern struct ceph_file_layout g_default_file_layout;

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".cache ";
}

long g_num_ino = 0;
long g_num_dir = 0;
long g_num_dn = 0;
long g_num_cap = 0;

long g_num_inoa = 0;
long g_num_dira = 0;
long g_num_dna = 0;
long g_num_capa = 0;

long g_num_inos = 0;
long g_num_dirs = 0;
long g_num_dns = 0;
long g_num_caps = 0;

set<int> SimpleLock::empty_gather_set;


MDCache::MDCache(MDS *m) : container(this), snaprealm(this), parentstats(m)
{
  mds = m;
  migrator = new Migrator(mds, this);
  root = NULL;
  myin = NULL;

  stray_index = 0;
  for (int i = 0; i < NUM_STRAY; ++i) {
    strays[i] = NULL;
  }

  num_inodes_with_caps = 0;
  num_caps = 0;

  max_dir_commit_size = g_conf->mds_dir_max_commit_size ?
                        (g_conf->mds_dir_max_commit_size << 20) :
                        (0.9 *(g_conf->osd_max_write_size << 20));

  discover_last_tid = 0;
  find_ino_peer_last_tid = 0;

  last_cap_id = 0;

  client_lease_durations[0] = 5.0;
  client_lease_durations[1] = 30.0;
  client_lease_durations[2] = 300.0;

  opening_root = open = false;
  lru.lru_set_max(g_conf->mds_cache_size);
  lru.lru_set_midpoint(g_conf->mds_cache_mid);

  decayrate.set_halflife(g_conf->mds_decay_halflife);

  memset(&default_log_layout, 0, sizeof(default_log_layout));

  did_shutdown_log_cap = false;
}

MDCache::~MDCache() 
{
  delete migrator;
  //delete renamer;
}



void MDCache::log_stat()
{
  mds->logger->set(l_mds_imax, g_conf->mds_cache_size);
  mds->logger->set(l_mds_i, lru.lru_get_size());
  mds->logger->set(l_mds_ipin, lru.lru_get_num_pinned());
  mds->logger->set(l_mds_itop, lru.lru_get_top());
  mds->logger->set(l_mds_ibot, lru.lru_get_bot());
  mds->logger->set(l_mds_iptail, lru.lru_get_pintail());
  mds->logger->set(l_mds_icap, num_inodes_with_caps);
  mds->logger->set(l_mds_cap, num_caps);
}


// 

bool MDCache::shutdown()
{
  if (lru.lru_get_size() > 0)
    dout(7) << "WARNING: mdcache shutdown with non-empty cache" << dendl;
  return true;
}


// ====================================================================
// some inode functions

void MDCache::add_inode(CInode *in)
{
  // add to lru, inode map
  pair<inode_map::iterator, bool> inserted = inodes.insert(
      make_pair(in->vino(), in));
  assert(inserted.second); // should be no dup inos!

  if (in->ino() < MDS_INO_SYSTEM_BASE) {
    if (in->ino() == MDS_INO_ROOT)
      root = in;
    else if (in->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
      myin = in;
    else if (in->ino() == MDS_INO_CONTAINER)
      container.in = in;
    else if (in->is_stray()) {
      if (MDS_INO_STRAY_OWNER(in->ino()) == mds->get_nodeid()) {
	strays[MDS_INO_STRAY_INDEX(in->ino())] = in;
      }
    }
    if (in->is_base())
      base_inodes.insert(in);
  }
}

void MDCache::remove_inode(CInode *o) 
{ 
  dout(14) << "remove_inode " << *o << dendl;

  if (o->get_parent_dn()) {
    // FIXME: multiple parents?
    CDentry *dn = o->get_parent_dn();
    assert(!dn->is_dirty());
    dn->dir->unlink_inode(dn);   // leave dentry ... FIXME?
  }

  if (o->is_dirty())
    o->mark_clean();

  o->filelock.remove_dirty();
  o->nestlock.remove_dirty();

  o->item_open_file.remove_myself();

  // remove from inode map
  inodes.erase(o->vino());    

  if (o->ino() < MDS_INO_SYSTEM_BASE) {
    if (o == root) root = 0;
    if (o == myin) myin = 0;
    if (o == container.get_inode()) container.in = 0;
    if (o->is_stray()) {
      if (MDS_INO_STRAY_OWNER(o->ino()) == mds->get_nodeid()) {
	strays[MDS_INO_STRAY_INDEX(o->ino())] = 0;
      }
    }
    if (o->is_base())
      base_inodes.erase(o);
    }

  // delete it
  assert(o->get_num_ref() == 0);
  delete o; 
}



void MDCache::init_layouts()
{
  default_file_layout = g_default_file_layout;
  default_file_layout.fl_pg_pool = mds->mdsmap->get_first_data_pool();

  default_log_layout = g_default_file_layout;
  default_log_layout.fl_pg_pool = mds->mdsmap->get_metadata_pool();
}

CInode *MDCache::create_system_inode(inodeno_t ino, int auth, int mode)
{
  dout(0) << "creating system inode with ino:" << ino << dendl;
  CInode *in = new CInode(this, auth);
  in->inode.ino = ino;
  in->inode.version = 1;
  in->inode.mode = 0500 | mode;
  in->inode.size = 0;
  in->inode.ctime = 
    in->inode.mtime = ceph_clock_now(g_ceph_context);
  in->inode.nlink = 1;
  in->inode.truncate_size = -1ull;

  memset(&in->inode.dir_layout, 0, sizeof(in->inode.dir_layout));
  if (in->inode.is_dir()) {
    memset(&in->inode.layout, 0, sizeof(in->inode.layout));
    in->inode.dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;
    ++in->inode.rstat.rsubdirs;
  } else {
    in->inode.layout = default_file_layout;
    ++in->inode.rstat.rfiles;
  }
  in->inode.accounted_rstat = in->inode.rstat;

  add_inode(in);
  return in;
}

CInode *MDCache::create_root_inode()
{
  int auth = mds->mdsmap->get_root();
  CInode *i = create_system_inode(MDS_INO_ROOT, auth, S_IFDIR|0755);
  i->set_stripe_auth(vector<int>(1, auth));
  i->default_layout = new struct default_file_layout;
  i->default_layout->layout = default_file_layout;
  i->default_layout->layout.fl_pg_pool = mds->mdsmap->get_first_data_pool();
  dout(10) << "create_root_inode " << *i << dendl;
  return i;
}

void MDCache::create_empty_hierarchy(C_Gather *gather)
{
  int auth = mds->get_nodeid();
  const vector<int> stripe_auth(1, auth);
  LogSegment *ls = mds->mdlog->get_current_segment();

  // create root dir
  CInode *root = create_root_inode();

  // force empty root dir
  root->set_stripe_auth(stripe_auth);
  CStripe *rootstripe = root->get_or_open_stripe(0);
  rootstripe->state_set(CStripe::STATE_OPEN);
  rootstripe->replicate = true;
  rootstripe->set_stripe_auth(mds->whoami);
  CDir *rootdir = rootstripe->get_or_open_dirfrag(frag_t());

  // create ceph dir
  CInode *ceph = create_system_inode(MDS_INO_CEPH, auth, S_IFDIR);
  CDentry *dn = rootdir->add_primary_dentry(".ceph", ceph);
  dn->_mark_dirty(ls);

  ceph->set_stripe_auth(stripe_auth);
  CStripe *cephstripe = ceph->get_or_open_stripe(0);
  cephstripe->state_set(CStripe::STATE_OPEN);
  cephstripe->replicate = true;
  CDir *cephdir = cephstripe->get_or_open_dirfrag(frag_t());

  ceph->inode.dirstat = cephstripe->fnode.fragstat;
  rootstripe->fnode.fragstat.nsubdirs++;
  rootstripe->fnode.rstat.add(ceph->inode.rstat);

  // create inodes dir
  CInode *inodes = container.create();
  inodes->set_stripe_auth(stripe_auth);
  CStripe *inodestripe = inodes->get_or_open_stripe(0);
  inodestripe->state_set(CStripe::STATE_OPEN);
  inodestripe->replicate = true;
  inodestripe->set_stripe_auth(mds->whoami);
  CDir *inodesdir = inodestripe->get_or_open_dirfrag(frag_t());
  inodes->inode.dirstat = inodestripe->fnode.fragstat;

  rootstripe->fnode.accounted_fragstat = rootstripe->fnode.fragstat;
  rootstripe->fnode.accounted_rstat = rootstripe->fnode.rstat;

  root->inode.dirstat = rootstripe->fnode.fragstat;
  root->inode.rstat = rootstripe->fnode.rstat;
  root->inode.accounted_rstat = root->inode.rstat;

  cephdir->mark_complete();
  cephdir->mark_dirty(ls);
  cephdir->commit(0, gather->new_sub());

  inodesdir->mark_complete();
  inodesdir->mark_dirty(ls);
  inodesdir->commit(0, gather->new_sub());

  rootdir->mark_complete();
  rootdir->mark_dirty(ls);
  rootdir->commit(0, gather->new_sub());

  cephstripe->mark_open();
  cephstripe->mark_dirty(ls);
  cephstripe->commit(gather->new_sub());

  inodestripe->mark_open();
  inodestripe->mark_dirty(ls);
  inodestripe->commit(gather->new_sub());

  rootstripe->mark_open();
  rootstripe->mark_dirty(ls);
  rootstripe->commit(gather->new_sub());

  root->store(gather->new_sub());
  inodes->store(gather->new_sub());
}

void MDCache::create_mydir_hierarchy(C_Gather *gather)
{
  // create mds dir
  char myname[10];
  snprintf(myname, sizeof(myname), "mds%d", mds->whoami);
  CInode *my = create_system_inode(MDS_INO_MDSDIR(mds->whoami),
                                   mds->whoami, S_IFDIR);
  //cephdir->add_remote_dentry(myname, MDS_INO_MDSDIR(mds->whoami), S_IFDIR);

  const vector<int> auth(1, mds->whoami);
  my->set_stripe_auth(auth);
  CStripe *mystripe = my->get_or_open_stripe(0);
  mystripe->set_stripe_auth(mds->whoami);
  CDir *mydir = mystripe->get_or_open_dirfrag(frag_t());

  // stray dir
  for (int i = 0; i < NUM_STRAY; ++i) {
    CInode *stray = create_system_inode(MDS_INO_STRAY(mds->whoami, i),
                                        mds->whoami, S_IFDIR);
    stray->set_stripe_auth(auth);
    CStripe *straystripe = stray->get_or_open_stripe(0);
    CDir *straydir = straystripe->get_or_open_dirfrag(frag_t());
    stringstream name;
    name << "stray" << i;
    CDentry *sdn = mydir->add_primary_dentry(name.str(), stray);
    sdn->_mark_dirty(mds->mdlog->get_current_segment());

    stray->inode.dirstat = straystripe->fnode.fragstat;

    mystripe->fnode.rstat.add(stray->inode.rstat);
    mystripe->fnode.fragstat.nsubdirs++;
    // save them
    straydir->mark_complete();
    straydir->mark_dirty(mds->mdlog->get_current_segment());
    straydir->commit(0, gather->new_sub());
  }

  CInode *journal = create_system_inode(MDS_INO_LOG_OFFSET + mds->whoami,
                                        mds->whoami, S_IFREG);
  string name = "journal";
  CDentry *jdn = mydir->add_primary_dentry(name, journal);
  jdn->_mark_dirty(mds->mdlog->get_current_segment());

  mystripe->fnode.fragstat.nfiles++;
  mystripe->fnode.rstat.rfiles++;
  mystripe->fnode.accounted_fragstat = mystripe->fnode.fragstat;
  mystripe->fnode.accounted_rstat = mystripe->fnode.rstat;

  myin->inode.dirstat = mystripe->fnode.fragstat;
  myin->inode.rstat = mystripe->fnode.rstat;
  myin->inode.accounted_rstat = myin->inode.rstat;


  mydir->mark_complete();
  mydir->mark_dirty(mds->mdlog->get_current_segment());
  mydir->commit(0, gather->new_sub());

  myin->store(gather->new_sub());
}

struct C_MDC_CreateSystemFile : public Context {
  MDCache *cache;
  Mutation *mut;
  CDentry *dn;
  Context *fin;
  C_MDC_CreateSystemFile(MDCache *c, Mutation *mu, CDentry *d, Context *f) :
    cache(c), mut(mu), dn(d), fin(f) {}
  void finish(int r) {
    cache->_create_system_file_finish(mut, dn, fin);
  }
};

void MDCache::_create_system_file(CDir *dir, const char *name, CInode *in, Context *fin)
{
  dout(10) << "_create_system_file " << name << " in " << *dir << dendl;
  CDentry *dn = dir->add_null_dentry(name);

  dn->push_projected_linkage(in);

  CDir *mdir = 0;
  if (in->inode.is_dir()) {
    in->inode.rstat.rsubdirs = 1;

    in->set_stripe_auth(vector<int>(1, mds->whoami));
    CStripe *stripe = in->get_or_open_stripe(0);
    stripe->mark_open();

    mdir = stripe->get_or_open_dirfrag(frag_t());
    mdir->mark_complete();
  } else
    in->inode.rstat.rfiles = 1;
  
  SnapRealm *realm = get_snaprealm();
  dn->first = in->first = realm->get_newest_seq() + 1;

  Mutation *mut = new Mutation;

  // force some locks.  hacky.
  mds->locker->wrlock_force(&dir->get_stripe()->linklock, mut);
  mds->locker->wrlock_force(&dir->get_stripe()->nestlock, mut);

  mut->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, "create system file");
  mds->mdlog->start_entry(le);

  if (!in->is_mdsdir()) {
    predirty_journal_parents(mut, &le->metablob, in, dir, PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  } else {
    predirty_journal_parents(mut, &le->metablob, in, dir, PREDIRTY_DIR, 1);
    journal_dirty_inode(mut, &le->metablob, in);
    dn->push_projected_linkage(in->ino(), in->d_type());
  }
  le->metablob.add_dentry(dn, true);
  le->metablob.add_inode(in, true);
  if (mdir)
    le->metablob.add_new_dir(mdir); // dirty AND complete AND new

  mds->mdlog->submit_entry(le);
  mds->mdlog->wait_for_safe(new C_MDC_CreateSystemFile(this, mut, dn, fin));
  mds->mdlog->flush();
}

void MDCache::_create_system_file_finish(Mutation *mut, CDentry *dn, Context *fin)
{
  dout(10) << "_create_system_file_finish " << *dn << dendl;
 
  dn->pop_projected_linkage();
  dn->mark_dirty(mut->ls);

  CInode *in = dn->get_linkage()->get_inode();
  in->mark_dirty(mut->ls);

  if (in->inode.is_dir()) {
    CStripe *stripe = in->get_stripe(0);
    stripe->mark_dirty(mut->ls);
    stripe->mark_new(mut->ls);

    CDir *dir = stripe->get_dirfrag(frag_t());
    dir->mark_dirty(mut->ls);
    dir->mark_new(mut->ls);
  }

  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  fin->finish(0);
  delete fin;
}



struct C_MDS_RetryOpenRoot : public Context {
  MDCache *cache;
  C_MDS_RetryOpenRoot(MDCache *c) : cache(c) {}
  void finish(int r) {
    if (r < 0)
      cache->mds->suicide();
    else
      cache->open_root();
  }
};

void MDCache::open_root_inode(Context *c)
{
  CInode *in;
  if (mds->whoami == mds->mdsmap->get_root()) {
    in = create_system_inode(MDS_INO_ROOT, mds->whoami, S_IFDIR|0755);  // initially inaccurate!
    in->fetch(c);
  } else {
    discover_base_ino(MDS_INO_ROOT, c, mds->mdsmap->get_root());
  }
}

void MDCache::open_mydir_inode(Context *c)
{
  CInode *in = create_system_inode(MDS_INO_MDSDIR(mds->whoami),
                                   mds->whoami, S_IFDIR|0755);  // initially inaccurate!
  in->fetch(c);
}

static void pin_stray(CInode *in)
{
  assert(in);
  if (!in->state_test(CInode::STATE_STRAYPINNED)) {
    in->get(CInode::PIN_STRAY);
    in->state_set(CInode::STATE_STRAYPINNED);
    in->get_stickystripes();
  }
}

static void unpin_stray(CInode *in)
{
  assert(in);
  if (in->state_test(CInode::STATE_STRAYPINNED)) {
    in->state_clear(CInode::STATE_STRAYPINNED);
    in->put(CInode::PIN_STRAY);
    in->put_stickystripes();
  }
}

void MDCache::open_root()
{
  dout(10) << "open_root" << dendl;

  if (!root) {
    open_root_inode(new C_MDS_RetryOpenRoot(this));
    return;
  }
  if (!container.get_inode()) {
    container.open(new C_MDS_RetryOpenRoot(this));
    return;
  }
  pin_stray(container.get_inode());
  if (mds->whoami == mds->mdsmap->get_root()) {
    assert(root->is_auth());  
    CStripe *rootstripe = root->get_or_open_stripe(0);
    rootstripe->set_stripe_auth(mds->whoami);
    CDir *rootdir = rootstripe->get_or_open_dirfrag(frag_t());
    if (!rootdir->is_complete()) {
      rootdir->fetch(new C_MDS_RetryOpenRoot(this));
      return;
    }
  } else {
    assert(!root->is_auth());
    CStripe *rootstripe = root->get_stripe(0);
    if (!rootstripe) {
      discover_dir_stripe(root, 0, new C_MDS_RetryOpenRoot(this));
      return;
    }
    assert(rootstripe);
    CDir *rootdir = rootstripe->get_dirfrag(frag_t());
    if (!rootdir) {
      discover_dir_frag(rootstripe, frag_t(), new C_MDS_RetryOpenRoot(this));
      return;
    }
    if (container.get_inode()->get_replica_nonce() == 0) {
      // must register with root mds as a replica
      discover_base_ino(MDS_INO_CONTAINER, new C_MDS_RetryOpenRoot(this), 0);
      return;
    }
  }

  if (!myin) {
    CInode *in = create_system_inode(MDS_INO_MDSDIR(mds->whoami),
                                     mds->whoami, S_IFDIR|0755);  // initially inaccurate!
    in->fetch(new C_MDS_RetryOpenRoot(this));
    return;
  }
  CStripe *mystripe = myin->get_or_open_stripe(0);
  mystripe->set_stripe_auth(mds->whoami);

  populate_mydir();
}

void MDCache::populate_mydir()
{
  assert(myin);
  CDir *mydir = myin->get_or_open_stripe(0)->get_or_open_dirfrag(frag_t());
  assert(mydir);

  dout(10) << "populate_mydir " << *mydir << dendl;

  if (!mydir->is_complete()) {
    mydir->fetch(new C_MDS_RetryOpenRoot(this));
    return;
  }    

  // open or create stray
  for (int i = 0; i < NUM_STRAY; ++i) {
    stringstream name;
    name << "stray" << i;
    CDentry *straydn = mydir->lookup(name.str());

    // allow for older fs's with stray instead of stray0
    if (straydn == NULL && i == 0)
      straydn = mydir->lookup("stray");

    if (!straydn || !straydn->get_linkage()->get_inode()) {
      CInode *in = create_system_inode(MDS_INO_STRAY(mds->whoami, i),
                                       mds->whoami, S_IFDIR);
      _create_system_file(mydir, name.str().c_str(), in,
                          new C_MDS_RetryOpenRoot(this));
      return;
    }
    assert(straydn);
    assert(strays[i]);
    // we make multiple passes through this method; make sure we only pin each stray once.
    pin_stray(strays[i]);
    dout(20) << " stray num " << i << " is " << *strays[i] << dendl;

    // open all frags
    list<frag_t> ls;
    CStripe *stripe = strays[i]->get_or_open_stripe(0);
    stripe->get_fragtree().get_leaves(ls);
    for (list<frag_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
      frag_t fg = *p;
      CDir *dir = stripe->get_dirfrag(fg);
      if (!dir)
	dir = stripe->get_or_open_dirfrag(fg);
      if (!dir->is_complete()) {
	dir->fetch(new C_MDS_RetryOpenRoot(this));
	return;
      }
    }
  }

  // open or create journal file
  string jname("journal");
  CDentry *jdn = mydir->lookup(jname);
  if (!jdn || !jdn->get_linkage()->get_inode()) {
    CInode *in = create_system_inode(MDS_INO_LOG_OFFSET + mds->whoami,
                                     mds->whoami, S_IFREG);
    _create_system_file(mydir, jname.c_str(), in,
                        new C_MDS_RetryOpenRoot(this));
    return;
  }

  // okay!
  dout(10) << "populate_mydir done" << dendl;
  assert(!open);    
  open = true;
  mds->queue_waiters(waiting_for_open);
}

void MDCache::open_foreign_mdsdir(inodeno_t ino, Context *fin)
{
  discover_base_ino(ino, fin, ino & (MAX_MDS-1));
}

CDentry *MDCache::get_or_create_stray_dentry(CInode *in)
{
  string straydname;
  in->name_stray_dentry(straydname);

  CInode *strayi = get_stray();
  assert(strayi);
  CStripe *stripe = strayi->get_or_open_stripe(0);
  frag_t fg = stripe->pick_dirfrag(straydname);
  CDir *straydir = stripe->get_dirfrag(fg);
  assert(straydir);
  CDentry *straydn = straydir->lookup(straydname);
  if (!straydn) {
    straydn = straydir->add_null_dentry(straydname);
    straydn->mark_new();
  } else 
    assert(straydn->get_projected_linkage()->is_null());

  return straydn;
}


MDSCacheObject *MDCache::get_object(MDSCacheObjectInfo &info) 
{
  // inode?
  if (info.ino) 
    return get_inode(info.ino, info.snapid);

  // dir or dentry.
  CDir *dir = get_dirfrag(info.dirfrag);
  if (!dir) return 0;
    
  if (info.dname.length()) 
    return dir->lookup(info.dname, info.snapid);
  else
    return dir;
}


// ===================================
// journal and snap/cow helpers


/*
 * find first inode in cache that follows given snapid.  otherwise, return current.
 */
CInode *MDCache::pick_inode_snap(CInode *in, snapid_t follows)
{
  dout(10) << "pick_inode_snap follows " << follows << " on " << *in << dendl;
  assert(in->last == CEPH_NOSNAP);

  SnapRealm *realm = get_snaprealm();
  const set<snapid_t>& snaps = realm->get_snaps();
  dout(10) << " realm " << *realm << dendl;
  dout(10) << " snaps " << snaps << dendl;

  if (snaps.empty())
    return in;

  for (set<snapid_t>::const_iterator p = snaps.upper_bound(follows);  // first item > follows
       p != snaps.end();
       p++) {
    CInode *t = get_inode(in->ino(), *p);
    if (t) {
      in = t;
      dout(10) << "pick_inode_snap snap " << *p << " found " << *in << dendl;
      break;
    }
  }
  return in;
}


/*
 * note: i'm currently cheating wrt dirty and inode.version on cow
 * items.  instead of doing a full dir predirty, i just take the
 * original item's version, and set the dirty flag (via
 * mutation::add_cow_{inode,dentry}() and mutation::apply().  that
 * means a special case in the dir commit clean sweep assertions.
 * bah.
 */
CInode *MDCache::cow_inode(CInode *in, snapid_t last)
{
  assert(last >= in->first);

  CInode *oldin = new CInode(this, true, in->first, last);
  oldin->inode = *in->get_previous_projected_inode();
  oldin->symlink = in->symlink;
  oldin->xattrs = *in->get_previous_projected_xattrs();

  oldin->inode.trim_client_ranges(last);

  in->first = last+1;

  dout(10) << "cow_inode " << *in << " to " << *oldin << dendl;
  add_inode(oldin);
 
  const set<snapid_t>& snaps = get_snaprealm()->get_snaps();

  // clone caps?
  for (map<client_t,Capability*>::iterator p = in->client_caps.begin();
      p != in->client_caps.end();
      p++) {
    client_t client = p->first;
    Capability *cap = p->second;
    int issued = cap->issued();
    if ((issued & CEPH_CAP_ANY_WR) &&
	cap->client_follows < last) {
      // note in oldin
      for (int i = 0; i < num_cinode_locks; i++) {
	if (issued & cinode_lock_info[i].wr_caps) {
	  int lockid = cinode_lock_info[i].lock;
	  SimpleLock *lock = oldin->get_lock(lockid);
	  assert(lock);
	  oldin->client_snap_caps[lockid].insert(client);
	  oldin->auth_pin(lock);
	  lock->set_state(LOCK_SNAP_SYNC);  // gathering
	  lock->get_wrlock(true);
	  dout(10) << " client." << client << " cap " << ccap_string(issued & cinode_lock_info[i].wr_caps)
		   << " wrlock lock " << *lock << " on " << *oldin << dendl;
	}
      }
      cap->client_follows = last;
      
      // we need snapflushes for any intervening snaps
      dout(10) << "  snaps " << snaps << dendl;
      for (set<snapid_t>::const_iterator q = snaps.lower_bound(oldin->first);
	   q != snaps.end() && *q <= last;
	   q++) {
	in->add_need_snapflush(oldin, *q, client);
      }
    } else {
      dout(10) << " ignoring client." << client << " cap follows " << cap->client_follows << dendl;
    }
  }

  return oldin;
}

void MDCache::journal_cow_dentry(Mutation *mut, EMetaBlob *metablob, CDentry *dn, snapid_t follows,
				 CInode **pcow_inode, CDentry::linkage_t *dnl)
{
  if (!dn) {
    dout(10) << "journal_cow_dentry got null CDentry, returning" << dendl;
    return;
  }
  dout(10) << "journal_cow_dentry follows " << follows << " on " << *dn << dendl;
  assert(dn->is_auth());

  // nothing to cow on a null dentry, fix caller
  if (!dnl)
    dnl = dn->get_projected_linkage();
  assert(!dnl->is_null());

  if (dnl->is_primary() && dnl->get_inode()->is_multiversion()) {
    // multiversion inode.
    CInode *in = dnl->get_inode();

    if (follows == CEPH_NOSNAP)
      follows = get_snaprealm()->get_newest_seq();

    if (in->get_projected_parent_dn() != dn &&
	follows+1 > dn->first) {
      snapid_t oldfirst = dn->first;
      dn->first = follows+1;
      CDentry *olddn = dn->dir->add_remote_dentry(dn->name, in->ino(),  in->d_type(),
						  oldfirst, follows);
      dout(10) << " olddn " << *olddn << dendl;
      metablob->add_dentry(olddn, true);
      mut->add_cow_dentry(olddn);

      // FIXME: adjust link count here?  hmm.
    }

    // already cloned?
    if (follows < in->first) {
      dout(10) << "journal_cow_dentry follows " << follows << " < first on " << *in << dendl;
      return;
    }

    in->cow_old_inode(follows, false);

  } else {
    if (follows == CEPH_NOSNAP)
      follows = get_snaprealm()->get_newest_seq();
    
    // already cloned?
    if (follows < dn->first) {
      dout(10) << "journal_cow_dentry follows " << follows << " < first on " << *dn << dendl;
      return;
    }
       
    // update dn.first before adding old dentry to cdir's map
    snapid_t oldfirst = dn->first;
    dn->first = follows+1;
    
    dout(10) << "    dn " << *dn << dendl;
    if (dnl->is_primary()) {
      assert(oldfirst == dnl->get_inode()->first);
      CInode *oldin = cow_inode(dnl->get_inode(), follows);
      mut->add_cow_inode(oldin);
      if (pcow_inode)
	*pcow_inode = oldin;
      CDentry *olddn = dn->dir->add_primary_dentry(dn->name, oldin, oldfirst, follows);
      dout(10) << " olddn " << *olddn << dendl;
      metablob->add_dentry(olddn, true);
      mut->add_cow_dentry(olddn);
    } else {
      assert(dnl->is_remote());
      CDentry *olddn = dn->dir->add_remote_dentry(dn->name, dnl->get_remote_ino(), dnl->get_remote_d_type(),
						  oldfirst, follows);
      dout(10) << " olddn " << *olddn << dendl;
      metablob->add_dentry(olddn, true);
      mut->add_cow_dentry(olddn);
    }
  }
}


void MDCache::journal_cow_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows,
				CInode **pcow_inode)
{
  dout(10) << "journal_cow_inode follows " << follows << " on " << *in << dendl;
  CDentry *dn = in->get_projected_parent_dn();
  journal_cow_dentry(mut, metablob, dn, follows, pcow_inode);
}

void MDCache::journal_dirty_inode(Mutation *mut, EMetaBlob *metablob, CInode *in, snapid_t follows)
{
  if (!in->is_base()) {
    if (follows == CEPH_NOSNAP && in->last != CEPH_NOSNAP)
      follows = in->first - 1;
    CDentry *dn = in->get_projected_parent_dn();
    if (!dn->get_projected_linkage()->is_null())  // no need to cow a null dentry
      journal_cow_dentry(mut, metablob, dn, follows);
  }
  metablob->add_inode(in, true);
}



// nested ---------------------------------------------------------------

void MDCache::project_rstat_inode_to_frag(CInode *cur, CStripe *parent, snapid_t first, int linkunlink)
{
  CDentry *parentdn = cur->get_projected_parent_dn();
  inode_t *curi = cur->get_projected_inode();

  if (cur->first > first)
    first = cur->first;

  dout(10) << "projected_rstat_inode_to_frag first " << first << " linkunlink " << linkunlink
	   << " " << *cur << dendl;
  dout(20) << "    frag head is [" << parent->first << ",head] " << dendl;
  dout(20) << " inode update is [" << first << "," << cur->last << "]" << dendl;

  /*
   * FIXME.  this incompletely propagates rstats to _old_ parents
   * (i.e. shortly after a directory rename).  but we need full
   * blown hard link backpointers to make this work properly...
   */
  snapid_t floor = parentdn->first;
  dout(20) << " floor of " << floor << " from parent dn " << *parentdn << dendl;

  if (cur->last >= floor)
    _project_rstat_inode_to_frag(*curi, MAX(first, floor), cur->last, parent, linkunlink);
      
  for (set<snapid_t>::iterator p = cur->dirty_old_rstats.begin();
       p != cur->dirty_old_rstats.end();
       p++) {
    old_inode_t& old = cur->old_inodes[*p];
    if (*p >= floor)
      _project_rstat_inode_to_frag(old.inode, MAX(old.first, floor), *p, parent);
  }
  cur->dirty_old_rstats.clear();
}


void MDCache::_project_rstat_inode_to_frag(inode_t& inode, snapid_t ofirst, snapid_t last,
					  CStripe *parent, int linkunlink)
{
  dout(10) << "_project_rstat_inode_to_frag [" << ofirst << "," << last << "]" << dendl;
  dout(20) << "  inode           rstat " << inode.rstat << dendl;
  dout(20) << "  inode accounted_rstat " << inode.accounted_rstat << dendl;
  nest_info_t delta;
  if (linkunlink == 0) {
    delta.add(inode.rstat);
    delta.sub(inode.accounted_rstat);
  } else if (linkunlink < 0) {
    delta.sub(inode.accounted_rstat);
  } else {
    delta.add(inode.rstat);
  }
  dout(20) << "                  delta " << delta << dendl;

  inode.accounted_rstat = inode.rstat;

  while (last >= ofirst) {
    /*
     * pick fnode version to update.  at each iteration, we want to
     * pick a segment ending in 'last' to update.  split as necessary
     * to make that work.  then, adjust first up so that we only
     * update one segment at a time.  then loop to cover the whole
     * [ofirst,last] interval.
     */    
    nest_info_t *prstat;
    snapid_t first;
    fnode_t *pf = parent->get_projected_fnode();
    if (last == CEPH_NOSNAP) {
      first = MAX(ofirst, parent->first);
      prstat = &pf->rstat;
      dout(20) << " projecting to head [" << first << "," << last << "] " << *prstat << dendl;

      if (first > parent->first &&
	  !(pf->rstat == pf->accounted_rstat)) {
	dout(10) << "  target snapped and not fully accounted, cow to dirty_old_rstat ["
		 << parent->first << "," << (first-1) << "] "
		 << " " << *prstat << "/" << pf->accounted_rstat
		 << dendl;
	parent->dirty_old_rstat[first-1].first = parent->first;
	parent->dirty_old_rstat[first-1].rstat = pf->rstat;
	parent->dirty_old_rstat[first-1].accounted_rstat = pf->accounted_rstat;
      }
      parent->first = first;
    } else if (last >= parent->first) {
      first = parent->first;
      parent->dirty_old_rstat[last].first = first;
      parent->dirty_old_rstat[last].rstat = pf->rstat;
      parent->dirty_old_rstat[last].accounted_rstat = pf->accounted_rstat;
      prstat = &parent->dirty_old_rstat[last].rstat;
      dout(10) << " projecting to newly split dirty_old_fnode [" << first << "," << last << "] "
	       << " " << *prstat << "/" << pf->accounted_rstat << dendl;
    } else {
      // be careful, dirty_old_rstat is a _sparse_ map.
      // sorry, this is ugly.
      first = ofirst;

      // find any intersection with last
      map<snapid_t,old_rstat_t>::iterator p = parent->dirty_old_rstat.lower_bound(last);
      if (p == parent->dirty_old_rstat.end()) {
	dout(20) << "  no dirty_old_rstat with last >= last " << last << dendl;
	if (!parent->dirty_old_rstat.empty() && parent->dirty_old_rstat.rbegin()->first >= first) {
	  dout(20) << "  last dirty_old_rstat ends at " << parent->dirty_old_rstat.rbegin()->first << dendl;
	  first = parent->dirty_old_rstat.rbegin()->first+1;
	}
      } else {
	// *p last is >= last
	if (p->second.first <= last) {
	  // *p intersects [first,last]
	  if (p->second.first < first) {
	    dout(10) << " splitting off left bit [" << p->second.first << "," << first-1 << "]" << dendl;
	    parent->dirty_old_rstat[first-1] = p->second;
	    p->second.first = first;
	  }
	  if (p->second.first > first)
	    first = p->second.first;
	  if (last < p->first) {
	    dout(10) << " splitting off right bit [" << last+1 << "," << p->first << "]" << dendl;
	    parent->dirty_old_rstat[last] = p->second;
	    p->second.first = last+1;
	  }
	} else {
	  // *p is to the _right_ of [first,last]
	  p = parent->dirty_old_rstat.lower_bound(first);
	  // new *p last is >= first
	  if (p->second.first <= last &&  // new *p isn't also to the right, and
	      p->first >= first) {        // it intersects our first bit,
	    dout(10) << " staying to the right of [" << p->second.first << "," << p->first << "]..." << dendl;
	    first = p->first+1;
	  }
	  dout(10) << " projecting to new dirty_old_rstat [" << first << "," << last << "]" << dendl;
	}
      }
      dout(20) << " projecting to dirty_old_rstat [" << first << "," << last << "]" << dendl;
      parent->dirty_old_rstat[last].first = first;
      prstat = &parent->dirty_old_rstat[last].rstat;
    }
    
    // apply
    dout(20) << "  project to [" << first << "," << last << "] " << *prstat << dendl;
    assert(last >= first);
    prstat->add(delta);
    inode.accounted_rstat = inode.rstat;
    dout(20) << "      result [" << first << "," << last << "] " << *prstat << " " << *parent << dendl;

    last = first-1;
  }
}

void MDCache::project_rstat_frag_to_inode(const nest_info_t& rstat,
                                          const nest_info_t& accounted_rstat,
					  snapid_t ofirst, snapid_t last, 
					  CInode *pin, bool cow_head)
{
  dout(10) << "project_rstat_frag_to_inode [" << ofirst << "," << last << "]" << dendl;
  dout(20) << "  frag           rstat " << rstat << dendl;
  dout(20) << "  frag accounted_rstat " << accounted_rstat << dendl;
  nest_info_t delta = rstat;
  delta.sub(accounted_rstat);
  dout(20) << "                 delta " << delta << dendl;

  while (last >= ofirst) {
    inode_t *pi;
    snapid_t first;
    if (last == pin->last) {
      pi = pin->get_projected_inode();
      first = MAX(ofirst, pin->first);
      if (first > pin->first) {
	old_inode_t& old = pin->cow_old_inode(first-1, cow_head);
	dout(20) << "   cloned old_inode rstat is " << old.inode.rstat << dendl;
      }
    } else {
      if (last >= pin->first) {
	first = pin->first;
	pin->cow_old_inode(last, cow_head);
      } else {
	// our life is easier here because old_inodes is not sparse
	// (although it may not begin at snapid 1)
	map<snapid_t,old_inode_t>::iterator p = pin->old_inodes.lower_bound(last);
	if (p == pin->old_inodes.end()) {
	  dout(10) << " no old_inode <= " << last << ", done." << dendl;
	  break;
	}
	first = p->second.first;
	if (first > last) {
	  dout(10) << " oldest old_inode is [" << first << "," << p->first << "], done." << dendl;
	  assert(p == pin->old_inodes.begin());
	  break;
	}
	if (p->first > last) {
	  dout(10) << " splitting right old_inode [" << first << "," << p->first << "] to ["
		   << (last+1) << "," << p->first << "]" << dendl;
	  pin->old_inodes[last] = p->second;
	  p->second.first = last+1;
	  pin->dirty_old_rstats.insert(p->first);
	}
      }
      if (first < ofirst) {
	dout(10) << " splitting left old_inode [" << first << "," << last << "] to ["
		 << first << "," << ofirst-1 << "]" << dendl;
	pin->old_inodes[ofirst-1] = pin->old_inodes[last];
	pin->dirty_old_rstats.insert(ofirst-1);
	pin->old_inodes[last].first = first = ofirst;
      }
      pi = &pin->old_inodes[last].inode;
      pin->dirty_old_rstats.insert(last);
    }
    dout(20) << " projecting to [" << first << "," << last << "] " << pi->rstat << dendl;
    pi->rstat.add(delta);
    dout(20) << "        result [" << first << "," << last << "] " << pi->rstat << dendl;

    if (pi->rstat.rbytes < 0)
      assert(!"negative rstat rbytes" == g_conf->mds_verify_scatter);

    last = first-1;
  }
}


/*
 * NOTE: we _have_ to delay the scatter if we are called during a
 * rejoin, because we can't twiddle locks between when the
 * rejoin_(weak|strong) is received and when we send the rejoin_ack.
 * normally, this isn't a problem: a recover mds doesn't twiddle locks
 * (no requests), and a survivor acks immediately.  _except_ that
 * during rejoin_(weak|strong) processing, we may complete a lock
 * gather, and do a scatter_writebehind.. and we _can't_ twiddle the
 * scatterlock state in that case or the lock states will get out of
 * sync between the auth and replica.
 *
 * the simple solution is to never do the scatter here.  instead, put
 * the scatterlock on a list if it isn't already wrlockable.  this is
 * probably the best plan anyway, since we avoid too many
 * scatters/locks under normal usage.
 */
/*
 * some notes on dirlock/nestlock scatterlock semantics:
 *
 * the fragstat (dirlock) will never be updated without
 * dirlock+nestlock wrlock held by the caller.
 *
 * the rstat (nestlock) _may_ get updated without a wrlock when nested
 * data is pushed up the tree.  this could be changed with some
 * restructuring here, but in its current form we ensure that the
 * fragstat+rstat _always_ reflect an accurrate summation over the dir
 * frag, which is nice.  and, we only need to track frags that need to
 * be nudged (and not inodes with pending rstat changes that need to
 * be pushed into the frag).  a consequence of this is that the
 * accounted_rstat on scatterlock sync may not match our current
 * rstat.  this is normal and expected.
 */
void MDCache::predirty_journal_parents(Mutation *mut, EMetaBlob *blob,
				       CInode *in, CDir *parent,
				       int flags, int linkunlink,
				       snapid_t cfollows)
{
  bool do_parent_mtime = flags & PREDIRTY_DIR;

  assert(mds->mdlog->entry_is_open());

  // declare now?
  if (mut->now == utime_t())
    mut->now = ceph_clock_now(g_ceph_context);

  if (in->is_base())
    return;

  dout(10) << "predirty_journal_parents"
	   << (do_parent_mtime ? " do_parent_mtime":"")
	   << " linkunlink=" <<  linkunlink
	   << " follows " << cfollows
	   << " " << *in << dendl;

  frag_info_t fragstat;
  nest_info_t rstat;

  if (linkunlink) {
    if (in->is_dir()) {
      fragstat.nsubdirs = rstat.rsubdirs = linkunlink;
    } else {
      fragstat.nfiles = rstat.rfiles = linkunlink;
    }
    fragstat.version = rstat.version = 1;
  }

  if (do_parent_mtime) {
    fragstat.mtime = rstat.rctime = mut->now;
    fragstat.version = rstat.version = 1;
  }

  parentstats.update(in, mut, blob, fragstat, rstat);
}


// ===================================
// slave requests


/*
 * some handlers for master requests with slaves.  we need to make 
 * sure slaves journal commits before we forget we mastered them and
 * remove them from the uncommitted_masters map (used during recovery
 * to commit|abort slaves).
 */
struct C_MDC_CommittedMaster : public Context {
  MDCache *cache;
  metareqid_t reqid;
  LogSegment *ls;
  list<Context*> waiters;
  C_MDC_CommittedMaster(MDCache *s, metareqid_t r, LogSegment *l, list<Context*> &w) :
    cache(s), reqid(r), ls(l) {
    waiters.swap(w);
  }
  void finish(int r) {
    cache->_logged_master_commit(reqid, ls, waiters);
  }
};

void MDCache::log_master_commit(metareqid_t reqid)
{
  dout(10) << "log_master_commit " << reqid << dendl;
  mds->mdlog->start_submit_entry(new ECommitted(reqid), 
				 new C_MDC_CommittedMaster(this, reqid, 
							   uncommitted_masters[reqid].ls,
							   uncommitted_masters[reqid].waiters));
  mds->mdcache->uncommitted_masters.erase(reqid);
}

void MDCache::_logged_master_commit(metareqid_t reqid, LogSegment *ls, list<Context*> &waiters)
{
  dout(10) << "_logged_master_commit " << reqid << dendl;
  ls->uncommitted_masters.erase(reqid);
  mds->queue_waiters(waiters);
}

// while active...

void MDCache::committed_master_slave(metareqid_t r, int from)
{
  dout(10) << "committed_master_slave mds." << from << " on " << r << dendl;
  assert(uncommitted_masters.count(r));
  uncommitted_masters[r].slaves.erase(from);
  if (uncommitted_masters[r].slaves.empty())
    log_master_commit(r);
}


/*
 * The mds could crash after receiving all slaves' commit acknowledgement,
 * but before journalling the ECommitted.
 */
void MDCache::finish_committed_masters()
{
  map<metareqid_t, umaster>::iterator p = uncommitted_masters.begin();
  while (p != uncommitted_masters.end()) {
    if (p->second.slaves.empty()) {
      metareqid_t reqid = p->first;
      dout(10) << "finish_committed_masters " << reqid << dendl;
      ++p;
      log_master_commit(reqid);
    } else {
      ++p;
    }
  }
}

/*
 * at end of resolve... we must journal a commit|abort for all slave
 * updates, before moving on.
 * 
 * this is so that the master can safely journal ECommitted on ops it
 * masters when it reaches up:active (all other recovering nodes must
 * complete resolve before that happens).
 */
struct C_MDC_SlaveCommit : public Context {
  MDCache *cache;
  int from;
  metareqid_t reqid;
  C_MDC_SlaveCommit(MDCache *c, int f, metareqid_t r) : cache(c), from(f), reqid(r) {}
  void finish(int r) {
    cache->_logged_slave_commit(from, reqid);
  }
};

void MDCache::_logged_slave_commit(int from, metareqid_t reqid)
{
  dout(10) << "_logged_slave_commit from mds." << from << " " << reqid << dendl;
  
  // send a message
  MMDSSlaveRequest *req = new MMDSSlaveRequest(reqid, 0, MMDSSlaveRequest::OP_COMMITTED);
  mds->send_message_mds(req, from);
}


void MDCache::resolve_start()
{
  dout(10) << "resolve_start " << *root << dendl;

  for (mds_slave_update_map::iterator p = uncommitted_slave_updates.begin();
       p != uncommitted_slave_updates.end(); p++)
    need_resolve_ack.insert(p->first);
}

void MDCache::send_resolves()
{
  // reset resolve state
  got_resolve.clear();

  // allocate resolve messages for everyone in recovery_set
  typedef map<int, MMDSResolve*> resolve_map;
  resolve_map resolves;
  for (set<int>::iterator p = recovery_set.begin(); p != recovery_set.end(); ++p)
    if (*p != mds->whoami)
      resolves[*p] = new MMDSResolve;

  // list prepare requests lacking a commit
  // [active survivor]
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
      p != active_requests.end();
      ++p) {
    if (!p->second->is_slave())
      continue;
    resolve_map::iterator r = resolves.find(p->second->slave_to_mds);
    if (r != resolves.end()) {
      dout(10) << " including uncommitted " << *p->second << dendl;
      r->second->add_slave_request(p->first);
    }
  }

  // [resolving]
  for (mds_slave_update_map::iterator p = uncommitted_slave_updates.begin();
       p != uncommitted_slave_updates.end();
       ++p) {
    resolve_map::iterator r = resolves.find(p->first);
    if (r == resolves.end())
      continue;
    for (req_slave_update_map::iterator q = p->second.begin();
         q != p->second.end();
         ++q) {
      dout(10) << " including uncommitted " << q->first
          << " for mds." << p->first << dendl;
      r->second->add_slave_request(q->first);
    }
  }

  // send all resolves
  for (resolve_map::iterator r = resolves.begin(); r != resolves.end(); ++r) {
    dout(10) << "sending " << *r->second << " to mds." << r->first << dendl;
    mds->send_message_mds(r->second, r->first);
  }
}

void MDCache::handle_mds_failure(int who)
{
  dout(7) << "handle_mds_failure mds." << who << dendl;
  
  // make note of recovery set
  mds->mdsmap->get_recovery_mds_set(recovery_set);
  recovery_set.erase(mds->get_nodeid());
  dout(1) << "handle_mds_failure mds." << who << " : recovery peers are " << recovery_set << dendl;

  // adjust my recovery lists
  wants_resolve.erase(who);   // MDS will ask again
  got_resolve.erase(who);     // i'll get another.

  rejoin_sent.erase(who);        // i need to send another
  rejoin_ack_gather.erase(who);  // i'll need/get another.

  dout(10) << " wants_resolve " << wants_resolve << dendl;
  dout(10) << " got_resolve " << got_resolve << dendl;
  dout(10) << " rejoin_sent " << rejoin_sent << dendl;
  dout(10) << " rejoin_gather " << rejoin_gather << dendl;
  dout(10) << " rejoin_ack_gather " << rejoin_ack_gather << dendl;

 
  // tell the migrator too.
  migrator->handle_mds_failure_or_stop(who);

  // clean up any requests slave to/from this node
  list<MDRequest*> finish;
  for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
       p != active_requests.end();
       ++p) {
    // slave to the failed node?
    if (p->second->slave_to_mds == who) {
      if (p->second->slave_did_prepare()) {
	need_resolve_ack.insert(who);
	dout(10) << " slave request " << *p->second << " uncommitted, will resolve shortly" << dendl;
      } else {
	dout(10) << " slave request " << *p->second << " has no prepare, finishing up" << dendl;
	if (p->second->slave_request)
	  p->second->aborted = true;
	else
	  finish.push_back(p->second);
      }
    }
    
    // failed node is slave?
    if (p->second->is_master() && !p->second->committing) {
      if (p->second->more()->witnessed.count(who)) {
	dout(10) << " master request " << *p->second << " no longer witnessed by slave mds." << who
		 << dendl;
	// discard this peer's prepare (if any)
	p->second->more()->witnessed.erase(who);
      }
      
      if (p->second->more()->waiting_on_slave.count(who)) {
	dout(10) << " master request " << *p->second << " waiting for slave mds." << who
		 << " to recover" << dendl;
	// retry request when peer recovers
	p->second->more()->waiting_on_slave.erase(who);
	mds->wait_for_active_peer(who, new C_MDS_RetryRequest(this, p->second));
      }

      if (p->second->has_more() && p->second->more()->is_ambiguous_auth &&
	  p->second->more()->rename_inode->authority().first == who) {
	dout(10) << " master request " << *p->second << " waiting for renamed inode's auth mds." << who
		 << " to recover" << dendl;
	p->second->clear_ambiguous_auth();
      }

      if (p->second->locking && p->second->locking_target_mds == who)
	p->second->finish_locking(p->second->locking);
    }
  }

  while (!finish.empty()) {
    dout(10) << "cleaning up slave request " << *finish.front() << dendl;
    request_finish(finish.front());
    finish.pop_front();
  }

  kick_find_ino_peers(who);
}

/*
 * handle_mds_recovery - called on another node's transition 
 * from resolve -> active.
 */
void MDCache::handle_mds_recovery(int who)
{
  dout(7) << "handle_mds_recovery mds." << who << dendl;

  list<Context*> waiters;
#if 0
  // wake up any waiters in their subtrees
  for (map<CStripe*,set<CStripe*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CStripe *stripe = p->first;

    if (stripe->authority().first != who ||
	stripe->authority().second == mds->whoami)
      continue;
    assert(!stripe->is_auth());
   
    // wake any waiters
    list<CStripe*> q;
    q.push_back(stripe);

    while (!q.empty()) {
      CStripe *stripe = q.front();
      q.pop_front();
      stripe->take_waiting(CStripe::WAIT_ANY_MASK, waiters);

      list<CDir*> ls;
      stripe->get_dirfrags(ls);
      for (list<CDir*>::iterator p = ls.begin();
           p != ls.end();
           ++p) {
        CDir *dir = *p;
        dir->take_waiting(CDir::WAIT_ANY_MASK, waiters);

        // inode waiters too
        for (CDir::map_t::iterator p = dir->items.begin();
             p != dir->items.end();
             ++p) {
          CDentry *dn = p->second;
          CDentry::linkage_t *dnl = dn->get_linkage();
          if (!dnl->is_primary())
            continue;

          CInode *in = dnl->get_inode();
          in->take_waiting(CInode::WAIT_ANY_MASK, waiters);
	  
	  // recurse?
          int count = in->get_stripe_count();
          for (int i = 0; i < count; i++) {
            CStripe *substripe = in->get_stripe(i);
            if (substripe && !substripe->is_subtree_root())
	      q.push_back(substripe);
          }
	}
      }
    }
  }
#endif
  kick_discovers(who);

  kick_find_ino_peers(who);

  // queue them up.
  mds->queue_waiters(waiters);
}

void MDCache::set_recovery_set(set<int>& s) 
{
  dout(7) << "set_recovery_set " << s << dendl;
  recovery_set = s;
}


/*
 * during resolve state, we share resolves to determine who
 * is authoritative for which trees.  we expect to get an resolve
 * from _everyone_ in the recovery_set (the mds cluster at the time of
 * the first failure).
 *
 * This functions puts the passed message before returning
 */
void MDCache::handle_resolve(MMDSResolve *m)
{
  dout(7) << "handle_resolve from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (mds->get_state() < MDSMap::STATE_RESOLVE) {
    if (mds->get_want_state() == CEPH_MDS_STATE_RESOLVE) {
      mds->wait_for_resolve(new C_MDS_RetryMessage(mds, m));
      return;
    }
    // wait until we reach the resolve stage!
    m->put();
    return;
  }

  // ambiguous slave requests?
  if (!m->slave_requests.empty()) {
    MMDSResolveAck *ack = new MMDSResolveAck;
    for (vector<metareqid_t>::iterator p = m->slave_requests.begin();
	 p != m->slave_requests.end();
	 ++p) {
      if (uncommitted_masters.count(*p)) {  //mds->sessionmap.have_completed_request(*p)) {
	// COMMIT
	dout(10) << " ambiguous slave request " << *p << " will COMMIT" << dendl;
	ack->add_commit(*p);
	uncommitted_masters[*p].slaves.insert(from);   // wait for slave OP_COMMITTED before we log ECommitted
      } else {
	// ABORT
	dout(10) << " ambiguous slave request " << *p << " will ABORT" << dendl;
	ack->add_abort(*p);      
      }
    }
    mds->send_message(ack, m->get_connection());
  }

  // did i get them all?
  got_resolve.insert(from);
 
  maybe_resolve_finish();

  m->put();
}

void MDCache::maybe_resolve_finish()
{
  if (!need_resolve_ack.empty()) {
    dout(10) << "maybe_resolve_finish still waiting for acks from "
        << need_resolve_ack << dendl;
    return;
  }
  if (!need_resolve_rollback.empty()) {
    dout(10) << "maybe_resolve_finish still waiting for rollbacks from "
        << need_resolve_rollback << dendl;
    return;
  }
  if (got_resolve != recovery_set) {
    dout(10) << "maybe_resolve_finish still waiting for more resolves, got (" 
	     << got_resolve << "), need (" << recovery_set << ")" << dendl;
    return;
  }

  dout(10) << "maybe_resolve_finish got all resolves+resolve_acks, done." << dendl;
  if (mds->is_resolve()) {
    trim_unlinked_inodes();
    recalc_auth_bits();
    trim_non_auth(); 
    mds->resolve_done();
  }
}

/* This functions puts the passed message before returning */
void MDCache::handle_resolve_ack(MMDSResolveAck *ack)
{
  dout(10) << "handle_resolve_ack " << *ack << " from " << ack->get_source() << dendl;
  int from = ack->get_source().num();

  if (!need_resolve_ack.count(from)) {
    ack->put();
    return;
  }

  for (vector<metareqid_t>::iterator p = ack->commit.begin();
       p != ack->commit.end();
       ++p) {
    dout(10) << " commit on slave " << *p << dendl;
    
    if (mds->is_resolve()) {
      // replay
      MDSlaveUpdate *su = get_uncommitted_slave_update(*p, from);
      assert(su);

      // log commit
      mds->mdlog->start_submit_entry(new ESlaveUpdate(mds->mdlog, "unknown", *p, from,
						      ESlaveUpdate::OP_COMMIT, su->origop));
      mds->mdlog->wait_for_safe(new C_MDC_SlaveCommit(this, from, *p));
      mds->mdlog->flush();

      finish_uncommitted_slave_update(*p, from);
    } else {
      MDRequest *mdr = request_get(*p);
      assert(mdr->slave_request == 0);  // shouldn't be doing anything!
      request_finish(mdr);
    }
  }

  for (vector<metareqid_t>::iterator p = ack->abort.begin();
       p != ack->abort.end();
       ++p) {
    dout(10) << " abort on slave " << *p << dendl;

    if (mds->is_resolve()) {
      MDSlaveUpdate *su = get_uncommitted_slave_update(*p, from);
      assert(su);

      // perform rollback (and journal a rollback entry)
      // note: this will hold up the resolve a bit, until the rollback entries journal.
      switch (su->origop) {
      case ESlaveUpdate::LINK:
	mds->server->do_link_rollback(su->rollback, from, 0);
	break;
      case ESlaveUpdate::RENAME:
	mds->server->do_rename_rollback(su->rollback, from, 0);
	break;
      case ESlaveUpdate::RMDIR:
	mds->server->do_rmdir_rollback(su->rollback, from, 0);
	break;
      case ESlaveUpdate::MKDIR:
	mds->server->do_mkdir_rollback(su->rollback, from, 0);
	break;
      default:
	assert(0);
      }
    } else {
      MDRequest *mdr = request_get(*p);
      if (mdr->more()->slave_commit) {
	Context *fin = mdr->more()->slave_commit;
	mdr->more()->slave_commit = 0;
	fin->finish(-1);
	delete fin;
      } else {
	if (mdr->slave_request) 
	  mdr->aborted = true;
	else
	  request_finish(mdr);
      }
    }
  }

  if (!mds->is_resolve()) {
    for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
	p != active_requests.end(); ++p)
      assert(p->second->slave_to_mds != from);
  }

  need_resolve_ack.erase(from);
  maybe_resolve_finish();

  ack->put();
}

void MDCache::add_uncommitted_slave_update(metareqid_t reqid, int master, MDSlaveUpdate *su)
{
  assert(uncommitted_slave_updates[master].count(reqid) == 0);
  uncommitted_slave_updates[master][reqid] = su;
  for(set<CStripe*>::iterator p = su->oldstripes.begin(); p != su->oldstripes.end(); p++)
    uncommitted_slave_rename_oldstripe[*p]++;
  for(set<CInode*>::iterator p = su->unlinked.begin(); p != su->unlinked.end(); p++)
    uncommitted_slave_unlink[*p]++;
}

void MDCache::finish_uncommitted_slave_update(metareqid_t reqid, int master)
{
  assert(uncommitted_slave_updates[master].count(reqid));
  MDSlaveUpdate* su = uncommitted_slave_updates[master][reqid];

  uncommitted_slave_updates[master].erase(reqid);
  if (uncommitted_slave_updates[master].empty())
    uncommitted_slave_updates.erase(master);
  // discard the non-auth subtree we renamed out of
  for(set<CStripe*>::iterator p = su->oldstripes.begin(); p != su->oldstripes.end(); p++) {
    CStripe *stripe = *p;
    uncommitted_slave_rename_oldstripe[stripe]--;
    if (uncommitted_slave_rename_oldstripe[stripe] == 0)
      uncommitted_slave_rename_oldstripe.erase(stripe);
  }
  // removed the inodes that were unlinked by slave update
  for(set<CInode*>::iterator p = su->unlinked.begin(); p != su->unlinked.end(); p++) {
    CInode *in = *p;
    uncommitted_slave_unlink[in]--;
    if (uncommitted_slave_unlink[in] == 0) {
      uncommitted_slave_unlink.erase(in);
      if (!in->get_projected_parent_dn())
	mds->mdcache->remove_inode_recursive(in);
    }
  }
  delete su;
}

MDSlaveUpdate* MDCache::get_uncommitted_slave_update(metareqid_t reqid, int master)
{

  MDSlaveUpdate* su = NULL;
  if (uncommitted_slave_updates.count(master) &&
      uncommitted_slave_updates[master].count(reqid)) {
    su = uncommitted_slave_updates[master][reqid];
    assert(su);
  }
  return su;
}

void MDCache::finish_rollback(metareqid_t reqid) {
  assert(need_resolve_rollback.count(reqid));
  if (mds->is_resolve())
    finish_uncommitted_slave_update(reqid, need_resolve_rollback[reqid]);
  need_resolve_rollback.erase(reqid);
  maybe_resolve_finish();
}

void MDCache::remove_inode_recursive(CInode *in)
{
  dout(10) << "remove_inode_recursive " << *in << dendl;

  int count = in->get_stripe_count();
  for (int i = 0; i < count; i++) {
    CStripe *stripe = in->get_stripe(i);
    if (stripe == NULL)
      continue;

    dout(10) << " removing stripe " << *stripe << dendl;
    list<CDir*> ls;
    stripe->get_dirfrags(ls);
    list<CDir*>::iterator p = ls.begin();
    while (p != ls.end()) {
      CDir *subdir = *p++;

      dout(10) << " removing dirfrag " << *subdir << dendl;
      CDir::map_t::iterator q = subdir->items.begin();
      while (q != subdir->items.end()) {
        CDentry *dn = q->second;
        ++q;
        CDentry::linkage_t *dnl = dn->get_linkage();
        if (dnl->is_primary()) {
          CInode *tin = dnl->get_inode();
          subdir->unlink_inode(dn);
          remove_inode_recursive(tin);
        }
        subdir->remove_dentry(dn);
      }
      stripe->close_dirfrag(subdir->get_frag());
    }

    in->close_stripe(stripe);
  }
  remove_inode(in);
}

void MDCache::trim_unlinked_inodes()
{
  dout(7) << "trim_unlinked_inodes" << dendl;
  list<CInode*> q;
  for (inode_map::iterator p = inodes.begin(); p != inodes.end(); ++p) {
    CInode *in = p->second;
    if (in->get_parent_dn() == NULL && !in->is_base()) {
      dout(7) << " will trim from " << *in << dendl;
      q.push_back(in);
    }
  }
  for (list<CInode*>::iterator p = q.begin(); p != q.end(); p++)
    remove_inode_recursive(*p);
}


// ===========================================================================
// REJOIN

/*
 * notes on scatterlock recovery:
 *
 * - recovering inode replica sends scatterlock data for any subtree
 *   roots (the only ones that are possibly dirty).
 *
 * - surviving auth incorporates any provided scatterlock data.  any
 *   pending gathers are then finished, as with the other lock types.
 *
 * that takes care of surviving auth + (recovering replica)*.
 *
 * - surviving replica sends strong_inode, which includes current
 *   scatterlock state, AND any dirty scatterlock data.  this
 *   provides the recovering auth with everything it might need.
 * 
 * - recovering auth must pick initial scatterlock state based on
 *   (weak|strong) rejoins.
 *   - always assimilate scatterlock data (it can't hurt)
 *   - any surviving replica in SCATTER state -> SCATTER.  otherwise, SYNC.
 *   - include base inode in ack for all inodes that saw scatterlock content
 *
 * also, for scatter gather,
 *
 * - auth increments {frag,r}stat.version on completion of any gather.
 *
 * - auth incorporates changes in a gather _only_ if the version
 *   matches.
 *
 * - replica discards changes any time the scatterlock syncs, and
 *   after recovery.
 */

/*
 * rejoin phase!
 *
 * this initiates rejoin.  it shoudl be called before we get any
 * rejoin or rejoin_ack messages (or else mdsmap distribution is broken).
 *
 * we start out by sending rejoins to everyone in the recovery set.
 *
 * if we are rejoin, send for all regions in our cache.
 * if we are active|stopping, send only to nodes that are are rejoining.
 */
void MDCache::rejoin_send_rejoins()
{
  dout(10) << "rejoin_send_rejoins with recovery_set " << recovery_set << dendl;

  typedef map<int, MMDSCacheRejoin*> rejoin_map;
  rejoin_map rejoins;

  // encode cap list once.
  bufferlist cap_export_bl;
  if (mds->is_rejoin()) {
    ::encode(cap_exports, cap_export_bl);
    ::encode(cap_export_paths, cap_export_bl);
  }

  // if i am rejoining, send a rejoin to everyone.
  // otherwise, just send to others who are rejoining.
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) {
    if (*p == mds->get_nodeid())  continue;  // nothing to myself!
    if (rejoin_sent.count(*p)) continue;     // already sent a rejoin to this node!
    if (mds->is_rejoin()) {
      rejoin_gather.insert(*p);
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_WEAK);
      rejoins[*p]->copy_cap_exports(cap_export_bl);
    } else if (mds->mdsmap->is_rejoin(*p))
      rejoins[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_STRONG);
  }	
  
  assert(!migrator->is_importing());
  assert(!migrator->is_exporting());

  list<CStripe*> stripes;

  // share inode lock state
  for (inode_map::iterator i = inodes.begin(); i != inodes.end(); ++i) {
    CInode *in = i->second;
    int who = in->authority().first;
    rejoin_map::iterator p = rejoins.find(who);
    if (p == rejoins.end())
      continue;

    dout(15) << "rejoining " << *in << " for mds." << who << dendl;
    p->second->add_weak_inode(in->vino());
    if (!mds->is_rejoin())
      p->second->add_strong_inode(in->vino(),
                                  in->get_caps_wanted(),
                                  in->filelock.get_state(),
                                  in->nestlock.get_state());

    if (in->is_dirty_scattered())
      p->second->add_scatterlock_state(in);
    if (in->is_dir())
      in->get_stripes(stripes);
  }

  for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
    CStripe *stripe = *s;
    int who = stripe->authority().first;
    rejoin_map::iterator p = rejoins.find(who);
    if (p == rejoins.end())
      continue;

    dout(15) << "rejoining " << *stripe << " for mds." << who << dendl;
    p->second->add_weak_stripe(stripe->dirstripe());
  }
#if 0 
  // check all subtrees
  for (map<CStripe*, set<CStripe*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    CStripe *stripe = p->first;
    assert(stripe->is_subtree_root());
    assert(!stripe->is_ambiguous_stripe_auth());

    // my subtree?
    if (stripe->is_auth())
      continue;  // skip my own regions!

    int auth = stripe->get_stripe_auth().first;
    assert(auth >= 0);
    if (rejoins.count(auth) == 0)
      continue;   // don't care about this node's subtrees

    rejoin_walk(stripe, rejoins[auth]);
  }

  // rejoin root inodes, too
  for (map<int, MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    if (mds->is_rejoin()) {
      // weak
      if (p->first == 0) {
        if (root) {
          p->second->add_weak_inode(root->vino());
          if (root->is_dirty_scattered()) {
            dout(10) << " sending scatterlock state on root " << *root << dendl;
            p->second->add_scatterlock_state(root);
          }
	}
        CInode *ctnr = container.get_inode();
        if (ctnr) {
          p->second->add_weak_inode(ctnr->vino());
          if (ctnr->is_dirty_scattered()) {
            dout(10) << " sending scatterlock state on container " << *ctnr << dendl;
            p->second->add_scatterlock_state(ctnr);
          }
	}
      }
      if (CInode *in = get_inode(MDS_INO_MDSDIR(p->first))) { 
	if (in)
	  p->second->add_weak_inode(in->vino());
      }
    } else {
      // strong
      if (p->first == 0) {
        if (root) {
          p->second->add_weak_inode(root->vino());
          p->second->add_strong_inode(root->vino(),
                                      root->get_caps_wanted(),
                                      root->filelock.get_state(),
                                      root->nestlock.get_state());
          if (root->is_dirty_scattered()) {
            dout(10) << " sending scatterlock state on root " << *root << dendl;
            p->second->add_scatterlock_state(root);
          }
	}
        CInode *ctnr = container.get_inode();
        if (ctnr) {
          p->second->add_weak_inode(ctnr->vino());
          p->second->add_strong_inode(ctnr->vino(),
                                      ctnr->get_caps_wanted(),
                                      ctnr->filelock.get_state(),
                                      ctnr->nestlock.get_state());
          if (ctnr->is_dirty_scattered()) {
            dout(10) << " sending scatterlock state on container " << *ctnr << dendl;
            p->second->add_scatterlock_state(ctnr);
          }
	}
      }

      if (CInode *in = get_inode(MDS_INO_MDSDIR(p->first))) {
	p->second->add_weak_inode(in->vino());
	p->second->add_strong_inode(in->vino(),
				    in->get_caps_wanted(),
				    in->filelock.get_state(),
				    in->nestlock.get_state());
      }
    }
  }  
#endif
  if (!mds->is_rejoin()) {
    // i am survivor.  send strong rejoin.
    // note request remote_auth_pins, xlocks
    for (hash_map<metareqid_t, MDRequest*>::iterator p = active_requests.begin();
	 p != active_requests.end();
	 ++p) {
      // auth pins
      for (set<MDSCacheObject*>::iterator q = p->second->remote_auth_pins.begin();
	   q != p->second->remote_auth_pins.end();
	   ++q) {
	if (!(*q)->is_auth()) {
	  int who = (*q)->authority().first;
	  if (rejoins.count(who) == 0) continue;
	  MMDSCacheRejoin *rejoin = rejoins[who];
	  
	  dout(15) << " " << *p->second << " authpin on " << **q << dendl;
	  MDSCacheObjectInfo i;
	  (*q)->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_authpin(vinodeno_t(i.ino, i.snapid), p->second->reqid, p->second->attempt);
	  else
	    rejoin->add_dentry_authpin(i.dirfrag, i.dname, i.snapid, p->second->reqid, p->second->attempt);

	  if (p->second->has_more() && p->second->more()->is_remote_frozen_authpin &&
	      p->second->more()->rename_inode == (*q))
	    rejoin->add_inode_frozen_authpin(vinodeno_t(i.ino, i.snapid),
					     p->second->reqid, p->second->attempt);
	}
      }
      // xlocks
      for (set<SimpleLock*>::iterator q = p->second->xlocks.begin();
	   q != p->second->xlocks.end();
	   ++q) {
	if (!(*q)->get_parent()->is_auth()) {
	  int who = (*q)->get_parent()->authority().first;
	  if (rejoins.count(who) == 0) continue;
	  MMDSCacheRejoin *rejoin = rejoins[who];
	  
	  dout(15) << " " << *p->second << " xlock on " << **q << " " << *(*q)->get_parent() << dendl;
	  MDSCacheObjectInfo i;
	  (*q)->get_parent()->set_object_info(i);
	  if (i.ino)
	    rejoin->add_inode_xlock(vinodeno_t(i.ino, i.snapid), (*q)->get_type(),
				    p->second->reqid, p->second->attempt);
	  else
	    rejoin->add_dentry_xlock(i.dirfrag, i.dname, i.snapid,
				     p->second->reqid, p->second->attempt);
	}
      }
      // remote wrlocks
      for (map<SimpleLock*, int>::iterator q = p->second->remote_wrlocks.begin();
	   q != p->second->remote_wrlocks.end();
	   ++q) {
	int who = q->second;
	if (rejoins.count(who) == 0) continue;
	MMDSCacheRejoin *rejoin = rejoins[who];

	dout(15) << " " << *p->second << " wrlock on " << q->second
		 << " " << q->first->get_parent() << dendl;
	MDSCacheObjectInfo i;
	q->first->get_parent()->set_object_info(i);
	assert(i.ino);
	rejoin->add_inode_wrlock(vinodeno_t(i.ino, i.snapid), q->first->get_type(),
				 p->second->reqid, p->second->attempt);
      }
    }
  }

  // send the messages
  for (map<int,MMDSCacheRejoin*>::iterator p = rejoins.begin();
       p != rejoins.end();
       ++p) {
    assert(rejoin_sent.count(p->first) == 0);
    assert(rejoin_ack_gather.count(p->first) == 0);
    rejoin_sent.insert(p->first);
    rejoin_ack_gather.insert(p->first);
    mds->send_message_mds(p->second, p->first);
  }
  rejoin_ack_gather.insert(mds->whoami);   // we need to complete rejoin_gather_finish, too

  // nothing?
  if (mds->is_rejoin() && rejoins.empty()) {
    dout(10) << "nothing to rejoin" << dendl;
    rejoin_gather_finish();
  }
}

#if 0
/** 
 * rejoin_walk - build rejoin declarations for a subtree
 * 
 * @param stripe subtree root
 * @param rejoin rejoin message
 *
 * from a rejoining node:
 *  weak stripe
 *  weak dirfrag
 *  weak dentries (w/ connectivity)
 *
 * from a surviving node:
 *  strong stripe
 *  strong dirfrag
 *  strong dentries (no connectivity!)
 *  strong inodes
 */
void MDCache::rejoin_walk(CStripe *stripe, MMDSCacheRejoin *rejoin)
{
  dout(10) << "rejoin_walk " << *stripe << dendl;

  list<CStripe*> nested;  // finish this stripe, then do nested items
 
  if (mds->is_rejoin()) {
    // WEAK
    rejoin->add_weak_stripe(stripe->dirstripe());
    list<CDir*> dirs;
    stripe->get_dirfrags(dirs);
    for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d) {
      CDir *dir = *d;
      rejoin->add_weak_dirfrag(dir->dirfrag());
      for (CDir::map_t::iterator p = dir->items.begin();
           p != dir->items.end();
           ++p) {
        CDentry *dn = p->second;
        CDentry::linkage_t *dnl = dn->get_linkage();
        dout(15) << " add_weak_primary_dentry " << *dn << dendl;
        assert(dnl->is_primary());
        CInode *in = dnl->get_inode();
        assert(dnl->get_inode()->is_dir());
        rejoin->add_weak_primary_dentry(stripe->dirstripe(), dn->name.c_str(),
                                        dn->first, dn->last, in->ino());
        in->get_nested_stripes(nested);
        if (in->is_dirty_scattered()) {
          dout(10) << " sending scatterlock state on " << *in << dendl;
          rejoin->add_scatterlock_state(in);
        }
      }
    }
  } else {
    // STRONG
    dout(15) << " add_strong_stripe " << *stripe << dendl;
    rejoin->add_strong_stripe(stripe->dirstripe(), stripe->get_replica_nonce());

    list<CDir*> dirs;
    stripe->get_dirfrags(dirs);
    for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d) {
      CDir *dir = *d;
      dout(15) << " add_strong_dirfrag " << *dir << dendl;
      rejoin->add_strong_dirfrag(dir->dirfrag(), dir->get_replica_nonce());

      for (CDir::map_t::iterator p = dir->items.begin();
           p != dir->items.end();
           ++p) {
        CDentry *dn = p->second;
        CDentry::linkage_t *dnl = dn->get_linkage();
        dout(15) << " add_strong_dentry " << *dn << dendl;
        rejoin->add_strong_dentry(dir->dirfrag(), dn->name, dn->first, dn->last,
                                  dnl->is_primary() ? dnl->get_inode()->ino():inodeno_t(0),
                                  dnl->is_remote() ? dnl->get_remote_ino():inodeno_t(0),
                                  dnl->is_remote() ? dnl->get_remote_d_type():0, 
                                  dn->get_replica_nonce(),
                                  dn->lock.get_state());
        if (dnl->is_primary()) {
          CInode *in = dnl->get_inode();
          dout(15) << " add_strong_inode " << *in << dendl;
          rejoin->add_strong_inode(in->vino(),
                                   in->get_caps_wanted(),
                                   in->filelock.get_state(),
                                   in->nestlock.get_state());
          in->get_nested_stripes(nested);
          if (in->is_dirty_scattered()) {
            dout(10) << " sending scatterlock state on " << *in << dendl;
            rejoin->add_scatterlock_state(in);
          }
	}
      }
    }
  }

  // recurse into nested stripes
  for (list<CStripe*>::iterator p = nested.begin(); p != nested.end(); ++p)
    rejoin_walk(*p, rejoin);
}
#endif

/*
 * i got a rejoin.
 *  - reply with the lockstate
 *
 * if i am active|stopping, 
 *  - remove source from replica list for everything not referenced here.
 * This function puts the passed message before returning.
 */
void MDCache::handle_cache_rejoin(MMDSCacheRejoin *m)
{
  dout(7) << "handle_cache_rejoin " << *m << " from " << m->get_source() 
	  << " (" << m->get_payload().length() << " bytes)"
	  << dendl;

  switch (m->op) {
  case MMDSCacheRejoin::OP_WEAK:
    handle_cache_rejoin_weak(m);
    break;
  case MMDSCacheRejoin::OP_STRONG:
    handle_cache_rejoin_strong(m);
    break;

  case MMDSCacheRejoin::OP_ACK:
    handle_cache_rejoin_ack(m);
    break;
  case MMDSCacheRejoin::OP_MISSING:
    handle_cache_rejoin_missing(m);
    break;

  case MMDSCacheRejoin::OP_FULL:
    handle_cache_rejoin_full(m);
    break;

  default: 
    assert(0);
  }
  m->put();
}


/*
 * handle_cache_rejoin_weak
 *
 * the sender 
 *  - is recovering from their journal.
 *  - may have incorrect (out of date) inode contents
 *  - will include weak dirfrag if sender is dirfrag auth and parent inode auth is recipient
 *
 * if the sender didn't trim_non_auth(), they
 *  - may have incorrect (out of date) dentry/inode linkage
 *  - may have deleted/purged inodes
 * and i may have to go to disk to get accurate inode contents.  yuck.
 * This functions DOES NOT put the passed message before returning
 */
void MDCache::handle_cache_rejoin_weak(MMDSCacheRejoin *weak)
{
  int from = weak->get_source().num();

  // possible response(s)
  MMDSCacheRejoin *ack = 0;      // if survivor
  set<vinodeno_t> acked_inodes;  // if survivor
  bool survivor = false;  // am i a survivor?

  if (mds->is_clientreplay() || mds->is_active() || mds->is_stopping()) {
    survivor = true;
    dout(10) << "i am a surivivor, and will ack immediately" << dendl;
    ack = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);

    // check cap exports
    for (map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> >::iterator p = weak->cap_exports.begin();
	 p != weak->cap_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      if (!in || !in->is_auth()) continue;
      for (map<client_t,ceph_mds_cap_reconnect>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " claiming cap import " << p->first << " client." << q->first << " on " << *in << dendl;
	rejoin_import_cap(in, q->first, q->second, from);
      }
    }
  } else {
    assert(mds->is_rejoin());

    // check cap exports.
    for (map<inodeno_t,map<client_t,ceph_mds_cap_reconnect> >::iterator p = weak->cap_exports.begin();
	 p != weak->cap_exports.end();
	 ++p) {
      CInode *in = get_inode(p->first);
      if (in && !in->is_auth())
	continue;
      filepath& path = weak->cap_export_paths[p->first];
      if (!in) {
	if (!path_is_mine(path))
	  continue;
	cap_import_paths[p->first] = path;
	dout(10) << " noting cap import " << p->first << " path " << path << dendl;
      }
      
      // note
      for (map<client_t,ceph_mds_cap_reconnect>::iterator q = p->second.begin();
	   q != p->second.end();
	   ++q) {
	dout(10) << " claiming cap import " << p->first << " client." << q->first << dendl;
	cap_imports[p->first][q->first][from] = q->second;
      }
    }
  }

  // assimilate any potentially dirty scatterlock state
  for (map<inodeno_t,MMDSCacheRejoin::lock_bls>::iterator p = weak->inode_scatterlocks.begin();
       p != weak->inode_scatterlocks.end();
       p++) {
    CInode *in = get_inode(p->first);
    if (!in)
      dout(0) << "missing ino " << p->first
          << " for scatterlock state" << dendl;
    assert(in);
    in->decode_lock_state(CEPH_LOCK_IFILE, p->second.file);
    in->decode_lock_state(CEPH_LOCK_INEST, p->second.nest);
    if (!survivor)
      rejoin_potential_updated_scatterlocks.insert(in);
  }

  // recovering peer may send incorrect dirfrags here.  we need to
  // infer which dirfrag they meant.  the ack will include a
  // strong_dirfrag that will set them straight on the fragmentation.
  
  // walk weak map
  set<CDir*> dirs_to_share;
  for (set<dirfrag_t>::iterator p = weak->weak_dirfrags.begin();
       p != weak->weak_dirfrags.end();
       ++p) {
    CStripe *stripe = get_dirstripe(p->stripe);
    if (!stripe)
      dout(0) << " missing dir stripe " << p->stripe << dendl;
    assert(stripe);
    
    frag_t fg = stripe->get_fragtree()[p->frag.value()];
    CDir *dir = stripe->get_dirfrag(fg);
    if (!dir)
      dout(0) << " missing dir for " << p->frag << " (which maps to " << fg << ") on " << *stripe << dendl;
    assert(dir);
    if (dirs_to_share.count(dir)) {
      dout(10) << " already have " << p->frag << " -> " << fg << " " << *dir << dendl;
    } else {
      dirs_to_share.insert(dir);
      int nonce = dir->add_replica(from);
      dout(10) << " have " << p->frag << " -> " << fg << " " << *dir << dendl;
      if (ack)
	ack->add_strong_dirfrag(dir->dirfrag(), nonce);
    }
  }

  for (map<dirstripe_t,map<string_snap_t,MMDSCacheRejoin::dn_weak> >::iterator p = weak->weak.begin();
       p != weak->weak.end();
       ++p) {
    CStripe *stripe = get_dirstripe(p->first);
    if (!stripe)
      dout(0) << " missing dir stripe " << p->first << dendl;
    assert(stripe);

    // weak dentries
    CDir *dir = 0;
    for (map<string_snap_t,MMDSCacheRejoin::dn_weak>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      // locate proper dirfrag.
      //  optimize for common case (one dirfrag) to avoid dirs_to_share set check
      frag_t fg = stripe->pick_dirfrag(q->first.name);
      if (!dir || dir->get_frag() != fg) {
	dir = stripe->get_dirfrag(fg);
	if (!dir)
	  dout(0) << " missing dir frag " << fg << " on " << *stripe << dendl;
	assert(dir);
	assert(dirs_to_share.count(dir));
      }

      // and dentry
      CDentry *dn = dir->lookup(q->first.name, q->first.snapid);
      assert(dn);
      CDentry::linkage_t *dnl = dn->get_linkage();
      assert(dnl->is_primary());
      
      if (survivor && dn->is_replica(from)) 
	dentry_remove_replica(dn, from);  // this induces a lock gather completion
      int dnonce = dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      if (ack) 
	ack->add_strong_dentry(dir->dirfrag(), dn->name, dn->first, dn->last,
			       dnl->get_inode()->ino(), inodeno_t(0), 0, 
			       dnonce, dn->lock.get_replica_state());

      // inode
      CInode *in = dnl->get_inode();
      assert(in);

      if (survivor && in->is_replica(from)) 
	inode_remove_replica(in, from);
      int inonce = in->add_replica(from);
      dout(10) << " have " << *in << dendl;

      // scatter the dirlock, just in case?
      if (!survivor && in->is_dir())
	in->filelock.set_state(LOCK_MIX);

      if (ack) {
	acked_inodes.insert(in->vino());
	ack->add_inode_base(in);
	ack->add_inode_locks(in, inonce);
      }
    }
  }
  
  // weak base inodes?  (root, stray, etc.)
  for (set<vinodeno_t>::iterator p = weak->weak_inodes.begin();
       p != weak->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    assert(in);   // hmm fixme wrt stray?
    if (survivor && in->is_replica(from)) 
      inode_remove_replica(in, from);    // this induces a lock gather completion
    int inonce = in->add_replica(from);
    dout(10) << " have base " << *in << dendl;
    
    if (ack) {
      acked_inodes.insert(in->vino());
      ack->add_inode_base(in);
      ack->add_inode_locks(in, inonce);
    }
  }

  for (set<dirstripe_t>::iterator s = weak->weak_stripes.begin();
       s != weak->weak_stripes.end(); ++s) {
    CStripe *stripe = get_dirstripe(*s);
    if (!stripe)
      dout(5) << " missing stripe " << *s << dendl;
    assert(stripe);
    if (survivor && stripe->is_replica(from)) 
      stripe->remove_replica(from);

    int snonce = stripe->add_replica(from);
    dout(10) << " have stripe " << *stripe << dendl;
    if (ack)
      ack->add_strong_stripe(*s, snonce);
  }

  if (survivor) {
    // survivor.  do everything now.
    for (map<inodeno_t,MMDSCacheRejoin::lock_bls>::iterator p = weak->inode_scatterlocks.begin();
	 p != weak->inode_scatterlocks.end();
	 p++) {
      CInode *in = get_inode(p->first);
      dout(10) << " including base inode (due to potential scatterlock update) " << *in << dendl;
      acked_inodes.insert(in->vino());
      ack->add_inode_base(in);
    }

    rejoin_scour_survivor_replicas(from, ack, acked_inodes);
    mds->send_message(ack, weak->get_connection());
  } else {
    // done?
    assert(rejoin_gather.count(from));
    rejoin_gather.erase(from);
    if (rejoin_gather.empty()) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}

class C_MDC_RejoinGatherFinish : public Context {
  MDCache *cache;
public:
  C_MDC_RejoinGatherFinish(MDCache *c) : cache(c) {}
  void finish(int r) {
    cache->rejoin_gather_finish();
  }
};

/**
 * parallel_fetch -- make a pass at fetching a bunch of paths in parallel
 *
 * @param pathmap map of inodeno to full pathnames.  we remove items
 *            from this map as we discover we have them.
 *
 *	      returns true if there is work to do, false otherwise.
 */

bool MDCache::parallel_fetch(map<inodeno_t,filepath>& pathmap, set<inodeno_t>& missing)
{
  dout(10) << "parallel_fetch on " << pathmap.size() << " paths" << dendl;

  C_GatherBuilder gather_bld(g_ceph_context, new C_MDC_RejoinGatherFinish(this));

  // scan list
  set<CDir*> fetch_queue;
  map<inodeno_t,filepath>::iterator p = pathmap.begin();
  while (p != pathmap.end()) {
    // do we have the target already?
    CInode *cur = get_inode(p->first);
    if (cur) {
      dout(15) << " have " << *cur << dendl;
      pathmap.erase(p++);
      continue;
    }

    // traverse
    dout(17) << " missing " << p->first << " at " << p->second << dendl;
    if (parallel_fetch_traverse_dir(p->first, p->second, fetch_queue,
				    missing, gather_bld))
      pathmap.erase(p++);
    else
      p++;
  }

  if (pathmap.empty() && (!gather_bld.has_subs())) {
    dout(10) << "parallel_fetch done" << dendl;
    assert(fetch_queue.empty());
    return false;
  }

  // do a parallel fetch
  for (set<CDir*>::iterator p = fetch_queue.begin();
       p != fetch_queue.end();
       ++p) {
    dout(10) << "parallel_fetch fetching " << **p << dendl;
    (*p)->fetch(gather_bld.new_sub());
  }
  
  if (gather_bld.get()) {
    gather_bld.activate();
    return true;
  }
  return false;
}

// true if we're done with this path
bool MDCache::parallel_fetch_traverse_dir(inodeno_t ino, filepath& path,
					  set<CDir*>& fetch_queue, set<inodeno_t>& missing,
					  C_GatherBuilder &gather_bld)
{
  CInode *cur = get_inode(path.get_ino());
  if (!cur) {
    dout(5) << " missing " << path << " base ino " << path.get_ino() << dendl;
    missing.insert(ino);
    return true;
  }

  for (unsigned i=0; i<path.depth(); i++) {
    dout(20) << " path " << path << " seg " << i << "/" << path.depth() << ": " << path[i]
	     << " under " << *cur << dendl;
    if (!cur->is_dir()) {
      dout(5) << " bad path " << path << " ENOTDIR at " << path[i] << dendl;
      missing.insert(ino);
      return true;
    }

    __u32 dnhash = cur->hash_dentry_name(path[i]);
    stripeid_t stripeid = cur->pick_stripe(dnhash);
    CStripe *stripe = cur->get_or_open_stripe(stripeid);

    frag_t fg = stripe->pick_dirfrag(dnhash);
    CDir *dir = stripe->get_or_open_dirfrag(fg);
    CDentry *dn = dir->lookup(path[i]);
    CDentry::linkage_t *dnl = dn->get_linkage();
    if (!dn || dnl->is_null()) {
      if (!dir->is_complete()) {
	// fetch dir
	fetch_queue.insert(dir);
	return false;
      } else {
	// probably because the client created it and held a cap but it never committed
	// to the journal, and the op hasn't replayed yet.
	dout(5) << " dne (not created yet?) " << ino << " at " << path << dendl;
	missing.insert(ino);
	return true;
      }
    }
    cur = dnl->get_inode();
    if (!cur) {
      assert(dnl->is_remote());
      cur = get_inode(dnl->get_remote_ino());
      if (cur) {
	dn->link_remote(dnl, cur);
      } else {
	// open remote ino
	open_remote_ino(dnl->get_remote_ino(), gather_bld.new_sub());
	return false;
      }
    }
  }

  dout(5) << " ino not found " << ino << " at " << path << dendl;
  missing.insert(ino);
  return true;
}




/*
 * rejoin_scour_survivor_replica - remove source from replica list on unmentioned objects
 *
 * all validated replicas are acked with a strong nonce, etc.  if that isn't in the
 * ack, the replica dne, and we can remove it from our replica maps.
 */
void MDCache::rejoin_scour_survivor_replicas(int from, MMDSCacheRejoin *ack, set<vinodeno_t>& acked_inodes)
{
  dout(10) << "rejoin_scour_survivor_replicas from mds." << from << dendl;

  // FIXME: what about root and stray inodes.
  
  for (inode_map::iterator p = inodes.begin(); p != inodes.end(); ++p) {
    CInode *in = p->second;
    
    // inode?
    if (in->is_auth() &&
	in->is_replica(from) &&
	acked_inodes.count(p->second->vino()) == 0) {
      inode_remove_replica(in, from);
      dout(10) << " rem " << *in << dendl;
    }

    if (!in->is_dir()) continue;

    list<CStripe*> stripes;
    in->get_stripes(stripes);
    for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
      CStripe *stripe = *s;

      if (stripe->is_auth() &&
          stripe->is_replica(from) &&
          ack->strong_stripes.count(stripe->dirstripe()) == 0) {
        stripe->remove_replica(from);
        dout(10) << " rem " << *stripe << dendl;
      }

      list<CDir*> dfs;
      stripe->get_dirfrags(dfs);
      for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
        CDir *dir = *p;

        if (dir->is_auth() &&
            dir->is_replica(from) &&
            ack->strong_dirfrags.count(dir->dirfrag()) == 0) {
          dir->remove_replica(from);
          dout(10) << " rem " << *dir << dendl;
        } 

        // dentries
        for (CDir::map_t::iterator p = dir->items.begin();
             p != dir->items.end();
             ++p) {
          CDentry *dn = p->second;

          if (dn->is_replica(from) &&
              (ack->strong_dentries.count(dir->dirfrag()) == 0 ||
               ack->strong_dentries[dir->dirfrag()].count(string_snap_t(dn->name, dn->last)) == 0)) {
            dentry_remove_replica(dn, from);
            dout(10) << " rem " << *dn << dendl;
          }
        }
      }
    }
  }
}

bool MDCache::rejoin_fetch_dirfrags(MMDSCacheRejoin *strong)
{
  int skipped = 0;
  set<CDir*> fetch_queue;
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = strong->strong_dirfrags.begin();
       p != strong->strong_dirfrags.end();
       ++p) {
    CStripe *stripe = get_dirstripe(p->first.stripe);
    if (!stripe) {
      skipped++;
      continue;
    }
    CDir *dir = stripe->get_dirfrag(p->first.frag);
    if (dir && dir->is_complete())
      continue;

    set<CDir*> frags;
    bool refragged = false;
    if (!dir) {
      const fragtree_t &dft = stripe->get_fragtree();
      if (dft.is_leaf(p->first.frag))
	dir = stripe->get_or_open_dirfrag(p->first.frag);
      else {
	list<frag_t> ls;
	dft.get_leaves_under(p->first.frag, ls);
	if (ls.empty())
	  ls.push_back(dft[p->first.frag.value()]);
	for (list<frag_t>::iterator q = ls.begin(); q != ls.end(); ++q) {
	  dir = stripe->get_or_open_dirfrag(p->first.frag);
	  frags.insert(dir);
	}
	refragged = true;
      }
    }

    map<string_snap_t,MMDSCacheRejoin::dn_strong>& dmap = strong->strong_dentries[p->first];
    for (map<string_snap_t,MMDSCacheRejoin::dn_strong>::iterator q = dmap.begin();
	q != dmap.end();
	++q) {
      if (!q->second.is_primary())
	continue;
      CDentry *dn;
      if (!refragged)
	dn = dir->lookup(q->first.name, q->first.snapid);
      else {
	frag_t fg = stripe->pick_dirfrag(q->first.name);
	dir = stripe->get_dirfrag(fg);
	assert(dir);
	dn = dir->lookup(q->first.name, q->first.snapid);
      }
      if (!dn) {
	fetch_queue.insert(dir);
	if (!refragged)
	  break;
	frags.erase(dir);
	if (frags.empty())
	  break;
      }
    }
  }

  if (!fetch_queue.empty()) {
    dout(10) << "rejoin_fetch_dirfrags " << fetch_queue.size() << " dirfrags" << dendl;
    strong->get();
    C_GatherBuilder gather(g_ceph_context, new C_MDS_RetryMessage(mds, strong));
    for (set<CDir*>::iterator p = fetch_queue.begin(); p != fetch_queue.end(); p++) {
      CDir *dir = *p;
      dir->fetch(gather.new_sub());
    }
    gather.activate();
    return true;
  }
  assert(!skipped);
  return false;
}

/* This functions DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_strong(MMDSCacheRejoin *strong)
{
  int from = strong->get_source().num();

  // only a recovering node will get a strong rejoin.
  assert(mds->is_rejoin());

  if (rejoin_fetch_dirfrags(strong))
    return;

  MMDSCacheRejoin *missing = 0;  // if i'm missing something..
  
  // assimilate any potentially dirty scatterlock state
  for (map<inodeno_t,MMDSCacheRejoin::lock_bls>::iterator p = strong->inode_scatterlocks.begin();
       p != strong->inode_scatterlocks.end();
       p++) {
    CInode *in = get_inode(p->first);
    assert(in);
    in->decode_lock_state(CEPH_LOCK_IFILE, p->second.file);
    in->decode_lock_state(CEPH_LOCK_INEST, p->second.nest);
    rejoin_potential_updated_scatterlocks.insert(in);
  }

  // surviving peer may send incorrect dirfrag here (maybe they didn't
  // get the fragment notify, or maybe we rolled back?).  we need to
  // infer the right frag and get them with the program.  somehow.
  // we don't normally send ACK.. so we'll need to bundle this with
  // MISSING or something.

  // strong dirfrags/dentries.
  //  also process auth_pins, xlocks.
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = strong->strong_dirfrags.begin();
       p != strong->strong_dirfrags.end();
       ++p) {
    CStripe *stripe = get_dirstripe(p->first.stripe);
    assert(stripe);
    CDir *dir = stripe->get_dirfrag(p->first.frag);
    const fragtree_t &dft = stripe->get_fragtree();
    bool refragged = false;
    if (dir) {
      dout(10) << " have " << *dir << dendl;
      dir->add_replica(from);
    } else {
      dout(10) << " frag " << p->first << " doesn't match dirfragtree " << *stripe << dendl;
      list<frag_t> ls;
      dft.get_leaves_under(p->first.frag, ls);
      if (ls.empty())
	ls.push_back(dft[p->first.frag.value()]);
      dout(10) << " maps to frag(s) " << ls << dendl;
      for (list<frag_t>::iterator q = ls.begin(); q != ls.end(); ++q) {
	CDir *dir = stripe->get_dirfrag(*q);
        assert(dir);
        dout(10) << " have(approx) " << *dir << dendl;
	dir->add_replica(from);
      }
      refragged = true;
    }
    
    map<string_snap_t,MMDSCacheRejoin::dn_strong>& dmap = strong->strong_dentries[p->first];
    for (map<string_snap_t,MMDSCacheRejoin::dn_strong>::iterator q = dmap.begin();
	 q != dmap.end();
	 ++q) {
      CDentry *dn;
      if (!refragged)
	dn = dir->lookup(q->first.name, q->first.snapid);
      else {
	frag_t fg = stripe->pick_dirfrag(q->first.name);
	dir = stripe->get_dirfrag(fg);
	assert(dir);
	dn = dir->lookup(q->first.name, q->first.snapid);
      }
      if (!dn) {
	if (q->second.is_remote()) {
	  dn = dir->add_remote_dentry(q->first.name, q->second.remote_ino, q->second.remote_d_type,
				      q->second.first, q->first.snapid);
	} else if (q->second.is_null()) {
	  dn = dir->add_null_dentry(q->first.name, q->second.first, q->first.snapid);
	} else {
	  assert(0);
	  CInode *in = get_inode(q->second.ino, q->first.snapid);
          assert(in);
	  dn = dir->add_primary_dentry(q->first.name, in, q->second.first, q->first.snapid);

	  dout(10) << " missing " << q->second.ino << "." << q->first.snapid << dendl;
	  if (!missing)
	    missing = new MMDSCacheRejoin(MMDSCacheRejoin::OP_MISSING);
	  missing->add_weak_inode(vinodeno_t(q->second.ino, q->first.snapid));  // we want it back!
	}
	dout(10) << " invented " << *dn << dendl;
      }
      CDentry::linkage_t *dnl = dn->get_linkage();

      // dn auth_pin?
      if (strong->authpinned_dentries.count(p->first) &&
	  strong->authpinned_dentries[p->first].count(q->first)) {
	MMDSCacheRejoin::slave_reqid r = strong->authpinned_dentries[p->first][q->first];
	dout(10) << " dn authpin by " << r << " on " << *dn << dendl;
	
	// get/create slave mdrequest
	MDRequest *mdr;
	if (have_request(r.reqid))
	  mdr = request_get(r.reqid);
	else
	  mdr = request_start_slave(r.reqid, r.attempt, from);
	mdr->auth_pin(dn);
      }

      // dn xlock?
      if (strong->xlocked_dentries.count(p->first) &&
	  strong->xlocked_dentries[p->first].count(q->first)) {
	MMDSCacheRejoin::slave_reqid r = strong->xlocked_dentries[p->first][q->first];
	dout(10) << " dn xlock by " << r << " on " << *dn << dendl;
	MDRequest *mdr = request_get(r.reqid);  // should have this from auth_pin above.
	assert(mdr->is_auth_pinned(dn));
	if (dn->lock.is_stable())
	  dn->auth_pin(&dn->lock);
	dn->lock.set_state(LOCK_XLOCK);
	dn->lock.get_xlock(mdr, mdr->get_client());
	mdr->xlocks.insert(&dn->lock);
	mdr->locks.insert(&dn->lock);
      }

      dn->add_replica(from);
      dout(10) << " have " << *dn << dendl;
      
      // inode?
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	assert(in);

	if (strong->strong_inodes.count(in->vino())) {
	  MMDSCacheRejoin::inode_strong &is = strong->strong_inodes[in->vino()];

	  // caps_wanted
	  if (is.caps_wanted) {
	    in->mds_caps_wanted[from] = is.caps_wanted;
	    dout(15) << " inode caps_wanted " << ccap_string(is.caps_wanted)
		     << " on " << *in << dendl;
	  } 
	  
	  // scatterlocks?
	  //  infer state from replica state:
	  //   * go to MIX if they might have wrlocks
	  //   * go to LOCK if they are LOCK (just bc identify_files_to_recover might start twiddling filelock)
	  in->filelock.infer_state_from_strong_rejoin(is.filelock, true);  // maybe also go to LOCK
	  in->nestlock.infer_state_from_strong_rejoin(is.nestlock, false);
	  
	  // auth pin?
	  if (strong->authpinned_inodes.count(in->vino())) {
	    MMDSCacheRejoin::slave_reqid r = strong->authpinned_inodes[in->vino()];
	    dout(10) << " inode authpin by " << r << " on " << *in << dendl;
	    
	    // get/create slave mdrequest
	    MDRequest *mdr;
	    if (have_request(r.reqid))
	      mdr = request_get(r.reqid);
	    else
	      mdr = request_start_slave(r.reqid, r.attempt, from);
	    if (strong->frozen_authpin_inodes.count(in->vino())) {
	      assert(!in->get_num_auth_pins());
	      mdr->freeze_auth_pin(in);
	    } else {
	      assert(!in->is_frozen_auth_pin());
	    }
	    mdr->auth_pin(in);
	  }
	  // xlock(s)?
	  if (strong->xlocked_inodes.count(in->vino())) {
	    for (map<int,MMDSCacheRejoin::slave_reqid>::iterator r = strong->xlocked_inodes[in->vino()].begin();
		 r != strong->xlocked_inodes[in->vino()].end();
		 ++r) {
	      SimpleLock *lock = in->get_lock(r->first);
	      dout(10) << " inode xlock by " << r->second << " on " << *lock << " on " << *in << dendl;
	      MDRequest *mdr = request_get(r->second.reqid);  // should have this from auth_pin above.
	      assert(mdr->is_auth_pinned(in));
	      if (lock->is_stable())
		in->auth_pin(lock);
	      lock->set_state(LOCK_XLOCK);
	      if (lock == &in->filelock)
		in->loner_cap = -1;
	      lock->get_xlock(mdr, mdr->get_client());
	      mdr->xlocks.insert(lock);
	      mdr->locks.insert(lock);
	    }
	  }
	  // wrlock(s)?
	  if (strong->wrlocked_inodes.count(in->vino())) {
	    for (map<int,MMDSCacheRejoin::slave_reqid>::iterator r = strong->wrlocked_inodes[in->vino()].begin();
		 r != strong->wrlocked_inodes[in->vino()].end();
		 ++r) {
	      SimpleLock *lock = in->get_lock(r->first);
	      dout(10) << " inode wrlock by " << r->second << " on " << *lock << " on " << *in << dendl;
	      MDRequest *mdr = request_get(r->second.reqid);  // should have this from auth_pin above.
	      assert(mdr->is_auth_pinned(in));
	      lock->set_state(LOCK_LOCK);
	      if (lock == &in->filelock)
		in->loner_cap = -1;
	      lock->get_wrlock(true);
	      mdr->wrlocks.insert(lock);
	      mdr->locks.insert(lock);
	    }
	  }
	} else {
	  dout(10) << " sender has dentry but not inode, adding them as a replica" << dendl;
	}
	
	in->add_replica(from);
	dout(10) << " have " << *in << dendl;
      }
    }
  }

  // base inodes?  (root, stray, etc.)
  for (set<vinodeno_t>::iterator p = strong->weak_inodes.begin();
       p != strong->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    dout(10) << " have base " << *in << dendl;
    in->add_replica(from);
  }

  // send missing?
  if (missing) {
    // we expect a FULL soon.
    mds->send_message(missing, strong->get_connection());
  } else {
    // done?
    assert(rejoin_gather.count(from));
    rejoin_gather.erase(from);
    if (rejoin_gather.empty()) {
      rejoin_gather_finish();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
    }
  }
}

/* This functions DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_ack(MMDSCacheRejoin *ack)
{
  dout(7) << "handle_cache_rejoin_ack from " << ack->get_source() << dendl;
  int from = ack->get_source().num();

  // dirs
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = ack->strong_dirfrags.begin();
       p != ack->strong_dirfrags.end();
       ++p) {
    // we may have had incorrect dir fragmentation; refragment based
    // on what they auth tells us.
    CDir *dir = get_force_dirfrag(p->first);
    assert(dir);

    dir->set_replica_nonce(p->second.nonce);
    dir->state_clear(CDir::STATE_REJOINING);
    dout(10) << " got " << *dir << dendl;

    // dentries
    map<string_snap_t,MMDSCacheRejoin::dn_strong>& dmap = ack->strong_dentries[p->first];
    for (map<string_snap_t,MMDSCacheRejoin::dn_strong>::iterator q = dmap.begin();
	 q != dmap.end();
	 ++q) {
      CDentry *dn = dir->lookup(q->first.name, q->first.snapid);
      assert(dn);
      CDentry::linkage_t *dnl = dn->get_linkage();

      assert(dn->last == q->first.snapid);
      if (dn->first != q->second.first) {
	dout(10) << " adjust dn.first " << dn->first << " -> " << q->second.first << " on " << *dn << dendl;
	dn->first = q->second.first;
      }

      // hmm, did we have the proper linkage here?
      if (dnl->is_null() &&
	  !q->second.is_null()) {
	dout(10) << " had bad (missing) linkage for " << *dn << dendl;
	if (q->second.is_remote()) {
	  dn->dir->link_remote_inode(dn, q->second.remote_ino, q->second.remote_d_type);
	} else {
	  CInode *in = get_inode(q->second.ino, q->first.snapid);
	  assert(in == 0);  // a rename would have been caught be the resolve stage.
	  // barebones inode; the full inode loop below will clean up.
	  in = new CInode(this, from, q->second.first, q->first.snapid);
	  in->inode.ino = q->second.ino;
	  add_inode(in);
	  dn->dir->link_primary_inode(dn, in);
	}
      }
      else if (!dnl->is_null() &&
	       q->second.is_null()) {
	dout(0) << " had bad linkage for " << *dn << dendl;
	/* 
	 * this should happen:
	 *  if we're a survivor, any unlink should commit or rollback during
	 * the resolve stage.
	 *  if we failed, we shouldn't have non-auth leaf dentries at all
	 */
	assert(0);  // uh oh.	
      }
      dn->set_replica_nonce(q->second.nonce);
      dn->lock.set_state_rejoin(q->second.lock, rejoin_waiters);
      dn->state_clear(CDentry::STATE_REJOINING);
      dout(10) << " got " << *dn << dendl;
    }
  }

  // full inodes
  bufferlist::iterator p = ack->inode_base.begin();
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    bufferlist basebl;
    ::decode(ino, p);
    ::decode(last, p);
    ::decode(basebl, p);
    CInode *in = get_inode(ino, last);
    assert(in);
    bufferlist::iterator q = basebl.begin();
    in->_decode_base(q);
    // auth will send a full inode for any inode it got scatterlock state for.
    //in->clear_scatter_dirty();
    dout(10) << " got inode base " << *in << dendl;
  }

  // inodes
  p = ack->inode_locks.begin();
  //dout(10) << "inode_locks len " << ack->inode_locks.length() << " is " << ack->inode_locks << dendl;
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    __u32 nonce;
    bufferlist lockbl;
    ::decode(ino, p);
    ::decode(last, p);
    ::decode(nonce, p);
    ::decode(lockbl, p);
    
    CInode *in = get_inode(ino, last);
    assert(in);
    in->set_replica_nonce(nonce);
    bufferlist::iterator q = lockbl.begin();
    in->_decode_locks_rejoin(q, rejoin_waiters);
    in->state_clear(CInode::STATE_REJOINING);
    dout(10) << " got inode locks " << *in << dendl;
  }

  // done?
  assert(rejoin_ack_gather.count(from));
  rejoin_ack_gather.erase(from);
  if (mds->is_rejoin()) {
    if (rejoin_gather.empty() &&     // make sure we've gotten our FULL inodes, too.
	rejoin_ack_gather.empty()) {
      do_delayed_cap_imports();
      open_undef_dirfrags();
    } else {
      dout(7) << "still need rejoin from (" << rejoin_gather << ")"
	      << ", rejoin_ack from (" << rejoin_ack_gather << ")" << dendl;
    }
  } else {
    // survivor.
    mds->queue_waiters(rejoin_waiters);
  }
}



/* This functions DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_missing(MMDSCacheRejoin *missing)
{
  dout(7) << "handle_cache_rejoin_missing from " << missing->get_source() << dendl;

  MMDSCacheRejoin *full = new MMDSCacheRejoin(MMDSCacheRejoin::OP_FULL);

  // dirs
  for (map<dirfrag_t, MMDSCacheRejoin::dirfrag_strong>::iterator p = missing->strong_dirfrags.begin();
       p != missing->strong_dirfrags.end();
       ++p) {
    // we may have had incorrect dir fragmentation; refragment based
    // on what they auth tells us.
    CDir *dir = get_force_dirfrag(p->first);
    assert(dir);

    dir->set_replica_nonce(p->second.nonce);
    dir->state_clear(CDir::STATE_REJOINING);
    dout(10) << " adjusted frag on " << *dir << dendl;
  }

  // inodes
  for (set<vinodeno_t>::iterator p = missing->weak_inodes.begin();
       p != missing->weak_inodes.end();
       ++p) {
    CInode *in = get_inode(*p);
    if (!in) {
      dout(10) << " don't have inode " << *p << dendl;
      continue; // we must have trimmed it after the originalo rejoin
    }
    
    dout(10) << " sending " << *in << dendl;
    full->add_inode_base(in);
  }

  mds->send_message(full, missing->get_connection());
}

/* This function DOES NOT put the passed message before returning */
void MDCache::handle_cache_rejoin_full(MMDSCacheRejoin *full)
{
  dout(7) << "handle_cache_rejoin_full from " << full->get_source() << dendl;
  int from = full->get_source().num();
  
  // integrate full inodes
  bufferlist::iterator p = full->inode_base.begin();
  while (!p.end()) {
    inodeno_t ino;
    snapid_t last;
    bufferlist basebl;
    ::decode(ino, p);
    ::decode(last, p);
    ::decode(basebl, p);

    CInode *in = get_inode(ino);
    assert(in);
    bufferlist::iterator pp = basebl.begin();
    in->_decode_base(pp);

    set<CInode*>::iterator q = rejoin_undef_inodes.find(in);
    if (q != rejoin_undef_inodes.end()) {
      CInode *in = *q;
      in->state_clear(CInode::STATE_REJOINUNDEF);
      dout(10) << " got full " << *in << dendl;
      rejoin_undef_inodes.erase(q);
    } else {
      dout(10) << " had full " << *in << dendl;
    }
  }

  // done?
  assert(rejoin_gather.count(from));
  rejoin_gather.erase(from);
  if (rejoin_gather.empty()) {
    rejoin_gather_finish();
  } else {
    dout(7) << "still need rejoin from (" << rejoin_gather << ")" << dendl;
  }
}



/**
 * rejoin_trim_undef_inodes() -- remove REJOINUNDEF flagged inodes
 *
 * FIXME: wait, can this actually happen?  a survivor should generate cache trim
 * messages that clean these guys up...
 */
void MDCache::rejoin_trim_undef_inodes()
{
  dout(10) << "rejoin_trim_undef_inodes" << dendl;

  while (!rejoin_undef_inodes.empty()) {
    set<CInode*>::iterator p = rejoin_undef_inodes.begin();
    CInode *in = *p;
    rejoin_undef_inodes.erase(p);

    in->clear_replica_map();
    
    if (in->is_dir()) {
      // close stripes
      list<CStripe*> dsls;
      in->get_stripes(dsls);
      for (list<CStripe*>::iterator s = dsls.begin(); s != dsls.end(); ++s) {
        CStripe *stripe = *s;
        stripe->clear_replica_map();

        // close dirfrags
        list<CDir*> dfls;
        stripe->get_dirfrags(dfls);
        for (list<CDir*>::iterator p = dfls.begin();
             p != dfls.end();
             ++p) {
          CDir *dir = *p;
          dir->clear_replica_map();

          for (CDir::map_t::iterator p = dir->items.begin();
               p != dir->items.end();
               ++p) {
            CDentry *dn = p->second;
            dn->clear_replica_map();

            dout(10) << " trimming " << *dn << dendl;
            dir->remove_dentry(dn);
          }

          dout(10) << " trimming " << *dir << dendl;
          stripe->close_dirfrag(dir->get_frag());
        }
        in->close_stripe(stripe);
      }
    }
    
    CDentry *dn = in->get_parent_dn();
    if (dn) {
      dn->clear_replica_map();
      dout(10) << " trimming " << *dn << dendl;
      dn->dir->remove_dentry(dn);
    } else {
      dout(10) << " trimming " << *in << dendl;
      remove_inode(in);
    }
  }

  assert(rejoin_undef_inodes.empty());
}

void MDCache::rejoin_gather_finish() 
{
  dout(10) << "rejoin_gather_finish" << dendl;
  assert(mds->is_rejoin());

  rejoin_trim_undef_inodes();

  // fetch paths?
  //  do this before ack, since some inodes we may have already gotten
  //  from surviving MDSs.
  if (!cap_import_paths.empty()) {
    if (parallel_fetch(cap_import_paths, cap_imports_missing)) {
      return;
    }
  }
  
  process_imported_caps();
  choose_lock_states();

  vector<CInode*> recover_q, check_q;
  identify_files_to_recover(rejoin_recover_q, rejoin_check_q);
  rejoin_send_acks();
  
  // signal completion of fetches, rejoin_gather_finish, etc.
  assert(rejoin_ack_gather.count(mds->whoami));
  rejoin_ack_gather.erase(mds->whoami);

  // did we already get our acks too?
  // this happens when the rejoin_gather has to wait on a MISSING/FULL exchange.
  if (rejoin_ack_gather.empty()) {
    do_delayed_cap_imports();
    open_undef_dirfrags();
  }
}

void MDCache::process_imported_caps()
{
  dout(10) << "process_imported_caps" << dendl;

  // process cap imports
  //  ino -> client -> frommds -> capex
  map<inodeno_t,map<client_t, map<int,ceph_mds_cap_reconnect> > >::iterator p = cap_imports.begin();
  while (p != cap_imports.end()) {
    CInode *in = get_inode(p->first);
    if (!in) {
      dout(10) << "process_imported_caps still missing " << p->first
	       << ", will try again after replayed client requests"
	       << dendl;
      p++;
      continue;
    }
    for (map<client_t, map<int,ceph_mds_cap_reconnect> >::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) 
      for (map<int,ceph_mds_cap_reconnect>::iterator r = q->second.begin();
	   r != q->second.end();
	   ++r) {
	rejoin_import_cap(in, q->first, r->second, r->first);
      }
    cap_imports.erase(p++);  // remove and move on
  }
}

/*
 * choose lock states based on reconnected caps
 */
void MDCache::choose_lock_states()
{
  dout(10) << "choose_lock_states" << dendl;

  for (inode_map::iterator i = inodes.begin(); i != inodes.end(); ++i) {
    CInode *in = i->second;

    in->choose_lock_states();
    dout(15) << " chose lock states on " << *in << dendl;
  }

  CInode *ctnr = container.get_inode();
  if (ctnr) {
    // these locks are not taken on the inode container
    // clean them up and rechoose its lock states
    ctnr->filelock.remove_dirty();
    ctnr->filelock.get_updated_item()->remove_myself();
    ctnr->nestlock.remove_dirty();
    ctnr->nestlock.get_updated_item()->remove_myself();

    ctnr->choose_lock_states();
  }
}

/*
 * remove any items from logsegment open_file lists that don't have
 * any caps
 */
void MDCache::clean_open_file_lists()
{
  dout(10) << "clean_open_file_lists" << dendl;
  
  for (map<uint64_t,LogSegment*>::iterator p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       p++) {
    LogSegment *ls = p->second;
    
    elist<CInode*>::iterator q = ls->open_files.begin(member_offset(CInode, item_open_file));
    while (!q.end()) {
      CInode *in = *q;
      ++q;
      if (!in->is_any_caps_wanted()) {
	dout(10) << " unlisting unwanted/capless inode " << *in << dendl;
	in->item_open_file.remove_myself();
      }
    }
  }
}



void MDCache::rejoin_import_cap(CInode *in, client_t client, ceph_mds_cap_reconnect& icr, int frommds)
{
  dout(10) << "rejoin_import_cap for client." << client << " from mds." << frommds
	   << " on " << *in << dendl;
  Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(client.v));
  assert(session);

  Capability *cap = in->reconnect_cap(client, icr, session);

  if (frommds >= 0)
    do_cap_import(session, in, cap);
}

void MDCache::try_reconnect_cap(CInode *in, Session *session)
{
  client_t client = session->get_client();
  ceph_mds_cap_reconnect *rc = get_replay_cap_reconnect(in->ino(), client);
  if (rc) {
    in->reconnect_cap(client, *rc, session);
    dout(10) << "try_reconnect_cap client." << client
	     << " reconnect wanted " << ccap_string(rc->wanted)
	     << " issue " << ccap_string(rc->issued)
	     << " on " << *in << dendl;
    remove_replay_cap_reconnect(in->ino(), client);

    if (in->is_replicated()) {
      mds->locker->try_eval(in, CEPH_CAP_LOCKS);
    } else {
      in->choose_lock_states();
      dout(15) << " chose lock states on " << *in << dendl;
    }
  }
}



// -------
// cap imports and delayed snap parent opens

void MDCache::do_cap_import(Session *session, CInode *in, Capability *cap)
{
  SnapRealm *realm = get_snaprealm();
  dout(10) << "do_cap_import " << session->inst.name
      << " mseq " << cap->get_mseq() << " on " << *in << dendl;
  cap->set_last_issue();
  MClientCaps *reap = new MClientCaps(CEPH_CAP_OP_IMPORT,
                                      in->ino(),
                                      MDS_INO_ROOT,
                                      cap->get_cap_id(), cap->get_last_seq(),
                                      cap->pending(), cap->wanted(), 0,
                                      cap->get_mseq());
  in->encode_cap_message(reap, cap);
  realm->build_snap_trace(reap->snapbl);
  mds->send_message_client_counted(reap, session);
}

void MDCache::do_delayed_cap_imports()
{
  dout(10) << "do_delayed_cap_imports" << dendl;

  map<client_t,set<CInode*> > d;
  d.swap(delayed_imported_caps);

  for (map<client_t,set<CInode*> >::iterator p = d.begin();
       p != d.end();
       p++) {
    for (set<CInode*>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      CInode *in = *q;
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p->first.v));
      if (session) {
	Capability *cap = in->get_client_cap(p->first);
	if (cap) {
	  do_cap_import(session, in, cap);  // note: this may fail and requeue!
	  cap->dec_suppress();
	}
      }
      in->auth_unpin(this);

      if (in->is_head())
	mds->locker->issue_caps(in);
    }
  }    
}

struct C_MDC_OpenUndefDirfragsFinish : public Context {
  MDCache *cache;
  C_MDC_OpenUndefDirfragsFinish(MDCache *c) : cache(c) {}
  void finish(int r) {
    cache->open_undef_dirfrags();
  }
};

struct C_MDC_StartParentStats : public Context {
  ParentStats *stats;
  C_MDC_StartParentStats(ParentStats *stats) : stats(stats) {}
  void finish(int r) {
    stats->propagate_unaccounted();
  }
};

void MDCache::open_undef_dirfrags()
{
  dout(10) << "open_undef_dirfrags " << rejoin_undef_dirfrags.size() << " dirfrags" << dendl;
  
  C_GatherBuilder gather(g_ceph_context);
  for (set<CDir*>::iterator p = rejoin_undef_dirfrags.begin();
       p != rejoin_undef_dirfrags.end();
       p++) {
    CDir *dir = *p;
    dir->fetch(gather.new_sub());
  }

  if (gather.has_subs()) {
    gather.set_finisher(new C_MDC_OpenUndefDirfragsFinish(this));
    gather.activate();
  }
  else {
    start_files_to_recover(rejoin_recover_q, rejoin_check_q);
    // start parent stats when root opens
    wait_for_open(new C_MDC_StartParentStats(&parentstats));
    mds->queue_waiters(rejoin_waiters);
    mds->rejoin_done();
  }
}

void MDCache::finish_snaprealm_reconnect(client_t client, SnapRealm *realm, snapid_t seq)
{
  if (seq < realm->get_newest_seq()) {
    dout(10) << "finish_snaprealm_reconnect client." << client << " has old seq " << seq << " < " 
	     << realm->get_newest_seq()
    	     << " on " << *realm << dendl;
    // send an update
    Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(client.v));
    if (session) {
      MClientSnap *snap = new MClientSnap(CEPH_SNAP_OP_UPDATE);
      realm->build_snap_trace(snap->bl);
      mds->send_message_client_counted(snap, session);
    } else {
      dout(10) << " ...or not, no session for this client!" << dendl;
    }
  } else {
    dout(10) << "finish_snaprealm_reconnect client." << client << " up to date"
	     << " on " << *realm << dendl;
  }
}



void MDCache::rejoin_send_acks()
{
  dout(7) << "rejoin_send_acks" << dendl;
  
  // send acks to everyone in the recovery set
  map<int,MMDSCacheRejoin*> ack;
  for (set<int>::iterator p = recovery_set.begin();
       p != recovery_set.end();
       ++p) 
    ack[*p] = new MMDSCacheRejoin(MMDSCacheRejoin::OP_ACK);
#if 0 
  // walk subtrees
  for (map<CStripe*,set<CStripe*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       p++) {
    CStripe *root = p->first;
    if (!root->is_auth())
      continue;
    dout(10) << "subtree " << *root << dendl;

    // stripe
    for (map<int,int>::iterator r = root->replicas_begin();
         r != root->replicas_end();
         ++r)
      ack[r->first]->add_strong_stripe(root->dirstripe(), r->second);
 
    // auth items in this subtree
    list<CDir*> dq;
    root->get_dirfrags(dq);

    while (!dq.empty()) {
      CDir *dir = dq.front();
      dq.pop_front();

      // dir
      for (map<int,int>::iterator r = dir->replicas_begin();
	   r != dir->replicas_end();
	   ++r)
	ack[r->first]->add_strong_dirfrag(dir->dirfrag(), r->second);

      for (CDir::map_t::iterator q = dir->items.begin();
	   q != dir->items.end();
	   ++q) {
	CDentry *dn = q->second;
	CDentry::linkage_t *dnl = dn->get_linkage();

	// dentry
	for (map<int,int>::iterator r = dn->replicas_begin();
	     r != dn->replicas_end();
	     ++r)
	  ack[r->first]->add_strong_dentry(dir->dirfrag(), dn->name, dn->first, dn->last,
					   dnl->is_primary() ? dnl->get_inode()->ino():inodeno_t(0),
					   dnl->is_remote() ? dnl->get_remote_ino():inodeno_t(0),
					   dnl->is_remote() ? dnl->get_remote_d_type():0,
					   r->second,
					   dn->lock.get_replica_state());

	if (!dnl->is_primary())
	  continue;

	// inode
	CInode *in = dnl->get_inode();

	for (map<int,int>::iterator r = in->replicas_begin();
	     r != in->replicas_end();
	     ++r) {
	  ack[r->first]->add_inode_base(in);
	  ack[r->first]->add_inode_locks(in, r->second);
	}

	// subdirs in this subtree?
        list<CStripe*> stripes;
        in->get_nested_stripes(stripes);
        for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
          // stripe
          CStripe *stripe = *s;
          for (map<int,int>::iterator r = stripe->replicas_begin();
               r != stripe->replicas_end();
               ++r)
            ack[r->first]->add_strong_stripe(stripe->dirstripe(), r->second);

          stripe->get_dirfrags(dq);
        }
      }
    }
  }
#endif

  list<CStripe*> stripes;
  typedef map<int,int>::iterator rep_iter;
  for (inode_map::iterator i = inodes.begin(); i != inodes.end(); ++i) {
    CInode *in = i->second;
    if (!in->is_auth())
      continue;

    for (rep_iter r = in->replicas_begin(); r != in->replicas_end(); ++r) {
      ack[r->first]->add_inode_base(in);
      ack[r->first]->add_inode_locks(in, r->second);
    }
    in->get_stripes(stripes);
  }

  for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
    CStripe *stripe = *s;
    if (!stripe->is_auth())
      continue;

    for (rep_iter r = stripe->replicas_begin(); r != stripe->replicas_end(); ++r)
      ack[r->first]->add_strong_stripe(stripe->dirstripe(), r->second);
  }

  // include inode base for any inodes whose scatterlocks may have updated
  for (set<CInode*>::iterator p = rejoin_potential_updated_scatterlocks.begin();
       p != rejoin_potential_updated_scatterlocks.end();
       p++) {
    CInode *in = *p;
    for (map<int,int>::iterator r = in->replicas_begin();
	 r != in->replicas_end();
	 ++r)
      ack[r->first]->add_inode_base(in);
  }

  // send acks
  for (map<int,MMDSCacheRejoin*>::iterator p = ack.begin();
       p != ack.end();
       ++p) 
    mds->send_message_mds(p->second, p->first);
  
}


void MDCache::reissue_all_caps()
{
  dout(10) << "reissue_all_caps" << dendl;

  for (inode_map::iterator p = inodes.begin(); p != inodes.end(); ++p) {
    CInode *in = p->second;
    if (in->is_head() && in->is_any_caps()) {
      if (!mds->locker->eval(in, CEPH_CAP_LOCKS))
	mds->locker->issue_caps(in);
    }
  }
}


// ===============================================================================

struct C_MDC_QueuedCow : public Context {
  MDCache *mdcache;
  CInode *in;
  Mutation *mut;
  C_MDC_QueuedCow(MDCache *mdc, CInode *i, Mutation *m) : mdcache(mdc), in(i), mut(m) {}
  void finish(int r) {
    mdcache->_queued_file_recover_cow(in, mut);
  }
};

void MDCache::queue_file_recover(CInode *in)
{
  dout(10) << "queue_file_recover " << *in << dendl;
  assert(in->is_auth());

  // cow?
  set<snapid_t> s = get_snaprealm()->get_snaps();
  while (!s.empty() && *s.begin() < in->first)
    s.erase(s.begin());
  while (!s.empty() && *s.rbegin() > in->last)
    s.erase(*s.rbegin());
  dout(10) << " snaps in [" << in->first << "," << in->last << "] are " << s << dendl;
  if (s.size() > 1) {
    Mutation *mut = new Mutation;
    mut->add_projected_inode(in);
    in->project_inode();
    mut->ls = mds->mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mds->mdlog, "queue_file_recover cow");
    mds->mdlog->start_entry(le);
    predirty_journal_parents(mut, &le->metablob, in, 0, PREDIRTY_PRIMARY);

    s.erase(*s.begin());
    while (s.size()) {
      snapid_t snapid = *s.begin();
      CInode *cow_inode = 0;
      journal_cow_inode(mut, &le->metablob, in, snapid-1, &cow_inode);
      assert(cow_inode);
      _queue_file_recover(cow_inode);
      s.erase(*s.begin());
    }
    
    in->parent->first = in->first;
    le->metablob.add_inode(in, true);
    mds->mdlog->submit_entry(le, new C_MDC_QueuedCow(this, in, mut));
    mds->mdlog->flush();
  }

  _queue_file_recover(in);
}

void MDCache::_queued_file_recover_cow(CInode *in, Mutation *mut)
{
  in->pop_and_dirty_projected_inode(mut->ls);
  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;
}

void MDCache::_queue_file_recover(CInode *in)
{
  dout(15) << "_queue_file_recover " << *in << dendl;
  assert(in->is_auth());
  in->state_clear(CInode::STATE_NEEDSRECOVER);
  in->state_set(CInode::STATE_RECOVERING);
  in->auth_pin(this);
  file_recover_queue.insert(in);
}

void MDCache::unqueue_file_recover(CInode *in)
{
  dout(15) << "unqueue_file_recover " << *in << dendl;
  in->state_clear(CInode::STATE_RECOVERING);
  in->auth_unpin(this);
  file_recover_queue.erase(in);
}

/*
 * called after recovery to recover file sizes for previously opened (for write)
 * files.  that is, those where max_size > size.
 */
void MDCache::identify_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q)
{
  dout(10) << "identify_files_to_recover" << dendl;
  for (inode_map::iterator p = inodes.begin(); p != inodes.end(); ++p) {
    CInode *in = p->second;
    if (!in->is_auth())
      continue;
    
    bool recover = false;
    for (map<client_t,client_writeable_range_t>::iterator p = in->inode.client_ranges.begin();
	 p != in->inode.client_ranges.end();
	 p++) {
      Capability *cap = in->get_client_cap(p->first);
      if (!cap) {
	dout(10) << " client." << p->first << " has range " << p->second << " but no cap on " << *in << dendl;
	recover = true;
	break;
      }
    }

    if (recover) {
      in->filelock.set_state(LOCK_PRE_SCAN);
      recover_q.push_back(in);
    } else {
      check_q.push_back(in);
    }
  }
}

void MDCache::start_files_to_recover(vector<CInode*>& recover_q, vector<CInode*>& check_q)
{
  for (vector<CInode*>::iterator p = check_q.begin(); p != check_q.end(); p++) {
    CInode *in = *p;
    mds->locker->check_inode_max_size(in);
  }
  for (vector<CInode*>::iterator p = recover_q.begin(); p != recover_q.end(); p++) {
    CInode *in = *p;
    mds->locker->file_recover(&in->filelock);
  }
}

struct C_MDC_Recover : public Context {
  MDCache *mdc;
  CInode *in;
  uint64_t size;
  utime_t mtime;
  C_MDC_Recover(MDCache *m, CInode *i) : mdc(m), in(i), size(0) {}
  void finish(int r) {
    mdc->_recovered(in, r, size, mtime);
  }
};

void MDCache::do_file_recover()
{
  dout(10) << "do_file_recover " << file_recover_queue.size() << " queued, "
	   << file_recovering.size() << " recovering" << dendl;

  while (file_recovering.size() < 5 &&
	 !file_recover_queue.empty()) {
    CInode *in = *file_recover_queue.begin();
    file_recover_queue.erase(in);

    inode_t *pi = in->get_projected_inode();

    // blech
    if (pi->client_ranges.size() && !pi->get_max_size()) {
      mds->clog.warn() << "bad client_range " << pi->client_ranges
	  << " on ino " << pi->ino << "\n";
    }

    if (pi->client_ranges.size() && pi->get_max_size()) {
      dout(10) << "do_file_recover starting " << in->inode.size << " " << pi->client_ranges
	       << " " << *in << dendl;
      file_recovering.insert(in);
      
      C_MDC_Recover *fin = new C_MDC_Recover(this, in);
      mds->filer->probe(in->inode.ino, &in->inode.layout, in->last,
			pi->get_max_size(), &fin->size, &fin->mtime, false,
			0, fin);    
    } else {
      dout(10) << "do_file_recover skipping " << in->inode.size
	       << " " << *in << dendl;
      in->state_clear(CInode::STATE_RECOVERING);
      in->auth_unpin(this);
      if (in->filelock.is_stable()) {
	bool need_issue = false;
	mds->locker->eval(&in->filelock, &need_issue);
	if (in->is_head() && need_issue)
	  mds->locker->issue_caps(in);
      } else
	mds->locker->eval_gather(&in->filelock);
    }
  }
}

void MDCache::_recovered(CInode *in, int r, uint64_t size, utime_t mtime)
{
  dout(10) << "_recovered r=" << r << " size=" << in->inode.size << " mtime=" << in->inode.mtime
	   << " for " << *in << dendl;

  if (r != 0) {
    dout(0) << "recovery error! " << r << dendl;
    if (r == -EBLACKLISTED)
      mds->suicide();
    assert(0);
  }

  file_recovering.erase(in);
  in->state_clear(CInode::STATE_RECOVERING);

  if (!in->get_parent_dn() && !in->get_projected_parent_dn()) {
    dout(10) << " inode has no parents, killing it off" << dendl;
    in->auth_unpin(this);
    remove_inode(in);
  } else {
    // journal
    mds->locker->check_inode_max_size(in, true, true, size, mtime);
    in->auth_unpin(this);
  }

  do_file_recover();
}

void MDCache::purge_prealloc_ino(inodeno_t ino, Context *fin)
{
  object_t oid = CInode::get_object_name(ino, frag_t(), "");
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());

  dout(10) << "purge_prealloc_ino " << ino << " oid " << oid << dendl;
  SnapContext snapc;
  mds->objecter->remove(oid, oloc, snapc, ceph_clock_now(g_ceph_context), 0, 0, fin);
}  




// ===============================================================================



// ----------------------------
// truncate

void MDCache::truncate_inode(CInode *in, LogSegment *ls)
{
  inode_t *pi = in->get_projected_inode();
  dout(10) << "truncate_inode "
	   << pi->truncate_from << " -> " << pi->truncate_size
	   << " on " << *in
	   << dendl;

  ls->truncating_inodes.insert(in);
  in->get(CInode::PIN_TRUNCATING);

  _truncate_inode(in, ls);
}

struct C_MDC_TruncateFinish : public Context {
  MDCache *mdc;
  CInode *in;
  LogSegment *ls;
  C_MDC_TruncateFinish(MDCache *c, CInode *i, LogSegment *l) :
    mdc(c), in(i), ls(l) {}
  void finish(int r) {
    assert(r != -EINVAL);
    mdc->truncate_inode_finish(in, ls);
  }
};

void MDCache::_truncate_inode(CInode *in, LogSegment *ls)
{
  inode_t *pi = &in->inode;
  dout(10) << "_truncate_inode "
	   << pi->truncate_from << " -> " << pi->truncate_size
	   << " on " << *in << dendl;

  assert(pi->is_truncating());
  assert(pi->truncate_size < (1ULL << 63));
  assert(pi->truncate_from < (1ULL << 63));
  assert(pi->truncate_size < pi->truncate_from);

  in->auth_pin(this);

  SnapContext snapc = get_snaprealm()->get_snap_context();
  dout(10) << "_truncate_inode  snapc " << snapc << " on " << *in << dendl;
  mds->filer->truncate(in->inode.ino, &in->inode.layout, snapc,
		       pi->truncate_size, pi->truncate_from-pi->truncate_size,
                       pi->truncate_seq, utime_t(), 0,
		       0, new C_MDC_TruncateFinish(this, in, ls));
}

struct C_MDC_TruncateLogged : public Context {
  MDCache *mdc;
  CInode *in;
  Mutation *mut;
  C_MDC_TruncateLogged(MDCache *m, CInode *i, Mutation *mu) : mdc(m), in(i), mut(mu) {}
  void finish(int r) {
    mdc->truncate_inode_logged(in, mut);
  }
};

void MDCache::truncate_inode_finish(CInode *in, LogSegment *ls)
{
  dout(10) << "truncate_inode_finish " << *in << dendl;
  
  set<CInode*>::iterator p = ls->truncating_inodes.find(in);
  assert(p != ls->truncating_inodes.end());
  ls->truncating_inodes.erase(p);

  // update
  inode_t *pi = in->project_inode();
  pi->truncate_from = 0;
  pi->truncate_pending--;

  Mutation *mut = new Mutation;
  mut->ls = mds->mdlog->get_current_segment();
  mut->add_projected_inode(in);

  EUpdate *le = new EUpdate(mds->mdlog, "truncate finish");
  mds->mdlog->start_entry(le);
  le->metablob.add_stripe_context(in->get_parent_stripe());
  le->metablob.add_inode(in, true);
  le->metablob.add_truncate_finish(in->ino(), ls->offset);

  journal_dirty_inode(mut, &le->metablob, in);
  mds->mdlog->submit_entry(le, new C_MDC_TruncateLogged(this, in, mut));

  // flush immediately if there are readers/writers waiting
  if (in->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR))
    mds->mdlog->flush();
}

void MDCache::truncate_inode_logged(CInode *in, Mutation *mut)
{
  dout(10) << "truncate_inode_logged " << *in << dendl;
  mut->apply();
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  in->put(CInode::PIN_TRUNCATING);
  in->auth_unpin(this);

  list<Context*> waiters;
  in->take_waiting(CInode::WAIT_TRUNC, waiters);
  mds->queue_waiters(waiters);
}


void MDCache::add_recovered_truncate(CInode *in, LogSegment *ls)
{
  dout(20) << "add_recovered_truncate " << *in << " in " << ls << " offset " << ls->offset << dendl;
  ls->truncating_inodes.insert(in);
  in->get(CInode::PIN_TRUNCATING);
}

void MDCache::remove_recovered_truncate(CInode *in, LogSegment *ls)
{
  dout(20) << "remove_recovered_truncate " << *in << " in " << ls << " offset " << ls->offset << dendl;
  // if we have the logseg the truncate started in, it must be in our list.
  set<CInode*>::iterator p = ls->truncating_inodes.find(in);
  assert(p != ls->truncating_inodes.end());
  ls->truncating_inodes.erase(p);
  in->put(CInode::PIN_TRUNCATING);
}

void MDCache::start_recovered_truncates()
{
  dout(10) << "start_recovered_truncates" << dendl;
  for (map<uint64_t,LogSegment*>::iterator p = mds->mdlog->segments.begin();
       p != mds->mdlog->segments.end();
       p++) {
    LogSegment *ls = p->second;
    for (set<CInode*>::iterator q = ls->truncating_inodes.begin();
	 q != ls->truncating_inodes.end();
	 q++)
      _truncate_inode(*q, ls);
  }
}






// ================================================================================
// cache trimming


/*
 * note: only called while MDS is active or stopping... NOT during recovery.
 * however, we may expire a replica whose authority is recovering.
 * 
 */
bool MDCache::trim(int max) 
{
  // trim LRU
  if (max < 0) {
    max = g_conf->mds_cache_size;
    if (!max) return false;
  }
  dout(7) << "trim max=" << max << "  cur=" << lru.lru_get_size() << dendl;

  map<int, MCacheExpire*> expiremap;

  bool is_standby_replay = mds->is_standby_replay();
  int unexpirable = 0;
  list<CDentry*> unexpirables;
  // trim dentries from the LRU
  while (lru.lru_get_size() + unexpirable > (unsigned)max) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    if (is_standby_replay && dn->get_linkage() &&
        dn->get_linkage()->inode->item_open_file.is_on_list()) {
      unexpirables.push_back(dn);
      ++unexpirable;
      continue;
    }
    trim_dentry(dn, expiremap);
  }
  for(list<CDentry*>::iterator i = unexpirables.begin();
      i != unexpirables.end();
      ++i)
    lru.lru_insert_mid(*i);
#if 0
  // trim non-auth, non-bound subtrees
  for (map<CStripe*, set<CStripe*> >::iterator p = subtrees.begin();
       p != subtrees.end();) {
    CStripe *stripe = p->first;
    p++;
    if (!stripe->is_auth() &&
        !stripe->get_inode()->is_auth() &&
        stripe->get_num_ref() == 1) // subtree pin
      trim_stripe(stripe, 0, expiremap);
  }
#endif
  // trim root?
  if (max == 0 && root) {
    list<CStripe*> ls;
    root->get_stripes(ls);
    for (list<CStripe*>::iterator p = ls.begin(); p != ls.end(); ++p) {
      CStripe *stripe = *p;
      if (stripe->get_num_ref() == 1)  // subtree pin
	trim_stripe(stripe, expiremap);
    }
    if (root->get_num_ref() == 0)
      trim_inode(0, root, expiremap);
  }

  // send any expire messages
  send_expire_messages(expiremap);

  return true;
}

void MDCache::send_expire_messages(map<int, MCacheExpire*>& expiremap)
{
  // send expires
  for (map<int, MCacheExpire*>::iterator it = expiremap.begin();
       it != expiremap.end();
       it++) {
    dout(7) << "sending cache_expire to " << it->first << dendl;
    mds->send_message_mds(it->second, it->first);
  }
}


void MDCache::trim_dentry(CDentry *dn, map<int, MCacheExpire*>& expiremap)
{
  dout(12) << "trim_dentry " << *dn << dendl;
  
  CDentry::linkage_t *dnl = dn->get_linkage();

  CDir *dir = dn->get_dir();
  assert(dir);

  CStripe *stripe = dir->get_stripe();
  assert(stripe);

  // notify dentry authority?
  if (!dn->is_auth()) {
    pair<int,int> auth = dn->authority();

    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds." << a << " on " << *dn << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0)
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dentry(dir->dirfrag(), dn->name, dn->last, dn->get_replica_nonce());
    }
  }

  // adjust the dir state
  // NOTE: we can safely remove a clean, null dentry without effecting
  //       directory completeness.
  // (check this _before_ we unlink the inode, below!)
  bool clear_complete = false;
  if (!(dnl->is_null() && dn->is_clean())) 
    clear_complete = true;
  
  // unlink the dentry
  if (dnl->is_remote()) {
    // just unlink.
    dir->unlink_inode(dn);
  } 
  else if (dnl->is_primary()) {
    // expire the inode, too.
    CInode *in = dnl->get_inode();
    assert(in);
    trim_inode(dn, in, expiremap);
  } 
  else {
    assert(dnl->is_null());
  }
    
  // remove dentry
  if (dir->is_auth())
    dir->add_to_bloom(dn);
  dir->remove_dentry(dn);

  if (clear_complete)
    dir->state_clear(CDir::STATE_COMPLETE);
  
  if (mds->logger) mds->logger->inc(l_mds_iex);
}


void MDCache::trim_dirfrag(CDir *dir, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_dirfrag " << *dir << dendl;

  assert(dir->get_num_ref() == 0);

  if (!dir->is_auth()) {
    pair<int,int> auth = dir->authority();

    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds." << a << " on   " << *dir << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0)
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_dir(dir->dirfrag(), dir->replica_nonce);
    }
  }

  dir->get_stripe()->close_dirfrag(dir->get_frag());
}

void MDCache::trim_stripe(CStripe *stripe, map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_stripe " << *stripe << dendl;

  assert(stripe->get_num_ref() == 0);

  // DIR
  list<CDir*> dirs;
  stripe->get_dirfrags(dirs);
  for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d)
    trim_dirfrag(*d, expiremap);

  // STRIPE
  if (!stripe->is_auth()) {
    pair<int,int> auth = stripe->authority();

    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (a == mds->get_nodeid()) continue;          // on export, ignore myself.

      dout(12) << "  sending expire to mds." << a << " on   " << *stripe << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0)
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_stripe(stripe->dirstripe(),
                               stripe->get_replica_nonce());
    }
  }
  stripe->get_inode()->close_stripe(stripe);
}

void MDCache::trim_inode(CDentry *dn, CInode *in,
                         map<int, MCacheExpire*>& expiremap)
{
  dout(15) << "trim_inode " << *in << dendl;
  assert(in->get_num_ref() == 0);

  // STRIPE
  list<CStripe*> stripes;
  in->get_stripes(stripes);
  for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s)
    trim_stripe(*s, expiremap);

  // INODE
  if (!in->is_auth()) {
    pair<int,int> auth = in->authority();
    
    for (int p=0; p<2; p++) {
      int a = auth.first;
      if (p) a = auth.second;
      if (a < 0 || (p == 1 && auth.second == auth.first)) break;
      if (a == mds->get_nodeid()) continue; // on export, ignore myself.

      dout(12) << "  sending expire to mds." << a << " on " << *in << dendl;
      assert(a != mds->get_nodeid());
      if (expiremap.count(a) == 0) 
	expiremap[a] = new MCacheExpire(mds->get_nodeid());
      expiremap[a]->add_inode(in->vino(), in->get_replica_nonce());
    }
  }

  /*
  if (in->is_auth()) {
    if (in->hack_accessed)
      mds->logger->inc("outt");
    else {
      mds->logger->inc("outut");
      mds->logger->fset("oututl", ceph_clock_now(g_ceph_context) - in->hack_load_stamp);
    }
  }
  */
    
  // unlink
  if (dn)
    dn->get_dir()->unlink_inode(dn);
  remove_inode(in);
}


/**
 * trim_non_auth - remove any non-auth items from our cache
 *
 * this reduces the amount of non-auth metadata in our cache, reducing the 
 * load incurred by the rejoin phase.
 *
 * the only non-auth items that remain are those that are needed to 
 * attach our own subtrees to the root.  
 *
 * when we are done, all dentries will be in the top bit of the lru.
 *
 * why we have to do this:
 *  we may not have accurate linkage for non-auth items.  which means we will 
 *  know which subtree it falls into, and can not be sure to declare it to the
 *  correct authority.  
 */
void MDCache::trim_non_auth()
{
  dout(7) << "trim_non_auth" << dendl;
  stringstream warn_str_dirs;

  // note first auth item we see.
  // when we see it the second time, stop.
  CDentry *first_auth = 0;

  // trim non-auth items from the lru
  while (lru.lru_get_size() > 0) {
    CDentry *dn = (CDentry*)lru.lru_expire();
    if (!dn) break;
    CDentry::linkage_t *dnl = dn->get_linkage();

    if (dn->is_auth()) {
      // add back into lru (at the top)
      lru.lru_insert_top(dn);

      if (!first_auth) {
	first_auth = dn;
      } else {
	if (first_auth == dn) 
	  break;
      }
    } else {
      // non-auth.  expire.
      CDir *dir = dn->get_dir();
      assert(dir);

      // unlink the dentry
      dout(15) << "trim_non_auth removing " << *dn << dendl;
      if (dnl->is_remote()) {
	dir->unlink_inode(dn);
      } 
      else if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
        warn_str_dirs << in->get_parent_dn()->get_name() << "\n";

        list<CStripe*> stripes;
        in->get_stripes(stripes);
        for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
          CStripe *stripe = *s;
          list<CDir*> ls;
          stripe->get_dirfrags(ls);
          for (list<CDir*>::iterator p = ls.begin(); p != ls.end(); ++p) {
            CDir *subdir = *p;
            filepath fp;
            subdir->get_inode()->make_path(fp);
            warn_str_dirs << fp << "\n";
            stripe->close_dirfrag(subdir->get_frag());
          }
          in->close_stripe(stripe);
	}
	dir->unlink_inode(dn);
	remove_inode(in);
      } 
      else {
	assert(dnl->is_null());
      }

      assert(!dir->has_bloom());
      dir->remove_dentry(dn);
      // adjust the dir state
      dir->state_clear(CDir::STATE_COMPLETE);  // dir incomplete!
    }
  }

  // move everything in the pintail to the top bit of the lru.
  lru.lru_touch_entire_pintail();

  if (lru.lru_get_size() == 0) {
    // root, stray, etc.?
    inode_map::iterator p = inodes.begin();
    while (p != inodes.end()) {
      hash_map<vinodeno_t,CInode*>::iterator next = p;
      ++next;
      CInode *in = p->second;
      if (!in->is_auth() && in->ino() != MDS_INO_CONTAINER) {
        list<CStripe*> stripes;
        in->get_stripes(stripes);
        for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
          CStripe *stripe = *s;
          list<CDir*> ls;
          stripe->get_dirfrags(ls);
          for (list<CDir*>::iterator p = ls.begin();
               p != ls.end();
               ++p) {
            CDir *dir = *p;
            dout(0) << " ... " << *dir << dendl;
            filepath fp;
            in->make_path(fp);
            warn_str_dirs << fp << "\n";
            assert(dir->get_num_ref() == 0);
            stripe->close_dirfrag(dir->get_frag());
          }
          assert(stripe->get_num_ref() == 0);
          in->close_stripe(stripe);
        }
	dout(0) << " ... " << *in << dendl;
	if (in->get_parent_dn())
	  warn_str_dirs << in->get_parent_dn()->get_name() << "\n";
	assert(in->get_num_ref() == 0);
	remove_inode(in);
      }
      p = next;
    }
  }

  if (warn_str_dirs.peek() != EOF) {
    mds->clog.info() << "trim_non_auth has deleted paths: " << "\n";
    mds->clog.info(warn_str_dirs);
  }
}

/**
 * Recursively trim the subtree rooted at directory to remove all
 * CInodes/CDentrys/CDirs that aren't links to remote MDSes, or ancestors
 * of those links. This is used to clear invalid data out of the cache.
 * Note that it doesn't clear the passed-in directory, since that's not
 * always safe.
 */
bool MDCache::trim_non_auth_subtree(CStripe *stripe)
{
  dout(10) << "trim_non_auth_subtree(" << stripe << ") " << *stripe << dendl;

  if (uncommitted_slave_rename_oldstripe.count(stripe)) // preserve the stripe for rollback
    return true;

  bool keep_stripe = false;
  list<CDir*> dirs;
  stripe->get_dirfrags(dirs);
  for (list<CDir*>::iterator i = dirs.begin(); i != dirs.end(); ++i) {
    CDir *dir = *i;
    dout(10) << "trim_non_auth_subtree(" << stripe << ") Checking dir " << dir << dendl;

    bool keep_dir = false;
    for (CDir::map_t::iterator j = dir->begin(); j != dir->end();) {
      CDentry *dn = j->second;
      j++;
      dout(10) << "trim_non_auth_subtree(" << stripe << ") Checking dentry " << dn << dendl;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_primary()) { // check for subdirectories, etc
        CInode *in = dnl->get_inode();
        bool keep_inode = false;
        if (in->is_dir()) {
          list<CStripe*> stripes;
          in->get_stripes(stripes);
          for (list<CStripe*>::iterator sub = stripes.begin(); sub != stripes.end(); ++sub) {
            CStripe *substripe = *sub;
            if (uncommitted_slave_rename_oldstripe.count(substripe)) {
              // preserve the stripe for rollback
              keep_inode = true;
              dout(10) << "trim_non_auth_subtree(" << stripe << ") substripe " << *substripe << " is kept!" << dendl;
            } else if (trim_non_auth_subtree(substripe))
              keep_inode = true;
            else
              in->close_stripe(substripe);
          }
        }
        if (!keep_inode) { // remove it!
          dout(20) << "trim_non_auth_subtree(" << stripe << ") removing inode " << in << " with dentry" << dn << dendl;
          dir->unlink_inode(dn);
          remove_inode(in);
          assert(!dir->has_bloom());
          dir->remove_dentry(dn);
        } else {
          dout(20) << "trim_non_auth_subtree(" << stripe << ") keeping inode " << in << " with dentry " << dn <<dendl;
          keep_dir = true;
        }
      } else { // just remove it
        dout(20) << "trim_non_auth_subtree(" << stripe << ") removing dentry " << dn << dendl;
        if (dnl->is_remote())
          dir->unlink_inode(dn);
        dir->remove_dentry(dn);
      }
    }
    if (!keep_dir) {
      dout(20) << "trim_non_auth_subtree(" << stripe << ") removing dir " << dir << dendl;
      stripe->close_dirfrag(dir->get_frag());
    } else {
      dout(20) << "trim_non_auth_subtree(" << stripe << ") keeping dir " << dir << dendl;
      keep_stripe = true;
    }
  }
  /**
   * We've now checked all our children and deleted those that need it.
   * Now return to caller, and tell them if *we're* a keeper.
   */
  return keep_stripe;
}
#if 0
/*
 * during replay, when we determine a subtree is no longer ours, we
 * try to trim it from our cache.  because subtrees must be connected
 * to the root, the fact that we can trim this tree may mean that our
 * children or parents can also be trimmed.
 */
void MDCache::try_trim_non_auth_subtree(CStripe *stripe)
{
  dout(10) << "try_trim_nonauth_subtree " << *stripe << dendl;

  // can we now trim child subtrees?
  set<CStripe*> bounds;
  get_subtree_bounds(stripe, bounds);
  for (set<CStripe*>::iterator p = bounds.begin(); p != bounds.end(); p++) {
    CStripe *bd = *p;
    if (bd->get_stripe_auth().first != mds->whoami && // we are not auth
	bd->get_num_any() == 0) {                     // and empty
      CInode *bi = bd->get_inode();
      dout(10) << " closing empty non-auth child subtree " << *bd << dendl;
      remove_subtree(bd);
      bd->mark_clean();
      bi->close_stripe(bd);
    }
  }

  if (trim_non_auth_subtree(stripe)) {
    // keep
    try_subtree_merge(stripe);
  } else {
    // can we trim this subtree (and possibly our ancestors) too?
    while (true) {
      CInode *diri = stripe->get_inode();
      if (diri->is_base())
	break;

      CStripe *psub = get_subtree_root(diri->get_parent_stripe());
      dout(10) << " parent subtree is " << *psub << dendl;
      if (psub->get_stripe_auth().first == mds->whoami)
	break;  // we are auth, keep.

      dout(10) << " closing empty non-auth subtree " << *stripe << dendl;
      remove_subtree(stripe);
      stripe->mark_clean();
      diri->close_stripe(stripe);

      dout(10) << " parent subtree also non-auth: " << *psub << dendl;
      if (trim_non_auth_subtree(psub))
	break;
      stripe = psub;
    }
  }

  show_subtrees();
}
#endif

/* This function DOES put the passed message before returning */
void MDCache::handle_cache_expire(MCacheExpire *m)
{
  int from = m->get_from();
  
  dout(7) << "cache_expire from mds." << from << dendl;

  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    m->put();
    return;
  }

  // INODES
  for (map<vinodeno_t,int>::iterator it = m->inodes.begin();
       it != m->inodes.end();
       it++) {
    CInode *in = get_inode(it->first);
    int nonce = it->second;

    if (!in) {
      dout(0) << " inode expire on " << it->first << " from " << from 
          << ", don't have it" << dendl;
      assert(in);
    }
    assert(in->is_auth());

    // check nonce
    if (nonce == in->get_replica_nonce(from)) {
      // remove from our cached_by
      dout(7) << " inode expire on " << *in << " from mds." << from 
          << " cached_by was " << in->get_replicas() << dendl;
      inode_remove_replica(in, from);
    } 
    else {
      // this is an old nonce, ignore expire.
      dout(7) << " inode expire on " << *in << " from mds." << from
          << " with old nonce " << nonce
          << " (current " << in->get_replica_nonce(from) << "), dropping" 
          << dendl;
    }
  }

  // STRIPES
  for (map<dirstripe_t,int>::iterator it = m->stripes.begin();
       it != m->stripes.end();
       it++) {
    CStripe *stripe = get_dirstripe(it->first);
    int nonce = it->second;

    if (!stripe) {
      dout(0) << " stripe expire on " << it->first << " from " << from 
          << ", don't have it" << dendl;
      assert(stripe);
    }
    assert(stripe->is_auth());

    // check nonce
    if (nonce == stripe->get_replica_nonce(from)) {
      // remove from our cached_by
      dout(7) << " stripe expire on " << *stripe << " from mds." << from
          << " replicas was " << stripe->get_replicas() << dendl;
      stripe->remove_replica(from);
    } 
    else {
      // this is an old nonce, ignore expire.
      dout(7) << " stripe expire on " << *stripe << " from mds." << from 
          << " with old nonce " << nonce << " (current " << stripe->get_replica_nonce(from)
          << "), dropping" << dendl;
    }
  }

  // DIRS
  for (map<dirfrag_t,int>::iterator it = m->dirs.begin();
       it != m->dirs.end();
       it++) {
    CDir *dir = get_dirfrag(it->first);
    int nonce = it->second;

    if (!dir) {
      dout(0) << " dir expire on " << it->first << " from " << from 
          << ", don't have it" << dendl;
      assert(dir);
    }  
    assert(dir->is_auth());

    // check nonce
    if (nonce == dir->get_replica_nonce(from)) {
      // remove from our cached_by
      dout(7) << " dir expire on " << *dir << " from mds." << from
          << " replicas was " << dir->replica_map << dendl;
      dir->remove_replica(from);
    }
    else {
      // this is an old nonce, ignore expire.
      dout(7) << " dir expire on " << *dir << " from mds." << from 
          << " with old nonce " << nonce << " (current " << dir->get_replica_nonce(from)
          << "), dropping" << dendl;
    }
  }

  // DENTRIES
  for (map<dirfrag_t, map<pair<string,snapid_t>,int> >::iterator pd = m->dentries.begin();
       pd != m->dentries.end();
       ++pd) {
    dout(10) << " dn expires in dir " << pd->first << dendl;
    CStripe *stripe = get_dirstripe(pd->first.stripe);
    assert(stripe);
    CDir *dir = stripe->get_dirfrag(pd->first.frag);

    if (!dir) {
      dout(0) << " dn expires on " << pd->first << " from " << from
          << ", must have refragmented" << dendl;
    } else {
      assert(dir->is_auth());
    }

    for (map<pair<string,snapid_t>,int>::iterator p = pd->second.begin();
         p != pd->second.end();
         ++p) {
      int nonce = p->second;
      CDentry *dn;

      if (dir) {
        dn = dir->lookup(p->first.first, p->first.second);
      } else {
        // which dirfrag for this dentry?
        CDir *dir = stripe->get_dirfrag(stripe->pick_dirfrag(p->first.first));
        assert(dir->is_auth());
        dn = dir->lookup(p->first.first, p->first.second);
      } 

      if (!dn) 
        dout(0) << "  missing dentry for " << p->first.first << " snap " << p->first.second << " in " << *dir << dendl;
      assert(dn);

      if (nonce == dn->get_replica_nonce(from)) {
        dout(7) << "  dentry_expire on " << *dn << " from mds." << from << dendl;
        dentry_remove_replica(dn, from);
      } 
      else {
        dout(7) << "  dentry_expire on " << *dn << " from mds." << from
            << " with old nonce " << nonce << " (current " << dn->get_replica_nonce(from)
            << "), dropping" << dendl;
      }
    }
  }

  // done
  m->put();
}

void MDCache::process_delayed_expire(CStripe *stripe)
{
  dout(7) << "process_delayed_expire on " << *stripe << dendl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[stripe].begin();
       p != delayed_expire[stripe].end();
       ++p) 
    handle_cache_expire(p->second);
  delayed_expire.erase(stripe);  
}

void MDCache::discard_delayed_expire(CStripe *stripe)
{
  dout(7) << "discard_delayed_expire on " << *stripe << dendl;
  for (map<int,MCacheExpire*>::iterator p = delayed_expire[stripe].begin();
       p != delayed_expire[stripe].end();
       ++p) 
    p->second->put();
  delayed_expire.erase(stripe);  
}

void MDCache::inode_remove_replica(CInode *in, int from)
{
  in->remove_replica(from);
  in->mds_caps_wanted.erase(from);
  
  // note: this code calls _eval more often than it needs to!
  // fix lock
  if (in->authlock.remove_replica(from)) mds->locker->eval_gather(&in->authlock);
  if (in->linklock.remove_replica(from)) mds->locker->eval_gather(&in->linklock);
  if (in->filelock.remove_replica(from)) mds->locker->eval_gather(&in->filelock);
  if (in->xattrlock.remove_replica(from)) mds->locker->eval_gather(&in->xattrlock);

  if (in->nestlock.remove_replica(from)) mds->locker->eval_gather(&in->nestlock);
  if (in->flocklock.remove_replica(from)) mds->locker->eval_gather(&in->flocklock);
  if (in->policylock.remove_replica(from)) mds->locker->eval_gather(&in->policylock);

  // trim?
  maybe_eval_stray(in);
}

void MDCache::dentry_remove_replica(CDentry *dn, int from)
{
  dn->remove_replica(from);

  // fix lock
  if (dn->lock.remove_replica(from))
    mds->locker->eval_gather(&dn->lock);

  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  if (dnl->is_primary())
    maybe_eval_stray(dnl->get_inode());
}

void MDCache::trim_client_leases()
{
  utime_t now = ceph_clock_now(g_ceph_context);
  
  dout(10) << "trim_client_leases" << dendl;

  for (int pool=0; pool<client_lease_pools; pool++) {
    int before = client_leases[pool].size();
    if (client_leases[pool].empty()) 
      continue;

    while (!client_leases[pool].empty()) {
      ClientLease *r = client_leases[pool].front();
      if (r->ttl > now) break;
      CDentry *dn = (CDentry*)r->parent;
      dout(10) << " expiring client." << r->client << " lease of " << *dn << dendl;
      dn->remove_client_lease(r, mds->locker);
    }
    int after = client_leases[pool].size();
    dout(10) << "trim_client_leases pool " << pool << " trimmed "
	     << (before-after) << " leases, " << after << " left" << dendl;
  }
}


void MDCache::check_memory_usage()
{
  static MemoryModel mm(g_ceph_context);
  static MemoryModel::snap last;
  mm.sample(&last);
  static MemoryModel::snap baseline = last;

  // check client caps
  int num_inodes = inodes.size();
  float caps_per_inode = (float)num_caps / (float)num_inodes;
  //float cap_rate = (float)num_inodes_with_caps / (float)inode_map.size();

  dout(2) << "check_memory_usage"
	   << " total " << last.get_total()
	   << ", rss " << last.get_rss()
	   << ", heap " << last.get_heap()
	   << ", malloc " << last.malloc << " mmap " << last.mmap
	   << ", baseline " << baseline.get_heap()
	   << ", buffers " << (buffer::get_total_alloc() >> 10)
	   << ", max " << g_conf->mds_mem_max
	   << ", " << num_inodes_with_caps << " / " << num_inodes << " inodes have caps"
	   << ", " << num_caps << " caps, " << caps_per_inode << " caps per inode"
	   << dendl;

  mds->mlogger->set(l_mdm_rss, last.get_rss());
  mds->mlogger->set(l_mdm_heap, last.get_heap());
  mds->mlogger->set(l_mdm_malloc, last.malloc);

  /*int size = last.get_total();
  if (size > g_conf->mds_mem_max * .9) {
    float ratio = (float)g_conf->mds_mem_max * .9 / (float)size;
    if (ratio < 1.0)
      mds->server->recall_client_state(ratio);
  } else 
    */
  if (num_inodes_with_caps > g_conf->mds_cache_size) {
    float ratio = (float)g_conf->mds_cache_size * .9 / (float)num_inodes_with_caps;
    if (ratio < 1.0)
      mds->server->recall_client_state(ratio);
  }

}



// =========================================================================================
// shutdown

class C_MDC_ShutdownCheck : public Context {
  MDCache *mdc;
public:
  C_MDC_ShutdownCheck(MDCache *m) : mdc(m) {}
  void finish(int) {
    mdc->shutdown_check();
  }
};

void MDCache::shutdown_check()
{
  dout(0) << "shutdown_check at " << ceph_clock_now(g_ceph_context) << dendl;

  // cache
  char old_val[32] = { 0 };
  char *o = old_val;
  g_conf->get_val("debug_mds", &o, sizeof(old_val));
  g_conf->set_val("debug_mds", "10");
  g_conf->apply_changes(NULL);
  show_cache();
  g_conf->set_val("debug_mds", old_val);
  g_conf->apply_changes(NULL);
  mds->timer.add_event_after(g_conf->mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  // this
  dout(0) << "lru size now " << lru.lru_get_size() << dendl;
  dout(0) << "log len " << mds->mdlog->get_num_events() << dendl;


  if (mds->objecter->is_active()) {
    dout(0) << "objecter still active" << dendl;
    mds->objecter->dump_active();
  }
}


void MDCache::shutdown_start()
{
  dout(2) << "shutdown_start" << dendl;

  if (g_conf->mds_shutdown_check)
    mds->timer.add_event_after(g_conf->mds_shutdown_check, new C_MDC_ShutdownCheck(this));

  //  g_conf->debug_mds = 10;
}


bool MDCache::shutdown_pass()
{
  dout(7) << "shutdown_pass" << dendl;

  if (mds->is_stopped()) {
    dout(7) << " already shut down" << dendl;
    show_cache();
    return true;
  }

  // close out any sessions (and open files!) before we try to trim the log, etc.
  if (!mds->server->terminating_sessions &&
      mds->sessionmap.have_unclosed_sessions()) {
    mds->server->terminate_sessions();
    return false;
  }


  // flush what we can from the log
  mds->mdlog->trim(0);

  if (mds->mdlog->get_num_segments() > 1) {
    dout(7) << "still >1 segments, waiting for log to trim" << dendl;
    return false;
  }

  // empty stray dir
  if (!shutdown_export_strays()) {
    dout(7) << "waiting for strays to migrate" << dendl;
    return false;
  }
  
  // drop our reference to our stray dir inode
  for (int i = 0; i < NUM_STRAY; ++i)
    if (strays[i])
      unpin_stray(strays[i]);

  // drop the reference to inode container
  if (container.get_inode())
    unpin_stray(container.get_inode());

  // trim cache
  trim(0);
  dout(5) << "lru size now " << lru.lru_get_size() << dendl;

  // (only do this once!)
  if (!mds->mdlog->is_capped()) {
    dout(7) << "capping the log" << dendl;
    mds->mdlog->cap();
    mds->mdlog->trim();
  }
  
  if (!mds->mdlog->empty()) {
    dout(7) << "waiting for log to flush.. " << mds->mdlog->get_num_events() 
	    << " in " << mds->mdlog->get_num_segments() << " segments" << dendl;
    return false;
  }
  
  if (!did_shutdown_log_cap) {
    // flush journal header
    dout(7) << "writing header for (now-empty) journal" << dendl;
    assert(mds->mdlog->empty());
    mds->mdlog->write_head(0);  
    // NOTE: filer active checker below will block us until this completes.
    did_shutdown_log_cap = true;
    return false;
  }

  // filer active?
  if (mds->objecter->is_active()) {
    dout(7) << "objecter still active" << dendl;
    mds->objecter->dump_active();
    return false;
  }

  // trim what we can from the cache
  if (lru.lru_get_size() > 0) {
    dout(7) << "there's still stuff in the cache: " << lru.lru_get_size() << dendl;
    show_cache();
    //dump();
    return false;
  } 
  
  // done!
  dout(2) << "shutdown done." << dendl;
  return true;
}

bool MDCache::shutdown_export_strays()
{
  if (mds->get_nodeid() == 0) return true;
  
  dout(10) << "shutdown_export_strays" << dendl;

  bool done = true;
  static set<inodeno_t> exported_strays;

  list<CStripe*> stripes;
  for (int i = 0; i < NUM_STRAY; ++i)
    if (strays[i])
      strays[i]->get_stripes(stripes);

  list<CDir*> dfs;
  while (!stripes.empty()) {
    CStripe *stripe = stripes.front();
    stripes.pop_front();
    stripe->get_dirfrags(dfs);
  }

  while (!dfs.empty()) {
    CDir *dir = dfs.front();
    dfs.pop_front();

    if (!dir->is_complete()) {
      dir->fetch(0);
      done = false;
    }

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 p++) {
      CDentry *dn = p->second;
      CDentry::linkage_t *dnl = dn->get_linkage();
      if (dnl->is_null()) continue;
      done = false;

      // FIXME: we'll deadlock if a rename fails.
      if (exported_strays.count(dnl->get_inode()->ino()) == 0) {
	exported_strays.insert(dnl->get_inode()->ino());
	migrate_stray(dn, 0);  // send to root!
      } else {
	dout(10) << "already exporting " << *dn << dendl;
      }
    }
  }

  return done;
}

bool MDCache::shutdown_export_caps()
{
#if 0
  // export caps?
  //  note: this runs more often than it should.
  static bool exported_caps = false;
  static set<CStripe*> exported_caps_in;
  if (!exported_caps) {
    dout(7) << "searching for caps to export" << dendl;
    exported_caps = true;

    list<CStripe*> stripes;
    for (map<CStripe*,set<CStripe*> >::iterator p = subtrees.begin();
	 p != subtrees.end();
	 ++p) {
      if (exported_caps_in.count(p->first)) continue;
      if (p->first->is_auth() ||
	  p->first->is_ambiguous_auth())
	exported_caps = false; // we'll have to try again
      else {
	stripes.push_back(p->first);
	exported_caps_in.insert(p->first);
      }
    }
    while (!stripes.empty()) {
      CStripe *stripe = stripes.front();
      stripes.pop_front();

      list<CDir*> dirs;
      stripe->get_dirfrags(dirs);
      for (list<CDir*>::iterator d = dirs.begin(); d != dirs.end(); ++d) {
        CDir *dir = *d;
        for (CDir::map_t::iterator p = dir->items.begin();
             p != dir->items.end();
             ++p) {
          CDentry *dn = p->second;
          CDentry::linkage_t *dnl = dn->get_linkage();
          if (!dnl->is_primary()) continue;
          CInode *in = dnl->get_inode();
          if (in->is_dir())
            in->get_nested_stripes(stripes);
          if (in->is_any_caps() && !in->state_test(CInode::STATE_EXPORTINGCAPS))
            migrator->export_caps(in);
        }
      }
    }
  }
#endif
  return true;
}





// ========= messaging ==============

/* This function DOES put the passed message before returning */
void MDCache::dispatch(Message *m)
{
  switch (m->get_type()) {

    // RESOLVE
  case MSG_MDS_RESOLVE:
    handle_resolve((MMDSResolve*)m);
    break;
  case MSG_MDS_RESOLVEACK:
    handle_resolve_ack((MMDSResolveAck*)m);
    break;

    // REJOIN
  case MSG_MDS_CACHEREJOIN:
    handle_cache_rejoin((MMDSCacheRejoin*)m);
    break;

  case MSG_MDS_DISCOVER:
    handle_discover((MDiscover*)m);
    break;
  case MSG_MDS_DISCOVERREPLY:
    handle_discover_reply((MDiscoverReply*)m);
    break;

  case MSG_MDS_DIRUPDATE:
    handle_dir_update((MDirUpdate*)m);
    break;

  case MSG_MDS_CACHEEXPIRE:
    handle_cache_expire((MCacheExpire*)m);
    break;



  case MSG_MDS_DENTRYLINK:
    handle_dentry_link((MDentryLink*)m);
    break;
  case MSG_MDS_DENTRYUNLINK:
    handle_dentry_unlink((MDentryUnlink*)m);
    break;


  case MSG_MDS_FRAGMENTNOTIFY:
    handle_fragment_notify((MMDSFragmentNotify*)m);
    break;
    

  case MSG_MDS_FINDINO:
    handle_find_ino((MMDSFindIno *)m);
    break;
  case MSG_MDS_FINDINOREPLY:
    handle_find_ino_reply((MMDSFindInoReply *)m);
    break;

  case MSG_MDS_RESTRIPE:
    container.handle_restripe((MMDSRestripe*)m);
    break;
  case MSG_MDS_RESTRIPEACK:
    container.handle_restripe_ack((MMDSRestripeAck*)m);
    break;

  case MSG_MDS_PARENTSTATS:
    if (!is_open())
      wait_for_open(new C_MDS_RetryMessage(mds, m));
    else
      parentstats.handle((MParentStats*)m);
    break;

  default:
    dout(7) << "cache unknown message " << m->get_type() << dendl;
    assert(0);
    m->put();
    break;
  }
}

Context *MDCache::_get_waiter(MDRequest *mdr, Message *req, Context *fin)
{
  if (mdr) {
    dout(20) << "_get_waiter retryrequest" << dendl;
    return new C_MDS_RetryRequest(this, mdr);
  } else if (req) {
    dout(20) << "_get_waiter retrymessage" << dendl;
    return new C_MDS_RetryMessage(mds, req);
  } else {
    return fin;
  }
}

int MDCache::path_traverse(MDRequest *mdr, Message *req, Context *fin,     // who
			   const filepath& path,                   // what
                           vector<CDentry*> *pdnvec,         // result
			   CInode **pin,
                           int onfail)
{
  bool discover = (onfail == MDS_TRAVERSE_DISCOVER);
  bool null_okay = (onfail == MDS_TRAVERSE_DISCOVERXLOCK);
  bool forward = (onfail == MDS_TRAVERSE_FORWARD);

  assert(mdr || req || fin);
  assert(!forward || mdr || req);  // forward requires a request

  snapid_t snapid = CEPH_NOSNAP;
  if (mdr)
    mdr->snapid = snapid;

  client_t client = (mdr && mdr->reqid.name.is_client()) ? mdr->reqid.name.num() : -1;

  if (mds->logger) mds->logger->inc(l_mds_t);

  dout(7) << "traverse: opening base ino " << path.get_ino() << " snap " << snapid << dendl;
  CInode *cur = get_inode(path.get_ino());
  if (cur == NULL) {
    if (MDS_INO_IS_MDSDIR(path.get_ino())) 
      open_foreign_mdsdir(path.get_ino(), _get_waiter(mdr, req, fin));
    else {
      //assert(0);  // hrm.. broken
      return -ESTALE;
    }
    return 1;
  }
  if (cur->state_test(CInode::STATE_PURGING))
    return -ESTALE;

  // start trace
  if (pdnvec)
    pdnvec->clear();
  if (pin)
    *pin = cur;

  unsigned depth = 0;
  while (depth < path.depth()) {
    dout(12) << "traverse: path seg depth " << depth << " '" << path[depth]
	     << "' snapid " << snapid << dendl;
    
    if (!cur->is_dir()) {
      dout(7) << "traverse: " << *cur << " not a dir " << dendl;
      return -ENOTDIR;
    }

    // open stripe
    __u32 dnhash = cur->hash_dentry_name(path[depth]);
    stripeid_t stripeid = cur->pick_stripe(dnhash);
    CStripe *curstripe = cur->get_stripe(stripeid);
    if (!curstripe) {
      int stripe_auth = cur->get_stripe_auth(stripeid);
      if (stripe_auth == mds->get_nodeid()) {
        if (cur->is_frozen_dir()) {
          dout(7) << "traverse: " << *cur->get_parent_dir() << " is frozen_dir, waiting" << dendl;
          cur->get_parent_dir()->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req, fin));
          return 1;
        }
        curstripe = cur->get_or_open_stripe(stripeid);
      } else {
        discover_dir_stripe(cur, stripeid, _get_waiter(mdr, req, fin),
                            stripe_auth);
        return 1;
      }
    }
    assert(curstripe);

    // open dir
    frag_t fg = curstripe->pick_dirfrag(dnhash);
    CDir *curdir = curstripe->get_dirfrag(fg);
    if (!curdir) {
      if (curstripe->is_auth()) {
        // parent dir frozen_dir?
        if (curstripe->is_frozen()) {
          dout(7) << "traverse: " << *curstripe << " is frozen, waiting" << dendl;
          curstripe->add_waiter(CStripe::WAIT_UNFREEZE, _get_waiter(mdr, req, fin));
          return 1;
        }
        curdir = curstripe->get_or_open_dirfrag(fg);
      } else {
        // discover?
	dout(10) << "traverse: need dirfrag " << fg << ", doing discover from " << *curstripe << dendl;
        discover_dir_frag(curstripe, fg, _get_waiter(mdr, req, fin),
                          curstripe->authority().first);
	if (mds->logger) mds->logger->inc(l_mds_tdis);
        return 1;
      }
    }
    assert(curdir);

#ifdef MDS_VERIFY_FRAGSTAT
    if (curdir->is_complete())
      curdir->verify_fragstat();
#endif

    // frozen?
    /*
    if (curdir->is_frozen()) {
    // doh!
      // FIXME: traverse is allowed?
      dout(7) << "traverse: " << *curdir << " is frozen, waiting" << dendl;
      curdir->add_waiter(CDir::WAIT_UNFREEZE, _get_waiter(mdr, req, fin));
      if (onfinish) delete onfinish;
      return 1;
    }
    */

    // must read directory hard data (permissions, x bit) to traverse
#if 0    
    if (!noperm && 
	!mds->locker->rdlock_try(&cur->authlock, client, 0)) {
      dout(7) << "traverse: waiting on authlock rdlock on " << *cur << dendl;
      cur->authlock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req, fin));
      return 1;
    }
#endif

    // dentry
    CDentry *dn = curdir->lookup(path[depth], snapid);
    CDentry::linkage_t *dnl = dn ? dn->get_projected_linkage() : 0;

    // null and last_bit and xlocked by me?
    if (dnl && dnl->is_null() && null_okay) {
      dout(10) << "traverse: hit null dentry at tail of traverse, succeeding" << dendl;
      if (pdnvec)
	pdnvec->push_back(dn);
      if (pin)
	*pin = 0;
      break; // done!
    }

    if (dnl &&
	dn->lock.is_xlocked() &&
	dn->lock.get_xlock_by() != mdr &&
	!dn->lock.can_read(client) &&
	(dnl->is_null() || forward)) {
      dout(10) << "traverse: xlocked dentry at " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req, fin));
      if (mds->logger) mds->logger->inc(l_mds_tlock);
      return 1;
    }
    
    // can we conclude ENOENT?
    if (dnl && dnl->is_null()) {
      if (dn->lock.can_read(client) ||
	  (dn->lock.is_xlocked() && dn->lock.get_xlock_by() == mdr)) {
        dout(10) << "traverse: miss on null+readable dentry " << path[depth] << " " << *dn << dendl;
        return -ENOENT;
      } else {
        dout(10) << "miss on dentry " << *dn << ", can't read due to lock" << dendl;
        dn->lock.add_waiter(SimpleLock::WAIT_RD, _get_waiter(mdr, req, fin));
        return 1;
      }
    }

    if (dnl && !dnl->is_null()) {
      CInode *in = dnl->get_inode();
      
      // do we have inode?
      if (!in) {
        assert(dnl->is_remote());
        // do i have it?
        in = get_inode(dnl->get_remote_ino());
        if (in) {
	  dout(7) << "linking in remote in " << *in << dendl;
	  dn->link_remote(dnl, in);
	} else {
          dout(7) << "remote link to " << dnl->get_remote_ino() << ", which i don't have" << dendl;
	  assert(mdr);  // we shouldn't hit non-primary dentries doing a non-mdr traversal!
          open_remote_ino(dnl->get_remote_ino(), _get_waiter(mdr, req, fin),
			  (null_okay && depth == path.depth() - 1));
	  if (mds->logger) mds->logger->inc(l_mds_trino);
          return 1;
        }        
      }

      // forwarder wants replicas?
#if 0
      if (mdr && mdr->client_request && 
	  mdr->client_request->get_mds_wants_replica_in_dirino()) {
	dout(30) << "traverse: REP is here, " 
		 << mdr->client_request->get_mds_wants_replica_in_dirino() 
		 << " vs " << curdir->dirfrag() << dendl;
	
	if (mdr->client_request->get_mds_wants_replica_in_dirino() == curdir->ino() &&
	    curdir->is_auth() && 
	    curdir->is_rep() &&
	    curdir->is_replica(req->get_source().num()) &&
	    dn->is_auth()
	    ) {
	  assert(req->get_source().is_mds());
	  int from = req->get_source().num();
	  
	  if (dn->is_replica(from)) {
	    dout(15) << "traverse: REP would replicate to mds." << from << ", but already cached_by " 
		     << req->get_source() << " dn " << *dn << dendl; 
	  } else {
	    dout(10) << "traverse: REP replicating to " << req->get_source() << " dn " << *dn << dendl;
	    MDiscoverReply *reply = new MDiscoverReply(curdir->dirfrag());
	    reply->mark_unsolicited();
	    reply->starts_with = MDiscoverReply::DENTRY;
	    replicate_dentry(dn, from, reply->trace);
	    if (dnl->is_primary())
	      replicate_inode(in, from, reply->trace);
	    if (req->get_source() != req->get_orig_source())
	      mds->send_message_mds(reply, req->get_source().num());
	    else mds->send_message(reply->req->get_connnection());
	  }
	}
      }
#endif
      
      // add to trace, continue.
      cur = in;
      touch_inode(cur);
      if (pdnvec)
	pdnvec->push_back(dn);
      if (pin)
	*pin = cur;
      depth++;
      continue;
    }
    

    // MISS.  dentry doesn't exist.
    dout(12) << "traverse: miss on dentry " << path[depth] << " in " << *curdir << dendl;

    if (curdir->is_auth()) {
      // dentry is mine.
      if (curdir->is_complete() || (curdir->has_bloom() &&
          !curdir->is_in_bloom(path[depth]))){
        // file not found
	if (pdnvec) {
	  // instantiate a null dn?
	  if (depth < path.depth()-1){
	    dout(20) << " didn't traverse full path; not returning pdnvec" << dendl;
	    dn = NULL;
	  } else if (dn) {
	    assert(0); // should have fallen out in ->is_null() check above
	  } else if (curdir->is_frozen()) {
	    dout(20) << " not adding null to frozen dir " << dendl;
	  } else if (snapid < CEPH_MAXSNAP) {
	    dout(20) << " not adding null for snapid " << snapid << dendl;
	  } else {
	    // create a null dentry
	    dn = curdir->add_null_dentry(path[depth]);
	    dout(20) << " added null " << *dn << dendl;
	  }
	  if (dn)
	    pdnvec->push_back(dn);
	  else
	    pdnvec->clear();   // do not confuse likes of rdlock_path_pin_ref();
	}
        return -ENOENT;
      } else {
	// directory isn't complete; reload
        dout(7) << "traverse: incomplete dir contents for " << *cur << ", fetching" << dendl;
        touch_inode(cur);
        curdir->fetch(_get_waiter(mdr, req, fin), path[depth]);
	if (mds->logger) mds->logger->inc(l_mds_tdirf);
        return 1;
      }
    } else {
      // dirfrag/dentry is not mine.
      pair<int,int> dauth = curdir->authority();

      if (forward &&
	  snapid && mdr && mdr->client_request &&
	  (int)depth < mdr->client_request->get_num_fwd()) {
	dout(7) << "traverse: snap " << snapid << " and depth " << depth
		<< " < fwd " << mdr->client_request->get_num_fwd()
		<< ", discovering instead of forwarding" << dendl;
	discover = true;
      }

      if ((discover || null_okay)) {
	dout(7) << "traverse: discover from " << path[depth] << " from " << *curdir << dendl;
	discover_path(curdir, snapid, path.postfixpath(depth), _get_waiter(mdr, req, fin),
		      null_okay);
	if (mds->logger) mds->logger->inc(l_mds_tdis);
        return 1;
      } 
      if (forward) {
        // forward
        dout(7) << "traverse: not auth for " << path << " in " << *curdir << dendl;
	
	if (curdir->is_ambiguous_auth()) {
	  // wait
	  dout(7) << "traverse: waiting for single auth in " << *curdir << dendl;
	  curdir->add_waiter(CDir::WAIT_SINGLEAUTH, _get_waiter(mdr, req, fin));
	  return 1;
	} 

	dout(7) << "traverse: forwarding, not auth for " << *curdir << dendl;
	
#if 0
	// request replication?
	if (mdr && mdr->client_request && curdir->is_rep()) {
	  dout(15) << "traverse: REP fw to mds." << dauth << ", requesting rep under "
		   << *curdir << " req " << *(MClientRequest*)req << dendl;
	  mdr->client_request->set_mds_wants_replica_in_dirino(curdir->ino());
	  req->clear_payload();  // reencode!
	}
#endif
	
	if (mdr) 
	  request_forward(mdr, dauth.first);
	else
	  mds->forward_message_mds(req, dauth.first);
	
	if (mds->logger) mds->logger->inc(l_mds_tfw);
	return 2;
      }    
    }
    
    assert(0);  // i shouldn't get here
  }
  
  // success.
  if (mds->logger) mds->logger->inc(l_mds_thit);
  dout(10) << "path_traverse finish on snapid " << snapid << dendl;
  if (mdr) 
    assert(mdr->snapid == snapid);
  return 0;
}

/**
 * Find out if the MDS is auth for a given path.
 *
 * Returns true if:
 * 1) The full path DNE and we are auth for the deepest existing piece
 * 2) We are auth for the inode linked to by the last dentry.
 */
bool MDCache::path_is_mine(filepath& path)
{
  dout(15) << "path_is_mine " << path.get_ino() << " " << path << dendl;
  
  CInode *cur = get_inode(path.get_ino());
  if (!cur)
    return false;  // who knows!

  for (unsigned i=0; i<path.depth(); i++) {
    dout(15) << "path_is_mine seg " << i << ": " << path[i] << " under " << *cur << dendl;
    __u32 dnhash = cur->hash_dentry_name(path[i]);
    stripeid_t stripeid = cur->pick_stripe(dnhash);
    CStripe *stripe = cur->get_stripe(stripeid);
    if (!stripe)
      return cur->is_auth();

    frag_t fg = stripe->pick_dirfrag(dnhash);
    CDir *dir = stripe->get_dirfrag(fg);
    if (!dir)
      return stripe->is_auth();
    CDentry *dn = dir->lookup(path[i]);
    CDentry::linkage_t *dnl = dn->get_linkage();
    if (!dn || dnl->is_null())
      return dir->is_auth();
    if (!dnl->is_primary())
      return false;
    cur = dnl->get_inode();
  }

  return cur->is_auth();
}

CInode *MDCache::cache_traverse(const filepath& fp)
{
  dout(10) << "cache_traverse " << fp << dendl;

  CInode *in;
  if (fp.get_ino())
    in = get_inode(fp.get_ino());
  else
    in = root;
  if (!in)
    return NULL;

  for (unsigned i = 0; i < fp.depth(); i++) {
    const string& dname = fp[i];
    __u32 dnhash = in->hash_dentry_name(dname);
    stripeid_t stripeid = in->pick_stripe(dnhash);
    CStripe *stripe = in->get_stripe(stripeid);
    if (!stripe)
      return NULL;

    frag_t fg = stripe->pick_dirfrag(dname);
    dout(20) << " " << i << " " << dname << " frag " << fg << " from " << *in << dendl;
    CDir *curdir = stripe->get_dirfrag(fg);
    if (!curdir)
      return NULL;
    CDentry *dn = curdir->lookup(dname, CEPH_NOSNAP);
    if (!dn)
      return NULL;
    in = dn->get_linkage()->get_inode();
    if (!in)
      return NULL;
  }
  dout(10) << " got " << *in << dendl;
  return in;
}


/**
 * open_remote_dirstripe -- open up a remote dirfrag
 *
 * @param diri base inode
 * @param stripeid stripe id
 * @param fin completion callback
 */
void MDCache::open_remote_dirstripe(CInode *diri, stripeid_t stripeid, Context *fin)
{
  dout(10) << "open_remote_dirstripe on " << *diri << dendl;

  assert(diri->is_dir());
  assert(!diri->is_auth());
  assert(diri->get_stripe(stripeid) == 0);

  int auth = diri->authority().first;

  if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
    discover_dir_stripe(diri, stripeid, fin);
  } else {
    // mds is down or recovering.  forge a replica!
    forge_replica_stripe(diri, stripeid, auth);
    if (fin)
      mds->queue_waiter(fin);
  }
}

/**
 * open_remote_dirfrag -- open up a remote dirfrag
 *
 * @param stripe base dir stripe
 * @param approxfg approximate fragment.
 * @param fin completion callback
 */
void MDCache::open_remote_dirfrag(CStripe *stripe, frag_t approxfg, Context *fin) 
{
  dout(10) << "open_remote_dirfrag on " << *stripe << dendl;

  assert(!stripe->is_auth());
  assert(stripe->get_dirfrag(approxfg) == 0);

  int auth = stripe->authority().first;

  if (mds->mdsmap->get_state(auth) >= MDSMap::STATE_REJOIN) {
    discover_dir_frag(stripe, approxfg, fin);
  } else {
    // mds is down or recovering.  forge a replica!
    forge_replica_dir(stripe, approxfg, auth);
    if (fin)
      mds->queue_waiter(fin);
  }
}


/** 
 * get_dentry_inode - get or open inode
 *
 * @param dn the dentry
 * @param mdr current request
 *
 * will return inode for primary, or link up/open up remote link's inode as necessary.
 * If it's not available right now, puts mdr on wait list and returns null.
 */
CInode *MDCache::get_dentry_inode(CDentry *dn, MDRequest *mdr, bool projected)
{
  CDentry::linkage_t *dnl;
  if (projected)
    dnl = dn->get_projected_linkage();
  else
    dnl = dn->get_linkage();

  assert(!dnl->is_null());
  
  if (dnl->is_primary())
    return dnl->inode;

  assert(dnl->is_remote());
  CInode *in = get_inode(dnl->get_remote_ino());
  if (in) {
    dout(7) << "get_dentry_inode linking in remote in " << *in << dendl;
    dn->link_remote(dnl, in);
    return in;
  } else {
    dout(10) << "get_dentry_inode on remote dn, opening inode for " << *dn << dendl;
    open_remote_ino(dnl->remote_ino, new C_MDS_RetryRequest(this, mdr));
    return 0;
  }
}

class C_MDC_RetryOpenRemoteIno : public Context {
  MDCache *mdcache;
  inodeno_t ino;
  bool want_xlocked;
  Context *onfinish;
public:
  C_MDC_RetryOpenRemoteIno(MDCache *mdc, inodeno_t i, Context *c, bool wx) :
    mdcache(mdc), ino(i), want_xlocked(wx), onfinish(c) {}
  void finish(int r) {
    if (mdcache->get_inode(ino)) {
      onfinish->finish(0);
      delete onfinish;
    } else
      mdcache->open_remote_ino(ino, onfinish, want_xlocked);
  }
};

void MDCache::open_remote_ino(inodeno_t ino, Context *onfinish, bool want_xlocked)
{
  dout(7) << "open_remote_ino on " << ino << (want_xlocked ? " want_xlocked":"") << dendl;

  // discover the inode from the inode container
  CInode *base = get_container()->get_inode();
  char dname[20];
  snprintf(dname, sizeof(dname), "%llx", (unsigned long long)ino.val);
  filepath path(dname, base->ino());

  discover_path(base, CEPH_NOSNAP, path, onfinish, want_xlocked);
}


struct C_MDC_OpenRemoteDentry : public Context {
  MDCache *mdc;
  CDentry *dn;
  bool projected;
  Context *onfinish;
  C_MDC_OpenRemoteDentry(MDCache *m, CDentry *d, bool p, Context *f) :
    mdc(m), dn(d), projected(p), onfinish(f) {}
  void finish(int r) {
    mdc->_open_remote_dentry_finish(r, dn, projected, onfinish);
  }
};

void MDCache::open_remote_dentry(CDentry *dn, bool projected, Context *fin)
{
  dout(10) << "open_remote_dentry " << *dn << dendl;
  CDentry::linkage_t *dnl = projected ? dn->get_projected_linkage() : dn->get_linkage();
  open_remote_ino(dnl->get_remote_ino(), 
		  new C_MDC_OpenRemoteDentry(this, dn, projected, fin));
}

void MDCache::_open_remote_dentry_finish(int r, CDentry *dn, bool projected, Context *fin)
{
  if (r == -ENOENT) {
    dout(0) << "open_remote_dentry_finish bad remote dentry " << *dn << dendl;
    dn->state_set(CDentry::STATE_BADREMOTEINO);
  } else if (r != 0)
    assert(0);
  fin->finish(r);
  delete fin;
}



void MDCache::make_trace(vector<CDentry*>& trace, CInode *in)
{
  // empty trace if we're a base inode
  if (in->is_base())
    return;

  CInode *parent = in->get_parent_inode();
  assert(parent);
  make_trace(trace, parent);

  CDentry *dn = in->get_parent_dn();
  dout(15) << "make_trace adding " << *dn << dendl;
  trace.push_back(dn);
}


/* ---------------------------- */

/*
 * search for a given inode on MDS peers.  optionally start with the given node.


 TODO 
  - recover from mds node failure, recovery
  - traverse path

 */
void MDCache::find_ino_peers(inodeno_t ino, Context *c, int hint)
{
  dout(5) << "find_ino_peers " << ino << " hint " << hint << dendl;
  assert(!have_inode(ino));
  
  tid_t tid = ++find_ino_peer_last_tid;
  find_ino_peer_info_t& fip = find_ino_peer[tid];
  fip.ino = ino;
  fip.tid = tid;
  fip.fin = c;
  fip.hint = hint;
  fip.checked.insert(mds->whoami);
  _do_find_ino_peer(fip);
}

void MDCache::_do_find_ino_peer(find_ino_peer_info_t& fip)
{
  set<int> all, active;
  mds->mdsmap->get_mds_set(all);
  mds->mdsmap->get_active_mds_set(active);
  mds->mdsmap->get_mds_set(active, MDSMap::STATE_STOPPING);

  dout(10) << "_do_find_ino_peer " << fip.tid << " " << fip.ino
	   << " active " << active << " all " << all
	   << " checked " << fip.checked
	   << dendl;
    
  int m = -1;
  if (fip.hint >= 0) {
    m = fip.hint;
    fip.hint = -1;
  } else {
    for (set<int>::iterator p = active.begin(); p != active.end(); p++)
      if (*p != mds->whoami &&
	  fip.checked.count(*p) == 0) {
	m = *p;
	break;
      }
  }
  if (m < 0) {
    if (all.size() > active.size()) {
      dout(10) << "_do_find_ino_peer waiting for more peers to be active" << dendl;
    } else {
      dout(10) << "_do_find_ino_peer failed on " << fip.ino << dendl;
      fip.fin->finish(-ESTALE);
      delete fip.fin;
      find_ino_peer.erase(fip.tid);
    }
  } else {
    fip.checking = m;
    mds->send_message_mds(new MMDSFindIno(fip.tid, fip.ino), m);
  }
}

void MDCache::handle_find_ino(MMDSFindIno *m)
{
  dout(10) << "handle_find_ino " << *m << dendl;
  MMDSFindInoReply *r = new MMDSFindInoReply(m->tid);
  CInode *in = get_inode(m->ino);
  if (in) {
    in->make_path(r->path);
    dout(10) << " have " << r->path << " " << *in << dendl;
  }
  mds->messenger->send_message(r, m->get_connection());
  m->put();
}


void MDCache::handle_find_ino_reply(MMDSFindInoReply *m)
{
  map<tid_t, find_ino_peer_info_t>::iterator p = find_ino_peer.find(m->tid);
  if (p != find_ino_peer.end()) {
    dout(10) << "handle_find_ino_reply " << *m << dendl;
    find_ino_peer_info_t& fip = p->second;

    // success?
    if (get_inode(fip.ino)) {
      dout(10) << "handle_find_ino_reply successfully found " << fip.ino << dendl;
      mds->queue_waiter(fip.fin);
      find_ino_peer.erase(p);
      m->put();
      return;
    }

    int from = m->get_source().num();
    if (fip.checking == from)
      fip.checking = -1;
    fip.checked.insert(from);

    if (!m->path.empty()) {
      // we got a path!
      vector<CDentry*> trace;
      int r = path_traverse(NULL, m, NULL, m->path, &trace, NULL, MDS_TRAVERSE_DISCOVER);
      if (r > 0)
	return; 
      dout(0) << "handle_find_ino_reply failed with " << r << " on " << m->path 
	      << ", retrying" << dendl;
      fip.checked.clear();
      _do_find_ino_peer(fip);
    } else {
      // nope, continue.
      _do_find_ino_peer(fip);
    }      
  } else {
    dout(10) << "handle_find_ino_reply tid " << m->tid << " dne" << dendl;
  }  
  m->put();
}

void MDCache::kick_find_ino_peers(int who)
{
  // find_ino_peers requests we should move on from
  for (map<tid_t,find_ino_peer_info_t>::iterator p = find_ino_peer.begin();
       p != find_ino_peer.end();
       ++p) {
    find_ino_peer_info_t& fip = p->second;
    if (fip.checking == who) {
      dout(10) << "kicking find_ino_peer " << fip.tid << " who was checking mds." << who << dendl;
      fip.checking = -1;
      _do_find_ino_peer(fip);
    } else if (fip.checking == -1) {
      dout(10) << "kicking find_ino_peer " << fip.tid << " who was waiting" << dendl;
      _do_find_ino_peer(fip);
    }
  }
}

/* ---------------------------- */

struct C_MDS_FindInoDir : public Context {
  MDCache *mdcache;
  inodeno_t ino;
  Context *fin;
  bufferlist bl;
  C_MDS_FindInoDir(MDCache *m, inodeno_t i, Context *f) : mdcache(m), ino(i), fin(f) {}
  void finish(int r) {
    mdcache->_find_ino_dir(ino, fin, bl, r);
  }
};

void MDCache::find_ino_dir(inodeno_t ino, Context *fin)
{
  dout(10) << "find_ino_dir " << ino << dendl;
  assert(!have_inode(ino));

  // get the backtrace from the dir
  object_t oid = CInode::get_object_name(ino, frag_t(), "");
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  
  C_MDS_FindInoDir *c = new C_MDS_FindInoDir(this, ino, fin);
  mds->objecter->getxattr(oid, oloc, "path", CEPH_NOSNAP, &c->bl, 0, c);
}

void MDCache::_find_ino_dir(inodeno_t ino, Context *fin, bufferlist& bl, int r)
{
  dout(10) << "_find_ino_dir " << ino << " got " << r << " " << bl.length() << " bytes" << dendl;
  if (r < 0) {
    fin->finish(r);
    delete fin;
    return;
  }

  string s(bl.c_str(), bl.length());
  filepath path(s.c_str());
  vector<CDentry*> trace;

  dout(10) << "_find_ino_dir traversing to path " << path << dendl;

  C_MDS_FindInoDir *c = new C_MDS_FindInoDir(this, ino, fin);
  c->bl = bl;
  r = path_traverse(NULL, NULL, c, path, &trace, NULL, MDS_TRAVERSE_DISCOVER);
  if (r > 0)
    return; 
  delete c;  // path_traverse doesn't clean it up for us.
  
  fin->finish(r);
  delete fin;
}


/* ---------------------------- */

/* This function takes over the reference to the passed Message */
MDRequest *MDCache::request_start(MClientRequest *req)
{
  // did we win a forward race against a slave?
  if (active_requests.count(req->get_reqid())) {
    MDRequest *mdr = active_requests[req->get_reqid()];
    if (mdr->is_slave()) {
      dout(10) << "request_start already had " << *mdr << ", forward new msg" << dendl;
      mds->forward_message_mds(req, mdr->slave_to_mds);
    } else {
      dout(10) << "request_start already processing " << *mdr << ", dropping new msg" << dendl;
      req->put();
    }
    return 0;
  }

  // register new client request
  MDRequest *mdr = new MDRequest(req->get_reqid(), req->get_num_fwd(), req);
  active_requests[req->get_reqid()] = mdr;
  dout(7) << "request_start " << *mdr << dendl;
  return mdr;
}

MDRequest *MDCache::request_start_slave(metareqid_t ri, __u32 attempt, int by)
{
  MDRequest *mdr = new MDRequest(ri, attempt, by);
  assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_slave " << *mdr << " by mds." << by << dendl;
  return mdr;
}

MDRequest *MDCache::request_start_internal(int op)
{
  MDRequest *mdr = new MDRequest;
  mdr->reqid.name = entity_name_t::MDS(mds->get_nodeid());
  mdr->reqid.tid = mds->issue_tid();
  mdr->internal_op = op;

  assert(active_requests.count(mdr->reqid) == 0);
  active_requests[mdr->reqid] = mdr;
  dout(7) << "request_start_internal " << *mdr << " op " << op << dendl;
  return mdr;
}


MDRequest *MDCache::request_get(metareqid_t rid)
{
  assert(active_requests.count(rid));
  dout(7) << "request_get " << rid << " " << *active_requests[rid] << dendl;
  return active_requests[rid];
}

void MDCache::request_finish(MDRequest *mdr)
{
  dout(7) << "request_finish " << *mdr << dendl;

  // slave finisher?
  if (mdr->more()->slave_commit) {
    Context *fin = mdr->more()->slave_commit;
    mdr->more()->slave_commit = 0;
    fin->finish(0);   // this must re-call request_finish.
    delete fin;
    return; 
  }

  request_cleanup(mdr);
}


void MDCache::request_forward(MDRequest *mdr, int who, int port)
{
  if (mdr->client_request->get_source().is_client()) {
    dout(7) << "request_forward " << *mdr << " to mds." << who << " req "
            << *mdr->client_request << dendl;
    mds->forward_message_mds(mdr->client_request, who);
    mds->server->rollback_allocated_inos(mdr);
    mdr->client_request = 0;
    if (mds->logger) mds->logger->inc(l_mds_fw);
  } else {
    dout(7) << "request_forward drop " << *mdr << " req " << *mdr->client_request
            << " was from mds" << dendl;
  }
  request_cleanup(mdr);
}


void MDCache::dispatch_request(MDRequest *mdr)
{
  if (mdr->killed) {
    dout(10) << "request " << *mdr << " was killed" << dendl;
    return;
  }
  if (mdr->client_request) {
    mds->server->dispatch_client_request(mdr);
  } else if (mdr->slave_request) {
    mds->server->dispatch_slave_request(mdr);
  } else {
    switch (mdr->internal_op) {
      
      // ...
      
    default:
      assert(0);
    }
  }
}


void MDCache::request_drop_foreign_locks(MDRequest *mdr)
{
  // clean up slaves
  //  (will implicitly drop remote dn pins)
  for (set<int>::iterator p = mdr->more()->slaves.begin();
       p != mdr->more()->slaves.end();
       ++p) {
    MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
					       MMDSSlaveRequest::OP_FINISH);
    mds->send_message_mds(r, *p);
  }

  /* strip foreign xlocks out of lock lists, since the OP_FINISH drops them
   * implicitly. Note that we don't call the finishers -- there shouldn't
   * be any on a remote lock and the request finish wakes up all
   * the waiters anyway! */
  set<SimpleLock*>::iterator p = mdr->xlocks.begin();
  while (p != mdr->xlocks.end()) {
    if ((*p)->get_parent()->is_auth()) 
      p++;
    else {
      dout(10) << "request_drop_foreign_locks forgetting lock " << **p
	       << " on " << *(*p)->get_parent() << dendl;
      (*p)->put_xlock();
      mdr->locks.erase(*p);
      mdr->xlocks.erase(p++);
    }
  }

  map<SimpleLock*, int>::iterator q = mdr->remote_wrlocks.begin();
  while (q != mdr->remote_wrlocks.end()) {
    dout(10) << "request_drop_foreign_locks forgetting remote_wrlock " << *q->first
	     << " on mds." << q->second
	     << " on " << *(q->first)->get_parent() << dendl;
    mdr->locks.erase(q->first);
    mdr->remote_wrlocks.erase(q++);
  }

  mdr->more()->slaves.clear(); /* we no longer have requests out to them, and
                                * leaving them in can cause double-notifies as
                                * this function can get called more than once */
}

void MDCache::request_drop_non_rdlocks(MDRequest *mdr)
{
  request_drop_foreign_locks(mdr);
  mds->locker->drop_non_rdlocks(mdr);
}

void MDCache::request_drop_locks(MDRequest *mdr)
{
  request_drop_foreign_locks(mdr);
  mds->locker->drop_locks(mdr);
}

void MDCache::request_cleanup(MDRequest *mdr)
{
  dout(15) << "request_cleanup " << *mdr << dendl;

  if (mdr->has_more() && mdr->more()->is_ambiguous_auth)
    mdr->clear_ambiguous_auth();

  request_drop_locks(mdr);

  // drop (local) auth pins
  mdr->drop_local_auth_pins();

  // drop stickystripes
  for (set<CInode*>::iterator p = mdr->stickystripes.begin();
       p != mdr->stickystripes.end();
       ++p) 
    (*p)->put_stickystripes();

  mds->locker->kick_cap_releases(mdr);

  // drop cache pins
  mdr->drop_pins();

  // remove from session
  mdr->item_session_request.remove_myself();

  bool was_replay = mdr->client_request && mdr->client_request->is_replay();

  // remove from map
  active_requests.erase(mdr->reqid);
  mdr->put();

  // fail-safe!
  if (was_replay && active_requests.empty()) {
    dout(10) << " fail-safe queueing next replay op" << dendl;
    mds->queue_one_replay();
  }

  if (mds->logger)
    log_stat();
}

void MDCache::request_kill(MDRequest *mdr)
{
  mdr->killed = true;
  if (!mdr->committing) {
    dout(10) << "request_kill " << *mdr << dendl;
    request_cleanup(mdr);
  } else {
    dout(10) << "request_kill " << *mdr << " -- already committing, no-op" << dendl;
  }
}


// -------------------------------------------------------------------------------
// STRAYS

void MDCache::scan_stray_dir()
{
  dout(10) << "scan_stray_dir" << dendl;
  
  list<CStripe*> stripes;
  for (int i = 0; i < NUM_STRAY; ++i)
    if (strays[i])
      strays[i]->get_stripes(stripes);

  for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); s++) {
    CStripe *stripe = *s;
    list<CDir*> dirs;
    stripe->get_dirfrags(dirs);
    for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
      CDir *dir = *p;
      for (CDir::map_t::iterator q = dir->items.begin(); q != dir->items.end(); q++) {
        CDentry *dn = q->second;
        CDentry::linkage_t *dnl = dn->get_projected_linkage();
        if (dnl->is_primary())
          maybe_eval_stray(dnl->get_inode());
      }
    }
  }
}

struct C_MDC_EvalStray : public Context {
  MDCache *mdcache;
  CDentry *dn;
  C_MDC_EvalStray(MDCache *c, CDentry *d) : mdcache(c), dn(d) {}
  void finish(int r) {
    mdcache->eval_stray(dn);
  }
};

void MDCache::eval_stray(CDentry *dn)
{
  dout(10) << "eval_stray " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  dout(10) << " inode is " << *dnl->get_inode() << dendl;
  assert(dnl->is_primary());
  CInode *in = dnl->get_inode();
  assert(in);

  assert(dn->get_dir()->get_inode()->is_stray());

  if (!dn->is_auth()) {
    // has to be mine
    // move to bottom of lru so that we trim quickly!
    touch_dentry_bottom(dn);
    return;
  }

  // purge?
  if (in->inode.nlink == 0) {
    if (dn->is_replicated()) {
      dout(20) << " replicated" << dendl;
      return;
    }
    if (dn->is_any_leases() || in->is_any_caps()) {
      dout(20) << " caps | leases" << dendl;
      return;  // wait
    }
    if (!in->has_open_stripes()) {
      dout(20) << " open stripes" << dendl;
      return;  // wait for dirs to close/trim
    }
    if (dn->state_test(CDentry::STATE_PURGING)) {
      dout(20) << " already purging" << dendl;
      return;  // already purging
    }
    if (in->state_test(CInode::STATE_NEEDSRECOVER) ||
	in->state_test(CInode::STATE_RECOVERING)) {
      dout(20) << " pending recovery" << dendl;
      return;  // don't mess with file size probing
    }
    if (in->get_num_ref() > (int)in->is_dirty()) {
      dout(20) << " too many inode refs" << dendl;
      return;
    }
    if (dn->get_num_ref() > (int)dn->is_dirty() + !!in->get_num_ref()) {
      dout(20) << " too many dn refs" << dendl;
      return;
    }
    purge_stray(dn);
  }
  else if (in->inode.nlink == 1) {
    // trivial reintegrate?
    if (!in->remote_parents.empty()) {
      CDentry *rlink = *in->remote_parents.begin();
      
      // don't do anything if the remote parent is projected, or we may
      // break user-visible semantics!
      // NOTE: we repeat this check in _rename(), since our submission path is racey.
      if (!rlink->is_projected()) {
	if (rlink->is_auth() && rlink->dir->can_auth_pin())
	  reintegrate_stray(dn, rlink);
	
	if (!rlink->is_auth() && dn->is_auth())
	  migrate_stray(dn, rlink->authority().first);
      }
    }
  } else {
    // wait for next use.
  }
}

void MDCache::eval_remote(CDentry *dn)
{
  dout(10) << "eval_remote " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  assert(dnl->is_remote());
  CInode *in = dnl->get_inode();
  if (!in) return;

  // refers to stray?
  if (in->get_parent_dn()->get_dir()->get_inode()->is_stray()) {
    if (in->is_auth())
      eval_stray(in->get_parent_dn());
    else
      migrate_stray(in->get_parent_dn(), mds->get_nodeid());
  }
}

class C_MDC_PurgeStrayPurged : public Context {
  MDCache *cache;
  CDentry *dn;
public:
  C_MDC_PurgeStrayPurged(MDCache *c, CDentry *d) : 
    cache(c), dn(d) { }
  void finish(int r) {
    cache->_purge_stray_purged(dn, r);
  }
};

void MDCache::purge_stray(CDentry *dn)
{
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();
  dout(10) << "purge_stray " << *dn << " " << *in << dendl;
  assert(!dn->is_replicated());

  dn->state_set(CDentry::STATE_PURGING);
  dn->get(CDentry::PIN_PURGING);
  in->state_set(CInode::STATE_PURGING);

  
  // CHEAT.  there's no real need to journal our intent to purge, since
  // that is implicit in the dentry's presence and non-use in the stray
  // dir.  on recovery, we'll need to re-eval all strays anyway.
  
  SnapRealm *realm = get_snaprealm();
  SnapContext nullsnap;
  const SnapContext *snapc;
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnap;
    assert(in->last == CEPH_NOSNAP);
  }

  if (in->is_dir()) {
    dout(10) << "purge_stray dir ... implement me!" << dendl;  // FIXME XXX
    _purge_stray_purged(dn);
  } else if (in->is_file()) {
    uint64_t period = (uint64_t)in->inode.layout.fl_object_size * (uint64_t)in->inode.layout.fl_stripe_count;
    uint64_t cur_max_size = in->inode.get_max_size();
    uint64_t to = MAX(in->inode.size, cur_max_size);
    if (to && period) {
      uint64_t num = (to + period - 1) / period;
      dout(10) << "purge_stray 0~" << to << " objects 0~" << num << " snapc " << snapc << " on " << *in << dendl;
      mds->filer->purge_range(in->inode.ino, &in->inode.layout, *snapc,
			      0, num, ceph_clock_now(g_ceph_context), 0,
			      new C_MDC_PurgeStrayPurged(this, dn));
    } else {
      dout(10) << "purge_stray 0 objects snapc " << snapc << " on " << *in << dendl;
      _purge_stray_purged(dn);
    }
  } else {
    // not a dir or file; purged!
    _purge_stray_purged(dn);
  }
}

class C_MDC_PurgeStrayLogged : public Context {
  MDCache *cache;
  CDentry *dn;
  LogSegment *ls;
public:
  C_MDC_PurgeStrayLogged(MDCache *c, CDentry *d, LogSegment *s)
      : cache(c), dn(d), ls(s) { }
  void finish(int r) {
    cache->_purge_stray_logged(dn, ls);
  }
};
class C_MDC_PurgeStrayLoggedTruncate : public Context {
  MDCache *cache;
  CDentry *dn;
  LogSegment *ls;
public:
  C_MDC_PurgeStrayLoggedTruncate(MDCache *c, CDentry *d, LogSegment *s) : 
    cache(c), dn(d), ls(s) { }
  void finish(int r) {
    cache->_purge_stray_logged_truncate(dn, ls);
  }
};

void MDCache::_purge_stray_purged(CDentry *dn, int r)
{
  assert (r == 0 || r == -ENOENT);
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_purged " << *dn << " " << *in << dendl;

  if (in->get_num_ref() == (int)in->is_dirty() &&
      dn->get_num_ref() == (int)dn->is_dirty() + !!in->get_num_ref() + 1/*PIN_PURGING*/) {
    // kill dentry.
    dn->push_projected_linkage(); // NULL

    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray");
    mds->mdlog->start_entry(le);

    // update dirfrag fragstat, rstat
    CStripe *stripe = dn->get_stripe();
    fnode_t *pf = stripe->project_fnode();
    if (in->is_dir())
      pf->fragstat.nsubdirs--;
    else
      pf->fragstat.nfiles--;
    pf->rstat.sub(in->inode.accounted_rstat);

    le->metablob.add_stripe_context(stripe);
    le->metablob.add_dentry(dn, true);
    le->metablob.add_destroyed_inode(in->ino());

    mds->mdlog->submit_entry(le, new C_MDC_PurgeStrayLogged(this, dn, mds->mdlog->get_current_segment()));
  } else {
    // new refs.. just truncate to 0
    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray truncate");
    mds->mdlog->start_entry(le);
    
    inode_t *pi = in->project_inode();
    pi->size = 0;
    pi->client_ranges.clear();
    pi->truncate_size = 0;
    pi->truncate_from = 0;

    le->metablob.add_stripe_context(dn->get_stripe());
    le->metablob.add_inode(in, true);

    mds->mdlog->submit_entry(le, new C_MDC_PurgeStrayLoggedTruncate(this, dn, mds->mdlog->get_current_segment()));
  }
}

void MDCache::_purge_stray_logged(CDentry *dn, LogSegment *ls)
{
  CInode *in = dn->get_linkage()->get_inode();
  dout(10) << "_purge_stray_logged " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  assert(!in->state_test(CInode::STATE_RECOVERING));

  // unlink
  CDir *dir = dn->get_dir();
  assert(dn->get_projected_linkage()->is_null());
  dir->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(ls);

  dir->get_stripe()->pop_and_dirty_projected_fnode(ls);

  // drop inode
  if (in->is_dirty())
    in->mark_clean();
  remove_inode(in);

  // drop dentry?
  if (dn->is_new()) {
    dout(20) << " dn is new, removing" << dendl;
    dn->mark_clean();
    dir->remove_dentry(dn);
  } else
    touch_dentry_bottom(dn);  // drop dn as quickly as possible.
}

void MDCache::_purge_stray_logged_truncate(CDentry *dn, LogSegment *ls)
{
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_logged_truncate " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  in->pop_and_dirty_projected_inode(ls);

  eval_stray(dn);
}



void MDCache::reintegrate_stray(CDentry *straydn, CDentry *rdn)
{
  dout(10) << "reintegrate_stray " << *straydn << " into " << *rdn << dendl;
  
  // rename it to another mds.
  filepath src;
  straydn->make_path(src);
  filepath dst;
  rdn->make_path(dst);

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, rdn->authority().first);
}
 

void MDCache::migrate_stray(CDentry *dn, int to)
{
  CInode *in = dn->get_linkage()->get_inode();
  assert(in);
  CInode *diri = dn->dir->get_inode();
  assert(diri->is_stray());
  dout(10) << "migrate_stray from mds." << MDS_INO_STRAY_OWNER(diri->inode.ino)
	   << " to mds." << to
	   << " " << *dn << " " << *in << dendl;

  // rename it to another mds.
  filepath src;
  dn->make_path(src);

  string dname;
  in->name_stray_dentry(dname);
  filepath dst(dname, MDS_INO_STRAY(to, 0));

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, to);
}




// ========================================================================================
// DISCOVER
/*

  - for all discovers (except base_inos, e.g. root, stray), waiters are attached
  to the parent metadata object in the cache (pinning it).

  - all discovers are tracked by tid, so that we can ignore potentially dup replies.

*/

void MDCache::_send_discover(discover_info_t& d)
{
  MDiscover *dis = new MDiscover(d.base, d.snap, d.want_path, d.want_ino,
                                 d.want_base_stripe, d.want_xlocked);
  dis->set_tid(d.tid);
  mds->send_message_mds(dis, d.mds);
}

void MDCache::discover_base_ino(inodeno_t want_ino,
				Context *onfinish,
				int from) 
{
  dout(7) << "discover_base_ino " << want_ino << " from mds." << from << dendl;
  if (waiting_for_base_ino[from].count(want_ino) == 0) {
    discover_info_t& d = _create_discover(from);
    d.base.stripe.ino = want_ino;
    _send_discover(d);
  }
  waiting_for_base_ino[from][want_ino].push_back(onfinish);
}


void MDCache::discover_dir_stripe(CInode *base, stripeid_t stripeid,
                                  Context *onfinish, int from)
{
  if (from < 0)
    from = base->authority().first;

  dout(7) << "discover_dir_stripe " << dirstripe_t(base->ino(), stripeid)
      << " under " << *base << " from mds." << from << dendl;

  if (!base->is_waiting_for_stripe(stripeid) || !onfinish) {  // FIXME: this is kind of weak!
    discover_info_t& d = _create_discover(from);
    d.base.stripe.ino = base->ino();
    d.base.stripe.stripeid = stripeid;
    d.want_base_stripe = true;
    _send_discover(d);
  }

  if (onfinish)
    base->add_stripe_waiter(stripeid, onfinish);
}

void MDCache::discover_dir_frag(CStripe *base,
				frag_t approx_fg,
				Context *onfinish,
				int from)
{
  if (from < 0)
    from = base->authority().first;

  dirfrag_t df(base->dirstripe(), approx_fg);
  dout(7) << "discover_dir_frag " << df
	  << " from mds." << from << dendl;

  if (!base->is_waiter_for(CStripe::WAIT_DIR) || !onfinish) {  // FIXME: this is kind of weak!
    discover_info_t& d = _create_discover(from);
    d.base.stripe = base->dirstripe();
    d.base.frag = approx_fg;
    d.want_base_stripe = true;
    _send_discover(d);
  }

  if (onfinish) 
    base->add_waiter(CStripe::WAIT_DIR, onfinish);
}

struct C_MDC_RetryDiscoverPath : public Context {
  MDCache *mdc;
  CInode *base;
  snapid_t snapid;
  filepath path;
  int from;
  C_MDC_RetryDiscoverPath(MDCache *c, CInode *b, snapid_t s, filepath &p, int f) :
    mdc(c), base(b), snapid(s), path(p), from(f)  {}
  void finish(int r) {
    mdc->discover_path(base, snapid, path, 0, from);
  }
};

void MDCache::discover_path(CInode *base,
			    snapid_t snap,
			    filepath want_path,
			    Context *onfinish,
			    bool want_xlocked,
			    int from)
{
  __u32 dnhash = base->hash_dentry_name(want_path[0]);
  dirstripe_t ds(base->ino(), dnhash % base->get_stripe_count());

  if (from < 0)
    from = base->get_stripe_auth(ds.stripeid);

  dout(7) << "discover_path " << ds << " " << want_path << " snap " << snap << " from mds." << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(10) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverPath(this, base, snap, want_path, from);
    base->add_waiter(CInode::WAIT_SINGLEAUTH, onfinish);
    return;
  } 

  if ((want_xlocked && want_path.depth() == 1) ||
      !base->is_waiting_for_stripe(ds.stripeid) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.base.stripe = ds;
    d.snap = snap;
    d.want_path = want_path;
    d.want_base_stripe = true;
    d.want_xlocked = want_xlocked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_stripe_waiter(ds.stripeid, onfinish);
}

struct C_MDC_RetryDiscoverPath2 : public Context {
  MDCache *mdc;
  CDir *base;
  snapid_t snapid;
  filepath path;
  C_MDC_RetryDiscoverPath2(MDCache *c, CDir *b, snapid_t s, filepath &p) :
    mdc(c), base(b), snapid(s), path(p) {}
  void finish(int r) {
    mdc->discover_path(base, snapid, path, 0);
  }
};

void MDCache::discover_path(CDir *base,
			    snapid_t snap,
			    filepath want_path,
			    Context *onfinish,
			    bool want_xlocked)
{
  int from = base->authority().first;

  dout(7) << "discover_path " << base->dirfrag() << " " << want_path << " snap " << snap << " from mds." << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;

  if (base->is_ambiguous_auth()) {
    dout(7) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverPath2(this, base, snap, want_path);
    base->add_waiter(CDir::WAIT_SINGLEAUTH, onfinish);
    return;
  }

  if ((want_xlocked && want_path.depth() == 1) ||
      !base->is_waiting_for_dentry(want_path[0].c_str(), snap) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.base = base->dirfrag();
    d.snap = snap;
    d.want_path = want_path;
    d.want_base_stripe = false;
    d.want_xlocked = want_xlocked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_dentry_waiter(want_path[0], snap, onfinish);
}

struct C_MDC_RetryDiscoverIno : public Context {
  MDCache *mdc;
  CDir *base;
  inodeno_t want_ino;
  C_MDC_RetryDiscoverIno(MDCache *c, CDir *b, inodeno_t i) :
    mdc(c), base(b), want_ino(i) {}
  void finish(int r) {
    mdc->discover_ino(base, want_ino, 0);
  }
};

void MDCache::discover_ino(CDir *base,
			   inodeno_t want_ino,
			   Context *onfinish,
			   bool want_xlocked)
{
  int from = base->authority().first;

  dout(7) << "discover_ino " << base->dirfrag() << " " << want_ino << " from mds." << from
	  << (want_xlocked ? " want_xlocked":"")
	  << dendl;
  
  if (base->is_ambiguous_auth()) {
    dout(10) << " waiting for single auth on " << *base << dendl;
    if (!onfinish)
      onfinish = new C_MDC_RetryDiscoverIno(this, base, want_ino);
    base->add_waiter(CDir::WAIT_SINGLEAUTH, onfinish);
    return;
  } 

  if (want_xlocked || !base->is_waiting_for_ino(want_ino) || !onfinish) {
    discover_info_t& d = _create_discover(from);
    d.base = base->dirfrag();
    d.want_ino = want_ino;
    d.want_base_stripe = false;
    d.want_xlocked = want_xlocked;
    _send_discover(d);
  }

  // register + wait
  if (onfinish)
    base->add_ino_waiter(want_ino, onfinish);
}



void MDCache::kick_discovers(int who)
{
  for (map<tid_t,discover_info_t>::iterator p = discovers.begin();
       p != discovers.end();
       p++)
    _send_discover(p->second);
}


/* This function DOES put the passed message before returning */
void MDCache::handle_discover(MDiscover *dis) 
{
  int whoami = mds->get_nodeid();
  int from = dis->get_source_inst().name._num;

  assert(from != whoami);

  if (mds->get_state() <= MDSMap::STATE_REJOIN) {
    int from = dis->get_source().num();
    // proceed if requester is in the REJOIN stage, the request is from parallel_fetch().
    // delay processing request from survivor because we may not yet choose lock states.
    if (mds->get_state() < MDSMap::STATE_REJOIN ||
	!mds->mdsmap->is_rejoin(from)) {
      dout(0) << "discover_reply not yet active(|still rejoining), delaying" << dendl;
      mds->wait_for_active(new C_MDS_RetryMessage(mds, dis));
      return;
    }
  }


  CInode *cur = 0;
  MDiscoverReply *reply = new MDiscoverReply(dis);

  snapid_t snapid = dis->get_snapid();

  // get started.
  if (MDS_INO_IS_BASE(dis->get_base_ino())) {
    // wants root
    dout(7) << "handle_discover from mds." << from
	    << " wants base + " << dis->get_want().get_path()
	    << " snap " << snapid
	    << dendl;

    cur = get_inode(dis->get_base_ino());

    // add root
    reply->starts_with = MDiscoverReply::INODE;
    replicate_inode(cur, from, reply->trace);
    dout(10) << "added base " << *cur << dendl;
  }
  else {
    // there's a base inode
    cur = get_inode(dis->get_base_ino(), snapid);
    if (!cur && snapid != CEPH_NOSNAP) {
      cur = get_inode(dis->get_base_ino());
      if (!cur->is_multiversion())
	cur = NULL;  // nope!
    }
    
    if (!cur) {
      dout(7) << "handle_discover mds." << from 
	      << " don't have base ino " << dis->get_base_ino() << "." << snapid
	      << dendl;
      if (!dis->wants_base_stripe() && dis->get_want().depth() > 0)
	reply->set_error_dentry(dis->get_dentry(0));
      reply->set_flag_error_dir();
    } else if (dis->wants_base_stripe()) {
      dout(7) << "handle_discover mds." << from
	      << " wants basedir+" << dis->get_want().get_path()
	      << " has " << *cur
	      << dendl;
    } else {
      dout(7) << "handle_discover mds." << from
	      << " wants " << dis->get_want().get_path()
	      << " has " << *cur
	      << dendl;
    }
  }

  assert(reply);
  
  // add content
  // do some fidgeting to include a dir if they asked for the base dir, or just root.
  for (unsigned i = 0; 
       cur && (i < dis->get_want().depth() || dis->get_want().depth() == 0); 
       i++) {

    // -- figure out the dir

    // is *cur even a dir at all?
    if (!cur->is_dir()) {
      dout(7) << *cur << " not a dir" << dendl;
      reply->set_flag_error_dir();
      break;
    }

    // pick stripe
    __u32 dnhash;
    stripeid_t stripeid;
    if (dis->get_want().depth()) {
      dnhash = cur->hash_dentry_name(dis->get_dentry(i));
      stripeid = cur->pick_stripe(dnhash);
    } else {
      stripeid = dis->get_base_stripe();
      assert(dis->wants_base_stripe() || dis->get_want_ino() || MDS_INO_IS_BASE(dis->get_base_ino()));
    }
    int stripe_auth = cur->get_stripe_auth(stripeid);
    if (stripe_auth != mds->get_nodeid()) {
	/* before:
	 * ONLY set flag if empty!!
	 * otherwise requester will wake up waiter(s) _and_ continue with discover,
	 * resulting in duplicate discovers in flight,
	 * which can wreak havoc when discovering rename srcdn (which may move)
	 */

      if (reply->is_empty()) {
	// only hint if empty.
	//  someday this could be better, but right now the waiter logic isn't smart enough.

	// hint
        dout(7) << " not stripe auth, setting auth_hint for "
            << stripe_auth << dendl;
        reply->set_auth_hint(stripe_auth);

	// note error dentry, if any
	//  NOTE: important, as it allows requester to issue an equivalent discover
	//        to whomever we hint at.
	if (dis->get_want().depth() > i)
	  reply->set_error_dentry(dis->get_dentry(i));
      }

      break;
    }

    // open stripe?
    CStripe *curstripe = cur->get_or_open_stripe(stripeid);
    assert(curstripe);
    assert(curstripe->is_auth());

    if (curstripe->is_frozen()) {
      if (reply->is_empty()) {
	dout(7) << *curstripe << " is frozen, empty reply, waiting" << dendl;
	curstripe->add_waiter(CStripe::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      } else {
	dout(7) << *curstripe << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }

    if (!curstripe->is_open()) {
      dout(7) << "incomplete stripe contents for " << *curstripe << ", fetching" << dendl;
      if (reply->is_empty()) {
        curstripe->fetch(new C_MDS_RetryMessage(mds, dis));
        reply->put();
        return;
      } else {
        // initiate fetch, but send what we have so far
        curstripe->fetch(0);
        break;
      }
    }
    if (!reply->trace.length())
      reply->starts_with = MDiscoverReply::STRIPE;
    replicate_stripe(curstripe, from, reply->trace);
    dout(7) << "handle_discover added stripe " << *curstripe << dendl;

    // pick frag
    frag_t fg;
    if (dis->get_want().depth()) {
      // dentry specifies
      fg = curstripe->pick_dirfrag(dnhash);
    } else {
      // requester explicity specified the frag
      fg = dis->get_base_frag();
      assert(dis->wants_base_stripe() || dis->get_want_ino() || MDS_INO_IS_BASE(dis->get_base_ino()));
    }
    CDir *curdir = curstripe->get_dirfrag(fg);

    // open dir?
    if (!curdir) 
      curdir = curstripe->get_or_open_dirfrag(fg);
    assert(curdir);
    assert(curdir->is_auth());
    
    // is dir frozen?
    if (curdir->is_frozen()) {
      if (reply->is_empty()) {
	dout(7) << *curdir << " is frozen, empty reply, waiting" << dendl;
	curdir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      } else {
	dout(7) << *curdir << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }
    
    // add dir
    if (reply->is_empty() && !dis->wants_base_stripe()) {
      dout(7) << "handle_discover not adding unwanted base dir " << *curdir << dendl;
      // make sure the base frag is correct, though, in there was a refragment since the
      // original request was sent.
      reply->set_base_frag(curdir->get_frag());
    } else {
      assert(!curdir->is_ambiguous_auth()); // would be frozen.
      if (!reply->trace.length())
	reply->starts_with = MDiscoverReply::DIR;
      replicate_dir(curdir, from, reply->trace);
      dout(7) << "handle_discover added dir " << *curdir << dendl;
    }

    // lookup
    CDentry *dn = 0;
    if (dis->get_want_ino()) {
      // lookup by ino
      CInode *in = get_inode(dis->get_want_ino(), snapid);
      if (in && in->is_auth() && in->get_parent_dn()->get_dir() == curdir)
	dn = in->get_parent_dn();
    } else if (dis->get_want().depth() > 0) {
      // lookup dentry
      dn = curdir->lookup(dis->get_dentry(i), snapid);
    } else 
      break; // done!
          
    // incomplete dir?
    if (!dn) {
      if (!curdir->is_complete()) {
	// readdir
	dout(7) << "incomplete dir contents for " << *curdir << ", fetching" << dendl;
	if (reply->is_empty()) {
	  // fetch and wait
	  curdir->fetch(new C_MDS_RetryMessage(mds, dis));
	  reply->put();
	  return;
	} else {
	  // initiate fetch, but send what we have so far
	  curdir->fetch(0);
	  break;
	}
      }
      
      // don't have wanted ino in this dir?
      if (dis->get_want_ino()) {
	// set error flag in reply
	dout(7) << "no ino " << dis->get_want_ino() << " in this dir, flagging error in "
		<< *curdir << dendl;
	reply->set_flag_error_ino();
	break;
      }
      
      // is this a new mds dir?
      /*
      if (curdir->ino() == MDS_INO_CEPH) {
	char t[10];
	snprintf(t, sizeof(t), "mds%d", from);
	if (t == dis->get_dentry(i)) {
	  // yes.
	  _create_mdsdir_dentry(curdir, from, t, new C_MDS_RetryMessage(mds, dis));
	  //_create_system_file(curdir, t, create_system_inode(MDS_INO_MDSDIR(from), S_IFDIR),
	  //new C_MDS_RetryMessage(mds, dis));
	  reply->put();
	  return;
	}
      }	
      */
      
      // send null dentry
      dout(7) << "dentry " << dis->get_dentry(i) << " dne, returning null in "
	      << *curdir << dendl;
      dn = curdir->add_null_dentry(dis->get_dentry(i));
    }
    assert(dn);

    CDentry::linkage_t *dnl = dn->get_linkage();

    // xlocked dentry?
    //  ...always block on non-tail items (they are unrelated)
    //  ...allow xlocked tail disocvery _only_ if explicitly requested
    bool tailitem = (dis->get_want().depth() == 0) || (i == dis->get_want().depth() - 1);
    if (dn->lock.is_xlocked()) {
      // is this the last (tail) item in the discover traversal?
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "handle_discover allowing discovery of xlocked tail " << *dn << dendl;
      } else if (reply->is_empty()) {
	dout(7) << "handle_discover blocking on xlocked " << *dn << dendl;
	dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      } else {
	dout(7) << "handle_discover non-empty reply, xlocked tail " << *dn << dendl;
	break;
      }
    }

    // frozen inode?
    if (dnl->is_primary() && dnl->get_inode()->is_frozen()) {
      if (tailitem && dis->wants_xlocked()) {
	dout(7) << "handle_discover allowing discovery of frozen tail " << *dnl->get_inode() << dendl;
      } else if (reply->is_empty()) {
	dout(7) << *dnl->get_inode() << " is frozen, empty reply, waiting" << dendl;
	dnl->get_inode()->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryMessage(mds, dis));
	reply->put();
	return;
      } else {
	dout(7) << *dnl->get_inode() << " is frozen, non-empty reply, stopping" << dendl;
	break;
      }
    }

    // add dentry
    if (!reply->trace.length())
      reply->starts_with = MDiscoverReply::DENTRY;
    replicate_dentry(dn, from, reply->trace);
    dout(7) << "handle_discover added dentry " << *dn << dendl;
    
    if (!dnl->is_primary()) break;  // stop on null or remote link.
    
    // add inode
    CInode *next = dnl->get_inode();
    assert(next->is_auth());
    
    replicate_inode(next, from, reply->trace);
    dout(7) << "handle_discover added inode " << *next << dendl;
    
    // descend, keep going.
    cur = next;
    continue;
  }

  // how did we do?
  assert(!reply->is_empty());
  dout(7) << "handle_discover sending result back to asker mds." << from << dendl;
  mds->send_message(reply, dis->get_connection());

  dis->put();
}

/* This function DOES put the passed message before returning */
void MDCache::handle_discover_reply(MDiscoverReply *m) 
{
  /*
  if (mds->get_state() < MDSMap::STATE_ACTIVE) {
    dout(0) << "discover_reply NOT ACTIVE YET" << dendl;
    m->put();
    return;
  }
  */
  dout(7) << "discover_reply " << *m << dendl;
  if (m->is_flag_error_stripe()) 
    dout(7) << " flag error, stripe" << dendl;
  if (m->is_flag_error_dir()) 
    dout(7) << " flag error, dir" << dendl;
  if (m->is_flag_error_dn()) 
    dout(7) << " flag error, dentry = " << m->get_error_dentry() << dendl;
  if (m->is_flag_error_ino()) 
    dout(7) << " flag error, ino = " << m->get_wanted_ino() << dendl;

  list<Context*> finished, error;
  int from = m->get_source().num();

  // starting point
  CInode *cur = get_inode(m->get_base_ino());
  bufferlist::iterator p = m->trace.begin();

  int next = m->starts_with;

  // decrement discover counters
  if (m->get_tid()) {
    map<tid_t,discover_info_t>::iterator p = discovers.find(m->get_tid());
    if (p != discovers.end()) {
      dout(10) << " found tid " << m->get_tid() << dendl;
      discovers.erase(p);
    } else {
      dout(10) << " tid " << m->get_tid() << " not found, must be dup reply" << dendl;
    }
  }

  // discover ino error
  if (p.end() && m->is_flag_error_ino()) {
    assert(cur->is_dir());
    CStripe *stripe = cur->get_stripe(m->get_base_stripe());
    assert(stripe);
    CDir *dir = stripe->get_dirfrag(m->get_base_frag());
    assert(dir);
    dout(7) << " flag_error on ino " << m->get_wanted_ino()
        << ", triggering ino" << dendl;
    dir->take_ino_waiting(m->get_wanted_ino(), error);
  }

  // discover may start with an inode
  if (!p.end() && next == MDiscoverReply::INODE) {
    cur = add_replica_inode(p, NULL, from, finished);
    dout(7) << "discover_reply got base inode " << *cur << dendl;
    assert(cur->is_base());
    
    next = MDiscoverReply::STRIPE;
    
    // take waiters?
    if (cur->is_base() &&
	waiting_for_base_ino[from].count(cur->ino())) {
      finished.swap(waiting_for_base_ino[from][cur->ino()]);
      waiting_for_base_ino[from].erase(cur->ino());
    }
  }
  assert(cur);
  
  // loop over discover results.
  // indexes follow each ([[stripe] dir] dentry] inode) 
  // can start, end with any type.
  while (!p.end()) {
    CStripe *curstripe;
    CDir *curdir;
    if (next == MDiscoverReply::STRIPE) {
      // stripe
      curstripe = add_replica_stripe(p, cur, from, finished);
      if (p.end())
        break;

      // dir
      curdir = add_replica_dir(p, curstripe, finished);
      if (p.end())
        break;
    } else if (p.end() && m->is_flag_error_dn()) {
      // note: this can only happen our first way around this loop.
      __u32 dnhash = cur->hash_dentry_name(m->get_error_dentry());
      stripeid_t stripeid = cur->pick_stripe(dnhash);
      curstripe = cur->get_stripe(stripeid);
      assert(curstripe);

      frag_t fg = curstripe->pick_dirfrag(dnhash);
      curdir = curstripe->get_dirfrag(fg);
      assert(curdir);
    } else {
      curstripe = cur->get_stripe(m->get_base_stripe());
      assert(curstripe);
      curdir = curstripe->get_dirfrag(m->get_base_frag());
      assert(curdir);
    }

    // dentry
    CDentry *dn = add_replica_dentry(p, curdir, finished);
    if (p.end())
      break;

    // inode
    cur = add_replica_inode(p, dn, from, finished);

    next = MDiscoverReply::STRIPE;
  }

  // dir error?
  // or dir_auth hint?
  if (m->is_flag_error_dir() && !cur->is_dir()) {
    // not a dir.
    cur->take_waiting(CInode::WAIT_STRIPE, error);
  } else if (m->is_flag_error_dir() || m->get_auth_hint() != CDIR_AUTH_UNKNOWN) {
    int who = m->get_auth_hint();
    if (who == mds->get_nodeid()) who = -1;
    if (who >= 0)
      dout(7) << " auth_hint is " << who << dendl;

    // try again?
    if (m->get_error_dentry().length()) {
      // wanted a dentry
      const string &dname = m->get_error_dentry();
      filepath relpath(dname, 0);

      __u32 dnhash = cur->hash_dentry_name(dname);
      stripeid_t stripeid = cur->pick_stripe(dnhash);
      CStripe *stripe = cur->get_stripe(stripeid);

      if (cur->is_waiting_for_stripe(stripeid)) {
	if (cur->is_auth() || stripe)
	  cur->take_stripe_waiting(stripeid, finished);
	else
	  discover_path(cur, m->get_wanted_snapid(), relpath, 0, m->get_wanted_xlocked(), who);
      } else
	  dout(7) << " doing nothing, nobody is waiting for stripe " << stripeid << dendl;

      if (stripe) {
        frag_t fg = stripe->pick_dirfrag(dnhash);
        CDir *dir = stripe->get_dirfrag(fg);

        if (stripe->is_waiter_for(CStripe::WAIT_DIR)) {
          if (dir)
            stripe->take_waiting(CStripe::WAIT_DIR, finished);
          else
            discover_path(cur, m->get_wanted_snapid(), relpath,
                          0, m->get_wanted_xlocked(), who);
        } else
	  dout(7) << " doing nothing, nobody is waiting for dir " << fg << dendl;

        if (dir) {
          // don't actaully need the hint, now
          if (dir->is_waiting_for_dentry(dname.c_str(), m->get_wanted_snapid())) {
            if (dir->is_auth() || dir->lookup(dname))
              dir->take_dentry_waiting(dname, m->get_wanted_snapid(),
                                       m->get_wanted_snapid(), finished);
            else
              discover_path(dir, m->get_wanted_snapid(), relpath, 0, m->get_wanted_xlocked());
          } else
            dout(7) << " doing nothing, have dir but nobody is waiting on dentry "
                << dname << dendl;
        }
      }
    } else {
      // wanted dir or ino
      stripeid_t stripeid = m->get_base_stripe();
      CStripe *stripe = cur->get_stripe(stripeid);
      if (cur->is_waiting_for_stripe(stripeid)) {
	if (cur->is_auth() || stripe)
	  cur->take_stripe_waiting(stripeid, finished);
	else
	  discover_dir_stripe(cur, stripeid, 0, who);
      } else
	dout(7) << " doing nothing, nobody is waiting for stripe " << stripeid << dendl;

      if (stripe) {
        frag_t fg = m->get_base_frag();
        CDir *dir = stripe->get_dirfrag(fg);

        if (stripe->is_waiter_for(CStripe::WAIT_DIR)) {
          if (cur->is_auth() || stripe)
            stripe->take_waiting(CStripe::WAIT_DIR, finished);
          else
            discover_dir_frag(stripe, fg, 0, who);
        } else
          dout(7) << " doing nothing, nobody is waiting for dir " << fg << dendl;

        inodeno_t wanted_ino = m->get_wanted_ino();
        if (dir && wanted_ino && dir->is_waiting_for_ino(wanted_ino)) {
          if (dir->is_auth() || get_inode(wanted_ino))
            dir->take_ino_waiting(wanted_ino, finished);
          else
            discover_ino(dir, wanted_ino, 0, m->get_wanted_xlocked());
        } else
          dout(7) << " doing nothing, nobody is waiting for ino " << wanted_ino << dendl;
      }
    }
  }

  // waiters
  finish_contexts(g_ceph_context, error, -ENOENT);  // finish errors directly
  mds->queue_waiters(finished);

  // done
  m->put();
}



// ----------------------------
// REPLICAS

CStripe* MDCache::add_replica_stripe(bufferlist::iterator& p, CInode *diri,
                                     int from, list<Context*>& finished)
{
  dirstripe_t ds;
  ::decode(ds, p);

  assert(diri->ino() == ds.ino);

  CStripe *stripe = get_dirstripe(ds);
  if (stripe) {
    stripe->decode_replica(p, false);
    dout(7) << "add_replica_stripe had " << *stripe
        << " nonce " << stripe->get_replica_nonce() << dendl;
  } else {
    assert(ds.stripeid < diri->get_stripe_count());
    stripe = diri->add_stripe(new CStripe(diri, ds.stripeid, from));
    stripe->decode_replica(p, true);
    dout(7) << "add_replica_stripe added " << *stripe
        << " nonce " << stripe->get_replica_nonce() << dendl;

    stripe->set_stripe_auth(from);

    diri->take_stripe_waiting(ds.stripeid, finished);
  }
  return stripe;
}

CStripe *MDCache::forge_replica_stripe(CInode *diri, stripeid_t stripeid, int from)
{
  assert(mds->mdsmap->get_state(from) < MDSMap::STATE_REJOIN);
 
  // forge a replica.
  CStripe *stripe = diri->add_stripe(new CStripe(diri, stripeid, from));
 
  // i'm assuming this is a subtree root. 
  stripe->set_stripe_auth(from);

  dout(7) << "forge_replica_stripe added " << *stripe << " while mds." << from << " is down" << dendl;

  return stripe;
}

CDir *MDCache::add_replica_dir(bufferlist::iterator& p, CStripe *stripe,
			       list<Context*>& finished)
{
  dirfrag_t df;
  ::decode(df, p);

  assert(stripe->dirstripe() == df.stripe);

  // add it (_replica_)
  CDir *dir = stripe->get_dirfrag(df.frag);

  if (dir) {
    // had replica. update w/ new nonce.
    dir->decode_replica(p);
    dout(7) << "add_replica_dir had " << *dir << " nonce " << dir->replica_nonce << dendl;
  } else {
    // force frag to leaf in the stripe tree
    fragtree_t &dft = stripe->get_fragtree();
    if (!dft.is_leaf(df.frag)) {
      dout(7) << "add_replica_dir forcing frag " << df.frag << " to leaf in the fragtree "
	      << dft << dendl;
      dft.force_to_leaf(g_ceph_context, df.frag);
    }

    // add replica.
    dir = stripe->add_dirfrag( new CDir(stripe, df.frag, this, false) );
    dir->decode_replica(p);

    dout(7) << "add_replica_dir added " << *dir << " nonce " << dir->replica_nonce << dendl;

    // get waiters
    stripe->take_waiting(CStripe::WAIT_DIR, finished);
  }

  return dir;
}

CDir *MDCache::forge_replica_dir(CStripe *stripe, frag_t fg, int from)
{
  assert(mds->mdsmap->get_state(from) < MDSMap::STATE_REJOIN);
 
  // forge a replica.
  CDir *dir = stripe->add_dirfrag(new CDir(stripe, fg, this, false));

  dout(7) << "forge_replica_dir added " << *dir << " while mds." << from << " is down" << dendl;

  return dir;
}

CDentry *MDCache::add_replica_dentry(bufferlist::iterator& p, CDir *dir, list<Context*>& finished)
{
  string name;
  snapid_t last;
  ::decode(name, p);
  ::decode(last, p);

  CDentry *dn = dir->lookup(name, last);
  
  // have it?
  if (dn) {
    dn->decode_replica(p, false);
    dout(7) << "add_replica_dentry had " << *dn << dendl;
  } else {
    dn = dir->add_null_dentry(name, 1 /* this will get updated below */, last);
    dn->decode_replica(p, true);
    dout(7) << "add_replica_dentry added " << *dn << dendl;
  }

  dir->take_dentry_waiting(name, dn->first, dn->last, finished);

  return dn;
}

CInode *MDCache::add_replica_inode(bufferlist::iterator& p, CDentry *dn,
                                   int from, list<Context*>& finished)
{
  inodeno_t ino;
  snapid_t last;
  ::decode(ino, p);
  ::decode(last, p);
  CInode *in = get_inode(ino, last);
  if (!in) {
    in = new CInode(this, from, 1, last);
    in->decode_replica(p, true);
    add_inode(in);
    dout(10) << "add_replica_inode added " << *in << dendl;
    if (dn) {
      assert(dn->get_linkage()->is_null());
      dn->dir->link_primary_inode(dn, in);
    }
  } else {
    in->decode_replica(p, false);
    dout(10) << "add_replica_inode had " << *in << dendl;
  }

  if (dn) {
    if (!dn->get_linkage()->is_primary() || dn->get_linkage()->get_inode() != in)
      dout(10) << "add_replica_inode different linkage in dentry " << *dn << dendl;
    
    dn->get_dir()->take_ino_waiting(in->ino(), finished);
  }
  
  return in;
}

 
void MDCache::replicate_stray(CDentry *straydn, int who, bufferlist& bl)
{
  CDir *straydir = straydn->get_dir();
  CStripe *straystripe = straydir->get_stripe();
  CInode *strayin = straystripe->get_inode();
  CDentry *parentdn = strayin->get_parent_dn();
  CDir *parentdir = parentdn->get_dir();
  CStripe *parentstripe = parentdir->get_stripe();
  assert(get_myin() == parentstripe->get_inode());

  replicate_inode(get_myin(), who, bl);
  replicate_stripe(parentstripe, who, bl);
  replicate_dir(parentdir, who, bl);
  replicate_dentry(parentdn, who, bl);
  replicate_inode(strayin, who, bl);
  replicate_stripe(straystripe, who, bl);
  replicate_dir(straydir, who, bl);
  replicate_dentry(straydn, who, bl);
}
   
CDentry *MDCache::add_replica_stray(bufferlist &bl, int from)
{
  list<Context*> finished;
  bufferlist::iterator p = bl.begin();

  CInode *parentin = add_replica_inode(p, NULL, from, finished);
  CStripe *parentstripe = add_replica_stripe(p, parentin, from, finished);
  CDir *parentdir = add_replica_dir(p, parentstripe, finished);
  CDentry *parentdn = add_replica_dentry(p, parentdir, finished);
  CInode *strayin = add_replica_inode(p, parentdn, from, finished);
  CStripe *straystripe = add_replica_stripe(p, strayin, from, finished);
  CDir *straydir = add_replica_dir(p, straystripe, finished);
  CDentry *straydn = add_replica_dentry(p, straydir, finished);
  if (!finished.empty())
    mds->queue_waiters(finished);

  return straydn;
}


int MDCache::send_dir_updates(CStripe *stripe, bool bcast)
{
  // this is an FYI, re: replication

  set<int> who;
  if (bcast) {
    mds->get_mds_map()->get_active_mds_set(who);
  } else {
    for (map<int,int>::iterator p = stripe->replicas_begin();
	 p != stripe->replicas_end();
	 ++p)
      who.insert(p->first);
  }
  
  dout(7) << "sending dir_update on " << *stripe << " bcast " << bcast << " to " << who << dendl;

  filepath path;
  stripe->get_inode()->make_path(path);

  int whoami = mds->get_nodeid();
  for (set<int>::iterator it = who.begin();
       it != who.end();
       it++) {
    if (*it == whoami) continue;
    //if (*it == except) continue;
    dout(7) << "sending dir_update on " << *stripe << " to " << *it << dendl;

    mds->send_message_mds(new MDirUpdate(mds->get_nodeid(), stripe->dirstripe(),
                                         path, stripe->replicate, bcast),
			  *it);
  }

  return 0;
}

/* This function DOES put the passed message before returning */
void MDCache::handle_dir_update(MDirUpdate *m)
{
  dirstripe_t ds = m->get_dirstripe();
  CStripe *stripe = get_dirstripe(ds);
  if (!stripe) {
    dout(5) << "dir_update on " << ds << ", don't have it" << dendl;

    // discover it?
    if (m->should_discover()) {
      // only try once!
      // this is key to avoid a fragtree update race, among other things.
      m->tried_discover();
      vector<CDentry*> trace;
      CInode *in;
      filepath path = m->get_path();
      dout(5) << "trying discover on dir_update for " << path << dendl;
      int r = path_traverse(NULL, m, NULL, path, &trace, &in, MDS_TRAVERSE_DISCOVER);
      if (r > 0)
        return;
      assert(r == 0);

      open_remote_dirstripe(in, ds.stripeid, new C_MDS_RetryMessage(mds, m));
      return;
    }

    m->put();
    return;
  }

  // update
  dout(5) << "dir_update on " << *stripe << dendl;
  stripe->replicate = m->should_replicate();
  
  // done
  m->put();
}





// LINK

void MDCache::send_dentry_link(CDentry *dn, const vector<int> *skip)
{
  dout(7) << "send_dentry_link " << *dn << dendl;

  for (map<int,int>::iterator p = dn->replicas_begin(); 
       p != dn->replicas_end(); 
       p++) {
    if (skip && find(skip->begin(), skip->end(), p->first) != skip->end())
      continue;
    if (mds->mdsmap->get_state(p->first) < MDSMap::STATE_REJOIN) 
      continue;
    CDentry::linkage_t *dnl = dn->get_linkage();
    MDentryLink *m = new MDentryLink(dn->get_dir()->dirfrag(),
				     dn->name, dnl->is_primary());
    if (dnl->is_primary()) {
      dout(10) << "  primary " << *dnl->get_inode() << dendl;
      replicate_inode(dnl->get_inode(), p->first, m->bl);
    } else if (dnl->is_remote()) {
      inodeno_t ino = dnl->get_remote_ino();
      __u8 d_type = dnl->get_remote_d_type();
      dout(10) << "  remote " << ino << " " << d_type << dendl;
      ::encode(ino, m->bl);
      ::encode(d_type, m->bl);
    } else
      assert(0);   // aie, bad caller!
    mds->send_message_mds(m, p->first);
  }
}

/* This function DOES put the passed message before returning */
void MDCache::handle_dentry_link(MDentryLink *m)
{
  int from = m->get_source().num();
  CDentry *dn = NULL;
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(7) << "handle_dentry_link don't have dirfrag " << m->get_dirfrag() << dendl;
  } else {
    dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_link don't have dentry " << *dir << " dn " << m->get_dn() << dendl;
    } else {
      dout(7) << "handle_dentry_link on " << *dn << dendl;
      CDentry::linkage_t *dnl = dn->get_linkage();

      assert(!dn->is_auth());
      assert(dnl->is_null());
    }
  }

  bufferlist::iterator p = m->bl.begin();
  list<Context*> finished;
  if (dn) {
    if (m->get_is_primary()) {
      // primary link.
      CInode *in = add_replica_inode(p, dn, from, finished);
      if (dn->get_linkage()->is_null()) // fix linkage
        dir->link_primary_inode(dn, in);
    } else {
      // remote link, easy enough.
      inodeno_t ino;
      __u8 d_type;
      ::decode(ino, p);
      ::decode(d_type, p);
      dir->link_remote_inode(dn, ino, d_type);
    }
  } else if (m->get_is_primary()) {
    CInode *in = add_replica_inode(p, NULL, from, finished);
    assert(in->get_num_ref() == 0);
    assert(in->get_parent_dn() == NULL);
    MCacheExpire* expire = new MCacheExpire(mds->get_nodeid());
    expire->add_inode(in->vino(), in->get_replica_nonce());
    mds->send_message_mds(expire, from);
    remove_inode(in);
  }

  if (!finished.empty())
    mds->queue_waiters(finished);

  m->put();
  return;
}


// UNLINK

void MDCache::send_dentry_unlink(CDentry *dn, MDRequest *mdr)
{
  dout(10) << "send_dentry_unlink " << *dn << dendl;
  // share unlink news with replicas
  for (map<int,int>::iterator it = dn->replicas_begin();
       it != dn->replicas_end();
       it++) {
    // don't tell (rmdir) witnesses; they already know
    if (mdr && mdr->more()->witnessed.count(it->first))
      continue;

    MDentryUnlink *unlink = new MDentryUnlink(dn->get_dir()->dirfrag(), dn->name);
    mds->send_message_mds(unlink, it->first);
  }
}

/* This function DOES put the passed message before returning */
void MDCache::handle_dentry_unlink(MDentryUnlink *m)
{
  CDir *dir = get_dirfrag(m->get_dirfrag());
  if (!dir) {
    dout(7) << "handle_dentry_unlink don't have dirfrag " << m->get_dirfrag() << dendl;
  } else {
    CDentry *dn = dir->lookup(m->get_dn());
    if (!dn) {
      dout(7) << "handle_dentry_unlink don't have dentry " << *dir << " dn " << m->get_dn() << dendl;
    } else {
      dout(7) << "handle_dentry_unlink on " << *dn << dendl;
      CDentry::linkage_t *dnl = dn->get_linkage();

      // open inode?
      if (dnl->is_primary()) {
	CInode *in = dnl->get_inode();
	dn->dir->unlink_inode(dn);

	// send caps to auth (if we're not already)
	if (in->is_any_caps() &&
	    !in->state_test(CInode::STATE_EXPORTINGCAPS))
	  migrator->export_caps(in);
      } else {
	assert(dnl->is_remote());
	dn->dir->unlink_inode(dn);
      }
      assert(dnl->is_null());
      
      // move to bottom of lru
      lru.lru_bottouch(dn);
    }
  }

  m->put();
}






// ===================================================================



// ===================================================================
// FRAGMENT


/** 
 * adjust_dir_fragments -- adjust fragmentation for a directory
 *
 * @param stripe directory stripe
 * @param basefrag base fragment
 * @param bits bit adjustment.  positive for split, negative for merge.
 */
void MDCache::adjust_dir_fragments(CStripe *stripe, frag_t basefrag, int bits,
				   list<CDir*>& resultfrags,
				   list<Context*>& waiters,
				   bool replay)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " " << bits
	   << " on " << *stripe << dendl;

  list<CDir*> srcfrags;
  stripe->get_dirfrags_under(basefrag, srcfrags);

  adjust_dir_fragments(stripe, srcfrags, basefrag, bits, resultfrags, waiters, replay);
}

CDir *MDCache::force_dir_fragment(CStripe *stripe, frag_t fg)
{
  CDir *dir = stripe->get_dirfrag(fg);
  if (dir)
    return dir;

  dout(10) << "force_dir_fragment " << fg << " on " << *stripe << dendl;

  list<CDir*> src, result;
  list<Context*> waiters;

  // split a parent?
  const fragtree_t &dft = stripe->get_fragtree();
  frag_t parent = dft.get_branch_or_leaf(fg);
  while (1) {
    CDir *pdir = stripe->get_dirfrag(parent);
    if (pdir) {
      int split = fg.bits() - parent.bits();
      dout(10) << " splitting parent by " << split << " " << *pdir << dendl;
      src.push_back(pdir);
      adjust_dir_fragments(stripe, src, parent, split, result, waiters, true);
      dir = stripe->get_dirfrag(fg);
      dout(10) << "force_dir_fragment result " << *dir << dendl;
      return dir;
    }
    if (parent == frag_t())
      break;
    frag_t last = parent;
    parent = parent.parent();
    dout(10) << " " << last << " parent is " << parent << dendl;
  }

  // hoover up things under fg?
  stripe->get_dirfrags_under(fg, src);
  if (src.empty()) {
    dout(10) << "force_dir_fragment no frags under " << fg << dendl;
    return NULL;
  }
  dout(10) << " will combine frags under " << fg << ": " << src << dendl;
  adjust_dir_fragments(stripe, src, fg, 0, result, waiters, true);
  dir = result.front();
  dout(10) << "force_dir_fragment result " << *dir << dendl;
  return dir;
}

void MDCache::adjust_dir_fragments(CStripe *stripe,
				   list<CDir*>& srcfrags,
				   frag_t basefrag, int bits,
				   list<CDir*>& resultfrags, 
				   list<Context*>& waiters,
				   bool replay)
{
  dout(10) << "adjust_dir_fragments " << basefrag << " bits " << bits
	   << " srcfrags " << srcfrags
	   << " on " << *stripe << dendl;

  // adjust fragtree
  // yuck.  we may have discovered the inode while it was being fragmented.
  fragtree_t &dft = stripe->get_fragtree();
  if (!dft.is_leaf(basefrag))
    dft.force_to_leaf(g_ceph_context, basefrag);

  if (bits > 0)
    dft.split(basefrag, bits);
  dout(10) << " new fragtree is " << dft << dendl;

  if (srcfrags.empty())
    return;

  if (bits > 0) {
    // SPLIT
    assert(srcfrags.size() == 1);
    CDir *dir = srcfrags.front();
    dir->split(bits, resultfrags, waiters, replay);
    stripe->close_dirfrag(dir->get_frag());
  } else {
    // MERGE
    CDir *f = new CDir(stripe, basefrag, this, srcfrags.front()->is_auth());
    f->merge(srcfrags, waiters, replay);
    stripe->add_dirfrag(f);
    resultfrags.push_back(f);
  }
}


class C_MDC_FragmentFrozen : public Context {
  MDCache *mdcache;
  list<CDir*> dirs;
  frag_t basefrag;
  int by;
public:
  C_MDC_FragmentFrozen(MDCache *m, list<CDir*> d, frag_t bf, int b)
      : mdcache(m), dirs(d), basefrag(bf), by(b) {}
  virtual void finish(int r) {
    mdcache->fragment_frozen(dirs, basefrag, by);
  }
};


bool MDCache::can_fragment_lock(CStripe *stripe)
{
  if (!stripe->dirfragtreelock.can_wrlock(-1)) {
    dout(7) << "can_fragment: can't wrlock dftlock" << dendl;
    //mds->locker->scatter_nudge(&stripe->dirfragtreelock, NULL);
    return false;
  }
  return true;
}

bool MDCache::can_fragment(CStripe *stripe, list<CDir*>& dirs)
{
  if (mds->mdsmap->is_degraded()) {
    dout(7) << "can_fragment: cluster degraded, no fragmenting for now" << dendl;
    return false;
  }
#if 0
  if (diri->get_parent_dir() &&
      diri->get_parent_dir()->get_inode()->is_stray()) {
    dout(7) << "can_fragment: i won't merge|split anything in stray" << dendl;
    return false;
  }
#endif
  inodeno_t ino = stripe->dirstripe().ino;
  if (MDS_INO_IS_MDSDIR(ino) || ino == MDS_INO_CEPH) {
    dout(7) << "can_fragment: i won't fragment the mdsdir or .ceph" << dendl;
    return false;
  }

  for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
    CDir *dir = *p;
    if (dir->state_test(CDir::STATE_FRAGMENTING)) {
      dout(7) << "can_fragment: already fragmenting " << *dir << dendl;
      return false;
    }
    if (!dir->is_auth()) {
      dout(7) << "can_fragment: not auth on " << *dir << dendl;
      return false;
    }
    if (dir->is_freezing_or_frozen()) {
      dout(7) << "can_fragment: can't merge, freezing|frozen.  wait for other exports to finish first." << dendl;
      return false;
    }
  }

  return true;
}

void MDCache::split_dir(CDir *dir, int bits)
{
  dout(7) << "split_dir " << *dir << " bits " << bits << dendl;
  assert(dir->is_auth());

  list<CDir*> dirs;
  dirs.push_back(dir);

  if (!can_fragment(dir->get_stripe(), dirs))
    return;
  if (!can_fragment_lock(dir->get_stripe())) {
    dout(10) << " requeuing dir " << dir->dirfrag() << dendl;
    mds->balancer->queue_split(dir);
    return;
  }

  C_GatherBuilder gather(g_ceph_context, 
	  new C_MDC_FragmentFrozen(this, dirs, dir->get_frag(), bits));
  fragment_freeze_dirs(dirs, gather);
  gather.activate();

  // initial mark+complete pass
  fragment_mark_and_complete(dirs);
}

void MDCache::merge_dir(CStripe *stripe, frag_t frag)
{
  dout(7) << "merge_dir to " << frag << " on " << *stripe << dendl;

  list<CDir*> dirs;
  if (!stripe->get_dirfrags_under(frag, dirs)) {
    dout(7) << "don't have all frags under " << frag << " for " << *stripe << dendl;
    return;
  }

  if (stripe->get_fragtree().is_leaf(frag)) {
    dout(10) << " " << frag << " already a leaf for " << *stripe << dendl;
    return;
  }

  if (!can_fragment(stripe, dirs))
    return;
  if (!can_fragment_lock(stripe)) {
    //dout(10) << " requeuing dir " << dir->dirfrag() << dendl;
    //mds->mdbalancer->split_queue.insert(dir->dirfrag());
    return;
  }

  CDir *first = dirs.front();
  int bits = first->get_frag().bits() - frag.bits();
  dout(10) << " we are merging by " << bits << " bits" << dendl;

  C_GatherBuilder gather(g_ceph_context,
	  new C_MDC_FragmentFrozen(this, dirs, frag, bits));
  fragment_freeze_dirs(dirs, gather);
  gather.activate();

  // initial mark+complete pass
  fragment_mark_and_complete(dirs);
}

void MDCache::fragment_freeze_dirs(list<CDir*>& dirs, C_GatherBuilder &gather)
{
  for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
    CDir *dir = *p;
    dir->auth_pin(dir);  // until we mark and complete them
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->freeze_dir();
    assert(dir->is_freezing());
    dir->add_waiter(CDir::WAIT_FROZEN, gather.new_sub());
  }
}

class C_MDC_FragmentMarking : public Context {
  MDCache *mdcache;
  list<CDir*> dirs;
public:
  C_MDC_FragmentMarking(MDCache *m, list<CDir*>& d) : mdcache(m), dirs(d) {}
  virtual void finish(int r) {
    mdcache->fragment_mark_and_complete(dirs);
  }
};

void MDCache::fragment_mark_and_complete(list<CDir*>& dirs)
{
  dout(10) << "fragment_mark_and_complete " << dirs
      << " on " << *dirs.front()->get_stripe() << dendl;

  C_GatherBuilder gather(g_ceph_context);
  
  for (list<CDir*>::iterator p = dirs.begin();
       p != dirs.end();
       ++p) {
    CDir *dir = *p;

    if (!dir->is_complete()) {
      dout(15) << " fetching incomplete " << *dir << dendl;
      dir->fetch(gather.new_sub(),
		 true);  // ignore authpinnability
    } 
    else if (!dir->state_test(CDir::STATE_DNPINNEDFRAG)) {
      dout(15) << " marking " << *dir << dendl;
      for (CDir::map_t::iterator p = dir->items.begin();
	   p != dir->items.end();
	   ++p) {
	CDentry *dn = p->second;
	dn->get(CDentry::PIN_FRAGMENTING);
	assert(!dn->state_test(CDentry::STATE_FRAGMENTING));
	dn->state_set(CDentry::STATE_FRAGMENTING);
      }
      dir->state_set(CDir::STATE_DNPINNEDFRAG);
      dir->auth_unpin(dir);
    }
    else {
      dout(15) << " already marked " << *dir << dendl;
    }
  }
  if (gather.has_subs()) {
    gather.set_finisher(new C_MDC_FragmentMarking(this, dirs));
    gather.activate();
  }

  // flush log so that request auth_pins are retired
  mds->mdlog->flush();
}

void MDCache::fragment_unmark_unfreeze_dirs(list<CDir*>& dirs)
{
  dout(10) << "fragment_unmark_unfreeze_dirs " << dirs << dendl;
  for (list<CDir*>::iterator p = dirs.begin(); p != dirs.end(); p++) {
    CDir *dir = *p;
    dout(10) << " frag " << *dir << dendl;

    assert(dir->state_test(CDir::STATE_DNPINNEDFRAG));
    dir->state_clear(CDir::STATE_DNPINNEDFRAG);

    assert(dir->state_test(CDir::STATE_FRAGMENTING));
    dir->state_clear(CDir::STATE_FRAGMENTING);

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) {
      CDentry *dn = p->second;
      assert(dn->state_test(CDentry::STATE_FRAGMENTING));
      dn->state_clear(CDentry::STATE_FRAGMENTING);
      dn->put(CDentry::PIN_FRAGMENTING);
    }

    dir->unfreeze_dir();
  }
}

class C_MDC_FragmentLoggedAndStored : public Context {
  MDCache *mdcache;
  Mutation *mut;
  list<CDir*> resultfrags;
  frag_t basefrag;
  int bits;
public:
  C_MDC_FragmentLoggedAndStored(MDCache *m, Mutation *mu, list<CDir*>& r, frag_t bf, int bi) : 
    mdcache(m), mut(mu), resultfrags(r), basefrag(bf), bits(bi) {}
  virtual void finish(int r) {
    mdcache->fragment_logged_and_stored(mut, resultfrags, basefrag, bits);
  }
};

void MDCache::fragment_frozen(list<CDir*>& dirs, frag_t basefrag, int bits)
{
  CStripe *stripe = dirs.front()->get_stripe();

  if (bits > 0) {
    assert(dirs.size() == 1);
  } else {
    assert(bits < 0);
  }

  dout(10) << "fragment_frozen " << dirs << " " << basefrag << " by " << bits 
	   << " on " << *stripe << dendl;

  Mutation *mut = new Mutation;

  // wrlock dirfragtreelock
  mds->locker->wrlock_force(&stripe->dirfragtreelock, mut);

  // prevent a racing gather on any other scatterlocks too
  mds->locker->wrlock_force(&stripe->linklock, mut);
  mds->locker->wrlock_force(&stripe->nestlock, mut);

  // refragment
  list<CDir*> resultfrags;
  list<Context*> waiters;
  adjust_dir_fragments(stripe, dirs, basefrag, bits, resultfrags, waiters, false);
  if (g_conf->mds_debug_frag)
    stripe->verify_dirfrags();
  mds->queue_waiters(waiters);

  // journal
  mut->ls = mds->mdlog->get_current_segment();
  EFragment *le = new EFragment(mds->mdlog, EFragment::OP_PREPARE,
                               dirfrag_t(stripe->dirstripe(), basefrag), bits);
  mds->mdlog->start_entry(le);

  le->metablob.add_stripe_context(stripe);
  le->metablob.add_stripe(stripe, true); // stripe is dirty

  // freeze, journal, and store resulting frags
  C_GatherBuilder gather(g_ceph_context,
		      new C_MDC_FragmentLoggedAndStored(this, mut,
				  resultfrags, basefrag, bits));

  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    le->metablob.add_dir(dir, false);

    // freeze and store them too
    dir->state_set(CDir::STATE_FRAGMENTING);
    dir->commit(0, gather.new_sub(), true);  // ignore authpinnability
  }

  mds->mdlog->submit_entry(le, gather.new_sub());
  mds->mdlog->flush();
  gather.activate();
}

void MDCache::fragment_logged_and_stored(Mutation *mut, list<CDir*>& resultfrags, frag_t basefrag, int bits)
{
  CStripe *stripe = resultfrags.front()->get_stripe();
  dirfrag_t dirfrag(stripe->dirstripe(), basefrag);

  dout(10) << "fragment_logged_and_stored " << resultfrags
      << " " << basefrag << " bits " << bits
      << " on " << *stripe << dendl;
 
  // journal commit
  EFragment *le = new EFragment(mds->mdlog, EFragment::OP_COMMIT, dirfrag, bits);
  mds->mdlog->start_entry(le);
  mds->mdlog->submit_entry(le);

  // tell peers
  CDir *first = *resultfrags.begin();
  for (map<int,int>::iterator p = first->replica_map.begin();
       p != first->replica_map.end();
       p++) {
    if (mds->mdsmap->get_state(p->first) <= MDSMap::STATE_REJOIN)
      continue;
    MMDSFragmentNotify *notify = new MMDSFragmentNotify(dirfrag, bits);
    mds->send_message_mds(notify, p->first);
  } 
  
  mut->apply();  // mark scatterlock
  mds->locker->drop_locks(mut);
  mut->cleanup();
  delete mut;

  // unfreeze resulting frags
  for (list<CDir*>::iterator p = resultfrags.begin();
       p != resultfrags.end();
       p++) {
    CDir *dir = *p;
    dout(10) << " result frag " << *dir << dendl;
    
    // unmark, unfreeze
    dir->state_clear(CDir::STATE_FRAGMENTING);  

    for (CDir::map_t::iterator p = dir->items.begin();
	 p != dir->items.end();
	 ++p) { 
      CDentry *dn = p->second;
      assert(dn->state_test(CDentry::STATE_FRAGMENTING));
      dn->state_clear(CDentry::STATE_FRAGMENTING);
      dn->put(CDentry::PIN_FRAGMENTING);
    }

    dir->unfreeze_dir();
  }
}


/* This function DOES put the passed message before returning */
void MDCache::handle_fragment_notify(MMDSFragmentNotify *notify)
{
  dout(10) << "handle_fragment_notify " << *notify << " from " << notify->get_source() << dendl;

  if (mds->get_state() < MDSMap::STATE_REJOIN) {
    notify->put();
    return;
  }

  CStripe *stripe = get_dirstripe(notify->get_dirstripe());
  if (stripe) {
    frag_t base = notify->get_frag();
    int bits = notify->get_bits();

    const fragtree_t &dft = stripe->get_fragtree();
    if ((bits < 0 && dft.is_leaf(base)) ||
	(bits > 0 && !dft.is_leaf(base))) {
      dout(10) << " dft " << dft << " state doesn't match " << base << " by " << bits
	       << ", must have found out during resolve/rejoin?  ignoring. " << *stripe << dendl;
      notify->put();
      return;
    }

    // refragment
    list<Context*> waiters;
    list<CDir*> resultfrags;
    adjust_dir_fragments(stripe, base, bits, resultfrags, waiters, false);
    if (g_conf->mds_debug_frag)
      stripe->verify_dirfrags();
    
    /*
    // add new replica dirs values
    bufferlist::iterator p = notify->basebl.begin();
    while (!p.end()) {
      add_replica_dir(p, stripe, waiters);
    */

    mds->queue_waiters(waiters);
  }

  notify->put();
}


void MDCache::rollback_uncommitted_fragments()
{
  dout(10) << "rollback_uncommitted_fragments: " << uncommitted_fragments.size() << " pending" << dendl;
  for (set< pair<dirfrag_t,int> >::iterator p = uncommitted_fragments.begin();
       p != uncommitted_fragments.end();
       ++p) {
    CStripe *stripe = get_dirstripe(p->first.stripe);
    assert(stripe);
    dout(10) << " rolling back " << p->first << " refragment by " << p->second << " bits" << dendl;
    list<CDir*> resultfrags;
    list<Context*> waiters;
    adjust_dir_fragments(stripe, p->first.frag, -p->second,
                         resultfrags, waiters, true);
    if (g_conf->mds_debug_frag)
      stripe->verify_dirfrags();

    EFragment *le = new EFragment(mds->mdlog, EFragment::OP_ROLLBACK,
                                  p->first, p->second);
    mds->mdlog->start_submit_entry(le);
  }
  uncommitted_fragments.clear();
}



// ==============================================================
// debug crap
#if 0
void MDCache::show_subtrees(int dbl)
{
  //dout(10) << "show_subtrees" << dendl;

  if (!g_conf->subsys.should_gather(ceph_subsys_mds, dbl))
    return;  // i won't print anything.

  if (subtrees.empty()) {
    dout(dbl) << "show_subtrees - no subtrees" << dendl;
    return;
  }

  // root frags
  list<CStripe*> basestripes;
  for (set<CInode*>::iterator p = base_inodes.begin();
       p != base_inodes.end();
       p++)
    (*p)->get_stripes(basestripes);
  //dout(15) << "show_subtrees, base dirfrags " << basefrags << dendl;
  dout(15) << "show_subtrees" << dendl;

  // queue stuff
  list<pair<CStripe*,int> > q;
  string indent;
  set<CStripe*> seen;

  // calc max depth
  for (list<CStripe*>::iterator p = basestripes.begin();
       p != basestripes.end();
       ++p)
    q.push_back(make_pair(*p, 0));

  set<CStripe*> subtrees_seen;

  int depth = 0;
  while (!q.empty()) {
    CStripe *stripe = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (subtrees.count(stripe) == 0) continue;

    subtrees_seen.insert(stripe);

    if (d > depth) depth = d;

    // sanity check
    //dout(25) << "saw depth " << d << " " << *dir << dendl;
    if (seen.count(stripe)) dout(0) << "aah, already seen " << *stripe << dendl;
    assert(seen.count(stripe) == 0);
    seen.insert(stripe);

    // nested items?
    if (!subtrees[stripe].empty()) {
      for (set<CStripe*>::iterator p = subtrees[stripe].begin();
	   p != subtrees[stripe].end();
	   ++p) {
	//dout(25) << " saw sub " << **p << dendl;
	q.push_front(make_pair(*p, d+1));
      }
    }
  }


  // print tree
  for (list<CStripe*>::iterator p = basestripes.begin(); p != basestripes.end(); ++p) 
    q.push_back(make_pair(*p, 0));

  while (!q.empty()) {
    CStripe *stripe = q.front().first;
    int d = q.front().second;
    q.pop_front();

    if (subtrees.count(stripe) == 0) continue;

    // adjust indenter
    while ((unsigned)d < indent.size()) 
      indent.resize(d);
    
    // pad
    string pad = "______________________________________";
    pad.resize(depth*2+1-indent.size());
    if (!subtrees[stripe].empty()) 
      pad[0] = '.'; // parent


    string auth;
    if (stripe->is_auth())
      auth = "auth ";
    else
      auth = " rep ";

    char s[10];
    if (stripe->get_stripe_auth().second == CDIR_AUTH_UNKNOWN)
      snprintf(s, sizeof(s), "%2d   ",
               stripe->get_stripe_auth().first);
    else
      snprintf(s, sizeof(s), "%2d,%2d",
               stripe->get_stripe_auth().first,
               stripe->get_stripe_auth().second);

    // print
    dout(dbl) << indent << "|_" << pad << s << " " << auth << *stripe << dendl;

    CInode *diri = stripe->get_inode();
    if (diri->ino() == MDS_INO_ROOT)
      assert(diri == root);
    if (diri->ino() == MDS_INO_CONTAINER)
      assert(diri == container.get_inode());
    if (diri->ino() == MDS_INO_MDSDIR(mds->get_nodeid()))
      assert(diri == myin);
    if (diri->is_stray() && (MDS_INO_STRAY_OWNER(diri->ino()) == mds->get_nodeid()))
      assert(diri == strays[MDS_INO_STRAY_INDEX(diri->ino())]);

    // nested items?
    if (!subtrees[stripe].empty()) {
      // more at my level?
      if (!q.empty() && q.front().second == d)
	indent += "| ";
      else
	indent += "  ";

      for (set<CStripe*>::iterator p = subtrees[stripe].begin();
	   p != subtrees[stripe].end();
	   ++p) 
	q.push_front(make_pair(*p, d+2));
    }
  }

  // verify there isn't stray crap in subtree map
  int lost = 0;
  for (map<CStripe*, set<CStripe*> >::iterator p = subtrees.begin();
       p != subtrees.end();
       ++p) {
    if (subtrees_seen.count(p->first)) continue;
    dout(10) << "*** stray/lost entry in subtree map: " << *p->first << dendl;
    lost++;
  }
  assert(lost == 0);
}
#endif

void MDCache::show_cache()
{
  dout(7) << "show_cache" << dendl;
 
  for (inode_map::iterator it = inodes.begin(); it != inodes.end(); ++it) {
    // unlinked?
    if (!it->second->parent)
      dout(7) << " unlinked " << *it->second << dendl;

    // stripes
    list<CStripe*> stripes;
    it->second->get_stripes(stripes);
    for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
      CStripe *stripe = *s;
      dout(7) << "  stripe " << *stripe << dendl;

      // dirfrags?
      list<CDir*> dfs;
      stripe->get_dirfrags(dfs);
      for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
        CDir *dir = *p;
        dout(7) << "  dirfrag " << *dir << dendl;

        for (CDir::map_t::iterator p = dir->items.begin();
             p != dir->items.end();
             ++p) {
          CDentry *dn = p->second;
          dout(7) << "   dentry " << *dn << dendl;
          CDentry::linkage_t *dnl = dn->get_linkage();
          if (dnl->is_primary() && dnl->get_inode())
            dout(7) << "    inode " << *dnl->get_inode() << dendl;
        }
      }
    }
  }
}


void MDCache::dump_cache(const char *fn)
{
  char deffn[200];
  if (!fn) {
    snprintf(deffn, sizeof(deffn), "cachedump.%d.mds%d", (int)mds->mdsmap->get_epoch(), mds->get_nodeid());
    fn = deffn;
  }

  dout(1) << "dump_cache to " << fn << dendl;

  ofstream myfile;
  myfile.open(fn);
  
  for (inode_map::iterator it = inodes.begin(); it != inodes.end(); ++it) {
    CInode *in = it->second;
    myfile << *in << std::endl;

    list<CStripe*> stripes;
    in->get_stripes(stripes);
    for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
      CStripe *stripe = *s;
      myfile << "  " << *stripe << std::endl;

      list<CDir*> dfs;
      stripe->get_dirfrags(dfs);
      for (list<CDir*>::iterator p = dfs.begin(); p != dfs.end(); ++p) {
        CDir *dir = *p;
        myfile << "  " << *dir << std::endl;

        for (CDir::map_t::iterator q = dir->items.begin();
             q != dir->items.end();
             ++q) {
          CDentry *dn = q->second;
          myfile << "   " << *dn << std::endl;
        }
      }
    }
  }

  myfile.close();
}



C_MDS_RetryRequest::C_MDS_RetryRequest(MDCache *c, MDRequest *r)
  : cache(c), mdr(r)
{
  mdr->get();
}

void C_MDS_RetryRequest::finish(int r)
{
  cache->dispatch_request(mdr);
  mdr->put();
}
