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

#include "common/config.h"
#include "osdc/Journaler.h"
#include "events/EString.h"
#include "events/ESubtreeMap.h"
#include "events/ESession.h"
#include "events/ESessions.h"

#include "events/EMetaBlob.h"
#include "events/EResetJournal.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"


#include "LogSegment.h"

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Server.h"
#include "Migrator.h"
#include "Mutation.h"

#include "InoTable.h"
#include "MDSTableClient.h"
#include "MDSTableServer.h"

#include "Locker.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log \
			      || l <= cct->_conf->debug_mds_log_expire)
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".journal "


// -----------------------
// LogSegment

void LogSegment::try_to_expire(MDS *mds, C_GatherBuilder &gather_bld)
{
  set<CDir*> commit;

  dout(6) << "LogSegment(" << offset << ").try_to_expire" << dendl;

  // commit dirs
  for (elist<CDir*>::iterator p = new_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " new_dirfrag " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDir*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dirfrag " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDentry*>::iterator p = dirty_dentries.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dentry " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert((*p)->get_dir());
  }
  for (elist<CInode*>::iterator p = dirty_inodes.begin(); !p.end(); ++p) {
    dout(20) << " dirty_inode " << **p << dendl;
    assert((*p)->is_auth());
    if ((*p)->is_base()) {
      (*p)->store(gather_bld.new_sub());
    } else
      commit.insert((*p)->get_parent_dn()->get_dir());
  }

  if (!commit.empty()) {
    for (set<CDir*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p) {
      CDir *dir = *p;
      assert(dir->is_auth());
      if (dir->can_auth_pin()) {
	dout(15) << "try_to_expire committing " << *dir << dendl;
	dir->commit(0, gather_bld.new_sub());
      } else {
	dout(15) << "try_to_expire waiting for unfreeze on " << *dir << dendl;
	dir->add_waiter(CDir::WAIT_UNFREEZE, gather_bld.new_sub());
      }
    }
  }

  // master ops with possibly uncommitted slaves
  for (set<metareqid_t>::iterator p = uncommitted_masters.begin();
       p != uncommitted_masters.end();
       p++) {
    dout(10) << "try_to_expire waiting for slaves to ack commit on " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_master(*p, gather_bld.new_sub());
  }

  // open files
  if (!open_files.empty()) {
    assert(!mds->mdlog->is_capped()); // hmm FIXME
    EOpen *le = 0;
    LogSegment *ls = mds->mdlog->get_current_segment();
    assert(ls != this);
    elist<CInode*>::iterator p = open_files.begin(member_offset(CInode, item_open_file));
    while (!p.end()) {
      CInode *in = *p;
      assert(in->last == CEPH_NOSNAP);
      ++p;
      if (in->is_auth() && in->is_any_caps()) {
	if (in->is_any_caps_wanted()) {
	  dout(20) << "try_to_expire requeueing open file " << *in << dendl;
	  if (!le) {
	    le = new EOpen(mds->mdlog);
	    mds->mdlog->start_entry(le);
	  }
	  le->add_clean_inode(in);
	  ls->open_files.push_back(&in->item_open_file);
	} else {
	  // drop inodes that aren't wanted
	  dout(20) << "try_to_expire not requeueing and delisting unwanted file " << *in << dendl;
	  in->item_open_file.remove_myself();
	}
      } else {
	/*
	 * we can get a capless inode here if we replay an open file, the client fails to
	 * reconnect it, but does REPLAY an open request (that adds it to the logseg).  AFAICS
	 * it's ok for the client to replay an open on a file it doesn't have in it's cache
	 * anymore.
	 *
	 * this makes the mds less sensitive to strict open_file consistency, although it does
	 * make it easier to miss subtle problems.
	 */
	dout(20) << "try_to_expire not requeueing and delisting capless file " << *in << dendl;
	in->item_open_file.remove_myself();
      }
    }
    if (le) {
      mds->mdlog->submit_entry(le, gather_bld.new_sub());
      dout(10) << "try_to_expire waiting for open files to rejournal" << dendl;
    }
  }

  // parent pointers on renamed dirs
  for (elist<CInode*>::iterator p = renamed_files.begin(); !p.end(); ++p) {
    CInode *in = *p;
    dout(10) << "try_to_expire waiting for dir parent pointer update on " << *in << dendl;
    assert(in->state_test(CInode::STATE_DIRTYPARENT));
    in->store_parent(gather_bld.new_sub());
  }

  // slave updates
  for (elist<MDSlaveUpdate*>::iterator p = slave_updates.begin(member_offset(MDSlaveUpdate,
									     item));
       !p.end(); ++p) {
    MDSlaveUpdate *su = *p;
    dout(10) << "try_to_expire waiting on slave update " << su << dendl;
    assert(su->waiter == 0);
    su->waiter = gather_bld.new_sub();
  }

  // idalloc
  if (inotablev > mds->inotable->get_committed_version()) {
    dout(10) << "try_to_expire saving inotable table, need " << inotablev
	      << ", committed is " << mds->inotable->get_committed_version()
	      << " (" << mds->inotable->get_committing_version() << ")"
	      << dendl;
    mds->inotable->save(gather_bld.new_sub(), inotablev);
  }

  // sessionmap
  if (sessionmapv > mds->sessionmap.committed) {
    dout(10) << "try_to_expire saving sessionmap, need " << sessionmapv 
	      << ", committed is " << mds->sessionmap.committed
	      << " (" << mds->sessionmap.committing << ")"
	      << dendl;
    mds->sessionmap.save(gather_bld.new_sub(), sessionmapv);
  }

  // pending commit atids
  for (map<int, hash_set<version_t> >::iterator p = pending_commit_tids.begin();
       p != pending_commit_tids.end();
       ++p) {
    MDSTableClient *client = mds->get_table_client(p->first);
    for (hash_set<version_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(10) << "try_to_expire " << get_mdstable_name(p->first) << " transaction " << *q 
	       << " pending commit (not yet acked), waiting" << dendl;
      assert(!client->has_committed(*q));
      client->wait_for_ack(*q, gather_bld.new_sub());
    }
  }
  
  // table servers
  for (map<int, version_t>::iterator p = tablev.begin();
       p != tablev.end();
       p++) {
    MDSTableServer *server = mds->get_table_server(p->first);
    if (p->second > server->get_committed_version()) {
      dout(10) << "try_to_expire waiting for " << get_mdstable_name(p->first) 
	       << " to save, need " << p->second << dendl;
      server->save(gather_bld.new_sub());
    }
  }

  // truncating
  for (set<CInode*>::iterator p = truncating_inodes.begin();
       p != truncating_inodes.end();
       p++) {
    dout(10) << "try_to_expire waiting for truncate of " << **p << dendl;
    (*p)->add_waiter(CInode::WAIT_TRUNC, gather_bld.new_sub());
  }
  
  // FIXME client requests...?
  // audit handling of anchor transactions?

  if (gather_bld.has_subs()) {
    dout(6) << "LogSegment(" << offset << ").try_to_expire waiting" << dendl;
    mds->mdlog->flush();
  } else {
    dout(6) << "LogSegment(" << offset << ").try_to_expire success" << dendl;
  }
}


#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log)


// -----------------------
// EString

void EString::replay(MDS *mds)
{
  dout(10) << "EString.replay " << event << dendl; 
}



// -----------------------
// EMetaBlob

EMetaBlob::EMetaBlob(MDLog *mdlog)
  : opened_ino(0), renamed_dirino(0), inotablev(0), sessionmapv(0), allocated_ino(0),
    last_subtree_map(mdlog ? mdlog->get_last_segment_offset() : 0),
    my_offset(mdlog ? mdlog->get_write_pos() : 0)
{
}

void EMetaBlob::add_stripe_context(CStripe *stripe, int mode)
{
  MDS *mds = stripe->get_inode()->mdcache->mds;

  list<CDentry*> parents;

  while (true) {
    // already have this stripe?  (we must always add in order)
    if (stripes.count(stripe->dirstripe())) {
      dout(20) << "EMetaBlob::add_stripe_context(" << stripe
          << ") have lump " << stripe->dirstripe() << dendl;
      break;
    }

    // stop at root/stray
    CInode *diri = stripe->get_inode();
    CDentry *parent = diri->get_projected_parent_dn();

    if (!parent)
      break;

    if (mode == TO_AUTH_SUBTREE_ROOT) {
      // was the inode journaled in this blob?
      if (my_offset && diri->last_journaled == my_offset) {
	dout(20) << "EMetaBlob::add_stripe_context(" << stripe
            << ") already have diri this blob " << *diri << dendl;
	break;
      }
    }

    dout(25) << "EMetaBlob::add_stripe_context(" << stripe
        << ") definitely " << *parent << dendl;
    parents.push_front(parent);

    stripe = parent->get_stripe();
  }

  dout(20) << "EMetaBlob::add_stripe_context final: " << parents << dendl;
  for (list<CDentry*>::iterator p = parents.begin(); p != parents.end(); p++) {
    assert((*p)->get_projected_linkage()->is_primary());
    add_dentry(*p, false);
  }
}

void EMetaBlob::update_segment(LogSegment *ls)
{
  if (inotablev)
    ls->inotablev = inotablev;
  if (sessionmapv)
    ls->sessionmapv = sessionmapv;
}

void EMetaBlob::Inode::apply(MDS *mds, CInode *in, bool isnew)
{
  list<inoparent_t> existing_parents;
  if (!isnew)
    swap(in->inode.parents, existing_parents);

  in->inode = inode;

  if (!isnew) {
    // apply added/removed parents to existing parents
    swap(in->inode.parents, existing_parents);
    update_inoparents(in->inode.parents, removed_parent, added_parent);
  }
  dout(10) << "inode " << in->ino() << " links " << inode.nlink
      << " inoparents " << in->inode.parents << dendl;

  in->inode_auth = inode_auth;
  if (inode_auth.first == mds->get_nodeid())
    in->state_set(CInode::STATE_AUTH);
  else
    in->state_clear(CInode::STATE_AUTH);
  in->xattrs = xattrs;
  if (in->inode.is_dir()) {
    in->set_stripe_auth(stripe_auth);

    delete in->default_layout;
    in->default_layout = dir_layout;
    dir_layout = NULL;
  } else if (in->inode.is_symlink()) {
    in->symlink = symlink;
  }
  in->old_inodes = old_inodes;

  if (in->is_auth() && inode.rstat.version != inode.accounted_rstat.version)
    mds->mdcache->parentstats.replay_unaccounted(in);

  assert(in->is_base() || static_cast<size_t>(inode.nlink) == in->inode.parents.size());
}

void EMetaBlob::Dir::apply(MDS *mds, CDir *dir, LogSegment *ls)
{
  dir->set_version(version);

  if (is_dirty())
    dir->_mark_dirty(ls);
  if (is_new())
    dir->mark_new(ls);
  if (is_complete())
    dir->mark_complete();

  dout(10) << "EMetaBlob.replay updated dir " << *dir << dendl;
}

static CStripe* open_stripe(MDS *mds, dirstripe_t ds)
{
  // find/create the inode
  CInode *diri = mds->mdcache->get_inode(ds.ino);
  if (!diri) {
    if (MDS_INO_IS_BASE(ds.ino)) {
      int auth;
      if (MDS_INO_IS_MDSDIR(ds.ino))
        auth = ds.ino - MDS_INO_MDSDIR_OFFSET;
      else
        auth = mds->mdsmap->get_root();
      diri = mds->mdcache->create_system_inode(ds.ino, auth, S_IFDIR|0755);
      dout(10) << "EMetaBlob.replay created base " << *diri << dendl;
    } else {
      dout(0) << "EMetaBlob.replay missing dir ino " << ds.ino << dendl;
      assert(diri);
    }
  }

  // find/create the stripe
  CStripe *stripe = diri->get_stripe(ds.stripeid);
  if (stripe) {
    dout(10) << "EMetaBlob had " << *stripe << dendl;
  } else {
    int auth = diri->get_stripe_auth(ds.stripeid);
    stripe = diri->add_stripe(new CStripe(diri, ds.stripeid, auth));
    dout(10) << "EMetaBlob added " << *stripe << dendl;
  }
  return stripe;
}

void EMetaBlob::Stripe::apply(MDS *mds, CStripe *stripe, LogSegment *ls)
{
  stripe->set_stripe_auth(auth);
  if (auth.first == mds->get_nodeid())
    stripe->state_set(CStripe::STATE_AUTH);
  else
    stripe->state_clear(CStripe::STATE_AUTH);
  stripe->fnode = fnode;
  if (is_open())
    stripe->mark_open();
  if (is_dirty()) {
    stripe->_mark_dirty(ls);

    if (stripe->is_auth() &&
        (fnode.rstat.version != fnode.accounted_rstat.version ||
        fnode.fragstat.version != fnode.accounted_fragstat.version))
      mds->mdcache->parentstats.replay_unaccounted(stripe);
  }
  if (is_new())
    stripe->mark_new(ls);
  if (is_unlinked())
    stripe->state_set(CStripe::STATE_UNLINKED);
  stripe->set_fragtree(dirfragtree);
  stripe->force_dirfrags();
  dout(10) << "EMetaBlob updated stripe " << *stripe << dendl;
}

void EMetaBlob::replay(MDS *mds, LogSegment *logseg, MDSlaveUpdate *slaveup)
{
  dout(10) << "EMetaBlob.replay " << inodes.size() << " inodes, "
      << stripes.size() << " stripes by " << client_name << dendl;

  assert(logseg);

  // replay all inodes
  for (inode_map::iterator p = inodes.begin(); p != inodes.end(); ++p) {
    CInode *in = mds->mdcache->get_inode(p->first);
    bool isnew = in ? false:true;
    if (!in)
      in = new CInode(mds->mdcache, mds->get_nodeid());
    p->second.apply(mds, in, isnew);
    if (isnew)
      mds->mdcache->add_inode(in);
    if (p->second.dirty) in->_mark_dirty(logseg);
    dout(10) << "EMetaBlob.replay " << (isnew ? " added ":" updated ") << *in << dendl;
  }

  // replay stripes->dirfrags->dentries
  for (stripe_map::iterator s = stripes.begin(); s != stripes.end(); ++s) {
    // open the stripe
    CStripe *stripe = open_stripe(mds, s->first);
    assert(stripe);

    s->second.apply(mds, stripe, logseg);

    dir_map &dirs = s->second.get_dirs();
    for (dir_map::iterator f = dirs.begin(); f != dirs.end(); ++f) {
      // open the fragment
      CDir *dir = stripe->get_dirfrag(f->first);
      if (!dir) {
        dir = stripe->get_or_open_dirfrag(f->first);
        dout(10) << "EMetaBlob.replay added " << *dir << dendl;
      } else {
        dout(10) << "EMetaBlob.replay had " << *dir << dendl;
      }

      f->second.apply(mds, dir, logseg);

      dentry_vec &dns = f->second.get_dentries();
      for (dentry_vec::iterator d = dns.begin(); d != dns.end(); ++d) {
        // open the dentry
        CDentry *dn = dir->lookup_exact_snap(d->name, d->last);
        if (!dn) {
          dn = dir->add_null_dentry(d->name, d->first, d->last);
          dn->set_version(d->version);
          if (d->dirty) dn->_mark_dirty(logseg);
          dout(10) << "EMetaBlob.replay added " << *dn << dendl;
        } else {
          dn->set_version(d->version);
          if (d->dirty) dn->_mark_dirty(logseg);
          dout(10) << "EMetaBlob.replay had " << *dn << dendl;
          dn->first = d->first;
          assert(dn->last == d->last);
        }

        // update linkage
        CDentry::linkage_t *dnl = dn->get_linkage();
        if (dnl->is_null()) {
          assert(d->ino); // don't journal a null dentry unless unlinking
          CInode *in = mds->mdcache->get_inode(d->ino, d->last);
          if (s->first.ino == MDS_INO_CONTAINER) {
            // primary links in inode container
            assert(in);
            dir->link_primary_inode(dn, in);
          } else if (in)
            dir->link_remote_inode(dn, in);
          else
            dir->link_remote_inode(dn, d->ino, d->d_type);
        } else if (d->ino == 0) {
          dir->unlink_inode(dn);
          // TODO: track unlinked
        } else if (dnl->is_primary())
          assert(d->ino == dnl->get_inode()->ino());
        else if (d->ino != dnl->get_remote_ino())
          dnl->set_remote(d->ino, d->d_type);
      }
    }
  }

  // table client transactions
  for (list<pair<__u8,version_t> >::iterator p = table_tids.begin();
       p != table_tids.end();
       ++p) {
    dout(10) << "EMetaBlob.replay noting " << get_mdstable_name(p->first)
	     << " transaction " << p->second << dendl;
    MDSTableClient *client = mds->get_table_client(p->first);
    client->got_journaled_agree(p->second, logseg);
  }

  // opened ino?
  if (opened_ino) {
    CInode *in = mds->mdcache->get_inode(opened_ino);
    assert(in);
    dout(10) << "EMetaBlob.replay noting opened inode " << *in << dendl;
    logseg->open_files.push_back(&in->item_open_file);
  }

  // allocated_inos
  if (inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "EMetaBlob.replay inotable tablev " << inotablev
	       << " <= table " << mds->inotable->get_version() << dendl;
    } else {
      dout(10) << "EMetaBlob.replay inotable v " << inotablev
	       << " - 1 == table " << mds->inotable->get_version()
	       << " allocated+used " << allocated_ino
	       << " prealloc " << preallocated_inos
	       << dendl;
      if (allocated_ino)
	mds->inotable->replay_alloc_id(allocated_ino);
      if (preallocated_inos.size())
	mds->inotable->replay_alloc_ids(preallocated_inos);

      // [repair bad inotable updates]
      if (inotablev > mds->inotable->get_version()) {
	mds->clog.error() << "journal replay inotablev mismatch "
	    << mds->inotable->get_version() << " -> " << inotablev << "\n";
	mds->inotable->force_replay_version(inotablev);
      }

      assert(inotablev == mds->inotable->get_version());
    }
  }
  if (sessionmapv) {
    if (mds->sessionmap.version >= sessionmapv) {
      dout(10) << "EMetaBlob.replay sessionmap v " << sessionmapv
	       << " <= table " << mds->sessionmap.version << dendl;
    } else {
      dout(10) << "EMetaBlob.replay sessionmap v" << sessionmapv
	       << " -(1|2) == table " << mds->sessionmap.version
	       << " prealloc " << preallocated_inos
	       << " used " << used_preallocated_ino
	       << dendl;
      Session *session = mds->sessionmap.get_session(client_name);
      assert(session);
      dout(20) << " (session prealloc " << session->prealloc_inos << ")" << dendl;
      if (used_preallocated_ino) {
	if (session->prealloc_inos.empty()) {
	  // HRM: badness in the journal
	  mds->clog.warn() << " replayed op " << client_reqs << " on session for " << client_name
			   << " with empty prealloc_inos\n";
	} else {
	  inodeno_t next = session->next_ino();
	  inodeno_t i = session->take_ino(used_preallocated_ino);
	  if (next != i)
	    mds->clog.warn() << " replayed op " << client_reqs << " used ino " << i
			     << " but session next is " << next << "\n";
	  assert(i == used_preallocated_ino);
	  session->used_inos.clear();
	}
	mds->sessionmap.projected = ++mds->sessionmap.version;
      }
      if (preallocated_inos.size()) {
	session->prealloc_inos.insert(preallocated_inos);
	mds->sessionmap.projected = ++mds->sessionmap.version;
      }
      assert(sessionmapv == mds->sessionmap.version);
    }
  }

  // truncating inodes
  for (list<inodeno_t>::iterator p = truncate_start.begin();
       p != truncate_start.end();
       p++) {
    CInode *in = mds->mdcache->get_inode(*p);
    assert(in);
    mds->mdcache->add_recovered_truncate(in, logseg);
  }
  for (map<inodeno_t,uint64_t>::iterator p = truncate_finish.begin();
       p != truncate_finish.end();
       p++) {
    LogSegment *ls = mds->mdlog->get_segment(p->second);
    if (ls) {
      CInode *in = mds->mdcache->get_inode(p->first);
      assert(in);
      mds->mdcache->remove_recovered_truncate(in, ls);
    }
  }

  // destroyed inodes
  for (vector<inodeno_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       p++) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (in) {
      dout(10) << "EMetaBlob.replay destroyed " << *p << ", dropping " << *in << dendl;
      mds->mdcache->remove_inode(in);
    } else {
      dout(10) << "EMetaBlob.replay destroyed " << *p << ", not in cache" << dendl;
    }
  }

  // client requests
  for (list<pair<metareqid_t, uint64_t> >::iterator p = client_reqs.begin();
       p != client_reqs.end();
       ++p)
    if (p->first.name.is_client()) {
      dout(10) << "EMetaBlob.replay request " << p->first << " " << p->second << dendl;
      if (mds->sessionmap.have_session(p->first.name))
	mds->sessionmap.add_completed_request(p->first, p->second);
    }


  // update segment
  update_segment(logseg);
}

// -----------------------
// ESession

void ESession::update_segment()
{
  _segment->sessionmapv = cmapv;
  if (inos.size() && inotablev)
    _segment->inotablev = inotablev;
}

void ESession::replay(MDS *mds)
{
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.version 
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESession.replay sessionmap " << mds->sessionmap.version
	     << " < " << cmapv << " " << (open ? "open":"close") << " " << client_inst << dendl;
    mds->sessionmap.projected = ++mds->sessionmap.version;
    assert(mds->sessionmap.version == cmapv);
    Session *session;
    if (open) {
      session = mds->sessionmap.get_or_add_session(client_inst);
      mds->sessionmap.set_state(session, Session::STATE_OPEN);
      dout(10) << " opened session " << session->inst << dendl;
    } else {
      session = mds->sessionmap.get_session(client_inst.name);
      if (session) { // there always should be a session, but there's a bug
	if (session->connection == NULL) {
	  dout(10) << " removed session " << session->inst << dendl;
	  mds->sessionmap.remove_session(session);
	} else {
	  session->clear();    // the client has reconnected; keep the Session, but reset
	  dout(10) << " reset session " << session->inst << " (they reconnected)" << dendl;
	}
      } else {
	mds->clog.error() << "replayed stray Session close event for " << client_inst
			  << " from time " << stamp << ", ignoring";
      }
    }
  }
  
  if (inos.size() && inotablev) {
    if (mds->inotable->get_version() >= inotablev) {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " >= " << inotablev << ", noop" << dendl;
    } else {
      dout(10) << "ESession.replay inotable " << mds->inotable->get_version()
	       << " < " << inotablev << " " << (open ? "add":"remove") << dendl;
      assert(!open);  // for now
      mds->inotable->replay_release_ids(inos);
      assert(mds->inotable->get_version() == inotablev);
    }
  }

  update_segment();
}

void ESessions::update_segment()
{
  _segment->sessionmapv = cmapv;
}

void ESessions::replay(MDS *mds)
{
  if (mds->sessionmap.version >= cmapv) {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.version
	     << " >= " << cmapv << ", noop" << dendl;
  } else {
    dout(10) << "ESessions.replay sessionmap " << mds->sessionmap.version
	     << " < " << cmapv << dendl;
    mds->sessionmap.open_sessions(client_map);
    assert(mds->sessionmap.version == cmapv);
    mds->sessionmap.projected = mds->sessionmap.version;
  }
  update_segment();
}




void ETableServer::update_segment()
{
  _segment->tablev[table] = version;
}

void ETableServer::replay(MDS *mds)
{
  MDSTableServer *server = mds->get_table_server(table);
  if (server->get_version() >= version) {
    dout(10) << "ETableServer.replay " << get_mdstable_name(table)
	     << " " << get_mdstableserver_opname(op)
	     << " event " << version
	     << " <= table " << server->get_version() << dendl;
    return;
  }
  
  dout(10) << " ETableServer.replay " << get_mdstable_name(table)
	   << " " << get_mdstableserver_opname(op)
	   << " event " << version << " - 1 == table " << server->get_version() << dendl;
  assert(version-1 == server->get_version());

  switch (op) {
  case TABLESERVER_OP_PREPARE:
    server->_prepare(mutation, reqid, bymds);
    server->_note_prepare(bymds, reqid);
    break;
  case TABLESERVER_OP_COMMIT:
    server->_commit(tid);
    server->_note_commit(tid);
    break;
  case TABLESERVER_OP_ROLLBACK:
    server->_rollback(tid);
    server->_note_rollback(tid);
    break;
  case TABLESERVER_OP_SERVER_UPDATE:
    server->_server_update(mutation);
    break;
  default:
    assert(0);
  }
  
  assert(version == server->get_version());
  update_segment();
}


void ETableClient::replay(MDS *mds)
{
  dout(10) << " ETableClient.replay " << get_mdstable_name(table)
	   << " op " << get_mdstableserver_opname(op)
	   << " tid " << tid << dendl;
    
  MDSTableClient *client = mds->get_table_client(table);
  assert(op == TABLESERVER_OP_ACK);
  client->got_journaled_ack(tid);
}


// -----------------------
// ESnap
/*
void ESnap::update_segment()
{
  _segment->tablev[TABLE_SNAP] = version;
}

void ESnap::replay(MDS *mds)
{
  if (mds->snaptable->get_version() >= version) {
    dout(10) << "ESnap.replay event " << version
	     << " <= table " << mds->snaptable->get_version() << dendl;
    return;
  } 
  
  dout(10) << " ESnap.replay event " << version
	   << " - 1 == table " << mds->snaptable->get_version() << dendl;
  assert(version-1 == mds->snaptable->get_version());

  if (create) {
    version_t v;
    snapid_t s = mds->snaptable->create(snap.dirino, snap.name, snap.stamp, &v);
    assert(s == snap.snapid);
  } else {
    mds->snaptable->remove(snap.snapid);
  }

  assert(version == mds->snaptable->get_version());
}
*/



// -----------------------
// EUpdate

void EUpdate::update_segment()
{
  metablob.update_segment(_segment);

  if (had_slaves)
    _segment->uncommitted_masters.insert(reqid);
}

void EUpdate::replay(MDS *mds)
{
  metablob.replay(mds, _segment);
  
  if (had_slaves) {
    dout(10) << "EUpdate.replay " << reqid << " had slaves, expecting a matching ECommitted" << dendl;
    _segment->uncommitted_masters.insert(reqid);
    set<int> slaves;
    mds->mdcache->add_uncommitted_master(reqid, _segment, slaves);
  }
  
  if (client_map.length()) {
    if (mds->sessionmap.version >= cmapv) {
      dout(10) << "EUpdate.replay sessionmap v " << cmapv
	       << " <= table " << mds->sessionmap.version << dendl;
    } else {
      dout(10) << "EUpdate.replay sessionmap " << mds->sessionmap.version
	       << " < " << cmapv << dendl;
      // open client sessions?
      map<client_t,entity_inst_t> cm;
      map<client_t, uint64_t> seqm;
      bufferlist::iterator blp = client_map.begin();
      ::decode(cm, blp);
      mds->server->prepare_force_open_sessions(cm, seqm);
      mds->server->finish_force_open_sessions(cm, seqm);

      assert(mds->sessionmap.version = cmapv);
      mds->sessionmap.projected = mds->sessionmap.version;
    }
  }
}


// ------------------------
// EOpen

void EOpen::update_segment()
{
  // ??
}

void EOpen::replay(MDS *mds)
{
  dout(10) << "EOpen.replay " << dendl;
  metablob.replay(mds, _segment);

  // note which segments inodes belong to, so we don't have to start rejournaling them
  for (vector<inodeno_t>::iterator p = inos.begin();
       p != inos.end();
       p++) {
    CInode *in = mds->mdcache->get_inode(*p);
    if (!in) {
      dout(0) << "EOpen.replay ino " << *p << " not in metablob" << dendl;
      assert(in);
    }
    _segment->open_files.push_back(&in->item_open_file);
  }
}


// -----------------------
// ECommitted

void ECommitted::replay(MDS *mds)
{
  if (mds->mdcache->uncommitted_masters.count(reqid)) {
    dout(10) << "ECommitted.replay " << reqid << dendl;
    mds->mdcache->uncommitted_masters[reqid].ls->uncommitted_masters.erase(reqid);
    mds->mdcache->uncommitted_masters.erase(reqid);
  } else {
    dout(10) << "ECommitted.replay " << reqid << " -- didn't see original op" << dendl;
  }
}



// -----------------------
// ESlaveUpdate

void ESlaveUpdate::replay(MDS *mds)
{
  MDSlaveUpdate *su;
  switch (op) {
  case ESlaveUpdate::OP_PREPARE:
    dout(10) << "ESlaveUpdate.replay prepare " << reqid << " for mds." << master 
	     << ": applying commit, saving rollback info" << dendl;
    su = new MDSlaveUpdate(origop, rollback, _segment->slave_updates);
    commit.replay(mds, _segment, su);
    mds->mdcache->add_uncommitted_slave_update(reqid, master, su);
    break;

  case ESlaveUpdate::OP_COMMIT:
    su = mds->mdcache->get_uncommitted_slave_update(reqid, master);
    if (su) {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds." << master << dendl;
      mds->mdcache->finish_uncommitted_slave_update(reqid, master);
    } else {
      dout(10) << "ESlaveUpdate.replay commit " << reqid << " for mds." << master 
	       << ": ignoring, no previously saved prepare" << dendl;
    }
    break;

  case ESlaveUpdate::OP_ROLLBACK:
    dout(10) << "ESlaveUpdate.replay abort " << reqid << " for mds." << master
	     << ": applying rollback commit blob" << dendl;
    commit.replay(mds, _segment);
    su = mds->mdcache->get_uncommitted_slave_update(reqid, master);
    if (su)
      mds->mdcache->finish_uncommitted_slave_update(reqid, master);
    break;

  default:
    assert(0);
  }
}


// -----------------------
// EFragment

void EFragment::replay(MDS *mds)
{
  dout(10) << "EFragment.replay " << op_name(op) << " " << dirfrag << " by " << bits << dendl;

  list<CDir*> resultfrags;
  list<Context*> waiters;
  pair<dirfrag_t,int> desc(dirfrag, bits);

  // in may be NULL if it wasn't in our cache yet.  if it's a prepare
  // it will be once we replay the metablob , but first we need to
  // refragment anything we already have in the cache.
  CStripe *stripe = mds->mdcache->get_dirstripe(dirfrag.stripe);

  switch (op) {
  case OP_PREPARE:
    mds->mdcache->uncommitted_fragments.insert(desc);
    // fall-thru
  case OP_ONESHOT:
    if (stripe)
      mds->mdcache->adjust_dir_fragments(stripe, dirfrag.frag, bits, resultfrags, waiters, true);
    break;

  case OP_COMMIT:
    mds->mdcache->uncommitted_fragments.erase(desc);
    break;

  case OP_ROLLBACK:
    if (mds->mdcache->uncommitted_fragments.count(desc)) {
      mds->mdcache->uncommitted_fragments.erase(desc);
      assert(stripe);
      mds->mdcache->adjust_dir_fragments(stripe, dirfrag.frag, -bits, resultfrags, waiters, true);
    } else {
      dout(10) << " no record of prepare for " << desc << dendl;
    }
    break;
  }
  metablob.replay(mds, _segment);
  if (stripe && g_conf->mds_debug_frag)
    stripe->verify_dirfrags();
}


// ------------------------
// EResetJournal

void EResetJournal::replay(MDS *mds)
{
  dout(1) << "EResetJournal" << dendl;

  mds->sessionmap.wipe();
  mds->inotable->replay_reset();

  if (mds->mdsmap->get_root() == mds->whoami) {
    CStripe *rootstripe = mds->mdcache->get_root()->get_or_open_stripe(0);
    rootstripe->set_stripe_auth(mds->whoami);
  }

  CStripe *mystripe = mds->mdcache->get_myin()->get_or_open_stripe(0);
  mystripe->set_stripe_auth(mds->whoami);
}

