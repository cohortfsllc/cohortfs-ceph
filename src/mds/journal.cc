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
#include "events/ESubtreeMap.h"
#include "events/ESession.h"
#include "events/ESessions.h"

#include "events/EMetaBlob.h"
#include "events/EResetJournal.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "events/EFragment.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"

#include "include/stringify.h"

#include "LogSegment.h"

#include "MDS.h"
#include "MDLog.h"
#include "MDCache.h"
#include "Server.h"
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
  set<CDirFrag*> commit;
  set<CDirStripe*> stripes;

  dout(6) << "LogSegment(" << offset << ").try_to_expire" << dendl;

  assert(g_conf->mds_kill_journal_expire_at != 1);

  // commit dirs
  for (elist<CDirFrag*>::iterator p = new_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " new_dirfrag " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDirFrag*>::iterator p = dirty_dirfrags.begin(); !p.end(); ++p) {
    dout(20) << " dirty_dirfrag " << **p << dendl;
    assert((*p)->is_auth());
    commit.insert(*p);
  }
  for (elist<CDirStripe*>::iterator p = new_stripes.begin(); !p.end(); ++p) {
    dout(20) << " new_stripe " << **p << dendl;
    assert((*p)->is_auth());
    stripes.insert(*p);
  }
  for (elist<CDirStripe*>::iterator p = dirty_stripes.begin(); !p.end(); ++p) {
    dout(20) << " dirty_stripe " << **p << dendl;
    assert((*p)->is_auth());
    stripes.insert(*p);
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
    for (set<CDirFrag*>::iterator p = commit.begin();
	 p != commit.end();
	 ++p) {
      CDirFrag *dir = *p;
      assert(dir->is_auth());
      if (dir->can_auth_pin()) {
	dout(15) << "try_to_expire committing " << *dir << dendl;
	dir->commit(0, gather_bld.new_sub());
      } else {
	dout(15) << "try_to_expire waiting for unfreeze on " << *dir << dendl;
	dir->add_waiter(CDirFrag::WAIT_UNFREEZE, gather_bld.new_sub());
      }
    }
  }

  if (!stripes.empty()) {
    for (set<CDirStripe*>::iterator p = stripes.begin();
	 p != stripes.end();
	 ++p) {
      CDirStripe *stripe = *p;
      assert(stripe->is_auth());
      if (stripe->can_auth_pin()) {
	dout(15) << "try_to_expire committing " << *stripe << dendl;
	stripe->commit(gather_bld.new_sub());
      } else {
	dout(15) << "try_to_expire waiting for unfreeze on " << *stripe << dendl;
	stripe->add_waiter(CDirStripe::WAIT_UNFREEZE, gather_bld.new_sub());
      }
    }
  }

  // master ops with possibly uncommitted slaves
  for (set<metareqid_t>::iterator p = uncommitted_masters.begin();
       p != uncommitted_masters.end();
       ++p) {
    dout(10) << "try_to_expire waiting for slaves to ack commit on " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_master(*p, gather_bld.new_sub());
  }

  // uncommitted fragments
  for (set<dirfrag_t>::iterator p = uncommitted_fragments.begin();
       p != uncommitted_fragments.end();
       ++p) {
    dout(10) << "try_to_expire waiting for uncommitted fragment " << *p << dendl;
    mds->mdcache->wait_for_uncommitted_fragment(*p, gather_bld.new_sub());
  }

  assert(g_conf->mds_kill_journal_expire_at != 2);

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

  assert(g_conf->mds_kill_journal_expire_at != 3);

  // backtraces to be stored/updated
  for (elist<CInode*>::iterator p = dirty_parent_inodes.begin(); !p.end(); ++p) {
    CInode *in = *p;
    assert(in->is_auth());
    if (in->can_auth_pin()) {
      dout(15) << "try_to_expire waiting for storing backtrace on " << *in << dendl;
      in->store_backtrace(gather_bld.new_sub());
    } else {
      dout(15) << "try_to_expire waiting for unfreeze on " << *in << dendl;
      in->add_waiter(CInode::WAIT_UNFREEZE, gather_bld.new_sub());
    }
  }

  assert(g_conf->mds_kill_journal_expire_at != 4);

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
       ++p) {
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
       ++p) {
    dout(10) << "try_to_expire waiting for truncate of " << **p << dendl;
    (*p)->add_waiter(CInode::WAIT_TRUNC, gather_bld.new_sub());
  }
  
  // FIXME client requests...?
  // audit handling of anchor transactions?

  if (gather_bld.has_subs()) {
    dout(6) << "LogSegment(" << offset << ").try_to_expire waiting" << dendl;
    mds->mdlog->flush();
  } else {
    assert(g_conf->mds_kill_journal_expire_at != 5);
    dout(6) << "LogSegment(" << offset << ").try_to_expire success" << dendl;
  }
}

#undef DOUT_COND
#define DOUT_COND(cct, l) (l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_log)


// -----------------------
// EMetaBlob

EMetaBlob::EMetaBlob(MDLog *mdlog)
  : opened_ino(0), renamed_dirino(0), inotablev(0), sessionmapv(0), allocated_ino(0),
    last_subtree_map(mdlog ? mdlog->get_last_segment_offset() : 0),
    my_offset(mdlog ? mdlog->get_write_pos() : 0)
{
}

void EMetaBlob::update_segment(LogSegment *ls)
{
  if (inotablev)
    ls->inotablev = inotablev;
  if (sessionmapv)
    ls->sessionmapv = sessionmapv;
}

// EMetaBlob::Inode

void EMetaBlob::Inode::encode(const inode_t &i, const pair<int, int> &iauth,
                              const vector<int> &sauth,
                              const map<string,bufferptr> &xa,
                              const string &sym, __u8 st,
                              const inoparent_t &ap, const inoparent_t &rp,
                              const old_inodes_t *oi) const
{
  _enc = bufferlist(1024);

  ::encode(i, _enc);
  ::encode(iauth, _enc);
  ::encode(xa, _enc);
  if (i.is_symlink())
    ::encode(sym, _enc);
  if (i.is_dir())
    ::encode(sauth, _enc);
  ::encode(st, _enc);
  ::encode(oi ? true : false, _enc);
  if (oi) ::encode(*oi, _enc);
  ::encode(ap ? true : false, _enc);
  if (ap) ::encode(ap, _enc);
  ::encode(rp ? true : false, _enc);
  if (rp) ::encode(rp, _enc);
}

void EMetaBlob::Inode::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  if (!_enc.length())
    encode(inode, inode_auth, stripe_auth, xattrs, symlink,
           state, added_parent, removed_parent, &old_inodes);
  bl.append(_enc);
  ENCODE_FINISH(bl);
}

void EMetaBlob::Inode::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
  ::decode(inode, bl);
  ::decode(inode_auth, bl);
  ::decode(xattrs, bl);
  if (inode.is_symlink())
    ::decode(symlink, bl);
  if (inode.is_dir())
    ::decode(stripe_auth, bl);
  ::decode(state, bl);

  bool old_inodes_present;
  ::decode(old_inodes_present, bl);
  if (old_inodes_present)
    ::decode(old_inodes, bl);

  bool has_parent;
  ::decode(has_parent, bl);
  if (has_parent)
    ::decode(added_parent, bl);
  ::decode(has_parent, bl);
  if (has_parent)
    ::decode(removed_parent, bl);

  DECODE_FINISH(bl);
}

void EMetaBlob::Inode::dump(Formatter *f) const
{
  if (_enc.length() && inode.ino == 0) {
    /* if our bufferlist has data but our inode is empty, we
     * haven't initialized ourselves; do so in order to print members!
     * We use const_cast here because the whole point is we aren't
     * fully set up and this isn't changing who we "are", just our
     * representation.
     */
    EMetaBlob::Inode *me = const_cast<EMetaBlob::Inode*>(this);
    bufferlist encoded;
    encode(encoded);
    bufferlist::iterator p = encoded.begin();
    me->decode(p);
  }
  f->open_object_section("inode");
  inode.dump(f);
  f->close_section(); // inode
  f->dump_stream("inode auth") << inode_auth;
  f->open_array_section("xattrs");
  for (map<string, bufferptr>::const_iterator iter = xattrs.begin();
      iter != xattrs.end(); ++iter) {
    f->dump_string(iter->first.c_str(), iter->second.c_str());
  }
  f->close_section(); // xattrs
  if (inode.is_symlink()) {
    f->dump_string("symlink", symlink);
  }
  if (inode.is_dir()) {
    f->dump_stream("stripe auth") << stripe_auth;
    if (inode.has_layout()) {
      f->open_object_section("file layout policy");
      // FIXME
      f->dump_string("layout", "the layout exists");
      f->close_section(); // file layout policy
    }
  }
  f->dump_string("state", state_string());
  if (!old_inodes.empty()) {
    f->open_array_section("old inodes");
    for (old_inodes_t::const_iterator iter = old_inodes.begin();
	iter != old_inodes.end(); ++iter) {
      f->open_object_section("inode");
      f->dump_int("snapid", iter->first);
      iter->second.dump(f);
      f->close_section(); // inode
    }
    f->close_section(); // old inodes
  }
  if (added_parent) {
    f->open_object_section("added parent");
    added_parent.dump(f);
    f->close_section();
  }
  if (removed_parent) {
    f->open_object_section("removed parent");
    removed_parent.dump(f);
    f->close_section();
  }
}

void EMetaBlob::Inode::generate_test_instances(list<EMetaBlob::Inode*>& ls)
{
  inode_t inode;
  map<string,bufferptr> empty_xattrs;
  inoparent_t empty_parent;
  Inode *sample = new Inode();
  sample->encode(inode, make_pair(0, 0), vector<int>(),
                 empty_xattrs, "", 0, empty_parent, empty_parent, NULL);
  ls.push_back(sample);
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

  in->inode_auth = inode_auth;
  if (inode_auth.first == mds->get_nodeid())
    in->state_set(CInode::STATE_AUTH);
  else
    in->state_clear(CInode::STATE_AUTH);
  in->xattrs = xattrs;
  if (in->is_dir())
    in->set_stripe_auth(stripe_auth);
  else if (in->is_symlink())
    in->symlink = symlink;
  in->old_inodes = old_inodes;

  if (in->is_auth() && inode.rstat.version != inode.accounted_rstat.version)
    mds->mdcache->parentstats.replay_unaccounted(in);
  if (in->is_auth() && inode.nlink == 0)
    mds->mdcache->add_stray(in);

  assert(in->is_base() || static_cast<size_t>(inode.nlink) == in->inode.parents.size());
}

// EMetaBlob::Dentry

void EMetaBlob::Dentry::encode(const string& d, snapid_t df, snapid_t dl,
                               version_t v, inodeno_t i, unsigned char dt,
                               bool dr) const
{
  _enc = bufferlist(256);
  ::encode(d, _enc);
  ::encode(df, _enc);
  ::encode(dl, _enc);
  ::encode(v, _enc);
  ::encode(i, _enc);
  ::encode(dt, _enc);
  ::encode(dr, _enc);
}

void EMetaBlob::Dentry::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  if (!_enc.length())
    encode(name, first, last, version, ino, d_type, dirty);
  bl.append(_enc);
  ENCODE_FINISH(bl);
}

void EMetaBlob::Dentry::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
  ::decode(name, bl);
  ::decode(first, bl);
  ::decode(last, bl);
  ::decode(version, bl);
  ::decode(ino, bl);
  ::decode(d_type, bl);
  ::decode(dirty, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::Dentry::dump(Formatter *f) const
{
  if (_enc.length() && !name.length()) {
    /* if our bufferlist has data but our name is empty, we
     * haven't initialized ourselves; do so in order to print members!
     * We use const_cast here because the whole point is we aren't
     * fully set up and this isn't changing who we "are", just our
     * representation.
     */
    EMetaBlob::Dentry *me = const_cast<EMetaBlob::Dentry*>(this);
    bufferlist encoded;
    encode(encoded);
    bufferlist::iterator p = encoded.begin();
    me->decode(p);
  }
  f->dump_string("dentry", name);
  f->dump_int("snapid.first", first);
  f->dump_int("snapid.last", last);
  f->dump_int("dentry version", version);
  f->dump_int("inodeno", ino);
  uint32_t type = DTTOIF(d_type) & S_IFMT; // convert to type entries
  string type_string;
  switch(type) {
  case S_IFREG:
    type_string = "file"; break;
  case S_IFLNK:
    type_string = "symlink"; break;
  case S_IFDIR:
    type_string = "directory"; break;
  default:
    assert (0 == "unknown d_type!");
  }
  f->dump_string("d_type", type_string);
  f->dump_string("dirty", dirty ? "true" : "false");
}

void EMetaBlob::Dentry::generate_test_instances(list<EMetaBlob::Dentry*>& ls)
{
  Dentry *dn = new Dentry();
  dn->encode("/test/dn", 0, 10, 15, 1, IFTODT(S_IFREG), false);
  ls.push_back(dn);
}

void EMetaBlob::Dentry::apply(MDS *mds, CDirFrag *dir,
                              CDentry *dn, LogSegment *ls) const
{
  // update linkage
  CDentry::linkage_t *dnl = dn->get_linkage();
  if (dnl->is_null()) {
    assert(ino); // don't journal a null dentry unless unlinking
    CInode *in = mds->mdcache->get_inode(ino, last);
    if (dir->ino() == MDS_INO_CONTAINER) {
      // primary links in inode container
      assert(in);
      dir->link_primary_inode(dn, in);
    } else if (in)
      dir->link_remote_inode(dn, in);
    else
      dir->link_remote_inode(dn, ino, d_type);
  } else if (ino == 0) {
    dir->unlink_inode(dn);
  } else if (dnl->is_primary()) {
    assert(ino == dnl->get_inode()->ino());
  } else if (ino != dnl->get_remote_ino())
    dnl->set_remote(ino, d_type);

  dout(10) << "EMetaBlob.replay updated " << *dn << dendl;
}

// EMetaBlob::Dir

void EMetaBlob::Dir::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(version, bl);
  ::encode(state, bl);
  encode_dentries();
  ::encode(dnbl, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::Dir::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl)
  ::decode(version, bl);
  ::decode(state, bl);
  ::decode(dnbl, bl);
  dn_decoded = false;      // don't decode bits unless we need them.
  DECODE_FINISH(bl);
}

void EMetaBlob::Dir::dump(Formatter *f) const
{
  if (!dn_decoded) {
    Dir *me = const_cast<Dir*>(this);
    me->decode_dentries();
  }
  f->dump_int("version", version);
  f->dump_string("state", state_string());

  f->open_array_section("dentries");
  typedef dentry_vec::const_iterator dentry_iter;
  for (dentry_iter p = dentries.begin(); p != dentries.end(); ++p) {
    f->open_object_section("dentry");
    p->dump(f);
    f->close_section(); // dentry
  }
  f->close_section(); // dentries
}

void EMetaBlob::Dir::generate_test_instances(list<Dir*>& ls)
{
  ls.push_back(new Dir());
}

void EMetaBlob::Dir::apply(MDS *mds, CDirFrag *dir, LogSegment *ls)
{
  dir->set_version(version);

  if (is_dirty())
    dir->_mark_dirty(ls);
  if (is_new())
    dir->mark_new(ls);
  if (is_complete())
    dir->mark_complete();

  dout(10) << "EMetaBlob.replay updated " << *dir << dendl;

  decode_dentries();
  for (dentry_vec::iterator d = dentries.begin(); d != dentries.end(); ++d) {
    // open the dentry
    CDentry *dn = dir->lookup_exact_snap(d->name, d->last);
    if (!dn) {
      dn = dir->add_null_dentry(d->name, d->first, d->last);
      dn->set_version(d->version);
      if (d->dirty) dn->_mark_dirty(ls);
      dout(10) << "EMetaBlob.replay added " << *dn << dendl;
    } else {
      dn->set_version(d->version);
      if (d->dirty) dn->_mark_dirty(ls);
      dout(10) << "EMetaBlob.replay had " << *dn << dendl;
      dn->first = d->first;
      assert(dn->last == d->last);
    }

    d->apply(mds, dir, dn, ls);
  }
}

// EMetaBlob::Stripe

void EMetaBlob::Stripe::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(auth, bl);
  ::encode(dirfragtree, bl);
  ::encode(fnode, bl);
  ::encode(state, bl);
  encode_dirs();
  ::encode(dfbl, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::Stripe::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl)
  ::decode(auth, bl);
  ::decode(dirfragtree, bl);
  ::decode(fnode, bl);
  ::decode(state, bl);
  ::decode(dfbl, bl);
  df_decoded = false;
  DECODE_FINISH(bl);
}

void EMetaBlob::Stripe::dump(Formatter *f) const
{
  f->dump_stream("auth") << auth;
  f->dump_stream("frag tree") << dirfragtree;
  f->open_object_section("fnode");
  fnode.dump(f);
  f->close_section(); // fnode
  f->dump_string("state", state_string());
 
  f->open_array_section("frags");
  for (dir_map::const_iterator i = dirs.begin(); i != dirs.end(); ++i) {
    f->open_object_section("frag");
    i->second.dump(f);
    f->close_section(); // frag
  }
  f->close_section(); // frags
}

void EMetaBlob::Stripe::generate_test_instances(list<Stripe*>& ls)
{
  ls.push_back(new Stripe());
}

void EMetaBlob::Stripe::apply(MDS *mds, CDirStripe *stripe, LogSegment *ls)
{
  stripe->set_stripe_auth(auth);
  if (auth.first == mds->get_nodeid())
    stripe->state_set(CDirStripe::STATE_AUTH);
  else
    stripe->state_clear(CDirStripe::STATE_AUTH);
  stripe->fnode = fnode;
  if (is_open())
    stripe->mark_open();
  if (is_unlinked()) {
    stripe->state_set(CDirStripe::STATE_UNLINKED);
    stripe->clear_dirty_parent_stats();
    if (stripe->is_auth())
      mds->mdcache->add_stray(stripe);
  }
  if (is_dirty()) {
    stripe->_mark_dirty(ls);

    if (stripe->is_auth() && !stripe->state_test(CDirStripe::STATE_UNLINKED) &&
        (fnode.rstat.version != fnode.accounted_rstat.version ||
        fnode.fragstat.version != fnode.accounted_fragstat.version))
      mds->mdcache->parentstats.replay_unaccounted(stripe);
  }
  if (is_new())
    stripe->mark_new(ls);
  stripe->set_fragtree(dirfragtree);
  stripe->force_dirfrags();
  dout(10) << "EMetaBlob updated stripe " << *stripe << dendl;

  decode_dirs();
  for (dir_map::iterator f = dirs.begin(); f != dirs.end(); ++f) {
    // open the fragment
    CDirFrag *dir = stripe->get_dirfrag(f->first);
    if (!dir) {
      dir = stripe->get_or_open_dirfrag(f->first);
      dout(10) << "EMetaBlob.replay added " << *dir << dendl;
    } else {
      dout(10) << "EMetaBlob.replay had " << *dir << dendl;
    }

    f->second.apply(mds, dir, ls);
  }
}

// EMetaBlob::Placement

void EMetaBlob::Placement::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(stripes, bl);
  ::encode(stripe_auth, bl);
  ::encode(layout, bl);
  ENCODE_FINISH(bl);
}

void EMetaBlob::Placement::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl)
  ::decode(stripes, bl);
  ::decode(stripe_auth, bl);
  ::decode(layout, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::Placement::dump(Formatter *f) const
{
  f->dump_stream("stripe auth") << stripe_auth;
  f->open_object_section("layout");
  ::dump(layout, f);
  f->close_section(); // layout
 
  f->open_array_section("stripes");
  for (stripe_map::const_iterator i = stripes.begin(); i != stripes.end(); ++i) {
    f->open_object_section("stripe");
    i->second.dump(f);
    f->close_section(); // stripe
  }
  f->close_section(); // stripes
}

void EMetaBlob::Placement::generate_test_instances(list<Placement*>& ls)
{
  ls.push_back(new Placement());
}

void EMetaBlob::Placement::apply(MDS *mds, CDirPlacement *placement,
                                 LogSegment *ls)
{
  placement->set_stripe_auth(stripe_auth);
  placement->set_layout(layout);
  dout(10) << "EMetaBlob updated placement " << *placement << dendl;

  for (stripe_map::iterator s = stripes.begin(); s != stripes.end(); ++s) {
    // find/create the stripe
    CDirStripe *stripe = placement->get_stripe(s->first);
    if (stripe) {
      dout(10) << "EMetaBlob had " << *stripe << dendl;
    } else {
      int auth = placement->get_stripe_auth(s->first);
      stripe = placement->add_stripe(new CDirStripe(placement, s->first, auth));
      dout(10) << "EMetaBlob added " << *stripe << dendl;
    }
    assert(stripe);

    s->second.apply(mds, stripe, ls);
  }
}

/**
 * EMetaBlob proper
 */
void EMetaBlob::encode(bufferlist& bl) const
{
  ENCODE_START(10, 10, bl);
  ::encode(inodes, bl);
  ::encode(dirs, bl);
  ::encode(table_tids, bl);
  ::encode(opened_ino, bl);
  ::encode(allocated_ino, bl);
  ::encode(used_preallocated_ino, bl);
  ::encode(preallocated_inos, bl);
  ::encode(client_name, bl);
  ::encode(inotablev, bl);
  ::encode(sessionmapv, bl);
  ::encode(truncate_start, bl);
  ::encode(truncate_finish, bl);
  ::encode(destroyed_inodes, bl);
  ::encode(client_reqs, bl);
  ::encode(renamed_dirino, bl);
  ::encode(renamed_dir_stripes, bl);
  ::encode(destroyed_stripes, bl);
  ENCODE_FINISH(bl);
}
void EMetaBlob::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(10, 10, 10, bl);
  ::decode(inodes, bl);
  ::decode(dirs, bl);
  ::decode(table_tids, bl);
  ::decode(opened_ino, bl);
  ::decode(allocated_ino, bl);
  ::decode(used_preallocated_ino, bl);
  ::decode(preallocated_inos, bl);
  ::decode(client_name, bl);
  ::decode(inotablev, bl);
  ::decode(sessionmapv, bl);
  ::decode(truncate_start, bl);
  ::decode(truncate_finish, bl);
  ::decode(destroyed_inodes, bl);
  ::decode(client_reqs, bl);
  ::decode(renamed_dirino, bl);
  ::decode(renamed_dir_stripes, bl);
  ::decode(destroyed_stripes, bl);
  DECODE_FINISH(bl);
}

void EMetaBlob::dump(Formatter *f) const
{
  f->open_array_section("inodes");
  for (inode_map::const_iterator i = inodes.begin(); i != inodes.end(); ++i) {
    f->open_object_section("inode");
    i->second.dump(f);
    f->close_section(); // inode
  }
  f->close_section(); // inodes
  
  f->open_array_section("dirs");
  for (placement_map::const_iterator i = dirs.begin(); i != dirs.end(); ++i) {
    f->open_object_section("dir");
    i->second.dump(f);
    f->close_section(); // dir
  }
  f->close_section(); // dirs

  f->open_array_section("tableclient tranactions");
  for (list<pair<__u8,version_t> >::const_iterator i = table_tids.begin();
       i != table_tids.end(); ++i) {
    f->open_object_section("transaction");
    f->dump_int("tid", i->first);
    f->dump_int("version", i->second);
    f->close_section(); // transaction
  }
  f->close_section(); // tableclient transactions
  
  f->dump_int("renamed directory inodeno", renamed_dirino);
  
  f->open_array_section("renamed directory stripes");
  for (list<dirstripe_t>::const_iterator i = renamed_dir_stripes.begin();
       i != renamed_dir_stripes.end(); ++i) {
    f->dump_stream("stripe") << *i;
  }
  f->close_section(); // renamed directory stripes

  f->dump_int("inotable version", inotablev);
  f->dump_int("SesionMap version", sessionmapv);
  f->dump_int("allocated ino", allocated_ino);
  
  f->dump_stream("preallocated inos") << preallocated_inos;
  f->dump_int("used preallocated ino", used_preallocated_ino);

  f->open_object_section("client name");
  client_name.dump(f);
  f->close_section(); // client name

  f->open_array_section("inodes starting a truncate");
  for(list<inodeno_t>::const_iterator i = truncate_start.begin();
      i != truncate_start.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // truncate inodes
  f->open_array_section("inodes finishing a truncated");
  for(map<inodeno_t,uint64_t>::const_iterator i = truncate_finish.begin();
      i != truncate_finish.end(); ++i) {
    f->open_object_section("inode+segment");
    f->dump_int("inodeno", i->first);
    f->dump_int("truncate starting segment", i->second);
    f->close_section(); // truncated inode
  }
  f->close_section(); // truncate finish inodes

  f->open_array_section("destroyed inodes");
  for(vector<inodeno_t>::const_iterator i = destroyed_inodes.begin();
      i != destroyed_inodes.end(); ++i) {
    f->dump_int("inodeno", *i);
  }
  f->close_section(); // destroyed inodes

  f->open_array_section("client requests");
  for(list<pair<metareqid_t,uint64_t> >::const_iterator i = client_reqs.begin();
      i != client_reqs.end(); ++i) {
    f->open_object_section("Client request");
    f->dump_stream("request ID") << i->first;
    f->dump_int("oldest request on client", i->second);
    f->close_section(); // request
  }
  f->close_section(); // client requests
}

void EMetaBlob::generate_test_instances(list<EMetaBlob*>& ls)
{
  ls.push_back(new EMetaBlob());
}

void EMetaBlob::replay(MDS *mds, LogSegment *logseg, MDSlaveUpdate *slaveup)
{
  dout(10) << "EMetaBlob.replay " << inodes.size() << " inodes, "
      << dirs.size() << " dirs by " << client_name << dendl;

  assert(logseg);

  assert(g_conf->mds_kill_journal_replay_at != 1);

  // replay all inodes
  for (inode_map::iterator p = inodes.begin(); p != inodes.end(); ++p) {
    CInode *in = mds->mdcache->get_inode(p->first);
    bool isnew = in ? false:true;
    if (!in)
      in = new CInode(mds->mdcache, mds->get_nodeid());
    p->second.apply(mds, in, isnew);
    if (isnew)
      mds->mdcache->add_inode(in);
    if (p->second.is_dirty()) in->_mark_dirty(logseg);
    dout(10) << "EMetaBlob.replay " << (isnew ? " added ":" updated ") << *in << dendl;
  }

  // replay all dirs
  for (placement_map::iterator p = dirs.begin(); p != dirs.end(); ++p) {
    // open the placement object
    CDirPlacement *placement = mds->mdcache->get_dir_placement(p->first);
    assert(placement);
    p->second.apply(mds, placement, logseg);
  }

  assert(g_conf->mds_kill_journal_replay_at != 2);

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
      dout(20) << " (session prealloc " << session->info.prealloc_inos << ")" << dendl;
      if (used_preallocated_ino) {
	if (session->info.prealloc_inos.empty()) {
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
	  session->info.used_inos.clear();
	}
	mds->sessionmap.projected = ++mds->sessionmap.version;
      }
      if (preallocated_inos.size()) {
	session->info.prealloc_inos.insert(preallocated_inos);
	mds->sessionmap.projected = ++mds->sessionmap.version;
      }
      assert(sessionmapv == mds->sessionmap.version);
    }
  }

  // truncating inodes
  for (list<inodeno_t>::iterator p = truncate_start.begin();
       p != truncate_start.end();
       ++p) {
    CInode *in = mds->mdcache->get_inode(*p);
    assert(in);
    mds->mdcache->add_recovered_truncate(in, logseg);
  }
  for (map<inodeno_t,uint64_t>::iterator p = truncate_finish.begin();
       p != truncate_finish.end();
       ++p) {
    LogSegment *ls = mds->mdlog->get_segment(p->second);
    if (ls) {
      CInode *in = mds->mdcache->get_inode(p->first);
      assert(in);
      mds->mdcache->remove_recovered_truncate(in, ls);
    }
  }

  // destroyed stripes
  for (vector<dirstripe_t>::iterator p = destroyed_stripes.begin();
       p != destroyed_stripes.end();
       p++) {
    CDirStripe *stripe = mds->mdcache->get_dirstripe(*p);
    if (stripe) {
      dout(10) << "EMetaBlob.replay destroyed " << *p
          << ", dropping " << *stripe << dendl;
      stripe->get_placement()->close_stripe(stripe);
    } else
      dout(10) << "EMetaBlob.replay destroyed " << *p
          << ", not in cache" << dendl;
  }

  // destroyed inodes
  for (vector<inodeno_t>::iterator p = destroyed_inodes.begin();
       p != destroyed_inodes.end();
       ++p) {
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
       ++p) {
    if (p->first.name.is_client()) {
      dout(10) << "EMetaBlob.replay request " << p->first << " trim_to " << p->second << dendl;

      // if we allocated an inode, there should be exactly one client request id.
      assert(allocated_ino == inodeno_t() || client_reqs.size() == 1);

      Session *session = mds->sessionmap.get_session(p->first.name);
      if (session) {
	session->add_completed_request(p->first.tid, allocated_ino);
	if (p->second)
	  session->trim_completed_requests(p->second);
      }
    }
  }

  // update segment
  update_segment(logseg);

  assert(g_conf->mds_kill_journal_replay_at != 4);
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
      dout(10) << " opened session " << session->info.inst << dendl;
    } else {
      session = mds->sessionmap.get_session(client_inst.name);
      if (session) { // there always should be a session, but there's a bug
	if (session->connection == NULL) {
	  dout(10) << " removed session " << session->info.inst << dendl;
	  mds->sessionmap.remove_session(session);
	} else {
	  session->clear();    // the client has reconnected; keep the Session, but reset
	  dout(10) << " reset session " << session->info.inst << " (they reconnected)" << dendl;
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

void ESession::encode(bufferlist &bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(client_inst, bl);
  ::encode(open, bl);
  ::encode(cmapv, bl);
  ::encode(inos, bl);
  ::encode(inotablev, bl);
  ENCODE_FINISH(bl);
}

void ESession::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(client_inst, bl);
  ::decode(open, bl);
  ::decode(cmapv, bl);
  ::decode(inos, bl);
  ::decode(inotablev, bl);
  DECODE_FINISH(bl);
}

void ESession::dump(Formatter *f) const
{
  f->dump_stream("client instance") << client_inst;
  f->dump_string("open", open ? "true" : "false");
  f->dump_int("client map version", cmapv);
  f->dump_stream("inos") << inos;
  f->dump_int("inotable version", inotablev);
}

void ESession::generate_test_instances(list<ESession*>& ls)
{
  ls.push_back(new ESession);
}

// -----------------------
// ESessions

void ESessions::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(client_map, bl);
  ::encode(cmapv, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void ESessions::decode_old(bufferlist::iterator &bl)
{
  ::decode(client_map, bl);
  ::decode(cmapv, bl);
  if (!bl.end())
    ::decode(stamp, bl);
}

void ESessions::decode_new(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(client_map, bl);
  ::decode(cmapv, bl);
  if (!bl.end())
    ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void ESessions::dump(Formatter *f) const
{
  f->dump_int("client map version", cmapv);

  f->open_array_section("client map");
  for (map<client_t,entity_inst_t>::const_iterator i = client_map.begin();
       i != client_map.end(); ++i) {
    f->open_object_section("client");
    f->dump_int("client id", i->first.v);
    f->dump_stream("client entity") << i->second;
    f->close_section(); // client
  }
  f->close_section(); // client map
}

void ESessions::generate_test_instances(list<ESessions*>& ls)
{
  ls.push_back(new ESessions());
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


// -----------------------
// ETableServer

void ETableServer::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(table, bl);
  ::encode(op, bl);
  ::encode(reqid, bl);
  ::encode(bymds, bl);
  ::encode(mutation, bl);
  ::encode(tid, bl);
  ::encode(version, bl);
  ENCODE_FINISH(bl);
}

void ETableServer::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(table, bl);
  ::decode(op, bl);
  ::decode(reqid, bl);
  ::decode(bymds, bl);
  ::decode(mutation, bl);
  ::decode(tid, bl);
  ::decode(version, bl);
  DECODE_FINISH(bl);
}

void ETableServer::dump(Formatter *f) const
{
  f->dump_int("table id", table);
  f->dump_int("op", op);
  f->dump_int("request id", reqid);
  f->dump_int("by mds", bymds);
  f->dump_int("tid", tid);
  f->dump_int("version", version);
}

void ETableServer::generate_test_instances(list<ETableServer*>& ls)
{
  ls.push_back(new ETableServer());
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


// ---------------------
// ETableClient

void ETableClient::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(table, bl);
  ::encode(op, bl);
  ::encode(tid, bl);
  ENCODE_FINISH(bl);
}

void ETableClient::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(table, bl);
  ::decode(op, bl);
  ::decode(tid, bl);
  DECODE_FINISH(bl);
}

void ETableClient::dump(Formatter *f) const
{
  f->dump_int("table", table);
  f->dump_int("op", op);
  f->dump_int("tid", tid);
}

void ETableClient::generate_test_instances(list<ETableClient*>& ls)
{
  ls.push_back(new ETableClient());
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

void EUpdate::encode(bufferlist &bl) const
{
  ENCODE_START(4, 4, bl);
  ::encode(stamp, bl);
  ::encode(type, bl);
  ::encode(metablob, bl);
  ::encode(client_map, bl);
  ::encode(cmapv, bl);
  ::encode(reqid, bl);
  ::encode(had_slaves, bl);
  ENCODE_FINISH(bl);
}
 
void EUpdate::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(type, bl);
  ::decode(metablob, bl);
  ::decode(client_map, bl);
  if (struct_v >= 3)
    ::decode(cmapv, bl);
  ::decode(reqid, bl);
  ::decode(had_slaves, bl);
  DECODE_FINISH(bl);
}

void EUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob

  f->dump_string("type", type);
  f->dump_int("client map length", client_map.length());
  f->dump_int("client map version", cmapv);
  f->dump_stream("reqid") << reqid;
  f->dump_string("had slaves", had_slaves ? "true" : "false");
}

void EUpdate::generate_test_instances(list<EUpdate*>& ls)
{
  ls.push_back(new EUpdate());
}


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
    mds->mdcache->add_uncommitted_master(reqid, _segment, slaves, true);
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

      assert(mds->sessionmap.version == cmapv);
      mds->sessionmap.projected = mds->sessionmap.version;
    }
  }
}


// ------------------------
// EOpen

void EOpen::encode(bufferlist &bl) const {
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl);
  ::encode(inos, bl);
  ENCODE_FINISH(bl);
} 

void EOpen::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(inos, bl);
  DECODE_FINISH(bl);
}

void EOpen::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  f->open_array_section("inos involved");
  for (vector<inodeno_t>::const_iterator i = inos.begin();
       i != inos.end(); ++i) {
    f->dump_int("ino", *i);
  }
  f->close_section(); // inos
}

void EOpen::generate_test_instances(list<EOpen*>& ls)
{
  ls.push_back(new EOpen());
  ls.push_back(new EOpen());
  ls.back()->add_ino(0);
}

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
       ++p) {
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

void ECommitted::encode(bufferlist& bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(reqid, bl);
  ENCODE_FINISH(bl);
} 

void ECommitted::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(reqid, bl);
  DECODE_FINISH(bl);
}

void ECommitted::dump(Formatter *f) const {
  f->dump_stream("stamp") << stamp;
  f->dump_stream("reqid") << reqid;
}

void ECommitted::generate_test_instances(list<ECommitted*>& ls)
{
  ls.push_back(new ECommitted);
  ls.push_back(new ECommitted);
  ls.back()->stamp = utime_t(1, 2);
  ls.back()->reqid = metareqid_t(entity_name_t::CLIENT(123), 456);
}

// -----------------------
// ESlaveUpdate

void link_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(4, 4, bl);
  ::encode(reqid, bl);
  ::encode(ino, bl);
  ::encode(was_inc, bl);
  ::encode(ctime, bl);
  ::encode(parent, bl);
  ENCODE_FINISH(bl);
}

void link_rollback::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  ::decode(reqid, bl);
  ::decode(ino, bl);
  ::decode(was_inc, bl);
  ::decode(ctime, bl);
  ::decode(parent, bl);
  DECODE_FINISH(bl);
}

void link_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->dump_int("ino", ino);
  f->dump_string("was incremented", was_inc ? "true" : "false");
  f->dump_stream("ctime") << ctime;
  f->dump_stream("parent") << parent;
}

void link_rollback::generate_test_instances(list<link_rollback*>& ls)
{
  ls.push_back(new link_rollback());
}

void rmdir_rollback::encode(bufferlist& bl) const
{
  ENCODE_START(4, 4, bl);
  ::encode(reqid, bl);
  ::encode(dn, bl);
  ::encode(ino, bl);
  ::encode(d_type, bl);
  ::encode(stripes, bl);
  ENCODE_FINISH(bl);
}

void rmdir_rollback::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
  ::decode(reqid, bl);
  ::decode(dn, bl);
  ::decode(ino, bl);
  ::decode(d_type, bl);
  ::decode(stripes, bl);
  DECODE_FINISH(bl);
}

static string type_string(char d_type)
{
  int type = DTTOIF(d_type) & S_IFMT; // convert to type entries
  switch (type) {
  case S_IFREG: return "file";
  case S_IFLNK: return "symlink";
  case S_IFDIR: return "directory";
  default: return "UNKNOWN-" + stringify((int)type);
  }
}

void rmdir_rollback::dump(Formatter *f) const
{
  f->dump_stream("metareqid") << reqid;
  f->open_object_section("dentry");
  dn.dump(f);
  f->close_section();
  f->dump_stream("inode") << ino;
  f->dump_string("dtype", type_string(d_type));
  f->dump_stream("stripes") << stripes;
}

void rmdir_rollback::generate_test_instances(list<rmdir_rollback*>& ls)
{
  ls.push_back(new rmdir_rollback());
}

void rename_rollback::drec::encode(bufferlist &bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(dn, bl);
  ::encode(ino, bl);
  ::encode(d_type, bl);
  ::encode(mtime, bl);
  ::encode(rctime, bl);
  ::encode(ctime, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::drec::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  ::decode(dn, bl);
  ::decode(ino, bl);
  ::decode(d_type, bl);
  ::decode(mtime, bl);
  ::decode(rctime, bl);
  ::decode(ctime, bl);
  DECODE_FINISH(bl);
}

void rename_rollback::drec::dump(Formatter *f) const
{
  f->open_object_section("parent");
  dn.dump(f);
  f->close_section();
  f->dump_stream("ino") << ino;
  f->dump_string("dtype", type_string(d_type));
  f->dump_stream("mtime") << mtime;
  f->dump_stream("rctime") << rctime;
  f->dump_stream("ctime") << ctime;
}

void rename_rollback::drec::generate_test_instances(list<drec*>& ls)
{
  ls.push_back(new drec());
  ls.back()->d_type = IFTODT(S_IFREG);
}

void rename_rollback::encode(bufferlist &bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(reqid, bl);
  encode(src, bl);
  encode(dest, bl);
  ::encode(ctime, bl);
  ::encode(stripes, bl);
  ENCODE_FINISH(bl);
}

void rename_rollback::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  ::decode(reqid, bl);
  decode(src, bl);
  decode(dest, bl);
  ::decode(ctime, bl);
  ::decode(stripes, bl);
  DECODE_FINISH(bl);
}

void rename_rollback::dump(Formatter *f) const
{
  f->dump_stream("request id") << reqid;
  f->open_object_section("original src drec");
  src.dump(f);
  f->close_section(); // original src drec
  f->open_object_section("original dest drec");
  dest.dump(f);
  f->close_section(); // original dest drec
  f->dump_stream("ctime") << ctime;
  f->dump_stream("stripes") << stripes;
}

void rename_rollback::generate_test_instances(list<rename_rollback*>& ls)
{
  ls.push_back(new rename_rollback());
}

void ESlaveUpdate::encode(bufferlist &bl) const
{
  ENCODE_START(3, 3, bl);
  ::encode(stamp, bl);
  ::encode(type, bl);
  ::encode(reqid, bl);
  ::encode(master, bl);
  ::encode(op, bl);
  ::encode(origop, bl);
  ::encode(commit, bl);
  ::encode(rollback, bl);
  ENCODE_FINISH(bl);
} 

void ESlaveUpdate::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(type, bl);
  ::decode(reqid, bl);
  ::decode(master, bl);
  ::decode(op, bl);
  ::decode(origop, bl);
  ::decode(commit, bl);
  ::decode(rollback, bl);
  DECODE_FINISH(bl);
}

void ESlaveUpdate::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  commit.dump(f);
  f->close_section(); // metablob

  f->dump_int("rollback length", rollback.length());
  f->dump_string("type", type);
  f->dump_stream("metareqid") << reqid;
  f->dump_int("master", master);
  f->dump_int("op", op);
  f->dump_int("original op", origop);
}

void ESlaveUpdate::generate_test_instances(list<ESlaveUpdate*>& ls)
{
  ls.push_back(new ESlaveUpdate());
}


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
// ESubtreeMap

void ESubtreeMap::encode(bufferlist& bl) const
{
  ENCODE_START(5, 5, bl);
  ::encode(stamp, bl);
  ::encode(metablob, bl);
  ::encode(subtrees, bl);
  ::encode(ambiguous_subtrees, bl);
  ::encode(expire_pos, bl);
  ENCODE_FINISH(bl);
}
 
void ESubtreeMap::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
  if (struct_v >= 2)
    ::decode(stamp, bl);
  ::decode(metablob, bl);
  ::decode(subtrees, bl);
  if (struct_v >= 4)
    ::decode(ambiguous_subtrees, bl);
  if (struct_v >= 3)
    ::decode(expire_pos, bl);
  DECODE_FINISH(bl);
}

void ESubtreeMap::dump(Formatter *f) const
{
  f->open_object_section("metablob");
  metablob.dump(f);
  f->close_section(); // metablob
  
  f->open_array_section("subtrees");
  for(map<dirstripe_t,vector<dirstripe_t> >::const_iterator i = subtrees.begin();
      i != subtrees.end(); ++i) {
    f->open_object_section("tree");
    f->dump_stream("root stripe") << i->first;
    for (vector<dirstripe_t>::const_iterator j = i->second.begin();
        j != i->second.end(); ++j) {
      f->dump_stream("bound stripe") << *j;
    }
    f->close_section(); // tree
  }
  f->close_section(); // subtrees

  f->open_array_section("ambiguous subtrees");
  for(set<dirstripe_t>::const_iterator i = ambiguous_subtrees.begin();
      i != ambiguous_subtrees.end(); ++i) {
    f->dump_stream("stripe") << *i;
  }
  f->close_section(); // ambiguous subtrees

  f->dump_int("expire position", expire_pos);
}

void ESubtreeMap::generate_test_instances(list<ESubtreeMap*>& ls)
{
  ls.push_back(new ESubtreeMap());
}


// -----------------------
// EFragment

void EFragment::replay(MDS *mds)
{
  dout(10) << "EFragment.replay " << op_name(op) << " " << dirfrag << " by " << bits << dendl;

  list<CDirFrag*> resultfrags;
  list<Context*> waiters;
  map<frag_t, version_t> old_frags;

  // in may be NULL if it wasn't in our cache yet.  if it's a prepare
  // it will be once we replay the metablob , but first we need to
  // refragment anything we already have in the cache.
  CDirStripe *stripe = mds->mdcache->get_dirstripe(dirfrag.stripe);

  switch (op) {
  case OP_PREPARE:
    mds->mdcache->add_uncommitted_fragment(dirfrag, bits, orig_frags, _segment);
    // fall-thru
  case OP_ONESHOT:
    if (stripe)
      mds->mdcache->adjust_dir_fragments(stripe, dirfrag.frag, bits,
                                         resultfrags, waiters, true);
    break;

  case OP_ROLLBACK:
    if (stripe) {
      list<frag_t> leaves;
      stripe->get_fragtree().get_leaves_under(dirfrag.frag, leaves);
      for (list<frag_t>::iterator p = leaves.begin(); p != leaves.end(); ++p)
        old_frags[*p] = 0;
      if (orig_frags.empty()) {
	// old format EFragment
	mds->mdcache->adjust_dir_fragments(stripe, dirfrag.frag, -bits,
                                           resultfrags, waiters, true);
      } else {
	for (map<frag_t, version_t>::iterator p = orig_frags.begin(); p != orig_frags.end(); ++p)
	  mds->mdcache->force_dir_fragment(stripe, p->first);
      }
    }
    mds->mdcache->rollback_uncommitted_fragment(dirfrag, old_frags);
    break;

  case OP_COMMIT:
  case OP_FINISH:
    mds->mdcache->finish_uncommitted_fragment(dirfrag, op);
    break;

  default:
    assert(0);
  }

  metablob.replay(mds, _segment);
  if (stripe && g_conf->mds_debug_frag)
    stripe->verify_dirfrags();
}

void EFragment::encode(bufferlist &bl) const {
  ENCODE_START(6, 6, bl);
  ::encode(stamp, bl);
  ::encode(op, bl);
  ::encode(dirfrag, bl);
  ::encode(bits, bl);
  ::encode(metablob, bl);
  ::encode(orig_frags, bl);
  ENCODE_FINISH(bl);
}

void EFragment::decode(bufferlist::iterator &bl) {
  DECODE_START_LEGACY_COMPAT_LEN(6, 6, 6, bl);
  ::decode(stamp, bl);
  ::decode(op, bl);
  ::decode(dirfrag, bl);
  ::decode(bits, bl);
  ::decode(metablob, bl);
  ::decode(orig_frags, bl);
  DECODE_FINISH(bl);
}

void EFragment::dump(Formatter *f) const
{
  /*f->open_object_section("Metablob");
  metablob.dump(f); // sadly we don't have this; dunno if we'll get it
  f->close_section();*/
  f->dump_string("op", op_name(op));
  f->dump_stream("dirfrag") << dirfrag;
  f->dump_int("bits", bits);
}

void EFragment::generate_test_instances(list<EFragment*>& ls)
{
  ls.push_back(new EFragment);
  ls.push_back(new EFragment);
  ls.back()->op = OP_PREPARE;
  ls.back()->dirfrag.stripe.ino = 1;
  ls.back()->bits = 5;
}


// ------------------------
// EResetJournal

void EResetJournal::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}
 
void EResetJournal::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void EResetJournal::dump(Formatter *f) const
{
  f->dump_stream("timestamp") << stamp;
}

void EResetJournal::generate_test_instances(list<EResetJournal*>& ls)
{
  ls.push_back(new EResetJournal());
}

void EResetJournal::replay(MDS *mds)
{
  dout(1) << "EResetJournal" << dendl;

  mds->sessionmap.wipe();
  mds->inotable->replay_reset();
#if 0
  if (mds->mdsmap->get_root() == mds->whoami) {
    CDirStripe *rootstripe = mds->mdcache->get_root()->get_or_open_stripe(0);
    rootstripe->set_stripe_auth(mds->whoami);
  }

  CDirStripe *mystripe = mds->mdcache->get_myin()->get_or_open_stripe(0);
  mystripe->set_stripe_auth(mds->whoami);
#endif
}

