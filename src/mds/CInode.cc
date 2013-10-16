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

#include <inttypes.h>
#include <string>
#include <stdio.h>

#include "CInode.h"
#include "CDirFrag.h"
#include "CDentry.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Locker.h"
#include "Mutation.h"

#include "events/EUpdate.h"

#include "osdc/Objecter.h"

#include "snap.h"

#include "LogSegment.h"

#include "common/Clock.h"

#include "messages/MLock.h"
#include "messages/MClientCaps.h"

#include "common/config.h"
#include "global/global_context.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


boost::pool<> CInode::pool(sizeof(CInode));
boost::pool<> Capability::pool(sizeof(Capability));

LockType CInode::authlock_type(CEPH_LOCK_IAUTH);
LockType CInode::linklock_type(CEPH_LOCK_ILINK);
LockType CInode::filelock_type(CEPH_LOCK_IFILE);
LockType CInode::xattrlock_type(CEPH_LOCK_IXATTR);
LockType CInode::nestlock_type(CEPH_LOCK_INEST);
LockType CInode::flocklock_type(CEPH_LOCK_IFLOCK);
LockType CInode::policylock_type(CEPH_LOCK_IPOLICY);

//int cinode_pins[CINODE_NUM_PINS];  // counts
ostream& CInode::print_db_line_prefix(ostream& out)
{
  return out << ceph_clock_now(g_ceph_context) << " mds." << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") ";
}

/*
 * write caps and lock ids
 */
struct cinode_lock_info_t cinode_lock_info[] = {
  { CEPH_LOCK_IFILE, CEPH_CAP_ANY_FILE_WR },
  { CEPH_LOCK_IAUTH, CEPH_CAP_AUTH_EXCL },
  { CEPH_LOCK_ILINK, CEPH_CAP_LINK_EXCL },
  { CEPH_LOCK_IXATTR, CEPH_CAP_XATTR_EXCL },
  { CEPH_LOCK_IFLOCK, CEPH_CAP_FLOCK_EXCL }  
};
int num_cinode_locks = 5;



void CInode::print(ostream& out)
{
  out << "[inode " << inode.ino;
  out << " [" 
      << (is_multiversion() ? "...":"")
      << first << "," << last << "]";

  for (list<inoparent_t>::iterator i = inode.parents.begin();
       i != inode.parents.end(); ++i)
    out << " " << *i << (is_dir() ? "/":"");

  if (is_auth()) {
    out << " auth";
    if (is_replicated()) 
      out << get_replicas();
  } else {
    pair<int,int> a = authority();
    out << " rep@" << a.first;
    if (a.second != CDIR_AUTH_UNKNOWN)
      out << "," << a.second;
    out << "." << get_replica_nonce();
  }

  if (is_symlink())
    out << " symlink='" << symlink << "'";
 
  out << " v" << get_version();
  if (get_projected_version() > get_version())
    out << " pv" << get_projected_version();

  if (is_auth_pinned()) {
    out << " ap=" << get_num_auth_pins();
#ifdef MDS_AUTHPIN_SET
    out << "(" << auth_pin_set << ")";
#endif
  }

  if (state_test(CInode::STATE_AMBIGUOUSAUTH)) out << " AMBIGAUTH";
  if (state_test(CInode::STATE_NEEDSRECOVER)) out << " needsrecover";
  if (state_test(CInode::STATE_RECOVERING)) out << " recovering";
  if (state_test(CInode::STATE_DIRTYPARENT)) out << " dirtyparent";
  if (is_freezing_inode()) out << " FREEZING=" << auth_pin_freeze_allowance;
  if (is_frozen_inode()) out << " FROZEN";
  if (is_frozen_auth_pin()) out << " FROZEN_AUTHPIN";

  inode_t *pi = get_projected_inode();
  if (pi->is_truncating())
    out << " truncating(" << pi->truncate_from << " to " << pi->truncate_size << ")";

  if (inode.is_dir()) {
    out << " " << inode.dirstat;
    if (g_conf->mds_debug_scatterstat && is_projected()) {
      inode_t *pi = get_projected_inode();
      out << "->" << pi->dirstat;
    }
  } else {
    out << " s=" << inode.size;
    if (inode.nlink != 1)
      out << " nl=" << inode.nlink;
  }

  // rstat
  out << " " << inode.rstat;
  if (!(inode.rstat == inode.accounted_rstat))
    out << "/" << inode.accounted_rstat;
  if (g_conf->mds_debug_scatterstat && is_projected()) {
    inode_t *pi = get_projected_inode();
    out << "->" << pi->rstat;
    if (!(pi->rstat == pi->accounted_rstat))
      out << "/" << pi->accounted_rstat;
  }

  if (!client_need_snapflush.empty())
    out << " need_snapflush=" << client_need_snapflush;


  // locks
  if (!authlock.is_sync_and_unlocked())
    out << " " << authlock;
  if (!linklock.is_sync_and_unlocked())
    out << " " << linklock;
  if (inode.is_dir()) {
    if (!nestlock.is_sync_and_unlocked())
      out << " " << nestlock;
    if (!policylock.is_sync_and_unlocked())
      out << " " << policylock;
  } else  {
    if (!flocklock.is_sync_and_unlocked())
      out << " " << flocklock;
  }
  if (!filelock.is_sync_and_unlocked())
    out << " " << filelock;
  if (!xattrlock.is_sync_and_unlocked())
    out << " " << xattrlock;

  // hack: spit out crap on which clients have caps
  if (inode.client_ranges.size())
    out << " cr=" << inode.client_ranges;

  CapObject::print(out);

  if (get_num_ref()) {
    out << " |";
    print_pin_set(out);
  }

  out << " " << this;
  out << "]";
}


ostream& operator<<(ostream& out, CInode& in)
{
  in.print(out);
  return out;
}


CInode::CInode(MDCache *c, int auth, snapid_t f, snapid_t l)
  : CapObject(c, f, l),
    placement(NULL),
    inode_auth(auth, CDIR_AUTH_UNKNOWN),
    last_journaled(0),
    default_layout(NULL),
    parent(0),
    item_dirty(this),
    item_open_file(this),
    item_renamed_file(this),
    item_stray(this),
    auth_pins(0),
    auth_pin_freeze_allowance(0),
    authlock(this, &authlock_type),
    linklock(this, &linklock_type),
    filelock(this, &filelock_type),
    xattrlock(this, &xattrlock_type),
    nestlock(this, &nestlock_type),
    flocklock(this, &flocklock_type),
    policylock(this, &policylock_type)
{
  g_num_ino++;
  g_num_inoa++;
  if (auth == mdcache->mds->get_nodeid())
    state_set(STATE_AUTH);

  // register locks with CapObject
  cap_locks.push_back(&authlock);
  cap_locks.push_back(&linklock);
  cap_locks.push_back(&filelock);
  cap_locks.push_back(&xattrlock);
  cap_locks.push_back(&nestlock);
};

CInode::~CInode()
{
  g_num_ino--;
  g_num_inos++;
  close_placement();
}

void CInode::set_stripe_auth(const vector<int> &stripe_auth)
{
  assert(is_dir());
  if (placement) {
    placement->set_stripe_auth(stripe_auth);
    dout(10) << "set_stripe_auth existing " << *placement << dendl;
  } else {
    placement = new CDirPlacement(mdcache, this, stripe_auth);
    mdcache->add_dir_placement(placement);
    dout(10) << "set_stripe_auth created " << *placement << dendl;
  }
}

void CInode::close_placement()
{
  if (placement) {
    assert(placement->get_num_ref() == 0);
    mdcache->remove_dir_placement(placement);
    dout(10) << "close_placement " << *placement << dendl;
    delete placement;
    placement = NULL;
  }
}

void CInode::add_need_snapflush(CInode *snapin, snapid_t snapid, client_t client)
{
  dout(10) << "add_need_snapflush client." << client << " snapid " << snapid << " on " << snapin << dendl;

  if (client_need_snapflush.empty()) {
    get(CInode::PIN_NEEDSNAPFLUSH);

    // FIXME: this is non-optimal, as we'll block freezes/migrations for potentially
    // long periods waiting for clients to flush their snaps.
    auth_pin(this);   // pin head inode...
  }

  set<client_t>& clients = client_need_snapflush[snapid];
  if (clients.empty())
    snapin->auth_pin(this);  // ...and pin snapped/old inode!
  
  clients.insert(client);
}

void CInode::remove_need_snapflush(CInode *snapin, snapid_t snapid, client_t client)
{
  dout(10) << "remove_need_snapflush client." << client << " snapid " << snapid << " on " << snapin << dendl;
  set<client_t>& clients = client_need_snapflush[snapid];
  clients.erase(client);
  if (clients.empty()) {
    client_need_snapflush.erase(snapid);
    snapin->auth_unpin(this);

    if (client_need_snapflush.empty()) {
      put(CInode::PIN_NEEDSNAPFLUSH);
      auth_unpin(this);
    }
  }
}


inode_t *CInode::project_inode(map<string,bufferptr> *px) 
{
  if (projected_nodes.empty()) {
    projected_nodes.push_back(new projected_inode_t(new inode_t(inode)));
    if (px)
      *px = xattrs;
    projected_nodes.back()->dir_layout = default_layout;
  } else {
    default_file_layout *last_dl = projected_nodes.back()->dir_layout;
    projected_nodes.push_back(new projected_inode_t(
        new inode_t(*projected_nodes.back()->inode)));
    if (px)
      *px = *get_projected_xattrs();
    projected_nodes.back()->dir_layout = last_dl;
  }
  projected_nodes.back()->xattrs = px;
  projected_nodes.back()->inode->version = get_projected_version();
  dout(15) << "project_inode " << projected_nodes.back()->inode << dendl;
  return projected_nodes.back()->inode;
}

void CInode::pop_and_dirty_projected_inode(LogSegment *ls) 
{
  assert(!projected_nodes.empty());
  projected_inode_t *projected = projected_nodes.front();
  dout(15) << "pop_and_dirty_projected_inode " << projected->inode
	   << " v" << get_projected_version() << dendl;
  mark_dirty(ls);

  // save original parents
  list<inoparent_t> parents;
  swap(parents, inode.parents);

  inode = *projected->inode;

  // restore original parents
  swap(parents, inode.parents);
  update_inoparents(inode.parents, projected->removed_parent,
                    projected->added_parent);

  map<string,bufferptr> *px = projected->xattrs;
  if (px) {
    xattrs = *px;
    delete px;
  }

  if (projected->dir_layout != default_layout) {
    delete default_layout;
    default_layout = projected->dir_layout;
  }

  delete projected->inode;
  delete projected;

  projected_nodes.pop_front();
}

void CInode::get_projected_parents(list<inoparent_t> &parents)
{
  // start with current, non-projected inode parents
  parents = inode.parents;
  // apply each projected change
  typedef list<projected_inode_t*>::const_iterator node_iter;
  for (node_iter i = projected_nodes.begin(); i != projected_nodes.end(); ++i)
    update_inoparents(parents, (*i)->removed_parent, (*i)->added_parent);
}

void CInode::project_added_parent(const inoparent_t &parent)
{
  assert(!projected_nodes.empty());
  projected_nodes.back()->added_parent = parent;
  get_projected_parents(projected_nodes.back()->inode->parents);
}

void CInode::project_removed_parent(const inoparent_t &parent)
{
  assert(!projected_nodes.empty());
  projected_nodes.back()->removed_parent = parent;
  get_projected_parents(projected_nodes.back()->inode->parents);
}

void CInode::project_renamed_parent(const inoparent_t &removed,
                                    const inoparent_t &added)
{
  assert(!projected_nodes.empty());
  projected_nodes.back()->removed_parent = removed;
  projected_nodes.back()->added_parent = added;
  get_projected_parents(projected_nodes.back()->inode->parents);
}


// pins

void CInode::first_get()
{
  // pin my dentry?
  if (parent) 
    parent->get(CDentry::PIN_INODEPIN);
}

void CInode::last_put() 
{
  // unpin my dentry?
  if (parent) 
    parent->put(CDentry::PIN_INODEPIN);
}

void CInode::add_remote_parent(CDentry *p) 
{
  if (remote_parents.empty())
    get(PIN_REMOTEPARENT);
  remote_parents.insert(p);
}
void CInode::remove_remote_parent(CDentry *p) 
{
  remote_parents.erase(p);
  if (remote_parents.empty())
    put(PIN_REMOTEPARENT);
}




CDirFrag *CInode::get_parent_dir()
{
  if (parent)
    return parent->dir;
  return NULL;
}
CDirFrag *CInode::get_projected_parent_dir()
{
  CDentry *p = get_projected_parent_dn();
  if (p)
    return p->dir;
  return NULL;
}
CDirStripe* CInode::get_parent_stripe()
{
  CDirFrag *dir = get_parent_dir();
  return dir ? dir->get_stripe() : NULL;
}
CDirStripe* CInode::get_projected_parent_stripe()
{
  CDirFrag *dir = get_projected_parent_dir();
  return dir ? dir->get_stripe() : NULL;
}
CInode *CInode::get_parent_inode() 
{
  if (parent) 
    return parent->dir->get_inode();
  return NULL;
}

bool CInode::is_projected_ancestor_of(CInode *other)
{
  while (other) {
    if (other == this)
      return true;
    if (!other->get_projected_parent_dn())
      break;
    other = other->get_projected_parent_dn()->get_dir()->get_inode();
  }
  return false;
}

void CInode::_mark_dirty(LogSegment *ls)
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
    assert(ls);
  }
  
  // move myself to this segment's dirty list
  if (ls) 
    ls->dirty_inodes.push_back(&item_dirty);
}

void CInode::mark_dirty(LogSegment *ls) {
  
  dout(10) << "mark_dirty " << *this << dendl;

  /*
    NOTE: I may already be dirty, but this fn _still_ needs to be called so that
    the directory is (perhaps newly) dirtied, and so that parent_dir_version is 
    updated below.
  */
  
  // only auth can get dirty.  "dirty" async data in replicas is relative to
  // filelock state, not the dirty flag.
  assert(is_auth());
  
  // touch my private version
  inode.version++;
  _mark_dirty(ls);

  // mark dentry too
  if (parent)
    parent->mark_dirty(ls);
}


void CInode::mark_clean()
{
  dout(10) << " mark_clean " << *this << dendl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
    
    // remove myself from ls dirty list
    item_dirty.remove_myself();
  }
}    


// --------------
// per-inode storage
// (currently for root inode only)

struct C_Inode_Stored : public Context {
  CInode *in;
  version_t version;
  Context *fin;
  C_Inode_Stored(CInode *i, version_t v, Context *f) : in(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored(version, fin);
  }
};

object_t CInode::get_object_name(inodeno_t ino, frag_t fg, const char *suffix)
{
  char n[60];
  snprintf(n, sizeof(n), "%llx.%08llx%s", (long long unsigned)ino, (long long unsigned)fg, suffix ? suffix : "");
  return object_t(n);
}

void CInode::store(Context *fin)
{
  dout(10) << "store " << get_version() << dendl;
  assert(is_base());

  // encode
  bufferlist bl;
  string magic = CEPH_FS_ONDISK_MAGIC;
  ::encode(magic, bl);
  encode_store(bl);

  // write it.
  SnapContext snapc;
  ObjectOperation m;
  m.write_full(bl);

  object_t oid = CInode::get_object_name(ino(), frag_t(), ".inode");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  mdcache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0,
				 NULL, new C_Inode_Stored(this, get_version(), fin) );
}

void CInode::_stored(version_t v, Context *fin)
{
  dout(10) << "_stored " << v << " " << *this << dendl;
  if (v == get_projected_version())
    mark_clean();

  fin->finish(0);
  delete fin;
}

struct C_Inode_Fetched : public Context {
  CInode *in;
  bufferlist bl, bl2;
  Context *fin;
  C_Inode_Fetched(CInode *i, Context *f) : in(i), fin(f) {}
  void finish(int r) {
    in->_fetched(bl, bl2, fin);
  }
};

void CInode::fetch(Context *fin)
{
  dout(10) << "fetch" << dendl;

  C_Inode_Fetched *c = new C_Inode_Fetched(this, fin);
  C_GatherBuilder gather(g_ceph_context, c);

  object_t oid = CInode::get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  ObjectOperation rd;
  rd.getxattr("inode", &c->bl, NULL);

  mdcache->mds->objecter->read(oid, oloc, rd, CEPH_NOSNAP, (bufferlist*)NULL, 0, gather.new_sub());

  // read from separate object too
  object_t oid2 = CInode::get_object_name(ino(), frag_t(), ".inode");
  mdcache->mds->objecter->read(oid2, oloc, 0, 0, CEPH_NOSNAP, &c->bl2, 0, gather.new_sub());

  gather.activate();
}

void CInode::_fetched(bufferlist& bl, bufferlist& bl2, Context *fin)
{
  dout(10) << "_fetched got " << bl.length() << " and " << bl2.length() << dendl;
  bufferlist::iterator p;
  if (bl2.length())
    p = bl2.begin();
  else
    p = bl.begin();
  string magic;
  ::decode(magic, p);
  dout(10) << " magic is '" << magic << "' (expecting '" << CEPH_FS_ONDISK_MAGIC << "')" << dendl;
  if (magic != CEPH_FS_ONDISK_MAGIC) {
    dout(0) << "on disk magic '" << magic << "' != my magic '" << CEPH_FS_ONDISK_MAGIC
	    << "'" << dendl;
    fin->finish(-EINVAL);
  } else {
    decode_store(p);
    dout(10) << "_fetched " << *this << dendl;
    fin->finish(0);
  }
  delete fin;
}



// ------------------
// parent dir

void CInode::build_backtrace(inode_backtrace_t& bt)
{
  bt.ino = inode.ino;
  bt.ancestors.clear();

  CInode *in = this;
  CDentry *pdn = get_parent_dn();
  while (pdn) {
    CInode *diri = pdn->get_dir()->get_inode();
    bt.ancestors.push_back(inode_backpointer_t(diri->ino(), pdn->name, in->inode.version));
    in = diri;
    pdn = in->get_parent_dn();
  }
}

unsigned CInode::encode_parent_mutation(ObjectOperation& m)
{
  string path;
  make_path_string(path);
  m.setxattr("path", path);

  inode_backtrace_t bt;
  build_backtrace(bt);
  
  bufferlist parent;
  ::encode(bt, parent);
  m.setxattr("parent", parent);
  return path.length() + parent.length();
}

struct C_Inode_StoredParent : public Context {
  CInode *in;
  version_t version;
  Context *fin;
  C_Inode_StoredParent(CInode *i, version_t v, Context *f) : in(i), version(v), fin(f) {}
  void finish(int r) {
    in->_stored_parent(version, fin);
  }
};

void CInode::store_parent(Context *fin)
{
  dout(10) << "store_parent" << dendl;
  
  ObjectOperation m;
  encode_parent_mutation(m);

  // write it.
  SnapContext snapc;

  object_t oid = get_object_name(ino(), frag_t(), "");
  object_locator_t oloc(mdcache->mds->mdsmap->get_metadata_pool());

  mdcache->mds->objecter->mutate(oid, oloc, m, snapc, ceph_clock_now(g_ceph_context), 0,
				 NULL, new C_Inode_StoredParent(this, inode.last_renamed_version, fin) );

}

void CInode::_stored_parent(version_t v, Context *fin)
{
  if (state_test(STATE_DIRTYPARENT)) {
    if (v == inode.last_renamed_version) {
      dout(10) << "stored_parent committed v" << v << ", removing from list" << dendl;
      item_renamed_file.remove_myself();
      state_clear(STATE_DIRTYPARENT);
      put(PIN_DIRTYPARENT);
    } else {
      dout(10) << "stored_parent committed v" << v << " < " << inode.last_renamed_version
	       << ", renamed again, not removing from list" << dendl;
    }
  } else {
    dout(10) << "stored_parent committed v" << v << ", tho i wasn't on the renamed_files list" << dendl;
  }
  if (fin) {
    fin->finish(0);
    delete fin;
  }
}


// ------------------
// locking

void CInode::set_object_info(MDSCacheObjectInfo &info)
{
  info.ino = ino();
  info.dirfrag.stripe.stripeid = CEPH_CAP_OBJECT_INODE;
  info.snapid = last;
}

void CInode::encode_lock_state(int type, bufferlist& bl)
{
  ::encode(first, bl);

  switch (type) {
  case CEPH_LOCK_IAUTH:
    ::encode(inode.ctime, bl);
    ::encode(inode.mode, bl);
    ::encode(inode.uid, bl);
    ::encode(inode.gid, bl);  
    break;
    
  case CEPH_LOCK_ILINK:
    ::encode(inode.ctime, bl);
    ::encode(inode.nlink, bl);
    ::encode(inode.anchored, bl);
    ::encode(inode.parents, bl);
    break;

  case CEPH_LOCK_IFILE:
    if (is_auth()) {
      ::encode(inode.layout, bl);
      ::encode(inode.size, bl);
      ::encode(inode.mtime, bl);
      ::encode(inode.atime, bl);
      ::encode(inode.time_warp_seq, bl);
      ::encode(inode.client_ranges, bl);
      ::encode(inode.dirstat, bl);
    }
    break;

  case CEPH_LOCK_INEST:
    if (is_auth())
      ::encode(inode.rstat, bl);
    break;
    
  case CEPH_LOCK_IXATTR:
    ::encode(xattrs, bl);
    break;

  case CEPH_LOCK_IFLOCK:
    ::encode(fcntl_locks, bl);
    ::encode(flock_locks, bl);
    break;

  case CEPH_LOCK_IPOLICY:
    if (inode.is_dir()) {
      ::encode((default_layout ? true : false), bl);
      if (default_layout)
        encode(*default_layout, bl);
    }
    break;
  
  default:
    assert(0);
  }
}


/* for more info on scatterlocks, see comments by Locker::scatter_writebehind */

void CInode::decode_lock_state(int type, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  utime_t tm;

  snapid_t newfirst;
  ::decode(newfirst, p);

  if (!is_auth() && newfirst != first) {
    dout(10) << "decode_lock_state first " << first << " -> " << newfirst << dendl;
    assert(newfirst > first);
    if (!is_multiversion() && parent) {
      assert(parent->first == first);
      parent->first = newfirst;
    }
    first = newfirst;
  }

  switch (type) {
  case CEPH_LOCK_IAUTH:
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.mode, p);
    ::decode(inode.uid, p);
    ::decode(inode.gid, p);
    break;

  case CEPH_LOCK_ILINK:
    ::decode(tm, p);
    if (inode.ctime < tm) inode.ctime = tm;
    ::decode(inode.nlink, p);
    ::decode(inode.anchored, p);
    ::decode(inode.parents, p);
    break;

  case CEPH_LOCK_IFILE:
    if (!is_auth()) {
      ::decode(inode.layout, p);
      ::decode(inode.size, p);
      ::decode(inode.mtime, p);
      ::decode(inode.atime, p);
      ::decode(inode.time_warp_seq, p);
      ::decode(inode.client_ranges, p);
      ::decode(inode.dirstat, p);
    }
    break;

  case CEPH_LOCK_INEST:
    if (!is_auth())
      ::decode(inode.rstat, p);
    break;

  case CEPH_LOCK_IXATTR:
    ::decode(xattrs, p);
    break;

  case CEPH_LOCK_IFLOCK:
    ::decode(fcntl_locks, p);
    ::decode(flock_locks, p);
    break;

  case CEPH_LOCK_IPOLICY:
    if (inode.is_dir()) {
      bool default_layout_exists;
      ::decode(default_layout_exists, p);
      if (default_layout_exists) {
        delete default_layout;
        default_layout = new default_file_layout;
        decode(*default_layout, p);
      }
    }
    break;

  default:
    assert(0);
  }
}


bool CInode::is_dirty_scattered()
{
  return
    filelock.is_dirty_or_flushing() ||
    nestlock.is_dirty_or_flushing();
}

void CInode::clear_scatter_dirty()
{
  filelock.remove_dirty();
  nestlock.remove_dirty();
}

void CInode::clear_dirty_scattered(int type)
{
}

// waiting

bool CInode::is_frozen()
{
  if (is_frozen_inode()) return true;
  if (parent && parent->dir->is_frozen()) return true;
  return false;
}

bool CInode::is_frozen_dir()
{
  if (parent && parent->dir->is_frozen_dir()) return true;
  return false;
}

bool CInode::is_freezing()
{
  if (is_freezing_inode()) return true;
  if (parent && parent->dir->is_freezing()) return true;
  return false;
}

void CInode::add_waiter(uint64_t tag, Context *c) 
{
  dout(10) << "add_waiter tag " << std::hex << tag << std::dec << " " << c
	   << " !ambig " << !state_test(STATE_AMBIGUOUSAUTH)
	   << " !frozen " << !is_frozen_inode()
	   << " !freezing " << !is_freezing_inode()
	   << dendl;
  // wait on the directory?
  //  make sure its not the inode that is explicitly ambiguous|freezing|frozen
  if (((tag & WAIT_SINGLEAUTH) && !state_test(STATE_AMBIGUOUSAUTH)) ||
      ((tag & WAIT_UNFREEZE) &&
       !is_frozen_inode() && !is_freezing_inode() && !is_frozen_auth_pin())) {
    dout(15) << "passing waiter up tree" << dendl;
    parent->dir->add_waiter(tag, c);
    return;
  }
  dout(15) << "taking waiter here" << dendl;
  MDSCacheObject::add_waiter(tag, c);
}

bool CInode::freeze_inode(int auth_pin_allowance)
{
  assert(auth_pin_allowance > 0);  // otherwise we need to adjust parent's nested_auth_pins
  assert(auth_pins >= auth_pin_allowance);
  if (auth_pins > auth_pin_allowance) {
    dout(10) << "freeze_inode - waiting for auth_pins to drop to " << auth_pin_allowance << dendl;
    auth_pin_freeze_allowance = auth_pin_allowance;
    get(PIN_FREEZING);
    state_set(STATE_FREEZING);
    return false;
  }

  dout(10) << "freeze_inode - frozen" << dendl;
  assert(auth_pins == auth_pin_allowance);
  if (!state_test(STATE_FROZEN)) {
    get(PIN_FROZEN);
    state_set(STATE_FROZEN);
  }
  return true;
}

void CInode::unfreeze_inode(list<Context*>& finished) 
{
  dout(10) << "unfreeze_inode" << dendl;
  if (state_test(STATE_FREEZING)) {
    state_clear(STATE_FREEZING);
    put(PIN_FREEZING);
  } else if (state_test(STATE_FROZEN)) {
    state_clear(STATE_FROZEN);
    put(PIN_FROZEN);
  } else 
    assert(0);
  take_waiting(WAIT_UNFREEZE, finished);
}

void CInode::unfreeze_inode()
{
    list<Context*> finished;
    unfreeze_inode(finished);
    mdcache->mds->queue_waiters(finished);
}

void CInode::freeze_auth_pin()
{
  assert(state_test(CInode::STATE_FROZEN));
  state_set(CInode::STATE_FROZENAUTHPIN);
}

void CInode::unfreeze_auth_pin()
{
  assert(state_test(CInode::STATE_FROZENAUTHPIN));
  state_clear(CInode::STATE_FROZENAUTHPIN);
  if (!state_test(STATE_FREEZING|STATE_FROZEN)) {
    list<Context*> finished;
    take_waiting(WAIT_UNFREEZE, finished);
    mdcache->mds->queue_waiters(finished);
  }
}

void CInode::clear_ambiguous_auth(list<Context*>& finished)
{
  assert(state_test(CInode::STATE_AMBIGUOUSAUTH));
  state_clear(CInode::STATE_AMBIGUOUSAUTH);
  take_waiting(CInode::WAIT_SINGLEAUTH, finished);
}

void CInode::clear_ambiguous_auth()
{
  list<Context*> finished;
  clear_ambiguous_auth(finished);
  mdcache->mds->queue_waiters(finished);
}

// auth_pins
bool CInode::can_auth_pin() {
  if (is_freezing_inode() || is_frozen_inode() || is_frozen_auth_pin())
    return false;
  if (parent)
    return parent->can_auth_pin();
  return true;
}

void CInode::auth_pin(void *by) 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

#ifdef MDS_AUTHPIN_SET
  auth_pin_set.insert(by);
#endif

  dout(10) << "auth_pin by " << by << " on " << *this
	   << " now " << auth_pins << dendl;
}

void CInode::auth_unpin(void *by) 
{
  auth_pins--;

#ifdef MDS_AUTHPIN_SET
  assert(auth_pin_set.count(by));
  auth_pin_set.erase(auth_pin_set.find(by));
#endif

  if (auth_pins == 0)
    put(PIN_AUTHPIN);
 
  dout(10) << "auth_unpin by " << by << " on " << *this
	   << " now " << auth_pins << dendl;
 
  assert(auth_pins >= 0);

  if (is_freezing_inode() &&
      auth_pins == auth_pin_freeze_allowance) {
    dout(10) << "auth_unpin freezing!" << dendl;
    get(PIN_FROZEN);
    put(PIN_FREEZING);
    state_clear(STATE_FREEZING);
    state_set(STATE_FROZEN);
    finish_waiting(WAIT_FROZEN);
  }
}

// SNAP

snapid_t CInode::get_oldest_snap()
{
  snapid_t t = first;
  if (!old_inodes.empty())
    t = old_inodes.begin()->second.first;
  return MIN(t, first);
}

old_inode_t& CInode::cow_old_inode(snapid_t follows, bool cow_head)
{
  assert(follows >= first);

  inode_t *pi = cow_head ? get_projected_inode() : get_previous_projected_inode();
  map<string,bufferptr> *px = cow_head ? get_projected_xattrs() : get_previous_projected_xattrs();

  old_inode_t &old = old_inodes[follows];
  old.first = first;
  old.inode = *pi;
  old.xattrs = *px;
  
  dout(10) << " " << px->size() << " xattrs cowed, " << *px << dendl;

  old.inode.trim_client_ranges(follows);

  if (!(old.inode.rstat == old.inode.accounted_rstat))
    dirty_old_rstats.insert(follows);
  
  first = follows+1;

  dout(10) << "cow_old_inode " << (cow_head ? "head" : "previous_head" )
	   << " to [" << old.first << "," << follows << "] on "
	   << *this << dendl;

  return old;
}

void CInode::pre_cow_old_inode()
{
  snapid_t follows = mdcache->get_snaprealm()->get_newest_seq();
  if (first <= follows)
    cow_old_inode(follows, true);
}

void CInode::purge_stale_snap_data(const set<snapid_t>& snaps)
{
  dout(10) << "purge_stale_snap_data " << snaps << dendl;

  if (old_inodes.empty())
    return;

  map<snapid_t,old_inode_t>::iterator p = old_inodes.begin();
  while (p != old_inodes.end()) {
    set<snapid_t>::const_iterator q = snaps.lower_bound(p->second.first);
    if (q == snaps.end() || *q > p->first) {
      dout(10) << " purging old_inode [" << p->second.first << "," << p->first << "]" << dendl;
      old_inodes.erase(p++);
    } else
      p++;
  }
}

/*
 * pick/create an old_inode
 */
old_inode_t * CInode::pick_old_inode(snapid_t snap)
{
  map<snapid_t, old_inode_t>::iterator p = old_inodes.lower_bound(snap);  // p is first key >= to snap
  if (p != old_inodes.end() && p->second.first <= snap) {
    dout(10) << "pick_old_inode snap " << snap << " -> [" << p->second.first << "," << p->first << "]" << dendl;
    return &p->second;
  }
  dout(10) << "pick_old_inode snap " << snap << " -> nothing" << dendl;
  return NULL;
}


// =============================================

Capability* CInode::add_client_cap(client_t client, Session *session,
                                   SnapRealm *conrealm)
{
  if (client_caps.empty())
    mdcache->num_inodes_with_caps++;

  return CapObject::add_client_cap(client, session, conrealm);
}

void CInode::remove_client_cap(client_t client)
{
  dout(10) << "remove_client_cap " << client << dendl;

  Locker *locker = mdcache->mds->locker;

  // clean out any pending snapflush state
  if (!client_need_snapflush.empty())
    locker->do_null_snapflush(this, client, 0);

  CapObject::remove_client_cap(client);

  if (is_auth()) {
    // make sure we clear out the client byte range
    if (get_projected_inode()->client_ranges.count(client)
        && !(inode.nlink == 0 && !is_any_caps()))
      // unless it's unlink + stray
      locker->check_inode_max_size(this);
  } else {
    locker->request_mds_caps(this);
  }

  locker->try_eval(this, CEPH_CAP_LOCKS);

  mdcache->maybe_eval_stray(this);

  if (client_caps.empty())
    mdcache->num_inodes_with_caps--;
}

// caps allowed
int CInode::get_caps_liked()
{
  if (is_dir())
    return CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;  // but not, say, FILE_RD|WR|WRBUFFER
  else
    return CEPH_CAP_ANY & ~CEPH_CAP_FILE_LAZYIO;
}

int CInode::get_caps_allowed_ever()
{
  int allowed;
  if (is_dir())
    allowed = CEPH_CAP_PIN | CEPH_CAP_ANY_EXCL | CEPH_CAP_ANY_SHARED;
  else
    allowed = CEPH_CAP_ANY;
  return allowed & CapObject::get_caps_allowed_ever();
}

void CInode::replicate_relax_locks()
{
  //dout(10) << " relaxing locks on " << *this << dendl;
  assert(is_auth());
  assert(!is_replicated());
  
  authlock.replicate_relax();
  linklock.replicate_relax();
  filelock.replicate_relax();
  xattrlock.replicate_relax();
  nestlock.replicate_relax();
  flocklock.replicate_relax();
  policylock.replicate_relax();
}



// =============================================

int CInode::encode_inodestat(bufferlist& bl, Session *session,
			      SnapRealm *dir_realm,
			      snapid_t snapid, unsigned max_bytes)
{
  int client = session->inst.name.num();
  assert(snapid);
  assert(session->connection);
  
  bool valid = true;

  // do not issue caps if inode differs from readdir snaprealm
  SnapRealm *realm = mdcache->get_snaprealm();
  bool no_caps = (realm && dir_realm && realm != dir_realm) ||
		 is_frozen() || state_test(CInode::STATE_EXPORTINGCAPS);
  if (no_caps)
    dout(20) << "encode_inodestat no caps"
	     << ((realm && dir_realm && realm != dir_realm)?", snaprealm differs ":"")
	     << (state_test(CInode::STATE_EXPORTINGCAPS)?", exporting caps":"")
	     << (is_frozen()?", frozen inode":"") << dendl;

  // pick a version!
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();

  map<string, bufferptr> *pxattrs = 0;

  if (snapid != CEPH_NOSNAP && is_multiversion()) {

    // for now at least, old_inodes is only defined/valid on the auth
    if (!is_auth())
      valid = false;

    map<snapid_t,old_inode_t>::iterator p = old_inodes.lower_bound(snapid);
    if (p != old_inodes.end()) {
      if (p->second.first > snapid) {
        if  (p != old_inodes.begin())
          --p;
        else dout(0) << "old_inode lower_bound starts after snapid!" << dendl;
      }
      dout(15) << "encode_inodestat snapid " << snapid
	       << " to old_inode [" << p->second.first << "," << p->first << "]" 
	       << " " << p->second.inode.rstat
	       << dendl;
      assert(p->second.first <= snapid && snapid <= p->first);
      pi = oi = &p->second.inode;
      pxattrs = &p->second.xattrs;
    }
  }
  
  /*
   * note: encoding matches struct ceph_client_reply_inode
   */
  struct ceph_mds_reply_inode e;
  memset(&e, 0, sizeof(e));
  e.ino = oi->ino;
  e.snapid = snapid;  // 0 -> NOSNAP
  e.rdev = oi->rdev;

  // "fake" a version that is old (stable) version, +1 if projected.
  e.version = (oi->version * 2) + is_projected();


  Capability *cap = get_client_cap(client);
  bool pfile = filelock.is_xlocked_by_client(client) || get_loner() == client;
  //(cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
  bool pauth = authlock.is_xlocked_by_client(client) || get_loner() == client;
  bool plink = linklock.is_xlocked_by_client(client) || get_loner() == client;
  bool pxattr = xattrlock.is_xlocked_by_client(client) || get_loner() == client;
  bool ppolicy = policylock.is_xlocked_by_client(client) || get_loner()==client;
  
  inode_t *i = (pfile|pauth|plink|pxattr) ? pi : oi;
  i->ctime.encode_timeval(&e.ctime);
  
  dout(20) << " pfile " << pfile << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " ctime " << i->ctime
	   << " valid=" << valid << dendl;

  // file
  i = pfile ? pi:oi;
  if (is_file()) {
    e.layout = i->layout;
  } else if (is_dir()) {
    ceph_file_layout *l = ppolicy ? get_projected_dir_layout() : ( default_layout ? &default_layout->layout : NULL );
    if (l)
      e.layout = *l;
    else
      memset(&e.layout, 0, sizeof(e.layout));
  } else {
    memset(&e.layout, 0, sizeof(e.layout));
  }
  e.size = i->size;
  e.truncate_seq = i->truncate_seq;
  e.truncate_size = i->truncate_size;
  i->mtime.encode_timeval(&e.mtime);
  i->atime.encode_timeval(&e.atime);
  e.time_warp_seq = i->time_warp_seq;

  // max_size is min of projected, actual
  e.max_size = MIN(oi->client_ranges.count(client) ? oi->client_ranges[client].range.last : 0,
		   pi->client_ranges.count(client) ? pi->client_ranges[client].range.last : 0);

  e.files = i->dirstat.nfiles;
  e.subdirs = i->dirstat.nsubdirs;

  // nest (do same as file... :/)
  i->rstat.rctime.encode_timeval(&e.rctime);
  e.rbytes = i->rstat.rbytes;
  e.rfiles = i->rstat.rfiles;
  e.rsubdirs = i->rstat.rsubdirs;

  // auth
  i = pauth ? pi:oi;
  e.mode = i->mode;
  e.uid = i->uid;
  e.gid = i->gid;

  // link
  i = plink ? pi:oi;
  e.nlink = i->nlink;
  
  // xattr
  i = pxattr ? pi:oi;
  bool had_latest_xattrs = cap && (cap->issued() & CEPH_CAP_XATTR_SHARED) &&
    cap->client_xattr_version == i->xattr_version &&
    snapid == CEPH_NOSNAP;

  // xattr
  bufferlist xbl;
  e.xattr_version = i->xattr_version;
  if (!had_latest_xattrs) {
    if (!pxattrs)
      pxattrs = pxattr ? get_projected_xattrs() : &xattrs;
    ::encode(*pxattrs, xbl);
  }
  
  // do we have room?
  if (max_bytes) {
    unsigned bytes = sizeof(e);
    bytes += sizeof(__u32);
    if (is_dir())
      bytes += sizeof(__u32) * placement->get_stripe_count();
    bytes += sizeof(__u32) + symlink.length();
    bytes += sizeof(__u32) + xbl.length();
    if (bytes > max_bytes)
      return -ENOSPC;
  }


  // encode caps
  if (snapid != CEPH_NOSNAP) {
    /*
     * snapped inodes (files or dirs) only get read-only caps.  always
     * issue everything possible, since it is read only.
     *
     * if a snapped inode has caps, limit issued caps based on the
     * lock state.
     *
     * if it is a live inode, limit issued caps based on the lock
     * state.
     *
     * do NOT adjust cap issued state, because the client always
     * tracks caps per-snap and the mds does either per-interval or
     * multiversion.
     */
    e.cap.caps = valid ? get_caps_allowed_by_type(CAP_ANY) : CEPH_STAT_CAP_INODE;
    if (last == CEPH_NOSNAP || is_any_caps())
      e.cap.caps = e.cap.caps & get_caps_allowed_for_client(client);
    e.cap.seq = 0;
    e.cap.mseq = 0;
    e.cap.realm = 0;
  } else {
    if (!no_caps && valid && !cap) {
      // add a new cap
      cap = add_client_cap(client, session, realm);
      if (is_auth()) {
	if (choose_ideal_loner() >= 0)
	  try_set_loner();
	else if (get_wanted_loner() < 0)
	  try_drop_loner();
      }
    }

    if (!no_caps && valid && cap) {
      int likes = get_caps_liked();
      int allowed = get_caps_allowed_for_client(client);
      int issue = (cap->wanted() | likes) & allowed;
      cap->issue_norevoke(issue);
      issue = cap->pending();
      cap->set_last_issue();
      cap->set_last_issue_stamp(ceph_clock_now(g_ceph_context));
      e.cap.caps = issue;
      e.cap.wanted = cap->wanted();
      e.cap.cap_id = cap->get_cap_id();
      e.cap.seq = cap->get_last_seq();
      dout(10) << "encode_inodestat issueing " << ccap_string(issue) << " seq " << cap->get_last_seq() << dendl;
      e.cap.mseq = cap->get_mseq();
      e.cap.realm = MDS_INO_ROOT;
    } else {
      e.cap.cap_id = 0;
      e.cap.caps = 0;
      e.cap.seq = 0;
      e.cap.mseq = 0;
      e.cap.realm = 0;
      e.cap.wanted = 0;
    }
  }
  e.cap.flags = is_auth() ? CEPH_CAP_FLAG_AUTH:0;
  dout(10) << "encode_inodestat caps " << ccap_string(e.cap.caps)
	   << " seq " << e.cap.seq << " mseq " << e.cap.mseq
	   << " xattrv " << e.xattr_version << " len " << xbl.length()
	   << dendl;

  // include those xattrs?
  if (xbl.length() && cap) {
    if (cap->pending() & CEPH_CAP_XATTR_SHARED) {
      dout(10) << "including xattrs version " << i->xattr_version << dendl;
      cap->client_xattr_version = i->xattr_version;
    } else {
      dout(10) << "dropping xattrs version " << i->xattr_version << dendl;
      xbl.clear(); // no xattrs .. XXX what's this about?!?
    }
  }

  // encode
  e.stripes = is_dir() ? placement->get_stripe_count() : 0;
  ::encode(e, bl);

  if (is_dir()) {
    ::encode(placement->get_layout(), bl);
    const vector<int> &sauth = placement->get_stripe_auth();
    for (vector<int>::const_iterator p = sauth.begin(); p != sauth.end(); ++p)
      ::encode(*p, bl);
  }
  ::encode(symlink, bl);
  ::encode(xbl, bl);

  return valid;
}

void CInode::encode_cap_message(MClientCaps *m, Capability *cap)
{
  client_t client = cap->get_client();

  m->head.ino = inode.ino;

  bool pfile = filelock.is_xlocked_by_client(client) ||
    (cap && (cap->issued() & CEPH_CAP_FILE_EXCL));
  bool pauth = authlock.is_xlocked_by_client(client);
  bool plink = linklock.is_xlocked_by_client(client);
  bool pxattr = xattrlock.is_xlocked_by_client(client);
 
  inode_t *oi = &inode;
  inode_t *pi = get_projected_inode();
  inode_t *i = (pfile|pauth|plink|pxattr) ? pi : oi;
  i->ctime.encode_timeval(&m->inode.ctime);
  
  dout(20) << "encode_cap_message pfile " << pfile
	   << " pauth " << pauth << " plink " << plink << " pxattr " << pxattr
	   << " ctime " << i->ctime << dendl;

  i = pfile ? pi:oi;
  m->inode.layout = i->layout;
  m->inode.size = i->size;
  m->inode.truncate_seq = i->truncate_seq;
  m->inode.truncate_size = i->truncate_size;
  i->mtime.encode_timeval(&m->inode.mtime);
  i->atime.encode_timeval(&m->inode.atime);
  m->inode.time_warp_seq = i->time_warp_seq;

  // max_size is min of projected, actual.
  uint64_t oldms = oi->client_ranges.count(client) ? oi->client_ranges[client].range.last : 0;
  uint64_t newms = pi->client_ranges.count(client) ? pi->client_ranges[client].range.last : 0;
  m->inode.max_size = MIN(oldms, newms);

  i = pauth ? pi:oi;
  m->inode.mode = i->mode;
  m->inode.uid = i->uid;
  m->inode.gid = i->gid;

  i = plink ? pi:oi;
  m->inode.nlink = i->nlink;

  i = pxattr ? pi:oi;
  map<string,bufferptr> *ix = pxattr ? get_projected_xattrs() : &xattrs;
  if ((cap->pending() & CEPH_CAP_XATTR_SHARED) &&
      i->xattr_version > cap->client_xattr_version) {
    dout(10) << "    including xattrs v " << i->xattr_version << dendl;
    ::encode(*ix, m->xattrbl);
    m->inode.xattr_version = i->xattr_version;
    cap->client_xattr_version = i->xattr_version;
  }
}



void CInode::_encode_base(bufferlist& bl)
{
  ::encode(first, bl);
  ::encode(inode, bl);
  ::encode(symlink, bl);
  ::encode(xattrs, bl);
  ::encode(old_inodes, bl);
}
void CInode::_decode_base(bufferlist::iterator& p)
{
  ::decode(first, p);
  ::decode(inode, p);
  ::decode(symlink, p);
  ::decode(xattrs, p);
  ::decode(old_inodes, p);
}

void CInode::_encode_locks_full(bufferlist& bl)
{
  ::encode(authlock, bl);
  ::encode(linklock, bl);
  ::encode(filelock, bl);
  ::encode(xattrlock, bl);
  ::encode(nestlock, bl);
  ::encode(flocklock, bl);
  ::encode(policylock, bl);

  ::encode(loner_cap, bl);
}
void CInode::_decode_locks_full(bufferlist::iterator& p)
{
  ::decode(authlock, p);
  ::decode(linklock, p);
  ::decode(filelock, p);
  ::decode(xattrlock, p);
  ::decode(nestlock, p);
  ::decode(flocklock, p);
  ::decode(policylock, p);

  ::decode(loner_cap, p);
  set_loner_cap(loner_cap);
  want_loner_cap = loner_cap;  // for now, we'll eval() shortly.
}

void CInode::_encode_locks_state_for_replica(bufferlist& bl)
{
  authlock.encode_state_for_replica(bl);
  linklock.encode_state_for_replica(bl);
  filelock.encode_state_for_replica(bl);
  nestlock.encode_state_for_replica(bl);
  xattrlock.encode_state_for_replica(bl);
  flocklock.encode_state_for_replica(bl);
  policylock.encode_state_for_replica(bl);
}
void CInode::_decode_locks_state(bufferlist::iterator& p, bool is_new)
{
  authlock.decode_state(p, is_new);
  linklock.decode_state(p, is_new);
  filelock.decode_state(p, is_new);
  nestlock.decode_state(p, is_new);
  xattrlock.decode_state(p, is_new);
  flocklock.decode_state(p, is_new);
  policylock.decode_state(p, is_new);
}
void CInode::_decode_locks_rejoin(bufferlist::iterator& p, list<Context*>& waiters)
{
  authlock.decode_state_rejoin(p, waiters);
  linklock.decode_state_rejoin(p, waiters);
  filelock.decode_state_rejoin(p, waiters);
  nestlock.decode_state_rejoin(p, waiters);
  xattrlock.decode_state_rejoin(p, waiters);
  flocklock.decode_state_rejoin(p, waiters);
  policylock.decode_state_rejoin(p, waiters);
}


// IMPORT/EXPORT

void CInode::encode_export(bufferlist& bl)
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  _encode_base(bl);

  bool dirty = is_dirty();
  ::encode(dirty, bl);

  ::encode(replica_map, bl);
#if 0
  // include scatterlock info for any bounding CDirFrags
  bufferlist bounding;
  if (inode.is_dir()) {
    for (stripe_map::const_iterator p = stripes.begin(); p != stripes.end(); ++p) {
      CDirStripe *stripe = p->second;
      if (stripe->is_subtree_root()) {
        ::encode(p->first, bounding);
	::encode(stripe->fnode.fragstat, bounding);
	::encode(stripe->fnode.accounted_fragstat, bounding);
	::encode(stripe->fnode.rstat, bounding);
	::encode(stripe->fnode.accounted_rstat, bounding);
	dout(10) << " encoded fragstat/rstat info for " << *stripe << dendl;
      }
    }
  }
  ::encode(bounding, bl);
#endif
  _encode_locks_full(bl);
  get(PIN_TEMPEXPORTING);
}

void CInode::finish_export(utime_t now)
{
  loner_cap = -1;

  put(PIN_TEMPEXPORTING);
}

void CInode::decode_import(bufferlist::iterator& p,
			   LogSegment *ls)
{
  __u8 struct_v;
  ::decode(struct_v, p);

  _decode_base(p);

  bool dirty;
  ::decode(dirty, p);
  if (dirty) 
    _mark_dirty(ls);

  ::decode(replica_map, p);
  if (!replica_map.empty())
    get(PIN_REPLICATED);
#if 0
  if (struct_v >= 2) {
    // decode fragstat info on bounding cdirs
    bufferlist bounding;
    ::decode(bounding, p);
    bufferlist::iterator q = bounding.begin();
    while (!q.end()) {
      stripeid_t stripeid;
      ::decode(stripeid, q);
      CDirStripe *stripe = get_stripe(stripeid);
      assert(stripe);  // we should have all bounds open

      // Only take the remote's fragstat/rstat if we are non-auth for
      // this dirfrag AND the lock is NOT in a scattered (MIX) state.
      // We know lock is stable, and MIX is the only state in which
      // the inode auth (who sent us this data) may not have the best
      // info.

      // HMM: Are there cases where dir->is_auth() is an insufficient
      // check because the dirfrag is under migration?  That implies
      // it is frozen (and in a SYNC or LOCK state).  FIXME.

      if (stripe->is_auth() || filelock.get_state() == LOCK_MIX) {
	dout(10) << " skipped fragstat info for " << *stripe << dendl;
	frag_info_t f;
	::decode(f, q);
	::decode(f, q);
      } else {
	::decode(stripe->fnode.fragstat, q);
	::decode(stripe->fnode.accounted_fragstat, q);
	dout(10) << " took fragstat info for " << *stripe << dendl;
      }
      if (stripe->is_auth() || nestlock.get_state() == LOCK_MIX) {
	dout(10) << " skipped rstat info for " << *stripe << dendl;
	nest_info_t n;
	::decode(n, q);
	::decode(n, q);
      } else {
	::decode(stripe->fnode.rstat, q);
	::decode(stripe->fnode.accounted_rstat, q);
	dout(10) << " took rstat info for " << *stripe << dendl;
      }
    }
  }
#endif
  _decode_locks_full(p);
}

void CInode::encode_store(bufferlist& bl)
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(inode, bl);
  if (is_symlink())
    ::encode(symlink, bl);
  ::encode(xattrs, bl);
  ::encode(old_inodes, bl);
  if (inode.is_dir()) {
    assert(placement);
    ::encode(placement->get_stripe_auth(), bl);
    ::encode((default_layout ? true : false), bl);
    if (default_layout)
      ::encode(*default_layout, bl);
  }
}

void CInode::decode_store(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(inode, bl);
  if (is_symlink())
    ::decode(symlink, bl);
  ::decode(xattrs, bl);
  ::decode(old_inodes, bl);
  if (struct_v >= 2 && inode.is_dir()) {
    vector<int> stripe_auth;
    ::decode(stripe_auth, bl);
    set_stripe_auth(stripe_auth);
    bool default_layout_exists;
    ::decode(default_layout_exists, bl);
    if (default_layout_exists) {
      delete default_layout;
      default_layout = new default_file_layout;
      ::decode(*default_layout, bl);
    }
  }
}

void CInode::encode_replica(int rep, bufferlist& bl)
{
  assert(is_auth());

  // relax locks?
  if (!is_replicated())
    replicate_relax_locks();

  __u32 nonce = add_replica(rep);
  ::encode(nonce, bl);

  _encode_base(bl);
  _encode_locks_state_for_replica(bl);
  if (inode.is_dir()) {
    ::encode((default_layout ? true : false), bl);
    if (default_layout)
      ::encode(*default_layout, bl);
  }
}

void CInode::decode_replica(bufferlist::iterator& p, bool is_new)
{
  __u32 nonce;
  ::decode(nonce, p);
  replica_nonce = nonce;

  _decode_base(p);
  _decode_locks_state(p, is_new);
  if (inode.is_dir()) {
    bool default_layout_exists;
    ::decode(default_layout_exists, p);
    if (default_layout_exists) {
      delete default_layout;
      default_layout = new default_file_layout;
      ::decode(*default_layout, p);
    }
  }
}

