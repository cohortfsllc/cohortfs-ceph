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

#ifndef CEPH_MDS_EMETABLOB_H
#define CEPH_MDS_EMETABLOB_H

#include <stdlib.h>

#include "../CInode.h"
#include "../CDir.h"
#include "../CDentry.h"
#include "../CStripe.h"

#include "include/triple.h"
#include "include/interval_set.h"

class MDS;
class MDLog;
class LogSegment;
class MDSlaveUpdate;

/*
 * a bunch of metadata in the journal
 */

/* notes:
 *
 * - make sure you adjust the inode.version for any modified inode you
 *   journal.  CDir and CDentry maintain a projected_version, but CInode
 *   doesn't, since the journaled inode usually has to be modifed 
 *   manually anyway (to delay the change in the MDS's cache until after
 *   it is journaled).
 *
 */


class EMetaBlob {

public:
  /* fullbit - a regular dentry + inode
   *
   * We encode this one a bit weirdly, just because (also, it's marginally faster
   * on multiple encodes, which I think can happen):
   * Encode a bufferlist on struct creation with all data members, without a struct_v.
   * When encode is called, encode struct_v and then append the bufferlist.
   * Decode straight into the appropriate variables.
   *
   * So, if you add members, encode them in the constructor and then change
   * the struct_v in the encode function!
   */
  struct fullbit {
    string  dn;         // dentry
    snapid_t dnfirst, dnlast;
    version_t dnv;
    inode_t inode;      // if it's not
    pair<int, int> inode_auth;
    vector<int> stripe_auth;
    map<string,bufferptr> xattrs;
    string symlink;
    bufferlist snapbl;
    bool dirty;
    struct default_file_layout *dir_layout;
    typedef map<snapid_t, old_inode_t> old_inodes_t;
    old_inodes_t old_inodes;

    bufferlist _enc;

    fullbit(const fullbit& o);
    const fullbit& operator=(const fullbit& o);

    typedef std::tr1::shared_ptr<fullbit> ptr;

    fullbit(const string& d, snapid_t df, snapid_t dl, version_t v,
            inode_t& i, const pair<int, int> &iauth,
            const vector<int> &sauth,
            const map<string,bufferptr> &xa, const string& sym,
            bufferlist &sbl, bool dr, default_file_layout *defl = NULL,
            old_inodes_t *oi = NULL) :
      //dn(d), dnfirst(df), dnlast(dl), dnv(v), 
      //inode(i), xattrs(xa), symlink(sym), snapbl(sbl), dirty(dr) 
      dir_layout(NULL), _enc(1024)
    {
      ::encode(d, _enc);
      ::encode(df, _enc);
      ::encode(dl, _enc);
      ::encode(v, _enc);
      ::encode(i, _enc);
      ::encode(iauth, _enc);
      ::encode(xa, _enc);
      if (i.is_symlink())
	::encode(sym, _enc);
      if (i.is_dir()) {
        ::encode(sauth, _enc);
	::encode(sbl, _enc);
	::encode((defl ? true : false), _enc);
	if (defl)
	  ::encode(*defl, _enc);
      }
      ::encode(dr, _enc);      
      ::encode(oi ? true : false, _enc);
      if (oi)
	::encode(*oi, _enc);
    }
    fullbit(bufferlist::iterator &p) : dir_layout(NULL) {
      decode(p);
    }
    fullbit() : dir_layout(NULL) {}
    ~fullbit() {
      delete dir_layout;
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 3;
      ::encode(struct_v, bl);
      assert(_enc.length());
      bl.append(_enc); 
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(dn, bl);
      ::decode(dnfirst, bl);
      ::decode(dnlast, bl);
      ::decode(dnv, bl);
      ::decode(inode, bl);
      ::decode(inode_auth, bl);
      ::decode(xattrs, bl);
      if (inode.is_symlink())
	::decode(symlink, bl);
      if (inode.is_dir()) {
	::decode(stripe_auth, bl);
	::decode(snapbl, bl);
	if (struct_v >= 2) {
	  bool dir_layout_exists;
	  ::decode(dir_layout_exists, bl);
	  if (dir_layout_exists) {
	    dir_layout = new default_file_layout;
	    ::decode(*dir_layout, bl);
	  }
	}
      }
      ::decode(dirty, bl);
      if (struct_v >= 3) {
	bool old_inodes_present;
	::decode(old_inodes_present, bl);
	if (old_inodes_present) {
	  ::decode(old_inodes, bl);
	}
      }
    }

    void update_inode(MDS *mds, CInode *in);

    void print(ostream& out) {
      out << " fullbit dn " << dn << " [" << dnfirst << "," << dnlast << "] dnv " << dnv
	  << " inode " << inode.ino
	  << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(fullbit)
  
  /* remotebit - a dentry + remote inode link (i.e. just an ino)
   */
  struct remotebit {
    string dn;
    snapid_t dnfirst, dnlast;
    version_t dnv;
    inodeno_t ino;
    unsigned char d_type;
    bool dirty;

    bufferlist _enc;

    remotebit(const string& d, snapid_t df, snapid_t dl, version_t v, inodeno_t i, unsigned char dt, bool dr) : 
      //dn(d), dnfirst(df), dnlast(dl), dnv(v), ino(i), d_type(dt), dirty(dr) { }
      _enc(256) {
      ::encode(d, _enc);
      ::encode(df, _enc);
      ::encode(dl, _enc);
      ::encode(v, _enc);
      ::encode(i, _enc);
      ::encode(dt, _enc);
      ::encode(dr, _enc);
    }
    remotebit(bufferlist::iterator &p) { decode(p); }
    remotebit() {}

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      assert(_enc.length());
      bl.append(_enc);
      /*
      ::encode(dn, bl);
      ::encode(dnfirst, bl);
      ::encode(dnlast, bl);
      ::encode(dnv, bl);
      ::encode(ino, bl);
      ::encode(d_type, bl);
      ::encode(dirty, bl);
      */
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(dn, bl);
      ::decode(dnfirst, bl);
      ::decode(dnlast, bl);
      ::decode(dnv, bl);
      ::decode(ino, bl);
      ::decode(d_type, bl);
      ::decode(dirty, bl);
    }
    void print(ostream& out) {
      out << " remotebit dn " << dn << " [" << dnfirst << "," << dnlast << "] dnv " << dnv
	  << " ino " << ino
	  << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(remotebit)

  /*
   * nullbit - a null dentry
   */
  struct nullbit {
    string dn;
    snapid_t dnfirst, dnlast;
    version_t dnv;
    bool dirty;

    bufferlist _enc;

    nullbit(const string& d, snapid_t df, snapid_t dl, version_t v, bool dr) : 
      //dn(d), dnfirst(df), dnlast(dl), dnv(v), dirty(dr) { }
      _enc(128) {
      ::encode(d, _enc);
      ::encode(df, _enc);
      ::encode(dl, _enc);
      ::encode(v, _enc);
      ::encode(dr, _enc);
    }
    nullbit(bufferlist::iterator &p) { decode(p); }
    nullbit() {}

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      assert(_enc.length());
      bl.append(_enc);
      /*
      ::encode(dn, bl);
      ::encode(dnfirst, bl);
      ::encode(dnlast, bl);
      ::encode(dnv, bl);
      ::encode(dirty, bl);
      */
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(dn, bl);
      ::decode(dnfirst, bl);
      ::decode(dnlast, bl);
      ::decode(dnv, bl);
      ::decode(dirty, bl);
    }
    void print(ostream& out) {
      out << " nullbit dn " << dn << " [" << dnfirst << "," << dnlast << "] dnv " << dnv
	  << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(nullbit)


  /* dirlump - contains metadata for any dir we have contents for.
   */
public:
  struct dirlump {
    static const int STATE_COMPLETE =    (1<<1);
    static const int STATE_DIRTY =       (1<<2);  // dirty due to THIS journal item, that is!
    static const int STATE_NEW =         (1<<3);  // new directory
    static const int STATE_IMPORTING =	 (1<<4);  // importing directory

    version_t dirv;
    __u32 state;
    __u32 nfull, nremote, nnull;

  private:
    mutable bufferlist dnbl;
    bool dn_decoded;
    list<fullbit::ptr>   dfull;
    list<remotebit> dremote;
    list<nullbit>   dnull;

  public:
    dirlump() : dirv(0), state(0), nfull(0), nremote(0), nnull(0), dn_decoded(true) { }

    bool is_complete() { return state & STATE_COMPLETE; }
    void mark_complete() { state |= STATE_COMPLETE; }
    bool is_dirty() { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }
    bool is_new() { return state & STATE_NEW; }
    void mark_new() { state |= STATE_NEW; }
    bool is_importing() { return state & STATE_IMPORTING; }
    void mark_importing() { state |= STATE_IMPORTING; }

    list<fullbit::ptr>   &get_dfull()   { return dfull; }
    list<remotebit> &get_dremote() { return dremote; }
    list<nullbit>   &get_dnull()   { return dnull; }

    void print(dirfrag_t dirfrag, ostream& out) {
      out << "dirlump " << dirfrag << " v " << dirv
	  << " state " << state
	  << " num " << nfull << "/" << nremote << "/" << nnull
	  << std::endl;
      _decode_bits();
      for (list<fullbit::ptr>::iterator p = dfull.begin(); p != dfull.end(); ++p)
	(*p)->print(out);
      for (list<remotebit>::iterator p = dremote.begin(); p != dremote.end(); ++p)
	p->print(out);
      for (list<nullbit>::iterator p = dnull.begin(); p != dnull.end(); ++p)
	p->print(out);
    }

    void _encode_bits() const {
      ::encode(dfull, dnbl);
      ::encode(dremote, dnbl);
      ::encode(dnull, dnbl);
    }
    void _decode_bits() { 
      if (dn_decoded) return;
      bufferlist::iterator p = dnbl.begin();
      ::decode(dfull, p);
      ::decode(dremote, p);
      ::decode(dnull, p);
      dn_decoded = true;
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(dirv, bl);
      ::encode(state, bl);
      ::encode(nfull, bl);
      ::encode(nremote, bl);
      ::encode(nnull, bl);
      _encode_bits();
      ::encode(dnbl, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(dirv, bl);
      ::decode(state, bl);
      ::decode(nfull, bl);
      ::decode(nremote, bl);
      ::decode(nnull, bl);
      ::decode(dnbl, bl);
      dn_decoded = false;      // don't decode bits unless we need them.
    }
  };
  WRITE_CLASS_ENCODER(dirlump)

  struct stripelump {
    static const int STATE_OPEN =   (1<<0);
    static const int STATE_DIRTY =  (1<<1);
    static const int STATE_NEW =    (1<<2);

    pair<int, int> auth;
    fragtree_t dirfragtree;
    fnode_t fnode;
    __u32 state;

    bool is_open() const { return state & STATE_OPEN; }
    void mark_open() { state |= STATE_OPEN; }
    bool is_dirty() const { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }
    bool is_new() const { return state & STATE_NEW; }
    void mark_new() { state |= STATE_NEW; }

    void encode(bufferlist& bl) const {
      ::encode(auth, bl);
      ::encode(dirfragtree, bl);
      ::encode(fnode, bl);
      ::encode(state, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(auth, bl);
      ::decode(dirfragtree, bl);
      ::decode(fnode, bl);
      ::decode(state, bl);
    }
  };
  WRITE_CLASS_ENCODER(stripelump)

private:
  // my lumps.  preserve the order we added them in a list.
  list<dirfrag_t>         lump_order;
  map<dirfrag_t, dirlump> lump_map;
  map<dirstripe_t, stripelump> stripe_map;
  list<fullbit::ptr> roots;

  list<pair<__u8,version_t> > table_tids;  // tableclient transactions

  inodeno_t opened_ino;
public:
  inodeno_t renamed_dirino;
  list<dirstripe_t> renamed_dir_stripes;
private:
  
  // ino (pre)allocation.  may involve both inotable AND session state.
  version_t inotablev, sessionmapv;
  inodeno_t allocated_ino;            // inotable
  interval_set<inodeno_t> preallocated_inos; // inotable + session
  inodeno_t used_preallocated_ino;    //            session
  entity_name_t client_name;          //            session

  // inodes i've truncated
  list<inodeno_t> truncate_start;        // start truncate 
  map<inodeno_t,uint64_t> truncate_finish;  // finished truncate (started in segment blah)

  vector<inodeno_t> destroyed_inodes;

  // idempotent op(s)
  list<pair<metareqid_t,uint64_t> > client_reqs;

 public:
  void encode(bufferlist& bl) const {
    __u8 struct_v = 4;
    ::encode(struct_v, bl);
    ::encode(lump_order, bl);
    ::encode(lump_map, bl);
    ::encode(stripe_map, bl);
    ::encode(roots, bl);
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
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(lump_order, bl);
    ::decode(lump_map, bl);
    ::decode(stripe_map, bl);
    if (struct_v >= 4) {
      ::decode(roots, bl);
    } else {
      bufferlist rootbl;
      ::decode(rootbl, bl);
      if (rootbl.length()) {
	bufferlist::iterator p = rootbl.begin();
	roots.push_back(fullbit::ptr(new fullbit(p)));
      }
    }
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
    if (struct_v >= 2) {
      ::decode(client_reqs, bl);
    } else {
      list<metareqid_t> r;
      ::decode(r, bl);
      while (!r.empty()) {
	client_reqs.push_back(pair<metareqid_t,uint64_t>(r.front(), 0));
	r.pop_front();
      }
    }
    if (struct_v >= 3) {
      ::decode(renamed_dirino, bl);
      ::decode(renamed_dir_stripes, bl);
    }
  }


  // soft stateadd
  uint64_t last_subtree_map;
  uint64_t my_offset;

  // for replay, in certain cases
  //LogSegment *_segment;

  EMetaBlob(MDLog *mdl = 0);  // defined in journal.cc
  ~EMetaBlob() { }

  void print(ostream& out) {
    for (list<dirfrag_t>::iterator p = lump_order.begin();
	 p != lump_order.end();
	 ++p) {
      lump_map[*p].print(*p, out);
    }
  }

  void add_client_req(metareqid_t r, uint64_t tid=0) {
    client_reqs.push_back(pair<metareqid_t,uint64_t>(r, tid));
  }

  void add_table_transaction(int table, version_t tid) {
    table_tids.push_back(pair<__u8, version_t>(table, tid));
  }

  void add_opened_ino(inodeno_t ino) {
    assert(!opened_ino);
    opened_ino = ino;
  }

  void set_ino_alloc(inodeno_t alloc,
		     inodeno_t used_prealloc,
		     interval_set<inodeno_t>& prealloc,
		     entity_name_t client,
		     version_t sv, version_t iv) {
    allocated_ino = alloc;
    used_preallocated_ino = used_prealloc;
    preallocated_inos = prealloc;
    client_name = client;
    sessionmapv = sv;
    inotablev = iv;
  }

  void add_truncate_start(inodeno_t ino) {
    truncate_start.push_back(ino);
  }
  void add_truncate_finish(inodeno_t ino, uint64_t segoff) {
    truncate_finish[ino] = segoff;
  }

  void add_destroyed_inode(inodeno_t ino) {
    destroyed_inodes.push_back(ino);
  }
  
  void add_null_dentry(CDentry *dn, bool dirty) {
    add_null_dentry(add_dir(dn->get_dir(), false), dn, dirty);
  }
  void add_null_dentry(dirlump& lump, CDentry *dn, bool dirty) {
    // add the dir
    lump.nnull++;
    lump.get_dnull().push_back(nullbit(dn->get_name(), 
				       dn->first, dn->last,
				       dn->get_projected_version(), 
				       dirty));
  }

  void add_remote_dentry(CDentry *dn, bool dirty) {
    add_remote_dentry(add_dir(dn->get_dir(), false), dn, dirty, 0, 0);
  }
  void add_remote_dentry(CDentry *dn, bool dirty, inodeno_t rino, int rdt) {
    add_remote_dentry(add_dir(dn->get_dir(), false), dn, dirty, rino, rdt);
  }
  void add_remote_dentry(dirlump& lump, CDentry *dn, bool dirty, 
			 inodeno_t rino=0, unsigned char rdt=0) {
    if (!rino) {
      rino = dn->get_projected_linkage()->get_remote_ino();
      rdt = dn->get_projected_linkage()->get_remote_d_type();
    }
    lump.nremote++;
    lump.get_dremote().push_back(remotebit(dn->get_name(), 
					   dn->first, dn->last,
					   dn->get_projected_version(), 
					   rino, rdt,
					   dirty));
  }

  // return remote pointer to to-be-journaled inode
  void add_primary_dentry(CDentry *dn, bool dirty, CInode *in=0) {
    add_primary_dentry(add_dir(dn->get_dir(), false),
		       dn, dirty, in);
  }
  void add_primary_dentry(dirlump& lump, CDentry *dn, bool dirty, CInode *in=0) {
    if (!in) 
      in = dn->get_projected_linkage()->get_inode();

    // make note of where this inode was last journaled
    in->last_journaled = my_offset;
    //cout << "journaling " << in->inode.ino << " at " << my_offset << std::endl;

    inode_t *pi = in->get_projected_inode();
    default_file_layout *default_layout = NULL;
    if (in->is_dir())
      default_layout = (in->get_projected_node() ?
                           in->get_projected_node()->dir_layout :
                           in->default_layout);

    bufferlist snapbl;
    sr_t *sr = in->get_projected_srnode();
    if (sr)
      sr->encode(snapbl);

    lump.nfull++;
    lump.get_dfull().push_back(fullbit::ptr(
            new fullbit(dn->get_name(), dn->first, dn->last,
                        dn->get_projected_version(), *pi,
                        in->inode_auth, in->get_stripe_auth(),
                        *in->get_projected_xattrs(), in->symlink,
                        snapbl, dirty, default_layout, &in->old_inodes)));
  }

  // convenience: primary or remote?  figure it out.
  void add_dentry(CDentry *dn, bool dirty) {
    dirlump& lump = add_dir(dn->get_dir(), false);
    add_dentry(lump, dn, dirty);
  }
  void add_dentry(dirlump& lump, CDentry *dn, bool dirty) {
    // primary or remote
    if (dn->get_projected_linkage()->is_remote()) {
      add_remote_dentry(dn, dirty);
      return;
    } else if (dn->get_projected_linkage()->is_null()) {
      add_null_dentry(dn, dirty);
      return;
    }
    assert(dn->get_projected_linkage()->is_primary());
    add_primary_dentry(dn, dirty);
  }

  void add_root(bool dirty, CInode *in, inode_t *pi=0, bufferlist *psnapbl=0,
		    map<string,bufferptr> *px=0) {
    in->last_journaled = my_offset;
    //cout << "journaling " << in->inode.ino << " at " << my_offset << std::endl;

    if (!pi) pi = in->get_projected_inode();
    if (!px) px = &in->xattrs;

    default_file_layout *default_layout = NULL;
    if (in->is_dir())
      default_layout = (in->get_projected_node() ?
                           in->get_projected_node()->dir_layout :
                           in->default_layout);

    bufferlist snapbl;
    if (psnapbl)
      snapbl = *psnapbl;
    else
      in->encode_snap_blob(snapbl);

    for (list<fullbit::ptr>::iterator p = roots.begin(); p != roots.end(); p++) {
      if ((*p)->inode.ino == in->ino()) {
	roots.erase(p);
	break;
      }
    }

    string empty;
    roots.push_back(fullbit::ptr(
            new fullbit(empty, in->first, in->last, 0, *pi,
                        in->inode_auth, in->get_stripe_auth(),
                        *px, in->symlink, snapbl, dirty,
                        default_layout, &in->old_inodes)));
  }
  
  dirlump& add_dir(CDir *dir, bool dirty, bool complete=false) {
    add_stripe(dir->get_stripe(), false);
    return add_dir(dir->dirfrag(), dir->get_projected_version(),
		   dirty, complete);
  }
  dirlump& add_new_dir(CDir *dir) {
    add_stripe(dir->get_stripe(), true);
    return add_dir(dir->dirfrag(), dir->get_projected_version(),
		   true, true, true); // dirty AND complete AND new
  }
  dirlump& add_import_dir(CDir *dir) {
    add_stripe(dir->get_stripe(), false);
    // dirty=false would be okay in some cases
    return add_dir(dir->dirfrag(), dir->get_projected_version(),
		   true, dir->is_complete(), false, true);
  }
  dirlump& add_dir(dirfrag_t df, version_t pv, bool dirty,
                   bool complete=false, bool isnew=false,
                   bool importing=false) {
    if (lump_map.count(df) == 0)
      lump_order.push_back(df);

    dirlump& l = lump_map[df];
    l.dirv = pv;
    if (complete) l.mark_complete();
    if (dirty) l.mark_dirty();
    if (isnew) l.mark_new();
    if (importing) l.mark_importing();
    return l;
  }


  stripelump& add_stripe(CStripe *stripe, bool dirty, bool isnew=false) {
    return add_stripe(stripe->dirstripe(),
                      stripe->get_stripe_auth(),
                      stripe->get_fragtree(),
                      stripe->get_projected_fnode(),
                      stripe->get_projected_version(),
                      stripe->is_open(), dirty, isnew);
  }
  stripelump& add_stripe(dirstripe_t stripe, const pair<int, int> &auth,
                         const fragtree_t &dft,
                         const fnode_t *pf, version_t pv,
                         bool open, bool dirty, bool isnew=false) {
    stripelump& l = stripe_map[stripe];
    l.auth = auth;
    l.dirfragtree = dft;
    l.fnode = *pf;
    l.fnode.version = pv;
    if (open) l.mark_open();
    if (dirty) l.mark_dirty();
    if (isnew) l.mark_new();
    return l;
  }
  

  static const int TO_AUTH_SUBTREE_ROOT = 0;  // default.
  static const int TO_ROOT = 1;
  
  void add_stripe_context(CStripe *stripe, int mode = TO_AUTH_SUBTREE_ROOT);
 
  void print(ostream& out) const {
    out << "[metablob";
    if (!lump_order.empty()) 
      out << " " << lump_order.front() << ", " << lump_map.size() << " dirs";
    if (!table_tids.empty())
      out << " table_tids=" << table_tids;
    if (allocated_ino || preallocated_inos.size()) {
      if (allocated_ino)
	out << " alloc_ino=" << allocated_ino;
      if (preallocated_inos.size())
	out << " prealloc_ino=" << preallocated_inos;
      if (used_preallocated_ino)
	out << " used_prealloc_ino=" << used_preallocated_ino;
      out << " v" << inotablev;
    }
    out << "]";
  }

  void update_segment(LogSegment *ls);
  void update_stripe(MDS *mds, LogSegment *ls,
                     CStripe *stripe, stripelump &lump);
  void replay(MDS *mds, LogSegment *ls, MDSlaveUpdate *su=NULL);
};
WRITE_CLASS_ENCODER(EMetaBlob)
WRITE_CLASS_ENCODER(EMetaBlob::fullbit)
WRITE_CLASS_ENCODER(EMetaBlob::remotebit)
WRITE_CLASS_ENCODER(EMetaBlob::nullbit)
WRITE_CLASS_ENCODER(EMetaBlob::dirlump)
WRITE_CLASS_ENCODER(EMetaBlob::stripelump)

inline ostream& operator<<(ostream& out, const EMetaBlob& t) {
  t.print(out);
  return out;
}

#endif
