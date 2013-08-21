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
#include "../CDentry.h"
#include "../CDirFrag.h"
#include "../CDirPlacement.h"
#include "../CDirStripe.h"

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
 *   journal.  CDirFrag and CDentry maintain a projected_version, but CInode
 *   doesn't, since the journaled inode usually has to be modifed 
 *   manually anyway (to delay the change in the MDS's cache until after
 *   it is journaled).
 *
 */


class EMetaBlob {
 public:
  // journal entry for an inode
  class Inode {
   private:
    bufferlist _enc;

   public:
    inode_t inode;
    pair<int, int> inode_auth;
    vector<int> stripe_auth;
    map<string,bufferptr> xattrs;
    string symlink;
    bool dirty;
    inoparent_t added_parent;
    inoparent_t removed_parent;
    struct default_file_layout *dir_layout;
    typedef map<snapid_t, old_inode_t> old_inodes_t;
    old_inodes_t old_inodes;

    Inode(bufferlist::iterator &p) : dir_layout(NULL) {
      decode(p);
    }
    Inode() : dir_layout(NULL) {}
    ~Inode() {
      delete dir_layout;
    }

    // initialize/overwrite the encoded contents
    void encode(inode_t &i, const pair<int, int> &iauth,
                const vector<int> &sauth, const map<string,bufferptr> &xa,
                const string &sym, bool dr, const inoparent_t *ap = NULL,
                const inoparent_t *rp = NULL, default_file_layout *defl = NULL,
                old_inodes_t *oi = NULL)
    {
      _enc = bufferlist(1024);

      ::encode(i, _enc);
      ::encode(iauth, _enc);
      ::encode(xa, _enc);
      if (i.is_symlink())
        ::encode(sym, _enc);
      if (i.is_dir()) {
        ::encode(sauth, _enc);
        ::encode((defl ? true : false), _enc);
        if (defl)
          ::encode(*defl, _enc);
      }
      ::encode(dr, _enc);
      ::encode(oi ? true : false, _enc);
      if (oi) ::encode(*oi, _enc);
      ::encode(ap ? true : false, _enc);
      if (ap) ::encode(*ap, _enc);
      ::encode(rp ? true : false, _enc);
      if (rp) ::encode(*rp, _enc);
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 4;
      ::encode(struct_v, bl);
      assert(_enc.length());
      bl.append(_enc); 
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(inode, bl);
      ::decode(inode_auth, bl);
      ::decode(xattrs, bl);
      if (inode.is_symlink())
	::decode(symlink, bl);
      if (inode.is_dir()) {
	::decode(stripe_auth, bl);
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
      if (struct_v >= 4) {
        bool has_parent;
        ::decode(has_parent, bl);
        if (has_parent)
          ::decode(added_parent, bl);
        ::decode(has_parent, bl);
        if (has_parent)
          ::decode(removed_parent, bl);
      }
    }

    void apply(MDS *mds, CInode *in, bool isnew);

    void print(ostream& out) {
      out << " inode " << inode.ino << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(Inode)
  typedef map<inodeno_t, Inode> inode_map;

  // journal entry for a dentry
  class Dentry {
   private:
    bufferlist _enc;

   public:
    string name;
    snapid_t first, last;
    version_t version;
    inodeno_t ino;
    unsigned char d_type;
    bool dirty;

    Dentry(const string& d, snapid_t df, snapid_t dl,
           version_t v, inodeno_t i, unsigned char dt, bool dr)
        : _enc(256)
    {
      ::encode(d, _enc);
      ::encode(df, _enc);
      ::encode(dl, _enc);
      ::encode(v, _enc);
      ::encode(i, _enc);
      ::encode(dt, _enc);
      ::encode(dr, _enc);
    }
    Dentry(bufferlist::iterator &p) { decode(p); }
    Dentry() {}

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      assert(_enc.length());
      bl.append(_enc);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(name, bl);
      ::decode(first, bl);
      ::decode(last, bl);
      ::decode(version, bl);
      ::decode(ino, bl);
      ::decode(d_type, bl);
      ::decode(dirty, bl);
    }
    void print(ostream& out) {
      out << " dn " << name << " [" << first << "," << last << "] v " << version
	  << " ino " << ino << " dirty=" << dirty << std::endl;
    }
  };
  WRITE_CLASS_ENCODER(Dentry)
  typedef vector<Dentry> dentry_vec;

  // journal entry for a dir fragment
  class Dir {
   private:
    dentry_vec dentries;

    mutable bufferlist dnbl;
    bool dn_decoded;

    void encode_dentries() const {
      ::encode(dentries, dnbl);
    }
    void decode_dentries() {
      if (dn_decoded) return;
      bufferlist::iterator p = dnbl.begin();
      ::decode(dentries, p);
      dn_decoded = true;
    }

   public:
    static const int STATE_COMPLETE =    (1<<1);
    static const int STATE_DIRTY =       (1<<2);  // dirty due to THIS journal item, that is!
    static const int STATE_NEW =         (1<<3);  // new directory

    version_t version;
    int state;

    Dir() : dn_decoded(true), version(0), state(0) {}

    dentry_vec& get_dentries()
    {
      decode_dentries();
      return dentries;
    }

    bool is_complete() { return state & STATE_COMPLETE; }
    void mark_complete() { state |= STATE_COMPLETE; }
    bool is_dirty() { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }
    bool is_new() { return state & STATE_NEW; }
    void mark_new() { state |= STATE_NEW; }

    void print(dirfrag_t df, ostream& out) {
      out << "dirfrag " << df << " v " << version << " state " << state << std::endl;
      dentry_vec &dns = get_dentries();
      for (dentry_vec::iterator p = dns.begin(); p != dns.end(); ++p)
        p->print(out);
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(version, bl);
      ::encode(state, bl);
      encode_dentries();
      ::encode(dnbl, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(version, bl);
      ::decode(state, bl);
      ::decode(dnbl, bl);
      dn_decoded = false;      // don't decode dentries unless we need them.
    }

    void apply(MDS *mds, CDirFrag *dir, LogSegment *ls);

    void add_dentry(const string& name, snapid_t first, snapid_t last,
                    version_t version, inodeno_t ino,
                    unsigned char dtype, bool dirty) {
      dentries.push_back(Dentry(name, first, last, version, ino, dtype, dirty));
    }
  };
  WRITE_CLASS_ENCODER(Dir)
  typedef map<frag_t, Dir> dir_map;

  // journal entry for a dir stripe
  class Stripe {
   private:
    dir_map dirs;

    mutable bufferlist dfbl;
    bool df_decoded;

    void encode_dirs() const {
      ::encode(dirs, dfbl);
    }
    void decode_dirs() {
      if (df_decoded) return;
      bufferlist::iterator p = dfbl.begin();
      ::decode(dirs, p);
      df_decoded = true;
    }

   public:
    static const int STATE_OPEN =   (1<<0);
    static const int STATE_DIRTY =  (1<<1);
    static const int STATE_NEW =    (1<<2);
    static const int STATE_UNLINKED = (1<<3);

    pair<int, int> auth;
    fragtree_t dirfragtree;
    fnode_t fnode;
    int state;

    Stripe() : df_decoded(true), state(0) {}

    dir_map& get_dirs()
    {
      decode_dirs();
      return dirs;
    }

    bool is_open() const { return state & STATE_OPEN; }
    void mark_open() { state |= STATE_OPEN; }
    bool is_dirty() const { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }
    bool is_new() const { return state & STATE_NEW; }
    void mark_new() { state |= STATE_NEW; }
    bool is_unlinked() const { return state & STATE_UNLINKED; }
    void mark_unlinked() { state |= STATE_UNLINKED; }

    void encode(bufferlist& bl) const {
      ::encode(auth, bl);
      ::encode(dirfragtree, bl);
      ::encode(fnode, bl);
      ::encode(state, bl);
      encode_dirs();
      ::encode(dfbl, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(auth, bl);
      ::decode(dirfragtree, bl);
      ::decode(fnode, bl);
      ::decode(state, bl);
      ::decode(dfbl, bl);
      df_decoded = false;
    }

    void apply(MDS *mds, CDirStripe *stripe, LogSegment *ls);

    Dir& add_dir(frag_t frag, version_t v, bool dirty,
                 bool complete=false, bool isnew=false) {
      Dir& d = dirs[frag];
      d.version = v;
      if (complete) d.mark_complete();
      if (dirty) d.mark_dirty();
      if (isnew) d.mark_new();
      return d;
    }
  };
  WRITE_CLASS_ENCODER(Stripe)
  typedef map<dirstripe_t, Stripe> stripe_map;

 private:
  inode_map inodes;
  stripe_map stripes;

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
  vector<dirstripe_t> destroyed_stripes;

  // idempotent op(s)
  list<pair<metareqid_t,uint64_t> > client_reqs;

 public:
  void encode(bufferlist& bl) const {
    __u8 struct_v = 4;
    ::encode(struct_v, bl);
    ::encode(inodes, bl);
    ::encode(stripes, bl);
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
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(inodes, bl);
    ::decode(stripes, bl);
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
      ::decode(destroyed_stripes, bl);
    }
  }


  // soft stateadd
  uint64_t last_subtree_map;
  uint64_t my_offset;


  EMetaBlob(MDLog *mdl = 0);  // defined in journal.cc
  ~EMetaBlob() { }

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

  void set_ino_alloc(inodeno_t alloc, inodeno_t used_prealloc,
                     interval_set<inodeno_t>& prealloc,
                     entity_name_t client, version_t sv, version_t iv) {
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
  void add_destroyed_stripe(dirstripe_t ds) {
    destroyed_stripes.push_back(ds);
  }
 
  void add_inode(CInode *in, bool dirty = false,
                 const inoparent_t *added_parent = NULL,
                 const inoparent_t *removed_parent = NULL) {
    // make note of where this inode was last journaled
    in->last_journaled = my_offset;

    default_file_layout *default_layout = NULL;
    if (in->is_dir())
      default_layout = (in->get_projected_node() ?
                        in->get_projected_node()->dir_layout :
                        in->default_layout);

    static const vector<int> empty_stripe_auth;
    const vector<int> &stripe_auth = in->is_dir() ?
        in->get_placement()->get_stripe_auth() : empty_stripe_auth;

    Inode &inode = inodes[in->ino()];
    inode.encode(*in->get_projected_inode(), in->inode_auth, stripe_auth,
                 *in->get_projected_xattrs(), in->symlink,
                 dirty, added_parent, removed_parent,
                 default_layout, &in->old_inodes);
  }

  void add_inode(CInode *in, CDentry *added_parent,
                 CDentry *removed_parent = NULL)
  {
    inoparent_t added, removed;
    if (added_parent)
      added = added_parent->inoparent();
    if (removed_parent)
      removed = removed_parent->inoparent();
    add_inode(in, true,
              added_parent ? &added : NULL,
              removed_parent ? &removed : NULL);
  }

  void add_dentry(CDentry *dn, bool dirty) {
    CDirFrag *dir = dn->get_dir();

    inodeno_t ino = 0;
    unsigned char d_type = 0;

    CDentry::linkage_t *dnl = dn->get_projected_linkage();
    if (dnl->is_primary()) {
      assert(dir->ino() == MDS_INO_CONTAINER);
      ino = dnl->get_inode()->ino();
      d_type = dnl->get_inode()->d_type();
    } else if (dnl->is_remote()) {
      ino = dnl->get_remote_ino();
      d_type = dnl->get_remote_d_type();
    }

    Dir& df = add_dir(dir, false);
    df.add_dentry(dn->get_name(), dn->first, dn->last,
                  dn->get_projected_version(), ino, d_type, dirty);
  }

  Dir& add_dir(CDirFrag *dir, bool dirty, bool complete=false) {
    Stripe &s = add_stripe(dir->get_stripe(), false);
    return s.add_dir(dir->get_frag(), dir->get_version(), dirty, complete);
  }
  Dir& add_new_dir(CDirFrag *dir) {
    Stripe &s = add_stripe(dir->get_stripe(), true);
    // dirty AND complete AND new
    return s.add_dir(dir->get_frag(), dir->get_version(), true, true, true);
  }

  Stripe& add_stripe(CDirStripe *stripe, bool dirty, bool isnew=false,
                     bool unlinked=false) {
    return add_stripe(stripe->dirstripe(),
                      stripe->get_stripe_auth(),
                      stripe->get_fragtree(),
                      stripe->get_projected_fnode(),
                      stripe->get_projected_version(),
                      stripe->is_open(), dirty, isnew, unlinked);
  }
  Stripe& add_stripe(dirstripe_t ds, const pair<int, int> &auth,
                     const fragtree_t &dft,
                     const fnode_t *pf, version_t pv,
                     bool open, bool dirty, bool isnew=false,
                     bool unlinked=false) {
    Stripe& s = stripes[ds];
    s.auth = auth;
    s.dirfragtree = dft;
    s.fnode = *pf;
    s.fnode.version = pv;
    if (open) s.mark_open();
    if (dirty) s.mark_dirty();
    if (isnew) s.mark_new();
    if (unlinked) s.mark_unlinked();
    return s;
  }


  static const int TO_AUTH_SUBTREE_ROOT = 0;  // default.
  static const int TO_ROOT = 1;
  
  void print(ostream& out) const {
    out << "[metablob";
    if (!inodes.empty()) 
      out << " " << inodes.size() << " inodes";
    if (!stripes.empty()) 
      out << " " << stripes.size() << " stripes";
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
                     CDirStripe *stripe, Stripe &lump);
  void replay(MDS *mds, LogSegment *ls, MDSlaveUpdate *su=NULL);
};
WRITE_CLASS_ENCODER(EMetaBlob)
WRITE_CLASS_ENCODER(EMetaBlob::Inode)
WRITE_CLASS_ENCODER(EMetaBlob::Dentry)
WRITE_CLASS_ENCODER(EMetaBlob::Dir)
WRITE_CLASS_ENCODER(EMetaBlob::Stripe)

inline ostream& operator<<(ostream& out, const EMetaBlob& t) {
  t.print(out);
  return out;
}

#endif
