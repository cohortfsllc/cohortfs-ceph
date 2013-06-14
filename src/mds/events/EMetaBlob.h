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
struct MDSlaveUpdate;

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
  // journal entry for an inode
  class Inode {
   private:
    static const int STATE_DIRTY =	 (1<<0);

    mutable bufferlist _enc;

   public:
    inode_t inode;
    pair<int, int> inode_auth;
    vector<int> stripe_auth;
    map<string,bufferptr> xattrs;
    string symlink;
    __u8 state;
    typedef map<snapid_t, old_inode_t> old_inodes_t;
    old_inodes_t old_inodes;

    Inode() : state(0) {}
    Inode(bufferlist::iterator &p) {
      decode(p);
    }

    // initialize/overwrite the encoded contents
    void encode(const inode_t &i, const pair<int, int> &iauth,
                const vector<int> &sauth, const map<string,bufferptr> &xa,
                const string &sym, __u8 st,
                const old_inodes_t *oi = NULL) const;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<EMetaBlob::Inode*>& ls);

    void apply(MDS *mds, CInode *in);
    bool is_dirty() const { return state & STATE_DIRTY; }

    void print(ostream& out) const {
      out << " inode " << inode.ino
          << " state=" << state_string() << std::endl;
    }
    string state_string() const {
      string state_string;
      if (is_dirty())
	state_string.append("dirty");
      return state_string;
    }
    static __u8 make_state(bool dirty) {
      __u8 st = 0;
      if (dirty) st |= STATE_DIRTY;
      return st;
    }
  };
  WRITE_CLASS_ENCODER(Inode)
  typedef map<inodeno_t, Inode> inode_map;

  // journal entry for a dentry
  class Dentry {
   private:
    mutable bufferlist _enc;

   public:
    string name;
    snapid_t first, last;
    version_t version;
    inodeno_t ino;
    unsigned char d_type;
    bool dirty;

    Dentry() : first(0), last(0), version(0), d_type('\0'), dirty(false) {}
    Dentry(bufferlist::iterator &p) { decode(p); }

    // initialize/overwrite the encoded contents
    void encode(const string& d, snapid_t df, snapid_t dl,
                version_t v, inodeno_t i, unsigned char dt, bool dr) const;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void print(ostream& out) {
      out << " dn " << name << " [" << first << "," << last << "] v " << version
	  << " ino " << ino << " dirty=" << dirty << std::endl;
    }
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Dentry*>& ls);
  };
  WRITE_CLASS_ENCODER(Dentry)
  typedef vector<Dentry> dentry_vec;

  // journal entry for a dir fragment
  class Dir {
   private:
    static const int STATE_COMPLETE =    (1<<1);
    static const int STATE_DIRTY =       (1<<2);  // dirty due to THIS journal item, that is!
    static const int STATE_NEW =         (1<<3);  // new directory

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
    version_t version;
    int state;

    Dir() : dn_decoded(true), version(0), state(0) {}

    dentry_vec& get_dentries()
    {
      decode_dentries();
      return dentries;
    }

    bool is_complete() const { return state & STATE_COMPLETE; }
    void mark_complete() { state |= STATE_COMPLETE; }
    bool is_dirty() const { return state & STATE_DIRTY; }
    void mark_dirty() { state |= STATE_DIRTY; }
    bool is_new() const { return state & STATE_NEW; }
    void mark_new() { state |= STATE_NEW; }

    string state_string() const {
      string state_string;
      bool marked_already = false;
      if (is_complete()) {
	state_string.append("complete");
	marked_already = true;
      }
      if (is_dirty()) {
	state_string.append(marked_already ? "+dirty" : "dirty");
	marked_already = true;
      }
      if (is_new()) {
	state_string.append(marked_already ? "+new" : "new");
      }
      return state_string;
    }

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void print(dirfrag_t df, ostream& out) {
      out << "dirfrag " << df << " v " << version
          << " state " << state_string() << std::endl;
      dentry_vec &dns = get_dentries();
      for (dentry_vec::iterator p = dns.begin(); p != dns.end(); ++p)
        p->print(out);
    }
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Dir*>& ls);

    void apply(MDS *mds, CDir *dir, LogSegment *ls) const;

    void add_dentry(const string& name, snapid_t first, snapid_t last,
                    version_t version, inodeno_t ino,
                    unsigned char dtype, bool dirty) {
      dentries.push_back(Dentry());
      dentries.back().encode(name, first, last, version, ino, dtype, dirty);
    }
  };
  WRITE_CLASS_ENCODER(Dir)
  typedef map<frag_t, Dir> dir_map;

  // journal entry for a dir stripe
  class Stripe {
   private:
    static const int STATE_OPEN =   (1<<0);
    static const int STATE_DIRTY =  (1<<1);
    static const int STATE_NEW =    (1<<2);

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

    string state_string() const {
      string state_string;
      bool marked_already = false;
      if (is_open()) {
	state_string.append("open");
	marked_already = true;
      }
      if (is_dirty()) {
	state_string.append(marked_already ? "+dirty" : "dirty");
	marked_already = true;
      }
      if (is_new())
	state_string.append(marked_already ? "+new" : "new");
      return state_string;
    }

    void apply(MDS *mds, CStripe *stripe, LogSegment *ls) const;

    Dir& add_dir(frag_t frag, version_t v, bool dirty,
                 bool complete=false, bool isnew=false) {
      Dir& d = dirs[frag];
      d.version = v;
      if (complete) d.mark_complete();
      if (dirty) d.mark_dirty();
      if (isnew) d.mark_new();
      return d;
    }

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator &bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Stripe*>& ls);
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

  // idempotent op(s)
  list<pair<metareqid_t,uint64_t> > client_reqs;

 public:
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<EMetaBlob*>& ls);


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
 
  void add_inode(CInode *in, bool dirty = false) {
    // make note of where this inode was last journaled
    in->last_journaled = my_offset;

    __u8 state = Inode::make_state(dirty);

    Inode &inode = inodes[in->ino()];
    inode.encode(*in->get_projected_inode(),
                 in->inode_auth, in->get_stripe_auth(),
                 *in->get_projected_xattrs(), in->symlink,
                 state, &in->old_inodes);
  }

  void add_dentry(CDentry *dn, bool dirty) {
    CDir *dir = dn->get_dir();

    inodeno_t ino = 0;
    unsigned char d_type = 0;

    CDentry::linkage_t *dnl = dn->get_projected_linkage();
    if (dnl->is_primary()) {
      assert(dir->dirfrag().stripe.ino == MDS_INO_CONTAINER);
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

  Dir& add_dir(CDir *dir, bool dirty, bool complete=false) {
    Stripe &s = add_stripe(dir->get_stripe(), false);
    return s.add_dir(dir->get_frag(), dir->get_version(), dirty, complete);
  }
  Dir& add_new_dir(CDir *dir) {
    Stripe &s = add_stripe(dir->get_stripe(), true);
    // dirty AND complete AND new
    return s.add_dir(dir->get_frag(), dir->get_version(), true, true, true);
  }

  Stripe& add_stripe(CStripe *stripe, bool dirty, bool isnew=false) {
    return add_stripe(stripe->dirstripe(),
                      stripe->get_stripe_auth(),
                      stripe->get_fragtree(),
                      stripe->get_projected_fnode(),
                      stripe->get_projected_version(),
                      stripe->is_open(), dirty, isnew);
  }
  Stripe& add_stripe(dirstripe_t ds, const pair<int, int> &auth,
                     const fragtree_t &dft,
                     const fnode_t *pf, version_t pv,
                     bool open, bool dirty, bool isnew=false) {
    Stripe& s = stripes[ds];
    s.auth = auth;
    s.dirfragtree = dft;
    s.fnode = *pf;
    s.fnode.version = pv;
    if (open) s.mark_open();
    if (dirty) s.mark_dirty();
    if (isnew) s.mark_new();
    return s;
  }


  static const int TO_AUTH_SUBTREE_ROOT = 0;  // default.
  static const int TO_ROOT = 1;
  
  void add_stripe_context(CStripe *stripe, int mode = TO_AUTH_SUBTREE_ROOT);

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
