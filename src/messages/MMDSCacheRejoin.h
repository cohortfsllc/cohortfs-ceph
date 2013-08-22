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

#ifndef CEPH_MMDSCACHEREJOIN_H
#define CEPH_MMDSCACHEREJOIN_H

#include "msg/Message.h"

#include "include/types.h"

#include "mds/CInode.h"

// sent from replica to auth

class MMDSCacheRejoin : public Message {
 public:
  static const int OP_WEAK    = 1;  // replica -> auth, i exist, + maybe open files.
  static const int OP_STRONG  = 2;  // replica -> auth, i exist, + open files and lock state.
  static const int OP_ACK     = 3;  // auth -> replica, here is your lock state.
  static const int OP_MISSING = 5;  // auth -> replica, i am missing these items
  static const int OP_FULL    = 6;  // replica -> auth, here is the full object.
  static const char *get_opname(int op) {
    switch (op) {
    case OP_WEAK: return "weak";
    case OP_STRONG: return "strong";
    case OP_ACK: return "ack";
    case OP_MISSING: return "missing";
    case OP_FULL: return "full";
    default: assert(0); return 0;
    }
  }

  // -- types --
  struct inode_strong { 
    int32_t caps_wanted;
    int32_t filelock, nestlock;
    inode_strong() {}
    inode_strong(int cw, int dl, int nl)
        : caps_wanted(cw), filelock(dl), nestlock(nl) {}
    void encode(bufferlist &bl) const {
      ::encode(caps_wanted, bl);
      ::encode(filelock, bl);
      ::encode(nestlock, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(caps_wanted, bl);
      ::decode(filelock, bl);
      ::decode(nestlock, bl);
    }
  };
  WRITE_CLASS_ENCODER(inode_strong)

  struct dirfrag_strong {
    int32_t nonce;
    dirfrag_strong() {}
    dirfrag_strong(int n) : nonce(n) {}
    void encode(bufferlist &bl) const {
      ::encode(nonce, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(nonce, bl);
    }
  };
  WRITE_CLASS_ENCODER(dirfrag_strong)

  struct dn_strong {
    snapid_t first;
    inodeno_t ino;
    inodeno_t remote_ino;
    unsigned char remote_d_type;
    int32_t nonce;
    int32_t lock;
    dn_strong() : 
      ino(0), remote_ino(0), remote_d_type(0), nonce(0), lock(0) {}
    dn_strong(snapid_t f, inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int l) : 
      first(f), ino(pi), remote_ino(ri), remote_d_type(rdt), nonce(n), lock(l) {}
    bool is_primary() { return ino > 0; }
    bool is_remote() { return remote_ino > 0; }
    bool is_null() { return ino == 0 && remote_ino == 0; }
    void encode(bufferlist &bl) const {
      ::encode(first, bl);
      ::encode(ino, bl);
      ::encode(remote_ino, bl);
      ::encode(remote_d_type, bl);
      ::encode(nonce, bl);
      ::encode(lock, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(first, bl);
      ::decode(ino, bl);
      ::decode(remote_ino, bl);
      ::decode(remote_d_type, bl);
      ::decode(nonce, bl);
      ::decode(lock, bl);
    }
  };
  WRITE_CLASS_ENCODER(dn_strong)

  struct dn_weak {
    snapid_t first;
    inodeno_t ino;
    dn_weak() : ino(0) {}
    dn_weak(snapid_t f, inodeno_t pi) : first(f), ino(pi) {}
    void encode(bufferlist &bl) const {
      ::encode(first, bl);
      ::encode(ino, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(first, bl);
      ::decode(ino, bl);
    }
  };
  WRITE_CLASS_ENCODER(dn_weak)

  // -- data --
  int32_t op;

  struct lock_bls {
    bufferlist file, nest;
    void encode(bufferlist& bl) const {
      ::encode(file, bl);
      ::encode(nest, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(file, bl);
      ::decode(nest, bl);
    }
  };
  WRITE_CLASS_ENCODER(lock_bls)

  // weak
  map<dirfrag_t, map<string_snap_t, dn_weak> > weak;
  set<inodeno_t> weak_placements;
  set<dirstripe_t> weak_stripes;
  set<dirfrag_t> weak_dirfrags;
  set<vinodeno_t> weak_inodes;
  map<inodeno_t, lock_bls> inode_scatterlocks;

  // strong
  map<inodeno_t, int> strong_placements;
  map<dirstripe_t, int> strong_stripes;
  map<dirfrag_t, dirfrag_strong> strong_dirfrags;
  map<dirfrag_t, map<string_snap_t, dn_strong> > strong_dentries;
  map<vinodeno_t, inode_strong> strong_inodes;

  // open
  bufferlist cap_export_bl;
  map<inodeno_t,map<client_t, ceph_mds_cap_reconnect> > cap_exports;
  map<inodeno_t,filepath> cap_export_paths;

  // full
  bufferlist inode_base;
  bufferlist inode_locks;

  // authpins, xlocks
  struct slave_reqid {
    metareqid_t reqid;
    __u32 attempt;
    slave_reqid() : attempt(0) {}
    slave_reqid(const metareqid_t& r, __u32 a)
      : reqid(r), attempt(a) {}
    void encode(bufferlist& bl) const {
      ::encode(reqid, bl);
      ::encode(attempt, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(reqid, bl);
      ::decode(attempt, bl);
    }
  };
  map<vinodeno_t, slave_reqid> authpinned_inodes;
  map<vinodeno_t, slave_reqid> frozen_authpin_inodes;
  map<vinodeno_t, map<__s32, slave_reqid> > xlocked_inodes;
  map<vinodeno_t, map<__s32, slave_reqid> > wrlocked_inodes;
  map<dirfrag_t, map<string_snap_t, slave_reqid> > authpinned_dentries;
  map<dirfrag_t, map<string_snap_t, slave_reqid> > xlocked_dentries;
  
  MMDSCacheRejoin() : Message(MSG_MDS_CACHEREJOIN) {}
  MMDSCacheRejoin(int o) : 
    Message(MSG_MDS_CACHEREJOIN),
    op(o) {}
private:
  ~MMDSCacheRejoin() {}

public:
  const char *get_type_name() const { return "cache_rejoin"; }
  void print(ostream& out) const {
    out << "cache_rejoin " << get_opname(op);
  }

  // -- builders --
  // inodes
  void add_weak_inode(vinodeno_t i) {
    weak_inodes.insert(i);
  }
  void add_strong_inode(vinodeno_t i, int cw, int dl, int nl) {
    strong_inodes[i] = inode_strong(cw, dl, nl);
  }
  void add_inode_locks(CInode *in, __u32 nonce) {
    ::encode(in->inode.ino, inode_locks);
    ::encode(in->last, inode_locks);
    ::encode(nonce, inode_locks);
    bufferlist bl;
    in->_encode_locks_state_for_replica(bl);
    ::encode(bl, inode_locks);
  }
  void add_inode_base(CInode *in) {
    ::encode(in->inode.ino, inode_base);
    ::encode(in->last, inode_base);
    bufferlist bl;
    in->_encode_base(bl);
    ::encode(bl, inode_base);
  }
  void add_inode_authpin(vinodeno_t ino, const metareqid_t& ri, __u32 attempt) {
    authpinned_inodes[ino] = slave_reqid(ri, attempt);
  }
  void add_inode_frozen_authpin(vinodeno_t ino, const metareqid_t& ri, __u32 attempt) {
    frozen_authpin_inodes[ino] = slave_reqid(ri, attempt);
  }
  void add_inode_xlock(vinodeno_t ino, int lt, const metareqid_t& ri, __u32 attempt) {
    xlocked_inodes[ino][lt] = slave_reqid(ri, attempt);
  }
  void add_inode_wrlock(vinodeno_t ino, int lt, const metareqid_t& ri, __u32 attempt) {
    wrlocked_inodes[ino][lt] = slave_reqid(ri, attempt);
  }

  void add_scatterlock_state(CInode *in) {
    if (inode_scatterlocks.count(in->ino()))
      return;  // already added this one
    in->encode_lock_state(CEPH_LOCK_IFILE, inode_scatterlocks[in->ino()].file);
    in->encode_lock_state(CEPH_LOCK_INEST, inode_scatterlocks[in->ino()].nest);
  }

  void copy_cap_exports(bufferlist &bl) {
    cap_export_bl = bl;
  }

  // placement
  void add_weak_placement(inodeno_t ino) {
    weak_placements.insert(ino);
  }
  void add_strong_placement(inodeno_t ino, int n) {
    strong_placements[ino] = n;
  }

  // stripes
  void add_weak_stripe(dirstripe_t ds) {
    weak_stripes.insert(ds);
  }
  void add_strong_stripe(dirstripe_t ds, int n) {
    strong_stripes[ds] = n;
  }

  // dirfrags
  void add_strong_dirfrag(dirfrag_t df, int n) {
    strong_dirfrags[df] = dirfrag_strong(n);
  }
  void add_weak_dirfrag(dirfrag_t df) {
    weak_dirfrags.insert(df);
  }

  // dentries
  void add_weak_dentry(dirfrag_t df, const string& dname, snapid_t last, dn_weak& dnw) {
    weak[df][string_snap_t(dname, last)] = dnw;
  }
  void add_weak_primary_dentry(dirfrag_t df, const string& dname, snapid_t first, snapid_t last, inodeno_t ino) {
    weak[df][string_snap_t(dname, last)] = dn_weak(first, ino);
  }
  void add_strong_dentry(dirfrag_t df, const string& dname, snapid_t first, snapid_t last, inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int ls) {
    strong_dentries[df][string_snap_t(dname, last)] = dn_strong(first, pi, ri, rdt, n, ls);
  }
  void add_dentry_authpin(dirfrag_t df, const string& dname, snapid_t last,
			  const metareqid_t& ri, __u32 attempt) {
    authpinned_dentries[df][string_snap_t(dname, last)] = slave_reqid(ri, attempt);
  }
  void add_dentry_xlock(dirfrag_t df, const string& dname, snapid_t last,
			const metareqid_t& ri, __u32 attempt) {
    xlocked_dentries[df][string_snap_t(dname, last)] = slave_reqid(ri, attempt);
  }

  // -- encoding --
  void encode_payload(uint64_t features) {
    ::encode(op, payload);
    ::encode(strong_inodes, payload);
    ::encode(inode_base, payload);
    ::encode(inode_locks, payload);
    ::encode(inode_scatterlocks, payload);
    ::encode(authpinned_inodes, payload);
    ::encode(frozen_authpin_inodes, payload);
    ::encode(xlocked_inodes, payload);
    ::encode(wrlocked_inodes, payload);
    ::encode(cap_export_bl, payload);
    ::encode(strong_placements, payload);
    ::encode(strong_stripes, payload);
    ::encode(strong_dirfrags, payload);
    ::encode(weak, payload);
    ::encode(weak_placements, payload);
    ::encode(weak_stripes, payload);
    ::encode(weak_dirfrags, payload);
    ::encode(weak_inodes, payload);
    ::encode(strong_dentries, payload);
    ::encode(authpinned_dentries, payload);
    ::encode(xlocked_dentries, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(strong_inodes, p);
    ::decode(inode_base, p);
    ::decode(inode_locks, p);
    ::decode(inode_scatterlocks, p);
    ::decode(authpinned_inodes, p);
    ::decode(frozen_authpin_inodes, p);
    ::decode(xlocked_inodes, p);
    ::decode(wrlocked_inodes, p);
    ::decode(cap_export_bl, p);
    if (cap_export_bl.length()) {
      bufferlist::iterator q = cap_export_bl.begin();
      ::decode(cap_exports, q);
      ::decode(cap_export_paths, q);
    }
    ::decode(strong_placements, p);
    ::decode(strong_stripes, p);
    ::decode(strong_dirfrags, p);
    ::decode(weak, p);
    ::decode(weak_placements, p);
    ::decode(weak_stripes, p);
    ::decode(weak_dirfrags, p);
    ::decode(weak_inodes, p);
    ::decode(strong_dentries, p);
    ::decode(authpinned_dentries, p);
    ::decode(xlocked_dentries, p);
  }

};

WRITE_CLASS_ENCODER(MMDSCacheRejoin::inode_strong)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::dirfrag_strong)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::dn_strong)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::dn_weak)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::lock_bls)
WRITE_CLASS_ENCODER(MMDSCacheRejoin::slave_reqid)

inline ostream& operator<<(ostream& out, const MMDSCacheRejoin::slave_reqid& r) {
  return out << r.reqid << '.' << r.attempt;
}

#endif
