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

#ifndef CEPH_MMDSCACHEREJOIN_H
#define CEPH_MMDSCACHEREJOIN_H

#include "msg/Message.h"

#include "include/types.h"

#include "mds/CInode.h"
#include "mds/CDir.h"

// sent from replica to auth

class MMDSCacheRejoin : public Message {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

 public:
  static const int OP_WEAK = 1;	 // replica -> auth, + maybe open files.
  static const int OP_STRONG = 2; // replica -> auth, open files/lock state.
  static const int OP_ACK = 3; // auth -> replica, here is your lock state.
  static const int OP_MISSING = 5; // auth -> replica, i am missing these items
  static const int OP_FULL = 6; // replica -> auth, here is the full object.
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
    uint32_t nonce;
    int32_t caps_wanted;
    int32_t filelock, nestlock, dftlock;
    inode_strong() {}
    inode_strong(int n, int cw, int dl, int nl, int dftl) :
      nonce(n), caps_wanted(cw),
      filelock(dl), nestlock(nl), dftlock(dftl) { }
    void encode(bufferlist &bl) const {
      ::encode(nonce, bl);
      ::encode(caps_wanted, bl);
      ::encode(filelock, bl);
      ::encode(nestlock, bl);
      ::encode(dftlock, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(nonce, bl);
      ::decode(caps_wanted, bl);
      ::decode(filelock, bl);
      ::decode(nestlock, bl);
      ::decode(dftlock, bl);
    }
  };
  WRITE_CLASS_ENCODER(inode_strong)

  struct dirfrag_strong {
    uint32_t nonce;
    int8_t  dir_rep;
    dirfrag_strong() {}
    dirfrag_strong(int n, int dr) : nonce(n), dir_rep(dr) {}
    void encode(bufferlist &bl) const {
      ::encode(nonce, bl);
      ::encode(dir_rep, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(nonce, bl);
      ::decode(dir_rep, bl);
    }
  };
  WRITE_CLASS_ENCODER(dirfrag_strong)

  struct dn_strong {
    inodeno_t ino;
    inodeno_t remote_ino;
    unsigned char remote_d_type;
    uint32_t nonce;
    int32_t lock;
    dn_strong() :
      ino(0), remote_ino(0), remote_d_type(0), nonce(0), lock(0) {}
    dn_strong(inodeno_t pi, inodeno_t ri, unsigned char rdt, int n, int l) :
      ino(pi), remote_ino(ri), remote_d_type(rdt), nonce(n), lock(l) {}
    bool is_primary() { return ino > 0; }
    bool is_remote() { return remote_ino > 0; }
    bool is_null() { return ino == 0 && remote_ino == 0; }
    void encode(bufferlist &bl) const {
      ::encode(ino, bl);
      ::encode(remote_ino, bl);
      ::encode(remote_d_type, bl);
      ::encode(nonce, bl);
      ::encode(lock, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(ino, bl);
      ::decode(remote_ino, bl);
      ::decode(remote_d_type, bl);
      ::decode(nonce, bl);
      ::decode(lock, bl);
    }
  };
  WRITE_CLASS_ENCODER(dn_strong)

  struct dn_weak {
    inodeno_t ino;
    dn_weak() : ino(0) {}
    dn_weak(inodeno_t pi) : ino(pi) {}
    void encode(bufferlist &bl) const {
      ::encode(ino, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(ino, bl);
    }
  };
  WRITE_CLASS_ENCODER(dn_weak)

  // -- data --
  int32_t op;

  struct lock_bls {
    bufferlist file, nest, dft;
    void encode(bufferlist& bl) const {
      ::encode(file, bl);
      ::encode(nest, bl);
      ::encode(dft, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(file, bl);
      ::decode(nest, bl);
      ::decode(dft, bl);
    }
  };
  WRITE_CLASS_ENCODER(lock_bls)

  // weak
  map<inodeno_t, map<string, dn_weak> > weak;
  set<dirfrag_t> weak_dirfrags;
  set<vinodeno_t> weak_inodes;
  map<inodeno_t, lock_bls> inode_scatterlocks;

  // strong
  map<dirfrag_t, dirfrag_strong> strong_dirfrags;
  map<dirfrag_t, map<string, dn_strong> > strong_dentries;
  map<vinodeno_t, inode_strong> strong_inodes;

  // open
  map<inodeno_t,map<client_t, ceph_mds_cap_reconnect> > cap_exports;
  map<client_t, entity_inst_t> client_map;
  bufferlist imported_caps;

  // full
  bufferlist inode_base;
  bufferlist inode_locks;
  map<dirfrag_t, bufferlist> dirfrag_bases;

  // authpins, xlocks
  struct slave_reqid {
    metareqid_t reqid;
    uint32_t attempt;
    slave_reqid() : attempt(0) {}
    slave_reqid(const metareqid_t& r, uint32_t a)
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
  map<vinodeno_t, list<slave_reqid> > authpinned_inodes;
  map<vinodeno_t, slave_reqid> frozen_authpin_inodes;
  map<vinodeno_t, map<int32_t, slave_reqid> > xlocked_inodes;
  map<vinodeno_t, map<int32_t, list<slave_reqid> > > wrlocked_inodes;
  map<dirfrag_t, map<string, list<slave_reqid> > > authpinned_dentries;
  map<dirfrag_t, map<string, slave_reqid> > xlocked_dentries;

  MMDSCacheRejoin() :
    Message(MSG_MDS_CACHEREJOIN, HEAD_VERSION, COMPAT_VERSION)
  {}
  MMDSCacheRejoin(int o) :
    Message(MSG_MDS_CACHEREJOIN, HEAD_VERSION, COMPAT_VERSION),
    op(o) {}
private:
  ~MMDSCacheRejoin() {}
  template <typename T>
  void _print(T& out) const {
    out << "cache_rejoin " << get_opname(op);
  }
public:
  const char *get_type_name() const { return "cache_rejoin"; }

  void print(ostream& out) const { _print(out); }
  void print(lttng_stream& out) const { _print(out); }  
  // -- builders --
  // inodes
  void add_weak_inode(vinodeno_t i) {
    weak_inodes.insert(i);
  }
  void add_strong_inode(vinodeno_t i, int n, int cw, int dl, int nl, int dftl) {
    strong_inodes[i] = inode_strong(n, cw, dl, nl, dftl);
  }
  void add_inode_locks(CInode *in, uint32_t nonce, bufferlist& bl) {
    ::encode(in->inode.ino, inode_locks);
    ::encode(nonce, inode_locks);
    ::encode(bl, inode_locks);
  }
  void add_inode_base(CInode *in) {
    ::encode(in->inode.ino, inode_base);
    bufferlist bl;
    in->_encode_base(bl);
    ::encode(bl, inode_base);
  }
  void add_inode_authpin(vinodeno_t ino, const metareqid_t& ri, uint32_t attempt) {
    authpinned_inodes[ino].push_back(slave_reqid(ri, attempt));
  }
  void add_inode_frozen_authpin(vinodeno_t ino, const metareqid_t& ri, uint32_t attempt) {
    frozen_authpin_inodes[ino] = slave_reqid(ri, attempt);
  }
  void add_inode_xlock(vinodeno_t ino, int lt, const metareqid_t& ri, uint32_t attempt) {
    xlocked_inodes[ino][lt] = slave_reqid(ri, attempt);
  }
  void add_inode_wrlock(vinodeno_t ino, int lt, const metareqid_t& ri, uint32_t attempt) {
    wrlocked_inodes[ino][lt].push_back(slave_reqid(ri, attempt));
  }

  void add_scatterlock_state(CInode *in) {
    if (inode_scatterlocks.count(in->ino()))
      return;  // already added this one
    in->encode_lock_state(CEPH_LOCK_IFILE, inode_scatterlocks[in->ino()].file);
    in->encode_lock_state(CEPH_LOCK_INEST, inode_scatterlocks[in->ino()].nest);
    in->encode_lock_state(CEPH_LOCK_IDFT, inode_scatterlocks[in->ino()].dft);
  }

  // dirfrags
  void add_strong_dirfrag(dirfrag_t df, int n, int dr) {
    strong_dirfrags[df] = dirfrag_strong(n, dr);
  }
  void add_dirfrag_base(CDir *dir) {
    bufferlist& bl = dirfrag_bases[dir->dirfrag()];
    dir->_encode_base(bl);
  }

  // dentries
  void add_weak_dirfrag(dirfrag_t df) {
    weak_dirfrags.insert(df);
  }
  void add_weak_dentry(inodeno_t dirino, const string& dname, dn_weak& dnw) {
    weak[dirino][dname] = dnw;
  }
  void add_weak_primary_dentry(inodeno_t dirino, const string& dname,
			       inodeno_t ino) {
    weak[dirino][dname] = dn_weak(ino);
  }
  void add_strong_dentry(dirfrag_t df, const string& dname, inodeno_t pi,
			 inodeno_t ri, unsigned char rdt, int n, int ls) {
    strong_dentries[df][dname] = dn_strong(pi, ri, rdt, n, ls);
  }
  void add_dentry_authpin(dirfrag_t df, const string& dname,
			  const metareqid_t& ri, uint32_t attempt) {
    authpinned_dentries[df][dname].push_back(slave_reqid(ri, attempt));
  }
  void add_dentry_xlock(dirfrag_t df, const string& dname,
			const metareqid_t& ri, uint32_t attempt) {
    xlocked_dentries[df][dname] = slave_reqid(ri, attempt);
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
    ::encode(cap_exports, payload);
    ::encode(client_map, payload);
    ::encode(imported_caps, payload);
    ::encode(strong_dirfrags, payload);
    ::encode(dirfrag_bases, payload);
    ::encode(weak, payload);
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
    ::decode(cap_exports, p);
    ::decode(client_map, p);
    ::decode(imported_caps, p);
    ::decode(strong_dirfrags, p);
    ::decode(dirfrag_bases, p);
    ::decode(weak, p);
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

template <typename T>
inline typename StrmRet<T>::type& operator<<(T& out, const MMDSCacheRejoin::slave_reqid& r) {
  return out << r.reqid << '.' << r.attempt;
}

#endif
