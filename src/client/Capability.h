// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_CAPABILITY_H
#define CEPH_CLIENT_CAPABILITY_H

#include "include/types.h"
#include "include/xlist.h"

#include "mds/mdstypes.h"

class Cond;
class MClientCaps;
class MetaSession;
class Messenger;
class SnapRealm;

class CapObject;

class Cap {
 public:
  MetaSession *session;
  CapObject *parent;
  xlist<Cap*>::item cap_item;

  uint64_t cap_id;
  unsigned issued;
  unsigned implemented;
  unsigned wanted;   // as known to mds.
  uint64_t seq, issue_seq;
  __u32 mseq;  // migration seq
  __u32 gen;

  Cap(MetaSession *session, CapObject *parent)
      : session(session), parent(parent),
        cap_item(this), cap_id(0), issued(0), implemented(0),
        wanted(0), seq(0), issue_seq(0), mseq(0), gen(0) {}

  void dump(Formatter *f) const;
};

typedef map<int, Cap*> cap_map; // mds -> Cap

class CapObject {
 public:
  CephContext *cct;

  inodeno_t ino;

  SnapRealm *snaprealm;
  snapid_t snapid;
  xlist<CapObject*>::item snaprealm_item;

  cap_map caps;
  Cap *auth_cap;
  unsigned dirty_caps, flushing_caps;
  tid_t flushing_cap_seq;
  __u16 flushing_cap_tid[CEPH_CAP_BITS];
  int shared_gen;
  int snap_caps, snap_cap_refs;
  unsigned exporting_issued;
  int exporting_mds;
  ceph_seq_t exporting_mseq;
  utime_t hold_caps_until;
  xlist<CapObject*>::item cap_item, flushing_cap_item;
  tid_t last_flush_tid;

  list<Cond*> waitfor_caps;

  typedef map<int,int> ref_map;
  ref_map cap_refs;

  CapObject(CephContext *cct, vinodeno_t vino);
  virtual ~CapObject() {}

  bool is_inode() const { return true; }

  bool is_any_caps() const;
  bool cap_is_valid(Cap *cap);

  int caps_issued(int *implemented = 0);
  bool caps_issued_mask(unsigned mask);

  int caps_used() const;
  int caps_dirty() const;

  void touch_cap(Cap *cap);
  void try_touch_cap(int mds);

  void get_cap_ref(int cap);
  bool put_cap_ref(int cap);


  // virtual interface
  virtual int caps_wanted() const;

  // return true if the cap needs to be sent
  virtual bool check_cap(const Cap *cap, int retain, bool unmounting) const;

  // initialize the fields of MClientCaps associated with this object
  virtual void fill_caps(const Cap *cap, MClientCaps *m, int mask) = 0;

  virtual void print(ostream &out);


  void dump(Formatter *f) const;
};

ostream& operator<<(ostream &out, CapObject &c);

#endif
