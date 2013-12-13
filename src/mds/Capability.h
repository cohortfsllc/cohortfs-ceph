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


#ifndef CEPH_CAPABILITY_H
#define CEPH_CAPABILITY_H

#include "include/buffer.h"
#include "include/xlist.h"

#include "common/config.h"

#include "mdstypes.h"

/*

  Capability protocol notes.

- two types of cap events from mds -> client:
  - cap "issue" in a MClientReply, or an MClientCaps IMPORT op.
  - cap "update" (revocation or grant) .. an MClientCaps message.
- if client has cap, the mds should have it too.

- if client has no dirty data, it can release it without waiting for an mds ack.
  - client may thus get a cap _update_ and not have the cap.  ignore it.

- mds should track seq of last issue.  any release
  attempt will only succeed if the client has seen the latest.

- a UPDATE updates the clients issued caps, wanted, etc.  it may also flush dirty metadata.
  - 'caps' are which caps the client retains.
    - if 0, client wishes to release the cap
  - 'wanted' is which caps the client wants.
  - 'dirty' is which metadata is to be written.
    - client gets a FLUSH_ACK with matching dirty flags indicating which caps were written.

- a FLUSH_ACK acks a FLUSH.
  - 'dirty' is the _original_ FLUSH's dirty (i.e., which metadata was written back)
  - 'seq' is the _original_ FLUSH's seq.
  - 'caps' is the _original_ FLUSH's caps (not actually important)
  - client can conclude that (dirty & ~caps) bits were successfully cleaned.

- a FLUSHSNAP flushes snapshot metadata.
  - 'dirty' indicates which caps, were dirty, if any.
  - mds writes metadata.  if dirty!=0, replies with FLUSHSNAP_ACK.

 */

class CapObject;

struct CapExport {
  int32_t wanted;
  int32_t issued;
  int32_t pending;
  snapid_t client_follows;
  ceph_seq_t mseq;
  utime_t last_issue_stamp;
  CapExport() {}
  CapExport(int w, int i, int p, snapid_t cf, ceph_seq_t s, utime_t lis) : 
      wanted(w), issued(i), pending(p), client_follows(cf), mseq(s), last_issue_stamp(lis) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<CapExport*>& ls);
};

namespace ceph {
  class Formatter;
}

class Capability {
private:
  static boost::pool<> pool;
public:
  static void *operator new(size_t num_bytes) { 
    void *n = pool.malloc();
    if (!n)
      throw std::bad_alloc();
    return n;
  }
  void operator delete(void *p) {
    pool.free(p);
  }

private:
  CapObject *parent;
  client_t client;

  uint64_t cap_id;

  __u32 _wanted;     // what the client wants (ideally)

  utime_t last_issue_stamp;


  // track in-flight caps --------------
  //  - add new caps to _pending
  //  - track revocations in _revokes list
public:
  struct revoke_info {
    __u32 before;
    ceph_seq_t seq, last_issue;
    revoke_info() {}
    revoke_info(__u32 b, ceph_seq_t s, ceph_seq_t li) : before(b), seq(s), last_issue(li) {}
    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<revoke_info*>& ls);
  };
private:
  __u32 _pending, _issued;
  list<revoke_info> _revokes;

  // waiters
  struct seq_cmp {
    bool operator()(ceph_seq_t a, ceph_seq_t b) {
      return ceph_seq_cmp(a, b) < 0;
    }
  };
  typedef map<ceph_seq_t, list<Context*>, seq_cmp> seq_wait_map;
  seq_wait_map waiting_on_confirm;

  void finish_confirm_waiters(ceph_seq_t seq) {
    // finish waiters for all seq numbers <= seq
    seq_wait_map::iterator end = waiting_on_confirm.upper_bound(seq);
    for (seq_wait_map::iterator i = waiting_on_confirm.begin(); i != end; ++i)
      finish_contexts(g_ceph_context, i->second);
    waiting_on_confirm.erase(waiting_on_confirm.begin(), end);
  }
public:
  int pending() { return _pending; }
  int issued() {
    if (0) {
      //#warning capability debug sanity check, remove me someday
      unsigned o = _issued;
      _calc_issued();
      assert(o == _issued);
    }
    return _issued;
  }
  bool is_null() { return !_pending && _revokes.empty(); }

  ceph_seq_t issue(unsigned c) {
    if (_pending & ~c) {
      // revoking (and maybe adding) bits.  note caps prior to this revocation
      _revokes.push_back(revoke_info(_pending, last_sent, last_issue));
      _pending = c;
      _issued |= c;
    } else if (~_pending & c) {
      // adding bits only.  remove obsolete revocations?
      _pending |= c;
      _issued |= c;
      // drop old _revokes with no bits we don't have
      while (!_revokes.empty() &&
	     (_revokes.back().before & ~_pending) == 0)
	_revokes.pop_back();
    } else {
      // no change.
      assert(_pending == c);
    }
    //last_issue = 
    ++last_sent;
    return last_sent;
  }
  ceph_seq_t issue_norevoke(unsigned c) {
    _pending |= c;
    _issued |= c;
    //check_rdcaps_list();
    ++last_sent;
    return last_sent;
  }
  void _calc_issued() {
    _issued = _pending;
    for (list<revoke_info>::iterator p = _revokes.begin(); p != _revokes.end(); ++p)
      _issued |= p->before;
  }

  void add_confirm_waiter(ceph_seq_t seq, Context *c) {
    waiting_on_confirm[seq].push_back(c);
  }
  void confirm_receipt(ceph_seq_t seq, unsigned caps) {
    if (seq == last_sent) {
      _revokes.clear();
      _issued = caps;
      // don't add bits
      _pending &= caps;
    } else {
      // can i forget any revocations?
      while (!_revokes.empty() && _revokes.front().seq < seq)
	_revokes.pop_front();
      if (!_revokes.empty() && _revokes.front().seq == seq)
	_revokes.begin()->before = caps;
      _calc_issued();
    }
    finish_confirm_waiters(seq);
    //check_rdcaps_list();
  }
  // we may get a release racing with revocations, which means our revokes will be ignored
  // by the client.  clean them out of our _revokes history so we don't wait on them.
  void clean_revoke_from(ceph_seq_t li) {
    bool changed = false;
    while (!_revokes.empty() && _revokes.front().last_issue <= li) {
      _revokes.pop_front();
      changed = true;
    }
    if (changed)
      _calc_issued();
  }


private:
  ceph_seq_t last_sent;
  ceph_seq_t last_issue;
  ceph_seq_t mseq;

  int suppress;
  bool stale;

public:
  snapid_t client_follows;
  version_t client_xattr_version;
  
  xlist<Capability*>::item item_session_caps;
  xlist<Capability*>::item item_snaprealm_caps;
  xlist<Capability*>::item item_parent_lru;

  Capability(CapObject *parent = NULL, uint64_t id = 0, client_t c = -2) :
    parent(parent), client(c),
    cap_id(id),
    _wanted(0),
    _pending(0), _issued(0),
    last_sent(0),
    last_issue(0),
    mseq(0),
    suppress(0), stale(false),
    client_follows(0), client_xattr_version(0),
    item_session_caps(this),
    item_snaprealm_caps(this),
    item_parent_lru(this) {
    g_num_cap++;
    g_num_capa++;
  }
  ~Capability() {
    g_num_cap--;
    g_num_caps++;
  }
  
  ceph_seq_t get_mseq() { return mseq; }

  ceph_seq_t get_last_sent() { return last_sent; }
  utime_t get_last_issue_stamp() { return last_issue_stamp; }

  void set_last_issue() { last_issue = last_sent; }
  void set_last_issue_stamp(utime_t t) { last_issue_stamp = t; }

  void set_cap_id(uint64_t i) { cap_id = i; }
  uint64_t get_cap_id() { return cap_id; }

  //ceph_seq_t get_last_issue() { return last_issue; }

  bool is_suppress() { return suppress > 0; }
  void inc_suppress() { suppress++; }
  void dec_suppress() { suppress--; }

  bool is_stale() { return stale; }
  void set_stale(bool b) { stale = b; }

  CapObject* get_parent() { return parent; }
  client_t get_client() { return client; }

  // caps this client wants to hold
  int wanted() { return _wanted; }
  void set_wanted(int w) {
    _wanted = w;
    //check_rdcaps_list();
  }

  ceph_seq_t get_last_seq() { return last_sent; }
  ceph_seq_t get_last_issue() { return last_issue; }

  void reset_seq() {
    last_sent = 0;
    last_issue = 0;
  }
  
  // -- exports --
  CapExport make_export() {
    return CapExport(_wanted, issued(), pending(), client_follows, mseq+1, last_issue_stamp);
  }
  void rejoin_import() { mseq++; }
  void merge(CapExport& other, bool auth_cap) {
    // issued + pending
    int newpending = other.pending | pending();
    if (other.issued & ~newpending)
      issue(other.issued | newpending);
    else
      issue(newpending);
    last_issue_stamp = other.last_issue_stamp;

    client_follows = other.client_follows;

    // wanted
    _wanted = _wanted | other.wanted;
    if (auth_cap)
      mseq = other.mseq;
  }
  void merge(int otherwanted, int otherissued) {
    // issued + pending
    int newpending = pending();
    if (otherissued & ~newpending)
      issue(otherissued | newpending);
    else
      issue(newpending);

    // wanted
    _wanted = _wanted | otherwanted;
  }

  void revoke() {
    if (pending())
      issue(0);
    confirm_receipt(last_sent, 0);
  }

  // serializers
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<Capability*>& ls);
  
};

WRITE_CLASS_ENCODER(CapExport)
WRITE_CLASS_ENCODER(Capability::revoke_info)
WRITE_CLASS_ENCODER(Capability)



#endif
