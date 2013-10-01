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



#ifndef CEPH_CAPOBJECT_H
#define CEPH_CAPOBJECT_H

#include "common/config.h"
#include "include/types.h"

#include "mdstypes.h"


class Capability;
struct CapExport;
class Context;
class Message;
class MClientCaps;
class MDCache;
class SnapRealm;
class Session;


class CapObject : public MDSCacheObject {
 public:
  MDCache *mdcache;

  int base_caps_allowed;

  map<client_t, Capability*> client_caps; // client -> caps
  map<int, int> mds_caps_wanted; // [auth] mds -> caps wanted
  int replica_caps_wanted; // [replica] what i've requested from auth

  vector<SimpleLock*> cap_locks;

  snapid_t first, last;
  SnapRealm *containing_realm;

  CapObject(MDCache *mdcache, snapid_t first, snapid_t last);
  virtual ~CapObject() {}

  bool is_head() const { return last == CEPH_NOSNAP; }

  virtual void encode_cap_message(MClientCaps *m, Capability *cap) = 0;
 
  // cap callbacks; force issue_caps() to send a CEPH_CAP_OP_SYNC_UPDATE
  // message to all clients matching cap_update_mask, and finish once all
  // updates are acked with CEPH_CAP_OP_UPDATE
  list<Context*> cap_updates;
  int cap_update_mask;

  xlist<Capability*> shared_cap_lru; // lru of caps with CEPH_CAP_ANY_SHARED
  xlist<Capability*> cap_blacklist; // caps blacklisted because of lru

  void update_cap_lru();
  bool is_cap_blacklisted(Capability *cap) const;


  // loner
  client_t loner_cap, want_loner_cap;

  client_t get_loner() { return loner_cap; }
  client_t get_wanted_loner() { return want_loner_cap; }

  // this is the loner state our locks should aim for
  client_t get_target_loner() {
    if (loner_cap == want_loner_cap)
      return loner_cap;
    else
      return -1;
  }

  client_t calc_ideal_loner();
  client_t choose_ideal_loner();
  bool try_set_loner();
  void set_loner_cap(client_t l);
  bool try_drop_loner();

  // choose new lock state during recovery, based on issued caps
  void choose_lock_state(SimpleLock *lock, int allissued);
  void choose_lock_states();

  int count_nonstale_caps() const;
  bool multiple_nonstale_caps() const;

  bool is_any_caps() const { return !client_caps.empty(); }
  bool is_any_nonstale_caps() const { return count_nonstale_caps(); }

  map<int,int>& get_mds_caps_wanted() { return mds_caps_wanted; }

  map<client_t,Capability*>& get_client_caps() { return client_caps; }
  Capability *get_client_cap(client_t client) {
    map<client_t,Capability*>::iterator i = client_caps.find(client);
    return i != client_caps.end() ? i->second : NULL;
  }
  int get_client_cap_pending(client_t client) const;

  virtual Capability *add_client_cap(client_t client, Session *session, SnapRealm *conrealm=0);
  virtual void remove_client_cap(client_t client);

  void move_to_realm(SnapRealm *realm);

  Capability *reconnect_cap(client_t client, ceph_mds_cap_reconnect& icr, Session *session);
  void clear_client_caps_after_export();
  void export_client_caps(map<client_t,CapExport>& cl);

  // caps allowed
  virtual int get_caps_liked() = 0;
  virtual int get_caps_allowed_ever();

  int get_caps_allowed_by_type(int type);
  int get_caps_careful();
  int get_xlocker_mask(client_t client);
  int get_caps_allowed_for_client(client_t client);

  // caps issued, wanted
  int get_caps_issued(int *ploner = 0, int *pother = 0, int *pxlocker = 0,
		      int shift = 0, int mask = 0xffff);
  bool is_any_caps_wanted();
  int get_caps_wanted(int *ploner = 0, int *pother = 0, int shift = 0, int mask = 0xffff);
  bool issued_caps_need_gather(SimpleLock *lock);

  virtual void print(ostream& out);
};

ostream& operator<<(ostream& out, CapObject& in);

#endif
