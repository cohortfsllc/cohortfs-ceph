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

#ifndef CEPH_MDS_SESSIONMAP_H
#define CEPH_MDS_SESSIONMAP_H

#include <set>
using std::set;

#include <ext/hash_map>
using __gnu_cxx::hash_map;

#include "include/Context.h"
#include "include/xlist.h"
#include "include/elist.h"
#include "include/interval_set.h"
#include "mdstypes.h"

class CInode;
class MDRequest;

#include "CInode.h"
#include "Capability.h"
#include "msg/Message.h"


/* 
 * session
 */

class Session : public RefCountedObject {
  // -- state etc --
public:
  /*
                    
        <deleted> <-- closed <------------+
             ^         |                  |
             |         v                  |
          killing <-- opening <----+      |
             ^         |           |      |
             |         v           |      |
           stale <--> open --> closing ---+

    + additional dimension of 'importing' (with counter)

  */
  enum {
    STATE_CLOSED = 0,
    STATE_OPENING = 1,   // journaling open
    STATE_OPEN = 2,
    STATE_CLOSING = 3,   // journaling close
    STATE_STALE = 4,
    STATE_KILLING = 5
  };

  const char *get_state_name(int s) {
    switch (s) {
    case STATE_CLOSED: return "closed";
    case STATE_OPENING: return "opening";
    case STATE_OPEN: return "open";
    case STATE_CLOSING: return "closing";
    case STATE_STALE: return "stale";
    case STATE_KILLING: return "killing";
    default: return "???";
    }
  }

private:
  int state;
  uint64_t state_seq;
  int importing_count;
  friend class SessionMap;
public:
  entity_inst_t inst;
  Connection *connection;
  xlist<Session*>::item item_session_list;

  elist<MDRequest*> requests;

  interval_set<inodeno_t> pending_prealloc_inos; // journaling prealloc, will be added to prealloc_inos
  interval_set<inodeno_t> prealloc_inos;   // preallocated, ready to use.
  interval_set<inodeno_t> used_inos;       // journaling use

  inodeno_t next_ino() {
    if (prealloc_inos.empty())
      return 0;
    return prealloc_inos.range_start();
  }
  inodeno_t take_ino(inodeno_t ino = 0) {
    assert(!prealloc_inos.empty());

    if (ino) {
      if (prealloc_inos.contains(ino))
	prealloc_inos.erase(ino);
      else
	ino = 0;
    }
    if (!ino) {
      ino = prealloc_inos.range_start();
      prealloc_inos.erase(ino);
    }
    used_inos.insert(ino, 1);
    return ino;
  }
  int get_num_projected_prealloc_inos() {
    return prealloc_inos.size() + pending_prealloc_inos.size();
  }

  client_t get_client() { return client_t(inst.name.num()); }

  int get_state() { return state; }
  const char *get_state_name() { return get_state_name(state); }
  uint64_t get_state_seq() { return state_seq; }
  bool is_closed() { return state == STATE_CLOSED; }
  bool is_opening() { return state == STATE_OPENING; }
  bool is_open() { return state == STATE_OPEN; }
  bool is_closing() { return state == STATE_CLOSING; }
  bool is_stale() { return state == STATE_STALE; }
  bool is_killing() { return state == STATE_KILLING; }

  void inc_importing() {
    ++importing_count;
  }
  void dec_importing() {
    assert(importing_count);
    --importing_count;
  }
  bool is_importing() { return importing_count > 0; }

  // -- caps --
private:
  version_t cap_push_seq;        // cap push seq #
public:
  xlist<Capability*> caps;     // inodes with caps; front=most recently used
  xlist<ClientLease*> leases;  // metadata leases to clients
  utime_t last_cap_renew;

public:
  version_t inc_push_seq() { return ++cap_push_seq; }
  version_t get_push_seq() const { return cap_push_seq; }

  void add_cap(Capability *cap) {
    caps.push_back(&cap->item_session_caps);
  }
  void touch_lease(ClientLease *r) {
    leases.push_back(&r->item_session_lease);
  }

  // -- leases --
  uint32_t lease_seq;

  // -- completed requests --
private:
  set<tid_t> completed_requests;

public:
  void add_completed_request(tid_t t) {
    completed_requests.insert(t);
  }
  void trim_completed_requests(tid_t mintid) {
    // trim
    while (!completed_requests.empty() && 
	   (mintid == 0 || *completed_requests.begin() < mintid))
      completed_requests.erase(completed_requests.begin());
  }
  bool have_completed_request(tid_t tid) const {
    return completed_requests.count(tid);
  }


  Session() : 
    state(STATE_CLOSED), state_seq(0), importing_count(0),
    connection(NULL), item_session_list(this),
    requests(0),  // member_offset passed to front() manually
    cap_push_seq(0),
    lease_seq(0) { }
  ~Session() {
    assert(!item_session_list.is_on_list());
  }

  void clear() {
    pending_prealloc_inos.clear();
    prealloc_inos.clear();
    used_inos.clear();

    cap_push_seq = 0;
    last_cap_renew = utime_t();

    completed_requests.clear();
  }

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(inst, bl);
    ::encode(completed_requests, bl);
    ::encode(prealloc_inos, bl);   // hacky, see below.
    ::encode(used_inos, bl);
  }
  void decode(bufferlist::iterator& p) {
    __u8 v;
    ::decode(v, p);
    ::decode(inst, p);
    ::decode(completed_requests, p);
    ::decode(prealloc_inos, p);
    ::decode(used_inos, p);
    prealloc_inos.insert(used_inos);
    used_inos.clear();
  }
};
WRITE_CLASS_ENCODER(Session)

/*
 * session map
 */

class MDS;

class SessionMap {
private:
  MDS *mds;
  hash_map<entity_name_t, Session*> session_map;
public:
  map<int,xlist<Session*> > by_state;
  
public:  // i am lazy
  version_t version, projected, committing, committed;
  map<version_t, list<Context*> > commit_waiters;

public:
  SessionMap(MDS *m) : mds(m), 
		       version(0), projected(0), committing(0), committed(0) 
  { }
    
  // sessions
  bool empty() { return session_map.empty(); }
  bool have_unclosed_sessions() {
    return
      !by_state[Session::STATE_OPENING].empty() ||
      !by_state[Session::STATE_OPEN].empty() ||
      !by_state[Session::STATE_CLOSING].empty() ||
      !by_state[Session::STATE_STALE].empty() ||
      !by_state[Session::STATE_KILLING].empty();
  }
  bool have_session(entity_name_t w) {
    return session_map.count(w);
  }
  Session* get_session(entity_name_t w) {
    if (session_map.count(w))
      return session_map[w];
    return 0;
  }
  Session* get_or_add_session(entity_inst_t i) {
    Session *s;
    if (session_map.count(i.name))
      s = session_map[i.name];
    else
      s = session_map[i.name] = new Session;
    s->inst = i;
    s->last_cap_renew = g_clock.now();
    return s;
  }
  void add_session(Session *s) {
    assert(session_map.count(s->inst.name) == 0);
    session_map[s->inst.name] = s;
    by_state[s->state].push_back(&s->item_session_list);
    s->get();
  }
  void remove_session(Session *s) {
    s->trim_completed_requests(0);
    s->item_session_list.remove_myself();
    session_map.erase(s->inst.name);
    s->put();
  }
  void touch_session(Session *session) {
    if (session->item_session_list.is_on_list()) {
      by_state[session->state].push_back(&session->item_session_list);
      session->last_cap_renew = g_clock.now();
    } else {
      assert(0);  // hrm, should happen?
    }
  }
  Session *get_oldest_session(int state) {
    if (by_state[state].empty()) return 0;
    return by_state[state].front();
  }
  uint64_t set_state(Session *session, int s) {
    if (session->state != s) {
      session->state = s;
      session->state_seq++;
      by_state[s].push_back(&session->item_session_list);
    }
    return session->state_seq;
  }
  void dump();

  void get_client_set(set<client_t>& s) {
    for (hash_map<entity_name_t,Session*>::iterator p = session_map.begin();
	 p != session_map.end();
	 p++)
      if (p->second->inst.name.is_client())
	s.insert(p->second->inst.name.num());
  }
  void get_client_session_set(set<Session*>& s) {
    for (hash_map<entity_name_t,Session*>::iterator p = session_map.begin();
	 p != session_map.end();
	 p++)
      if (p->second->inst.name.is_client())
	s.insert(p->second);
  }

  void open_sessions(map<client_t,entity_inst_t>& client_map) {
    for (map<client_t,entity_inst_t>::iterator p = client_map.begin(); 
	 p != client_map.end(); 
	 ++p) {
      Session *s = get_or_add_session(p->second);
      set_state(s, Session::STATE_OPEN);
    }
    version++;
  }

  // helpers
  entity_inst_t& get_inst(entity_name_t w) {
    assert(session_map.count(w));
    return session_map[w]->inst;
  }
  version_t inc_push_seq(client_t client) {
    return get_session(entity_name_t::CLIENT(client.v))->inc_push_seq();
  }
  version_t get_push_seq(client_t client) {
    return get_session(entity_name_t::CLIENT(client.v))->get_push_seq();
  }
  bool have_completed_request(metareqid_t rid) {
    Session *session = get_session(rid.name);
    return session && session->have_completed_request(rid.tid);
  }
  void add_completed_request(metareqid_t rid, tid_t tid=0) {
    Session *session = get_session(rid.name);
    assert(session);
    session->add_completed_request(rid.tid);
    if (tid)
      session->trim_completed_requests(tid);
  }
  void trim_completed_requests(entity_name_t c, tid_t tid) {
    Session *session = get_session(c);
    assert(session);
    session->trim_completed_requests(tid);
  }

  void wipe();
  void wipe_ino_prealloc();

  // -- loading, saving --
  inodeno_t ino;
  list<Context*> waiting_for_load;

  void encode(bufferlist& bl);
  void decode(bufferlist::iterator& blp);

  object_t get_object_name();

  void load(Context *onload);
  void _load_finish(int r, bufferlist &bl);
  void save(Context *onsave, version_t needv=0);
  void _save_finish(version_t v);
 
};


#endif
