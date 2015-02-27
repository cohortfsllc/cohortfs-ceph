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

#include <cassert>
#include "MDS.h"
#include "MDCache.h"
#include "SessionMap.h"

#include "common/config.h"
#include "common/errno.h"
#include "osdc/Objecter.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".sessionmap "

SessionMap::SessionMap(MDS *m)
  : mds(m), cct(mds->cct), version(0), projected(0), committing(0),
    committed(0), commit_waiters() {
}

void SessionMap::dump()
{
  dout(10) << "dump" << dendl;
  for (std::unordered_map<entity_name_t,Session*>::iterator p = session_map.begin();
       p != session_map.end();
       ++p)
    dout(10) << p->first << " " << p->second
	     << " state " << p->second->get_state_name()
	     << " completed " << p->second->info.completed_requests
	     << " prealloc_inos " << p->second->info.prealloc_inos
	     << " used_ions " << p->second->info.used_inos
	     << dendl;
}


// ----------------
// LOAD


oid_t SessionMap::get_object_name()
{
  char s[30];
  snprintf(s, sizeof(s), "mds%d_sessionmap", mds->whoami);
  return oid_t(s);
}

class SM_Load {
  SessionMap *sessionmap;
public:
  bufferlist bl;
  SM_Load(SessionMap *cm) : sessionmap(cm) {}
  void operator()(int r) {
    sessionmap->_load_finish(r, bl);
  }
};

void SessionMap::load(Context *onload)
{
  dout(10) << "load" << dendl;

  if (onload)
    waiting_for_load.push_back(*onload);

  OSDC::op_callback c(SM_Load(this));
  oid_t oid = get_object_name();
  VolumeRef volume(mds->get_metadata_volume());
  mds->objecter->read_full(oid, volume,
			   &c.target<SM_Load>()->bl, 0,
			   std::move(c));
}

void SessionMap::_load_finish(int r, bufferlist &bl)
{
  bufferlist::iterator blp = bl.begin();
  if (r < 0) {
    derr << "_load_finish got " << cpp_strerror(r) << dendl;
    assert(0 == "failed to load sessionmap");
  }
  dump();
  decode(blp);	// note: this sets last_cap_renew = now()
  dout(10) << "_load_finish v " << version
	   << ", " << session_map.size() << " sessions, "
	   << bl.length() << " bytes"
	   << dendl;
  projected = committing = committed = version;
  dump();
  finish_contexts(waiting_for_load);
}


// ----------------
// SAVE

class SM_Save {
  SessionMap *sessionmap;
  version_t version;
public:
  SM_Save(SessionMap *cm, version_t v) : sessionmap(cm), version(v) {}
  void operator()(int r) {
    assert(r == 0);
    sessionmap->_save_finish(version);
  }
};

void SessionMap::save(Context *onsave, version_t needv)
{
  dout(10) << "save needv " << needv << ", v " << version << dendl;

  if (needv && committing >= needv) {
    assert(committing > committed);
    commit_waiters[committing].push_back(*onsave);
    return;
  }

  commit_waiters[version].push_back(*onsave);

  bufferlist bl;

  encode(bl);
  committing = version;
  oid_t oid = get_object_name();
  VolumeRef volume(mds->get_metadata_volume());
  mds->objecter->write_full(oid, volume, bl, ceph::real_clock::now(), 0,
			    NULL, SM_Save(this, version));
}

void SessionMap::_save_finish(version_t v)
{
  dout(10) << "_save_finish v" << v << dendl;
  committed = v;

  finish_contexts(commit_waiters[v]);
  commit_waiters.erase(v);
}


// -------------------

void SessionMap::encode(bufferlist& bl) const
{
  uint64_t pre = -1;	 // for 0.19 compatibility; we forgot an encoding prefix.
  ::encode(pre, bl);

  ENCODE_START(3, 3, bl);
  ::encode(version, bl);

  for (std::unordered_map<entity_name_t,Session*>::const_iterator p = session_map.begin();
       p != session_map.end();
       ++p) {
    if (p->second->is_open() ||
	p->second->is_closing() ||
	p->second->is_stale() ||
	p->second->is_killing()) {
      ::encode(p->first, bl);
      p->second->info.encode(bl);
    }
  }
  ENCODE_FINISH(bl);
}

void SessionMap::decode(bufferlist::iterator& p)
{
  ceph::mono_time now = ceph::mono_clock::now();
  uint64_t pre;
  ::decode(pre, p);
  if (pre == (uint64_t)-1) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, p);
    assert(struct_v >= 2);

    ::decode(version, p);

    while (!p.end()) {
      entity_inst_t inst;
      ::decode(inst.name, p);
      Session *s = get_or_add_session(inst);
      if (s->is_closed())
	set_state(s, Session::STATE_OPEN);
      s->info.decode(p);
    }

    DECODE_FINISH(p);
  } else {
    // --- old format ----
    version = pre;

    // this is a meaningless upper bound.  can be ignored.
    uint32_t n;
    ::decode(n, p);

    while (n-- && !p.end()) {
      bufferlist::iterator p2 = p;
      Session *s = new Session;
      s->info.decode(p);
      if (session_map.count(s->info.inst.name)) {
	// eager client connected too fast!  aie.
	dout(10) << " already had session for " << s->info.inst.name << ", recovering" << dendl;
	entity_name_t n = s->info.inst.name;
	delete s;
	s = session_map[n];
	p = p2;
	s->info.decode(p);
      } else {
	session_map[s->info.inst.name] = s;
      }
      set_state(s, Session::STATE_OPEN);
      s->last_cap_renew = now;
    }
  }
}

void SessionMap::dump(Formatter *f) const
{
  f->open_array_section("Sessions");
  for (std::unordered_map<entity_name_t,Session*>::const_iterator p = session_map.begin();
       p != session_map.end();
       ++p)  {
    f->open_object_section("Session");
    f->open_object_section("entity name");
    p->first.dump(f);
    f->close_section(); // entity name
    f->open_object_section("Session info");
    p->second->info.dump(f);
    f->close_section(); // Session info
    f->close_section(); // Session
  }
  f->close_section(); // Sessions
}

void SessionMap::generate_test_instances(list<SessionMap*>& ls)
{
  // pretty boring for now
  ls.push_back(new SessionMap(NULL));
}

void SessionMap::wipe()
{
  dout(1) << "wipe start" << dendl;
  dump();
  while (!session_map.empty()) {
    Session *s = session_map.begin()->second;
    remove_session(s);
  }
  version = ++projected;
  dout(1) << "wipe result" << dendl;
  dump();
  dout(1) << "wipe done" << dendl;
}

void SessionMap::wipe_ino_prealloc()
{
  for (std::unordered_map<entity_name_t,Session*>::iterator p = session_map.begin();
       p != session_map.end();
       ++p) {
    p->second->pending_prealloc_inos.clear();
    p->second->info.prealloc_inos.clear();
    p->second->info.used_inos.clear();
  }
  projected = ++version;
}
