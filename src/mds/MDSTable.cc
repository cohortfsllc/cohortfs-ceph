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
#include "MDSTable.h"

#include "MDS.h"
#include "MDLog.h"

#include "include/types.h"

#include "common/config.h"
#include "osdc/Objecter.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << (mds ? mds->get_nodeid() : -1) << "." \
  << table_name << ": "


class C_MT_Save : public Context {
  MDSTable *ida;
  version_t version;
public:
  C_MT_Save(MDSTable *i, version_t v) : ida(i), version(v) {}
  void finish(int r) {
    ida->save_2(r, version);
  }
};

MDSTable::MDSTable(MDS *m, const char *n, bool is_per_mds) :
  mds(m), cct(mds->cct), table_name(n), per_mds(is_per_mds),
  state(STATE_UNDEF),
  version(0), committing_version(0), committed_version(0),
  projected_version(0) {
}

void MDSTable::save(Context *onfinish, version_t v)
{
  if (v > 0 && v <= committing_version) {
    dout(10) << "save v " << version << " - already saving "
	     << committing_version << " >= needed " << v << dendl;
    waitfor_save[v].push_back(onfinish);
    return;
  }

  dout(10) << "save v " << version << dendl;
  assert(is_active());

  bufferlist bl;
  ::encode(version, bl);
  encode_state(bl);

  committing_version = version;

  if (onfinish)
    waitfor_save[version].push_back(onfinish);

  // write (async)
  oid_t oid = get_object_name();
  VolumeRef volume(mds->get_metadata_volume());
  mds->objecter->write_full(oid, volume, bl, ceph::real_clock::now(), 0,
			    nullptr, new C_MT_Save(this, version));
}

void MDSTable::save_2(int r, version_t v)
{
  dout(10) << "save_2 v " << v << dendl;
  if (r == -EBLACKLISTED) {
    MDS::unique_lock l(mds->mds_lock, std::defer_lock);
    dout(10) << __func__ << " mds_lock" << dendl;
    mds->suicide(l);
    return;
  }
  if (r < 0) {
    dout(10) << "save_2 could not write table: " << r << dendl;
    assert(r >= 0);
  }
  assert(r >= 0);
  committed_version = v;

  std::vector<Context*> vs;
  while (!waitfor_save.empty()) {
    if (waitfor_save.begin()->first > v) break;
    move_left(vs, waitfor_save.begin()->second);
    waitfor_save.erase(waitfor_save.begin());
  }
  finish_contexts(vs,0);
}


void MDSTable::reset()
{
  reset_state();
  state = STATE_ACTIVE;
}



// -----------------------

class C_MT_Load : public Context {
public:
  MDSTable *ida;
  Context *onfinish;
  bufferlist bl;
  C_MT_Load(MDSTable *i, Context *o) : ida(i), onfinish(o) {}
  void finish(int r) {
    ida->load_2(r, bl, onfinish);
  }
};

oid_t MDSTable::get_object_name()
{
  char n[50];
  if (per_mds)
    snprintf(n, sizeof(n), "mds%d_%s", mds->whoami, table_name);
  else
    snprintf(n, sizeof(n), "mds_%s", table_name);
  return oid_t(n);
}

void MDSTable::load(Context *onfinish)
{
  dout(10) << "load" << dendl;

  assert(is_undef());
  state = STATE_OPENING;

  C_MT_Load *c = new C_MT_Load(this, onfinish);
  oid_t oid = get_object_name();
  VolumeRef volume(mds->get_metadata_volume());
  mds->objecter->read_full(oid, volume, &c->bl, 0, c);
}

void MDSTable::load_2(int r, bufferlist& bl, Context *onfinish)
{
  assert(is_opening());
  state = STATE_ACTIVE;
  if (r == -EBLACKLISTED) {
    MDS::unique_lock l(mds->mds_lock, std::defer_lock);
    dout(10) << __func__ << " mds_lock" << dendl;
    mds->suicide(l);
    return;
  }
  if (r < 0) {
    derr << "load_2 could not read table: " << r << dendl;
    assert(r >= 0);
  }

  dout(10) << "load_2 got " << bl.length() << " bytes" << dendl;
  bufferlist::iterator p = bl.begin();
  ::decode(version, p);
  projected_version = committed_version = version;
  dout(10) << "load_2 loaded v" << version << dendl;
  decode_state(p);

  if (onfinish) {
    onfinish->complete(0);
  }
}
