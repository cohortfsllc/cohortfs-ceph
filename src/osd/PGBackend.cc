// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Inktank Storage, Inc.
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "PGBackend.h"
#include "OSD.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, PGBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

void PGBackend::on_change(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;
  // clear temp
  for (set<hobject_t>::iterator i = temp_contents.begin();
       i != temp_contents.end();
       ++i) {
    dout(10) << __func__ << ": Removing oid "
	     << *i << " from the temp collection" << dendl;
    t->remove(get_temp_coll(t), *i);
  }
  temp_contents.clear();
  _on_change(t);
}

coll_t PGBackend::get_temp_coll(ObjectStore::Transaction *t)
{
  if (temp_created)
    return temp_coll;
  if (!store->collection_exists(temp_coll))
      t->create_collection(temp_coll);
  temp_created = true;
  return temp_coll;
}

int PGBackend::objects_list_partial(
  const hobject_t &begin,
  int min,
  int max,
  vector<hobject_t> *ls,
  hobject_t *next)
{
  assert(ls);
  hobject_t _next(begin);
  ls->reserve(max);
  int r = 0;
  while (!_next.is_max() && ls->size() < (unsigned)min) {
    vector<hobject_t> objects;
    int r = store->collection_list_partial(
      coll,
      _next,
      min - ls->size(),
      max - ls->size(),
      &objects,
      &_next);
    if (r != 0)
      break;
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      ls->push_back(*i);
    }
  }
  if (r == 0)
    *next = _next;
  return r;
}

int PGBackend::objects_list_range(
  const hobject_t &start,
  const hobject_t &end,
  vector<hobject_t> *ls)
{
  assert(ls);
  vector<hobject_t> objects;
  int r = store->collection_list_range(
    coll,
    start,
    end,
    &objects);
  ls->reserve(objects.size());
  for (vector<hobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
      ls->push_back(*i);
  }
  return r;
}

int PGBackend::objects_get_attr(
  const hobject_t &hoid,
  const string &attr,
  bufferlist *out)
{
  bufferptr bp;
  int r = store->getattr(hoid.is_temp() ? temp_coll : coll,
			 hoid, attr.c_str(), bp);
  if (r >= 0 && out) {
    out->clear();
    out->push_back(bp);
  }
  return r;
}

int PGBackend::objects_get_attrs(
  const hobject_t &hoid,
  map<string, bufferlist> *out)
{
  return store->getattrs(hoid.is_temp() ? temp_coll : coll,
			 hoid, *out);
}

void PGBackend::trim_stashed_object(
  const hobject_t &hoid,
  version_t old_version,
  ObjectStore::Transaction *t) {
  assert(!hoid.is_temp());
  t->remove(coll, hoid);
}

PGBackend *PGBackend::build_pg_backend(
  const pg_pool_t &pool,
  const OSDMapRef curmap,
  Listener *l,
  coll_t coll,
  coll_t temp_coll,
  ObjectStore *store,
  CephContext *cct)
{
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED: {
    return new ReplicatedBackend(l, coll, temp_coll, store, cct);
  }

  default:
    assert(0);
    return NULL;
  }
}
