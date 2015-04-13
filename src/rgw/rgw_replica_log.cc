// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 * Copyright 2013 Inktank
 */

#include "common/ceph_json.h"

#include "rgw_replica_log.h"
#include "cls/replica_log/cls_replica_log_client.h"
#include "rgw_rados.h"


void RGWReplicaBounds::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  encode_json("oldest_time", oldest_time, f);
  encode_json("markers", markers, f);
}

void RGWReplicaBounds::decode_json(JSONObj *oid) {
  JSONDecoder::decode_json("marker", marker, oid);
  JSONDecoder::decode_json("oldest_time", oldest_time, oid);
  JSONDecoder::decode_json("markers", markers, oid);
}

RGWReplicaLogger::RGWReplicaLogger(RGWRados *_store) :
    cct(_store->cct), store(_store) {}

int RGWReplicaLogger::open_volume(VolumeRef& vol, const string& name)
{
  int r = 0;
  vol = store->rc.lookup_volume(name);
  if (!vol) {
    rgw_bucket p(name);
    r = store->create_vol(p);
    if (r < 0)
      return r;

    // retry
    vol = store->rc.lookup_volume(name);
    if (!vol)
      r = -ENOENT;
  }
  if (r < 0) {
    lderr(cct) << "ERROR: could not open rados vol " << name << dendl;
  }
  return r;
}

int RGWReplicaLogger::update_bound(const string& oid, const string& volname,
				   const string& daemon_id,
				   const string& marker,
				   const ceph::real_time& time,
				   const list<RGWReplicaItemMarker> *entries)
{
  cls_replica_log_progress_marker progress;
  progress.entity_id = daemon_id;
  progress.position_marker = marker;
  progress.position_time = time;
  progress.items = *entries;

  VolumeRef vol;
  int r = open_volume(vol, volname);
  if (r < 0) {
    return r;
  }

  rados::ObjectOperation opw(vol->op());
  cls_replica_log_update_bound(opw, progress);
  return store->rc.objecter->mutate(oid, vol, opw);
}

int RGWReplicaLogger::delete_bound(const string& oid, const string& volname,
				   const string& daemon_id)
{
  VolumeRef vol;
  int r = open_volume(vol, volname);
  if (r < 0) {
    return r;
  }

  rados::ObjectOperation opw(vol->op());
  cls_replica_log_delete_bound(opw, daemon_id);
  return store->rc.objecter->mutate(oid, vol, opw);
}

int RGWReplicaLogger::get_bounds(const string& oid, const string& volname,
				 RGWReplicaBounds& bounds)
{
  VolumeRef vol;
  int r = open_volume(vol, volname);
  if (r < 0) {
    return r;
  }

  return cls_replica_log_get_bounds(store->rc.objecter, vol, oid,
				    bounds.marker, bounds.oldest_time,
				    bounds.markers);
}

RGWReplicaObjectLogger::
RGWReplicaObjectLogger(RGWRados *_store,
		       const string& _volname,
		       const string& _prefix) : RGWReplicaLogger(_store),
						volname(_volname),
						prefix(_prefix) {
  if (volname.empty())
    store->get_log_vol_name(volname);
}

int RGWReplicaObjectLogger::create_log_objects(int shards)
{
  VolumeRef vol;
  int r = open_volume(vol, volname);
  if (r < 0) {
    return r;
  }
  for (int i = 0; i < shards; ++i) {
    string oid;
    get_shard_oid(i, oid);
    r = store->rc.objecter->create(oid, vol, false);
    if (r < 0)
      return r;
  }
  return r;
}

RGWReplicaBucketLogger::RGWReplicaBucketLogger(RGWRados *_store) :
  RGWReplicaLogger(_store)
{
  store->get_log_vol_name(vol);
  prefix = _store->ctx()->_conf->rgw_replica_log_obj_prefix;
  prefix.append(".");
}
