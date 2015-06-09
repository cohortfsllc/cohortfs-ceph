// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 */

#include <errno.h>

#include "osdc/RadosClient.h"
#include "cls/replica_log/cls_replica_log_ops.h"

using namespace rados;

void cls_replica_log_prepare_marker(
    cls_replica_log_progress_marker& progress, const string& entity,
    const string& marker, const ceph::real_time& time,
    const list<pair<string, ceph::real_time> > *entries)
{
  progress.entity_id = entity;
  progress.position_marker = marker;
  progress.position_time = time;
  if (entries) {
    for (const auto& i : *entries) {
      cls_replica_log_item_marker item(i.first, i.second);
      progress.items.push_back(item);
    }
  }
}

void cls_replica_log_extract_marker(
  const cls_replica_log_progress_marker& progress,
  string& entity, string& marker, ceph::real_time& time,
  list<pair<string, ceph::real_time> >& entries)
{
  entity = progress.entity_id;
  marker = progress.position_marker;
  time = progress.position_time;
  list<cls_replica_log_item_marker>::const_iterator i;
  for (i = progress.items.begin(); i != progress.items.end(); ++i) {
    entries.push_back(make_pair(i->item_name, i->item_timestamp));
  }
}

void cls_replica_log_update_bound(ObjOpUse o,
				  const cls_replica_log_progress_marker& progress)
{
  cls_replica_log_set_marker_op op(progress);
  bufferlist in;
  ::encode(op, in);
  o->call("replica_log", "set", in);
}

void cls_replica_log_delete_bound(ObjOpUse o,
				  const string& entity)
{
  cls_replica_log_delete_marker_op op(entity);
  bufferlist in;
  ::encode(op, in);
  o->call("replica_log", "delete", in);
}

void cls_replica_log_get_bounds(Objecter* o, const AVolRef& vol,
				const oid_t& oid, string& position_marker,
				ceph::real_time& oldest_time,
				list<cls_replica_log_progress_marker>& markers)
{
  bufferlist in;
  cls_replica_log_get_bounds_op op;
  ::encode(op, in);
  ObjectOperation obj_op(vol->op());

  cls_replica_log_get_bounds_ret ret;
  obj_op->call("replica_log", "get", in,
	       [&ret](std::error_code e, bufferlist& bl) {
		 bufferlist::iterator i = bl.begin();
		 ::decode(ret, i);
	       });

  o->read(oid, vol, obj_op);

  position_marker = ret.position_marker;
  oldest_time = ret.oldest_time;
  markers = ret.markers;
}
