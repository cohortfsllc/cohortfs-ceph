// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLS_LOG_CLIENT_H
#define CEPH_CLS_LOG_CLIENT_H

#include "include/types.h"
#include "cls_log_types.h"

/*
 * log objclass
 */

void cls_log_add_prepare_entry(cls_log_entry& entry,
			       const ceph::real_time& timestamp,
			       const string& section,
			       const string& name, bufferlist& bl);

void cls_log_add(rados::ObjOpUse op,
		 list<cls_log_entry>& entry);
void cls_log_add(rados::ObjOpUse op, cls_log_entry& entry);
void cls_log_add(rados::ObjOpUse op,
		 const ceph::real_time& timestamp,
		 const string& section, const string& name, bufferlist& bl);

void cls_log_list(rados::ObjOpUse op,
		  ceph::real_time& from, ceph::real_time& to,
		  const string& in_marker, int max_entries,
		  list<cls_log_entry>& entries,
		  string *out_marker, bool *truncated);

void cls_log_trim(rados::ObjOpUse op,
		  const ceph::real_time& from_time,
		  const ceph::real_time& to_time,
		  const string& from_marker, const string& to_marker);
int cls_log_trim(Objecter* o, const VolumeRef& vol, const oid_t& oid,
		 const ceph::real_time& from_time,
		 const ceph::real_time& to_time,
		 const string& from_marker, const string& to_marker);

void cls_log_info(rados::ObjOpUse op, cls_log_header *header);

#endif
