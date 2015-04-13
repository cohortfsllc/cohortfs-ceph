// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLS_STATELOG_CLIENT_H
#define CEPH_CLS_STATELOG_CLIENT_H

#include "include/types.h"
#include "cls_statelog_types.h"

/*
 * log objclass
 */

void cls_statelog_add_prepare_entry(cls_statelog_entry& entry,
				    const string& client_id,
				    const string& op_id,
				    const string& object,
				    const ceph::real_time& timestamp,
				    uint32_t state, bufferlist& bl);

void cls_statelog_add(rados::ObjOpUse op,
		      list<cls_statelog_entry>& entry);
void cls_statelog_add(rados::ObjOpUse op,
		      cls_statelog_entry& entry);
void cls_statelog_add(rados::ObjOpUse op,
		      const string& client_id, const string& op_id,
		      const string& object, const ceph::real_time& timestamp,
		      uint32_t state, bufferlist& bl);

void cls_statelog_list(rados::ObjOpUse op,
		       const string& client_id, const string& op_id,
		       const string& object, /* op_id may be empty,
						also one of client_id,
						object*/
		       const string& in_marker, int max_entries,
		       list<cls_statelog_entry>& entries,
		       string *out_marker, bool *truncated);

void cls_statelog_remove_by_client(rados::ObjOpUse op,
				   const string& client_id,
				   const string& op_id);
void cls_statelog_remove_by_object(rados::ObjOpUse op,
				   const string& object, const string& op_id);

void cls_statelog_check_state(rados::ObjOpUse op,
			      const string& client_id,const string& op_id,
			      const string& object, uint32_t state);
#endif
