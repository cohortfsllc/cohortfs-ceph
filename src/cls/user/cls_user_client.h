// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_USER_CLIENT_H
#define CEPH_CLS_USER_CLIENT_H

#include "include/types.h"
#include "cls_user_types.h"
#include "common/RefCountedObj.h"

class RGWGetUserHeader_CB : public RefCountedObject {
public:
  virtual ~RGWGetUserHeader_CB() {}
  virtual void handle_response(int r, cls_user_header& header) = 0;
};

/*
 * user objclass
 */

void cls_user_set_buckets(rados::ObjOpUse op,
			  list<cls_user_bucket_entry>& entries, bool add);
void cls_user_complete_stats_sync(rados::ObjOpUse op);
void cls_user_remove_bucket(rados::ObjOpUse op, const cls_user_bucket& bucket);
void cls_user_bucket_list(rados::ObjOpUse op,
		       const string& in_marker, int max_entries,
		       list<cls_user_bucket_entry>& entries,
		       string *out_marker, bool *truncated,
		       int *pret);
void cls_user_get_header(rados::ObjOpUse op, cls_user_header *header,
			 int *pret);
int cls_user_get_header_async(rados::Objecter* o, const AVolRef& v,
			      const oid_t& oid, RGWGetUserHeader_CB *ctx);

#endif
