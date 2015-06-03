// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLS_RGW_CLIENT_H
#define CEPH_CLS_RGW_CLIENT_H

#include "include/types.h"
#include "cls_rgw_types.h"
#include "common/RefCountedObj.h"
#include "osdc/RadosClient.h"

class RGWGetDirHeader_CB : public RefCountedObject {
public:
  virtual ~RGWGetDirHeader_CB() {}
  virtual void handle_response(int r, rgw_bucket_dir_header& header) = 0;
};

/* bucket index */
void cls_rgw_bucket_init(rados::ObjOpUse o);

void cls_rgw_bucket_set_tag_timeout(rados::ObjOpUse o,
				    uint64_t tag_timeout);

void cls_rgw_bucket_prepare_op(rados::ObjOpUse o,
			       RGWModifyOp op, string& tag,
			       string& name, bool log_op);

void cls_rgw_bucket_complete_op(rados::ObjOpUse o,
				RGWModifyOp op, string& tag,
				rgw_bucket_entry_ver& ver, string& name,
				rgw_bucket_dir_entry_meta& dir_meta,
				list<string> *remove_objs, bool log_op);

int cls_rgw_list_op(rados::Objecter* o, AVolRef& vol, const oid_t& oid,
		    string& start_obj, string& filter_prefix,
		    uint32_t num_entries, rgw_bucket_dir *dir,
		    bool *is_truncated);

int cls_rgw_bucket_check_index_op(rados::Objecter* o, AVolRef& vol,
				  const oid_t& oid,
				  rgw_bucket_dir_header *existing_header,
				  rgw_bucket_dir_header *calculated_header);
int cls_rgw_bucket_rebuild_index_op(rados::Objecter* o, AVolRef& vol,
				    const oid_t& oid);

int cls_rgw_get_dir_header(rados::Objecter* o, AVolRef& vol,
			   const oid_t& oid, rgw_bucket_dir_header *header);
int cls_rgw_get_dir_header_async(rados::Objecter* o, AVolRef& vol,
				 const oid_t& oid, RGWGetDirHeader_CB *ctx);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent,
			       bufferlist& updates);

void cls_rgw_suggest_changes(rados::ObjOpUse o, bufferlist& updates);

/* bucket index log */

int cls_rgw_bi_log_list(rados::Objecter* o, AVolRef& vol,
			 const oid_t& oid, string& marker, uint32_t max,
		    list<rgw_bi_log_entry>& entries, bool *truncated);
int cls_rgw_bi_log_trim(rados::Objecter* o, AVolRef& vol,
			const oid_t& oid, string& start_marker,
			string& end_marker);

/* usage logging */
int cls_rgw_usage_log_read(rados::Objecter* o, AVolRef& vol,
			   const oid_t& oid, string& user,
			   uint32_t max_entries, string& read_iter,
			   map<rgw_user_bucket, rgw_usage_log_entry>& usage,
			   bool *is_truncated);

void cls_rgw_usage_log_trim(rados::ObjOpUse op, string& user);

void cls_rgw_usage_log_add(rados::ObjOpUse op, rgw_usage_log_info& info);

/* garbage collection */
void cls_rgw_gc_set_entry(rados::ObjOpUse op,
			  ceph::timespan expiration,
			  cls_rgw_gc_obj_info& info);
void cls_rgw_gc_defer_entry(rados::ObjOpUse op,
			    ceph::timespan expiration,
			    const string& tag);

int cls_rgw_gc_list(rados::Objecter* o, AVolRef& vol, const oid_t& oid,
		    string& marker, uint32_t max, bool expired_only,
		    list<cls_rgw_gc_obj_info>& entries, bool *truncated);

void cls_rgw_gc_remove(rados::ObjOpUse op, const list<string>& tags);

#endif
