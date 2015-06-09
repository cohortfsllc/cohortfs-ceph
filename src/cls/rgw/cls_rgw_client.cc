// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "include/types.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"

#include "common/debug.h"

using namespace rados;

void cls_rgw_bucket_init(ObjOpUse o)
{
  bufferlist in;
  o->call("rgw", "bucket_init_index", in);
}

void cls_rgw_bucket_set_tag_timeout(ObjOpUse o, uint64_t tag_timeout)
{
  bufferlist in;
  struct rgw_cls_tag_timeout_op call;
  call.tag_timeout = tag_timeout;
  ::encode(call, in);
  o->call("rgw", "bucket_set_tag_timeout", in);
}

void cls_rgw_bucket_prepare_op(ObjOpUse o,
			       RGWModifyOp op, string& tag,
			       string& name, bool log_op)
{
  struct rgw_cls_obj_prepare_op call;
  call.op = op;
  call.tag = tag;
  call.name = name;
  call.log_op = log_op;
  bufferlist in;
  ::encode(call, in);
  o->call("rgw", "bucket_prepare_op", in);
}

void cls_rgw_bucket_complete_op(ObjOpUse o, RGWModifyOp op, string& tag,
				rgw_bucket_entry_ver& ver, string& name,
				rgw_bucket_dir_entry_meta& dir_meta,
				list<string> *remove_objs, bool log_op)
{

  bufferlist in;
  struct rgw_cls_obj_complete_op call;
  call.op = op;
  call.tag = tag;
  call.name = name;
  call.ver = ver;
  call.meta = dir_meta;
  call.log_op = log_op;
  if (remove_objs)
    call.remove_objs = *remove_objs;
  ::encode(call, in);
  o->call("rgw", "bucket_complete_op", in);
}


rgw_bucket_dir cls_rgw_list_op(Objecter* o, AVolRef& vol, const oid_t& oid,
			       string& start_obj, string& filter_prefix,
			       uint32_t num_entries, bool *is_truncated)
{
  bufferlist in, out;
  struct rgw_cls_list_op call;
  call.start_obj = start_obj;
  call.filter_prefix = filter_prefix;
  call.num_entries = num_entries;
  ::encode(call, in);
  o->call(oid, vol, "rgw", "bucket_list", in);

  struct rgw_cls_list_ret ret;
  bufferlist::iterator iter = out.begin();
  ::decode(ret, iter);

  if (is_truncated)
    *is_truncated = ret.is_truncated;

  return std::move(ret.dir);
}

std::tuple<rgw_bucket_dir_header, rgw_bucket_dir_header>
cls_rgw_bucket_check_index_op(Objecter* o, AVolRef& vol, const oid_t& oid)
{
  bufferlist in;
  bufferlist out(o->call(oid, vol, "rgw", "bucket_check_index", in));
  struct rgw_cls_check_index_ret ret;
  bufferlist::iterator iter = out.begin();
  ::decode(ret, iter);

  return std::make_tuple(std::move(ret.existing_header),
			 std::move(ret.calculated_header));
}

void cls_rgw_bucket_rebuild_index_op(Objecter* o, AVolRef& vol,
				     const oid_t& oid)
{
  bufferlist in;
  o->call(oid, vol, "rgw", "bucket_rebuild_index", in);
}

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent,
			       bufferlist& updates)
{
  updates.append(op);
  ::encode(dirent, updates);
}

void cls_rgw_suggest_changes(ObjOpUse o, bufferlist& updates)
{
  o->call("rgw", "dir_suggest_changes", updates);
}

rgw_bucket_dir_header cls_rgw_get_dir_header(Objecter* o, AVolRef& vol,
					     const oid_t& oid)
{
  bufferlist in;
  struct rgw_cls_list_op call;
  call.num_entries = 0;
  ObjectOperation op(vol->op());
  ::encode(call, in);
  struct rgw_cls_list_ret ret;
  op->call("rgw", "bucket_list", in,
	   [&ret](std::error_code e, bufferlist& bl) {
	     bufferlist::iterator iter = bl.begin();
	     ::decode(ret, iter);
	   });
  o->read(oid, vol, op);
  return std::move(ret.dir.header);
}

class GetDirHeaderCompletion {
  RGWGetDirHeader_CB *ret_ctx;
public:
  GetDirHeaderCompletion(RGWGetDirHeader_CB *_ctx) : ret_ctx(_ctx) {}
  ~GetDirHeaderCompletion() {
    ret_ctx->put();
  }
  void operator()(std::error_code e, bufferlist& bl) {
    struct rgw_cls_list_ret ret;
    int r = 0;
    if (!e)
      try {
	bufferlist::iterator iter = bl.begin();
	::decode(ret, iter);
      } catch (std::system_error& err) {
	r = -EIO;
      }
    else
      r = -e.value();

    ret_ctx->handle_response(r, ret.dir.header);
  };
};

void cls_rgw_get_dir_header_async(Objecter* o, AVolRef& vol, const oid_t& oid,
				  RGWGetDirHeader_CB *ctx)
{
  bufferlist in, out;
  struct rgw_cls_list_op call;
  call.num_entries = 0;
  ::encode(call, in);
  ObjectOperation op(vol->op());
  op->call("rgw", "bucket_list", in, GetDirHeaderCompletion(ctx));
  o->read(oid, vol, op, nullptr, nullptr);
}

list<rgw_bi_log_entry> cls_rgw_bi_log_list(Objecter* o, AVolRef& vol,
					   const oid_t& oid,
					   string& marker, uint32_t max,
					   bool *truncated)
{
  bufferlist in, out;
  cls_rgw_bi_log_list_op call;
  call.marker = marker;
  call.max = max;
  ::encode(call, in);
  out = o->call(oid, vol, "rgw", "bi_log_list", in);

  cls_rgw_bi_log_list_ret ret;
  bufferlist::iterator iter = out.begin();
  ::decode(ret, iter);

  return std::move(ret.entries);
}

void cls_rgw_bi_log_trim(Objecter* o, AVolRef& vol, const oid_t& oid,
			 string& start_marker, string& end_marker)
{
  do {
    bufferlist in;
    cls_rgw_bi_log_trim_op call;
    call.start_marker = start_marker;
    call.end_marker = end_marker;
    ::encode(call, in);
    try {
      o->call(oid, vol, "rgw", "bi_log_trim", in);
    } catch (const std::system_error& e) {
      if (e.code() == std::errc::no_message_available)
	break;
      else
	throw;
    }
  } while (1);
}


map<rgw_user_bucket, rgw_usage_log_entry>
cls_rgw_usage_log_read(Objecter* o, AVolRef& vol, const oid_t& oid,
		       string& user, uint32_t max_entries,
		       string& read_iter, bool *is_truncated)
{
  *is_truncated = false;

  bufferlist in, out;
  rgw_cls_usage_log_read_op call;
  call.owner = user;
  call.max_entries = max_entries;
  call.iter = read_iter;
  ::encode(call, in);
  out = o->call(oid, vol, "rgw", "user_usage_log_read", in);

  rgw_cls_usage_log_read_ret result;
  bufferlist::iterator iter = out.begin();
  ::decode(result, iter);
  read_iter = result.next_iter;
  if (is_truncated)
    *is_truncated = result.truncated;

  return std::move(result.usage);
}

void cls_rgw_usage_log_trim(ObjOpUse op, string& user)
{
  bufferlist in;
  rgw_cls_usage_log_trim_op call;
  call.user = user;
  ::encode(call, in);
  op->call("rgw", "user_usage_log_trim", in);
}

void cls_rgw_usage_log_add(ObjOpUse op, rgw_usage_log_info& info)
{
  bufferlist in;
  rgw_cls_usage_log_add_op call;
  call.info = info;
  ::encode(call, in);
  op->call("rgw", "user_usage_log_add", in);
}

/* garbage collection */

void cls_rgw_gc_set_entry(ObjOpUse op,
			  ceph::timespan expiration, cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_set_entry_op call;
  call.expiration = expiration;
  call.info = info;
  ::encode(call, in);
  op->call("rgw", "gc_set_entry", in);
}

void cls_rgw_gc_defer_entry(ObjOpUse op,
			    ceph::timespan expiration,
			    const string& tag)
{
  bufferlist in;
  cls_rgw_gc_defer_entry_op call;
  call.expiration = expiration;
  call.tag = tag;
  ::encode(call, in);
  op->call("rgw", "gc_defer_entry", in);
}

list<cls_rgw_gc_obj_info> cls_rgw_gc_list(Objecter* o, AVolRef& vol,
					  const oid_t& oid,
					  string& marker, uint32_t max,
					  bool expired_only,
					  bool *truncated)
{
  bufferlist in, out;
  cls_rgw_gc_list_op call;
  call.marker = marker;
  call.max = max;
  call.expired_only = expired_only;
  ::encode(call, in);
  out = o->call(oid, vol, "rgw", "gc_list", in);

  cls_rgw_gc_list_ret ret;
  bufferlist::iterator iter = out.begin();
  ::decode(ret, iter);

  if (truncated)
    *truncated = ret.truncated;

  return std::move(ret.entries);
}

void cls_rgw_gc_remove(ObjOpUse op, const list<string>& tags)
{
  bufferlist in;
  cls_rgw_gc_remove_op call;
  call.tags = tags;
  ::encode(call, in);
  op->call("rgw", "gc_remove", in);
}
