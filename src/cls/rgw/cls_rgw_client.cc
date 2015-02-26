#include <errno.h>

#include "include/types.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_client.h"
#include "include/rados/librados.hpp"

#include "common/debug.h"

using namespace librados;

void cls_rgw_bucket_init(ObjectWriteOperation& o)
{
  bufferlist in;
  o.exec("rgw", "bucket_init_index", in);
}

void cls_rgw_bucket_set_tag_timeout(ObjectWriteOperation& o, uint64_t tag_timeout)
{
  bufferlist in;
  struct rgw_cls_tag_timeout_op call;
  call.tag_timeout = tag_timeout;
  ::encode(call, in);
  o.exec("rgw", "bucket_set_tag_timeout", in);
}

void cls_rgw_bucket_prepare_op(ObjectWriteOperation& o, RGWModifyOp op, string& tag,
			       string& name, string& locator, bool log_op)
{
  struct rgw_cls_obj_prepare_op call;
  call.op = op;
  call.tag = tag;
  call.name = name;
  call.locator = locator;
  call.log_op = log_op;
  bufferlist in;
  ::encode(call, in);
  o.exec("rgw", "bucket_prepare_op", in);
}

void cls_rgw_bucket_complete_op(ObjectWriteOperation& o, RGWModifyOp op, string& tag,
				rgw_bucket_entry_ver& ver, string& name, rgw_bucket_dir_entry_meta& dir_meta,
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
  o.exec("rgw", "bucket_complete_op", in);
}


int cls_rgw_list_op(IoCtx& io_ctx, string& oid_t, string& start_obj,
		    string& filter_prefix, uint32_t num_entries,
		    rgw_bucket_dir *dir, bool *is_truncated)
{
  bufferlist in, out;
  struct rgw_cls_list_op call;
  call.start_obj = start_obj;
  call.filter_prefix = filter_prefix;
  call.num_entries = num_entries;
  ::encode(call, in);
  int r = io_ctx.exec(oid_t, "rgw", "bucket_list", in, out);
  if (r < 0)
    return r;

  struct rgw_cls_list_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  if (dir)
    *dir = ret.dir;
  if (is_truncated)
    *is_truncated = ret.is_truncated;

 return r;
}

int cls_rgw_bucket_check_index_op(IoCtx& io_ctx, string& oid_t,
				  rgw_bucket_dir_header *existing_header,
				  rgw_bucket_dir_header *calculated_header)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid_t, "rgw", "bucket_check_index", in, out);
  if (r < 0)
    return r;

  struct rgw_cls_check_index_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  if (existing_header)
    *existing_header = ret.existing_header;
  if (calculated_header)
    *calculated_header = ret.calculated_header;

  return 0;
}

int cls_rgw_bucket_rebuild_index_op(IoCtx& io_ctx, string& oid_t)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid_t, "rgw", "bucket_rebuild_index", in, out);
  if (r < 0)
    return r;

  return 0;
}

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates)
{
  updates.append(op);
  ::encode(dirent, updates);
}

void cls_rgw_suggest_changes(ObjectWriteOperation& o, bufferlist& updates)
{
  o.exec("rgw", "dir_suggest_changes", updates);
}

int cls_rgw_get_dir_header(IoCtx& io_ctx, string& oid_t, rgw_bucket_dir_header *header)
{
  bufferlist in, out;
  struct rgw_cls_list_op call;
  call.num_entries = 0;
  ::encode(call, in);
  int r = io_ctx.exec(oid_t, "rgw", "bucket_list", in, out);
  if (r < 0)
    return r;

  struct rgw_cls_list_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  if (header)
    *header = ret.dir.header;

 return r;
}

class GetDirHeaderCompletion : public ObjectOperationCompletion {
  RGWGetDirHeader_CB *ret_ctx;
public:
  GetDirHeaderCompletion(RGWGetDirHeader_CB *_ctx) : ret_ctx(_ctx) {}
  ~GetDirHeaderCompletion() {
    ret_ctx->put();
  }
  void handle_completion(int r, bufferlist& outbl) {
    struct rgw_cls_list_ret ret;
    try {
      bufferlist::iterator iter = outbl.begin();
      ::decode(ret, iter);
    } catch (buffer::error& err) {
      r = -EIO;
    }

    ret_ctx->handle_response(r, ret.dir.header);
  };
};

int cls_rgw_get_dir_header_async(IoCtx& io_ctx, string& oid_t, RGWGetDirHeader_CB *ctx)
{
  bufferlist in, out;
  struct rgw_cls_list_op call;
  call.num_entries = 0;
  ::encode(call, in);
  ObjectReadOperation op;
  GetDirHeaderCompletion *cb = new GetDirHeaderCompletion(ctx);
  op.exec("rgw", "bucket_list", in, cb);
  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  int r = io_ctx.aio_operate(oid_t, c, &op, NULL);
  c->release();
  if (r < 0)
    return r;

  return 0;
}

int cls_rgw_bi_log_list(IoCtx& io_ctx, string& oid_t, string& marker, uint32_t max,
		    list<rgw_bi_log_entry>& entries, bool *truncated)
{
  bufferlist in, out;
  cls_rgw_bi_log_list_op call;
  call.marker = marker;
  call.max = max;
  ::encode(call, in);
  int r = io_ctx.exec(oid_t, "rgw", "bi_log_list", in, out);
  if (r < 0)
    return r;

  cls_rgw_bi_log_list_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries = ret.entries;

  if (truncated)
    *truncated = ret.truncated;

 return r;
}

int cls_rgw_bi_log_trim(IoCtx& io_ctx, string& oid_t, string& start_marker, string& end_marker)
{
  do {
    int r;
    bufferlist in, out;
    cls_rgw_bi_log_trim_op call;
    call.start_marker = start_marker;
    call.end_marker = end_marker;
    ::encode(call, in);
    r = io_ctx.exec(oid_t, "rgw", "bi_log_trim", in, out);

    if (r == -ENODATA)
      break;

    if (r < 0)
      return r;

  } while (1);

  return 0;
}

int cls_rgw_usage_log_read(IoCtx& io_ctx, string& oid_t, string& user,
			   uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
			   string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
			   bool *is_truncated)
{
  *is_truncated = false;

  bufferlist in, out;
  rgw_cls_usage_log_read_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.owner = user;
  call.max_entries = max_entries;
  call.iter = read_iter;
  ::encode(call, in);
  int r = io_ctx.exec(oid_t, "rgw", "user_usage_log_read", in, out);
  if (r < 0)
    return r;

  try {
    rgw_cls_usage_log_read_ret result;
    bufferlist::iterator iter = out.begin();
    ::decode(result, iter);
    read_iter = result.next_iter;
    if (is_truncated)
      *is_truncated = result.truncated;

    usage = result.usage;
  } catch (buffer::error& e) {
    return -EINVAL;
  }

  return 0;
}

void cls_rgw_usage_log_trim(ObjectWriteOperation& op, string& user,
			   uint64_t start_epoch, uint64_t end_epoch)
{
  bufferlist in;
  rgw_cls_usage_log_trim_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.user = user;
  ::encode(call, in);
  op.exec("rgw", "user_usage_log_trim", in);
}

void cls_rgw_usage_log_add(ObjectWriteOperation& op, rgw_usage_log_info& info)
{
  bufferlist in;
  rgw_cls_usage_log_add_op call;
  call.info = info;
  ::encode(call, in);
  op.exec("rgw", "user_usage_log_add", in);
}

/* garbage collection */

void cls_rgw_gc_set_entry(ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_set_entry_op call;
  call.expiration_secs = expiration_secs;
  call.info = info;
  ::encode(call, in);
  op.exec("rgw", "gc_set_entry", in);
}

void cls_rgw_gc_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const string& tag)
{
  bufferlist in;
  cls_rgw_gc_defer_entry_op call;
  call.expiration_secs = expiration_secs;
  call.tag = tag;
  ::encode(call, in);
  op.exec("rgw", "gc_defer_entry", in);
}

int cls_rgw_gc_list(IoCtx& io_ctx, string& oid_t, string& marker, uint32_t max, bool expired_only,
		    list<cls_rgw_gc_obj_info>& entries, bool *truncated)
{
  bufferlist in, out;
  cls_rgw_gc_list_op call;
  call.marker = marker;
  call.max = max;
  call.expired_only = expired_only;
  ::encode(call, in);
  int r = io_ctx.exec(oid_t, "rgw", "gc_list", in, out);
  if (r < 0)
    return r;

  cls_rgw_gc_list_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries = ret.entries;

  if (truncated)
    *truncated = ret.truncated;

 return r;
}

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const list<string>& tags)
{
  bufferlist in;
  cls_rgw_gc_remove_op call;
  call.tags = tags;
  ::encode(call, in);
  op.exec("rgw", "gc_remove", in);
}
