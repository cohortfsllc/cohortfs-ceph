// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "include/types.h"
#include "osdc/RadosClient.h"
#include "cls/user/cls_user_ops.h"
#include "cls/user/cls_user_client.h"


using namespace rados;


void cls_user_set_buckets(ObjOpUse op, list<cls_user_bucket_entry>& entries,
			  bool add)
{
  bufferlist in;
  cls_user_set_buckets_op call;
  call.entries = entries;
  call.add = add;
  call.time = ceph::real_clock::now();
  ::encode(call, in);
  op->call("user", "set_buckets_info", in);
}

void cls_user_complete_stats_sync(ObjOpUse op)
{
  bufferlist in;
  cls_user_complete_stats_sync_op call;
  call.time = ceph::real_clock::now();
  ::encode(call, in);
  op->call("user", "complete_stats_sync", in);
}

void cls_user_remove_bucket(ObjOpUse op, const cls_user_bucket& bucket)
{
  bufferlist in;
  cls_user_remove_bucket_op call;
  call.bucket = bucket;
  ::encode(call, in);
  op->call("user", "remove_bucket", in);
}

class ClsUserListCB {
  list<cls_user_bucket_entry> *entries;
  string *marker;
  bool *truncated;
  int *pret;
public:
  ClsUserListCB(list<cls_user_bucket_entry> *_entries, string *_marker,
		bool *_truncated, int *_pret) :
    entries(_entries), marker(_marker), truncated(_truncated), pret(_pret) {}
  void operator()(int r, bufferlist&& outbl) {
    if (r >= 0) {
      cls_user_list_buckets_ret ret;
      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(ret, iter);
	if (entries)
	  *entries = ret.entries;
	if (truncated)
	  *truncated = ret.truncated;
	if (marker)
	  *marker = ret.marker;
      } catch (buffer::error& err) {
	r = -EIO;
      }
    }
    if (pret) {
      *pret = r;
    }
  }
};

void cls_user_bucket_list(ObjOpUse op,
			  const string& in_marker, int max_entries,
			  list<cls_user_bucket_entry>& entries,
			  string *out_marker, bool *truncated, int *pret)
{
  bufferlist inbl;
  cls_user_list_buckets_op call;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op->call("user", "list_buckets", inbl,
	   ClsUserListCB(&entries, out_marker, truncated, pret));
}

class ClsUserGetHeaderCB {
  cls_user_header *header;
  RGWGetUserHeader_CB *ret_ctx;
  int *pret;
public:
  ClsUserGetHeaderCB(cls_user_header *_h, RGWGetUserHeader_CB *_ctx,
		     int *_pret) : header(_h), ret_ctx(_ctx), pret(_pret) {}
  ~ClsUserGetHeaderCB() {
    if (ret_ctx) {
      ret_ctx->put();
    }
  }
  void operator()(int r, bufferlist&& outbl) {
    if (r >= 0) {
      cls_user_get_header_ret ret;
      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(ret, iter);
	if (header)
	  *header = ret.header;
      } catch (buffer::error& err) {
	r = -EIO;
      }
      if (ret_ctx) {
	ret_ctx->handle_response(r, ret.header);
      }
    }
    if (pret) {
      *pret = r;
    }
  }
};

void cls_user_get_header(ObjOpUse op,
			 cls_user_header *header, int *pret)
{
  bufferlist inbl;
  cls_user_get_header_op call;

  ::encode(call, inbl);

  op->call("user", "get_header", inbl, ClsUserGetHeaderCB(header, NULL, pret));
}

int cls_user_get_header_async(Objecter* o, const AVolRef& vol,
			      const oid_t& oid, RGWGetUserHeader_CB *ctx)
{
  bufferlist in, out;
  cls_user_get_header_op call;
  ::encode(call, in);
  ObjectOperation op(vol->op());
  op->call("user", "get_header", in,
	   /* no need to pass pret, as we'll call
	      ctx->handle_response() with correct error */
	   ClsUserGetHeaderCB(NULL, ctx, NULL));
  int r = o->mutate(oid, vol, op, nullptr);
  if (r < 0)
    return r;

  return 0;
}
