// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "include/types.h"
#include "osdc/RadosClient.h"
#include "cls/refcount/cls_refcount_ops.h"

using namespace rados;


void cls_refcount_get(ObjOpUse op, const string& tag, bool implicit_ref)
{
  bufferlist in;
  cls_refcount_get_op call;
  call.tag = tag;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  op->call("refcount", "get", in);
}

void cls_refcount_put(ObjOpUse op, const string& tag, bool implicit_ref)
{
  bufferlist in;
  cls_refcount_put_op call;
  call.tag = tag;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  op->call("refcount", "put", in);
}

void cls_refcount_set(ObjOpUse op, list<string>& refs)
{
  bufferlist in;
  cls_refcount_set_op call;
  call.refs = refs;
  ::encode(call, in);
  op->call("refcount", "set", in);
}

list<string> cls_refcount_read(Objecter* o, oid_t& oid, const AVolRef& vol,
			       bool implicit_ref)
{
  bufferlist in;
  cls_refcount_read_op call;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  ObjectOperation op(vol->op());
  cls_refcount_read_ret ret;
  op->call("refcount", "read", in,
	   [&ret](std::error_code e, bufferlist& bl){
	       if (!e) {
		   bufferlist::iterator iter = bl.begin();
		   ::decode(ret, iter);
	       }
	   });
  o->read(oid, vol, op);

  return std::move(ret.refs);
}
