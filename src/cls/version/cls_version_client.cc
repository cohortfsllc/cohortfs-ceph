// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "include/types.h"
#include "osdc/RadosClient.h"
#include "cls/version/cls_version_ops.h"


using namespace rados;


void cls_version_set(ObjOpUse op, obj_version& objv)
{
  bufferlist in;
  cls_version_set_op call;
  call.objv = objv;
  ::encode(call, in);
  op->call("version", "set", in);
}

void cls_version_inc(ObjOpUse op)
{
  bufferlist in;
  cls_version_inc_op call;
  ::encode(call, in);
  op->call("version", "inc", in);
}

void cls_version_inc(ObjOpUse op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_inc_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  ::encode(call, in);
  op->call("version", "inc_conds", in);
}

void cls_version_check(ObjOpUse op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_inc_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  ::encode(call, in);
  op->call("version", "check_conds", in);
}

class VersionReadCB {
  obj_version *objv;
public:
  VersionReadCB(obj_version *_objv) : objv(_objv) {}
  void operator()(std::error_code r, bufferlist& outbl) {
    if (!r) {
      cls_version_read_ret ret;
      bufferlist::iterator iter = outbl.begin();
      ::decode(ret, iter);
      *objv = ret.objv;
    }
  }
};

void cls_version_read(ObjOpUse op, obj_version *objv)
{
  bufferlist inbl;
  op->call("version", "read", inbl, VersionReadCB(objv));
}

obj_version cls_version_read(Objecter* o, const oid_t& oid, const AVolRef& vol)
{
  bufferlist in;
  ObjectOperation op(vol->op());
  cls_version_read_ret ret;
  op->call("version", "read", in,
	   [&ret](std::error_code e, bufferlist& bl) {
	     bufferlist::iterator iter = bl.begin();
	     ::decode(ret, iter);
	   });
  o->read(oid, vol, op);
  return std::move(ret.objv);
}
