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
    void operator()(int r, bufferlist&& outbl) {
    if (r >= 0) {
      cls_version_read_ret ret;
      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(ret, iter);
	*objv = ret.objv;
      } catch (std::system_error& err) {
	  // nothing we can do about it atm This is proabably a good
	  // candidate to be a future.
      }
    }
  }
};

void cls_version_read(ObjOpUse op, obj_version *objv)
{
  bufferlist inbl;
  op->call("version", "read", inbl, VersionReadCB(objv));
}

int cls_version_read(Objecter* o, const oid_t& oid, const AVolRef& vol,
		     obj_version *ver)
{
  bufferlist in, out;
  ObjectOperation op(vol->op());
  op->call("version", "read", in, &out);
  int r = o->read(oid, vol, op);
  if (r < 0)
    return r;

  cls_version_read_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (std::system_error& err) {
    return -EIO;
  }

  *ver = ret.objv;

  return r;
}
