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

int cls_refcount_read(Objecter* o, oid_t& oid, const AVolRef& vol,
		      list<string> *refs, bool implicit_ref)
{
  bufferlist in;
  bufferlist out;
  cls_refcount_read_op call;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  ObjectOperation op(vol->op());
  op->call("refcount", "read", in, &out);
  int r = o->read(oid, vol, op);
  if (r < 0)
    return r;

  cls_refcount_read_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  *refs = ret.refs;

  return r;
}
