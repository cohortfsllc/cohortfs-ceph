// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "include/types.h"
#include "osdc/RadosClient.h"
#include "cls/statelog/cls_statelog_ops.h"


using namespace rados;


void cls_statelog_add(ObjOpUse op, list<cls_statelog_entry>& entries)
{
  bufferlist in;
  cls_statelog_add_op call;
  call.entries = entries;
  ::encode(call, in);
  op->call("statelog", "add", in);
}

void cls_statelog_add(ObjOpUse op, cls_statelog_entry& entry)
{
  bufferlist in;
  cls_statelog_add_op call;
  call.entries.push_back(entry);
  ::encode(call, in);
  op->call("statelog", "add", in);
}

void cls_statelog_add_prepare_entry(cls_statelog_entry& entry,
				    const string& client_id,
				    const string& op_id, const string& object,
				    const ceph::real_time& timestamp,
				    uint32_t state, bufferlist& bl)
{
  entry.client_id = client_id;
  entry.op_id = op_id;
  entry.object = object;
  entry.timestamp = timestamp;
  entry.state = state;
  entry.data = bl;
}

void cls_statelog_add(ObjOpUse op,
		      const string& client_id, const string& op_id,
		      const string& object, const ceph::real_time& timestamp,
		      uint32_t state, bufferlist& bl)

{
  cls_statelog_entry entry;

  cls_statelog_add_prepare_entry(entry, client_id, op_id, object, timestamp,
				 state, bl);
  cls_statelog_add(op, entry);
}

void cls_statelog_remove_by_client(ObjOpUse op, const string& client_id,
				   const string& op_id)
{
  bufferlist in;
  cls_statelog_remove_op call;
  call.client_id = client_id;
  call.op_id = op_id;
  ::encode(call, in);
  op->call("statelog", "remove", in);
}

void cls_statelog_remove_by_object(ObjOpUse op, const string& object,
				   const string& op_id)
{
  bufferlist in;
  cls_statelog_remove_op call;
  call.object = object;
  call.op_id = op_id;
  ::encode(call, in);
  op->call("statelog", "remove", in);
}

class StateLogListCB {
  list<cls_statelog_entry> *entries;
  string *marker;
  bool *truncated;
public:
  StateLogListCB(list<cls_statelog_entry> *_entries, string *_marker,
		 bool *_truncated) : entries(_entries), marker(_marker),
				      truncated(_truncated) {}
  void operator()(std::error_code r, bufferlist& outbl) {
    if (!r) {
      cls_statelog_list_ret ret;
      bufferlist::iterator iter = outbl.begin();
      ::decode(ret, iter);
      if (entries)
	*entries = ret.entries;
      if (truncated)
	*truncated = ret.truncated;
      if (marker)
	*marker = ret.marker;
    }
  }
};

void cls_statelog_list(ObjOpUse op, const string& client_id,
		       /* op_id may be empty, also one of client_id, object*/
		       const string& op_id, const string& object,
		       const string& in_marker, int max_entries,
		       list<cls_statelog_entry>& entries,
		       string *out_marker, bool *truncated)
{
  bufferlist inbl;
  cls_statelog_list_op call;
  call.client_id = client_id;
  call.op_id = op_id;
  call.object = object;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op->call("statelog", "list", inbl,
	   StateLogListCB(&entries, out_marker, truncated));
}

void cls_statelog_check_state(ObjOpUse op, const string& client_id,
			      const string& op_id, const string& object,
			      uint32_t state)
{
  bufferlist inbl;
  bufferlist outbl;
  cls_statelog_check_state_op call;
  call.client_id = client_id;
  call.op_id = op_id;
  call.object = object;
  call.state = state;

  ::encode(call, inbl);

  op->call("statelog", "check_state", inbl, NULL);
}

