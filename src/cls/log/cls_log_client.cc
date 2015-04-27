// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "include/types.h"
#include "cls/log/cls_log_ops.h"
#include "osdc/RadosClient.h"


using namespace rados;



void cls_log_add(ObjOpUse op, list<cls_log_entry>& entries)
{
  bufferlist in;
  cls_log_add_op call;
  call.entries = entries;
  ::encode(call, in);
  op->call("log", "add", in);
}

void cls_log_add(ObjOpUse op, cls_log_entry& entry)
{
  bufferlist in;
  cls_log_add_op call;
  call.entries.push_back(entry);
  ::encode(call, in);
  op->call("log", "add", in);
}

void cls_log_add_prepare_entry(cls_log_entry& entry,
			       const ceph::real_time& timestamp,
			       const string& section, const string& name,
			       bufferlist& bl)
{
  entry.timestamp = timestamp;
  entry.section = section;
  entry.name = name;
  entry.data = bl;
}

void cls_log_add(ObjOpUse op,
		 const ceph::real_time& timestamp,
		 const string& section, const string& name, bufferlist& bl)
{
  cls_log_entry entry;

  cls_log_add_prepare_entry(entry, timestamp, section, name, bl);
  cls_log_add(op, entry);
}

void cls_log_trim(ObjOpUse op,
		  const ceph::real_time& from_time,
		  const ceph::real_time& to_time,
		  const string& from_marker, const string& to_marker)
{
  bufferlist in;
  cls_log_trim_op call;
  call.from_time = from_time;
  call.to_time = to_time;
  call.from_marker = from_marker;
  call.to_marker = to_marker;
  ::encode(call, in);
  op->call("log", "trim", in);
}

int cls_log_trim(Objecter* o, const AVolRef& vol, const oid_t& oid,
		 const ceph::real_time& from_time,
		 const ceph::real_time& to_time,
		 const string& from_marker, const string& to_marker)
{
  bool done = false;

  do {
    ObjectOperation op(vol->op());

    cls_log_trim(op, from_time, to_time, from_marker, to_marker);

    int r = o->mutate(oid, vol, op);
    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;

  } while (!done);


  return 0;
}

class LogListCB {
  list<cls_log_entry> *entries;
  string *marker;
  bool *truncated;
public:
  LogListCB(list<cls_log_entry> *_entries, string *_marker, bool *_truncated) :
    entries(_entries), marker(_marker), truncated(_truncated) {}
  void operator()(int r, bufferlist&& outbl) {
    if (r >= 0) {
      cls_log_list_ret ret;
      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(ret, iter);
	if (entries)
	  *entries = ret.entries;
	if (truncated)
	  *truncated = ret.truncated;
	if (marker)
	  *marker = ret.marker;
      } catch (std::system_error& err) {
	// nothing we can do about it atm
      }
    }
  }
};

void cls_log_list(ObjOpUse op,
		  ceph::real_time& from, ceph::real_time& to,
		  const string& in_marker, int max_entries,
		  list<cls_log_entry>& entries,
		  string *out_marker, bool *truncated)
{
  bufferlist inbl;
  cls_log_list_op call;
  call.from_time = from;
  call.to_time = to;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op->call("log", "list", inbl, LogListCB(&entries, out_marker, truncated));
}

class LogInfoCB {
  cls_log_header *header;
public:
  LogInfoCB(cls_log_header *_header) : header(_header) {}
  void operator()(int r, bufferlist&& outbl) {
    if (r >= 0) {
      cls_log_info_ret ret;
      try {
	bufferlist::iterator iter = outbl.begin();
	::decode(ret, iter);
	if (header)
	  *header = ret.header;
      } catch (std::system_error& err) {
	// nothing we can do about it atm
      }
    }
  }
};

void cls_log_info(ObjOpUse op, cls_log_header *header)
{
  bufferlist inbl;
  cls_log_info_op call;

  ::encode(call, inbl);

  op->call("log", "info", inbl, LogInfoCB(header));
}

