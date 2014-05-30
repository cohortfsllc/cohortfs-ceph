// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "OpRequest.h"
#include "common/Formatter.h"
#include <iostream>
#include <vector>
#include "common/debug.h"
#include "common/config.h"
#include "msg/Message.h"
#include "messages/MOSDOp.h"
#include "include/assert.h"
#include "osd/osd_types.h"



OpRequest::OpRequest(Message *req, OpTracker *tracker) :
  TrackedOp(req, tracker),
  rmw_flags(0),
  hit_flag_points(0), latest_flag_point(0) {
}

void OpRequest::_dump(utime_t now, Formatter *f) const
{
  Message *m = request;
  f->dump_string("flag_point", state_string());
  if (m->get_orig_source().is_client()) {
    f->open_object_section("client_info");
    stringstream client_name;
    client_name << m->get_orig_source();
    f->dump_string("client", client_name.str());
    f->dump_int("tid", m->get_tid());
    f->close_section(); // client_info
  }
  {
    f->open_array_section("events");
    for (list<pair<utime_t, string> >::const_iterator i = events.begin();
	 i != events.end();
	 ++i) {
      f->open_object_section("event");
      f->dump_stream("time") << i->first;
      f->dump_string("event", i->second);
      f->close_section();
    }
    f->close_section();
  }
}

void OpRequest::init_from_message()
{
  if (request->get_type() == CEPH_MSG_OSD_OP) {
    reqid = static_cast<MOSDOp*>(request)->get_reqid();
  }
}

bool OpRequest::check_rmw(int flag) {
  return rmw_flags & flag;
}
bool OpRequest::may_read() { return need_read_cap() || need_class_read_cap(); }
bool OpRequest::may_write() { return need_write_cap() || need_class_write_cap(); }
bool OpRequest::may_cache() { return check_rmw(CEPH_OSD_RMW_FLAG_CACHE); }
bool OpRequest::need_read_cap() {
  return check_rmw(CEPH_OSD_RMW_FLAG_READ);
}
bool OpRequest::need_write_cap() {
  return check_rmw(CEPH_OSD_RMW_FLAG_WRITE);
}
bool OpRequest::need_class_read_cap() {
  return check_rmw(CEPH_OSD_RMW_FLAG_CLASS_READ);
}
bool OpRequest::need_class_write_cap() {
  return check_rmw(CEPH_OSD_RMW_FLAG_CLASS_WRITE);
}
void OpRequest::set_read() { rmw_flags |= CEPH_OSD_RMW_FLAG_READ; }
void OpRequest::set_write() { rmw_flags |= CEPH_OSD_RMW_FLAG_WRITE; }
void OpRequest::set_class_read() { rmw_flags |= CEPH_OSD_RMW_FLAG_CLASS_READ; }
void OpRequest::set_class_write() { rmw_flags |= CEPH_OSD_RMW_FLAG_CLASS_WRITE; }
void OpRequest::set_pg_op() { rmw_flags |= CEPH_OSD_RMW_FLAG_PGOP; }
void OpRequest::set_cache() { rmw_flags |= CEPH_OSD_RMW_FLAG_CACHE; }
