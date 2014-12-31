// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "OpRequest.h"
#include "common/Formatter.h"
#include <iostream>
#include <vector>
#include <cassert>
#include "common/debug.h"
#include "common/config.h"
#include "msg/Message.h"
#include "messages/MOSDOp.h"
#include "osd/osd_types.h"



OpRequest::OpRequest(Message *req)
  : rmw_flags(0), request(req), hit_flag_points(0), latest_flag_point(0)
{
  Messenger *msgr = req->get_connection()->get_messenger();
  trace.init("op", msgr->get_trace_endpoint(), &req->trace);
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
}

OpRequestRef OpRequest::create_request(Message *ref)
{
  OpRequestRef retval(new OpRequest(ref));

  if (ref->get_type() == CEPH_MSG_OSD_OP) {
    retval->reqid = static_cast<MOSDOp*>(ref)->get_reqid();
  }
  return retval;
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
void OpRequest::set_cache() { rmw_flags |= CEPH_OSD_RMW_FLAG_CACHE; }
