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

OpRequestRef OpRequest::create_request(Message *ref)
{
  OpRequestRef retval(new OpRequest(ref));

  if (ref->get_type() == CEPH_MSG_OSD_OP) {
    retval->reqid = static_cast<MOSDOp*>(ref)->get_reqid();
  }
  return retval;
}

