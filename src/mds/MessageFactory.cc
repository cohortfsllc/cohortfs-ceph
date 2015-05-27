// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MessageFactory.h"

// MDS
#include "messages/MHeartbeat.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSMap.h"
#include "messages/MMonCommand.h"
#include "messages/MMonMap.h"


Message* MDSMessageFactory::create(int type)
{
  switch (type) {
  // MDS
  case MSG_MDS_HEARTBEAT:             return new MHeartbeat;
  case MSG_MDS_BEACON:                return new MMDSBeacon;
  case CEPH_MSG_MDS_MAP:              return new MMDSMap(cct);
  case MSG_MON_COMMAND:               return new MMonCommand;
  case CEPH_MSG_MON_MAP:              return new MMonMap;
  default: return parent ? parent->create(type) : nullptr;
  }
}
