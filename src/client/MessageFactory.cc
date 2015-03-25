// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MessageFactory.h"

#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientReply.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientSession.h"
#include "messages/MMDSMap.h"


Message* ClientMessageFactory::create(int type)
{
  switch (type) {
  case CEPH_MSG_CLIENT_CAPS:            return new MClientCaps;
  case CEPH_MSG_CLIENT_LEASE:           return new MClientLease;
  case CEPH_MSG_CLIENT_REPLY:           return new MClientReply;
  case CEPH_MSG_CLIENT_REQUEST_FORWARD: return new MClientRequestForward;
  case CEPH_MSG_CLIENT_SESSION:         return new MClientSession;
  case CEPH_MSG_MDS_MAP:                return new MMDSMap(cct);
  default: return parent ? parent->create(type) : nullptr;
  }
}
