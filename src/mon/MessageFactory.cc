// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MessageFactory.h"

// AuthMonitor
#include "messages/MAuth.h"
#include "messages/MMonGlobalID.h"
// Monitor
#include "messages/MForward.h"
#include "messages/MMonCommand.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonHealth.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MMonProbe.h"
#include "messages/MMonScrub.h"
#include "messages/MMonSubscribe.h"
#include "messages/MMonSync.h"
#include "messages/MMonElection.h"
#include "messages/MPing.h"
#include "messages/MRoute.h"
#include "messages/MTimeCheck.h"
// LogMonitor
#include "messages/MLog.h"
#include "messages/MLogAck.h"
// MDSMonitor
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
// OSDMonitor
#include "messages/MOSDAlive.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"


Message* MonMessageFactory::create(int type)
{
  switch (type) {
  // AuthMonitor
  case CEPH_MSG_AUTH:             return new MAuth;
  case MSG_MON_GLOBAL_ID:         return new MMonGlobalID;
  // Monitor
  case MSG_FORWARD:               return new MForward;
  case MSG_MON_COMMAND:           return new MMonCommand;
  case CEPH_MSG_MON_GET_MAP:      return new MMonGetMap;
  case CEPH_MSG_MON_GET_VERSION:  return new MMonGetVersion;
  case MSG_MON_HEALTH:            return new MMonHealth;
  case MSG_MON_JOIN:              return new MMonJoin;
  case MSG_MON_PAXOS:             return new MMonPaxos;
  case MSG_MON_PROBE:             return new MMonProbe;
  case MSG_MON_SCRUB:             return new MMonScrub;
  case CEPH_MSG_MON_SUBSCRIBE:    return new MMonSubscribe;
  case MSG_MON_SYNC:              return new MMonSync;
  case MSG_MON_ELECTION:          return new MMonElection;
  case CEPH_MSG_PING:             return new MPing;
  case MSG_ROUTE:                 return new MRoute;
  case MSG_TIMECHECK:             return new MTimeCheck;
  // LogMonitor
  case MSG_LOG:                   return new MLog;
  case MSG_LOGACK:                return new MLogAck;
  // MDSMonitor
  case MSG_MDS_BEACON:            return new MMDSBeacon;
  case MSG_MDS_OFFLOAD_TARGETS:   return new MMDSLoadTargets;
  // OSDMonitor
  case MSG_OSD_ALIVE:             return new MOSDAlive;
  case MSG_OSD_BOOT:              return new MOSDBoot;
  case MSG_OSD_FAILURE:           return new MOSDFailure;
  case MSG_OSD_MARK_ME_DOWN:      return new MOSDMarkMeDown;
  default:                        return nullptr;
  }
}
