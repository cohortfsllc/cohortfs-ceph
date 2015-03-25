// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MessageFactory.h"

// MDS
#include "messages/MClientCapRelease.h"
#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientSession.h"
#include "messages/MHeartbeat.h"
#include "messages/MInodeFileCaps.h"
#include "messages/MLock.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSMap.h"
#include "messages/MMDSSlaveRequest.h"
#include "messages/MMDSTableRequest.h"
#include "messages/MMonCommand.h"
#include "messages/MMonMap.h"
// MDCache
#include "messages/MCacheExpire.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MDentryLink.h"
#include "messages/MDentryUnlink.h"
#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"
#include "messages/MMDSFindIno.h"
#include "messages/MMDSFindInoReply.h"
#include "messages/MMDSFragmentNotify.h"
#include "messages/MMDSOpenIno.h"
#include "messages/MMDSOpenInoReply.h"
#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
// Migrator
#include "messages/MExportCaps.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirFinish.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"


Message* MDSMessageFactory::create(int type)
{
  switch (type) {
  // MDS
  case CEPH_MSG_CLIENT_CAPRELEASE:    return new MClientCapRelease;
  case CEPH_MSG_CLIENT_CAPS:          return new MClientCaps;
  case CEPH_MSG_CLIENT_LEASE:         return new MClientLease;
  case CEPH_MSG_CLIENT_RECONNECT:     return new MClientReconnect;
  case CEPH_MSG_CLIENT_REQUEST:       return new MClientRequest;
  case CEPH_MSG_CLIENT_SESSION:       return new MClientSession;
  case MSG_MDS_HEARTBEAT:             return new MHeartbeat;
  case MSG_MDS_INODEFILECAPS:         return new MInodeFileCaps;
  case MSG_MDS_LOCK:                  return new MLock;
  case MSG_MDS_BEACON:                return new MMDSBeacon;
  case CEPH_MSG_MDS_MAP:              return new MMDSMap(cct);
  case MSG_MDS_SLAVE_REQUEST:         return new MMDSSlaveRequest;
  case MSG_MDS_TABLE_REQUEST:         return new MMDSTableRequest;
  case MSG_MON_COMMAND:               return new MMonCommand;
  case CEPH_MSG_MON_MAP:              return new MMonMap;
  // MDCache
  case MSG_MDS_CACHEEXPIRE:           return new MCacheExpire;
  case MSG_MDS_CACHEREJOIN:           return new MMDSCacheRejoin;
  case MSG_MDS_DENTRYLINK:            return new MDentryLink;
  case MSG_MDS_DENTRYUNLINK:          return new MDentryUnlink;
  case MSG_MDS_DIRUPDATE:             return new MDirUpdate;
  case MSG_MDS_DISCOVER:              return new MDiscover;
  case MSG_MDS_DISCOVERREPLY:         return new MDiscoverReply;
  case MSG_MDS_FINDINO:               return new MMDSFindIno;
  case MSG_MDS_FINDINOREPLY:          return new MMDSFindInoReply;
  case MSG_MDS_FRAGMENTNOTIFY:        return new MMDSFragmentNotify;
  case MSG_MDS_OPENINO:               return new MMDSOpenIno;
  case MSG_MDS_OPENINOREPLY:          return new MMDSOpenInoReply;
  case MSG_MDS_RESOLVE:               return new MMDSResolve;
  case MSG_MDS_RESOLVEACK:            return new MMDSResolveAck;
  // Migrator
  case MSG_MDS_EXPORTCAPS:            return new MExportCaps;
  case MSG_MDS_EXPORTDIR:             return new MExportDir;
  case MSG_MDS_EXPORTDIRACK:          return new MExportDirAck;
  case MSG_MDS_EXPORTDIRCANCEL:       return new MExportDirCancel;
  case MSG_MDS_EXPORTDIRDISCOVER:     return new MExportDirDiscover;
  case MSG_MDS_EXPORTDIRDISCOVERACK:  return new MExportDirDiscoverAck;
  case MSG_MDS_EXPORTDIRFINISH:       return new MExportDirFinish;
  case MSG_MDS_EXPORTDIRNOTIFY:       return new MExportDirNotify;
  case MSG_MDS_EXPORTDIRNOTIFYACK:    return new MExportDirNotifyAck;
  case MSG_MDS_EXPORTDIRPREP:         return new MExportDirPrep;
  case MSG_MDS_EXPORTDIRPREPACK:      return new MExportDirPrepAck;
  default: return parent ? parent->create(type) : nullptr;
  }
}
