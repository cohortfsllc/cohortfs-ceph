#include "include/CompatSet.h"
TYPE(CompatSet)

#include "include/filepath.h"
TYPE(filepath)

#include "common/snap_types.h"
TYPE(SnapContext)
TYPE(SnapRealmInfo)

#include "common/LogEntry.h"
TYPE(LogEntryKey)
TYPE(LogEntry)
TYPE(LogSummary)

#include "msg/msg_types.h"
TYPE(entity_name_t)
TYPE(entity_addr_t)

#include "osd/OSDMap.h"
TYPE(osd_info_t)
TYPE(osd_xinfo_t)
TYPEWITHSTRAYDATA(OSDMap)
TYPEWITHSTRAYDATA(OSDMap::Incremental)

#include "crush/CrushWrapper.h"
TYPE(CrushWrapper)

#include "osd/PG.h"
TYPE(PG::OndiskLog)

#include "osd/osd_types.h"
TYPE(osd_reqid_t)
TYPE(object_locator_t)
TYPE(pg_t)
TYPE(coll_t)
TYPE(osd_stat_t)
TYPE(OSDSuperblock)
TYPE_FEATUREFUL(pool_snap_info_t)
TYPE_FEATUREFUL(pg_pool_t)
TYPE(object_stat_sum_t)
TYPE(object_stat_collection_t)
TYPE(pg_stat_t)
TYPE_FEATUREFUL(pool_stat_t)
TYPE(pg_history_t)
TYPE(pg_info_t)
TYPE(pg_interval_t)
//TYPE(pg_query_t)
TYPE(pg_log_entry_t)
TYPE(pg_log_t)
TYPE(pg_missing_t::item)
TYPE(pg_missing_t)
TYPE(pg_ls_response_t)
TYPE(pg_create_t)
TYPE(watch_info_t)
TYPE(object_info_t)
TYPE(SnapSet)
TYPE(ObjectRecoveryInfo)
TYPE(ObjectRecoveryProgress)
TYPE(ScrubMap::object)
TYPE(ScrubMap)
TYPE(osd_peer_stat_t)

#include "os/ObjectStore.h"
TYPE(ObjectStore::Transaction)

#include "os/SequencerPosition.h"
TYPE(SequencerPosition)

#include "os/hobject.h"
TYPE(hobject_t)

#include "mon/AuthMonitor.h"
TYPE(AuthMonitor::Incremental)

#include "mon/PGMap.h"
TYPE(PGMap::Incremental)
TYPE(PGMap)

#include "mon/MonMap.h"
TYPE_FEATUREFUL(MonMap)

#include "mon/MonCaps.h"
TYPE(MonCap)
TYPE(MonCaps)

#include "os/DBObjectMap.h"
TYPE(DBObjectMap::_Header)
TYPE(DBObjectMap::State)

#ifdef WITH_RADOSGW

#include "rgw/rgw_rados.h"
TYPE(RGWObjManifestPart);
TYPE(RGWObjManifest);

#include "rgw/rgw_acl.h"
TYPE(ACLPermission)
TYPE(ACLGranteeType)
TYPE(ACLGrant)
TYPE(RGWAccessControlList)
TYPE(ACLOwner)
TYPE(RGWAccessControlPolicy)

#include "rgw/rgw_cache.h"
TYPE(ObjectMetaInfo)
TYPE(ObjectCacheInfo)
TYPE(RGWCacheNotifyInfo)

#include "cls/rgw/cls_rgw_types.h"
TYPE(rgw_bucket_pending_info)
TYPE(rgw_bucket_dir_entry_meta)
TYPE(rgw_bucket_dir_entry)
TYPE(rgw_bucket_category_stats)
TYPE(rgw_bucket_dir_header)
TYPE(rgw_bucket_dir)

#include "cls/rgw/cls_rgw_ops.h"
TYPE(rgw_cls_obj_prepare_op)
TYPE(rgw_cls_obj_complete_op)
TYPE(rgw_cls_list_op)
TYPE(rgw_cls_list_ret)

#include "rgw/rgw_common.h"
TYPE(RGWAccessKey);
TYPE(RGWSubUser);
TYPE(RGWUserInfo)
TYPE(rgw_bucket)
TYPE(RGWBucketInfo)
TYPE(RGWBucketEnt)
TYPE(RGWUploadPartInfo)
TYPE(rgw_obj)

#include "rgw/rgw_log.h"
TYPE(rgw_log_entry)
TYPE(rgw_intent_log_entry)

#include "cls/rbd/cls_rbd.h"
TYPE(cls_rbd_parent)
TYPE(cls_rbd_snap)

#endif

#include "cls/lock/cls_lock_types.h"
TYPE(rados::cls::lock::locker_id_t)
TYPE(rados::cls::lock::locker_info_t)

#include "cls/lock/cls_lock_ops.h"
TYPE(cls_lock_lock_op)
TYPE(cls_lock_unlock_op)
TYPE(cls_lock_break_op)
TYPE(cls_lock_get_info_op)
TYPE(cls_lock_get_info_reply)
TYPE(cls_lock_list_locks_reply)


// --- messages ---
#include "messages/MAuth.h"
MESSAGE(MAuth)
#include "messages/MAuthReply.h"
MESSAGE(MAuthReply)
#include "messages/MCacheExpire.h"
MESSAGE(MCacheExpire)
#include "messages/MClientCapRelease.h"
MESSAGE(MClientCapRelease)
#include "messages/MClientCaps.h"
MESSAGE(MClientCaps)
#include "messages/MClientLease.h"
MESSAGE(MClientLease)
#include "messages/MClientReconnect.h"
MESSAGE(MClientReconnect)
#include "messages/MClientReply.h"
MESSAGE(MClientReply)
#include "messages/MClientRequest.h"
MESSAGE(MClientRequest)
#include "messages/MClientRequestForward.h"
MESSAGE(MClientRequestForward)
#include "messages/MClientSession.h"
MESSAGE(MClientSession)
#include "messages/MClientSnap.h"
MESSAGE(MClientSnap)
#include "messages/MCommand.h"
MESSAGE(MCommand)
#include "messages/MCommandReply.h"
MESSAGE(MCommandReply)
#include "messages/MDentryLink.h"
MESSAGE(MDentryLink)
#include "messages/MDentryUnlink.h"
MESSAGE(MDentryUnlink)
#include "messages/MDirUpdate.h"
MESSAGE(MDirUpdate)
#include "messages/MDiscover.h"
MESSAGE(MDiscover)
#include "messages/MDiscoverReply.h"
MESSAGE(MDiscoverReply)
#include "messages/MExportCaps.h"
MESSAGE(MExportCaps)
#include "messages/MExportCapsAck.h"
MESSAGE(MExportCapsAck)
#include "messages/MExportDir.h"
MESSAGE(MExportDir)
#include "messages/MExportDirAck.h"
MESSAGE(MExportDirAck)
#include "messages/MExportDirCancel.h"
MESSAGE(MExportDirCancel)
#include "messages/MExportDirDiscover.h"
MESSAGE(MExportDirDiscover)
#include "messages/MExportDirDiscoverAck.h"
MESSAGE(MExportDirDiscoverAck)
#include "messages/MExportDirFinish.h"
MESSAGE(MExportDirFinish)
#include "messages/MExportDirNotify.h"
MESSAGE(MExportDirNotify)
#include "messages/MExportDirNotifyAck.h"
MESSAGE(MExportDirNotifyAck)
#include "messages/MExportDirPrep.h"
MESSAGE(MExportDirPrep)
#include "messages/MExportDirPrepAck.h"
MESSAGE(MExportDirPrepAck)
#include "messages/MForward.h"
MESSAGE(MForward)
#include "messages/MGetPoolStats.h"
MESSAGE(MGetPoolStats)
#include "messages/MGetPoolStatsReply.h"
MESSAGE(MGetPoolStatsReply)
#include "messages/MHeartbeat.h"
MESSAGE(MHeartbeat)
#include "messages/MMDSCaps.h"
MESSAGE(MMDSCaps)
#include "messages/MLock.h"
MESSAGE(MLock)
#include "messages/MLog.h"
MESSAGE(MLog)
#include "messages/MLogAck.h"
MESSAGE(MLogAck)
#include "messages/MMDSBeacon.h"
MESSAGE(MMDSBeacon)
#include "messages/MMDSCacheRejoin.h"
MESSAGE(MMDSCacheRejoin)
#include "messages/MMDSFindIno.h"
MESSAGE(MMDSFindIno)
#include "messages/MMDSFindInoReply.h"
MESSAGE(MMDSFindInoReply)
#include "messages/MMDSFragmentNotify.h"
MESSAGE(MMDSFragmentNotify)
#include "messages/MMDSLoadTargets.h"
MESSAGE(MMDSLoadTargets)
#include "messages/MMDSMap.h"
MESSAGE(MMDSMap)
#include "messages/MMDSResolve.h"
MESSAGE(MMDSResolve)
#include "messages/MMDSResolveAck.h"
MESSAGE(MMDSResolveAck)
#include "messages/MMDSSlaveRequest.h"
MESSAGE(MMDSSlaveRequest)
#include "messages/MMDSTableRequest.h"
MESSAGE(MMDSTableRequest)
#include "messages/MMonCommand.h"
MESSAGE(MMonCommand)
#include "messages/MMonCommandAck.h"
MESSAGE(MMonCommandAck)
#include "messages/MMonElection.h"
MESSAGE(MMonElection)
#include "messages/MMonGetMap.h"
MESSAGE(MMonGetMap)
#include "messages/MMonGetVersion.h"
MESSAGE(MMonGetVersion)
#include "messages/MMonGetVersionReply.h"
MESSAGE(MMonGetVersionReply)
#include "messages/MMonGlobalID.h"
MESSAGE(MMonGlobalID)
#include "messages/MMonJoin.h"
MESSAGE(MMonJoin)
#include "messages/MMonMap.h"
MESSAGE(MMonMap)
#include "messages/MMonPaxos.h"
MESSAGE(MMonPaxos)
#include "messages/MMonProbe.h"
MESSAGE(MMonProbe)
#include "messages/MMonSubscribe.h"
MESSAGE(MMonSubscribe)
#include "messages/MMonSubscribeAck.h"
MESSAGE(MMonSubscribeAck)
#include "messages/MOSDAlive.h"
MESSAGE(MOSDAlive)
#include "messages/MOSDBoot.h"
MESSAGE(MOSDBoot)
#include "messages/MOSDFailure.h"
MESSAGE(MOSDFailure)
#include "messages/MOSDMap.h"
MESSAGE(MOSDMap)
#include "messages/MOSDOp.h"
MESSAGE(MOSDOp)
#include "messages/MOSDOpReply.h"
MESSAGE(MOSDOpReply)
#include "messages/MOSDPGBackfill.h"
MESSAGE(MOSDPGBackfill)
#include "messages/MOSDPGCreate.h"
MESSAGE(MOSDPGCreate)
#include "messages/MOSDPGInfo.h"
MESSAGE(MOSDPGInfo)
#include "messages/MOSDPGLog.h"
MESSAGE(MOSDPGLog)
#include "messages/MOSDPGMissing.h"
MESSAGE(MOSDPGMissing)
#include "messages/MOSDPGNotify.h"
MESSAGE(MOSDPGNotify)
#include "messages/MOSDPGQuery.h"
MESSAGE(MOSDPGQuery)
#include "messages/MOSDPGRemove.h"
MESSAGE(MOSDPGRemove)
#include "messages/MOSDPGScan.h"
MESSAGE(MOSDPGScan)
#include "messages/MOSDPGTemp.h"
MESSAGE(MOSDPGTemp)
#include "messages/MOSDPGTrim.h"
MESSAGE(MOSDPGTrim)
#include "messages/MOSDPing.h"
MESSAGE(MOSDPing)
#include "messages/MOSDRepScrub.h"
MESSAGE(MOSDRepScrub)
#include "messages/MOSDScrub.h"
MESSAGE(MOSDScrub)
#include "messages/MOSDSubOp.h"
MESSAGE(MOSDSubOp)
#include "messages/MOSDSubOpReply.h"
MESSAGE(MOSDSubOpReply)
#include "messages/MPGStats.h"
MESSAGE(MPGStats)
#include "messages/MPGStatsAck.h"
MESSAGE(MPGStatsAck)
#include "messages/MPing.h"
MESSAGE(MPing)
#include "messages/MPoolOp.h"
MESSAGE(MPoolOp)
#include "messages/MPoolOpReply.h"
MESSAGE(MPoolOpReply)
#include "messages/MRemoveSnaps.h"
MESSAGE(MRemoveSnaps)
#include "messages/MRoute.h"
MESSAGE(MRoute)
#include "messages/MStatfs.h"
MESSAGE(MStatfs)
#include "messages/MStatfsReply.h"
MESSAGE(MStatfsReply)
#include "messages/MWatchNotify.h"
MESSAGE(MWatchNotify)
