// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <fstream>
#include <iostream>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>
#include <boost/scoped_ptr.hpp>

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN || __FreeBSD__

#include "pg/PG.h"
#include "pg/ReplicatedPG.h"

#include "include/types.h"
#include "include/compat.h"

#include "OSD.h"
#include "OSDMap.h"
#include "Watch.h"

#include "common/ceph_argparse.h"
#include "os/FileStore.h"
#include "os/FileJournal.h"

#include "Ager.h"


#include "msg/Messenger.h"
#include "msg/Message.h"

#include "mon/MonClient.h"

#include "messages/MLog.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDPGTemp.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGMissing.h"

#include "messages/MOSDAlive.h"

#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"

#include "messages/MMonCommand.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MWatchNotify.h"

#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/LogClient.h"
#include "common/safe_io.h"
#include "common/HeartbeatMap.h"
#include "common/admin_socket.h"

#include "global/signal_handler.h"
#include "global/pidfile.h"

#include "include/color.h"
#include "perfglue/cpu_profiler.h"
#include "perfglue/heap_profiler.h"

#include "osd/ClassHandler.h"
#include "osd/OpRequest.h"

#include "auth/AuthAuthorizeHandler.h"

#include "common/errno.h"

#include "objclass/objclass.h"

#include "include/assert.h"
#include "common/config.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(*_dout, whoami, get_osdmap())

static ostream& _prefix(std::ostream* _dout, int whoami, OSDMapRef osdmap) {
  return *_dout << "osd." << whoami << " "
		<< (osdmap ? osdmap->get_epoch():0)
		<< " ";
}

const coll_t coll_t::META_COLL("meta");

static CompatSet get_osd_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_OLOC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BIGINFO);
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

OSDService::OSDService(OSD *osd) :
  osd(osd),
  whoami(osd->whoami), store(osd->store), clog(osd->clog),
  cluster_messenger(osd->cluster_messenger),
  client_messenger(osd->client_messenger),
  logger(osd->logger),
  monc(osd->monc),
  class_handler(osd->class_handler),
  publish_lock("OSDService::publish_lock"),
  sched_scrub_lock("OSDService::sched_scrub_lock"), scrubs_pending(0),
  scrubs_active(0),
  watch_lock("OSD::watch_lock"),
  watch_timer(osd->client_messenger->cct, watch_lock),
  watch(NULL),
  last_tid(0),
  tid_lock("OSDService::tid_lock"),
  map_cache_lock("OSDService::map_lock"),
  map_cache(g_conf->osd_map_cache_size),
  map_bl_cache(g_conf->osd_map_cache_size),
  map_bl_inc_cache(g_conf->osd_map_cache_size)
{
  // empty
}

void OSDService::need_heartbeat_peer_update()
{
  osd->need_heartbeat_peer_update();
}

ObjectStore *OSD::create_object_store(const std::string &dev, const std::string &jdev)
{
  struct stat st;
  if (::stat(dev.c_str(), &st) != 0)
    return 0;

  if (g_conf->filestore)
    return new FileStore(dev, jdev);

  if (S_ISDIR(st.st_mode))
    return new FileStore(dev, jdev);
  else
    return 0;
}

#undef dout_prefix
#define dout_prefix *_dout

int OSD::convert_collection(ObjectStore *store, coll_t cid)
{
  coll_t tmp0("convertfs_temp");
  coll_t tmp1("convertfs_temp1");
  vector<hobject_t> objects;

  map<string, bufferptr> aset;
  int r = store->collection_getattrs(cid, aset);
  if (r < 0)
    return r;

  {
    ObjectStore::Transaction t;
    t.create_collection(tmp0);
    for (map<string, bufferptr>::iterator i = aset.begin();
	 i != aset.end();
	 ++i) {
      bufferlist val;
      val.push_back(i->second);
      t.collection_setattr(tmp0, i->first, val);
    }
    store->apply_transaction(t);
  }

  hobject_t next;
  while (!next.is_max()) {
    objects.clear();
    hobject_t start = next;
    r = store->collection_list_partial(cid, start,
				       200, 300, 0,
				       &objects, &next);
    if (r < 0)
      return r;

    ObjectStore::Transaction t;
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      t.collection_add(tmp0, cid, *i);
    }
    store->apply_transaction(t);
  }

  {
    ObjectStore::Transaction t;
    t.collection_rename(cid, tmp1);
    t.collection_rename(tmp0, cid);
    store->apply_transaction(t);
  }

  clear_temp(store, tmp1);
  store->sync_and_flush();
  store->sync();
  return 0;
}

int OSD::do_convertfs(ObjectStore *store)
{
  int r = store->mount();
  if (r < 0)
    return r;

  uint32_t version;
  r = store->version_stamp_is_valid(&version);
  if (r < 0)
    return r;
  if (r == 1)
    return store->umount();

  derr << "FileStore is old at version " << version << ".  Updating..."  << dendl;

  derr << "Removing tmp pgs" << dendl;
  vector<coll_t> collections;
  r = store->list_collections(collections);
  if (r < 0)
    return r;
  for (vector<coll_t>::iterator i = collections.begin();
       i != collections.end();
       ++i) {
    pg_t pgid;
    if (i->is_temp(pgid))
      clear_temp(store, *i);
    else if (i->to_str() == "convertfs_temp" ||
	     i->to_str() == "convertfs_temp1")
      clear_temp(store, *i);
  }
  store->flush();


  derr << "Getting collections" << dendl;

  derr << collections.size() << " to process." << dendl;
  collections.clear();
  r = store->list_collections(collections);
  if (r < 0)
    return r;
  int processed = 0;
  for (vector<coll_t>::iterator i = collections.begin();
       i != collections.end();
       ++i, ++processed) {
    derr << processed << "/" << collections.size() << " processed" << dendl;
    uint32_t collection_version;
    r = store->collection_version_current(*i, &collection_version);
    if (r < 0) {
      return r;
    } else if (r == 1) {
      derr << "Collection " << *i << " is up to date" << dendl;
    } else {
      derr << "Updating collection " << *i << " current version is " 
	   << collection_version << dendl;
      r = convert_collection(store, *i);
      if (r < 0)
	return r;
      derr << "collection " << *i << " updated" << dendl;
    }
  }
  derr << "All collections up to date, updating version stamp..." << dendl;
  r = store->update_version_stamp();
  if (r < 0)
    return r;
  store->sync_and_flush();
  store->sync();
  derr << "Version stamp updated, done with upgrade!" << dendl;
  return store->umount();
}

int OSD::convertfs(const std::string &dev, const std::string &jdev)
{
  boost::scoped_ptr<ObjectStore> store(
    new FileStore(dev, jdev, "filestore", 
		  true));
  int r = do_convertfs(store.get());
  return r;
}

int OSD::mkfs(const std::string &dev, const std::string &jdev, uuid_d fsid, int whoami)
{
  int ret;
  ObjectStore *store = NULL;

  try {
    store = create_object_store(dev, jdev);
    if (!store) {
      ret = -ENOENT;
      goto out;
    }

    // if we are fed a uuid for this osd, use it.
    store->set_fsid(g_conf->osd_uuid);

    ret = store->mkfs();
    if (ret) {
      derr << "OSD::mkfs: FileStore::mkfs failed with error " << ret << dendl;
      goto free_store;
    }

    ret = store->mount();
    if (ret) {
      derr << "OSD::mkfs: couldn't mount FileStore: error " << ret << dendl;
      goto free_store;
    }

    // age?
    if (g_conf->osd_age_time != 0) {
      if (g_conf->osd_age_time >= 0) {
	dout(0) << "aging..." << dendl;
	Ager ager(store);
	ager.age(g_conf->osd_age_time,
		 g_conf->osd_age,
		 g_conf->osd_age - .05,
		 50000,
		 g_conf->osd_age - .05);
      }
    }

    OSDSuperblock sb;
    bufferlist sbbl;
    ret = store->read(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, 0, sbbl);
    if (ret >= 0) {
      dout(0) << " have superblock" << dendl;
      if (whoami != sb.whoami) {
	derr << "provided osd id " << whoami << " != superblock's " << sb.whoami << dendl;
	ret = -EINVAL;
	goto umount_store;
      }
      if (fsid != sb.cluster_fsid) {
	derr << "provided cluster fsid " << fsid << " != superblock's " << sb.cluster_fsid << dendl;
	ret = -EINVAL;
	goto umount_store;
      }
    } else {
      // create superblock
      if (fsid.is_zero()) {
	derr << "must specify cluster fsid" << dendl;
	ret = -EINVAL;
	goto umount_store;
      }

      sb.cluster_fsid = fsid;
      sb.osd_fsid = store->get_fsid();
      sb.whoami = whoami;
      sb.compat_features = get_osd_compat_set();

      // benchmark?
      if (g_conf->osd_auto_weight) {
	bufferlist bl;
	bufferptr bp(1048576);
	bp.zero();
	bl.push_back(bp);
	dout(0) << "testing disk bandwidth..." << dendl;
	utime_t start = ceph_clock_now(g_ceph_context);
	object_t oid("disk_bw_test");
	for (int i=0; i<1000; i++) {
	  ObjectStore::Transaction *t = new ObjectStore::Transaction;
	  t->write(coll_t::META_COLL, hobject_t(sobject_t(oid, 0)), i*bl.length(), bl.length(), bl);
	  store->queue_transaction(NULL, t);
	}
	store->sync();
	utime_t end = ceph_clock_now(g_ceph_context);
	end -= start;
	dout(0) << "measured " << (1000.0 / (double)end) << " mb/sec" << dendl;
	ObjectStore::Transaction tr;
	tr.remove(coll_t::META_COLL, hobject_t(sobject_t(oid, 0)));
	ret = store->apply_transaction(tr);
	if (ret) {
	  derr << "OSD::mkfs: error while benchmarking: apply_transaction returned "
	       << ret << dendl;
	  goto umount_store;
	}
	
	// set osd weight
	sb.weight = (1000.0 / (double)end);
      }

      bufferlist bl;
      ::encode(sb, bl);

      ObjectStore::Transaction t;
      t.create_collection(coll_t::META_COLL);
      t.write(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
      ret = store->apply_transaction(t);
      if (ret) {
	derr << "OSD::mkfs: error while writing OSD_SUPERBLOCK_POBJECT: "
	     << "apply_transaction returned " << ret << dendl;
	goto umount_store;
      }
    }

    store->sync_and_flush();

    ret = write_meta(dev, sb.cluster_fsid, sb.osd_fsid, whoami);
    if (ret) {
      derr << "OSD::mkfs: failed to write fsid file: error " << ret << dendl;
      goto umount_store;
    }

    ret = write_meta(dev, "ready", "ready\n", 6);
    if (ret) {
      derr << "OSD::mkfs: failed to write ready file: error " << ret << dendl;
      goto umount_store;
    }

  }
  catch (const std::exception &se) {
    derr << "OSD::mkfs: caught exception " << se.what() << dendl;
    ret = 1000;
  }
  catch (...) {
    derr << "OSD::mkfs: caught unknown exception." << dendl;
    ret = 1000;
  }

umount_store:
  store->umount();
free_store:
  delete store;
out:
  return ret;
}

int OSD::mkjournal(const std::string &dev, const std::string &jdev)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  return store->mkjournal();
}

int OSD::flushjournal(const std::string &dev, const std::string &jdev)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  int err = store->mount();
  if (!err) {
    store->sync_and_flush();
    store->umount();
  }
  delete store;
  return err;
}

int OSD::dump_journal(const std::string &dev, const std::string &jdev, ostream& out)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  int err = store->dump_journal(out);
  delete store;
  return err;
}

int OSD::write_meta(const std::string &base, const std::string &file,
		    const char *val, size_t vallen)
{
  int ret;
  char fn[PATH_MAX];
  char tmp[PATH_MAX];
  int fd;

  // does the file already have correct content?
  char oldval[80];
  ret = read_meta(base, file, oldval, sizeof(oldval));
  if (ret == (int)vallen && memcmp(oldval, val, vallen) == 0)
    return 0;  // yes.

  snprintf(fn, sizeof(fn), "%s/%s", base.c_str(), file.c_str());
  snprintf(tmp, sizeof(tmp), "%s/%s.tmp", base.c_str(), file.c_str());
  fd = ::open(tmp, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    ret = errno;
    derr << "write_meta: error opening '" << tmp << "': "
	 << cpp_strerror(ret) << dendl;
    return -ret;
  }
  ret = safe_write(fd, val, vallen);
  if (ret) {
    derr << "write_meta: failed to write to '" << tmp << "': "
	 << cpp_strerror(ret) << dendl;
    TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }

  ret = ::fsync(fd);
  TEMP_FAILURE_RETRY(::close(fd));
  if (ret) {
    ::unlink(tmp);
    derr << "write_meta: failed to fsync to '" << tmp << "': "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }
  ret = ::rename(tmp, fn);
  if (ret) {
    ::unlink(tmp);
    derr << "write_meta: failed to rename '" << tmp << "' to '" << fn << "': "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }

  fd = ::open(base.c_str(), O_RDONLY);
  if (fd < 0) {
    ret = errno;
    derr << "write_meta: failed to open dir '" << base << "': "
	 << cpp_strerror(ret) << dendl;
    return -ret;
  }
  ::fsync(fd);
  TEMP_FAILURE_RETRY(::close(fd));

  return 0;
}

int OSD::read_meta(const  std::string &base, const std::string &file,
		   char *val, size_t vallen)
{
  char fn[PATH_MAX];
  int fd, len;

  snprintf(fn, sizeof(fn), "%s/%s", base.c_str(), file.c_str());
  fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    return -err;
  }
  len = safe_read(fd, val, vallen);
  if (len < 0) {
    TEMP_FAILURE_RETRY(::close(fd));
    return len;
  }
  // close sometimes returns errors, but only after write()
  TEMP_FAILURE_RETRY(::close(fd));

  val[len] = 0;
  return len;
}

int OSD::write_meta(const std::string &base, uuid_d& cluster_fsid, uuid_d& osd_fsid, int whoami)
{
  char val[80];
  
  snprintf(val, sizeof(val), "%s\n", CEPH_OSD_ONDISK_MAGIC);
  write_meta(base, "magic", val, strlen(val));

  snprintf(val, sizeof(val), "%d\n", whoami);
  write_meta(base, "whoami", val, strlen(val));

  cluster_fsid.print(val);
  strcat(val, "\n");
  write_meta(base, "ceph_fsid", val, strlen(val));

  return 0;
}

int OSD::peek_meta(const std::string &dev, std::string& magic,
		   uuid_d& cluster_fsid, uuid_d& osd_fsid, int& whoami)
{
  char val[80] = { 0 };

  if (read_meta(dev, "magic", val, sizeof(val)) < 0)
    return -errno;
  int l = strlen(val);
  if (l && val[l-1] == '\n')
    val[l-1] = 0;
  magic = val;

  if (read_meta(dev, "whoami", val, sizeof(val)) < 0)
    return -errno;
  whoami = atoi(val);

  if (read_meta(dev, "ceph_fsid", val, sizeof(val)) < 0)
    return -errno;
  if (strlen(val) > 36)
    val[36] = 0;
  cluster_fsid.parse(val);

  if (read_meta(dev, "fsid", val, sizeof(val)) < 0)
    osd_fsid = uuid_d();
  else {
    if (strlen(val) > 36)
      val[36] = 0;
    osd_fsid.parse(val);
  }

  return 0;
}

int OSD::peek_journal_fsid(string path, uuid_d& fsid)
{
  FileJournal j(fsid, 0, 0, path.c_str());
  return j.peek_fsid(fsid);
}


#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, osdmap)

// cons/des

OSD::OSD(int id, Messenger *internal_messenger, Messenger *external_messenger,
	 Messenger *hbclientm, Messenger *hbserverm, MonClient *mc,
	 const std::string &dev, const std::string &jdev, OSDService* osdSvc) :
  Dispatcher(external_messenger->cct),
  osd_lock("OSD::osd_lock"),
  timer(external_messenger->cct, osd_lock),
  authorize_handler_registry(new AuthAuthorizeHandlerRegistry(external_messenger->cct)),
  cluster_messenger(internal_messenger),
  client_messenger(external_messenger),
  monc(mc),
  logger(NULL),
  store(NULL),
  clog(external_messenger->cct, client_messenger, &mc->monmap, LogClient::NO_FLAGS),
  whoami(id),
  dev_path(dev), journal_path(jdev),
  dispatch_running(false),
  osd_compat(get_osd_compat_set()),
  state(STATE_BOOTING), boot_epoch(0), up_epoch(0), bind_epoch(0),
  op_tp(external_messenger->cct, "OSD::op_tp", g_conf->osd_op_threads),
  recovery_tp(external_messenger->cct, "OSD::recovery_tp", g_conf->osd_recovery_threads),
  disk_tp(external_messenger->cct, "OSD::disk_tp", g_conf->osd_disk_threads),
  command_tp(external_messenger->cct, "OSD::command_tp", 1),
  heartbeat_lock("OSD::heartbeat_lock"),
  heartbeat_stop(false), heartbeat_need_update(true), heartbeat_epoch(0),
  hbclient_messenger(hbclientm),
  hbserver_messenger(hbserverm),
  heartbeat_thread(this),
  heartbeat_dispatcher(this),
  stat_lock("OSD::stat_lock"),
  finished_lock("OSD::finished_lock"),
  admin_ops_hook(NULL),
  historic_ops_hook(NULL),
  map_lock("OSD::map_lock"),
  peer_map_epoch_lock("OSD::peer_map_epoch_lock"),
  up_thru_wanted(0), up_thru_pending(0),
  command_wq(this, g_conf->osd_command_thread_timeout, &command_tp),
  remove_wq(store, g_conf->osd_remove_thread_timeout, &disk_tp),
  serviceRef(shared_ptr<OSDService>(osdSvc)),
  osd_stat_updated(false)
{
  monc->set_messenger(client_messenger);
}

OSD::~OSD()
{
  delete authorize_handler_registry;
  delete class_handler;
  g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
  delete store;
}

void cls_initialize(ClassHandler *ch);

void OSD::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sys_siglist[signum] << " ***" << dendl;
  //suicide(128 + signum);
  suicide(0);
}

int OSD::pre_init()
{
  Mutex::Locker lock(osd_lock);
  
  assert(!store);
  store = create_object_store(dev_path, journal_path);
  if (!store) {
    derr << "OSD::pre_init: unable to create object store" << dendl;
    return -ENODEV;
  }

  if (store->test_mount_in_use()) {
    derr << "OSD::pre_init: object store '" << dev_path << "' is "
         << "currently in use. (Is ceph-osd already running?)" << dendl;
    return -EBUSY;
  }
  return 0;
}

class HistoricOpsSocketHook : public AdminSocketHook {
  OSD *osd;
public:
  HistoricOpsSocketHook(OSD *o) : osd(o) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    osd->dump_historic_ops(ss);
    out.append(ss);
    return true;
  }
};


class OpsFlightSocketHook : public AdminSocketHook {
  OSD *osd;
public:
  OpsFlightSocketHook(OSD *o) : osd(o) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    osd->dump_ops_in_flight(ss);
    out.append(ss);
    return true;
  }
};

int OSD::init_super()
{
  Mutex::Locker lock(osd_lock);

  timer.init();
  serviceRef->watch_timer.init();
  serviceRef->watch = new Watch();

  // mount.
  dout(2) << "mounting " << dev_path << " "
	  << (journal_path.empty() ? "(no journal)" : journal_path) << dendl;
  assert(store);  // call pre_init() first!

  int r = store->mount();
  if (r < 0) {
    derr << "OSD:init: unable to mount object store" << dendl;
    return r;
  }

  dout(2) << "boot" << dendl;

  // read superblock
  r = read_superblock();
  if (r < 0) {
    derr << "OSD::init() : unable to read osd superblock" << dendl;
    store->umount();
    delete store;
    return -EINVAL;
  }
  if (osd_compat.compare(superblock.compat_features) != 0) {
    // We need to persist the new compat_set before we
    // do anything else
    dout(5) << "Upgrading superblock compat_set" << dendl;
    superblock.compat_features = osd_compat;
    ObjectStore::Transaction t;
    write_superblock(t);
    r = store->apply_transaction(t);
    if (r < 0)
      return r;
  }
  serviceRef->publish_superblock(superblock);

  class_handler = new ClassHandler();
  cls_initialize(class_handler);

  // load up "current" osdmap
  assert_warn(!osdmap);
  if (osdmap) {
    derr << "OSD::init: unable to read current osdmap" << dendl;
    return -EINVAL;
  }
  osdmap = get_map(superblock.current_epoch);
  serviceRef->publish_map(osdmap);

  bind_epoch = osdmap->get_epoch();

  dout(2) << "superblock: i am osd." << superblock.whoami << dendl;
  assert_warn(whoami == superblock.whoami);
  if (whoami != superblock.whoami) {
    derr << "OSD::init: logic error: superblock says osd"
	 << superblock.whoami << " but i am osd." << whoami << dendl;
    return -EINVAL;
  }

  create_logger();
    
  // i'm ready!
  client_messenger->add_dispatcher_head(this);
  cluster_messenger->add_dispatcher_head(this);

  hbclient_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hbserver_messenger->add_dispatcher_head(&heartbeat_dispatcher);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  r = monc->init();
  if (r < 0)
    return r;

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&clog);

  osd_lock.Unlock();

  r = monc->authenticate();
  if (r < 0) {
    monc->shutdown();
    store->umount();
    osd_lock.Lock(); // locker is going to unlock this on function exit
    return r;
  }

  monc->wait_auth_rotating(30.0);

  osd_lock.Lock();

  op_tp.start();
  recovery_tp.start();
  disk_tp.start();
  command_tp.start();

  // start the heartbeat
  heartbeat_thread.create();

  // tick
  timer.add_event_after(g_conf->osd_heartbeat_interval, new C_Tick(this));

#if 0
  int ret = monc->start_auth_rotating(ename, KEY_ROTATE_TIME);
  if (ret < 0) {
    dout(0) << "could not start rotating keys, err=" << ret << dendl;
    return ret;
  }
  dout(0) << "started rotating keys" << dendl;
#endif

  admin_ops_hook = new OpsFlightSocketHook(this);
  AdminSocket *admin_socket = cct->get_admin_socket();
  r = admin_socket->register_command("dump_ops_in_flight", admin_ops_hook,
                                         "show the ops currently in flight");
  historic_ops_hook = new HistoricOpsSocketHook(this);
  r = admin_socket->register_command("dump_historic_ops", historic_ops_hook,
                                         "show slowest recent ops");
  assert(r == 0);

  return 0;
}

void OSD::create_logger()
{
  dout(10) << "create_logger" << dendl;

  PerfCountersBuilder osd_plb(g_ceph_context, "osd", l_osd_first, l_osd_last);

  osd_plb.add_u64(l_osd_opq, "opq");       // op queue length (waiting to be processed yet)
  osd_plb.add_u64(l_osd_op_wip, "op_wip");   // rep ops currently being processed (primary)

  osd_plb.add_u64_counter(l_osd_op,       "op");           // client ops
  osd_plb.add_u64_counter(l_osd_op_inb,   "op_in_bytes");       // client op in bytes (writes)
  osd_plb.add_u64_counter(l_osd_op_outb,  "op_out_bytes");      // client op out bytes (reads)
  osd_plb.add_fl_avg(l_osd_op_lat,   "op_latency");       // client op latency

  osd_plb.add_u64_counter(l_osd_op_r,      "op_r");        // client reads
  osd_plb.add_u64_counter(l_osd_op_r_outb, "op_r_out_bytes");   // client read out bytes
  osd_plb.add_fl_avg(l_osd_op_r_lat,  "op_r_latency");    // client read latency
  osd_plb.add_u64_counter(l_osd_op_w,      "op_w");        // client writes
  osd_plb.add_u64_counter(l_osd_op_w_inb,  "op_w_in_bytes");    // client write in bytes
  osd_plb.add_fl_avg(l_osd_op_w_rlat, "op_w_rlat");   // client write readable/applied latency
  osd_plb.add_fl_avg(l_osd_op_w_lat,  "op_w_latency");    // client write latency
  osd_plb.add_u64_counter(l_osd_op_rw,     "op_rw");       // client rmw
  osd_plb.add_u64_counter(l_osd_op_rw_inb, "op_rw_in_bytes");   // client rmw in bytes
  osd_plb.add_u64_counter(l_osd_op_rw_outb,"op_rw_out_bytes");  // client rmw out bytes
  osd_plb.add_fl_avg(l_osd_op_rw_rlat,"op_rw_rlat");  // client rmw readable/applied latency
  osd_plb.add_fl_avg(l_osd_op_rw_lat, "op_rw_latency");   // client rmw latency

  osd_plb.add_u64_counter(l_osd_sop,       "subop");         // subops
  osd_plb.add_u64_counter(l_osd_sop_inb,   "subop_in_bytes");     // subop in bytes
  osd_plb.add_fl_avg(l_osd_sop_lat,   "subop_latency");     // subop latency

  osd_plb.add_u64_counter(l_osd_sop_w,     "subop_w");          // replicated (client) writes
  osd_plb.add_u64_counter(l_osd_sop_w_inb, "subop_w_in_bytes");      // replicated write in bytes
  osd_plb.add_fl_avg(l_osd_sop_w_lat, "subop_w_latency");      // replicated write latency
  osd_plb.add_u64_counter(l_osd_sop_pull,     "subop_pull");       // pull request
  osd_plb.add_fl_avg(l_osd_sop_pull_lat, "subop_pull_latency");
  osd_plb.add_u64_counter(l_osd_sop_push,     "subop_push");       // push (write)
  osd_plb.add_u64_counter(l_osd_sop_push_inb, "subop_push_in_bytes");
  osd_plb.add_fl_avg(l_osd_sop_push_lat, "subop_push_latency");

  osd_plb.add_u64_counter(l_osd_pull,      "pull");       // pull requests sent
  osd_plb.add_u64_counter(l_osd_push,      "push");       // push messages
  osd_plb.add_u64_counter(l_osd_push_outb, "push_out_bytes");  // pushed bytes

  osd_plb.add_u64_counter(l_osd_rop, "recovery_ops");       // recovery ops (started)

  osd_plb.add_fl(l_osd_loadavg, "loadavg");
  osd_plb.add_u64(l_osd_buf, "buffer_bytes");       // total ceph::buffer bytes

  osd_plb.add_u64(l_osd_pg, "numpg");   // num pgs
  osd_plb.add_u64(l_osd_pg_primary, "numpg_primary"); // num primary pgs
  osd_plb.add_u64(l_osd_pg_replica, "numpg_replica"); // num replica pgs
  osd_plb.add_u64(l_osd_pg_stray, "numpg_stray");   // num stray pgs
  osd_plb.add_u64(l_osd_hb_to, "heartbeat_to_peers");     // heartbeat peers we send to
  osd_plb.add_u64(l_osd_hb_from, "heartbeat_from_peers"); // heartbeat peers we recv from
  osd_plb.add_u64_counter(l_osd_map, "map_messages");           // osdmap messages
  osd_plb.add_u64_counter(l_osd_mape, "map_message_epochs");         // osdmap epochs
  osd_plb.add_u64_counter(l_osd_mape_dup, "map_message_epoch_dups"); // dup osdmap epochs

  logger = osd_plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void OSD::suicide(int exitcode)
{
  if (g_conf->filestore_blackhole) {
    derr << " filestore_blackhole=true, doing abbreviated shutdown" << dendl;
    _exit(exitcode);
  }

  derr << " pausing thread pools" << dendl;
  op_tp.pause();
  disk_tp.pause();
  recovery_tp.pause();
  command_tp.pause();

  derr << " flushing io" << dendl;
  store->sync_and_flush();

  derr << " removing pid file" << dendl;
  pidfile_remove();

  derr << " exit" << dendl;
  exit(exitcode);
}

int OSD::shutdown_super()
{
  g_ceph_context->_conf->set_val("debug_osd", "100");
  g_ceph_context->_conf->set_val("debug_journal", "100");
  g_ceph_context->_conf->set_val("debug_filestore", "100");
  g_ceph_context->_conf->set_val("debug_ms", "100");
  g_ceph_context->_conf->apply_changes(NULL);
  
  derr << "shutdown" << dendl;

  state = STATE_STOPPING;

  timer.shutdown();

  serviceRef->watch_lock.Lock();
  serviceRef->watch_timer.shutdown();
  serviceRef->watch_lock.Unlock();

  heartbeat_lock.Lock();
  heartbeat_stop = true;
  heartbeat_cond.Signal();
  heartbeat_lock.Unlock();
  heartbeat_thread.join();

  command_tp.stop();

  cct->get_admin_socket()->unregister_command("dump_ops_in_flight");
  delete admin_ops_hook;
  delete historic_ops_hook;
  admin_ops_hook = NULL;
  historic_ops_hook = NULL;

  recovery_tp.stop();
  dout(10) << "recovery tp stopped" << dendl;
  op_tp.stop();
  dout(10) << "op tp stopped" << dendl;

  // pause _new_ disk work first (to avoid racing with thread pool),
  disk_tp.pause_new();
  dout(10) << "disk tp paused (new), kicking all pgs" << dendl;

  osd_lock.Unlock();
  store->sync();
  store->flush();
  osd_lock.Lock();

  // zap waiters (bleh, this is messy)
  finished_lock.Lock();
  finished.clear();
  finished_lock.Unlock();

  // note unmount epoch
  dout(10) << "noting clean unmount in epoch " << osdmap->get_epoch() << dendl;
  superblock.mounted = boot_epoch;
  superblock.clean_thru = osdmap->get_epoch();
  ObjectStore::Transaction t;
  write_superblock(t);
  int r = store->apply_transaction(t);
  if (r) {
    derr << "OSD::shutdown: error writing superblock: "
	 << cpp_strerror(r) << dendl;
  }

  // flush data to disk
  osd_lock.Unlock();
  dout(10) << "sync" << dendl;
  store->sync();
  r = store->umount();
  delete store;
  store = 0;
  dout(10) << "sync done" << dendl;
  osd_lock.Lock();


  client_messenger->shutdown();
  cluster_messenger->shutdown();
  hbclient_messenger->shutdown();
  hbserver_messenger->shutdown();

  monc->shutdown();

  delete serviceRef->watch;

  return r;
}

void OSD::write_superblock(ObjectStore::Transaction& t)
{
  dout(10) << "write_superblock " << superblock << dendl;

  //hack: at minimum it's using the baseline feature set
  if (!superblock.compat_features.incompat.mask |
      CEPH_OSD_FEATURE_INCOMPAT_BASE.id)
    superblock.compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);

  bufferlist bl;
  ::encode(superblock, bl);
  t.write(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, 0, bl);
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  ::decode(superblock, p);

  dout(10) << "read_superblock " << superblock << dendl;
  if (osd_compat.compare(superblock.compat_features) < 0) {
    derr << "The disk uses features unsupported by the executable." << dendl;
    derr << " ondisk features " << superblock.compat_features << dendl;
    derr << " daemon features " << osd_compat << dendl;

    if (osd_compat.writeable(superblock.compat_features)) {
      derr << "it is still writeable, though. Missing features:" << dendl;
      CompatSet diff = osd_compat.unsupported(superblock.compat_features);
      return -EOPNOTSUPP;
    }
    else {
      derr << "Cannot write to disk! Missing features:" << dendl;
      CompatSet diff = osd_compat.unsupported(superblock.compat_features);
      return -EOPNOTSUPP;
    }
  }

  if (whoami != superblock.whoami) {
    derr << "read_superblock superblock says osd." << superblock.whoami
         << ", but i (think i) am osd." << whoami << dendl;
    return -1;
  }
  
  return 0;
}



void OSD::clear_temp(ObjectStore *store, coll_t tmp)
{
  vector<hobject_t> objects;
  store->collection_list(tmp, objects);

  // delete them.
  ObjectStore::Transaction t;
  unsigned removed = 0;
  for (vector<hobject_t>::iterator p = objects.begin();
       p != objects.end();
       p++, removed++) {
    t.collection_remove(tmp, *p);
    if (removed > 300) {
      int r = store->apply_transaction(t);
      assert(r == 0);
      t = ObjectStore::Transaction();
      removed = 0;
    }
  }
  t.remove_collection(tmp);
  int r = store->apply_transaction(t);
  assert(r == 0);
  store->sync_and_flush();
}


// -------------------------------------

void OSD::update_osd_stat()
{
  // fill in osd stats too
  struct statfs stbuf;
  store->statfs(&stbuf);

  osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
  osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
  osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;

  osd_stat.hb_in.clear();
  for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin(); p != heartbeat_peers.end(); p++)
    osd_stat.hb_in.push_back(p->first);
  osd_stat.hb_out.clear();

  dout(20) << "update_osd_stat " << osd_stat << dendl;
}

void OSD::_add_heartbeat_peer(int p)
{
  if (p == whoami)
    return;
  HeartbeatInfo *hi;

  map<int,HeartbeatInfo>::iterator i = heartbeat_peers.find(p);
  if (i == heartbeat_peers.end()) {
    hi = &heartbeat_peers[p];
    hi->con = hbclient_messenger->get_connection(osdmap->get_hb_inst(p));
    dout(10) << "_add_heartbeat_peer: new peer osd." << p
	     << " " << hi->con->get_peer_addr() << dendl;
  } else {
    hi = &i->second;
  }
  hi->epoch = osdmap->get_epoch();
}

void OSD::need_heartbeat_peer_update()
{
  heartbeat_lock.Lock();
  dout(20) << "need_heartbeat_peer_update" << dendl;
  heartbeat_need_update = true;
  heartbeat_lock.Unlock();
}

void OSD::maybe_update_heartbeat_peers()
{
  assert(osd_lock.is_locked());
  Mutex::Locker l(heartbeat_lock);

  if (!heartbeat_need_update)
    return;
  heartbeat_need_update = false;

  heartbeat_epoch = osdmap->get_epoch();

  build_heartbeat_peers_list();

  map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
  while (p != heartbeat_peers.end()) {
    if (p->second.epoch < osdmap->get_epoch()) {
      dout(20) << " removing heartbeat peer osd." << p->first
	       << " " << p->second.con->get_peer_addr()
	       << dendl;
      hbclient_messenger->mark_down(p->second.con);
      p->second.con->put();
      heartbeat_peers.erase(p++);
    } else {
      ++p;
    }
  }
  dout(10) << "maybe_update_heartbeat_peers " << heartbeat_peers.size() << " peers" << dendl;
}

void OSD::reset_heartbeat_peers()
{
  dout(10) << "reset_heartbeat_peers" << dendl;
  heartbeat_lock.Lock();
  while (!heartbeat_peers.empty()) {
    hbclient_messenger->mark_down(heartbeat_peers.begin()->second.con);
    heartbeat_peers.begin()->second.con->put();
    heartbeat_peers.erase(heartbeat_peers.begin());
  }
  failure_queue.clear();
  heartbeat_lock.Unlock();
}

void OSD::handle_osd_ping(MOSDPing *m)
{
  if (superblock.cluster_fsid != m->fsid) {
    dout(20) << "handle_osd_ping from " << m->get_source_inst()
	     << " bad fsid " << m->fsid << " != " << superblock.cluster_fsid << dendl;
    m->put();
    return;
  }

  int from = m->get_source().num();

  heartbeat_lock.Lock();

  OSDMapRef curmap = serviceRef->get_osdmap();
  
  switch (m->op) {

  case MOSDPing::PING:
    {
      if (g_conf->osd_debug_drop_ping_probability > 0) {
	if (debug_heartbeat_drops_remaining.count(from)) {
	  if (debug_heartbeat_drops_remaining[from] == 0) {
	    debug_heartbeat_drops_remaining.erase(from);
	  } else {
	    debug_heartbeat_drops_remaining[from]--;
	    dout(5) << "Dropping heartbeat from " << from
		    << ", " << debug_heartbeat_drops_remaining[from]
		    << " remaining to drop" << dendl;
	    break;
	  }
	} else if (g_conf->osd_debug_drop_ping_probability >
	           ((((double)(rand()%100))/100.0))) {
	  debug_heartbeat_drops_remaining[from] =
	    g_conf->osd_debug_drop_ping_duration;
	  dout(5) << "Dropping heartbeat from " << from
		  << ", " << debug_heartbeat_drops_remaining[from]
		  << " remaining to drop" << dendl;
	  break;
	}
      }
      Message *r = new MOSDPing(monc->get_fsid(),
				curmap->get_epoch(),
				MOSDPing::PING_REPLY,
				m->stamp);
      hbserver_messenger->send_message(r, m->get_connection());

      if (curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active())
	  _share_map_outgoing(curmap->get_cluster_inst(from));
      }
    }
    break;

  case MOSDPing::PING_REPLY:
    {
      map<int,HeartbeatInfo>::iterator i = heartbeat_peers.find(from);
      if (i != heartbeat_peers.end()) {
	dout(25) << "handle_osd_ping got reply from osd." << from
		 << " first_rx " << i->second.first_tx
		 << " last_tx " << i->second.last_tx
		 << " last_rx " << i->second.last_rx << " -> " << m->stamp
		 << dendl;
	i->second.last_rx = m->stamp;
      }

      if (m->map_epoch &&
	  curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active())
	  _share_map_outgoing(curmap->get_cluster_inst(from));
      }

      // Cancel false reports
      if (failure_queue.count(from))
	failure_queue.erase(from);
      if (failure_pending.count(from))
	send_still_alive(curmap->get_epoch(), failure_pending[from]);
    }
    break;

  case MOSDPing::YOU_DIED:
    dout(10) << "handle_osd_ping " << m->get_source_inst() << " says i am down in " << m->map_epoch
	     << dendl;
    monc->sub_want("osdmap", m->map_epoch, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
    break;
  }

  heartbeat_lock.Unlock();
  m->put();
}

void OSD::heartbeat_entry()
{
  heartbeat_lock.Lock();
  while (!heartbeat_stop) {
    heartbeat();

    double wait = .5 + ((float)(rand() % 10)/10.0) * (float)g_conf->osd_heartbeat_interval;
    utime_t w;
    w.set_from_double(wait);
    dout(30) << "heartbeat_entry sleeping for " << wait << dendl;
    heartbeat_cond.WaitInterval(g_ceph_context, heartbeat_lock, w);
    dout(30) << "heartbeat_entry woke up" << dendl;
  }
  heartbeat_lock.Unlock();
}

void OSD::heartbeat_check()
{
  assert(heartbeat_lock.is_locked());

  // check for incoming heartbeats (move me elsewhere?)
  utime_t cutoff = ceph_clock_now(g_ceph_context);
  cutoff -= g_conf->osd_heartbeat_grace;
  for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
       p != heartbeat_peers.end();
       p++) {
    dout(25) << "heartbeat_check osd." << p->first
	     << " first_rx " << p->second.first_tx
	     << " last_tx " << p->second.last_tx
	     << " last_rx " << p->second.last_rx
	     << dendl;
    if (p->second.last_rx == utime_t()) {
      if (p->second.last_tx == utime_t() ||
	  p->second.last_tx > cutoff)
	continue;  // just started sending recently
      derr << "heartbeat_check: no reply from osd." << p->first
	   << " ever, first ping sent " << p->second.first_tx
	   << " (cutoff " << cutoff << ")" << dendl;
    } else {
      if (p->second.last_rx > cutoff)
	continue;  // got recent reply
      derr << "heartbeat_check: no reply from osd." << p->first
	   << " since " << p->second.last_rx
	   << " (cutoff " << cutoff << ")" << dendl;
    }

    // fail!
    queue_failure(p->first);
  }
}

void OSD::heartbeat()
{
  dout(30) << "heartbeat" << dendl;

  // get CPU load avg
  double loadavgs[1];
  if (getloadavg(loadavgs, 1) == 1)
    logger->fset(l_osd_loadavg, loadavgs[0]);

  dout(30) << "heartbeat checking stats" << dendl;

  // refresh stats?
  {
    Mutex::Locker lock(stat_lock);
    update_osd_stat();
  }

  dout(5) << "heartbeat: " << osd_stat << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);

  // send heartbeats
  for (map<int,HeartbeatInfo>::iterator i = heartbeat_peers.begin();
       i != heartbeat_peers.end();
       i++) {
    int peer = i->first;
    dout(30) << "heartbeat allocating ping for osd." << peer << dendl;
    Message *m = new MOSDPing(monc->get_fsid(),
			      serviceRef->get_osdmap()->get_epoch(),
			      MOSDPing::PING,
			      now);
    i->second.last_tx = now;
    if (i->second.first_tx == utime_t())
      i->second.first_tx = now;
    dout(30) << "heartbeat sending ping to osd." << peer << dendl;
    hbclient_messenger->send_message(m, i->second.con);
  }

  dout(30) << "heartbeat check" << dendl;
  heartbeat_check();

  if (logger) {
    logger->set(l_osd_hb_to, heartbeat_peers.size());
    logger->set(l_osd_hb_from, 0);
  }
  
  // hmm.. am i all alone?
  dout(30) << "heartbeat lonely?" << dendl;
  if (heartbeat_peers.empty()) {
    if (now - last_mon_heartbeat > g_conf->osd_mon_heartbeat_interval && is_active()) {
      last_mon_heartbeat = now;
      dout(10) << "i have no heartbeat peers; checking mon for new map" << dendl;
      monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
      monc->renew_subs();
    }
  }

  dout(30) << "heartbeat done" << dendl;
}


// =========================================

void OSD::tick()
{
  assert(osd_lock.is_locked());
  dout(5) << "tick" << dendl;

  logger->set(l_osd_buf, buffer::get_total_alloc());

  // periodically kick recovery work queue
  recovery_tp.wake();
  
  if (serviceRef->scrub_should_schedule()) {
    sched_scrub();
  }

  map_lock.get_read();

  maybe_update_heartbeat_peers();

  heartbeat_lock.Lock();
  heartbeat_check();
  heartbeat_lock.Unlock();

  check_replay_queue();

  // mon report?
  utime_t now = ceph_clock_now(g_ceph_context);
  if (now - last_stats_sent > g_conf->osd_mon_report_interval_max) {
    osd_stat_updated = true;
    do_mon_report();
  }
  else if (now - last_mon_report > g_conf->osd_mon_report_interval_min) {
    do_mon_report();
  }

  map_lock.put_read();

  timer.add_event_after(1.0, new C_Tick(this));

  tick_sub();


  // only do waiters if dispatch() isn't currently running.  (if it is,
  // it'll do the waiters, and doing them here may screw up ordering
  // of op_queue vs handle_osd_map.)
  if (!dispatch_running) {
    dispatch_running = true;
    do_waiters();
    dispatch_running = false;
    dispatch_cond.Signal();
  }

  check_ops_in_flight();
}

void OSD::check_ops_in_flight()
{
  vector<string> warnings;
  if (op_tracker.check_ops_in_flight(warnings)) {
    for (vector<string>::iterator i = warnings.begin();
        i != warnings.end();
        ++i) {
      clog.warn() << *i;
    }
  }
  return;
}

void OSD::dump_ops_in_flight(ostream& ss)
{
  op_tracker.dump_ops_in_flight(ss);
}

// =========================================
void OSD::RemoveWQ::_process(boost::tuple<coll_t, SequencerRef, DeletingStateRef> *item)
{
  coll_t &coll = item->get<0>();
  ObjectStore::Sequencer *osr = item->get<1>().get();
  if (osr)
    osr->flush();
  vector<hobject_t> olist;
  store->collection_list(coll, olist);
  //*_dout << "OSD::RemoveWQ::_process removing coll " << coll << std::endl;
  uint64_t num = 1;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<hobject_t>::iterator i = olist.begin();
       i != olist.end();
       ++i, ++num) {
    if (num % 20 == 0) {
      store->queue_transaction(
	osr, t,
	new ObjectStore::C_DeleteTransactionHolder<SequencerRef>(t, item->get<1>()),
	new ContainerContext<SequencerRef>(item->get<1>()));
      t = new ObjectStore::Transaction;
    }
    t->remove(coll, *i);
  }
  t->remove_collection(coll);
  store->queue_transaction(
    osr, t,
    new ObjectStore::C_DeleteTransactionHolder<SequencerRef>(t, item->get<1>()),
    new ContainerContext<SequencerRef>(item->get<1>()));
  delete item;
}
// =========================================

void OSD::do_mon_report()
{
  dout(7) << "do_mon_report" << dendl;

  utime_t now(ceph_clock_now(g_ceph_context));
  last_mon_report = now;

  // do any pending reports
  send_alive();
  do_mon_report_sub();
  send_failures();
}

void OSD::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    Mutex::Locker l(osd_lock);
    dout(10) << "ms_handle_connect on mon" << dendl;
    if (is_booting()) {
      start_boot();
    } else {
      send_alive();
      send_failures();

      ms_handle_connect_sub(con);
      
      monc->renew_subs();
    }
  }
}

void OSD::complete_notify(void *_notif, void *_obc)
{
  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)_obc;
  Watch::Notification *notif = (Watch::Notification *)_notif;
  dout(10) << "got the last reply from pending watchers, can send response now" << dendl;
  MWatchNotify *reply = notif->reply;
  client_messenger->send_message(reply, notif->session->con);
  notif->session->put();
  notif->session->con->put();
  serviceRef->watch->remove_notification(notif);
  if (notif->timeout)
    serviceRef->watch_timer.cancel_event(notif->timeout);
  map<Watch::Notification *, bool>::iterator iter = obc->notifs.find(notif);
  if (iter != obc->notifs.end())
    obc->notifs.erase(iter);
  delete notif;
}

void OSD::disconnect_session_watches(Session *session)
{
  // get any watched obc's
  map<ReplicatedPG::ObjectContext *, pg_t> obcs;
  serviceRef->watch_lock.Lock();
  for (map<void *, pg_t>::iterator iter = session->watches.begin(); iter != session->watches.end(); ++iter) {
    ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)iter->first;
    obcs[obc] = iter->second;
  }
  serviceRef->watch_lock.Unlock();

  for (map<ReplicatedPG::ObjectContext *, pg_t>::iterator oiter = obcs.begin(); oiter != obcs.end(); ++oiter) {
    ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)oiter->first;
    dout(10) << "obc=" << (void *)obc << dendl;

    ReplicatedPG *pg = static_cast<ReplicatedPG *>(lookup_lock_raw_pg(oiter->second));
    assert(pg);
    serviceRef->watch_lock.Lock();
    /* NOTE! fix this one, should be able to just lookup entity name,
       however, we currently only keep EntityName on the session and not
       entity_name_t. */
    map<entity_name_t, Session *>::iterator witer = obc->watchers.begin();
    while (1) {
      while (witer != obc->watchers.end() && witer->second == session) {
        dout(10) << "removing watching session entity_name=" << session->entity_name
		<< " from " << obc->obs.oi << dendl;
	entity_name_t entity = witer->first;
	watch_info_t& w = obc->obs.oi.watchers[entity];
	utime_t expire = ceph_clock_now(g_ceph_context);
	expire += w.timeout_seconds;
	pg->register_unconnected_watcher(obc, entity, expire);
	dout(10) << " disconnected watch " << w << " by " << entity << " session " << session
		 << ", expires " << expire << dendl;
        obc->watchers.erase(witer++);
	pg->put_object_context(obc);
	session->put();
      }
      if (witer == obc->watchers.end())
        break;
      ++witer;
    }
    serviceRef->watch_lock.Unlock();
    pg->unlock();
  }
}

bool OSD::ms_handle_reset(Connection *con)
{
  dout(1) << "OSD::ms_handle_reset()" << dendl;
  OSD::Session *session = (OSD::Session *)con->get_priv();
  if (!session)
    return false;
  ms_handle_reset_sub(session);
  session->put();
  return true;
}

void OSD::handle_notify_timeout(void *_notif)
{
  assert(serviceRef->watch_lock.is_locked());
  Watch::Notification *notif = (Watch::Notification *)_notif;
  dout(10) << "OSD::handle_notify_timeout notif " << notif->id << dendl;

  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)notif->obc;

  complete_notify(_notif, obc);
  serviceRef->watch_lock.Unlock(); /* drop lock to change locking order */

  put_object_context(obc, notif->pgid);
  serviceRef->watch_lock.Lock();
  /* exiting with watch_lock held */
}

struct C_OSD_GetVersion : public Context {
  OSD *osd;
  uint64_t oldest, newest;
  C_OSD_GetVersion(OSD *o) : osd(o), oldest(0), newest(0) {}
  void finish(int r) {
    if (r >= 0)
      osd->_maybe_boot(oldest, newest);
  }
};

void OSD::start_boot()
{
  dout(10) << "start_boot - have maps " << superblock.oldest_map
	   << ".." << superblock.newest_map << dendl;
  C_OSD_GetVersion *c = new C_OSD_GetVersion(this);
  monc->get_version("osdmap", &c->newest, &c->oldest, c);
}

void OSD::_maybe_boot(epoch_t oldest, epoch_t newest)
{
  Mutex::Locker l(osd_lock);
  dout(10) << "_maybe_boot mon has osdmaps " << oldest << ".." << newest << dendl;

  // if our map within recent history, try to add ourselves to the osdmap.
  if (osdmap->test_flag(CEPH_OSDMAP_NOUP)) {
    dout(5) << "osdmap NOUP flag is set, waiting for it to clear" << dendl;
  } else if (!g_ceph_context->get_heartbeat_map()->is_healthy()) {
    dout(1) << "internal heartbeats indicate we are not healthy; waiting to boot" << dendl;
  } else if (osdmap->get_epoch() >= oldest - 1 &&
	     osdmap->get_epoch() < newest + g_conf->osd_map_message_max) {
    _send_boot();
    return;
  }
  
  // get all the latest maps
  if (osdmap->get_epoch() > oldest)
    monc->sub_want("osdmap", osdmap->get_epoch(), CEPH_SUBSCRIBE_ONETIME);
  else
    monc->sub_want("osdmap", oldest - 1, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();
}

void OSD::_send_boot()
{
  dout(10) << "_send_boot" << dendl;
  entity_addr_t cluster_addr = cluster_messenger->get_myaddr();
  if (cluster_addr.is_blank_ip()) {
    int port = cluster_addr.get_port();
    cluster_addr = client_messenger->get_myaddr();
    cluster_addr.set_port(port);
    cluster_messenger->set_addr_unknowns(cluster_addr);
    dout(10) << " assuming cluster_addr ip matches client_addr" << dendl;
  }
  entity_addr_t hb_addr = hbserver_messenger->get_myaddr();
  if (hb_addr.is_blank_ip()) {
    int port = hb_addr.get_port();
    hb_addr = cluster_addr;
    hb_addr.set_port(port);
    hbserver_messenger->set_addr_unknowns(hb_addr);
    dout(10) << " assuming hb_addr ip matches cluster_addr" << dendl;
  }
  MOSDBoot *mboot = new MOSDBoot(superblock, hb_addr, cluster_addr);
  dout(10) << " client_addr " << client_messenger->get_myaddr()
	   << ", cluster_addr " << cluster_addr
	   << ", hb addr " << hb_addr
	   << dendl;
  monc->send_mon_message(mboot);
}

void OSD::queue_want_up_thru(epoch_t want)
{
  map_lock.get_read();
  epoch_t cur = osdmap->get_up_thru(whoami);
  if (want > up_thru_wanted) {
    dout(10) << "queue_want_up_thru now " << want << " (was " << up_thru_wanted << ")" 
	     << ", currently " << cur
	     << dendl;
    up_thru_wanted = want;

    // expedite, a bit.  WARNING this will somewhat delay other mon queries.
    last_mon_report = ceph_clock_now(g_ceph_context);
    send_alive();
  } else {
    dout(10) << "queue_want_up_thru want " << want << " <= queued " << up_thru_wanted 
	     << ", currently " << cur
	     << dendl;
  }
  map_lock.put_read();
}

void OSD::send_alive()
{
  if (!osdmap->exists(whoami))
    return;
  epoch_t up_thru = osdmap->get_up_thru(whoami);
  dout(10) << "send_alive up_thru currently " << up_thru << " want " << up_thru_wanted << dendl;
  if (up_thru_wanted > up_thru) {
    up_thru_pending = up_thru_wanted;
    dout(10) << "send_alive want " << up_thru_wanted << dendl;
    monc->send_mon_message(new MOSDAlive(osdmap->get_epoch(), up_thru_wanted));
  }
}

void OSD::send_failures()
{
  bool locked = false;
  if (!failure_queue.empty()) {
    heartbeat_lock.Lock();
    locked = true;
  }
  while (!failure_queue.empty()) {
    int osd = *failure_queue.begin();
    entity_inst_t i = osdmap->get_inst(osd);
    monc->send_mon_message(new MOSDFailure(monc->get_fsid(), i, osdmap->get_epoch()));
    failure_pending[osd] = i;
    failure_queue.erase(osd);
  }
  if (locked) heartbeat_lock.Unlock();
}

void OSD::send_still_alive(epoch_t epoch, entity_inst_t i)
{
  MOSDFailure *m = new MOSDFailure(monc->get_fsid(), i, epoch);
  m->is_failed = false;
  monc->send_mon_message(m);
}

void OSD::send_pg_stats(const utime_t &now)
{
  assert(osd_lock.is_locked());

  dout(20) << "send_pg_stats" << dendl;

  stat_lock.Lock();
  osd_stat_t cur_stat = osd_stat;
  stat_lock.Unlock();
   
  pg_stat_queue_lock.Lock();

  if (osd_stat_updated || !pg_stat_queue.empty()) {
    last_stats_sent = now;
    osd_stat_updated = false;

    dout(10) << "send_pg_stats - " << pg_stat_queue.size() << " pgs updated" << dendl;

    utime_t had_for(now);
    had_for -= had_map_since;

    MPGStats *m = new MPGStats(monc->get_fsid(), osdmap->get_epoch(), had_for);
    m->set_tid(++pg_stat_tid);
    m->osd_stat = cur_stat;

    xlist<PG*>::iterator p = pg_stat_queue.begin();
    while (!p.end()) {
      PG *pg = *p;
      ++p;
      if (!pg->is_primary()) {  // we hold map_lock; role is stable.
	pg->stat_queue_item.remove_myself();
	pg->put();
	continue;
      }
      pg->pg_stats_lock.Lock();
      if (pg->pg_stats_valid) {
	m->pg_stat[pg->info.pgid] = pg->pg_stats_stable;
	dout(25) << " sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
      } else {
	dout(25) << " NOT sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << ", not valid" << dendl;
      }
      pg->pg_stats_lock.Unlock();
    }

    if (!outstanding_pg_stats) {
      outstanding_pg_stats = true;
      last_pg_stats_ack = ceph_clock_now(g_ceph_context);
    }
    monc->send_mon_message(m);
  }

  pg_stat_queue_lock.Unlock();
}

void OSD::handle_pg_stats_ack(MPGStatsAck *ack)
{
  dout(10) << "handle_pg_stats_ack " << dendl;

  if (!require_mon_peer(ack)) {
    ack->put();
    return;
  }

  last_stats_ack = ceph_clock_now(g_ceph_context);

  pg_stat_queue_lock.Lock();

  if (ack->get_tid() > pg_stat_tid_flushed) {
    pg_stat_tid_flushed = ack->get_tid();
    pg_stat_queue_cond.Signal();
  }

  xlist<PG*>::iterator p = pg_stat_queue.begin();
  while (!p.end()) {
    PG *pg = *p;
    ++p;

    if (ack->pg_stat.count(pg->info.pgid)) {
      eversion_t acked = ack->pg_stat[pg->info.pgid];
      pg->pg_stats_lock.Lock();
      if (acked == pg->pg_stats_stable.reported) {
	dout(25) << " ack on " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
	pg->stat_queue_item.remove_myself();
	pg->put();
      } else {
	dout(25) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported
		 << " > acked " << acked << dendl;
      }
      pg->pg_stats_lock.Unlock();
    } else
      dout(30) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
  }
  
  if (!pg_stat_queue.size()) {
    outstanding_pg_stats = false;
  }

  pg_stat_queue_lock.Unlock();

  ack->put();
}

void OSD::flush_pg_stats()
{
  dout(10) << "flush_pg_stats" << dendl;
  utime_t now = ceph_clock_now(cct);
  send_pg_stats(now);

  osd_lock.Unlock();

  pg_stat_queue_lock.Lock();
  uint64_t tid = pg_stat_tid;
  dout(10) << "flush_pg_stats waiting for stats tid " << tid << " to flush" << dendl;
  while (tid > pg_stat_tid_flushed)
    pg_stat_queue_cond.Wait(pg_stat_queue_lock);
  dout(10) << "flush_pg_stats finished waiting for stats tid " << tid << " to flush" << dendl;
  pg_stat_queue_lock.Unlock();

  osd_lock.Lock();
}


void OSD::handle_command(MMonCommand *m)
{
  if (!require_mon_peer(m))
    return;

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), NULL);
  command_wq.queue(c);
  m->put();
}

void OSD::handle_command(MCommand *m)
{
  Connection *con = m->get_connection();
  Session *session = (Session *)con->get_priv();
  if (!session) {
    client_messenger->send_message(new MCommandReply(m, -EPERM), con);
    m->put();
    return;
  }

  OSDCap& caps = session->caps;
  session->put();

  if (!caps.allow_all() || m->get_source().is_mon()) {
    client_messenger->send_message(new MCommandReply(m, -EPERM), con);
    m->put();
    return;
  }

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), con);
  command_wq.queue(c);

  m->put();
}

void OSD::do_command(Connection *con, tid_t tid, vector<string>& cmd, bufferlist& data)
{
  int r = 0;
  ostringstream ss;
  bufferlist odata;

  dout(20) << "do_command tid " << tid << " " << cmd << dendl;

  if (cmd[0] == "injectargs") {
    if (cmd.size() < 2) {
      r = -EINVAL;
      ss << "ignoring empty injectargs";
      goto out;
    }
    osd_lock.Unlock();
    g_conf->injectargs(cmd[1], &ss);
    osd_lock.Lock();
  }

  else if (cmd[0] == "pg") {
    pg_t pgid;

    if (cmd.size() < 2) {
      ss << "no pgid specified";
      r = -EINVAL;
    } else if (!pgid.parse(cmd[1].c_str())) {
      ss << "couldn't parse pgid '" << cmd[1] << "'";
      r = -EINVAL;
    } else {
      PG *pg = _lookup_lock_pg(pgid);
      if (!pg) {
	ss << "i don't have pgid " << pgid;
	r = -ENOENT;
      } else {
	cmd.erase(cmd.begin(), cmd.begin() + 2);
	r = pg->do_command(cmd, ss, data, odata);
	pg->unlock();
      }
    }
  }

  else if (cmd[0] == "bench") {
    uint64_t count = 1 << 30;  // 1gb
    uint64_t bsize = 4 << 20;
    if (cmd.size() > 1)
      bsize = atoll(cmd[1].c_str());
    if (cmd.size() > 2)
      count = atoll(cmd[2].c_str());
    
    bufferlist bl;
    bufferptr bp(bsize);
    bp.zero();
    bl.push_back(bp);

    ObjectStore::Transaction *cleanupt = new ObjectStore::Transaction;

    store->sync_and_flush();
    utime_t start = ceph_clock_now(g_ceph_context);
    for (uint64_t pos = 0; pos < count; pos += bsize) {
      char nm[30];
      snprintf(nm, sizeof(nm), "disk_bw_test_%lld", (long long)pos);
      object_t oid(nm);
      hobject_t soid(sobject_t(oid, 0));
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      t->write(coll_t::META_COLL, soid, 0, bsize, bl);
      store->queue_transaction(NULL, t);
      cleanupt->remove(coll_t::META_COLL, soid);
    }
    store->sync_and_flush();
    utime_t end = ceph_clock_now(g_ceph_context);

    // clean up
    store->queue_transaction(NULL, cleanupt);

    uint64_t rate = (double)count / (end - start);
    ss << "bench: wrote " << prettybyte_t(count)
       << " in blocks of " << prettybyte_t(bsize) << " in "
       << (end-start) << " sec at " << prettybyte_t(rate) << "/sec";
  }

  else if (cmd.size() >= 1 && cmd[0] == "flush_pg_stats") {
    flush_pg_stats();
  }
  
  else if (cmd[0] == "heap") {
    if (ceph_using_tcmalloc()) {
      ceph_heap_profiler_handle_command(cmd, clog);
    } else {
      r = -EOPNOTSUPP;
      ss << "could not issue heap profiler command -- not using tcmalloc!";
    }
  }

  else if (cmd.size() > 1 && cmd[0] == "debug") {
    if (cmd.size() == 3 && cmd[1] == "dump_missing") {
      const string &file_name(cmd[2]);
      std::ofstream fout(file_name.c_str());
      if (!fout.is_open()) {
	ss << "failed to open file '" << file_name << "'";
	r = -EINVAL;
	goto out;
      }

      std::set <pg_t> keys;
      for (hash_map<pg_t, PG*>::const_iterator pg_map_e = pg_map.begin();
	   pg_map_e != pg_map.end(); ++pg_map_e) {
	keys.insert(pg_map_e->first);
      }

      fout << "*** osd " << whoami << ": dump_missing ***" << std::endl;
      for (std::set <pg_t>::iterator p = keys.begin();
	   p != keys.end(); ++p) {
	hash_map<pg_t, PG*>::iterator q = pg_map.find(*p);
	assert(q != pg_map.end());
	PG *pg = q->second;
	pg->lock();

	fout << *pg << std::endl;
	std::map<hobject_t, pg_missing_t::item>::iterator mend = pg->missing.missing.end();
	std::map<hobject_t, pg_missing_t::item>::iterator mi = pg->missing.missing.begin();
	for (; mi != mend; ++mi) {
	  fout << mi->first << " -> " << mi->second << std::endl;
	  map<hobject_t, set<int> >::const_iterator mli =
	    pg->missing_loc.find(mi->first);
	  if (mli == pg->missing_loc.end())
	    continue;
	  const set<int> &mls(mli->second);
	  if (mls.empty())
	    continue;
	  fout << "missing_loc: " << mls << std::endl;
	}
	pg->unlock();
	fout << std::endl;
      }

      fout.close();
    }
    else if (cmd.size() == 3 && cmd[1] == "kick_recovery_wq") {
      r = g_conf->set_val("osd_recovery_delay_start", cmd[2].c_str());
      if (r != 0) {
	ss << "kick_recovery_wq: error setting "
	   << "osd_recovery_delay_start to '" << cmd[2] << "': error "
	   << r;
	goto out;
      }
      g_conf->apply_changes(NULL);
      ss << "kicking recovery queue. set osd_recovery_delay_start "
	 << "to " << g_conf->osd_recovery_delay_start;
      defer_recovery_until = ceph_clock_now(g_ceph_context);
      defer_recovery_until += g_conf->osd_recovery_delay_start;
      recovery_wq.wake();
    }
  }

  else if (cmd[0] == "cpu_profiler") {
    cpu_profiler_handle_command(cmd, clog);
  }

  else if (cmd[0] == "dump_pg_recovery_stats") {
    stringstream s;
    pg_recovery_stats.dump(s);
    ss << "dump pg recovery stats: " << s.str();
  }

  else if (cmd[0] == "reset_pg_recovery_stats") {
    ss << "reset pg recovery stats";
    pg_recovery_stats.reset();
  }

  else {
    ss << "unrecognized command! " << cmd;
    r = -EINVAL;
  }

 out:
  string rs = ss.str();
  dout(0) << "do_command r=" << r << " " << rs << dendl;
  clog.info() << rs << "\n";
  if (con) {
    MCommandReply *reply = new MCommandReply(r, rs);
    reply->set_tid(tid);
    reply->set_data(odata);
    client_messenger->send_message(reply, con);
  }
  return;
}




// --------------------------------------
// dispatch

epoch_t OSD::get_peer_epoch(int peer)
{
  Mutex::Locker l(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p == peer_map_epoch.end())
    return 0;
  return p->second;
}

epoch_t OSD::note_peer_epoch(int peer, epoch_t e)
{
  Mutex::Locker l(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p != peer_map_epoch.end()) {
    if (p->second < e) {
      dout(10) << "note_peer_epoch osd." << peer << " has " << e << dendl;
      p->second = e;
    } else {
      dout(30) << "note_peer_epoch osd." << peer << " has " << p->second << " >= " << e << dendl;
    }
    return p->second;
  } else {
    dout(10) << "note_peer_epoch osd." << peer << " now has " << e << dendl;
    peer_map_epoch[peer] = e;
    return e;
  }
}
 
void OSD::forget_peer_epoch(int peer, epoch_t as_of) 
{
  Mutex::Locker l(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p != peer_map_epoch.end()) {
    if (p->second <= as_of) {
      dout(10) << "forget_peer_epoch osd." << peer << " as_of " << as_of
	       << " had " << p->second << dendl;
      peer_map_epoch.erase(p);
    } else {
      dout(10) << "forget_peer_epoch osd." << peer << " as_of " << as_of
	       << " has " << p->second << " - not forgetting" << dendl;
    }
  }
}


bool OSD::_share_map_incoming(const entity_inst_t& inst, epoch_t epoch,
			      Session* session)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << inst << " " << epoch << dendl;
  //assert(osd_lock.is_locked());

  assert(is_active());

  // does client have old map?
  if (inst.name.is_client()) {
    bool sendmap = epoch < osdmap->get_epoch();
    if (sendmap && session) {
      if ( session->last_sent_epoch < osdmap->get_epoch() ) {
	session->last_sent_epoch = osdmap->get_epoch();
      }
      else {
	sendmap = false; //we don't need to send it out again
	dout(15) << inst.name << " already sent incremental to update from epoch "<< epoch << dendl;
      }
    }
    if (sendmap) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, inst);
      shared = true;
    }
  }

  // does peer have old map?
  if (inst.name.is_osd() &&
      osdmap->is_up(inst.name.num()) &&
      (osdmap->get_cluster_inst(inst.name.num()) == inst ||
       osdmap->get_hb_inst(inst.name.num()) == inst)) {
    // remember
    epoch_t has = note_peer_epoch(inst.name.num(), epoch);

    // share?
    if (has < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      note_peer_epoch(inst.name.num(), osdmap->get_epoch());
      send_incremental_map(epoch, osdmap->get_cluster_inst(inst.name.num()));
      shared = true;
    }
  }

  if (session)
    session->put();
  return shared;
}


void OSD::_share_map_outgoing(const entity_inst_t& inst,
			      OSDMapRef map)
{
  if (!map)
    map = serviceRef->get_osdmap();
  assert(inst.name.is_osd());

  int peer = inst.name.num();

  // send map?
  epoch_t pe = get_peer_epoch(peer);
  if (pe) {
    if (pe < map->get_epoch()) {
      send_incremental_map(pe, inst);
      note_peer_epoch(peer, map->get_epoch());
    } else
      dout(20) << "_share_map_outgoing " << inst << " already has epoch " << pe << dendl;
  } else {
    dout(20) << "_share_map_outgoing " << inst << " don't know epoch, doing nothing" << dendl;
    // no idea about peer's epoch.
    // ??? send recent ???
    // do nothing.
  }
}


bool OSD::heartbeat_dispatch(Message *m)
{
  dout(30) << "heartbeat_dispatch " << m << dendl;

  switch (m->get_type()) {
    
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source_inst() << dendl;
    m->put();
    break;

  case MSG_OSD_PING:
    handle_osd_ping((MOSDPing*)m);
    break;

  default:
    return false;
  }

  return true;
}

bool OSD::ms_dispatch(Message *m)
{
  // lock!
  osd_lock.Lock();
  while (dispatch_running) {
    dout(10) << "ms_dispatch waiting for other dispatch thread to complete" << dendl;
    dispatch_cond.Wait(osd_lock);
  }
  dispatch_running = true;

  do_waiters();
  _dispatch(m);
  do_waiters();

  dispatch_running = false;
  dispatch_cond.Signal();

  osd_lock.Unlock();
  return true;
}

bool OSD::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "OSD::ms_get_authorizer type=" << ceph_entity_type_name(dest_type) << dendl;

  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    /* the MonClient checks keys every tick(), so we should just wait for that cycle
       to get through */
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

bool OSD::ms_verify_authorizer(Connection *con, int peer_type,
			       int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			       bool& isvalid)
{
  AuthAuthorizeHandler *authorize_handler = authorize_handler_registry->get_handler(protocol);
  if (!authorize_handler) {
    dout(0) << "No AuthAuthorizeHandler found for protocol " << protocol << dendl;
    isvalid = false;
    return true;
  }

  AuthCapsInfo caps_info;
  EntityName name;
  uint64_t global_id;
  uint64_t auid = CEPH_AUTH_UID_DEFAULT;

  isvalid = authorize_handler->verify_authorizer(g_ceph_context, monc->rotating_secrets,
						 authorizer_data, authorizer_reply, name, global_id, caps_info, &auid);

  dout(10) << "OSD::ms_verify_authorizer name=" << name << " auid=" << auid << dendl;

  if (isvalid) {
    Session *s = (Session *)con->get_priv();
    if (!s) {
      s = new Session;
      con->set_priv(s->get());
      s->con = con;
      dout(10) << " new session " << s << " con=" << s->con << " addr=" << s->con->get_peer_addr() << dendl;
    }

    s->entity_name = name;
    if (caps_info.allow_all)
      s->caps.set_allow_all();
    s->auid = auid;
 
    if (caps_info.caps.length() > 0) {
      bufferlist::iterator p = caps_info.caps.begin();
      string str;
      try {
	::decode(str, p);
      }
      catch (buffer::error& e) {
      }
      bool success = s->caps.parse(str);
      if (success)
	dout(10) << " session " << s << " " << s->entity_name << " has caps " << s->caps << " '" << str << "'" << dendl;
      else
	dout(10) << " session " << s << " " << s->entity_name << " failed to parse caps '" << str << "'" << dendl;
    }
    
    s->put();
  }
  return true;
};


void OSD::do_waiters()
{
  assert(osd_lock.is_locked());
  
  finished_lock.Lock();
  if (finished.empty()) {
    finished_lock.Unlock();
  } else {
    list<OpRequestRef> waiting;
    waiting.splice(waiting.begin(), finished);

    finished_lock.Unlock();
    
    dout(2) << "do_waiters -- start" << dendl;
    for (list<OpRequestRef>::iterator it = waiting.begin();
         it != waiting.end();
         it++)
      dispatch_op(*it);
    dout(2) << "do_waiters -- finish" << dendl;
  }
}

void OSD::dispatch_op(OpRequestRef op)
{
  switch (op->request->get_type()) {

  case MSG_OSD_PG_CREATE:
    handle_pg_create(op);
    break;

  case MSG_OSD_PG_NOTIFY:
    handle_pg_notify(op);
    break;
  case MSG_OSD_PG_QUERY:
    handle_pg_query(op);
    break;
  case MSG_OSD_PG_LOG:
    handle_pg_log(op);
    break;
  case MSG_OSD_PG_REMOVE:
    handle_pg_remove(op);
    break;
  case MSG_OSD_PG_INFO:
    handle_pg_info(op);
    break;
  case MSG_OSD_PG_TRIM:
    handle_pg_trim(op);
    break;
  case MSG_OSD_PG_MISSING:
    assert(0 ==
	   "received MOSDPGMissing; this message is supposed to be unused!?!");
    break;
  case MSG_OSD_PG_SCAN:
    handle_pg_scan(op);
    break;
  case MSG_OSD_PG_BACKFILL:
    handle_pg_backfill(op);
    break;

    // client ops
  case CEPH_MSG_OSD_OP:
    handle_op(op);
    break;

    // for replication etc.
  case MSG_OSD_SUBOP:
    handle_sub_op(op);
    break;
  case MSG_OSD_SUBOPREPLY:
    handle_sub_op_reply(op);
    break;
  }
}

void OSD::_dispatch(Message *m)
{
  assert(osd_lock.is_locked());
  dout(20) << "_dispatch " << m << " " << *m << dendl;
  Session *session = NULL;

  logger->set(l_osd_buf, buffer::get_total_alloc());

  switch (m->get_type()) {

    // -- don't need lock -- 
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source() << dendl;
    m->put();
    break;

    // -- don't need OSDMap --

    // map and replication
  case CEPH_MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    break;

    // osd
  case CEPH_MSG_SHUTDOWN:
    session = (Session *)m->get_connection()->get_priv();
    if (!session ||
	session->entity_name.is_mon() ||
	session->entity_name.is_osd()) shutdown();
    else dout(0) << "shutdown message from connection with insufficient privs!"
		 << m->get_connection() << dendl;
    m->put();
    if (session)
      session->put();
    break;

  case MSG_PGSTATSACK:
    handle_pg_stats_ack((MPGStatsAck*)m);
    break;

  case MSG_MON_COMMAND:
    handle_command((MMonCommand*) m);
    break;
  case MSG_COMMAND:
    handle_command((MCommand*) m);
    break;

  case MSG_OSD_SCRUB:
    handle_scrub((MOSDScrub*)m);
    break;    

  case MSG_OSD_REP_SCRUB:
    handle_rep_scrub((MOSDRepScrub*)m);
    break;    

    // -- need OSDMap --

  default:
    {
      OpRequestRef op = op_tracker.create_request(m);
      op->mark_event("waiting_for_osdmap");
      // no map?  starting up?
      if (!osdmap) {
        dout(7) << "no OSDMap, not booted" << dendl;
        waiting_for_osdmap.push_back(op);
        break;
      }
      
      // need OSDMap
      dispatch_op(op);
    }
  }

  logger->set(l_osd_buf, buffer::get_total_alloc());

}

void OSD::handle_rep_scrub(MOSDRepScrub *m)
{
  dout(10) << "queueing MOSDRepScrub " << *m << dendl;
  rep_scrub_wq.queue(m);
}

void OSD::handle_scrub(MOSDScrub *m)
{
  dout(10) << "handle_scrub " << *m << dendl;
  if (!require_mon_peer(m))
    return;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_scrub fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  if (m->scrub_pgs.empty()) {
    for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
	 p != pg_map.end();
	 p++) {
      PG *pg = p->second;
      pg->lock();
      if (pg->is_primary()) {
	if (m->repair)
	  pg->state_set(PG_STATE_REPAIR);
	if (pg->queue_scrub()) {
	  dout(10) << "queueing " << *pg << " for scrub" << dendl;
	}
      }
      pg->unlock();
    }
  } else {
    for (vector<pg_t>::iterator p = m->scrub_pgs.begin();
	 p != m->scrub_pgs.end();
	 p++)
      if (pg_map.count(*p)) {
	PG *pg = pg_map[*p];
	pg->lock();
	if (pg->is_primary()) {
	  if (m->repair)
	    pg->state_set(PG_STATE_REPAIR);
	  if (pg->queue_scrub()) {
	    dout(10) << "queueing " << *pg << " for scrub" << dendl;
	  }
	}
	pg->unlock();
      }
  }
  
  m->put();
}

bool OSDService::scrub_should_schedule()
{
  double loadavgs[1];

  // TODOSAM: is_active should be conveyed to OSDService
  /*
  if (!is_active())
    return false;
  */

  if (getloadavg(loadavgs, 1) != 1) {
    dout(10) << "scrub_should_schedule couldn't read loadavgs\n" << dendl;
    return false;
  }

  if (loadavgs[0] >= g_conf->osd_scrub_load_threshold) {
    dout(20) << "scrub_should_schedule loadavg " << loadavgs[0]
	     << " >= max " << g_conf->osd_scrub_load_threshold
	     << " = no, load too high" << dendl;
    return false;
  }

  bool coin_flip = (rand() % 3) == whoami % 3;
  if (!coin_flip) {
    dout(20) << "scrub_should_schedule loadavg " << loadavgs[0]
	     << " < max " << g_conf->osd_scrub_load_threshold
	     << " = no, randomly backing off"
	     << dendl;
    return false;
  }
  
  dout(20) << "scrub_should_schedule loadavg " << loadavgs[0]
	   << " < max " << g_conf->osd_scrub_load_threshold
	   << " = yes" << dendl;
  return loadavgs[0] < g_conf->osd_scrub_load_threshold;
}


bool OSDService::inc_scrubs_pending()
{
  bool result = false;

  sched_scrub_lock.Lock();
  if (scrubs_pending + scrubs_active < g_conf->osd_max_scrubs) {
    dout(20) << "inc_scrubs_pending " << scrubs_pending << " -> " << (scrubs_pending+1)
	     << " (max " << g_conf->osd_max_scrubs << ", active " << scrubs_active << ")" << dendl;
    result = true;
    ++scrubs_pending;
  } else {
    dout(20) << "inc_scrubs_pending " << scrubs_pending << " + " << scrubs_active << " active >= max " << g_conf->osd_max_scrubs << dendl;
  }
  sched_scrub_lock.Unlock();

  return result;
}

void OSDService::dec_scrubs_pending()
{
  sched_scrub_lock.Lock();
  dout(20) << "dec_scrubs_pending " << scrubs_pending << " -> " << (scrubs_pending-1)
	   << " (max " << g_conf->osd_max_scrubs << ", active " << scrubs_active << ")" << dendl;
  --scrubs_pending;
  assert(scrubs_pending >= 0);
  sched_scrub_lock.Unlock();
}

void OSDService::dec_scrubs_active()
{
  sched_scrub_lock.Lock();
  dout(20) << "dec_scrubs_active " << scrubs_active << " -> " << (scrubs_active-1)
	   << " (max " << g_conf->osd_max_scrubs << ", pending " << scrubs_pending << ")" << dendl;
  --scrubs_active;
  sched_scrub_lock.Unlock();
}

// =====================================================
// MAP

void OSD::wait_for_new_map(OpRequestRef op)
{
  // ask?
  if (waiting_for_osdmap.empty()) {
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
  
  waiting_for_osdmap.push_back(op);
  op->mark_delayed();
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */

void OSD::note_down_osd(int peer)
{
  cluster_messenger->mark_down(osdmap->get_cluster_addr(peer));

  heartbeat_lock.Lock();
  failure_queue.erase(peer);
  failure_pending.erase(peer);
  map<int,HeartbeatInfo>::iterator p = heartbeat_peers.find(peer);
  if (p != heartbeat_peers.end()) {
    hbclient_messenger->mark_down(p->second.con);
    p->second.con->put();
    heartbeat_peers.erase(p);
  }
  heartbeat_lock.Unlock();
}

void OSD::note_up_osd(int peer)
{
  forget_peer_epoch(peer, osdmap->get_epoch() - 1);
}

void OSD::handle_osd_map(MOSDMap *m)
{
  assert(osd_lock.is_locked());
  list<OSDMapRef> pinned_maps;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  Session *session = (Session *)m->get_connection()->get_priv();
  if (session && !(session->entity_name.is_mon() || session->entity_name.is_osd())) {
    //not enough perms!
    m->put();
    session->put();
    return;
  }
  if (session)
    session->put();

  epoch_t first = m->get_first();
  epoch_t last = m->get_last();
  dout(3) << "handle_osd_map epochs [" << first << "," << last << "], i have "
	  << osdmap->get_epoch()
	  << ", src has [" << m->oldest_map << "," << m->newest_map << "]"
	  << dendl;

  if (logger) {
    logger->inc(l_osd_map);
    logger->inc(l_osd_mape, last - first + 1);
    if (first <= osdmap->get_epoch())
      logger->inc(l_osd_mape_dup, osdmap->get_epoch() - first + 1);
  }

  // make sure there is something new, here, before we bother flushing the queues and such
  if (last <= osdmap->get_epoch()) {
    dout(10) << " no new maps here, dropping" << dendl;
    m->put();
    return;
  }

  // missing some?
  bool skip_maps = false;
  if (first > osdmap->get_epoch() + 1) {
    dout(10) << "handle_osd_map message skips epochs " << osdmap->get_epoch() + 1
	     << ".." << (first-1) << dendl;
    if ((m->oldest_map < first && osdmap->get_epoch() == 0) ||
	m->oldest_map <= osdmap->get_epoch()) {
      monc->sub_want("osdmap", osdmap->get_epoch()+1, CEPH_SUBSCRIBE_ONETIME);
      monc->renew_subs();
      m->put();
      return;
    }
    skip_maps = true;
  }

  ObjectStore::Transaction t;

  // store new maps: queue for disk and put in the osdmap cache
  epoch_t start = MAX(osdmap->get_epoch() + 1, first);
  for (epoch_t e = start; e <= last; e++) {
    map<epoch_t,bufferlist>::iterator p;
    p = m->maps.find(e);
    if (p != m->maps.end()) {
      dout(10) << "handle_osd_map  got full map for epoch " << e << dendl;
      OSDMap *o = new OSDMap;
      bufferlist& bl = p->second;
      
      o->decode(bl);
      pinned_maps.push_back(add_map(o));

      hobject_t fulloid = get_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, fulloid, 0, bl.length(), bl);
      pin_map_bl(e, bl);
      continue;
    }

    p = m->incremental_maps.find(e);
    if (p != m->incremental_maps.end()) {
      dout(10) << "handle_osd_map  got inc map for epoch " << e << dendl;
      bufferlist& bl = p->second;
      hobject_t oid = get_inc_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, oid, 0, bl.length(), bl);
      pin_map_inc_bl(e, bl);

      OSDMap *o = new OSDMap;
      if (e > 1) {
	bufferlist obl;
	OSDMapRef prev = get_map(e - 1);
	prev->encode(obl);
	o->decode(obl);
      }

      OSDMap::Incremental inc;
      bufferlist::iterator p = bl.begin();
      inc.decode(p);
      if (o->apply_incremental(inc) < 0) {
	derr << "ERROR: bad fsid?  i have " << osdmap->get_fsid() << " and inc has " << inc.fsid << dendl;
	assert(0 == "bad fsid");
      }

      pinned_maps.push_back(add_map(o));

      bufferlist fbl;
      o->encode(fbl);

      hobject_t fulloid = get_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, fulloid, 0, fbl.length(), fbl);
      pin_map_bl(e, fbl);
      continue;
    }

    assert(0 == "MOSDMap lied about what maps it had?");
  }

  if (superblock.oldest_map) {
    for (epoch_t e = superblock.oldest_map; e < m->oldest_map; ++e) {
      dout(20) << " removing old osdmap epoch " << e << dendl;
      t.remove(coll_t::META_COLL, get_osdmap_pobject_name(e));
      t.remove(coll_t::META_COLL, get_inc_osdmap_pobject_name(e));
      superblock.oldest_map = e+1;
    }
  }

  if (!superblock.oldest_map || skip_maps)
    superblock.oldest_map = first;
  superblock.newest_map = last;

 
  map_lock.get_write();

  C_Contexts *fin = new C_Contexts(g_ceph_context);

  // advance through the new maps
  for (epoch_t cur = start; cur <= superblock.newest_map; cur++) {
    dout(10) << " advance to epoch " << cur << " (<= newest " << superblock.newest_map << ")" << dendl;

    OSDMapRef newmap = get_map(cur);
    assert(newmap);  // we just cached it above!

    // kill connections to newly down osds
    set<int> old;
    osdmap->get_all_osds(old);
    for (set<int>::iterator p = old.begin(); p != old.end(); p++)
      if (*p != whoami &&
	  osdmap->have_inst(*p) &&                        // in old map
	  (!newmap->exists(*p) || !newmap->is_up(*p)))    // but not the new one
	note_down_osd(*p);
    
    osdmap = newmap;

    superblock.current_epoch = cur;
    advance_map(t, fin);
    had_map_since = ceph_clock_now(g_ceph_context);
  }

  if (osdmap->is_up(whoami) &&
      osdmap->get_addr(whoami) == client_messenger->get_myaddr() &&
      bind_epoch < osdmap->get_up_from(whoami)) {

    if (is_booting()) {
      dout(1) << "state: booting -> active" << dendl;
      state = STATE_ACTIVE;
    }
  }

  bool do_shutdown = false;
  bool do_restart = false;
  if (osdmap->get_epoch() > 0 &&
      state == STATE_ACTIVE) {
    if (!osdmap->exists(whoami)) {
      dout(0) << "map says i do not exist.  shutting down." << dendl;
      do_shutdown = true;   // don't call shutdown() while we have everything paused
    } else if (!osdmap->is_up(whoami) ||
	       !osdmap->get_addr(whoami).probably_equals(client_messenger->get_myaddr()) ||
	       !osdmap->get_cluster_addr(whoami).probably_equals(cluster_messenger->get_myaddr()) ||
	       !osdmap->get_hb_addr(whoami).probably_equals(hbserver_messenger->get_myaddr())) {
      if (!osdmap->is_up(whoami))
	clog.warn() << "map e" << osdmap->get_epoch()
		    << " wrongly marked me down";
      else if (!osdmap->get_addr(whoami).probably_equals(client_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong client addr (" << osdmap->get_addr(whoami)
		     << " != my " << client_messenger->get_myaddr() << ")";
      else if (!osdmap->get_cluster_addr(whoami).probably_equals(cluster_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong cluster addr (" << osdmap->get_cluster_addr(whoami)
		     << " != my " << cluster_messenger->get_myaddr() << ")";
      else if (!osdmap->get_hb_addr(whoami).probably_equals(hbserver_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb addr (" << osdmap->get_hb_addr(whoami)
		     << " != my " << hbserver_messenger->get_myaddr() << ")";
      
      state = STATE_BOOTING;
      up_epoch = 0;
      do_restart = true;
      bind_epoch = osdmap->get_epoch();

      int cport = cluster_messenger->get_myaddr().get_port();
      int hbport = hbserver_messenger->get_myaddr().get_port();

      int r = cluster_messenger->rebind(hbport);
      if (r != 0)
	do_shutdown = true;  // FIXME: do_restart?

      r = hbserver_messenger->rebind(cport);
      if (r != 0)
	do_shutdown = true;  // FIXME: do_restart?

      hbclient_messenger->mark_down_all();

      reset_heartbeat_peers();
    }
  }


  // note in the superblock that we were clean thru the prior epoch
  if (boot_epoch && boot_epoch >= superblock.mounted) {
    superblock.mounted = boot_epoch;
    superblock.clean_thru = osdmap->get_epoch();
  }

  // superblock and commit
  write_superblock(t);
  int r = store->apply_transaction(t, fin);
  if (r) {
    map_lock.put_write();
    derr << "error writing map: " << cpp_strerror(-r) << dendl;
    m->put();
    shutdown();
    return;
  }
  serviceRef->publish_superblock(superblock);

  clear_map_bl_cache_pins();
  map_lock.put_write();

  // yay!
  if (is_active())
    activate_map();

  if (m->newest_map && m->newest_map > last) {
    dout(10) << " msg say newest map is " << m->newest_map << ", requesting more" << dendl;
    monc->sub_want("osdmap", osdmap->get_epoch()+1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
  else if (is_booting()) {
    start_boot();  // retry
  }
  else if (do_restart)
    start_boot();

  if (do_shutdown)
    shutdown();

  m->put();
}

void OSD::advance_pg(epoch_t osd_epoch, PG *pg, PG::RecoveryCtx *rctx)
{
  assert(pg->is_locked());
  epoch_t next_epoch = pg->get_osdmap()->get_epoch() + 1;
  OSDMapRef lastmap = pg->get_osdmap();

  if (lastmap->get_epoch() == osd_epoch)
    return;
  assert(lastmap->get_epoch() < osd_epoch);

  for (;
       next_epoch <= osd_epoch;
       ++next_epoch) {
    OSDMapRef nextmap = get_map(next_epoch);
    vector<int> newup, newacting;
    nextmap->pg_to_up_acting_osds(pg->info.pgid, newup, newacting);
    pg->handle_advance_map(nextmap, lastmap, newup, newacting, rctx);
    lastmap = nextmap;
  }
  pg->handle_activate_map(rctx);
}

/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t, C_Contexts *tfin)
{
  assert(osd_lock.is_locked());

  dout(7) << "advance_map epoch " << osdmap->get_epoch()
          << "  " << pg_map.size() << " pgs"
          << dendl;

  if (!up_epoch &&
      osdmap->is_up(whoami) &&
      osdmap->get_inst(whoami) == client_messenger->get_myinst()) {
    up_epoch = osdmap->get_epoch();
    dout(10) << "up_epoch is " << up_epoch << dendl;
    if (!boot_epoch) {
      boot_epoch = osdmap->get_epoch();
      dout(10) << "boot_epoch is " << boot_epoch << dendl;
    }
  }

  map<int64_t, int> pool_resize;  // poolid -> old size

  // scan pg creations
  hash_map<pg_t, create_pg_info>::iterator n = creating_pgs.begin();
  while (n != creating_pgs.end()) {
    hash_map<pg_t, create_pg_info>::iterator p = n++;
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);
    if (role != 0) {
      dout(10) << " no longer primary for " << pgid << ", stopping creation" << dendl;
      creating_pgs.erase(p);
    } else {
      /*
       * adding new ppl to our pg has no effect, since we're still primary,
       * and obviously haven't given the new nodes any data.
       */
      p->second.acting.swap(acting);  // keep the latest
    }
  }

  // scan pgs with waiters
  map<pg_t, list<OpRequestRef> >::iterator p = waiting_for_pg.begin();
  while (p != waiting_for_pg.end()) {
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);
    if (role >= 0) {
      ++p;  // still me
    } else {
      dout(10) << " discarding waiting ops for " << pgid << dendl;
      while (!p->second.empty()) {
	p->second.pop_front();
      }
      waiting_for_pg.erase(p++);
    }
  }
}

void OSD::activate_map()
{
  assert(osd_lock.is_locked());

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  map< int, vector<pair<pg_notify_t,pg_interval_map_t> > >  notify_list;  // primary -> list
  map< int, map<pg_t,pg_query_t> > query_map;    // peer -> PG -> get_summary_since
  map<int,MOSDPGInfo*> info_map;  // peer -> message

  int num_pg_primary = 0, num_pg_replica = 0, num_pg_stray = 0;

  epoch_t oldest_last_clean = osdmap->get_epoch();

  list<PG*> to_remove;

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock();
    if (pg->is_primary())
      num_pg_primary++;
    else if (pg->is_replica())
      num_pg_replica++;
    else
      num_pg_stray++;

    if (pg->is_primary() && pg->info.history.last_epoch_clean < oldest_last_clean)
      oldest_last_clean = pg->info.history.last_epoch_clean;

    if (!osdmap->have_pg_pool(pg->info.pgid.pool())) {
      //pool is deleted!
      pg->get();
      to_remove.push_back(pg);
    }
    pg->unlock();
  }

  for (list<PG*>::iterator i = to_remove.begin();
       i != to_remove.end();
       ++i) {
    (*i)->lock();
    _remove_pg((*i));
    (*i)->unlock();
    (*i)->put();
  }
  to_remove.clear();

  serviceRef->publish_map(osdmap);

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock();
    pg->queue_null(osdmap->get_epoch(), osdmap->get_epoch());
    pg->unlock();
  }

  
  logger->set(l_osd_pg, pg_map.size());
  logger->set(l_osd_pg_primary, num_pg_primary);
  logger->set(l_osd_pg_replica, num_pg_replica);
  logger->set(l_osd_pg_stray, num_pg_stray);

  wake_all_pg_waiters();   // the pg mapping may have shifted
  maybe_update_heartbeat_peers();

  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " osdmap flagged full, doing onetime osdmap subscribe" << dendl;
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }

  // process waiters
  take_waiters(waiting_for_osdmap);
}


MOSDMap *OSD::build_incremental_map_msg(epoch_t since, epoch_t to)
{
  MOSDMap *m = new MOSDMap(monc->get_fsid());
  m->oldest_map = superblock.oldest_map;
  m->newest_map = superblock.newest_map;
  
  for (epoch_t e = to; e > since; e--) {
    bufferlist bl;
    if (e > m->oldest_map && get_inc_map_bl(e, bl)) {
      m->incremental_maps[e].claim(bl);
    } else if (get_map_bl(e, bl)) {
      m->maps[e].claim(bl);
      break;
    } else {
      derr << "since " << since << " to " << to
	   << " oldest " << m->oldest_map << " newest " << m->newest_map
	   << dendl;
      assert(0 == "missing an osdmap on disk");  // we should have all maps.
    }
  }
  return m;
}

void OSD::send_map(MOSDMap *m, const entity_inst_t& inst, bool lazy)
{
  Messenger *msgr = client_messenger;
  if (entity_name_t::TYPE_OSD == inst.name._type)
    msgr = cluster_messenger;
  if (lazy)
    msgr->lazy_send_message(m, inst);  // only if we already have an open connection
  else
    msgr->send_message(m, inst);
}

void OSD::send_incremental_map(epoch_t since, const entity_inst_t& inst, bool lazy)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << inst << dendl;
  
  if (since < superblock.oldest_map) {
    // just send latest full map
    MOSDMap *m = new MOSDMap(monc->get_fsid());
    m->oldest_map = superblock.oldest_map;
    m->newest_map = superblock.newest_map;
    epoch_t e = osdmap->get_epoch();
    get_map_bl(e, m->maps[e]);
    send_map(m, inst, lazy);
    return;
  }

  while (since < osdmap->get_epoch()) {
    epoch_t to = osdmap->get_epoch();
    if (to - since > (epoch_t)g_conf->osd_map_message_max)
      to = since + g_conf->osd_map_message_max;
    MOSDMap *m = build_incremental_map_msg(since, to);
    send_map(m, inst, lazy);
    since = to;
  }
}

bool OSDService::_get_map_bl(epoch_t e, bufferlist& bl)
{
  bool found = map_bl_cache.lookup(e, &bl);
  if (found)
    return true;
  found = store->read(
    coll_t::META_COLL, OSD::get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
  if (found)
    _add_map_bl(e, bl);
  return found;
}

bool OSDService::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  Mutex::Locker l(map_cache_lock);
  bool found = map_bl_inc_cache.lookup(e, &bl);
  if (found)
    return true;
  found = store->read(
    coll_t::META_COLL, OSD::get_inc_osdmap_pobject_name(e), 0, 0, bl) >= 0;
  if (found)
    _add_map_inc_bl(e, bl);
  return found;
}

void OSDService::_add_map_bl(epoch_t e, bufferlist& bl)
{
  dout(10) << "add_map_bl " << e << " " << bl.length() << " bytes" << dendl;
  map_bl_cache.add(e, bl);
}

void OSDService::_add_map_inc_bl(epoch_t e, bufferlist& bl)
{
  dout(10) << "add_map_inc_bl " << e << " " << bl.length() << " bytes" << dendl;
  map_bl_inc_cache.add(e, bl);
}

void OSDService::pin_map_inc_bl(epoch_t e, bufferlist &bl)
{
  Mutex::Locker l(map_cache_lock);
  map_bl_inc_cache.pin(e, bl);
}

void OSDService::pin_map_bl(epoch_t e, bufferlist &bl)
{
  Mutex::Locker l(map_cache_lock);
  map_bl_cache.pin(e, bl);
}

void OSDService::clear_map_bl_cache_pins()
{
  Mutex::Locker l(map_cache_lock);
  map_bl_inc_cache.clear_pinned();
  map_bl_cache.clear_pinned();
}

OSDMapRef OSDService::_add_map(OSDMap *o)
{
  epoch_t e = o->get_epoch();

  if (g_conf->osd_map_dedup) {
    // Dedup against an existing map at a nearby epoch
    OSDMapRef for_dedup = map_cache.lower_bound(e);
    if (for_dedup) {
      OSDMap::dedup(for_dedup.get(), o);
    }
  }
  OSDMapRef l = map_cache.add(e, o);
  return l;
}

OSDMapRef OSDService::get_map(epoch_t epoch)
{
  Mutex::Locker l(map_cache_lock);
  OSDMapRef retval = map_cache.lookup(epoch);
  if (retval) {
    dout(30) << "get_map " << epoch << " -cached" << dendl;
    return retval;
  }

  OSDMap *map = new OSDMap;
  if (epoch > 0) {
    dout(20) << "get_map " << epoch << " - loading and decoding " << map << dendl;
    bufferlist bl;
    assert(_get_map_bl(epoch, bl));
    map->decode(bl);
  } else {
    dout(20) << "get_map " << epoch << " - return initial " << map << dendl;
  }
  return _add_map(map);
}

bool OSD::require_mon_peer(Message *m)
{
  if (!m->get_connection()->peer_is_mon()) {
    dout(0) << "require_mon_peer received from non-mon " << m->get_connection()->get_peer_addr()
	    << " " << *m << dendl;
    m->put();
    return false;
  }
  return true;
}

bool OSD::require_osd_peer(OpRequestRef op)
{
  if (!op->request->get_connection()->peer_is_osd()) {
    dout(0) << "require_osd_peer received from non-osd " << op->request->get_connection()->get_peer_addr()
	    << " " << *op->request << dendl;
    return false;
  }
  return true;
}

/*
 * require that we have same (or newer) map, and that
 * the source is the pg primary.
 */
bool OSD::require_same_or_newer_map(OpRequestRef op, epoch_t epoch)
{
  Message *m = op->request;
  dout(15) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ") " << m << dendl;

  // do they have a newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << epoch << " > my " << osdmap->get_epoch() << " with " << m << dendl;
    wait_for_new_map(op);
    return false;
  }

  if (epoch < up_epoch) {
    dout(7) << "from pre-up epoch " << epoch << " < " << up_epoch << dendl;
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_cluster_addr(from) != m->get_source_inst().addr) {
      dout(0) << "from dead osd." << from << ", dropping, sharing map" << dendl;
      send_incremental_map(epoch, m->get_source_inst(), true);

      // close after we send the map; don't reconnect
      Connection *con = m->get_connection();
      cluster_messenger->mark_down_on_empty(con);
      cluster_messenger->mark_disposable(con);

      return false;
    }
  }

  // ok, we have at least as new a map as they do.  are we (re)booting?
  if (!is_active()) {
    dout(7) << "still in boot state, dropping message " << *m << dendl;
    return false;
  }

  return true;
}





// ----------------------------------------
// pg creation


bool OSD::can_create_pg(pg_t pgid)
{
  assert(creating_pgs.count(pgid));

  // priors empty?
  if (!creating_pgs[pgid].prior.empty()) {
    dout(10) << "can_create_pg " << pgid
	     << " - waiting for priors " << creating_pgs[pgid].prior << dendl;
    return false;
  }

  if (creating_pgs[pgid].split_bits) {
    dout(10) << "can_create_pg " << pgid << " - split" << dendl;
    return false;
  }

  dout(10) << "can_create_pg " << pgid << " - can create now" << dendl;
  return true;
}

void OSD::do_split(PG *parent, set<pg_t>& childpgids, ObjectStore::Transaction& t,
		   C_Contexts *tfin)
{
  dout(10) << "do_split to " << childpgids << " on " << *parent << dendl;

  parent->lock_with_map_lock_held();
 
  // create and lock children
  map<pg_t,PG*> children;
  for (set<pg_t>::iterator q = childpgids.begin();
       q != childpgids.end();
       q++) {
    pg_history_t history;
    history.epoch_created = history.same_up_since =
      history.same_interval_since = history.same_primary_since =
      osdmap->get_epoch();
    pg_interval_map_t pi;
    PG *pg = _create_lock_pg(serviceRef->get_osdmap(), *q, true, true,
			     parent->get_role(), parent->up, parent->acting, history, pi, t);
    children[*q] = pg;
    dout(10) << "  child " << *pg << dendl;
  }

  split_pg(parent, children, t); 

#if 0
  // reset pg
  map< int, vector<pair<pg_notify_t, pg_interval_map_t> > >  notify_list;  // primary -> list
  map< int, map<pg_t,pg_query_t> > query_map;    // peer -> PG -> get_summary_since
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > info_map;  // peer -> message
  PG::RecoveryCtx rctx(&query_map, &info_map, &notify_list, &tfin->contexts, &t);

  // FIXME: this breaks if we have a map discontinuity
  //parent->handle_split(osdmap, get_map(osdmap->get_epoch() - 1), &rctx);

  // unlock parent, children
  parent->unlock();

  for (map<pg_t,PG*>::iterator q = children.begin(); q != children.end(); q++) {
    PG *pg = q->second;
    pg->handle_create(&rctx);
    pg->write_if_dirty(t);
    wake_pg_waiters(pg->info.pgid);
    pg->unlock();
  }

  do_notifies(notify_list);
  do_queries(query_map);
  do_infos(info_map);
#endif
}

void OSD::split_pg(PG *parent, map<pg_t,PG*>& children, ObjectStore::Transaction &t)
{
  dout(10) << "split_pg " << *parent << dendl;
  pg_t parentid = parent->info.pgid;

  // split objects
  vector<hobject_t> olist;
  store->collection_list(coll_t(parent->info.pgid), olist);

  for (vector<hobject_t>::iterator p = olist.begin(); p != olist.end(); p++) {
    hobject_t poid = *p;
    object_locator_t oloc(parentid.pool());
    if (poid.get_key().size())
      oloc.key = poid.get_key();
    pg_t rawpg = osdmap->object_locator_to_pg(poid.oid, oloc);
    pg_t pgid = osdmap->raw_pg_to_pg(rawpg);
    if (pgid != parentid) {
      dout(20) << "  moving " << poid << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];
      assert(child);
      bufferlist bv;

      struct stat st;
      store->stat(coll_t(parentid), poid, &st);
      store->getattr(coll_t(parentid), poid, OI_ATTR, bv);
      object_info_t oi(bv);

      t.collection_move(coll_t(pgid), coll_t(parentid), poid);
      if (oi.snaps.size()) {
	snapid_t first = oi.snaps[0];
	t.collection_move(coll_t(pgid, first), coll_t(parentid), poid);
	if (oi.snaps.size() > 1) {
	  snapid_t last = oi.snaps[oi.snaps.size()-1];
	  t.collection_move(coll_t(pgid, last), coll_t(parentid), poid);
	}
      }

      // add to child stats
      child->info.stats.stats.sum.num_bytes += st.st_size;
      child->info.stats.stats.sum.num_objects++;
      if (poid.snap && poid.snap != CEPH_NOSNAP)
	child->info.stats.stats.sum.num_object_clones++;
    } else {
      dout(20) << " leaving " << poid << "   in " << parentid << dendl;
    }
  }

  // split log
  parent->log.index();
  dout(20) << " parent " << parent->info.pgid << " log was ";
  parent->log.print(*_dout);
  *_dout << dendl;
  parent->log.unindex();

  list<pg_log_entry_t>::iterator p = parent->log.log.begin();
  while (p != parent->log.log.end()) {
    list<pg_log_entry_t>::iterator cur = p;
    p++;
    hobject_t& poid = cur->soid;
    object_locator_t oloc(parentid.pool());
    if (poid.get_key().size())
      oloc.key = poid.get_key();
    pg_t rawpg = osdmap->object_locator_to_pg(poid.oid, oloc);
    pg_t pgid = osdmap->raw_pg_to_pg(rawpg);
    if (pgid != parentid) {
      dout(20) << "  moving " << *cur << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];

      child->log.log.splice(child->log.log.end(), parent->log.log, cur);
    }
  }

  parent->log.index();
  dout(20) << " parent " << parent->info.pgid << " log now ";
  parent->log.print(*_dout);
  *_dout << dendl;

  for (map<pg_t,PG*>::iterator p = children.begin();
       p != children.end();
       p++) {
    PG *child = p->second;

    // fix log bounds
    if (!child->log.empty()) {
      child->log.head = child->log.log.rbegin()->version;
      child->log.tail =  parent->log.tail;
      child->log.index();
    }
    child->info.last_update = child->log.head;
    child->info.last_complete = child->info.last_update;
    child->info.log_tail = parent->log.tail;
    child->info.history.last_epoch_split = osdmap->get_epoch();

    child->snap_trimq = parent->snap_trimq;

    dout(20) << " child " << p->first << " log now ";
    child->log.print(*_dout);
    *_dout << dendl;

    // sub off child stats
    parent->info.stats.sub(child->info.stats);
  }
}  


/*
 * holding osd_lock
 */
void OSD::handle_pg_create(OpRequestRef op)
{
  MOSDPGCreate *m = (MOSDPGCreate*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_CREATE);

  dout(10) << "handle_pg_create " << *m << dendl;

  // drop the next N pg_creates in a row?
  if (debug_drop_pg_create_left < 0 &&
      g_conf->osd_debug_drop_pg_create_probability >
      ((((double)(rand()%100))/100.0))) {
    debug_drop_pg_create_left = debug_drop_pg_create_duration;
  }
  if (debug_drop_pg_create_left >= 0) {
    --debug_drop_pg_create_left;
    if (debug_drop_pg_create_left >= 0) {
      dout(0) << "DEBUG dropping/ignoring pg_create, will drop the next "
	      << debug_drop_pg_create_left << " too" << dendl;
      return;
    }
  }

  if (!require_mon_peer(op->request)) {
    // we have to hack around require_mon_peer's interface limits
    op->request = NULL;
    return;
  }

  if (!require_same_or_newer_map(op, m->epoch)) return;

  op->mark_started();

  int num_created = 0;

  for (map<pg_t,pg_create_t>::iterator p = m->mkpg.begin();
       p != m->mkpg.end();
       p++) {
    pg_t pgid = p->first;
    epoch_t created = p->second.created;
    pg_t parent = p->second.parent;
    int split_bits = p->second.split_bits;
    pg_t on = pgid;

    if (pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    if (split_bits) {
      on = parent;
      dout(20) << "mkpg " << pgid << " e" << created << " from parent " << parent
	       << " split by " << split_bits << " bits" << dendl;
    } else {
      dout(20) << "mkpg " << pgid << " e" << created << dendl;
    }
   
    // is it still ours?
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(on, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    if (role != 0) {
      dout(10) << "mkpg " << pgid << "  not primary (role=" << role << "), skipping" << dendl;
      continue;
    }
    if (up != acting) {
      dout(10) << "mkpg " << pgid << "  up " << up << " != acting " << acting << dendl;
      clog.error() << "mkpg " << pgid << " up " << up << " != acting "
	    << acting << "\n";
      continue;
    }

    // does it already exist?
    if (_have_pg(pgid)) {
      dout(10) << "mkpg " << pgid << "  already exists, skipping" << dendl;
      continue;
    }

    // does parent exist?
    if (split_bits && !_have_pg(parent)) {
      dout(10) << "mkpg " << pgid << "  missing parent " << parent << ", skipping" << dendl;
      continue;
    }

    // figure history
    pg_history_t history;
    history.epoch_created = created;
    history.last_epoch_clean = created;
    project_pg_history(pgid, history, created, up, acting);
    
    // register.
    creating_pgs[pgid].history = history;
    creating_pgs[pgid].parent = parent;
    creating_pgs[pgid].split_bits = split_bits;
    creating_pgs[pgid].acting.swap(acting);
    calc_priors_during(pgid, created, history.same_interval_since, 
		       creating_pgs[pgid].prior);

    PG::RecoveryCtx rctx = create_context();
    // poll priors
    set<int>& pset = creating_pgs[pgid].prior;
    dout(10) << "mkpg " << pgid << " e" << created
	     << " h " << history
	     << " : querying priors " << pset << dendl;
    for (set<int>::iterator p = pset.begin(); p != pset.end(); p++) 
      if (osdmap->is_up(*p))
	(*rctx.query_map)[*p][pgid] = pg_query_t(pg_query_t::INFO, history,
						 osdmap->get_epoch());

    PG *pg = NULL;
    if (can_create_pg(pgid)) {
      pg_interval_map_t pi;
      pg = _create_lock_pg(
	osdmap, pgid, true, false,
	0, creating_pgs[pgid].acting, creating_pgs[pgid].acting,
	history, pi,
	*rctx.transaction);
      creating_pgs.erase(pgid);
      wake_pg_waiters(pg->info.pgid);
      pg->handle_create(&rctx);
      pg->write_if_dirty(*rctx.transaction);
      pg->update_stats();
      pg->unlock();
      num_created++;
    }
    dispatch_context(rctx, pg, osdmap);
  }

  maybe_update_heartbeat_peers();
}


// ----------------------------------------
// peering and recovery

PG::RecoveryCtx OSD::create_context()
{
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  C_Contexts *on_applied = new C_Contexts(g_ceph_context);
  C_Contexts *on_safe = new C_Contexts(g_ceph_context);
  map< int, map<pg_t,pg_query_t> > *query_map =
    new map<int, map<pg_t, pg_query_t> >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  PG::RecoveryCtx rctx(query_map, info_map, notify_list,
		       on_applied, on_safe, t);
  return rctx;
}

void OSD::dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg)
{
  if (!ctx.transaction->empty()) {
    ctx.on_applied->add(new ObjectStore::C_DeleteTransaction(ctx.transaction));
    int tr = store->queue_transaction(
      pg->osr.get(),
      ctx.transaction, ctx.on_applied, ctx.on_safe);
    assert(tr == 0);
    ctx.transaction = new ObjectStore::Transaction;
    ctx.on_applied = new C_Contexts(g_ceph_context);
    ctx.on_safe = new C_Contexts(g_ceph_context);
  }
}

void OSD::dispatch_context(PG::RecoveryCtx &ctx, PG *pg, OSDMapRef curmap)
{
  do_notifies(*ctx.notify_list, curmap);
  delete ctx.notify_list;
  do_queries(*ctx.query_map, curmap);
  delete ctx.query_map;
  do_infos(*ctx.info_map, curmap);
  delete ctx.info_map;
  if (ctx.transaction->empty() || !pg) {
    delete ctx.transaction;
    delete ctx.on_applied;
    delete ctx.on_safe;
  } else {
    ctx.on_applied->add(new ObjectStore::C_DeleteTransaction(ctx.transaction));
    int tr = store->queue_transaction(
      pg->osr.get(),
      ctx.transaction, ctx.on_applied, ctx.on_safe);
    assert(tr == 0);
  }
}

/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void OSD::do_notifies(
  map< int,vector<pair<pg_notify_t,pg_interval_map_t> > >& notify_list,
  OSDMapRef curmap)
{
  for (map< int, vector<pair<pg_notify_t,pg_interval_map_t> > >::iterator it = notify_list.begin();
       it != notify_list.end();
       it++) {
    if (it->first == whoami) {
      dout(7) << "do_notify osd." << it->first << " is self, skipping" << dendl;
      continue;
    }
    if (!curmap->is_up(it->first))
      continue;
    Connection *con =
      cluster_messenger->get_connection(curmap->get_cluster_inst(it->first));
    _share_map_outgoing(curmap->get_cluster_inst(it->first), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_notify osd." << it->first
	      << " on " << it->second.size() << " PGs" << dendl;
      MOSDPGNotify *m = new MOSDPGNotify(curmap->get_epoch(),
					 it->second);
      cluster_messenger->send_message(m, curmap->get_cluster_inst(it->first));
    } else {
      dout(7) << "do_notify osd." << it->first
	      << " sending seperate messages" << dendl;
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     it->second.begin();
	   i != it->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > list(1);
	list[0] = *i;
	MOSDPGNotify *m = new MOSDPGNotify(i->first.epoch_sent,
					   list);
	cluster_messenger->send_message(m, curmap->get_cluster_inst(it->first));
      }
    }
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void OSD::do_queries(map< int, map<pg_t,pg_query_t> >& query_map,
		     OSDMapRef curmap)
{
  for (map< int, map<pg_t,pg_query_t> >::iterator pit = query_map.begin();
       pit != query_map.end();
       pit++) {
    if (!curmap->is_up(pit->first))
      continue;
    int who = pit->first;
    Connection *con =
      cluster_messenger->get_connection(curmap->get_cluster_inst(pit->first));
    _share_map_outgoing(curmap->get_cluster_inst(who), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_queries querying osd." << who
	      << " on " << pit->second.size() << " PGs" << dendl;
      MOSDPGQuery *m = new MOSDPGQuery(curmap->get_epoch(), pit->second);
      cluster_messenger->send_message(m, curmap->get_cluster_inst(who));
    } else {
      dout(7) << "do_queries querying osd." << who
	      << " sending seperate messages "
	      << " on " << pit->second.size() << " PGs" << dendl;
      for (map<pg_t, pg_query_t>::iterator i = pit->second.begin();
	   i != pit->second.end();
	   ++i) {
	map<pg_t, pg_query_t> to_send;
	to_send.insert(*i);
	MOSDPGQuery *m = new MOSDPGQuery(i->second.epoch_sent, to_send);
	cluster_messenger->send_message(m, curmap->get_cluster_inst(who));
      }
    }
  }
}


void OSD::do_infos(map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		   OSDMapRef curmap)
{
  for (map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >::iterator p = info_map.begin();
       p != info_map.end();
       ++p) { 
    if (!curmap->is_up(p->first))
      continue;
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator i = p->second.begin();
	 i != p->second.end();
	 ++i) {
      dout(20) << "Sending info " << i->first.info << " to osd." << p->first << dendl;
    }
    Connection *con =
      cluster_messenger->get_connection(curmap->get_cluster_inst(p->first));
    _share_map_outgoing(curmap->get_cluster_inst(p->first), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      MOSDPGInfo *m = new MOSDPGInfo(curmap->get_epoch());
      m->pg_list = p->second;
      cluster_messenger->send_message(m, curmap->get_cluster_inst(p->first));
    } else {
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     p->second.begin();
	   i != p->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > to_send(1);
	to_send[0] = *i;
	MOSDPGInfo *m = new MOSDPGInfo(i->first.epoch_sent);
	m->pg_list = to_send;
	cluster_messenger->send_message(m, curmap->get_cluster_inst(p->first));
      }
    }
  }
  info_map.clear();
}


/** PGNotify
 * from non-primary to primary
 * includes pg_info_t.
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_notify(OpRequestRef op)
{
  MOSDPGNotify *m = (MOSDPGNotify*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_NOTIFY);

  dout(7) << "handle_pg_notify from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (!require_osd_peer(op))
    return;

  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       it++) {
    PG *pg = 0;

    if (it->first.info.pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << it->first.info.pgid << dendl;
      continue;
    }

    int created = 0;
    pg = get_or_create_pg(it->first.info, it->second,
			  it->first.query_epoch, from, created, true);
    if (!pg)
      continue;
    pg->queue_notify(it->first.epoch_sent, it->first.query_epoch, from, it->first);
    pg->unlock();
  }
}

void OSD::handle_pg_log(OpRequestRef op)
{
  MOSDPGLog *m = (MOSDPGLog*) op->request;
  assert(m->get_header().type == MSG_OSD_PG_LOG);
  dout(7) << "handle_pg_log " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  if (m->info.pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->info.pgid << dendl;
    return;
  }

  int created = 0;
  PG *pg = get_or_create_pg(m->info, m->past_intervals, m->get_epoch(), 
			    from, created, false);
  if (!pg)
    return;
  op->mark_started();
  pg->queue_log(m->get_epoch(), m->get_query_epoch(), from, m);
  pg->unlock();
}

void OSD::handle_pg_info(OpRequestRef op)
{
  MOSDPGInfo *m = (MOSDPGInfo *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_INFO);
  dout(7) << "handle_pg_info " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  int created = 0;

  for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator p = m->pg_list.begin();
       p != m->pg_list.end();
       ++p) {
    if (p->first.info.pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << p->first.info.pgid << dendl;
      continue;
    }

    PG *pg = get_or_create_pg(p->first.info, p->second, p->first.epoch_sent,
			      from, created, false);
    if (!pg)
      continue;
    pg->queue_info(p->first.epoch_sent, p->first.query_epoch, from,
		   p->first.info);
    pg->unlock();
  }
}

void OSD::handle_pg_trim(OpRequestRef op)
{
  MOSDPGTrim *m = (MOSDPGTrim *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_TRIM);

  dout(7) << "handle_pg_trim " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->epoch)) return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  op->mark_started();

  if (!_have_pg(m->pgid)) {
    dout(10) << " don't have pg " << m->pgid << dendl;
  } else {
    PG *pg = _lookup_lock_pg(m->pgid);
    if (m->epoch < pg->info.history.same_interval_since) {
      dout(10) << *pg << " got old trim to " << m->trim_to << ", ignoring" << dendl;
      pg->unlock();
      return;
    }
    assert(pg);

    if (pg->is_primary()) {
      // peer is informing us of their last_complete_ondisk
      dout(10) << *pg << " replica osd." << from << " lcod " << m->trim_to << dendl;
      pg->peer_last_complete_ondisk[from] = m->trim_to;
      if (pg->calc_min_last_complete_ondisk()) {
	dout(10) << *pg << " min lcod now " << pg->min_last_complete_ondisk << dendl;
	pg->trim_peers();
      }
    } else {
      // primary is instructing us to trim
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      pg->trim(*t, m->trim_to);
      pg->write_info(*t);
      int tr = store->queue_transaction(pg->osr.get(), t,
					new ObjectStore::C_DeleteTransaction(t));
      assert(tr == 0);
    }
    pg->unlock();
  }
}

void OSD::handle_pg_scan(OpRequestRef op)
{
  MOSDPGScan *m = (MOSDPGScan*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_SCAN);
  dout(10) << "handle_pg_scan " << *m << " from " << m->get_source() << dendl;
  
  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  PG *pg;
  
  if (!_have_pg(m->pgid)) {
    return;
  }

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
  pg->unlock();
}

void OSD::handle_pg_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = (MOSDPGBackfill*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);
  dout(10) << "handle_pg_backfill " << *m << " from " << m->get_source() << dendl;
  
  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  PG *pg;
  
  if (!_have_pg(m->pgid)) {
    return;
  }

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
  pg->unlock();
}


/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_query(OpRequestRef op)
{
  assert(osd_lock.is_locked());

  MOSDPGQuery *m = (MOSDPGQuery*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_QUERY);

  if (!require_osd_peer(op))
    return;

  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << dendl;
  int from = m->get_source().num();
  
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > notify_list;
  
  for (map<pg_t,pg_query_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = it->first;

    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    PG *pg = 0;

    if (pg_map.count(pgid)) {
      pg = _lookup_lock_pg(pgid);
      pg->queue_query(it->second.epoch_sent, it->second.epoch_sent,
		      from, it->second);
      pg->unlock();
      continue;
    }

    // get active crush mapping
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pgid, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    // same primary?
    pg_history_t history = it->second.history;
    project_pg_history(pgid, history, it->second.epoch_sent, up, acting);

    if (it->second.epoch_sent < history.same_interval_since) {
      dout(10) << " pg " << pgid << " dne, and pg has changed in "
	       << history.same_interval_since
	       << " (msg from " << it->second.epoch_sent << ")" << dendl;
      continue;
    }

    assert(role != 0);
    dout(10) << " pg " << pgid << " dne" << dendl;
    pg_info_t empty(pgid);
    if (it->second.type == pg_query_t::LOG ||
	it->second.type == pg_query_t::FULLLOG) {
      MOSDPGLog *mlog = new MOSDPGLog(osdmap->get_epoch(), empty,
				      it->second.epoch_sent);
      _share_map_outgoing(osdmap->get_cluster_inst(from));
      cluster_messenger->send_message(mlog,
				      osdmap->get_cluster_inst(from));
    } else {
      notify_list[from].push_back(make_pair(pg_notify_t(it->second.epoch_sent,
							osdmap->get_epoch(),
							empty),
					    pg_interval_map_t()));
    }
  }
  do_notifies(notify_list, osdmap);
}


void OSD::handle_pg_remove(OpRequestRef op)
{
  MOSDPGRemove *m = (MOSDPGRemove *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_REMOVE);
  assert(osd_lock.is_locked());

  if (!require_osd_peer(op))
    return;

  dout(7) << "handle_pg_remove from " << m->get_source() << " on "
	  << m->pg_list.size() << " pgs" << dendl;
  
  if (!require_same_or_newer_map(op, m->get_epoch())) return;
  
  op->mark_started();

  for (vector<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = *it;
    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }
    
    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << dendl;
      continue;
    }
    dout(5) << "queue_pg_for_deletion: " << pgid << dendl;
    PG *pg = _lookup_lock_pg(pgid);
    pg_history_t history = pg->info.history;
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pgid, up, acting);
    project_pg_history(pg->info.pgid, history, pg->get_osdmap()->get_epoch(),
		       up, acting);
    if (history.same_interval_since <= m->get_epoch()) {
      assert(pg->get_primary() == m->get_source().num());
      pg->get();
      _remove_pg(pg);
      pg->unlock();
      pg->put();
    } else {
      dout(10) << *pg << " ignoring remove request, pg changed in epoch "
	       << history.same_interval_since
	       << " > " << m->get_epoch() << dendl;
      pg->unlock();
    }
  }
}

void OSD::_remove_pg(PG *pg)
{
  vector<coll_t> removals;
  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;
  for (interval_set<snapid_t>::iterator p = pg->snap_collections.begin();
       p != pg->snap_collections.end();
       ++p) {
    for (snapid_t cur = p.get_start();
	 cur < p.get_start() + p.get_len();
	 ++cur) {
      coll_t to_remove = get_next_removal_coll(pg->info.pgid);
      removals.push_back(to_remove);
      rmt->collection_rename(coll_t(pg->info.pgid, cur), to_remove);
    }
  }
  coll_t to_remove = get_next_removal_coll(pg->info.pgid);
  removals.push_back(to_remove);
  rmt->collection_rename(coll_t(pg->info.pgid), to_remove);
  if (pg->have_temp_coll()) {
    to_remove = get_next_removal_coll(pg->info.pgid);
    removals.push_back(to_remove);
    rmt->collection_rename(pg->get_temp_coll(), to_remove);
  }
  rmt->remove(coll_t::META_COLL, pg->log_oid);
  rmt->remove(coll_t::META_COLL, pg->biginfo_oid);

  store->queue_transaction(
    pg->osr.get(), rmt,
    new ObjectStore::C_DeleteTransactionHolder<
      SequencerRef>(rmt, pg->osr),
    new ContainerContext<
      SequencerRef>(pg->osr));

  // on_removal, which calls remove_watchers_and_notifies, and the erasure from 
  // the pg_map must be done together without unlocking the pg lock,
  // to avoid racing with watcher cleanup in ms_handle_reset
  // and handle_notify_timeout
  pg->on_removal();

  DeletingStateRef deleting = serviceRef->deleting_pgs.lookup_or_create(pg->info.pgid);
  for (vector<coll_t>::iterator i = removals.begin();
       i != removals.end();
       ++i) {
    remove_wq.queue(new boost::tuple<coll_t, SequencerRef, DeletingStateRef>(
		      *i, pg->osr, deleting));
  }

  recovery_wq.dequeue(pg);
  scrub_wq.dequeue(pg);
  scrub_finalize_wq.dequeue(pg);
  snap_trim_wq.dequeue(pg);
  pg_stat_queue_dequeue(pg);
  op_wq.dequeue(pg);
  peering_wq.dequeue(pg);

  pg->deleting = true;

  // remove from map
  pg_map.erase(pg->info.pgid);
  pg->put(); // since we've taken it out of map

  serviceRef->unreg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);
}


// =========================================================
// RECOVERY

/*
 * caller holds osd_lock
 */
void OSD::check_replay_queue()
{
  assert(osd_lock.is_locked());

  utime_t now = ceph_clock_now(g_ceph_context);
  list< pair<pg_t,utime_t> > pgids;
  replay_queue_lock.Lock();
  while (!replay_queue.empty() &&
	 replay_queue.front().second <= now) {
    pgids.push_back(replay_queue.front());
    replay_queue.pop_front();
  }
  replay_queue_lock.Unlock();

  for (list< pair<pg_t,utime_t> >::iterator p = pgids.begin(); p != pgids.end(); p++) {
    pg_t pgid = p->first;
    if (pg_map.count(pgid)) {
      PG *pg = _lookup_lock_pg_with_map_lock_held(pgid);
      dout(10) << "check_replay_queue " << *pg << dendl;
      if (pg->is_active() &&
	  pg->is_replay() &&
	  pg->get_role() == 0 &&
	  pg->replay_until == p->second) {
	pg->replay_queued_ops();
      }
      pg->unlock();
    } else {
      dout(10) << "check_replay_queue pgid " << pgid << " (not found)" << dendl;
    }
  }
  
  // wake up _all_ pg waiters; raw pg -> actual pg mapping may have shifted
  wake_all_pg_waiters();
}


bool OSDService::queue_for_recovery(PG *pg)
{
  bool b = recovery_wq.queue(pg);
  if (b)
    dout(10) << "queue_for_recovery queued " << *pg << dendl;
  else
    dout(10) << "queue_for_recovery already queued " << *pg << dendl;
  return b;
}

bool OSD::_recover_now()
{
  if (recovery_ops_active >= g_conf->osd_recovery_max_active) {
    dout(15) << "_recover_now active " << recovery_ops_active
	     << " >= max " << g_conf->osd_recovery_max_active << dendl;
    return false;
  }
  if (ceph_clock_now(g_ceph_context) < defer_recovery_until) {
    dout(15) << "_recover_now defer until " << defer_recovery_until << dendl;
    return false;
  }

  return true;
}

void OSD::do_recovery(PG *pg)
{
  // see how many we should try to start.  note that this is a bit racy.
  recovery_wq.lock();
  int max = g_conf->osd_recovery_max_active - recovery_ops_active;
  recovery_wq.unlock();
  if (max == 0) {
    dout(10) << "do_recovery raced and failed to start anything; requeuing " << *pg << dendl;
    recovery_wq.queue(pg);
  } else {
    pg->lock();
    if (pg->deleting || !(pg->is_active() && pg->is_primary())) {
      pg->unlock();
      return;
    }
    
    dout(10) << "do_recovery starting " << max
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;
#ifdef DEBUG_RECOVERY_OIDS
    dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
#endif
    
    PG::RecoveryCtx rctx = create_context();
    int started = pg->start_recovery_ops(max, &rctx);
    dout(10) << "do_recovery started " << started
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;

    /*
     * if we couldn't start any recovery ops and things are still
     * unfound, see if we can discover more missing object locations.
     * It may be that our initial locations were bad and we errored
     * out while trying to pull.
     */
    if (!started && pg->have_unfound()) {
      pg->discover_all_missing(*rctx.query_map);
      if (!rctx.query_map->size()) {
	dout(10) << "do_recovery  no luck, giving up on this pg for now" << dendl;
	recovery_wq.lock();
	recovery_wq._dequeue(pg);
	recovery_wq.unlock();
      }
    }

    pg->write_if_dirty(*rctx.transaction);
    OSDMapRef curmap = pg->get_osdmap();
    pg->unlock();
    dispatch_context(rctx, pg, curmap);
  }
}

void OSD::start_recovery_op(PG *pg, const hobject_t& soid)
{
  recovery_wq.lock();
  dout(10) << "start_recovery_op " << *pg << " " << soid
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid) == 0);
  recovery_oids[pg->info.pgid].insert(soid);
#endif

  recovery_wq.unlock();
}

void OSD::finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue)
{
  recovery_wq.lock();
  dout(10) << "finish_recovery_op " << *pg << " " << soid
	   << " dequeue=" << dequeue
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;

  // adjust count
  recovery_ops_active--;
  assert(recovery_ops_active >= 0);

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active oids was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid));
  recovery_oids[pg->info.pgid].erase(soid);
#endif

  if (dequeue)
    recovery_wq._dequeue(pg);
  else {
    recovery_wq._queue_front(pg);
  }

  recovery_wq._wake();
  recovery_wq.unlock();
}

void OSD::defer_recovery(PG *pg)
{
  dout(10) << "defer_recovery " << *pg << dendl;

  // move pg to the end of the queue...
  recovery_wq.queue(pg);
}


// =========================================================
// OPS

void OSDService::reply_op_error(OpRequestRef op, int err)
{
  reply_op_error(op, err, eversion_t());
}

void OSDService::reply_op_error(OpRequestRef op, int err, eversion_t v)
{
  MOSDOp *m = (MOSDOp*)op->request;
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  int flags;
  flags = m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);

  MOSDOpReply *reply = new MOSDOpReply(m, err, osdmap->get_epoch(), flags);
  Messenger *msgr = client_messenger;
  reply->set_version(v);
  if (m->get_source().is_osd())
    msgr = cluster_messenger;
  msgr->send_message(reply, m->get_connection());
}

void OSDService::handle_misdirected_op(PG *pg, OpRequestRef op)
{
  MOSDOp *m = (MOSDOp*)op->request;
  assert(m->get_header().type == CEPH_MSG_OSD_OP);

  if (m->get_map_epoch() < pg->info.history.same_primary_since) {
    dout(7) << *pg << " changed after " << m->get_map_epoch() << ", dropping" << dendl;
    return;
  }

  dout(7) << *pg << " misdirected op in " << m->get_map_epoch() << dendl;
  clog.warn() << m->get_source_inst() << " misdirected " << m->get_reqid()
	      << " pg " << m->get_pg()
	      << " to osd." << whoami
	      << " not " << pg->acting
	      << " in e" << m->get_map_epoch() << "/" << osdmap->get_epoch() << "\n";
  reply_op_error(op, -ENXIO);
}

void OSD::handle_op(OpRequestRef op)
{
  MOSDOp *m = (MOSDOp*)op->request;
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  if (op_is_discardable(m)) {
    return;
  }

  // we don't need encoded payload anymore
  m->clear_payload();

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_map_epoch()))
    return;

  // object name too long?
  if (m->get_oid().name.size() > MAX_CEPH_OBJECT_NAME_LEN) {
    dout(4) << "handle_op '" << m->get_oid().name << "' is longer than "
	    << MAX_CEPH_OBJECT_NAME_LEN << " bytes!" << dendl;
    serviceRef->reply_op_error(op, -ENAMETOOLONG);
    return;
  }

  // blacklisted?
  if (osdmap->is_blacklisted(m->get_source_addr())) {
    dout(4) << "handle_op " << m->get_source_addr() << " is blacklisted" << dendl;
    serviceRef->reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source_inst(), m->get_map_epoch(),
		      (Session *)m->get_connection()->get_priv());

  int r = init_op_flags(m);
  if (r) {
    serviceRef->reply_op_error(op, r);
    return;
  }

  if (m->may_write()) {
    // full?
    if (osdmap->test_flag(CEPH_OSDMAP_FULL) &&
	!m->get_source().is_mds()) {  // FIXME: we'll exclude mds writes for now.
      serviceRef->reply_op_error(op, -ENOSPC);
      return;
    }

    // invalid?
    if (m->get_snapid() != CEPH_NOSNAP) {
      serviceRef->reply_op_error(op, -EINVAL);
      return;
    }

    // too big?
    if (g_conf->osd_max_write_size &&
	m->get_data_len() > g_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      serviceRef->reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }

  // calc actual pgid
  pg_t pgid = m->get_pg();
  int64_t pool = pgid.pool();
  if ((m->get_flags() & CEPH_OSD_FLAG_PGOP) == 0 &&
      osdmap->have_pg_pool(pool))
    pgid = osdmap->raw_pg_to_pg(pgid);

  // get and lock *pg.
  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
  if (!pg) {
    dout(7) << "hit non-existent pg " << pgid << dendl;

    if (osdmap->get_pg_acting_role(pgid, whoami) >= 0) {
      dout(7) << "we are valid target for op, waiting" << dendl;
      waiting_for_pg[pgid].push_back(op);
      op->mark_delayed();
      return;
    }

    // okay, we aren't valid now; check send epoch
    if (m->get_map_epoch() < superblock.oldest_map) {
      dout(7) << "don't have sender's osdmap; assuming it was valid and that client will resend" << dendl;
      return;
    }
    OSDMapRef send_map = get_map(m->get_map_epoch());

    // remap pgid
    pgid = m->get_pg();
    if ((m->get_flags() & CEPH_OSD_FLAG_PGOP) == 0 &&
	send_map->have_pg_pool(pgid.pool()))
      pgid = send_map->raw_pg_to_pg(pgid);
    
    if (send_map->get_pg_acting_role(m->get_pg(), whoami) >= 0) {
      dout(7) << "dropping request; client will resend when they get new map" << dendl;
    } else {
      dout(7) << "we are invalid target" << dendl;
      clog.warn() << m->get_source_inst() << " misdirected " << m->get_reqid()
		  << " pg " << m->get_pg()
		  << " to osd." << whoami
		  << " in e" << osdmap->get_epoch()
		  << ", client e" << m->get_map_epoch()
		  << " pg " << pgid
		  << " features " << m->get_connection()->get_features()
		  << "\n";
      serviceRef->reply_op_error(op, -ENXIO);
    }
    return;
  } else if (!op_has_sufficient_caps(pg, m)) {
    pg->unlock();
    return;
  }


  enqueue_op(pg, op);
  pg->unlock();
}

bool OSD::op_has_sufficient_caps(PG *pg, MOSDOp *op)
{
  Session *session = (Session *)op->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *op << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();
  
  string key = op->get_object_locator().key;
  if (key.length() == 0)
    key = op->get_oid().name;

  bool cap = caps.is_capable(pg->pool.name, pg->pool.auid, key,
			     op->may_read(), op->may_write(), op->require_exec_caps());

  dout(20) << "op_has_sufficient_caps pool=" << pg->pool.id << " (" << pg->pool.name
	   << ") owner=" << pg->pool.auid
	   << " may_read=" << op->may_read()
	   << " may_write=" << op->may_write()
	   << " may_exec=" << op->may_exec()
           << " require_exec_caps=" << op->require_exec_caps()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void OSD::handle_sub_op(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);

  dout(10) << "handle_sub_op " << *m << " epoch " << m->map_epoch << dendl;
  if (m->map_epoch < up_epoch) {
    dout(3) << "replica op from before up" << dendl;
    return;
  }

  if (!require_osd_peer(op))
    return;

  // must be a rep op.
  assert(m->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = m->pgid;

  // require same or newer map
  if (!require_same_or_newer_map(op, m->map_epoch))
    return;

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source_inst(), m->map_epoch,
		      (Session*)m->get_connection()->get_priv());

  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
  if (!pg) {
    return;
  }
  enqueue_op(pg, op);
  pg->unlock();
}

void OSD::handle_sub_op_reply(OpRequestRef op)
{
  MOSDSubOpReply *m = (MOSDSubOpReply*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOPREPLY);
  if (m->get_map_epoch() < up_epoch) {
    dout(3) << "replica op reply from before up" << dendl;
    return;
  }

  if (!require_osd_peer(op))
    return;

  // must be a rep op.
  assert(m->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = m->get_pg();

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_map_epoch())) return;

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source_inst(), m->get_map_epoch(),
		      (Session*)m->get_connection()->get_priv());

  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
  if (!pg) {
    return;
  }
  enqueue_op(pg, op);
  pg->unlock();
}

bool OSD::op_is_discardable(MOSDOp *op)
{
  // drop client request if they are not connected and can't get the
  // reply anyway.  unless this is a replayed op, in which case we
  // want to do what we can to apply it.
  if (!op->get_connection()->is_connected() &&
      op->get_version().version == 0) {
    return true;
  }
  return false;
}

/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(PG *pg, OpRequestRef op)
{
  dout(15) << *pg << " enqueue_op " << op->request << " "
           << *(op->request) << dendl;
  assert(pg->is_locked());
  pg->queue_op(op);
}

bool OSD::OpWQ::_enqueue(PG *pg)
{
  pg->get();
  osd->op_queue.push_back(pg);
  osd->op_queue_len++;
  osd->logger->set(l_osd_opq, osd->op_queue_len);
  return true;
}

PG *OSD::OpWQ::_dequeue()
{
  if (osd->op_queue.empty())
    return NULL;
  PG *pg = osd->op_queue.front();
  osd->op_queue.pop_front();
  osd->op_queue_len--;
  osd->logger->set(l_osd_opq, osd->op_queue_len);
  return pg;
}

void OSDService::queue_for_peering(PG *pg)
{
  peering_wq.queue(pg);
}

void OSDService::queue_for_op(PG *pg)
{
  op_wq.queue(pg);
}

void OSD::process_peering_events(const list<PG*> &pgs)
{
  bool need_up_thru = false;
  epoch_t same_interval_since = 0;
  OSDMapRef curmap = serviceRef->get_osdmap();
  PG::RecoveryCtx rctx = create_context();
  for (list<PG*>::const_iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    PG *pg = *i;
    pg->lock();
    curmap = serviceRef->get_osdmap();
    if (pg->deleting) {
      pg->unlock();
      continue;
    }
    advance_pg(curmap->get_epoch(), pg, &rctx);
    if (!pg->peering_queue.empty()) {
      PG::CephPeeringEvtRef evt = pg->peering_queue.front();
      pg->peering_queue.pop_front();
      pg->handle_peering_event(evt, &rctx);
    }
    need_up_thru = pg->need_up_thru || need_up_thru;
    same_interval_since = MAX(pg->info.history.same_interval_since,
			      same_interval_since);
    pg->write_if_dirty(*rctx.transaction);
    dispatch_context_transaction(rctx, pg);
    pg->unlock();
  }
  if (need_up_thru)
    queue_want_up_thru(same_interval_since);
  dispatch_context(rctx, 0, curmap);

  serviceRef->send_pg_temp();
}

/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void OSD::dequeue_op(PG *pg)
{
  OpRequestRef op;

  pg->lock();
  if (pg->deleting) {
    pg->unlock();
    pg->put();
    return;
  }
  assert(!pg->op_queue.empty());
  op = pg->op_queue.front();
  pg->op_queue.pop_front();
    
  dout(10) << "dequeue_op " << *op->request << " pg " << *pg << dendl;

  op->mark_reached_pg();

  pg->do_request(op);

  // unlock and put pg
  pg->unlock();
  pg->put();
  
  //#warning foo
  //scrub_wq.queue(pg);

  // finish
  dout(10) << "dequeue_op " << op << " finish" << dendl;
}


// --------------------------------

int OSD::init_op_flags(MOSDOp *op)
{
  vector<OSDOp>::iterator iter;

  // did client explicitly set either bit?
  op->rmw_flags = op->get_flags() & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE|CEPH_OSD_FLAG_EXEC|CEPH_OSD_FLAG_EXEC_PUBLIC);

  // implicitly set bits based on op codes, called methods.
  for (iter = op->ops.begin(); iter != op->ops.end(); ++iter) {
    if (iter->op.op & CEPH_OSD_OP_MODE_WR)
      op->rmw_flags |= CEPH_OSD_FLAG_WRITE;
    if (iter->op.op & CEPH_OSD_OP_MODE_RD)
      op->rmw_flags |= CEPH_OSD_FLAG_READ;

    // set READ flag if there are src_oids
    if (iter->soid.oid.name.length())
      op->rmw_flags |= CEPH_OSD_FLAG_READ;

    // set PGOP flag if there are PG ops
    if (ceph_osd_op_type_pg(iter->op.op))
      op->rmw_flags |= CEPH_OSD_FLAG_PGOP;

    switch (iter->op.op) {
    case CEPH_OSD_OP_CALL:
      {
	bufferlist::iterator bp = iter->indata.begin();
	int is_write, is_read, is_public;
	string cname, mname;
	bp.copy(iter->op.cls.class_len, cname);
	bp.copy(iter->op.cls.method_len, mname);

	ClassHandler::ClassData *cls;
	int r = class_handler->open_class(cname, &cls);
	if (r)
	  return r;
	int flags = cls->get_method_flags(mname.c_str());
	is_read = flags & CLS_METHOD_RD;
	is_write = flags & CLS_METHOD_WR;
        is_public = flags & CLS_METHOD_PUBLIC;

	dout(10) << "class " << cname << " method " << mname
		<< " flags=" << (is_read ? "r" : "") << (is_write ? "w" : "") << dendl;
	if (is_read)
	  op->rmw_flags |= CEPH_OSD_FLAG_READ;
	if (is_write)
	  op->rmw_flags |= CEPH_OSD_FLAG_WRITE;
        if (is_public)
	  op->rmw_flags |= CEPH_OSD_FLAG_EXEC_PUBLIC;
        else
	  op->rmw_flags |= CEPH_OSD_FLAG_EXEC;
	break;
      }
      
    default:
      break;
    }
  }

  return 0;
}
