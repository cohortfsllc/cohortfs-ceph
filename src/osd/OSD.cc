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
#include <ctype.h>
#include <boost/scoped_ptr.hpp>

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN || __FreeBSD__

#include "include/types.h"
#include "include/compat.h"

#include "OSD.h"
#include "OSDMap.h"

#include "common/ceph_argparse.h"
#include "common/version.h"
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
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDMap.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDAlive.h"
#include "messages/MMonCommand.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
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

#include "common/cmdparse.h"
#include "include/str_list.h"

#include "common/config.h"
#include "SnapMapper.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, get_osdmap())

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
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER);
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

OSDService::OSDService(OSD *osd) :
  osd(osd),
  whoami(osd->whoami), store(osd->store), clog(osd->clog),
  infos_oid(OSD::make_infos_oid()),
  cluster_messenger(osd->cluster_messenger),
  client_messenger(osd->client_messenger),
  logger(osd->logger),
  monc(osd->monc),
  class_handler(osd->class_handler),
  publish_lock("OSDService::publish_lock"),
  pre_publish_lock("OSDService::pre_publish_lock"),
  sched_scrub_lock("OSDService::sched_scrub_lock"), scrubs_pending(0),
  scrubs_active(0),
  watch_lock("OSD::watch_lock"),
  watch_timer(osd->client_messenger->cct, watch_lock),
  next_notif_id(0),
  backfill_request_lock("OSD::backfill_request_lock"),
  backfill_request_timer(g_ceph_context, backfill_request_lock, false),
  last_tid(0),
  tid_lock("OSDService::tid_lock"),
  map_cache_lock("OSDService::map_lock"),
  map_cache(g_conf->osd_map_cache_size),
  map_bl_cache(g_conf->osd_map_cache_size),
  map_bl_inc_cache(g_conf->osd_map_cache_size),
  full_status_lock("OSDService::full_status_lock"),
  cur_state(NONE),
  last_msg(0),
  cur_ratio(0),
  is_stopping_lock("OSDService::is_stopping_lock"),
  state(NOT_STOPPING)
{}

void OSDService::need_heartbeat_peer_update()
{
  osd->need_heartbeat_peer_update();
}

void OSDService::shutdown()
{
  shutdown_sub();
  osdmap = OSDMapRef();
  next_osdmap = OSDMapRef();
}

void OSDService::init()
{
  init_sub();
}

ObjectStore *OSD::create_object_store(const uuid_d &vol,
				      const std::string &dev,
				      const std::string &jdev)
{
  struct stat st;
  if (::stat(dev.c_str(), &st) != 0)
    return 0;

  if (g_conf->filestore)
    return new FileStore(vol, dev, jdev);

  if (S_ISDIR(st.st_mode))
    return new FileStore(vol, dev, jdev);
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

  recursive_remove_collection(store, tmp1);
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
    if (i->to_str() == "convertfs_temp" ||
	i->to_str() == "convertfs_temp1")
      recursive_remove_collection(store, *i);
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

int OSD::convertfs(const uuid_d &vol, const std::string &dev,
		   const std::string &jdev)
{
  boost::scoped_ptr<ObjectStore> store(
    new FileStore(vol, dev, jdev, "filestore", true));
  int r = do_convertfs(store.get());
  return r;
}

int OSD::mkfs(const uuid_d &vol, const std::string &dev,
	      const std::string &jdev, uuid_d fsid, int whoami)
{
  int ret;
  ObjectStore *store = NULL;

  try {
    store = create_object_store(vol, dev, jdev);
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
	object_t oid(0, "disk_bw_test");
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

int OSD::mkjournal(const uuid_d &vol, const std::string &dev,
		   const std::string &jdev)
{
  ObjectStore *store = create_object_store(vol, dev, jdev);
  if (!store)
    return -ENOENT;
  return store->mkjournal();
}

int OSD::flushjournal(const uuid_d &vol, const std::string &dev,
		      const std::string &jdev)
{
  ObjectStore *store = create_object_store(vol, dev, jdev);
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

int OSD::dump_journal(const uuid_d &vol, const std::string &dev,
		      const std::string &jdev, ostream& out)
{
  ObjectStore *store = create_object_store(vol, dev, jdev);
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
  // make sure we don't try to use aio or direct_io (and get annoying
  // error messages from failing to do so); performance implications
  // should be irrelevant for this use
  FileJournal j(fsid, 0, 0, path.c_str(), false, false);
  return j.peek_fsid(fsid);
}


#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, osdmap)

// cons/des

OSD::OSD(int id, Messenger *internal_messenger, Messenger *external_messenger,
	 Messenger *hb_clientm,
	 Messenger *hb_front_serverm,
	 Messenger *hb_back_serverm,
	 MonClient *mc,
	 const std::string &dev, const std::string &jdev) :
  Dispatcher(external_messenger->cct),
  osd_lock("OSD::osd_lock"),
  tick_timer(external_messenger->cct, osd_lock),
  authorize_handler_cluster_registry(new AuthAuthorizeHandlerRegistry(external_messenger->cct,
								      cct->_conf->auth_supported.length() ?
								      cct->_conf->auth_supported :
								      cct->_conf->auth_cluster_required)),
  authorize_handler_service_registry(new AuthAuthorizeHandlerRegistry(external_messenger->cct,
								      cct->_conf->auth_supported.length() ?
								      cct->_conf->auth_supported :
								      cct->_conf->auth_service_required)),
  cluster_messenger(internal_messenger),
  client_messenger(external_messenger),
  monc(mc),
  logger(NULL),
  store(NULL),
  clog(external_messenger->cct, client_messenger, &mc->monmap, LogClient::NO_FLAGS),
  whoami(id),
  dev_path(dev), journal_path(jdev),
  dispatch_running(false),
  asok_hook(NULL),
  osd_compat(get_osd_compat_set()),
  state(STATE_INITIALIZING), boot_epoch(0), up_epoch(0), bind_epoch(0),
  op_tp(external_messenger->cct, "OSD::op_tp", g_conf->osd_op_threads, "osd_op_threads"),
  recovery_tp(external_messenger->cct, "OSD::recovery_tp", g_conf->osd_recovery_threads, "osd_recovery_threads"),
  disk_tp(external_messenger->cct, "OSD::disk_tp", g_conf->osd_disk_threads, "osd_disk_threads"),
  command_tp(external_messenger->cct, "OSD::command_tp", 1),
  paused_recovery(false),
  heartbeat_lock("OSD::heartbeat_lock"),
  heartbeat_stop(false), heartbeat_need_update(true), heartbeat_epoch(0),
  hbclient_messenger(hb_clientm),
  hb_front_server_messenger(hb_front_serverm),
  hb_back_server_messenger(hb_back_serverm),
  heartbeat_thread(this),
  heartbeat_dispatcher(this),
  stat_lock("OSD::stat_lock"),
  finished_lock("OSD::finished_lock"),
  test_ops_hook(NULL),
  map_lock("OSD::map_lock"),
  peer_map_epoch_lock("OSD::peer_map_epoch_lock"),
  up_thru_wanted(0), up_thru_pending(0),
  command_wq(this, g_conf->osd_command_thread_timeout, &command_tp),
  osd_stat_updated(false)
{
  monc->set_messenger(client_messenger);
}

OSD::~OSD()
{
  delete authorize_handler_cluster_registry;
  delete authorize_handler_service_registry;
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
  shutdown();
}

int OSD::pre_init()
{
  Mutex::Locker lock(osd_lock);
  if (is_stopping())
    return 0;


  assert(!store);
#warning Volume stuff!  Revisit
  store = create_object_store(uuid_d(), dev_path, journal_path);
  if (!store) {
    derr << "OSD::pre_init: unable to create object store" << dendl;
    return -ENODEV;
  }

  if (store->test_mount_in_use()) {
    derr << "OSD::pre_init: object store '" << dev_path << "' is "
         << "currently in use. (Is ceph-osd already running?)" << dendl;
    return -EBUSY;
  }

  g_conf->add_observer(this);
  return 0;
}

// asok

class OSDSocketHook : public AdminSocketHook {
  OSD *osd;
public:
  OSDSocketHook(OSD *o) : osd(o) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    bool r = osd->asok_command(command, args, ss);
    out.append(ss);
    return r;
  }
};

bool OSD::asok_command(string command, string args, ostream& ss)
{
  if (command == "dump_ops_in_flight") {
    op_tracker.dump_ops_in_flight(ss);
  } else if (command == "dump_historic_ops") {
    op_tracker.dump_historic_ops(ss);
  } else if (command == "dump_blacklist") {
    list<pair<entity_addr_t,utime_t> > bl;
    OSDMapRef curmap = service->get_osdmap();

    JSONFormatter f(true);
    f.open_array_section("blacklist");
    curmap->get_blacklist(&bl);
    for (list<pair<entity_addr_t,utime_t> >::iterator it = bl.begin();
	it != bl.end(); ++it) {
      f.open_array_section("entry");
      f.open_object_section("entity_addr_t");
      it->first.dump(&f);
      f.close_section(); //entity_addr_t
      it->second.localtime(f.dump_stream("expire_time"));
      f.close_section(); //entry
    }
    f.close_section(); //blacklist
    f.flush(ss);
  } else if (!asok_command_sub(command, args, ss)) {
    assert(0 == "broken asok registration");
  }
  return true;
}

class TestOpsSocketHook : public AdminSocketHook {
  OSDService *service;
  ObjectStore *store;
public:
  TestOpsSocketHook(OSDService *s, ObjectStore *st) : service(s), store(st) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    out.append(ss);
    return true;
  }
};

int OSD::init_super()
{
  Mutex::Locker lock(osd_lock);
  if (is_stopping())
    return 0;

  service->backfill_request_timer.init();

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

  // make sure info object exists
  if (!store->exists(coll_t::META_COLL, service->infos_oid)) {
    dout(10) << "init creating/touching snapmapper object" << dendl;
    ObjectStore::Transaction t;
    t.touch(coll_t::META_COLL, service->infos_oid);
    r = store->apply_transaction(t);
    if (r < 0)
      return r;
  }

  // make sure snap mapper object exists
  if (!store->exists(coll_t::META_COLL, OSD::make_snapmapper_oid())) {
    dout(10) << "init creating/touching infos object" << dendl;
    ObjectStore::Transaction t;
    t.touch(coll_t::META_COLL, OSD::make_snapmapper_oid());
    r = store->apply_transaction(t);
    if (r < 0)
      return r;
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

  class_handler = new ClassHandler();
  cls_initialize(class_handler);

  // load up "current" osdmap
  assert_warn(!osdmap);
  if (osdmap) {
    derr << "OSD::init: unable to read current osdmap" << dendl;
    return -EINVAL;
  }
  osdmap = get_map(superblock.current_epoch);
  check_osdmap_features();

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
  hb_front_server_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hb_back_server_messenger->add_dispatcher_head(&heartbeat_dispatcher);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  r = monc->init();
  if (r < 0)
    return r;

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&clog);

  op_tp.start();
  recovery_tp.start();
  disk_tp.start();
  command_tp.start();

  // start the heartbeat
  heartbeat_thread.create();

  // tick
  tick_timer.add_event_after(g_conf->osd_heartbeat_interval, new C_Tick(this));

  AdminSocket *admin_socket = cct->get_admin_socket();
  asok_hook = new OSDSocketHook(this);
  r = admin_socket->register_command("dump_ops_in_flight",
				     "dump_ops_in_flight", asok_hook,
				     "show the ops currently in flight");
  assert(r == 0);
  r = admin_socket->register_command("dump_historic_ops", "dump_historic_ops",
				     asok_hook,
				     "show slowest recent ops");
  assert(r == 0);
  r = admin_socket->register_command("dump_op_pq_state", "dump_op_pq_state",
				     asok_hook,
				     "dump op priority queue state");
  assert(r == 0);
  r = admin_socket->register_command("dump_blacklist", "dump_blacklist",
				     asok_hook,
				     "dump blacklisted clients and times");
  assert(r == 0);

  test_ops_hook = new TestOpsSocketHook(this->service.get(), this->store);
  r = admin_socket->register_command(
   "setomapval",
   "setomapval " \
   "name=pool,type=CephPoolname " \
   "name=objname,type=CephObjectname " \
   "name=key,type=CephString "\
   "name=val,type=CephString",
   test_ops_hook,
   "set omap key");
  assert(r == 0);
  r = admin_socket->register_command(
    "rmomapkey",
    "rmomapkey " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname " \
    "name=key,type=CephString",
    test_ops_hook,
    "remove omap key");
  assert(r == 0);
  r = admin_socket->register_command(
    "setomapheader",
    "setomapheader " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname " \
    "name=header,type=CephString",
    test_ops_hook,
    "set omap header");
  assert(r == 0);

  r = admin_socket->register_command(
    "getomap",
    "getomap " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname",
    test_ops_hook,
    "output entire object map");
  assert(r == 0);

  r = admin_socket->register_command(
    "truncobj",
    "truncobj " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname " \
    "name=len,type=CephInt",
    test_ops_hook,
    "truncate object to length");
  assert(r == 0);

  r = admin_socket->register_command(
    "injectdataerr",
    "injectdataerr " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname",
    test_ops_hook,
    "inject data error into omap");
  assert(r == 0);

  r = admin_socket->register_command(
    "injectmdataerr",
    "injectmdataerr " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname",
    test_ops_hook,
    "inject metadata error");
  assert(r == 0);

  service->init();
  service->publish_map(osdmap);
#warning Volume!
  service->publish_superblock(uuid_d(), superblock);

  osd_lock.Unlock();

  r = monc->authenticate();
  if (r < 0) {
    monc->shutdown();
    store->umount();
    osd_lock.Lock(); // locker is going to unlock this on function exit
    if (is_stopping())
      return 0;
    return r;
  }

  while (monc->wait_auth_rotating(30.0) < 0) {
    derr << "unable to obtain rotating service keys; retrying" << dendl;
  }

  osd_lock.Lock();
  if (is_stopping())
    return 0;

  dout(10) << "ensuring pgs have consumed prior maps" << dendl;
  consume_map();

  dout(10) << "done with init, starting boot process" << dendl;
  state = STATE_BOOTING;
  start_boot();

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
  osd_plb.add_time_avg(l_osd_op_lat,   "op_latency");       // client op latency

  osd_plb.add_u64_counter(l_osd_op_r,      "op_r");        // client reads
  osd_plb.add_u64_counter(l_osd_op_r_outb, "op_r_out_bytes");   // client read out bytes
  osd_plb.add_time_avg(l_osd_op_r_lat,  "op_r_latency");    // client read latency
  osd_plb.add_u64_counter(l_osd_op_w,      "op_w");        // client writes
  osd_plb.add_u64_counter(l_osd_op_w_inb,  "op_w_in_bytes");    // client write in bytes
  osd_plb.add_time_avg(l_osd_op_w_rlat, "op_w_rlat");   // client write readable/applied latency
  osd_plb.add_time_avg(l_osd_op_w_lat,  "op_w_latency");    // client write latency
  osd_plb.add_u64_counter(l_osd_op_rw,     "op_rw");       // client rmw
  osd_plb.add_u64_counter(l_osd_op_rw_inb, "op_rw_in_bytes");   // client rmw in bytes
  osd_plb.add_u64_counter(l_osd_op_rw_outb,"op_rw_out_bytes");  // client rmw out bytes
  osd_plb.add_time_avg(l_osd_op_rw_rlat,"op_rw_rlat");  // client rmw readable/applied latency
  osd_plb.add_time_avg(l_osd_op_rw_lat, "op_rw_latency");   // client rmw latency

  osd_plb.add_u64_counter(l_osd_sop,       "subop");         // subops
  osd_plb.add_u64_counter(l_osd_sop_inb,   "subop_in_bytes");     // subop in bytes
  osd_plb.add_time_avg(l_osd_sop_lat,   "subop_latency");     // subop latency

  osd_plb.add_u64_counter(l_osd_sop_w,     "subop_w");          // replicated (client) writes
  osd_plb.add_u64_counter(l_osd_sop_w_inb, "subop_w_in_bytes");      // replicated write in bytes
  osd_plb.add_time_avg(l_osd_sop_w_lat, "subop_w_latency");      // replicated write latency
  osd_plb.add_u64_counter(l_osd_sop_pull,     "subop_pull");       // pull request
  osd_plb.add_time_avg(l_osd_sop_pull_lat, "subop_pull_latency");
  osd_plb.add_u64_counter(l_osd_sop_push,     "subop_push");       // push (write)
  osd_plb.add_u64_counter(l_osd_sop_push_inb, "subop_push_in_bytes");
  osd_plb.add_time_avg(l_osd_sop_push_lat, "subop_push_latency");

  osd_plb.add_u64_counter(l_osd_pull,      "pull");       // pull requests sent
  osd_plb.add_u64_counter(l_osd_push,      "push");       // push messages
  osd_plb.add_u64_counter(l_osd_push_outb, "push_out_bytes");  // pushed bytes

  osd_plb.add_u64_counter(l_osd_push_in,    "push_in");        // inbound push messages
  osd_plb.add_u64_counter(l_osd_push_inb,   "push_in_bytes");  // inbound pushed bytes

  osd_plb.add_u64_counter(l_osd_rop, "recovery_ops");       // recovery ops (started)

  osd_plb.add_u64(l_osd_loadavg, "loadavg");
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

  // turn off lockdep; the surviving threads tend to fight with exit() below
  g_lockdep = 0;

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
  if (!service->prepare_to_stop())
    return 0; // already shutting down
  osd_lock.Lock();
  if (is_stopping()) {
    osd_lock.Unlock();
    return 0;
  }
  derr << "shutdown" << dendl;

  heartbeat_lock.Lock();
  state = STATE_STOPPING;
  heartbeat_lock.Unlock();

  // Debugging
  g_ceph_context->_conf->set_val("debug_osd", "100");
  g_ceph_context->_conf->set_val("debug_journal", "100");
  g_ceph_context->_conf->set_val("debug_filestore", "100");
  g_ceph_context->_conf->set_val("debug_ms", "100");
  g_ceph_context->_conf->apply_changes(NULL);
  
  // unregister commands
  cct->get_admin_socket()->unregister_command("dump_ops_in_flight");
  cct->get_admin_socket()->unregister_command("dump_historic_ops");
  cct->get_admin_socket()->unregister_command("dump_op_pq_state");
  cct->get_admin_socket()->unregister_command("dump_blacklist");
  cct->get_admin_socket()->unregister_command("dump_watchers");
  delete asok_hook;
  asok_hook = NULL;

  cct->get_admin_socket()->unregister_command("setomapval");
  cct->get_admin_socket()->unregister_command("rmomapkey");
  cct->get_admin_socket()->unregister_command("setomapheader");
  cct->get_admin_socket()->unregister_command("getomap");
  cct->get_admin_socket()->unregister_command("truncobj");
  cct->get_admin_socket()->unregister_command("injectdataerr");
  cct->get_admin_socket()->unregister_command("injectmdataerr");
  delete test_ops_hook;
  test_ops_hook = NULL;

  osd_lock.Unlock();
  service->watch_lock.Lock();
  service->watch_timer.shutdown();
  service->watch_lock.Unlock();

  heartbeat_lock.Lock();
  heartbeat_stop = true;
  heartbeat_cond.Signal();
  heartbeat_lock.Unlock();
  heartbeat_thread.join();

  recovery_tp.drain();
  recovery_tp.stop();
  dout(10) << "recovery tp stopped" << dendl;

  op_tp.drain();
  op_tp.stop();
  dout(10) << "op tp stopped" << dendl;

  command_tp.drain();
  command_tp.stop();
  dout(10) << "command tp stopped" << dendl;

  disk_tp.drain();
  disk_tp.stop();
  dout(10) << "disk tp paused (new), kicking all pgs" << dendl;

  osd_lock.Lock();

  reset_heartbeat_peers();

  tick_timer.shutdown();

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

  dout(10) << "syncing store" << dendl;
  store->flush();
  store->sync();
  store->umount();
  delete store;
  store = 0;
  dout(10) << "Store synced" << dendl;

  g_conf->remove_observer(this);

  monc->shutdown();
  osd_lock.Unlock();

  osdmap = OSDMapRef();
  service->shutdown();
  op_tracker.on_shutdown();

  class_handler->shutdown();
  client_messenger->shutdown();
  cluster_messenger->shutdown();
  hbclient_messenger->shutdown();
  hb_front_server_messenger->shutdown();
  hb_back_server_messenger->shutdown();
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



void OSD::recursive_remove_collection(ObjectStore *store,
				      coll_t tmp)
{
  OSDriver driver(
    store,
    coll_t(),
    make_snapmapper_oid());
  SnapMapper mapper(&driver, 0, 0, 0);

  vector<hobject_t> objects;
  store->collection_list(tmp, objects);

  // delete them.
  ObjectStore::Transaction t;
  unsigned removed = 0;
  for (vector<hobject_t>::iterator p = objects.begin();
       p != objects.end();
       ++p, removed++) {
    OSDriver::OSTransaction _t(driver.get_transaction(&t));
    int r = mapper.remove_oid(*p, &_t);
    if (r != 0 && r != -ENOENT)
      assert(0);
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

float OSDService::get_full_ratio()
{
  float full_ratio = g_conf->osd_failsafe_full_ratio;
  if (full_ratio > 1.0) full_ratio /= 100.0;
  return full_ratio;
}

float OSDService::get_nearfull_ratio()
{
  float nearfull_ratio = g_conf->osd_failsafe_nearfull_ratio;
  if (nearfull_ratio > 1.0) nearfull_ratio /= 100.0;
  return nearfull_ratio;
}

void OSDService::check_nearfull_warning(const osd_stat_t &osd_stat)
{
  Mutex::Locker l(full_status_lock);
  enum s_names new_state;

  time_t now = ceph_clock_gettime(NULL);

  float ratio = ((float)osd_stat.kb_used) / ((float)osd_stat.kb);
  float nearfull_ratio = get_nearfull_ratio();
  float full_ratio = get_full_ratio();
  cur_ratio = ratio;

  if (full_ratio > 0 && ratio > full_ratio) {
    new_state = FULL;
  } else if (nearfull_ratio > 0 && ratio > nearfull_ratio) {
    new_state = NEAR;
  } else {
    cur_state = NONE;
    return;
  }

  if (cur_state != new_state) {
    cur_state = new_state;
  } else if (now - last_msg < g_conf->osd_op_complaint_time) {
    return;
  }
  last_msg = now;
  if (cur_state == FULL)
    clog.error() << "OSD full dropping all updates " << (int)(ratio * 100) << "% full";
  else
    clog.warn() << "OSD near full (" << (int)(ratio * 100) << "%)";
}

bool OSDService::check_failsafe_full()
{
  Mutex::Locker l(full_status_lock);
  if (cur_state == FULL)
    return true;
  return false;
}

bool OSDService::too_full_for_backfill(double *_ratio, double *_max_ratio)
{
  Mutex::Locker l(full_status_lock);
  double max_ratio;
  max_ratio = g_conf->osd_backfill_full_ratio;
  if (_ratio)
    *_ratio = cur_ratio;
  if (_max_ratio)
    *_max_ratio = max_ratio;
  return cur_ratio >= max_ratio;
}

void OSD::update_osd_stat()
{
  // fill in osd stats too
  struct statfs stbuf;
  store->statfs(&stbuf);

  osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
  osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
  osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;

  osd_stat.hb_in.clear();
  for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin(); p != heartbeat_peers.end(); ++p)
    osd_stat.hb_in.push_back(p->first);
  osd_stat.hb_out.clear();

  service->check_nearfull_warning(osd_stat);

  dout(20) << "update_osd_stat " << osd_stat << dendl;
}

void OSD::_add_heartbeat_peer(int p)
{
  if (p == whoami)
    return;
  HeartbeatInfo *hi;

  map<int,HeartbeatInfo>::iterator i = heartbeat_peers.find(p);
  if (i == heartbeat_peers.end()) {
    pair<ConnectionRef,ConnectionRef> cons = service->get_con_osd_hb(p, osdmap->get_epoch());
    if (!cons.first)
      return;
    hi = &heartbeat_peers[p];
    hi->peer = p;
    HeartbeatSession *s = new HeartbeatSession(p);
    hi->con_back = cons.first.get();
    hi->con_back->set_priv(s);
    if (cons.second) {
      hi->con_front = cons.second.get();
      hi->con_front->set_priv(s->get());
      dout(10) << "_add_heartbeat_peer: new peer osd." << p
	       << " " << hi->con_back->get_peer_addr()
	       << " " << hi->con_front->get_peer_addr()
	       << dendl;
    } else {
      hi->con_front.reset(NULL);
      dout(10) << "_add_heartbeat_peer: new peer osd." << p
	       << " " << hi->con_back->get_peer_addr()
	       << dendl;
    }
  } else {
    hi = &i->second;
  }
  hi->epoch = osdmap->get_epoch();
}

void OSD::need_heartbeat_peer_update()
{
  Mutex::Locker l(heartbeat_lock);
  if (is_stopping())
    return;
  dout(20) << "need_heartbeat_peer_update" << dendl;
  heartbeat_need_update = true;
}

void OSD::maybe_update_heartbeat_peers()
{
  assert(osd_lock.is_locked());

  if (is_waiting_for_healthy()) {
    utime_t now = ceph_clock_now(g_ceph_context);
    if (last_heartbeat_resample == utime_t()) {
      last_heartbeat_resample = now;
      heartbeat_need_update = true;
    } else if (!heartbeat_need_update) {
      utime_t dur = now - last_heartbeat_resample;
      if (dur > g_conf->osd_heartbeat_grace) {
	dout(10) << "maybe_update_heartbeat_peers forcing update after " << dur << " seconds" << dendl;
	heartbeat_need_update = true;
	last_heartbeat_resample = now;
	reset_heartbeat_peers();   // we want *new* peers!
      }
    }
  }

  Mutex::Locker l(heartbeat_lock);
  if (!heartbeat_need_update)
    return;
  heartbeat_need_update = false;

  dout(10) << "maybe_update_heartbeat_peers updating" << dendl;

  heartbeat_epoch = osdmap->get_epoch();

  set<int> want, extras;
  int next = osdmap->get_next_up_osd_after(whoami);
  if (next >= 0)
    want.insert(next);
  int prev = osdmap->get_previous_up_osd_before(whoami);
  if (prev >= 0)
    want.insert(prev);

  for (set<int>::iterator p = want.begin(); p != want.end(); ++p) {
    dout(10) << " adding neighbor peer osd." << *p << dendl;
    extras.insert(*p);
    _add_heartbeat_peer(*p);
  }

  // remove down peers; enumerate extras
  map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
  while (p != heartbeat_peers.end()) {
    if (!osdmap->is_up(p->first)) {
      int o = p->first;
      ++p;
      _remove_heartbeat_peer(o);
      continue;
    }
    if (p->second.epoch < osdmap->get_epoch()) {
      extras.insert(p->first);
    }
    ++p;
  }

  // too few?
  int start = osdmap->get_next_up_osd_after(whoami);
  for (int n = start; n >= 0; ) {
    if ((int)heartbeat_peers.size() >= g_conf->osd_heartbeat_min_peers)
      break;
    if (!extras.count(n) && !want.count(n) && n != whoami) {
      dout(10) << " adding random peer osd." << n << dendl;
      extras.insert(n);
      _add_heartbeat_peer(n);
    }
    n = osdmap->get_next_up_osd_after(n);
    if (n == start)
      break;  // came full circle; stop
  }

  // too many?
  for (set<int>::iterator p = extras.begin();
       (int)heartbeat_peers.size() > g_conf->osd_heartbeat_min_peers && p != extras.end();
       ++p) {
    if (want.count(*p))
      continue;
    _remove_heartbeat_peer(*p);
  }

  dout(10) << "maybe_update_heartbeat_peers " << heartbeat_peers.size() << " peers, extras " << extras << dendl;
}

void OSD::reset_heartbeat_peers()
{
  assert(osd_lock.is_locked());
  dout(10) << "reset_heartbeat_peers" << dendl;
  Mutex::Locker l(heartbeat_lock);
  while (!heartbeat_peers.empty()) {
    HeartbeatInfo& hi = heartbeat_peers.begin()->second;
    hbclient_messenger->mark_down(hi.con_back);
    if (hi.con_front) {
      hbclient_messenger->mark_down(hi.con_front);
    }
    heartbeat_peers.erase(heartbeat_peers.begin());
  }
  failure_queue.clear();
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
  if (is_stopping()) {
    heartbeat_lock.Unlock();
    m->put();
    return;
  }

  OSDMapRef curmap = service->get_osdmap();

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

      if (!g_ceph_context->get_heartbeat_map()->is_healthy()) {
	dout(10) << "internal heartbeat not healthy, dropping ping request" << dendl;
	break;
      }

      Message *r = new MOSDPing(monc->get_fsid(),
				curmap->get_epoch(),
				MOSDPing::PING_REPLY,
				m->stamp);
      m->get_connection()->get_messenger()->send_message(r, m->get_connection());

      if (curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active()) {
	  ConnectionRef con
	    = service->get_con_osd_cluster(from,curmap->get_epoch());
	  if (con) {
	    _share_map_outgoing(from, con.get());
	  }
	}
      } else if (!curmap->exists(from) ||
		 curmap->get_down_at(from) > m->map_epoch) {
	// tell them they have died
	Message *r = new MOSDPing(monc->get_fsid(),
				  curmap->get_epoch(),
				  MOSDPing::YOU_DIED,
				  m->stamp);
	m->get_connection()->get_messenger()->send_message(r, m->get_connection());
      }
    }
    break;

  case MOSDPing::PING_REPLY:
    {
      map<int,HeartbeatInfo>::iterator i = heartbeat_peers.find(from);
      if (i != heartbeat_peers.end()) {
	if (m->get_connection() == i->second.con_back) {
	  dout(25) << "handle_osd_ping got reply from osd." << from
		   << " first_rx " << i->second.first_tx
		   << " last_tx " << i->second.last_tx
		   << " last_rx_back " << i->second.last_rx_back << " -> " << m->stamp
		   << " last_rx_front " << i->second.last_rx_front
		   << dendl;
	  i->second.last_rx_back = m->stamp;
	  // if there is no front con, set both stamps.
	  if (i->second.con_front == NULL)
	    i->second.last_rx_front = m->stamp;
	} else if (m->get_connection() == i->second.con_front) {
	  dout(25) << "handle_osd_ping got reply from osd." << from
		   << " first_rx " << i->second.first_tx
		   << " last_tx " << i->second.last_tx
		   << " last_rx_back " << i->second.last_rx_back
		   << " last_rx_front " << i->second.last_rx_front << " -> " << m->stamp
		   << dendl;
	  i->second.last_rx_front = m->stamp;
	}
      }

      if (m->map_epoch &&
	  curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active()) {
	  ConnectionRef con = service->get_con_osd_cluster(from, curmap->get_epoch());
	  if (con) {
	    _share_map_outgoing(from, con.get());
	  }
	}
      }

      utime_t cutoff = ceph_clock_now(g_ceph_context);
      cutoff -= g_conf->osd_heartbeat_grace;
      if (i->second.is_healthy(cutoff)) {
	// Cancel false reports
	if (failure_queue.count(from)) {
	  dout(10) << "handle_osd_ping canceling queued failure report for osd." << from<< dendl;
	  failure_queue.erase(from);
	}
	if (failure_pending.count(from)) {
	  dout(10) << "handle_osd_ping canceling in-flight failure report for osd." << from<< dendl;
	  send_still_alive(curmap->get_epoch(), failure_pending[from]);
	  failure_pending.erase(from);
	}
      }
    }
    break;

  case MOSDPing::YOU_DIED:
    dout(10) << "handle_osd_ping " << m->get_source_inst() << " says i am down in " << m->map_epoch
	     << dendl;
    if (monc->sub_want("osdmap", m->map_epoch, CEPH_SUBSCRIBE_ONETIME))
      monc->renew_subs();
    break;
  }

  heartbeat_lock.Unlock();
  m->put();
}

void OSD::heartbeat()
{
  dout(30) << "heartbeat" << dendl;

  // get CPU load avg
  double loadavgs[1];
  if (getloadavg(loadavgs, 1) == 1)
    logger->set(l_osd_loadavg, 100 * loadavgs[0]);

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
       ++i) {
    int peer = i->first;
    i->second.last_tx = now;
    if (i->second.first_tx == utime_t())
      i->second.first_tx = now;
    dout(30) << "heartbeat sending ping to osd." << peer << dendl;
    hbclient_messenger->send_message(new MOSDPing(monc->get_fsid(),
						  service->get_osdmap()->get_epoch(),
						  MOSDPing::PING,
						  now),
				     i->second.con_back);
    if (i->second.con_front)
      hbclient_messenger->send_message(new MOSDPing(monc->get_fsid(),
						    service->get_osdmap()->get_epoch(),
						    MOSDPing::PING,
						    now),
				       i->second.con_front);
  }

  dout(30) << "heartbeat check" << dendl;
  heartbeat_check();

  logger->set(l_osd_hb_to, heartbeat_peers.size());
  logger->set(l_osd_hb_from, 0);

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

void OSD::heartbeat_entry()
{
  Mutex::Locker l(heartbeat_lock);
  if (is_stopping())
    return;
  while (!heartbeat_stop) {
    heartbeat();

    double wait = .5 + ((float)(rand() % 10)/10.0) * (float)g_conf->osd_heartbeat_interval;
    utime_t w;
    w.set_from_double(wait);
    dout(30) << "heartbeat_entry sleeping for " << wait << dendl;
    heartbeat_cond.WaitInterval(g_ceph_context, heartbeat_lock, w);
    if (is_stopping())
      return;
    dout(30) << "heartbeat_entry woke up" << dendl;
  }
}

void OSD::heartbeat_check()
{
  assert(heartbeat_lock.is_locked());
  utime_t now = ceph_clock_now(g_ceph_context);
  double age = hbclient_messenger->get_dispatch_queue_max_age(now);
  if (age > (g_conf->osd_heartbeat_grace / 2)) {
    derr << "skipping heartbeat_check, hbqueue max age: " << age << dendl;
    return; // hb dispatch is too backed up for our hb status to be meaningful
  }

  // check for incoming heartbeats (move me elsewhere?)
  utime_t cutoff = now;
  cutoff -= g_conf->osd_heartbeat_grace;
  for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
       p != heartbeat_peers.end();
       ++p) {
    dout(25) << "heartbeat_check osd." << p->first
	     << " first_tx " << p->second.first_tx
	     << " last_tx " << p->second.last_tx
	     << " last_rx_back " << p->second.last_rx_back
	     << " last_rx_front " << p->second.last_rx_front
	     << dendl;
    if (p->second.is_unhealthy(cutoff)) {
      if (p->second.last_rx_back == utime_t() ||
	  p->second.last_rx_front == utime_t()) {
	derr << "heartbeat_check: no reply from osd." << p->first
	     << " ever on either front or back, first ping sent " << p->second.first_tx
	     << " (cutoff " << cutoff << ")" << dendl;
	// fail
	failure_queue[p->first] = p->second.last_tx;
      } else {
	derr << "heartbeat_check: no reply from osd." << p->first
	     << " since back " << p->second.last_rx_back
	     << " front " << p->second.last_rx_front
	     << " (cutoff " << cutoff << ")" << dendl;
	// fail
	failure_queue[p->first] = MIN(p->second.last_rx_back, p->second.last_rx_front);
      }
    }
  }
}

// =========================================

void OSD::tick()
{
  utime_t now = ceph_clock_now(g_ceph_context);
  assert(osd_lock.is_locked());
  dout(5) << "tick" << dendl;

  logger->set(l_osd_buf, buffer::get_total_alloc());

  if (is_active() || is_waiting_for_healthy()) {
    map_lock.get_read();

    maybe_update_heartbeat_peers();

    heartbeat_lock.Lock();
    heartbeat_check();
    heartbeat_lock.Unlock();

    if (now - last_mon_report > g_conf->osd_mon_report_interval_min) {
      do_mon_report();
    }

    map_lock.put_read();
  }

  tick_sub(ceph_clock_now(g_ceph_context));

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

  tick_timer.add_event_after(1.0, new C_Tick(this));
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

void OSD::_remove_heartbeat_peer(int n)
{
  map<int,HeartbeatInfo>::iterator q = heartbeat_peers.find(n);
  assert(q != heartbeat_peers.end());
  dout(20) << " removing heartbeat peer osd." << n
	   << " " << q->second.con_back->get_peer_addr()
	   << " " << (q->second.con_front ? q->second.con_front->get_peer_addr() : entity_addr_t())
	   << dendl;
  hbclient_messenger->mark_down(q->second.con_back);
  if (q->second.con_front) {
    hbclient_messenger->mark_down(q->second.con_front);
  }
  heartbeat_peers.erase(q);
}

// =========================================

void OSD::do_mon_report()
{
  dout(7) << "do_mon_report" << dendl;

  utime_t now(ceph_clock_now(g_ceph_context));
  last_mon_report = now;

  // do any pending reports
  send_alive();
  do_mon_report_sub(now);
  send_failures();
}

void OSD::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    Mutex::Locker l(osd_lock);
    if (is_stopping())
      return;
    dout(10) << "ms_handle_connect on mon" << dendl;
    if (is_booting()) {
      start_boot();
    } else {
      send_alive();
      send_failures();
      monc->renew_subs();
    }
  }

  ms_handle_connect_sub(con);
}

bool OSD::ms_handle_reset(Connection *con)
{
  assert(osd_lock.is_locked());
  dout(10) << "reset_heartbeat_peers" << dendl;
  Mutex::Locker l(heartbeat_lock);
  while (!heartbeat_peers.empty()) {
    HeartbeatInfo& hi = heartbeat_peers.begin()->second;
    hbclient_messenger->mark_down(hi.con_back);
    if (hi.con_front) {
      hbclient_messenger->mark_down(hi.con_front);
    }
    heartbeat_peers.erase(heartbeat_peers.begin());
  }
  return true;
};

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
  if (is_stopping())
    return;
  dout(10) << "_maybe_boot mon has osdmaps " << oldest << ".." << newest << dendl;

  if (is_initializing()) {
    dout(10) << "still initializing" << dendl;
    return;
  }

  // if our map within recent history, try to add ourselves to the osdmap.
  if (osdmap->test_flag(CEPH_OSDMAP_NOUP)) {
    dout(5) << "osdmap NOUP flag is set, waiting for it to clear" << dendl;
  } else if (is_waiting_for_healthy() || !_is_healthy()) {
    // if we are not healthy, do not mark ourselves up (yet)
    dout(1) << "not healthy; waiting to boot" << dendl;
    if (!is_waiting_for_healthy())
      start_waiting_for_healthy();
    // send pings sooner rather than later
    heartbeat_kick();
  } else if (osdmap->get_epoch() >= oldest - 1 &&
	     osdmap->get_epoch() + g_conf->osd_map_message_max > newest) {
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

ConnectionRef OSDService::get_con_osd_cluster(int peer, epoch_t from_epoch)
{
  Mutex::Locker l(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    return NULL;
  }
  return osd->cluster_messenger->get_connection(next_osdmap->get_cluster_inst(peer));
}

pair<ConnectionRef,ConnectionRef> OSDService::get_con_osd_hb(int peer, epoch_t from_epoch)
{
  Mutex::Locker l(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  pair<ConnectionRef,ConnectionRef> ret;
  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    return ret;
  }
  ret.first = osd->hbclient_messenger->get_connection(next_osdmap->get_hb_back_inst(peer));
  if (next_osdmap->get_hb_front_addr(peer) != entity_addr_t())
    ret.second = osd->hbclient_messenger->get_connection(next_osdmap->get_hb_front_inst(peer));
  return ret;
}

bool OSD::heartbeat_reset(Connection *con)
{
  HeartbeatSession *s = static_cast<HeartbeatSession*>(con->get_priv());
  if (s) {
    heartbeat_lock.Lock();
    if (is_stopping()) {
      heartbeat_lock.Unlock();
      s->put();
      return true;
    }
    map<int,HeartbeatInfo>::iterator p = heartbeat_peers.find(s->peer);
    if (p != heartbeat_peers.end() &&
	(p->second.con_back == con ||
	 p->second.con_front == con)) {
      dout(10) << "heartbeat_reset failed hb con " << con << " for osd." << p->second.peer
	       << ", reopening" << dendl;
      if (con != p->second.con_back) {
	hbclient_messenger->mark_down(p->second.con_back);
      }
      p->second.con_back.reset(NULL);
      if (p->second.con_front && con != p->second.con_front) {
	hbclient_messenger->mark_down(p->second.con_front);
      }
      p->second.con_front.reset(NULL);
      pair<ConnectionRef,ConnectionRef> newcon = service->get_con_osd_hb(p->second.peer, p->second.epoch);
      if (newcon.first) {
	p->second.con_back = newcon.first.get();
	p->second.con_back->set_priv(s->get());
	if (newcon.second) {
	  p->second.con_front = newcon.second.get();
	  p->second.con_front->set_priv(s->get());
	}
      } else {
	dout(10) << "heartbeat_reset failed hb con " << con << " for osd." << p->second.peer
		 << ", raced with osdmap update, closing out peer" << dendl;
	heartbeat_peers.erase(p);
      }
    } else {
      dout(10) << "heartbeat_reset closing (old) failed hb con " << con << dendl;
    }
    heartbeat_lock.Unlock();
    s->put();
  }
  return true;
}

// =========================================

void OSD::start_waiting_for_healthy()
{
  dout(1) << "start_waiting_for_healthy" << dendl;
  state = STATE_WAITING_FOR_HEALTHY;
  last_heartbeat_resample = utime_t();
}

bool OSD::_is_healthy()
{
  if (!g_ceph_context->get_heartbeat_map()->is_healthy()) {
    dout(1) << "is_healthy false -- internal heartbeat failed" << dendl;
    return false;
  }

  if (is_waiting_for_healthy()) {
    Mutex::Locker l(heartbeat_lock);
    utime_t cutoff = ceph_clock_now(g_ceph_context);
    cutoff -= g_conf->osd_heartbeat_grace;
    int num = 0, up = 0;
    for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
	 p != heartbeat_peers.end();
	 ++p) {
      if (p->second.is_healthy(cutoff))
	++up;
      ++num;
    }
    if (up < num / 3) {
      dout(1) << "is_healthy false -- only " << up << "/" << num << " up peers (less than 1/3)" << dendl;
      return false;
    }
  }

  return true;
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
  entity_addr_t hb_back_addr = hb_back_server_messenger->get_myaddr();
  if (hb_back_addr.is_blank_ip()) {
    int port = hb_back_addr.get_port();
    hb_back_addr = cluster_addr;
    hb_back_addr.set_port(port);
    hb_back_server_messenger->set_addr_unknowns(hb_back_addr);
    dout(10) << " assuming hb_back_addr ip matches cluster_addr" << dendl;
  }
  entity_addr_t hb_front_addr = hb_front_server_messenger->get_myaddr();
  if (hb_front_addr.is_blank_ip()) {
    int port = hb_front_addr.get_port();
    hb_front_addr = client_messenger->get_myaddr();
    hb_front_addr.set_port(port);
    hb_front_server_messenger->set_addr_unknowns(hb_front_addr);
    dout(10) << " assuming hb_front_addr ip matches client_addr" << dendl;
  }

  MOSDBoot *mboot = new MOSDBoot(superblock, boot_epoch, hb_back_addr, hb_front_addr, cluster_addr);
  dout(10) << " client_addr " << client_messenger->get_myaddr()
	   << ", cluster_addr " << cluster_addr
	   << ", hb_back_addr " << hb_back_addr
	   << ", hb_front_addr " << hb_front_addr
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
  assert(osd_lock.is_locked());
  bool locked = false;
  if (!failure_queue.empty()) {
    heartbeat_lock.Lock();
    locked = true;
  }
  utime_t now = ceph_clock_now(g_ceph_context);
  while (!failure_queue.empty()) {
    int osd = failure_queue.begin()->first;
    int failed_for = (int)(double)(now - failure_queue.begin()->second);
    entity_inst_t i = osdmap->get_inst(osd);
    monc->send_mon_message(new MOSDFailure(monc->get_fsid(), i, failed_for, osdmap->get_epoch()));
    failure_pending[osd] = i;
    failure_queue.erase(osd);
  }
  if (locked) heartbeat_lock.Unlock();
}

void OSD::send_still_alive(epoch_t epoch, const entity_inst_t &i)
{
  MOSDFailure *m = new MOSDFailure(monc->get_fsid(), i, 0, epoch);
  m->is_failed = false;
  monc->send_mon_message(m);
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
  ConnectionRef con = m->get_connection();
  Session *session = static_cast<Session *>(con->get_priv());
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

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), con.get());
  command_wq.queue(c);

  m->put();
}

struct OSDCommand {
  string cmdstring;
  string helpstring;
} osd_commands[] = {

#define COMMAND(parsesig, helptext) \
  {parsesig, helptext},

// yes, these are really pg commands, but there's a limit to how
// much work it's worth.  The OSD returns all of them.

COMMAND("pg " \
	"name=pgid,type=CephPgid " \
	"name=cmd,type=CephChoices,strings=query", \
	"show details of a specific pg")
COMMAND("pg " \
	"name=pgid,type=CephPgid " \
	"name=cmd,type=CephChoices,strings=mark_unfound_lost " \
	"name=mulcmd,type=CephChoices,strings=revert", \
	"mark all unfound objects in this pg as lost, either removing or reverting to a prior version if one is available")
COMMAND("pg " \
	"name=pgid,type=CephPgid " \
	"name=cmd,type=CephChoices,strings=list_missing " \
	"name=offset,type=CephString,req=false",
	"list missing objects on this pg, perhaps starting at an offset given in JSON")

COMMAND("version", "report version of OSD")
COMMAND("injectargs " \
	"name=injected_args,type=CephString,n=N",
	"inject configuration arguments into running OSD")
COMMAND("bench " \
	"name=count,type=CephInt,req=false " \
	"name=size,type=CephInt,req=false ", \
	"OSD benchmark: write <count> <size>-byte objects, " \
	"(default 1G size 4MB). Results in log.")
COMMAND("flush_pg_stats", "flush pg stats")
COMMAND("debug dump_missing " \
	"name=filename,type=CephFilepath",
	"dump missing objects to a named file")
COMMAND("debug kick_recovery_wq " \
	"name=delay,type=CephInt,range=0",
	"set osd_recovery_delay_start to <val>")
COMMAND("cpu_profiler " \
	"name=arg,type=CephChoices,strings=status|flush",
	"run cpu profiling on daemon")
COMMAND("dump_pg_recovery_stats", "dump pg recovery statistics")
COMMAND("reset_pg_recovery_stats", "reset pg recovery statistics")

};

void OSD::do_command(Connection *con, tid_t tid, vector<string>& cmd, bufferlist& data)
{
  int r = 0;
  stringstream ss, ds;
  string rs;
  bufferlist odata;

  dout(20) << "do_command tid " << tid << " " << cmd << dendl;

  map<string, cmd_vartype> cmdmap;
  string prefix;

  if (cmd.empty()) {
    ss << "no command given";
    goto out;
  }

  if (!cmdmap_from_json(cmd, &cmdmap, ss)) {
    r = -EINVAL;
    goto out;
  }

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  if (prefix == "get_command_descriptions") {
    int cmdnum = 0;
    JSONFormatter *f = new JSONFormatter();
    f->open_object_section("command_descriptions");
    for (OSDCommand *cp = osd_commands;
	 cp < &osd_commands[ARRAY_SIZE(osd_commands)]; cp++) {

      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(f, secname.str(),
				cp->cmdstring, cp->helpstring);
      cmdnum++;
    }
    f->close_section();	// command_descriptions

    f->flush(ds);
    odata.append(ds);
    delete f;
    goto out;
  }

  if (prefix == "version") {
    ds << pretty_version_to_str();
    goto out;
  }
  else if (prefix == "injectargs") {
    vector<string> argsvec;
    cmd_getval(g_ceph_context, cmdmap, "injected_args", argsvec);

    if (argsvec.empty()) {
      r = -EINVAL;
      ss << "ignoring empty injectargs";
      goto out;
    }
    string args = argsvec.front();
    for (vector<string>::iterator a = ++argsvec.begin(); a != argsvec.end(); ++a)
      args += " " + *a;
    osd_lock.Unlock();
    g_conf->injectargs(args, &ss);
    osd_lock.Lock();
  }

  else if (prefix == "bench") {
    int64_t count;
    int64_t bsize;
    // default count 1G, size 4MB
    cmd_getval(g_ceph_context, cmdmap, "count", count, (int64_t)1 << 30);
    cmd_getval(g_ceph_context, cmdmap, "bsize", bsize, (int64_t)4 << 20);

    bufferlist bl;
    bufferptr bp(bsize);
    bp.zero();
    bl.push_back(bp);

    ObjectStore::Transaction *cleanupt = new ObjectStore::Transaction;

    store->sync_and_flush();
    utime_t start = ceph_clock_now(g_ceph_context);
    for (int64_t pos = 0; pos < count; pos += bsize) {
      char nm[30];
      snprintf(nm, sizeof(nm), "disk_bw_test_%lld", (long long)pos);
      object_t oid(0, nm);
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

  else if (prefix == "heap") {
    if (!ceph_using_tcmalloc()) {
      r = -EOPNOTSUPP;
      ss << "could not issue heap profiler command -- not using tcmalloc!";
    } else {
      string heapcmd;
      cmd_getval(g_ceph_context, cmdmap, "heapcmd", heapcmd);
      // XXX 1-element vector, change at callee or make vector here?
      vector<string> heapcmd_vec;
      get_str_vec(heapcmd, heapcmd_vec);
      ceph_heap_profiler_handle_command(heapcmd_vec, ds);
    }
  }

  else if (prefix == "cpu_profiler") {
    string arg;
    cmd_getval(g_ceph_context, cmdmap, "arg", arg);
    vector<string> argvec;
    get_str_vec(arg, argvec);
    cpu_profiler_handle_command(argvec, ds);
  }

  else {
    ss << "unrecognized command! " << cmd;
    r = -EINVAL;
  }

 out:
  rs = ss.str();
  odata.append(ds);
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


bool OSD::_share_map_incoming(entity_name_t name, Connection *con, epoch_t epoch, Session* session)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << name << " " << con->get_peer_addr() << " " << epoch << dendl;
  //assert(osd_lock.is_locked());

  assert(is_active());

  // does client have old map?
  if (name.is_client()) {
    bool sendmap = epoch < osdmap->get_epoch();
    if (sendmap && session) {
      if (session->last_sent_epoch < osdmap->get_epoch()) {
	session->last_sent_epoch = osdmap->get_epoch();
      } else {
	sendmap = false; //we don't need to send it out again
	dout(15) << name << " already sent incremental to update from epoch "<< epoch << dendl;
      }
    }
    if (sendmap) {
      dout(10) << name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, con);
      shared = true;
    }
  }

  // does peer have old map?
  if (name.is_osd() &&
      osdmap->is_up(name.num()) &&
      (osdmap->get_cluster_addr(name.num()) == con->get_peer_addr() ||
       osdmap->get_hb_back_addr(name.num()) == con->get_peer_addr())) {
    // remember
    epoch_t has = note_peer_epoch(name.num(), epoch);

    // share?
    if (has < osdmap->get_epoch()) {
      dout(10) << name << " " << con->get_peer_addr() << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      note_peer_epoch(name.num(), osdmap->get_epoch());
      send_incremental_map(epoch, con);
      shared = true;
    }
  }

  if (session)
    session->put();
  return shared;
}


void OSD::_share_map_outgoing(int peer, Connection *con, OSDMapRef map)
{
  if (!map)
    map = service->get_osdmap();

  // send map?
  epoch_t pe = get_peer_epoch(peer);
  if (pe) {
    if (pe < map->get_epoch()) {
      send_incremental_map(pe, con);
      note_peer_epoch(peer, map->get_epoch());
    } else
      dout(20) << "_share_map_outgoing " << con << " already has epoch " << pe << dendl;
  } else {
    dout(20) << "_share_map_outgoing " << con << " don't know epoch, doing nothing" << dendl;
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
    handle_osd_ping(static_cast<MOSDPing*>(m));
    break;

  case CEPH_MSG_OSD_MAP:
    {
      ConnectionRef self = cluster_messenger->get_loopback_connection();
      cluster_messenger->send_message(m, self);
    }
    break;

  default:
    dout(0) << "dropping unexpected message " << *m << " from " << m->get_source_inst() << dendl;
    m->put();
  }

  return true;
}

bool OSD::ms_dispatch(Message *m)
{
  if (m->get_type() == MSG_OSD_MARK_ME_DOWN) {
    service->got_stop_ack();
    m->put();
    return true;
  }

  // lock!

  osd_lock.Lock();
  if (is_stopping()) {
    osd_lock.Unlock();
    m->put();
    return true;
  }

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
			       bool& isvalid, CryptoKey& session_key)
{
  AuthAuthorizeHandler *authorize_handler = 0;
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
    /*
     * note: mds is technically a client from our perspective, but
     * this makes the 'cluster' consistent w/ monitor's usage.
     */
  case CEPH_ENTITY_TYPE_OSD:
    authorize_handler = authorize_handler_cluster_registry->get_handler(protocol);
    break;
  default:
    authorize_handler = authorize_handler_service_registry->get_handler(protocol);
  }
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
						 authorizer_data, authorizer_reply, name, global_id, caps_info, session_key, &auid);

  if (isvalid) {
    Session *s = static_cast<Session *>(con->get_priv());
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
  
  dout(10) << "do_waiters -- start" << dendl;
  finished_lock.Lock();
  while (!finished.empty()) {
    OpRequestRef next = finished.front();
    finished.pop_front();
    finished_lock.Unlock();
    dispatch_op(next);
    finished_lock.Lock();
  }
  finished_lock.Unlock();
  dout(10) << "do_waiters -- finish" << dendl;
}

void OSD::dispatch_op(OpRequestRef op)
{
  switch (op->request->get_type()) {

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
  default:
    dispatch_op_sub(op);
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
    handle_osd_map(static_cast<MOSDMap*>(m));
    break;

    // osd
  case CEPH_MSG_SHUTDOWN:
    session = static_cast<Session *>(m->get_connection()->get_priv());
    if (!session ||
	session->entity_name.is_mon() ||
	session->entity_name.is_osd())
      shutdown();
    else dout(0) << "shutdown message from connection with insufficient privs!"
		 << m->get_connection() << dendl;
    m->put();
    if (session)
      session->put();
    break;

  case MSG_MON_COMMAND:
    handle_command(static_cast<MMonCommand*>(m));
    break;
  case MSG_COMMAND:
    handle_command(static_cast<MCommand*>(m));
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

bool OSD::scrub_random_backoff()
{
  bool coin_flip = (rand() % 3) == whoami % 3;
  if (!coin_flip) {
    dout(20) << "scrub_random_backoff lost coin flip, randomly backing off" << dendl;
    return true;
  }
  return false;
}

bool OSD::scrub_should_schedule()
{
  double loadavgs[1];
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

bool OSDService::prepare_to_stop()
{
  Mutex::Locker l(is_stopping_lock);
  if (state != NOT_STOPPING)
    return false;

  if (get_osdmap()->is_up(whoami)) {
    state = PREPARING_TO_STOP;
    monc->send_mon_message(new MOSDMarkMeDown(monc->get_fsid(),
					      get_osdmap()->get_inst(whoami),
					      get_osdmap()->get_epoch(),
					      false
					      ));
    utime_t now = ceph_clock_now(g_ceph_context);
    utime_t timeout;
    timeout.set_from_double(now + g_conf->osd_mon_shutdown_timeout);
    while ((ceph_clock_now(g_ceph_context) < timeout) &&
	   (state != STOPPING)) {
      is_stopping_cond.WaitUntil(is_stopping_lock, timeout);
    }
  }
  state = STOPPING;
  return true;
}

void OSDService::got_stop_ack()
{
  Mutex::Locker l(is_stopping_lock);
  dout(10) << "Got stop ack" << dendl;
  state = STOPPING;
  is_stopping_cond.Signal();
}


// =====================================================
// MAP

void OSD::wait_for_new_map(OpRequestRef op)
{
#warning Do volmap in here, too.
  // ask?
  if (waiting_for_osdmap.empty()) {
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
  
  waiting_for_osdmap.push_back(op);
  op->mark_delayed("wait for new map");
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */

void OSD::note_down_osd(int peer)
{
  assert(osd_lock.is_locked());
  cluster_messenger->mark_down(osdmap->get_cluster_addr(peer));

  heartbeat_lock.Lock();
  failure_queue.erase(peer);
  failure_pending.erase(peer);
  map<int,HeartbeatInfo>::iterator p = heartbeat_peers.find(peer);
  if (p != heartbeat_peers.end()) {
    hbclient_messenger->mark_down(p->second.con_back);
    if (p->second.con_front) {
      hbclient_messenger->mark_down(p->second.con_front);
    }
    heartbeat_peers.erase(p);
  }
  heartbeat_lock.Unlock();
}

void OSD::note_up_osd(int peer)
{
  forget_peer_epoch(peer, osdmap->get_epoch() - 1);
}

struct C_OnMapApply : public Context {
  OSDService *service;
  boost::scoped_ptr<ObjectStore::Transaction> t;
  list<OSDMapRef> pinned_maps;
  epoch_t e;
  C_OnMapApply(OSDService *service,
	       ObjectStore::Transaction *t,
	       const list<OSDMapRef> &pinned_maps,
	       epoch_t e)
    : service(service), t(t), pinned_maps(pinned_maps), e(e) {}
  void finish(int r) {
    service->clear_map_bl_cache_pins(e);
  }
};

void OSD::handle_osd_map(MOSDMap *m)
{
  assert(osd_lock.is_locked());
  list<OSDMapRef> pinned_maps;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
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

  logger->inc(l_osd_map);
  logger->inc(l_osd_mape, last - first + 1);
  if (first <= osdmap->get_epoch())
    logger->inc(l_osd_mape_dup, osdmap->get_epoch() - first + 1);

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

  ObjectStore::Transaction *_t = new ObjectStore::Transaction;
  ObjectStore::Transaction &t = *_t;

  // store new maps: queue for disk and put in the osdmap cache
  epoch_t start = MAX(osdmap->get_epoch() + 1, first);
  for (epoch_t e = start; e <= last; e++) {
    map<epoch_t,bufferlist>::iterator p;
    p = m->maps.find(e);
    if (p != m->maps.end()) {
      dout(10) << "handle_osd_map  got full map for epoch " << e << dendl;
      OSDMap *o = newOSDMap(volmap);
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

      OSDMap *o = newOSDMap(volmap);
      if (e > 1) {
	bufferlist obl;
	OSDMapRef prev = get_map(e - 1);
	prev->encode(obl);
	o->decode(obl);
      }

      OSDMap::Incremental *inc = o->newIncremental();
      bufferlist::iterator p = bl.begin();
      inc->decode(p);
      if (o->apply_incremental(*inc) < 0) {
	derr << "ERROR: bad fsid?  i have " << osdmap->get_fsid()
	     << " and inc has " << inc->fsid << dendl;
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
    int num = 0;
    epoch_t min(
      MIN(m->oldest_map,
	  service->map_cache.cached_key_lower_bound()));
    for (epoch_t e = superblock.oldest_map; e < min; ++e) {
      dout(20) << " removing old osdmap epoch " << e << dendl;
      t.remove(coll_t::META_COLL, get_osdmap_pobject_name(e));
      t.remove(coll_t::META_COLL, get_inc_osdmap_pobject_name(e));
      superblock.oldest_map = e+1;
      num++;
      if (num >= g_conf->osd_target_transaction_size &&
	  (uint64_t)num > (last - first))  // make sure we at least keep pace with incoming maps
	break;
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

    // start blacklisting messages sent to peers that go down.
    service->pre_publish_map(newmap);

    // kill connections to newly down osds
    set<int> old;
    osdmap->get_all_osds(old);
    for (set<int>::iterator p = old.begin(); p != old.end(); ++p) {
      if (*p != whoami &&
	  osdmap->have_inst(*p) &&                        // in old map
	  (!newmap->exists(*p) || !newmap->is_up(*p))) {  // but not the new one
	note_down_osd(*p);
      }
    }
    
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
	       !osdmap->get_hb_back_addr(whoami).probably_equals(hb_back_server_messenger->get_myaddr()) ||
	       (osdmap->get_hb_front_addr(whoami) != entity_addr_t() &&
                !osdmap->get_hb_front_addr(whoami).probably_equals(hb_front_server_messenger->get_myaddr()))) {
      if (!osdmap->is_up(whoami)) {
	if (service->is_preparing_to_stop()) {
	  service->got_stop_ack();
	} else {
	  clog.warn() << "map e" << osdmap->get_epoch()
		      << " wrongly marked me down";
	}
      }
      else if (!osdmap->get_addr(whoami).probably_equals(client_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong client addr (" << osdmap->get_addr(whoami)
		     << " != my " << client_messenger->get_myaddr() << ")";
      else if (!osdmap->get_cluster_addr(whoami).probably_equals(cluster_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong cluster addr (" << osdmap->get_cluster_addr(whoami)
		     << " != my " << cluster_messenger->get_myaddr() << ")";
      else if (!osdmap->get_hb_back_addr(whoami).probably_equals(hb_back_server_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb back addr (" << osdmap->get_hb_back_addr(whoami)
		     << " != my " << hb_back_server_messenger->get_myaddr() << ")";
      else if (osdmap->get_hb_front_addr(whoami) != entity_addr_t() &&
               !osdmap->get_hb_front_addr(whoami).probably_equals(hb_front_server_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb front addr (" << osdmap->get_hb_front_addr(whoami)
		     << " != my " << hb_front_server_messenger->get_myaddr() << ")";

      if (!service->is_stopping()) {
	up_epoch = 0;
	do_restart = true;
	bind_epoch = osdmap->get_epoch();

	start_waiting_for_healthy();

	set<int> avoid_ports;
	avoid_ports.insert(cluster_messenger->get_myaddr().get_port());
	avoid_ports.insert(hb_back_server_messenger->get_myaddr().get_port());
	avoid_ports.insert(hb_front_server_messenger->get_myaddr().get_port());

	int r = cluster_messenger->rebind(avoid_ports);
	if (r != 0)
	  do_shutdown = true;  // FIXME: do_restart?

	r = hb_back_server_messenger->rebind(avoid_ports);
	if (r != 0)
	  do_shutdown = true;  // FIXME: do_restart?

	r = hb_front_server_messenger->rebind(avoid_ports);
	if (r != 0)
	  do_shutdown = true;  // FIXME: do_restart?

	hbclient_messenger->mark_down_all();

	reset_heartbeat_peers();
      }
    }
  }


  // note in the superblock that we were clean thru the prior epoch
  if (boot_epoch && boot_epoch >= superblock.mounted) {
    superblock.mounted = boot_epoch;
    superblock.clean_thru = osdmap->get_epoch();
  }

  // superblock and commit
  write_superblock(t);
  store->queue_transaction(
    0,
    _t,
    new C_OnMapApply(service.get(), _t, pinned_maps, osdmap->get_epoch()),
    0, fin);
#warning Volume!
  service->publish_superblock(uuid_d(), superblock);

  map_lock.put_write();

  check_osdmap_features();

  // yay!
  consume_map();

  if (is_active() || is_waiting_for_healthy())
    maybe_update_heartbeat_peers();

  if (is_active()) {
    activate_map();
  }

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

void OSD::check_osdmap_features()
{
  // adjust required feature bits?

  // we have to be a bit careful here, because we are accessing the
  // Policy structures without taking any lock.  in particular, only
  // modify integer values that can safely be read by a racing CPU.
  // since we are only accessing existing Policy structures a their
  // current memory location, and setting or clearing bits in integer
  // fields, and we are the only writer, this is not a problem.

  uint64_t mask;
  uint64_t features = osdmap->get_features(&mask);

  {
    Messenger::Policy p = client_messenger->get_default_policy();
    if ((p.features_required & mask) != features) {
      dout(0) << "crush map has features " << features
	      << ", adjusting msgr requires for clients" << dendl;
      p.features_required = (p.features_required & ~mask) | features;
      client_messenger->set_default_policy(p);
    }
  }
  {
    Messenger::Policy p = cluster_messenger->get_policy(entity_name_t::TYPE_OSD);
    if ((p.features_required & mask) != features) {
      dout(0) << "crush map has features " << features
	      << ", adjusting msgr requires for osds" << dendl;
      p.features_required = (p.features_required & ~mask) | features;
      cluster_messenger->set_policy(entity_name_t::TYPE_OSD, p);
    }
  }
}


// =====================================================
// MAP

/**
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t, C_Contexts *tfin)
{
  advance_map_sub(t, tfin);
}

void OSD::consume_map()
{
  assert(osd_lock.is_locked());
  dout(7) << "consume_map version " << osdmap->get_epoch() << dendl;
  consume_map_sub();

  service->pre_publish_map(osdmap);
  service->publish_map(osdmap);
}

void OSD::activate_map()
{
  assert(osd_lock.is_locked());

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  maybe_update_heartbeat_peers();

  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " osdmap flagged full, doing onetime osdmap subscribe" << dendl;
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }

  // norecover?
  if (osdmap->test_flag(CEPH_OSDMAP_NORECOVER)) {
    if (!paused_recovery) {
      dout(1) << "pausing recovery (NORECOVER flag set)" << dendl;
      paused_recovery = true;
      recovery_tp.pause_new();
    }
  } else {
    if (paused_recovery) {
      dout(1) << "resuming recovery (NORECOVER flag cleared)" << dendl;
      paused_recovery = false;
      recovery_tp.unpause();
    }
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

void OSD::send_map(MOSDMap *m, Connection *con)
{
  Messenger *msgr = client_messenger;
  if (entity_name_t::TYPE_OSD == con->get_peer_type())
    msgr = cluster_messenger;
  msgr->send_message(m, con);
}

void OSD::send_incremental_map(epoch_t since, Connection *con)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << con << " " << con->get_peer_addr() << dendl;

  if (since < superblock.oldest_map) {
    // just send latest full map
    MOSDMap *m = new MOSDMap(monc->get_fsid());
    m->oldest_map = superblock.oldest_map;
    m->newest_map = superblock.newest_map;
    epoch_t e = osdmap->get_epoch();
    get_map_bl(e, m->maps[e]);
    send_map(m, con);
    return;
  }

  while (since < osdmap->get_epoch()) {
    epoch_t to = osdmap->get_epoch();
    if (to - since > (epoch_t)g_conf->osd_map_message_max)
      to = since + g_conf->osd_map_message_max;
    MOSDMap *m = build_incremental_map_msg(since, to);
    send_map(m, con);
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

void OSDService::clear_map_bl_cache_pins(epoch_t e)
{
  Mutex::Locker l(map_cache_lock);
  map_bl_inc_cache.clear_pinned(e);
  map_bl_cache.clear_pinned(e);
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

  OSDMap *map = newOSDMap(volmap);
  if (epoch > 0) {
    dout(20) << "get_map " << epoch << " - loading and decoding "
	     << map << dendl;
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
bool OSD::require_same_or_newer_map(OpRequestRef op,
				    epoch_t osdmap_epoch,
				    epoch_t volmap_epoch)
{
  Message *m = op->request;
  dout(15) << "require_same_or_newer_maps (" << osdmap_epoch
	   << ", " << volmap_epoch << ") (i am (" << volmap->get_epoch()
	   << ", " << volmap->get_epoch() << ")) " << m << dendl;

  assert(osd_lock.is_locked());

  // do they have a newer map?
  if (osdmap_epoch > osdmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << osdmap_epoch << " > my "
	    << osdmap->get_epoch() << " with " << m << dendl;
    wait_for_new_map(op);
    return false;
  }

  // do they have a newer map?
  if (volmap_epoch > volmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << volmap_epoch << " > my "
	    << volmap->get_epoch() << " with " << m << dendl;
    wait_for_new_map(op);
    return false;
  }

  if (osdmap_epoch < up_epoch) {
    dout(7) << "from pre-up epoch " << osdmap_epoch
	    << " < " << up_epoch << dendl;
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_cluster_addr(from) != m->get_source_inst().addr) {
      dout(10) << "from dead osd." << from << ", marking down" << dendl;
      cluster_messenger->mark_down(m->get_connection());
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

// =========================================================
// OPS

void OSDService::reply_op_error(OpRequestRef op, int err)
{
  reply_op_error(op, err, eversion_t());
}

void OSDService::reply_op_error(OpRequestRef op, int err, eversion_t v)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  int flags;
  flags = m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);

  MOSDOpReply *reply = new MOSDOpReply(m, err, osdmap->get_epoch(),
				       volmap->get_epoch(), flags);
  Messenger *msgr = client_messenger;
  reply->set_version(v);
  if (m->get_source().is_osd())
    msgr = cluster_messenger;
  msgr->send_message(reply, m->get_connection());
}

void OSD::handle_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  if (op_is_discardable(m)) {
    dout(10) << " discardable " << *m << dendl;
    return;
  }

  // we don't need encoded payload anymore
  m->clear_payload();

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_osdmap_epoch(),
				 m->get_volmap_epoch()))
    return;

  // object name too long?
  if (m->get_oid().name.size() > MAX_CEPH_OBJECT_NAME_LEN) {
    dout(4) << "handle_op '" << m->get_oid().name << "' is longer than "
	    << MAX_CEPH_OBJECT_NAME_LEN << " bytes!" << dendl;
    service->reply_op_error(op, -ENAMETOOLONG);
    return;
  }

  // blacklisted?
  if (osdmap->is_blacklisted(m->get_source_addr())) {
    dout(4) << "handle_op " << m->get_source_addr() << " is blacklisted" << dendl;
    service->reply_op_error(op, -EBLACKLISTED);
    return;
  }
  // share our map with sender, if they're old
  _share_map_incoming(m->get_source(), m->get_connection().get(),
		      m->get_osdmap_epoch(),
		      static_cast<Session *>(m->get_connection()->get_priv()));

  if (op->rmw_flags == 0) {
    int r = init_op_flags(op);
    if (r) {
      service->reply_op_error(op, r);
      return;
    }
  }

  if (g_conf->osd_debug_drop_op_probability > 0 &&
      !m->get_source().is_mds()) {
    if ((double)rand() / (double)RAND_MAX < g_conf->osd_debug_drop_op_probability) {
      dout(0) << "handle_op DEBUG artificially dropping op " << *m << dendl;
      return;
    }
  }

  if (op->may_write()) {
    // full?
    if ((service->check_failsafe_full() ||
		  osdmap->test_flag(CEPH_OSDMAP_FULL)) &&
	!m->get_source().is_mds()) {  // FIXME: we'll exclude mds writes for now.
      service->reply_op_error(op, -ENOSPC);
      return;
    }

    // invalid?
    if (m->get_snapid() != CEPH_NOSNAP) {
      service->reply_op_error(op, -EINVAL);
      return;
    }

    // too big?
    if (g_conf->osd_max_write_size &&
	m->get_data_len() > g_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      derr << "handle_op msg data len " << m->get_data_len()
	   << " > osd_max_write_size " << (g_conf->osd_max_write_size << 20)
	   << " on " << *m << dendl;
      service->reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }

  handle_op_sub(op);
}

void OSD::handle_sub_op(OpRequestRef op)
{
  // hand this down to the subclass
  // ignore return value as we having nothing else to try
  handle_sub_op_sub(op);
}

void OSD::handle_sub_op_reply(OpRequestRef op)
{
  // hand this down to the subclass
  // ignore return value as we having nothing else to try
  handle_sub_op_reply_sub(op);
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

// --------------------------------

const char** OSD::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_max_backfills",
    NULL
  };
  return KEYS;
}

void OSD::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed)
{
}

// --------------------------------

int OSD::init_op_flags(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  vector<OSDOp>::iterator iter;

  // client flags have no bearing on whether an op is a read, write, etc.
  op->rmw_flags = 0;

  // set bits based on op codes, called methods.
  for (iter = m->ops.begin(); iter != m->ops.end(); ++iter) {
    if (ceph_osd_op_mode_modify(iter->op.op))
      op->set_write();
    if (ceph_osd_op_mode_read(iter->op.op))
      op->set_read();

    // set READ flag if there are src_oids
    if (iter->soid.oid.name.length())
      op->set_read();

    // set PGOP flag if there are PG ops
    if (ceph_osd_op_type_pg(iter->op.op))
      op->set_pg_op();

    switch (iter->op.op) {
    case CEPH_OSD_OP_CALL:
      {
	bufferlist::iterator bp = iter->indata.begin();
	int is_write, is_read;
	string cname, mname;
	bp.copy(iter->op.cls.class_len, cname);
	bp.copy(iter->op.cls.method_len, mname);

	ClassHandler::ClassData *cls;
	int r = class_handler->open_class(cname, &cls);
	if (r) {
	  derr << "class " << cname << " open got " << cpp_strerror(r) << dendl;
	  if (r == -ENOENT)
	    r = -EOPNOTSUPP;
	  else
	    r = -EIO;
	  return r;
	}
	int flags = cls->get_method_flags(mname.c_str());
	if (flags < 0) {
	  if (flags == -ENOENT)
	    r = -EOPNOTSUPP;
	  else
	    r = flags;
	  return r;
	}
	is_read = flags & CLS_METHOD_RD;
	is_write = flags & CLS_METHOD_WR;

	dout(10) << "class " << cname << " method " << mname
		<< " flags=" << (is_read ? "r" : "") << (is_write ? "w" : "") << dendl;
	if (is_read)
	  op->set_class_read();
	if (is_write)
	  op->set_class_write();
	break;
      }
    default:
      break;
    }
  }

  if (op->rmw_flags == 0)
    return -EINVAL;

  return 0;
}
