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
 * Foundation.	See file COPYING.
 *
 */
#include "acconfig.h"

#include <cassert>
#include <fstream>
#include <iostream>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <errno.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <signal.h>
#include <ctype.h>
#include <boost/scoped_ptr.hpp>

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#include "osd/OSDVol.h"

#include "include/types.h"
#include "include/compat.h"

#include "OSD.h"
#include "OSDMap.h"
#include "Watch.h"
#include "OpQueue.h"

#include "common/ceph_argparse.h"
#include "common/version.h"

#include "os/ObjectStore.h"
#include "os/Transaction.h"

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

#include "messages/MOSDAlive.h"

#include "messages/MMonCommand.h"

#include "messages/MWatchNotify.h"

#include "common/Timer.h"
#include "common/LogClient.h"
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

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, get_osdmap())

/* thread-local Volume cache */
thread_local OSDVol::VolCache OSD::tls_vol_cache;

static ostream& _prefix(std::ostream* _dout, int whoami, OSDMapRef osdmap) {
  return *_dout << "osd." << whoami << " "
		<< (osdmap ? osdmap->get_epoch():0)
		<< " ";
}

//Initial features in new superblock.
CompatSet OSD::get_osd_initial_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG);
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

//Features are added here that this OSD supports.
CompatSet OSD::get_osd_compat_set() {
  CompatSet compat =  get_osd_initial_compat_set();
  return compat;
}

OSDService::OSDService(OSD *osd) :
  osd(osd), lru(20), // More sophisticated later
  cct(osd->cct),
  whoami(osd->whoami), store(osd->store),
  meta_col(nullptr), clog(osd->clog),
  infos_oid(OSD::make_infos_oid()),
  infos_oh(nullptr),
  cluster_messenger(osd->cluster_messenger),
  client_messenger(osd->client_messenger),
  client_xio_messenger(osd->client_xio_messenger),
  monc(osd->monc),
  class_handler(osd->class_handler),
  next_notif_id(0),
  last_tid(0),
  map_cache(cct->_conf->osd_map_cache_size),
  map_bl_cache(cct->_conf->osd_map_cache_size),
  map_bl_inc_cache(cct->_conf->osd_map_cache_size),
  cur_state(NONE),
  cur_ratio(0),
  state(NOT_STOPPING)
{
}

OSDService::~OSDService()
{
}

void OSDService::need_heartbeat_peer_update()
{
  osd->need_heartbeat_peer_update();
}

void OSDService::shutdown()
{
  osdmap = OSDMapRef();
  next_osdmap = OSDMapRef();
}

#undef dout_prefix
#define dout_prefix *_dout

int OSD::mkfs(CephContext *cct, ObjectStore *store, const string &dev,
	      const boost::uuids::uuid& fsid, int whoami)
{
  CollectionHandle meta_col(NULL);
  ObjectHandle oh;
  bool create_superblock = true;
  OSDSuperblock sb;
  bufferlist sbbl;

  // if we are fed a uuid for this osd, use it.
  store->set_fsid(cct->_conf->osd_uuid);

  int ret = store->mkfs();
  if (ret) {
    derr << "OSD::mkfs: ObjectStore::mkfs failed with error " << ret << dendl;
    goto free_store;
  }

  ret = store->mount();
  if (ret) {
    derr << "OSD::mkfs: couldn't mount ObjectStore: error " << ret << dendl;
    goto free_store;
  }
  {
    Transaction t;
    t.create_collection(coll_t::META_COLL);
    ret = store->apply_transaction(t);
  }
  if (ret) {
    derr << "OSD::mkfs: error while creating meta collection: "
      << "apply_transaction returned " << ret << dendl;
    goto umount_store;
  }

  meta_col = store->open_collection(coll_t::META_COLL);
  assert(meta_col);

  oh = store->get_object(meta_col, OSD_SUPERBLOCK_POBJECT);
  if (oh) {
    ret = store->read(meta_col, oh, 0, CEPH_READ_ENTIRE, sbbl);
    store->put_object(oh);
    if (ret >= 0) {
      dout(0) << " have superblock" << dendl;
      if (whoami != sb.whoami) {
	derr << "provided osd id " << whoami << " != superblock's "
	     << sb.whoami << dendl;
	ret = -EINVAL;
	goto umount_store;
      }
      if (fsid != sb.cluster_fsid) {
	derr << "provided cluster fsid " << fsid << " != superblock's "
	     << sb.cluster_fsid << dendl;
	ret = -EINVAL;
	goto umount_store;
      }
      create_superblock = false;
    }
  }

  if (create_superblock) {
    if (fsid.is_nil()) {
      derr << "must specify cluster fsid" << dendl;
      ret = -EINVAL;
      goto umount_store;
    }

    sb.cluster_fsid = fsid;
    sb.osd_fsid = store->get_fsid();
    sb.whoami = whoami;
    sb.compat_features = get_osd_initial_compat_set();

    uint16_t c_ix;
    uint16_t o_ix;

    // benchmark?
    if (cct->_conf->osd_auto_weight) {
      bufferlist bl;
      bufferptr bp(1048576);
      bp.zero();
      bl.push_back(bp);
      dout(0) << "testing disk bandwidth..." << dendl;
      ceph::mono_time start = ceph::mono_clock::now();
      oid_t oid("disk_bw_test");
      for (int i=0; i<1000; i++) {
	Transaction *t = new Transaction(1);
	c_ix = t->push_col(meta_col);
	o_ix = t->push_oid(hoid_t(oid)); // XXXX
	t->write(c_ix, o_ix, i*bl.length(), bl.length(), bl);
	store->queue_transaction_and_cleanup(t);
      }
      store->sync();
      std::chrono::duration<double> elapsed = ceph::mono_clock::now() - start;
      dout(0) << "measured "
	      << (1000.0 / elapsed.count())
	      << " mb/sec" << dendl;
      Transaction tr;
      c_ix = tr.push_col(meta_col);
      o_ix = tr.push_oid(hoid_t(oid)); // XXXX
      tr.remove(c_ix, o_ix);
      ret = store->apply_transaction(tr);
      if (ret) {
	derr << "OSD::mkfs: error while benchmarking: "
	  "apply_transaction returned " << ret << dendl;
	goto umount_store;
      }
    }

    bufferlist bl;
    ::encode(sb, bl);

    {
      Transaction t;
      c_ix = t.push_col(meta_col);
      o_ix = t.push_oid(OSD_SUPERBLOCK_POBJECT);
      t.write(c_ix, o_ix, 0, bl.length(), bl);
      ret = store->apply_transaction(t);
    }
    if (ret) {
      derr << "OSD::mkfs: error while writing OSD_SUPERBLOCK_POBJECT: "
	<< "apply_transaction returned " << ret << dendl;
      goto umount_store;
    }
  } /* create super */

  store->sync_and_flush();

  ret = write_meta(store, sb.cluster_fsid, sb.osd_fsid, whoami);
  if (ret) {
    derr << "OSD::mkfs: failed to write fsid file: error " << ret << dendl;
    goto umount_store;
  }

umount_store:
  if (meta_col)
    store->close_collection(meta_col);
  store->umount();
free_store:
  delete store;
  return ret;
}

int OSD::write_meta(ObjectStore *store, const boost::uuids::uuid& cluster_fsid,
		    const boost::uuids::uuid& osd_fsid, int whoami)
{
  char val[80];
  int r;

  snprintf(val, sizeof(val), "%s", CEPH_OSD_ONDISK_MAGIC);
  r = store->write_meta("magic", val);
  if (r < 0)
    return r;

  snprintf(val, sizeof(val), "%d", whoami);
  r = store->write_meta("whoami", val);
  if (r < 0)
    return r;

  strcpy(val, to_string(cluster_fsid).c_str());
  r = store->write_meta("ceph_fsid", val);
  if (r < 0)
    return r;

  r = store->write_meta("ready", "ready");
  if (r < 0)
    return r;

  return 0;
}

int OSD::peek_meta(ObjectStore *store, std::string& magic, boost::uuids::uuid& cluster_fsid,
		   boost::uuids::uuid& osd_fsid, int& whoami)
{
  string val;

  int r = store->read_meta("magic", &val);
  if (r < 0)
    return r;
  magic = val;

  r = store->read_meta("whoami", &val);
  if (r < 0)
    return r;
  whoami = atoi(val.c_str());

  r = store->read_meta("ceph_fsid", &val);
  if (r < 0)
    return r;

  boost::uuids::string_generator parse;
  try {
    cluster_fsid = parse(val);
  } catch (std::runtime_error& e) {
    return -EINVAL;
  }

  r = store->read_meta("fsid", &val);
  if (r < 0) {
    osd_fsid = boost::uuids::nil_uuid();
  } else {
    try {
      osd_fsid = parse(val);
    } catch (std::runtime_error& e) {
      return -EINVAL;
    }
  }

  return 0;
}


#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, osdmap)

// cons/des

OSD::OSD(CephContext *cct_, ObjectStore *store_,
	 int id,
	 Messenger *internal_messenger,
	 Messenger *external_messenger,
	 Messenger *xio_external_messenger,
	 Messenger *hb_clientm,
	 Messenger *hb_front_serverm,
	 Messenger *hb_back_serverm,
	 MonClient *mc,
	 const std::string &dev, const std::string &jdev) :
  Dispatcher(cct_),
  authorize_handler_cluster_registry(
    new AuthAuthorizeHandlerRegistry(
      cct, cct->_conf->auth_supported.length() ? cct->_conf->auth_supported : cct->_conf->auth_cluster_required)),
  authorize_handler_service_registry(new AuthAuthorizeHandlerRegistry(cct,
								      cct->_conf->auth_supported.length() ?
								      cct->_conf->auth_supported :
								      cct->_conf->auth_service_required)),
  cluster_messenger(internal_messenger),
  client_messenger(external_messenger),
  client_xio_messenger(xio_external_messenger),
  monc(mc),
  store(store_),
  clog(cct, client_messenger, &mc->monmap, LogClient::NO_FLAGS),
  whoami(id),
  dev_path(dev), journal_path(jdev),
  dispatch_running(false),
  asok_hook(NULL),
  osd_compat(get_osd_compat_set()),
  state(STATE_INITIALIZING), boot_epoch(0), up_epoch(0), bind_epoch(0),
  disk_tp(cct, "OSD::disk_tp", cct->_conf->osd_disk_threads, "osd_disk_threads"),
  command_tp(cct, "OSD::command_tp", 1),
  heartbeat_stop(false), heartbeat_need_update(true), heartbeat_epoch(0),
  hbclient_messenger(hb_clientm),
  hb_front_server_messenger(hb_front_serverm),
  hb_back_server_messenger(hb_back_serverm),
  heartbeat_thread(this),
  heartbeat_dispatcher(this),
  multi_wq(new cohort::OpQueue(this,
                               static_dequeue_op,
                               static_wq_thread_exit,
                               cct->_conf->osd_wq_lanes,
                               cct->_conf->osd_wq_thrd_lowat,
                               cct->_conf->osd_wq_thrd_hiwat)),
  up_thru_wanted(0), up_thru_pending(0), service(this)
{
  monc->set_messenger(client_messenger);
}

OSD::~OSD()
{
  delete authorize_handler_cluster_registry;
  delete authorize_handler_service_registry;
  delete class_handler;
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
  lock_guard lock(osd_lock);
  if (is_stopping())
    return 0;

  if (store->test_mount_in_use()) {
    derr << "OSD::pre_init: object store '" << dev_path << "' is "
	 << "currently in use. (Is ceph-osd already running?)" << dendl;
    return -EBUSY;
  }

  cct->_conf->add_observer(this);
  return 0;
}

// asok

class OSDSocketHook : public AdminSocketHook {
  OSD *osd;
public:
  OSDSocketHook(OSD *o) : osd(o) {}
  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    stringstream ss;
    bool r = osd->asok_command(command, cmdmap, format, ss);
    out.append(ss);
    return r;
  }
};

bool OSD::asok_command(string command, cmdmap_t& cmdmap, string format,
		       ostream& ss)
{
  Formatter *f = new_formatter(format);
  if (!f)
    f = new_formatter("json-pretty");
  if (command == "status") {
    f->open_object_section("status");
    f->dump_stream("cluster_fsid") << superblock.cluster_fsid;
    f->dump_stream("osd_fsid") << superblock.osd_fsid;
    f->dump_unsigned("whoami", superblock.whoami);
    f->dump_string("state", get_state_name(state));
    f->dump_unsigned("oldest_map", superblock.oldest_map);
    f->dump_unsigned("newest_map", superblock.newest_map);
    {
      lock_guard ol(osd_lock);
      f->dump_unsigned("num_vols", vol_map.size());
    }
    f->close_section();
  } else if (command == "flush_journal") {
    store->sync_and_flush();
  } else if (command == "dump_blacklist") {
    list<pair<entity_addr_t,ceph::real_time> > bl;
    OSDMapRef curmap = service.get_osdmap();

    f->open_array_section("blacklist");
    curmap->get_blacklist(&bl);
    for (const auto& blentry : bl) {
      f->open_array_section("entry");
      f->open_object_section("entity_addr_t");
      blentry.first.dump(f);
      f->close_section(); //entity_addr_t
      f->dump_stream("expire_time") << blentry.second;
      f->close_section(); //entry
    }
    f->close_section(); //blacklist
  } else if (command == "dump_watchers") {
    list<obj_watch_item_t> watchers;
    unique_lock ol(osd_lock);
    for (auto it = vol_map.begin(); it != vol_map.end(); ++it) {
      list<obj_watch_item_t> vol_watchers;
      OSDVolRef vol = it->second;
      OSDVol::unique_lock vl(vol->lock);
      vol->get_watchers(vol_watchers);
      vl.unlock();
      watchers.splice(watchers.end(), vol_watchers);
    }
    ol.unlock();

    f->open_array_section("watchers");
    for (list<obj_watch_item_t>::iterator it = watchers.begin();
	it != watchers.end(); ++it) {

      f->open_array_section("watch");

      f->dump_string("object", it->oid.oid.name);

      f->open_object_section("entity_name");
      it->wi.name.dump(f);
      f->close_section(); //entity_name_t

      f->dump_int("cookie", it->wi.cookie);
      f->dump_stream("timeout") << it->wi.timeout;

      f->open_object_section("entity_addr_t");
      it->wi.addr.dump(f);
      f->close_section(); //entity_addr_t

      f->close_section(); //watch
    }

    f->close_section(); //watches
  } else {
    assert(0 == "broken asok registration");
  }
  f->flush(ss);
  delete f;
  return true;
}

class TestOpsSocketHook : public AdminSocketHook {
  OSDService *service;
  ObjectStore *store;
public:
  TestOpsSocketHook(OSDService *s, ObjectStore *st) : service(s), store(st) {}
  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    stringstream ss;
    test_ops(service, store, command, cmdmap, ss);
    out.append(ss);
    return true;
  }
  void test_ops(OSDService *service, ObjectStore *store, std::string command,
     cmdmap_t& cmdmap, ostream &ss);

};

int OSD::init()
{
  CompatSet initial, diff;
  unique_lock ol(osd_lock);
  if (is_stopping())
    return 0;

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

  service.meta_col = store->open_collection(coll_t::META_COLL);
  if (!service.meta_col) {
    derr << "OSD::init() : unable to open osd meta collection" << dendl;
    r = -EINVAL;
    goto out;
  }

  // read superblock
  r = read_superblock(service.meta_col);
  if (r < 0) {
    derr << "OSD::init() : unable to read osd superblock" << dendl;
    r = -EINVAL;
    goto out;
  }

  if (osd_compat.compare(superblock.compat_features) < 0) {
    derr << "The disk uses features unsupported by the executable." << dendl;
    derr << " ondisk features " << superblock.compat_features << dendl;
    derr << " daemon features " << osd_compat << dendl;

    if (osd_compat.writeable(superblock.compat_features)) {
      CompatSet diff = osd_compat.unsupported(superblock.compat_features);
      derr << "it is still writeable, though. Missing features: " << diff << dendl;
      r = -EOPNOTSUPP;
      goto out;
    }
    else {
      CompatSet diff = osd_compat.unsupported(superblock.compat_features);
      derr << "Cannot write to disk! Missing features: " << diff << dendl;
      r = -EOPNOTSUPP;
      goto out;
    }
  }

  if (whoami != superblock.whoami) {
    derr << "OSD::init: superblock says osd"
	 << superblock.whoami << " but i am osd." << whoami << dendl;
    r = -EINVAL;
    goto out;
  }

  initial = get_osd_initial_compat_set();
  diff = superblock.compat_features.unsupported(initial);
  if (superblock.compat_features.merge(initial)) {
    // We need to persist the new compat_set before we
    // do anything else
    dout(5) << "Upgrading superblock adding: " << diff << dendl;
    Transaction t;
    write_superblock(service.meta_col, t);
    r = store->apply_transaction(t);
    if (r < 0)
      goto out;
  }

  // make sure info object exists
  uint16_t c_ix;
  uint16_t o_ix;
  
  if (!store->exists(service.meta_col, service.infos_oid)) {
    dout(10) << "init creating/touching snapmapper object" << dendl;
    Transaction t;
    c_ix = t.push_col(service.meta_col);
    o_ix = t.push_oid(service.infos_oid);
    t.touch(c_ix, o_ix);
    r = store->apply_transaction(t);
    if (r < 0)
      goto out;
  }

  class_handler = new ClassHandler(cct);
  cls_initialize(class_handler);

  if (cct->_conf->osd_open_classes_on_start) {
    int r = class_handler->open_all_classes();
    if (r)
      dout(1) << "warning: got an error loading one or more classes: "
	      << cpp_strerror(r) << dendl;
  }

  // load up "current" osdmap
  if (osdmap) {
    derr << "OSD::init: unable to read current osdmap" << dendl;
    r = -EINVAL;
    goto out;
  }
  osdmap = get_map(superblock.current_epoch);
  check_osdmap_features(store);

  bind_epoch = osdmap->get_epoch();

  dout(2) << "superblock: i am osd." << superblock.whoami << dendl;

  // i'm ready!
  client_messenger->add_dispatcher_head(this);
  cluster_messenger->add_dispatcher_head(this);

  hbclient_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hb_front_server_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hb_back_server_messenger->add_dispatcher_head(&heartbeat_dispatcher);

  if (client_xio_messenger)
    client_xio_messenger->add_dispatcher_head(this);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  r = monc->init();
  if (r < 0)
    goto out;

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&clog);

  disk_tp.start();
  command_tp.start();

  // start the heartbeat
  heartbeat_thread.create();

  // tick
  tick_timer.add_event(cct->_conf->osd_heartbeat_interval,
		       &OSD::tick, this);

  service.publish_map(osdmap);
  service.publish_superblock(superblock);

  ol.unlock();

  r = monc->authenticate();
  if (r < 0) {
    ol.lock(); // unique_lock is going to unlock this on function exit
    if (is_stopping())
      r =  0;
    goto monout;
  }

  while (monc->wait_auth_rotating(30s) < 0) {
    derr << "unable to obtain rotating service keys; retrying" << dendl;
  }

  ol.lock();
  if (is_stopping())
    return 0;

  dout(0) << "done with init, starting boot process" << dendl;
  state = STATE_BOOTING;
  start_boot();

  dout(10) << "ensuring vols have consumed prior maps" << dendl;
  consume_map();

  return 0;
monout:
  monc->shutdown();

out:
  store->umount();
  delete store;
  return r;
}

void OSD::final_init()
{
  int r;
  AdminSocket *admin_socket = cct->get_admin_socket();
  asok_hook = new OSDSocketHook(this);
  r = admin_socket->register_command("status", "status", asok_hook,
				     "high-level status of OSD");
  assert(r == 0);
  r = admin_socket->register_command("flush_journal", "flush_journal",
				     asok_hook,
				     "flush the journal to permanent store");
  assert(r == 0);
  r = admin_socket->register_command("dump_op_pq_state", "dump_op_pq_state",
				     asok_hook,
				     "dump op priority queue state");
  assert(r == 0);
  r = admin_socket->register_command("dump_blacklist", "dump_blacklist",
				     asok_hook,
				     "dump blacklisted clients and times");
  assert(r == 0);
  r = admin_socket->register_command("dump_watchers", "dump_watchers",
				     asok_hook,
				     "show clients which have active watches,"
				     " and on which objects");
  assert(r == 0);
}

void OSD::suicide(int exitcode)
{
  if (cct->_conf->filestore_blackhole) {
    derr << " filestore_blackhole=true, doing abbreviated shutdown" << dendl;
    _exit(exitcode);
  }

  derr << " pausing thread pools" << dendl;
  disk_tp.pause();
  command_tp.pause();

  derr << " flushing io" << dendl;
  store->sync_and_flush();

  derr << " removing pid file" << dendl;
  pidfile_remove();

  derr << " exit" << dendl;
  exit(exitcode);
}

int OSD::shutdown()
{
  if (!service.prepare_to_stop())
    return 0; // already shutting down

  unique_lock ol(osd_lock);
  if (is_stopping()) {
    ol.unlock();
    return 0;
  }
  derr << "shutdown" << dendl;

  unique_lock hl(heartbeat_lock);
  state = STATE_STOPPING;
  hl.unlock();

  notify_state_observers(state, osdmap->get_epoch());

  // Debugging
  cct->_conf->set_val("debug_osd", "100");
  cct->_conf->set_val("debug_journal", "100");
  cct->_conf->set_val("debug_filestore", "100");
  cct->_conf->set_val("debug_ms", "100");
  cct->_conf->apply_changes(NULL);

  for (const auto& p : vol_map) {
    dout(20) << " kicking vol " << p.first << dendl;
    lock_guard vl(p.second->lock);
    p.second->on_shutdown();
  }

  // XXX drain multi_wq
  multi_wq->shutdown();

  // unregister commands
  cct->get_admin_socket()->unregister_command("status");
  cct->get_admin_socket()->unregister_command("flush_journal");
  cct->get_admin_socket()->unregister_command("dump_op_pq_state");
  cct->get_admin_socket()->unregister_command("dump_blacklist");
  cct->get_admin_socket()->unregister_command("dump_watchers");
  delete asok_hook;
  asok_hook = NULL;

  ol.unlock();

  hl.lock();
  heartbeat_stop = true;
  heartbeat_cond.notify_all();
  hl.unlock();
  heartbeat_thread.join();

  dout(10) << "op tp stopped" << dendl;

  command_tp.drain();
  command_tp.stop();
  dout(10) << "command tp stopped" << dendl;

  disk_tp.drain();
  disk_tp.stop();
  dout(10) << "disk tp paused (new)" << dendl;

  ol.lock();

  // note unmount epoch
  dout(10) << "noting clean unmount in epoch " << osdmap->get_epoch() << dendl;
  superblock.mounted = boot_epoch;
  int r;
  {
    Transaction t;
    write_superblock(service.meta_col, t);
    r = store->apply_transaction(t);
    if (r) {
      derr << "OSD::shutdown: error writing superblock: "
          << cpp_strerror(r) << dendl;
    }
    if (service.infos_oh)
      store->put_object(service.infos_oh);
    store->close_collection(service.meta_col);
    service.meta_col = nullptr;
  }

  dout(10) << "syncing store" << dendl;
  store->flush();
  store->sync();
  store->umount();
  delete store;
  store = 0;
  dout(10) << "Store synced" << dendl;

  for (const auto p : vol_map) {
    dout(20) << " kicking vol " << p.first << dendl;
    OSDVol::lock_guard vl(p.second->lock);
    if (p.second->ref != 1) {
      derr << "volume " << p.first << " has ref count of "
	   << p.second->ref << dendl;
    }
  }
  vol_map.clear();
  cct->_conf->remove_observer(this);

  monc->shutdown();
  ol.unlock();

  osdmap = OSDMapRef();
  service.shutdown();

  class_handler->shutdown();
  client_messenger->shutdown();
  cluster_messenger->shutdown();
  hbclient_messenger->shutdown();
  hb_front_server_messenger->shutdown();
  hb_back_server_messenger->shutdown();
  if (client_xio_messenger && client_xio_messenger != client_messenger)
    client_xio_messenger->shutdown();
  return r;
}

void OSD::write_superblock(CollectionHandle meta, Transaction& t)
{
  dout(10) << "write_superblock " << superblock << dendl;

  //hack: at minimum it's using the baseline feature set
  if (!superblock.compat_features.incompat.mask |
      CEPH_OSD_FEATURE_INCOMPAT_BASE.id)
    superblock.compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);

  bufferlist bl;
  ::encode(superblock, bl);

  uint16_t c_ix = t.push_col(meta);
  uint16_t o_ix = t.push_oid(OSD_SUPERBLOCK_POBJECT); // XXX oid? open it?
  t.write(c_ix, o_ix, 0, bl.length(), bl);
}

int OSD::read_superblock(CollectionHandle meta)
{
  bufferlist bl;
  bufferlist::iterator p;
  ObjectHandle oh = nullptr;

  oh = store->get_object(meta, OSD_SUPERBLOCK_POBJECT);
  if (unlikely(!oh))
    return -ENOENT;

  int r = store->read(meta, oh, 0, CEPH_READ_ENTIRE, bl);
  store->put_object(oh);
  if (r < 0)
    return r;

  p = bl.begin();
  ::decode(superblock, p);
  dout(10) << "read_superblock " << superblock << dendl;
  return r;
}

void OSD::recursive_remove_collection(ObjectStore *store, coll_t cid)
{
  boost::uuids::uuid vol;
  cid.is_vol(vol);

  CollectionHandle ch = store->open_collection(cid);
  if (! ch)
    return;

  vector<hoid_t> objects;
  store->collection_list(ch, objects);

  Transaction t;
  uint16_t c_ix = t.push_col(ch);
  uint16_t o_ix;

  // delete them.
  unsigned removed = 0;
  for (vector<hoid_t>::iterator p = objects.begin();
       p != objects.end();
       ++p, removed++) {
    o_ix = t.push_oid(*p); // XXX oid? open it?
    t.collection_remove(c_ix, o_ix);
    if (removed > 300) {
      int r = store->apply_transaction(t);
      assert(r == 0);
      t.clear();
      c_ix = t.push_col(ch);
      removed = 0;
    }
  }
  t.remove_collection(c_ix);
  int r = store->apply_transaction(t);
  assert(r == 0);
  store->sync_and_flush();
  store->close_collection(ch);
}


// ======================================================
// Volumes

bool OSD::_have_vol(const boost::uuids::uuid& volume)
{
  // Caller should hold a lock on osd_lock
  return vol_map.count(volume);
}

OSDVolRef OSD::_lookup_vol(const boost::uuids::uuid& volid)
{
  // Caller should hold a lock on osd_lock
  OSDVol* v = tls_vol_cache.get(volid);
  if (v)
    return v;
  unique_lock vlock(vol_lock);
  auto i = vol_map.find(volid);
  if (i != vol_map.end()) {
    OSDVolRef vol = i->second;
    service.lru.lru_touch(&*vol);
    tls_vol_cache.put(vol.get() /* intrusive_ptr */);
    return vol;
  } else {
    return _load_vol(volid);
  }
}

OSDVolRef OSD::_load_vol(const boost::uuids::uuid& volid)
{
  // Caller should hold a lock on osd_lock
  OSDVol* vol = new OSDVol(&service, osdmap, volid);
  service.lru.lru_insert_top(vol);
  vol_map[volid] = vol;
  trim_vols();
  return OSDVolRef(vol);
}

void OSD::trim_vols(void)
{
  unsigned last = 0;
  // Caller should hold a lock on osd_lock
  while (service.lru.lru_get_size() != last) {
    last = service.lru.lru_get_size();

    if (service.lru.lru_get_size() <=
	service.lru.lru_get_max())
      break;

    OSDVol* vol = static_cast<OSDVol*>(service.lru.lru_expire());
    if (!vol)
      break;

    vol_map.erase(vol->id);
  }
}

OSDVolRef OSD::_lookup_lock_vol(const boost::uuids::uuid& volid,
				OSDVol::unique_lock& vl)
{
  OSDVolRef vol = _lookup_vol(volid);
  if (vol)
    vl = OSDVol::unique_lock(vol->lock);
  return vol;
}

// -------------------------------------

float OSDService::get_full_ratio()
{
  float full_ratio = cct->_conf->osd_failsafe_full_ratio;
  if (full_ratio > 1.0) full_ratio /= 100.0;
  return full_ratio;
}

float OSDService::get_nearfull_ratio()
{
  float nearfull_ratio = cct->_conf->osd_failsafe_nearfull_ratio;
  if (nearfull_ratio > 1.0) nearfull_ratio /= 100.0;
  return nearfull_ratio;
}

void OSDService::check_nearfull_warning(const osd_stat_t &osd_stat)
{
  lock_guard fsl(full_status_lock);
  enum s_names new_state;

  // We base ratio on kb_avail rather than kb_used because they can
  // differ significantly e.g. on btrfs volumes with a large number of
  // chunks reserved for metadata, and for our purposes (avoiding
  // completely filling the disk) it's far more important to know how
  // much space is available to use than how much we've already used.
  float ratio = ((float)(osd_stat.kb - osd_stat.kb_avail)) / ((float)osd_stat.kb);
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
  }

  if (cur_state == FULL)
    clog.error() << "OSD full dropping all updates " << (int)(ratio * 100) << "% full";
  else
    clog.warn() << "OSD near full (" << (int)(ratio * 100) << "%)";
}

bool OSDService::check_failsafe_full()
{
  lock_guard fsl(full_status_lock);
  if (cur_state == FULL)
    return true;
  return false;
}

void OSD::need_heartbeat_peer_update()
{
  lock_guard hl(heartbeat_lock);
  if (is_stopping())
    return;
  dout(20) << "need_heartbeat_peer_update" << dendl;
  heartbeat_need_update = true;
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

  unique_lock hl(heartbeat_lock);
  if (is_stopping()) {
    hl.unlock();
    m->put();
    return;
  }

  OSDMapRef curmap = service.get_osdmap();

  switch (m->op) {

  case MOSDPing::PING:
    {
      if (!cct->get_heartbeat_map()->is_healthy()) {
	dout(10) << "internal heartbeat not healthy, dropping ping request"
		 << dendl;
	break;
      }

      Message *r = new MOSDPing(monc->get_fsid(),
				curmap->get_epoch(),
				MOSDPing::PING_REPLY,
				m->stamp);
      m->get_connection()->get_messenger()
	->send_message(r, m->get_connection());

      if (curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active()) {
	  ConnectionRef con = service.get_con_osd_cluster(from, curmap->get_epoch());
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
	  ConnectionRef con = service.get_con_osd_cluster(from, curmap->get_epoch());
	  if (con) {
	    _share_map_outgoing(from, con.get());
	  }
	}
      }

      ceph::real_time cutoff = ceph::real_clock::now()
	- cct->_conf->osd_heartbeat_grace;
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
    dout(10) << "handle_osd_ping " << m->get_source_inst()
	     << " says i am down in " << m->map_epoch << dendl;
    osdmap_subscribe(curmap->get_epoch()+1, false);
    break;
  }

  hl.unlock();
  m->put();
}

void OSD::heartbeat_entry()
{
  unique_lock hl(heartbeat_lock);
  if (is_stopping())
    return;
  while (!heartbeat_stop) {
    heartbeat();

    ceph::timespan wait = 500ms + (rand() % 10) *
      cct->_conf->osd_heartbeat_interval / 10;
    dout(30) << "heartbeat_entry sleeping for " << wait << dendl;
    heartbeat_cond.wait_for(hl, wait);
    if (is_stopping())
      return;
    dout(30) << "heartbeat_entry woke up" << dendl;
  }
}

void OSD::heartbeat_check()
{
  // Should be called with a lock on heartbeat_lock held
  ceph::timespan age = hbclient_messenger->get_dispatch_queue_max_age();
  if (age > cct->_conf->osd_heartbeat_grace / 2) {
    derr << "skipping heartbeat_check, hbqueue max age: " << age << dendl;
    return; // hb dispatch is too backed up for our hb status to be meaningful
  }

  // check for incoming heartbeats (move me elsewhere?)
  ceph::real_time cutoff = ceph::real_clock::now()
    - cct->_conf->osd_heartbeat_grace;
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
      if (p->second.last_rx_back == ceph::real_time::min() ||
	  p->second.last_rx_front == ceph::real_time::min()) {
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

void OSD::heartbeat()
{
  dout(30) << "heartbeat" << dendl;

  ceph::real_time now = ceph::real_clock::now();

  // send heartbeats
  for (auto& i : heartbeat_peers) {
    int peer = i.first;
    i.second.last_tx = now;
    if (i.second.first_tx == ceph::real_time::min())
      i.second.first_tx = now;
    dout(30) << "heartbeat sending ping to osd." << peer << dendl;
    hbclient_messenger->send_message(new MOSDPing(
				       monc->get_fsid(),
				       service.get_osdmap()->get_epoch(),
				       MOSDPing::PING,
				       now),
				     i.second.con_back);
    if (i.second.con_front)
      hbclient_messenger->send_message(new MOSDPing(
					 monc->get_fsid(),
					 service.get_osdmap()->get_epoch(),
					 MOSDPing::PING,
					 now),
				       i.second.con_front);
  }

  dout(30) << "heartbeat check" << dendl;
  heartbeat_check();

  // hmm.. am i all alone?
  dout(30) << "heartbeat lonely?" << dendl;
  if (heartbeat_peers.empty()) {
    if (now - last_mon_heartbeat > cct->_conf->osd_mon_heartbeat_interval
	&& is_active()) {
      last_mon_heartbeat = now;
      dout(10) << "i have no heartbeat peers; checking mon for new map" << dendl;
      osdmap_subscribe(osdmap->get_epoch() + 1, true);
    }
  }

  dout(30) << "heartbeat done" << dendl;
}

bool OSD::heartbeat_reset(Connection *con)
{
  HeartbeatSession *s = static_cast<HeartbeatSession*>(con->get_priv());
  if (s) {
    unique_lock hl(heartbeat_lock);
    if (is_stopping()) {
      hl.unlock();
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
      pair<ConnectionRef,ConnectionRef> newcon = service.get_con_osd_hb(p->second.peer, p->second.epoch);
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
    hl.unlock();
    s->put();
  }
  return true;
}

// =========================================

void OSD::tick()
{
  unique_lock l(osd_lock);
  // Should be called with a lock on osd_lock
  dout(5) << "tick" << dendl;

  if (is_active() || is_waiting_for_healthy()) {
    shared_map_lock ml(map_lock);
    unique_lock hl(heartbeat_lock);

    heartbeat_check();
    hl.unlock();
    ml.unlock();
  }

  if (is_waiting_for_healthy()) {
    if (_is_healthy()) {
      dout(1) << "healthy again, booting" << dendl;
      state = STATE_BOOTING;
      start_boot();
      notify_state_observers(state, osdmap->get_epoch());
    }
  }

  tick_timer.reschedule_me(1s);
}

// =========================================

void OSD::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    lock_guard ol(osd_lock);
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
}

bool OSD::ms_handle_reset(Connection *con)
{
  OSD::Session *session = (OSD::Session *)con->get_priv();
  dout(1) << "ms_handle_reset con " << con << " session " << session << dendl;
  if (!session)
    return false;
  session->wstate.reset();
  session->con.reset(NULL);  // break con <-> session ref cycle
  session->put();
  return true;
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
  lock_guard ol(osd_lock);
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
	     osdmap->get_epoch() + cct->_conf->osd_map_message_max > newest) {
    _send_boot();
    return;
  }

  // get all the latest maps
  if (osdmap->get_epoch() > oldest)
    osdmap_subscribe(osdmap->get_epoch() + 1, true);
  else
    osdmap_subscribe(oldest - 1, true);
}

void OSD::start_waiting_for_healthy()
{
  dout(1) << "start_waiting_for_healthy" << dendl;
  state = STATE_WAITING_FOR_HEALTHY;
  notify_state_observers(state, osdmap->get_epoch());
  last_heartbeat_resample = ceph::mono_time::min();
}

bool OSD::_is_healthy()
{
  if (!cct->get_heartbeat_map()->is_healthy()) {
    dout(1) << "is_healthy false -- internal heartbeat failed" << dendl;
    return false;
  }

  if (is_waiting_for_healthy()) {
    lock_guard hl(heartbeat_lock);
    ceph::real_time cutoff = ceph::real_clock::now() -
      cct->_conf->osd_heartbeat_grace;
    int num = 0, up = 0;
    for (const auto& p : heartbeat_peers) {
      if (p.second.is_healthy(cutoff))
	++up;
      ++num;
    }
    if ((float)up < (float)num * cct->_conf->osd_heartbeat_min_healthy_ratio) {
      dout(1) << "is_healthy false -- only " << up << "/" << num
	      << " up peers (less than 1/3)" << dendl;
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
  _collect_metadata(&mboot->metadata);
  monc->send_mon_message(mboot);
}

void OSD::_collect_metadata(map<string,string> *pm)
{
  (*pm)["ceph_version"] = pretty_version_to_str();

  // config info
  (*pm)["osd_data"] = dev_path;
  (*pm)["osd_journal"] = journal_path;
  (*pm)["front_addr"] = stringify(client_messenger->get_myaddr());
  (*pm)["back_addr"] = stringify(cluster_messenger->get_myaddr());
  (*pm)["hb_front_addr"] = stringify(hb_front_server_messenger->get_myaddr());
  (*pm)["hb_back_addr"] = stringify(hb_back_server_messenger->get_myaddr());

  // kernel info
  struct utsname u;
  int r = uname(&u);
  if (r >= 0) {
    (*pm)["os"] = u.sysname;
    (*pm)["kernel_version"] = u.release;
    (*pm)["kernel_description"] = u.version;
    (*pm)["hostname"] = u.nodename;
    (*pm)["arch"] = u.machine;
  }

  // memory
  FILE *f = fopen("/proc/meminfo", "r");
  if (f) {
    char buf[100];
    while (!feof(f)) {
      char *line = fgets(buf, sizeof(buf), f);
      if (!line)
	break;
      char key[40];
      long long value;
      int r = sscanf(line, "%s %lld", key, &value);
      if (r == 2) {
	if (strcmp(key, "MemTotal:") == 0)
	  (*pm)["mem_total_kb"] = stringify(value);
	else if (strcmp(key, "SwapTotal:") == 0)
	  (*pm)["mem_swap_kb"] = stringify(value);
      }
    }
    fclose(f);
  }

  // processor
  f = fopen("/proc/cpuinfo", "r");
  if (f) {
    char buf[100];
    while (!feof(f)) {
      char *line = fgets(buf, sizeof(buf), f);
      if (!line)
	break;
      if (strncmp(line, "model name", 10) == 0) {
	char *c = strchr(buf, ':');
	c++;
	while (*c == ' ')
	  ++c;
	char *nl = c;
	while (*nl != '\n')
	  ++nl;
	*nl = '\0';
	(*pm)["cpu"] = c;
	break;
      }
    }
    fclose(f);
  }

  // distro info
  f = fopen("/etc/lsb-release", "r");
  if (f) {
    char buf[100];
    while (!feof(f)) {
      char *line = fgets(buf, sizeof(buf), f);
      if (!line)
	break;
      char *eq = strchr(buf, '=');
      if (!eq)
	break;
      *eq = '\0';
      ++eq;
      while (*eq == '\"')
	++eq;
      while (*eq && (eq[strlen(eq)-1] == '\n' ||
		     eq[strlen(eq)-1] == '\"'))
	eq[strlen(eq)-1] = '\0';
      if (strcmp(buf, "DISTRIB_ID") == 0)
	(*pm)["distro"] = eq;
      else if (strcmp(buf, "DISTRIB_RELEASE") == 0)
	(*pm)["distro_version"] = eq;
      else if (strcmp(buf, "DISTRIB_CODENAME") == 0)
	(*pm)["distro_codename"] = eq;
      else if (strcmp(buf, "DISTRIB_DESCRIPTION") == 0)
	(*pm)["distro_description"] = eq;
    }
    fclose(f);
  }

  dout(10) << __func__ << " " << *pm << dendl;
}

void OSD::queue_want_up_thru(epoch_t want)
{
  shared_map_lock ml(map_lock);
  epoch_t cur = osdmap->get_up_thru(whoami);
  if (want > up_thru_wanted) {
    dout(10) << "queue_want_up_thru now " << want << " (was " << up_thru_wanted << ")"
	     << ", currently " << cur
	     << dendl;
    up_thru_wanted = want;

    // expedite, a bit.	 WARNING this will somewhat delay other mon queries.
    send_alive();
  } else {
    dout(10) << "queue_want_up_thru want " << want << " <= queued " << up_thru_wanted
	     << ", currently " << cur
	     << dendl;
  }
  ml.unlock();
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

void OSDService::send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch)
{
  lock_guard ppl(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    m->put();
    return;
  }
  const entity_inst_t& peer_inst = next_osdmap->get_cluster_inst(peer);
  Connection *peer_con = osd->cluster_messenger->get_connection(peer_inst).get();
  osd->_share_map_outgoing(peer, peer_con, next_osdmap);
  osd->cluster_messenger->send_message(m, peer_inst);
}

ConnectionRef OSDService::get_con_osd_cluster(int peer, epoch_t from_epoch)
{
  lock_guard ppl(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    return NULL;
  }
  return osd->cluster_messenger->get_connection(next_osdmap->get_cluster_inst(peer));
}

pair<ConnectionRef,ConnectionRef> OSDService::get_con_osd_hb(
  int peer,epoch_t from_epoch)
{
  lock_guard ppl(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  pair<ConnectionRef,ConnectionRef> ret;
  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    return ret;
  }
  ret.first = osd->hbclient_messenger->get_connection(
    next_osdmap->get_hb_back_inst(peer));
  if (next_osdmap->get_hb_front_addr(peer) != entity_addr_t())
    ret.second = osd->hbclient_messenger->get_connection(
      next_osdmap->get_hb_front_inst(peer));
  return ret;
}

void OSD::send_failures()
{
  // Should be called with the osd_lock locked
  unique_lock hl(heartbeat_lock, std::defer_lock);
  if (!failure_queue.empty()) {
    hl.lock();
  }
  ceph::real_time now = ceph::real_clock::now();
  while (!failure_queue.empty()) {
    int osd = failure_queue.begin()->first;
    ceph::timespan failed_for = now - failure_queue.begin()->second;
    entity_inst_t i = osdmap->get_inst(osd);
    monc->send_mon_message(
      new MOSDFailure(monc->get_fsid(), i,
		      std::chrono::duration_cast<
		      std::chrono::seconds>(failed_for).count(),
		      osdmap->get_epoch()));
    failure_pending[osd] = i;
    failure_queue.erase(osd);
  }
}

void OSD::send_still_alive(epoch_t epoch, const entity_inst_t &i)
{
  MOSDFailure *m = new MOSDFailure(monc->get_fsid(), i, 0, epoch);
  m->is_failed = false;
  monc->send_mon_message(m);
}

// --------------------------------------
// dispatch

epoch_t OSD::get_peer_epoch(int peer)
{
  lock_guard pmel(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p == peer_map_epoch.end())
    return 0;
  return p->second;
}

epoch_t OSD::note_peer_epoch(int peer, epoch_t e)
{
  lock_guard pmel(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p != peer_map_epoch.end()) {
    if (p->second < e) {
      dout(10) << "note_peer_epoch osd." << peer << " has " << e << dendl;
      p->second = e;
    } else {
      dout(30) << "note_peer_epoch osd." << peer << " has " << p->second
	       << " >= " << e << dendl;
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
  lock_guard pmel(peer_map_epoch_lock);
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


bool OSD::_share_map_incoming(entity_name_t name, Connection *con,
			      epoch_t epoch, Session* session)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << name << " " << con->get_peer_addr()
	   << " " << epoch << dendl;

  assert(is_active());

  // does client have old map?
  if (name.is_client()) {
    bool sendmap = epoch < osdmap->get_epoch();
    if (sendmap && session) {
      if (session->last_sent_epoch < osdmap->get_epoch()) {
	session->last_sent_epoch = osdmap->get_epoch();
      } else {
	sendmap = false; //we don't need to send it out again
	dout(15) << name << " already sent incremental to update from epoch "
		 << epoch << dendl;
      }
    }
    if (sendmap) {
      dout(10) << name << " has old map " << epoch << " < "
	       << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, con);
      shared = true;
    }
  }

  // does peer have old map?
  if (con->get_messenger() == cluster_messenger &&
      osdmap->is_up(name.num()) &&
      (osdmap->get_cluster_addr(name.num()) == con->get_peer_addr() ||
       osdmap->get_hb_back_addr(name.num()) == con->get_peer_addr())) {
    // remember
    epoch_t has = note_peer_epoch(name.num(), epoch);

    // share?
    if (has < osdmap->get_epoch()) {
      dout(10) << name << " " << con->get_peer_addr() << " has old map "
	       << epoch << " < " << osdmap->get_epoch() << dendl;
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
    map = service.get_osdmap();

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
  /* ops which don't take osd_lock */
  switch (m->get_type()) {
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source() << dendl;
    m->put();
    return true;
    break;
  case CEPH_MSG_OSD_OP:
    if (op_is_discardable(static_cast<MOSDOp*>(m))) {
      dout(10) << " discardable " << *m << dendl;
      return true;
    }
    break;
  case MSG_OSD_MARK_ME_DOWN:
    service.got_stop_ack(); // is_stopping_lock
    m->put();
    return true;
  default:
    break;
  } /* switch */

  /* osd_lock LOCKED */
  unique_lock osd_lk(osd_lock);
  if (is_stopping()) {
    osd_lk.unlock();
    m->put();
    return true;
  }

  switch (m->get_type()) {
    // map and replication
  case CEPH_MSG_OSD_MAP:
    handle_osd_map(static_cast<MOSDMap*>(m));
    break;
    // special needing osd_lock
  case CEPH_MSG_SHUTDOWN:
    {
      Session* session = static_cast<Session *>(m->get_connection()->get_priv());
      if (!session ||
	  session->entity_name.is_mon() ||
	  session->entity_name.is_osd())
	shutdown();
      else dout(0) << "shutdown message from connection with "
		   << "insufficient privs!" << m->get_connection()
		   << dendl;
      m->put();
      if (session)
	session->put();
    }
    break;
  default:
    {
      /* all other ops, need OSDMap in general */
      OpRequestRef op(static_cast<OpRequest*>(m));
      op->put();
      handle_op(op.get(), osd_lk);
    }
  }

  return true;
}

bool OSD::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			    bool force_new)
{
  dout(10) << "OSD::ms_get_authorizer type="
	   << ceph_entity_type_name(dest_type) << dendl;

  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    /* the MonClient checks keys every tick(), so we should just wait
       for that cycle to get through */
    if (monc->wait_auth_rotating(10s) < 0)
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

  isvalid = authorize_handler->verify_authorizer(
    cct, monc->rotating_secrets, authorizer_data, authorizer_reply, name,
    global_id, caps_info, session_key, &auid);

  if (isvalid) {
    Session *s = static_cast<Session *>(con->get_priv());
    if (!s) {
      s = new Session;
      con->set_priv(s->get());
      s->con = con;
      dout(10) << " new session " << s << " con=" << s->con << " addr=" << s->con->get_peer_addr() << dendl;
    }

    s->entity_name = name;
    s->auid = auid;
    s->put();
  }
  return true;
};

bool OSDService::prepare_to_stop()
{
  unique_lock isl(is_stopping_lock);
  if (state != NOT_STOPPING)
    return false;

  OSDMapRef osdmap = get_osdmap();
  if (osdmap && osdmap->is_up(whoami)) {
    dout(0) << __func__ << " telling mon we are shutting down" << dendl;
    state = PREPARING_TO_STOP;
    monc->send_mon_message(new MOSDMarkMeDown(monc->get_fsid(),
					      osdmap->get_inst(whoami),
					      osdmap->get_epoch(),
					      false
					      ));
    ceph::mono_time now = ceph::mono_clock::now();
    ceph::mono_time timeout = now + cct->_conf->osd_mon_shutdown_timeout;
    while ((ceph::mono_clock::now() < timeout) &&
	   (state != STOPPING)) {
      is_stopping_cond.wait_until(isl, timeout);
    }
  }
  dout(0) << __func__ << " starting shutdown" << dendl;
  state = STOPPING;
  return true;
}

void OSDService::got_stop_ack()
{
  lock_guard sl(is_stopping_lock);
  dout(0) << __func__ << " starting shutdown" << dendl;
  state = STOPPING;
  is_stopping_cond.notify_all();
}


// =====================================================
// MAP

void OSD::wait_for_new_map(OpRequest* op)
{
  // ask?
  if (waiting_for_osdmap.empty()) {
    osdmap_subscribe(osdmap->get_epoch() + 1, true);
  }

  op->get(); // queue ref
  waiting_for_osdmap.push_back(*op);
  op->mark_delayed("wait for new map");
}


/** update_map
 * assimilate new OSDMap(s).
 */

void OSD::note_down_osd(int peer)
{
  // require osd_lock LOCKED
  cluster_messenger->mark_down(osdmap->get_cluster_addr(peer));

  unique_lock hl(heartbeat_lock);
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
  hl.unlock();
}

void OSD::note_up_osd(int peer)
{
  forget_peer_epoch(peer, osdmap->get_epoch() - 1);
}

struct C_OnMapApply : public Context {
  OSDService *service;
  boost::scoped_ptr<Transaction> t;
  list<OSDMapRef> pinned_maps;
  epoch_t e;
  C_OnMapApply(OSDService *service,
	       Transaction *t,
	       const list<OSDMapRef> &pinned_maps,
	       epoch_t e)
    : service(service), t(t), pinned_maps(pinned_maps), e(e) {}
  void finish(int r) {
    service->clear_map_bl_cache_pins(e);
  }
};

void OSD::osdmap_subscribe(version_t epoch, bool force_request)
{
  OSDMapRef osdmap = service.get_osdmap();
  if (osdmap->get_epoch() >= epoch)
    return;

  if (monc->sub_want_increment("osdmap", epoch, CEPH_SUBSCRIBE_ONETIME) ||
      force_request) {
    monc->renew_subs();
  }
}

void OSD::handle_osd_map(MOSDMap *m)
{
  // Must be called with a lock on osd_lock
  list<OSDMapRef> pinned_maps;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != "
	    << monc->get_fsid() << dendl;
    m->put();
    return;
  }
  if (is_initializing()) {
    dout(0) << "ignoring osdmap until we have initialized" << dendl;
    m->put();
    return;
  }

  Session *session =
    static_cast<Session *>(m->get_connection()->get_priv());
  if (session && !(session->entity_name.is_mon() ||
		   session->entity_name.is_osd())) {
    //not enough perms!
    m->put();
    session->put();
    return;
  }
  if (session)
    session->put();

  epoch_t first = m->get_first();
  epoch_t last = m->get_last();
  dout(3) << "handle_osd_map epochs [" << first << ","
	  << last << "], i have "
	  << osdmap->get_epoch()
	  << ", src has [" << m->oldest_map << ","
	  << m->newest_map << "]"
	  << dendl;

  /* make sure there is something new, here, before we bother
   * flushing the queues and such */
  if (last <= osdmap->get_epoch()) {
    dout(10) << " no new maps here, dropping" << dendl;
    m->put();
    return;
  }

  /* even if this map isn't from a mon, we may have satisfied our
   * subscription */
  monc->sub_got("osdmap", last);

  // missing some?
  bool skip_maps = false;
  if (first > osdmap->get_epoch() + 1) {
    dout(10) << "handle_osd_map message skips epochs "
	     << osdmap->get_epoch() + 1
	     << ".." << (first-1) << dendl;
    if (m->oldest_map <= osdmap->get_epoch() + 1) {
      osdmap_subscribe(osdmap->get_epoch()+1, true);
      m->put();
      return;
    }
    /* always try to get the full range of maps--as many as we can.
     * this
     *	1- is good to have
     *	2- is at present the only way to ensure that we get a *full*
     *     map as the first map!
     */
    if (m->oldest_map < first) {
      osdmap_subscribe(m->oldest_map - 1, true);
      m->put();
      return;
    }
    skip_maps = true;
  }

  Transaction *_t = new Transaction(cct->_conf->osd_target_transaction_size);
  Transaction &t = *_t;
  uint16_t c_ix = t.push_col(service.meta_col);
  uint16_t o_ix;

  // store new maps: queue for disk and put in the osdmap cache
  epoch_t last_marked_full = 0;
  epoch_t start = MAX(osdmap->get_epoch() + 1, first);
  for (epoch_t e = start; e <= last; e++) {
    map<epoch_t,bufferlist>::iterator p;
    p = m->maps.find(e);
    if (p != m->maps.end()) {
      dout(10) << "handle_osd_map  got full map for epoch "
	       << e << dendl;
      OSDMap *o = new OSDMap;
      bufferlist& bl = p->second;

      o->decode(bl);
      if (o->test_flag(CEPH_OSDMAP_FULL))
	last_marked_full = e;
      pinned_maps.push_back(add_map(o));

      oid_t fulloid = get_osdmap_pobject_name(e);
      /* XXX surely service has started */
      o_ix = t.push_oid(fulloid); // XXXX oid?  open it?
      t.write(c_ix, o_ix, 0, bl.length(), bl);
      pin_map_bl(e, bl);
      continue;
    }

    p = m->incremental_maps.find(e);
    if (p != m->incremental_maps.end()) {
      dout(10) << "handle_osd_map got inc map for epoch "
	       << e << dendl;
      bufferlist& bl = p->second;
      oid_t oid = get_inc_osdmap_pobject_name(e);
      o_ix = t.push_oid(oid); // XXXX oid?  open it?
      t.write(c_ix, o_ix, 0, bl.length(), bl);
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
      // In the original this just asserts. Do we want to do that?
      o->apply_incremental(inc);
      // if (o->apply_incremental(inc) < 0) {
      //	derr << "ERROR: bad fsid?  i have " << osdmap->get_fsid()
      //	     << " and inc has " << inc.fsid << dendl;
      //	assert(0 == "bad fsid");
      // }

      if (o->test_flag(CEPH_OSDMAP_FULL))
	last_marked_full = e;
      pinned_maps.push_back(add_map(o));

      bufferlist fbl;
      o->encode(fbl);

      oid_t fulloid = get_osdmap_pobject_name(e);
      o_ix = t.push_oid(fulloid); // XXXX oid?  open it?
      t.write(c_ix, o_ix, 0, fbl.length(), fbl);
      pin_map_bl(e, fbl);
      continue;
    }

    assert(0 == "MOSDMap lied about what maps it had?");
  }

  if (superblock.oldest_map) {
    int num = 0;
    epoch_t min(
      MIN(m->oldest_map,
	  service.map_cache.cached_key_lower_bound()));
    for (epoch_t e = superblock.oldest_map; e < min; ++e) {
      dout(20) << " removing old osdmap epoch " << e << dendl;
      o_ix = t.push_oid(get_osdmap_pobject_name(e));
      t.remove(c_ix, o_ix);
      o_ix = t.push_oid(get_inc_osdmap_pobject_name(e));
      t.remove(c_ix, o_ix);
      superblock.oldest_map = e+1;
      num++;
      if (num >= cct->_conf->osd_target_transaction_size &&
	  // make sure we at least keep pace with incoming maps
	  (uint64_t)num > (last - first))
	break;
    }
  }

  if (!superblock.oldest_map || skip_maps)
    superblock.oldest_map = first;
  superblock.newest_map = last;

  if (last_marked_full > superblock.last_map_marked_full)
    superblock.last_map_marked_full = last_marked_full;

  unique_map_lock ml(map_lock);

  C_Contexts *fin = new C_Contexts;

  // advance through the new maps
  for (epoch_t cur = start; cur <= superblock.newest_map; cur++) {
    dout(10) << " advance to epoch " << cur << " (<= newest "
	     << superblock.newest_map << ")" << dendl;

    OSDMapRef newmap = get_map(cur);
    assert(newmap);  // we just cached it above!

    // start blacklisting messages sent to peers that go down.
    service.pre_publish_map(newmap);

    // kill connections to newly down osds
    set<int> old;
    osdmap->get_all_osds(old);
    for (set<int>::iterator p = old.begin(); p != old.end(); ++p) {
      if (*p != whoami &&
	  osdmap->have_inst(*p) &&			  // in old map
	  (!newmap->exists(*p) || !newmap->is_up(*p))) {  // but not the new one
	note_down_osd(*p);
      }
    }

    osdmap = newmap;

    superblock.current_epoch = cur;
    advance_map(t, fin);
    had_map_since = ceph::mono_clock::now();
  }

  if (osdmap->is_up(whoami) &&
      bind_epoch < osdmap->get_up_from(whoami)) {
    if (osdmap->get_addr(whoami) != client_messenger->get_myaddr()) {
      dout(1) << "osd addr " << osdmap->get_addr(whoami)
	<< " doesn't match myaddr " << client_messenger->get_myaddr() << dendl;
    } else if (is_booting()) {
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
      // don't call shutdown() while we have everything paused
      do_shutdown = true;
    } else if (!osdmap->is_up(whoami) ||
	       !osdmap->get_addr(whoami).probably_equals(client_messenger
							 ->get_myaddr()) ||
	       !osdmap->get_cluster_addr(whoami)
	       .probably_equals(cluster_messenger->get_myaddr()) ||
	       !osdmap->get_hb_back_addr(whoami)
	       .probably_equals(hb_back_server_messenger->get_myaddr()) ||
	       (osdmap->get_hb_front_addr(whoami) != entity_addr_t() &&
		!osdmap->get_hb_front_addr(whoami)
		.probably_equals(hb_front_server_messenger->get_myaddr()))) {
      if (!osdmap->is_up(whoami)) {
	if (service.is_preparing_to_stop()) {
	  service.got_stop_ack();
	} else {
	  clog.warn() << "map e" << osdmap->get_epoch()
		      << " wrongly marked me down";
	}
      }
      else if (!osdmap->get_addr(whoami).probably_equals(client_messenger
							 ->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong client addr (" << osdmap->get_addr(whoami)
		     << " != my " << client_messenger->get_myaddr() << ")";
      else if (!osdmap->get_cluster_addr(whoami)
	       .probably_equals(cluster_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong cluster addr ("
		     << osdmap->get_cluster_addr(whoami)
		     << " != my " << cluster_messenger->get_myaddr() << ")";
      else if (!osdmap->get_hb_back_addr(whoami)
	       .probably_equals(hb_back_server_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb back addr ("
		     << osdmap->get_hb_back_addr(whoami)
		     << " != my " << hb_back_server_messenger->get_myaddr()
		     << ")";
      else if (osdmap->get_hb_front_addr(whoami) != entity_addr_t() &&
	       !osdmap->get_hb_front_addr(whoami)
	       .probably_equals(hb_front_server_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb front addr ("
		     << osdmap->get_hb_front_addr(whoami)
		     << " != my " << hb_front_server_messenger->get_myaddr()
		     << ")";

      if (!service.is_stopping()) {
	up_epoch = 0;
	do_restart = true;
	bind_epoch = osdmap->get_epoch();

	start_waiting_for_healthy();

	set<int> avoid_ports;
	avoid_ports.insert(cluster_messenger->get_myaddr().get_port());
	avoid_ports.insert(hb_back_server_messenger->get_myaddr().get_port());
	avoid_ports.insert(hb_front_server_messenger->get_myaddr().get_port());

	int r = cluster_messenger->rebind(avoid_ports);
	if (r != 0) {
	  dout(0) << "cluster_messenger rebind failed, shutting down" << dendl;
	  do_shutdown = true;  // FIXME: do_restart?
	}

	r = hb_back_server_messenger->rebind(avoid_ports);
	if (r != 0) {
	  dout(0) << "hb_back_server__messenger rebind failed, shutting down"
		  << dendl;
	  do_shutdown = true;  // FIXME: do_restart?
	}

	r = hb_front_server_messenger->rebind(avoid_ports);
	if (r != 0) {
	  dout(0) << "hb_front_server__messenger rebind failed, shutting down"
		  << dendl;
	  do_shutdown = true;  // FIXME: do_restart?
	}

	hbclient_messenger->mark_down_all();
      }
    }
  }

  // note in the superblock that we were clean thru the prior epoch
  if (boot_epoch && boot_epoch >= superblock.mounted) {
    superblock.mounted = boot_epoch;
  }

  // superblock and commit
  write_superblock(service.meta_col, t);
  store->queue_transaction(
    _t,
    new C_OnMapApply(&service, _t, pinned_maps, osdmap->get_epoch()),
    0, fin);
  service.publish_superblock(superblock);

  ml.unlock();

  check_osdmap_features(store);

  // yay!
  consume_map();

  if (is_active()) {
    activate_map();
  }

  if (m->newest_map && m->newest_map > last) {
    dout(10) << " msg say newest map is " << m->newest_map
	     << ", requesting more" << dendl;
    osdmap_subscribe(osdmap->get_epoch()+1, true);
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

void OSD::check_osdmap_features(ObjectStore *fs)
{
  // adjust required feature bits?

  // we have to be a bit careful here, because we are accessing the
  // Policy structures without taking any lock.	 in particular, only
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

void OSD::advance_vol(epoch_t osd_epoch, OSDVolRef& vol)
{
  // Lock must be held on volume
  epoch_t next_epoch = vol->get_osdmap()->get_epoch() + 1;
  OSDMapRef lastmap = vol->get_osdmap();

  if (lastmap->get_epoch() == osd_epoch)
    return;
  assert(lastmap->get_epoch() < osd_epoch);

  for (;
       next_epoch <= osd_epoch;
       ++next_epoch) {
    OSDMapRef nextmap = service.try_get_map(next_epoch);
    if (!nextmap)
      continue;

    vol->handle_advance_map(nextmap);
  }
  vol->handle_activate_map();
}

/**
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(Transaction& t, C_Contexts *tfin)
{
  // Must be called with lock on osd_lock

  dout(7) << "advance_map epoch " << osdmap->get_epoch()
	  << "	" << vol_map.size() << " voluimes"
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
}

void OSD::consume_map()
{
  // Must be called with lock on osd_lock
  epoch_t to = osdmap->get_epoch();
  dout(7) << "consume_map version " << to << dendl;

#if 0 // For future use, Volume Deletion
  int num_vols;
  list<OSDVolRef> to_remove;
  // scan volumes
  for (auto it = vol_map.begin(); it != vol_map.end(); ++it) {
    OSDVol *vol = it->second;
    vol->lock();
    num_vols++;

    if (!osdmap->vol_exists(it->first))
      to_remove.push_back(vol);

    vol->unlock();
  }
#endif

  notify_state_observers(state, to);

  for (auto it = vol_map.begin(); it != vol_map.end(); ++it) {
    OSDVolRef vol = it->second;
    OSDVol::unique_lock vl(vol->lock);
    advance_vol(to, vol);
    vl.unlock();
  }

  service.pre_publish_map(osdmap);
  service.publish_map(osdmap);
}

void OSD::activate_map()
{
  // requires osd_lock

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " osdmap flagged full, doing onetime osdmap subscribe"
	     << dendl;
    osdmap_subscribe(osdmap->get_epoch() + 1, true);
  }

  // dispatch blocked ops
  unique_lock osd_lk(osd_lock, std::adopt_lock);
  while (! waiting_for_osdmap.empty()) {
    OpRequest* op = &(waiting_for_osdmap.front());
    waiting_for_osdmap.pop_front();
    handle_op(op, osd_lk);
    op->put(); // release queue's ref
  }
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
      assert(0 == "missing an osdmap on disk");	 // we should have all maps.
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
  epoch_t to = osdmap->get_epoch();
  dout(10) << "send_incremental_map " << since << " -> " << to
	   << " to " << con << " " << con->get_peer_addr() << dendl;

  if (since < superblock.oldest_map) {
    // just send latest full map
    MOSDMap *m = new MOSDMap(monc->get_fsid());
    m->oldest_map = superblock.oldest_map;
    m->newest_map = superblock.newest_map;
    get_map_bl(to, m->maps[to]);
    send_map(m, con);
    return;
  }

  if (to > since && (int64_t)(to - since) > cct->_conf->osd_map_share_max_epochs) {
    dout(10) << "  " << (to - since) << " > max " << cct->_conf->osd_map_share_max_epochs
	     << ", only sending most recent" << dendl;
    since = to - cct->_conf->osd_map_share_max_epochs;
  }

  while (since < to) {
    if (to - since > (epoch_t)cct->_conf->osd_map_message_max)
      to = since + cct->_conf->osd_map_message_max;
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
  ObjectHandle oh =
    store->get_object(meta_col, OSD::get_osdmap_pobject_name(e));
  if (oh) {
    found = store->read(
      meta_col, oh, 0, CEPH_READ_ENTIRE, bl) >= 0;
    store->put_object(oh);
  }
  if (found)
    _add_map_bl(e, bl);
  return found;
}

bool OSDService::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  lock_guard mcl(map_cache_lock);
  bool found = map_bl_inc_cache.lookup(e, &bl);
  if (found)
    return true;
  ObjectHandle oh =
    store->get_object(meta_col, OSD::get_inc_osdmap_pobject_name(e));
  if (oh) {
    found = store->read(meta_col, oh, 0,
			CEPH_READ_ENTIRE, bl) >= 0;
    store->put_object(oh);
    if (found)
      _add_map_inc_bl(e, bl);
  }
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
  lock_guard mcl(map_cache_lock);
  map_bl_inc_cache.pin(e, bl);
}

void OSDService::pin_map_bl(epoch_t e, bufferlist &bl)
{
  lock_guard mcl(map_cache_lock);
  map_bl_cache.pin(e, bl);
}

void OSDService::clear_map_bl_cache_pins(epoch_t e)
{
  lock_guard mcl(map_cache_lock);
  map_bl_inc_cache.clear_pinned(e);
  map_bl_cache.clear_pinned(e);
}

OSDMapRef OSDService::_add_map(OSDMap *o)
{
  epoch_t e = o->get_epoch();

  if (cct->_conf->osd_map_dedup) {
    // Dedup against an existing map at a nearby epoch
    OSDMapRef for_dedup = map_cache.lower_bound(e);
    if (for_dedup) {
      OSDMap::dedup(for_dedup.get(), o);
    }
  }
  OSDMapRef l = map_cache.add(e, o);
  return l;
}

OSDMapRef OSDService::try_get_map(epoch_t epoch)
{
  lock_guard mcl(map_cache_lock);
  OSDMapRef retval = map_cache.lookup(epoch);
  if (retval) {
    dout(30) << "get_map " << epoch << " -cached" << dendl;
    return retval;
  }

  OSDMap *map = new OSDMap;
  if (epoch > 0) {
    dout(20) << "get_map " << epoch << " - loading and decoding " << map
	     << dendl;
    bufferlist bl;
    if (!_get_map_bl(epoch, bl)) {
      delete map;
      return OSDMapRef();
    }
    map->decode(bl);
  } else {
    dout(20) << "get_map " << epoch << " - return initial " << map << dendl;
  }
  return _add_map(map);
}

/*
 * require that we have same (or newer) map
 */
bool OSD::require_same_or_newer_map(OpRequest* op, epoch_t epoch)
{
  dout(15) << "require_same_or_newer_map " << epoch << " (i am "
	   << osdmap->get_epoch() << ") " << op << dendl;

  // Must be called with lock on osd_lock

  // do they have a newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << epoch << " > my "
	    << osdmap->get_epoch() << " with " << op << dendl;
    wait_for_new_map(op);
    return false;
  }

  if (epoch < up_epoch) {
    dout(7) << "from pre-up epoch " << epoch << " < "
	    << up_epoch << dendl;
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (op->get_connection()->get_messenger() == cluster_messenger) {
    int from = op->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_cluster_addr(from) != op->get_source_inst().addr) {
      dout(5) << "from dead osd." << from << ", marking down, "
	      << " msg was " << op->get_source_inst().addr
	      << " expected " << (osdmap->have_inst(from) ?
				  osdmap->get_cluster_addr(from) :
				  entity_addr_t())
	      << dendl;
      ConnectionRef con = op->get_connection();
      con->set_priv(NULL);   // break ref <-> session cycle, if any
      cluster_messenger->mark_down(con.get());
      return false;
    }
  }

  // ok, we have at least as new a map as they do.  are we (re)booting?
  if (!is_active()) {
    dout(7) << "still in boot state, dropping message " << *op << dendl;
    return false;
  }

  return true;
}

// =========================================================
// OPS

void OSDService::reply_op_error(OpRequest* op, int err)
{
  reply_op_error(op, err, eversion_t(), 0);
}

void OSDService::reply_op_error(OpRequest* op, int err, eversion_t v,
				version_t uv)
{
  assert(op->get_header().type == CEPH_MSG_OSD_OP);
  int flags;
  flags = op->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);

  op->trace.event("reply_op_error");
  op->trace.keyval("result", err);

  MOSDOpReply *reply = new MOSDOpReply(op, err, osdmap->get_epoch(), flags,
				       true);
  Messenger *msgr = op->get_connection()->get_messenger();
  reply->trace.init("MOSDOpReply", msgr->get_trace_endpoint(), &op->trace);

  reply->set_reply_versions(v, uv);
  reply->libosd_context = op->libosd_context;
  msgr->send_message(reply, op->get_connection());
}

void OSD::handle_op(OpRequest* op, unique_lock& osd_lk)
{
  // we don't need encoded payload anymore
  op->clear_payload();

  // require same or newer map (queues op)
  if (!require_same_or_newer_map(op, op->get_map_epoch()))
    return;

  // blacklisted?
  if (osdmap->is_blacklisted(op->get_source_addr())) {
    dout(4) << "handle_op " << op->get_source_addr() << " is blacklisted"
	    << dendl;
    service.reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source(), op->get_connection().get(),
		      op->get_map_epoch(),
		      static_cast<Session *>(op->get_connection()->get_priv()));

  /* drop osd_lock (osdmap) */
  osd_lk.unlock();

  // object name too long?
  if (op->get_oid().name.size() > MAX_CEPH_OBJECT_NAME_LEN) {
    dout(4) << "handle_op '" << op->get_oid().name << "' is longer than "
	    << MAX_CEPH_OBJECT_NAME_LEN << " bytes!" << dendl;
    service.reply_op_error(op, -ENAMETOOLONG);
    return;
  }

  if (op->rmw_flags == 0) {
    int r = init_op_flags(op);
    if (r) {
      service.reply_op_error(op, r);
      return;
    }
  }

  if (op->may_write()) {
    // full?
    if ((service.check_failsafe_full() ||
	 osdmap->test_flag(CEPH_OSDMAP_FULL) ||
	 op->get_map_epoch() < superblock.last_map_marked_full) &&
	!op->get_source().is_mds()) {  // FIXME: we'll exclude mds
				      // writes for now.
      // Drop the request, since the client will retry when the full
      // flag is unset.
      return;
    }

    // too big?
    if (cct->_conf->osd_max_write_size &&
	op->get_data_len() > cct->_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      derr << "handle_op msg data len " << op->get_data_len()
	   << " > osd_max_write_size "
	   << (cct->_conf->osd_max_write_size << 20)
	   << " on " << *op << dendl;
      service.reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }

  if (cct->_conf->osd_early_reply_at == 1) {
    service.reply_op_error(op, 0);
    return;
  }

  /* pick a queue priority band */
  cohort::OpQueue::Bands band;
  if (op->get_priority() > CEPH_MSG_PRIO_LOW)
    band = cohort::OpQueue::Bands::HIGH;
  else
    band = cohort::OpQueue::Bands::BASE;

  /* enqueue on multi_wq, defers vol resolution */
  op->get(); // take queue ref
  multi_wq->enqueue(*op, band);
} /* handle_op */

void OSD::static_dequeue_op(OSD* osd, OpRequest* op)
{
  osd->dequeue_op(op);
  op->put(); // release queue ref
}

void OSD::static_wq_thread_exit(OSD* osd) {
  OSDVol::wq_thread_exit(osd);
}

void OSD::dequeue_op(OpRequest* op)
{
  ceph::real_time now = ceph::real_clock::now();
  op->set_dequeued_time(now);
  ceph::timespan latency = now - op->get_recv_stamp();
  const boost::uuids::uuid& volume = op->get_volume();

  OSDVolRef vol = _lookup_vol(volume);
  if (!vol) {
    dout(7) << "hit non-existent volume " << volume << dendl;
    service.reply_op_error(op, -ENXIO);
    return;
  }

  dout(10) << "dequeue_op " << op << " prio " << op->get_priority()
	   << " cost " << op->get_cost()
	   << " latency " << latency
	   << " " << *op
	   << " osdvol " << *vol << dendl;

  if (vol->deleting)
    return;

  op->mark_reached_vol();
  vol->do_op(op); // get intrusive ptr

  // finish
  dout(10) << "dequeue_op " << op << " finish" << dendl;
}

// --------------------------------

const char** OSD::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_op_complaint_time", "osd_op_log_threshold",
    "osd_op_history_size", "osd_op_history_duration",
    NULL
  };
  return KEYS;
}

void OSD::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed)
{
  // Nothing for now.
}

// --------------------------------

int OSD::init_op_flags(OpRequest* op)
{
  vector<OSDOp>::iterator iter;

  // client flags have no bearing on whether an op is a read, write, etc.
  op->rmw_flags = 0;

  // set bits based on op codes, called methods.
  for (iter = op->ops.begin(); iter != op->ops.end(); ++iter) {
    if (ceph_osd_op_mode_modify(iter->op.op))
      op->set_write();
    if (ceph_osd_op_mode_read(iter->op.op))
      op->set_read();

    // set READ flag if there are src_objs
    if (iter->oid.name.length())
      op->set_read();

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
