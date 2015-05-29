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

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include <cassert>
#include <condition_variable>
#include <iostream>
#include <string>
#include <map>
#include <mutex>

#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>


#include "include/ceph_time.h"
#include "osd/osd_types.h"
#include "osd/OSD.h"
#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "msg/MessageFactory.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/LogEntry.h"
#include "auth/KeyRing.h"
#include "auth/AuthAuthorizeHandler.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDMap.h"
#include "messages/MLog.h"

typedef std::lock_guard<std::mutex> lock_guard;
typedef std::unique_lock<std::mutex> unique_lock;

static CephContext* cct;

#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix _prefix(_dout, get_name())
static ostream& _prefix(std::ostream *_dout, string n) {
  return *_dout << " stub(" << n << ") ";
}


typedef boost::mt11213b rngen_t;
typedef boost::scoped_ptr<Messenger> MessengerRef;
typedef boost::scoped_ptr<Objecter> ObjecterRef;

class TestFactory : public MessageFactory {
  Message* create(int type) {
    return type == CEPH_MSG_OSD_MAP ? new MOSDMap : nullptr;
  }
};

class TestStub : public Dispatcher
{
 protected:
  TestFactory factory;
  MessengerRef messenger;
  MonClient monc;

  std::mutex lock;
  std::condition_variable cond;
  cohort::Timer<ceph::mono_clock> timer;

  bool do_shutdown;
  ceph::timespan tick_time;

  virtual bool ms_dispatch(Message *m) = 0;
  virtual void ms_handle_connect(Connection *con) = 0;
  virtual void ms_handle_remote_reset(Connection *con) = 0;
  virtual int _shutdown() = 0;
  // courtesy method to be implemented by the stubs at their
  // own discretion
  virtual void _tick() { }
  // different stubs may have different needs; if a stub needs
  // to tick, then it must call this function.
  void start_ticking(ceph::timespan t = 1s) {
    tick_time = t;
    if (t == 0s) {
      stop_ticking();
      return;
    }
    dout(20) << __func__ << " adding tick timer" << dendl;
    timer.add_event(tick_time,
		    &TestStub::tick, this);
  }
  // If we have a function to start ticking that the stubs can
  // use at their own discretion, then we should also have a
  // function to disable said ticking to be used the same way.
  // Just in case.
  // For simplicity's sake, we don't cancel the tick right off
  // the bat; instead, we wait for the next tick to kick in and
  // disable itself.
  void stop_ticking() {
    dout(20) << __func__ << " disable tick" << dendl;
    tick_time = 0s;
  }

 public:
  void tick() {
    lock_guard l(lock);
    std::cout << __func__ << std::endl;
    if (do_shutdown || (tick_time == 0ns)) {
      std::cout << __func__ << " "
		<< (do_shutdown ? "shutdown" : "stop ticking")
		<< std::endl;
      return;
    }
    _tick();
    timer.reschedule_me(tick_time);
  }

  virtual const string get_name() = 0;
  virtual int init() = 0;

  virtual int shutdown() {
    unique_lock l(lock);
    do_shutdown = true;
    int r = _shutdown();
    if (r < 0) {
      dout(10) << __func__ << " error shutting down: "
	       << cpp_strerror(-r) << dendl;
      return r;
    }
    monc.shutdown();
    messenger->shutdown();
    return 0;
  }

  virtual void print(ostream &out) {
    out << "stub(" << get_name() << ")";
  }

  void wait() {
    if (messenger != NULL)
      messenger->wait();
  }

  TestStub(CephContext *cct, string who)
    : Dispatcher(cct),
      monc(cct),
      do_shutdown(false),
      tick_time(0s) { }
};

class ClientStub : public TestStub
{
  ObjecterRef objecter;
  rngen_t gen;

 protected:
  bool ms_dispatch(Message *m) {
    lock_guard l(lock);
    dout(1) << "client::" << __func__ << " " << *m << dendl;
    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP:
      objecter->handle_osd_map((MOSDMap*)m);
      cond.notify_all();
      break;
    }
    return true;
  }

  void ms_handle_connect(Connection *con) {
    dout(1) << "client::" << __func__ << " " << con << dendl;
    lock_guard l(lock);
    objecter->ms_handle_connect(con);
  }

  void ms_handle_remote_reset(Connection *con) {
    dout(1) << "client::" << __func__ << " " << con << dendl;
    lock_guard l(lock);
    objecter->ms_handle_remote_reset(con);
  }

  bool ms_handle_reset(Connection *con) {
    dout(1) << "client::" << __func__ << dendl;
    lock_guard l(lock);
    objecter->ms_handle_reset(con);
    return false;
  }

  const string get_name() {
    return "client";
  }

  virtual int _shutdown() {
    if (objecter) {
      objecter->shutdown();
    }
    return 0;
  }

 public:
  ClientStub(CephContext *cct)
    : TestStub(cct, "client"),
      gen((int) time(NULL))
  { }

  int init() {
    int err;
    err = monc.build_initial_monmap();
    if (err < 0) {
      derr << "ClientStub::" << __func__ << " ERROR: build initial monmap: "
	   << cpp_strerror(err) << dendl;
      return err;
    }

    messenger.reset(Messenger::create(cct, entity_name_t::CLIENT(-1),
				      "stubclient", getpid(), &factory));
    assert(messenger.get() != NULL);

    messenger->set_default_policy(
	Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));
    dout(10) << "ClientStub::" << __func__ << " starting messenger at "
	    << messenger->get_myaddr() << dendl;

    objecter.reset(new Objecter(cct, messenger.get(), &monc));
    assert(objecter.get() != NULL);

    monc.set_messenger(messenger.get());
    messenger->add_dispatcher_head(this);
    messenger->start();
    monc.set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD);

    err = monc.init();
    if (err < 0) {
      derr << "ClientStub::" << __func__ << " monc init error: "
	   << cpp_strerror(-err) << dendl;
      return err;
    }

    err = monc.authenticate();
    if (err < 0) {
      derr << "ClientStub::" << __func__ << " monc authenticate error: "
	   << cpp_strerror(-err) << dendl;
      monc.shutdown();
      return err;
    }
    monc.wait_auth_rotating(30s);

    objecter->set_client_incarnation(0);
    monc.renew_subs();

    objecter->wait_for_osd_map();

    dout(10) << "ClientStub::" << __func__ << " done" << dendl;

    return 0;
  }
};

typedef boost::scoped_ptr<AuthAuthorizeHandlerRegistry> AuthHandlerRef;
class OSDStub : public TestStub
{
  AuthHandlerRef auth_handler_registry;
  int whoami;
  OSDSuperblock sb;
  OSDMap osdmap;
  osd_stat_t osd_stat;

  rngen_t gen;
  boost::uniform_int<> mon_osd_rng;

  ceph::real_time last_boot_attempt;
  static const ceph::timespan STUB_BOOT_INTERVAL;


 public:

  enum {
    STUB_MON_OSD_ALIVE	  = 1,
    STUB_MON_OSD_FAILURE  = 3,
    STUB_MON_LOG	  = 5,

    STUB_MON_OSD_FIRST	  = STUB_MON_OSD_ALIVE,
    STUB_MON_OSD_LAST	  = STUB_MON_LOG,
  };

  OSDStub(int whoami, CephContext *cct)
    : TestStub(cct, "osd"),
      auth_handler_registry(new AuthAuthorizeHandlerRegistry(
				  cct,
				  cct->_conf->auth_cluster_required.length() ?
				  cct->_conf->auth_cluster_required :
				  cct->_conf->auth_supported)),
      whoami(whoami),
      gen(whoami),
      mon_osd_rng(STUB_MON_OSD_FIRST, STUB_MON_OSD_LAST)
  {
    dout(20) << __func__ << " auth supported: "
	     << cct->_conf->auth_supported << dendl;
    stringstream ss;
    ss << "client-osd" << whoami;
    messenger.reset(Messenger::create(cct, entity_name_t::OSD(whoami),
				      ss.str().c_str(), getpid(), &factory));

    Throttle throttler(cct,
	cct->_conf->osd_client_message_size_cap);
    uint64_t supported =
      CEPH_FEATURE_UID |
      CEPH_FEATURE_NOSRCADDR;

    messenger->set_default_policy(
	Messenger::Policy::stateless_server(supported, 0));
    messenger->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				    &throttler, NULL);
    messenger->set_policy(entity_name_t::TYPE_MON,
	Messenger::Policy::lossy_client(supported, CEPH_FEATURE_UID |
	  CEPH_FEATURE_OSDENC));
    messenger->set_policy(entity_name_t::TYPE_OSD,
	Messenger::Policy::stateless_server(0,0));

    dout(10) << __func__ << " public addr "
	     << cct->_conf->public_addr << dendl;
    int err = messenger->bind(cct->_conf->public_addr);
    if (err < 0)
      exit(1);

    if (monc.build_initial_monmap() < 0)
      exit(1);

    messenger->start();
    monc.set_messenger(messenger.get());
  }

  int init() {
    dout(10) << __func__ << dendl;
    lock_guard l(lock);

    dout(1) << __func__ << " fsid " << monc.monmap.fsid
	    << " osd_fsid " << cct->_conf->osd_uuid << dendl;
    dout(1) << __func__ << " name " << cct->_conf->name << dendl;

    messenger->add_dispatcher_head(this);
    monc.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);

    int err = monc.init();
    if (err < 0) {
      derr << __func__ << " monc init error: "
	   << cpp_strerror(-err) << dendl;
      return err;
    }

    err = monc.authenticate();
    if (err < 0) {
      derr << __func__ << " monc authenticate error: "
	   << cpp_strerror(-err) << dendl;
      monc.shutdown();
      return err;
    }
    assert(!monc.get_fsid().is_nil());

    monc.wait_auth_rotating(30s);


    dout(10) << __func__ << " creating osd superblock" << dendl;
    sb.cluster_fsid = monc.monmap.fsid;
    sb.osd_fsid = boost::uuids::random_generator()();
    sb.whoami = whoami;
    sb.compat_features = CompatSet();
    dout(20) << __func__ << " " << sb << dendl;
    dout(20) << __func__ << " osdmap " << osdmap << dendl;

    update_osd_stat();

    start_ticking();
    return 0;
  }

  int _shutdown() {

    return 0;
  }

  void boot() {
    dout(1) << __func__ << " boot?" << dendl;

    auto now = ceph::real_clock::now();
    if ((last_boot_attempt > ceph::real_time::min())
	&& ((now - last_boot_attempt)) <= STUB_BOOT_INTERVAL) {
      dout(1) << __func__ << " backoff and try again later." << dendl;
      return;
    }

    dout(1) << __func__ << " boot!" << dendl;
    MOSDBoot *mboot = new MOSDBoot;
    mboot->sb = sb;
    last_boot_attempt = now;
    monc.send_mon_message(mboot);
  }

  void update_osd_stat() {
    struct statfs stbuf;
    int ret = statfs(".", &stbuf);
    if (ret < 0) {
      ret = -errno;
      dout(0) << __func__
	      << " cannot statfs ." << cpp_strerror(ret) << dendl;
      return;
    }

    osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
    osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
    osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;
  }

  void op_alive() {
    dout(10) << __func__ << dendl;
    if (!osdmap.exists(whoami)) {
      dout(0) << __func__ << " I'm not in the osdmap!!\n";
      JSONFormatter f(true);
      osdmap.dump(&f);
      f.flush(*_dout);
      *_dout << dendl;
    }
    if (osdmap.get_epoch() == 0) {
      dout(1) << __func__ << " wait for osdmap" << dendl;
      return;
    }
    epoch_t up_thru = osdmap.get_up_thru(whoami);
    dout(10) << __func__ << "up_thru: " << osdmap.get_up_thru(whoami) << dendl;

    monc.send_mon_message(new MOSDAlive(osdmap.get_epoch(), up_thru));
  }

  void op_failure() {
    dout(10) << __func__ << dendl;
  }

  void op_log() {
    dout(10) << __func__ << dendl;

    MLog *m = new MLog(monc.get_fsid());

    boost::uniform_int<> log_rng(1, 10);
    size_t num_entries = log_rng(gen);
    dout(10) << __func__
	     << " send " << num_entries << " log messages" << dendl;

    auto now = ceph::real_clock::now();
    int seq = 0;
    for (; num_entries > 0; --num_entries) {
      LogEntry e;
      e.who = messenger->get_myinst();
      e.stamp = now;
      e.seq = seq++;
      e.type = CLOG_DEBUG;
      e.msg = "OSDStub::op_log";
      m->entries.push_back(e);
    }

    monc.send_mon_message(m);
  }

  void _tick() {
    if (!osdmap.exists(whoami)) {
      std::cout << __func__ << " not in the cluster; boot!" << std::endl;
      boot();
      return;
    }

    update_osd_stat();

    boost::uniform_int<> op_rng(STUB_MON_OSD_FIRST, STUB_MON_OSD_LAST);
    int op = op_rng(gen);
    switch (op) {
    case STUB_MON_OSD_ALIVE:
      op_alive();
      break;
    case STUB_MON_OSD_FAILURE:
      op_failure();
      break;
    case STUB_MON_LOG:
      op_log();
      break;
    }
  }

  void handle_osd_map(MOSDMap *m) {
    dout(1) << __func__ << dendl;
    if (m->fsid != monc.get_fsid()) {
      dout(0) << __func__
	      << " message fsid " << m->fsid << " != " << monc.get_fsid()
	      << dendl;
      dout(0) << __func__ << " " << m
	      << " from " << m->get_source_inst()
	      << dendl;
      dout(0) << monc.get_monmap() << dendl;
    }
    assert(m->fsid == monc.get_fsid());

    epoch_t first = m->get_first();
    epoch_t last = m->get_last();
    dout(5) << __func__
	    << " epochs [" << first << "," << last << "]"
	    << " current " << osdmap.get_epoch() << dendl;

    if (last <= osdmap.get_epoch()) {
      dout(5) << __func__ << " no new maps here; dropping" << dendl;
      m->put();
      return;
    }

    if (first > osdmap.get_epoch() + 1) {
      dout(5) << __func__
	      << osdmap.get_epoch() + 1 << ".." << (first-1) << dendl;
      if ((m->oldest_map < first && osdmap.get_epoch() == 0) ||
	  m->oldest_map <= osdmap.get_epoch()) {
	monc.sub_want("osdmap", osdmap.get_epoch()+1,
		       CEPH_SUBSCRIBE_ONETIME);
	monc.renew_subs();
	m->put();
	return;
      }
    }

    epoch_t start_full = MAX(osdmap.get_epoch() + 1, first);

    if (m->maps.size() > 0) {
      map<epoch_t,bufferlist>::reverse_iterator rit;
      rit = m->maps.rbegin();
      if (start_full <= rit->first) {
	start_full = rit->first;
	dout(5) << __func__
		<< " full epoch " << start_full << dendl;
	bufferlist &bl = rit->second;
	bufferlist::iterator p = bl.begin();
	osdmap.decode(p);
      }
    }

    for (epoch_t e = start_full; e <= last; e++) {
      map<epoch_t,bufferlist>::iterator it;
      it = m->incremental_maps.find(e);
      if (it == m->incremental_maps.end())
	continue;

      dout(20) << __func__
	       << " incremental epoch " << e
	       << " on full epoch " << start_full << dendl;
      OSDMap::Incremental inc;
      bufferlist &bl = it->second;
      bufferlist::iterator p = bl.begin();
      inc.decode(p);

      osdmap.apply_incremental(inc);
    }
    dout(30) << __func__ << "\nosdmap:\n";
    JSONFormatter f(true);
    osdmap.dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    if (osdmap.is_up(whoami) &&
	osdmap.get_addr(whoami) == messenger->get_myaddr()) {
      dout(1) << __func__
	      << " got into the osdmap and we're up!" << dendl;
    }

    if (m->newest_map && m->newest_map > last) {
      dout(1) << __func__
	      << " they have more maps; requesting them!" << dendl;
      monc.sub_want("osdmap", osdmap.get_epoch()+1, CEPH_SUBSCRIBE_ONETIME);
      monc.renew_subs();
    }

    dout(10) << __func__ << " done" << dendl;
    m->put();
  }

  bool ms_dispatch(Message *m) {
    dout(1) << __func__ << " " << *m << dendl;

    switch (m->get_type()) {
    case CEPH_MSG_OSD_MAP:
      handle_osd_map((MOSDMap*)m);
      break;
    default:
      m->put();
      break;
    }
    return true;
  }

  void ms_handle_connect(Connection *con) {
    dout(1) << __func__ << " " << con << dendl;
    if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      dout(10) << __func__ << " on mon" << dendl;
    }
  }

  void ms_handle_remote_reset(Connection *con) {}

  bool ms_handle_reset(Connection *con) {
    dout(1) << __func__ << dendl;
    OSD::Session *session = (OSD::Session *)con->get_priv();
    if (!session)
      return false;
    session->put();
    return true;
  }

  const string get_name() {
    stringstream ss;
    ss << "osd." << whoami;
    return ss.str();
  }
};

ceph::timespan const OSDStub::STUB_BOOT_INTERVAL = 10s;

#undef dout_prefix
#define dout_prefix *_dout << "main "

const char *our_name = NULL;
vector<TestStub*> stubs;
std::mutex shutdown_lock;
std::condition_variable shutdown_cond;
Context *shutdown_cb = NULL;
cohort::Timer<ceph::mono_clock> *shutdown_timer = nullptr;

void shutdown_notify()
{
  generic_dout(10) << "main::shutdown time has ran out" << dendl;
  unique_lock sl(shutdown_lock);
  shutdown_cond.notify_all();
};

void handle_test_signal(int signum)
{
  if ((signum != SIGINT) && (signum != SIGTERM))
    return;

  std::cerr << "*** Got signal " << sys_siglist[signum] << " ***" << std::endl;
  lock_guard l(shutdown_lock);
  if (shutdown_timer) {
    shutdown_timer->cancel_all_events();
    shutdown_cond.notify_all();
  }
}

void usage() {
  assert(our_name != NULL);

  std::cout << "usage: " << our_name
	    << " <--stub-id ID> [--stub-id ID...]"
	    << std::endl;
  std::cout << "\n\
Global Options:\n\
  -c FILE		    Read configuration from FILE\n\
  --keyring FILE	    Read keyring from FILE\n\
  --help		    This message\n\
\n\
Test-specific Options:\n\
  --stub-id ID1..ID2	    Interval of OSD ids for multiple stubs to mimic.\n\
  --stub-id ID		    OSD id a stub will mimic to be\n\
			    (same as --stub-id ID..ID)\n\
" << std::endl;
}

int get_id_interval(int &first, int &last, string &str)
{
  size_t found = str.find("..");
  string first_str, last_str;
  if (found == string::npos) {
    first_str = last_str = str;
  } else {
    first_str = str.substr(0, found);
    last_str = str.substr(found+2);
  }

  string err;
  first = strict_strtol(first_str.c_str(), 10, &err);
  if ((first == 0) && (!err.empty())) {
    std::cerr << err << std::endl;
    return -1;
  }

  last = strict_strtol(last_str.c_str(), 10, &err);
  if ((last == 0) && (!err.empty())) {
    std::cerr << err << std::endl;
    return -1;
  }
  return 0;
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  our_name = argv[0];
  argv_to_vec(argc, argv, args);

  cct = global_init(&def_args, args,
		    CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY,
		    0);

  common_init_finish(cct);
  cct->_conf->apply_changes(NULL);

  set<int> stub_ids;
  ceph::timespan duration = 300s;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end();) {
    string val;

    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val,
	"--stub-id", (char*) NULL)) {
      int first = -1, last = -1;
      if (get_id_interval(first, last, val) < 0) {
	std::cerr << "** error parsing stub id '" << val << "'" << std::endl;
	exit(1);
      }

      for (; first <= last; ++first)
	stub_ids.insert(first);
    } else if (ceph_argparse_witharg(args, i, &val,
	"--duration", (char*) NULL)) {
      string err;
      duration = strict_strtol(val.c_str(), 10, &err) * 1s;
      if ((duration == 0ns) && (!err.empty())) {
	std::cerr << "** error parsing '--duration " << val << "': '"
		  << err << std::endl;
	exit(1);
      }
    } else if (ceph_argparse_flag(args, &i, "--help", (char*) NULL)) {
      usage();
      exit(0);
    } else {
      std::cerr << "unknown argument '" << *i << "'" << std::endl;
      return 1;
    }
  }

  if (stub_ids.empty()) {
    std::cerr << "** error: must specify at least one '--stub-id <ID>'"
	 << std::endl;
    usage();
    return 1;
  }

  for (set<int>::iterator i = stub_ids.begin(); i != stub_ids.end(); ++i) {
    int whoami = *i;

    std::cout << __func__ << " starting stub." << whoami << std::endl;
    OSDStub *stub = new OSDStub(whoami, cct);
    int err = stub->init();
    if (err < 0) {
      std::cerr << "** osd stub error: " << cpp_strerror(-err) << std::endl;
      return 1;
    }
    stubs.push_back(stub);
  }

  std::cout << __func__ << " starting client stub" << std::endl;
  ClientStub *cstub = new ClientStub(cct);
  int err = cstub->init();
  if (err < 0) {
    std::cerr << "** client stub error: " << cpp_strerror(-err) << std::endl;
    return 1;
  }
  stubs.push_back(cstub);

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_test_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_test_signal);

  unique_lock sl(shutdown_lock);
  shutdown_timer = new cohort::Timer<ceph::mono_clock>();
  if (duration != 0ns) {
    std::cout << __func__
	    << " run test for " << duration << " seconds" << std::endl;
    shutdown_timer->add_event(duration, &shutdown_notify);
  }
  shutdown_cond.wait(sl);

  delete shutdown_timer;
  shutdown_timer = NULL;
  sl.unlock();

  unregister_async_signal_handler(SIGINT, handle_test_signal);
  unregister_async_signal_handler(SIGTERM, handle_test_signal);

  std::cout << __func__ << " waiting for stubs to finish" << std::endl;
  vector<TestStub*>::iterator it;
  int i;
  for (i = 0, it = stubs.begin(); it != stubs.end(); ++it, ++i) {
    if (*it != NULL) {
      (*it)->shutdown();
      (*it)->wait();
      std::cout << __func__ << " finished " << (*it)->get_name() << std::endl;
      delete (*it);
      (*it) = NULL;
    }
  }

  return 0;
}
