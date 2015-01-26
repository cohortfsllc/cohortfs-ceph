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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/uuid/uuid_io.hpp>
#include <boost/scoped_ptr.hpp>
#include <cassert>

#include <iostream>
#include <string>
using namespace std;

#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "include/ceph_features.h"

#include "common/config.h"

#include "mon/MonMap.h"

#include "msg/Messenger.h"
#include "libosd/Messengers.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/color.h"
#include "common/errno.h"
#include "common/pick_address.h"

#include "perfglue/heap_profiler.h"


#define dout_subsys ceph_subsys_osd

OSD *osd = NULL;
static CephContext* cct;

static void sighup_handler(int signum)
{
  cct->reopen_logs();
}


void handle_osd_signal(int signum)
{
  if (osd)
    osd->handle_signal(signum);
}

void usage()
{
  derr << "usage: ceph-osd -i osdid [--osd-data=path] [--osd-journal=path] "
       << "[--mkfs] [--mkjournal]" << dendl;
  derr << "   --debug_osd N   set debug level (e.g. 10)" << dendl;
  generic_server_usage();
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON, 0);
  ceph_heap_profiler_init();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  bool mkkey = false;
  bool flushjournal = false;
  bool dump_journal = false;
  bool get_journal_fsid = false;
  bool get_osd_fsid = false;
  bool get_cluster_fsid = false;

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, &i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_flag(args, &i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_flag(args, &i, "--mkjournal", (char*)NULL)) {
      mkjournal = true;
    } else if (ceph_argparse_flag(args, &i, "--mkkey", (char*)NULL)) {
      mkkey = true;
    } else if (ceph_argparse_flag(args, &i, "--flush-journal", (char*)NULL)) {
      flushjournal = true;
    } else if (ceph_argparse_flag(args, &i, "--dump-journal", (char*)NULL)) {
      dump_journal = true;
    } else if (ceph_argparse_flag(args, &i, "--get-cluster-fsid", (char*)NULL)) {
      get_cluster_fsid = true;
    } else if (ceph_argparse_flag(args, &i, "--get-osd-fsid", "--get-osd-uuid", (char*)NULL)) {
      get_osd_fsid = true;
    } else if (ceph_argparse_flag(args, &i, "--get-journal-fsid", "--get-journal-uuid", (char*)NULL)) {
      get_journal_fsid = true;
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    derr << "unrecognized arg " << args[0] << dendl;
    usage();
  }

  // whoami
  char *end;
  const char *id = cct->_conf->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);
  if (*end || end == id || whoami < 0) {
    derr << "must specify '-i #' where # is the osd number" << dendl;
    usage();
  }

  if (cct->_conf->osd_data.empty()) {
    derr << "must specify '--osd-data=foo' data path" << dendl;
    usage();
  }

  // the store
  ObjectStore *store = ObjectStore::create(cct,
					   cct->_conf->osd_objectstore,
					   cct->_conf->osd_data,
					   cct->_conf->osd_journal);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  if (mkfs) {
    common_init_finish(cct);
    MonClient mc(cct);
    if (mc.build_initial_monmap() < 0)
      return -1;
    if (mc.get_monmap_privately() < 0)
      return -1;

    int err = OSD::mkfs(cct, store, cct->_conf->osd_data,
			mc.monmap.fsid, whoami);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating empty object store in "
	   << cct->_conf->osd_data << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created object store " << cct->_conf->osd_data;
    if (!cct->_conf->osd_journal.empty())
      *_dout << " journal " << cct->_conf->osd_journal;
    *_dout << " for osd." << whoami << " fsid " << mc.monmap.fsid << dendl;
  }
  if (mkkey) {
    common_init_finish(cct);
    KeyRing *keyring = KeyRing::create_empty();
    if (!keyring) {
      derr << "Unable to get a Ceph keyring." << dendl;
      return 1;
    }

    EntityName ename(cct->_conf->name);
    EntityAuth eauth;

    int ret = keyring->load(cct, cct->_conf->keyring);
    if (ret == 0 &&
	keyring->get_auth(ename, eauth)) {
      derr << "already have key in keyring " << cct->_conf->keyring << dendl;
    } else {
      eauth.key.create(cct, CEPH_CRYPTO_AES);
      keyring->add(ename, eauth);
      bufferlist bl;
      keyring->encode_plaintext(bl);
      int r = bl.write_file(cct->_conf->keyring.c_str(), 0600);
      if (r)
	derr << TEXT_RED << " ** ERROR: writing new keyring to " << cct->_conf->keyring
	     << ": " << cpp_strerror(r) << TEXT_NORMAL << dendl;
      else
	derr << "created new key in keyring " << cct->_conf->keyring << dendl;
    }
  }
  if (mkfs || mkkey)
    exit(0);
  if (mkjournal) {
    common_init_finish(cct);
    int err = store->mkjournal();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating fresh journal " << cct->_conf->osd_journal
	   << " for object store " << cct->_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created new journal " << cct->_conf->osd_journal
	 << " for object store " << cct->_conf->osd_data << dendl;
    exit(0);
  }
  if (flushjournal) {
    common_init_finish(cct);
    int err = store->mount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error flushing journal " << cct->_conf->osd_journal
	   << " for object store " << cct->_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    store->sync_and_flush();
    store->umount();
    derr << "flushed journal " << cct->_conf->osd_journal
	 << " for object store " << cct->_conf->osd_data
	 << dendl;
    exit(0);
  }
  if (dump_journal) {
    common_init_finish(cct);
    int err = store->dump_journal(cout);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error dumping journal " << cct->_conf->osd_journal
	   << " for object store " << cct->_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "dumped journal " << cct->_conf->osd_journal
	 << " for object store " << cct->_conf->osd_data
	 << dendl;
    exit(0);

  }

  if (get_journal_fsid) {
    boost::uuids::uuid fsid;
    int r = store->peek_journal_fsid(&fsid);
    if (r == 0)
      cout << fsid << std::endl;
    exit(r);
  }

  string magic;
  boost::uuids::uuid cluster_fsid, osd_fsid;
  int w;
  int r = OSD::peek_meta(store, magic, cluster_fsid, osd_fsid, w);
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: unable to open OSD superblock on "
	 << cct->_conf->osd_data << ": " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    if (r == -ENOTSUP) {
      derr << TEXT_RED << " **	      please verify that underlying storage "
	   << "supports xattrs" << TEXT_NORMAL << dendl;
    }
    exit(1);
  }
  if (w != whoami) {
    derr << "OSD id " << w << " != my id " << whoami << dendl;
    exit(1);
  }
  if (strcmp(magic.c_str(), CEPH_OSD_ONDISK_MAGIC)) {
    derr << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC
	 << dendl;
    exit(1);
  }

  if (get_cluster_fsid) {
    cout << cluster_fsid << std::endl;
    exit(0);
  }
  if (get_osd_fsid) {
    cout << osd_fsid << std::endl;
    exit(0);
  }

  // create and bind messengers
  OSDMessengers ms;
  r = ms.create(cct, cct->_conf, entity_name_t::OSD(whoami), getpid());
  if (r != 0)
    return r;

  r = ms.bind(cct, cct->_conf);
  if (r != 0)
    return r;

  // Set up crypto, daemonize, etc.
  global_init_daemonize(cct, 0);
  common_init_finish(cct);

  MonClient mc(cct);
  if (mc.build_initial_monmap() < 0)
    return -1;
  global_init_chdir(cct);

  osd = new OSD(cct,
		store,
		whoami,
		ms.cluster,
		ms.client,
		ms.client_xio,
		ms.client_hb,
		ms.front_hb,
		ms.back_hb,
		ms.objecter,
		ms.objecter_xio,
		&mc,
		cct->_conf->osd_data,
		cct->_conf->osd_journal);

  int err = osd->pre_init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    return 1;
  }

  ms.start();

  // start osd
  err = osd->init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    return 1;
  }

  // install signal handlers
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_osd_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_osd_signal);

  osd->final_init();

  if (cct->_conf->inject_early_sigterm)
    kill(getpid(), SIGTERM);

  ms.wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_osd_signal);
  unregister_async_signal_handler(SIGTERM, handle_osd_signal);
  shutdown_async_signal_handler();

  // done
  delete osd;
  cct->put();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-osd: gmon.out should be in " << s << dendl;
  }

  return 0;
}
