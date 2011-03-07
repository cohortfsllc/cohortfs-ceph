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

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"
#include "msg/SimpleMessenger.h"
#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "osdc/Journaler.h"
#include "mds/mdstypes.h"

#include "common/Timer.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


OSDMap osdmap;
Mutex lock("dumpjournal.cc lock");
Cond cond;

Messenger *messenger = 0;
Objecter *objecter = 0;
Journaler *journaler = 0;
SafeTimer *obj_timer = 0;
SafeTimer *jnl_timer = 0;

class Dumper : public Dispatcher {
  bool ms_dispatch(Message *m) {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OPREPLY:
      objecter->handle_osd_op_reply((MOSDOpReply *)m);
      break;
    case CEPH_MSG_OSD_MAP:
      objecter->handle_osd_map((MOSDMap*)m);
      break;
    default:
      return false;
    }
    return true;
  }
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}

} dispatcher;


void usage() 
{
  exit(1);
}

int main(int argc, const char **argv, const char *envp[]) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_init(args, "dumpjournal", STARTUP_FLAG_FORCE_FG_LOGGING);

  vec_to_argv(args, argc, argv);

  int mds = 0;

  // get monmap
  MonClient mc;
  if (mc.build_initial_monmap() < 0)
    return -1;
  
  // start up network
  SimpleMessenger *messenger = new SimpleMessenger();
  messenger->bind();
  g_conf.daemonize = false; // not us!
  messenger->start();
  messenger->register_entity(entity_name_t::CLIENT());
  messenger->add_dispatcher_head(&dispatcher);

  inodeno_t ino = MDS_INO_LOG_OFFSET + mds;
  unsigned pg_pool = CEPH_METADATA_RULE;

  obj_timer = new SafeTimer(lock);
  jnl_timer = new SafeTimer(lock);
  objecter = new Objecter(messenger, &mc, &osdmap, lock, *obj_timer);
  journaler = new Journaler(ino, pg_pool, CEPH_FS_ONDISK_MAGIC, objecter, 0, 0,  jnl_timer);

  objecter->set_client_incarnation(0);

  bool done;
  journaler->recover(new C_SafeCond(&lock, &cond, &done));
  lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();
  
  uint64_t start = journaler->get_read_pos();
  uint64_t end = journaler->get_write_pos();
  uint64_t len = end-start;
  cout << "journal is " << start << "~" << len << std::endl;

  Filer filer(objecter);
  bufferlist bl;
  filer.read(ino, &journaler->get_layout(), 0,
	     start, len, &bl, 0, new C_SafeCond(&lock, &cond, &done));
    lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();

  cout << "read " << bl.length() << " bytes" << std::endl;
  bl.write_file("mds.journal.dump");
  messenger->shutdown();

  // wait for messenger to finish
  messenger->wait();
  
  return 0;
}

