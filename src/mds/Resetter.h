// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Greg Farnum <gregf@hq.newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef JOURNAL_RESETTER_H_
#define JOURNAL_RESETTER_H_

#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "osdc/Journaler.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "msg/SimpleMessenger.h"
#include "auth/Auth.h"

/**
 * This class lets you dump out an mds journal for troubleshooting or whatever.
 *
 * It was built to work with cmds so some of the design choices are random.
 * To use, create a Resetter, call init(), and then call dump() with the name
 * of the file to dump to.
 */

class Resetter : public Dispatcher {
public:
  Objecter *objecter;
  Journaler *journaler;
  OSDMap *osdmap;
  SimpleMessenger *messenger;
  MonClient *monc;
  Mutex lock;
  SafeTimer timer;

  /*
   * The messenger should be a valid SimpleMessenger. You should call bind()
   * before passing it in, but not do anything else.
   * The MonClient needs to be valid, and you should have called
   * build_initial_monmap().
   */
  Resetter(SimpleMessenger *messenger_, MonClient *monc_) :
    messenger(messenger_),
    monc(monc_),
    lock("Resetter::lock"), timer(lock)
  {}

  virtual ~Resetter();

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
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  void init(int rank);
  void shutdown();
  void reset();
};

#endif /* JOURNAL_RESETTER_H_ */
