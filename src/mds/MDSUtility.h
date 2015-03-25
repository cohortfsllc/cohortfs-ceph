// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 */

#ifndef MDS_UTILITY_H_
#define MDS_UTILITY_H_

#include "osd/OSDMap.h"
#include "osdc/Objecter.h"
#include "mds/MDSMap.h"
#include "messages/MMDSMap.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "msg/MessageFactory.h"
#include "auth/Auth.h"

/// MDS Utility
/**
 * This class is the parent for MDS utilities, i.e. classes that
 * need access the objects belonging to the MDS without actually
 * acting as an MDS daemon themselves.
 */
class MDSUtility : public Dispatcher {
protected:
  struct Factory : public MessageFactory {
    CephContext *cct;
    MessageFactory *parent;
    Message* create(int type);
  };
  Factory factory;
  Objecter *objecter;
  MDSMap *mdsmap;
  Messenger *messenger;
  MonClient *monc;

  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  cohort::Timer<ceph::mono_clock> timer;

  Context *waiting_for_mds_map;

public:
  MDSUtility(CephContext *cct);
  ~MDSUtility();

  void handle_mds_map(MMDSMap* m);
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			 bool force_new);
  int init();
  void shutdown();
};

#endif /* MDS_UTILITY_H_ */
