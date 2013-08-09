// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 *
 */

/* Metadata Server Monitor
 */

#ifndef CEPH_VOLMONITOR_H
#define CEPH_VOLMONITOR_H


using namespace std;


#include "PaxosService.h"
#include "Session.h"

#include "vol/VolMap.h"
#include "messages/MVolMap.h"


class MMonCommand;


class VolMonitor : public PaxosService {

 public:

  VolMapRef volmap; // current
  VolMapRef pending_volmap; // current + pending updates
  VolMap::Incremental pending_inc; // increments to go from current to pending

  VolMonitor(Monitor *mn, Paxos *p, const string& service_name) :
    PaxosService(mn, p, service_name),
    volmap(new VolMap)
  {
    // empty
  }

  // pure firtual functions defined in PaxosService
  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void create_pending();
  void update_trim();
  void encode_pending(MonitorDBStore::Transaction* t);
  void encode_full(MonitorDBStore::Transaction* t);
  bool preprocess_query(PaxosServiceMessage *m);
  bool prepare_update(PaxosServiceMessage *m);

  // support functions
  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  MVolMap *build_latest_full(void);
  void check_subs(void);
  void check_sub(Subscription *sub);

 private:

  // no copying allowed
  VolMonitor(const VolMonitor &rhs);
  VolMonitor &operator=(const VolMonitor &rhs);

};


#endif // CEPH_VOLMONITOR_H
