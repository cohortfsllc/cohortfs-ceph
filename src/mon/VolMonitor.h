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


class MMonCommand;


class VolMonitor : public PaxosService {

 public:

  VolMap volmap;          // current
  VolMap pending_volmap;  // current + pending updates

  VolMonitor(Monitor *mn, Paxos *p)
    : PaxosService(mn, p)
  {
    // empty
  }

  // pure firtual functions defined in PaxosService
  void create_initial();
  void update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);
  bool preprocess_command(MMonCommand *m);

 private:
  // no copying allowed
  VolMonitor(const VolMonitor &rhs);
  VolMonitor &operator=(const VolMonitor &rhs);
};


#endif // CEPH_VOLMONITOR_H
