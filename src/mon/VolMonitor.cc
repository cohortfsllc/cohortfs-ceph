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

#include "VolMonitor.h"
#include "Monitor.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"

// #include "messages/MMDSMap.h"
// #include "messages/MMDSBeacon.h"
// #include "messages/MMDSLoadTargets.h"
// #include "messages/MMonCommand.h"


  // pure firtual functions defined in PaxosService

void VolMonitor::create_initial()
{
}

void VolMonitor::update_from_paxos()
{
}

void VolMonitor::create_pending()
{
}

void VolMonitor::encode_pending(bufferlist &bl)
{
}

bool VolMonitor::preprocess_query(PaxosServiceMessage *m)
{
  return true;
}

bool VolMonitor::prepare_update(PaxosServiceMessage *m)
{
  return true;
}
