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
}

bool VolMonitor::prepare_update(PaxosServiceMessage *m)
{
}
