// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CONNECT_HELPER_H
#define CEPH_CONNECT_HELPER_H

#include "XioConnection.h"
#include "messages/MConnect.h"

class ConnectHelper
{
public:

  static int next_state(XioConnection *con, Message *m) {
    // TODO:  implement
    return 0;
  }

};

#endif /* CEPH_CONNNECT_HELPER */
