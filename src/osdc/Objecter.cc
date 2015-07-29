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

#include <algorithm>
#include "Objecter.h"
#include "osd/OSDMap.h"

#include "mon/MonClient.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MOSDFailure.h"
#include "messages/MMonCommand.h"

#include <errno.h>

#include "common/config.h"
#include "include/str_list.h"
#include "common/errno.h"


#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "

namespace rados {
  Message* MessageFactory::create(int type)
  {
    switch (type) {
    case CEPH_MSG_OSD_MAP:      return new MOSDMap;
    case CEPH_MSG_OSD_OPREPLY:  return new MOSDOpReply;
    case CEPH_MSG_STATFS_REPLY: return new MStatfsReply;
    default: return parent ? parent->create(type) : nullptr;
    }
  }
};
