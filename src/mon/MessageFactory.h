// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MON_MESSAGE_FACTORY_H
#define MON_MESSAGE_FACTORY_H

#include "msg/MessageFactory.h"

class CephContext;

class MonMessageFactory : public MessageFactory {
 private:
  CephContext *cct;
 public:
  MonMessageFactory(CephContext *cct) : cct(cct) {}

  Message* create(int type);
};

#endif // MON_MESSAGE_FACTORY_H
