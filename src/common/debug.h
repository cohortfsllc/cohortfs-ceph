// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_DEBUG_H
#define CEPH_DEBUG_H

#include "common/dout.h"

/* Global version of the stuff in common/dout.h
 */

#define dout(v) ldout((cct), v)

#define pdout(v, p) lpdout((cct), v, p)

#define dlog_p(sub, v) ldlog_p1((cct), sub, v)

#define generic_dout(v) lgeneric_dout((cct), v)

#define derr lderr((cct))

#define generic_derr lgeneric_derr((cct))

#endif
