// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_DOUT_H
#define CEPH_DOUT_H

#include <cassert>
#include "include/ceph_time.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/likely.h"
#include "log/Log.h"

#include <iostream>
#include <pthread.h>
#include <streambuf>
#include <sstream>

using std::ostream;
using std::cerr;
using std::hex;
using std::dec;

extern void dout_emergency(const char * const str);
extern void dout_emergency(const std::string &str);

// generic macros
#define dout_prefix *_dout

#define dout_impl(cct, sub, v)						\
  do {									\
  if (unlikely(cct->_conf->subsys.should_gather(sub, v))) {		\
    if (0) {								\
      char __array[((v >= -1) && (v <= 200)) ? 0 : -1] __attribute__((unused)); \
    }									\
    ceph::log::Entry *_dout_e = cct->_log->create_entry(v, sub);	\
    std::ostream _dout_os(&_dout_e->m_streambuf);			\
    CephContext *_dout_cct = cct;					\
    std::ostream* _dout = &_dout_os;

#define lsubdout(cct, sub, v)  dout_impl(cct, ceph_subsys_##sub, v) dout_prefix
#define ldout(cct, v)  dout_impl(cct, dout_subsys, v) dout_prefix
#define lderr(cct) dout_impl(cct, ceph_subsys_, -1) dout_prefix

#define lgeneric_subdout(cct, sub, v) dout_impl(cct, ceph_subsys_##sub, v) *_dout
#define lgeneric_dout(cct, v) dout_impl(cct, ceph_subsys_, v) *_dout
#define lgeneric_derr(cct) dout_impl(cct, ceph_subsys_, -1) *_dout

#define ldlog_p1(cct, sub, lvl)			\
  (unlikely(cct->_conf->subsys.should_gather(sub, lvl)))

// NOTE: depend on magic value in _ASSERT_H so that we detect when
// /usr/include/assert.h clobbers our fancier version.
#define dendl std::flush;				\
  _dout_cct->_log->submit_entry(_dout_e);		\
  }							\
  } while (0)

#endif
