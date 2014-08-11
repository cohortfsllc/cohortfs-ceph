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

#ifndef XIO_HELO_H
#define XIO_HELO_H

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "msg/msg_types.h"

#define XIO_HELO_FLAG_NONE             0x000
#define XIO_HELO_FLAG_BOUND_ADDR       0x001 // server

class XioHelo
{
public:
  static const int version = 1;

  __u32 vers; // check|discriminator
  __u32 flags;
  entity_inst_t src;
  entity_inst_t dest;
  // sequence numbers?

  XioHelo(__u32 _flags, entity_inst_t _src, entity_inst_t _dest)
    : vers(version), flags(_flags), src(_src), dest(_dest) {}

  inline void encode(buffer::list& bl) const {
    ::encode(vers, bl);
    ::encode(flags, bl);
    ::encode(src, bl);
    ::encode(dest, bl);
  }

  inline void decode(buffer::list::iterator& bl) {
    ::decode(vers, bl);
    ::decode(flags, bl);
    ::decode(src, bl);
    ::decode(dest, bl);
  }
};

WRITE_CLASS_ENCODER(XioHelo);

#endif /* XIO_HELO_H */
