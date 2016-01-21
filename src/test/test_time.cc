// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_time.h"
#include "include/buffer.h"
#include <gtest/gtest.h>

TEST(CephTime, EncodeDecode)
{
  auto a = ceph::real_clock::now();

  // encode a
  bufferlist bl;
  ::encode(a, bl);

  auto p = bl.begin();

  // decode into b
  ceph::real_time b;
  ::decode(b, p);

  ASSERT_EQ(a, b);
}
