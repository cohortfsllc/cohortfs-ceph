// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_ASSERT_H
#define CEPH_ASSERT_H

#include <cassert>

class CephContext;

namespace ceph
{
void register_assert_context(CephContext *cct);

struct BackTrace;

struct FailedAssertion {
  BackTrace *backtrace;
  FailedAssertion(BackTrace *bt) : backtrace(bt) {}
};
}

using ceph::register_assert_context;
using ceph::FailedAssertion;

#endif
