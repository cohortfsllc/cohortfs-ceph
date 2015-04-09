// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_RADOS_TYPES_H
#define CEPH_RADOS_TYPES_H

#include <stdint.h>
#include "include/ceph_time.h"

/**
 * @struct obj_watch_t
 * One item from list_watchers
 */
struct obj_watch_t {
  char addr[256];
  int64_t watcher_id;
  uint64_t cookie;
  ceph::timespan timeout;
};

#endif
