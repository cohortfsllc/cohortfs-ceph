// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSD_MESSENGERS_H
#define CEPH_OSD_MESSENGERS_H

#include "include/types.h"

class CephContext;
struct md_config_t;
class Messenger;
class Throttle;
struct entity_name_t;

struct OSDMessengers {
  Messenger *cluster;
  Messenger *client;
  Messenger *client_xio;
  Messenger *objecter;
  Messenger *objecter_xio;
  Messenger *client_hb;
  Messenger *front_hb;
  Messenger *back_hb;
  Throttle *byte_throttler;
  Throttle *msg_throttler;

  OSDMessengers();
  ~OSDMessengers();

  // create the messengers and set up policy
  int create(CephContext *cct, md_config_t *conf,
	     const entity_name_t &name, pid_t pid);

  int bind(CephContext *cct, md_config_t *conf);

  void start();

  void cleanup();
};

#endif // CEPH_OSD_MESSENGERS_H
