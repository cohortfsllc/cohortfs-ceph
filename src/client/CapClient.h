// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_CAPCLIENT
#define CEPH_CLIENT_CAPCLIENT

#include "include/types.h"
#include "include/xlist.h"

class Cap;
class CapObject;

class CapClient {
 public:
  xlist<CapObject*> cap_list, delayed_caps;
  int num_flushing_caps;
  tid_t last_flush_seq;

  CapClient() : num_flushing_caps(0), last_flush_seq(0) {}
  virtual ~CapClient() {}

  virtual int mark_caps_flushing(CapObject *o) = 0;
  virtual void flush_caps() = 0;
  virtual void cap_delay_requeue(CapObject *o) = 0;
  virtual void send_cap(Cap *cap, int used, int want, int retain, int flush) = 0;
};

#endif
