// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_DISPATCHER
#define CEPH_LIBOSD_DISPATCHER

#include "msg/Dispatcher.h"
#include "include/Spinlock.h"

class CephContext;
class DirectMessenger;
class OSD;

class LibOSDDispatcher : public Dispatcher {
public:
  struct OnReply {
    virtual ~OnReply() {}
    virtual void on_reply(ceph_tid_t id, Message *m) = 0;
    virtual void on_failure(ceph_tid_t id, int r) = 0;
  };

private:
  // direct messenger pair
  DirectMessenger *ms_client, *ms_server;

  Spinlock tid_lock; // protects next_tid and callback map
  ceph_tid_t next_tid;
  typedef map<ceph_tid_t, OnReply*> cb_map;
  cb_map callbacks;

public:
  LibOSDDispatcher(CephContext *cct, OSD *osd);
  ~LibOSDDispatcher();

  ceph_tid_t send_request(Message *m, OnReply *c);

  void shutdown();
  void wait();

  bool ms_dispatch(Message *m);

  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
};

#endif // CEPH_LIBOSD_DISPATCHER
