// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_DISPATCHER_H
#define CEPH_LIBOSD_DISPATCHER_H

#include "msg/Dispatcher.h"
#include "include/Spinlock.h"

class CephContext;
class OSD;

namespace ceph
{
namespace osd
{

class Dispatcher : public ::Dispatcher {
public:
  struct OnReply {
    virtual ~OnReply() {}
    virtual void on_reply(Message *m) = 0;
    virtual void on_failure(int r) = 0;
    // return false if we expect more replies on this tid
    virtual bool is_last_reply(Message *m) { return true; }
  };

private:
  Messenger *ms;
  ConnectionRef conn;

  Spinlock tid_lock; // protects next_tid and callback map
  ceph_tid_t next_tid;
  typedef map<ceph_tid_t, OnReply*> cb_map;
  cb_map callbacks;

public:
  Dispatcher(CephContext *cct, Messenger *ms, ConnectionRef conn);

  void send_request(Message *m, OnReply *c);

  void shutdown();

  bool ms_dispatch(Message *m);

  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
};

} // namespace osd
} // namespace ceph

#endif // CEPH_LIBOSD_DISPATCHER_H
