// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Dispatcher.h"

#include "msg/Messenger.h"

#define dout_subsys ceph_subsys_osd

#undef dout_prefix
#define dout_prefix (*_dout << "libosd ")

namespace ceph
{
namespace osd
{

Dispatcher::Dispatcher(CephContext *cct, Messenger *ms, ConnectionRef conn)
  : ::Dispatcher(cct),
    ms(ms),
    conn(conn),
    next_tid(0)
{
}

void Dispatcher::shutdown()
{
  // drop any outstanding requests
  tid_lock.lock();
  for (cb_map::iterator i = callbacks.begin(); i != callbacks.end(); ++i) {
    i->second->on_failure(-ENODEV);
    delete i->second;
  }
  callbacks.clear();
  tid_lock.unlock();
}

void Dispatcher::send_request(Message *m, OnReply *c)
{
  // register tid/callback
  tid_lock.lock();
  const ceph_tid_t tid = next_tid++;
  m->set_tid(tid);
  callbacks.insert(cb_map::value_type(tid, c));
  tid_lock.unlock();

  // send to server messenger
  ms->send_message(m, conn.get());
}

bool Dispatcher::ms_dispatch(Message *m)
{
  const ceph_tid_t tid = m->get_tid();
  tid_lock.lock();
  cb_map::iterator i = callbacks.find(tid);
  if (i == callbacks.end()) {
    tid_lock.unlock();
    // drop messages that aren't replies
    ldout(cct, 10) << "ms_dispatch dropping " << *m << dendl;
    return false;
  }

  ldout(cct, 10) << "ms_dispatch " << *m << dendl;
  OnReply *c = i->second;

  bool last_reply = c == nullptr || c->is_last_reply(m);
  if (last_reply)
    callbacks.erase(i);
  tid_lock.unlock();

  if (c) {
    c->on_reply(m);
    if (last_reply)
      delete c;
  }
  return true;
}

} // namespace osd
} // namespace ceph
