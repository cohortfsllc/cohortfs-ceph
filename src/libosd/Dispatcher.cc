// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Dispatcher.h"
#include "msg/DirectMessenger.h"
#include "msg/FastStrategy.h"
#include "osd/OSD.h"

#define dout_subsys ceph_subsys_osd

LibOSDDispatcher::LibOSDDispatcher(CephContext *cct, OSD *osd)
  : Dispatcher(cct),
    next_tid(0)
{
  DirectMessenger *cmsgr = new DirectMessenger(cct,
      entity_name_t::CLIENT(osd->get_nodeid()), // XXX
      "direct osd client", 0, new FastStrategy());
  DirectMessenger *smsgr = new DirectMessenger(cct,
      entity_name_t::OSD(osd->get_nodeid()),
      "direct osd server", 0, new FastStrategy());

  cmsgr->set_direct_peer(smsgr);
  smsgr->set_direct_peer(cmsgr);

  cmsgr->add_dispatcher_head(this);
  smsgr->add_dispatcher_head(osd);

  ms_client = cmsgr;
  ms_server = smsgr;
}

LibOSDDispatcher::~LibOSDDispatcher()
{
  delete ms_client;
  delete ms_server;
}

struct C_ReplyCond : public C_SafeCond {
  Message **reply;

  C_ReplyCond(Mutex *l, Cond *c, bool *d, Message **reply)
    : C_SafeCond(l, c, d), reply(reply) {}

  void on_reply(Message *m) {
    *reply = m;
    complete(0);
  }
};

void LibOSDDispatcher::shutdown()
{
  ms_client->shutdown();
  ms_server->shutdown();

  // drop any outstanding requests
  tid_lock.lock();
  for (cb_map::iterator i = callbacks.begin(); i != callbacks.end(); ++i)
    i->second->complete(-1);
  tid_lock.unlock();
}

void LibOSDDispatcher::wait()
{
  ms_client->wait();
  ms_server->wait();
}

Message* LibOSDDispatcher::send_and_wait_for_reply(Message *m)
{
  // initialize wait context
  Mutex mtx("LibOSDDispatcher::send_and_wait");
  Cond cond;
  bool done;
  Message *reply = NULL;
  C_ReplyCond *c = new C_ReplyCond(&mtx, &cond, &done, &reply);

  // register tid/callback
  tid_lock.lock();
  m->set_tid(next_tid++);
  callbacks.insert(make_pair(m->get_tid(), c));
  tid_lock.unlock();

  // get connection
  ConnectionRef conn = ms_client->get_connection(ms_server->get_myinst());
  if (conn->get_priv() == NULL) {
    // create a session
    OSD::Session *s = new OSD::Session;
    conn->set_priv(s->get());
    s->con = conn;
    s->entity_name.set_name(ms_client->get_myname());
    s->auid = CEPH_AUTH_UID_DEFAULT;
    s->put();
  }

  // send to server messenger
  ms_client->send_message(m, conn.get());

  // wait for response
  mtx.Lock();
  while (!done)
    cond.Wait(mtx);
  mtx.Unlock();

  return reply;
}

bool LibOSDDispatcher::ms_dispatch(Message *m)
{
  tid_lock.lock();
  cb_map::iterator i = callbacks.find(m->get_tid());
  if (i == callbacks.end()) {
    tid_lock.unlock();
    // drop messages that aren't replies
    return false;
  }

  C_ReplyCond *c = i->second;
  callbacks.erase(i);
  tid_lock.unlock();

  c->on_reply(m);
  return true;
}
