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

void LibOSDDispatcher::shutdown()
{
  ms_client->shutdown();
  ms_server->shutdown();

  // drop any outstanding requests
  tid_lock.lock();
  for (cb_map::iterator i = callbacks.begin(); i != callbacks.end(); ++i) {
    i->second->on_failure(-ENODEV);
    delete i->second;
  }
  callbacks.clear();
  tid_lock.unlock();
}

void LibOSDDispatcher::wait()
{
  ms_client->wait();
  ms_server->wait();
}

void LibOSDDispatcher::send_request(Message *m, OnReply *c)
{
  // register tid/callback
  tid_lock.lock();
  const ceph_tid_t tid = next_tid++;
  m->set_tid(tid);
  callbacks.insert(cb_map::value_type(tid, c));
  tid_lock.unlock();

  // get connection
  ConnectionRef conn = ms_client->get_connection(ms_server->get_myinst());
  if (conn->get_priv() == NULL) {
    // create an osd session
    OSD::Session *s = new OSD::Session;
    conn->set_priv(s->get());
    s->con = conn;
    s->entity_name.set_name(ms_client->get_myname());
    s->auid = CEPH_AUTH_UID_DEFAULT;
    s->put();
  }

  // send to server messenger
  ms_client->send_message(m, conn.get());
}

bool LibOSDDispatcher::ms_dispatch(Message *m)
{
  const ceph_tid_t tid = m->get_tid();
  tid_lock.lock();
  cb_map::iterator i = callbacks.find(tid);
  if (i == callbacks.end()) {
    tid_lock.unlock();
    // drop messages that aren't replies
    return false;
  }

  OnReply *c = i->second;
  callbacks.erase(i);
  tid_lock.unlock();

  c->on_reply(m);
  delete c;
  return true;
}
