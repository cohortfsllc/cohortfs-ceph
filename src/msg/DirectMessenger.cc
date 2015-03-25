// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "DirectMessenger.h"
#include "DispatchStrategy.h"


DirectMessenger::DirectMessenger(CephContext *cct, entity_name_t name,
				 string mname, uint64_t nonce,
				 MessageFactory *factory,
				 DispatchStrategy *my_dispatchers)
  : SimplePolicyMessenger(cct, name, mname, nonce, factory),
    my_dispatchers(my_dispatchers),
    peer_dispatchers(NULL)
{
  my_dispatchers->set_messenger(this);
}

DirectMessenger::~DirectMessenger()
{
  delete my_dispatchers;
}

class DirectConnection : public Connection {
public:
  DirectConnection(Messenger *m) : Connection(m) {}
  virtual bool is_connected() { return true; }
};

void DirectMessenger::set_direct_peer(DirectMessenger *peer)
{
  peer_inst = peer->get_myinst();
  peer_dispatchers = peer->get_direct_dispatcher();
  connection.reset(new DirectConnection(peer));
}

int DirectMessenger::bind(const entity_addr_t &bind_addr)
{
  set_myaddr(bind_addr);
  return 0;
}

int DirectMessenger::start()
{
  my_dispatchers->start();
  return SimplePolicyMessenger::start();
}

int DirectMessenger::shutdown()
{
  // signal wait()
  sem.Put();

  my_dispatchers->shutdown();

  return SimplePolicyMessenger::shutdown();
}

void DirectMessenger::wait()
{
  // wait on signal from shutdown()
  sem.Get();

  my_dispatchers->wait();
}

// return a single connection for all calls to get_connection()
ConnectionRef DirectMessenger::get_connection(const entity_inst_t& dst)
{
  assert(dst == peer_inst); // DirectMessenger can only send to its peer
  return connection;
}

ConnectionRef DirectMessenger::get_loopback_connection()
{
  // allow direct loopback, despite its questionable utility
  if (!loopback_connection)
    loopback_connection.reset(new DirectConnection(this));
  return loopback_connection;
}

// pass messages through the peer's dispatcher
int DirectMessenger::send_message(Message *m, const entity_inst_t& dst)
{
  assert(dst == peer_inst); // DirectMessenger can only send to its peer
  m->set_connection(connection);
  peer_dispatchers->ds_dispatch(m);
  return 0;
}

int DirectMessenger::send_message(Message *m, Connection *con)
{
  if (con && con == loopback_connection) {
    my_dispatchers->ds_dispatch(m);
  } else {
    assert(peer_dispatchers); // missed call to set_direct_peer()
    m->set_connection(connection);
    peer_dispatchers->ds_dispatch(m);
  }
  return 0;
}
