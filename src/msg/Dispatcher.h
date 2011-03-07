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


#ifndef CEPH_DISPATCHER_H
#define CEPH_DISPATCHER_H

#include "Message.h"
#include "common/config.h"
#include "auth/Auth.h"

class Messenger;

class Dispatcher {
public:
  virtual ~Dispatcher() { }
  Dispatcher() { }

  // how i receive messages
  virtual bool ms_dispatch(Message *m) = 0;

  // after a connection connects
  virtual void ms_handle_connect(Connection *con) { };

  /*
   * on any connection reset.
   * this indicates that the ordered+reliable delivery semantics have 
   * been violated.  messages may have been lost.
   */
  virtual bool ms_handle_reset(Connection *con) = 0;

  // on deliberate reset of connection by remote
  //  implies incoming messages dropped; possibly/probably some of our previous outgoing too.
  virtual void ms_handle_remote_reset(Connection *con) = 0;
  
  // authorization handshake provides mutual authentication of peers.
  //  connecting side
  virtual bool ms_get_authorizer(int dest_type, AuthAuthorizer **a, bool force_new) { return false; };
  //  accepting side
  virtual bool ms_verify_authorizer(Connection *con, int peer_type,
				    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
				    bool& isvalid) { return false; };
};

#endif
