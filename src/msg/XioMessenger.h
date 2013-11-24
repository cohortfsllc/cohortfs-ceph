// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_MESSENGER_H
#define XIO_MESSENGER_H

extern "C" {
#include "libxio.h"
}
#include "msg/SimplePolicyMessenger.h"
#include "XioConnection.h"


class XioMessenger : public SimplePolicyMessenger
{
private:
  struct xio_context *ctx;
public:
  XioMessenger(CephContext *cct, entity_name_t name,
	       string mname, uint64_t nonce)
    : SimplePolicyMessenger(cct, name, mname, nonce)
    { }

  virtual ~XioMessenger()
    { }

  virtual void set_addr_unknowns(entity_addr_t &addr) 
    { } /* XXX applicable? */

  virtual int get_dispatch_queue_len()
    { return 0; } /* XXX bogus? */

  virtual double get_dispatch_queue_max_age(utime_t now)
    { return 0; } /* XXX bogus? */

  virtual void set_cluster_protocol(int p)
    { }

  virtual int bind(const entity_addr_t& bind_addr)
    { return EINVAL; }

  virtual int start() { started = true; return 0; }

  virtual void wait()
    { }

  virtual int shutdown() { started = false; return 0; }

  virtual int send_message(Message *m, const entity_inst_t& dest)
    { return EINVAL; }

  virtual int send_message(Message *m, Connection *con)
    { return EINVAL; }

  virtual int lazy_send_message(Message *m, const entity_inst_t& dest)
    { return EINVAL; }

  virtual int lazy_send_message(Message *m, Connection *con)
    { return EINVAL; }

  virtual ConnectionRef get_connection(const entity_inst_t& dest)
    { return NULL; }

  virtual ConnectionRef get_loopback_connection()
    { return NULL; }

  virtual int send_keepalive(const entity_inst_t& dest)
    { return EINVAL; }

  virtual int send_keepalive(Connection *con)
    { return EINVAL; } 

  virtual void mark_down(const entity_addr_t& a)
    { }

  virtual void mark_down(Connection *con) 
    { }

  virtual void mark_down_on_empty(Connection *con)
    { }

  virtual void mark_disposable(Connection *con)
    { }

  virtual void mark_down_all()
    { }

protected:
  virtual void ready()
    { }

public:
  
  
  
};

#endif /* XIO_MESSENGER_H */
