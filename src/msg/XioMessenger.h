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

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "XioPortal.h"
#include "DispatchStrategy.h"
#include "include/atomic.h"
#include "common/Thread.h"
#include "common/Mutex.h"
#include "include/Spinlock.h"

class XioMessenger : public SimplePolicyMessenger
{
private:
  static atomic_t nInstances;
  Spinlock conns_sp;
  XioConnection::ConnList conns_list;
  XioConnection::EntitySet conns_entity_map;
  XioPortals portals;
  DispatchStrategy* dispatch_strategy;
  XioLoopbackConnection loop_con;
  int port_shift;
  int cluster_protocol;
  uint32_t magic;
  uint32_t special_handling;
  atomic_t global_seq;
  bool bound;

  friend class XioConnection;

public:
  XioMessenger(CephContext *cct, entity_name_t name,
	       string mname, uint64_t nonce, int nportals,
	       DispatchStrategy* ds);

  virtual ~XioMessenger();

  void set_port_shift(int shift) { port_shift = shift; }

  XioPortal* default_portal() { return portals.get_portal0(); }

  virtual void set_myaddr(const entity_addr_t& a) {
    Messenger::set_myaddr(a);
    loop_con.set_peer_addr(a);
  }

  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }
  uint32_t get_special_handling() { return special_handling; }
  void set_special_handling(int n) { special_handling = n; }
  int pool_hint(uint32_t size);

  uint32_t get_global_seq(uint32_t old=0) {
    uint32_t gseq = global_seq.inc();
    if (old > gseq) {
      global_seq.set(old);
      gseq = global_seq.inc();
    }
    return gseq;
  }

  /* xio hooks */
  int new_session(struct xio_session *session,
		  struct xio_new_session_req *req,
		  void *cb_user_context);

  int session_event(struct xio_session *session,
		    struct xio_session_event_data *event_data,
		    void *cb_user_context);

  /* Messenger interface */
  virtual void set_addr_unknowns(entity_addr_t &addr)
    { } /* XXX applicable? */

  virtual int get_dispatch_queue_len()
    { return 0; } /* XXX bogus? */

  virtual double get_dispatch_queue_max_age(utime_t now)
    { return 0; } /* XXX bogus? */

  /**
   * Get the protocol version we support for the given peer type: either
   * a peer protocol (if it matches our own), the protocol version for the
   * peer (if we're connecting), or our protocol version (if we're accepting).
   */
  int get_proto_version(int peer_type, bool connect) {
    int my_type = my_inst.name.type();

    // set reply protocol version
    if (peer_type == my_type) {
      // internal
      return cluster_protocol;
    } else {
      // public
      if (connect) {
	switch (peer_type) {
	case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
	case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
	case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
	}
      } else {
	switch (my_type) {
	case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
	case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
	case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
	}
      }
    }
    return 0;
  }

  virtual void set_cluster_protocol(int p) {
    cluster_protocol = p;
  }

  virtual int bind(const entity_addr_t& addr);

  virtual int start();

  virtual void wait();

  virtual int shutdown();

  virtual int send_message(Message *m, const entity_inst_t& dest);

  virtual int send_message(Message *m, Connection *con);

  int send_message_impl(Message *m, XioConnection *xcon);

  virtual int lazy_send_message(Message *m, const entity_inst_t& dest)
    { return EINVAL; }

  virtual int lazy_send_message(Message *m, Connection *con)
    { return EINVAL; }

  virtual ConnectionRef get_connection(const entity_inst_t& dest);

  virtual ConnectionRef get_loopback_connection();

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

  void ds_dispatch(Message *m)
    { dispatch_strategy->ds_dispatch(m); }

private:
  void try_insert(XioConnection *xcon);
  void unmap_connection(XioConnection *xcon);

protected:
  virtual void ready()
    { }

public:
};

#endif /* XIO_MESSENGER_H */
