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

#ifndef XIO_CONNECTION_H
#define XIO_CONNECTION_H

#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive/list.hpp>
extern "C" {
#include "libxio.h"
}
#include "XioInSeq.h"
#include "Connection.h"
#include "Messenger.h"
#include "include/atomic.h"
#include "messages/MConnect.h"
#include "auth/AuthSessionHandler.h"

#define XIO_ALL_FEATURES (CEPH_FEATURES_ALL & \
			  ~CEPH_FEATURE_MSGR_KEEPALIVE2)

namespace bi = boost::intrusive;

class XioPortal;
class XioMessenger;
class XioMsg;

class XioConnection : public Connection
{
public:
  enum type { ACTIVE, PASSIVE };

  enum session_states {
    INIT = 0,
    START,
    UP,
    DISCONNECTED,
    DELETED
  };

  enum session_startup_states {
    IDLE = 0,
    CONNECTING,
    ACCEPTING,
    READY,
    FAIL
  };

private:
  XioConnection::type xio_conn_type;
  XioPortal *portal;
  atomic_t connected;
  entity_inst_t peer;
  struct xio_session *session;
  struct xio_connection	*conn;
  Mutex mtx;
  Cond cv;
  pthread_spinlock_t sp;
  atomic_t send;
  atomic_t recv;
  uint32_t magic;
  uint32_t special_handling;

  /* batching */
  XioInSeq in_seq;

  class CState
  {
  public:

    static const int FLAG_NONE = 0x0000;
    static const int FLAG_BAD_AUTH = 0x0001;
    static const int FLAG_MAPPED = 0x0002;
    static const int FLAG_RESET = 0x0004;

    static const int OP_FLAG_NONE = 0x0000;
    static const int OP_FLAG_LOCKED = 0x0001;
    static const int OP_FLAG_LRU = 0x0002;

    uint64_t features;
    Messenger::Policy policy;

    CryptoKey session_key;
    ceph::shared_ptr<AuthSessionHandler> session_security;
    AuthAuthorizer *authorizer;
    XioConnection *xcon;
    uint32_t protocol_version;

    atomic_t session_state;
    atomic_t startup_state;

    uint32_t reconnects;
    uint32_t connect_seq, global_seq, peer_global_seq;
    uint32_t in_seq, out_seq_acked; // atomic<uint64_t>, got receipt
    atomic_t out_seq; // atomic<uint32_t>

    uint32_t flags;

    CState(XioConnection* _xcon)
      : xcon(_xcon),
	protocol_version(0),
	session_state(INIT),
	startup_state(IDLE),
	in_seq(0),
	out_seq(0),
	flags(FLAG_NONE) {}

    uint64_t get_session_state() {
      return session_state.read();
    }

    uint64_t get_startup_state() {
      return startup_state.read();
    }

    void set_in_seq(uint32_t seq) {
      in_seq = seq;
    }

    uint32_t next_out_seq() {
      return out_seq.inc();
    };

    // state machine
    int init_state();
    int next_state(Message* m);
    int msg_connect(MConnect *m);
    int msg_connect_reply(MConnectReply *m);
    int msg_connect_reply(MConnectAuthReply *m);
    int msg_connect_auth(MConnectAuth *m);
    int msg_connect_auth_reply(MConnectAuthReply *m);
    int state_up_ready();
    int state_discon();
    int state_fail(Message* m, uint32_t flags);

  } cstate; /* CState */

  // message submission queue
  struct SendQ {
    Message::Queue mqueue; // deferred
  } outgoing;

  // conns_entity_map comparison functor
  struct EntityComp
  {
    // for internal ordering
    bool operator()(const XioConnection &lhs,  const XioConnection &rhs) const
      {  return lhs.get_peer().addr < rhs.get_peer().addr; }

    // for external search by entity_inst_t(peer)
    bool operator()(const entity_inst_t &peer, const XioConnection &c) const
      {  return peer.addr < c.get_peer().addr; }

    bool operator()(const XioConnection &c, const entity_inst_t &peer) const
      {  return c.get_peer().addr < peer.addr;  }
  };

  bi::list_member_hook<> conns_hook;
  bi::avl_set_member_hook<> conns_entity_map_hook;

  typedef bi::list< XioConnection,
		    bi::member_hook<XioConnection, bi::list_member_hook<>,
				    &XioConnection::conns_hook > > ConnList;

  typedef bi::member_hook<XioConnection, bi::avl_set_member_hook<>,
			  &XioConnection::conns_entity_map_hook> EntityHook;

  typedef bi::avl_set< XioConnection, EntityHook,
		       bi::compare<EntityComp> > EntitySet;

  friend class XioPortal;
  friend class XioMessenger;
  friend class XioCompletionHook;
  friend class XioMsg;

  int on_disconnect_event() {
    connected.set(false);
    pthread_spin_lock(&sp);
    cstate.state_discon();
    pthread_spin_unlock(&sp);
    return 0;
  }

  int on_teardown_event() {
    pthread_spin_lock(&sp);
    if (!! conn) {
      xio_connection_destroy(conn);
      conn = NULL;
    }
    pthread_spin_unlock(&sp);
    return 0;
  }

  int mark_down(uint32_t flags);

public:
  XioConnection(XioMessenger *m, XioConnection::type _type,
		const entity_inst_t& peer);

  ~XioConnection() {
    if (conn)
      xio_connection_destroy(conn);
  }

  bool is_connected() { return connected.read(); }

  const entity_inst_t& get_peer() const { return peer; }

  XioConnection* get() {
#if 1
    int refs = nref.read();
    cout << "XioConnection::get " << this << " " << refs << std::endl;
#endif
    RefCountedObject::get();
    return this;
  }

  void put() {
    RefCountedObject::put();
#if 1
    int refs = nref.read();
    cout << "XioConnection::put " << this << " " << refs << std::endl;
#endif
  }

  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }
  uint32_t get_special_handling() { return special_handling; }
  void set_special_handling(int n) { special_handling = n; }

  int passive_setup(); /* XXX */

  int on_msg_req(struct xio_session *session, struct xio_msg *req,
		 int more_in_batch, void *cb_user_context);

  int on_msg_delivered(struct xio_session *session, struct xio_msg *msg,
		       int more_in_batch, void *conn_user_context);

  int on_msg_error(struct xio_session *session, enum xio_status error,
		   struct xio_msg  *msg, void *conn_user_context);

  int on_msg_send_complete(struct xio_session *session,
			   struct xio_msg *rsp, void *conn_user_context);

  void msg_send_fail(XioMsg *xmsg, int code);

  void msg_release_fail(struct xio_msg *msg, int code);

  int flush_input_queue(uint32_t flags);
  int discard_input_queue(uint32_t flags);
  int adjust_clru(uint32_t flags);

};

typedef boost::intrusive_ptr<XioConnection> XioConnectionRef;

class XioLoopbackConnection : public Connection
{
private:
  atomic_t seq;
public:
  XioLoopbackConnection(Messenger *m) : Connection(m), seq(0)
    {
      const entity_inst_t& m_inst = m->get_myinst();
      peer_addr = m_inst.addr;
      peer_type = m_inst.name.type();
      set_features(XIO_ALL_FEATURES); /* XXXX set to ours */
    }

  XioLoopbackConnection* get() {
    return static_cast<XioLoopbackConnection*>(RefCountedObject::get());
  }

  virtual bool is_connected() { return true; }

  uint32_t get_seq() {
    return seq.read();
  }

  uint32_t next_seq() {
    return seq.inc();
  }
};

typedef boost::intrusive_ptr<XioLoopbackConnection> LoopbackConnectionRef;

#endif /* XIO_CONNECTION_H */
