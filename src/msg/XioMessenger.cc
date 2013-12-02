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

#include <arpa/inet.h>
#include <boost/lexical_cast.hpp>
#include <set>

#include "XioMessenger.h"
#include "common/Mutex.h"

Mutex mtx("XioMessenger Package Lock");
atomic_t initialized;

atomic_t XioMessenger::nInstances;
void* XioMessenger::ev_loop;

static struct xio_session_ops xio_msgr_ops;

extern "C" {

  static int on_session_event(struct xio_session *session,
			      struct xio_session_event_data *event_data,
			      void *cb_user_context);

  static int on_new_session(struct xio_session *session,
			    struct xio_new_session_req *req,
			    void *cb_user_context);
  
  static int on_request(struct xio_session *session,
			struct xio_msg *req,
			int more_in_batch,
			void *cb_user_context);

} /* extern "C" */

void* XioMessenger::EventThread::entry(void)
{
  /* we always need an event loop */
  xio_ev_loop_run(msgr->ev_loop);

  /* unbind only if we actually did a bind */
  if (msgr->bound) {
    xio_unbind(msgr->server);
  }

  return NULL;
}

XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    ctx(0),
    server(0),
    conns_lock("XioMessenger::conns_lock"),
    bound(false),
    event_thread(this)
{
  /* package init */
  if (! initialized.read()) {

    mtx.Lock();
    if (! initialized.read()) {

      xio_init();

      unsigned log_level = XIO_LOG_LEVEL_TRACE;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_LEVEL,
		  &log_level, sizeof(unsigned));

      /* initialize ops singleton */
      xio_msgr_ops.on_session_event = on_session_event;
      xio_msgr_ops.on_session_established = NULL;
      xio_msgr_ops.on_new_session = on_new_session;
      xio_msgr_ops.on_msg_send_complete	= NULL;
      xio_msgr_ops.on_msg = on_request;
      xio_msgr_ops.on_msg_error = NULL;

      ev_loop = xio_ev_loop_init();

      ctx = xio_ctx_open(NULL, ev_loop, 0);

      /* mark initialized */
      initialized.set(1);
    }
    mtx.Unlock();
  }

  /* update class instance count */
  nInstances.inc();

} /* ctor */

static string xio_uri_from_entity(const entity_addr_t& addr)
{
  const char *host = NULL;
  char addr_buf[129];
  
  switch(addr.addr.ss_family) {
  case AF_INET:
    host = inet_ntop(AF_INET, &addr.addr4.sin_addr, addr_buf,
		     INET_ADDRSTRLEN);
    break;
  case AF_INET6:
    host = inet_ntop(AF_INET6, &addr.addr6.sin6_addr, addr_buf,
		     INET6_ADDRSTRLEN);
    break;
  default:
    abort();
    break;
  };

  /* The following can only succeed if the host is rdma-capable */
  string xio_uri = "rdma://";
  xio_uri += host;
  xio_uri += ":";
  xio_uri += boost::lexical_cast<std::string>(addr.get_port());

  return xio_uri;
}

int XioMessenger::bind(const entity_addr_t& addr)
{
  if (bound)
    return (EINVAL);

  string xio_uri = xio_uri_from_entity(addr);

  printf("XioMessenger::bind %s %p\n",
	 xio_uri.c_str(),
	 this);

  server = xio_bind(ctx, &xio_msgr_ops, xio_uri.c_str(), NULL, 0, this);
  if (!server)
    return EINVAL;
  else
    bound = true;  

  return 0;
} /* bind */

int XioMessenger::start()
{
  started = true;
  event_thread.create();
  return 0;
}

void XioMessenger::wait()
{
  event_thread.join();
} /* wait */

int XioMessenger::send_message(Message *m, const entity_inst_t& dest)
{
  ConnectionRef conn = get_connection(dest);
  if (conn)
    return send_message(m, conn.get() /* intrusive_pointer */);
  else
    return EINVAL;
} /* send_message(Message *, const entity_inst_t&) */


static char msg_tag = CEPH_MSGR_TAG_MSG;

int XioMessenger::send_message(Message *m, Connection *con)
{
  /* XXX */
  XioConnection *xcon = static_cast<XioConnection*>(con);
#if 0
  m->set_connection(con);
  m->set_seq(0); // placholder--avoid marshalling in a critical section

  ceph_msg_header& header = m->get_header();
  ceph_msg_footer& footer = m->get_footer();
  
  bufferlist blist = m->get_payload();
  blist.append(m->get_middle());
  blist.append(m->get_data());

  struct xio_msg req;
  struct xio_iovec_ex *msg_iov = req.out.data_iov, *iov;
  int msg_off;

  memset(&req, 0, sizeof(struct xio_msg));
  msg_iov[0].iov_base = &msg_tag;
  msg_iov[0].iov_len = 1;

  msg_iov[1].iov_base = (char*) &header;
  msg_iov[1].iov_len = sizeof(ceph_msg_header);

  /* XXX for now, only handles up to XIO_MAX_IOV */
  if (XIO_MAX_IOV < 3 + blist.buffers().size())
    abort();

  const std::list<buffer::ptr>& buffers = blist.buffers();
  list<bufferptr>::const_iterator pb;

  msg_off = 2;
  for (pb = buffers.begin(); pb != buffers.end(); ++pb) {
    iov = &msg_iov[msg_off];
    iov->iov_base = (void *) pb->c_str(); // is this efficient?
    iov->iov_len = pb->length();
    msg_off++;
  }

  msg_iov[msg_off].iov_base = (void*) &footer;
  msg_iov[msg_off].iov_len = sizeof(ceph_msg_footer);
  msg_off++;

  req.out.data_iovlen = msg_off;

  /* XXX ideally, don't serialize and increment a sequence number,
   * but instead rely on xio ordering */
  //pthread_spin_lock(&xcon->sp);
  // associate and inc seq data
  xio_send_request(xcon->conn, &req);
  //pthread_spin_unlock(&xcon->sp);
#else
    struct xio_msg req;
    memset(&req, 0, sizeof(struct xio_msg));
    req.out.header.iov_base = strdup("im your venus");
    req.out.header.iov_len = strlen((char*)req.out.header.iov_base)+1;
    xio_send_request(xcon->conn, &req);
#endif

  /* XXX */
  m->put();

  return 0;
} /* send_message(Message *, Connection *) */

ConnectionRef XioMessenger::get_connection(const entity_inst_t& dest)
{
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(dest, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end())
    return static_cast<Connection*>(&(*conn_iter));
  else {
    string xio_uri = xio_uri_from_entity(dest.addr);

    /* XXX client session attributes */
    struct xio_session_attr attr = {
      &xio_msgr_ops,
      NULL, /* XXX server private data? */
      0     /* XXX? */
    };

    XioConnection *conn = new XioConnection(this, XioConnection::ACTIVE,
					    dest);

    conn->session = xio_session_open(XIO_SESSION_REQ, &attr, xio_uri.c_str(),
				     0, 0, this);
    if (! conn->session) {
      delete conn;
      return NULL;
    }

    /* this should cause callbacks with user context of conn, but
     * we can always set it explicitly */
    conn->conn = xio_connect(conn->session, ctx, 0, NULL, conn);

    /* conn has nref == 1 */
    conns_entity_map.insert(*conn);

    return conn;
  }
} /* get_connection */

ConnectionRef XioMessenger::get_loopback_connection()
{
  abort();
  return NULL;
} /* get_loopback_connection */

XioMessenger::~XioMessenger()
{
  if (nInstances.dec() == 0) {
    xio_ctx_close(ctx);
    xio_ev_loop_destroy(&ev_loop);
  }
} /* dtor */

// xio hooks
void XioMessenger::xio_new_session(struct xio_session *session,
				   struct xio_new_session_req *req,
				   void *cb_user_context)
{
  
}

extern "C" {

  int xio_conn_get_src_addr(struct xio_conn *conn,
			    struct sockaddr_storage *sa, socklen_t len);

  static int on_session_event(struct xio_session *session,
			      struct xio_session_event_data *event_data,
			      void *cb_user_context)
  {
    XioMessenger *m = static_cast<XioMessenger*>(cb_user_context);
    XioConnection *xcon;

    printf("session event: %s. reason: %s\n",
	   xio_session_event_str(event_data->event),
	   xio_strerror(event_data->reason));

    switch (event_data->event) {
    case XIO_SESSION_NEW_CONNECTION_EVENT:
    {
      struct xio_connection_params params;
      xcon = new XioConnection(m, XioConnection::PASSIVE, entity_inst_t());
      /* XXX the only member at present */
      params.user_context = xcon;
      xio_set_connection_params(event_data->conn, &params);
      printf("new connection session %p xcon %p\n", session, xcon);
    }
      break;
    case XIO_SESSION_CONNECTION_CLOSED_EVENT:
      /* XXXX need to convert session to connection, remove from
	 conn_map, and release */
      xcon = static_cast<XioConnection*>(event_data->conn_user_context);
      /* XXX remove from ephemeral_conns list? */
      xcon->put();
      break;
    case XIO_SESSION_TEARDOWN_EVENT:
      xio_session_close(session);
      break;
    default:
      break;
    };

    return 0;
  }

  static int on_new_session(struct xio_session *session,
			    struct xio_new_session_req *req,
			    void *cb_user_context)
  {
 
    printf("new session %p user_context %p\n", session, cb_user_context);

    xio_accept(session, NULL, 0, NULL, 0);

    return 0;
  }

  static int on_request(struct xio_session *session,
			struct xio_msg *req,
			int more_in_batch,
			void *cb_user_context)
  {
    XioConnection *xcon = static_cast<XioConnection*>(cb_user_context);

    printf("new request session %p xcon %p\n", session, xcon);

    return 0;
  }  

} /* extern "C" */

