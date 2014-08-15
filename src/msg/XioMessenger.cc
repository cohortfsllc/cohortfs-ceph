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
#include <stdlib.h>
#include <memory>

#include "XioMsg.h"
#include "XioMessenger.h"
#include "common/address_helper.h"
#include "XioHelo.h"
#include "messages/MNop.h"

#define dout_subsys ceph_subsys_xio

Mutex mtx("XioMessenger Package Lock");
atomic_t initialized;

atomic_t XioMessenger::nInstances;

struct xio_mempool *xio_msgr_noreg_mpool;

uint64_t xio_rcount;
static struct xio_session_ops xio_msgr_ops;

/* Accelio API callouts */

/* string table */
static const char *xio_session_event_types[] =
{ "XIO_SESSION_REJECT_EVENT",
  "XIO_SESSION_TEARDOWN_EVENT",
  "XIO_SESSION_NEW_CONNECTION_EVENT",
  "XIO_SESSION_CONNECTION_ESTABLISHED_EVENT",
  "XIO_SESSION_CONNECTION_TEARDOWN_EVENT",
  "XIO_SESSION_CONNECTION_CLOSED_EVENT",
  "XIO_SESSION_CONNECTION_DISCONNECTED_EVENT",
  "XIO_SESSION_CONNECTION_REFUSED_EVENT",
  "XIO_SESSION_CONNECTION_ERROR_EVENT",
  "XIO_SESSION_ERROR_EVENT"
};

namespace xio_log {
  typedef pair<const char*, int> level_pair;
  static const level_pair LEVELS[] = {
    make_pair("fatal", 0),
    make_pair("error", 0),
    make_pair("warn", 1),
    make_pair("info", 5),
    make_pair("debug", 10),
    make_pair("trace", 20)
  };

  int get_level()
  {
    int level = 0;
    for (size_t i = 0; i < sizeof(LEVELS); i++) {
      if (!dlog_p(dout_subsys, LEVELS[i].second))
	break;
      level++;
    }
    return level;
  }

  void log_dout(const char *file, unsigned line,
		const char *function, unsigned level,
		const char *fmt, ...)
  {
    char buffer[2048];
    va_list args;
    va_start(args, fmt);
    int n = vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);

    if (n > 0) {
      const char *short_file = strrchr(file, '/');
      short_file = (short_file == NULL) ? file : short_file + 1;
      
      const level_pair &lvl = LEVELS[level];
      dout(lvl.second) << '[' << lvl.first << "] "
		       << short_file << ':' << line << ' '
		       << function << " - " << buffer << dendl;
    }
  }
}

static int on_session_event(struct xio_session *session,
			    struct xio_session_event_data *event_data,
			    void *cb_user_context)
{
  XioMessenger *msgr =
    static_cast<XioMessenger*>(
      static_cast<XioConnection*>(cb_user_context)->get_messenger());

  dout(4) << "session event: " << xio_session_event_str(event_data->event)
    << ". reason: " << xio_strerror(event_data->reason) << dendl;

  return msgr->session_event(session, event_data, cb_user_context);
}

static int on_new_session(struct xio_session *session,
			  struct xio_new_session_req *req,
			  void *cb_user_context)
{
  XioMessenger *msgr = static_cast<XioMessenger*>(cb_user_context);

  dout(4) << "new session " << session
    << " user_context " << cb_user_context << dendl;

  return (msgr->new_session(session, req, cb_user_context));
}

static int on_msg_send_complete(struct xio_session *session,
				struct xio_msg *rsp,
				void *conn_user_context)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) << "msg send complete: session: " << session
    << " rsp: " << rsp << " user_context " << conn_user_context << dendl;

  return xcon->on_msg_send_complete(session, rsp, conn_user_context);
}

static int on_msg(struct xio_session *session,
		  struct xio_msg *req,
		  int more_in_batch,
		  void *cb_user_context)
{
  XioConnection* xcon __attribute__((unused)) =
    static_cast<XioConnection*>(cb_user_context);

  dout(25) << "on_msg session " << session << " xcon " << xcon << dendl;

  return xcon->on_msg_req(session, req, more_in_batch,
			  cb_user_context);
}

static int on_msg_delivered(struct xio_session *session,
			    struct xio_msg *msg,
			    int more_in_batch,
			    void *conn_user_context)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(25) << "msg delivered session: " << session
    << " msg: " << msg << " more: " << more_in_batch
    << " conn_user_context " << conn_user_context << dendl;

  return xcon->on_msg_delivered(session, msg, more_in_batch,
				conn_user_context);
}

static int on_msg_error(struct xio_session *session,
			enum xio_status error,
			struct xio_msg  *msg,
			void *conn_user_context)
{
  /* XIO promises to flush back undelivered messages */
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) << "msg error session: " << session
    << " error: " << xio_strerror(error) << " msg: " << msg
    << " conn_user_context " << conn_user_context << dendl;

  return xcon->on_msg_error(session, error, msg, conn_user_context);
}

static int on_cancel(struct xio_session *session,
		     struct xio_msg  *msg,
		     enum xio_status result,
		     void *conn_user_context)
{
  XioConnection* xcon __attribute__((unused)) =
    static_cast<XioConnection*>(conn_user_context);

  dout(25) << "on cancel: session: " << session << " msg: " << msg
    << " conn_user_context " << conn_user_context << dendl;

  return 0;
}

static int on_cancel_request(struct xio_session *session,
			     struct xio_msg  *msg,
			     void *conn_user_context)
{
  XioConnection* xcon __attribute__((unused)) =
    static_cast<XioConnection*>(conn_user_context);

  dout(25) << "on cancel request: session: " << session << " msg: " << msg
    << " conn_user_context " << conn_user_context << dendl;

  return 0;
}

/* free functions */
static string xio_uri_from_entity(const entity_addr_t& addr, bool want_port)
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
  if (want_port) {
    xio_uri += ":";
    xio_uri += boost::lexical_cast<std::string>(addr.get_port());
  }

  return xio_uri;
} /* xio_uri_from_entity */

/* XioMessenger */
XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce, int nportals,
			   DispatchStrategy *ds)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    portals(this, nportals),
    dispatch_strategy(ds),
    loop_con(this),
    port_shift(0),
    cluster_protocol(0),
    magic(0),
    special_handling(0),
    global_seq(0),
    bound(false)
{

  if (cct->_conf->xio_trace_xcon)
    magic |= MSG_MAGIC_TRACE_XCON;

  /* package init */
  if (! initialized.read()) {

    mtx.Lock();
    if (! initialized.read()) {

      xio_init();

      unsigned xopt;

      xopt = xio_log::get_level();
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_LEVEL,
		  &xopt, sizeof(unsigned));
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_FN,
		  (const void*)xio_log::log_dout, sizeof(xio_log_fn));

      xopt = 1;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_DISABLE_HUGETBL,
		  &xopt, sizeof(unsigned));

      xopt = XIO_MSGR_IOVLEN;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_IN_IOVLEN,
		  &xopt, sizeof(unsigned));
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_MAX_OUT_IOVLEN,
		  &xopt, sizeof(unsigned));

      /* and unregisterd one */
#define XMSG_MEMPOOL_MIN 4096
#define XMSG_MEMPOOL_MAX 4096

      xio_msgr_noreg_mpool =
	xio_mempool_create(-1 /* nodeid */,
			   XIO_MEMPOOL_FLAG_REGULAR_PAGES_ALLOC);

      (void) xio_mempool_add_allocator(xio_msgr_noreg_mpool, 64, 15,
				       XMSG_MEMPOOL_MAX, XMSG_MEMPOOL_MIN);
      (void) xio_mempool_add_allocator(xio_msgr_noreg_mpool, 256, 15,
				       XMSG_MEMPOOL_MAX, XMSG_MEMPOOL_MIN);
      (void) xio_mempool_add_allocator(xio_msgr_noreg_mpool, 1024, 15,
				       XMSG_MEMPOOL_MAX, XMSG_MEMPOOL_MIN);
      (void) xio_mempool_add_allocator(xio_msgr_noreg_mpool, getpagesize(), 15,
				       XMSG_MEMPOOL_MAX, XMSG_MEMPOOL_MIN);

      /* initialize ops singleton */
      xio_msgr_ops.on_session_event = on_session_event;
      xio_msgr_ops.on_new_session = on_new_session;
      xio_msgr_ops.on_session_established = NULL;
      xio_msgr_ops.on_msg_send_complete	= on_msg_send_complete;
      xio_msgr_ops.on_msg = on_msg;
      xio_msgr_ops.on_msg_delivered = on_msg_delivered;
      xio_msgr_ops.on_msg_error = on_msg_error;
      xio_msgr_ops.on_cancel = on_cancel;
      xio_msgr_ops.on_cancel_request = on_cancel_request;

      xio_rcount = 0;

      /* mark initialized */
      initialized.set(1);
    }
    mtx.Unlock();
  }

  dispatch_strategy->set_messenger(this);

  /* update class instance count */
  nInstances.inc();

} /* ctor */

int XioMessenger::pool_hint(uint32_t dsize) {
  if (dsize > 1024*1024)
    return 0;

  /* if dsize is already present, returns -EEXIST */
  return xio_mempool_add_allocator(xio_msgr_noreg_mpool, dsize, 0,
				   XMSG_MEMPOOL_MAX, XMSG_MEMPOOL_MIN);
}

int XioMessenger::new_session(struct xio_session *session,
			      struct xio_new_session_req *req,
			      void *cb_user_context)
{
  XioHelo xhelo;
  XioConnection *xcon = NULL;
  entity_inst_t s_inst;
  int code = 0;

  /* new xio_sessions now have startup info */
  decode_xiohelo(xhelo, (char*) req->private_data, req->private_data_len);

  conns_sp.lock();

  if (xhelo.flags & XIO_HELO_FLAG_BOUND_ADDR ) {
    /* check for reconnection */
    XioConnection::EntitySet::iterator conn_iter =
      conns_entity_map.find(xhelo.src, XioConnection::EntityComp());
    if (unlikely(conn_iter != conns_entity_map.end())) {
      xcon = (*conn_iter).get(); // could skip ref (conns_sp)
      pthread_spin_lock(&xcon->sp);
      switch (xcon->cstate.startup_state.read()) {
      case XioConnection::IDLE:
	/* normal reconnect */
	xcon->xio_conn_type = XioConnection::PASSIVE;
	xcon->cstate.session_state.set(XioConnection::START);
	xcon->cstate.startup_state.set(
	  XioConnection::ACCEPTING);
	break;
      case XioConnection::CONNECTING:
	/* outbound connection in progress (statup race) */
	code = xio_reject(session, (enum xio_status) EISCONN, NULL, 0);
	pthread_spin_unlock(&xcon->sp);
	xcon->put();
	conns_sp.unlock();
	return code;
	break;
      case XioConnection::ACCEPTING: /* bad user state */
      case XioConnection::READY: /* Accelio state violation */
	code = xio_reject(session, (enum xio_status) EISCONN, NULL, 0);
	pthread_spin_unlock(&xcon->sp);
	xcon->put();
	conns_sp.unlock();
	return code;
	/* RESET? */
	break;
      };
      pthread_spin_unlock(&xcon->sp);
      xcon->put();
    }
  } else {
    /* fall back on the remote's src address (ephemeral port for RDMA) */
    (void) entity_addr_from_sockaddr(
      &s_inst.addr, (struct sockaddr *) &req->src_addr);
  }

  if (likely(! xcon)) {
    // save new XioConnection with state {PASSIVE, INIT, IDLE}
    xcon = new XioConnection(this, XioConnection::PASSIVE, s_inst);
    xcon->session = session;
    struct xio_session_attr attr;
    attr.user_context = xcon;
    xio_modify_session(session, &attr, XIO_SESSION_ATTR_USER_CTX);
    xcon->get(); // sentinel
    conns_list.push_back(*xcon); // MRU
    conns_entity_map.insert(*xcon);
    xcon->cstate.flags |= XioConnection::CState::FLAG_MAPPED;
  }

  conns_sp.unlock();

  code = portals.accept(session, req, cb_user_context);

  return code;
} /* new_session */

void XioMessenger::unmap_connection(XioConnection *xcon) {
  Spinlock::Locker lckr(conns_sp);
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(xcon->peer, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end()) {
    XioConnection *xcon2 = &(*conn_iter);
    if (xcon == xcon2) {
      conns_entity_map.erase(conn_iter);
    }
  }
  /* now find xcon on conns_list, erase, and release sentinel ref */
  XioConnection::ConnList::iterator citer =
    XioConnection::ConnList::s_iterator_to(*xcon);
  /* XXX check if citer on conns_list? */
  conns_list.erase(citer);
  xcon->cstate.flags &= ~XioConnection::CState::FLAG_MAPPED;

  /* release map ref */
  xcon->put();
}

int XioMessenger::session_event(struct xio_session *session,
				struct xio_session_event_data *event_data,
				void *cb_user_context)
{
  XioConnection *xcon;

  switch (event_data->event) {
  case XIO_SESSION_CONNECTION_ESTABLISHED_EVENT:
    xcon = static_cast<XioConnection*>(event_data->conn_user_context);

    dout(4) << "connection established " << event_data->conn
      << " session " << session << " xcon " << xcon << dendl;

    /* notify hook */
    this->ms_deliver_handle_connect(xcon);
    break;

  case XIO_SESSION_NEW_CONNECTION_EVENT:
  {
    struct xio_connection *conn = event_data->conn;
    struct xio_connection_attr xcona;
    entity_inst_t s_inst;

    (void) xio_query_connection(conn, &xcona,
				XIO_CONNECTION_ATTR_CTX|
				XIO_CONNECTION_ATTR_PEER_ADDR);

    struct xio_context_attr xctxa;
    (void) xio_query_context(xcona.ctx, &xctxa, XIO_CONTEXT_ATTR_USER_CTX);

    /* recover stashed XioConnection */
    struct xio_session_attr sattr;
    xio_query_session(session, &sattr, XIO_SESSION_ATTR_USER_CTX);
    xcon = static_cast<XioConnection*>(sattr.user_context);
    assert(xcon);

    xcon->conn = conn;
    xcon->portal = static_cast<XioPortal*>(xctxa.user_context);
    assert(xcon->portal);

    xcona.user_context = xcon->get();
    (void) xio_modify_connection(conn, &xcona, XIO_CONNECTION_ATTR_USER_CTX);

    xcon->cstate.session_state.set(XioConnection::START);
    xcon->cstate.startup_state.set(XioConnection::ACCEPTING);
    xcon->connected.set(true);

    dout(4) << "new connection session " << session
      << " xcon " << xcon << dendl;
  }
  break;
  case XIO_SESSION_CONNECTION_ERROR_EVENT:
    dout(4) << xio_session_event_types[event_data->event]
      << " user_context " << event_data->conn_user_context << dendl;
    /* informational (Eyal)*/
    break;
  case XIO_SESSION_CONNECTION_CLOSED_EVENT: /* orderly discon */
  case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT: /* unexpected discon */
    dout(2) << xio_session_event_types[event_data->event]
      << " user_context " << event_data->conn_user_context << dendl;
    xcon = static_cast<XioConnection*>(event_data->conn_user_context);
    if (likely(!!xcon)) {
      xcon->on_disconnect_event();
    }
    break;
  case XIO_SESSION_CONNECTION_REFUSED_EVENT:
    dout(2) << xio_session_event_types[event_data->event]
      << " user_context " << event_data->conn_user_context << dendl;
    xcon = static_cast<XioConnection*>(event_data->conn_user_context);
    if (likely(!!xcon)) {
      unmap_connection(xcon);
      xcon->on_disconnect_event();
    }
    break;
  case XIO_SESSION_CONNECTION_TEARDOWN_EVENT:
    dout(2) << xio_session_event_types[event_data->event]
      << " user_context " << event_data->conn_user_context << dendl;
    xcon = static_cast<XioConnection*>(event_data->conn_user_context);
    xcon->on_teardown_event();
    break;
  case XIO_SESSION_TEARDOWN_EVENT:
  {
    dout(2) << "xio_session_teardown " << session << dendl;
    xio_session_destroy(session);
  }
    break;
  default:
    break;
  };

  return 0;
}

enum bl_type
{
  BUFFER_PAYLOAD,
  BUFFER_MIDDLE,
  BUFFER_DATA
};

#define MAX_XIO_BUF_SIZE 1044480

static inline int
xio_count_buffers(buffer::list& bl, int& req_size, int& msg_off, int& req_off)
{

  const std::list<buffer::ptr>& buffers = bl.buffers();
  list<bufferptr>::const_iterator pb;
  size_t size, off, count;
  int result;
  int first = 1;

  off = size = 0;
  result = 0;
  for (;;) {
    if (off >= size) {
      if (first) pb = buffers.begin(); else ++pb;
      if (pb == buffers.end()) {
	break;
      }
      off = 0;
      size = pb->length();
      first = 0;
    }
    count = size - off;
    if (!count) continue;
    if (req_size + count > MAX_XIO_BUF_SIZE) {
	count = MAX_XIO_BUF_SIZE - req_size;
    }

    ++result;

    /* advance iov and perhaps request */

    off += count;
    req_size += count;
    ++msg_off;
    if (unlikely(msg_off >= XIO_MSGR_IOVLEN || req_size >= MAX_XIO_BUF_SIZE)) {
      ++req_off;
      msg_off = 0;
      req_size = 0;
    }
  }

  return result;
}

static inline void
xio_place_buffers(buffer::list& bl, XioMsg *xmsg, struct xio_msg*& req,
		  struct xio_iovec_ex*& msg_iov, int& req_size,
		  int ex_cnt, int& msg_off, int& req_off, bl_type type)
{

  const std::list<buffer::ptr>& buffers = bl.buffers();
  list<bufferptr>::const_iterator pb;
  struct xio_iovec_ex* iov;
  size_t size, off, count;
  const char *data = NULL;
  int first = 1;

  off = size = 0;
  for (;;) {
    if (off >= size) {
      if (first) pb = buffers.begin(); else ++pb;
      if (pb == buffers.end()) {
	break;
      }
      off = 0;
      size = pb->length();
      data = pb->c_str();	 // is c_str() efficient?
      first = 0;
    }
    count = size - off;
    if (!count) continue;
    if (req_size + count > MAX_XIO_BUF_SIZE) {
	count = MAX_XIO_BUF_SIZE - req_size;
    }

    /* assign buffer */
    iov = &msg_iov[msg_off];
    iov->iov_base = (void *) (&data[off]);
    iov->iov_len = count;

    switch (type) {
    case BUFFER_DATA:
      //break;
    default:
    {
      struct xio_mempool_obj *mp = get_xio_mp(*pb);
      iov->mr = (mp) ? mp->mr : NULL;
    }
      break;
    }

    /* advance iov(s) */

    off += count;
    req_size += count;
    ++msg_off;

    /* next request if necessary */

    if (unlikely(msg_off >= XIO_MSGR_IOVLEN || req_size >= MAX_XIO_BUF_SIZE)) {
      /* finish this request */
      req->out.pdata_iov.nents = msg_off;
      req->more_in_batch = 1;
      /* advance to next, and write in it if it's not the last one. */
      if (++req_off >= ex_cnt) {
	req = 0;	/* poison.  trap if we try to use it. */
	msg_iov = NULL;
      } else {
	req = &xmsg->req_arr[req_off].msg;
	msg_iov = req->out.pdata_iov.sglist;
      }
      msg_off = 0;
      req_size = 0;
    }
  }
}

int XioMessenger::bind(const entity_addr_t& addr)
{
  bound = true;
  const entity_addr_t *a = &addr;
  if (a->is_blank_ip()) {
    struct entity_addr_t _addr = *a;
    a = &_addr;
    std::vector <std::string> my_sections;
    g_conf->get_my_sections(my_sections);
    std::string rdma_local_str;
    if (g_conf->get_val_from_conf_file(my_sections, "rdma local",
				      rdma_local_str, true) == 0) {
      struct entity_addr_t local_rdma_addr;
      local_rdma_addr = *a;
      const char *ep;
      if (!local_rdma_addr.parse(rdma_local_str.c_str(), &ep)) {
	derr << "ERROR:  Cannot parse rdma local: " << rdma_local_str << dendl;
	return -1;
      }
      if (*ep) {
	derr << "WARNING: 'rdma local trailing garbage ignored: '" << ep << dendl;
      }
      int p = _addr.get_port();
      _addr.set_sockaddr(reinterpret_cast<struct sockaddr *>(
			  &local_rdma_addr.ss_addr()));
      _addr.set_port(p);
    } else {
      derr << "WARNING: need 'rdma local' config for remote use!" <<dendl;
    }
  }

  set_myaddr(*a);

  entity_addr_t shift_addr = *a;
  if (port_shift) {
    shift_addr.set_port(shift_addr.get_port() + port_shift);
  }

  string base_uri = xio_uri_from_entity(shift_addr, false /* want_port */);
  dout(4) << "XioMessenger " << this << " bind: xio_uri "
    << base_uri << ':' << shift_addr.get_port() << dendl;

  return portals.bind(&xio_msgr_ops, base_uri, shift_addr.get_port());
} /* bind */

int XioMessenger::start()
{
  portals.start();
  dispatch_strategy->start();
  started = true;
  return 0;
}

void XioMessenger::wait()
{
  portals.join();
} /* wait */

int XioMessenger::send_message(Message *m, const entity_inst_t& dest)
{
  ConnectionRef conn = get_connection(dest);
  if (conn)
    return send_message(m, &(*conn));
  else
    return EINVAL;
} /* send_message(Message *, const entity_inst_t&) */

int XioMessenger::send_message(Message *m, Connection *con)
{
  if (con == &loop_con) {
    m->set_connection(con);
    m->set_src(get_myinst().name);
    XioLoopbackConnection *xlcon = static_cast<XioLoopbackConnection*>(con);
    m->set_seq(xlcon->next_seq());
    ds_dispatch(m);
    return 0;
  }

  XioConnection *xcon = static_cast<XioConnection*>(con);

  /* If con is not in READY state, we have to enforce policy */
  if (xcon->cstate.session_state.read() != XioConnection::UP) {
    pthread_spin_lock(&xcon->sp);
    if (xcon->cstate.session_state.read() != XioConnection::UP) {
      xcon->outgoing.mqueue.push_back(*m);
      pthread_spin_unlock(&xcon->sp);
      return 0;
    }
    pthread_spin_unlock(&xcon->sp);
  }

  return send_message_impl(m, xcon);
} /* send_message(Message* m, Connection *con) */

static inline XioMsg* pool_alloc_xio_msg(Message *m, XioConnection *xcon,
  int ex_cnt)
{
  struct xio_mempool_obj mp_mem;
  int e = xio_mempool_alloc(xio_msgr_noreg_mpool, sizeof(XioMsg), &mp_mem);
  if (!!e)
    return NULL;
  XioMsg *xmsg = (XioMsg*) mp_mem.addr;
  assert(!!xmsg);
  new (xmsg) XioMsg(m, xcon, mp_mem, ex_cnt);
  return xmsg;
}

int XioMessenger::send_message_impl(Message *m, XioConnection *xcon)
{
  int code = 0;

  m->set_seq(xcon->cstate.next_out_seq());
  m->set_magic(magic); // trace flag
  m->set_special_handling(special_handling);

  m->encode(xcon->get_features(), this->crcflags);

  buffer::list &payload = m->get_payload();
  buffer::list &middle = m->get_middle();
  buffer::list &data = m->get_data();

  int msg_off = 0;
  int req_off = 0;
  int req_size = 0;
  int nbuffers =
    xio_count_buffers(payload, req_size, msg_off, req_off) +
    xio_count_buffers(middle, req_size, msg_off, req_off) +
    xio_count_buffers(data, req_size, msg_off, req_off);

  int ex_cnt = req_off;
  if (msg_off == 0 && ex_cnt > 0) {
    // no buffers for last msg
    dout(10) << "msg_off 0, ex_cnt " << ex_cnt << " -> " << ex_cnt-1 << dendl;
    ex_cnt--;
  }

  /* get an XioMsg frame */
  XioMsg *xmsg = pool_alloc_xio_msg(m, xcon, ex_cnt);
  if (! xmsg) {
    /* could happen if Accelio has been shutdown */
    return ENOMEM;
  }

  dout(4) << __func__ << " " << m << " new XioMsg " << xmsg
       << " req_0 " << &xmsg->req_0.msg << " msg type " << m->get_type()
       << " features: " << xcon->get_features()
       << " conn " << xcon->conn << " sess " << xcon->session << dendl;

  if (magic & (MSG_MAGIC_XIO)) {

    /* XXXX verify */
    switch (m->get_type()) {
    case 43:
    // case 15:
      dout(4) << __func__ << "stop 43 " << m->get_type() << " " << *m << dendl;
      buffer::list &payload = m->get_payload();
      dout(4) << __func__ << "payload dump:" << dendl;
      payload.hexdump(cout);
    }
  }

  struct xio_msg *req = &xmsg->req_0.msg;
  struct xio_iovec_ex *msg_iov = req->out.pdata_iov.sglist;

  if (magic & (MSG_MAGIC_XIO)) {
    dout(4) << "payload: " << payload.buffers().size() <<
      " middle: " << middle.buffers().size() <<
      " data: " << data.buffers().size() <<
      dendl;
  }

  if (unlikely(ex_cnt > 0)) {
    dout(4) << __func__ << " buffer cnt > XIO_MSGR_IOVLEN (" <<
      ((XIO_MSGR_IOVLEN-1) + nbuffers) << ")" << dendl;
  }

  /* do the invariant part */
  msg_off = 0;
  req_off = -1; /* most often, not used */
  req_size = 0;

  xio_place_buffers(payload, xmsg, req, msg_iov, req_size, ex_cnt, msg_off,
		    req_off, BUFFER_PAYLOAD);

  xio_place_buffers(middle, xmsg, req, msg_iov, req_size, ex_cnt, msg_off,
		    req_off, BUFFER_MIDDLE);

  xio_place_buffers(data, xmsg, req, msg_iov, req_size, ex_cnt, msg_off,
		    req_off, BUFFER_DATA);
  dout(10) << "ex_cnt " << ex_cnt << ", req_off " << req_off
    << ", msg_cnt " << xmsg->hdr.msg_cnt << dendl;

  /* finalize request */
  if (msg_off)
    req->out.pdata_iov.nents = msg_off;

  /* fixup first msg */
  req = &xmsg->req_0.msg;

  const std::list<buffer::ptr>& header = xmsg->hdr.get_bl().buffers();
  assert(header.size() == 1); /* XXX */
  list<bufferptr>::const_iterator pb = header.begin();
  req->out.header.iov_base = (char*) pb->c_str();
  req->out.header.iov_len = pb->length();

  /* deliver via xio, preserve ordering */
  xmsg->xio_pre_submit();
  xcon->portal->enqueue_for_send(xcon, xmsg);

  return code;
} /* send_message_impl(Message*, XioConnection *) */

int XioMessenger::shutdown()
{
  portals.shutdown();
  started = false;
  return 0;
} /* shutdown */

ConnectionRef XioMessenger::get_connection(const entity_inst_t& dest)
{
  const entity_inst_t& self_inst = get_myinst();
  if ((&dest == &self_inst) ||
      (dest == self_inst)) {
    return get_loopback_connection();
  }

  entity_inst_t _dest = dest;
  if (port_shift) {
    _dest.addr.set_port(
      _dest.addr.get_port() + port_shift);
  }

retry:
  conns_sp.lock();
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(_dest, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end()) {
    XioConnectionRef cref  = &(*conn_iter); /* ref+1 */
    XioConnection *xcon = cref->get(); /* ref'd above, so safe this scope */
    conns_sp.unlock();
    /* lock xcon until state established */
    pthread_spin_lock(&xcon->sp);
    /* the DISCONNECTED state connotes mapped AND discon (cf. DELETED) */
    uint32_t session_state = xcon->cstate.session_state.read();
    if (unlikely(session_state == XioConnection::DISCONNECTED)) {
      string xio_uri = xio_uri_from_entity(_dest.addr, true /* want_port */);
	  buffer::list xhelo_bl;
	  XioHelo xhelo(bound ? XIO_HELO_FLAG_BOUND_ADDR : XIO_HELO_FLAG_NONE,
			self_inst, dest);
	  ::encode(xhelo, xhelo_bl);
	  /* XXX client session attributes */
	  struct xio_session_attr attr;
	  attr.ses_ops = &xio_msgr_ops;
	  attr.user_context_len = xhelo_bl.length();
	  attr.user_context = malloc(attr.user_context_len);
	  memcpy(attr.user_context, xhelo_bl.c_str(), attr.user_context_len);
	  attr.uri = NULL; /* XXX */
	  xcon->session = xio_session_create(
	    XIO_SESSION_REQ, &attr, xio_uri.c_str(), 0, 0, xcon);
	  /* this should cause callbacks with user context of conn, but
	   * we can always set it explicitly */
	  xcon->conn = xio_connect(
	    xcon->session, this->portals.get_portal0()->ctx, 0, NULL, xcon);
	  xcon->connected.set(true);
	  /* kickoff session negotiation */
	  xcon->cstate.init_state(); /* LOCKED */
    } else if (unlikely(session_state ==
			XioConnection::DELETED)) {
      pthread_spin_unlock(&xcon->sp);
      pthread_yield();
      goto retry;
    }
    pthread_spin_unlock(&xcon->sp);
    return cref;
  }
  else {
    string xio_uri = xio_uri_from_entity(_dest.addr, true /* want_port */);

    dout(4) << "XioMessenger " << this << " get_connection: xio_uri "
      << xio_uri << dendl;

    buffer::list xhelo_bl;
    XioHelo xhelo(bound ? XIO_HELO_FLAG_BOUND_ADDR : XIO_HELO_FLAG_NONE,
    self_inst, dest);
    ::encode(xhelo, xhelo_bl);

    /* XXX client session attributes */
    struct xio_session_attr attr;
    attr.ses_ops = &xio_msgr_ops;
    attr.user_context_len = xhelo_bl.length();
    attr.user_context = malloc(attr.user_context_len);
    memcpy(attr.user_context, xhelo_bl.c_str(), attr.user_context_len);
    attr.uri = NULL; /* XXX */

    XioConnection *xcon = new XioConnection(this, XioConnection::ACTIVE,
					    _dest);
    /* sentinel ref */
    xcon->get();

    /* lock xcon until state established */
    pthread_spin_lock(&xcon->sp);

    conns_list.push_back(*xcon); // MRU
    conns_entity_map.insert(*xcon);
    xcon->cstate.flags |= XioConnection::CState::FLAG_MAPPED;
    conns_sp.unlock();

    xcon->session = xio_session_create(XIO_SESSION_REQ, &attr, xio_uri.c_str(),
				       0, 0, xcon);
    if (! xcon->session)
      abort();

    /* this should cause callbacks with user context of conn, but
     * we can always set it explicitly */
    xcon->conn = xio_connect(xcon->session, this->portals.get_portal0()->ctx,
			     0, NULL, xcon);
    xcon->connected.set(true);

    /* kickoff session negotition */
    xcon->cstate.init_state(); /* LOCKED */

    pthread_spin_unlock(&xcon->sp);

    return xcon->get(); /* nref +1 */
  }
} /* get_connection */

ConnectionRef XioMessenger::get_loopback_connection()
{
  return (loop_con.get());
} /* get_loopback_connection */

void XioMessenger::mark_down(const entity_addr_t& addr)
{
  entity_inst_t inst(entity_name_t(), addr);
  Spinlock::Locker lckr(conns_sp);
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(inst, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end()) {
      (*conn_iter).mark_down(XioConnection::CState::OP_FLAG_NONE);
    }
} /* mark_down(const entity_addr_t& */

void XioMessenger::mark_down(Connection* con)
{
  XioConnection *xcon = static_cast<XioConnection*>(con);
  xcon->mark_down(XioConnection::CState::OP_FLAG_NONE);
} /* mark_down(Connection*) */

void XioMessenger::mark_down_all()
{
  Spinlock::Locker lckr(conns_sp);
  XioConnection::EntitySet::iterator conn_iter;
  for (conn_iter = conns_entity_map.begin(); conn_iter !=
	 conns_entity_map.begin(); ++conn_iter) {
    (*conn_iter).mark_down(XioConnection::CState::OP_FLAG_NONE);
  }
} /* mark_down_all */

static inline XioMarkDownHook* pool_alloc_markdown_hook(
  XioConnection *xcon, Message *m)
{
  struct xio_mempool_obj mp_mem;
  int e = xio_mempool_alloc(xio_msgr_noreg_mpool,
			    sizeof(XioMarkDownHook), &mp_mem);
  if (!!e)
    return NULL;
  XioMarkDownHook *hook = static_cast<XioMarkDownHook*>(mp_mem.addr);
  new (hook) XioMarkDownHook(xcon, m, mp_mem);
  return hook;
}

void XioMessenger::mark_down_on_empty(Connection* con)
{
  XioConnection *xcon = static_cast<XioConnection*>(con);
  MNop* m = new MNop();
  m->tag = XIO_NOP_TAG_MARKDOWN;
  m->set_completion_hook(pool_alloc_markdown_hook(xcon, m));
  // stall new messages
  xcon->cstate.session_state.set(XioConnection::BARRIER);
  (void) send_message_impl(m, xcon);
}

void XioMessenger::mark_disposable(Connection *con)
{
  XioConnection *xcon = static_cast<XioConnection*>(con);
  xcon->mark_disposable(XioConnection::CState::OP_FLAG_NONE);
}

void XioMessenger::try_insert(XioConnection *xcon)
{
  Spinlock::Locker lckr(conns_sp);
  /* already resident in conns_list */
  conns_entity_map.insert(*xcon);
  xcon->cstate.flags |= XioConnection::CState::FLAG_MAPPED;
}

XioMessenger::~XioMessenger()
{
  delete dispatch_strategy;
  nInstances.dec();
} /* dtor */
