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

#include "XioMsg.h"
#include "XioConnection.h"
#include "XioMessenger.h"
#include "messages/MDataPing.h"

#include "auth/none/AuthNoneProtocol.h" // XXX

#include "include/assert.h"
#include "common/dout.h"

extern struct xio_mempool *xio_msgr_mpool;
extern struct xio_mempool *xio_msgr_noreg_mpool;

#define dout_subsys ceph_subsys_xio

void print_xio_msg_hdr(const char *tag, const XioMsgHdr &hdr,
		       const struct xio_msg *msg)
{

  if (msg) {
    dout(4) << tag <<
      " xio msg:" <<
      " sn: " << msg->sn <<
      " timestamp: " << msg->timestamp <<
      dendl;
  }

  dout(4) << tag <<
    " ceph header: " <<
    " front_len: " << hdr.hdr->front_len <<
    " seq: " << hdr.hdr->seq <<
    " tid: " << hdr.hdr->tid <<
    " type: " << hdr.hdr->type <<
    " prio: " << hdr.hdr->priority <<
    " name type: " << (int) hdr.hdr->src.type <<
    " name num: " << (int) hdr.hdr->src.num <<
    " version: " << hdr.hdr->version <<
    " compat_version: " << hdr.hdr->compat_version <<
    " front_len: " << hdr.hdr->front_len <<
    " middle_len: " << hdr.hdr->middle_len <<
    " data_len: " << hdr.hdr->data_len <<
    " xio header: " <<
    " msg_cnt: " << hdr.msg_cnt <<
    dendl;

  dout(4) << tag <<
    " ceph footer: " <<
    " front_crc: " << hdr.ftr->front_crc <<
    " middle_crc: " << hdr.ftr->middle_crc <<
    " data_crc: " << hdr.ftr->data_crc <<
    " sig: " << hdr.ftr->sig <<
    " flags: " << (uint32_t) hdr.ftr->flags <<
    dendl;
}

void print_ceph_msg(const char *tag, Message *m)
{
  if (m->get_magic() & (MSG_MAGIC_XIO & MSG_MAGIC_TRACE_DTOR)) {
    ceph_msg_header& header = m->get_header();
    dout(4) << tag << " header version " << header.version <<
      " compat version " << header.compat_version <<
      dendl;
  }
}

XioConnection::XioConnection(XioMessenger *m, XioConnection::type _type,
			     const entity_inst_t& _peer) :
  Connection(m),
  xio_conn_type(_type),
  portal(m->default_portal()),
  connected(false),
  peer(_peer),
  session(NULL),
  conn(NULL),
  mtx("XioConnection"),
  magic(m->get_magic()),
  in_seq(),
  cstate(this)
{
  pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
  if (xio_conn_type == XioConnection::ACTIVE)
    peer_addr = peer.addr;
  peer_type = peer.name.type();
  set_peer_addr(peer.addr);

  cstate.policy = m->get_policy(_peer.name.type());

  /* XXXX fake features, aieee! */
  set_features(XIO_ALL_FEATURES);
}

int XioConnection::passive_setup()
{
  /* XXX passive setup is a placeholder for (potentially active-side
     initiated) feature and auth* negotiation */
  static bufferlist authorizer_reply; /* static because fake */
  static CryptoKey session_key; /* ditto */
  bool authorizer_valid;

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());

  // fake an auth buffer
  EntityName name;
  name.set_type(peer.name.type());

  AuthNoneAuthorizer auth;
  auth.build_authorizer(name, peer.name.num());

  /* XXX fake authorizer! */
  msgr->ms_deliver_verify_authorizer(
    this, peer_type, CEPH_AUTH_NONE,
    auth.bl,
    authorizer_reply,
    authorizer_valid,
    session_key);

  /* notify hook */
  msgr->ms_deliver_handle_accept(this);

  /* try to insert in conns_entity_map */
  msgr->try_insert(this);
  return (0);
}

#define uint_to_timeval(tv, s) ((tv).tv_sec = (s), (tv).tv_usec = 0)

static inline XioDispatchHook* pool_alloc_xio_completion_hook(
  XioConnection *xcon, Message *m, XioInSeq& msg_seq)
{
  struct xio_mempool_obj mp_mem;
  int e = xio_mempool_alloc(xio_msgr_noreg_mpool,
			    sizeof(XioDispatchHook), &mp_mem);
  if (!!e)
    return NULL;
  XioDispatchHook *xhook = (XioDispatchHook*) mp_mem.addr;
  new (xhook) XioDispatchHook(xcon, m, msg_seq, mp_mem);
  return xhook;
}

int XioConnection::on_msg_req(struct xio_session *session,
			      struct xio_msg *req,
			      int more_in_batch,
			      void *cb_user_context)
{
  struct xio_msg *treq = req;
  bool xio_ptr;

  /* XXX Accelio guarantees message ordering at
   * xio_session */

  if (! in_seq.p()) {
    if (!treq->in.header.iov_len) {
	derr << __func__ << " empty header: packet out of sequence?" << dendl;
	return 0;
    }
    XioMsgCnt msg_cnt(
      buffer::create_static(treq->in.header.iov_len,
			    (char*) treq->in.header.iov_base));
    dout(10) << __func__ << " receive req " << "treq " << treq
      << " msg_cnt " << msg_cnt.msg_cnt
      << " iov_base " << treq->in.header.iov_base
      << " iov_len " << (int) treq->in.header.iov_len
      << " nents " << treq->in.pdata_iov.nents
      << " conn " << conn << " sess " << session
      << " sn " << treq->sn << dendl;
    assert(session == this->session);
    in_seq.set_count(msg_cnt.msg_cnt);
  } else {
    /* XXX major sequence error */
    assert(! treq->in.header.iov_len);
  }

  in_seq.append(req);
  if (in_seq.count() > 0) {
    return 0;
  }

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());
  XioDispatchHook *m_hook =
    pool_alloc_xio_completion_hook(this, NULL /* msg */, in_seq);
  XioInSeq& msg_seq = m_hook->msg_seq;
  in_seq.clear();

  ceph_msg_header header;
  ceph_msg_footer footer;
  buffer::list payload, middle, data;

  struct timeval t1, t2;

  dout(4) << __func__ << " " << "msg_seq.size()="  << msg_seq.size() <<
    dendl;

  struct xio_msg* msg_iter = msg_seq.begin();
  treq = msg_iter;
  XioMsgHdr hdr(header, footer,
		buffer::create_static(treq->in.header.iov_len,
				      (char*) treq->in.header.iov_base));

  uint_to_timeval(t1, treq->timestamp);

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    if (hdr.hdr->type == 43) {
      print_xio_msg_hdr("on_msg_req", hdr, NULL);
    }
  }

  unsigned int ix, blen, iov_len;
  struct xio_iovec_ex *msg_iov, *iovs;
  uint32_t take_len, left_len = 0;
  char *left_base = NULL;

  ix = 0;
  blen = header.front_len;

  while (blen && (msg_iter != msg_seq.end())) {
    treq = msg_iter;
    xio_ptr = (req->in.sgl_type == XIO_SGL_TYPE_IOV_PTR);
    iov_len = (xio_ptr) ? treq->in.pdata_iov.nents : treq->in.data_iov.nents;
    iovs = (xio_ptr) ? treq->in.pdata_iov.sglist : treq->in.data_iov.sglist;
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];

      /* XXX need to detect any buffer which needs to be
       * split due to coalescing of a segment (front, middle,
       * data) boundary */

      take_len = MIN(blen, msg_iov->iov_len);
      payload.append(
	buffer::create_msg(
	  take_len, (char*) msg_iov->iov_base, m_hook));
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len) {
	  left_base = ((char*) msg_iov->iov_base) + take_len;
	}
      }
    }
    /* XXX as above, if a buffer is split, then we needed to track
     * the new start (carry) and not advance */
    if (ix == iov_len) {
      msg_seq.next(&msg_iter);
      ix = 0;
    }
  }

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    if (hdr.hdr->type == 43) {
      dout(4) << "front (payload) dump:";
      payload.hexdump( *_dout );
      *_dout << dendl;
    }
  }

  blen = header.middle_len;

  if (blen && left_len) {
    middle.append(
      buffer::create_msg(left_len, left_base, m_hook));
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    treq = msg_iter;
    xio_ptr = (req->in.sgl_type == XIO_SGL_TYPE_IOV_PTR);
    iov_len = (xio_ptr) ? treq->in.pdata_iov.nents : treq->in.data_iov.nents;
    iovs = (xio_ptr) ? treq->in.pdata_iov.sglist : treq->in.data_iov.sglist;
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
      take_len = MIN(blen, msg_iov->iov_len);
      middle.append(
	buffer::create_msg(
	  take_len, (char*) msg_iov->iov_base, m_hook));
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len) {
	  left_base = ((char*) msg_iov->iov_base) + take_len;
	}
      }
    }
    if (ix == iov_len) {
      msg_seq.next(&msg_iter);
      ix = 0;
    }
  }

  blen = header.data_len;

  if (blen && left_len) {
    data.append(
      buffer::create_msg(left_len, left_base, m_hook));
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    treq = msg_iter;
    xio_ptr = (req->in.sgl_type == XIO_SGL_TYPE_IOV_PTR);
    iov_len = (xio_ptr) ? treq->in.pdata_iov.nents : treq->in.data_iov.nents;
    iovs = (xio_ptr) ? treq->in.pdata_iov.sglist : treq->in.data_iov.sglist;
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
      data.append(
	buffer::create_msg(
	  msg_iov->iov_len, (char*) msg_iov->iov_base, m_hook));
      blen -= msg_iov->iov_len;
    }
    if (ix == iov_len) {
      msg_seq.next(&msg_iter);
      ix = 0;
    }
  }

  uint_to_timeval(t2, treq->timestamp);

  /* update connection timestamp */
  recv.set(treq->timestamp);

  Message *m =
    decode_message(msgr->cct, msgr->crcflags, header, footer, payload, middle,
		   data);

  if (m) {
    /* completion */
    m->set_connection(this);

    /* reply hook */
    m_hook->set_message(m);
    m->set_completion_hook(m_hook);

    /* trace flag */
    m->set_magic(magic);

    /* update timestamps */
    m->set_recv_stamp(t1);
    m->set_recv_complete_stamp(t2);
    m->set_seq(header.seq);

    /* handle connect negotiation */
    if (unlikely(cstate.get_session_state() == XioConnection::START))
      return cstate.next_state(m);

    assert(cstate.get_session_state() == XioConnection::UP);

    /* MP-SAFE */
    cstate.set_in_seq(header.seq);

#if 0 /* XXXX going away, replaced by challenge-response */
    if (peer_type != (int) hdr.peer_type) { /* XXX isn't peer_type -1? */
      peer_type = hdr.peer_type;
      peer_addr = hdr.addr;
      peer.addr = peer_addr;
      peer.name = hdr.hdr->src;
      if (xio_conn_type == XioConnection::PASSIVE) {
	/* XXX kick off feature/authn/authz negotiation
	 * nb:  very possibly the active side should initiate this, but
	 * for now, call a passive hook so OSD and friends can create
	 * sessions without actually negotiating
	 */
	passive_setup();
      }
    }
#endif

    if (magic & (MSG_MAGIC_TRACE_XCON)) {
      dout(4) << "decode m is " << m->get_type() << dendl;
    }

    /* dispatch it */
    msgr->ds_dispatch(m);
  } else {
    /* responds for undecoded messages and frees hook */
    dout(4) << "decode m failed" << dendl;
    m_hook->on_err_finalize(this);
  }

  return 0;
}

int XioConnection::on_msg_send_complete(struct xio_session *session,
					struct xio_msg *rsp,
					void *conn_user_context)
{
  abort(); /* XXX */
} /* on_msg_send_complete */

extern uint64_t xio_rcount;

int XioConnection::on_msg_delivered(struct xio_session *session,
				    struct xio_msg *req,
				    int more_in_batch,
				    void *conn_user_context)
{
  /* requester send complete (one-way) */
  uint64_t rc = ++xio_rcount;

  XioMsg* xmsg = static_cast<XioMsg*>(req->user_context);
  if (unlikely(magic & MSG_MAGIC_TRACE_CTR)) {
    if (unlikely((rc % 1000000) == 0)) {
      std::cout << "xio finished " << rc << " " << time(0) << std::endl;
    }
  } /* trace ctr */

  dout(11) << "on_msg_delivered xcon: " << xmsg->xcon <<
    " session: " << session << " msg: " << req << " sn: " << req->sn <<
    " type: " << xmsg->m->get_type() << " tid: " << xmsg->m->get_tid() <<
    " seq: " << xmsg->m->get_seq() << dendl;

  xmsg->put();

  return 0;
}  /* on_msg_delivered */

void XioConnection::msg_send_fail(XioMsg *xmsg, int code)
{
  dout(4) << "xio_send_msg FAILED " << &xmsg->req_0.msg << " code=" << code <<
    " (" << xio_strerror(code) << ")" << dendl;
  /* return refs taken for each xio_msg */
  xmsg->put_msg_refs();
} /* msg_send_fail */

void XioConnection::msg_release_fail(struct xio_msg *msg, int code)
{
  dout(4) << "xio_release_msg FAILED " << msg <<  "code=" << code <<
    " (" << xio_strerror(code) << ")" << dendl;
} /* msg_release_fail */

int XioConnection::flush_input_queue(uint32_t flags) {
  XioMessenger* msgr = static_cast<XioMessenger*>(get_messenger());
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);
  int ix, q_size = outgoing.mqueue.size();
  for (ix = 0; ix < q_size; ++ix) {
    Message::Queue::iterator q_iter = outgoing.mqueue.begin();
    Message* m = &(*q_iter);
    outgoing.mqueue.erase(q_iter);
    msgr->send_message_impl(m, this);
  }
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);
  return 0;
}

int XioConnection::discard_input_queue(uint32_t flags)
{
  Message::Queue disc_q;
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);
  Message::Queue::const_iterator i1 = disc_q.end();
  disc_q.splice(i1, outgoing.mqueue);
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  int ix, q_size =  disc_q.size();
  for (ix = 0; ix < q_size; ++ix) {
    Message::Queue::iterator q_iter = disc_q.begin();
    Message* m = &(*q_iter);
    disc_q.erase(q_iter);
    m->put();
  }
  return 0;
}

int XioConnection::adjust_clru(uint32_t flags)
{
  if (flags & CState::OP_FLAG_LOCKED)
    pthread_spin_unlock(&sp);

  XioMessenger* msgr = static_cast<XioMessenger*>(get_messenger());
  msgr->conns_sp.lock();
  pthread_spin_lock(&sp);

  if (cstate.flags & CState::FLAG_MAPPED) {
    XioConnection::ConnList::iterator citer =
      XioConnection::ConnList::s_iterator_to(*this);
    msgr->conns_list.erase(citer);
    msgr->conns_list.push_front(*this); // LRU
  }

  msgr->conns_sp.unlock();

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  return 0;
}

int XioConnection::on_msg_error(struct xio_session *session,
				enum xio_status error,
				struct xio_msg  *msg,
				void *conn_user_context)
{
  XioMsg *xmsg = static_cast<XioMsg*>(msg->user_context);

  /* XXXX need to return these to the front of the input queue */
#if 1
  if (xmsg)
    xmsg->put();
#else
#endif

  return 0;
} /* on_msg_error */

int XioConnection::mark_down(uint32_t flags)
{
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);

  // per interface comment, we only stage a remote reset if the
  // current policy required it
  if (cstate.policy.resetcheck)
    cstate.flags |= CState::FLAG_RESET;

  // Accelio disconnect
  xio_disconnect(conn);

  // XXX always discrd input--but are we in startup?  ie, should mark_down
  // be forcing a disconnect?
  discard_input_queue(flags|CState::OP_FLAG_LOCKED);

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  return 0;
}

int XioConnection::mark_disposable(uint32_t flags)
{
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);

  cstate.policy.lossy = true;

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  return 0;
}

int XioConnection::CState::init_state()
{
  dout(11) << __func__ << " ENTER " << dendl;

  assert(xcon->xio_conn_type==XioConnection::ACTIVE);
  session_state.set(XioConnection::START);
  startup_state.set(XioConnection::CONNECTING);
  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  MConnect* m = new MConnect();
  m->addr = msgr->get_myinst().addr;
  m->name = msgr->get_myinst().name;
  m->flags = 0;

  // XXXX needed?  correct?
  m->connect_seq = ++connect_seq;
  m->last_in_seq = in_seq;
  m->last_out_seq = out_seq.read();

  // send it
  msgr->send_message_impl(m, xcon);

  return 0;
}

int XioConnection::CState::next_state(Message* m)
{
  dout(11) << __func__ << " ENTER " << dendl;

  switch (m->get_type()) {
  case MSG_CONNECT:
    return msg_connect(static_cast<MConnect*>(m));
  break;
  case MSG_CONNECT_REPLY:
    return msg_connect_reply(static_cast<MConnectReply*>(m));
    break;
  case MSG_CONNECT_AUTH:
    return msg_connect_auth(static_cast<MConnectAuth*>(m));
  break;
  case MSG_CONNECT_AUTH_REPLY:
    return msg_connect_auth_reply(static_cast<MConnectAuthReply*>(m));
    break;
  default:
    abort();
  };

  m->put();
  return 0;
} /* next_state */

int XioConnection::CState::state_up_ready() {
  dout(11) << __func__ << " ENTER " << dendl;

  xcon->flush_input_queue(OP_FLAG_NONE);

  session_state.set(UP);
  startup_state.set(READY);

  return (0);
}

int XioConnection::CState::state_discon() {
  dout(11) << __func__ << " ENTER " << dendl;

  session_state.set(DISCONNECTED);
  startup_state.set(IDLE);

  return 0;
}

int XioConnection::CState::state_fail(Message* m, uint32_t flags)
{
  if (! (flags & OP_FLAG_LOCKED))
    pthread_spin_lock(&xcon->sp);

  // advance to state FAIL, drop queued, msgs, adjust LRU
  session_state.set(DISCONNECTED);
  startup_state.set(FAIL);

  xcon->discard_input_queue(flags|OP_FLAG_LOCKED);
  xcon->adjust_clru(flags|OP_FLAG_LOCKED|OP_FLAG_LRU);

  if (! (flags & OP_FLAG_LOCKED))
    pthread_spin_unlock(&xcon->sp);

  // notify ULP
  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  msgr->ms_deliver_handle_reset(xcon);
  m->put();

  return 0;
}

int XioConnection::CState::msg_connect(MConnect *m)
{
  if (xcon->xio_conn_type != XioConnection::PASSIVE) {
    m->put();
    return -EINVAL;
  }

  dout(11) << __func__ << " ENTER " << dendl;

  xcon->peer.name = m->name;
  xcon->peer.addr = xcon->peer_addr = m->addr;
  xcon->peer_type = m->name.type();
  xcon->peer_addr = m->addr;

  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  policy = msgr->get_policy(xcon->peer_type);

  dout(11) << "accept of host_type " << xcon->peer_type
	   << ", policy.lossy=" << policy.lossy
	   << " policy.server=" << policy.server
	   << " policy.standby=" << policy.standby
	   << " policy.resetcheck=" << policy.resetcheck
	   << dendl;

  MConnectReply* m2 = new MConnectReply();
  m2->addr = msgr->get_myinst().addr;
  m2->name = msgr->get_myinst().name;
  m2->flags = 0;

  // XXXX needed?  correct?
  m2->last_in_seq = in_seq;
  m2->last_out_seq = out_seq.read();

  // send m2
  msgr->send_message_impl(m2, xcon);

  // dispose m
  m->put();

  return 0;
} /* msg_connect */

int XioConnection::CState::msg_connect_reply(MConnectReply *m)
{
  if (xcon->xio_conn_type != XioConnection::ACTIVE) {
    m->put();
    return -EINVAL;
  }

  dout(11) << __func__ << " ENTER " << dendl;

 // XXX do we need any data from this phase?
  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  authorizer =
    msgr->ms_deliver_get_authorizer(xcon->peer_type, false /* force_new */);

  MConnectAuth* m2 = new MConnectAuth();
  m2->features = policy.features_supported;
  m2->flags = 0;
  if (policy.lossy)
    m2->flags |= CEPH_MSG_CONNECT_LOSSY; // fyi, actually, server decides

  // XXX move seq capture to init_state()?
  m2->global_seq = global_seq = msgr->get_global_seq(); // msgr-wide seq
  m2->connect_seq = connect_seq; // semantics?

  // serialize authorizer in data[0]
  m2->authorizer_protocol = authorizer ? authorizer->protocol : 0;
  m2->authorizer_len = authorizer ? authorizer->bl.length() : 0;
  if (m2->authorizer_len) {
    buffer::ptr bp = buffer::create(m2->authorizer_len);
    bp.copy_in(0 /* off */, m2->authorizer_len, authorizer->bl.c_str());
    m2->get_data().append(bp);
  }

  // send m2
  msgr->send_message_impl(m2, xcon);

  // dispose m
  m->put();

  return 0;
} /* msg_connect_reply 1 */

int XioConnection::CState::msg_connect_reply(MConnectAuthReply *m)
{
  if (xcon->xio_conn_type != XioConnection::ACTIVE) {
    m->put();
    return -EINVAL;
  }

  dout(11) << __func__ << " ENTER " << dendl;

 // XXX do we need any data from this phase?
  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  authorizer =
    msgr->ms_deliver_get_authorizer(xcon->peer_type, true /* force_new */);

  MConnectAuth* m2 = new MConnectAuth();
  m2->features = policy.features_supported;
  m2->flags = 0;
  if (policy.lossy)
    m2->flags |= CEPH_MSG_CONNECT_LOSSY; // fyi, actually, server decides

  // XXX move seq capture to init_state()?
  m2->global_seq = global_seq = msgr->get_global_seq(); // msgr-wide seq
  m2->connect_seq = connect_seq; // semantics?

  // serialize authorizer in data[0]
  m2->authorizer_protocol = authorizer ? authorizer->protocol : 0;
  m2->authorizer_len = authorizer ? authorizer->bl.length() : 0;
  if (m2->authorizer_len) {
    buffer::ptr bp = buffer::create(m2->authorizer_len);
    bp.copy_in(0 /* off */, m2->authorizer_len, authorizer->bl.c_str());
    m2->get_data().append(bp);
  }

  // send m2
  msgr->send_message_impl(m2, xcon);

  // dispose m
  m->put();

  return 0;
} /* msg_connect_reply 2 */

int XioConnection::CState::msg_connect_auth(MConnectAuth *m)
{
  if (xcon->xio_conn_type != XioConnection::PASSIVE) {
    m->put();
    return -EINVAL;
  }

  dout(11) << __func__ << " ENTER " << dendl;

  bool auth_valid;
  uint64_t fdelta;
  buffer::list auth_bl, auth_reply_bl;
  buffer::ptr bp;

  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  int peer_type = xcon->peer.name.type();

  MConnectAuthReply* m2 = new MConnectAuthReply();

  m2->protocol_version = protocol_version =
    msgr->get_proto_version(peer_type, false);
  if (m->protocol_version != m2->protocol_version) {
    m2->tag = CEPH_MSGR_TAG_BADPROTOVER;
    goto send_m2;
  }

  // required CephX features
  if (m->authorizer_protocol == CEPH_AUTH_CEPHX) {
    if (peer_type == CEPH_ENTITY_TYPE_OSD ||
	peer_type == CEPH_ENTITY_TYPE_MDS) {
      if (msgr->cct->_conf->cephx_require_signatures ||
	  msgr->cct->_conf->cephx_cluster_require_signatures) {
	policy.features_required |= CEPH_FEATURE_MSG_AUTH;
	}
    } else {
      if (msgr->cct->_conf->cephx_require_signatures ||
	  msgr->cct->_conf->cephx_service_require_signatures) {
	policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
    }
  }

  fdelta = policy.features_required & ~(uint64_t(m->features));
  if (fdelta) {
    m2->tag = CEPH_MSGR_TAG_FEATURES;
    goto send_m2;
  }

  // decode authorizer
  if (m->authorizer_len) {
    bp = buffer::create(m->authorizer_len);
    bp.copy_in(0 /* off */, m->authorizer_len, m->get_data().c_str());
    auth_bl.push_back(bp);
  }

  if (!msgr->ms_deliver_verify_authorizer(
	xcon->get(), peer_type, m->authorizer_protocol, auth_bl,
	auth_reply_bl, auth_valid, session_key) || !auth_valid) {
    m2->tag = CEPH_MSGR_TAG_BADAUTHORIZER;
    session_security.reset();
    goto send_m2;
  }

  // RESET check
  if (policy.resetcheck) {
    pthread_spin_lock(&xcon->sp);
    if (xcon->cstate.flags & FLAG_RESET) {
      m2->tag = CEPH_MSGR_TAG_RESETSESSION;
      // XXX need completion functor (XioMsg::on_msg_delivered)
      pthread_spin_unlock(&xcon->sp);
      goto send_m2;
    }
    pthread_spin_unlock(&xcon->sp);
  }

  // XXX sequence checks

  // ready
  m2->tag = CEPH_MSGR_TAG_READY;
  m2->features = policy.features_supported;
  m2->global_seq = msgr->get_global_seq();
  m2->connect_seq = connect_seq;
  m2->flags = 0;
  m2->authorizer_len = auth_reply_bl.length();
  if (m2->authorizer_len) {
    buffer::ptr bp = buffer::create(m2->authorizer_len);
    bp.copy_in(0 /* off */, m2->authorizer_len, auth_reply_bl.c_str());
    m2->get_data().append(bp);
  }

  if (policy.lossy)
    m2->flags |= CEPH_MSG_CONNECT_LOSSY;

  // XXXX locking?
  features = m2->features;

  session_security.reset(
    get_auth_session_handler(msgr->cct, m2->authorizer_protocol,
			     session_key, features));

  // notify ULP
  msgr->ms_deliver_handle_accept(xcon);

  state_up_ready();

  // XXX need queue for up-ready races (other side)?

send_m2:
  msgr->send_message_impl(m2, xcon);

  // dispose m
  m->put();

  return 0;
} /* msg_connect_auth */

int XioConnection::CState::msg_connect_auth_reply(MConnectAuthReply *m)
{
  if (xcon->xio_conn_type != XioConnection::ACTIVE) {
    m->put();
    return -EINVAL;
  }

  dout(11) << __func__ << " ENTER " << dendl;

  buffer::list auth_bl;
  buffer::ptr bp;

  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());

  m->features = ceph_sanitize_features(m->features);

  if (m->tag == CEPH_MSGR_TAG_FEATURES) {
    dout(4)  << "connect protocol feature mismatch, my " << std::hex
	     << features << " < peer " << m->features
	     << " missing " << (m->features & ~policy.features_supported)
	     << std::dec << dendl;
  }

  if (m->tag == CEPH_MSGR_TAG_BADPROTOVER) {
    dout(4) << "connect protocol version mismatch, my " << protocol_version
	    << " != " << m->protocol_version << dendl;
    goto dispose_m;
  }

  if (m->tag == CEPH_MSGR_TAG_BADAUTHORIZER) {
    dout(4) << "connect got BADAUTHORIZER" << dendl;
    if (flags & FLAG_BAD_AUTH) {
      // prevent oscillation
      flags &= ~FLAG_BAD_AUTH;
      goto dispose_m;
    } else {
      flags |= FLAG_BAD_AUTH;
      return msg_connect_reply(m);
    }
  }

  if (m->tag == CEPH_MSGR_TAG_RESETSESSION) {
    dout(4) << "connect got RESETSESSION" << dendl;

    xcon->discard_input_queue(OP_FLAG_NONE);
    in_seq = 0;
    if (features & CEPH_FEATURE_MSG_AUTH) {
      (void) get_random_bytes((char *)&out_seq, sizeof(out_seq));
      out_seq.set(out_seq.read() & 0x7fffffff);
    } else {
      out_seq.set(0);
    }
    connect_seq = 0;
    // notify ULP
    msgr->ms_deliver_handle_reset(xcon);
    init_state();
    goto dispose_m;
  }

  // can we remove global_seq?
  if (m->tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
    global_seq = msgr->get_global_seq(m->global_seq);
    dout(4) << "connect got RETRY_GLOBAL " << m->global_seq
	    << " chose new " << global_seq << dendl;
    init_state();
    goto dispose_m;
  }

  if (!! authorizer) {
    if (m->authorizer_len) {
      bp = buffer::create(m->authorizer_len);
      bp.copy_in(0 /* off */, m->authorizer_len, m->get_data().c_str());
      auth_bl.push_back(bp);
      bufferlist::iterator iter = auth_bl.begin();
      if (!authorizer->verify_reply(iter)) {
	return state_fail(m, OP_FLAG_NONE);
      }
    } else {
      return state_fail(m, OP_FLAG_NONE);
    }
  }

  if (m->tag == CEPH_MSGR_TAG_READY) {
    uint64_t fdelta = policy.features_required & ~((uint64_t) m->features);
    if (fdelta) {
      dout(4) << "missing required features " << std::hex << fdelta
	      << std::dec << dendl;
      // XXX don't understand intended behavior
      return state_fail(m, OP_FLAG_NONE);
    }
  }

  // hooray!
  //peer_global_seq = m->global_seq;
  policy.lossy = m->flags & CEPH_MSG_CONNECT_LOSSY;
  //connect_seq = cseq + 1;
  //assert(connect_seq == reply.connect_seq);
  features = ((uint64_t) m->features & (uint64_t) features);

  if (!! authorizer) {
    session_security.reset(
      get_auth_session_handler(
	msgr->cct, authorizer->protocol, authorizer->session_key, features));
    delete authorizer; authorizer = NULL;
      }  else {
      // no authorizer, so we shouldn't be applying security to messages
      session_security.reset();
    }

  state_up_ready();

  // notify ULP
  msgr->ms_deliver_handle_connect(xcon);

dispose_m:
  m->put();

  return 0;
} /* msg_connect_reply */
