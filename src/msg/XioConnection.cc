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

XioConnection::XioConnection(XioMessenger* m,
			     XioConnection::type _type,
			     const entity_inst_t& _peer) :
  Connection(m),
  xio_conn_type(_type),
  portal(m->default_portal()),
  connected(false),
  peer(_peer),
  session(NULL),
  conn(NULL),
  magic(m->get_magic()),
  send_ctr(0),
  in_seq(this)
{
  pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
  if (xio_conn_type == XioConnection::ACTIVE)
    peer_addr = peer.addr;
  peer_type = peer.name.type();
  set_peer_addr(peer.addr);

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

static uint32_t xio_recv_cnt;

static uint32_t pcount;

static inline XioRecvMsg* pool_alloc_xio_recv_msg(
  XioConnection *xcon, struct xio_msg *msg)
{
  struct xio_mempool_obj mp_mem;
retry:
#if 1
  int e = xpool_alloc(xio_msgr_noreg_mpool, sizeof(XioRecvMsg), &mp_mem);
  if (!!e) {
    goto retry;
  }
  XioRecvMsg *in_msg = static_cast<XioRecvMsg*>(mp_mem.addr);
  new (in_msg) XioRecvMsg(xcon, msg, mp_mem);
#else
  XioRecvMsg* in_msg = new XioRecvMsg(xcon, msg, mp_mem);
  if (! in_msg) {
    abort();
  }
#endif
  ++xio_recv_cnt;
  return in_msg;
}

int XioConnection::decode_dispatch(XioDecoder* d, XioRecvMsg* in_msg)
{
  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());

  XioMsgHdr hdr(d->scratch_header, d->scratch_footer, in_msg->header_buf());

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    print_xio_msg_hdr("decode_dispatch", hdr, NULL);
    dout(11) << "XioConnection.cc:on_msg_req msg detail"
	     << " payload: " << in_msg->payload.length()
	     << " (" << in_msg->payload.buffers().size() << ")"
	     << " middle: " << in_msg->middle.length()
	     << " (" << in_msg->middle.buffers().size() << ")"
	     << " data: " << in_msg->data.length()
	     << " (" << in_msg->data.buffers().size() << ")"
	     << dendl;
    dout(11) << "XioConnection.cc:on_msg_req payload dump: ";
    in_msg->payload.hexdump( *_dout );
    *_dout << dendl;
  }


  Message *m =
    decode_message(msgr->cct,
		   msgr->crcflags,
		   d->scratch_header, d->scratch_footer,
		   in_msg->payload, in_msg->middle, in_msg->data);

  if (m) {
    /* completion */
    m->set_connection(this);

    /* trace flag */
    m->set_magic(magic);

    /* update timestamps */
    m->set_recv_stamp(in_msg->get_t1());
    m->set_recv_complete_stamp(in_msg->get_t1());
    m->set_seq(d->scratch_header.seq);

    /* MP-SAFE */
    state.set_in_seq(d->scratch_header.seq);

    /* XXXX validate peer type */
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

    if (magic & (MSG_MAGIC_TRACE_XCON)) {
      dout(4) << "decode m is " << m->get_type() << dendl;
    }

    /* dispatch it */
    msgr->ds_dispatch(m);
  } else {
    /* responds for undecoded messages and frees hook */
    dout(4) << "decode m failed" << dendl;
  }

  /* release in_msg */
#if 1
  struct xio_mempool_obj *mp = &in_msg->mp_this;
  in_msg->~XioRecvMsg();
  xpool_free(sizeof(XioRecvMsg), mp);
#else
  delete(in_msg);
//  --xio_recv_cnt;
//  if ((++pcount % 30000) == 0) std::cout << "xio_recv_cnt: "
//					 << xio_recv_cnt << std::endl;
#endif
  return 0;
}

#define uint_to_timeval(tv, s) ((tv).tv_sec = (s), (tv).tv_usec = 0)

int XioConnection::on_msg_req(struct xio_session *session,
			      struct xio_msg *req,
			      int more_in_batch,
			      void *cb_user_context)
{
  struct xio_msg *treq = req;

  /* XXX Accelio guarantees message ordering at
   * xio_session */
  if (! in_seq.p) {
    XioMsgCnt msg_cnt(
      buffer::create_static(treq->in.header.iov_len,
			    (char*) treq->in.header.iov_base));
    in_seq.cnt = msg_cnt.msg_cnt;
    in_seq.p = true;
    if (unlikely(magic & (MSG_MAGIC_TRACE_XCON))) {
      dout(11) << __func__ << " !in_seq.p" <<
	" req " << req <<
	" in.header.iov_base " << req->in.header.iov_base <<
	" in.header.iov_len " << (int) req->in.header.iov_len <<
	" in_seq.cnt " << in_seq.cnt << dendl;
    }
  }
  in_seq.append(req);
  if (in_seq.cnt > 0) {
    if (unlikely(magic & (MSG_MAGIC_TRACE_XCON))) {
      dout(11) << __func__ << " in_seq.cnt > 0 (" << in_seq.cnt << ")"
	       << dendl;
    }
    return 0;
  }
  else
    in_seq.p = false;

  if (unlikely(magic & (MSG_MAGIC_TRACE_XCON))) {
    dout(11) << __func__ << " start decode" << dendl;
  }

  /* unpack sequence */
  list<struct xio_msg *>& msg_seq = in_seq.seq;
  list<struct xio_msg *>::iterator msg_iter = msg_seq.begin();

  dout(11) << __func__ << " " << "msg_seq.size()="  << msg_seq.size()
	   << dendl;

  treq = *msg_iter;

  XioRecvMsg* in_msg = pool_alloc_xio_recv_msg(this, treq);

  /* update msg and connection timestamps */
  uint_to_timeval(in_msg->get_t1(), treq->timestamp);
  recv.set(treq->timestamp);

  struct xio_iovec_ex *msg_iov, *iovs;
  uint32_t ix = 0, blen, iov_len, msg_off = 0;
  uint32_t take_len, left_len = 0;
  buffer::ptr bp;

  blen = in_msg->front_len;

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = vmsg_sglist_nents(&treq->in);
    iovs = vmsg_sglist(&treq->in);
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
      bp = buffer::ptr(buffer::ptr_to_raw(msg_iov->user_context));
      /* XXX need to detect any buffer which needs to be
       * split due to coalescing of a segment (front, middle,
       * data) boundary */
      take_len = MIN(blen, msg_iov->iov_len);
      if (take_len == msg_iov->iov_len)
	in_msg->payload.append(bp);
      else {
	in_msg->payload.append(buffer::ptr(bp, msg_off, take_len));
      }
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len)
	  msg_off += take_len;
      }
    }
    /* XXX as above, if a buffer is split, then we needed to track
     * the new start (carry) and not advance */
    if (ix == iov_len) {
      ++msg_iter;
      msg_off = 0;
      ix = 0;
    }
  }

  blen = in_msg->middle_len;

  if (blen && left_len) {
    in_msg->middle.append(bp, msg_off, left_len);
    blen -= left_len;
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = vmsg_sglist_nents(&treq->in);
    iovs = vmsg_sglist(&treq->in);
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
      bp = buffer::ptr(buffer::ptr_to_raw(msg_iov->user_context));
      /* XXX need to detect any buffer which needs to be
       * split due to coalescing of a segment (front, middle,
       * data) boundary */
      take_len = MIN(blen, msg_iov->iov_len);
      if (take_len == msg_iov->iov_len)
	in_msg->middle.append(bp);
      else {
	in_msg->middle.append(buffer::ptr(bp, msg_off, take_len));
      }
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len)
	  msg_off += take_len;
      }
    }
    /* XXX as above, if a buffer is split, then we needed to track
     * the new start (carry) and not advance */
    if (ix == iov_len) {
      ++msg_iter;
      msg_off = 0;
      ix = 0;
    }
  }

  blen = in_msg->data_len;

  if (blen && left_len) {
    in_msg->data.append(bp, msg_off, left_len);
    blen -= left_len;
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = vmsg_sglist_nents(&treq->in);
    iovs = vmsg_sglist(&treq->in);
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
      bp = buffer::ptr(buffer::ptr_to_raw(msg_iov->user_context));
      /* XXX need to detect any buffer which needs to be
       * split due to coalescing of a segment (front, middle,
       * data) boundary */
      take_len = MIN(blen, msg_iov->iov_len);
      if (take_len == msg_iov->iov_len)
	in_msg->data.append(bp);
      else {
	in_msg->data.append(buffer::ptr(bp, msg_off, take_len));
      }
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len)
	  msg_off += take_len;
      }
    }
    /* XXX as above, if a buffer is split, then we needed to track
     * the new start (carry) and not advance */
    if (ix == iov_len) {
      ++msg_iter;
      msg_off = 0;
      ix = 0;
    }
  }

  /* enqueue for out-of-line decode */
  portal->enqueue_for_decode(in_msg);

  /* release Accelio msgs */
  in_seq.release();

  return 0;
}

static uint64_t rcount;

int XioConnection::on_ow_msg_send_complete(struct xio_session *session,
					   struct xio_msg *req,
					   void *conn_user_context)
{
  /* requester send complete (one-way) */
  uint64_t rc = ++rcount;

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

  --send_ctr; /* atomic, because portal thread */
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

int XioConnection::on_msg_error(struct xio_session *session,
				enum xio_status error,
				struct xio_msg  *msg,
				void *conn_user_context)
{
  XioMsg *xmsg = static_cast<XioMsg*>(msg->user_context);
  if (xmsg)
    xmsg->put();
  --send_ctr; /* atomic, because portal thread */
  return 0;
} /* on_msg_error */
