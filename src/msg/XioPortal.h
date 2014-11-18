// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *s
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_PORTAL_H
#define XIO_PORTAL_H

extern "C" {
#include "libxio.h"
}
#include <boost/lexical_cast.hpp>
#include "SimplePolicyMessenger.h"
#include "XioConnection.h"
#include "XioMsg.h"

#include "include/assert.h"
#include "common/dout.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

class XioMessenger;

#define ndecoders 4

class XioDecoder : public Thread
{
private:

  struct SubmitQueue
  {
    const static int nlanes = 5;

    struct Lane
    {
      uint32_t size;
      XioRecvMsg::Queue q;
      pthread_spinlock_t sp;
      CACHE_PAD(0);
    };

    Lane qlane[nlanes];

    SubmitQueue() {
      int ix;
      Lane* lane;
      for (ix = 0; ix < nlanes; ++ix) {
	lane = &qlane[ix];
	pthread_spin_init(&lane->sp, PTHREAD_PROCESS_PRIVATE);
	lane->size = 0;
      }
    }

    inline Lane* get_lane(XioConnection *xcon) {
      return &qlane[((uint64_t) xcon) % nlanes];
    }

    void enq(XioRecvMsg* in_msg) {
      Lane* lane = get_lane(in_msg->xcon);
      pthread_spin_lock(&lane->sp);
      lane->q.push_back(*in_msg);
      ++(lane->size);
      pthread_spin_unlock(&lane->sp);
    }

    void deq(XioRecvMsg::Queue &decode_q) {
      int ix;
      Lane* lane;

      for (ix = 0; ix < nlanes; ++ix) {
	lane = &qlane[ix];
	if (lane->size > 0) {
	  pthread_spin_lock(&lane->sp);
	  if (lane->size > 0) {
	    XioRecvMsg::Queue::const_iterator i1 = decode_q.end();
	    decode_q.splice(i1, lane->q);
	    lane->size = 0;
	  }
	  pthread_spin_unlock(&lane->sp);
	}
      }
    }
  };

  friend class XioConnection;

  bool _shutdown;
  bool drained;
  bool idle;

  pthread_spinlock_t sp;
  pthread_mutex_t mtx;
  pthread_cond_t cv;

  // queues of decodable
  SubmitQueue decode_q;

  ceph_msg_header scratch_header;
  ceph_msg_footer scratch_footer;

public:
  XioDecoder() :
    _shutdown(false), drained(false), idle(false)
    {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
      pthread_mutex_init(&mtx, NULL);
      pthread_cond_init(&cv, NULL);
    }

    void *entry()
    {
      int size;
      XioRecvMsg::Queue q;
      XioRecvMsg::Queue::iterator q_iter;
      XioRecvMsg* in_msg;

      struct timespec ts;

      do {
	decode_q.deq(q);
	size = q.size();

	if (! size) {
	  (void) clock_gettime(CLOCK_REALTIME_COARSE, &ts);
	  ts.tv_sec += 1;
	  pthread_mutex_lock(&mtx);
	  pthread_cond_timedwait(&cv, &mtx, &ts);
	  pthread_mutex_unlock(&mtx);
	  // at worst, we shut down in a few 100 ms
	  if (_shutdown)
	    break;
	  continue;
	}

	if (size > 0) {
	  q_iter = q.begin();
	  while (q_iter != q.end()) {
	    in_msg = &(*q_iter);
	    q_iter = q.erase(q_iter);
	    in_msg->xcon->decode_dispatch(this, in_msg); // disposes in_msg
	  }
	}
      } while (!_shutdown);

      /* shutting down */
      return NULL;
    }

  void enqueue_for_decode(XioRecvMsg* in_msg) {

    if (! _shutdown) {
      decode_q.enq(in_msg);
    }

    /* prod the decoder thread if it appears to be idle;  we will tolerate
     * just missing a decoder going idle */
    if (idle) {
      pthread_cond_signal(&cv);
    }
  }

  void shutdown() {
    pthread_spin_lock(&sp);
    _shutdown = true;
    pthread_spin_unlock(&sp);

    pthread_mutex_lock(&mtx);
    pthread_cond_signal(&cv);
    pthread_mutex_unlock(&mtx);
  }
}; /* XioDecoder */

class XioPortal : public Thread
{
private:

  struct SubmitQueue
  {
    const static int nlanes = 7;

    struct Lane
    {
      uint32_t size;
      XioMsg::Queue q;
      pthread_spinlock_t sp;
      CACHE_PAD(0);
    };

    Lane qlane[nlanes];

    SubmitQueue()
      {
	int ix;
	Lane* lane;

	for (ix = 0; ix < nlanes; ++ix) {
	  lane = &qlane[ix];
	  pthread_spin_init(&lane->sp, PTHREAD_PROCESS_PRIVATE);
	  lane->size = 0;
	}
      }

    inline Lane* get_lane(XioConnection *xcon)
      {
	return &qlane[((uint64_t) xcon) % nlanes];
      }

    void enq(XioConnection *xcon, XioSubmit* xs)
      {
	Lane* lane = get_lane(xcon);
	pthread_spin_lock(&lane->sp);
	lane->q.push_back(*xs);
	++(lane->size);
	pthread_spin_unlock(&lane->sp);
      }

    void deq(XioSubmit::Queue &send_q)
      {
	int ix;
	Lane* lane;

	for (ix = 0; ix < nlanes; ++ix) {
	  lane = &qlane[ix];
	  if (lane->size > 0) {
	    pthread_spin_lock(&lane->sp);
	    if (lane->size > 0) {
	      XioSubmit::Queue::const_iterator i1 = send_q.end();
	      send_q.splice(i1, lane->q);
	    lane->size = 0;
	    }
	    pthread_spin_unlock(&lane->sp);
	  }
	}
      }

  };

  XioMessenger* msgr;
  struct xio_context *ctx;
  struct xio_server *server;
  SubmitQueue submit_q;
  XioDecoder decoder[ndecoders];
  pthread_spinlock_t sp;
  pthread_mutex_t mtx;
  void *ev_loop;
  string xio_uri;
  char *portal_id;
  bool _shutdown;
  bool drained;
  uint32_t magic;
  uint32_t special_handling;

  friend class XioPortals;
  friend class XioMessenger;
  friend class XioConnection;

public:
  XioPortal(XioMessenger *_msgr) :
    msgr(_msgr), ctx(NULL), server(NULL), submit_q(), xio_uri(""),
    portal_id(NULL), _shutdown(false), drained(false),
    magic(0),
    special_handling(0)
    {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
      pthread_mutex_init(&mtx, NULL);

      /* a portal is an xio_context and event loop */
      ctx = xio_context_create(NULL, 0 /* poll timeout */, -1 /* cpu hint */);

      /* associate this XioPortal object with the xio_context handle */
      struct xio_context_attr xca;
      xca.user_context = this;
      xio_modify_context(ctx, &xca, XIO_CONTEXT_ATTR_USER_CTX);

      if (magic & (MSG_MAGIC_XIO)) {
	printf("XioPortal %p created ev_loop %p ctx %p\n",
	       this, ev_loop, ctx);
      }
    }

  int bind(struct xio_session_ops *ops, const string &base_uri,
	   uint16_t port, uint16_t *assigned_port);

  void enqueue_for_send(XioConnection *xcon, XioSubmit *xs)
    {
      if (! _shutdown) {
	submit_q.enq(xcon, xs);
	xio_context_stop_loop(ctx, false);
	return;
      }

      /* dispose xs */
      switch(xs->type) {
      case XioSubmit::OUTGOING_MSG: /* it was an outgoing 1-way */
      {
	XioMsg* xmsg = static_cast<XioMsg*>(xs);
	xs->xcon->msg_send_fail(xmsg, -EINVAL);
      }
	break;
      default:
	abort();
      break;
      };
    }

  void enqueue_for_decode(XioRecvMsg* in_msg) {
    static int d_ix;
    int ix = ++d_ix;
    decoder[(ix % ndecoders)].enqueue_for_decode(in_msg);
  }

  void *entry()
    {
      int size, code = 0;
      uint32_t xio_qdepth;
      XioSubmit::Queue send_q;
      XioSubmit::Queue::iterator q_iter;
      struct xio_msg *msg = NULL;
      XioConnection *xcon;
      XioSubmit *xs;
      XioMsg *xmsg;

      /* decoder thread */
      for (int ix = 0; ix  < ndecoders; ++ix) {
	decoder[ix].create();
      }

      do {
	submit_q.deq(send_q);
	size = send_q.size();

	/* shutdown() barrier */
	pthread_spin_lock(&sp);

	if (_shutdown) {
	  drained = true;
	}

	if (size > 0) {
	  q_iter = send_q.begin();
	  while (q_iter != send_q.end()) {
	    xs = &(*q_iter);
	    xcon = xs->xcon;
	    xmsg = static_cast<XioMsg*>(xs);

	    /* guard Accelio send queue */
	    xio_qdepth = xcon->xio_queue_depth();
	    if (unlikely((xcon->send_ctr + xmsg->hdr.msg_cnt) > xio_qdepth)) {
	      ++q_iter;
	      continue;
	    }

	    q_iter = send_q.erase(q_iter);

	    /* XXX we know we are not racing with a disconnect
	     * thread */
	    if (unlikely(!xcon->conn))
	      code = ENOTCONN;
	    else {
	      msg = &xmsg->req_0.msg;
	      code = xio_send_msg(xcon->conn, msg);
	      /* header trace moved here to capture xio serial# */
	      if (dlog_p(ceph_subsys_xio, 11)) {
		print_xio_msg_hdr("xio_send_msg", xmsg->hdr, msg);
		print_ceph_msg("xio_send_msg", xmsg->m);
	      }
	    }
	    if (unlikely(code)) {
	      xcon->msg_send_fail(xmsg, code);
	    } else {
	      xcon->send.set(msg->timestamp); // need atomic?
	      xcon->send_ctr += xmsg->hdr.msg_cnt; // only inc if cb promised
	    }
	  }
	}

	pthread_spin_unlock(&sp);
	xio_context_run_loop(ctx, 300);

      } while ((!_shutdown) || (!drained));

      /* shutting down */
      if (server) {
	xio_unbind(server);
      }
      xio_context_destroy(ctx);
      return NULL;
    }

  void shutdown()
    {
	pthread_spin_lock(&sp);
	xio_context_stop_loop(ctx, false);
	_shutdown = true;
	for (int ix = 0; ix < ndecoders; ++ix) {
	  decoder[ix].shutdown();
	}
	pthread_spin_unlock(&sp);
    }
};

class XioPortals
{
private:
  vector<XioPortal*> portals;
  char **p_vec;
  int n;

public:
  XioPortals(XioMessenger *msgr, int _n) : p_vec(NULL), n(_n)
    {
      /* portal0 */
      portals.push_back(new XioPortal(msgr));

      /* n bound session portals */
      for (int ix = 1; ix < n; ++ix) {
	portals.push_back(new XioPortal(msgr));
      }
    }

  vector<XioPortal*>& get() { return portals; }

  const char **get_vec()
    {
      return (const char **) p_vec;
    }

  int get_portals_len()
    {
      return n;
    }

  XioPortal* get_portal0()
    {
      return portals[0];
    }

  int bind(struct xio_session_ops *ops, const string& base_uri,
	   uint16_t port, uint16_t *port0);

    int accept(struct xio_session *session,
		 struct xio_new_session_req *req,
		 void *cb_user_context)
    {
      const char **portals_vec = get_vec();
      int portals_len = get_portals_len()-1;

      return xio_accept(session,
			portals_vec,
			portals_len,
			NULL, 0);
    }

  void start()
    {
      XioPortal *portal;
      int p_ix, nportals = portals.size();

      /* portal_0 is the new-session handler, portal_1+ terminate
       * active sessions */

      p_vec = new char*[(nportals-1)];
      for (p_ix = 1; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	/* shift left */
	p_vec[(p_ix-1)] = (char*) /* portal->xio_uri.c_str() */
	  portal->portal_id;
      }

      for (p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->create();
      }
    }

  void shutdown()
    {
      XioPortal *portal;
      int nportals = portals.size();
      for (int p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->shutdown();
      }
    }

  void join()
    {
      XioPortal *portal;
      int nportals = portals.size();
      for (int p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->join();
      }
    }

  ~XioPortals()
    {
      int nportals = portals.size();
      for (int ix = 0; ix < nportals; ++ix) {
	delete(portals[ix]);
      }
      portals.clear();
      if (p_vec) {
	delete[] p_vec;
      }
    }
};

#endif /* XIO_PORTAL_H */
