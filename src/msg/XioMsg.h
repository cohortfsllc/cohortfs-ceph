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

#ifndef XIO_MSG_H
#define XIO_MSG_H

#include <boost/intrusive/list.hpp>
#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "msg/msg_types.h"
#include "XioPool.h"

namespace bi = boost::intrusive;

class XioMsgCnt
{
public:
  __le32 msg_cnt;
  buffer::list bl;
public:
  XioMsgCnt(buffer::ptr p)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      ::decode(msg_cnt, bl_iter);
    }
};

class XioMsgHdr
{
public:
  __le32 msg_cnt;
  __le32 peer_type;
  entity_addr_t addr; /* XXX hack! */
  ceph_msg_header* hdr;
  ceph_msg_footer* ftr;
  buffer::list bl;
public:
  XioMsgHdr(ceph_msg_header& _hdr, ceph_msg_footer& _ftr)
    : msg_cnt(0), hdr(&_hdr), ftr(&_ftr)
    { }

  XioMsgHdr(ceph_msg_header& _hdr, ceph_msg_footer &_ftr, buffer::ptr p)
    : hdr(&_hdr), ftr(&_ftr)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      decode(bl_iter);
    }

  const buffer::list& get_bl() { encode(bl); return bl; };

  inline void encode_hdr(buffer::list& bl) const {
    ::encode(msg_cnt, bl);
    ::encode(peer_type, bl);
    ::encode(addr, bl);
    ::encode(hdr->seq, bl);
    ::encode(hdr->tid, bl);
    ::encode(hdr->type, bl);
    ::encode(hdr->priority, bl);
    ::encode(hdr->version, bl);
    ::encode(hdr->front_len, bl);
    ::encode(hdr->middle_len, bl);
    ::encode(hdr->data_len, bl);
    ::encode(hdr->data_off, bl);
    ::encode(hdr->src.type, bl);
    ::encode(hdr->src.num, bl);
    ::encode(hdr->compat_version, bl);
    ::encode(hdr->crc, bl);
  }

  inline void encode_ftr(buffer::list& bl) const {
    ::encode(ftr->front_crc, bl);
    ::encode(ftr->middle_crc, bl);
    ::encode(ftr->data_crc, bl);
    ::encode(ftr->sig, bl);
    ::encode(ftr->flags, bl);
  }

  inline void encode(buffer::list& bl) const {
    encode_hdr(bl);
    encode_ftr(bl);
  }

  inline void decode_hdr(buffer::list::iterator& bl) {
    ::decode(msg_cnt, bl);
    ::decode(peer_type, bl);
    ::decode(addr, bl);
    ::decode(hdr->seq, bl);
    ::decode(hdr->tid, bl);
    ::decode(hdr->type, bl);
    ::decode(hdr->priority, bl);
    ::decode(hdr->version, bl);
    ::decode(hdr->front_len, bl);
    ::decode(hdr->middle_len, bl);
    ::decode(hdr->data_len, bl);
    ::decode(hdr->data_off, bl);
    ::decode(hdr->src.type, bl);
    ::decode(hdr->src.num, bl);
    ::decode(hdr->compat_version, bl);
    ::decode(hdr->crc, bl);
  }

  inline void decode_ftr(buffer::list::iterator& bl) {
    ::decode(ftr->front_crc, bl);
    ::decode(ftr->middle_crc, bl);
    ::decode(ftr->data_crc, bl);
    ::decode(ftr->sig, bl);
    ::decode(ftr->flags, bl);
  }

  inline void decode(buffer::list::iterator& bl) {
    decode_hdr(bl);
    decode_ftr(bl);
  }

  virtual ~XioMsgHdr()
    {}
};

WRITE_CLASS_ENCODER(XioMsgHdr);

struct XioSubmit
{
public:
  enum submit_type
  {
    OUTGOING_MSG,
    INCOMING_MSG_RELEASE
  };
  enum submit_type type;
  bi::list_member_hook<> submit_list;
  XioConnection *xcon;

  XioSubmit(enum submit_type _type, XioConnection *_xcon) :
    type(_type), xcon(_xcon)
    {}

  typedef bi::list< XioSubmit,
		    bi::member_hook< XioSubmit,
				     bi::list_member_hook<>,
				     &XioSubmit::submit_list >
		    > Queue;
};

extern struct xio_mempool *xio_msgr_noreg_mpool;

#define XIO_MSGR_IOVLEN 16

struct xio_msg_ex
{
  struct xio_msg msg;
  struct xio_iovec_ex iovs[XIO_MSGR_IOVLEN];

  xio_msg_ex(void* user_context) {
    memset(&msg, 0, sizeof(struct xio_msg)); // XXXX going away

    // minimal initialize an "out" msg
    msg.type = XIO_MSG_TYPE_ONE_WAY;
    msg.request = NULL;
    msg.more_in_batch = 0;
    // for now, we DO NEED receipts for every msg
    msg.flags = XIO_MSG_FLAG_REQUEST_READ_RECEIPT | XIO_MSG_FLAG_IMM_SEND_COMP;
    msg.user_context = user_context;
    // minimal zero "in" side
    msg.in.header.iov_len = 0;
    msg.in.header.iov_base = NULL;  /* XXX Accelio requires this currently */

    msg.in.sgl_type = XIO_SGL_TYPE_IOV_PTR;
    msg.in.pdata_iov.max_nents = XIO_MSGR_IOVLEN;
    msg.in.pdata_iov.nents = 0;
    msg.in.pdata_iov.sglist = iovs;

    // minimal zero "out" side
    msg.out.header.iov_len = 0;
    msg.out.header.iov_base = NULL;  /* XXX Accelio requires this currently,
				      * against spec */
    // out (some members adjusted later)
    msg.out.sgl_type = XIO_SGL_TYPE_IOV_PTR;
    msg.out.pdata_iov.max_nents = XIO_MSGR_IOVLEN;
    msg.out.pdata_iov.nents = 0;
    msg.out.pdata_iov.sglist = iovs;
  }
};

struct XioMsg : public XioSubmit
{
public:
  Message* m;
  XioMsgHdr hdr;
  xio_msg_ex req_0;
  xio_msg_ex* req_arr;
  struct xio_mempool_obj mp_this;
  atomic_t nrefs;

public:
  XioMsg(Message *_m, XioConnection *_xcon, struct xio_mempool_obj& _mp,
	 int _ex_cnt) :
    XioSubmit(XioSubmit::OUTGOING_MSG, _xcon),
    m(_m), hdr(m->get_header(), m->get_footer()),
    req_0(this), req_arr(NULL), mp_this(_mp), nrefs(_ex_cnt+1)
    {
      const entity_inst_t &inst = xcon->get_messenger()->get_myinst();
      hdr.peer_type = inst.name.type();
      hdr.addr = xcon->get_messenger()->get_myaddr();
      hdr.hdr->src.type = inst.name.type();
      hdr.hdr->src.num = inst.name.num();
      hdr.msg_cnt = _ex_cnt+1;

      if (unlikely(_ex_cnt > 0)) {
	alloc_trailers(_ex_cnt);
      }

      // submit queue ref
      xcon->get();
    }

  XioMsg* get() { nrefs.inc(); return this; };

  void put(int n) {
    int refs = nrefs.sub(n);
    if (refs == 0) {
      struct xio_mempool_obj *mp = &this->mp_this;
      this->~XioMsg();
      xio_mempool_free(mp);
    }
  }

  void put() {
    put(1);
  }

  void put_msg_refs() {
    put(hdr.msg_cnt);
  }

  void alloc_trailers(int cnt) {
    req_arr = (xio_msg_ex*) malloc(cnt * sizeof(xio_msg_ex));
    for (int ix = 0; ix < cnt; ++ix) {
      xio_msg_ex* xreq = &(req_arr[ix]);
      new (xreq) xio_msg_ex(this);
    }
  }

  Message *get_message() { return m; }

  ~XioMsg()
    {
      if (unlikely(!!req_arr)) {
	for (unsigned int ix = 0; ix < hdr.msg_cnt-1; ++ix) {
	  xio_msg_ex* xreq = &(req_arr[ix]);
	  xreq->~xio_msg_ex();
	}
	free(req_arr);
      }

      /* testing only! server's ready, resubmit request (not reached on
       * PASSIVE/server side) */
      if (unlikely(m->get_special_handling() & MSG_SPECIAL_HANDLING_REDUPE)) {
	if (likely(xcon->is_connected())) {
	  xcon->get_messenger()->send_message(m, xcon);
	} else {
	  /* dispose it */
	  m->put();
	}
      } else {
	  /* the normal case: done with message */
	  m->put();
      }
      /* submit queue ref */
      xcon->put();
    }
};

class XioCompletionHook : public Message::CompletionHook
{
private:
  XioConnection *xcon;
  XioInSeq msg_seq;
  XioPool rsp_pool;
  atomic_t nrefs;
  bool cl_flag;
  friend class XioConnection;
  friend class XioMessenger;
public:
  struct xio_mempool_obj mp_this;

  XioCompletionHook(XioConnection *_xcon, Message *_m, XioInSeq& _msg_seq,
		    struct xio_mempool_obj& _mp) :
    CompletionHook(_m),
    xcon(_xcon->get()),
    msg_seq(_msg_seq),
    rsp_pool(xio_msgr_noreg_mpool),
    nrefs(1),
    cl_flag(false),
    mp_this(_mp)
    {}

  virtual void finish(int r) {
    this->put();
  }

  virtual void complete(int r) {
    finish(r);
  }

  int release_msgs();

  XioCompletionHook* get() {
    nrefs.inc(); return this;
  }
  virtual void set_message(Message *_m) {
    m = _m;
  }

  void put(int n) {
    int refs = nrefs.sub(n);
    if (refs == 0) {
      /* in Marcus' new system, refs reaches 0 twice:  once in
       * Message lifecycle, and again after xio_release_msg.
       */
      if (!cl_flag && release_msgs())
	return;
      struct xio_mempool_obj *mp = &this->mp_this;
      this->~XioCompletionHook();
      xio_mempool_free(mp);
    }
  }

  void put() {
    put(1);
  }

  XioInSeq& get_seq() { return msg_seq; }

  void claim(int r) {}

  XioPool& get_pool() { return rsp_pool; }

  void on_err_finalize(XioConnection *xcon) {
    /* can't decode message; even with one-way must free
     * xio_msg structures, and then xiopool
     */
    this->finish(-1);
  }

  ~XioCompletionHook() {
    xcon->put();
  }
};

struct XioRsp : public XioSubmit
{
  XioCompletionHook *xhook;
public:
  XioRsp(XioConnection *_xcon, XioCompletionHook *_xhook)
    : XioSubmit(XioSubmit::INCOMING_MSG_RELEASE, _xcon /* not xcon! */),
      xhook(_xhook->get()) {
      // submit queue ref
      xcon->get();
    };

  struct xio_msg* dequeue() {
    return xhook->get_seq().dequeue();
  }

  XioCompletionHook *get_xhook() { return xhook; }

  void finalize() {
    xcon->put();
    xhook->put();
  }
};

void print_xio_msg_hdr(const char *tag,  const XioMsgHdr &hdr,
		       const struct xio_msg *msg);
void print_ceph_msg(const char *tag, Message *m);

#endif /* XIO_MSG_H */
