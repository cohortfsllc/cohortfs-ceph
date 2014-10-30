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

#include <atomic>
#include <boost/intrusive/list.hpp>
#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "msg/msg_types.h"
#include "XioPool.h"

namespace bi = boost::intrusive;

class XioMsgHdr
{
public:
  __le32 msg_cnt;
  __le32 peer_type;
  entity_addr_t addr; /* XXX hack! */
  ceph_msg_header* hdr;
  ceph_msg_footer* ftr;
  bufferlist bl;
public:
  XioMsgHdr(ceph_msg_header& _hdr, ceph_msg_footer& _ftr)
    : msg_cnt(0), hdr(&_hdr), ftr(&_ftr)
    { }

  XioMsgHdr(ceph_msg_header& _hdr, ceph_msg_footer &_ftr, bufferptr p)
    : hdr(&_hdr), ftr(&_ftr)
    {
      bl.append(p);
      bufferlist::iterator bl_iter = bl.begin();
      decode(bl_iter);
    }

  const bufferlist& get_bl() { encode(bl); return bl; };

  inline void encode_hdr(bufferlist& bl) const {
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

  inline void encode_ftr(bufferlist& bl) const {
    ::encode(ftr->front_crc, bl);
    ::encode(ftr->middle_crc, bl);
    ::encode(ftr->data_crc, bl);
    ::encode(ftr->sig, bl);
    ::encode(ftr->flags, bl);
  }

  inline void encode(bufferlist& bl) const {
    encode_hdr(bl);
    encode_ftr(bl);
  }

  inline void decode_hdr(bufferlist::iterator& bl) {
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

  inline void decode_ftr(bufferlist::iterator& bl) {
    ::decode(ftr->front_crc, bl);
    ::decode(ftr->middle_crc, bl);
    ::decode(ftr->data_crc, bl);
    ::decode(ftr->sig, bl);
    ::decode(ftr->flags, bl);
  }

  inline void decode(bufferlist::iterator& bl) {
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

extern struct xio_mempool *xio_msgr_reg_mpool;
extern struct xio_mempool *xio_msgr_noreg_mpool;

#define XIO_MSGR_IOVLEN 16

struct xio_msg_ex
{
  struct xio_msg msg;
  struct xio_iovec_ex iovs[XIO_MSGR_IOVLEN];

  xio_msg_ex(void* user_context) {
    // go in structure order
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

    // minimal initialize an "out" msg
    msg.request = NULL;
    msg.type = XIO_MSG_TYPE_ONE_WAY;
    // for now, we DO NEED receipts for every msg
    msg.flags = 0;
    msg.user_context = user_context;
    msg.next = NULL;
    // minimal zero "in" side
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
  std::atomic<uint64_t> nrefs;

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

  XioMsg* get() { ++nrefs; return this; };

  void put(int n) {
    int refs = (nrefs -= n);
    if (refs == 0) {
      struct xio_mempool_obj *mp = &this->mp_this;
      this->~XioMsg();
      xpool_free(sizeof(XioMsg), mp);
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

void print_xio_msg_hdr(const char *tag,  const XioMsgHdr &hdr,
		       const struct xio_msg *msg);
void print_ceph_msg(const char *tag, Message *m);

#endif /* XIO_MSG_H */
