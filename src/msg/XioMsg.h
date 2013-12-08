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

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"

class xio_msg_cnt
{
public:
  __le32 msg_cnt;
  bufferlist bl;
public:
  xio_msg_cnt(buffer::ptr p)
    {
      bl.append(p);
      bufferlist::iterator bl_iter = bl.begin();
      ::decode(msg_cnt, bl_iter);
    }
};

class xio_msg_hdr
{
public:
  __le32 msg_cnt;
  ceph_msg_header &hdr;
  bufferlist bl;
public:
  xio_msg_hdr(ceph_msg_header& _hdr) :msg_cnt(0), hdr(_hdr)
    { }

  xio_msg_hdr(ceph_msg_header& _hdr, buffer::ptr p) : hdr(_hdr)
    {
      bl.append(p);
      bufferlist::iterator bl_iter = bl.begin();
      decode(bl_iter);
    }

  const bufferlist& get_bl() { encode(bl); return bl; };

  inline void update_lengths(bufferlist &payload, bufferlist &middle, 
			     bufferlist &data) {
    hdr.front_len = payload.buffers().size();
    hdr.middle_len = middle.buffers().size();
    hdr.data_len = data.buffers().size();
  }

  void encode(bufferlist& bl) const {
    ::encode(msg_cnt, bl);
    ::encode(hdr.seq, bl);
    ::encode(hdr.tid, bl);
    ::encode(hdr.type, bl);
    ::encode(hdr.priority, bl);
    ::encode(hdr.version, bl);
    ::encode(hdr.front_len, bl);
    ::encode(hdr.middle_len, bl);
    ::encode(hdr.data_len, bl);
    ::encode(hdr.data_off, bl);
  }

  void decode(bufferlist::iterator& bl) {
    ::decode(msg_cnt, bl);
    ::decode(hdr.seq, bl);
    ::decode(hdr.tid, bl);
    ::decode(hdr.type, bl);
    ::decode(hdr.priority, bl);
    ::decode(hdr.version, bl);
    ::decode(hdr.front_len, bl);
    ::decode(hdr.middle_len, bl);
    ::decode(hdr.data_len, bl);
    ::decode(hdr.data_off, bl);
  }

};

WRITE_CLASS_ENCODER(xio_msg_hdr);

class xio_msg_ftr
{
public:
  ceph_msg_footer ftr;
  bufferlist bl;
public:
  xio_msg_ftr(ceph_msg_footer &_ftr) : ftr(_ftr)
    { }

  xio_msg_ftr(ceph_msg_footer& _ftr, buffer::ptr p)
    {
      bl.append(p);
      bufferlist::iterator bl_iter = bl.begin();
      decode(bl_iter);
      _ftr = ftr;
    }

  const bufferlist& get_bl() { encode(bl); return bl; };

  void encode(bufferlist& bl) const {
    ::encode(ftr.front_crc, bl);
    ::encode(ftr.middle_crc, bl);
    ::encode(ftr.data_crc, bl);
    ::encode(ftr.sig, bl);
    ::encode(ftr.flags, bl);
  }

  void decode(bufferlist::iterator& bl) {
    ::decode(ftr.front_crc, bl);
    ::decode(ftr.middle_crc, bl);
    ::decode(ftr.data_crc, bl);
    ::decode(ftr.sig, bl);
    ::decode(ftr.flags, bl);
  }

};

WRITE_CLASS_ENCODER(xio_msg_ftr);

struct XioMsg
{
public:
  Message* m;
  xio_msg_hdr hdr;
  xio_msg_ftr ftr;
  struct xio_msg req_0;
  struct xio_msg *req_arr;
  int nbuffers;

public:
  XioMsg(Message *_m) : m(_m),
			hdr(m->get_header()),
			ftr(m->get_footer()),
			req_arr(NULL)
    {
      memset(&req_0, 0, sizeof(struct xio_msg));
      req_0.user_context = this;
    }

  ~XioMsg()
    {
      free(req_arr);
      m->put();
    }
};

#endif /* XIO_MSG_H */
