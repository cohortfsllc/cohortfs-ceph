// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_MDATAPING_H
#define CEPH_MDATAPING_H


#include "msg/Message.h"

#ifdef HAVE_XIO
#include "messages/MPing.h"
extern "C" {
#include "libxio.h"
}

typedef void (*mdata_hook_func)(struct xio_mempool_obj *mp);
#endif

class MDataPing : public Message {

 public:

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  std::string tag;
  uint32_t counter;
#ifdef HAVE_XIO
  mdata_hook_func mdata_hook;
  struct xio_mempool_obj mp;

  struct xio_mempool_obj *get_mp() { return &mp; }
  void set_rdma_hook(mdata_hook_func hook) { mdata_hook = hook; }
#endif
  bool free_data;

  MDataPing()
    : Message(MSG_DATA_PING, HEAD_VERSION, COMPAT_VERSION),
#ifdef HAVE_XIO
      mdata_hook(NULL),
#endif
      free_data(false)
  {}

private:
  ~MDataPing()
    {
#ifdef HAVE_XIO
      if (mdata_hook)
	mdata_hook(&mp);
#endif
      if (free_data)  {
	const std::list<buffer::ptr>& buffers = data.buffers();
	list<bufferptr>::const_iterator pb;
	for (pb = buffers.begin(); pb != buffers.end(); ++pb) {
	  free((void*) pb->c_str());
	}
      }
    }

public:
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(tag, p);
    ::decode(counter, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(tag, payload);
    ::encode(counter, payload);
  }

  const char *get_type_name() const { return "data_ping"; }

  void print(ostream& out) const {
    out << get_type_name() << " " << tag << " " << counter;
  }
};

#endif /* CEPH_MDATAPING_H */
