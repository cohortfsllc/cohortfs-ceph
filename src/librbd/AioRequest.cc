// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"

#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "librbd/AioRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioRequest: "

namespace librbd {

  AioRequest::AioRequest() :
    m_ictx(NULL), m_ioctx(NULL),
    m_off(0), m_len(0),
    m_completion(NULL),
    m_hide_enoent(false) {}
  AioRequest::AioRequest(ImageCtx *ictx, const std::string &oid,
			 uint64_t off, uint64_t len,
			 Context *completion,
			 bool hide_enoent) :
    m_ictx(ictx), m_ioctx(&ictx->io_ctx), m_oid(oid),
    m_off(off), m_len(len), m_completion(completion),
    m_hide_enoent(hide_enoent) {}

  AioRequest::~AioRequest() { }

  /** read **/

  int AioRead::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " "
			   << m_off << "~" << m_len << dendl;

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, rados_req_cb, NULL);
    int r;
    librados::ObjectReadOperation op(*m_ioctx);
    r = m_ioctx->aio_read(m_oid, rados_completion, &m_read_data, m_len, m_off);

    rados_completion->release();
    return r;
  }

  /** write **/

  AbstractWrite::AbstractWrite(ImageCtx *ictx, const std::string &oid,
			       uint64_t object_off, uint64_t len,
			       Context *completion,
			       bool hide_enoent)
    : AioRequest(ictx, oid, object_off, len, completion, hide_enoent),
      m_write(*m_ioctx) { }

  int AbstractWrite::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " "
			   << m_off << "~" << m_len << dendl;
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    int r;
    assert(m_write.size());
    r = m_ioctx->aio_operate(m_oid, rados_completion, &m_write);
    rados_completion->release();
    return r;
  }
}
