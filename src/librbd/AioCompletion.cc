// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"

#include "librbd/AioRequest.h"
#include "librbd/internal.h"

#include "librbd/AioCompletion.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioCompletion: "

namespace librbd {

  void AioCompletion::finish_adding_requests(CephContext *cct)
  {
    ldout(cct, 20) << "AioCompletion::finish_adding_requests " << (void*)this << " pending " << pending_count << dendl;
    lock.Lock();
    assert(building);
    building = false;
    if (!pending_count) {
      finalize(cct, rval);
      complete();
    }
    lock.Unlock();
  }

  void AioCompletion::finalize(CephContext *cct, ssize_t rval)
  {
    ldout(cct, 20) << "AioCompletion::finalize() " << (void*)this << " rval "
		   << rval << " read_buf " << (void*)read_buf
		   << " read_bl " << (void*)read_bl << dendl;
    if (rval >= 0 && aio_type == AIO_TYPE_READ) {
      // FIXME: make the destriper write directly into a buffer so
      // that we avoid shuffling pointers and copying zeros around.
      bufferlist bl;
    }
  }

  void AioCompletion::complete_request(CephContext *cct, ssize_t r)
  {
    ldout(cct, 20) << "AioCompletion::complete_request() "
		   << (void *)this << " complete_cb=" << (void *)complete_cb
		   << " pending " << pending_count << dendl;
    lock.Lock();
    if (rval >= 0) {
      if (r < 0 && r != -EEXIST)
	rval = r;
      else if (r > 0)
	rval += r;
    }
    assert(pending_count);
    int count = --pending_count;
    if (!count && !building) {
      finalize(cct, rval);
      complete();
    }
    put_unlock();
  }

  void C_AioRead::finish(int r)
  {
    ldout(m_cct, 10) << "C_AioRead::finish() " << this << " r = " << r << dendl;
    if (r >= 0 || r == -ENOENT) { // this was a sparse_read operation
      ldout(m_cct, 10) << " got " << " bl " << m_req->data().length() << dendl;
      m_completion->lock.Lock();
      if (m_completion->read_buf) {
	if (m_completion ->read_buf_len > m_req->data().length())
	  memset(m_completion-> read_buf + m_req->data().length(), 0,
		 m_completion->read_buf_len - m_req->data().length());
	try {
	  unsigned int len = (m_req->data().length() <
			      m_completion->read_buf_len ?
			      m_completion->read_buf_len :
			      m_req->data().length());
	  m_req->data().copy(0, len, m_completion->read_buf);
	} catch (buffer::end_of_buffer &e) { }
      }
      if (m_completion->read_bl) {
	m_completion->read_bl->claim(m_req->data());
	if (m_completion->read_bl->length() < m_req->m_len) {
	  unsigned int spare = m_req->m_len - m_completion->read_bl->length();
	  m_completion->read_bl->append_zero(spare);
	}
      }
      m_completion->lock.Unlock();
      r = m_req->m_len;
    }
    m_completion->complete_request(m_cct, r);
  }

  void C_CacheRead::finish(int r)
  {
    m_req->complete(r);
  }
}
