// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cassert>
#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#include "librbd/AioRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/LibrbdWriteback.h"


#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdwriteback: "

namespace librbd {

  /**
   * callback to finish a rados completion as a Context
   *
   * @param c completion
   * @param arg Context* recast as void*
   */
  void context_cb(rados_completion_t c, void *arg)
  {
    Context *con = reinterpret_cast<Context *>(arg);
    con->complete(rados_aio_get_return_value(c));
  }

  /**
   * context to wrap another context in a Mutex
   *
   * @param cct cct
   * @param c context to finish
   * @param l mutex to lock
   */
  class C_Request : public Context {
  public:
    C_Request(CephContext *cct, Context *c, Mutex *l)
      : m_cct(cct), m_ctx(c), m_lock(l) {}
    virtual ~C_Request() {}
    virtual void finish(int r) {
      ldout(m_cct, 20) << "aio_cb completing " << dendl;
      {
	Mutex::Locker l(*m_lock);
	m_ctx->complete(r);
      }
      ldout(m_cct, 20) << "aio_cb finished" << dendl;
    }
  private:
    CephContext *m_cct;
    Context *m_ctx;
    Mutex *m_lock;
  };

  class C_OrderedWrite : public Context {
  public:
    C_OrderedWrite(CephContext *cct, LibrbdWriteback::write_result_d *result,
		   LibrbdWriteback *wb)
      : m_cct(cct), m_result(result), m_wb_handler(wb) {}
    virtual ~C_OrderedWrite() {}
    virtual void finish(int r) {
      ldout(m_cct, 20) << "C_OrderedWrite completing " << m_result << dendl;
      {
	Mutex::Locker l(m_wb_handler->m_lock);
	assert(!m_result->done);
	m_result->done = true;
	m_result->ret = r;
	m_wb_handler->complete_writes(m_result->oid);
      }
      ldout(m_cct, 20) << "C_OrderedWrite finished " << m_result << dendl;
    }
  private:
    CephContext *m_cct;
    LibrbdWriteback::write_result_d *m_result;
    LibrbdWriteback *m_wb_handler;
  };

  LibrbdWriteback::LibrbdWriteback(ImageCtx *ictx, Mutex& lock)
    : m_tid(0), m_lock(lock), m_ictx(ictx)
  {
  }

  void LibrbdWriteback::read(const object_t& oid, const uuid_d& volume,
			     uint64_t off, uint64_t len,
			     bufferlist *pbl, uint64_t trunc_size,
			     uint32_t trunc_seq, Context *onfinish)
  {
    // on completion, take the mutex and then call onfinish.
    Context *req = new C_Request(m_ictx->cct, onfinish, &m_lock);
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(req, context_cb, NULL);
    librados::ObjectReadOperation op;
    op.read(off, len, pbl, NULL);
    // Volume is actually unused since it's in data_ctx
    int r = m_ictx->data_ctx.aio_operate(oid.name, rados_completion, &op,
					 0, NULL);
    rados_completion->release();
    assert(r >= 0);
  }

  bool LibrbdWriteback::may_copy_on_write(const object_t& oid,
					  uint64_t read_off, uint64_t read_len)
  {
    return false;
  }

  ceph_tid_t LibrbdWriteback::write(const object_t& oid,
				    const uuid_d& volume,
				    uint64_t off, uint64_t len,
				    const bufferlist &bl, utime_t mtime,
				    uint64_t trunc_size, uint32_t trunc_seq,
				    Context *oncommit)
  {
    write_result_d *result = new write_result_d(oid.name, oncommit);
    m_writes[oid.name].push(result);
    ldout(m_ictx->cct, 20) << "write will wait for result " << result << dendl;
    C_OrderedWrite *req_comp = new C_OrderedWrite(m_ictx->cct, result, this);
    AioWrite *req = new AioWrite(m_ictx, oid.name, off, bl, req_comp);
    req->send();
    return ++m_tid;
  }

  void LibrbdWriteback::complete_writes(const std::string& oid)
  {
    assert(m_lock.is_locked());
    std::queue<write_result_d*>& results = m_writes[oid];
    ldout(m_ictx->cct, 20) << "complete_writes() oid " << oid << dendl;
    std::list<write_result_d*> finished;

    while (!results.empty()) {
      write_result_d *result = results.front();
      if (!result->done)
	break;
      finished.push_back(result);
      results.pop();
    }

    if (results.empty())
      m_writes.erase(oid);

    for (std::list<write_result_d*>::iterator it = finished.begin();
	 it != finished.end(); ++it) {
      write_result_d *result = *it;
      ldout(m_ictx->cct, 20) << "complete_writes() completing " << result
			     << dendl;
      result->oncommit->complete(result->ret);
      delete result;
    }
  }
}
