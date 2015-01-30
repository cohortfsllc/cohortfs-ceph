// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOCOMPLETION_H
#define CEPH_LIBRBD_AIOCOMPLETION_H

#include <condition_variable>
#include <mutex>

#include "include/ceph_time.h"
#include "common/ceph_context.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"

#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

namespace librbd {

  class AioRead;

  typedef enum {
    AIO_TYPE_READ = 0,
    AIO_TYPE_WRITE,
    AIO_TYPE_DISCARD,
    AIO_TYPE_FLUSH,
    AIO_TYPE_NONE,
  } aio_type_t;

  /**
   * AioCompletion is the overall completion for a single
   * rbd I/O request. It may be composed of many AioRequests,
   * which each go to a single object.
   *
   * The retrying of individual requests is handled at a lower level,
   * so all AioCompletion cares about is the count of outstanding
   * requests. Note that this starts at 1 to prevent the reference
   * count from reaching 0 while more requests are being added. When
   * all requests have been added, finish_adding_requests() releases
   * this initial reference.
   */
  struct AioCompletion {
    std::recursive_mutex lock;
    typedef std::lock_guard<std::recursive_mutex> lock_guard;
    typedef std::unique_lock<std::recursive_mutex> unique_lock;
    std::condition_variable_any cond;
    bool done;
    ssize_t rval;
    callback_t complete_cb;
    void *complete_arg;
    rbd_completion_t rbd_comp;
    int pending_count;   ///< number of requests
    bool building;       ///< true if we are still building this completion
    int ref;
    bool released;
    ImageCtx *ictx;
    ceph::mono_time start_time;
    aio_type_t aio_type;

    bufferlist *read_bl;
    char *read_buf;
    size_t read_buf_len;

    AioCompletion() : done(false), rval(0), complete_cb(NULL),
		      complete_arg(NULL), rbd_comp(NULL),
		      pending_count(0), building(true),
		      ref(1), released(false), ictx(NULL),
		      aio_type(AIO_TYPE_NONE),
		      read_bl(NULL), read_buf(NULL), read_buf_len(0) {
    }
    ~AioCompletion() {
    }

    int wait_for_complete() {
      unique_lock l(lock);
      cond.wait(l, [&](){ return done; });
      return 0;
    }

    void add_request() {
      unique_lock l(lock);
      pending_count++;
      l.unlock();
      get();
    }

    void finalize(CephContext *cct, ssize_t rval);

    void finish_adding_requests(CephContext *cct);

    void init_time(ImageCtx *i, aio_type_t t) {
      ictx = i;
      aio_type = t;
      start_time = ceph::mono_clock::now();
    }

    void complete() {
      if (complete_cb) {
	complete_cb(rbd_comp, complete_arg);
      }
      done = true;
      cond.notify_all();
    }

    void set_complete_cb(void *cb_arg, callback_t cb) {
      complete_cb = cb;
      complete_arg = cb_arg;
    }

    void complete_request(CephContext *cct, ssize_t r);

    bool is_complete() {
      lock_guard l(lock);
      return done;
    }

    ssize_t get_return_value() {
      lock_guard l(lock);
      ssize_t r = rval;
      return r;
    }

    void get() {
      lock_guard l(lock);
      assert(ref > 0);
      ref++;
    }
    void release() {
      lock_guard l(lock);
      assert(!released);
      released = true;
    }
    void put() {
      unique_lock l(lock);
      put_unlock(l);
    }
    void put_unlock(unique_lock& l) {
      assert(ref > 0);
      int n = --ref;
      l.unlock();
      l.release(); // Just so we don't have it pointint at bad memory.
      if (!n)
	delete this;
    }
  };

  class C_AioRead : public Context {
  public:
    C_AioRead(CephContext *cct, AioCompletion *completion)
      : m_cct(cct), m_completion(completion), m_req(NULL)
    { }
    virtual ~C_AioRead() {}
    virtual void finish(int r);
    void set_req(AioRead *req) {
      m_req = req;
    }
  private:
    CephContext *m_cct;
    AioCompletion *m_completion;
    AioRead *m_req;
  };

  class C_AioWrite : public Context {
  public:
    C_AioWrite(CephContext *cct, AioCompletion *completion)
      : m_cct(cct), m_completion(completion) {}
    virtual ~C_AioWrite() {}
    virtual void finish(int r) {
      m_completion->complete_request(m_cct, r);
    }
  private:
    CephContext *m_cct;
    AioCompletion *m_completion;
  };

  class C_CacheRead : public Context {
  public:
    explicit C_CacheRead(AioRead *req) : m_req(req) {}
    virtual ~C_CacheRead() {}
    virtual void finish(int r);
  private:
    AioRead *m_req;
  };
}

#endif
