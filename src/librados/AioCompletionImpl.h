// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOS_AIOCOMPLETIONIMPL_H
#define CEPH_LIBRADOS_AIOCOMPLETIONIMPL_H

#include <condition_variable>
#include <mutex>

#include "include/buffer.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/xlist.h"
#include "osd/osd_types.h"

class IoCtxImpl;

struct librados::AioCompletionImpl {
  std::mutex lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  std::condition_variable cond;
  int ref, rval;
  bool released;
  bool ack, safe;
  version_t objver;

  rados_callback_t callback_complete, callback_safe;
  void *callback_complete_arg, *callback_safe_arg;

  // for read
  bool is_read;
  bufferlist bl;
  bufferlist *blp;

  IoCtxImpl *io;
  ceph_tid_t aio_write_seq;
  xlist<AioCompletionImpl*>::item aio_write_list_item;

  AioCompletionImpl() : lock(),
			ref(1), rval(0), released(false), ack(false), safe(false),
			objver(0),
			callback_complete(0),
			callback_safe(0),
			callback_complete_arg(0),
			callback_safe_arg(0),
			is_read(false), blp(NULL),
			io(NULL), aio_write_seq(0), aio_write_list_item(this) { }

  int set_complete_callback(void *cb_arg, rados_callback_t cb) {
    lock_guard l(lock);
    callback_complete = cb;
    callback_complete_arg = cb_arg;
    return 0;
  }
  int set_safe_callback(void *cb_arg, rados_callback_t cb) {
    lock_guard l(lock);
    callback_safe = cb;
    callback_safe_arg = cb_arg;
    return 0;
  }
  int wait_for_complete() {
    unique_lock l(lock);
    while (!ack)
      cond.wait(l);
    l.unlock();
    return 0;
  }
  int wait_for_safe() {
    unique_lock l(lock);
    while (!safe)
      cond.wait(l);
    l.unlock();
    return 0;
  }
  int is_complete() {
    lock_guard l(lock);
    int r = ack;
    return r;
  }
  int is_safe() {
    lock_guard l(lock);
    int r = safe;
    return r;
  }
  int wait_for_complete_and_cb() {
    unique_lock l(lock);
    while (!ack || callback_complete)
      cond.wait(l);
    l.unlock();
    return 0;
  }
  int wait_for_safe_and_cb() {
    unique_lock l(lock);
    while (!safe || callback_safe)
      cond.wait(l);
    l.unlock();
    return 0;
  }
  int is_complete_and_cb() {
    lock_guard l(lock);
    int r = ack && !callback_complete;
    return r;
  }
  int is_safe_and_cb() {
    lock_guard l(lock);
    int r = safe && !callback_safe;
    return r;
  }
  int get_return_value() {
    lock_guard l(lock);
    int r = rval;
    return r;
  }
  uint64_t get_version() {
    lock_guard l(lock);
    version_t v = objver;
    return v;
  }

  void get() {
    lock_guard l(lock);
    _get();
  }
  void _get() {
    assert(ref > 0);
    ++ref;
  }
  void release() {
    unique_lock l(lock);
    assert(!released);
    released = true;
    put_unlock(l);
  }
  void put() {
    unique_lock l(lock);
    put_unlock(l);
  }
  void put_unlock(unique_lock& l) {
    assert(l && l.mutex() == &lock);
    assert(ref > 0);
    int n = --ref;
    l.unlock();
    if (!n)
      delete this;
  }
};

namespace librados {
struct C_AioComplete : public Context {
  AioCompletionImpl *c;

  C_AioComplete(AioCompletionImpl *cc) : c(cc) {
    c->_get();
  }

  void finish(int r) {
    rados_callback_t cb = c->callback_complete;
    void *cb_arg = c->callback_complete_arg;
    cb(c, cb_arg);

    librados::AioCompletionImpl::unique_lock cl(c->lock);
    c->callback_complete = NULL;
    c->cond.notify_all();
    c->put_unlock(cl);
  }
};

struct C_AioSafe : public Context {
  AioCompletionImpl *c;

  C_AioSafe(AioCompletionImpl *cc) : c(cc) {
    c->_get();
  }

  void finish(int r) {
    rados_callback_t cb = c->callback_safe;
    void *cb_arg = c->callback_safe_arg;
    cb(c, cb_arg);

    librados::AioCompletionImpl::unique_lock cl(c->lock);
    c->callback_safe = NULL;
    c->cond.notify_all();
    c->put_unlock(cl);
  }
};

/**
  * Fills in all completed request data, and calls both
  * complete and safe callbacks if they exist.
  *
  * Not useful for usual I/O, but for special things like
  * flush where we only want to wait for things to be safe,
  * but allow users to specify any of the callbacks.
  */
struct C_AioCompleteAndSafe : public Context {
  AioCompletionImpl *c;

  C_AioCompleteAndSafe(AioCompletionImpl *cc) : c(cc) {
    c->get();
  }

  void finish(int r) {
    librados::AioCompletionImpl::unique_lock cl(c->lock);
    c->rval = r;
    c->ack = true;
    c->safe = true;
    cl.unlock();
    rados_callback_t cb_complete = c->callback_complete;
    void *cb_complete_arg = c->callback_complete_arg;
    if (cb_complete)
      cb_complete(c, cb_complete_arg);

    rados_callback_t cb_safe = c->callback_safe;
    void *cb_safe_arg = c->callback_safe_arg;
    if (cb_safe)
      cb_safe(c, cb_safe_arg);

    cl.lock();
    c->callback_complete = NULL;
    c->callback_safe = NULL;
    c->cond.notify_all();
    c->put_unlock(cl);
  }
};

}

#endif
