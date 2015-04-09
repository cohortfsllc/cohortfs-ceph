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
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOS_IOCTXIMPL_H
#define CEPH_LIBRADOS_IOCTXIMPL_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include "include/types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/xlist.h"
#include "osd/osd_types.h"
#include "osdc/Objecter.h"

namespace librados {
  class RadosClient;
};

struct librados::IoCtxImpl {
  std::atomic<uint64_t> ref_cnt;
  RadosClient *client;
  std::shared_ptr<const Volume> volume;
  uint64_t assert_ver;
  map<oid_t, uint64_t> assert_src_version;
  version_t last_objver;
  ceph::timespan notify_timeout;

  std::mutex aio_write_list_lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  ceph_tid_t aio_write_seq;
  std::condition_variable aio_write_cond;
  xlist<AioCompletionImpl*> aio_write_list;
  map<ceph_tid_t, std::list<AioCompletionImpl*> > aio_write_waiters;

  Objecter *objecter;

  IoCtxImpl();
  IoCtxImpl(RadosClient *c, Objecter *objecter,
	    const std::shared_ptr <const Volume>& volume);

  void dup(const IoCtxImpl& rhs) {
    // Copy everything except the ref count
    client = rhs.client;
    volume = rhs.volume;
    assert_ver = rhs.assert_ver;
    assert_src_version = rhs.assert_src_version;
    last_objver = rhs.last_objver;
    notify_timeout = rhs.notify_timeout;
    objecter = rhs.objecter;
  }

  void get() {
    ++ref_cnt;
  }

  void put() {
    if (--ref_cnt == 0)
      delete this;
  }

  void queue_aio_write(struct AioCompletionImpl *c);
  void complete_aio_write(struct AioCompletionImpl *c);
  void flush_aio_writes_async(AioCompletionImpl *c);
  void flush_aio_writes();

  const boost::uuids::uuid& get_volume() {
    return volume->id;
  }

  std::unique_ptr<ObjOp> prepare_assert_ops();

  // io
  int create(const oid_t& oid, bool exclusive);
  int create(const oid_t& oid, bool exclusive, const std::string& category);
  int write(const oid_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int append(const oid_t& oid, bufferlist& bl, size_t len);
  int write_full(const oid_t& oid, bufferlist& bl);
  int read(const oid_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int sparse_read(const oid_t& oid, std::map<uint64_t,uint64_t>& m,
		  bufferlist& bl, size_t len, uint64_t off);
  int remove(const oid_t& oid);
  int stat(const oid_t& oid, uint64_t *psize, time_t *pmtime);
  int trunc(const oid_t& oid, uint64_t size);

  int exec(const oid_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(const oid_t& oid, const char *name, bufferlist& bl);
  int setxattr(const oid_t& oid, const char *name, bufferlist& bl);
  int getxattrs(const oid_t& oid, map<string, bufferlist>& attrset);
  int rmxattr(const oid_t& oid, const char *name);

  int operate(const oid_t& oid, std::unique_ptr<ObjOp>& o, time_t *pmtime,
	      int flags=0);
  int operate(const oid_t& oid, librados::ObjectOperation *op,
	      time_t *pmtime, int flags=0) {
    return operate(oid, op->impl, pmtime, flags);
  }
  int operate_read(const oid_t& oid, std::unique_ptr<ObjOp>& o,
		   bufferlist *pbl, int flags=0);
  int operate_read(const oid_t& oid, librados::ObjectOperation *op,
		   bufferlist *pbl, int flags=0) {
    return operate_read(oid, op->impl, pbl, flags);
  }
  int aio_operate(const oid_t& oid, std::unique_ptr<ObjOp>& o,
		  AioCompletionImpl *c, int flags);
  int aio_operate(const oid_t& oid, librados::ObjectOperation *op,
		  AioCompletionImpl *c, int flags) {
    return aio_operate(oid, op->impl, c, flags);
  }
  int aio_operate_read(const oid_t& oid, std::unique_ptr<ObjOp>& o,
		       AioCompletionImpl *c, int flags, bufferlist *pbl);
  int aio_operate_read(const oid_t& oid, librados::ObjectOperation *op,
		       AioCompletionImpl *c, int flags, bufferlist *pbl) {
    return aio_operate_read(oid, op->impl, c, flags, pbl);
  }

  struct CB_aio_Ack {
    librados::AioCompletionImpl *c;
    CB_aio_Ack(AioCompletionImpl *_c);
    void operator()(int r);
  };

  struct CB_aio_stat_Ack {
    librados::AioCompletionImpl *c;
    uint64_t* psize;
    time_t *pmtime;
    CB_aio_stat_Ack(AioCompletionImpl *_c, uint64_t* ps, time_t* pm);
    void operator()(int r, uint64_t s, ceph::real_time t);
  };

  struct CB_aio_Safe {
    AioCompletionImpl *c;
    CB_aio_Safe(AioCompletionImpl *_c);
    void operator()(int r);
  };

  int aio_read(const oid_t oid, AioCompletionImpl *c,
	       bufferlist *pbl, size_t len, uint64_t off);
  int aio_read(oid_t oid, AioCompletionImpl *c,
	       char *buf, size_t len, uint64_t off);
  int aio_write(const oid_t &oid, AioCompletionImpl *c,
		const bufferlist& bl, size_t len, uint64_t off);
  int aio_append(const oid_t &oid, AioCompletionImpl *c,
		 const bufferlist& bl, size_t len);
  int aio_write_full(const oid_t &oid, AioCompletionImpl *c,
		     const bufferlist& bl);
  int aio_remove(const oid_t &oid, AioCompletionImpl *c);
  int aio_exec(const oid_t& oid, AioCompletionImpl *c, const char *cls,
	       const char *method, bufferlist& inbl, bufferlist *outbl);
  int aio_stat(const oid_t& oid, AioCompletionImpl *c, uint64_t *psize, time_t *pmtime);

  void set_sync_op_version(version_t ver);
  int watch(const oid_t& oid, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx);
  int unwatch(const oid_t& oid, uint64_t cookie);
  int notify(const oid_t& oid, bufferlist& bl);
  int _notify_ack(
    const oid_t& oid, uint64_t notify_id, uint64_t ver,
    uint64_t cookie);

  int set_alloc_hint(const oid_t& oid,
		     uint64_t expected_object_size,
		     uint64_t expected_write_size);

  version_t last_version();
  void set_assert_version(uint64_t ver);
  void set_assert_src_version(const oid_t& oid, uint64_t ver);
  void set_notify_timeout(ceph::timespan timeout);
  uint64_t op_size() const {
    return volume->op_size();
  }

  struct C_NotifyComplete : public librados::WatchCtx {
    std::mutex *lock;
    typedef std::unique_lock<std::mutex> unique_lock;
    typedef std::lock_guard<std::mutex> lock_guard;
    std::condition_variable *cond;
    bool *done;

    C_NotifyComplete(std::mutex *_l, std::condition_variable *_c, bool *_d);
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl);
  };
};

namespace librados {

  /**
   * watch/notify info
   *
   * Capture state about a watch or an in-progress notify
   */
struct WatchNotifyInfo : public RefCountedWaitObject {
  IoCtxImpl *io_ctx_impl; // parent
  const oid_t oid; // the object
  uint64_t linger_id; // we use this to unlinger when we are done
  uint64_t cookie; // callback cookie

  // watcher
  librados::WatchCtx *watch_ctx;

  // notify that we initiated
  std::mutex *notify_lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  std::condition_variable *notify_cond;
  bool *notify_done;
  int *notify_rval;

  WatchNotifyInfo(IoCtxImpl *io_ctx_impl_,
		  const oid_t& _oc)
    : io_ctx_impl(io_ctx_impl_),
      oid(_oc),
      linger_id(0),
      cookie(0),
      watch_ctx(NULL),
      notify_lock(NULL),
      notify_cond(NULL),
      notify_done(NULL),
      notify_rval(NULL) {
    io_ctx_impl->get();
  }

  ~WatchNotifyInfo() {
    io_ctx_impl->put();
  }

  void notify(std::mutex *lock, uint8_t opcode, uint64_t ver,
	      uint64_t notify_id, bufferlist& payload, int return_code);
};
}

#endif
