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
#include "common/Cond.h"
#include "common/Mutex.h"
#include "include/types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/xlist.h"
#include "osd/osd_types.h"
#include "osdc/Objecter.h"

class RadosClient;

struct librados::IoCtxImpl {
  std::atomic<uint64_t> ref_cnt;
  RadosClient *client;
  std::shared_ptr<const Volume> volume;
  uint64_t assert_ver;
  map<oid, uint64_t> assert_src_version;
  version_t last_objver;
  uint32_t notify_timeout;

  Mutex aio_write_list_lock;
  ceph_tid_t aio_write_seq;
  Cond aio_write_cond;
  xlist<AioCompletionImpl*> aio_write_list;
  map<ceph_tid_t, std::list<AioCompletionImpl*> > aio_write_waiters;

  Mutex *lock;
  Objecter *objecter;

  IoCtxImpl();
  IoCtxImpl(RadosClient *c, Objecter *objecter, Mutex *client_lock,
	    const std::shared_ptr <const Volume>& volume);

  void dup(const IoCtxImpl& rhs) {
    // Copy everything except the ref count
    client = rhs.client;
    volume = rhs.volume;
    assert_ver = rhs.assert_ver;
    assert_src_version = rhs.assert_src_version;
    last_objver = rhs.last_objver;
    notify_timeout = rhs.notify_timeout;
    lock = rhs.lock;
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
  int create(const oid& obj, bool exclusive);
  int create(const oid& obj, bool exclusive, const std::string& category);
  int write(const oid& obj, bufferlist& bl, size_t len, uint64_t off);
  int append(const oid& obj, bufferlist& bl, size_t len);
  int write_full(const oid& obj, bufferlist& bl);
  int read(const oid& obj, bufferlist& bl, size_t len, uint64_t off);
  int sparse_read(const oid& obj, std::map<uint64_t,uint64_t>& m,
		  bufferlist& bl, size_t len, uint64_t off);
  int remove(const oid& obj);
  int stat(const oid& obj, uint64_t *psize, time_t *pmtime);
  int trunc(const oid& obj, uint64_t size);

  int exec(const oid& obj, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(const oid& obj, const char *name, bufferlist& bl);
  int setxattr(const oid& obj, const char *name, bufferlist& bl);
  int getxattrs(const oid& obj, map<string, bufferlist>& attrset);
  int rmxattr(const oid& obj, const char *name);

  int operate(const oid& obj, std::unique_ptr<ObjOp>& o, time_t *pmtime,
	      int flags=0);
  int operate(const oid& obj, librados::ObjectOperation *op,
	      time_t *pmtime, int flags=0) {
    return operate(obj, op->impl, pmtime, flags);
  }
  int operate_read(const oid& obj, std::unique_ptr<ObjOp>& o,
		   bufferlist *pbl, int flags=0);
  int operate_read(const oid& obj, librados::ObjectOperation *op,
		   bufferlist *pbl, int flags=0) {
    return operate_read(obj, op->impl, pbl, flags);
  }
  int aio_operate(const oid& obj, std::unique_ptr<ObjOp>& o,
		  AioCompletionImpl *c, int flags);
  int aio_operate(const oid& obj, librados::ObjectOperation *op,
		  AioCompletionImpl *c, int flags) {
    return aio_operate(obj, op->impl, c, flags);
  }
  int aio_operate_read(const oid& obj, std::unique_ptr<ObjOp>& o,
		       AioCompletionImpl *c, int flags, bufferlist *pbl);
  int aio_operate_read(const oid& obj, librados::ObjectOperation *op,
		       AioCompletionImpl *c, int flags, bufferlist *pbl) {
    return aio_operate_read(obj, op->impl, c, flags, pbl);
  }

  struct C_aio_Ack : public Context {
    librados::AioCompletionImpl *c;
    C_aio_Ack(AioCompletionImpl *_c);
    void finish(int r);
  };

  struct C_aio_stat_Ack : public Context {
    librados::AioCompletionImpl *c;
    time_t *pmtime;
    utime_t mtime;
    C_aio_stat_Ack(AioCompletionImpl *_c, time_t *pm);
    void finish(int r);
  };

  struct C_aio_Safe : public Context {
    AioCompletionImpl *c;
    C_aio_Safe(AioCompletionImpl *_c);
    void finish(int r);
  };

  int aio_read(const oid obj, AioCompletionImpl *c,
	       bufferlist *pbl, size_t len, uint64_t off);
  int aio_read(oid obj, AioCompletionImpl *c,
	       char *buf, size_t len, uint64_t off);
  int aio_sparse_read(const oid obj, AioCompletionImpl *c,
		      std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
		      size_t len, uint64_t off);
  int aio_write(const oid &obj, AioCompletionImpl *c,
		const bufferlist& bl, size_t len, uint64_t off);
  int aio_append(const oid &obj, AioCompletionImpl *c,
		 const bufferlist& bl, size_t len);
  int aio_write_full(const oid &obj, AioCompletionImpl *c,
		     const bufferlist& bl);
  int aio_remove(const oid &obj, AioCompletionImpl *c);
  int aio_exec(const oid& obj, AioCompletionImpl *c, const char *cls,
	       const char *method, bufferlist& inbl, bufferlist *outbl);
  int aio_stat(const oid& obj, AioCompletionImpl *c, uint64_t *psize, time_t *pmtime);

  void set_sync_op_version(version_t ver);
  int watch(const oid& obj, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx);
  int unwatch(const oid& obj, uint64_t cookie);
  int notify(const oid& obj, bufferlist& bl);
  int _notify_ack(
    const oid& obj, uint64_t notify_id, uint64_t ver,
    uint64_t cookie);

  int set_alloc_hint(const oid& obj,
		     uint64_t expected_object_size,
		     uint64_t expected_write_size);

  version_t last_version();
  void set_assert_version(uint64_t ver);
  void set_assert_src_version(const oid& obj, uint64_t ver);
  void set_notify_timeout(uint32_t timeout);
  uint64_t op_size() const {
    return volume->op_size();
  }

  struct C_NotifyComplete : public librados::WatchCtx {
    Mutex *lock;
    Cond *cond;
    bool *done;

    C_NotifyComplete(Mutex *_l, Cond *_c, bool *_d);
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
  const oid obj; // the object
  uint64_t linger_id; // we use this to unlinger when we are done
  uint64_t cookie; // callback cookie

  // watcher
  librados::WatchCtx *watch_ctx;

  // notify that we initiated
  Mutex *notify_lock;
  Cond *notify_cond;
  bool *notify_done;
  int *notify_rval;

  WatchNotifyInfo(IoCtxImpl *io_ctx_impl_,
		  const oid& _oc)
    : io_ctx_impl(io_ctx_impl_),
      obj(_oc),
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

  void notify(Mutex *lock, uint8_t opcode, uint64_t ver, uint64_t notify_id,
	      bufferlist& payload,
	      int return_code);
};
}

#endif
