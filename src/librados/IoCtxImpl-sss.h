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
  uuid_d volume;
  uint64_t assert_ver;
  map<object_t, uint64_t> assert_src_version;
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
	    const uuid_d& volume);

  void dup(const IoCtxImpl& rhs) {
    // Copy everything except the ref count
    client = rhs.client;
    volume = volume;
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

  uuid_d get_volume() {
    return volume;
  }

  ::ObjectOperation *prepare_assert_ops(::ObjectOperation *op);

  // io
  int create(const object_t& oid, bool exclusive);
  int create(const object_t& oid, bool exclusive, const std::string& category);
  int write(const object_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int append(const object_t& oid, bufferlist& bl, size_t len);
  int write_full(const object_t& oid, bufferlist& bl);
  int read(const object_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int mapext(const object_t& oid, uint64_t off, size_t len,
	     std::map<uint64_t,uint64_t>& m);
  int sparse_read(const object_t& oid, std::map<uint64_t,uint64_t>& m,
		  bufferlist& bl, size_t len, uint64_t off);
  int remove(const object_t& oid);
  int stat(const object_t& oid, uint64_t *psize, time_t *pmtime);
  int trunc(const object_t& oid, uint64_t size);

  int tmap_update(const object_t& oid, bufferlist& cmdbl);
  int tmap_put(const object_t& oid, bufferlist& bl);
  int tmap_get(const object_t& oid, bufferlist& bl);
  int tmap_to_omap(const object_t& oid, bool nullok=false);

  int exec(const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(const object_t& oid, const char *name, bufferlist& bl);
  int setxattr(const object_t& oid, const char *name, bufferlist& bl);
  int getxattrs(const object_t& oid, map<string, bufferlist>& attrset);
  int rmxattr(const object_t& oid, const char *name);

  int operate(const object_t& oid, ::ObjectOperation *o, time_t *pmtime, int flags=0);
  int operate_read(const object_t& oid, ::ObjectOperation *o, bufferlist *pbl, int flags=0);
  int aio_operate(const object_t& oid, ::ObjectOperation *o,
		  AioCompletionImpl *c, int flags);
  int aio_operate_read(const object_t& oid, ::ObjectOperation *o,
		       AioCompletionImpl *c, int flags, bufferlist *pbl);

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

  int aio_read(const object_t oid, AioCompletionImpl *c,
	       bufferlist *pbl, size_t len, uint64_t off);
  int aio_read(object_t oid, AioCompletionImpl *c,
	       char *buf, size_t len, uint64_t off);
  int aio_sparse_read(const object_t oid, AioCompletionImpl *c,
		      std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
		      size_t len, uint64_t off);
  int aio_write(const object_t &oid, AioCompletionImpl *c,
		const bufferlist& bl, size_t len, uint64_t off);
  int aio_append(const object_t &oid, AioCompletionImpl *c,
		 const bufferlist& bl, size_t len);
  int aio_write_full(const object_t &oid, AioCompletionImpl *c,
		     const bufferlist& bl);
  int aio_remove(const object_t &oid, AioCompletionImpl *c);
  int aio_exec(const object_t& oid, AioCompletionImpl *c, const char *cls,
	       const char *method, bufferlist& inbl, bufferlist *outbl);
  int aio_stat(const object_t& oid, AioCompletionImpl *c, uint64_t *psize, time_t *pmtime);

  void set_sync_op_version(version_t ver);
  int watch(const object_t& oid, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx);
  int unwatch(const object_t& oid, uint64_t cookie);
  int notify(const object_t& oid, uint64_t ver, bufferlist& bl);
  int _notify_ack(
    const object_t& oid, uint64_t notify_id, uint64_t ver,
    uint64_t cookie);

  int set_alloc_hint(const object_t& oid,
		     uint64_t expected_object_size,
		     uint64_t expected_write_size);

  version_t last_version();
  void set_assert_version(uint64_t ver);
  void set_assert_src_version(const object_t& oid, uint64_t ver);
  void set_notify_timeout(uint32_t timeout);

  struct C_NotifyComplete : public librados::WatchCtx {
    Mutex *lock;
    Cond *cond;
    bool *done;

    C_NotifyComplete(Mutex *_l, Cond *_c, bool *_d);
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl);
  };
};

namespace librados {
struct WatchContext : public RefCountedWaitObject {
  IoCtxImpl *io_ctx_impl;
  const object_t oid;
  librados::WatchCtx *ctx;
  uint64_t linger_id;
  uint64_t cookie;

  WatchContext(IoCtxImpl *io_ctx_impl_,
	       const object_t& _oc,
	       librados::WatchCtx *_ctx);
  ~WatchContext();
  void notify(Mutex *lock, uint8_t opcode, uint64_t ver, uint64_t notify_id,
	      bufferlist& payload);
};
}
#endif
