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

#ifndef CEPH_LIBRADOS_IOCTXIMPL_H
#define CEPH_LIBRADOS_IOCTXIMPL_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/snap_types.h"
#include "include/atomic.h"
#include "include/rados.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include "include/xlist.h"
#include "osd/osd_types.h"
#include "osdc/Objecter.h"
#include "pg/PGOSDMap.h"

class RadosClient;

struct librados::IoCtxImpl {
  atomic_t ref_cnt;
  RadosClient *client;
  int64_t poolid;
  string pool_name;
  snapid_t snap_seq;
  ::SnapContext snapc;
  uint64_t assert_ver;
  map<object_t, uint64_t> assert_src_version;
  eversion_t last_objver;
  uint32_t notify_timeout;
  object_locator_t oloc;

  Mutex aio_write_list_lock;
  tid_t aio_write_seq;
  Cond aio_write_cond;
  xlist<AioCompletionImpl*> aio_write_list;

  Mutex *lock;
  Objecter *objecter;

  IoCtxImpl();
  IoCtxImpl(RadosClient *c, Objecter *objecter, Mutex *client_lock,
	    int poolid, const char *pool_name, snapid_t s);

  void dup(const IoCtxImpl& rhs) {
    // Copy everything except the ref count
    client = rhs.client;
    poolid = rhs.poolid;
    pool_name = rhs.pool_name;
    snap_seq = rhs.snap_seq;
    snapc = rhs.snapc;
    assert_ver = rhs.assert_ver;
    assert_src_version = rhs.assert_src_version;
    last_objver = rhs.last_objver;
    notify_timeout = rhs.notify_timeout;
    oloc = rhs.oloc;
    lock = rhs.lock;
    objecter = rhs.objecter;
  }

  void set_snap_read(snapid_t s);
  int set_snap_write_context(snapid_t seq, vector<snapid_t>& snaps);

  void get() {
    ref_cnt.inc();
  }

  void put() {
    if (ref_cnt.dec() == 0)
      delete this;
  }

  void queue_aio_write(struct AioCompletionImpl *c);
  void complete_aio_write(struct AioCompletionImpl *c);
  void flush_aio_writes();

  int64_t get_id() {
    return poolid;
  }


  const map<int64_t, pg_pool_t>& get_pools() const {
    const PGOSDMap* pgosdmap = dynamic_cast<PGOSDMap*>(objecter->osdmap);
    return pgosdmap->get_pools();
  }

  const pg_pool_t* get_pg_pool(int64_t poolid) const {
    const PGOSDMap* pgosdmap = dynamic_cast<PGOSDMap*>(objecter->osdmap);
    return pgosdmap->get_pg_pool(poolid);
  }

  ::ObjectOperation *prepare_assert_ops(::ObjectOperation *op);

  // snaps
  int snap_list(vector<uint64_t> *snaps);
  int snap_lookup(const char *name, uint64_t *snapid);
  int snap_get_name(uint64_t snapid, std::string *s);
  int snap_get_stamp(uint64_t snapid, time_t *t);
  int snap_create(const char* snapname);
  int selfmanaged_snap_create(uint64_t *snapid);
  int snap_remove(const char* snapname);
  int rollback(const object_t& oid, const char *snapName);
  int selfmanaged_snap_remove(uint64_t snapid);
  int selfmanaged_snap_rollback_object(const object_t& oid,
                                       ::SnapContext& snapc, uint64_t snapid);

  // io
  int list(Objecter::ListContext *context, int max_entries);
  int create(const object_t& oid, bool exclusive);
  int create(const object_t& oid, bool exclusive, const std::string& category);
  int write(const object_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int append(const object_t& oid, bufferlist& bl, size_t len);
  int write_full(const object_t& oid, bufferlist& bl);
  int clone_range(const object_t& dst_oid, uint64_t dst_offset,
                  const object_t& src_oid, uint64_t src_offset, uint64_t len);
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

  int exec(const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(const object_t& oid, const char *name, bufferlist& bl);
  int setxattr(const object_t& oid, const char *name, bufferlist& bl);
  int getxattrs(const object_t& oid, map<string, bufferlist>& attrset);
  int rmxattr(const object_t& oid, const char *name);

  int operate(const object_t& oid, ::ObjectOperation *o, time_t *pmtime);
  int operate_read(const object_t& oid, ::ObjectOperation *o, bufferlist *pbl);
  int aio_operate(const object_t& oid, ::ObjectOperation *o, AioCompletionImpl *c);
  int aio_operate_read(const object_t& oid, ::ObjectOperation *o, AioCompletionImpl *c, bufferlist *pbl);

  struct C_aio_Ack : public Context {
    librados::AioCompletionImpl *c;
    C_aio_Ack(AioCompletionImpl *_c);
    void finish(int r);
  };

  struct C_aio_sparse_read_Ack : public Context {
    AioCompletionImpl *c;
    bufferlist *data_bl;
    std::map<uint64_t,uint64_t> *m;
    C_aio_sparse_read_Ack(AioCompletionImpl *_c);
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
  int aio_exec(const object_t& oid, AioCompletionImpl *c, const char *cls,
	       const char *method, bufferlist& inbl, bufferlist *outbl);

  int pool_change_auid(unsigned long long auid);
  int pool_change_auid_async(unsigned long long auid, PoolAsyncCompletionImpl *c);

  void set_sync_op_version(eversion_t& ver);
  int watch(const object_t& oid, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx);
  int unwatch(const object_t& oid, uint64_t cookie);
  int notify(const object_t& oid, uint64_t ver, bufferlist& bl);
  int _notify_ack(const object_t& oid, uint64_t notify_id, uint64_t ver);

  eversion_t last_version();
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
  uint64_t cookie;
  uint64_t ver;
  librados::WatchCtx *ctx;
  uint64_t linger_id;

  WatchContext(IoCtxImpl *io_ctx_impl_,
	       const object_t& _oc,
	       librados::WatchCtx *_ctx);
  ~WatchContext();
  void notify(Mutex *lock, uint8_t opcode, uint64_t ver, uint64_t notify_id,
	      bufferlist& payload);
};
}
#endif
