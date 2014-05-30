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

#include <limits.h>

#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include <include/stringify.h>

#include "librados/AioCompletionImpl.h"
#include "librados/IoCtxImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"
#include <cls/lock/cls_lock_client.h>

#include <string>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <stdexcept>

using std::string;
using std::map;
using std::set;
using std::vector;
using std::list;
using std::runtime_error;

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

#define RADOS_LIST_MAX_ENTRIES 1024

/*
 * Structure of this file
 *
 * RadosClient and the related classes are the internal implementation of librados.
 * Above that layer sits the C API, found in include/rados/librados.h, and
 * the C++ API, found in include/rados/librados.hpp
 *
 * The C++ API sometimes implements things in terms of the C API.
 * Both the C++ and C API rely on RadosClient.
 *
 * Visually:
 * +--------------------------------------+
 * |             C++ API                  |
 * +--------------------+                 |
 * |       C API        |                 |
 * +--------------------+-----------------+
 * |          RadosClient                 |
 * +--------------------------------------+
 */

size_t librados::ObjectOperation::size()
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  return o->size();
}

static void set_op_flags(::ObjectOperation *o, int flags)
{
  int rados_flags = 0;
  if (flags & LIBRADOS_OP_FLAG_EXCL)
    rados_flags |= CEPH_OSD_OP_FLAG_EXCL;
  if (flags & LIBRADOS_OP_FLAG_FAILOK)
    rados_flags |= CEPH_OSD_OP_FLAG_FAILOK;
  o->set_last_op_flags(rados_flags);
}

void librados::ObjectOperation::set_op_flags(ObjectOperationFlags flags)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  ::set_op_flags(o, (int)flags);
}

void librados::ObjectOperation::cmpxattr(const char *name, uint8_t op, const bufferlist& v)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_STRING, v);
}

void librados::ObjectOperation::cmpxattr(const char *name, uint8_t op, uint64_t v)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  bufferlist bl;
  ::encode(v, bl);
  o->cmpxattr(name, op, CEPH_OSD_CMPXATTR_MODE_U64, bl);
}

void librados::ObjectOperation::src_cmpxattr(const std::string& src_oid,
					 const char *name, int op, const bufferlist& v)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  object_t oid(src_oid);
  o->src_cmpxattr(oid, name, v, op, CEPH_OSD_CMPXATTR_MODE_STRING);
}

void librados::ObjectOperation::src_cmpxattr(const std::string& src_oid,
					 const char *name, int op, uint64_t val)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  object_t oid(src_oid);
  bufferlist bl;
  ::encode(val, bl);
  o->src_cmpxattr(oid, name, bl, op, CEPH_OSD_CMPXATTR_MODE_U64);
}

void librados::ObjectOperation::assert_version(uint64_t ver)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->assert_version(ver);
}

void librados::ObjectOperation::assert_exists()
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->stat(NULL, (utime_t*)NULL, NULL);
}

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->call(cls, method, inbl);
}

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl, bufferlist *outbl, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->call(cls, method, inbl, outbl, NULL, prval);
}

class ObjectOpCompletionCtx : public Context {
  librados::ObjectOperationCompletion *completion;
  bufferlist bl;
public:
  ObjectOpCompletionCtx(librados::ObjectOperationCompletion *c) : completion(c) {}
  void finish(int r) {
    completion->handle_completion(r, bl);
    delete completion;
  }

  bufferlist *outbl() {
    return &bl;
  }
};

void librados::ObjectOperation::exec(const char *cls, const char *method, bufferlist& inbl, librados::ObjectOperationCompletion *completion)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;

  ObjectOpCompletionCtx *ctx = new ObjectOpCompletionCtx(completion);

  o->call(cls, method, inbl, ctx->outbl(), ctx, NULL);
}

void librados::ObjectReadOperation::stat(uint64_t *psize, time_t *pmtime, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->stat(psize, pmtime, prval);
}

void librados::ObjectReadOperation::read(size_t off, uint64_t len, bufferlist *pbl, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->read(off, len, pbl, prval, NULL);
}

void librados::ObjectReadOperation::sparse_read(uint64_t off, uint64_t len,
						std::map<uint64_t,uint64_t> *m,
						bufferlist *data_bl, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->sparse_read(off, len, m, data_bl, prval);
}

void librados::ObjectReadOperation::tmap_get(bufferlist *pbl, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->tmap_get(pbl, prval);
}

void librados::ObjectReadOperation::getxattr(const char *name, bufferlist *pbl, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->getxattr(name, pbl, prval);
}

void librados::ObjectReadOperation::omap_get_vals(
  const std::string &start_after,
  const std::string &filter_prefix,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_get_vals(start_after, filter_prefix, max_return, out_vals, prval);
}

void librados::ObjectReadOperation::omap_get_vals(
  const std::string &start_after,
  uint64_t max_return,
  std::map<std::string, bufferlist> *out_vals,
  int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_get_vals(start_after, "", max_return, out_vals, prval);
}

void librados::ObjectReadOperation::omap_get_keys(
  const std::string &start_after,
  uint64_t max_return,
  std::set<std::string> *out_keys,
  int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_get_keys(start_after, max_return, out_keys, prval);
}

void librados::ObjectReadOperation::omap_get_header(bufferlist *bl, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_get_header(bl, prval);
}

void librados::ObjectReadOperation::omap_get_vals_by_keys(
  const std::set<std::string> &keys,
  std::map<std::string, bufferlist> *map,
  int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_get_vals_by_keys(keys, map, prval);
}

void librados::ObjectOperation::omap_cmp(
  const std::map<std::string, pair<bufferlist, int> > &assertions,
  int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_cmp(assertions, prval);
}

void librados::ObjectReadOperation::list_watchers(
  list<obj_watch_t> *out_watchers,
  int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->list_watchers(out_watchers, prval);
}

int librados::IoCtx::omap_get_vals(const std::string& oid,
				   const std::string& start_after,
				   const std::string& filter_prefix,
				   uint64_t max_return,
				   std::map<std::string, bufferlist> *out_vals)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_vals(start_after, filter_prefix, max_return, out_vals, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

void librados::ObjectReadOperation::getxattrs(map<string, bufferlist> *pattrs, int *prval)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->getxattrs(pattrs, prval);
}

void librados::ObjectWriteOperation::create(bool exclusive)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->create(exclusive);
}

void librados::ObjectWriteOperation::create(bool exclusive, const std::string& category)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->create(exclusive, category);
}

void librados::ObjectWriteOperation::write(uint64_t off, const bufferlist& bl)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  bufferlist c = bl;
  o->write(off, c);
}

void librados::ObjectWriteOperation::write_full(const bufferlist& bl)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  bufferlist c = bl;
  o->write_full(c);
}

void librados::ObjectWriteOperation::append(const bufferlist& bl)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  bufferlist c = bl;
  o->append(c);
}

void librados::ObjectWriteOperation::remove()
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->remove();
}

void librados::ObjectWriteOperation::truncate(uint64_t off)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->truncate(off);
}

void librados::ObjectWriteOperation::zero(uint64_t off, uint64_t len)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->zero(off, len);
}

void librados::ObjectWriteOperation::rmxattr(const char *name)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->rmxattr(name);
}

void librados::ObjectWriteOperation::setxattr(const char *name, const bufferlist& v)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->setxattr(name, v);
}

void librados::ObjectWriteOperation::omap_set(
  const map<string, bufferlist> &map)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_set(map);
}

void librados::ObjectWriteOperation::omap_set_header(const bufferlist &bl)
{
  bufferlist c = bl;
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_set_header(c);
}

void librados::ObjectWriteOperation::omap_clear()
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_clear();
}

void librados::ObjectWriteOperation::omap_rm_keys(
  const std::set<std::string> &to_rm)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->omap_rm_keys(to_rm);
}

void librados::ObjectWriteOperation::tmap_put(const bufferlist &bl)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  bufferlist c = bl;
  o->tmap_put(c);
}

void librados::ObjectWriteOperation::tmap_update(const bufferlist& cmdbl)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  bufferlist c = cmdbl;
  o->tmap_update(c);
}

void librados::ObjectWriteOperation::set_alloc_hint(
  uint64_t expected_object_size,
  uint64_t expected_write_size)
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  o->set_alloc_hint(expected_object_size, expected_write_size);
}

librados::WatchCtx::
~WatchCtx()
{
}

///////////////////////////// PoolAsyncCompletion //////////////////////////////
int librados::PoolAsyncCompletion::PoolAsyncCompletion::set_callback(void *cb_arg,
								     rados_callback_t cb)
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->set_callback(cb_arg, cb);
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::wait()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->wait();
}

bool librados::PoolAsyncCompletion::PoolAsyncCompletion::is_complete()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->is_complete();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::get_return_value()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->get_return_value();
}

void librados::PoolAsyncCompletion::PoolAsyncCompletion::release()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  c->release();
  delete this;
}

///////////////////////////// AioCompletion //////////////////////////////
int librados::AioCompletion::AioCompletion::set_complete_callback(void *cb_arg, rados_callback_t cb)
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->set_complete_callback(cb_arg, cb);
}

int librados::AioCompletion::AioCompletion::set_safe_callback(void *cb_arg, rados_callback_t cb)
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->set_safe_callback(cb_arg, cb);
}

int librados::AioCompletion::AioCompletion::wait_for_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete();
}

int librados::AioCompletion::AioCompletion::wait_for_safe()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_safe();
}

bool librados::AioCompletion::AioCompletion::is_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_complete();
}

bool librados::AioCompletion::AioCompletion::is_safe()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_safe();
}

int librados::AioCompletion::AioCompletion::wait_for_complete_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete_and_cb();
}

int librados::AioCompletion::AioCompletion::wait_for_safe_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_safe_and_cb();
}

bool librados::AioCompletion::AioCompletion::is_complete_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_complete_and_cb();
}

bool librados::AioCompletion::AioCompletion::is_safe_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_safe_and_cb();
}

int librados::AioCompletion::AioCompletion::get_return_value()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_return_value();
}

int librados::AioCompletion::AioCompletion::get_version()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_version();
}

uint64_t librados::AioCompletion::AioCompletion::get_version64()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_version();
}

void librados::AioCompletion::AioCompletion::release()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  c->release();
  delete this;
}

///////////////////////////// IoCtx //////////////////////////////
librados::IoCtx::IoCtx() : io_ctx_impl(NULL)
{
}

void librados::IoCtx::from_rados_ioctx_t(rados_ioctx_t p, IoCtx &io)
{
  IoCtxImpl *io_ctx_impl = (IoCtxImpl*)p;

  io.io_ctx_impl = io_ctx_impl;
  if (io_ctx_impl) {
    io_ctx_impl->get();
  }
}

librados::IoCtx::IoCtx(const IoCtx& rhs)
{
  io_ctx_impl = rhs.io_ctx_impl;
  if (io_ctx_impl) {
    io_ctx_impl->get();
  }
}

librados::IoCtx& librados::IoCtx::operator=(const IoCtx& rhs)
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = rhs.io_ctx_impl;
  io_ctx_impl->get();
  return *this;
}

librados::IoCtx::~IoCtx()
{
  close();
}

void librados::IoCtx::close()
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = 0;
}

void librados::IoCtx::dup(const IoCtx& rhs)
{
  if (io_ctx_impl)
    io_ctx_impl->put();
  io_ctx_impl = new IoCtxImpl();
  io_ctx_impl->get();
  io_ctx_impl->dup(*rhs.io_ctx_impl);
}

int librados::IoCtx::set_auid(uint64_t auid_)
{
  return io_ctx_impl->pool_change_auid(auid_);
}

int librados::IoCtx::set_auid_async(uint64_t auid_, PoolAsyncCompletion *c)
{
  return io_ctx_impl->pool_change_auid_async(auid_, c->pc);
}

int librados::IoCtx::get_auid(uint64_t *auid_)
{
  return rados_ioctx_pool_get_auid(io_ctx_impl, auid_);
}

std::string librados::IoCtx::get_pool_name()
{
  std::string s;
  io_ctx_impl->client->pool_get_name(get_id(), &s);
  return s;
}

int librados::IoCtx::create(const std::string& oid, bool exclusive)
{
  object_t obj(oid);
  return io_ctx_impl->create(obj, exclusive);
}

int librados::IoCtx::create(const std::string& oid, bool exclusive, const std::string& category)
{
  object_t obj(oid);
  return io_ctx_impl->create(obj, exclusive, category);
}

int librados::IoCtx::write(const std::string& oid, bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->write(obj, bl, len, off);
}

int librados::IoCtx::append(const std::string& oid, bufferlist& bl, size_t len)
{
  object_t obj(oid);
  return io_ctx_impl->append(obj, bl, len);
}

int librados::IoCtx::write_full(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->write_full(obj, bl);
}

int librados::IoCtx::read(const std::string& oid, bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->read(obj, bl, len, off);
}

int librados::IoCtx::remove(const std::string& oid)
{
  object_t obj(oid);
  return io_ctx_impl->remove(obj);
}

int librados::IoCtx::trunc(const std::string& oid, uint64_t size)
{
  object_t obj(oid);
  return io_ctx_impl->trunc(obj, size);
}

int librados::IoCtx::mapext(const std::string& oid, uint64_t off, size_t len,
			    std::map<uint64_t,uint64_t>& m)
{
  object_t obj(oid);
  return io_ctx_impl->mapext(oid, off, len, m);
}

int librados::IoCtx::sparse_read(const std::string& oid, std::map<uint64_t,uint64_t>& m,
				 bufferlist& bl, size_t len, uint64_t off)
{
  object_t obj(oid);
  return io_ctx_impl->sparse_read(oid, m, bl, len, off);
}

int librados::IoCtx::getxattr(const std::string& oid, const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->getxattr(obj, name, bl);
}

int librados::IoCtx::getxattrs(const std::string& oid, map<std::string, bufferlist>& attrset)
{
  object_t obj(oid);
  return io_ctx_impl->getxattrs(obj, attrset);
}

int librados::IoCtx::setxattr(const std::string& oid, const char *name, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->setxattr(obj, name, bl);
}

int librados::IoCtx::rmxattr(const std::string& oid, const char *name)
{
  object_t obj(oid);
  return io_ctx_impl->rmxattr(obj, name);
}

int librados::IoCtx::stat(const std::string& oid, uint64_t *psize, time_t *pmtime)
{
  object_t obj(oid);
  return io_ctx_impl->stat(oid, psize, pmtime);
}

int librados::IoCtx::exec(const std::string& oid, const char *cls, const char *method,
			  bufferlist& inbl, bufferlist& outbl)
{
  object_t obj(oid);
  return io_ctx_impl->exec(obj, cls, method, inbl, outbl);
}

int librados::IoCtx::tmap_update(const std::string& oid, bufferlist& cmdbl)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_update(obj, cmdbl);
}

int librados::IoCtx::tmap_put(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_put(obj, bl);
}

int librados::IoCtx::tmap_get(const std::string& oid, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_get(obj, bl);
}

int librados::IoCtx::tmap_to_omap(const std::string& oid, bool nullok)
{
  object_t obj(oid);
  return io_ctx_impl->tmap_to_omap(obj, nullok);
}

int librados::IoCtx::omap_get_vals(const std::string& oid,
                                   const std::string& start_after,
                                   uint64_t max_return,
                                   std::map<std::string, bufferlist> *out_vals)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_vals(start_after, max_return, out_vals, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_get_keys(const std::string& oid,
                                   const std::string& start_after,
                                   uint64_t max_return,
                                   std::set<std::string> *out_keys)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_keys(start_after, max_return, out_keys, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_get_header(const std::string& oid,
                                     bufferlist *bl)
{
  ObjectReadOperation op;
  int r;
  op.omap_get_header(bl, &r);
  bufferlist b;
  int ret = operate(oid, &op, &b);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_get_vals_by_keys(const std::string& oid,
                                           const std::set<std::string>& keys,
                                           std::map<std::string, bufferlist> *vals)
{
  ObjectReadOperation op;
  int r;
  bufferlist bl;
  op.omap_get_vals_by_keys(keys, vals, &r);
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

int librados::IoCtx::omap_set(const std::string& oid,
                              const map<string, bufferlist>& m)
{
  ObjectWriteOperation op;
  op.omap_set(m);
  return operate(oid, &op);
}

int librados::IoCtx::omap_set_header(const std::string& oid,
                                     const bufferlist& bl)
{
  ObjectWriteOperation op;
  op.omap_set_header(bl);
  return operate(oid, &op);
}

int librados::IoCtx::omap_clear(const std::string& oid)
{
  ObjectWriteOperation op;
  op.omap_clear();
  return operate(oid, &op);
}

int librados::IoCtx::omap_rm_keys(const std::string& oid,
                                  const std::set<std::string>& keys)
{
  ObjectWriteOperation op;
  op.omap_rm_keys(keys);
  return operate(oid, &op);
}



static int translate_flags(int flags)
{
  int op_flags = 0;
  if (flags & librados::OPERATION_ORDER_READS_WRITES)
    op_flags |= CEPH_OSD_FLAG_RWORDERED;
  if (flags & librados::OPERATION_SKIPRWLOCKS)
    op_flags |= CEPH_OSD_FLAG_SKIPRWLOCKS;

  return op_flags;
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectWriteOperation *o)
{
  object_t obj(oid);
  return io_ctx_impl->operate(obj, (::ObjectOperation*)o->impl, o->pmtime);
}

int librados::IoCtx::operate(const std::string& oid, librados::ObjectReadOperation *o, bufferlist *pbl)
{
  object_t obj(oid);
  return io_ctx_impl->operate_read(obj, (::ObjectOperation*)o->impl, pbl);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectWriteOperation *o)
{
  object_t obj(oid);
  return io_ctx_impl->aio_operate(obj, (::ObjectOperation*)o->impl, c->pc, 0);
}
int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 ObjectWriteOperation *o, int flags)
{
  object_t obj(oid);
  return io_ctx_impl->aio_operate(obj, (::ObjectOperation*)o->impl, c->pc,
				  translate_flags(flags));
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectReadOperation *o,
				 bufferlist *pbl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_operate_read(obj, (::ObjectOperation*)o->impl, c->pc,
				       0, pbl);
}

int librados::IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
				 librados::ObjectReadOperation *o,
				 int flags, bufferlist *pbl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_operate_read(obj, (::ObjectOperation*)o->impl, c->pc,
				       translate_flags(flags), pbl);
}


int librados::IoCtx::lock_exclusive(const std::string &oid, const std::string &name,
				    const std::string &cookie,
				    const std::string &description,
				    struct timeval * duration, uint8_t flags)
{
  utime_t dur = utime_t();
  if (duration)
    dur.set_from_timeval(duration);

  return rados::cls::lock::lock(this, oid, name, LOCK_EXCLUSIVE, cookie, "",
				description, dur, flags);
}

int librados::IoCtx::lock_shared(const std::string &oid, const std::string &name,
				 const std::string &cookie, const std::string &tag,
				 const std::string &description,
				 struct timeval * duration, uint8_t flags)
{
  utime_t dur = utime_t();
  if (duration)
    dur.set_from_timeval(duration);

  return rados::cls::lock::lock(this, oid, name, LOCK_SHARED, cookie, tag,
		  		description, dur, flags);
}

int librados::IoCtx::unlock(const std::string &oid, const std::string &name,
			    const std::string &cookie)
{
  return rados::cls::lock::unlock(this, oid, name, cookie);
}

int librados::IoCtx::break_lock(const std::string &oid, const std::string &name,
				const std::string &client, const std::string &cookie)
{
  entity_name_t locker;
  if (!locker.parse(client))
    return -EINVAL;
  return rados::cls::lock::break_lock(this, oid, name, cookie, locker);
}

int librados::IoCtx::list_lockers(const std::string &oid, const std::string &name,
				  int *exclusive,
				  std::string *tag,
				  std::list<librados::locker_t> *lockers)
{
  std::list<librados::locker_t> tmp_lockers;
  map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> rados_lockers;
  std::string tmp_tag;
  ClsLockType tmp_type;
  int r = rados::cls::lock::get_lock_info(this, oid, name, &rados_lockers, &tmp_type, &tmp_tag);
  if (r < 0)
	  return r;

  map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t>::iterator map_it;
  for (map_it = rados_lockers.begin(); map_it != rados_lockers.end(); ++map_it) {
    librados::locker_t locker;
    locker.client = stringify(map_it->first.locker);
    locker.cookie = map_it->first.cookie;
    locker.address = stringify(map_it->second.addr);
    tmp_lockers.push_back(locker);
  }

  if (lockers)
    *lockers = tmp_lockers;
  if (tag)
    *tag = tmp_tag;
  if (exclusive) {
    if (tmp_type == LOCK_EXCLUSIVE)
      *exclusive = 1;
    else
      *exclusive = 0;
  }

  return tmp_lockers.size();
}

uint64_t librados::IoCtx::get_last_version()
{
  return io_ctx_impl->last_version();
}

int librados::IoCtx::aio_read(const std::string& oid, librados::AioCompletion *c,
			      bufferlist *pbl, size_t len, uint64_t off)
{
  return io_ctx_impl->aio_read(oid, c->pc, pbl, len, off);
}

int librados::IoCtx::aio_exec(const std::string& oid,
			      librados::AioCompletion *c, const char *cls,
			      const char *method, bufferlist& inbl,
			      bufferlist *outbl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_exec(obj, c->pc, cls, method, inbl, outbl);
}

int librados::IoCtx::aio_sparse_read(const std::string& oid, librados::AioCompletion *c,
				     std::map<uint64_t,uint64_t> *m, bufferlist *data_bl,
				     size_t len, uint64_t off)
{
  return io_ctx_impl->aio_sparse_read(oid, c->pc,
				      m, data_bl, len, off);
}

int librados::IoCtx::aio_write(const std::string& oid, librados::AioCompletion *c,
			       const bufferlist& bl, size_t len, uint64_t off)
{
  return io_ctx_impl->aio_write(oid, c->pc, bl, len, off);
}

int librados::IoCtx::aio_append(const std::string& oid, librados::AioCompletion *c,
				const bufferlist& bl, size_t len)
{
  return io_ctx_impl->aio_append(oid, c->pc, bl, len);
}

int librados::IoCtx::aio_write_full(const std::string& oid, librados::AioCompletion *c,
				    const bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->aio_write_full(obj, c->pc, bl);
}

int librados::IoCtx::aio_remove(const std::string& oid, librados::AioCompletion *c)
{
  return io_ctx_impl->aio_remove(oid, c->pc);
}

int librados::IoCtx::aio_flush_async(librados::AioCompletion *c)
{
  io_ctx_impl->flush_aio_writes_async(c->pc);
  return 0;
}

int librados::IoCtx::aio_flush()
{
  io_ctx_impl->flush_aio_writes();
  return 0;
}

int librados::IoCtx::aio_stat(const std::string& oid, librados::AioCompletion *c,
			      uint64_t *psize, time_t *pmtime)
{
  object_t obj(oid);
  return io_ctx_impl->aio_stat(oid, c->pc, psize, pmtime);
}


int librados::IoCtx::watch(const string& oid, uint64_t ver, uint64_t *cookie,
			   librados::WatchCtx *ctx)
{
  object_t obj(oid);
  return io_ctx_impl->watch(obj, ver, cookie, ctx);
}

int librados::IoCtx::unwatch(const string& oid, uint64_t handle)
{
  uint64_t cookie = handle;
  object_t obj(oid);
  return io_ctx_impl->unwatch(obj, cookie);
}

int librados::IoCtx::notify(const string& oid, uint64_t ver, bufferlist& bl)
{
  object_t obj(oid);
  return io_ctx_impl->notify(obj, ver, bl);
}

int librados::IoCtx::list_watchers(const std::string& oid,
                                   std::list<obj_watch_t> *out_watchers)
{
  ObjectReadOperation op;
  int r;
  op.list_watchers(out_watchers, &r);
  bufferlist bl;
  int ret = operate(oid, &op, &bl);
  if (ret < 0)
    return ret;

  return r;
}

void librados::IoCtx::set_notify_timeout(uint32_t timeout)
{
  io_ctx_impl->set_notify_timeout(timeout);
}

int librados::IoCtx::set_alloc_hint(const std::string& o,
                                    uint64_t expected_object_size,
                                    uint64_t expected_write_size)
{
  object_t oid(o);
  return io_ctx_impl->set_alloc_hint(oid, expected_object_size,
                                     expected_write_size);
}

void librados::IoCtx::set_assert_version(uint64_t ver)
{
  io_ctx_impl->set_assert_version(ver);
}

void librados::IoCtx::set_assert_src_version(const std::string& oid, uint64_t ver)
{
  object_t obj(oid);
  io_ctx_impl->set_assert_src_version(obj, ver);
}

const std::string& librados::IoCtx::get_pool_name() const
{
  return io_ctx_impl->pool_name;
}

void librados::IoCtx::locator_set_key(const string& key)
{
  io_ctx_impl->oloc.key = key;
}

void librados::IoCtx::set_namespace(const string& nspace)
{
  io_ctx_impl->oloc.nspace = nspace;
}

int64_t librados::IoCtx::get_id()
{
  return io_ctx_impl->get_id();
}

uint32_t librados::IoCtx::get_object_hash_position(const std::string& oid)
{
  return io_ctx_impl->get_object_hash_position(oid);
}

uint32_t librados::IoCtx::get_object_pg_hash_position(const std::string& oid)
{
  return io_ctx_impl->get_object_pg_hash_position(oid);
}

librados::config_t librados::IoCtx::cct()
{
  return (config_t)io_ctx_impl->client->cct;
}

librados::IoCtx::IoCtx(IoCtxImpl *io_ctx_impl_)
  : io_ctx_impl(io_ctx_impl_)
{
}

///////////////////////////// Rados //////////////////////////////
void librados::Rados::version(int *major, int *minor, int *extra)
{
  rados_version(major, minor, extra);
}

librados::Rados::Rados() : client(NULL)
{
}

librados::Rados::Rados(IoCtx &ioctx)
{
  client = ioctx.io_ctx_impl->client;
  assert(client != NULL);
  client->get();
}

librados::Rados::~Rados()
{
  shutdown();
}

int librados::Rados::init(const char * const id)
{
  return rados_create((rados_t *)&client, id);
}

int librados::Rados::init2(const char * const name,
			   const char * const clustername, uint64_t flags)
{
  return rados_create2((rados_t *)&client, clustername, name, flags);
}

int librados::Rados::init_with_context(config_t cct_)
{
  return rados_create_with_context((rados_t *)&client, (rados_config_t)cct_);
}

int librados::Rados::connect()
{
  return client->connect();
}

librados::config_t librados::Rados::cct()
{
  return (config_t)client->cct;
}

void librados::Rados::shutdown()
{
  if (!client)
    return;
  if (client->put()) {
    client->shutdown();
    delete client;
    client = NULL;
  }
}

uint64_t librados::Rados::get_instance_id()
{
  return client->get_instance_id();
}

int librados::Rados::conf_read_file(const char * const path) const
{
  return rados_conf_read_file((rados_t)client, path);
}

int librados::Rados::conf_parse_argv(int argc, const char ** argv) const
{
  return rados_conf_parse_argv((rados_t)client, argc, argv);
}

int librados::Rados::conf_parse_argv_remainder(int argc, const char ** argv,
					       const char ** remargv) const
{
  return rados_conf_parse_argv_remainder((rados_t)client, argc, argv, remargv);
}

int librados::Rados::conf_parse_env(const char *name) const
{
  return rados_conf_parse_env((rados_t)client, name);
}

int librados::Rados::conf_set(const char *option, const char *value)
{
  return rados_conf_set((rados_t)client, option, value);
}

int librados::Rados::conf_get(const char *option, std::string &val)
{
  char *str = NULL;
  md_config_t *conf = client->cct->_conf;
  int ret = conf->get_val(option, &str, -1);
  if (ret) {
    free(str);
    return ret;
  }
  val = str;
  free(str);
  return 0;
}

int librados::Rados::pool_create(const char *name)
{
  string str(name);
  return client->pool_create(str);
}

int librados::Rados::pool_create(const char *name, uint64_t auid)
{
  string str(name);
  return client->pool_create(str, auid);
}

int librados::Rados::pool_create(const char *name, uint64_t auid, uint8_t crush_rule)
{
  string str(name);
  return client->pool_create(str, auid, crush_rule);
}

int librados::Rados::pool_create_async(const char *name, PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc);
}

int librados::Rados::pool_create_async(const char *name, uint64_t auid, PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc, auid);
}

int librados::Rados::pool_create_async(const char *name, uint64_t auid, uint8_t crush_rule,
				       PoolAsyncCompletion *c)
{
  string str(name);
  return client->pool_create_async(str, c->pc, auid, crush_rule);
}

int librados::Rados::pool_delete(const char *name)
{
  return client->pool_delete(name);
}

int librados::Rados::pool_delete_async(const char *name, PoolAsyncCompletion *c)
{
  return client->pool_delete_async(name, c->pc);
}

int librados::Rados::pool_list(std::list<std::string>& v)
{
  return client->pool_list(v);
}

int64_t librados::Rados::pool_lookup(const char *name)
{
  return client->lookup_pool(name);
}

int librados::Rados::pool_reverse_lookup(int64_t id, std::string *name)
{
  return client->pool_get_name(id, name);
}

int librados::Rados::mon_command(string cmd, const bufferlist& inbl,
				 bufferlist *outbl, string *outs)
{
  vector<string> cmdvec;
  cmdvec.push_back(cmd);
  return client->mon_command(cmdvec, inbl, outbl, outs);
}

int librados::Rados::ioctx_create(const char *name, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_create((rados_t)client, name, &p);
  if (ret)
    return ret;
  io.io_ctx_impl = (IoCtxImpl*)p;
  return 0;
}

void librados::Rados::test_blacklist_self(bool set)
{
  client->blacklist_self(set);
}

int librados::Rados::get_pool_stats(std::list<string>& v, std::map<string, stats_map>& result)
{
  string category;
  return get_pool_stats(v, category, result);
}

int librados::Rados::get_pool_stats(std::list<string>& v, string& category,
				    std::map<string, stats_map>& result)
{
  map<string,::pool_stat_t> rawresult;
  int r = client->get_pool_stats(v, rawresult);
  if (r < 0)
    return r;
  for (map<string,::pool_stat_t>::iterator p = rawresult.begin();
       p != rawresult.end();
       ++p) {
    stats_map& c = result[p->first];

    string cat;
    vector<string> cats;

    if (!category.size()) {
      cats.push_back(cat);
      map<string,object_stat_sum_t>::iterator iter;
      for (iter = p->second.stats.cat_sum.begin(); iter != p->second.stats.cat_sum.end(); ++iter) {
	cats.push_back(iter->first);
      }
    } else {
      cats.push_back(category);
    }

    vector<string>::iterator cat_iter;
    for (cat_iter = cats.begin(); cat_iter != cats.end(); ++cat_iter) {
      string& cur_category = *cat_iter;
      object_stat_sum_t *sum;

      if (!cur_category.size()) {
	sum = &p->second.stats.sum;
      } else {
	map<string,object_stat_sum_t>::iterator iter = p->second.stats.cat_sum.find(cur_category);
	if (iter == p->second.stats.cat_sum.end())
	  continue;
	sum = &iter->second;
      }

      pool_stat_t& pv = c[cur_category];
      pv.num_kb = SHIFT_ROUND_UP(sum->num_bytes, 10);
      pv.num_bytes = sum->num_bytes;
      pv.num_objects = sum->num_objects;
      pv.num_rd = sum->num_rd;
      pv.num_rd_kb = sum->num_rd_kb;
      pv.num_wr = sum->num_wr;
      pv.num_wr_kb = sum->num_wr_kb;
    }
  }
  return r;
}

int librados::Rados::cluster_stat(cluster_stat_t& result)
{
  ceph_statfs stats;
  int r = client->get_fs_stats(stats);
  result.kb = stats.kb;
  result.kb_used = stats.kb_used;
  result.kb_avail = stats.kb_avail;
  result.num_objects = stats.num_objects;
  return r;
}

int librados::Rados::cluster_fsid(string *fsid)
{
  return client->get_fsid(fsid);
}

int librados::Rados::wait_for_latest_osdmap()
{
  return client->wait_for_latest_osdmap();
}

librados::PoolAsyncCompletion *librados::Rados::pool_async_create_completion()
{
  PoolAsyncCompletionImpl *c = new PoolAsyncCompletionImpl;
  return new PoolAsyncCompletion(c);
}

librados::AioCompletion *librados::Rados::aio_create_completion()
{
  AioCompletionImpl *c = new AioCompletionImpl;
  return new AioCompletion(c);
}

librados::AioCompletion *librados::Rados::aio_create_completion(void *cb_arg,
								callback_t cb_complete,
								callback_t cb_safe)
{
  AioCompletionImpl *c;
  int r = rados_aio_create_completion(cb_arg, cb_complete, cb_safe, (void**)&c);
  assert(r == 0);
  return new AioCompletion(c);
}

librados::ObjectOperation::ObjectOperation()
{
  impl = (ObjectOperationImpl *)new ::ObjectOperation;
}

librados::ObjectOperation::~ObjectOperation()
{
  ::ObjectOperation *o = (::ObjectOperation *)impl;
  delete o;
}

///////////////////////////// C API //////////////////////////////
static
int rados_create_common(rados_t *pcluster,
			const char * const clustername,
			CephInitParameters *iparams)
{
  // missing things compared to global_init:
  // g_ceph_context, g_conf, g_lockdep, signal handlers
  CephContext *cct = common_preinit(*iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  if (clustername)
    cct->_conf->cluster = clustername;
  cct->_conf->parse_env(); // environment variables override
  cct->_conf->apply_changes(NULL);

  librados::RadosClient *radosp = new librados::RadosClient(cct);
  *pcluster = (void *)radosp;

  cct->put();
  return 0;
}

extern "C" int rados_create(rados_t *pcluster, const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }
  return rados_create_common(pcluster, "ceph", &iparams);
}

// as above, but 
// 1) don't assume 'client.'; name is a full type.id namestr
// 2) allow setting clustername
// 3) flags is for future expansion (maybe some of the global_init()
//    behavior is appropriate for some consumers of librados, for instance)

extern "C" int rados_create2(rados_t *pcluster, const char *const clustername,
			     const char * const name, uint64_t flags)
{
  // client is assumed, but from_str will override
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (!name || !iparams.name.from_str(name)) 
    return -EINVAL;

  return rados_create_common(pcluster, clustername, &iparams);
}

/* This function is intended for use by Ceph daemons. These daemons have
 * already called global_init and want to use that particular configuration for
 * their cluster.
 */
extern "C" int rados_create_with_context(rados_t *pcluster, rados_config_t cct_)
{
  CephContext *cct = (CephContext *)cct_;
  librados::RadosClient *radosp = new librados::RadosClient(cct);
  *pcluster = (void *)radosp;
  return 0;
}

extern "C" rados_config_t rados_cct(rados_t cluster)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return (rados_config_t)client->cct;
}

extern "C" int rados_connect(rados_t cluster)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return client->connect();
}

extern "C" void rados_shutdown(rados_t cluster)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  radosp->shutdown();
  delete radosp;
}

extern "C" uint64_t rados_get_instance_id(rados_t cluster)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return client->get_instance_id();
}

extern "C" void rados_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRADOS_VER_MAJOR;
  if (minor)
    *minor = LIBRADOS_VER_MINOR;
  if (extra)
    *extra = LIBRADOS_VER_EXTRA;
}


// -- config --
extern "C" int rados_conf_read_file(rados_t cluster, const char *path_list)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  std::deque<std::string> parse_errors;
  int ret = conf->parse_config_files(path_list, &parse_errors, NULL, 0);
  if (ret)
    return ret;
  conf->parse_env(); // environment variables override

  conf->apply_changes(NULL);
  complain_about_parse_errors(client->cct, &parse_errors);
  return 0;
}

extern "C" int rados_conf_parse_argv(rados_t cluster, int argc, const char **argv)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  int ret = conf->parse_argv(args);
  if (ret)
    return ret;
  conf->apply_changes(NULL);
  return 0;
}

// like above, but return the remainder of argv to contain remaining
// unparsed args.  Must be allocated to at least argc by caller.
// remargv will contain n <= argc pointers to original argv[], the end
// of which may be NULL

extern "C" int rados_conf_parse_argv_remainder(rados_t cluster, int argc,
					       const char **argv,
					       const char **remargv)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  vector<const char*> args;
  for (int i=0; i<argc; i++)
    args.push_back(argv[i]);
  int ret = conf->parse_argv(args);
  if (ret)
    return ret;
  conf->apply_changes(NULL);
  assert(args.size() <= (unsigned int)argc);
  unsigned int i;
  for (i = 0; i < (unsigned int)argc; ++i) {
    if (i < args.size())
      remargv[i] = args[i];
    else
      remargv[i] = (const char *)NULL;
  }
  return 0;
}

extern "C" int rados_conf_parse_env(rados_t cluster, const char *env)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  vector<const char*> args;
  env_to_vec(args, env);
  int ret = conf->parse_argv(args);
  if (ret)
    return ret;
  conf->apply_changes(NULL);
  return 0;
}

extern "C" int rados_conf_set(rados_t cluster, const char *option, const char *value)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  int ret = conf->set_val(option, value);
  if (ret)
    return ret;
  conf->apply_changes(NULL);
  return 0;
}

/* cluster info */
extern "C" int rados_cluster_stat(rados_t cluster, rados_cluster_stat_t *result)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;

  ceph_statfs stats;
  int r = client->get_fs_stats(stats);
  result->kb = stats.kb;
  result->kb_used = stats.kb_used;
  result->kb_avail = stats.kb_avail;
  result->num_objects = stats.num_objects;
  return r;
}

extern "C" int rados_conf_get(rados_t cluster, const char *option, char *buf, size_t len)
{
  char *tmp = buf;
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  md_config_t *conf = client->cct->_conf;
  return conf->get_val(option, &tmp, len);
}

extern "C" int64_t rados_pool_lookup(rados_t cluster, const char *name)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  return radosp->lookup_pool(name);
}

extern "C" int rados_pool_reverse_lookup(rados_t cluster, int64_t id,
					 char *buf, size_t maxlen)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  std::string name;
  int r = radosp->pool_get_name(id, &name);
  if (r < 0)
    return r;
  if (name.length() >= maxlen)
    return -ERANGE;
  strcpy(buf, name.c_str());
  return name.length();
}

extern "C" int rados_cluster_fsid(rados_t cluster, char *buf,
				  size_t maxlen)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  std::string fsid;
  radosp->get_fsid(&fsid);
  if (fsid.length() >= maxlen)
    return -ERANGE;
  strcpy(buf, fsid.c_str());
  return fsid.length();
}

extern "C" int rados_wait_for_latest_osdmap(rados_t cluster)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  return radosp->wait_for_latest_osdmap();
}

extern "C" int rados_pool_list(rados_t cluster, char *buf, size_t len)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  std::list<std::string> pools;
  int r = client->pool_list(pools);
  if (r < 0)
    return r;

  if (len > 0 && !buf)
    return -EINVAL;

  char *b = buf;
  if (b)
    memset(b, 0, len);
  int needed = 0;
  std::list<std::string>::const_iterator i = pools.begin();
  std::list<std::string>::const_iterator p_end = pools.end();
  for (; i != p_end; ++i) {
    if (len == 0)
      break;
    int rl = i->length() + 1;
    strncat(b, i->c_str(), len - 2); // leave space for two NULLs
    needed += rl;
    len -= rl;
    b += rl;
  }
  for (; i != p_end; ++i) {
    int rl = i->length() + 1;
    needed += rl;
  }
  return needed + 1;
}

static void do_out_buffer(bufferlist& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

static void do_out_buffer(string& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

extern "C" int rados_ping_monitor(rados_t cluster, const char *mon_id,
                                  char **outstr, size_t *outstrlen)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  string str;

  if (!mon_id)
    return -EINVAL;

  int ret = client->ping_monitor(mon_id, &str);
  if (ret == 0 && !str.empty() && outstr && outstrlen) {
    do_out_buffer(str, outstr, outstrlen);
  }
  return ret;
}

extern "C" int rados_mon_command(rados_t cluster, const char **cmd,
				 size_t cmdlen,
				 const char *inbuf, size_t inbuflen,
				 char **outbuf, size_t *outbuflen,
				 char **outs, size_t *outslen)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++)
    cmdvec.push_back(cmd[i]);

  inbl.append(inbuf, inbuflen);
  int ret = client->mon_command(cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  return ret;
}

extern "C" int rados_mon_command_target(rados_t cluster, const char *name,
					const char **cmd,
					size_t cmdlen,
					const char *inbuf, size_t inbuflen,
					char **outbuf, size_t *outbuflen,
					char **outs, size_t *outslen)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  // is this a numeric id?
  char *endptr;
  errno = 0;
  long rank = strtol(name, &endptr, 10);
  if ((errno == ERANGE && (rank == LONG_MAX || rank == LONG_MIN)) ||
      (errno != 0 && rank == 0) ||
      endptr == name ||    // no digits
      *endptr != '\0') {   // extra characters
    rank = -1;
  }

  for (size_t i = 0; i < cmdlen; i++)
    cmdvec.push_back(cmd[i]);

  inbl.append(inbuf, inbuflen);
  int ret;
  if (rank >= 0)
    ret = client->mon_command(rank, cmdvec, inbl, &outbl, &outstring);
  else
    ret = client->mon_command(name, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  return ret;
}

extern "C" int rados_osd_command(rados_t cluster, int osdid, const char **cmd,
				 size_t cmdlen,
				 const char *inbuf, size_t inbuflen,
				 char **outbuf, size_t *outbuflen,
				 char **outs, size_t *outslen)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++)
    cmdvec.push_back(cmd[i]);

  inbl.append(inbuf, inbuflen);
  int ret = client->osd_command(osdid, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  return ret;
}



extern "C" int rados_pg_command(rados_t cluster, const char *pgstr,
				const char **cmd, size_t cmdlen,
				const char *inbuf, size_t inbuflen,
				char **outbuf, size_t *outbuflen,
				char **outs, size_t *outslen)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  pg_t pgid;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++)
    cmdvec.push_back(cmd[i]);

  inbl.append(inbuf, inbuflen);
  if (!pgid.parse(pgstr))
    return -EINVAL;

  int ret = client->pg_command(pgid, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  return ret;
}

extern "C" void rados_buffer_free(char *buf)
{
  if (buf)
    free(buf);
}

extern "C" int rados_monitor_log(rados_t cluster, const char *level, rados_log_callback_t cb, void *arg)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return client->monitor_log(level, cb, arg);
}


extern "C" int rados_ioctx_create(rados_t cluster, const char *name, rados_ioctx_t *io)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::IoCtxImpl *ctx;

  int r = client->create_ioctx(name, &ctx);
  if (r < 0)
    return r;

  *io = ctx;
  ctx->get();
  return 0;
}

extern "C" void rados_ioctx_destroy(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->put();
}

extern "C" int rados_ioctx_pool_stat(rados_ioctx_t io, struct rados_pool_stat_t *stats)
{
  librados::IoCtxImpl *io_ctx_impl = (librados::IoCtxImpl *)io;
  list<string> ls;
  ls.push_back(io_ctx_impl->pool_name);
  map<string, ::pool_stat_t> rawresult;

  int err = io_ctx_impl->client->get_pool_stats(ls, rawresult);
  if (err)
    return err;

  ::pool_stat_t& r = rawresult[io_ctx_impl->pool_name];
  stats->num_kb = SHIFT_ROUND_UP(r.stats.sum.num_bytes, 10);
  stats->num_bytes = r.stats.sum.num_bytes;
  stats->num_objects = r.stats.sum.num_objects;
  stats->num_rd = r.stats.sum.num_rd;
  stats->num_rd_kb = r.stats.sum.num_rd_kb;
  stats->num_wr = r.stats.sum.num_wr;
  stats->num_wr_kb = r.stats.sum.num_wr_kb;
  return 0;
}

extern "C" rados_config_t rados_ioctx_cct(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return (rados_config_t)ctx->client->cct;
}

extern "C" int rados_write(rados_ioctx_t io, const char *o, const char *buf, size_t len, uint64_t off)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->write(oid, bl, len, off);
}

extern "C" int rados_append(rados_ioctx_t io, const char *o, const char *buf, size_t len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->append(oid, bl, len);
}

extern "C" int rados_write_full(rados_ioctx_t io, const char *o, const char *buf, size_t len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->write_full(oid, bl);
}

extern "C" int rados_trunc(rados_ioctx_t io, const char *o, uint64_t size)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->trunc(oid, size);
}

extern "C" int rados_remove(rados_ioctx_t io, const char *o)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->remove(oid);
}

extern "C" int rados_read(rados_ioctx_t io, const char *o, char *buf, size_t len, uint64_t off)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);

  bufferlist bl;
  bufferptr bp = buffer::create_static(len, buf);
  bl.push_back(bp);

  ret = ctx->read(oid, bl, len, off);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    if (bl.c_str() != buf)
      bl.copy(0, bl.length(), buf);
    ret = bl.length();    // hrm :/
  }

  return ret;
}

extern "C" uint64_t rados_get_last_version(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->last_version();
}

extern "C" int rados_pool_create(rados_t cluster, const char *name)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname);
}

extern "C" int rados_pool_create_with_auid(rados_t cluster, const char *name,
					   uint64_t auid)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname, auid);
}

extern "C" int rados_pool_create_with_crush_rule(rados_t cluster, const char *name,
						 uint8_t crush_rule_num)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname, 0, crush_rule_num);
}

extern "C" int rados_pool_create_with_all(rados_t cluster, const char *name,
					  uint64_t auid, uint8_t crush_rule_num)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  return radosp->pool_create(sname, auid, crush_rule_num);
}

extern "C" int rados_pool_delete(rados_t cluster, const char *pool_name)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return client->pool_delete(pool_name);
}

extern "C" int rados_ioctx_pool_set_auid(rados_ioctx_t io, uint64_t auid)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->pool_change_auid(auid);
}

extern "C" int rados_ioctx_pool_get_auid(rados_ioctx_t io, uint64_t *auid)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->client->pool_get_auid(ctx->get_id(), (unsigned long long *)auid);
}

extern "C" void rados_ioctx_locator_set_key(rados_ioctx_t io, const char *key)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (key)
    ctx->oloc.key = key;
  else
    ctx->oloc.key = "";
}

extern "C" void rados_ioctx_set_namespace(rados_ioctx_t io, const char *nspace)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (nspace)
    ctx->oloc.nspace = nspace;
  else
    ctx->oloc.nspace = "";
}

extern "C" rados_t rados_ioctx_get_cluster(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return (rados_t)ctx->client;
}

extern "C" int64_t rados_ioctx_get_id(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->get_id();
}

extern "C" int rados_ioctx_get_pool_name(rados_ioctx_t io, char *s, unsigned maxlen)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (ctx->pool_name.length() >= maxlen)
    return -ERANGE;
  strcpy(s, ctx->pool_name.c_str());
  return ctx->pool_name.length();
}

extern "C" int rados_getxattr(rados_ioctx_t io, const char *o, const char *name,
			      char *buf, size_t len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);
  bufferlist bl;
  ret = ctx->getxattr(oid, name, bl);
  if (ret >= 0) {
    if (bl.length() > len)
      return -ERANGE;
    bl.copy(0, bl.length(), buf);
    ret = bl.length();
  }

  return ret;
}

class RadosXattrsIter {
public:
  RadosXattrsIter()
    : val(NULL)
  {
    i = attrset.end();
  }
  ~RadosXattrsIter()
  {
    free(val);
    val = NULL;
  }
  std::map<std::string, bufferlist> attrset;
  std::map<std::string, bufferlist>::iterator i;
  char *val;
};

extern "C" int rados_getxattrs(rados_ioctx_t io, const char *oid,
			       rados_xattrs_iter_t *iter)
{
  RadosXattrsIter *it = new RadosXattrsIter();
  if (!it)
    return -ENOMEM;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t obj(oid);
  int ret = ctx->getxattrs(obj, it->attrset);
  if (ret) {
    delete it;
    return ret;
  }
  it->i = it->attrset.begin();

  RadosXattrsIter **iret = (RadosXattrsIter**)iter;
  *iret = it;
  *iter = it;
  return 0;
}

extern "C" int rados_getxattrs_next(rados_xattrs_iter_t iter,
				    const char **name, const char **val, size_t *len)
{
  RadosXattrsIter *it = static_cast<RadosXattrsIter*>(iter);
  if (it->i == it->attrset.end()) {
    *name = NULL;
    *val = NULL;
    *len = 0;
    return 0;
  }
  free(it->val);
  const std::string &s(it->i->first);
  *name = s.c_str();
  bufferlist &bl(it->i->second);
  size_t bl_len = bl.length();
  it->val = (char*)malloc(bl_len);
  if (!it->val)
    return -ENOMEM;
  memcpy(it->val, bl.c_str(), bl_len);
  *val = it->val;
  *len = bl_len;
  ++it->i;
  return 0;
}

extern "C" void rados_getxattrs_end(rados_xattrs_iter_t iter)
{
  RadosXattrsIter *it = static_cast<RadosXattrsIter*>(iter);
  delete it;
}

extern "C" int rados_setxattr(rados_ioctx_t io, const char *o, const char *name, const char *buf, size_t len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->setxattr(oid, name, bl);
}

extern "C" int rados_rmxattr(rados_ioctx_t io, const char *o, const char *name)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->rmxattr(oid, name);
}

extern "C" int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->stat(oid, psize, pmtime);
}

extern "C" int rados_tmap_update(rados_ioctx_t io, const char *o, const char *cmdbuf, size_t cmdbuflen)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist cmdbl;
  cmdbl.append(cmdbuf, cmdbuflen);
  return ctx->tmap_update(oid, cmdbl);
}

extern "C" int rados_tmap_put(rados_ioctx_t io, const char *o, const char *buf, size_t buflen)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, buflen);
  return ctx->tmap_put(oid, bl);
}

extern "C" int rados_tmap_get(rados_ioctx_t io, const char *o, char *buf, size_t buflen)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  int r = ctx->tmap_get(oid, bl);
  if (r < 0)
    return r;
  if (bl.length() > buflen)
    return -ERANGE;
  bl.copy(0, bl.length(), buf);
  return bl.length();
}

extern "C" int rados_tmap_to_omap(rados_ioctx_t io, const char *o, bool nullok)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->tmap_to_omap(oid, nullok);
}

extern "C" int rados_exec(rados_ioctx_t io, const char *o, const char *cls, const char *method,
                         const char *inbuf, size_t in_len, char *buf, size_t out_len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist inbl, outbl;
  int ret;
  inbl.append(inbuf, in_len);
  ret = ctx->exec(oid, cls, method, inbl, outbl);
  if (ret >= 0) {
    if (outbl.length()) {
      if (outbl.length() > out_len)
	return -ERANGE;
      outbl.copy(0, outbl.length(), buf);
      ret = outbl.length();   // hrm :/
    }
  }
  return ret;
}

/* list objects */

// -------------------------
// aio

extern "C" int rados_aio_create_completion(void *cb_arg,
					   rados_callback_t cb_complete,
					   rados_callback_t cb_safe,
					   rados_completion_t *pc)
{
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete)
    c->set_complete_callback(cb_arg, cb_complete);
  if (cb_safe)
    c->set_safe_callback(cb_arg, cb_safe);
  *pc = c;
  return 0;
}

extern "C" int rados_aio_wait_for_complete(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->wait_for_complete();
}

extern "C" int rados_aio_wait_for_safe(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->wait_for_safe();
}

extern "C" int rados_aio_is_complete(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->is_complete();
}

extern "C" int rados_aio_is_safe(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->is_safe();
}

extern "C" int rados_aio_wait_for_complete_and_cb(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->wait_for_complete_and_cb();
}

extern "C" int rados_aio_wait_for_safe_and_cb(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->wait_for_safe_and_cb();
}

extern "C" int rados_aio_is_complete_and_cb(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->is_complete_and_cb();
}

extern "C" int rados_aio_is_safe_and_cb(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->is_safe_and_cb();
}

extern "C" int rados_aio_get_return_value(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->get_return_value();
}

extern "C" uint64_t rados_aio_get_version(rados_completion_t c)
{
  return ((librados::AioCompletionImpl*)c)->get_version();
}

extern "C" void rados_aio_release(rados_completion_t c)
{
  ((librados::AioCompletionImpl*)c)->put();
}

extern "C" int rados_aio_read(rados_ioctx_t io, const char *o,
			       rados_completion_t completion,
			       char *buf, size_t len, uint64_t off)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->aio_read(oid, (librados::AioCompletionImpl*)completion,
		       buf, len, off);
}

extern "C" int rados_aio_write(rados_ioctx_t io, const char *o,
				rados_completion_t completion,
				const char *buf, size_t len, uint64_t off)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->aio_write(oid, (librados::AioCompletionImpl*)completion,
			bl, len, off);
}

extern "C" int rados_aio_append(rados_ioctx_t io, const char *o,
				rados_completion_t completion,
				const char *buf, size_t len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->aio_append(oid, (librados::AioCompletionImpl*)completion,
			 bl, len);
}

extern "C" int rados_aio_write_full(rados_ioctx_t io, const char *o,
				    rados_completion_t completion,
				    const char *buf, size_t len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  return ctx->aio_write_full(oid, (librados::AioCompletionImpl*)completion, bl);
}

extern "C" int rados_aio_remove(rados_ioctx_t io, const char *o,
				rados_completion_t completion)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->aio_remove(oid, (librados::AioCompletionImpl*)completion);
}

extern "C" int rados_aio_flush_async(rados_ioctx_t io,
				     rados_completion_t completion)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->flush_aio_writes_async((librados::AioCompletionImpl*)completion);
  return 0;
}

extern "C" int rados_aio_flush(rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->flush_aio_writes();
  return 0;
}

extern "C" int rados_aio_stat(rados_ioctx_t io, const char *o, 
			      rados_completion_t completion,
			      uint64_t *psize, time_t *pmtime)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->aio_stat(oid, (librados::AioCompletionImpl*)completion, 
		       psize, pmtime);
}


struct C_WatchCB : public librados::WatchCtx {
  rados_watchcb_t wcb;
  void *arg;
  C_WatchCB(rados_watchcb_t _wcb, void *_arg) : wcb(_wcb), arg(_arg) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    wcb(opcode, ver, arg);
  }
};

int rados_watch(rados_ioctx_t io, const char *o, uint64_t ver, uint64_t *handle,
                rados_watchcb_t watchcb, void *arg)
{
  uint64_t *cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  C_WatchCB *wc = new C_WatchCB(watchcb, arg);
  return ctx->watch(oid, ver, cookie, wc);
}

int rados_unwatch(rados_ioctx_t io, const char *o, uint64_t handle)
{
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->unwatch(oid, cookie);
}

int rados_notify(rados_ioctx_t io, const char *o, uint64_t ver, const char *buf, int buf_len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  return ctx->notify(oid, ver, bl);
}

extern "C" int rados_set_alloc_hint(rados_ioctx_t io, const char *o,
                                    uint64_t expected_object_size,
                                    uint64_t expected_write_size)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  return ctx->set_alloc_hint(oid, expected_object_size, expected_write_size);
}

extern "C" int rados_lock_exclusive(rados_ioctx_t io, const char * o,
			  const char * name, const char * cookie,
			  const char * desc, struct timeval * duration,
			  uint8_t flags)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  return ctx.lock_exclusive(o, name, cookie, desc, duration, flags);
}

extern "C" int rados_lock_shared(rados_ioctx_t io, const char * o,
			  const char * name, const char * cookie,
			  const char * tag, const char * desc,
			  struct timeval * duration, uint8_t flags)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  return ctx.lock_shared(o, name, cookie, tag, desc, duration, flags);
}
extern "C" int rados_unlock(rados_ioctx_t io, const char *o, const char *name,
			    const char *cookie)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  return ctx.unlock(o, name, cookie);

}

extern "C" ssize_t rados_list_lockers(rados_ioctx_t io, const char *o,
				      const char *name, int *exclusive,
				      char *tag, size_t *tag_len,
				      char *clients, size_t *clients_len,
				      char *cookies, size_t *cookies_len,
				      char *addrs, size_t *addrs_len)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);
  std::string name_str = name;
  std::string oid = o;
  std::string tag_str;
  int tmp_exclusive;
  std::list<librados::locker_t> lockers;
  int r = ctx.list_lockers(oid, name_str, &tmp_exclusive, &tag_str, &lockers);
  if (r < 0)
	  return r;

  size_t clients_total = 0;
  size_t cookies_total = 0;
  size_t addrs_total = 0;
  list<librados::locker_t>::const_iterator it;
  for (it = lockers.begin(); it != lockers.end(); ++it) {
    clients_total += it->client.length() + 1;
    cookies_total += it->cookie.length() + 1;
    addrs_total += it->address.length() + 1;
  }

  bool too_short = ((clients_total > *clients_len) ||
                    (cookies_total > *cookies_len) ||
                    (addrs_total > *addrs_len) ||
                    (tag_str.length() + 1 > *tag_len));
  *clients_len = clients_total;
  *cookies_len = cookies_total;
  *addrs_len = addrs_total;
  *tag_len = tag_str.length() + 1;
  if (too_short)
    return -ERANGE;

  strcpy(tag, tag_str.c_str());
  char *clients_p = clients;
  char *cookies_p = cookies;
  char *addrs_p = addrs;
  for (it = lockers.begin(); it != lockers.end(); ++it) {
    strcpy(clients_p, it->client.c_str());
    clients_p += it->client.length() + 1;
    strcpy(cookies_p, it->cookie.c_str());
    cookies_p += it->cookie.length() + 1;
    strcpy(addrs_p, it->address.c_str());
    addrs_p += it->address.length() + 1;
  }
  if (tmp_exclusive)
    *exclusive = 1;
  else
    *exclusive = 0;

  return lockers.size();
}

extern "C" int rados_break_lock(rados_ioctx_t io, const char *o,
				const char *name, const char *client,
				const char *cookie)
{
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  return ctx.break_lock(o, name, client, cookie);
}

extern "C" rados_write_op_t rados_create_write_op()
{
  return new (std::nothrow)::ObjectOperation;
}

extern "C" void rados_release_write_op(rados_write_op_t write_op)
{
  delete (::ObjectOperation*)write_op;
}

extern "C" void rados_write_op_set_flags(rados_write_op_t write_op, int flags)
{
  set_op_flags((::ObjectOperation *)write_op, flags);
}

extern "C" void rados_write_op_assert_exists(rados_write_op_t write_op)
{
  ((::ObjectOperation *)write_op)->stat(NULL, (utime_t *)NULL, NULL);
}

extern "C" void rados_write_op_cmpxattr(rados_write_op_t write_op,
                                       const char *name,
				       uint8_t comparison_operator,
				       const char *value,
				       size_t value_len)
{
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)write_op)->cmpxattr(name,
					    comparison_operator,
					    CEPH_OSD_CMPXATTR_MODE_STRING,
					    bl);
}

static void rados_c_omap_cmp(ObjectOperation *op,
			     const char *key,
			     uint8_t comparison_operator,
			     const char *val,
			     size_t val_len,
			     int *prval)
{
  bufferlist bl;
  bl.append(val, val_len);
  std::map<std::string, pair<bufferlist, int> > assertions;
  assertions[key] = std::make_pair(bl, comparison_operator);
  op->omap_cmp(assertions, prval);
}

extern "C" void rados_write_op_omap_cmp(rados_write_op_t write_op,
					const char *key,
					uint8_t comparison_operator,
					const char *val,
					size_t val_len,
					int *prval)
{
  rados_c_omap_cmp((::ObjectOperation *)write_op, key, comparison_operator,
		   val, val_len, prval);
}

extern "C" void rados_write_op_setxattr(rados_write_op_t write_op,
                                       const char *name,
				       const char *value,
				       size_t value_len)
{
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)write_op)->setxattr(name, bl);
}

extern "C" void rados_write_op_rmxattr(rados_write_op_t write_op,
                                       const char *name)
{
  bufferlist bl;
  ((::ObjectOperation *)write_op)->rmxattr(name);
}

extern "C" void rados_write_op_create(rados_write_op_t write_op,
                                      int exclusive,
				      const char* category)
{
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  if(category) {
    oo->create(exclusive, category);
  } else {
    oo->create(!!exclusive);
  }
}

extern "C" void rados_write_op_write(rados_write_op_t write_op,
				     const char *buffer,
				     size_t len,
                                     uint64_t offset)
{
  bufferlist bl;
  bl.append(buffer,len);
  ((::ObjectOperation *)write_op)->write(offset, bl);
}

extern "C" void rados_write_op_write_full(rados_write_op_t write_op,
				          const char *buffer,
				          size_t len)
{
  bufferlist bl;
  bl.append(buffer,len);
  ((::ObjectOperation *)write_op)->write_full(bl);
}

extern "C" void rados_write_op_append(rados_write_op_t write_op,
				      const char *buffer,
				      size_t len)
{
  bufferlist bl;
  bl.append(buffer,len);
  ((::ObjectOperation *)write_op)->append(bl);
}

extern "C" void rados_write_op_remove(rados_write_op_t write_op)
{
  ((::ObjectOperation *)write_op)->remove();
}

extern "C" void rados_write_op_truncate(rados_write_op_t write_op,
				        uint64_t offset)
{
  ((::ObjectOperation *)write_op)->truncate(offset);
}

extern "C" void rados_write_op_zero(rados_write_op_t write_op,
				    uint64_t offset,
				    uint64_t len)
{
  ((::ObjectOperation *)write_op)->zero(offset, len);
}

extern "C" void rados_write_op_exec(rados_write_op_t write_op,
				    const char *cls,
				    const char *method,
				    const char *in_buf,
				    size_t in_len,
				    int *prval)
{
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  ((::ObjectOperation *)write_op)->call(cls, method, inbl, NULL, NULL, prval);
}

extern "C" void rados_write_op_omap_set(rados_write_op_t write_op,
					char const* const* keys,
					char const* const* vals,
					const size_t *lens,
					size_t num)
{
  std::map<std::string, bufferlist> entries;
  for (size_t i = 0; i < num; ++i) {
    bufferlist bl(lens[i]);
    bl.append(vals[i], lens[i]);
    entries[keys[i]] = bl;
  }
  ((::ObjectOperation *)write_op)->omap_set(entries);
}

extern "C" void rados_write_op_omap_rm_keys(rados_write_op_t write_op,
					    char const* const* keys,
					    size_t keys_len)
{
  std::set<std::string> to_remove(keys, keys + keys_len);
  ((::ObjectOperation *)write_op)->omap_rm_keys(to_remove);
}

extern "C" void rados_write_op_omap_clear(rados_write_op_t write_op)
{
  ((::ObjectOperation *)write_op)->omap_clear();
}

extern "C" void rados_write_op_set_alloc_hint(rados_write_op_t write_op,
                                            uint64_t expected_object_size,
                                            uint64_t expected_write_size)
{
  ((::ObjectOperation *)write_op)->set_alloc_hint(expected_object_size,
                                                  expected_write_size);
}

extern "C" int rados_write_op_operate(rados_write_op_t write_op,
                                      rados_ioctx_t io,
                                      const char *oid,
				      time_t *mtime,
				      int flags)
{
  object_t obj(oid);
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->operate(obj, oo, mtime, flags);
}

extern "C" int rados_aio_write_op_operate(rados_write_op_t write_op,
					  rados_ioctx_t io,
					  rados_completion_t completion,
					  const char *oid,
					  time_t *mtime,
					  int flags)
{
  object_t obj(oid);
  ::ObjectOperation *oo = (::ObjectOperation *) write_op;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  return ctx->aio_operate(obj, oo, c, flags);
}

extern "C" rados_read_op_t rados_create_read_op()
{
  return new (std::nothrow)::ObjectOperation;
}

extern "C" void rados_release_read_op(rados_read_op_t read_op)
{
  delete (::ObjectOperation *)read_op;
}

extern "C" void rados_read_op_set_flags(rados_read_op_t read_op, int flags)
{
  set_op_flags((::ObjectOperation *)read_op, flags);
}

extern "C" void rados_read_op_assert_exists(rados_read_op_t read_op)
{
  ((::ObjectOperation *)read_op)->stat(NULL, (utime_t *)NULL, NULL);
}

extern "C" void rados_read_op_cmpxattr(rados_read_op_t read_op,
				       const char *name,
				       uint8_t comparison_operator,
				       const char *value,
				       size_t value_len)
{
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)read_op)->cmpxattr(name,
					   comparison_operator,
					   CEPH_OSD_CMPXATTR_MODE_STRING,
					   bl);
}

extern "C" void rados_read_op_omap_cmp(rados_read_op_t read_op,
				       const char *key,
				       uint8_t comparison_operator,
				       const char *val,
				       size_t val_len,
				       int *prval)
{
  rados_c_omap_cmp((::ObjectOperation *)read_op, key, comparison_operator,
		   val, val_len, prval);
}

extern "C" void rados_read_op_stat(rados_read_op_t read_op,
				   uint64_t *psize,
				   time_t *pmtime,
				   int *prval)
{
  ((::ObjectOperation *)read_op)->stat(psize, pmtime, prval);
}

class C_bl_to_buf : public Context {
  char *out_buf;
  size_t out_len;
  size_t *bytes_read;
  int *prval;
public:
  bufferlist out_bl;
  C_bl_to_buf(char *out_buf,
	      size_t out_len,
	      size_t *bytes_read,
	      int *prval) : out_buf(out_buf), out_len(out_len),
			    bytes_read(bytes_read), prval(prval) {}
  void finish(int r) {
    if (out_bl.length() > out_len) {
      if (prval)
	*prval = -ERANGE;
      if (bytes_read)
	*bytes_read = 0;
      return;
    }
    if (bytes_read)
      *bytes_read = out_bl.length();
    if (out_buf && out_bl.c_str() != out_buf)
      out_bl.copy(0, out_bl.length(), out_buf);
  }
};

extern "C" void rados_read_op_read(rados_read_op_t read_op,
				   uint64_t offset,
				   size_t len,
				   char *buf,
				   size_t *bytes_read,
				   int *prval)
{
  C_bl_to_buf *ctx = new C_bl_to_buf(buf, len, bytes_read, prval);
  ctx->out_bl.push_back(buffer::create_static(len, buf));
  ((::ObjectOperation *)read_op)->read(offset, len, &ctx->out_bl, prval, ctx);
}

class C_out_buffer : public Context {
  char **out_buf;
  size_t *out_len;
public:
  bufferlist out_bl;
  C_out_buffer(char **out_buf, size_t *out_len) : out_buf(out_buf),
						  out_len(out_len) {}
  void finish(int r) {
    // ignore r since we don't know the meaning of return values
    // from custom class methods
    do_out_buffer(out_bl, out_buf, out_len);
  }
};

extern "C" void rados_read_op_exec(rados_read_op_t read_op,
				   const char *cls,
				   const char *method,
				   const char *in_buf,
				   size_t in_len,
				   char **out_buf,
				   size_t *out_len,
				   int *prval)
{
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  C_out_buffer *ctx = new C_out_buffer(out_buf, out_len);
  ((::ObjectOperation *)read_op)->call(cls, method, inbl, &ctx->out_bl, ctx,
				       prval);
}

extern "C" void rados_read_op_exec_user_buf(rados_read_op_t read_op,
					    const char *cls,
					    const char *method,
					    const char *in_buf,
					    size_t in_len,
					    char *out_buf,
					    size_t out_len,
					    size_t *used_len,
					    int *prval)
{
  C_bl_to_buf *ctx = new C_bl_to_buf(out_buf, out_len, used_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  ((::ObjectOperation *)read_op)->call(cls, method, inbl, &ctx->out_bl, ctx,
				       prval);
}

struct RadosOmapIter {
  std::map<std::string, bufferlist> values;
  std::map<std::string, bufferlist>::iterator i;
};

class C_OmapIter : public Context {
  RadosOmapIter *iter;
public:
  C_OmapIter(RadosOmapIter *iter) : iter(iter) {}
  void finish(int r) {
    iter->i = iter->values.begin();
  }
};

class C_XattrsIter : public Context {
  RadosXattrsIter *iter;
public:
  C_XattrsIter(RadosXattrsIter *iter) : iter(iter) {}
  void finish(int r) {
    iter->i = iter->attrset.begin();
  }
};

extern "C" void rados_read_op_getxattrs(rados_read_op_t read_op,
					rados_xattrs_iter_t *iter,
					int *prval)
{
  RadosXattrsIter *xattrs_iter = new RadosXattrsIter;
  ((::ObjectOperation *)read_op)->getxattrs(&xattrs_iter->attrset, prval);
  ((::ObjectOperation *)read_op)->add_handler(new C_XattrsIter(xattrs_iter));
  *iter = xattrs_iter;
}

extern "C" void rados_read_op_omap_get_vals(rados_read_op_t read_op,
					    const char *start_after,
					    const char *filter_prefix,
					    uint64_t max_return,
					    rados_omap_iter_t *iter,
					    int *prval)
{
  RadosOmapIter *omap_iter = new RadosOmapIter;
  const char *start = start_after ? start_after : "";
  const char *filter = filter_prefix ? filter_prefix : "";
  ((::ObjectOperation *)read_op)->omap_get_vals(start,
						filter,
						max_return,
						&omap_iter->values,
						prval);
  ((::ObjectOperation *)read_op)->add_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
}

struct C_OmapKeysIter : public Context {
  RadosOmapIter *iter;
  std::set<std::string> keys;
  C_OmapKeysIter(RadosOmapIter *iter) : iter(iter) {}
  void finish(int r) {
    // map each key to an empty bl
    for (std::set<std::string>::const_iterator i = keys.begin();
	 i != keys.end(); ++i) {
      iter->values[*i];
    }
    iter->i = iter->values.begin();
  }
};

extern "C" void rados_read_op_omap_get_keys(rados_read_op_t read_op,
					    const char *start_after,
					    uint64_t max_return,
					    rados_omap_iter_t *iter,
					    int *prval)
{
  RadosOmapIter *omap_iter = new RadosOmapIter;
  C_OmapKeysIter *ctx = new C_OmapKeysIter(omap_iter);
  ((::ObjectOperation *)read_op)->omap_get_keys(start_after ? start_after : "",
						max_return, &ctx->keys, prval);
  ((::ObjectOperation *)read_op)->add_handler(ctx);
  *iter = omap_iter;
}

extern "C" void rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op,
						    char const* const* keys,
						    size_t keys_len,
						    rados_omap_iter_t *iter,
						    int *prval)
{
  std::set<std::string> to_get(keys, keys + keys_len);

  RadosOmapIter *omap_iter = new RadosOmapIter;
  ((::ObjectOperation *)read_op)->omap_get_vals_by_keys(to_get,
							&omap_iter->values,
							prval);
  ((::ObjectOperation *)read_op)->add_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
}

extern "C" int rados_omap_get_next(rados_omap_iter_t iter,
				   char **key,
				   char **val,
				   size_t *len)
{
  RadosOmapIter *it = (RadosOmapIter *)iter;
  if (it->i == it->values.end()) {
    *key = NULL;
    *val = NULL;
    *len = 0;
    return 0;
  }
  if (key)
    *key = (char*)it->i->first.c_str();
  if (val)
    *val = it->i->second.c_str();
  if (len)
    *len = it->i->second.length();
  ++it->i;
  return 0;
}

extern "C" void rados_omap_get_end(rados_omap_iter_t iter)
{
  RadosOmapIter *it = (RadosOmapIter *)iter;
  delete it;
}

extern "C" int rados_read_op_operate(rados_read_op_t read_op,
				     rados_ioctx_t io,
				     const char *oid,
				     int flags)
{
  object_t obj(oid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->operate_read(obj, (::ObjectOperation *)read_op, NULL, flags);
}

extern "C" int rados_aio_read_op_operate(rados_read_op_t read_op,
					 rados_ioctx_t io,
					 rados_completion_t completion,
					 const char *oid,
					 int flags)
{
  object_t obj(oid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  return ctx->aio_operate_read(obj, (::ObjectOperation *)read_op,
			       c, flags, NULL);
}
