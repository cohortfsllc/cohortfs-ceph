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

#include <cassert>
#include <limits.h>

#include "IoCtxImpl.h"

#include "librados/AioCompletionImpl.h"
#include "librados/RadosClient.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

using std::shared_ptr;
using std::unique_ptr;

librados::IoCtxImpl::IoCtxImpl() :
  ref_cnt(0), client(NULL), volume(), assert_ver(0), last_objver(0),
  notify_timeout(30),
  aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
  aio_write_seq(0), lock(NULL), objecter(NULL)
{
}

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, Objecter *objecter,
			       Mutex *client_lock,
			       const shared_ptr<const Volume>& volume)
  : ref_cnt(0), client(c), volume(volume), assert_ver(0),
    notify_timeout(c->cct->_conf->client_notify_timeout),
    aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
    aio_write_seq(0), lock(client_lock), objecter(objecter)
{
}

void librados::IoCtxImpl::queue_aio_write(AioCompletionImpl *c)
{
  get();
  aio_write_list_lock.Lock();
  assert(c->io == this);
  c->aio_write_seq = ++aio_write_seq;
  ldout(client->cct, 20) << "queue_aio_write " << this << " completion " << c
			 << " write_seq " << aio_write_seq << dendl;
  aio_write_list.push_back(&c->aio_write_list_item);
  aio_write_list_lock.Unlock();
}

void librados::IoCtxImpl::complete_aio_write(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "complete_aio_write " << c << dendl;
  aio_write_list_lock.Lock();
  assert(c->io == this);
  c->aio_write_list_item.remove_myself();

  map<ceph_tid_t, std::list<AioCompletionImpl*> >::iterator waiters = aio_write_waiters.begin();
  while (waiters != aio_write_waiters.end()) {
    if (!aio_write_list.empty() &&
	aio_write_list.front()->aio_write_seq <= waiters->first) {
      ldout(client->cct, 20) << " next outstanding write is " << aio_write_list.front()->aio_write_seq
			     << " <= waiter " << waiters->first
			     << ", stopping" << dendl;
      break;
    }
    ldout(client->cct, 20) << " waking waiters on seq " << waiters->first << dendl;
    for (std::list<AioCompletionImpl*>::iterator it = waiters->second.begin();
	 it != waiters->second.end(); ++it) {
      client->finisher.queue(new C_AioCompleteAndSafe(*it));
      (*it)->put();
    }
    aio_write_waiters.erase(waiters++);
  }

  aio_write_cond.Signal();
  aio_write_list_lock.Unlock();
  put();
}

void librados::IoCtxImpl::flush_aio_writes_async(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "flush_aio_writes_async " << this
			 << " completion " << c << dendl;
  Mutex::Locker l(aio_write_list_lock);
  ceph_tid_t seq = aio_write_seq;
  if (aio_write_list.empty()) {
    ldout(client->cct, 20) << "flush_aio_writes_async no writes. (tid "
			   << seq << ")" << dendl;
    client->finisher.queue(new C_AioCompleteAndSafe(c));
  } else {
    ldout(client->cct, 20) << "flush_aio_writes_async " << aio_write_list.size()
			   << " writes in flight; waiting on tid " << seq << dendl;
    c->get();
    aio_write_waiters[seq].push_back(c);
  }
}

void librados::IoCtxImpl::flush_aio_writes()
{
  ldout(client->cct, 20) << "flush_aio_writes" << dendl;
  aio_write_list_lock.Lock();
  ceph_tid_t seq = aio_write_seq;
  while (!aio_write_list.empty() &&
	 aio_write_list.front()->aio_write_seq <= seq)
    aio_write_cond.Wait(aio_write_list_lock);
  aio_write_list_lock.Unlock();
}

// IO

int librados::IoCtxImpl::create(const object_t& oid, bool exclusive)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->create(exclusive);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::create(const object_t& oid, bool exclusive,
				const std::string& category)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->create(exclusive, category);
  return operate(oid, op, NULL);
}

/*
 * add any version assert operations that are appropriate given the
 * stat in the IoCtx, either the target version assert or any src
 * object asserts.  these affect a single ioctx operation, so clear
 * the ioctx state when we're doing.
 *
 * return a pointer to the ObjectOperation if we added any events;
 * this is convenient for passing the extra_ops argument into Objecter
 * methods.
 */
unique_ptr<ObjOp> librados::IoCtxImpl::prepare_assert_ops()
{
  unique_ptr<ObjOp> op = volume->op();
  if (assert_ver) {
    op->assert_version(assert_ver);
    assert_ver = 0;
  }

  while (!assert_src_version.empty()) {
    map<object_t,uint64_t>::iterator p = assert_src_version.begin();
    op->assert_src_version(p->first, p->second);
    assert_src_version.erase(p);
  }
  return op;
}

int librados::IoCtxImpl::write(const object_t& oid, bufferlist& bl,
			       size_t len, uint64_t off)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  bufferlist mybl;
  mybl.substr_of(bl, 0, len);
  op->write(off, mybl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::append(const object_t& oid, bufferlist& bl, size_t len)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  bufferlist mybl;
  mybl.substr_of(bl, 0, len);
  op->append(mybl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::write_full(const object_t& oid, bufferlist& bl)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->write_full(bl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::operate(const object_t& oid, unique_ptr<ObjOp>& o,
				 time_t *pmtime, int flags)
{
  utime_t ut;
  if (pmtime) {
    ut = utime_t(*pmtime, 0);
  } else {
    ut = ceph_clock_now(client->cct);
  }

  if (!o->size())
    return 0;

  Mutex mylock;
  Cond cond;
  bool done;
  int r;
  version_t ver;

  Context *oncommit = new C_SafeCond(&mylock, &cond, &done, &r);

  Objecter::Op *objecter_op = objecter->prepare_mutate_op(
    oid, volume, o, ut, flags, NULL, oncommit, &ver);
  lock->Lock();
  objecter->op_submit(objecter_op);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::operate_read(const object_t& oid,
				      unique_ptr<ObjOp>& o,
				      bufferlist *pbl,
				      int flags)
{
  if (!o->size())
    return 0;

  Mutex mylock;
  Cond cond;
  bool done;
  int r;
  version_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  Objecter::Op *objecter_op = objecter->prepare_read_op(oid, volume,
							o, pbl, flags,
							onack, &ver);
  lock->Lock();
  objecter->op_submit(objecter_op);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::aio_operate_read(const object_t &oid,
					  unique_ptr<ObjOp>& o,
					  AioCompletionImpl *c,
					  int flags,
					  bufferlist *pbl)
{
  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;

  Objecter::Op *objecter_op = objecter->prepare_read_op(oid, volume,
							o, pbl, flags,
							onack, &c->objver);
  Mutex::Locker l(*lock);
  objecter->op_submit(objecter_op);
  return 0;
}

int librados::IoCtxImpl::aio_operate(const object_t& oid,
				     unique_ptr<ObjOp>& o,
				     AioCompletionImpl *c,
				     int flags)
{
  utime_t ut = ceph_clock_now(client->cct);

  Context *onack = new C_aio_Ack(c);
  Context *oncommit = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Mutex::Locker l(*lock);
  objecter->mutate(oid, volume, o, ut, flags, onack, oncommit, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  bufferlist *pbl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;
  c->blp = pbl;

  Mutex::Locker l(*lock);
  objecter->read(oid, volume, off, len, pbl, 0, onack, &c->objver);
  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  char *buf, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;
  c->bl.clear();
  c->bl.push_back(buffer::create_static(len, buf));
  c->blp = &c->bl;

  Mutex::Locker l(*lock);
  objecter->read(oid, volume, off, len, &c->bl, 0, onack, &c->objver);

  return 0;
}

class C_ObjectOperation : public Context {
public:
  unique_ptr<ObjOp> m_ops;
  C_ObjectOperation(Context *c) : m_ctx(c) {}
  virtual void finish(int r) {
    m_ctx->complete(r);
  }
private:
  Context *m_ctx;
};

int librados::IoCtxImpl::aio_sparse_read(const object_t oid,
					 AioCompletionImpl *c,
					 std::map<uint64_t,uint64_t> *m,
					 bufferlist *data_bl, size_t len,
					 uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *nested = new C_aio_Ack(c);
  C_ObjectOperation *onack = new C_ObjectOperation(nested);

  c->is_read = true;
  c->io = this;

  onack->m_ops->sparse_read(off, len, m, data_bl, NULL);

  Mutex::Locker l(*lock);
  objecter->read(oid, volume, onack->m_ops, NULL, 0, onack, &c->objver);
  return 0;
}

int librados::IoCtxImpl::aio_write(const object_t &oid, AioCompletionImpl *c,
				   const bufferlist& bl, size_t len,
				   uint64_t off)
{
  utime_t ut = ceph_clock_now(client->cct);
  ldout(client->cct, 20) << "aio_write " << oid << " " << off << "~"
			 << len << dendl;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->write(oid, volume,
		  off, len, bl, ut, 0, onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_append(const object_t &oid, AioCompletionImpl *c,
				    const bufferlist& bl, size_t len)
{
  utime_t ut = ceph_clock_now(client->cct);

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->append(oid, volume, len, bl, ut, 0, onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_write_full(const object_t &oid,
					AioCompletionImpl *c,
					const bufferlist& bl)
{
  utime_t ut = ceph_clock_now(client->cct);

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->write_full(oid, volume, bl, ut, 0, onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_remove(const object_t &oid, AioCompletionImpl *c)
{
  utime_t ut = ceph_clock_now(client->cct);

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->remove(oid, volume, ut, 0, onack, onsafe, &c->objver);

  return 0;
}


int librados::IoCtxImpl::aio_stat(const object_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, time_t *pmtime)
{
  c->io = this;
  C_aio_stat_Ack *onack = new C_aio_stat_Ack(c, pmtime);

  Mutex::Locker l(*lock);
  objecter->stat(oid, volume, psize, &onack->mtime, 0, onack, &c->objver);

  return 0;
}

int librados::IoCtxImpl::remove(const object_t& oid)
{
  std::unique_ptr<ObjOp> op = prepare_assert_ops();
  op->remove();
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::trunc(const object_t& oid, uint64_t size)
{
  std::unique_ptr<ObjOp> op = prepare_assert_ops();
  op->truncate(size);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::exec(const object_t& oid,
			      const char *cls, const char *method,
			      bufferlist& inbl, bufferlist& outbl)
{
  std::unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->call(cls, method, inbl);
  return operate_read(oid, rd, &outbl);
}

int librados::IoCtxImpl::aio_exec(const object_t& oid, AioCompletionImpl *c,
				  const char *cls, const char *method,
				  bufferlist& inbl, bufferlist *outbl)
{
  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;

  Mutex::Locker l(*lock);
  std::unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->call(cls, method, inbl);
  objecter->read(oid, volume, rd, outbl, 0, onack, &c->objver);

  return 0;
}

int librados::IoCtxImpl::read(const object_t& oid,
			      bufferlist& bl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  std::unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->read(off, len, &bl);
  int r = operate_read(oid, rd, &bl);
  if (r < 0)
    return r;

  if (bl.length() < len) {
    ldout(client->cct, 10) << "Returned length " << bl.length()
	     << " less than original length "<< len << dendl;
  }

  return bl.length();
}

int librados::IoCtxImpl::sparse_read(const object_t& oid,
				     std::map<uint64_t,uint64_t>& m,
				     bufferlist& data_bl, size_t len,
				     uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->sparse_read(off, len, &m, &data_bl, NULL);

  int r = operate_read(oid, rd, NULL);
  if (r < 0)
    return r;

  return m.size();
}

int librados::IoCtxImpl::stat(const object_t& oid, uint64_t *psize, time_t *pmtime)
{
  uint64_t size;
  utime_t mtime;

  if (!psize)
    psize = &size;

  unique_ptr<ObjOp> rd =  prepare_assert_ops();
  rd->stat(psize, &mtime, NULL);
  int r = operate_read(oid, rd, NULL);

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  return r;
}

int librados::IoCtxImpl::getxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->getxattr(name, &bl, NULL);
  int r = operate_read(oid, rd, NULL);
  if (r < 0)
    return r;

  return bl.length();
}

int librados::IoCtxImpl::rmxattr(const object_t& oid, const char *name)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->rmxattr(name);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::setxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->setxattr(name, bl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::getxattrs(const object_t& oid,
				   map<std::string, bufferlist>& attrset)
{
  map<string, bufferlist> aset;

  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->getxattrs(aset, NULL);
  int r = operate_read(oid, rd, NULL);

  attrset.clear();
  if (r >= 0) {
    for (map<string,bufferlist>::iterator p = aset.begin(); p != aset.end(); ++p) {
      ldout(client->cct, 10) << "IoCtxImpl::getxattrs: xattr=" << p->first << dendl;
      attrset[p->first.c_str()] = p->second;
    }
  }

  return r;
}

void librados::IoCtxImpl::set_sync_op_version(version_t ver)
{
  last_objver = ver;
}

int librados::IoCtxImpl::watch(const object_t& oid, uint64_t ver,
			       uint64_t *cookie, librados::WatchCtx *ctx)
{
  unique_ptr<ObjOp> wr = prepare_assert_ops();
  Mutex mylock;
  Cond cond;
  bool done;
  int r;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
  version_t objver;

  WatchNotifyInfo *wc = new WatchNotifyInfo(this, oid);
  wc->watch_ctx = ctx;
  lock->Lock();
  client->register_watch_notify_callback(wc, cookie);
  lock->Unlock();
  wr->watch(*cookie, ver, 1);
  bufferlist bl;
  wc->linger_id = objecter->linger_mutate(oid, volume, wr,
					  ceph_clock_now(NULL), bl,
					  0, NULL, onfinish, &objver);
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(objver);

  if (r < 0) {
    lock->Lock();
    client->unregister_watch_notify_callback(*cookie); // destroys wc
    lock->Unlock();
  }

  return r;
}


/* this is called with IoCtxImpl::lock held */
int librados::IoCtxImpl::_notify_ack(
  const object_t& oid,
  uint64_t notify_id, uint64_t ver,
  uint64_t cookie)
{
  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->notify_ack(notify_id, ver, cookie);
  objecter->read(oid, volume, rd, (bufferlist*)NULL, 0, 0, 0);

  return 0;
}

int librados::IoCtxImpl::unwatch(const object_t& oid, uint64_t cookie)
{
  bufferlist inbl, outbl;

  Mutex mylock;
  Cond cond;
  bool done;
  int r;
  Context *oncommit = new C_SafeCond(&mylock, &cond, &done, &r);
  version_t ver;

  lock->Lock();
  client->unregister_watch_notify_callback(cookie); // destroys wc
  lock->Unlock();

  unique_ptr<ObjOp> wr = prepare_assert_ops();
  wr->watch(cookie, 0, 0);
  objecter->mutate(oid, volume, wr, ceph_clock_now(client->cct),
		   0, NULL, oncommit, &ver);

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::notify(const object_t& oid, bufferlist& bl)
{
  bufferlist inbl, outbl;
  // Construct WatchNotifyInfo
  Cond cond_all;
  Mutex mylock_all;
  bool done_all = false;
  int r_notify = 0;
  unique_ptr<ObjOp> rd = prepare_assert_ops();

  lock->Lock();
  WatchNotifyInfo *wc = new WatchNotifyInfo(this, oid);
  wc->notify_done = &done_all;
  wc->notify_lock = &mylock_all;
  wc->notify_cond = &cond_all;
  wc->notify_rval = &r_notify;

  // Acquire cookie
  uint64_t cookie;
  lock->Lock();
  client->register_watch_notify_callback(wc, &cookie);
  lock->Unlock();
  uint32_t prot_ver = 1;
  uint32_t timeout = notify_timeout;
  ::encode(prot_ver, inbl);
  ::encode(timeout, inbl);
  ::encode(bl, inbl);
  rd->notify(cookie, inbl);
  C_SaferCond onack;
  version_t objver;
  wc->linger_id = objecter->linger_read(oid, volume, rd, inbl, NULL, 0,
					&onack, &objver);
  lock->Unlock();

  int r_issue = onack.wait();

  if (r_issue == 0) {
    mylock_all.Lock();
    while (!done_all)
      cond_all.Wait(mylock_all);
    mylock_all.Unlock();
  }

  lock->Lock();
  client->unregister_watch_notify_callback(cookie);   // destroys wc
  lock->Unlock();

  set_sync_op_version(objver);

  return r_issue == 0 ? r_notify : r_issue;
}

int librados::IoCtxImpl::set_alloc_hint(const object_t& oid,
					uint64_t expected_object_size,
					uint64_t expected_write_size)
{
  unique_ptr<ObjOp> wr = prepare_assert_ops();
  wr->set_alloc_hint(expected_object_size, expected_write_size);
  return operate(oid, wr, NULL);
}

version_t librados::IoCtxImpl::last_version()
{
  return last_objver;
}

void librados::IoCtxImpl::set_assert_version(uint64_t ver)
{
  assert_ver = ver;
}
void librados::IoCtxImpl::set_assert_src_version(const object_t& oid,
						 uint64_t ver)
{
  assert_src_version[oid] = ver;
}

void librados::IoCtxImpl::set_notify_timeout(uint32_t timeout)
{
  notify_timeout = timeout;
}

///////////////////////////// C_aio_Ack ////////////////////////////////

librados::IoCtxImpl::C_aio_Ack::C_aio_Ack(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::C_aio_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  if (c->is_read)
    c->safe = true;
  c->cond.Signal();

  if (r == 0 && c->blp && c->blp->length() > 0) {
    c->rval = c->blp->length();
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }
  if (c->is_read && c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->put_unlock();
}

///////////////////////////// C_aio_stat_Ack ////////////////////////////

librados::IoCtxImpl::C_aio_stat_Ack::C_aio_stat_Ack(AioCompletionImpl *_c,
						    time_t *pm)
   : c(_c), pmtime(pm)
{
  c->get();
}

void librados::IoCtxImpl::C_aio_stat_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }

  c->put_unlock();
}

//////////////////////////// C_aio_Safe ////////////////////////////////

librados::IoCtxImpl::C_aio_Safe::C_aio_Safe(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::C_aio_Safe::finish(int r)
{
  c->lock.Lock();
  if (!c->ack) {
    c->rval = r;
    c->ack = true;
  }
  c->safe = true;
  c->cond.Signal();

  if (c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->io->complete_aio_write(c);

  c->put_unlock();
}

///////////////////////// C_NotifyComplete /////////////////////////////

librados::IoCtxImpl::C_NotifyComplete::C_NotifyComplete(Mutex *_l,
							Cond *_c,
							bool *_d)
  : lock(_l), cond(_c), done(_d)
{
  *done = false;
}

void librados::IoCtxImpl::C_NotifyComplete::notify(uint8_t opcode,
						   uint64_t ver,
						   bufferlist& bl)
{
  lock->Lock();
  *done = true;
  cond->Signal();
  lock->Unlock();
}

