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
  aio_write_seq(0), objecter(NULL)
{
}

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, Objecter *objecter,
			       const shared_ptr<const Volume>& volume)
  : ref_cnt(0), client(c), volume(volume), assert_ver(0),
    notify_timeout(c->cct->_conf->client_notify_timeout),
    aio_write_seq(0), objecter(objecter)
{
}

void librados::IoCtxImpl::queue_aio_write(AioCompletionImpl *c)
{
  get();
  lock_guard awl(aio_write_list_lock);
  assert(c->io == this);
  c->aio_write_seq = ++aio_write_seq;
  ldout(client->cct, 20) << "queue_aio_write " << this << " completion " << c
			 << " write_seq " << aio_write_seq << dendl;
  aio_write_list.push_back(&c->aio_write_list_item);
}

void librados::IoCtxImpl::complete_aio_write(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "complete_aio_write " << c << dendl;
  lock_guard awl(aio_write_list_lock);
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

  aio_write_cond.notify_all();
  put();
}

void librados::IoCtxImpl::flush_aio_writes_async(AioCompletionImpl *c)
{
  ldout(client->cct, 20) << "flush_aio_writes_async " << this
			 << " completion " << c << dendl;
  lock_guard awl(aio_write_list_lock);
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
  unique_lock awl(aio_write_list_lock);
  ceph_tid_t seq = aio_write_seq;
  while (!aio_write_list.empty() &&
	 aio_write_list.front()->aio_write_seq <= seq)
    aio_write_cond.wait(awl);
  awl.unlock();
}

// IO

int librados::IoCtxImpl::create(const oid_t& oid, bool exclusive)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->create(exclusive);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::create(const oid_t& oid, bool exclusive,
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
    map<oid_t,uint64_t>::iterator p = assert_src_version.begin();
    op->assert_src_version(p->first, p->second);
    assert_src_version.erase(p);
  }
  return op;
}

int librados::IoCtxImpl::write(const oid_t& oid, bufferlist& bl,
			       size_t len, uint64_t off)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  bufferlist mybl;
  mybl.substr_of(bl, 0, len);
  op->write(off, mybl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::append(const oid_t& oid, bufferlist& bl, size_t len)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  bufferlist mybl;
  mybl.substr_of(bl, 0, len);
  op->append(mybl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::write_full(const oid_t& oid, bufferlist& bl)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->write_full(bl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::operate(const oid_t& oid, unique_ptr<ObjOp>& o,
				 time_t *pmtime, int flags)
{
  auto ut = pmtime ?
    ceph::real_clock::from_time_t(*pmtime) :
    ceph::real_clock::now();

  if (!o->size())
    return 0;

  OSDC::CB_Waiter w;
  version_t ver;

  Objecter::Op *objecter_op = objecter->prepare_mutate_op(
    oid, volume, o, ut, flags, NULL, std::ref(w), &ver);

  objecter->op_submit(objecter_op);

  int r = w.wait();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::operate_read(const oid_t& oid,
				      unique_ptr<ObjOp>& o,
				      bufferlist *pbl,
				      int flags)
{
  if (!o->size())
    return 0;

  OSDC::CB_Waiter w;
  version_t ver;

  Objecter::Op *objecter_op = objecter->prepare_read_op(oid, volume,
							o, pbl, flags,
							std::ref(w), &ver);
  objecter->op_submit(objecter_op);

  int r = w.wait();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::aio_operate_read(const oid_t &oid,
					  unique_ptr<ObjOp>& o,
					  AioCompletionImpl *c,
					  int flags,
					  bufferlist *pbl)
{
  c->is_read = true;
  c->io = this;

  Objecter::Op *objecter_op = objecter->prepare_read_op(oid, volume,
							o, pbl, flags,
							CB_aio_Ack(c),
							&c->objver);
  objecter->op_submit(objecter_op);
  return 0;
}

int librados::IoCtxImpl::aio_operate(const oid_t& oid,
				     unique_ptr<ObjOp>& o,
				     AioCompletionImpl *c,
				     int flags)
{
  auto ut = ceph::real_clock::now();

  c->io = this;
  queue_aio_write(c);

  objecter->mutate(oid, volume, o, ut, flags, CB_aio_Ack(c),
		   CB_aio_Safe(c), &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_read(const oid_t oid, AioCompletionImpl *c,
				  bufferlist *pbl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  c->is_read = true;
  c->io = this;
  c->blp = pbl;

  objecter->read(oid, volume, off, len, pbl, 0, CB_aio_Ack(c), &c->objver);
  return 0;
}

int librados::IoCtxImpl::aio_read(const oid_t oid, AioCompletionImpl *c,
				  char *buf, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  c->is_read = true;
  c->io = this;
  c->bl.clear();
  c->bl.push_back(buffer::create_static(len, buf));
  c->blp = &c->bl;

  objecter->read(oid, volume, off, len, &c->bl, 0, CB_aio_Ack(c), &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_write(const oid_t &oid, AioCompletionImpl *c,
				   const bufferlist& bl, size_t len,
				   uint64_t off)
{
  auto ut = ceph::real_clock::now();
  ldout(client->cct, 20) << "aio_write " << oid << " " << off << "~"
			 << len << dendl;

  c->io = this;
  queue_aio_write(c);

  objecter->write(oid, volume, off, len, bl, ut, 0, CB_aio_Ack(c),
		  CB_aio_Ack(c), &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_append(const oid_t &oid, AioCompletionImpl *c,
				    const bufferlist& bl, size_t len)
{
  auto ut = ceph::real_clock::now();

  c->io = this;
  queue_aio_write(c);

  objecter->append(oid, volume, len, bl, ut, 0, CB_aio_Ack(c), CB_aio_Ack(c),
		   &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_write_full(const oid_t &oid,
					AioCompletionImpl *c,
					const bufferlist& bl)
{
  auto ut = ceph::real_clock::now();

  c->io = this;
  queue_aio_write(c);

  objecter->write_full(oid, volume, bl, ut, 0, CB_aio_Ack(c), CB_aio_Safe(c),
		       &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_remove(const oid_t &oid, AioCompletionImpl *c)
{
  auto ut = ceph::real_clock::now();

  c->io = this;
  queue_aio_write(c);

  objecter->remove(oid, volume, ut, 0, CB_aio_Ack(c), CB_aio_Safe(c),
		   &c->objver);

  return 0;
}


int librados::IoCtxImpl::aio_stat(const oid_t& oid, AioCompletionImpl *c,
				  uint64_t *psize, time_t *pmtime)
{
  c->io = this;

  objecter->stat(oid, volume, 0,
		 CB_aio_stat_Ack(c, psize, pmtime),
		 &c->objver);

  return 0;
}

int librados::IoCtxImpl::remove(const oid_t& oid)
{
  std::unique_ptr<ObjOp> op = prepare_assert_ops();
  op->remove();
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::trunc(const oid_t& oid, uint64_t size)
{
  std::unique_ptr<ObjOp> op = prepare_assert_ops();
  op->truncate(size);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::exec(const oid_t& oid,
			      const char *cls, const char *method,
			      bufferlist& inbl, bufferlist& outbl)
{
  std::unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->call(cls, method, inbl);
  return operate_read(oid, rd, &outbl);
}

int librados::IoCtxImpl::aio_exec(const oid_t& oid, AioCompletionImpl *c,
				  const char *cls, const char *method,
				  bufferlist& inbl, bufferlist *outbl)
{
  c->is_read = true;
  c->io = this;

  std::unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->call(cls, method, inbl);
  objecter->read(oid, volume, rd, outbl, 0, CB_aio_Ack(c), &c->objver);

  return 0;
}

int librados::IoCtxImpl::read(const oid_t& oid,
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

int librados::IoCtxImpl::sparse_read(const oid_t& oid,
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

int librados::IoCtxImpl::stat(const oid_t& oid, uint64_t *psize,
			      time_t *pmtime)
{
  uint64_t size;
  ceph::real_time mtime;

  if (!psize)
    psize = &size;

  unique_ptr<ObjOp> rd =  prepare_assert_ops();
  rd->stat(psize, &mtime, NULL);
  int r = operate_read(oid, rd, NULL);

  if (r >= 0 && pmtime) {
    *pmtime = ceph::real_clock::to_time_t(mtime);
  }

  return r;
}

int librados::IoCtxImpl::getxattr(const oid_t& oid,
				    const char *name, bufferlist& bl)
{
  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->getxattr(name, &bl, NULL);
  int r = operate_read(oid, rd, NULL);
  if (r < 0)
    return r;

  return bl.length();
}

int librados::IoCtxImpl::rmxattr(const oid_t& oid, const char *name)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->rmxattr(name);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::setxattr(const oid_t& oid,
				    const char *name, bufferlist& bl)
{
  unique_ptr<ObjOp> op = prepare_assert_ops();
  op->setxattr(name, bl);
  return operate(oid, op, NULL);
}

int librados::IoCtxImpl::getxattrs(const oid_t& oid,
				   map<std::string, bufferlist>& attrset)
{
  map<string, bufferlist> aset;

  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->getxattrs(aset, NULL);
  int r = operate_read(oid, rd, NULL);

  attrset.clear();
  if (r >= 0) {
    for (const auto& p : aset) {
      ldout(client->cct, 10) << "IoCtxImpl::getxattrs: xattr=" << p.first
			     << dendl;
      attrset[p.first.c_str()] = p.second;
    }
  }

  return r;
}

void librados::IoCtxImpl::set_sync_op_version(version_t ver)
{
  last_objver = ver;
}

int librados::IoCtxImpl::watch(const oid_t& oid, uint64_t ver,
			       uint64_t *cookie, librados::WatchCtx *ctx)
{
  unique_ptr<ObjOp> wr = prepare_assert_ops();
  std::mutex mylock;
  std::condition_variable cond;
  bool done;
  int r;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
  version_t objver;

  WatchNotifyInfo *wc = new WatchNotifyInfo(this, oid);
  wc->watch_ctx = ctx;
  client->register_watch_notify_callback(wc, cookie);
  wr->watch(*cookie, ver, 1);
  bufferlist bl;
  wc->linger_id = objecter->linger_mutate(oid, volume, wr,
					  ceph::real_clock::now(), bl,
					  0, NULL, onfinish, &objver);
  unique_lock l(mylock);
  cond.wait(l, [&](){ return done; });
  l.unlock();

  set_sync_op_version(objver);

  if (r < 0) {
    client->unregister_watch_notify_callback(*cookie); // destroys wc
  }

  return r;
}


/* this is called with IoCtxImpl::lock held */
int librados::IoCtxImpl::_notify_ack(
  const oid_t& oid,
  uint64_t notify_id, uint64_t ver,
  uint64_t cookie)
{
  unique_ptr<ObjOp> rd = prepare_assert_ops();
  rd->notify_ack(notify_id, ver, cookie);
  objecter->read(oid, volume, rd, (bufferlist*)NULL, 0, 0, 0);

  return 0;
}

int librados::IoCtxImpl::unwatch(const oid_t& oid, uint64_t cookie)
{
  bufferlist inbl, outbl;

  OSDC::CB_Waiter w;
  version_t ver;

  client->unregister_watch_notify_callback(cookie); // destroys wc

  unique_ptr<ObjOp> wr = prepare_assert_ops();
  wr->watch(cookie, 0, 0);
  objecter->mutate(oid, volume, wr, ceph::real_clock::now(),
		   0, NULL, std::ref(w), &ver);

  int r = w.wait();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::notify(const oid_t& oid, bufferlist& bl)
{
  bufferlist inbl, outbl;
  // Construct WatchNotifyInfo
  std::condition_variable cond_all;
  std::mutex mylock_all;
  bool done_all = false;
  int r_notify = 0;
  unique_ptr<ObjOp> rd = prepare_assert_ops();

  WatchNotifyInfo *wc = new WatchNotifyInfo(this, oid);
  wc->notify_done = &done_all;
  wc->notify_lock = &mylock_all;
  wc->notify_cond = &cond_all;
  wc->notify_rval = &r_notify;

  // Acquire cookie
  uint64_t cookie;
  client->register_watch_notify_callback(wc, &cookie);
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

  int r_issue = onack.wait();

  if (r_issue == 0) {
    unique_lock mla(mylock_all);
    cond_all.wait(mla, [&](){ return done_all; });
    mla.unlock();
  }

  client->unregister_watch_notify_callback(cookie);   // destroys wc

  set_sync_op_version(objver);

  return r_issue == 0 ? r_notify : r_issue;
}

int librados::IoCtxImpl::set_alloc_hint(const oid_t& oid,
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
void librados::IoCtxImpl::set_assert_src_version(const oid_t& oid,
						 uint64_t ver)
{
  assert_src_version[oid] = ver;
}

void librados::IoCtxImpl::set_notify_timeout(uint32_t timeout)
{
  notify_timeout = timeout;
}

///////////////////////////// CB_aio_Ack ////////////////////////////////

librados::IoCtxImpl::CB_aio_Ack::CB_aio_Ack(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::CB_aio_Ack::operator()(int r)
{
  unique_lock cl(c->lock);
  c->rval = r;
  c->ack = true;
  if (c->is_read)
    c->safe = true;
  c->cond.notify_all();

  if (r == 0 && c->blp && c->blp->length() > 0) {
    c->rval = c->blp->length();
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }
  if (c->is_read && c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->put_unlock(cl);
}

///////////////////////////// CB_aio_stat_Ack ////////////////////////////

librados::IoCtxImpl::CB_aio_stat_Ack::CB_aio_stat_Ack(AioCompletionImpl *_c,
						      uint64_t* ps,
						      time_t* pm)
  : c(_c), psize(ps), pmtime(pm)
{
  c->get();
}

void librados::IoCtxImpl::CB_aio_stat_Ack::operator()(int r, uint64_t s,
						      ceph::real_time t)
{
  unique_lock cl(c->lock);
  c->rval = r;
  c->ack = true;
  c->cond.notify_all();

  if (r >= 0 && pmtime) {
    *pmtime = ceph::real_clock::to_time_t(t);
  }

  if (r >= 0 && psize) {
    *psize = s;
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }

  c->put_unlock(cl);
}

//////////////////////////// CB_aio_Safe ////////////////////////////////

librados::IoCtxImpl::CB_aio_Safe::CB_aio_Safe(AioCompletionImpl *_c) : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::CB_aio_Safe::operator()(int r)
{
  unique_lock cl(c->lock);
  if (!c->ack) {
    c->rval = r;
    c->ack = true;
  }
  c->safe = true;
  c->cond.notify_all();

  if (c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->io->complete_aio_write(c);

  c->put_unlock(cl);
}

///////////////////////// C_NotifyComplete /////////////////////////////

librados::IoCtxImpl::C_NotifyComplete::C_NotifyComplete(
  std::mutex *_l, std::condition_variable *_c, bool *_d)
  : lock(_l), cond(_c), done(_d)
{
  *done = false;
}

void librados::IoCtxImpl::C_NotifyComplete::notify(uint8_t opcode,
						   uint64_t ver,
						   bufferlist& bl)
{
  lock_guard l(*lock);
  *done = true;
  cond->notify_all();
}

