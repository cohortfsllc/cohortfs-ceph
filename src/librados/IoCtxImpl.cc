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

#include "IoCtxImpl.h"

#include "librados/AioCompletionImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"

#include <limits.h>

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

librados::IoCtxImpl::IoCtxImpl()
  : aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock")
{
}

librados::IoCtxImpl::IoCtxImpl(RadosClient *c, Objecter *objecter,
			       Mutex *client_lock, int poolid,
			       const char *pool_name, snapid_t s)
  : ref_cnt(0), client(c), poolid(poolid), pool_name(pool_name), snap_seq(s),
    assert_ver(0), notify_timeout(c->cct->_conf->client_notify_timeout),
    oloc(poolid),
    aio_write_list_lock("librados::IoCtxImpl::aio_write_list_lock"),
    aio_write_seq(0), lock(client_lock), objecter(objecter)
{
}

void librados::IoCtxImpl::set_snap_read(snapid_t s)
{
  if (!s)
    s = CEPH_NOSNAP;
  ldout(client->cct, 10) << "set snap read " << snap_seq << " -> " << s << dendl;
  snap_seq = s;
}

int librados::IoCtxImpl::set_snap_write_context(snapid_t seq, vector<snapid_t>& snaps)
{
  ::SnapContext n;
  ldout(client->cct, 10) << "set snap write context: seq = " << seq
			 << " and snaps = " << snaps << dendl;
  n.seq = seq;
  n.snaps = snaps;
  if (!n.is_valid())
    return -EINVAL;
  snapc = n;
  return 0;
}

void librados::IoCtxImpl::queue_aio_write(AioCompletionImpl *c)
{
  get();
  aio_write_list_lock.Lock();
  assert(c->io == this);
  c->aio_write_seq = ++aio_write_seq;
  aio_write_list.push_back(&c->aio_write_list_item);
  aio_write_list_lock.Unlock();
}

void librados::IoCtxImpl::complete_aio_write(AioCompletionImpl *c)
{
  aio_write_list_lock.Lock();
  assert(c->io == this);
  c->aio_write_list_item.remove_myself();
  aio_write_cond.Signal();
  aio_write_list_lock.Unlock();
  put();
}

void librados::IoCtxImpl::flush_aio_writes()
{
  aio_write_list_lock.Lock();
  tid_t seq = aio_write_seq;
  while (!aio_write_list.empty() &&
	 aio_write_list.front()->aio_write_seq <= seq)
    aio_write_cond.Wait(aio_write_list_lock);
  aio_write_list_lock.Unlock();
}

// SNAPS

int librados::IoCtxImpl::snap_create(const char *snapName)
{
  int reply;
  string sName(snapName);

  Mutex mylock ("IoCtxImpl::snap_create::mylock");
  Cond cond;
  bool done;
  lock->Lock();
  objecter->create_pool_snap(poolid,
			     sName,
			     new C_SafeCond(&mylock, &cond, &done, &reply));
  lock->Unlock();

  mylock.Lock();
  while(!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::IoCtxImpl::selfmanaged_snap_create(uint64_t *psnapid)
{
  int reply;

  Mutex mylock("IoCtxImpl::selfmanaged_snap_create::mylock");
  Cond cond;
  bool done;
  lock->Lock();
  snapid_t snapid;
  objecter->allocate_selfmanaged_snap(poolid, &snapid,
				      new C_SafeCond(&mylock, &cond, &done, &reply));
  lock->Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  if (reply == 0)
    *psnapid = snapid;
  return reply;
}

int librados::IoCtxImpl::snap_remove(const char *snapName)
{
  int reply;
  string sName(snapName);

  Mutex mylock ("IoCtxImpl::snap_remove::mylock");
  Cond cond;
  bool done;
  lock->Lock();
  objecter->delete_pool_snap(poolid,
			     sName,
			     new C_SafeCond(&mylock, &cond, &done, &reply));
  lock->Unlock();

  mylock.Lock();
  while(!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::IoCtxImpl::selfmanaged_snap_rollback_object(const object_t& oid,
							  ::SnapContext& snapc,
							  uint64_t snapid)
{
  int reply;

  Mutex mylock("IoCtxImpl::snap_rollback::mylock");
  Cond cond;
  bool done;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &reply);

  lock->Lock();
  objecter->rollback_object(oid, oloc, snapc, snapid,
			    ceph_clock_now(client->cct), onack, NULL);
  lock->Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::IoCtxImpl::rollback(const object_t& oid, const char *snapName)
{
  string sName(snapName);

  lock->Lock();
  snapid_t snap;
  const map<int64_t, pg_pool_t>& pools = objecter->osdmap->get_pools();
  const pg_pool_t& pg_pool = pools.find(poolid)->second;
  map<snapid_t, pool_snap_info_t>::const_iterator p;
  for (p = pg_pool.snaps.begin();
       p != pg_pool.snaps.end();
       ++p) {
    if (p->second.name == snapName) {
      snap = p->first;
      break;
    }
  }
  if (p == pg_pool.snaps.end()) {
    lock->Unlock();
    return -ENOENT;
  }
  lock->Unlock();

  return selfmanaged_snap_rollback_object(oid, snapc, snap);
}

int librados::IoCtxImpl::selfmanaged_snap_remove(uint64_t snapid)
{
  int reply;

  Mutex mylock("IoCtxImpl::selfmanaged_snap_remove::mylock");
  Cond cond;
  bool done;
  lock->Lock();
  objecter->delete_selfmanaged_snap(poolid, snapid_t(snapid),
				    new C_SafeCond(&mylock, &cond, &done, &reply));
  lock->Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return (int)reply;
}

int librados::IoCtxImpl::pool_change_auid(unsigned long long auid)
{
  int reply;

  Mutex mylock("IoCtxImpl::pool_change_auid::mylock");
  Cond cond;
  bool done;
  lock->Lock();
  objecter->change_pool_auid(poolid,
			     new C_SafeCond(&mylock, &cond, &done, &reply),
			     auid);
  lock->Unlock();

  mylock.Lock();
  while (!done) cond.Wait(mylock);
  mylock.Unlock();
  return reply;
}

int librados::IoCtxImpl::pool_change_auid_async(unsigned long long auid,
						  PoolAsyncCompletionImpl *c)
{
  Mutex::Locker l(*lock);
  objecter->change_pool_auid(poolid,
			     new C_PoolAsync_Safe(c),
			     auid);
  return 0;
}

int librados::IoCtxImpl::snap_list(vector<uint64_t> *snaps)
{
  Mutex::Locker l(*lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.begin();
       p != pi->snaps.end();
       p++)
    snaps->push_back(p->first);
  return 0;
}

int librados::IoCtxImpl::snap_lookup(const char *name, uint64_t *snapid)
{
  Mutex::Locker l(*lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(poolid);
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.begin();
       p != pi->snaps.end();
       p++) {
    if (p->second.name == name) {
      *snapid = p->first;
      return 0;
    }
  }
  return -ENOENT;
}

int librados::IoCtxImpl::snap_get_name(uint64_t snapid, std::string *s)
{
  Mutex::Locker l(*lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.find(snapid);
  if (p == pi->snaps.end())
    return -ENOENT;
  *s = p->second.name.c_str();
  return 0;
}

int librados::IoCtxImpl::snap_get_stamp(uint64_t snapid, time_t *t)
{
  Mutex::Locker l(*lock);
  const pg_pool_t *pi = objecter->osdmap->get_pg_pool(poolid);
  map<snapid_t,pool_snap_info_t>::const_iterator p = pi->snaps.find(snapid);
  if (p == pi->snaps.end())
    return -ENOENT;
  *t = p->second.stamp.sec();
  return 0;
}


// IO

int librados::IoCtxImpl::list(Objecter::ListContext *context, int max_entries)
{
  Cond cond;
  bool done;
  int r = 0;
  object_t oid;
  Mutex mylock("IoCtxImpl::list::mylock");

  if (context->at_end)
    return 0;

  context->max_entries = max_entries;

  lock->Lock();
  objecter->list_objects(context, new C_SafeCond(&mylock, &cond, &done, &r));
  lock->Unlock();

  mylock.Lock();
  while(!done)
    cond.Wait(mylock);
  mylock.Unlock();

  return r;
}

int librados::IoCtxImpl::create(const object_t& oid, bool exclusive)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::create::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock->Lock();
  objecter->create(oid, oloc,
		  snapc, ut, 0, (exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0),
		  onack, NULL, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::create(const object_t& oid, bool exclusive,
				const std::string& category)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::create::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  ::ObjectOperation o;
  o.create(exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0, category);

  lock->Lock();
  objecter->mutate(oid, oloc, o, snapc, ut, 0, onack, NULL, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
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
::ObjectOperation *librados::IoCtxImpl::prepare_assert_ops(::ObjectOperation *op)
{
  ::ObjectOperation *pop = NULL;
  if (assert_ver) {
    op->assert_version(assert_ver);
    assert_ver = 0;
    pop = op;
  }
  while (!assert_src_version.empty()) {
    map<object_t,uint64_t>::iterator p = assert_src_version.begin();
    op->assert_src_version(p->first, CEPH_NOSNAP, p->second);
    assert_src_version.erase(p);
    pop = op;
  }
  return pop;
}

int librados::IoCtxImpl::write(const object_t& oid, bufferlist& bl,
			       size_t len, uint64_t off)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::write::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  // extra ops?
  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->write(oid, oloc,
		  off, len, snapc, bl, ut, 0,
		  onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  if (r < 0)
    return r;

  return len;
}

int librados::IoCtxImpl::append(const object_t& oid, bufferlist& bl, size_t len)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::append::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->append(oid, oloc,
		   len, snapc, bl, ut, 0,
		   onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  if (r < 0)
    return r;

  return len;
}

int librados::IoCtxImpl::write_full(const object_t& oid, bufferlist& bl)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->write_full(oid, oloc,
		       snapc, bl, ut, 0,
		       onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::clone_range(const object_t& dst_oid,
				     uint64_t dst_offset,
				     const object_t& src_oid,
				     uint64_t src_offset,
				     uint64_t len)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::clone_range::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock->Lock();
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.clone_range(src_oid, src_offset, len, dst_offset);
  objecter->mutate(dst_oid, oloc, wr, snapc, ut, 0, onack, NULL, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::operate(const object_t& oid, ::ObjectOperation *o,
				 time_t *pmtime)
{
  utime_t ut;
  if (pmtime) {
    ut = utime_t(*pmtime, 0);
  } else {
    ut = ceph_clock_now(client->cct);
  }

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  if (!o->size())
    return 0;

  Mutex mylock("IoCtxImpl::mutate::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock->Lock();
  objecter->mutate(oid, oloc,
	           *o, snapc, ut, 0,
	           onack, NULL, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::operate_read(const object_t& oid,
				      ::ObjectOperation *o, bufferlist *pbl)
{
  if (!o->size())
    return 0;

  Mutex mylock("IoCtxImpl::mutate::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock->Lock();
  objecter->read(oid, oloc,
	           *o, snap_seq, pbl, 0,
	           onack, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::aio_operate_read(const object_t &oid,
					  ::ObjectOperation *o,
					  AioCompletionImpl *c, bufferlist *pbl)
{
  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;
  c->pbl = pbl;

  Mutex::Locker l(*lock);
  objecter->read(oid, oloc,
		 *o, snap_seq, pbl, 0,
		 onack, 0);
  return 0;
}

int librados::IoCtxImpl::aio_operate(const object_t& oid,
				     ::ObjectOperation *o, AioCompletionImpl *c)
{
  utime_t ut = ceph_clock_now(client->cct);
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Context *onack = new C_aio_Ack(c);
  Context *oncommit = new C_aio_Safe(c);

  c->io = this;
  queue_aio_write(c);

  Mutex::Locker l(*lock);
  objecter->mutate(oid, oloc, *o, snapc, ut, 0, onack, oncommit, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_read(const object_t oid, AioCompletionImpl *c,
				  bufferlist *pbl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Context *onack = new C_aio_Ack(c);
  eversion_t ver;

  c->is_read = true;
  c->io = this;
  c->pbl = pbl;

  Mutex::Locker l(*lock);
  objecter->read(oid, oloc,
		 off, len, snap_seq, &c->bl, 0,
		 onack, &c->objver);
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
  c->buf = buf;
  c->maxlen = len;

  Mutex::Locker l(*lock);
  objecter->read(oid, oloc,
		 off, len, snap_seq, &c->bl, 0,
		 onack, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_sparse_read(const object_t oid,
					 AioCompletionImpl *c,
					 std::map<uint64_t,uint64_t> *m,
					 bufferlist *data_bl, size_t len,
					 uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  C_aio_sparse_read_Ack *onack = new C_aio_sparse_read_Ack(c);
  onack->m = m;
  onack->data_bl = data_bl;
  eversion_t ver;

  c->io = this;
  c->pbl = NULL;

  Mutex::Locker l(*lock);
  objecter->sparse_read(oid, oloc,
		 off, len, snap_seq, &c->bl, 0,
		 onack);
  return 0;
}

int librados::IoCtxImpl::aio_write(const object_t &oid, AioCompletionImpl *c,
				   const bufferlist& bl, size_t len,
				   uint64_t off)
{
  utime_t ut = ceph_clock_now(client->cct);
  ldout(client->cct, 20) << "aio_write " << oid << " " << off << "~" << len << " snapc=" << snapc << " snap_seq=" << snap_seq << dendl;

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->write(oid, oloc,
		  off, len, snapc, bl, ut, 0,
		  onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_append(const object_t &oid, AioCompletionImpl *c,
				    const bufferlist& bl, size_t len)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->append(oid, oloc,
		   len, snapc, bl, ut, 0,
		   onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::aio_write_full(const object_t &oid,
					AioCompletionImpl *c,
					const bufferlist& bl)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  c->io = this;
  queue_aio_write(c);

  Context *onack = new C_aio_Ack(c);
  Context *onsafe = new C_aio_Safe(c);

  Mutex::Locker l(*lock);
  objecter->write_full(oid, oloc,
		       snapc, bl, ut, 0,
		       onack, onsafe, &c->objver);

  return 0;
}

int librados::IoCtxImpl::remove(const object_t& oid)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::remove::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->remove(oid, oloc,
		   snapc, ut, 0,
		   onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::trunc(const object_t& oid, uint64_t size)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::write_full::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->trunc(oid, oloc,
		  snapc, ut, 0,
		  size, 0,
		  onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::tmap_update(const object_t& oid, bufferlist& cmdbl)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::tmap_update::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock->Lock();
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.tmap_update(cmdbl);
  objecter->mutate(oid, oloc, wr, snapc, ut, 0, onack, NULL, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::tmap_put(const object_t& oid, bufferlist& bl)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::tmap_put::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock->Lock();
  ::ObjectOperation wr;
  prepare_assert_ops(&wr);
  wr.tmap_put(bl);
  objecter->mutate(oid, oloc, wr, snapc, ut, 0, onack, NULL, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::tmap_get(const object_t& oid, bufferlist& bl)
{
  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::tmap_put::mylock");
  Cond cond;
  bool done;
  int r = 0;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  bufferlist outbl;

  lock->Lock();
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.tmap_get(&bl, NULL);
  objecter->read(oid, oloc, rd, snap_seq, 0, 0, onack, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}


int librados::IoCtxImpl::exec(const object_t& oid,
			      const char *cls, const char *method,
			      bufferlist& inbl, bufferlist& outbl)
{
  Mutex mylock("IoCtxImpl::exec::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;


  lock->Lock();
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.call(cls, method, inbl);
  objecter->read(oid, oloc, rd, snap_seq, &outbl, 0, onack, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::aio_exec(const object_t& oid, AioCompletionImpl *c,
				  const char *cls, const char *method,
				  bufferlist& inbl, bufferlist *outbl)
{
  Context *onack = new C_aio_Ack(c);

  c->is_read = true;
  c->io = this;

  Mutex::Locker l(*lock);
  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.call(cls, method, inbl);
  objecter->read(oid, oloc, rd, snap_seq, outbl, 0, onack, &c->objver);

  return 0;
}

int librados::IoCtxImpl::read(const object_t& oid,
			      bufferlist& bl, size_t len, uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  Mutex mylock("IoCtxImpl::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;


  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->read(oid, oloc,
		 off, len, snap_seq, &bl, 0,
		 onack, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from read r=" << r << dendl;

  set_sync_op_version(ver);

  if (r < 0)
    return r;

  if (bl.length() < len) {
    ldout(client->cct, 10) << "Returned length " << bl.length()
	     << " less than original length "<< len << dendl;
  }

  return bl.length();
}

int librados::IoCtxImpl::mapext(const object_t& oid,
				uint64_t off, size_t len,
				std::map<uint64_t,uint64_t>& m)
{
  bufferlist bl;

  Mutex mylock("IoCtxImpl::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock->Lock();
  objecter->mapext(oid, oloc,
		   off, len, snap_seq, &bl, 0,
		   onack);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);

  return m.size();
}

int librados::IoCtxImpl::sparse_read(const object_t& oid,
				     std::map<uint64_t,uint64_t>& m,
				     bufferlist& data_bl, size_t len,
				     uint64_t off)
{
  if (len > (size_t) INT_MAX)
    return -EDOM;

  bufferlist bl;

  Mutex mylock("IoCtxImpl::read::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock->Lock();
  objecter->sparse_read(oid, oloc,
			off, len, snap_seq, &bl, 0,
			onack);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from read r=" << r << dendl;

  if (r < 0)
    return r;

  bufferlist::iterator iter = bl.begin();
  ::decode(m, iter);
  ::decode(data_bl, iter);

  return m.size();
}

int librados::IoCtxImpl::stat(const object_t& oid, uint64_t *psize, time_t *pmtime)
{
  Mutex mylock("IoCtxImpl::stat::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  uint64_t size;
  utime_t mtime;
  eversion_t ver;

  if (!psize)
    psize = &size;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->stat(oid, oloc,
		 snap_seq, psize, &mtime, 0,
		 onack, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from stat" << dendl;

  if (r >= 0 && pmtime) {
    *pmtime = mtime.sec();
  }

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::getxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  Mutex mylock("IoCtxImpl::getxattr::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->getxattr(oid, oloc,
		     name, snap_seq, &bl, 0,
		     onack, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();
  ldout(client->cct, 10) << "Objecter returned from getxattr" << dendl;

  set_sync_op_version(ver);

  if (r < 0)
    return r;

  return bl.length();
}

int librados::IoCtxImpl::rmxattr(const object_t& oid, const char *name)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::rmxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->removexattr(oid, oloc, name,
			snapc, ut, 0,
			onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  if (r < 0)
    return r;

  return 0;
}

int librados::IoCtxImpl::setxattr(const object_t& oid,
				    const char *name, bufferlist& bl)
{
  utime_t ut = ceph_clock_now(client->cct);

  /* can't write to a snapshot */
  if (snap_seq != CEPH_NOSNAP)
    return -EROFS;

  Mutex mylock("IoCtxImpl::setxattr::mylock");
  Cond cond;
  bool done;
  int r;

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  lock->Lock();
  objecter->setxattr(oid, oloc, name,
		     snapc, bl, ut, 0,
		     onack, NULL, &ver, pop);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  if (r < 0)
    return r;

  return 0;
}

int librados::IoCtxImpl::getxattrs(const object_t& oid,
				     map<std::string, bufferlist>& attrset)
{
  Mutex mylock("IoCtxImpl::getexattrs::mylock");
  Cond cond;
  bool done;
  int r;
  eversion_t ver;

  ::ObjectOperation op;
  ::ObjectOperation *pop = prepare_assert_ops(&op);

  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);

  lock->Lock();
  map<string, bufferlist> aset;
  objecter->getxattrs(oid, oloc, snap_seq,
		      aset,
		      0, onack, &ver, pop);
  lock->Unlock();

  attrset.clear();


  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  for (map<string,bufferlist>::iterator p = aset.begin(); p != aset.end(); p++) {
    ldout(client->cct, 10) << "IoCtxImpl::getxattrs: xattr=" << p->first << dendl;
    attrset[p->first.c_str()] = p->second;
  }

  set_sync_op_version(ver);

  return r;
}

void librados::IoCtxImpl::set_sync_op_version(eversion_t& ver)
{
  last_objver = ver;
}

int librados::IoCtxImpl::watch(const object_t& oid, uint64_t ver,
			       uint64_t *cookie, librados::WatchCtx *ctx)
{
  ::ObjectOperation rd;
  Mutex mylock("IoCtxImpl::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t objver;

  lock->Lock();

  WatchContext *wc = new WatchContext(this, oid, ctx);
  client->register_watcher(wc, oid, ctx, cookie);
  prepare_assert_ops(&rd);
  rd.watch(*cookie, ver, 1);
  bufferlist bl;
  wc->linger_id = objecter->linger(oid, oloc, rd, snap_seq, bl, NULL, 0,
				   NULL, onfinish, &objver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(objver);

  if (r < 0) {
    lock->Lock();
    client->unregister_watcher(*cookie);
    lock->Unlock();
  }

  return r;
}


/* this is called with IoCtxImpl::lock held */
int librados::IoCtxImpl::_notify_ack(const object_t& oid,
				     uint64_t notify_id, uint64_t ver)
{
  Mutex mylock("IoCtxImpl::watch::mylock");
  Cond cond;
  eversion_t objver;

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.notify_ack(notify_id, ver);
  objecter->read(oid, oloc, rd, snap_seq, (bufferlist*)NULL, 0, 0, 0);

  return 0;
}

int librados::IoCtxImpl::unwatch(const object_t& oid, uint64_t cookie)
{
  bufferlist inbl, outbl;

  Mutex mylock("IoCtxImpl::watch::mylock");
  Cond cond;
  bool done;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t ver;
  lock->Lock();

  client->unregister_watcher(cookie);

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);
  rd.watch(cookie, 0, 0);
  objecter->read(oid, oloc, rd, snap_seq, &outbl, 0, onack, &ver);
  lock->Unlock();

  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  set_sync_op_version(ver);

  return r;
}

int librados::IoCtxImpl::notify(const object_t& oid, uint64_t ver, bufferlist& bl)
{
  bufferlist inbl, outbl;

  Mutex mylock("IoCtxImpl::notify::mylock");
  Mutex mylock_all("IoCtxImpl::notify::mylock_all");
  Cond cond, cond_all;
  bool done, done_all;
  int r;
  Context *onack = new C_SafeCond(&mylock, &cond, &done, &r);
  eversion_t objver;
  uint64_t cookie;
  C_NotifyComplete *ctx = new C_NotifyComplete(&mylock_all, &cond_all, &done_all);

  ::ObjectOperation rd;
  prepare_assert_ops(&rd);

  lock->Lock();
  WatchContext *wc = new WatchContext(this, oid, ctx);
  client->register_watcher(wc, oid, ctx, &cookie);
  uint32_t prot_ver = 1;
  uint32_t timeout = notify_timeout;
  ::encode(prot_ver, inbl);
  ::encode(timeout, inbl);
  ::encode(bl, inbl);
  rd.notify(cookie, ver, inbl);
  wc->linger_id = objecter->linger(oid, oloc, rd, snap_seq, inbl, NULL,
				   0, onack, NULL, &objver);
  lock->Unlock();

  mylock_all.Lock();
  mylock.Lock();
  while (!done)
    cond.Wait(mylock);
  mylock.Unlock();

  if (r == 0) {
    while (!done_all)
      cond_all.Wait(mylock_all);
  }

  mylock_all.Unlock();

  lock->Lock();
  client->unregister_watcher(cookie);
  lock->Unlock();

  set_sync_op_version(objver);
  delete ctx;

  return r;
}

eversion_t librados::IoCtxImpl::last_version()
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

  if (c->buf && c->bl.length() > 0) {
    unsigned l = MIN(c->bl.length(), c->maxlen);
    c->bl.copy(0, l, c->buf);
    c->rval = c->bl.length();
  }
  if (c->pbl) {
    *c->pbl = c->bl;
  }

  if (c->callback_complete) {
    c->io->client->finisher.queue(new C_AioComplete(c));
  }
  if (c->is_read && c->callback_safe) {
    c->io->client->finisher.queue(new C_AioSafe(c));
  }

  c->put_unlock();
}

/////////////////////// C_aio_sparse_read_Ack //////////////////////////

librados::IoCtxImpl::C_aio_sparse_read_Ack::C_aio_sparse_read_Ack(AioCompletionImpl *_c)
  : c(_c)
{
  c->get();
}

void librados::IoCtxImpl::C_aio_sparse_read_Ack::finish(int r)
{
  c->lock.Lock();
  c->rval = r;
  c->ack = true;
  c->cond.Signal();

  bufferlist::iterator iter = c->bl.begin();
  if (r >= 0) {
    ::decode(*m, iter);
    ::decode(*data_bl, iter);
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
  *done = true;
  cond->Signal();
}

/////////////////////////// WatchContext ///////////////////////////////

librados::WatchContext::WatchContext(IoCtxImpl *io_ctx_impl_,
				     const object_t& _oc,
				     librados::WatchCtx *_ctx)
  : io_ctx_impl(io_ctx_impl_), oid(_oc), ctx(_ctx), linger_id(0)
{
  io_ctx_impl->get();
}

librados::WatchContext::~WatchContext()
{
  io_ctx_impl->put();
}

void librados::WatchContext::notify(Mutex *client_lock,
                                    uint8_t opcode,
				    uint64_t ver,
				    uint64_t notify_id,
				    bufferlist& payload)
{
  ctx->notify(opcode, ver, payload);
  if (opcode != WATCH_NOTIFY_COMPLETE) {
    client_lock->Lock();
    io_ctx_impl->_notify_ack(oid, notify_id, ver);
    client_lock->Unlock();
  }
}
