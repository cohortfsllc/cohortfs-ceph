// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "messages/MOSDOp.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg,
  coll_t coll,
  coll_t temp_coll,
  ObjectStore *store,
  CephContext *cct) :
  PGBackend(pg, store,
	    coll, temp_coll),
  cct(cct) {}

void ReplicatedBackend::_on_change(ObjectStore::Transaction *t)
{
  for (map<ceph_tid_t, InProgressOp>::iterator i = in_progress_ops.begin();
       i != in_progress_ops.end();
       in_progress_ops.erase(i++)) {
    if (i->second.on_commit)
      delete i->second.on_commit;
    if (i->second.on_applied)
      delete i->second.on_applied;
  }
}

void ReplicatedBackend::on_flushed()
{
  if (have_temp_coll() &&
      !store->collection_empty(get_temp_coll())) {
    vector<hobject_t> objects;
    store->collection_list(get_temp_coll(), objects);
    derr << __func__ << ": found objects in the temp collection: "
	 << objects << ", crashing now"
	 << dendl;
    assert(0 == "found garbage in the temp collection");
  }
}

int ReplicatedBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  bufferlist *bl)
{
  return store->read(coll, hoid, off, len, *bl);
}

struct AsyncReadCallback : public GenContext<ThreadPool::TPHandle&> {
  int r;
  Context *c;
  AsyncReadCallback(int r, Context *c) : r(r), c(c) {}
  void finish(ThreadPool::TPHandle&) {
    c->complete(r);
    c = NULL;
  }
  ~AsyncReadCallback() {
    delete c;
  }
};
void ReplicatedBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<pair<uint64_t, uint64_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete)
{
  int r = 0;
  for (list<pair<pair<uint64_t, uint64_t>,
		 pair<bufferlist*, Context*> > >::const_iterator i =
	   to_read.begin();
       i != to_read.end() && r >= 0;
       ++i) {
    int _r = store->read(coll, hoid, i->first.first,
			 i->first.second, *(i->second.first));
    if (i->second.second) {
      get_parent()->schedule_work(
	get_parent()->bless_gencontext(
	  new AsyncReadCallback(_r, i->second.second)));
    }
    if (_r < 0)
      r = _r;
  }
  get_parent()->schedule_work(
    get_parent()->bless_gencontext(
      new AsyncReadCallback(r, on_complete)));
}


class RPGTransaction : public PGBackend::PGTransaction {
  coll_t coll;
  coll_t temp_coll;
  set<hobject_t> temp_added;
  set<hobject_t> temp_cleared;
  ObjectStore::Transaction *t;
  const coll_t &get_coll_ct(const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_cleared.erase(hoid);
      temp_added.insert(hoid);
    }
    return get_coll(hoid);
  }
  const coll_t &get_coll_rm(const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_added.erase(hoid);
      temp_cleared.insert(hoid);
    }
    return get_coll(hoid);
  }
  const coll_t &get_coll(const hobject_t &hoid) {
    if (hoid.is_temp())
      return temp_coll;
    else
      return coll;
  }
public:
  RPGTransaction(coll_t coll, coll_t temp_coll)
    : coll(coll), temp_coll(temp_coll), t(new ObjectStore::Transaction)
    {}

  /// Yields ownership of contained transaction
  ObjectStore::Transaction *get_transaction() {
    ObjectStore::Transaction *_t = t;
    t = 0;
    return _t;
  }
  const set<hobject_t> &get_temp_added() {
    return temp_added;
  }
  const set<hobject_t> &get_temp_cleared() {
    return temp_cleared;
  }

  void write(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist &bl
    ) {
    t->write(get_coll_ct(hoid), hoid, off, len, bl);
  }
  void remove(
    const hobject_t &hoid
    ) {
    t->remove(get_coll_rm(hoid), hoid);
  }
  void stash(
    const hobject_t &hoid,
    version_t former_version) {
    t->collection_move_rename(coll, hoid, coll, hoid);
  }
  void setattrs(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs
    ) {
    t->setattrs(get_coll(hoid), hoid, attrs);
  }
  void setattr(
    const hobject_t &hoid,
    const string &attrname,
    bufferlist &bl
    ) {
    t->setattr(get_coll(hoid), hoid, attrname, bl);
  }
  void rmattr(
    const hobject_t &hoid,
    const string &attrname
    ) {
    t->rmattr(get_coll(hoid), hoid, attrname);
  }
  void omap_setkeys(
    const hobject_t &hoid,
    map<string, bufferlist> &keys
    ) {
    return t->omap_setkeys(get_coll(hoid), hoid, keys);
  }
  void omap_rmkeys(
    const hobject_t &hoid,
    set<string> &keys
    ) {
    t->omap_rmkeys(get_coll(hoid), hoid, keys);
  }
  void omap_clear(
    const hobject_t &hoid
    ) {
    t->omap_clear(get_coll(hoid), hoid);
  }
  void omap_setheader(
    const hobject_t &hoid,
    bufferlist &header
    ) {
    t->omap_setheader(get_coll(hoid), hoid, header);
  }
  void clone_range(
    const hobject_t &from,
    const hobject_t &to,
    uint64_t fromoff,
    uint64_t len,
    uint64_t tooff
    ) {
    assert(get_coll(from) == get_coll_ct(to)  && get_coll(from) == coll);
    t->clone_range(coll, from, to, fromoff, len, tooff);
  }
  void clone(
    const hobject_t &from,
    const hobject_t &to
    ) {
    assert(get_coll(from) == get_coll_ct(to)  && get_coll(from) == coll);
    t->clone(coll, from, to);
  }
  void rename(
    const hobject_t &from,
    const hobject_t &to
    ) {
    t->collection_move_rename(
      get_coll_rm(from),
      from,
      get_coll_ct(to),
      to);
  }

  void touch(
    const hobject_t &hoid
    ) {
    t->touch(get_coll_ct(hoid), hoid);
  }

  void truncate(
    const hobject_t &hoid,
    uint64_t off
    ) {
    t->truncate(get_coll(hoid), hoid, off);
  }
  void zero(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len
    ) {
    t->zero(get_coll(hoid), hoid, off, len);
  }

  void set_alloc_hint(
    const hobject_t &hoid,
    uint64_t expected_object_size,
    uint64_t expected_write_size
    ) {
    t->set_alloc_hint(get_coll(hoid), hoid, expected_object_size,
                      expected_write_size);
  }

  void append(
    PGTransaction *_to_append
    ) {
    RPGTransaction *to_append = dynamic_cast<RPGTransaction*>(_to_append);
    assert(to_append);
    t->append(*(to_append->t));
    for (set<hobject_t>::iterator i = to_append->temp_added.begin();
	 i != to_append->temp_added.end();
	 ++i) {
      temp_cleared.erase(*i);
      temp_added.insert(*i);
    }
    for (set<hobject_t>::iterator i = to_append->temp_cleared.begin();
	 i != to_append->temp_cleared.end();
	 ++i) {
      temp_added.erase(*i);
      temp_cleared.insert(*i);
    }
  }
  void nop() {
    t->nop();
  }
  bool empty() const {
    return t->empty();
  }
  uint64_t get_bytes_written() const {
    return t->get_encoded_bytes();
  }
  ~RPGTransaction() { delete t; }
};

PGBackend::PGTransaction *ReplicatedBackend::get_transaction()
{
  return new RPGTransaction(coll, get_temp_coll());
}

class C_OSD_OnOpCommit : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpCommit(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}
  void finish(int) {
    pg->op_commit(op);
  }
};

class C_OSD_OnOpApplied : public Context {
  ReplicatedBackend *pg;
  ReplicatedBackend::InProgressOp *op;
public:
  C_OSD_OnOpApplied(ReplicatedBackend *pg, ReplicatedBackend::InProgressOp *op) 
    : pg(pg), op(op) {}
  void finish(int) {
    pg->op_applied(op);
  }
};

void ReplicatedBackend::submit_transaction(
  const hobject_t &soid,
  const eversion_t &at_version,
  PGTransaction *_t,
  vector<pg_log_entry_t> &log_entries,
  Context *on_local_applied_sync,
  Context *on_all_acked,
  Context *on_all_commit,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef orig_op)
{
  RPGTransaction *t = dynamic_cast<RPGTransaction*>(_t);
  assert(t);
  ObjectStore::Transaction *op_t = t->get_transaction();

  assert(t->get_temp_added().size() <= 1);
  assert(t->get_temp_cleared().size() <= 1);

  assert(!in_progress_ops.count(tid));
  InProgressOp &op = in_progress_ops.insert(
    make_pair(
      tid,
      InProgressOp(
	tid, on_all_commit, on_all_acked,
	orig_op, at_version)
      )
    ).first->second;

  ObjectStore::Transaction local_t;
  if (t->get_temp_added().size()) {
    get_temp_coll(&local_t);
    add_temp_objs(t->get_temp_added());
  }
  clear_temp_objs(t->get_temp_cleared());

  parent->log_operation(log_entries, true, &local_t);
  local_t.append(*op_t);
  local_t.swap(*op_t);

  op_t->register_on_applied_sync(on_local_applied_sync);
  op_t->register_on_applied(
    parent->bless_context(
      new C_OSD_OnOpApplied(this, &op)));
  op_t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(op_t));
  op_t->register_on_commit(
    parent->bless_context(
      new C_OSD_OnOpCommit(this, &op)));
      
  parent->queue_transaction(op_t, op.op);
  delete t;
}

void ReplicatedBackend::op_applied(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_applied");

  parent->op_applied(op->v);
}

void ReplicatedBackend::op_commit(
  InProgressOp *op)
{
  dout(10) << __func__ << ": " << op->tid << dendl;
  if (op->op)
    op->op->mark_event("op_commit");
}
