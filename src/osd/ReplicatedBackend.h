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

#ifndef REPBACKEND_H
#define REPBACKEND_H

#include "PGBackend.h"
#include "osd_types.h"
#include "../include/memory.h"

class ReplicatedBackend : public PGBackend {
public:
  CephContext *cct;

  ReplicatedBackend(
    PGBackend::Listener *pg,
    coll_t coll,
    coll_t temp_coll,
    ObjectStore *store,
    CephContext *cct);

  void _on_change(ObjectStore::Transaction *t);
  void on_flushed();

  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist *bl);

  void objects_read_async(
    const hobject_t &hoid,
    const list<pair<pair<uint64_t, uint64_t>,
	       pair<bufferlist*, Context*> > > &to_read,
    Context *on_complete);

private:
  /**
   * Client IO
   */
  struct InProgressOp {
    ceph_tid_t tid;
    Context *on_commit;
    Context *on_applied;
    OpRequestRef op;
    eversion_t v;
    InProgressOp(
      ceph_tid_t tid, Context *on_commit, Context *on_applied,
      OpRequestRef op, eversion_t v)
      : tid(tid), on_commit(on_commit), on_applied(on_applied),
	op(op), v(v) {}
  };
  map<ceph_tid_t, InProgressOp> in_progress_ops;
public:
  PGTransaction *get_transaction();
  friend class C_OSD_OnOpCommit;
  friend class C_OSD_OnOpApplied;
  void submit_transaction(
    const hobject_t &hoid,
    const eversion_t &at_version,
    PGTransaction *t,
    Context *on_local_applied_sync,
    Context *on_all_applied,
    Context *on_all_commit,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op
    );

private:
  void op_applied(InProgressOp *op);
  void op_commit(InProgressOp *op);

  struct RepModify {
    OpRequestRef op;
    bool applied, committed;
    int ackerosd;
    eversion_t last_complete;
    epoch_t epoch_started;

    uint64_t bytes_written;

    ObjectStore::Transaction opt, localt;

    RepModify() : applied(false), committed(false), ackerosd(-1),
		  epoch_started(0), bytes_written(0) {}
  };
  typedef ceph::shared_ptr<RepModify> RepModifyRef;

  uint64_t be_get_ondisk_size(uint64_t logical_size) { return logical_size; }
};

#endif
