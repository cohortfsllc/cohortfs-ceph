// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_JOURNALINGOBJECTSTORE_H
#define CEPH_JOURNALINGOBJECTSTORE_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include "ObjectStore.h"
#include "Journal.h"

class JournalingObjectStore : public ObjectStore {
protected:
  Journal *journal;
  Finisher finisher;
  std::atomic<uint64_t> submit_op_seq;

  class ApplyManager {
    CephContext *cct;
    Journal *&journal;
    Finisher &finisher;

    std::mutex apply_lock;
    typedef std::unique_lock<std::mutex> unique_lock;
    bool blocked;
    std::condition_variable blocked_cond;
    int open_ops;
    uint64_t max_applied_seq;

    std::mutex com_lock;
    map<version_t, Context::List> commit_waiters;
    uint64_t committing_seq, committed_seq;

  public:
    ApplyManager(CephContext *_cct, Journal *&j, Finisher &f) :
      cct(_cct),
      journal(j), finisher(f),
      blocked(false),
      open_ops(0),
      max_applied_seq(0),
      committing_seq(0), committed_seq(0) {}
    void reset() {
      assert(open_ops == 0);
      assert(blocked == false);
      max_applied_seq = 0;
      committing_seq = 0;
      committed_seq = 0;
    }
    void add_waiter(uint64_t, Context*);
    uint64_t op_apply_start(uint64_t op);
    void op_apply_finish(uint64_t op);
    bool commit_start();
    void commit_started();
    void commit_finish();
    bool is_committing() {
      unique_lock l(com_lock);
      return committing_seq != committed_seq;
    }
    uint64_t get_committed_seq() {
      unique_lock l(com_lock);
      return committed_seq;
    }
    uint64_t get_committing_seq() {
      unique_lock l(com_lock);
      return committing_seq;
    }
    void init_seq(uint64_t fs_op_seq) {
      {
	unique_lock l(com_lock);
	committed_seq = fs_op_seq;
	committing_seq = fs_op_seq;
      }
      {
	unique_lock l(apply_lock);
	max_applied_seq = fs_op_seq;
      }
    }
  } apply_manager;

  bool replaying;

protected:
  void journal_start();
  void journal_stop();
  int journal_replay(uint64_t fs_op_seq);

  void _op_journal_transactions(list<Transaction*>& tls, uint64_t op,
				Context *onjournal, OpRequestRef osd_op,
				ZTracer::Trace &trace);

  virtual int do_transactions(list<Transaction*>& tls, uint64_t op_seq,
                              ZTracer::Trace &trace) = 0;

public:
  bool is_committing() {
    return apply_manager.is_committing();
  }
  uint64_t get_committed_seq() {
    return apply_manager.get_committed_seq();
  }

public:
  JournalingObjectStore(CephContext *cct, const std::string& path)
    : ObjectStore(cct, path),
      journal(NULL),
      finisher(cct),
      submit_op_seq(0),
      apply_manager(cct, journal, finisher),
      replaying(false) {}

};

#endif
