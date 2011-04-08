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
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_JOURNALINGOBJECTSTORE_H
#define CEPH_JOURNALINGOBJECTSTORE_H

#include "ObjectStore.h"
#include "Journal.h"
#include "common/RWLock.h"

class JournalingObjectStore : public ObjectStore {
protected:
  uint64_t op_seq, applied_seq;
  uint64_t committing_seq, committed_seq;
  map<version_t, vector<Context*> > commit_waiters;

  int open_ops;
  bool blocked;

  Journal *journal;
  Finisher finisher;

  Cond cond;
  Mutex journal_lock;
  Mutex com_lock;

  list<uint64_t> ops_submitting;
  list<Cond*> ops_apply_blocked;

protected:
  void journal_start();
  void journal_stop();
  int journal_replay(uint64_t fs_op_seq);

  // --
  uint64_t op_submit_start();
  void op_submit_finish(uint64_t op_seq);

  uint64_t op_apply_start(uint64_t op);
  uint64_t _op_apply_start(uint64_t op);
  void op_apply_finish(uint64_t op);

  void op_journal_transactions(list<ObjectStore::Transaction*>& tls, uint64_t op, Context *onjournal);
  void _op_journal_transactions(list<ObjectStore::Transaction*>& tls, uint64_t op, Context *onjournal);

  virtual int do_transactions(list<ObjectStore::Transaction*>& tls, uint64_t op_seq) = 0;

  bool commit_start();
  void commit_started();  // allow new ops (underlying fs should now be committing all prior ops)
  void commit_finish();
  
  bool is_committing() {
    Mutex::Locker l(com_lock);
    return committing_seq != committed_seq;
  }

public:
  JournalingObjectStore() : op_seq(0), 
			    applied_seq(0), committing_seq(0), committed_seq(0), 
			    open_ops(0), blocked(false),
			    journal(NULL),
			    journal_lock("JournalingObjectStore::journal_lock"),
			    com_lock("JournalingObjectStore::com_lock") { }
  
};

#endif
