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

#ifndef CEPH_PGOSDMONITOR_H
#define CEPH_PGOSDMONITOR_H


#include "mon/OSDMonitor.h"
#include "pg/PGOSDMap.h"


class MPoolOp;


class PGOSDMonitor : public OSDMonitor {

  typedef OSDMonitor inherited;

public:
  PGOSDMonitor(Monitor *mn, Paxos *p);
  virtual ~PGOSDMonitor() { };

  virtual PGOSDMap* newOSDMap() const { return new PGOSDMap(); }

  virtual void tick_sub(bool& do_propose);

  virtual void create_pending();  // prepare a new pending

  virtual bool preprocess_query_sub(PaxosServiceMessage *m);  // true if processed.
  virtual bool prepare_update_sub(PaxosServiceMessage *m);  // true if processed.
  virtual bool preprocess_remove_snaps_sub(class MRemoveSnaps *m);
  virtual bool prepare_remove_snaps(MRemoveSnaps *m);
  virtual void preprocess_command_sub(MMonCommand *m, int& r);

  bool preprocess_pgtemp(class MOSDPGTemp *m);
  bool prepare_pgtemp(class MOSDPGTemp *m);

  void remove_redundant_pg_temp();

  bool preprocess_pool_op(MPoolOp *m);
  bool preprocess_pool_op_create(MPoolOp *m);
  bool prepare_pool_op(MPoolOp *m);
  bool prepare_pool_op_create(MPoolOp *m);
  bool prepare_pool_op_delete(MPoolOp *m);
  bool prepare_pool_op_auid(MPoolOp *m);
  int prepare_new_pool(MPoolOp *m);
  int prepare_new_pool(string& name, uint64_t auid, int crush_rule,
                       unsigned pg_num, unsigned pgp_num);
  void _pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp=NULL);
  int _prepare_remove_pool(uint64_t pool);
  int _prepare_rename_pool(uint64_t pool, string newname);

  struct C_PoolOp : public Context {
    PGOSDMonitor *osdmon;
    MPoolOp *m;
    int replyCode;
    int epoch;
    bufferlist *reply_data;
    C_PoolOp(PGOSDMonitor * osd, MPoolOp *m_, int rc, int e, bufferlist *rd=NULL) : 
      osdmon(osd), m(m_), replyCode(rc), epoch(e), reply_data(rd) {}
    void finish(int r) {
      osdmon->_pool_op_reply(m, replyCode, epoch, reply_data);
    }
  };
};


#endif // CEPH_PGOSDMONITOR_H
