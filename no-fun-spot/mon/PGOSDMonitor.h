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


#include "include/ceph_features.h"

#include "common/config.h"
#include "common/strtol.h"

#include "mon/MonMap.h"
#include "mds/MDS.h"
#include "mds/Dumper.h"
#include "mds/Resetter.h"

#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"

#include "global/global_init.h"
#include "global/signal_handler.h"
#include "global/pidfile.h"

#include "mon/MonClient.h"

#include "auth/KeyRing.h"

#include "include/assert.h"
#include "mon/OSDMonitor.h"
#include "pg/PGOSDMap.h"


class MPoolOp;


class PGOSDMonitor : public OSDMonitor {

  typedef OSDMonitor inherited;

protected:
  bool _have_pending_crush();
  CrushWrapper &_get_stable_crush();
  void _get_pending_crush(CrushWrapper& newcrush);

public:
  virtual void dump_info_sub(Formatter *f);
  PGOSDMonitor(Monitor *mn, Paxos *p, const string& service_name);
  virtual ~PGOSDMonitor() { };

  virtual PGOSDMap* newOSDMap() const { return new PGOSDMap(); }

  virtual void tick_sub(bool& do_propose);

  virtual void create_pending();  // prepare a new pending

  virtual bool preprocess_query_sub(PaxosServiceMessage *m);  // true if processed.
  virtual bool prepare_update_sub(PaxosServiceMessage *m);  // true if processed.
  virtual bool preprocess_remove_snaps_sub(class MRemoveSnaps *m);
  virtual bool prepare_remove_snaps(MRemoveSnaps *m);
  virtual bool preprocess_command_sub(MMonCommand *m, int& r,
				      stringstream& ss);
  virtual bool prepare_command_sub(MMonCommand *m, int& err,
				   stringstream& ss, string& rs);

  bool preprocess_pgtemp(class MOSDPGTemp *m);
  bool prepare_pgtemp(class MOSDPGTemp *m);

  void remove_redundant_pg_temp();
  void remove_down_pg_temp();

  bool preprocess_pool_op ( class MPoolOp *m);
  bool preprocess_pool_op_create ( class MPoolOp *m);
  bool prepare_pool_op (MPoolOp *m);
  bool prepare_pool_op_create (MPoolOp *m);
  bool prepare_pool_op_delete(MPoolOp *m);
  bool prepare_pool_op_auid(MPoolOp *m);

  void update_trim();

  int prepare_new_pool(string& name, uint64_t auid, int crush_rule,
		       unsigned pg_num, unsigned pgp_num);
  int prepare_new_pool(MPoolOp *m);

  void _pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp=NULL);
  int _prepare_remove_pool(uint64_t pool);
  int _prepare_rename_pool(uint64_t pool, string newname);

  void update_pool_flags(int64_t pool_id, uint64_t flags);
  bool update_pools_status();
  void get_health_sub(list<pair<health_status_t,string> >& summary,
		      list<pair<health_status_t,string> > *detail) const;

  struct C_PoolOp : public Context {
    PGOSDMonitor *osdmon;
    MPoolOp *m;
    int replyCode;
    int epoch;
    bufferlist reply_data;
    C_PoolOp(PGOSDMonitor * osd, MPoolOp *m_, int rc, int e,
	     bufferlist *rd = NULL) :
      osdmon(osd), m(m_), replyCode(rc), epoch(e) {
      if (rd)
	reply_data = *rd;
    }
    void finish(int r) {
      if (r >= 0)
	osdmon->_pool_op_reply(m, replyCode, epoch, &reply_data);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	osdmon->dispatch(m);
      else
	assert(0 == "bad C_PoolOp return value");
    }
  };
};


#endif // CEPH_PGOSDMONITOR_H
