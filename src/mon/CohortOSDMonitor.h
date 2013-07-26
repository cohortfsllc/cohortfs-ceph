// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

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
#include "cohort/CohortOSDMap.h"


class CohortOSDMonitor : public OSDMonitor {

  typedef OSDMonitor inherited;

public:
  virtual void dump_info_sub(Formatter *f);
  CohortOSDMonitor(Monitor *mn, Paxos *p, const string& service_name);
  virtual ~CohortOSDMonitor() { };

  virtual CohortOSDMap* newOSDMap() const { return new CohortOSDMap(); }

  virtual void tick_sub(bool& do_propose);

  virtual void create_pending();  // prepare a new pending

  virtual bool preprocess_query_sub(PaxosServiceMessage *m);
  virtual bool prepare_update_sub(PaxosServiceMessage *m);
  virtual bool preprocess_remove_snaps_sub(class MRemoveSnaps *m);
  virtual bool prepare_remove_snaps(MRemoveSnaps *m);
  virtual bool preprocess_command_sub(MMonCommand *m, int& r,
				      stringstream& ss);
  virtual bool prepare_command_sub(MMonCommand *m, int& err,
				   stringstream& ss, string& rs);

  void update_trim();

  void get_health_sub(list<pair<health_status_t,string> >& summary,
		      list<pair<health_status_t,string> > *detail) const;

};


#endif // CEPH_COHORTOSDMONITOR_H
