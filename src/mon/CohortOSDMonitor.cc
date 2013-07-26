// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "messages/MRemoveSnaps.h"
#include "CohortOSDMonitor.h"
#include "cohort/CohortPlaceSystem.h"

const CohortOSDMonitorPlaceSystem placeSystem(
  CohortPlaceSystem::systemName, CohortPlaceSystem::systemIdentifier);

void CohortOSDMonitor::dump_info_sub(Formatter *f)
{
  CohortOSDMapRef p = static_pointer_cast<const CohortOSDMap>(osdmap);
}

CohortOSDMonitor::CohortOSDMonitor(Monitor *mn, Paxos *p,
				   const string& service_name)
  : OSDMonitor(mn, p, service_name)
{
  osdmap.reset(newOSDMap());
  pending_inc.reset(new CohortOSDMap::Incremental());
}

void CohortOSDMonitor::tick_sub(bool& do_propose)
{
  shared_ptr <const CohortOSDMap> l_osdmap
    = static_pointer_cast<const CohortOSDMap>(osdmap);
  shared_ptr <const CohortOSDMap::Incremental> l_pending_inc
    = static_pointer_cast<CohortOSDMap::Incremental>(pending_inc);
//  utime_t now = ceph_clock_now(g_ceph_context);
}

void CohortOSDMonitor::create_pending()
{
  pending_inc.reset(new CohortOSDMap::Incremental(osdmap->epoch + 1));
  create_pending_super();
}

bool CohortOSDMonitor::preprocess_query_sub(PaxosServiceMessage *m)
{
  m->put();
  return false;
}

bool CohortOSDMonitor::prepare_update_sub(PaxosServiceMessage *m)
{
  m->put();
  return false;
}

bool CohortOSDMonitor::preprocess_remove_snaps_sub(class MRemoveSnaps *m)
{
  shared_ptr<const CohortOSDMap> l_osdmap
    = static_pointer_cast<const CohortOSDMap>(osdmap);

  return true;
}

bool CohortOSDMonitor::prepare_remove_snaps(MRemoveSnaps *m)
{
  const shared_ptr<CohortOSDMap> l_osdmap
    = static_pointer_cast<CohortOSDMap>(osdmap);
  const shared_ptr<CohortOSDMap::Incremental> l_pending_inc
    = static_pointer_cast<CohortOSDMap::Incremental>(pending_inc);

  m->put();
  return true;
}

bool CohortOSDMonitor::preprocess_command_sub(MMonCommand *m, int& r,
					      stringstream& ss)
{
  const shared_ptr<CohortOSDMap> l_osdmap
    = static_pointer_cast<CohortOSDMap>(osdmap);

  return false;
}

bool CohortOSDMonitor::prepare_command_sub(MMonCommand *m,
					   int& err,
					   stringstream& ss,
					   string& rs)
{
  const shared_ptr<CohortOSDMap> l_osdmap
    = static_pointer_cast<CohortOSDMap>(osdmap);
  const shared_ptr<CohortOSDMap::Incremental> l_pending_inc
    = static_pointer_cast<CohortOSDMap::Incremental>(pending_inc);

  return false;
}

void CohortOSDMonitor::update_trim()
{
}

void CohortOSDMonitor::get_health_sub(
  list<pair<health_status_t,string> >& summary,
  list<pair<health_status_t,string> > *detail) const
{
  shared_ptr<const CohortOSDMap> l_osdmap
    = static_pointer_cast<const CohortOSDMap>(osdmap);
}
