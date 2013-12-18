// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "messages/MRemoveSnaps.h"
#include "CohortOSDMonitor.h"
#include "cohort/CohortPlaceSystem.h"
#include "vol/Volume.h"

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
#warning Foo.
//  create_pending_super();
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
  bufferlist rdata;

  r = -1;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "list" && m->cmd.size() == 2) {
      if (l_osdmap->volmap_empty()) {
	ss << "volmap is empty" << std::endl;
      } else {
	ss << "volmap has " << l_osdmap->vols.by_name.size()
	   << " entries" << std::endl;
	stringstream ds;
	for (map<string,VolumeRef>::const_iterator i
	       = l_osdmap->vols.by_name.begin();
	     i != l_osdmap->vols.by_name.end();
	     ++i) {
	  ds << *i->second << std::endl;
	}
	rdata.append(ds);
      }
      r = 0;
    }
  }

  string rs;
  getline(ss, rs);

  if (r != -1) {
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else {
    return false;
  }
}

bool CohortOSDMonitor::prepare_command_sub(MMonCommand *m,
					   int& r,
					   stringstream& ss,
					   string& rs)
{
  const shared_ptr<CohortOSDMap> l_osdmap
    = static_pointer_cast<CohortOSDMap>(osdmap);
  const shared_ptr<CohortOSDMap::Incremental> l_pending_inc
    = static_pointer_cast<CohortOSDMap::Incremental>(pending_inc);
  bufferlist rdata;
  r = -1;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "create" && m->cmd.size() == 4) {
      const string& name = m->cmd[2];
      uuid_d uuid;
      string error_message;

      if (!Volume::valid_name(name, error_message)) {
	ss << error_message;
	r = -EINVAL;
      } else {
#warning Adapt for typed creation.
//	r = pending_volmap->create_volume(name, uuid);
	if (r == 0) {
	  ss << "volume " << uuid << " created with name \"" << name << "\"";
//	  pending_inc.include_addition(uuid, name);
	} else if (r == -EEXIST) {
	  ss << "volume with name \"" << name << "\" already exists";
	} else {
	  ss << "volume could not be created due to error code " << -r;
	}
      }
    } else if (m->cmd[1] == "remove" && m->cmd.size() == 3) {
      const string& uuid_str = m->cmd[2];
      string error_message;
      uuid_d uuid;

      try {
	uuid = uuid_d::parse(uuid_str);
	pending_inc->include_removal(uuid);
	/* Error handling */
      } catch (const std::invalid_argument& ia) {
	ss << "provided volume uuid " << uuid << " is not a valid uuid";
	r = -EINVAL;
      }
    }
  }

  if (r == -1) {
    r = -EINVAL;
    ss << "unrecognized command";
  }
  getline(ss, rs);

  if (r != -1) {
    // success.. delay reply
    // paxos->wait_for_commit(new Monitor::C_Command(mon, m, r, rs, paxos->get_version()));
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else {
    return false;
  }
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
