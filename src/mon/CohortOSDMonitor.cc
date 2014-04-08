// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "messages/MRemoveSnaps.h"
#include "CohortOSDMonitor.h"
#include "cohort/CohortPlaceSystem.h"
#include "vol/Volume.h"
#include "cohort/CohortVolume.h"

void CohortOSDMonitor::dump_info_sub(Formatter *f)
{
  CohortOSDMapRef p = static_pointer_cast<const CohortOSDMap>(osdmap);
}

CohortOSDMonitor::CohortOSDMonitor(Monitor *mn, Paxos *p,
				   const string& service_name)
  : OSDMonitor(mn, p, service_name)
{
  osdmap = newOSDMap();
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

bool CohortOSDMonitor::preprocess_command_sub(const string &prefix, int& r,
					      stringstream& ss,
					      bufferlist &rdata)
{
  const shared_ptr<CohortOSDMap> l_osdmap
    = static_pointer_cast<CohortOSDMap>(osdmap);

  r = -1;

  if (prefix == "osd volume list") {
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
  } else {
    return false;
  }

  return true;
}

bool CohortOSDMonitor::prepare_command_sub(string& prefix,
					   map<string, cmd_vartype>& map,
					   int& err,
					   stringstream& ss,
					   bufferlist &rdata)
{
  const shared_ptr<CohortOSDMap> l_osdmap
    = static_pointer_cast<CohortOSDMap>(osdmap);

  if (prefix == "osd volume create") {
    string name;
    string place_text;
    string symbols;
    string erasure_type;
    int64_t data_blocks;
    int64_t code_blocks;
    int64_t word_size;
    int64_t packet_size;
    int64_t size;
    epoch_t last_update = l_osdmap->epoch;
    string error_message;

    VolumeRef vol;

    cmd_getval(g_ceph_context, map, "volumeName", name);
    cmd_getval(g_ceph_context, map, "placeCode", place_text);
    cmd_getval(g_ceph_context, map, "placeSymbols", symbols);
    cmd_getval(g_ceph_context, map, "erasureType", erasure_type);
    cmd_getval(g_ceph_context, map, "erasureSize", size, int64_t(4096));
    cmd_getval(g_ceph_context, map, "erasureDataBlocks", data_blocks, int64_t(1));
    cmd_getval(g_ceph_context, map, "erasureCodeBlocks", code_blocks, int64_t(0));
    cmd_getval(g_ceph_context, map, "erasureWordSize", word_size, int64_t(0));
    cmd_getval(g_ceph_context, map, "erasurePktSize", packet_size, int64_t(0));

    /* Only one volume type for now, when we implement more I'll
       come back and complexify this. */

    if (!Volume::valid_name(name, error_message)) {
      ss << error_message;
      err = -EINVAL;
      return true;
    }
    vol = CohortVolume::create(name, last_update, place_text,
	symbols, erasure_type, data_blocks,
	code_blocks, word_size, packet_size,
	size, error_message);
    if (vol) {
      ss << "volume: " << vol << " created.";
      pending_inc->include_addition(vol);
    } else {
      ss << error_message;
      err = -EINVAL;
      return true;
    }
  } else if (prefix == "osd volume remove") {
    string name;
    string error_message;
    VolumeRef vol;

    cmd_getval(g_ceph_context, map, "volumeName", name);
    if (!osdmap->find_by_name(name, vol)) {
      ss << "volume named " << name << " not found";
      err = -EINVAL;
      return true;
    }
    pending_inc->include_removal(vol->uuid);
  } else {
    return false;
  }

  return true;
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
