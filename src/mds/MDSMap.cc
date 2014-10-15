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


#include "MDSMap.h"

#include <sstream>
using std::stringstream;


// features
CompatSet get_mdsmap_compat_set_all() {
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_BASE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_CLIENTRANGES);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILELAYOUT);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_DIRINODE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_ENCODING);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_OMAPDIRFRAG);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

CompatSet get_mdsmap_compat_set_default() {
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_BASE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_CLIENTRANGES);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_FILELAYOUT);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_DIRINODE);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_ENCODING);
  feature_incompat.insert(MDS_FEATURE_INCOMPAT_OMAPDIRFRAG);

  return CompatSet(feature_compat, feature_ro_compat, feature_incompat);
}

// base (pre v0.20)
CompatSet get_mdsmap_compat_set_base() {
  CompatSet::FeatureSet feature_compat_base;
  CompatSet::FeatureSet feature_incompat_base;
  feature_incompat_base.insert(MDS_FEATURE_INCOMPAT_BASE);
  CompatSet::FeatureSet feature_ro_compat_base;

  return CompatSet(feature_compat_base, feature_incompat_base, feature_ro_compat_base);
}

void MDSMap::mds_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("gid", global_id);
  f->dump_string("name", name);
  f->dump_int("rank", rank);
  f->dump_int("incarnation", inc);
  f->dump_stream("state") << ceph_mds_state_name(state);
  f->dump_int("state_seq", state_seq);
  f->dump_stream("addr") << addr;
  if (laggy_since != utime_t())
    f->dump_stream("laggy_since") << laggy_since;

  f->dump_int("standby_for_rank", standby_for_rank);
  f->dump_string("standby_for_name", standby_for_name);
  f->open_array_section("export_targets");
  for (set<int32_t>::iterator p = export_targets.begin();
       p != export_targets.end(); ++p) {
    f->dump_int("mds", *p);
  }
  f->close_section();
}

void MDSMap::mds_info_t::generate_test_instances(list<mds_info_t*>& ls)
{
  mds_info_t *sample = new mds_info_t();
  ls.push_back(sample);
  sample = new mds_info_t();
  sample->global_id = 1;
  sample->name = "test_instance";
  sample->rank = 0;
  ls.push_back(sample);
}

void MDSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_unsigned("flags", flags);
  f->dump_stream("created") << created;
  f->dump_stream("modified") << modified;
  f->dump_int("tableserver", tableserver);
  f->dump_int("root", root);
  f->dump_int("session_timeout", session_timeout);
  f->dump_int("session_autoclose", session_autoclose);
  f->dump_int("max_file_size", max_file_size);
  f->dump_int("last_failure", last_failure);
  f->dump_int("last_failure_osd_epoch", last_failure_osd_epoch);
  f->open_object_section("compat");
  compat.dump(f);
  f->close_section();
  f->dump_int("max_mds", max_mds);
  f->open_array_section("in");
  for (set<int32_t>::const_iterator p = in.begin(); p != in.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("up");
  for (map<int32_t,uint64_t>::const_iterator p = up.begin(); p != up.end(); ++p) {
    char s[10];
    sprintf(s, "mds_%d", p->first);
    f->dump_int(s, p->second);
  }
  f->close_section();
  f->open_array_section("failed");
  for (set<int32_t>::const_iterator p = failed.begin(); p != failed.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_array_section("stopped");
  for (set<int32_t>::const_iterator p = stopped.begin(); p != stopped.end(); ++p)
    f->dump_int("mds", *p);
  f->close_section();
  f->open_object_section("info");
  for (map<uint64_t,mds_info_t>::const_iterator p = mds_info.begin(); p != mds_info.end(); ++p) {
    char s[25]; // 'gid_' + len(str(ULLONG_MAX)) + '\0'
    sprintf(s, "gid_%llu", (long long unsigned)p->first);
    f->open_object_section(s);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("data_volumes");
  for (set<uuid_d>::const_iterator p = data_volumes.begin(); p != data_volumes.end(); ++p)
    f->dump_stream("uuid") << *p;
  f->close_section();
  f->dump_stream("metadata_uuid") << metadata_uuid;
  f->dump_bool("inline_data", inline_data_enabled);
}

void MDSMap::generate_test_instances(list<MDSMap*>& ls)
{
  MDSMap *m = new MDSMap();
  m->max_mds = 1;
  uuid_d uuid1, uuid2, uuid3;
  uuid1.parse("5a9e54a4-7740-4d03-b0fb-e1f3b899b185");
  uuid2.parse("5edbdba8-af1a-4b48-8f2f-1ec5cf84efbe");
  uuid3.parse("e9013f90-e7a3-4f69-bb85-bcf74559e68d");
  m->metadata_uuid = uuid1;
  m->cas_uuid = uuid2;
  m->data_volumes.insert(uuid3);
#if 0
  m->metadata_pool = 1;
  m->cas_pool = 2;
#endif
  m->compat = get_mdsmap_compat_set_all();

  // these aren't the defaults, just in case anybody gets confused
  m->session_timeout = 61;
  m->session_autoclose = 301;
  m->max_file_size = 1<<24;
  ls.push_back(m);
}

template <typename T>
typename StrmRet<T>::type& summary(Formatter *f, T *out, MDSMap *mdsmap)
{
  map<int,string> by_rank;
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", mdsmap->get_epoch());
    f->dump_unsigned("up", mdsmap->get_num_up_mds());
    f->dump_unsigned("in", mdsmap->get_num_in_mds());
    f->dump_unsigned("max", mdsmap->get_max_mds());
  } else {
    *out << "e" << mdsmap->get_epoch() << ": " << mdsmap->get_num_up_mds() << "/" << mdsmap->get_num_in_mds() << "/" << mdsmap->get_max_mds() << " up";
  }

  if (f)
    f->open_array_section("by_rank");
  for (map<uint64_t, MDSMap::mds_info_t>::const_iterator p = mdsmap->get_mds_info().begin();
       p != mdsmap->get_mds_info().end();
       ++p) {
    string s = ceph_mds_state_name(p->second.state);
    if (p->second.laggy())
      s += "(laggy or crashed)";

    if (p->second.rank >= 0) {
      if (f) {
	f->open_object_section("mds");
	f->dump_unsigned("rank", p->second.rank);
	f->dump_string("name", p->second.name);
	f->dump_string("status", s);
	f->close_section();
      } else {
	by_rank[p->second.rank] = p->second.name + "=" + s;
      }
    } else {
      by_state[s]++;
    }
  }
  if (f) {
    f->close_section();
  } else {
    if (!by_rank.empty())
      *out << " " << by_rank;
  }

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); ++p) {
    if (f) {
      f->dump_unsigned(p->first.c_str(), p->second);
    } else {
      *out << ", " << p->second << " " << p->first;
    }
  }

  if (mdsmap->is_any_failed()) {
    if (f) {
      f->dump_unsigned("failed", mdsmap->get_num_failed_mds());
    } else {
      *out << ", " << mdsmap->get_num_failed_mds() << " failed";
    }
  }
  //if (stopped.size())
  //out << ", " << stopped.size() << " stopped";
}

void MDSMap::print_summary(Formatter *f,ostream *out)
{
 summary(f, out, this);
}

void MDSMap::print_summary(Formatter *f,lttng_stream *out)
{
 summary(f, out, this);
}

void MDSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  if (!failed.empty()) {
    std::ostringstream oss;
    oss << "mds rank"
	<< ((failed.size() > 1) ? "s ":" ")
	<< failed
	<< ((failed.size() > 1) ? " have":" has")
	<< " failed";
    summary.push_back(make_pair(HEALTH_ERR, oss.str()));
    if (detail) {
      for (set<int>::const_iterator p = failed.begin(); p != failed.end(); ++p) {
	std::ostringstream oss;
	oss << "mds." << *p << " has failed";
	detail->push_back(make_pair(HEALTH_ERR, oss.str()));
      }
    }
  }

  if (is_degraded()) {
    summary.push_back(make_pair(HEALTH_WARN, "mds cluster is degraded"));
    if (detail) {
      detail->push_back(make_pair(HEALTH_WARN, "mds cluster is degraded"));
      for (unsigned i=0; i< get_max_mds(); i++) {
	if (!is_up(i))
	  continue;
	uint64_t gid = up.find(i)->second;
	map<uint64_t,mds_info_t>::const_iterator info = mds_info.find(gid);
	stringstream ss;
	if (is_resolve(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is resolving";
	if (is_replay(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is replaying journal";
	if (is_rejoin(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is rejoining";
	if (is_reconnect(i))
	  ss << "mds." << info->second.name << " at " << info->second.addr << " rank " << i << " is reconnecting to clients";
	if (ss.str().length())
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
  }

  map<int32_t,uint64_t>::const_iterator u = up.begin();
  map<int32_t,uint64_t>::const_iterator u_end = up.end();
  map<uint64_t,mds_info_t>::const_iterator m_end = mds_info.end();
  set<string> laggy;
  for (; u != u_end; ++u) {
    map<uint64_t,mds_info_t>::const_iterator m = mds_info.find(u->second);
    assert(m != m_end);
    const mds_info_t &mds_info(m->second);
    if (mds_info.laggy()) {
      laggy.insert(mds_info.name);
      if (detail) {
	std::ostringstream oss;
	oss << "mds." << mds_info.name << " at " << mds_info.addr << " is laggy/unresponsive";
	detail->push_back(make_pair(HEALTH_WARN, oss.str()));
      }
    }
  }

  if (!laggy.empty()) {
    std::ostringstream oss;
    oss << "mds " << laggy
	<< ((laggy.size() > 1) ? " are":" is")
	<< " laggy";
    summary.push_back(make_pair(HEALTH_WARN, oss.str()));
  }
}

void MDSMap::mds_info_t::encode_versioned(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(84, 84, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(rank, bl);
  ::encode(inc, bl);
  ::encode(state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ::encode(standby_for_rank, bl);
  ::encode(standby_for_name, bl);
  ::encode(export_targets, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::mds_info_t::encode_unversioned(bufferlist& bl) const
{
  uint8_t struct_v = 84;
  ::encode(struct_v, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(rank, bl);
  ::encode(inc, bl);
  ::encode(state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ::encode(standby_for_rank, bl);
  ::encode(standby_for_name, bl);
  ::encode(export_targets, bl);
}

void MDSMap::mds_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(84, 84, 4, bl);
  ::decode(global_id, bl);
  ::decode(name, bl);
  ::decode(rank, bl);
  ::decode(inc, bl);
  ::decode(state, bl);
  ::decode(state_seq, bl);
  ::decode(addr, bl);
  ::decode(laggy_since, bl);
  ::decode(standby_for_rank, bl);
  ::decode(standby_for_name, bl);
  ::decode(export_targets, bl);
  DECODE_FINISH(bl);
}



void MDSMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(84, 84, bl);
  ::encode(epoch, bl);
  ::encode(flags, bl);
  ::encode(last_failure, bl);
  ::encode(root, bl);
  ::encode(session_timeout, bl);
  ::encode(session_autoclose, bl);
  ::encode(max_file_size, bl);
  ::encode(max_mds, bl);
  ::encode(mds_info, bl, features);
  ::encode(data_volumes, bl);
  ::encode(cas_uuid, bl);

  // kclient ignores everything from here
  uint16_t ev = 87;
  ::encode(ev, bl);
  ::encode(compat, bl);
  ::encode(metadata_uuid, bl);
  ::encode(created, bl);
  ::encode(modified, bl);
  ::encode(tableserver, bl);
  ::encode(in, bl);
  ::encode(inc, bl);
  ::encode(up, bl);
  ::encode(failed, bl);
  ::encode(stopped, bl);
  ::encode(last_failure_osd_epoch, bl);
  ::encode(inline_data_enabled, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN_16(84, 84, 4, p);
  ::decode(epoch, p);
  ::decode(flags, p);
  ::decode(last_failure, p);
  ::decode(root, p);
  ::decode(session_timeout, p);
  ::decode(session_autoclose, p);
  ::decode(max_file_size, p);
  ::decode(max_mds, p);
  ::decode(mds_info, p);
  if (1) {
    ::decode(data_volumes, p);
    ::decode(cas_uuid, p);
  }

  // kclient ignores everything from here
  uint16_t ev = 87;
  ::decode(ev, p);
  ::decode(compat, p);
  ::decode(metadata_uuid, p);
  ::decode(created, p);
  ::decode(modified, p);
  ::decode(tableserver, p);
  ::decode(in, p);
  ::decode(inc, p);
  ::decode(up, p);
  ::decode(failed, p);
  ::decode(stopped, p);
  ::decode(last_failure_osd_epoch, p);
  ::decode(inline_data_enabled, p);
  DECODE_FINISH(p);
}
