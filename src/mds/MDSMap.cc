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


#include <boost/uuid/string_generator.hpp>
#include "MDSMap.h"

#include "global/global_init.h"

#include <sstream>
using std::stringstream;


void MDSMap::mds_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("gid", global_id);
  f->dump_string("name", name);
  f->dump_stream("state") << ceph_mds_state_name(state);
  f->dump_int("state_seq", state_seq);
  f->dump_stream("addr") << addr;
  if (laggy_since != ceph::real_time::min())
    f->dump_stream("laggy_since") << laggy_since;

  f->close_section();
}

void MDSMap::mds_info_t::generate_test_instances(list<mds_info_t*>& ls)
{
  mds_info_t *sample = new mds_info_t();
  ls.push_back(sample);
  sample = new mds_info_t();
  sample->global_id = 1;
  sample->name = "test_instance";
  ls.push_back(sample);
}

void MDSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_unsigned("flags", flags);
  f->dump_stream("created") << created;
  f->dump_stream("modified") << modified;
  f->dump_int("last_failure", last_failure);
  f->dump_int("last_failure_osd_epoch", last_failure_osd_epoch);
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
}

void MDSMap::generate_test_instances(list<MDSMap*>& ls)
{
  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  MDSMap *m = new MDSMap(cct);
  ls.push_back(m);
  common_cleanup(cct);
}

void MDSMap::print(ostream& out)
{
  out << "epoch\t" << epoch << "\n";
  out << "flags\t" << hex << flags << dec << "\n";
  out << "created\t" << created << "\n";
  out << "modified\t" << modified << "\n";
  out << "last_failure\t" << last_failure << "\n"
      << "last_failure_osd_epoch\t" << last_failure_osd_epoch << "\n";
  out << "up\t" << up << "\n"
      << "failed\t" << failed << "\n"
      << "stopped\t" << stopped << "\n";

  multimap< pair<unsigned,unsigned>, uint64_t > foo;
  for (map<uint64_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       ++p) {
    mds_info_t& info = p->second;

    out << p->first << ":\t"
	<< info.addr
	<< " '" << info.name << "'"
	<< " " << ceph_mds_state_name(info.state)
	<< " seq " << info.state_seq;
    if (info.laggy())
      out << " laggy since " << info.laggy_since;
    out << "\n";
  }
}



void MDSMap::print_summary(Formatter *f, ostream *out)
{
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    f->dump_unsigned("up", up.size());
  } else {
    *out << "e" << get_epoch() << ": " << up.size() << " up";
  }

  for (map<uint64_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       ++p) {
    string s = ceph_mds_state_name(p->second.state);
    by_state[s]++;
  }

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); ++p) {
    if (f) {
      f->dump_unsigned(p->first.c_str(), p->second);
    } else {
      *out << ", " << p->second << " " << p->first;
    }
  }

  if (!failed.empty()) {
    if (f) {
      f->dump_unsigned("failed", failed.size());
    } else {
      *out << ", " << failed.size() << " failed";
    }
  }
  //if (stopped.size())
  //out << ", " << stopped.size() << " stopped";
}

void MDSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  if (!failed.empty()) {
    std::ostringstream oss;
    oss << "mds health"
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
  ::encode(state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::mds_info_t::encode_unversioned(bufferlist& bl) const
{
  uint8_t struct_v = 84;
  ::encode(struct_v, bl);
  ::encode(global_id, bl);
  ::encode(name, bl);
  ::encode(state, bl);
  ::encode(state_seq, bl);
  ::encode(addr, bl);
  ::encode(laggy_since, bl);
}

void MDSMap::mds_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(84, 84, 4, bl);
  ::decode(global_id, bl);
  ::decode(name, bl);
  ::decode(state, bl);
  ::decode(state_seq, bl);
  ::decode(addr, bl);
  ::decode(laggy_since, bl);
  ::decode(export_targets, bl);
  DECODE_FINISH(bl);
}



void MDSMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(84, 84, bl);
  ::encode(epoch, bl);
  ::encode(flags, bl);
  ::encode(last_failure, bl);
  ::encode(mds_info, bl, features);

  // kclient ignores everything from here
  uint16_t ev = 87;
  ::encode(ev, bl);
  ::encode(created, bl);
  ::encode(modified, bl);
  ::encode(up, bl);
  ::encode(failed, bl);
  ::encode(stopped, bl);
  ::encode(last_failure_osd_epoch, bl);
  ENCODE_FINISH(bl);
}

void MDSMap::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN_16(84, 84, 4, p);
  ::decode(epoch, p);
  ::decode(flags, p);
  ::decode(last_failure, p);
  ::decode(mds_info, p);

  // kclient ignores everything from here
  uint16_t ev = 87;
  ::decode(ev, p);
  ::decode(created, p);
  ::decode(modified, p);
  ::decode(up, p);
  ::decode(failed, p);
  ::decode(stopped, p);
  ::decode(last_failure_osd_epoch, p);
  DECODE_FINISH(p);
}
