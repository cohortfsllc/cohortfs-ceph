// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "include/ceph_time.h"
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "OSDMap.h"

#include "common/config.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"
#include "include/str_map.h"

#include "global/global_init.h"

using namespace std::literals;

#define dout_subsys ceph_subsys_osd

// ----------------------------------
// osd_info_t

void osd_info_t::dump(Formatter *f) const
{
  f->dump_int("up_from", up_from);
  f->dump_int("up_thru", up_thru);
  f->dump_int("down_at", down_at);
}

void osd_info_t::encode(bufferlist& bl) const
{
  uint8_t struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(up_from, bl);
  ::encode(up_thru, bl);
  ::encode(down_at, bl);
}

void osd_info_t::decode(bufferlist::iterator& bl)
{
  uint8_t struct_v;
  ::decode(struct_v, bl);
  ::decode(up_from, bl);
  ::decode(up_thru, bl);
  ::decode(down_at, bl);
}

void osd_info_t::generate_test_instances(list<osd_info_t*>& o)
{
  o.push_back(new osd_info_t);
  o.push_back(new osd_info_t);
  o.back()->up_from = 30;
  o.back()->up_thru = 40;
  o.back()->down_at = 5;
}

ostream& operator<<(ostream& out, const osd_info_t& info)
{
  out << "up_from " << info.up_from
      << " up_thru " << info.up_thru
      << " down_at " << info.down_at;
    return out;
}

// ----------------------------------
// osd_xinfo_t

void osd_xinfo_t::dump(Formatter *f) const
{
  f->dump_stream("down_stamp") << down_stamp;
  f->dump_float("laggy_probability", laggy_probability);
  f->dump_stream("laggy_interval") << laggy_interval;
  f->dump_int("features", features);
}

void osd_xinfo_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 1, bl);
  ::encode(down_stamp, bl);
  uint32_t lp = laggy_probability * 0xfffffffful;
  ::encode(lp, bl);
  ::encode(laggy_interval, bl);
  ::encode(features, bl);
  ENCODE_FINISH(bl);
}

void osd_xinfo_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  ::decode(down_stamp, bl);
  uint32_t lp;
  ::decode(lp, bl);
  laggy_probability = (float)lp / (float)0xffffffff;
  ::decode(laggy_interval, bl);
  if (struct_v >= 2)
    ::decode(features, bl);
  else
    features = 0;
  DECODE_FINISH(bl);
}

void osd_xinfo_t::generate_test_instances(list<osd_xinfo_t*>& o)
{
  o.push_back(new osd_xinfo_t);
  o.push_back(new osd_xinfo_t);
  o.back()->down_stamp
    = ceph::real_time(std::chrono::seconds(2) +
		      std::chrono::nanoseconds(3));
  o.back()->laggy_probability = .123;
  o.back()->laggy_interval = 123456s;
}

ostream& operator<<(ostream& out, const osd_xinfo_t& xi)
{
  return out << "down_stamp " << xi.down_stamp
	     << " laggy_probability " << xi.laggy_probability
	     << " laggy_interval " << xi.laggy_interval;
}

// ----------------------------------
// OSDMap::Incremental

int OSDMap::Incremental::get_net_marked_out(const OSDMap *previous) const
{
  int n = 0;
  for (map<int32_t,uint32_t>::const_iterator p = new_weight.begin();
       p != new_weight.end();
       ++p) {
    if (p->second == CEPH_OSD_OUT && !previous->is_out(p->first))
      n++;  // marked out
    if (p->second != CEPH_OSD_OUT && previous->is_out(p->first))
      n--;  // marked in
  }
  return n;
}

int OSDMap::Incremental::get_net_marked_down(const OSDMap *previous) const
{
  int n = 0;
  for (map<int32_t,uint8_t>::const_iterator p = new_state.begin();
       p != new_state.end();
       ++p) {
    if (p->second & CEPH_OSD_UP) {
      if (previous->is_up(p->first))
	n++;  // marked down
      else
	n--;  // marked up
    }
  }
  return n;
}

int OSDMap::Incremental::identify_osd(const boost::uuids::uuid u) const
{
  for (auto p = new_uuid.begin();
       p != new_uuid.end();
       ++p)
    if (p->second == u)
      return p->first;
  return -1;
}

void OSDMap::Incremental::encode(bufferlist& bl, uint64_t features) const
{
  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(7, 7, bl);

  {
    ENCODE_START(3, 1, bl); // client-usable data
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(modified, bl);
    ::encode(new_flags, bl);
    ::encode(fullmap, bl);

    ::encode(new_max_osd, bl);
    ::encode(new_up_client, bl);
    ::encode(new_state, bl);
    ::encode(new_weight, bl);
    ::encode(new_primary_affinity, bl);
    ::encode(placer_additions, bl);
    ::encode(placer_removals, bl);
    ::encode(vol_additions, bl);
    ::encode(vol_removals, bl);
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    ENCODE_START(2, 1, bl); // extended, osd-only data
    ::encode(new_hb_back_up, bl);
    ::encode(new_up_thru, bl);
    ::encode(new_blacklist, bl);
    ::encode(old_blacklist, bl);
    ::encode(new_up_cluster, bl);
    ::encode(new_uuid, bl);
    ::encode(new_xinfo, bl);
    ::encode(new_hb_front_up, bl);
    ::encode(features, bl);	    // NOTE: features arg, not the member
    ENCODE_FINISH(bl); // osd-only data
  }

  ENCODE_FINISH(bl); // meta-encoding wrapper

}

void OSDMap::Incremental::decode(bufferlist::iterator& bl)
{
  /**
   * Older encodings of the Incremental had a single struct_v which
   * covered the whole encoding, and was prior to our modern
   * stuff which includes a compatv and a size. So if we see
   * a struct_v < 7, we must rewind to the beginning and use our
   * classic decoder.
   */
  DECODE_START_LEGACY_COMPAT_LEN(7, 7, 7, bl); // wrapper
  {
    DECODE_START(3, bl); // client-usable data
    ::decode(fsid, bl);
    ::decode(epoch, bl);
    ::decode(modified, bl);
    ::decode(new_flags, bl);
    ::decode(fullmap, bl);

    ::decode(new_max_osd, bl);
    ::decode(new_up_client, bl);
    ::decode(new_state, bl);
    ::decode(new_weight, bl);
    ::decode(new_primary_affinity, bl);
    ::decode(placer_additions, bl);
    ::decode(placer_removals, bl);
    ::decode(vol_additions, bl);
    ::decode(vol_removals, bl);
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(2, bl); // extended, osd-only data
    ::decode(new_hb_back_up, bl);
    ::decode(new_up_thru, bl);
    ::decode(new_blacklist, bl);
    ::decode(old_blacklist, bl);
    ::decode(new_up_cluster, bl);
    ::decode(new_uuid, bl);
    ::decode(new_xinfo, bl);
    ::decode(new_hb_front_up, bl);
    if (struct_v >= 2)
      ::decode(encode_features, bl);
    else
      encode_features = 0;
    DECODE_FINISH(bl); // osd-only data
  }

  DECODE_FINISH(bl); // wrapper
}

void OSDMap::Incremental::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("fsid") << fsid;
  f->dump_stream("modified") << modified;
  f->dump_int("new_flags", new_flags);

  if (fullmap.length()) {
    f->open_object_section("full_map");
    OSDMap full;
    bufferlist fbl = fullmap;  // kludge around constness.
    bufferlist::iterator p = fbl.begin();
    full.decode(p);
    full.dump(f);
    f->close_section();
  }

  f->dump_int("new_max_osd", new_max_osd);

  f->open_array_section("new_up_osds");
  for (map<int32_t,entity_addr_t>::const_iterator p = new_up_client.begin(); p != new_up_client.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_stream("public_addr") << p->second;
    f->dump_stream("cluster_addr") << new_up_cluster.find(p->first)->second;
    f->dump_stream("heartbeat_back_addr") << new_hb_back_up.find(p->first)->second;
    map<int32_t, entity_addr_t>::const_iterator q;
    if ((q = new_hb_front_up.find(p->first)) != new_hb_front_up.end())
      f->dump_stream("heartbeat_front_addr") << q->second;
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_weight");
  for (map<int32_t,uint32_t>::const_iterator p = new_weight.begin(); p != new_weight.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("weight", p->second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_state_xor");
  for (map<int32_t,uint8_t>::const_iterator p = new_state.begin(); p != new_state.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    set<string> st;
    calc_state_set(new_state.find(p->first)->second, st);
    f->open_array_section("state_xor");
    for (set<string>::iterator p = st.begin(); p != st.end(); ++p)
      f->dump_string("state", *p);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_up_thru");
  for (map<int32_t,uint32_t>::const_iterator p = new_up_thru.begin(); p != new_up_thru.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("up_thru", p->second);
    f->close_section();
  }
  f->close_section();

    f->open_array_section("new_blacklist");
  for (auto p = new_blacklist.begin();
       p != new_blacklist.end();
       ++p) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();
  f->open_array_section("old_blacklist");
  for (vector<entity_addr_t>::const_iterator p = old_blacklist.begin(); p != old_blacklist.end(); ++p)
    f->dump_stream("addr") << *p;
  f->close_section();

  f->open_array_section("new_xinfo");
  for (map<int32_t,osd_xinfo_t>::const_iterator p = new_xinfo.begin(); p != new_xinfo.end(); ++p) {
    f->open_object_section("xinfo");
    f->dump_int("osd", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_uuid");
  for (auto p = new_uuid.begin(); p != new_uuid.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_stream("uuid") << p->second;
    f->close_section();
  }
  f->close_section();
}

void OSDMap::Incremental::generate_test_instances(list<Incremental*>& o)
{
  o.push_back(new Incremental);
}

void OSDMap::Incremental::vol_inc_add::encode(bufferlist& bl,
					      uint64_t features) const
{
  ::encode(sequence, bl);
  ::encode(vol, bl);
}

void OSDMap::Incremental::vol_inc_add::decode(bufferlist::iterator &p)
{
  ::decode(sequence, p);
  ::decode(vol, p);
}

void OSDMap::Incremental::vol_inc_remove::encode(bufferlist& bl, uint64_t features) const
{
  ::encode(sequence, bl);
  ::encode(id, bl);
}

void OSDMap::Incremental::vol_inc_remove::decode(bufferlist::iterator &p)
{
  ::decode(sequence, p);
  ::decode(id, p);
}

void OSDMap::Incremental::placer_inc_add::encode(bufferlist& bl, uint64_t features) const
{
  ::encode(sequence, bl);
  ::encode(*placer, bl);
}

void OSDMap::Incremental::placer_inc_add::decode(bufferlist::iterator &p)
{
  ::decode(sequence, p);
  placer = Placer::decode_placer(p);
}

void OSDMap::Incremental::placer_inc_remove::encode(bufferlist& bl, uint64_t features) const
{
  ::encode(sequence, bl);
  ::encode(id, bl);
}

void OSDMap::Incremental::placer_inc_remove::decode(bufferlist::iterator &p)
{
  ::decode(sequence, p);
  ::decode(id, p);
}

// ----------------------------------
// OSDMap

void OSDMap::set_epoch(epoch_t e)
{
  epoch = e;
}

bool OSDMap::is_blacklisted(const entity_addr_t& a) const
{
  if (blacklist.empty())
    return false;

  // this specific instance?
  if (blacklist.count(a))
    return true;

  // is entire ip blacklisted?
  if (a.is_ip()) {
    entity_addr_t b = a;
    b.set_port(0);
    b.set_nonce(0);
    if (blacklist.count(b)) {
      return true;
    }
  }

  return false;
}

void OSDMap::get_blacklist(list<pair<entity_addr_t,
			   ceph::real_time> > *bl) const
{
  for (auto it = blacklist.begin() ;
       it != blacklist.end(); ++it) {
    bl->push_back(*it);
  }
}

void OSDMap::set_max_osd(int m)
{
  int o = max_osd;
  max_osd = m;
  osd_state.resize(m);
  osd_weight.resize(m);
  for (; o<max_osd; o++) {
    osd_state[o] = 0;
    osd_weight[o] = CEPH_OSD_OUT;
  }
  osd_info.resize(m);
  osd_xinfo.resize(m);
  osd_addrs->client_addr.resize(m);
  osd_addrs->cluster_addr.resize(m);
  osd_addrs->hb_back_addr.resize(m);
  osd_addrs->hb_front_addr.resize(m);
  osd_uuid->resize(m);

  calc_num_osds();
}

int OSDMap::calc_num_osds()
{
  num_osd = 0;
  for (int i=0; i<max_osd; i++)
    if (osd_state[i] & CEPH_OSD_EXISTS)
      num_osd++;
  return num_osd;
}

void OSDMap::get_all_osds(set<int32_t>& ls) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i))
      ls.insert(i);
}

void OSDMap::get_up_osds(set<int32_t>& ls) const
{
  for (int i = 0; i < max_osd; i++) {
    if (is_up(i))
      ls.insert(i);
  }
}

unsigned OSDMap::get_num_up_osds() const
{
  unsigned n = 0;
  for (int i=0; i<max_osd; i++)
    if ((osd_state[i] & CEPH_OSD_EXISTS) &&
	(osd_state[i] & CEPH_OSD_UP)) n++;
  return n;
}

unsigned OSDMap::get_num_in_osds() const
{
  unsigned n = 0;
  for (int i=0; i<max_osd; i++)
    if ((osd_state[i] & CEPH_OSD_EXISTS) &&
	get_weight(i) != CEPH_OSD_OUT) n++;
  return n;
}

void OSDMap::calc_state_set(int state, set<string>& st)
{
  unsigned t = state;
  for (unsigned s = 1; t; s <<= 1) {
    if (t & s) {
      t &= ~s;
      st.insert(ceph_osd_state_name(s));
    }
  }
}

void OSDMap::adjust_osd_weights(const map<int,double>& weights, Incremental& inc) const
{
  float max = 0;
  for (map<int,double>::const_iterator p = weights.begin();
       p != weights.end(); ++p) {
    if (p->second > max)
      max = p->second;
  }

  for (map<int,double>::const_iterator p = weights.begin();
       p != weights.end(); ++p) {
    inc.new_weight[p->first] = (unsigned)((p->second / max) * CEPH_OSD_IN);
  }
}

int OSDMap::identify_osd(const entity_addr_t& addr) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i) == addr || get_cluster_addr(i) == addr))
      return i;
  return -1;
}

int OSDMap::identify_osd(const boost::uuids::uuid& u) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && get_uuid(i) == u)
      return i;
  return -1;
}

bool OSDMap::find_osd_on_ip(const entity_addr_t& ip) const
{
  for (int i=0; i<max_osd; i++)
    if (exists(i) && (get_addr(i).is_same_host(ip) || get_cluster_addr(i).is_same_host(ip)))
      return i;
  return -1;
}


uint64_t OSDMap::get_features(uint64_t *pmask) const
{
  uint64_t features = 0;  // things we actually have
  uint64_t mask = 0;	  // things we could have

  if (pmask)
    *pmask = mask;
  return features;
}

uint64_t OSDMap::get_up_osd_features() const
{
  bool first = true;
  uint64_t features = 0;
  for (int osd = 0; osd < max_osd; ++osd) {
    if (!is_up(osd))
      continue;
    const osd_xinfo_t &xi = get_xinfo(osd);
    if (first) {
      features = xi.features;
      first = false;
    } else {
      features &= xi.features;
    }
  }
  return features;
}

void OSDMap::dedup(const OSDMap *o, OSDMap *n)
{
  if (o->epoch == n->epoch)
    return;

  int diff = 0;

  // do addrs match?
  if (o->max_osd != n->max_osd)
    diff++;
  for (int i = 0; i < o->max_osd && i < n->max_osd; i++) {
    if ( n->osd_addrs->client_addr[i] &&  o->osd_addrs->client_addr[i] &&
	*n->osd_addrs->client_addr[i] == *o->osd_addrs->client_addr[i])
      n->osd_addrs->client_addr[i] = o->osd_addrs->client_addr[i];
    else
      diff++;
    if ( n->osd_addrs->cluster_addr[i] &&  o->osd_addrs->cluster_addr[i] &&
	*n->osd_addrs->cluster_addr[i] == *o->osd_addrs->cluster_addr[i])
      n->osd_addrs->cluster_addr[i] = o->osd_addrs->cluster_addr[i];
    else
      diff++;
    if ( n->osd_addrs->hb_back_addr[i] &&  o->osd_addrs->hb_back_addr[i] &&
	*n->osd_addrs->hb_back_addr[i] == *o->osd_addrs->hb_back_addr[i])
      n->osd_addrs->hb_back_addr[i] = o->osd_addrs->hb_back_addr[i];
    else
      diff++;
    if ( n->osd_addrs->hb_front_addr[i] &&  o->osd_addrs->hb_front_addr[i] &&
	*n->osd_addrs->hb_front_addr[i] == *o->osd_addrs->hb_front_addr[i])
      n->osd_addrs->hb_front_addr[i] = o->osd_addrs->hb_front_addr[i];
    else
      diff++;
  }
  if (diff == 0) {
    // zoinks, no differences at all!
    n->osd_addrs = o->osd_addrs;
  }

  // do uuids match?
  if (o->osd_uuid->size() == n->osd_uuid->size() &&
      *o->osd_uuid == *n->osd_uuid)
    n->osd_uuid = o->osd_uuid;
}

void OSDMap::apply_incremental(const Incremental &inc)
{
  new_blacklist_entries = false;
  if (inc.epoch == 1)
    fsid = inc.fsid;
  else if (inc.fsid != fsid)
    throw std::system_error(std::make_error_code(std::errc::invalid_argument),
			    "Incremental FSID "s +
			    boost::uuids::to_string(inc.fsid) +
			    " does not match map FSID "s +
			    boost::uuids::to_string(fsid));

  assert(inc.epoch == epoch+1);
  epoch++;
  modified = inc.modified;

  // full map?
  if (inc.fullmap.length()) {
    bufferlist bl(inc.fullmap);
    decode(bl);
    return;
  }

  // nope, incremental.
  if (inc.new_flags >= 0)
    flags = inc.new_flags;

  if (inc.new_max_osd >= 0)
    set_max_osd(inc.new_max_osd);

  for (map<int32_t,uint32_t>::const_iterator i = inc.new_weight.begin();
       i != inc.new_weight.end();
       ++i) {
    set_weight(i->first, i->second);

    // if we are marking in, clear the AUTOOUT and NEW bits.
    if (i->second)
      osd_state[i->first] &= ~(CEPH_OSD_AUTOOUT | CEPH_OSD_NEW);
  }

  // up/down
  for (map<int32_t,uint8_t>::const_iterator i = inc.new_state.begin();
       i != inc.new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if ((osd_state[i->first] & CEPH_OSD_UP) &&
	(s & CEPH_OSD_UP)) {
      osd_info[i->first].down_at = epoch;
      osd_xinfo[i->first].down_stamp = modified;
    }
    if ((osd_state[i->first] & CEPH_OSD_EXISTS) &&
	(s & CEPH_OSD_EXISTS))
      (*osd_uuid)[i->first] = boost::uuids::nil_uuid();
    osd_state[i->first] ^= s;
  }
  for (map<int32_t,entity_addr_t>::const_iterator i = inc.new_up_client.begin();
       i != inc.new_up_client.end();
       ++i) {
    osd_state[i->first] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
    osd_addrs->client_addr[i->first].reset(new entity_addr_t(i->second));
    if (inc.new_hb_back_up.empty())
      osd_addrs->hb_back_addr[i->first].reset(new entity_addr_t(i->second)); //this is a backward-compatibility hack
    else
      osd_addrs->hb_back_addr[i->first].reset(
	new entity_addr_t(inc.new_hb_back_up.find(i->first)->second));
    map<int32_t,entity_addr_t>::const_iterator j = inc.new_hb_front_up.find(i->first);
    if (j != inc.new_hb_front_up.end())
      osd_addrs->hb_front_addr[i->first].reset(new entity_addr_t(j->second));
    else
      osd_addrs->hb_front_addr[i->first].reset();

    osd_info[i->first].up_from = epoch;
  }
  for (map<int32_t,entity_addr_t>::const_iterator i = inc.new_up_cluster.begin();
       i != inc.new_up_cluster.end();
       ++i)
    osd_addrs->cluster_addr[i->first].reset(new entity_addr_t(i->second));

  // info
  for (map<int32_t,epoch_t>::const_iterator i = inc.new_up_thru.begin();
       i != inc.new_up_thru.end();
       ++i)
    osd_info[i->first].up_thru = i->second;

  // xinfo
  for (map<int32_t,osd_xinfo_t>::const_iterator p = inc.new_xinfo.begin(); p != inc.new_xinfo.end(); ++p)
    osd_xinfo[p->first] = p->second;

  // uuid
  for (const auto& p : inc.new_uuid)
    (*osd_uuid)[p.first] = p.second;

  // blacklist
  for (auto p = inc.new_blacklist.begin();
       p != inc.new_blacklist.end();
       ++p) {
    blacklist[p->first] = p->second;
    new_blacklist_entries = true;
  }
  for (vector<entity_addr_t>::const_iterator p = inc.old_blacklist.begin();
       p != inc.old_blacklist.end();
       ++p)
    blacklist.erase(*p);

  calc_num_osds();

  for (const auto& removal : inc.placer_removals) {
    remove_placer(removal.id);
  }

  for (vector<OSDMap::Incremental::placer_inc_add>::const_iterator p =
      inc.placer_additions.begin();
      p != inc.placer_additions.end();
      ++p) {
    // Is this necessary? The monitor at least needs the incremental
    // after applying it.
    add_placer(p->placer->clone());
  }

  for (const auto& removal : inc.vol_removals) {
    remove_volume(removal.id);
  }

  for (vector<OSDMap::Incremental::vol_inc_add>::const_iterator p =
      inc.vol_additions.begin();
      p != inc.vol_additions.end();
      ++p) {
    add_volume(p->vol);
  }
}

// serialize, unserialize
void OSDMap::encode(bufferlist& bl, uint64_t features) const
{
  // meta-encoding: how we include client-used and osd-specific data
  ENCODE_START(7, 7, bl);

  {
    ENCODE_START(3, 1, bl); // client-usable data
    // base
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(created, bl);
    ::encode(modified, bl);

    ::encode(flags, bl);

    ::encode(max_osd, bl);
    ::encode(osd_state, bl);
    ::encode(osd_weight, bl);
    ::encode(osd_addrs->client_addr, bl);

    uint32_t count;
    count = placers.by_uuid.size();
    ::encode(count, bl);
    for (const auto& p : placers.by_uuid) {
      p.second->encode(bl);
    }

    count = vols.by_uuid.size();
    ::encode(count, bl);
    for (const auto& v : vols.by_uuid) {
      v.second.encode(bl);
    }
    ENCODE_FINISH(bl); // client-usable data
  }

  {
    ENCODE_START(1, 1, bl); // extended, osd-only data
    ::encode(osd_addrs->hb_back_addr, bl);
    ::encode(osd_info, bl);
    ::encode(blacklist, bl);
    ::encode(osd_addrs->cluster_addr, bl);
    ::encode(*osd_uuid, bl);
    ::encode(osd_xinfo, bl);
    ::encode(osd_addrs->hb_front_addr, bl);
    ENCODE_FINISH(bl); // osd-only data
  }

  ENCODE_FINISH(bl); // meta-encoding wrapper
}

void OSDMap::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}

void OSDMap::decode(bufferlist::iterator& bl)
{
  DECODE_START(7, bl); // wrapper
  {
    DECODE_START(3, bl); // client-usable data
    // base
    ::decode(fsid, bl);
    ::decode(epoch, bl);
    ::decode(created, bl);
    ::decode(modified, bl);

    ::decode(flags, bl);

    ::decode(max_osd, bl);
    ::decode(osd_state, bl);
    ::decode(osd_weight, bl);
    ::decode(osd_addrs->client_addr, bl);

    // Placers
    uint32_t count;
    placers.by_uuid.clear();
    placers.by_name.clear();
    ::decode(count, bl);
    for (uint32_t i = 0; i < count; ++i) {
      PlacerRef v = Placer::decode_placer(bl);
      auto r = placers.by_uuid.emplace(v->id, std::move(v));
      placers.by_name.emplace(r.first->second->name, r.first->second.get());
    }

    // Volumes
    vols.by_uuid.clear();
    vols.by_name.clear();
    ::decode(count, bl);
    for (uint32_t i = 0; i < count; ++i) {
      Volume v;
      ::decode(v, bl);
      auto r = vols.by_uuid.emplace(v.id, v);
      vols.by_name.emplace(r.first->second.name, &r.first->second);
    }
    DECODE_FINISH(bl); // client-usable data
  }

  {
    DECODE_START(1, bl); // extended, osd-only data
    ::decode(osd_addrs->hb_back_addr, bl);
    ::decode(osd_info, bl);
    ::decode(blacklist, bl);
    ::decode(osd_addrs->cluster_addr, bl);
    ::decode(*osd_uuid, bl);
    ::decode(osd_xinfo, bl);
    ::decode(osd_addrs->hb_front_addr, bl);
    DECODE_FINISH(bl); // osd-only data
  }

  DECODE_FINISH(bl); // wrapper

  post_decode();
}

void OSDMap::post_decode()
{
  calc_num_osds();
}

void OSDMap::dump_json(ostream& out) const
{
  JSONFormatter jsf(true);
  jsf.open_object_section("osdmap");
  dump(&jsf);
  jsf.close_section();
  jsf.flush(out);
}

void OSDMap::dump(Formatter *f) const
{
  f->dump_int("epoch", get_epoch());
  f->dump_stream("fsid") << get_fsid();
  f->dump_stream("created") << get_created();
  f->dump_stream("modified") << get_modified();
  f->dump_string("flags", get_flag_string());
  f->dump_int("max_osd", get_max_osd());

  f->open_array_section("osds");
  for (int i=0; i<get_max_osd(); i++)
    if (exists(i)) {
      f->open_object_section("osd_info");
      f->dump_int("osd", i);
      f->dump_stream("uuid") << get_uuid(i);
      f->dump_int("up", is_up(i));
      f->dump_int("in", is_in(i));
      f->dump_float("weight", get_weightf(i));
      get_info(i).dump(f);
      f->dump_stream("public_addr") << get_addr(i);
      f->dump_stream("cluster_addr") << get_cluster_addr(i);
      f->dump_stream("heartbeat_back_addr") << get_hb_back_addr(i);
      f->dump_stream("heartbeat_front_addr") << get_hb_front_addr(i);

      set<string> st;
      get_state(i, st);
      f->open_array_section("state");
      for (set<string>::iterator p = st.begin(); p != st.end(); ++p)
	f->dump_string("state", *p);
      f->close_section();

      f->close_section();
    }
  f->close_section();

  f->open_array_section("osd_xinfo");
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      f->open_object_section("xinfo");
      f->dump_int("osd", i);
      osd_xinfo[i].dump(f);
      f->close_section();
    }
  }
  f->close_section();

  f->open_array_section("blacklist");
  for (auto p = blacklist.begin();
       p != blacklist.end();
       ++p) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();

  f->open_array_section("volumes");
  for(const auto& v : vols.by_uuid) {
    f->open_object_section("volume_info");
    v.second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("placers");
  for(const auto& v : placers.by_uuid) {
    f->open_object_section("placer_info");
    v.second->dump(f);
    f->close_section();
  }
  f->close_section();
}

void OSDMap::generate_test_instances(list<OSDMap*>& o)
{
  o.push_back(new OSDMap);

  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  o.push_back(new OSDMap);
  boost::uuids::uuid fsid;
  o.back()->build_simple(cct, 1, fsid, 16);
  o.back()->created = o.back()->modified
    = ceph::real_time(std::chrono::seconds(1) +
		      std::chrono::nanoseconds(2));  // fix timestamp
  common_cleanup(cct);
}

string OSDMap::get_flag_string(unsigned f)
{
  string s;
  if ( f& CEPH_OSDMAP_NEARFULL)
    s += ",nearfull";
  if (f & CEPH_OSDMAP_FULL)
    s += ",full";
  if (f & CEPH_OSDMAP_PAUSERD)
    s += ",pauserd";
  if (f & CEPH_OSDMAP_PAUSEWR)
    s += ",pausewr";
  if (f & CEPH_OSDMAP_PAUSEREC)
    s += ",pauserec";
  if (f & CEPH_OSDMAP_NOUP)
    s += ",noup";
  if (f & CEPH_OSDMAP_NODOWN)
    s += ",nodown";
  if (f & CEPH_OSDMAP_NOOUT)
    s += ",noout";
  if (f & CEPH_OSDMAP_NOIN)
    s += ",noin";
  if (s.length())
    s = s.erase(0, 1);
  return s;
}

string OSDMap::get_flag_string() const
{
  return get_flag_string(flags);
}

struct qi {
  int item;
  int depth;
  float weight;
  qi() : item(0), depth(0), weight(0) {}
  qi(int i, int d, float w) : item(i), depth(d), weight(w) {}
};

void OSDMap::print(ostream& out) const
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "created " << get_created() << "\n"
      << "modified " << get_modified() << "\n";

  out << "flags " << get_flag_string() << "\n";

  out << "max_osd " << get_max_osd() << "\n";
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      out << "osd." << i;
      out << (is_up(i) ? " up  ":" down");
      out << (is_in(i) ? " in ":" out");
      out << " weight " << get_weightf(i);
      const osd_info_t& info(get_info(i));
      out << " " << info;
      out << " " << get_addr(i) << " " << get_cluster_addr(i) << " " << get_hb_back_addr(i)
	  << " " << get_hb_front_addr(i);
      set<string> st;
      get_state(i, st);
      out << " " << st;
      if (!get_uuid(i).is_nil())
	out << " " << get_uuid(i);
      out << "\n";
    }
  }
  out << std::endl;

  for (auto p = blacklist.begin(); p != blacklist.end(); ++p)
    out << "blacklist " << p->first << " expires " << p->second << "\n";

  // ignore pg_swap_primary
}

void OSDMap::print_osd_line(int cur, ostream *out, Formatter *f) const
{
  if (f) {
    f->dump_unsigned("id", cur);
    f->dump_stream("name") << "osd." << cur;
    f->dump_unsigned("exists", (int)exists(cur));
    f->dump_int("type_id", 0);
  }
  if (out)
    *out << "osd." << cur << "\t";
  if (!exists(cur)) {
    if (out)
      *out << "DNE\t\t";
  } else {
    if (is_up(cur)) {
      if (out)
	*out << "up\t";
      if (f)
	f->dump_string("status", "up");
    } else {
      if (out)
	*out << "down\t";
      if (f)
	f->dump_string("status", "down");
    }
    if (out) {
      std::streamsize p = out->precision();
      *out << std::setprecision(4)
	   << (exists(cur) ? get_weightf(cur) : 0)
	   << std::setprecision(p)
	   << "\t";
    }
    if (f) {
      f->dump_float("reweight", get_weightf(cur));
    }
  }
}

void OSDMap::print_summary(Formatter *f, ostream& out) const
{
  if (f) {
    f->open_object_section("osdmap");
    f->dump_int("epoch", get_epoch());
    f->dump_int("num_osds", get_num_osds());
    f->dump_int("num_up_osds", get_num_up_osds());
    f->dump_int("num_in_osds", get_num_in_osds());
    f->dump_bool("full", test_flag(CEPH_OSDMAP_FULL) ? true : false);
    f->dump_bool("nearfull", test_flag(CEPH_OSDMAP_NEARFULL) ? true : false);
    f->close_section();
  } else {
    out << "	 osdmap e" << get_epoch() << ": "
	<< get_num_osds() << " osds: "
	<< get_num_up_osds() << " up, "
	<< get_num_in_osds() << " in\n";
    if (flags)
      out << "		  flags " << get_flag_string() << "\n";
  }
}

void OSDMap::print_oneline_summary(ostream& out) const
{
  out << "e" << get_epoch() << ": "
      << get_num_osds() << " osds: "
      << get_num_up_osds() << " up, "
      << get_num_in_osds() << " in";
  if (test_flag(CEPH_OSDMAP_FULL))
    out << " full";
  else if (test_flag(CEPH_OSDMAP_NEARFULL))
    out << " nearfull";
}

int OSDMap::build_simple(CephContext *cct, epoch_t e,
			 const boost::uuids::uuid& fsid,
			 int nosd)
{
  ldout(cct, 10) << "build_simple on " << num_osd
		 << " osds." << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = ceph::real_clock::now();

  if (nosd >=  0) {
    set_max_osd(nosd);
  } else {
    // count osds
    int maxosd = 0, numosd = 0;
    const md_config_t *conf = cct->_conf;
    vector<string> sections;
    conf->get_all_sections(sections);
    for (vector<string>::iterator i = sections.begin(); i != sections.end(); ++i) {
      if (i->find("osd.") != 0)
	continue;

      const char *begin = i->c_str() + 4;
      char *end = (char*)begin;
      int o = strtol(begin, &end, 10);
      if (*end != '\0')
	continue;

      if (o > cct->_conf->mon_max_osd) {
	lderr(cct) << "[osd." << o << "] in config has id > mon_max_osd " << cct->_conf->mon_max_osd << dendl;
	return -ERANGE;
      }
      numosd++;
      if (o > maxosd)
	maxosd = o;
    }

    set_max_osd(maxosd + 1);
  }

  for (int i=0; i<get_max_osd(); i++) {
    set_state(i, 0);
    set_weight(i, CEPH_OSD_OUT);
  }

  return 0;
}

void OSDMap::add_volume(const Volume& vol) {
  vol.valid();

  if (vols.by_uuid.count(vol.id) > 0) {
    throw std::system_error(vol_errc::exists, "UUID "s +
			    boost::uuids::to_string(vol.id) +
			    " already used"s);
  }

  if (vols.by_name.count(vol.name) > 0) {
    throw std::system_error(vol_errc::exists, "Name "s + vol.name +
			    " already used"s);
  }

  if (placers.by_uuid.count(vol.placer_id) == 0) {
    throw std::system_error(placer_errc::no_such_placer,
			    "Referenced placer "s +
			    boost::uuids::to_string(vol.placer_id) +
			    " does not exist"s);
  }

  auto r = vols.by_uuid.emplace(vol.id, vol);
  vols.by_name.emplace(vol.name, &r.first->second);
}

void OSDMap::remove_volume(const boost::uuids::uuid& id)
{
  auto i = vols.by_uuid.find(id);
  if (i == vols.by_uuid.end()) {
    throw std::system_error(vol_errc::no_such_volume,
			    boost::uuids::to_string(id));
  }

  vols.by_name.erase(i->second.name);
  vols.by_uuid.erase(i);
}

void OSDMap::add_placer(PlacerRef&& placer) {
  std::stringstream ss;
  if (!placer->valid(ss)) {
    throw std::system_error(std::make_error_code(std::errc::invalid_argument),
			    ss.str());
  }

  if (placers.by_uuid.count(placer->id) > 0) {
    throw std::system_error(placer_errc::exists, "UUID "s +
			    boost::uuids::to_string(placer->id) +
			    " already used"s);
  }

  if (placers.by_name.count(placer->name) > 0) {
    throw std::system_error(placer_errc::exists, "Name "s + placer->name +
			    " already used"s);
  }

  auto r = placers.by_uuid.emplace(placer->id, std::move(placer));
  placers.by_name.emplace(r.first->second->name, r.first->second.get());
}

void OSDMap::remove_placer(const boost::uuids::uuid& id)
{
  auto i = placers.by_uuid.find(id);

  if (i == placers.by_uuid.end()) {
    throw std::system_error(placer_errc::no_such_placer,
			    boost::uuids::to_string(id));
  }

  placers.by_name.erase(i->second->name);
  placers.by_uuid.erase(i);
}
