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

#include <errno.h>
#include "OSDMap.h"

#include "common/config.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"

#include "common/code_environment.h"

#define dout_subsys ceph_subsys_osd

// ----------------------------------
// osd_info_t

void osd_info_t::dump(Formatter *f) const
{
  f->dump_int("last_clean_begin", last_clean_begin);
  f->dump_int("last_clean_end", last_clean_end);
  f->dump_int("up_from", up_from);
  f->dump_int("up_thru", up_thru);
  f->dump_int("down_at", down_at);
  f->dump_int("lost_at", lost_at);
}

void osd_info_t::encode(bufferlist& bl) const
{
  __u8 struct_v = 1;
  ::encode(struct_v, bl);
  ::encode(last_clean_begin, bl);
  ::encode(last_clean_end, bl);
  ::encode(up_from, bl);
  ::encode(up_thru, bl);
  ::encode(down_at, bl);
  ::encode(lost_at, bl);
}

void osd_info_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  ::decode(last_clean_begin, bl);
  ::decode(last_clean_end, bl);
  ::decode(up_from, bl);
  ::decode(up_thru, bl);
  ::decode(down_at, bl);
  ::decode(lost_at, bl);
}

void osd_info_t::generate_test_instances(list<osd_info_t*>& o)
{
  o.push_back(new osd_info_t);
  o.push_back(new osd_info_t);
  o.back()->last_clean_begin = 1;
  o.back()->last_clean_end = 2;
  o.back()->up_from = 30;
  o.back()->up_thru = 40;
  o.back()->down_at = 5;
  o.back()->lost_at = 6;
}

ostream& operator<<(ostream& out, const osd_info_t& info)
{
  out << "up_from " << info.up_from
      << " up_thru " << info.up_thru
      << " down_at " << info.down_at
      << " last_clean_interval [" << info.last_clean_begin << "," << info.last_clean_end << ")";
  if (info.lost_at)
    out << " lost_at " << info.lost_at;
  return out;
}

// ----------------------------------
// osd_xinfo_t

void osd_xinfo_t::dump(Formatter *f) const
{
  f->dump_stream("down_stamp") << down_stamp;
  f->dump_float("laggy_probability", laggy_probability);
  f->dump_int("laggy_interval", laggy_interval);
}

void osd_xinfo_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(down_stamp, bl);
  __u32 lp = laggy_probability * 0xfffffffful;
  ::encode(lp, bl);
  ::encode(laggy_interval, bl);
  ENCODE_FINISH(bl);
}

void osd_xinfo_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  ::decode(down_stamp, bl);
  __u32 lp;
  ::decode(lp, bl);
  laggy_probability = (float)lp / (float)0xffffffff;
  ::decode(laggy_interval, bl);
  DECODE_FINISH(bl);
}

void osd_xinfo_t::generate_test_instances(list<osd_xinfo_t*>& o)
{
  o.push_back(new osd_xinfo_t);
  o.push_back(new osd_xinfo_t);
  o.back()->down_stamp = utime_t(2, 3);
  o.back()->laggy_probability = .123;
  o.back()->laggy_interval = 123456;
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

int OSDMap::Incremental::identify_osd(uuid_d u) const
{
  for (map<int32_t,uuid_d>::const_iterator p = new_uuid.begin();
       p != new_uuid.end();
       ++p)
    if (p->second == u)
      return p->first;
  return -1;
}

void OSDMap::Incremental::encode(bufferlist& bl, uint64_t features) const
{
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);

  ::encode(new_max_osd, bl);
  ::encode(new_up_client, bl);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);

  // extended
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(new_hb_back_up, bl);
  ::encode(new_up_thru, bl);
  ::encode(new_last_clean_interval, bl);
  ::encode(new_lost, bl);
  ::encode(new_blacklist, bl);
  ::encode(old_blacklist, bl);
  ::encode(new_up_cluster, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(new_uuid, bl);
  ::encode(new_xinfo, bl);
  ::encode(new_hb_front_up, bl);
}

// pre-condition -- the version has already been read and it is at
// least 7
void OSDMap::Incremental::decode(bufferlist::iterator &p)
{
  // base
  __u16 v;
  ::decode(v, p);
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(modified, p);
  ::decode(new_flags, p);
  ::decode(fullmap, p);

  ::decode(new_max_osd, p);
  ::decode(new_up_client, p);
  ::decode(new_state, p);
  ::decode(new_weight, p);

  // decode short map, too.
  if (v == 5 && p.end())
    return;

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(new_hb_back_up, p);
  ::decode(new_up_thru, p);
  ::decode(new_last_clean_interval, p);
  ::decode(new_lost, p);
  ::decode(new_blacklist, p);
  ::decode(old_blacklist, p);
  if (ev >= 6)
    ::decode(new_up_cluster, p);
  if (ev >= 7)
    ::decode(cluster_snapshot, p);
  if (ev >= 8)
    ::decode(new_uuid, p);
  if (ev >= 9)
    ::decode(new_xinfo, p);
  if (ev >= 10)
    ::decode(new_hb_front_up, p);
}

void OSDMap::Incremental::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_stream("fsid") << fsid;
  f->dump_stream("modified") << modified;
  f->dump_int("new_flags", new_flags);

  if (fullmap.length()) {
    f->open_object_section("full_map");
    OSDMap* full = newOSDMap(VolMapRef());
    bufferlist fbl = fullmap;  // kludge around constness.
    bufferlist::iterator p = fbl.begin();
    full->decode(p);
    full->dump(f);
    f->close_section();
    delete full;
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

  f->open_array_section("new_lost");
  for (map<int32_t,uint32_t>::const_iterator p = new_lost.begin(); p != new_lost.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("epoch_lost", p->second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_last_clean_interval");
  for (map<int32_t,pair<epoch_t,epoch_t> >::const_iterator p = new_last_clean_interval.begin();
       p != new_last_clean_interval.end();
       ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_int("first", p->second.first);
    f->dump_int("last", p->second.second);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("new_blacklist");
  for (map<entity_addr_t,utime_t>::const_iterator p = new_blacklist.begin();
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

  if (cluster_snapshot.size())
    f->dump_string("cluster_snapshot", cluster_snapshot);

  f->open_array_section("new_uuid");
  for (map<int32_t,uuid_d>::const_iterator p = new_uuid.begin(); p != new_uuid.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_stream("uuid") << p->second;
    f->close_section();
  }
  f->close_section();
} // dump



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

void OSDMap::get_blacklist(list<pair<entity_addr_t,utime_t> > *bl) const
{
  for (hash_map<entity_addr_t,utime_t>::const_iterator it = blacklist.begin() ;
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

int OSDMap::get_num_up_osds() const
{
  int n = 0;
  for (int i=0; i<max_osd; i++)
    if ((osd_state[i] & CEPH_OSD_EXISTS) &&
	(osd_state[i] & CEPH_OSD_UP)) n++;
  return n;
}

int OSDMap::get_num_in_osds() const
{
  int n = 0;
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

int OSDMap::identify_osd(const uuid_d& u) const
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

// calls virtual function apply_incremental_subclass to finish the
// work; that way this code and do the initial error checks and
// completeness checks and return early.
int OSDMap::apply_incremental(const Incremental &inc)
{
  new_blacklist_entries = false;
  if (inc.epoch == 1)
    fsid = inc.fsid;
  else if (inc.fsid != fsid)
    return -EINVAL;

  assert(inc.epoch == epoch+1);
  epoch++;
  modified = inc.modified;

  // full map?
  if (inc.fullmap.length()) {
    bufferlist bl(inc.fullmap);
    decode(bl);
    return 0;
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
      (*osd_uuid)[i->first] = uuid_d();
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
  for (map<int32_t,pair<epoch_t,epoch_t> >::const_iterator i = inc.new_last_clean_interval.begin();
       i != inc.new_last_clean_interval.end();
       ++i) {
    osd_info[i->first].last_clean_begin = i->second.first;
    osd_info[i->first].last_clean_end = i->second.second;
  }
  for (map<int32_t,epoch_t>::const_iterator p = inc.new_lost.begin(); p != inc.new_lost.end(); ++p)
    osd_info[p->first].lost_at = p->second;

  // xinfo
  for (map<int32_t,osd_xinfo_t>::const_iterator p = inc.new_xinfo.begin(); p != inc.new_xinfo.end(); ++p)
    osd_xinfo[p->first] = p->second;

  // uuid
  for (map<int32_t,uuid_d>::const_iterator p = inc.new_uuid.begin(); p != inc.new_uuid.end(); ++p) 
    (*osd_uuid)[p->first] = p->second;

  // blacklist
  for (map<entity_addr_t,utime_t>::const_iterator p = inc.new_blacklist.begin();
       p != inc.new_blacklist.end();
       ++p) {
    blacklist[p->first] = p->second;
    new_blacklist_entries = true;
  }
  for (vector<entity_addr_t>::const_iterator p = inc.old_blacklist.begin();
       p != inc.old_blacklist.end();
       ++p)
    blacklist.erase(*p);

  // cluster snapshot?
  if (inc.cluster_snapshot.length()) {
    cluster_snapshot = inc.cluster_snapshot;
    cluster_snapshot_epoch = inc.epoch;
  } else {
    cluster_snapshot.clear();
    cluster_snapshot_epoch = 0;
  }

  calc_num_osds();

  return apply_incremental_subclass(inc);
} // OSDMap::apply_incremental


void OSDMap::_remove_nonexistent_osds(vector<int>& osds) const
{
  unsigned removed = 0;
  for (unsigned i = 0; i < osds.size(); i++) {
    if (!exists(osds[i])) {
      removed++;
      continue;
    }
    if (removed) {
      osds[i - removed] = osds[i];
    }
  }
  if (removed)
    osds.resize(osds.size() - removed);
}


void OSDMap::encodeOSDMap(bufferlist& bl, uint64_t features) const
{
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

  // extended
  __u16 ev = 10;
  ::encode(ev, bl);
  ::encode(osd_addrs->hb_back_addr, bl);
  ::encode(osd_info, bl);
  ::encode(blacklist, bl);
  ::encode(osd_addrs->cluster_addr, bl);
  ::encode(cluster_snapshot_epoch, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(*osd_uuid, bl);
  ::encode(osd_xinfo, bl);
  ::encode(osd_addrs->hb_front_addr, bl);
}


void OSDMap::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}


void OSDMap::decodeOSDMap(bufferlist::iterator& p, __u16 v)
{
  // base
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(created, p);
  ::decode(modified, p);
  ::decode(flags, p);
  ::decode(max_osd, p);
  ::decode(osd_state, p);
  ::decode(osd_weight, p);
  ::decode(osd_addrs->client_addr, p);

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(osd_addrs->hb_back_addr, p);
  ::decode(osd_info, p);

  ::decode(blacklist, p);
  if (ev >= 6)
    ::decode(osd_addrs->cluster_addr, p);
  else
    osd_addrs->cluster_addr.resize(osd_addrs->client_addr.size());

  if (ev >= 7) {
    ::decode(cluster_snapshot_epoch, p);
    ::decode(cluster_snapshot, p);
  }

  if (ev >= 8) {
    ::decode(*osd_uuid, p);
  } else {
    osd_uuid->resize(max_osd);
  }
  if (ev >= 9)
    ::decode(osd_xinfo, p);
  else
    osd_xinfo.resize(max_osd);

  if (ev >= 10)
    ::decode(osd_addrs->hb_front_addr, p);
  else
    osd_addrs->hb_front_addr.resize(osd_addrs->hb_back_addr.size());

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
  f->dump_string("cluster_snapshot", get_cluster_snapshot());
  f->dump_int("max_osd", get_max_osd());

  f->open_array_section("osds");
  for (int i=0; i<get_max_osd(); i++)
    if (exists(i)) {
      f->open_object_section("osd_info");
      f->dump_int("osd", i);
      f->dump_stream("uuid") << get_uuid(i);
      f->dump_int("up", is_up(i));
      f->dump_int("in", is_in(i));
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
  for (hash_map<entity_addr_t,utime_t>::const_iterator p = blacklist.begin();
       p != blacklist.end();
       ++p) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();
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
  if (f & CEPH_OSDMAP_NOBACKFILL)
    s += ",nobackfill";
  if (f & CEPH_OSDMAP_NORECOVER)
    s += ",norecover";
  if (f & CEPH_OSDMAP_NOSCRUB)
    s += ",noscrub";
  if (f & CEPH_OSDMAP_NODEEP_SCRUB)
    s += ",nodeep-scrub";
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
  if (get_cluster_snapshot().length())
    out << "cluster_snapshot " << get_cluster_snapshot() << "\n";
  out << "\n";


  out << "max_osd " << get_max_osd() << "\n";
  for (int i = 0; i<get_max_osd(); i++) {
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
      if (!get_uuid(i).is_zero())
	out << " " << get_uuid(i);
      out << "\n";
    }
  }
  out << std::endl;

  for (hash_map<entity_addr_t,utime_t>::const_iterator p = blacklist.begin();
       p != blacklist.end();
       ++p)
    out << "blacklist " << p->first << " expires " << p->second << "\n";
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

void OSDMap::print_summary(ostream& out) const
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
