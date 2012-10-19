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

void OSDMap::Incremental::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  int32_t new_t = pgIncremental->new_pool_max;
  ::encode(new_t, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  // for ::encode(new_pools, bl);
  __u32 n = pgIncremental->new_pools.size();
  ::encode(n, bl);
  for (map<int64_t,pg_pool_t>::const_iterator p =
	 pgIncremental->new_pools.begin();
       p != pgIncremental->new_pools.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl, 0);
  }
  // for ::encode(new_pool_names, bl);
  n = pgIncremental->new_pool_names.size();
  ::encode(n, bl);
  for (map<int64_t, string>::const_iterator p =
	 pgIncremental->new_pool_names.begin();
       p != pgIncremental->new_pool_names.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl);
  }
  // for ::encode(old_pools, bl);
  n = pgIncremental->old_pools.size();
  ::encode(n, bl);
  for (set<int64_t>::iterator p = pgIncremental->old_pools.begin();
       p != pgIncremental->old_pools.end();
       ++p) {
    n = *p;
    ::encode(n, bl);
  }
  ::encode(new_up_client, bl);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);
  // for ::encode(new_pg_temp, bl);
  n = pgIncremental->new_pg_temp.size();
  ::encode(n, bl);
  for (map<pg_t,vector<int32_t> >::const_iterator p =
	 pgIncremental->new_pg_temp.begin();
       p != pgIncremental->new_pg_temp.end();
       ++p) {
    old_pg_t opg = p->first.get_old_pg();
    ::encode(opg, bl);
    ::encode(p->second, bl);
  }
}

void OSDMap::Incremental::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  // base
  __u16 v = 7;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  ::encode(new_up_client, bl);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);

  // extended
  __u16 ev = 8;
  ::encode(ev, bl);
  ::encode(new_hb_up, bl);
  ::encode(new_up_thru, bl);
  ::encode(new_last_clean_interval, bl);
  ::encode(new_lost, bl);
  ::encode(new_blacklist, bl);
  ::encode(old_blacklist, bl);
  ::encode(new_up_internal, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(new_uuid, bl);

  // pgIncrement -- will eventually be more general
  ::encode(*pgIncremental, bl, features);
}

void OSDMap::Incremental::decode(bufferlist::iterator &p)
{
  __u32 n, t;
  // base
  __u16 v;
  ::decode(v, p);
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(modified, p);

  if (v == 4 || v == 5) {
    ::decode(n, p);
    pgIncremental->new_pool_max = n;
  } else if (v == 6) {
    ::decode(pgIncremental->new_pool_max, p);
  } else if (v >= 7) {
    // loaded in call-out below
  }

  ::decode(new_flags, p);
  ::decode(fullmap, p);
  ::decode(crush, p);

  ::decode(new_max_osd, p);
  if (v < 6) {
    pgIncremental->new_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(pgIncremental->new_pools[t], p);
    }
  } else if (v == 6) {
    ::decode(pgIncremental->new_pools, p);
  } else if (v >= 7) {
    // loaded in call-out below
  }

  if (v == 5) {
    pgIncremental->new_pool_names.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(pgIncremental->new_pool_names[t], p);
    }
  } else if (v == 6) {
    ::decode(pgIncremental->new_pool_names, p);
  } else if (v >= 7) {
    // loaded in call-out below
  }

  if (v < 6) {
    pgIncremental->old_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      pgIncremental->old_pools.insert(t);
    }
  } else if (v == 6) {
    ::decode(pgIncremental->old_pools, p);
  } else if (v >= 7) {
    // loaded in call-out below
  }

  ::decode(new_up_client, p);
  ::decode(new_state, p);
  ::decode(new_weight, p);

  if (v < 6) {
    pgIncremental->new_pg_temp.clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode(pgIncremental->new_pg_temp[pg_t(opg)], p);
    }
  } else if (v == 6) {
    ::decode(pgIncremental->new_pg_temp, p);
  } else if (v >= 7) {
    // loaded in call-out below
  }

  // decode short map, too.
  if (v == 5 && p.end())
    return;

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(new_hb_up, p);
  if (v < 5)
    ::decode(pgIncremental->new_pool_names, p);
  ::decode(new_up_thru, p);
  ::decode(new_last_clean_interval, p);
  ::decode(new_lost, p);
  ::decode(new_blacklist, p);
  ::decode(old_blacklist, p);
  if (ev >= 6)
    ::decode(new_up_internal, p);
  if (ev >= 7)
    ::decode(cluster_snapshot, p);
  if (ev >= 8)
    ::decode(new_uuid, p);

  // pgIncremental call out
  if (v >= 7) {
    ::decode(*pgIncremental, p);
  }
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
  if (crush.length()) {
    f->open_object_section("crush");
    CrushWrapper c;
    bufferlist tbl = crush;  // kludge around constness.
    bufferlist::iterator p = tbl.begin();
    c.decode(p);
    c.dump(f);
    f->close_section();
  }

  f->dump_int("new_max_osd", new_max_osd);

  f->open_array_section("new_up_osds");
  for (map<int32_t,entity_addr_t>::const_iterator p = new_up_client.begin(); p != new_up_client.end(); ++p) {
    f->open_object_section("osd");
    f->dump_int("osd", p->first);
    f->dump_stream("public_addr") << p->second;
    f->dump_stream("cluster_addr") << new_up_internal.find(p->first)->second;
    f->dump_stream("heartbeat_addr") << new_hb_up.find(p->first)->second;
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
       p++) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();
  f->open_array_section("old_blacklist");
  for (vector<entity_addr_t>::const_iterator p = old_blacklist.begin(); p != old_blacklist.end(); ++p)
    f->dump_stream("addr") << *p;
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

  pgIncremental->dump(f);
} // dump

void OSDMap::Incremental::generate_test_instances(list<Incremental*>& o)
{
  o.push_back(new Incremental);
}

// ----------------------------------
// OSDMap

void OSDMap::set_epoch(epoch_t e)
{
  epoch = e;
  pgBridge->set_epoch(e);
}

bool OSDMap::is_blacklisted(const entity_addr_t& a) const
{
  if (blacklist.empty())
    return false;

  // this specific instance?
  if (blacklist.count(a))
    return true;

  // is entire ip blacklisted?
  entity_addr_t b = a;
  b.set_port(0);
  b.set_nonce(0);
  return blacklist.count(b);
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
  osd_addrs->client_addr.resize(m);
  osd_addrs->cluster_addr.resize(m);
  osd_addrs->hb_addr.resize(m);
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
    if (osd_state[i] & CEPH_OSD_EXISTS &&
	osd_state[i] & CEPH_OSD_UP) n++;
  return n;
}

int OSDMap::get_num_in_osds() const
{
  int n = 0;
  for (int i=0; i<max_osd; i++)
    if (osd_state[i] & CEPH_OSD_EXISTS &&
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
    if ( n->osd_addrs->hb_addr[i] &&  o->osd_addrs->hb_addr[i] &&
	*n->osd_addrs->hb_addr[i] == *o->osd_addrs->hb_addr[i])
      n->osd_addrs->hb_addr[i] = o->osd_addrs->hb_addr[i];
    else
      diff++;
  }
  if (diff == 0) {
    // zoinks, no differences at all!
    n->osd_addrs = o->osd_addrs;
  }

  // does crush match?
  bufferlist oc, nc;
  ::encode(*o->crush, oc);
  ::encode(*n->crush, nc);
  if (oc.contents_equal(nc)) {
    n->crush = o->crush;
  }

  // handle page group deduplication
  OSDMapPGBridge::dedup(o->pgBridge, n->pgBridge);

  // do uuids match?
  if (o->osd_uuid->size() == n->osd_uuid->size() &&
      *o->osd_uuid == *n->osd_uuid)
    n->osd_uuid = o->osd_uuid;
}

int OSDMap::apply_incremental(Incremental &inc)
{
  if (inc.epoch == 1)
    fsid = inc.fsid;
  else if (inc.fsid != fsid)
    return -EINVAL;
  
  assert(inc.epoch == epoch+1);
  epoch++;
  modified = inc.modified;

  // full map?
  if (inc.fullmap.length()) {
    decode(inc.fullmap);
    return 0;
  }

  // nope, incremental.
  if (inc.new_flags >= 0)
    flags = inc.new_flags;

  if (inc.new_max_osd >= 0)
    set_max_osd(inc.new_max_osd);

  for (map<int32_t,uint32_t>::iterator i = inc.new_weight.begin();
       i != inc.new_weight.end();
       i++) {
    set_weight(i->first, i->second);

    // if we are marking in, clear the AUTOOUT and NEW bits.
    if (i->second)
      osd_state[i->first] &= ~(CEPH_OSD_AUTOOUT | CEPH_OSD_NEW);
  }

  // up/down
  for (map<int32_t,uint8_t>::iterator i = inc.new_state.begin();
       i != inc.new_state.end();
       i++) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if ((osd_state[i->first] & CEPH_OSD_UP) &&
	(s & CEPH_OSD_UP)) {
      osd_info[i->first].down_at = epoch;
    }
    if ((osd_state[i->first] & CEPH_OSD_EXISTS) &&
	(s & CEPH_OSD_EXISTS))
      (*osd_uuid)[i->first] = uuid_d();
    osd_state[i->first] ^= s;
  }
  for (map<int32_t,entity_addr_t>::iterator i = inc.new_up_client.begin();
       i != inc.new_up_client.end();
       i++) {
    osd_state[i->first] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
    osd_addrs->client_addr[i->first].reset(new entity_addr_t(i->second));
    if (inc.new_hb_up.empty())
      osd_addrs->hb_addr[i->first].reset(new entity_addr_t(i->second)); //this is a backward-compatibility hack
    else
      osd_addrs->hb_addr[i->first].reset(new entity_addr_t(inc.new_hb_up[i->first]));
    osd_info[i->first].up_from = epoch;
  }
  for (map<int32_t,entity_addr_t>::iterator i = inc.new_up_internal.begin();
       i != inc.new_up_internal.end();
       i++)
    osd_addrs->cluster_addr[i->first].reset(new entity_addr_t(i->second));

  // info
  for (map<int32_t,epoch_t>::iterator i = inc.new_up_thru.begin();
       i != inc.new_up_thru.end();
       i++)
    osd_info[i->first].up_thru = i->second;
  for (map<int32_t,pair<epoch_t,epoch_t> >::iterator i = inc.new_last_clean_interval.begin();
       i != inc.new_last_clean_interval.end();
       i++) {
    osd_info[i->first].last_clean_begin = i->second.first;
    osd_info[i->first].last_clean_end = i->second.second;
  }
  for (map<int32_t,epoch_t>::iterator p = inc.new_lost.begin(); p != inc.new_lost.end(); p++)
    osd_info[p->first].lost_at = p->second;

  // uuid
  for (map<int32_t,uuid_d>::iterator p = inc.new_uuid.begin(); p != inc.new_uuid.end(); ++p) 
    (*osd_uuid)[p->first] = p->second;

  // blacklist
  for (map<entity_addr_t,utime_t>::iterator p = inc.new_blacklist.begin();
       p != inc.new_blacklist.end();
       p++)
    blacklist[p->first] = p->second;
  for (vector<entity_addr_t>::iterator p = inc.old_blacklist.begin();
       p != inc.old_blacklist.end();
       p++)
    blacklist.erase(*p);

  // cluster snapshot?
  if (inc.cluster_snapshot.length()) {
    cluster_snapshot = inc.cluster_snapshot;
    cluster_snapshot_epoch = inc.epoch;
  } else {
    cluster_snapshot.clear();
    cluster_snapshot_epoch = 0;
  }

  // do new crush map last (after up/down stuff)
  if (inc.crush.length()) {
    bufferlist::iterator blp = inc.crush.begin();
    crush.reset(new CrushWrapper);
    crush->decode(blp);
  }

  calc_num_osds();

  return pgBridge->apply_incremental(*inc.pgIncremental, epoch);
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

// serialize, unserialize
void OSDMap::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);

  // base
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(created, bl);
  ::encode(modified, bl);

  // for ::encode(pools, bl);
  __u32 n = pgBridge->pools.size();
  ::encode(n, bl);
  for (map<int64_t,pg_pool_t>::const_iterator p = pgBridge->pools.begin();
       p != pgBridge->pools.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl, 0);
  }
  // for ::encode(pool_name, bl);
  n = pgBridge->pool_name.size();
  ::encode(n, bl);
  for (map<int64_t, string>::const_iterator p = pgBridge->pool_name.begin();
       p != pgBridge->pool_name.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl);
  }
  // for ::encode(pool_max, bl);
  n = pgBridge->pool_max;
  ::encode(n, bl);

  ::encode(flags, bl);

  ::encode(max_osd, bl);
  ::encode(osd_state, bl);
  ::encode(osd_weight, bl);
  ::encode(osd_addrs->client_addr, bl);

  // for ::encode(pg_temp, bl);
  n = pgBridge->pg_temp->size();
  ::encode(n, bl);
  for (map<pg_t,vector<int32_t> >::const_iterator p =
	 pgBridge->pg_temp->begin();
       p != pgBridge->pg_temp->end();
       ++p) {
    old_pg_t opg = p->first.get_old_pg();
    ::encode(opg, bl);
    ::encode(p->second, bl);
  }

  // crush
  bufferlist cbl;
  crush->encode(cbl);
  ::encode(cbl, bl);
}

void OSDMap::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  __u16 v = 7;
  ::encode(v, bl);

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

  // crush
  bufferlist cbl;
  crush->encode(cbl);
  ::encode(cbl, bl);

  // extended
  __u16 ev = 8;
  ::encode(ev, bl);
  ::encode(osd_addrs->hb_addr, bl);
  ::encode(osd_info, bl);
  ::encode(blacklist, bl);
  ::encode(osd_addrs->cluster_addr, bl);
  ::encode(cluster_snapshot_epoch, bl);
  ::encode(cluster_snapshot, bl);
  ::encode(*osd_uuid, bl);

  ::encode(*pgBridge, bl, features);
}

void OSDMap::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}

void OSDMap::decode(bufferlist::iterator& p)
{
  __u32 n, t;
  __u16 v;
  ::decode(v, p);

  // base
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(created, p);
  ::decode(modified, p);

  if (v < 6) {
    if (v < 4) {
      int32_t max_pools = 0;
      ::decode(max_pools, p);
      pgBridge->pool_max = max_pools;
    }
    pgBridge->pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(pgBridge->pools[t], p);
    }
    if (v == 4) {
      ::decode(n, p);
      pgBridge->pool_max = n;
    } else if (v == 5) {
      pgBridge->pool_name.clear();
      ::decode(n, p);
      while (n--) {
	::decode(t, p);
	::decode(pgBridge->pool_name[t], p);
      }
      ::decode(n, p);
      pgBridge->pool_max = n;
    }
  } else if (v == 6) {
    ::decode(pgBridge->pools, p);
    ::decode(pgBridge->pool_name, p);
    ::decode(pgBridge->pool_max, p);
  } else if (v >= 7) {
    // see callout below
  }

  // kludge around some old bug that zeroed out pool_max (#2307)
  if (pgBridge->pools.size() &&
      pgBridge->pool_max < pgBridge->pools.rbegin()->first) {
    pgBridge->pool_max = pgBridge->pools.rbegin()->first;
  }

  ::decode(flags, p);

  ::decode(max_osd, p);
  ::decode(osd_state, p);
  ::decode(osd_weight, p);
  ::decode(osd_addrs->client_addr, p);

  if (v <= 5) {
    pgBridge->pg_temp->clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode((*pgBridge->pg_temp)[pg_t(opg)], p);
    }
  } else if (v == 6) {
    ::decode(*pgBridge->pg_temp, p);
  } else if (v >= 7) {
    // see callout before
  }

  // crush
  bufferlist cbl;
  ::decode(cbl, p);
  bufferlist::iterator cblp = cbl.begin();
  crush->decode(cblp);

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(osd_addrs->hb_addr, p);
  ::decode(osd_info, p);
  if (v < 5)
    ::decode(pgBridge->pool_name, p);

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

  // index pool names
  pgBridge->name_pool.clear();
  for (map<int64_t,string>::iterator i = pgBridge->pool_name.begin();
       i != pgBridge->pool_name.end();
       ++i)
    pgBridge->name_pool[i->second] = i->first;

  calc_num_osds();

  if (v >= 7) {
    ::decode(*pgBridge, p);
  }
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
      f->dump_stream("heartbeat_addr") << get_hb_addr(i);

      set<string> st;
      get_state(i, st);
      f->open_array_section("state");
      for (set<string>::iterator p = st.begin(); p != st.end(); ++p)
	f->dump_string("state", *p);
      f->close_section();

      f->close_section();
    }
  f->close_section();

  f->open_array_section("blacklist");
  for (hash_map<entity_addr_t,utime_t>::const_iterator p = blacklist.begin();
       p != blacklist.end();
       p++) {
    stringstream ss;
    ss << p->first;
    f->dump_stream(ss.str().c_str()) << p->second;
  }
  f->close_section();

  pgBridge->dump(f);
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
    s += ",no-up";
  if (f & CEPH_OSDMAP_NODOWN)
    s += ",no-down";
  if (f & CEPH_OSDMAP_NOOUT)
    s += ",no-out";
  if (f & CEPH_OSDMAP_NOIN)
    s += ",no-in";
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
  qi() {}
  qi(int i, int d, float w) : item(i), depth(d), weight(w) {}
};

void OSDMap::print(ostream& out) const
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "created " << get_created() << "\n"
      << "modifed " << get_modified() << "\n";

  out << "flags " << get_flag_string() << "\n";
  if (get_cluster_snapshot().length())
    out << "cluster_snapshot " << get_cluster_snapshot() << "\n";
  out << "\n";

  out << "max_osd " << get_max_osd() << "\n";
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      out << "osd." << i;
      out << (is_up(i) ? " up  ":" down");
      out << (is_in(i) ? " in ":" out");
      out << " weight " << get_weightf(i);
      const osd_info_t& info(get_info(i));
      out << " " << info;
      out << " " << get_addr(i) << " " << get_cluster_addr(i) << " " << get_hb_addr(i);
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
       p++)
    out << "blacklist " << p->first << " expires " << p->second << "\n";

  // ignore pg_swap_primary

  pgBridge->print(out);
}

void OSDMap::print_osd_line(int cur, ostream& out) const
{
  out << "osd." << cur << "\t";
  if (!exists(cur))
    out << "DNE\t\t";
  else {
    if (is_up(cur))
      out << "up\t";
    else
      out << "down\t";
    out << (exists(cur) ? get_weightf(cur):0) << "\t";
  }
}

void OSDMap::print_tree(ostream& out) const
{
  out << "# id\tweight\ttype name\tup/down\treweight\n";
  set<int> touched;
  set<int> roots;
  crush->find_roots(roots);
  for (set<int>::iterator p = roots.begin(); p != roots.end(); p++) {
    list<qi> q;
    q.push_back(qi(*p, 0, crush->get_bucket_weight(*p) / (float)0x10000));
    while (!q.empty()) {
      int cur = q.front().item;
      int depth = q.front().depth;
      float weight = q.front().weight;
      q.pop_front();

      out << cur << "\t" << weight << "\t";
      for (int k=0; k<depth; k++)
	out << "\t";

      if (cur >= 0) {
	print_osd_line(cur, out);
	out << "\n";
	touched.insert(cur);
	continue;
      }

      int type = crush->get_bucket_type(cur);
      out << crush->get_type_name(type) << " " << crush->get_item_name(cur) << "\n";

      // queue bucket contents...
      int s = crush->get_bucket_size(cur);
      for (int k=s-1; k>=0; k--)
	q.push_front(qi(crush->get_bucket_item(cur, k), depth+1,
			(float)crush->get_bucket_item_weight(cur, k) / (float)0x10000));
    }
  }

  set<int> stray;
  for (int i=0; i<max_osd; i++)
    if (exists(i) && touched.count(i) == 0)
      stray.insert(i);

  if (!stray.empty()) {
    out << "\n";
    for (set<int>::iterator p = stray.begin(); p != stray.end(); ++p) {
      out << *p << "\t0\t";
      print_osd_line(*p, out);
      out << "\n";
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
