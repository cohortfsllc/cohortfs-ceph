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
#include <fstream>

#include "mon/MonClient.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGMissing.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"

#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"

#include "ReplicatedPG.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "PGOSD.h"
#include "PGPlaceSystem.h"

#include "PGOSDMap.h"
#include "PGPlaceSystem.h"
#include "common/code_environment.h"
#include "mon/PGMonitor.h"
#include "osdc/Objecter.h"


#define dout_subsys ceph_subsys_osd


const PGOSDMapPlaceSystem placeSystem(PGPlaceSystem::systemName,
				      PGPlaceSystem::systemIdentifier);


// PGOSDMap::Incremental


void
PGOSDMap::Incremental::generate_test_instances(list<PGOSDMap::Incremental*>& o)
{
  o.push_back(new Incremental);
}


void PGOSDMap::Incremental::encode(bufferlist& bl,
                                   uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    encode_client_old(bl);
    return;
  }

  // base
  __u16 v = 7;
  ::encode(v, bl);

  inherited::encode(bl, features);

  ::encode(new_pool_max, bl);
  ::encode(new_pools, bl, features);
  ::encode(new_pool_names, bl);
  ::encode(old_pools, bl);
  ::encode(new_pg_temp, bl);
}

void PGOSDMap::Incremental::decode(bufferlist::iterator& p)
{
  // base
  __u16 v;
  ::decode(v, p);

  if (v >= 7) {
    inherited::decode(p);

    ::decode(new_pool_max, p);
    ::decode(new_pools, p);
    ::decode(new_pool_names, p);
    ::decode(old_pools, p);
    ::decode(new_pg_temp, p);
  } else {
    decode_pre7(p, v);
  }
}


void PGOSDMap::Incremental::dump(Formatter *f) const {
  f->dump_int("new_pool_max", new_pool_max);

  f->open_array_section("new_pools");
  for (map<int64_t,pg_pool_t>::const_iterator p = new_pools.begin();
       p != new_pools.end();
       ++p) {
    f->open_object_section("pool");
    f->dump_int("pool", p->first);
    f->dump_string("name", new_pool_names.find(p->first)->second);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("old_pools");
  for (set<int64_t>::const_iterator p = old_pools.begin();
       p != old_pools.end();
       ++p)
    f->dump_int("pool", *p);
  f->close_section();

  f->open_array_section("new_pg_temp");
  for (map<pg_t,vector<int> >::const_iterator p = new_pg_temp.begin();
       p != new_pg_temp.end();
       p++) {
    f->open_object_section("pg");
    f->dump_stream("pgid") << p->first;
    f->open_array_section("osds");
    for (vector<int>::const_iterator q = p->second.begin();
	 q != p->second.end();
	 ++q)
      f->dump_int("osd", *q);
    f->close_section();
    f->close_section();    
  }
  f->close_section();
}


void PGOSDMap::Incremental::decode_pre7(bufferlist::iterator &p, __u16 v)
{
  __u32 n, t;

  // base
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(modified, p);

  if (v == 4 || v == 5) {
    ::decode(n, p);
    new_pool_max = n;
  } else if (v == 6) {
    ::decode(new_pool_max, p);
  }

  ::decode(new_flags, p);
  ::decode(fullmap, p);
  ::decode(crush, p);

  ::decode(new_max_osd, p);
  if (v < 6) {
    new_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(new_pools[t], p);
    }
  } else if (v == 6) {
    ::decode(new_pools, p);
  }

  if (v == 5) {
    new_pool_names.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(new_pool_names[t], p);
    }
  } else if (v == 6) {
    ::decode(new_pool_names, p);
  }

  if (v < 6) {
    old_pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      old_pools.insert(t);
    }
  } else if (v == 6) {
    ::decode(old_pools, p);
  }

  ::decode(new_up_client, p);
  ::decode(new_state, p);
  ::decode(new_weight, p);

  if (v < 6) {
    new_pg_temp.clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode(new_pg_temp[pg_t(opg)], p);
    }
  } else if (v == 6) {
    ::decode(new_pg_temp, p);
  }

  // decode short map, too.
  if (v == 5 && p.end())
    return;

  // extended
  __u16 ev = 0;
  if (v >= 5)
    ::decode(ev, p);
  ::decode(new_hb_up, p);
  if (v < 5) {
    ::decode(new_pool_names, p);
  }
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
}


// PGOSDMap


// serialize, unserialize
void PGOSDMap::Incremental::encode_client_old(bufferlist& bl) const
{
  __u16 v = 5;
  ::encode(v, bl);
  ::encode(fsid, bl);
  ::encode(epoch, bl);
  ::encode(modified, bl);
  int32_t new_t = new_pool_max;
  ::encode(new_t, bl);
  ::encode(new_flags, bl);
  ::encode(fullmap, bl);
  ::encode(crush, bl);

  ::encode(new_max_osd, bl);
  // for ::encode(new_pools, bl);
  __u32 n = new_pools.size();
  ::encode(n, bl);
  for (map<int64_t,pg_pool_t>::const_iterator p = new_pools.begin();
       p != new_pools.end();
       ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl, 0);
  }
  // for ::encode(new_pool_names, bl);
  n = new_pool_names.size();
  ::encode(n, bl);
  for (map<int64_t, string>::const_iterator p = new_pool_names.begin(); p != new_pool_names.end(); ++p) {
    n = p->first;
    ::encode(n, bl);
    ::encode(p->second, bl);
  }
  // for ::encode(old_pools, bl);
  n = old_pools.size();
  ::encode(n, bl);
  for (set<int64_t>::iterator p = old_pools.begin(); p != old_pools.end(); ++p) {
    n = *p;
    ::encode(n, bl);
  }
  ::encode(new_up_client, bl);
  ::encode(new_state, bl);
  ::encode(new_weight, bl);
  // for ::encode(new_pg_temp, bl);
  n = new_pg_temp.size();
  ::encode(n, bl);
  for (map<pg_t,vector<int32_t> >::const_iterator p = new_pg_temp.begin();
       p != new_pg_temp.end();
       ++p) {
    old_pg_t opg = p->first.get_old_pg();
    ::encode(opg, bl);
    ::encode(p->second, bl);
  }
}


void PGOSDMap::encode(bufferlist& bl, uint64_t features) const {
  if ((features & CEPH_FEATURE_PGID64) == 0) {
    // encode_client_old(bl);
    return;
  }

  __u16 v = 7;
  ::encode(v, bl);

  encodeOSDMap(bl, features);

  ::encode(pools, bl, features);
  ::encode(pool_name, bl);
  ::encode(pool_max, bl);
  ::encode(*pg_temp, bl);
}


void PGOSDMap::decode(bufferlist::iterator &p) {
  __u16 v;
  ::decode(v, p);

  if (v >= 7) {
    decodeOSDMap(p, v);

    // index pool names
    name_pool.clear();
    for (map<int64_t,string>::iterator i = pool_name.begin(); i != pool_name.end(); ++i)
      name_pool[i->second] = i->first;


    ::decode(pools, p);
    ::decode(pool_name, p);
    ::decode(pool_max, p);
    ::decode(*pg_temp, p);
  } else {
    decode_pre7(p, v);
  }
}


void PGOSDMap::decode_pre7(bufferlist::iterator& p, __u16 v)
{
  __u32 n, t;

  // base
  ::decode(fsid, p);
  ::decode(epoch, p);
  ::decode(created, p);
  ::decode(modified, p);

  if (v < 6) {
    if (v < 4) {
      int32_t max_pools = 0;
      ::decode(max_pools, p);
      pool_max = max_pools;
    }
    pools.clear();
    ::decode(n, p);
    while (n--) {
      ::decode(t, p);
      ::decode(pools[t], p);
    }
    if (v == 4) {
      ::decode(n, p);
      pool_max = n;
    } else if (v == 5) {
      pool_name.clear();
      ::decode(n, p);
      while (n--) {
	::decode(t, p);
	::decode(pool_name[t], p);
      }
      ::decode(n, p);
      pool_max = n;
    }
  } else if (v == 6) {
    ::decode(pools, p);
    ::decode(pool_name, p);
    ::decode(pool_max, p);
  } else if (v >= 7) {
    // see callout below
  }

  // kludge around some old bug that zeroed out pool_max (#2307)
  if (pools.size() &&
      pool_max < pools.rbegin()->first) {
    pool_max = pools.rbegin()->first;
  }

  ::decode(flags, p);

  ::decode(max_osd, p);
  ::decode(osd_state, p);
  ::decode(osd_weight, p);
  ::decode(osd_addrs->client_addr, p);

  if (v <= 5) {
    pg_temp->clear();
    ::decode(n, p);
    while (n--) {
      old_pg_t opg;
      ::decode_raw(opg, p);
      ::decode((*pg_temp)[pg_t(opg)], p);
    }
  } else if (v == 6) {
    ::decode(*pg_temp, p);
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
    ::decode(pool_name, p);

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
  name_pool.clear();
  for (map<int64_t,string>::iterator i = pool_name.begin();
       i != pool_name.end();
       ++i)
    name_pool[i->second] = i->first;

  calc_num_osds();
}


void PGOSDMap::dump(Formatter *f) const {
  inherited::dump(f);

  f->open_array_section("pools");
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    std::string name("<unknown>");
    map<int64_t,string>::const_iterator pni = pool_name.find(p->first);
    if (pni != pool_name.end())
      name = pni->second;
    f->open_object_section("pool");
    f->dump_int("pool", p->first);
    f->dump_string("pool_name", name);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("pg_temp");
  for (map<pg_t,vector<int> >::const_iterator p = pg_temp->begin();
       p != pg_temp->end();
       ++p) {
    f->open_object_section("osds");
    f->dump_stream("pgid") << p->first;
    f->open_array_section("osds");
    for (vector<int>::const_iterator q = p->second.begin(); q != p->second.end(); ++q)
      f->dump_int("osd", *q);
    f->close_section();
    f->close_section();
  }
  f->close_section();
}


void PGOSDMap::print(ostream& out) const
{
  inherited::print(out);

  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    std::string name("<unknown>");
    map<int64_t,string>::const_iterator pni = pool_name.find(p->first);
    if (pni != pool_name.end())
      name = pni->second;
    out << "pool " << p->first
	<< " '" << name
	<< "' " << p->second << "\n";
    for (map<snapid_t,pool_snap_info_t>::const_iterator q = p->second.snaps.begin();
	 q != p->second.snaps.end();
	 ++q)
      out << "\tsnap " << q->second.snapid << " '" << q->second.name << "' " << q->second.stamp << "\n";
    if (!p->second.removed_snaps.empty())
      out << "\tremoved_snaps " << p->second.removed_snaps << "\n";
  }
  out << std::endl;

  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    std::string name("<unknown>");
    map<int64_t,string>::const_iterator pni = pool_name.find(p->first);
    if (pni != pool_name.end())
      name = pni->second;
    out << "pool " << p->first
	<< " '" << name
	<< "' " << p->second << "\n";
    for (map<snapid_t,pool_snap_info_t>::const_iterator q = p->second.snaps.begin();
	 q != p->second.snaps.end();
	 ++q)
      out << "\tsnap " << q->second.snapid << " '" << q->second.name << "' " << q->second.stamp << "\n";
    if (!p->second.removed_snaps.empty())
      out << "\tremoved_snaps " << p->second.removed_snaps << "\n";
  }
  out << std::endl;
}


// pg -> (up osd list)
void PGOSDMap::_raw_to_up_osds(pg_t pg,
                               vector<int>& raw,
                               vector<int>& up) const
{
  up.clear();
  for (unsigned i=0; i<raw.size(); i++) {
    if (!exists(raw[i]) || is_down(raw[i])) 
      continue;
    up.push_back(raw[i]);
  }
}
  
bool PGOSDMap::_raw_to_temp_osds(const pg_pool_t& pool,
				       pg_t pg,
				       vector<int>& raw,
				       vector<int>& temp) const
{
  pg = pool.raw_pg_to_pg(pg);
  map<pg_t,vector<int> >::const_iterator p = pg_temp->find(pg);
  if (p != pg_temp->end()) {
    temp.clear();
    for (unsigned i=0; i<p->second.size(); i++) {
      if (!exists(p->second[i]) || is_down(p->second[i]))
	continue;
      temp.push_back(p->second[i]);
    }
    return true;
  }
  return false;
}


int PGOSDMap::_pg_to_osds(const pg_pool_t& pool,
				pg_t pg,
				vector<int>& osds) const
{
  // map to osds[]
  ps_t pps = pool.raw_pg_to_pps(pg);  // placement ps
  unsigned size = pool.get_size();

  // what crush rule?
  int ruleno = crush->find_rule(pool.get_crush_ruleset(),
				pool.get_type(),
				size);
  if (ruleno >= 0)
    crush->do_rule(ruleno, pps, osds, size, get_weights());

  _remove_nonexistent_osds(osds);

  return osds.size();
}


ceph_object_layout PGOSDMap::make_object_layout(object_t oid, int pg_pool) const
{
  object_locator_t loc(pg_pool);

  ceph_object_layout ol;
  pg_t pgid = object_locator_to_pg(oid, loc);
  ol.ol_pgid = pgid.get_old_pg().v;
  ol.ol_stripe_unit = 0;
  return ol;
}


int PGOSDMap::apply_incremental_subclass(const OSDMap::Incremental& incOrig) {
  const PGOSDMap::Incremental& inc =
    dynamic_cast<const PGOSDMap::Incremental&>(incOrig);

  if (inc.new_pool_max != -1)
    pool_max = inc.new_pool_max;

  for (set<int64_t>::const_iterator p = inc.old_pools.begin();
       p != inc.old_pools.end();
       ++p) {
    pools.erase(*p);
    name_pool.erase(pool_name[*p]);
    pool_name.erase(*p);
  }

  for (map<int64_t,pg_pool_t>::const_iterator p = inc.new_pools.begin();
       p != inc.new_pools.end();
       ++p) {
    pools[p->first] = p->second;
    pools[p->first].last_change = epoch;
  }
  for (map<int64_t,string>::const_iterator p = inc.new_pool_names.begin();
       p != inc.new_pool_names.end();
       ++p) {
    if (pool_name.count(p->first))
      name_pool.erase(pool_name[p->first]);
    pool_name[p->first] = p->second;
    name_pool[p->second] = p->first;
  }

  // pg rebuild
  for (map<pg_t, vector<int> >::const_iterator p = inc.new_pg_temp.begin(); p != inc.new_pg_temp.end(); ++p) {
    if (p->second.empty())
      pg_temp->erase(p->first);
    else
      (*pg_temp)[p->first] = p->second;
  }

  return 0;
}


void PGOSDMap::dedup(const OSDMap* os, OSDMap* ns) {
  const PGOSDMap *o = static_cast<const PGOSDMap*>(os);
  PGOSDMap *n = static_cast<PGOSDMap*>(ns);
  inherited::dedup(o, n);

  // does pg_temp match?
  if (o->pg_temp->size() == n->pg_temp->size()) {
    if (*o->pg_temp == *n->pg_temp)
      n->pg_temp = o->pg_temp;
  }
}


void PGOSDMap::set_epoch(epoch_t e) {
  inherited::set_epoch(e);

  for (map<int64_t,pg_pool_t>::iterator p = pools.begin();
       p != pools.end();
       p++) {
    p->second.last_change = e;
  }
}


int PGOSDMap::pg_to_osds(pg_t pg, vector<int>& raw) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return 0;
  return _pg_to_osds(*pool, pg, raw);
}

int PGOSDMap::pg_to_acting_osds(pg_t pg, vector<int>& acting) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return 0;
  vector<int> raw;
  _pg_to_osds(*pool, pg, raw);
  if (!_raw_to_temp_osds(*pool, pg, raw, acting))
    _raw_to_up_osds(pg, raw, acting);
  return acting.size();
}

void PGOSDMap::pg_to_raw_up(pg_t pg, vector<int>& up) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return;
  vector<int> raw;
  _pg_to_osds(*pool, pg, raw);
  _raw_to_up_osds(pg, raw, up);
}
  
void PGOSDMap::pg_to_up_acting_osds(pg_t pg, vector<int>& up, vector<int>& acting) const
{
  const pg_pool_t *pool = get_pg_pool(pg.pool());
  if (!pool)
    return;
  vector<int> raw;
  _pg_to_osds(*pool, pg, raw);
  _raw_to_up_osds(pg, raw, up);
  if (!_raw_to_temp_osds(*pool, pg, raw, acting))
    acting = up;
}

int PGOSDMap::calc_pg_rank(int osd, vector<int>& acting, int nrep)
{
  if (!nrep)
    nrep = acting.size();
  for (int i=0; i<nrep; i++) 
    if (acting[i] == osd)
      return i;
  return -1;
}

int PGOSDMap::calc_pg_role(int osd, vector<int>& acting, int nrep)
{
  if (!nrep)
    nrep = acting.size();
  return calc_pg_rank(osd, acting, nrep);
}


// mapping

int PGOSDMap::object_locator_to_pg(const object_t& oid,
				   const object_locator_t& loc,
				   pg_t& pg) const
{
  // calculate ps (placement seed)
  const pg_pool_t *pool = get_pg_pool(loc.get_pool());
  if (!pool)
    return -ENOENT;
  ps_t ps;
  if (loc.key.length())
    ps = ceph_str_hash(pool->object_hash, loc.key.c_str(), loc.key.length());
  else
    ps = ceph_str_hash(pool->object_hash, oid.name.c_str(), oid.name.length());
  pg = pg_t(ps, loc.get_pool(), -1);
  return 0;
}


// building


PGOSDMap::Incremental* PGOSDMap::newIncremental() const {
  return new PGOSDMap::Incremental();
}

bool PGOSDMap::crush_ruleset_in_use(int ruleset) const
{
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    if (p->second.crush_ruleset == ruleset)
      return true;
  }
  return false;
}


void PGOSDMap::build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
			    int nosd)
{
  ldout(cct, 10) << "build_simple on " << num_osd
		 << " osds" << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = ceph_clock_now(cct);

  set_max_osd(nosd);

  // crush map
  map<int, const char*> rulesets;
  rulesets[CEPH_DATA_RULE] = "data";
  rulesets[CEPH_METADATA_RULE] = "metadata";
  rulesets[CEPH_RBD_RULE] = "rbd";

  int poolbase = nosd ? nosd : 1;

  for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); ++p) {
    int64_t pool = ++pool_max;
    pools[pool].type = pg_pool_t::TYPE_REP;
    pools[pool].flags = cct->_conf->osd_pool_default_flags;
    if (cct->_conf->osd_pool_default_flag_hashpspool)
      pools[pool].flags |= pg_pool_t::FLAG_HASHPSPOOL;
    pools[pool].size = cct->_conf->osd_pool_default_size;
    pools[pool].min_size = cct->_conf->get_osd_pool_default_min_size();
    pools[pool].crush_ruleset = p->first;
    pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
    pools[pool].last_change = epoch;
    if (p->first == CEPH_DATA_RULE)
      pools[pool].crash_replay_interval = cct->_conf->osd_default_data_pool_replay_window;
    pool_name[pool] = p->second;
    name_pool[p->second] = pool;
  }

  build_simple_crush_map(cct, *crush, rulesets, nosd);

  for (int i=0; i<nosd; i++) {
    set_state(i, 0);
    set_weight(i, CEPH_OSD_OUT);
  }
}

void PGOSDMap::build_simple_crush_map(CephContext *cct, CrushWrapper& crush,
				      map<int, const char*>& rulesets, int nosd)
{
  const md_config_t *conf = cct->_conf;

  crush.create();

  crush.set_type_name(0, "osd");
  crush.set_type_name(1, "host");
  crush.set_type_name(2, "rack");
  crush.set_type_name(3, "row");
  crush.set_type_name(4, "room");
  crush.set_type_name(5, "datacenter");
  crush.set_type_name(6, "root");

  // root
  int rootid = crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, 6 /* pool */, 0, NULL, NULL);
  crush.set_item_name(rootid, "default");

  for (int o=0; o<nosd; o++) {
    map<string,string> loc;
    loc["host"] = "localhost";
    loc["rack"] = "localrack";
    loc["root"] = "default";
    ldout(cct, 10) << " adding osd." << o << " at " << loc << dendl;
    char name[8];
    sprintf(name, "osd.%d", o);
    crush.insert_item(cct, o, 1.0, name, loc);
  }

  // rules
  int minrep = conf->osd_min_rep;
  int maxrep = conf->osd_max_rep;
  assert(maxrep >= minrep);
  for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); ++p) {
    int ruleset = p->first;
    crush_rule *rule = crush_make_rule(3, ruleset, pg_pool_t::TYPE_REP, minrep, maxrep);
    assert(rule);
    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
    crush_rule_set_step(rule, 1,
			cct->_conf->osd_crush_chooseleaf_type ? CRUSH_RULE_CHOOSE_LEAF_FIRSTN : CRUSH_RULE_CHOOSE_FIRSTN,
			CRUSH_CHOOSE_N,
			cct->_conf->osd_crush_chooseleaf_type);
    crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
    int rno = crush_add_rule(crush.crush, rule, -1);
    crush.set_rule_name(rno, p->second);
  }

  crush.finalize();
}

int PGOSDMap::build_simple_from_conf(CephContext *cct, epoch_t e, uuid_d &fsid)
{
  ldout(cct, 10) << "build_simple_from_conf" << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = ceph_clock_now(cct);

  const md_config_t *conf = cct->_conf;

  // count osds
  int maxosd = 0, numosd = 0;

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

  // crush map
  map<int, const char*> rulesets;
  rulesets[CEPH_DATA_RULE] = "data";
  rulesets[CEPH_METADATA_RULE] = "metadata";
  rulesets[CEPH_RBD_RULE] = "rbd";

  for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); ++p) {
    int64_t pool = ++pool_max;
    pools[pool].type = pg_pool_t::TYPE_REP;
    pools[pool].flags = cct->_conf->osd_pool_default_flags;
    if (cct->_conf->osd_pool_default_flag_hashpspool)
      pools[pool].flags |= pg_pool_t::FLAG_HASHPSPOOL;
    pools[pool].size = cct->_conf->osd_pool_default_size;
    pools[pool].min_size = cct->_conf->get_osd_pool_default_min_size();
    pools[pool].crush_ruleset = p->first;
    pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
    pools[pool].last_change = epoch;
    if (p->first == CEPH_DATA_RULE)
      pools[pool].crash_replay_interval = cct->_conf->osd_default_data_pool_replay_window;
    pool_name[pool] = p->second;
    name_pool[p->second] = pool;
  }

  build_simple_crush_map_from_conf(cct, *crush, rulesets);

  for (int i=0; i<=maxosd; i++) {
    set_state(i, 0);
    set_weight(i, CEPH_OSD_OUT);
  }

  return 0;
}

void PGOSDMap::build_simple_crush_map_from_conf(CephContext *cct, CrushWrapper& crush,
						map<int, const char*>& rulesets)
{
  const md_config_t *conf = cct->_conf;

  crush.create();

  crush.set_type_name(0, "osd");
  crush.set_type_name(1, "host");
  crush.set_type_name(2, "rack");
  crush.set_type_name(3, "row");
  crush.set_type_name(4, "room");
  crush.set_type_name(5, "datacenter");
  crush.set_type_name(6, "root");

  set<string> hosts, racks;

  // root
  int rootid = crush.add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, 6 /* pool */, 0, NULL, NULL);
  crush.set_item_name(rootid, "default");

  // add osds
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

    string host, rack, row, room, dc, pool;
    vector<string> sections;
    sections.push_back("osd");
    sections.push_back(*i);
    conf->get_val_from_conf_file(sections, "host", host, false);
    conf->get_val_from_conf_file(sections, "rack", rack, false);
    conf->get_val_from_conf_file(sections, "row", row, false);
    conf->get_val_from_conf_file(sections, "room", room, false);
    conf->get_val_from_conf_file(sections, "datacenter", dc, false);
    conf->get_val_from_conf_file(sections, "root", pool, false);

    if (host.length() == 0)
      host = "unknownhost";
    if (rack.length() == 0)
      rack = "unknownrack";

    hosts.insert(host);
    racks.insert(rack);

    map<string,string> loc;
    loc["host"] = host;
    loc["rack"] = rack;
    if (row.size())
      loc["row"] = row;
    if (room.size())
      loc["room"] = room;
    if (dc.size())
      loc["datacenter"] = dc;
    loc["root"] = "default";

    ldout(cct, 5) << " adding osd." << o << " at " << loc << dendl;
    crush.insert_item(cct, o, 1.0, *i, loc);
  }

  // rules
  int minrep = conf->osd_min_rep;
  int maxrep = conf->osd_max_rep;
  for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); ++p) {
    int ruleset = p->first;
    crush_rule *rule = crush_make_rule(3, ruleset, pg_pool_t::TYPE_REP, minrep, maxrep);
    assert(rule);
    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);

    if (racks.size() > 3) {
      // spread replicas across hosts
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, CRUSH_CHOOSE_N, 2);
    } else if (hosts.size() > 1) {
      // spread replicas across hosts
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, CRUSH_CHOOSE_N, 1);
    } else {
      // just spread across osds
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, CRUSH_CHOOSE_N, 0);
    }
    crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
    int rno = crush_add_rule(crush.crush, rule, -1);
    crush.set_rule_name(rno, p->second);
  }

  crush.finalize();
}

void PGOSDMap::generate_test_instances(list<PGOSDMap*>& o)
{
  o.push_back(new PGOSDMap);

  PGOSDMap* map2 = new PGOSDMap;
  CephContext *cct = new CephContext(CODE_ENVIRONMENT_UTILITY);
  o.push_back(map2);
  uuid_d fsid;
  map2->build_simple(cct, 1, fsid, 16, 7, 8);
  map2->created = map2->modified = utime_t(1, 2);  // fix timestamp
  cct->put();
}


void PGOSDMap::thrash(Monitor* mon,
		      OSDMap::Incremental& pending_inc_orig)
{
  Incremental& pending_inc = dynamic_cast<Incremental&>(pending_inc_orig);

  // generate some pg_temp entries.
  // let's assume the hash_map iterates in a random-ish order.
  int n = rand() % mon->pgmon()->pg_map.pg_stat.size();
  hash_map<pg_t,pg_stat_t>::iterator p = mon->pgmon()->pg_map.pg_stat.begin();
  hash_map<pg_t,pg_stat_t>::iterator e = mon->pgmon()->pg_map.pg_stat.end();
  while (n--)
    ++p;

  for (int i=0; i<50; i++) {
    vector<int> v;
    for (int j=0; j<3; j++) {
      int o = rand() % get_num_osds();
      if (exists(o) && std::find(v.begin(), v.end(), o) == v.end())
	v.push_back(o);
    }
    if (v.size() < 3) {
      for (vector<int>::iterator q = p->second.acting.begin(); q != p->second.acting.end(); ++q)
	if (std::find(v.begin(), v.end(), *q) == v.end())
	  v.push_back(*q);
    }
    if (!v.empty())
      pending_inc.new_pg_temp[p->first] = v;

    dout(5) << "thrash_map pg " << p->first << " pg_temp remapped to "
	    << v << dendl;

    ++p;
    if (p == e)
      p = mon->pgmon()->pg_map.pg_stat.begin();
  }
}


int PGOSDMap::get_oid_osd(const Objecter* objecter,
			  const object_t& oid,
			  const ceph_file_layout* layout)
{
  ceph_object_layout olayout = file_to_object_layout(oid, *layout);

  pg_t pg = (pg_t)olayout.ol_pgid;
  vector<int> osds;
  pg_to_osds(pg, osds);
  return osds[0];
}


int PGOSDMap::get_pool_replication(int64_t pool)
{
  if (!have_pg_pool(pool)) {
    return -ENOENT;
  }
  return get_pg_pool(pool)->get_size();
}


int PGOSDMap::get_file_stripe_address(vector<ObjectExtent>& extents,
				      vector<entity_addr_t>& address)
{
  // now we have the object and its 'layout'
  pg_t pg = object_locator_to_pg(extents[0].oid, extents[0].oloc);
  vector<int> osds;
  pg_to_acting_osds(pg, osds);
  if (!osds.size()) {
    return -EINVAL;
  }

  for (unsigned i = 0; i < osds.size(); i++) {
    entity_addr_t addr = get_addr(osds[i]);
    address.push_back(addr);
  }

  return 0;
}

uint64_t PGOSDMap::get_features(uint64_t *pmask) const
{
  uint64_t features = 0;
  uint64_t mask = 0;

  if (crush->has_nondefault_tunables())
    features |= CEPH_FEATURE_CRUSH_TUNABLES;
  if (crush->has_nondefault_tunables2())
    features |= CEPH_FEATURE_CRUSH_TUNABLES2;
  mask |= CEPH_FEATURES_CRUSH;

  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    if (p->second.flags & pg_pool_t::FLAG_HASHPSPOOL) {
      features |= CEPH_FEATURE_OSDHASHPSPOOL;
    }
  }
  mask |= CEPH_FEATURE_OSDHASHPSPOOL;

  if (pmask)
    *pmask = mask;
  return features;
}
