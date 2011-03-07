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



void OSDMap::print(ostream& out) const
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "created " << get_created() << "\n"
      << "modifed " << get_modified() << "\n";

  out << "flags";
  if (test_flag(CEPH_OSDMAP_NEARFULL))
    out << " nearfull";
  if (test_flag(CEPH_OSDMAP_FULL))
    out << " full";
  if (test_flag(CEPH_OSDMAP_PAUSERD))
    out << " pauserd";
  if (test_flag(CEPH_OSDMAP_PAUSEWR))
    out << " pausewr";
  if (test_flag(CEPH_OSDMAP_PAUSEREC))
    out << " pauserec";
  out << "\n" << std::endl;
 
  for (map<int,pg_pool_t>::const_iterator p = pools.begin(); p != pools.end(); ++p) {
    std::string name("<unknown>");
    map<int32_t,string>::const_iterator pni = pool_name.find(p->first);
    if (pni != pool_name.end())
      name = pni->second;
    out << "pg_pool " << p->first
	<< " '" << name
	<< "' " << p->second << "\n";
    for (map<snapid_t,pool_snap_info_t>::const_iterator q = p->second.snaps.begin();
	 q != p->second.snaps.end();
	 q++)
      out << "\tsnap " << q->second.snapid << " '" << q->second.name << "' " << q->second.stamp << "\n";
    if (!p->second.removed_snaps.empty())
      out << "\tremoved_snaps " << p->second.removed_snaps << "\n";
  }
  out << std::endl;

  out << "max_osd " << get_max_osd() << "\n";
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      out << "osd" << i;
      out << (is_up(i) ? " up  ":" down");
      out << (is_in(i) ? " in ":" out");
      if (is_in(i))
	out << " weight " << get_weightf(i);
      const osd_info_t& info(get_info(i));
      out << " " << info;
      if (is_up(i))
	out << " " << get_addr(i) << " " << get_cluster_addr(i) << " " << get_hb_addr(i);
      out << "\n";
    }
  }
  out << std::endl;

  for (map<pg_t,vector<int> >::const_iterator p = pg_temp.begin();
       p != pg_temp.end();
       p++)
    out << "pg_temp " << p->first << " " << p->second << "\n";
  
  for (hash_map<entity_addr_t,utime_t>::const_iterator p = blacklist.begin();
       p != blacklist.end();
       p++)
    out << "blacklist " << p->first << " expires " << p->second << "\n";
  
  // ignore pg_swap_primary
}

void OSDMap::print_summary(ostream& out) const
{
  out << "e" << get_epoch() << ": "
      << get_num_osds() << " osds: "
      << get_num_up_osds() << " up, " 
      << get_num_in_osds() << " in";
}


void OSDMap::build_simple(epoch_t e, ceph_fsid_t &fsid,
			  int num_osd, int num_dom, int pg_bits, int pgp_bits, int lpg_bits)
{
  dout(10) << "build_simple on " << num_osd
	   << " osds with " << pg_bits << " pg bits per osd, "
	   << lpg_bits << " lpg bits" << dendl;
  epoch = e;
  set_fsid(fsid);
  created = modified = g_clock.now();

  set_max_osd(num_osd);

  // pgp_num <= pg_num
  if (pgp_bits > pg_bits)
    pgp_bits = pg_bits; 
  
  // crush map
  map<int, const char*> rulesets;
  rulesets[CEPH_DATA_RULE] = "data";
  rulesets[CEPH_METADATA_RULE] = "metadata";
  rulesets[CEPH_CASDATA_RULE] = "casdata";
  rulesets[CEPH_RBD_RULE] = "rbd";
  
  for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); p++) {
    int pool = ++pool_max;
    pools[pool].v.type = CEPH_PG_TYPE_REP;
    pools[pool].v.size = 2;
    pools[pool].v.crush_ruleset = p->first;
    pools[pool].v.object_hash = CEPH_STR_HASH_RJENKINS;
    pools[pool].v.pg_num = num_osd << pg_bits;
    pools[pool].v.pgp_num = num_osd << pgp_bits;
    pools[pool].v.lpg_num = lpg_bits ? (1 << (lpg_bits-1)) : 0;
    pools[pool].v.lpgp_num = lpg_bits ? (1 << (lpg_bits-1)) : 0;
    pools[pool].v.last_change = epoch;
    pool_name[pool] = p->second;
  }

  build_simple_crush_map(crush, rulesets, num_osd, num_dom);

  for (int i=0; i<num_osd; i++) {
    set_state(i, 0);
    set_weight(i, CEPH_OSD_OUT);
  }
}

void OSDMap::build_simple_crush_map(CrushWrapper& crush, map<int, const char*>& rulesets, int num_osd,
				    int num_dom)
{
  // new
  crush.create();

  crush.set_type_name(1, "domain");
  crush.set_type_name(2, "pool");

  int minrep = g_conf.osd_min_rep;
  int maxrep = g_conf.osd_max_rep;
  assert(maxrep >= minrep);
  int ndom = num_dom;
  if (!ndom)
    ndom = MAX(maxrep, g_conf.osd_max_raid_width);
  if (ndom > 1 &&
      num_osd >= ndom*3 &&
      num_osd > 8) {
    int ritems[ndom];
    int rweights[ndom];

    int nper = ((num_osd - 1) / ndom) + 1;
    dout(0) << ndom << " failure domains, " << nper << " osds each" << dendl;
    
    int o = 0;
    for (int i=0; i<ndom; i++) {
      int items[nper], weights[nper];      
      int j;
      rweights[i] = 0;
      for (j=0; j<nper; j++, o++) {
	if (o == num_osd) break;
	dout(20) << "added osd" << o << dendl;
	items[j] = o;
	weights[j] = 0x10000;
	//w[j] = weights[o] ? (0x10000 - (int)(weights[o] * 0x10000)):0x10000;
	//rweights[i] += w[j];
	rweights[i] += 0x10000;
      }

      crush_bucket *domain = crush_make_bucket(CRUSH_BUCKET_UNIFORM, CRUSH_HASH_DEFAULT, 1, j, items, weights);
      ritems[i] = crush_add_bucket(crush.crush, 0, domain);
      dout(20) << "added domain bucket i " << ritems[i] << " of size " << j << dendl;

      char bname[10];
      snprintf(bname, sizeof(bname), "dom%d", i);
      crush.set_item_name(ritems[i], bname);
    }
    
    // root
    crush_bucket *root = crush_make_bucket(CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, 2, ndom, ritems, rweights);
    int rootid = crush_add_bucket(crush.crush, 0, root);
    crush.set_item_name(rootid, "root");

    // rules
    for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); p++) {
      int ruleset = p->first;
      crush_rule *rule = crush_make_rule(3, ruleset, CEPH_PG_TYPE_REP, minrep, maxrep);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, CRUSH_CHOOSE_N, 1); // choose N domains
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      int rno = crush_add_rule(crush.crush, rule, -1);
      crush.set_rule_name(rno, p->second);
    }
    
  } else {
    // one bucket

    int items[num_osd];
    int weights[num_osd];
    for (int i=0; i<num_osd; i++) {
      items[i] = i;
      weights[i] = 0x10000;
    }

    crush_bucket *b = crush_make_bucket(CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, 1, num_osd, items, weights);
    int rootid = crush_add_bucket(crush.crush, 0, b);
    crush.set_item_name(rootid, "root");

    // replication
    for (map<int,const char*>::iterator p = rulesets.begin(); p != rulesets.end(); p++) {
      int ruleset = p->first;
      crush_rule *rule = crush_make_rule(3, ruleset, CEPH_PG_TYPE_REP, g_conf.osd_min_rep, maxrep);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, CRUSH_CHOOSE_N, 0);
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      int rno = crush_add_rule(crush.crush, rule, -1);
      crush.set_rule_name(rno, p->second);
    }

  }

  crush.finalize();

  dout(20) << "crush max_devices " << crush.crush->max_devices << dendl;
}

