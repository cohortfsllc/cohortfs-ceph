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


#ifndef CEPH_PGOSDMAP_H
#define CEPH_PGOSDMAP_H


#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"

#include "pg_types.h"


class PGOSDMap : public OSDMap {

private:
  typedef OSDMap inherited;

public:
  void thrash(Monitor* mon, OSDMap::Incremental& pending_inc_orig);

  /**
   * get feature bits required by the current structure
   *
   * @param mask [out] set of all possible map-related features we could set
   * @return feature bits used by this map
   */
  virtual uint64_t get_features(uint64_t *mask) const;


  class Incremental : public OSDMap::Incremental {
  private:
    typedef OSDMap::Incremental inherited;

  public:
    bufferlist crush;
    int64_t new_pool_max; //incremented by the OSDMonitor on each pool create

    // incremental
    map<int64_t,pg_pool_t> new_pools;
    map<int64_t,string> new_pool_names;
    set<int64_t> old_pools;
    map<pg_t,vector<int32_t> > new_pg_temp;     // [] to remove

    void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
    void decode(bufferlist::iterator &p);
    void decode(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    void dump(Formatter *f) const;

  private:
    void decode_pre7(bufferlist::iterator &p, __u16 v);
    void encode_client_old(bufferlist& bl) const;

  public:

    Incremental(epoch_t e=0) :
      OSDMap::Incremental(e),
      new_pool_max(-1)
    {
      // empty
    }
    Incremental(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    Incremental(bufferlist::iterator &p) {
      decode(p);
    }

    PGOSDMap* newOSDMap() const {
      return new PGOSDMap();
    }

    static void generate_test_instances(list<Incremental*>& o);
  }; // class PGOSDMap::Incremental

  int32_t pool_max;     // the largest pool num, ever

  // temp pg mapping (e.g. while we rebuild)
  std::tr1::shared_ptr< map<pg_t,vector<int> > > pg_temp;

  map<int64_t,pg_pool_t> pools;
  map<int64_t,string> pool_name;
  map<string,int64_t> name_pool;

public:

  PGOSDMap()
    : OSDMap(),
      pool_max(-1),
      pg_temp(new map<pg_t,vector<int> >)
  {
    // empty for now
  }

  ~PGOSDMap() {}

  std::tr1::shared_ptr<CrushWrapper> crush;       // hierarchical map

  friend class PGOSDMonitor;
  friend class PGMonitor;

  /**
   * check if an entire crush subtre is down
   */
  bool subtree_is_down(int id, set<int> *down_cache) const;
  bool containing_subtree_is_down(CephContext *cct, int osd, int subtree_type, set<int> *down_cache) const;

  // serialize, unserialize

  void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f) const;
  void print(ostream& out) const;

  void set_epoch(epoch_t e);

private:

  void encode_client_old(bufferlist& bl) const;
  void decode_pre7(bufferlist::iterator &p, __u16 v);

public:

  virtual Incremental* newIncremental() const;

  /*
   * handy helpers to build simple maps...
   */
  void build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
		    int num_osd);

  int build_simple_from_conf(CephContext *cct, epoch_t e, uuid_d &fsid);
  static void build_simple_crush_map(CephContext *cct, CrushWrapper& crush,
				     map<int, const char*>& poolsets, int num_osd);
  static void build_simple_crush_map_from_conf(CephContext *cct, CrushWrapper& crush,
					       map<int, const char*>& rulesets);

  bool crush_ruleset_in_use(int ruleset) const;

  virtual int get_oid_osd(const Objecter* objecter,
			  const object_t& oid,
			  const ceph_file_layout* layout);

  virtual int get_pool_replication(int64_t pool);

  virtual int get_file_stripe_address(vector<ObjectExtent>& extents,
				      vector<entity_addr_t>& address);

  static void generate_test_instances(list<PGOSDMap*>& o);

  int apply_incremental_subclass(const OSDMap::Incremental& inc);

  /// try to re-use/reference addrs in oldmap from newmap
  static void dedup(const OSDMap *oldmap, OSDMap *newmap);

  /****   mapping facilities   ****/
  int object_locator_to_pg(const object_t& oid,
			   const object_locator_t& loc,
			   pg_t& pg) const;
  pg_t object_locator_to_pg(const object_t& oid,
			    const object_locator_t& loc) const {
    pg_t pg;
    int ret = object_locator_to_pg(oid, loc, pg);
    assert(ret == 0);
    return pg;
  }

  static object_locator_t
  file_to_object_locator(const ceph_file_layout& layout) {
    return object_locator_t(layout.fl_pg_pool);
  }

  // oid -> pg
  ceph_object_layout file_to_object_layout(object_t oid,
					   const ceph_file_layout& layout) const {
    return make_object_layout(oid, layout.fl_pg_pool);
  }

  ceph_object_layout make_object_layout(object_t oid, int pg_pool) const;

  int get_pg_num(int pg_pool) const
  {
    const pg_pool_t *pool = get_pg_pool(pg_pool);
    return pool->get_pg_num();
  }

private:

  /// pg -> (raw osd list)
  int _pg_to_osds(const pg_pool_t& pool, pg_t pg, vector<int>& osds) const;

  /// pg -> (up osd list)
  void _raw_to_up_osds(pg_t pg, vector<int>& raw, vector<int>& up) const;

  bool _raw_to_temp_osds(const pg_pool_t& pool, pg_t pg, vector<int>& raw, vector<int>& temp) const;

public:

  int pg_to_osds(pg_t pg, vector<int>& raw) const;
  int pg_to_acting_osds(pg_t pg, vector<int>& acting) const;
  void pg_to_raw_up(pg_t pg, vector<int>& up) const;
  void pg_to_up_acting_osds(pg_t pg, vector<int>& up, vector<int>& acting) const;

  int64_t lookup_pg_pool_name(const string& name) {
    if (name_pool.count(name))
      return name_pool[name];
    return -ENOENT;
  }

  int64_t const_lookup_pg_pool_name(const char *name) const {
    return const_cast<PGOSDMap *>(this)->lookup_pg_pool_name(name);
  }

  int64_t get_pool_max() const {
    return pool_max;
  }
  const map<int64_t,pg_pool_t>& get_pools() const {
    return pools;
  }
  const char *get_pool_name(int64_t p) const {
    map<int64_t, string>::const_iterator i = pool_name.find(p);
    if (i != pool_name.end())
      return i->second.c_str();
    return 0;
  }

  bool have_pg_pool(int64_t p) const {
    return pools.count(p);
  }

  const pg_pool_t* get_pg_pool(int64_t p) const {
    map<int64_t, pg_pool_t>::const_iterator i = pools.find(p);
    if (i != pools.end())
      return &i->second;
    return NULL;
  }
  unsigned get_pg_size(pg_t pg) const {
    map<int64_t,pg_pool_t>::const_iterator p = pools.find(pg.pool());
    assert(p != pools.end());
    return p->second.get_size();
  }
  int get_pg_type(pg_t pg) const {
    assert(pools.count(pg.pool()));
    return pools.find(pg.pool())->second.get_type();
  }

  pg_t raw_pg_to_pg(pg_t pg) const {
    assert(pools.count(pg.pool()));
    return pools.find(pg.pool())->second.raw_pg_to_pg(pg);
  }

  // pg -> primary osd
  int get_pg_primary(pg_t pg) const {
    vector<int> group;
    int nrep = pg_to_osds(pg, group);
    if (nrep)
      return group[0];
    return -1;  // we fail!
  }

  // pg -> acting primary osd
  int get_pg_acting_primary(pg_t pg) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    if (nrep > 0)
      return group[0];
    return -1;  // we fail!
  }

  int get_pg_acting_tail(pg_t pg) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    if (nrep > 0)
      return group[group.size()-1];
    return -1;  // we fail!
  }

  /* what replica # is a given osd? 0 primary, -1 for none. */
  static int calc_pg_rank(int osd, vector<int>& acting, int nrep=0);
  static int calc_pg_role(int osd, vector<int>& acting, int nrep=0);

  /* rank is -1 (stray), 0 (primary), 1,2,3,... (replica) */
  int get_pg_acting_rank(pg_t pg, int osd) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    return calc_pg_rank(osd, group, nrep);
  }
  /* role is -1 (stray), 0 (primary), 1 (replica) */
  int get_pg_acting_role(pg_t pg, int osd) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    return calc_pg_role(osd, group, nrep);
  }
};
WRITE_CLASS_ENCODER_FEATURES(PGOSDMap)
WRITE_CLASS_ENCODER_FEATURES(PGOSDMap::Incremental)

typedef std::tr1::shared_ptr<const PGOSDMap> PGOSDMapRef;

#endif // CEPH_PGOSDMAP_H
