// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <tuple>
#include "common/cohort_lru.h"
#include "common/hobject.h"
#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

namespace {
  namespace bi = boost::intrusive;

  CephContext *cct;

  template <uint8_t N=3>
  class TObject : public cohort::lru::Object
    {
    public:
    typedef bi::link_mode<bi::safe_link> link_mode;

    typedef bi::set_member_hook<link_mode> tree_hook_type;
    tree_hook_type oid_hook;

    hobject_t oid;
    uint64_t hk; /* hash key */

    TObject(const hobject_t& _oid) : oid(_oid), hk(0) {}

    TObject(const hobject_t& _oid, uint64_t _hk) : oid(_oid), hk(_hk) {}

    /* per ObjectStore LRU */
    const static int n_lanes = 17; // # of lanes in LRU system

    typedef cohort::lru::LRU<cohort::SpinLock, n_lanes> ObjLRU;

    const static int n_partitions = N;
    const static int cache_size = 373; // per-partiion cache size

    typedef std::tuple<uint64_t, const hobject_t&> ObjKeyType;

    /* per-volume lookup table */
    struct OidLT
    {
      // for internal ordering
      bool operator()(const TObject& lhs,  const TObject& rhs) const
      {
	return ((lhs.hk < rhs.hk) ||
		((lhs.hk == rhs.hk) && (lhs.oid < rhs.oid)));
      }

      bool operator()(const ObjKeyType k, const TObject& o) const
      {
	return ((std::get<0>(k) < o.hk) ||
		((std::get<0>(k) == o.hk) &&
		 (std::get<1>(k) < o.oid)));
      }

      bool operator()(const TObject& o, const ObjKeyType k) const
      {
	return ((o.hk < std::get<0>(k)) ||
		((o.hk == std::get<0>(k)) &&
		 (o.oid < std::get<1>(k))));
      }
    };

    struct OidEQ
    {
      bool operator()(const TObject& lhs,  const TObject& rhs) const
      {  return lhs.oid == rhs.oid; }

      bool operator()(const ObjKeyType k, const TObject& o) const
      {
	return (std::get<1>(k) == o.oid);
      }

      bool operator()(const TObject& o, const ObjKeyType k) const
      {
	return (o.oid == std::get<1>(k));
      }
    };

    typedef bi::member_hook<
    TObject, tree_hook_type, &TObject::oid_hook> OidHook;

    typedef bi::rbtree<TObject, bi::compare<OidLT>, OidHook,
    bi::constant_time_size<true> > OidTree;

    typedef cohort::lru::TreeX<
    TObject, OidTree, OidLT, OidEQ, ObjKeyType, cohort::SpinLock,
    n_partitions, cache_size>
    ObjCache;

    bool reclaim() { return true; }

    }; /* TObject */

  typedef TObject<3> T3;
  typedef TObject<5> T5;

  TObject<1>::ObjCache T1Cache;
  TObject<3>::ObjCache T3Cache;
  TObject<5>::ObjCache T5Cache;

  static constexpr int32_t n_create = 30;

  vector<T3*> vt3;
  vector<T5*> vt5;

} /* namespace */

TEST(CohortLRU, T3_NEW)
{
  for (int ix = 0; ix < n_create; ++ix) {
    string name{"osbench_"};
    name += std::to_string(ix);
    uint64_t hk = XXH64(name.c_str(), name.size(), 667);
    T3* o3 = new T3(hobject_t(name), hk);
    vt3.push_back(o3);
  }
  ASSERT_EQ(vt3.size(), n_create);
}

TEST(CohortLRU, T5_NEW)
{
  for (int ix = 0; ix < n_create; ++ix) {
    string name{"osbench_"};
    name += std::to_string(ix);
    uint64_t hk = XXH64(name.c_str(), name.size(), 667);
    T5* o5 = new T5(hobject_t(name), hk);
    vt5.push_back(o5);
  }
  ASSERT_EQ(vt5.size(), n_create);
}

TEST(CohortLRU, T3_TREEX_INSERT_CHECK) {
  for (unsigned int ix = 0; ix < vt3.size(); ++ix) {
    T3* o3 = vt3[ix];
    std::tuple<uint64_t, const hobject_t&> k(o3->hk, o3->oid);
    TObject<3>::ObjCache::Latch lat;
    T3* o3f = T3Cache.find_latch(o3->hk, k, lat,
				 TObject<3>::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o3f, nullptr);
    T3Cache.insert_latched(o3, lat, TObject<3>::ObjCache::FLAG_UNLOCK);
  }
}

TEST(CohortLRU, T5_TREEX_INSERT_CHECK) {
  for (unsigned int ix = 0; ix < vt5.size(); ++ix) {
    T5* o5 = vt5[ix];
    std::tuple<uint64_t, const hobject_t&> k(o5->hk, o5->oid);
    TObject<5>::ObjCache::Latch lat;
    T5* o5f = T5Cache.find_latch(o5->hk, k, lat,
				 TObject<5>::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o5f, nullptr);
    T5Cache.insert_latched(o5, lat, TObject<5>::ObjCache::FLAG_UNLOCK);
  }
}


TEST(CohortLRU, T3_FIND_ALL) {
 for (unsigned int ix = 0; ix < vt3.size(); ++ix) {
    T3* o3 = vt3[ix];
    std::tuple<uint64_t, const hobject_t&> k(o3->hk, o3->oid);
    T3* o3a = T3Cache.find(o3->hk, k, TObject<3>::ObjCache::FLAG_NONE);
    ASSERT_EQ(o3, o3a);
 }
}

TEST(CohortLRU, T3_FIND_LATCH_ALL) {
 for (unsigned int ix = 0; ix < vt3.size(); ++ix) {
    T3* o3 = vt3[ix];
    TObject<3>::ObjCache::Latch lat;
    std::tuple<uint64_t, const hobject_t&> k(o3->hk, o3->oid);
    T3* o3a = T3Cache.find_latch(o3->hk, k, lat,
				 TObject<3>::ObjCache::FLAG_LOCK|
				 TObject<3>::ObjCache::FLAG_UNLOCK);
    ASSERT_EQ(o3, o3a);
 }
}

TEST(CohortLRU, T5_FIND_ALL) {
 for (unsigned int ix = 0; ix < vt5.size(); ++ix) {
    T5* o5 = vt5[ix];
    std::tuple<uint64_t, const hobject_t&> k(o5->hk, o5->oid);
    T5* o5a = T5Cache.find(o5->hk, k, TObject<5>::ObjCache::FLAG_NONE);
    ASSERT_EQ(o5, o5a);
 }
}

TEST(CohortLRU, T5_FIND_LATCH_ALL) {
 for (unsigned int ix = 0; ix < vt5.size(); ++ix) {
    T5* o5 = vt5[ix];
    TObject<5>::ObjCache::Latch lat;
    std::tuple<uint64_t, const hobject_t&> k(o5->hk, o5->oid);
    T5* o5a = T5Cache.find_latch(o5->hk, k, lat,
				 TObject<5>::ObjCache::FLAG_LOCK|
				 TObject<5>::ObjCache::FLAG_UNLOCK);
    ASSERT_EQ(o5, o5a);
 }
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
	      CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
