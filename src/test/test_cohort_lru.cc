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
#include "common/oid.h"
#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

namespace {
  namespace bi = boost::intrusive;

  class TObject : public cohort::lru::Object
    {
    public:
    typedef bi::link_mode<bi::safe_link> link_mode;

    typedef bi::set_member_hook<link_mode> tree_hook_type;
    tree_hook_type oid_hook;

    hoid_t oid;
    uint64_t hk; /* hash key */

    TObject(const hoid_t& _oid) : oid(_oid), hk(0) {}

    TObject(const hoid_t& _oid, uint64_t _hk) : oid(_oid), hk(_hk) {}

    typedef cohort::lru::LRU<cohort::SpinLock> ObjLRU;

    /* per-volume lookup table */
    struct OidLT
    {
      // for internal ordering
      bool operator()(const TObject& lhs,  const TObject& rhs) const
      { return lhs.oid < rhs.oid; }

      // for external search by hoid_t
      bool operator()(const hoid_t& oid, const TObject& o) const
      { return oid < o.oid; }

      bool operator()(const TObject& o, const hoid_t& oid) const
      { return o.oid < oid; }
    };

    struct OidEQ
    {
      bool operator()(const TObject& lhs, const TObject& rhs) const
      { return lhs.oid == rhs.oid; }

      bool operator()(const hoid_t& oid, const TObject& o) const
      { return oid == o.oid; }

      bool operator()(const TObject& o, const hoid_t& oid) const
      { return o.oid == oid; }
    };

    typedef bi::member_hook<
    TObject, tree_hook_type, &TObject::oid_hook> OidHook;

    typedef bi::rbtree<TObject, bi::compare<OidLT>, OidHook,
    bi::constant_time_size<true> > OidTree;

    typedef cohort::lru::TreeX<
    TObject, OidTree, OidLT, OidEQ, hoid_t, cohort::SpinLock>
    ObjCache;

    bool reclaim() { return true; }

    }; /* TObject */

  typedef TObject T3;
  typedef TObject T5;

  TObject::ObjCache T1Cache(1);
  TObject::ObjCache T3Cache(3);
  TObject::ObjCache T5Cache(5);

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
    T3* o3 = new T3(hoid_t(name), hk);
    vt3.push_back(o3);
  }
  ASSERT_EQ(vt3.size(), uint32_t(n_create));
}

TEST(CohortLRU, T5_NEW)
{
  for (int ix = 0; ix < n_create; ++ix) {
    string name{"osbench_"};
    name += std::to_string(ix);
    uint64_t hk = XXH64(name.c_str(), name.size(), 667);
    T5* o5 = new T5(hoid_t(name), hk);
    vt5.push_back(o5);
  }
  ASSERT_EQ(vt5.size(), uint32_t(n_create));
}

TEST(CohortLRU, T3_TREEX_INSERT_CHECK) {
  for (unsigned int ix = 0; ix < vt3.size(); ++ix) {
    T3* o3 = vt3[ix];
    TObject::ObjCache::Latch lat;
    T3* o3f = T3Cache.find_latch(o3->hk, o3->oid, lat,
				 TObject::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o3f, nullptr);
    T3Cache.insert_latched(o3, lat, TObject::ObjCache::FLAG_UNLOCK);
  }
}

TEST(CohortLRU, T5_TREEX_INSERT_CHECK) {
  for (unsigned int ix = 0; ix < vt5.size(); ++ix) {
    T5* o5 = vt5[ix];
    TObject::ObjCache::Latch lat;
    T5* o5f = T5Cache.find_latch(o5->hk, o5->oid, lat,
				 TObject::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o5f, nullptr);
    T5Cache.insert_latched(o5, lat, TObject::ObjCache::FLAG_UNLOCK);
  }
}

TEST(CohortLRU, T3_FIND_ALL) {
 for (unsigned int ix = 0; ix < vt3.size(); ++ix) {
    T3* o3 = vt3[ix];
    T3* o3a = T3Cache.find(o3->hk, o3->oid, TObject::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o3, o3a);
 }
}

TEST(CohortLRU, T3_FIND_LATCH_ALL) {
 for (unsigned int ix = 0; ix < vt3.size(); ++ix) {
    T3* o3 = vt3[ix];
    TObject::ObjCache::Latch lat;
    T3* o3a = T3Cache.find_latch(o3->hk, o3->oid, lat,
				 TObject::ObjCache::FLAG_LOCK|
				 TObject::ObjCache::FLAG_UNLOCK);
    ASSERT_EQ(o3, o3a);
 }
}

TEST(CohortLRU, T5_FIND_ALL) {
 for (unsigned int ix = 0; ix < vt5.size(); ++ix) {
    T5* o5 = vt5[ix];
    T5* o5a = T5Cache.find(o5->hk, o5->oid, TObject::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o5, o5a);
 }
}

TEST(CohortLRU, T5_FIND_LATCH_ALL) {
 for (unsigned int ix = 0; ix < vt5.size(); ++ix) {
    T5* o5 = vt5[ix];
    TObject::ObjCache::Latch lat;
    T5* o5a = T5Cache.find_latch(o5->hk, o5->oid, lat,
				 TObject::ObjCache::FLAG_LOCK|
				 TObject::ObjCache::FLAG_UNLOCK);
    ASSERT_EQ(o5, o5a);
 }
}

TEST(CohortLRU, T5_REMOVE) {
  vector<T5*> del5;
  for (unsigned int ix = 0; ix < vt5.size(); ix += 3) {
    T5* o5 = vt5[ix];
    T5Cache.remove(o5->hk, o5, TObject::ObjCache::FLAG_LOCK);
    del5.push_back(o5);
  }
  /* find none of del5 */
  for (unsigned int ix = 0; ix < del5.size(); ++ix) {
    T5* o5 = del5[ix];
    T5* o5a = T5Cache.find(o5->hk, o5->oid, TObject::ObjCache::FLAG_LOCK);
    ASSERT_EQ(o5a, nullptr);
  }
  /* delete removed */
  while (del5.size() > 0) {
    T5* o5 = del5.back();
    del5.pop_back();
    delete o5;
  }
}

TEST(CohortLRU, ALL_DRAIN) {
  /* clear vecs */
  vt3.clear();
  vt5.clear();

  /* remove and dispose */
  T3Cache.drain([](T3* o3){ delete o3; },
    TObject::ObjCache::FLAG_LOCK);
  T5Cache.drain([](T5* o5){ delete o5; },
    TObject::ObjCache::FLAG_LOCK);
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  CephContext *cct =
    global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
