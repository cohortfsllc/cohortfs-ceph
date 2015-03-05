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

#include <iostream>
#include <map>
#include "include/AssocVector.hpp"
#include "common/oid.h"
#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

namespace {

  typedef std::map<oid_t,uint32_t> map_oid_dict;
  typedef std::map<hoid_t,uint32_t> map_hoid_dict;

  typedef av::AssocVector<oid_t,uint32_t> av_oid_dict;
  typedef av::AssocVector<hoid_t,uint32_t> av_hoid_dict;

  map_oid_dict d1;
  map_hoid_dict d2;
  av_oid_dict d3;
  av_hoid_dict d4;

  static constexpr uint32_t n_small = 30;
  static constexpr uint32_t n_large = 1400;

  vector<oid_t> vo_small;
  vector<hoid_t> vho_small;

  vector<oid_t> vo_large;
  vector<hoid_t> vho_large;
} /* namespace */

TEST(AV, SIZE_EMPTY)
{
  std::cout << "\nsizes: map_oid_dict: " << sizeof(d1)
	    << " map_hoid_dict: " << sizeof(d2)
	    << " av_oid_dict: " << sizeof(d3)
	    << " av_hoid_dict: " << sizeof(d4)
	    << std::endl;

  ASSERT_EQ(0, 0);
}

TEST(AV, OIDS)
{
  string oid("oid_");
  for (unsigned ix = 0; ix < n_small; ++ix) {
    oid_t o{oid + std::to_string(ix)};
    vo_small.push_back(o);
    vho_small.push_back(hoid_t(o));
  }
  for (unsigned ix = 0; ix < n_large; ++ix) {
    oid_t o{oid + std::to_string(ix)};
    vo_large.push_back(o);
    vho_large.push_back(hoid_t(o));
  }
}

TEST(AV, INSERTS_SMALL)
{
  string oid("oid_");
  auto t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    d1.insert(map_oid_dict::value_type(vo_small[ix], ix));
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto dur1 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    d2.insert(map_hoid_dict::value_type(vho_small[ix], ix));
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur2 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    d3.insert(av_oid_dict::value_type(vo_small[ix], ix));
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur3 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    d4.insert(av_hoid_dict::value_type(vho_small[ix], ix));
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur4 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  std::cout << "map_oid_dict insert " << n_small
	    << " in " << dur1 << std::endl;

  std::cout << "map_hoid_dict insert " << n_small
	    << " in " << dur2 << std::endl;

  std::cout << "av_oid_dict insert " << n_small
	    << " in " << dur3 << std::endl;

  std::cout << "av_hoid_dict insert " << n_small
	    << " in " << dur4 << std::endl;
}

TEST(AV, FIND_SMALL)
{
  string oid("oid_");
  auto t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    (void) d1.find(vo_small[ix]);
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto dur1 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    (void) d2.find(vho_small[ix]);
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur2 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    (void) d3.find(vo_small[ix]);
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur3 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_small; ++ix) {
    (void) d4.find(vho_small[ix]);
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur4 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  std::cout << "map_oid_dict find " << n_small
	    << " in " << dur1 << std::endl;

  std::cout << "map_hoid_dict find " << n_small
	    << " in " << dur2 << std::endl;

  std::cout << "av_oid_dict find " << n_small
	    << " in " << dur3 << std::endl;

  std::cout << "av_hoid_dict find " << n_small
	    << " in " << dur4 << std::endl;
}

TEST(AV, CLEAR_SMALL)
{
  d1.clear();
  d2.clear();
  d3.clear();
  d4.clear();
}

TEST(AV, INSERTS_LARGE)
{
  auto t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    d1.insert(map_oid_dict::value_type(vo_large[ix], ix));
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto dur1 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    d2.insert(map_hoid_dict::value_type(vho_large[ix], ix));
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur2 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    d3.insert(av_oid_dict::value_type(vo_large[ix], ix));
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur3 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    d4.insert(av_hoid_dict::value_type(vho_large[ix], ix));
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur4 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  std::cout << "map_oid_dict insert " << n_large
	    << " in " << dur1 << std::endl;

  std::cout << "map_hoid_dict insert " << n_large
	    << " in " << dur2 << std::endl;

  std::cout << "av_oid_dict insert " << n_large
	    << " in " << dur3 << std::endl;

  std::cout << "av_hoid_dict insert " << n_large
	    << " in " << dur4 << std::endl;
}

TEST(AV, FIND_LARGE)
{
  auto t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    (void) d1.find(vo_large[ix]);
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto dur1 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    (void) d2.find(vho_large[ix]);
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur2 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    (void) d3.find(vo_large[ix]);
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur3 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  t1 = std::chrono::high_resolution_clock::now();
  for (unsigned ix = 0; ix < n_large; ++ix) {
    (void) d4.find(vho_large[ix]);
  }
  t2 = std::chrono::high_resolution_clock::now();
  auto dur4 = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  std::cout << "map_oid_dict find " << n_large
	    << " in " << dur1 << std::endl;

  std::cout << "map_hoid_dict find " << n_large
	    << " in " << dur2 << std::endl;

  std::cout << "av_oid_dict find " << n_large
	    << " in " << dur3 << std::endl;

  std::cout << "av_hoid_dict find " << n_large
	    << " in " << dur4 << std::endl;
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
