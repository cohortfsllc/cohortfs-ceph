// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_RADOS_TESTCASE_H
#define CEPH_TEST_RADOS_TESTCASE_H

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "gtest/gtest.h"

#include <string>

class RadosTest : public ::testing::Test {
public:
  RadosTest() {}
  virtual ~RadosTest() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static rados_t s_cluster;
  static std::string volume_name;

  virtual void SetUp();
  virtual void TearDown();
  rados_t cluster;
  rados_ioctx_t ioctx;
};

class RadosTestPP : public ::testing::Test {
public:
  RadosTestPP() : cluster(s_cluster) {}
  virtual ~RadosTestPP() {}
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static librados::Rados s_cluster;
  static std::string volume_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
};

class RadosTestParamPP : public ::testing::TestWithParam<const char*> {
public:
  RadosTestParamPP() : cluster(s_cluster) {}
  virtual ~RadosTestParamPP() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
protected:
  static librados::Rados s_cluster;
  static std::string volume_name;
  static std::string cache_volume_name;

  virtual void SetUp();
  virtual void TearDown();
  librados::Rados &cluster;
  librados::IoCtx ioctx;
};

#endif
