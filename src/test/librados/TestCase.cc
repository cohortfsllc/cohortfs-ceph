// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

using namespace librados;

std::string RadosTest::volume_name;
rados_t RadosTest::s_cluster = NULL;

void RadosTest::SetUpTestCase()
{
  volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &s_cluster));
}

void RadosTest::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_volume(volume_name, &s_cluster));
}

void RadosTest::SetUp()
{
  cluster = RadosTest::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, volume_name.c_str(), &ioctx));
}

void RadosTest::TearDown()
{
  rados_ioctx_destroy(ioctx);
}

std::string RadosTestPP::volume_name;
Rados RadosTestPP::s_cluster;

void RadosTestPP::SetUpTestCase()
{
  volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, s_cluster));
}

void RadosTestPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, s_cluster));
}

void RadosTestPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(volume_name.c_str(), ioctx));
}

void RadosTestPP::TearDown()
{
  ioctx.close();
}

std::string RadosTestParamPP::volume_name;
std::string RadosTestParamPP::cache_volume_name;
Rados RadosTestParamPP::s_cluster;

void RadosTestParamPP::SetUpTestCase()
{
  volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, s_cluster));
}

void RadosTestParamPP::TearDownTestCase()
{
  if (cache_volume_name.length()) {
    // tear down tiers
    bufferlist inbl;
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"volume\": \"" + volume_name +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"volume\": \"" + volume_name +
      "\", \"tiervolume\": \"" + cache_volume_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd volume delete\", \"volume\": \"" + cache_volume_name +
      "\", \"volume2\": \"" + cache_volume_name + "\", \"sure\": \"--yes-i-really-really-mean-it\"}",
      inbl, NULL, NULL));
    cache_volume_name = "";
  }
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, s_cluster));
}

void RadosTestParamPP::SetUp()
{
  if (strcmp(GetParam(), "cache") == 0 && cache_volume_name.empty()) {
    cache_volume_name = get_temp_volume_name();
    bufferlist inbl;
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd volume create\", \"volume\": \"" + cache_volume_name +
      "\", \"pg_num\": 4}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier add\", \"volume\": \"" + volume_name +
      "\", \"tiervolume\": \"" + cache_volume_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"volume\": \"" + volume_name +
      "\", \"overlayvolume\": \"" + cache_volume_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"volume\": \"" + cache_volume_name +
      "\", \"mode\": \"writeback\"}",
      inbl, NULL, NULL));
    cluster.wait_for_latest_osdmap();
  }

  ASSERT_EQ(0, cluster.ioctx_create(volume_name.c_str(), ioctx));
}

void RadosTestParamPP::TearDown()
{
  ioctx.close();
}
