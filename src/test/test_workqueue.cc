// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/ceph_context.h"

using std::cout;

CephContext *cct;
//conf = cct->conf;

TEST(WorkQueue, StartStop)
{
  ThreadPool tp(cct, "foo", 10, "");

  tp.start();
  tp.pause();
  tp.pause_new();
  tp.unpause();
  tp.unpause();
  tp.drain();
  tp.stop();
}

TEST(WorkQueue, Resize)
{
  ThreadPool tp(cct, "bar", 2, "osd_op_threads");

  tp.start();

  sleep(1);
  ASSERT_EQ(2, tp.get_num_threads());

  cct->_conf->set_val("osd op threads", "5");
  cct->_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(5, tp.get_num_threads());

  cct->_conf->set_val("osd op threads", "3");
  cct->_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(3, tp.get_num_threads());

  cct->_conf->set_val("osd op threads", "15");
  cct->_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  cct->_conf->set_val("osd op threads", "0");
  cct->_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  cct->_conf->set_val("osd op threads", "-1");
  cct->_conf->apply_changes(&cout);
  sleep(1);
  ASSERT_EQ(15, tp.get_num_threads());

  sleep(1);
  tp.stop();
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  return RUN_ALL_TESTS();
}
