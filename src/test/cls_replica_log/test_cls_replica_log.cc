// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 * Copyright 2013 Inktank
 */

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include "cls/replica_log/cls_replica_log_client.h"
#include "cls/replica_log/cls_replica_log_types.h"

class cls_replica_log_Test : public ::testing::Test {
public:
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name;
  string oid;
  string entity;
  string marker;
  utime_t time;
  list<pair<string, utime_t> > entries;
  cls_replica_log_progress_marker progress;

  void SetUp() {
    volume_name = get_temp_volume_name();
    ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));
    oid = "obj";
    ASSERT_EQ(0, ioctx.create(oid, true));
  }

  void add_marker() {
    entity = "tester_entity";
    marker = "tester_marker1";
    time.set_from_double(10);
    entries.push_back(make_pair("tester_obj1", time));
    time.set_from_double(20);
    cls_replica_log_prepare_marker(progress, entity, marker, time, &entries);
    librados::ObjectWriteOperation opw(ioctx);
    cls_replica_log_update_bound(opw, progress);
    ASSERT_EQ(0, ioctx.operate(oid, &opw));
  }
};

TEST_F(cls_replica_log_Test, test_set_get_marker)
{
  add_marker();

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(0, cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
					  reply_time, return_progress_list));

  ASSERT_EQ(reply_position_marker, marker);
  ASSERT_EQ((double)10, (double)reply_time);
  string response_entity;
  string response_marker;
  utime_t response_time;
  list<pair<string, utime_t> > response_item_list;

  cls_replica_log_extract_marker(return_progress_list.front(),
				 response_entity, response_marker,
				 response_time, response_item_list);
  ASSERT_EQ(response_entity, entity);
  ASSERT_EQ(response_marker, marker);
  ASSERT_EQ(response_time, time);
  ASSERT_EQ((unsigned)1, response_item_list.size());
  ASSERT_EQ("tester_obj1", response_item_list.front().first);
}

TEST_F(cls_replica_log_Test, test_bad_update)
{
  add_marker();

  time.set_from_double(15);
  cls_replica_log_progress_marker bad_marker;
  cls_replica_log_prepare_marker(bad_marker, entity, marker, time, &entries);
  librados::ObjectWriteOperation badw(ioctx);
  cls_replica_log_update_bound(badw, bad_marker);
  ASSERT_EQ(-EINVAL, ioctx.operate(oid, &badw));
}

TEST_F(cls_replica_log_Test, test_bad_delete)
{
  add_marker();

  librados::ObjectWriteOperation badd(ioctx);
  cls_replica_log_delete_bound(badd, entity);
  ASSERT_EQ(-ENOTEMPTY, ioctx.operate(oid, &badd));
}

TEST_F(cls_replica_log_Test, test_good_delete)
{
  add_marker();

  librados::ObjectWriteOperation opc(ioctx);
  progress.items.clear();
  cls_replica_log_update_bound(opc, progress);
  ASSERT_EQ(0, ioctx.operate(oid, &opc));
  librados::ObjectWriteOperation opd(ioctx);
  cls_replica_log_delete_bound(opd, entity);
  ASSERT_EQ(0, ioctx.operate(oid, &opd));

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(0, cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
					  reply_time, return_progress_list));
  ASSERT_EQ((unsigned)0, return_progress_list.size());
}

TEST_F(cls_replica_log_Test, test_bad_get)
{
  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(-ENOENT,
	    cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
				       reply_time, return_progress_list));
}

TEST_F(cls_replica_log_Test, test_double_delete)
{
  add_marker();

  librados::ObjectWriteOperation opc(ioctx);
  progress.items.clear();
  cls_replica_log_update_bound(opc, progress);
  ASSERT_EQ(0, ioctx.operate(oid, &opc));
  librados::ObjectWriteOperation opd(ioctx);
  cls_replica_log_delete_bound(opd, entity);
  ASSERT_EQ(0, ioctx.operate(oid, &opd));

  librados::ObjectWriteOperation opd2(ioctx);
  cls_replica_log_delete_bound(opd2, entity);
  ASSERT_EQ(0, ioctx.operate(oid, &opd2));

  string reply_position_marker;
  utime_t reply_time;
  list<cls_replica_log_progress_marker> return_progress_list;
  ASSERT_EQ(0, cls_replica_log_get_bounds(ioctx, oid, reply_position_marker,
					  reply_time, return_progress_list));
  ASSERT_EQ((unsigned)0, return_progress_list.size());

}
