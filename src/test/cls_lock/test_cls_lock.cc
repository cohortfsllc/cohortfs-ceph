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
 * Foundation.	See file COPYING.
 *
 */

#include <thread>
#include <iostream>
#include <errno.h>

#include "include/types.h"
#include "msg/msg_types.h"
#include "osdc/RadosClient.h"

#include "test/librados/test.h"
#include "gtest/gtest.h"


#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_ops.h"

using std::cout;
using namespace rados;
using namespace rados::cls::lock;

void lock_info(Objecter* o, const VolumeRef& vol, const oid_t& oid,
	       string& name, map<locker_id_t, locker_info_t>& lockers,
	       ClsLockType *assert_type, string *assert_tag)
{
  ClsLockType lock_type = LOCK_NONE;
  string tag;
  lockers.clear();
  ASSERT_EQ(0, get_lock_info(ioctx, oid_t, name, &lockers, &lock_type, &tag));
  cout << "lock: " << name << std::endl;
  cout << "  lock_type: " << cls_lock_type_str(lock_type) << std::endl;
  cout << "  tag: " << tag << std::endl;
  cout << "  lockers:" << std::endl;

  if (assert_type)
    ASSERT_EQ(*assert_type, lock_type);

  if (assert_tag)
    ASSERT_EQ(*assert_tag, tag);

  map<locker_id_t, locker_info_t>::iterator liter;
  for (liter = lockers.begin(); liter != lockers.end(); ++liter) {
    const locker_id_t& locker = liter->first;
    cout << "	 " << locker.locker << " expiration=" << liter->second.expiration
	 << " addr=" << liter->second.addr << " cookie=" << locker.cookie << std::endl;
  }
}

void lock_info(Objecter* o, const VolumeRef& vol, const oid_t oid,
	       string& name, map<locker_id_t, locker_info_t>& lockers)
{
  lock_info(o, vol, oid, name, lockers, NULL, NULL);
}

TEST(ClsLock, TestMultiLocking) {
  RadosClient cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(volume_name.c_str(), ioctx);
  ClsLockType lock_type_shared = LOCK_SHARED;
  ClsLockType lock_type_exclusive = LOCK_EXCLUSIVE;


  Rados cluster2;
  IoCtx ioctx2;
  ASSERT_EQ("", connect_cluster_pp(cluster2));
  cluster2.ioctx_create(volume_name.c_str(), ioctx2);

  string oid_t = "foo";
  bufferlist bl;
  string lock_name = "mylock";

  ASSERT_EQ(0, ioctx.write(oid_t, bl, bl.length(), 0));

  Lock l(lock_name);

  /* test lock object */

  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));

  /* test exclusive lock */
  ASSERT_EQ(-EEXIST, l.lock_exclusive(&ioctx, oid_t));

  /* test idempotency */
  l.set_renew(true);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));

  l.set_renew(false);

  /* test second client */
  Lock l2(lock_name);
  ASSERT_EQ(-EBUSY, l2.lock_exclusive(&ioctx2, oid_t));
  ASSERT_EQ(-EBUSY, l2.lock_shared(&ioctx2, oid_t));

  list<string> locks;
  ASSERT_EQ(0, list_locks(&ioctx, oid_t, &locks));

  ASSERT_EQ(1, (int)locks.size());
  list<string>::iterator iter = locks.begin();
  map<locker_id_t, locker_info_t> lockers;
  lock_info(&ioctx, oid_t, *iter, lockers, &lock_type_exclusive, NULL);

  ASSERT_EQ(1, (int)lockers.size());

  /* test unlock */
  ASSERT_EQ(0, l.unlock(&ioctx, oid_t));
  locks.clear();
  ASSERT_EQ(0, list_locks(&ioctx, oid_t, &locks));

  /* test shared lock */
  ASSERT_EQ(0, l2.lock_shared(&ioctx2, oid_t));
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid_t));

  locks.clear();
  ASSERT_EQ(0, list_locks(&ioctx, oid_t, &locks));
  ASSERT_EQ(1, (int)locks.size());
  iter = locks.begin();
  lock_info(&ioctx, oid_t, *iter, lockers, &lock_type_shared, NULL);
  ASSERT_EQ(2, (int)lockers.size());

  /* test break locks */
  entity_name_t name = entity_name_t::CLIENT(cluster.get_instance_id());
  entity_name_t name2 = entity_name_t::CLIENT(cluster2.get_instance_id());

  l2.break_lock(&ioctx2, oid_t, name);
  lock_info(&ioctx, oid_t, *iter, lockers);
  ASSERT_EQ(1, (int)lockers.size());
  map<locker_id_t, locker_info_t>::iterator liter = lockers.begin();
  const locker_id_t& id = liter->first;
  ASSERT_EQ(name2, id.locker);

  /* test lock tag */
  Lock l_tag(lock_name);
  l_tag.set_tag("non-default tag");
  ASSERT_EQ(-EBUSY, l_tag.lock_shared(&ioctx, oid_t));


  /* test modify description */
  string description = "new description";
  l.set_description(description);
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid_t));

  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, cluster));
}

TEST(ClsLock, TestMeta) {
  Rados cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(volume_name.c_str(), ioctx);


  Rados cluster2;
  IoCtx ioctx2;
  ASSERT_EQ("", connect_cluster_pp(cluster2));
  cluster2.ioctx_create(volume_name.c_str(), ioctx2);

  string oid_t = "foo";
  bufferlist bl;
  string lock_name = "mylock";

  ASSERT_EQ(0, ioctx.write(oid_t, bl, bl.length(), 0));

  Lock l(lock_name);
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid_t));

  /* test lock tag */
  Lock l_tag(lock_name);
  l_tag.set_tag("non-default tag");
  ASSERT_EQ(-EBUSY, l_tag.lock_shared(&ioctx2, oid_t));


  ASSERT_EQ(0, l.unlock(&ioctx, oid_t));

  /* test description */
  Lock l2(lock_name);
  string description = "new description";
  l2.set_description(description);
  ASSERT_EQ(0, l2.lock_shared(&ioctx2, oid_t));

  map<locker_id_t, locker_info_t> lockers;
  lock_info(&ioctx, oid_t, lock_name, lockers, NULL, NULL);
  ASSERT_EQ(1, (int)lockers.size());

  map<locker_id_t, locker_info_t>::iterator iter = lockers.begin();
  locker_info_t locker = iter->second;
  ASSERT_EQ("new description", locker.description);

  ASSERT_EQ(0, l2.unlock(&ioctx2, oid_t));

  /* check new tag */
  string new_tag = "new_tag";
  l.set_tag(new_tag);
  l.set_renew(true);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));
  lock_info(&ioctx, oid_t, lock_name, lockers, NULL, &new_tag);
  ASSERT_EQ(1, (int)lockers.size());
  l.set_tag("");
  ASSERT_EQ(-EBUSY, l.lock_exclusive(&ioctx, oid_t));
  l.set_tag(new_tag);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));

  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, cluster));
}

TEST(ClsLock, TestCookie) {
  Rados cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(volume_name.c_str(), ioctx);

  string oid_t = "foo";
  string lock_name = "mylock";
  Lock l(lock_name);

  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));

  /* new cookie */
  string cookie = "new cookie";
  l.set_cookie(cookie);
  ASSERT_EQ(-EBUSY, l.lock_exclusive(&ioctx, oid_t));
  ASSERT_EQ(-ENOENT, l.unlock(&ioctx, oid_t));
  l.set_cookie("");
  ASSERT_EQ(0, l.unlock(&ioctx, oid_t));

  map<locker_id_t, locker_info_t> lockers;
  lock_info(&ioctx, oid_t, lock_name, lockers);
  ASSERT_EQ(0, (int)lockers.size());

  l.set_cookie(cookie);
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid_t));
  l.set_cookie("");
  ASSERT_EQ(0, l.lock_shared(&ioctx, oid_t));

  lock_info(&ioctx, oid_t, lock_name, lockers);
  ASSERT_EQ(2, (int)lockers.size());

  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, cluster));
}

TEST(ClsLock, TestMultipleLocks) {
  Rados cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(volume_name.c_str(), ioctx);

  string oid_t = "foo";
  Lock l("lock1");
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));

  Lock l2("lock2");
  ASSERT_EQ(0, l2.lock_exclusive(&ioctx, oid_t));

  list<string> locks;
  ASSERT_EQ(0, list_locks(&ioctx, oid_t, &locks));

  ASSERT_EQ(2, (int)locks.size());

  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, cluster));
}

TEST(ClsLock, TestLockDuration) {
  Rados cluster;
  std::string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume_pp(volume_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(volume_name.c_str(), ioctx);

  string oid_t = "foo";
  Lock l("lock");
  ceph::timespan dur = 5s;
  l.set_duration(dur);
  auto start = ceph::real_clock::now();
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));
  int r = l.lock_exclusive(&ioctx, oid_t);
  if (r == 0) {
    // it's possible to get success if we were just really slow...
    ASSERT_TRUE(ceph::real_clock::now() > start + dur);
  } else {
    ASSERT_EQ(-EEXIST, r);
  }

  std::this_thread::sleep_for(dur);
  ASSERT_EQ(0, l.lock_exclusive(&ioctx, oid_t));

  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, cluster));
}
