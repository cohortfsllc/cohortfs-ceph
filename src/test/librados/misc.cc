// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "include/buffer.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

typedef RadosTest LibRadosMisc;
typedef RadosTestPP LibRadosMiscPP;
static CephContext* cct;

TEST(LibRadosMiscVersion, Version) {
  int major, minor, extra;
  rados_version(&major, &minor, &extra);
}

TEST(LibRadosMiscVersion, VersionPP) {
  int major, minor, extra;
  Rados::version(&major, &minor, &extra);
}

TEST_F(LibRadosMisc, ClusterFSID) {
  char fsid[37];
  ASSERT_EQ(-ERANGE, rados_cluster_fsid(cluster, fsid, sizeof(fsid) - 1));
  ASSERT_EQ(sizeof(fsid) - 1,
	    (size_t)rados_cluster_fsid(cluster, fsid, sizeof(fsid)));
}

TEST_F(LibRadosMiscPP, WaitOSDMapPP) {
  ASSERT_EQ(0, cluster.wait_for_latest_osdmap());
}

TEST_F(LibRadosMiscPP, Operate1PP) {
  ObjectWriteOperation o(ioctx);
  {
    bufferlist bl;
    o.write(0, bl);
  }
  std::string val1("val1");
  {
    bufferlist bl;
    bl.append(val1.c_str(), val1.size() + 1);
    o.setxattr("key1", bl);
    o.omap_clear(); // shouldn't affect attrs!
  }
  ASSERT_EQ(0, ioctx.operate("foo", &o));

  ObjectWriteOperation empty(ioctx);
  ASSERT_EQ(0, ioctx.operate("foo", &empty));

  {
    bufferlist bl;
    ASSERT_GT(ioctx.getxattr("foo", "key1", bl), 0);
    ASSERT_EQ(0, strcmp(bl.c_str(), val1.c_str()));
  }
  ObjectWriteOperation o2(ioctx);
  {
    bufferlist bl;
    bl.append(val1);
    o2.cmpxattr("key1", CEPH_OSD_CMPXATTR_OP_EQ, bl);
    o2.rmxattr("key1");
  }
  ASSERT_EQ(-ECANCELED, ioctx.operate("foo", &o2));
  ObjectWriteOperation o3(ioctx);
  {
    bufferlist bl;
    bl.append(val1);
    o3.cmpxattr("key1", CEPH_OSD_CMPXATTR_OP_EQ, bl);
  }
  ASSERT_EQ(-ECANCELED, ioctx.operate("foo", &o3));
}

TEST_F(LibRadosMiscPP, Operate2PP) {
  ObjectWriteOperation o(ioctx);
  {
    bufferlist bl;
    bl.append("abcdefg");
    o.write(0, bl);
  }
  std::string val1("val1");
  {
    bufferlist bl;
    bl.append(val1.c_str(), val1.size() + 1);
    o.setxattr("key1", bl);
    o.truncate(0);
  }
  ASSERT_EQ(0, ioctx.operate("foo", &o));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(0U, size);
}

TEST_F(LibRadosMiscPP, BigObjectPP) {
  bufferlist bl;
  bl.append("abcdefg");
  ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));

  {
    ObjectWriteOperation o(ioctx);
    o.truncate(500000000000ull);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }
  {
    ObjectWriteOperation o(ioctx);
    o.zero(500000000000ull, 1);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }
  {
    ObjectWriteOperation o(ioctx);
    o.zero(1, 500000000000ull);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }
  {
    ObjectWriteOperation o(ioctx);
    o.zero(500000000000ull, 500000000000ull);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }

#ifdef __LP64__
  // this test only works on 64-bit platforms
  ASSERT_EQ(-EFBIG, ioctx.write("foo", bl, bl.length(), 500000000000ull));
#endif
}

void set_completion_complete(rados_completion_t cb, void *arg)
{
  bool *my_aio_complete = (bool*)arg;
  *my_aio_complete = true;
}

TEST_F(LibRadosMiscPP, AioOperatePP) {
  bool my_aio_complete = false;
  AioCompletion *my_completion = cluster.aio_create_completion(
	  (void*)&my_aio_complete, set_completion_complete, NULL);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);

  ObjectWriteOperation o(ioctx);
  {
    bufferlist bl;
    o.write(0, bl);
  }
  std::string val1("val1");
  {
    bufferlist bl;
    bl.append(val1.c_str(), val1.size() + 1);
    o.setxattr("key1", bl);
    bufferlist bl2;
    char buf2[1024];
    memset(buf2, 0xdd, sizeof(buf2));
    bl2.append(buf2, sizeof(buf2));
    o.append(bl2);
  }
  ASSERT_EQ(0, ioctx.aio_operate("foo", my_completion, &o));
  ASSERT_EQ(0, my_completion->wait_for_complete_and_cb());
  ASSERT_EQ(my_aio_complete, true);

  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(1024U, size);
}

TEST_F(LibRadosMiscPP, AssertExistsPP) {
  char buf[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  ObjectWriteOperation op(ioctx);
  op.assert_exists();
  op.write(0, bl);
  ASSERT_EQ(-ENOENT, ioctx.operate("asdffoo", &op));
  ASSERT_EQ(0, ioctx.create("asdffoo", true));
  ASSERT_EQ(0, ioctx.operate("asdffoo", &op));
  ASSERT_EQ(-EEXIST, ioctx.create("asdffoo", true));
}

TEST_F(LibRadosMiscPP, BigAttrPP) {
  char buf[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  ASSERT_EQ(0, ioctx.create("foo", true));

  bufferlist got;

  std::cout << "osd_max_attr_size = " << cct->_conf->osd_max_attr_size
	    << std::endl;
  if (cct->_conf->osd_max_attr_size) {
    bl.clear();
    got.clear();
    bl.append(buffer::create(cct->_conf->osd_max_attr_size));
    ASSERT_EQ(0, ioctx.setxattr("foo", "one", bl));
    ASSERT_EQ((int)bl.length(), ioctx.getxattr("foo", "one", got));
    ASSERT_TRUE(bl.contents_equal(got));

    bl.clear();
    bl.append(buffer::create(cct->_conf->osd_max_attr_size+1));
    ASSERT_EQ(-EFBIG, ioctx.setxattr("foo", "one", bl));
  } else {
    std::cout << "osd_max_attr_size == 0; skipping test" << std::endl;
  }

  for (int i=0; i<1000; i++) {
    bl.clear();
    got.clear();
    bl.append(buffer::create(MIN(cct->_conf->osd_max_attr_size, 1024)));
    char n[10];
    snprintf(n, sizeof(n), "a%d", i);
    ASSERT_EQ(0, ioctx.setxattr("foo", n, bl));
    ASSERT_EQ((int)bl.length(), ioctx.getxattr("foo", n, got));
    ASSERT_TRUE(bl.contents_equal(got));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
		    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);

  return RUN_ALL_TESTS();
}
