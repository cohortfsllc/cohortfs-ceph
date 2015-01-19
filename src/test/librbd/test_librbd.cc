// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include <iostream>
#include <algorithm>
#include <sstream>

#include "librados/RadosClient.h"

#include "global/global_init.h"
#include "common/ceph_argparse.h"

#include "gtest/gtest.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "test/librados/test.h"
#include "common/errno.h"
#include "include/interval_set.h"
#include "include/stringify.h"
#include "librbd/Image.h"

using namespace std;
using namespace librados;
using namespace librbd;
static CephContext* cct;

TEST(LibRBD, CreateAndStat)
{
  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  librados::RadosClient rados(cct);
  rados.connect();
  string volume_name = get_temp_volume_name();
  ASSERT_EQ(0, rados.vol_create(volume_name));
  VolumeRef v = rados.lookup_volume(volume_name);
  ASSERT_TRUE(!!v);

  const string name = "testimg";
  const uint64_t demanded_size = 2 << 20;
  uint64_t actual_size = 0;

  ASSERT_NO_THROW(Image::create(&rados, v, name, demanded_size));
  librbd::Image* image;
  ASSERT_NO_THROW(image = new Image(&rados, v, name));
  ASSERT_NO_THROW(actual_size = image->get_size());
  std::cout << "image has size " << actual_size << "." << std::endl;
  ASSERT_EQ(demanded_size, actual_size);
  delete image;
  ASSERT_NO_THROW(Image::remove(&rados, v, name));
  ASSERT_EQ(0, rados.vol_delete(volume_name));
  rados.shutdown();
  common_cleanup(cct);
}

TEST(LibRBD, ResizeAndStat)
{
  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  librados::RadosClient rados(cct);
  rados.connect();
  string volume_name = get_temp_volume_name();
  ASSERT_EQ(0, rados.vol_create(volume_name));
  VolumeRef v = rados.lookup_volume(volume_name);
  ASSERT_TRUE(!!v);

  const string name = "testimg";
  const uint64_t demanded_size = 2 << 20;
  uint64_t actual_size;

  ASSERT_NO_THROW(Image::create(&rados, v, name, demanded_size));
  librbd::Image* image;
  ASSERT_NO_THROW(image = new Image(&rados, v, name));
  ASSERT_NO_THROW(image->resize(demanded_size * 4));
  ASSERT_NO_THROW(actual_size = image->get_size());
  ASSERT_EQ(actual_size, demanded_size * 4);

  ASSERT_NO_THROW(image->resize(demanded_size / 2));
  ASSERT_NO_THROW(actual_size = image->get_size());
  ASSERT_EQ(actual_size, demanded_size / 2);

  delete image;
  ASSERT_NO_THROW(Image::remove(&rados, v, name));
  ASSERT_EQ(0, rados.vol_delete(volume_name));
  rados.shutdown();
  common_cleanup(cct);
}

static constexpr size_t TEST_IO_SIZE = 512;

struct Simple_Read : public Context {
  int *ret;

  Simple_Read(int* _ret) : ret(_ret) { };
  void finish(int r) {
    if (ret)
      *ret = r;
    std::cout << "read completion cb called!\n";
  }
};

void aio_write_test_data(Image& image, const bufferlist& test_data,
			 uint64_t off, size_t len)
{
  int r;
  printf("created completion\n");
  image.write(off, len, test_data, nullptr,
	      [&r](int _r) {
		r = _r;
		std::cout << "write completion cb called!\n";
	      });

  printf("started write\n");
  image.flush();
  printf("return value is: %d\n", r);
  assert(r == 0);
}

void write_test_data(Image& image, const bufferlist& test_data,
		     uint64_t off, size_t len)
{
  image.write_sync(off, len, test_data);
}

void aio_discard_test_data(Image& image, uint64_t off, uint64_t len)
{
  image.discard(off, len);
  image.flush();
}

void discard_test_data(Image& image, uint64_t off, size_t len)
{
  image.discard_sync(off, len);
}

void aio_read_test_data(Image& image, const bufferlist& expected,
			uint64_t off, size_t len)
{
  bufferlist bl;

  cout << "started read" << std::endl;
  image.read(off, len, &bl);
  image.flush();
  assert(bl.length() == len);
  assert(bl == expected);
  cout << "read: %s\nexpected: " << bl << std::endl;
}

void read_test_data(const Image& image, const bufferlist& expected,
		    uint64_t off, size_t len)
{
  bufferlist bl;

  image.read_sync(off, len, &bl);
  cout << "read: " << bl.length() << std::endl;
  assert(bl.length() == len);
  assert(bl == expected);
  cout << "read: " << bl << std::endl
       << "expected: " << expected << std::endl;
}

TEST(LibRBD, TestIO)
{
  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  librados::RadosClient rados(cct);
  rados.connect();
  string volume_name = get_temp_volume_name();
  ASSERT_EQ(0, rados.vol_create(volume_name));
  VolumeRef v = rados.lookup_volume(volume_name);
  ASSERT_TRUE(!!v);

  const string& name = "testimg";
  uint64_t size = 2 << 20;

  Image* image;
  ASSERT_NO_THROW(Image::create(&rados, v, name, size));
  ASSERT_NO_THROW(image = new Image(&rados, v, name));

  bufferlist test_data(TEST_IO_SIZE);
  bufferlist zero_data(TEST_IO_SIZE);
  size_t i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data.append((char) (rand() % (126 - 33) + 33));
  }
  zero_data.append_zero(TEST_IO_SIZE);

  for (i = 0; i < 5; ++i)
    write_test_data(*image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_write_test_data(*image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 0; i < 5; ++i)
    read_test_data(*image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_read_test_data(*image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  // discard 2nd, 4th sections.
  discard_test_data(*image, TEST_IO_SIZE, TEST_IO_SIZE);
  aio_discard_test_data(*image, TEST_IO_SIZE*3, TEST_IO_SIZE);

  read_test_data(*image, test_data, 0, TEST_IO_SIZE);
  read_test_data(*image, zero_data, TEST_IO_SIZE, TEST_IO_SIZE);
  read_test_data(*image, test_data, TEST_IO_SIZE * 2, TEST_IO_SIZE);
  read_test_data(*image, zero_data, TEST_IO_SIZE * 3, TEST_IO_SIZE);
  read_test_data(*image, test_data, TEST_IO_SIZE * 4, TEST_IO_SIZE);

  // can't read or write starting past end
  ASSERT_NO_THROW(size = image->get_size());
  ASSERT_THROW(image->write_sync(size, 1, test_data), std::error_condition);
  ASSERT_THROW(image->read_sync(size, 1, &test_data), std::error_condition);
  // reading through end returns amount up to end
  //ASSERT_EQ(10, rbd_read(image, info.size - 10, 100, test_data));
  // writing through end returns amount up to end
  //ASSERT_EQ(10, rbd_write(image, info.size - 10, 100, test_data));

  //ASSERT_EQ(-EINVAL, rbd_aio_write(image, info.size, 1, test_data, comp));
  //ASSERT_EQ(-EINVAL, rbd_aio_read(image, info.size, 1, test_data, comp));

  delete image;
  ASSERT_NO_THROW(Image::remove(&rados, v, name));
  ASSERT_EQ(0, rados.vol_delete(volume_name));
  rados.shutdown();
  common_cleanup(cct);
}

TEST(LibRBD, TestEmptyDiscard)
{
  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  librados::RadosClient rados(cct);
  rados.connect();
  string volume_name = get_temp_volume_name();
  ASSERT_EQ(0, rados.vol_create(volume_name));
  VolumeRef v = rados.lookup_volume(volume_name);
  ASSERT_TRUE(!!v);

  Image* image = nullptr;
  const char *name = "testimg";
  uint64_t size = 20 << 20;

  ASSERT_NO_THROW(Image::create(&rados, v, name, size));
  ASSERT_NO_THROW(image = new Image(&rados, v, name));

  aio_discard_test_data(*image, 0, 1*1024*1024);
  aio_discard_test_data(*image, 0, 4*1024*1024);

  delete image;
  ASSERT_NO_THROW(Image::remove(&rados, v, name));
  ASSERT_EQ(0, rados.vol_delete(volume_name));
  rados.shutdown();
  common_cleanup(cct);
}

TEST(LibRBD, FlushAio)
{
  CephContext *cct = test_init(CODE_ENVIRONMENT_UTILITY);
  librados::RadosClient rados(cct);
  rados.connect();
  string volume_name = get_temp_volume_name();
  ASSERT_EQ(0, rados.vol_create(volume_name));
  VolumeRef v = rados.lookup_volume(volume_name);
  ASSERT_TRUE(!!v);

  bufferlist test_data(TEST_IO_SIZE);
  size_t i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data.append((char) (rand() % (126 - 33) + 33));
  }

  const string& name = "testimg";
  uint64_t size = 20 << 20;
  ASSERT_NO_THROW(Image::create(&rados, v, name, size));
  {
    Image image;
    uint64_t size = 2 << 20;
    size_t num_aios = 256;
    ASSERT_NO_THROW(image = Image(&rados, v, name));

    for (i = 0; i < num_aios; ++i) {
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_NO_THROW(image.write(offset, TEST_IO_SIZE, test_data));
    }
    ASSERT_NO_THROW(image.flush());
  }

  ASSERT_NO_THROW(Image::remove(&rados, v, name));
  ASSERT_EQ(0, rados.vol_delete(volume_name));
  rados.shutdown();
  common_cleanup(cct);
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
