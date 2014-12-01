// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include "include/rados/librados.h"
#include "include/rbd_types.h"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"

#include "global/global_context.h"
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
#include <iostream>
#include <algorithm>
#include <sstream>

#include "test/librados/test.h"
#include "common/errno.h"
#include "include/interval_set.h"
#include "include/stringify.h"

using namespace std;

static int create_image(rados_ioctx_t ioctx, const char *name, uint64_t size)
{
  return rbd_create(ioctx, name, size);
}

static int create_image_pp(librbd::RBD &rbd,
			   librados::IoCtx &ioctx,
			   const char *name,
			   uint64_t size)  {
  return rbd.create(ioctx, name, size);
}

TEST(LibRBD, CreateAndStat)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  ASSERT_EQ(0, rados_ioctx_create(cluster, volume_name.c_str(), &ioctx));

  rbd_image_info_t info;
  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  printf("image has size %" PRIu64 ".\n", info.size);
  ASSERT_EQ(info.size, size);
  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRBD, CreateAndStatPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::image_info_t info;
    librbd::Image image;
    const char *name = "testimg";
    uint64_t size = 2 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(0, rbd.open(ioctx, image, name));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}

TEST(LibRBD, ResizeAndStat)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_info_t info;
  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  ASSERT_EQ(0, rbd_resize(image, size * 4));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size * 4);

  ASSERT_EQ(0, rbd_resize(image, size / 2));
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  ASSERT_EQ(info.size, size / 2);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRBD, ResizeAndStatPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::image_info_t info;
    librbd::Image image;
    const char *name = "testimg";
    uint64_t size = 2 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(0, rbd.open(ioctx, image, name));

    ASSERT_EQ(0, image.resize(size * 4));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size * 4);

    ASSERT_EQ(0, image.resize(size / 2));
    ASSERT_EQ(0, image.stat(info, sizeof(info)));
    ASSERT_EQ(info.size, size / 2);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}

int test_ls(rados_ioctx_t io_ctx, size_t num_expected, ...)
{
#if 0
  int num_images, i, j;
  char *names, *cur_name;
  va_list ap;
  size_t max_size = 1024;

  names = (char *) malloc(sizeof(char *) * 1024);
  int len = rbd_list(io_ctx, names, &max_size);

  for (i = 0, num_images = 0, cur_name = names; cur_name < names + len; i++) {
    printf("image: %s\n", cur_name);
    cur_name += strlen(cur_name) + 1;
    num_images++;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    printf("expected = %s\n", expected);
    int found = 0;
    for (j = 0, cur_name = names; j < num_images; j++) {
      if (cur_name[0] == '_') {
	cur_name += strlen(cur_name) + 1;
	continue;
      }
      if (strcmp(cur_name, expected) == 0) {
	printf("found %s\n", cur_name);
	cur_name[0] = '_';
	found = 1;
	break;
      }
    }
    assert(found);
  }
  va_end(ap);

  for (i = 0, cur_name = names; cur_name < names + len; i++) {
    assert(cur_name[0] == '_');
    cur_name += strlen(cur_name) + 1;
  }
  free(names);

  return num_images;
#endif
  return 0;
}

TEST(LibRBD, TestCreateLsDelete)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  const char *name = "testimg";
  const char *name2 = "testimg2";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(1, test_ls(ioctx, 1, name));
  ASSERT_EQ(0, create_image(ioctx, name2, size));
  ASSERT_EQ(2, test_ls(ioctx, 2, name, name2));
  ASSERT_EQ(0, rbd_remove(ioctx, name));
  ASSERT_EQ(1, test_ls(ioctx, 1, name2));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

int test_ls_pp(librbd::RBD& rbd, librados::IoCtx& io_ctx, size_t num_expected, ...)
{
#if 0
  int r;
  size_t i;
  va_list ap;
  vector<string> names;
  r = rbd.list(io_ctx, names);
  if (r == -ENOENT)
    r = 0;
  assert(r >= 0);
  cout << "num images is: " << names.size() << endl
       << "expected: " << num_expected << endl;
  int num = names.size();

  for (i = 0; i < names.size(); i++) {
    cout << "image: " << names[i] << endl;
  }

  va_start(ap, num_expected);
  for (i = num_expected; i > 0; i--) {
    char *expected = va_arg(ap, char *);
    cout << "expected = " << expected << endl;
    vector<string>::iterator listed_name = find(names.begin(), names.end(), string(expected));
    assert(listed_name != names.end());
    names.erase(listed_name);
  }
  va_end(ap);

  assert(names.empty());

  return num;
#endif
  return 0;
}

TEST(LibRBD, TestCreateLsDeletePP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    const char *name = "testimg";
    const char *name2 = "testimg2";
    uint64_t size = 2 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name));
    ASSERT_EQ(0, rbd.create(ioctx, name2, size));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name, name2));
    ASSERT_EQ(0, rbd.remove(ioctx, name));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name2));
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}


static int print_progress_percent(uint64_t offset, uint64_t src_size,
				     void *data)
{
  float percent = ((float)offset * 100) / src_size;
  printf("%3.2f%% done\n", percent);
  return 0;
}

TEST(LibRBD, TestCopy)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  const char *name2 = "testimg2";
  const char *name3 = "testimg3";

  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));
  ASSERT_EQ(1, test_ls(ioctx, 1, name));
  ASSERT_EQ(0, rbd_copy(image, ioctx, name2));
  ASSERT_EQ(2, test_ls(ioctx, 2, name, name2));
  ASSERT_EQ(0, rbd_copy_with_progress(image, ioctx, name3, print_progress_percent, NULL));
  ASSERT_EQ(3, test_ls(ioctx, 3, name, name2, name3));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

class PrintProgress : public librbd::ProgressContext
{
public:
  int update_progress(uint64_t offset, uint64_t src_size)
  {
    float percent = ((float)offset * 100) / src_size;
    printf("%3.2f%% done\n", percent);
    return 0;
  }
};

TEST(LibRBD, TestCopyPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    const char *name = "testimg";
    const char *name2 = "testimg2";
    const char *name3 = "testimg3";
    uint64_t size = 2 << 20;
    PrintProgress pp;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(0, rbd.open(ioctx, image, name));
    ASSERT_EQ(1, test_ls_pp(rbd, ioctx, 1, name));
    ASSERT_EQ(0, image.copy(ioctx, name2));
    ASSERT_EQ(2, test_ls_pp(rbd, ioctx, 2, name, name2));
    ASSERT_EQ(0, image.copy_with_progress(ioctx, name3, pp));
    ASSERT_EQ(3, test_ls_pp(rbd, ioctx, 3, name, name2, name3));
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}

#define TEST_IO_SIZE 512

void simple_write_cb(rbd_completion_t cb, void *arg)
{
  printf("write completion cb called!\n");
}

void simple_read_cb(rbd_completion_t cb, void *arg)
{
  printf("read completion cb called!\n");
}

void aio_write_test_data(rbd_image_t image, const char *test_data, uint64_t off, size_t len)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  printf("created completion\n");
  rbd_aio_write(image, off, len, test_data, comp);
  printf("started write\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == 0);
  printf("finished write\n");
  rbd_aio_release(comp);
}

void write_test_data(rbd_image_t image, const char *test_data, uint64_t off, size_t len)
{
  ssize_t written;
  written = rbd_write(image, off, len, test_data);
  printf("wrote: %d\n", (int) written);
  assert(written == (ssize_t)len);
}

void aio_discard_test_data(rbd_image_t image, uint64_t off, uint64_t len)
{
  rbd_completion_t comp;
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_write_cb, &comp);
  rbd_aio_discard(image, off, len, comp);
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  assert(r == 0);
  printf("aio discard: %d~%d = %d\n", (int)off, (int)len, (int)r);
  rbd_aio_release(comp);
}

void discard_test_data(rbd_image_t image, uint64_t off, size_t len)
{
  ssize_t written;
  written = rbd_discard(image, off, len);
  printf("discard: %d~%d = %d\n", (int)off, (int)len, (int)written);
  assert(written == (ssize_t)len);
}

void aio_read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len)
{
  rbd_completion_t comp;
  char *result = (char *)malloc(len + 1);

  assert(result);
  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  printf("created completion\n");
  rbd_aio_read(image, off, len, result, comp);
  printf("started read\n");
  rbd_aio_wait_for_complete(comp);
  int r = rbd_aio_get_return_value(comp);
  printf("return value is: %d\n", r);
  assert(r == (ssize_t)len);
  rbd_aio_release(comp);
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    assert(memcmp(result, expected, len) == 0);
  }
  free(result);
}

void read_test_data(rbd_image_t image, const char *expected, uint64_t off, size_t len)
{
  ssize_t read;
  char *result = (char *)malloc(len + 1);

  assert(result);
  read = rbd_read(image, off, len, result);
  printf("read: %d\n", (int) read);
  assert(read == (ssize_t)len);
  result[len] = '\0';
  if (memcmp(result, expected, len)) {
    printf("read: %s\nexpected: %s\n", result, expected);
    assert(memcmp(result, expected, len) == 0);
  }
  free(result);
}

TEST(LibRBD, TestIO)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  char test_data[TEST_IO_SIZE + 1];
  char zero_data[TEST_IO_SIZE + 1];
  int i;

  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }
  test_data[TEST_IO_SIZE] = '\0';
  memset(zero_data, 0, sizeof(zero_data));

  for (i = 0; i < 5; ++i)
    write_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_write_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 0; i < 5; ++i)
    read_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  for (i = 5; i < 10; ++i)
    aio_read_test_data(image, test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);

  // discard 2nd, 4th sections.
  discard_test_data(image, TEST_IO_SIZE, TEST_IO_SIZE);
  aio_discard_test_data(image, TEST_IO_SIZE*3, TEST_IO_SIZE);

  read_test_data(image, test_data,  0, TEST_IO_SIZE);
  read_test_data(image,	 zero_data, TEST_IO_SIZE, TEST_IO_SIZE);
  read_test_data(image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE);
  read_test_data(image,	 zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE);
  read_test_data(image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE);

  rbd_image_info_t info;
  rbd_completion_t comp;
  ASSERT_EQ(0, rbd_stat(image, &info, sizeof(info)));
  // can't read or write starting past end
  ASSERT_EQ(-EINVAL, rbd_write(image, info.size, 1, test_data));
  ASSERT_EQ(-EINVAL, rbd_read(image, info.size, 1, test_data));
  // reading through end returns amount up to end
  ASSERT_EQ(10, rbd_read(image, info.size - 10, 100, test_data));
  // writing through end returns amount up to end
  ASSERT_EQ(10, rbd_write(image, info.size - 10, 100, test_data));

  rbd_aio_create_completion(NULL, (rbd_callback_t) simple_read_cb, &comp);
  ASSERT_EQ(-EINVAL, rbd_aio_write(image, info.size, 1, test_data, comp));
  ASSERT_EQ(-EINVAL, rbd_aio_read(image, info.size, 1, test_data, comp));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRBD, TestEmptyDiscard)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 20 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  aio_discard_test_data(image, 0, 1*1024*1024);
  aio_discard_test_data(image, 0, 4*1024*1024);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}


void simple_write_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "write completion cb called!" << std::endl;
}

void simple_read_cb_pp(librbd::completion_t cb, void *arg)
{
  cout << "read completion cb called!" << std::endl;
}

void aio_write_test_data(librbd::Image& image, const char *test_data, off_t off)
{
  ceph::bufferlist bl;
  bl.append(test_data, strlen(test_data));
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  printf("created completion\n");
  image.aio_write(off, strlen(test_data), bl, comp);
  printf("started write\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r >= 0);
  printf("finished write\n");
  comp->release();
}

void aio_discard_test_data(librbd::Image& image, off_t off, size_t len)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_write_cb_pp);
  image.aio_discard(off, len, comp);
  comp->wait_for_complete();
  int r = comp->get_return_value();
  assert(r >= 0);
  comp->release();
}

void write_test_data(librbd::Image& image, const char *test_data, off_t off)
{
  size_t written;
  size_t len = strlen(test_data);
  ceph::bufferlist bl;
  bl.append(test_data, len);
  written = image.write(off, len, bl);
  printf("wrote: %u\n", (unsigned int) written);
  assert(written == bl.length());
}

void discard_test_data(librbd::Image& image, off_t off, size_t len)
{
  size_t written;
  written = image.discard(off, len);
  printf("discard: %u~%u\n", (unsigned)off, (unsigned)len);
  assert(written == len);
}

void aio_read_test_data(librbd::Image& image, const char *expected, off_t off, size_t expected_len)
{
  librbd::RBD::AioCompletion *comp = new librbd::RBD::AioCompletion(NULL, (librbd::callback_t) simple_read_cb_pp);
  ceph::bufferlist bl;
  printf("created completion\n");
  image.aio_read(off, expected_len, bl, comp);
  printf("started read\n");
  comp->wait_for_complete();
  int r = comp->get_return_value();
  printf("return value is: %d\n", r);
  assert(r == TEST_IO_SIZE);
  assert(strncmp(expected, bl.c_str(), TEST_IO_SIZE) == 0);
  printf("finished read\n");
  comp->release();
}

void read_test_data(librbd::Image& image, const char *expected, off_t off, size_t expected_len)
{
  int read, total_read = 0;
  size_t len = expected_len;
  ceph::bufferlist bl;
  read = image.read(off + total_read, len, bl);
  assert(read >= 0);
  printf("read: %u\n", (unsigned int) read);
  printf("read: %s\nexpected: %s\n", bl.c_str(), expected);
  assert(strncmp(bl.c_str(), expected, expected_len) == 0);
}

TEST(LibRBD, TestIOPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    const char *name = "testimg";
    uint64_t size = 2 << 20;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(0, rbd.open(ioctx, image, name));

    char test_data[TEST_IO_SIZE + 1];
    char zero_data[TEST_IO_SIZE + 1];
    int i;

    srand(time(0));
    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    test_data[TEST_IO_SIZE] = '\0';
    memset(zero_data, 0, sizeof(zero_data));

    for (i = 0; i < 5; ++i)
      write_test_data(image, test_data, strlen(test_data) * i);

    for (i = 5; i < 10; ++i)
      aio_write_test_data(image, test_data, strlen(test_data) * i);

    for (i = 0; i < 5; ++i)
      read_test_data(image, test_data, strlen(test_data) * i, TEST_IO_SIZE);

    for (i = 5; i < 10; ++i)
      aio_read_test_data(image, test_data, strlen(test_data) * i, TEST_IO_SIZE);

    // discard 2nd, 4th sections.
    discard_test_data(image, TEST_IO_SIZE, TEST_IO_SIZE);
    aio_discard_test_data(image, TEST_IO_SIZE*3, TEST_IO_SIZE);

    read_test_data(image, test_data,  0, TEST_IO_SIZE);
    read_test_data(image,  zero_data, TEST_IO_SIZE, TEST_IO_SIZE);
    read_test_data(image, test_data,  TEST_IO_SIZE*2, TEST_IO_SIZE);
    read_test_data(image,  zero_data, TEST_IO_SIZE*3, TEST_IO_SIZE);
    read_test_data(image, test_data,  TEST_IO_SIZE*4, TEST_IO_SIZE);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}


TEST(LibRBD, LockingPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    std::string cookie1 = "foo";
    std::string cookie2 = "bar";

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(0, rbd.open(ioctx, image, name));

    // no lockers initially
    std::list<librbd::locker_t> lockers;
    std::string tag;
    bool exclusive;
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_EQ(0u, lockers.size());
    ASSERT_EQ("", tag);

    // exclusive lock is exclusive
    ASSERT_EQ(0, image.lock_exclusive(cookie1));
    ASSERT_EQ(-EEXIST, image.lock_exclusive(cookie1));
    ASSERT_EQ(-EBUSY, image.lock_exclusive(""));
    ASSERT_EQ(-EEXIST, image.lock_shared(cookie1, ""));
    ASSERT_EQ(-EBUSY, image.lock_shared(cookie1, "test"));
    ASSERT_EQ(-EBUSY, image.lock_shared("", "test"));
    ASSERT_EQ(-EBUSY, image.lock_shared("", ""));

    // list exclusive
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_TRUE(exclusive);
    ASSERT_EQ("", tag);
    ASSERT_EQ(1u, lockers.size());
    ASSERT_EQ(cookie1, lockers.front().cookie);

    // unlock
    ASSERT_EQ(-ENOENT, image.unlock(""));
    ASSERT_EQ(-ENOENT, image.unlock(cookie2));
    ASSERT_EQ(0, image.unlock(cookie1));
    ASSERT_EQ(-ENOENT, image.unlock(cookie1));
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_EQ(0u, lockers.size());

    ASSERT_EQ(0, image.lock_shared(cookie1, ""));
    ASSERT_EQ(-EEXIST, image.lock_shared(cookie1, ""));
    ASSERT_EQ(0, image.lock_shared(cookie2, ""));
    ASSERT_EQ(-EEXIST, image.lock_shared(cookie2, ""));
    ASSERT_EQ(-EEXIST, image.lock_exclusive(cookie1));
    ASSERT_EQ(-EEXIST, image.lock_exclusive(cookie2));
    ASSERT_EQ(-EBUSY, image.lock_exclusive(""));
    ASSERT_EQ(-EBUSY, image.lock_exclusive("test"));

    // list shared
    ASSERT_EQ(0, image.list_lockers(&lockers, &exclusive, &tag));
    ASSERT_EQ(2u, lockers.size());
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}

TEST(LibRBD, FlushAio)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;
  size_t num_aios = 256;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  char test_data[TEST_IO_SIZE + 1];
  size_t i;
  for (i = 0; i < TEST_IO_SIZE; ++i) {
    test_data[i] = (char) (rand() % (126 - 33) + 33);
  }

  rbd_completion_t write_comps[num_aios];
  for (i = 0; i < num_aios; ++i) {
    ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &write_comps[i]));
    uint64_t offset = rand() % (size - TEST_IO_SIZE);
    ASSERT_EQ(0, rbd_aio_write(image, offset, TEST_IO_SIZE, test_data,
			       write_comps[i]));
  }

  rbd_completion_t flush_comp;
  ASSERT_EQ(0, rbd_aio_create_completion(NULL, NULL, &flush_comp));
  ASSERT_EQ(0, rbd_aio_flush(image, flush_comp));
  ASSERT_EQ(0, rbd_aio_wait_for_complete(flush_comp));
  ASSERT_EQ(1, rbd_aio_is_complete(flush_comp));
  rbd_aio_release(flush_comp);

  for (i = 0; i < num_aios; ++i) {
    ASSERT_EQ(1, rbd_aio_is_complete(write_comps[i]));
    rbd_aio_release(write_comps[i]);
  }

  ASSERT_EQ(0, rbd_close(image));
  ASSERT_EQ(0, rbd_remove(ioctx, name));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRBD, FlushAioPP)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string volume_name = get_temp_volume_name();

  ASSERT_EQ("", create_one_volume_pp(volume_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(volume_name.c_str(), ioctx));

  {
    librbd::RBD rbd;
    librbd::Image image;
    const char *name = "testimg";
    uint64_t size = 2 << 20;
    size_t num_aios = 256;

    ASSERT_EQ(0, create_image_pp(rbd, ioctx, name, size));
    ASSERT_EQ(0, rbd.open(ioctx, image, name));

    char test_data[TEST_IO_SIZE + 1];
    size_t i;
    for (i = 0; i < TEST_IO_SIZE; ++i) {
      test_data[i] = (char) (rand() % (126 - 33) + 33);
    }

    librbd::RBD::AioCompletion *write_comps[num_aios];
    for (i = 0; i < num_aios; ++i) {
      ceph::bufferlist bl;
      bl.append(test_data, strlen(test_data));
      write_comps[i] = new librbd::RBD::AioCompletion(NULL, NULL);
      uint64_t offset = rand() % (size - TEST_IO_SIZE);
      ASSERT_EQ(0, image.aio_write(offset, TEST_IO_SIZE, bl,
				   write_comps[i]));
    }

    librbd::RBD::AioCompletion *flush_comp =
      new librbd::RBD::AioCompletion(NULL, NULL);
    ASSERT_EQ(0, image.aio_flush(flush_comp));
    ASSERT_EQ(0, flush_comp->wait_for_complete());
    ASSERT_EQ(1, flush_comp->is_complete());
    delete flush_comp;

    for (i = 0; i < num_aios; ++i) {
      librbd::RBD::AioCompletion *comp = write_comps[i];
      ASSERT_EQ(1, comp->is_complete());
      delete comp;
    }
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_volume_pp(volume_name, rados));
}


int iterate_cb(uint64_t off, size_t len, int exists, void *arg)
{
  //cout << "iterate_cb " << off << "~" << len << std::endl;
  interval_set<uint64_t> *diff = static_cast<interval_set<uint64_t> *>(arg);
  diff->insert(off, len);
  return 0;
}

void scribble(librbd::Image& image, int n, int max, interval_set<uint64_t> *exists, interval_set<uint64_t> *what)
{
  uint64_t size;
  image.size(&size);
  interval_set<uint64_t> exists_at_start = *exists;
  for (int i=0; i<n; i++) {
    uint64_t off = rand() % (size - max + 1);
    uint64_t len = 1 + rand() % max;
    if (rand() % 4 == 0) {
      ASSERT_EQ((int)len, image.discard(off, len));
      interval_set<uint64_t> w;
      w.insert(off, len);

      // the zeroed bit no longer exists...
      w.intersection_of(*exists);
      exists->subtract(w);

      // the bits we discarded are no long written...
      interval_set<uint64_t> w2 = w;
      w2.intersection_of(*what);
      what->subtract(w2);

      // except for the extents that existed at the start that we overwrote.
      interval_set<uint64_t> w3;
      w3.insert(off, len);
      w3.intersection_of(exists_at_start);
      what->union_of(w3);

    } else {
      bufferlist bl;
      bl.append(buffer::create(len));
      bl.zero();
      ASSERT_EQ((int)len, image.write(off, len, bl));
      interval_set<uint64_t> w;
      w.insert(off, len);
      what->union_of(w);
      exists->union_of(w);
    }
  }
}

TEST(LibRBD, ZeroLengthWrite)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  char read_data[1];
  ASSERT_EQ(0, rbd_write(image, 0, 0, NULL));
  ASSERT_EQ(1, rbd_read(image, 0, 1, read_data));
  ASSERT_EQ('\0', read_data[0]);

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}


TEST(LibRBD, ZeroLengthDiscard)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  const char *data = "blah";
  char read_data[strlen(data)];
  ASSERT_EQ((int)strlen(data), rbd_write(image, 0, strlen(data), data));
  ASSERT_EQ(0, rbd_discard(image, 0, 0));
  ASSERT_EQ((int)strlen(data), rbd_read(image, 0, strlen(data), read_data));
  ASSERT_EQ(0, memcmp(data, read_data, strlen(data)));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

TEST(LibRBD, ZeroLengthRead)
{
  rados_t cluster;
  rados_ioctx_t ioctx;
  string volume_name = get_temp_volume_name();
  ASSERT_EQ("", create_one_volume(volume_name, &cluster));
  rados_ioctx_create(cluster, volume_name.c_str(), &ioctx);

  rbd_image_t image;
  const char *name = "testimg";
  uint64_t size = 2 << 20;

  ASSERT_EQ(0, create_image(ioctx, name, size));
  ASSERT_EQ(0, rbd_open(ioctx, name, &image));

  char read_data[1];
  ASSERT_EQ(0, rbd_read(image, 0, 0, read_data));

  ASSERT_EQ(0, rbd_close(image));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_volume(volume_name, &cluster));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
