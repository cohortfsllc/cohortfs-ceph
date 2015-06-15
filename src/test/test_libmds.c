// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/stat.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

#include "mds/ceph_mds.h"

const uint8_t VOLUME[16] = {
  0, 1, 2, 3, 4, 5, 6, 7,
  8, 9, 0, 1, 2, 3, 4, 5 };
const uint8_t VOLUMEA[16] = {
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
  'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p' };
const uint8_t VOLUMEB[16] = {
  'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
  's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

static int test_unlink_notempty(struct libmds *mds, const libmds_fileid_t *root)
{
  libmds_fileid_t dir = { root->volume, 0 };
  int r = libmds_mkdir(mds, root, "dir");
  if (r) {
    fprintf(stderr, "libmds_mkdir(\"dir\") failed with %d\n", r);
    return r;
  }
  r = libmds_lookup(mds, root, "dir", &dir.ino);
  if (r) {
    fprintf(stderr, "libmds_lookup(\"dir\") failed with %d\n", r);
    return r;
  }
  r = libmds_create(mds, &dir, "file");
  if (r) {
    fprintf(stderr, "libmds_create(\"file\") failed with %d\n", r);
    return r;
  }
  r = libmds_unlink(mds, root, "dir");
  if (r != -ENOTEMPTY) {
    fprintf(stderr, "libmds_unlink(\"dir\") returned %d, expected -ENOTEMPTY\n", r);
    return -ENOTEMPTY;
  }
  r = libmds_unlink(mds, &dir, "file");
  if (r) {
    fprintf(stderr, "libmds_unlink(\"file\") failed with %d\n", r);
    return r;
  }
  r = libmds_unlink(mds, root, "dir");
  if (r) {
    fprintf(stderr, "libmds_unlink(\"dir\") failed with %d\n", r);
    return r;
  }
  struct stat st;
  r = libmds_getattr(mds, &dir, &st);
  if (r != -ENOENT) {
    fprintf(stderr, "libmds_getattr(\"dir\") returned %d, expected -ENOENT\n", r);
    return -ENOENT;
  }
  return 0;
}

#define NUM_ENTRIES 3
static const char* entries[] = {"a", "b", "c"};

struct test_readdir_data {
  uint64_t pos;
  uint64_t gen;
  int counts[NUM_ENTRIES];
  int unexpected;
  int single;
};

static int test_readdir_cb(const char *name, libmds_ino_t ino,
    uint64_t pos, uint64_t gen, void *user)
{
  struct test_readdir_data *data = user;
  data->pos = pos;
  data->gen = gen;
  for (int i = 0; i < NUM_ENTRIES; i++) {
    if (strcmp(name, entries[i]) == 0) {
      data->counts[i]++;
      return data->single;
    }
  }
  data->unexpected++;
  fprintf(stderr, "test_readdir_cb unexpected entry \"%s\" at position %d\n",
      name, (int)pos);
  return data->single;
}

static int test_readdir_verify(const struct test_readdir_data *data)
{
  for (int i = 0; i < NUM_ENTRIES; i++) {
    if (data->counts[i] != 1) {
      fprintf(stderr, "libmds_readdir() expected \"%s\" once, got %d\n",
	  entries[i], data->counts[i]);
      return EINVAL;
    }
  }
  if (data->unexpected) {
    fprintf(stderr, "libmds_readdir() got %d unexpected entries\n",
	data->unexpected);
    return EINVAL;
  }
  return 0;
}

static int test_readdir_full(struct libmds *mds, const libmds_fileid_t *dir)
{
  struct test_readdir_data data = {0};
  int r = libmds_readdir(mds, dir, data.pos, data.gen, test_readdir_cb, &data);
  if (r) {
    fprintf(stderr, "libmds_readdir() failed with %d\n", r);
    return r;
  }
  // verify the expected entries
  r = test_readdir_verify(&data);
  if (r)
    return r;
  // verify that readdir returns EOF when pos > size
  r = libmds_readdir(mds, dir, data.pos, data.gen, test_readdir_cb, &data);
  if (r != -EOF) {
    fprintf(stderr, "libmds_readdir() returned %d, expected -EOF\n", r);
    return -EOF;
  }
  return 0;
}

static int test_readdir_single(struct libmds *mds, const libmds_fileid_t *dir)
{
  struct test_readdir_data data = {0};
  // test_readdir_cb() will return nonzero for each call
  data.single = 1;
  int r;
  for (;;) {
    // call libmds_readdir() until it returns -EOF
    r = libmds_readdir(mds, dir, data.pos, data.gen, test_readdir_cb, &data);
    if (r == -EOF)
      break;
    if (r) {
      fprintf(stderr, "libmds_readdir() failed with %d\n", r);
      return r;
    }
  }
  // verify the expected entries
  return test_readdir_verify(&data);
}

static int test_readdir(struct libmds *mds, const libmds_fileid_t *root)
{
  libmds_fileid_t dir = { root->volume, 0 };
  int r = libmds_mkdir(mds, root, "readdir");
  if (r) {
    fprintf(stderr, "libmds_mkdir(\"readdir\") failed with %d\n", r);
    return r;
  }
  r = libmds_lookup(mds, root, "readdir", &dir.ino);
  if (r) {
    fprintf(stderr, "libmds_lookup(\"readdir\") failed with %d\n", r);
    return r;
  }
  // create all entries (except . and ..)
  for (int i = 0; i < NUM_ENTRIES; i++) {
    if (*entries[i] == '.')
      continue;
    r = libmds_create(mds, &dir, entries[i]);
    if (r) {
      fprintf(stderr, "libmds_create(\"%s\") failed with %d\n", entries[i], r);
      return r;
    }
  }
  r = test_readdir_full(mds, &dir);
  if (r) {
    fprintf(stderr, "test_readdir_full() failed with %d\n", r);
    return r;
  }
  r = test_readdir_single(mds, &dir);
  if (r) {
    fprintf(stderr, "test_readdir_single() failed with %d\n", r);
    return r;
  }
  return 0;
}

static int test_volumes(struct libmds *mds)
{
  const libmds_fileid_t roota = { VOLUMEA, 1 };
  const libmds_fileid_t rootb = { VOLUMEB, 1 };
  libmds_ino_t ino;

  // create a directory in volume a
  int r = libmds_mkdir(mds, &roota, "vola");
  if (r) {
    fprintf(stderr, "libmds_mkdir(\"vola\") failed with %d\n", r);
    return r;
  }
  // make sure that we can't see it in volume b
  r = libmds_lookup(mds, &rootb, "vola", &ino);
  if (r != -ENOENT) {
    fprintf(stderr, "libmds_lookup(\"vola\") returned %d, expected -ENOENT\n",
	r);
    return -ENOENT;
  }
  return 0;
}

static int run_tests(struct libmds *mds)
{
  const libmds_fileid_t root = { VOLUME, 1 };
  int r = test_unlink_notempty(mds, &root);
  if (r) {
    fprintf(stderr, "test_unlink_notempty() failed with %d\n", r);
    return r;
  }
  r = test_readdir(mds, &root);
  if (r) {
    fprintf(stderr, "test_readdir() failed with %d\n", r);
    return r;
  }
  r = test_volumes(mds);
  if (r) {
    fprintf(stderr, "test_volumes() failed with %d\n", r);
    return r;
  }
  puts("libmds tests passed");
  return 0;
}

int main(int argc, const char *argv[])
{
  int r = 0;
  struct libmds_init_args args = {
    .id = 0,
    .config = NULL,
  };
  struct libmds *mds = libmds_init(&args);
  if (mds == NULL) {
    fputs("mds init failed\n", stderr);
    return -1;
  }
  signal(SIGINT, libmds_signal);

  r = run_tests(mds);

  libmds_shutdown(mds);
  libmds_cleanup(mds);
  return r;
}
