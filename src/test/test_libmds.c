// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/stat.h>
#include <signal.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>

#include "mds/ceph_mds.h"

static int test_unlink_notempty(struct libmds *mds, inodenum_t root)
{
  int r = libmds_mkdir(mds, root, "dir");
  if (r) {
    fprintf(stderr, "libmds_mkdir(\"dir\") failed with %d\n", r);
    return r;
  }
  inodenum_t dir;
  r = libmds_lookup(mds, root, "dir", &dir);
  if (r) {
    fprintf(stderr, "libmds_lookup(\"dir\") failed with %d\n", r);
    return r;
  }
  r = libmds_create(mds, dir, "file");
  if (r) {
    fprintf(stderr, "libmds_create(\"file\") failed with %d\n", r);
    return r;
  }
  r = libmds_unlink(mds, root, "dir");
  if (r != -ENOTEMPTY) {
    fprintf(stderr, "libmds_unlink(\"dir\") returned %d, expected -ENOTEMPTY\n", r);
    return r;
  }
  r = libmds_unlink(mds, dir, "file");
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
  r = libmds_getattr(mds, dir, &st);
  if (r != -ENOENT) {
    fprintf(stderr, "libmds_getattr(\"dir\") returned %d, expected -ENOENT\n", r);
    return r;
  }
  puts("libmds tests passed");
  return 0;
}

static int run_tests(struct libmds *mds)
{
  const inodenum_t root = 0;
  return test_unlink_notempty(mds, root);
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
