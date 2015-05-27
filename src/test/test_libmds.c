// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>

#include "mds/ceph_mds.h"

static int run_tests(struct libmds *mds)
{
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
