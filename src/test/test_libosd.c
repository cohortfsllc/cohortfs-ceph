// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>
#include <stdio.h>

#include "libosd/ceph_osd.h"

int test_single()
{
  struct libosd_init_args args = {
    .id = 0,
    .config = "/etc/ceph/ceph.conf",
  };
  struct libosd *osd = libosd_init(&args);
  if (osd == NULL) {
    fputs("osd init failed\n", stderr);
    return 1;
  }

  signal(SIGINT, libosd_signal);
  signal(SIGTERM, libosd_signal);

  libosd_join(osd);
  fputs("libosd_join returned\n", stderr);

  libosd_cleanup(osd);
  fputs("libosd_cleanup finished\n", stderr);
  return 0;
}

int test_double()
{
  struct libosd *osd1, *osd2;
  struct libosd_init_args args1 = {
    .id = 0,
    .config = "/etc/ceph/ceph.conf",
  };
  struct libosd_init_args args2 = {
    .id = 1,
    .config = "/etc/ceph/ceph.conf",
  };

  // start osds
  osd1 = libosd_init(&args1);
  if (osd1 == NULL) {
    fputs("osd1 init failed\n", stderr);
    return 1;
  }
  printf("osd1 created %p\n", osd1);

  osd2 = libosd_init(&args2);
  if (osd2 == NULL) {
    fputs("osd2 init failed\n", stderr);
    return 1;
  }
  printf("osd2 created %p\n", osd2);

  // join osds
  printf("waiting on osd1 %p\n", osd1);
  libosd_join(osd1);
  printf("waiting on osd2 %p\n", osd2);
  libosd_join(osd2);

  // clean up
  libosd_cleanup(osd1);
  libosd_cleanup(osd2);
  puts("finished");
  return 0;
}


int main(int argc, const char *argv[])
{
  signal(SIGINT, libosd_signal);
  signal(SIGTERM, libosd_signal);

  int r = test_single();
  if (r != 0) {
    fprintf(stderr, "test_single() failed with %d\n", r);
    return r;
  }

  r = test_double();
  if (r != 0) {
    fprintf(stderr, "test_double() failed with %d\n", r);
    return r;
  }
  return 0;
}
