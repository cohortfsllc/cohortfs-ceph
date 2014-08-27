// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>
#include <stdio.h>

#include "libosd/ceph_osd.h"

int main(int argc, const char *argv[])
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

  int r = libosd_run(osd);
  fprintf(stderr, "libosd_run returned %d\n", r);

  libosd_cleanup(osd);
  fputs("libosd_cleanup finished\n", stderr);
  return r;
}
