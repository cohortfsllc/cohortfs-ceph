// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "stdio.h"
#include "libosd/ceph_osd.h"

int main(int argc, const char *argv[])
{
  struct libosd *osd = libosd_init(0);
  if (osd == NULL) {
    fputs("osd init failed\n", stderr);
    return 1;
  }
  libosd_cleanup(osd);
  return 0;
}
