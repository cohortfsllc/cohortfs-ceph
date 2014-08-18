// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "libosd.h"

int main(int argc, const char *argv[])
{

  libosd *osd = libosd_init(0);
  if (osd == NULL) {
    std::cerr << "osd init failed" << std::endl;
    return 1;
  }
  libosd_cleanup(osd);
  return 0;
}
