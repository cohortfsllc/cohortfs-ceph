// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_H
#define CEPH_LIBOSD_H

#ifdef __cplusplus

// C++ interface
struct libosd {
  virtual ~libosd() {}
};

#endif

// C interface
struct libosd* libosd_init(int name);
void libosd_cleanup(struct libosd *osd);

#endif
