// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_H
#define CEPH_LIBOSD_H

#ifdef __cplusplus

// C++ interface
struct libosd {
  const int whoami;
  libosd(int name) : whoami(name) {}
  virtual ~libosd() {}

  virtual void signal(int signum) = 0;
};

#endif

// C interface
struct libosd* libosd_init(int name);
void libosd_cleanup(struct libosd *osd);

void libosd_signal(int signum);

#endif
