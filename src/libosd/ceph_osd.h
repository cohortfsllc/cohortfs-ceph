// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBCEPH_OSD_H
#define LIBCEPH_OSD_H

#ifdef __cplusplus

/* C++ interface */
struct libosd {
  const int whoami;
  libosd(int name) : whoami(name) {}
  virtual ~libosd() {}

  virtual void signal(int signum) = 0;
};


/* C interface */
extern "C" {
#endif /* __cplusplus */

struct libosd_init_args {
  int id; /* osd instance id */
  const char *config; /* path to ceph configuration file */
  const char *cluster; /* ceph cluster name (default "ceph") */
};

struct libosd* libosd_init(const struct libosd_init_args *args);
void libosd_cleanup(struct libosd *osd);

void libosd_signal(int signum);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_OSD_H */
