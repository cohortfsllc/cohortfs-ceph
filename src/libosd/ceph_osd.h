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

  virtual int run() = 0;
  virtual void shutdown() = 0;
  virtual void signal(int signum) = 0;
};


/* C interface */
extern "C" {
#endif /* __cplusplus */

/* initialization arguments for libosd_init() */
struct libosd_init_args {
  int id;		/* osd instance id */
  const char *config;	/* path to ceph configuration file */
  const char *cluster;	/* ceph cluster name (default "ceph") */
};

/* bind messengers, create an objectstore, and create an OSD */
struct libosd* libosd_init(const struct libosd_init_args *args);

/* starts the osd and blocks until shutdown */
int libosd_run(struct libosd *osd);

/* starts shutting down a running osd */
void libosd_shutdown(struct libosd *osd);

/* release resources associated with an osd that is not running */
void libosd_cleanup(struct libosd *osd);

/* send the given signal to all osds */
void libosd_signal(int signum);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_OSD_H */
