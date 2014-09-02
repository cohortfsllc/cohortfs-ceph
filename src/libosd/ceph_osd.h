/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
/* vim: ts=8 sw=2 smarttab */

#ifndef LIBCEPH_OSD_H
#define LIBCEPH_OSD_H

#include <stdint.h>
#include <uuid/uuid.h>

#ifdef __cplusplus

/* C++ interface */
struct libosd {
  const int whoami;
  libosd(int name) : whoami(name) {}

  virtual void join() = 0;
  virtual void shutdown() = 0;
  virtual void signal(int signum) = 0;

  virtual int get_volume(const char *name, uuid_t uuid) = 0;

  virtual int read(const char *object, const uuid_t volume,
		   uint64_t offset, uint64_t length, char *data,
		   uint64_t *id) = 0;
  virtual int write(const char *object, const uuid_t volume,
		    uint64_t offset, uint64_t length, char *data,
		    uint64_t *id) = 0;

protected: /* must be deleted by libosd_cleanup() */
  virtual ~libosd() {}
};


/* C interface */
extern "C" {
#endif /* __cplusplus */

/* i/o completion function for async read and write callbacks */
typedef void (*io_completion_fn)(uint64_t id, int result, uint64_t length,
				 void *user);

/* osd callback function table */
struct libosd_callbacks {
  void (*osd_active)();
  void (*osd_shutdown)();
  io_completion_fn read_completion;
  io_completion_fn write_completion;
};

/* initialization arguments for libosd_init() */
struct libosd_init_args {
  int id;		/* osd instance id */
  const char *config;	/* path to ceph configuration file */
  const char *cluster;	/* ceph cluster name (default "ceph") */
  struct libosd_callbacks *callbacks; /* optional callbacks */
};

/* bind messengers, create an objectstore, and start an OSD.
 * returns before initialization is complete; refer to osd_active
 * callback to determine when the osd becomes active */
struct libosd* libosd_init(const struct libosd_init_args *args);

/* blocks until the osd shuts down */
void libosd_join(struct libosd *osd);

/* starts shutting down a running osd */
void libosd_shutdown(struct libosd *osd);

/* release resources associated with an osd that is not running */
void libosd_cleanup(struct libosd *osd);

/* send the given signal to all osds */
void libosd_signal(int signum);


/* look up a volume by name, and set its uuid.  returns 0 on success */
int libosd_get_volume(struct libosd *osd, const char *name, uuid_t uuid);

/**
 * Read from an object asynchronously. 
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
 * @param offset  Offset into the object
 * @param length  Number of bytes to read
 * @param data	  Buffer to receive the object data
 * @param id	  Operation identifier assigned by libosd. Will match the
 *		  id argument passed to the read_completion callback
 * @param user	  User data passed to the read_completion callback
 *
 * @return Nonzero on immediate errors. Otherwise returns 0 and promises
 * to call the read_completion callback on success or failure.
 */
int libosd_read(struct libosd *osd, const char *object, const uuid_t volume,
		uint64_t offset, uint64_t length, char *data,
		uint64_t *id, void *user);

/**
 * Write to an object asynchronously. 
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
 * @param offset  Offset into the object
 * @param length  Number of bytes to write
 * @param data	  Buffer to receive the object data
 * @param id	  Operation identifier assigned by libosd. Will match the
 *		  id argument passed to the read_completion callback
 * @param user	  User data passed to the read_completion callback
 *
 * @return Nonzero on immediate errors. Otherwise returns 0 and promises
 * to call the read_completion callback on success or failure.
 */
int libosd_write(struct libosd *osd, const char *object, const uuid_t volume,
		 uint64_t offset, uint64_t length, char *data,
		 uint64_t *id, void *user);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_OSD_H */
