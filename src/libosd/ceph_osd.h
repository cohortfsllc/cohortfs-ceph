/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
/* vim: ts=8 sw=2 smarttab
*/

/** \mainpage
 * Contains the C and C++ APIs for Ceph's LibOSD, which allows an
 * application to bring up one or more Ceph OSDs within the process.
 *
 * The application must link against libceph-osd.so.
 */
/** \file */

#ifndef LIBCEPH_OSD_H
#define LIBCEPH_OSD_H

#include <stdint.h>
#include <uuid/uuid.h>

#ifdef __cplusplus

/**
 * The abstract C++ libosd interface, whose member functions take
 * the same arguments as the C interface.
 *
 * Must be created with libosd_init() and destroyed with libosd_cleanup().
 */
struct libosd {
  const int whoami; /**< osd instance id */
  libosd(int name) : whoami(name) {}

  /**
   * Blocks until the osd shuts down.
   * @see libosd_join()
   */
  virtual void join() = 0;

  /**
   * Starts shutting down a running osd.
   * @see libosd_shutdown()
   */
  virtual void shutdown() = 0;

  /**
   * Send the given signal to this osd.
   * @see libosd_signal()
   */
  virtual void signal(int signum) = 0;

  /**
   * Look up a volume by name to get its uuid.
   * @see libosd_get_volume()
   */
  virtual int get_volume(const char *name, uuid_t uuid) = 0;

  /**
   * Read from an object.
   * @see libosd_read()
   */
  virtual int read(const char *object, const uuid_t volume,
		   uint64_t offset, uint64_t length, char *data,
		   int flags, void *user) = 0;

  /** Write to an object.
   * @see libosd_write()
   */
  virtual int write(const char *object, const uuid_t volume,
		    uint64_t offset, uint64_t length, char *data,
		    int flags, void *user) = 0;

  /** Truncate an object.
   * @see libosd_truncate()
   */
  virtual int truncate(const char *object, const uuid_t volume,
		       uint64_t offset, int flags, void *user) = 0;

protected:
  /** Destructor protected: must be deleted by libosd_cleanup() */
  virtual ~libosd() {}
};


/* C interface */
extern "C" {
#else
struct libosd;
#endif /* __cplusplus */

/**
 * Completion callback for asynchronous io
 *
 * @param result  The operation result
 * @param length  Number of bytes read/written
 * @param flags	  LIBOSD_READ_/WRITE_ flags
 * @param user	  Private user data passed to libosd_read/write/truncate()
 */
typedef void (*libosd_io_completion_fn)(int result, uint64_t length,
					int flags, void *user);

/** osd callback function table */
struct libosd_callbacks {
  /**
   * Called when the OSD's OSDMap state switches to up:active.
   *
   * @param osd	  The libosd object returned by libosd_init()
   * @param user  Private user data provided in libosd_init_args
   */
  void (*osd_active)(struct libosd *osd, void *user);

  /**
   * Called if the OSD decides to shut down on its own, not as
   * a result of libosd_shutdown().
   *
   * @param osd	  The libosd object returned by libosd_init()
   * @param user  Private user data provided in libosd_init_args
   */
  void (*osd_shutdown)(struct libosd *osd, void *user);

  /** Called when an asynchronous read completes to deliver the
   * status and number of bytes read */
  libosd_io_completion_fn read_completion;

  /** Called when an asynchronous write or truncate completes to
   * deliver the status and number of bytes written */
  libosd_io_completion_fn write_completion;
};

/** Initialization arguments for libosd_init() */
struct libosd_init_args {
  int id;		/**< osd instance id */
  const char *config;	/**< path to ceph configuration file */
  const char *cluster;	/**< ceph cluster name (default "ceph") */
  struct libosd_callbacks *callbacks; /**< optional callbacks */
  void *user;		/**< user data for osd_active and osd_shutdown */
};

/**
 * Create and initialize an osd from the given arguments. Reads
 * the ceph.conf, binds messengers, creates an objectstore,
 * and starts running the osd. Returns before initialization is
 * complete; refer to osd_active callback to determine when the
 * osd becomes active.
 *
 * @param args	  Initialization arguments
 *
 * @return A pointer to the new libosd instance, or NULL on failure.
 */
struct libosd* libosd_init(const struct libosd_init_args *args);

/**
 * Blocks until the osd shuts down, either because of a call to
 * libosd_shutdown(), or an osd_shutdown() callback initiated by the osd.
 *
 * @param osd	  The libosd object returned by libosd_init()
 */
void libosd_join(struct libosd *osd);

/**
 * Starts shutting down a running osd.
 *
 * @param osd	  The libosd object returned by libosd_init()
 */
void libosd_shutdown(struct libosd *osd);

/**
 * Release resources associated with an osd that is not running.
 *
 * @param osd	  The libosd object returned by libosd_init()
 */
void libosd_cleanup(struct libosd *osd);

/**
 * Send the given signal to all osds.
 *
 * @param signum  The signal from a signal handler
 */
void libosd_signal(int signum);

/**
 * Look up a volume by name to get its uuid.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param name    The volume name in the osd map
 * @param uuid    The uuid to be assigned
 *
 * @return 0 on success, ENOENT if not found.
 */
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
 * @param flags	  LIBOSD_READ_ flags
 * @param user	  User data passed to the read_completion callback
 *
 * @return Nonzero on immediate errors. Otherwise returns 0 and promises
 * to call the read_completion callback on success or failure. EINVAL
 * if no read_completion callback is given.
 */
int libosd_read(struct libosd *osd, const char *object, const uuid_t volume,
		uint64_t offset, uint64_t length, char *data,
		int flags, void *user);

/* Completion flags for libosd_write() and libosd_truncate() */
/** Request a completion once the data is written to cache */
#define LIBOSD_WRITE_CB_UNSTABLE  0x01
/** Request a completion once the data is written to stable storage */
#define LIBOSD_WRITE_CB_STABLE	  0x02

/**
 * Write to an object asynchronously.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
 * @param offset  Offset into the object
 * @param length  Number of bytes to write
 * @param data	  Buffer to receive the object data
 * @param flags	  LIBOSD_WRITE_ flags
 * @param user	  User data passed to the write_completion callback
 *
 * @return Nonzero on immediate errors. Otherwise returns 0 and promises
 * to call the write_completion callback on success or failure. EINVAL
 * if no write_completion callback is given.
 */
int libosd_write(struct libosd *osd, const char *object, const uuid_t volume,
		 uint64_t offset, uint64_t length, char *data,
		 int flags, void *user);

/**
 * Truncate an object asynchronously.
 *
 * @param osd	  The libosd object returned by libosd_init()
 * @param object  The object name string
 * @param volume  The volume uuid returned by libosd_get_volume()
 * @param offset  Offset into the object
 * @param flags	  LIBOSD_WRITE_ flags
 * @param user	  User data passed to the write_completion callback
 *
 * @return Nonzero on immediate errors. Otherwise returns 0 and promises
 * to call the write_completion callback on success or failure. EINVAL
 * if no write_completion callback is given.
 */
int libosd_truncate(struct libosd *osd, const char *object,
		    const uuid_t volume, uint64_t offset,
		    int flags, void *user);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_OSD_H */