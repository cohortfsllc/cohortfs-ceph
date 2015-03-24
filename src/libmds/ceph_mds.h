/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
/* vim: ts=8 sw=2 smarttab
*/

/** \mainpage
 * Contains the C and C++ APIs for Ceph's LibMDS, which allows an
 * application to bring up one or more Ceph MDSs within the process.
 *
 * The application must link against libceph-mds.so.
 */
/** \file */

#ifndef LIBCEPH_MDS_H
#define LIBCEPH_MDS_H

#include <stdint.h>

/**
 * Completion callback for asynchronous io
 *
 * @param result  The operation result
 * @param length  Number of bytes read/written
 * @param flags	  LIBMDS_READ_/WRITE_ flags
 * @param user	  Private user data back...
 */
typedef void (*libmds_io_completion_fn)(int result, uint64_t length,
					int flags, void *user);

#ifdef __cplusplus
class CephContext;
namespace ceph {
  namespace mds {
    int context_create(int id, const char *config, const char *cluster,
		       CephContext** cct);
  }
}

/**
 * The abstract C++ libmds interface, whose member functions take
 * the same arguments as the C interface.
 *
 * Must be created with libmds_init() and destroyed with libmds_cleanup().
 */
struct libmds {
  const int whoami; /**< mds instance id */
  CephContext* cct;
  libmds(int name) : whoami(name), cct(nullptr) {}

  /**
   * Blocks until the mds shuts down.
   * @see libmds_join()
   */
  virtual void join() = 0;

  /**
   * Starts shutting down a running mds.
   * @see libmds_shutdown()
   */
  virtual void shutdown() = 0;

  /**
   * Send the given signal to this mds.
   * @see libmds_signal()
   */
  virtual void signal(int signum) = 0;

/// more instance methods to go here

protected:
  /** Destructor protected: must be deleted by libmds_cleanup() */
  virtual ~libmds() {}
};


/* C interface */
extern "C" {
#else
struct libmds;
#endif /* __cplusplus */

/** mds callback function table */
struct libmds_callbacks {
  /**
   * Called when the MDS's MDSMap state switches to up:active.
   *
   * @param mds	  The libmds object returned by libmds_init()
   * @param user  Private user data provided in libmds_init_args
   */
  void (*mds_active)(struct libmds *mds, void *user);

  /**
   * Called if the MDS decides to shut down on its own, not as
   * a result of libmds_shutdown().
   *
   * @param mds	  The libmds object returned by libmds_init()
   * @param user  Private user data provided in libmds_init_args
   */
  void (*mds_shutdown)(struct libmds *mds, void *user);
};

/** Initialization arguments for libmds_init() */
struct libmds_init_args {
  int id;		/**< mds instance id */
  const char *config;	/**< path to ceph configuration file */
  const char *cluster;	/**< ceph cluster name (default "ceph") */
  struct libmds_callbacks *callbacks; /**< optional callbacks */
  void *user;		/**< user data for mds_active and mds_shutdown */
};

/**
 * Create and initialize an mds from the given arguments. Reads
 * the ceph.conf, binds messengers, creates an objectstore,
 * and starts running the mds. Returns before initialization is
 * complete; refer to mds_active callback to determine when the
 * mds becomes active.
 *
 * @param args	  Initialization arguments
 *
 * @return A pointer to the new libmds instance, or NULL on failure.
 */
struct libmds* libmds_init(const struct libmds_init_args *args);

/**
 * Blocks until the mds shuts down, either because of a call to
 * libmds_shutdown(), or an mds_shutdown() callback initiated by the mds.
 *
 * @param mds	  The libmds object returned by libmds_init()
 */
void libmds_join(struct libmds *mds);

/**
 * Starts shutting down a running mds.
 *
 * @param mds	  The libmds object returned by libmds_init()
 */
void libmds_shutdown(struct libmds *mds);

/**
 * Release resources associated with an mds that is not running.
 *
 * @param mds	  The libmds object returned by libmds_init()
 */
void libmds_cleanup(struct libmds *mds);

/**
 * Send the given signal to all mdss.
 *
 * @param signum  The signal from a signal handler
 */
void libmds_signal(int signum);

//// C versions of instance methods

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_MDS_H */
