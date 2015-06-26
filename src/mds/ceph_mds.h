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


typedef uint64_t inodenum_t;

#ifdef __cplusplus
/**
 * The abstract C++ libmds interface, whose member functions take
 * the same arguments as the C interface.
 *
 * Must be created with libmds_init() and destroyed with libmds_cleanup().
 */
struct libmds {
  const int whoami; /**< mds instance id */
  libmds(int name) : whoami(name) {}

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

  /**
   * Create a regular file in the parent directory.
   * @see libmds_create()
   */
  virtual int create(inodenum_t parent, const char *name) = 0;

  /**
   * Create a subdirectory in the parent directory.
   * @see libmds_mkdir()
   */
  virtual int mkdir(inodenum_t parent, const char *name) = 0;

  /**
   * Unlink the given file from the parent directory.
   * @see libmds_unlink()
   */
  virtual int unlink(inodenum_t parent, const char *name) = 0;

  /**
   * Find an entry in the parent directory.
   * @see libmds_lookup()
   */
  virtual int lookup(inodenum_t parent, const char *name, inodenum_t *ino) = 0;

 protected:
  /** Destructor protected: must be deleted by libmds_cleanup() */
  virtual ~libmds() {}
};


/* C interface */
extern "C" {
#else
  struct libmds;
#endif /* __cplusplus */

  /** Initialization arguments for libmds_init() */
  struct libmds_init_args {
    int id;               /**< mds instance id */
    const char *config;   /**< path to ceph configuration file */
    const char *cluster;  /**< ceph cluster name (default "ceph") */
  };

  /**
   * Create and initialize an mds from the given arguments. Reads
   * the ceph.conf, binds messengers, creates an objectstore,
   * and starts running the mds. Returns before initialization is
   * complete; refer to mds_active callback to determine when the
   * mds becomes active.
   *
   * @param args  Initialization arguments
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

  /**
   * Create a regular file in the parent directory.
   *
   * @param mds     The libmds object returned by libmds_init()
   * @param parent  Inode number of the parent directory
   * @param name    Filename of the new directory entry
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENOENT if the parent does not exist.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -EEXIST if the parent directory already has an entry with \a name.
   */
  int libmds_create(struct libmds *mds, inodenum_t parent, const char *name);

  /**
   * Create a subdirectory in the parent directory.
   *
   * @param mds     The libmds object returned by libmds_init()
   * @param parent  Inode number of the parent directory
   * @param name    Filename of the new directory entry
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENOENT if the parent does not exist.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -EEXIST if the parent directory already has an entry with \a name.
   */
  int libmds_mkdir(struct libmds *mds, inodenum_t parent, const char *name);

  /**
   * Unlink the given file from the parent directory.
   *
   * @param mds     The libmds object returned by libmds_init()
   * @param parent  Inode number of the parent directory
   * @param name    Filename of the directory entry to remove
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENOENT if the parent does not have an entry with \a name.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -ENOTEMPTY if the entry is a non-empty directory.
   */
  int libmds_unlink(struct libmds *mds, inodenum_t parent, const char *name);

  /**
   * Find an entry in the parent directory.
   *
   * @param mds      The libmds object returned by libmds_init()
   * @param parent   Inode number of the parent directory
   * @param name     Filename of the directory entry to find
   * @param[out] ino Inode number of the directory entry found
   *
   * @return Returns 0 on success and sets \a ino or a negative error code.
   * @retval -ENOENT if the parent does not have an entry with \a name.
   * @retval -ENOTDIR if the parent is not a directory.
   */
  int libmds_lookup(struct libmds *mds, inodenum_t parent, const char *name,
                    inodenum_t *ino);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_MDS_H */
