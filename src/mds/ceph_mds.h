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


#define LIBMDS_VOLUME_LEN 16

typedef uint64_t libmds_ino_t;
typedef const uint8_t* libmds_volume_t;

typedef struct libmds_fileid {
  libmds_volume_t volume;
  libmds_ino_t ino;
} libmds_fileid_t;

typedef struct libmds_identity {
  int uid;
  int gid;
} libmds_identity_t;

/* attribute mask for libmds_setattr() */
#define LIBMDS_ATTR_SIZE  0x01
#define LIBMDS_ATTR_MODE  0x02
#define LIBMDS_ATTR_OWNER 0x04
#define LIBMDS_ATTR_GROUP 0x08
#define LIBMDS_ATTR_MTIME 0x10
#define LIBMDS_ATTR_ATIME 0x20
#define LIBMDS_ATTR_CTIME 0x40

/**
 * Callback for libmds_readdir, containing a single directory entry.
 *
 * @param name  Filename of the directory entry
 * @param ino   Inode number of the directory entry
 * @param pos   Index of the next directory entry
 * @param gen   Generation number of the directory
 * @param user  User data passed to libmds_readdir()
 *
 * @return 0 if the caller is prepared to accept more entries
 */
typedef int (*libmds_readdir_fn)(const char *name, libmds_ino_t ino,
                                 uint64_t pos, uint64_t gen, void *user);

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
   * @see libmds_get_root()
   */
  virtual int get_root(libmds_volume_t volume, libmds_ino_t *ino) = 0;

  /**
   * Create a regular file in the parent directory.
   * @see libmds_create()
   */
  virtual int create(const libmds_fileid_t *parent, const char *name,
                     int mode, const libmds_identity_t *who,
                     libmds_ino_t *ino, struct stat *st) = 0;

  /**
   * Create a subdirectory in the parent directory.
   * @see libmds_mkdir()
   */
  virtual int mkdir(const libmds_fileid_t *parent, const char *name,
                    int mode, const libmds_identity_t *who,
                    libmds_ino_t *ino, struct stat *st) = 0;

  /**
   * Create a link in the parent directory.
   * @see libmds_link()
   */
  virtual int link(const libmds_fileid_t *parent, const char *name,
                   libmds_ino_t ino, struct stat *st) = 0;

  /**
   * Create a symbolic link in the parent directory.
   * @see libmds_symlink()
   */
  virtual int symlink(const libmds_fileid_t *parent, const char *name,
                      const char *target, const libmds_identity_t *who,
                      libmds_ino_t *ino, struct stat *st) = 0;

  /**
   * Read the contents of a symbolic link.
   * @see libmds_readlink()
   */
  virtual int readlink(const libmds_fileid_t *parent,
                       char *buf, int buf_len) = 0;

  /**
   * Rename a directory entry.
   * @see libmds_rename()
   */
  virtual int rename(const libmds_fileid_t *srcp, const char *src_name,
                     const libmds_fileid_t *dstp, const char *dst_name) = 0;

  /**
   * Unlink the given file from the parent directory.
   * @see libmds_unlink()
   */
  virtual int unlink(const libmds_fileid_t *parent, const char *name) = 0;

  /**
   * Find an entry in the parent directory.
   * @see libmds_lookup()
   */
  virtual int lookup(const libmds_fileid_t *parent, const char *name,
                     libmds_ino_t *ino) = 0;

  /**
   * List the entries of a directory.
   * @see libmds_readdir()
   */
  virtual int readdir(const libmds_fileid_t *dir, uint64_t pos, uint64_t gen,
                      libmds_readdir_fn callback, void *user) = 0;

  /**
   * Query the attributes of a file.
   * @see libmds_getattr()
   */
  virtual int getattr(const libmds_fileid_t *file, struct stat *st) = 0;

  /**
   * Set the attributes of a file.
   * @see libmds_setattr()
   */
  virtual int setattr(const libmds_fileid_t *file, int mask,
                      const struct stat *st) = 0;

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
    const char **argv;    /**< command-line argument array */
    int argc;             /**< size of argv array */
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
   * Get the inode number of the root directory in the given volume.
   *
   * @param mds       The libmds object returned by libmds_init()
   * @param volume    The volume uuid
   * @param[out] ino  Inode number of the root directory
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if the given volume does not exist.
   */
  int libmds_get_root(struct libmds *mds, libmds_volume_t volume,
                      libmds_ino_t *ino);

  /**
   * Create a regular file in the parent directory.
   *
   * @param mds       The libmds object returned by libmds_init()
   * @param parent    Fileid of the parent directory
   * @param name      Filename of the new directory entry
   * @param mode      Mode bits to set on the created file
   * @param who       User identity
   * @param[out] ino  Inode number of the created file
   * @param[out] st   Attributes of the created file
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if the parent does not exist.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -EEXIST if the parent directory already has an entry with \a name.
   */
  int libmds_create(struct libmds *mds, const libmds_fileid_t *parent,
                    const char *name, int mode, const libmds_identity_t *who,
                    libmds_ino_t *ino, struct stat *st);

  /**
   * Create a subdirectory in the parent directory.
   *
   * @param mds       The libmds object returned by libmds_init()
   * @param parent    Fileid of the parent directory
   * @param name      Filename of the new directory entry
   * @param mode      Mode bits to set on the created file
   * @param who       User identity
   * @param[out] ino  Inode number of the created file
   * @param[out] st   Attributes of the created file
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if the parent does not exist.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -EEXIST if the parent directory already has an entry with \a name.
   */
  int libmds_mkdir(struct libmds *mds, const libmds_fileid_t *parent,
                   const char *name, int mode, const libmds_identity_t *who,
                   libmds_ino_t *ino, struct stat *st);

  /**
   * Create a link in the parent directory.
   *
   * @param mds     The libmds object returned by libmds_init()
   * @param parent  Fileid of the parent directory
   * @param name    Filename of the new directory entry
   * @param ino     Inode number of the file to link
   * @param[out] st New attributes of the file
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if the parent does not exist.
   * @retval -EEXIST if the parent directory already has an entry with \a name.
   */
  int libmds_link(struct libmds *mds, const libmds_fileid_t *parent,
                  const char *name, libmds_ino_t ino, struct stat *st);

  /**
   * Create a symbolic link in the parent directory.
   *
   * @param mds       The libmds object returned by libmds_init()
   * @param parent    Fileid of the parent directory
   * @param name      Filename of the new directory entry
   * @param target    Path of the target to link
   * @param who       User identity
   * @param[out] ino  Inode number of the created file
   * @param[out] st   Attributes of the created file
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if the parent does not exist.
   * @retval -EEXIST if the parent directory already has an entry with \a name.
   */
  int libmds_symlink(struct libmds *mds, const libmds_fileid_t *parent,
                     const char *name, const char *target,
                     const libmds_identity_t *who, libmds_ino_t *ino,
                     struct stat *st);

  /**
   * Read the contents of a symbolic link.
   *
   * @param mds       The libmds object returned by libmds_init()
   * @param file      Fileid of the symbolic link
   * @param[out] buf  Buffer to receive the link contents
   * @param buf_len   Length of the provided buffer
   *
   * @return Returns number of bytes copied on success, or a negative error code.
   * @retval -ENODEV if the volume does not exist.
   * @retval -ENOENT if the given file does not exist.
   */
  int libmds_readlink(struct libmds *mds, const libmds_fileid_t *parent,
                      char *buf, int buf_len);

  /**
   * Rename a directory entry.
   *
   * @param mds         The libmds object returned by libmds_init()
   * @param src_parent  Fileid of the initial parent directory
   * @param src_name    Filename of the initial directory entry
   * @param dst_parent  Fileid of the destination parent directory
   * @param dst_name    Filename of the destination directory entry
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if either parent volume does not exist.
   * @retval -ENOENT if either parent directory does not exist.
   * @retval -EEXIST if dst_parent already has an entry with \a dst_name.
   * @retval -EXDEV if the directories are located on different filesystems.
   */
  int libmds_rename(struct libmds *mds,
                    const libmds_fileid_t *src_parent, const char *src_name,
                    const libmds_fileid_t *dst_parent, const char *dst_name);

  /**
   * Unlink the given file from the parent directory.
   *
   * @param mds     The libmds object returned by libmds_init()
   * @param parent  Fileid of the parent directory
   * @param name    Filename of the directory entry to remove
   *
   * @return Returns 0 on success, or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if the parent does not have an entry with \a name.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -ENOTEMPTY if the entry is a non-empty directory.
   */
  int libmds_unlink(struct libmds *mds, const libmds_fileid_t *parent,
                    const char *name);

  /**
   * Find an entry in the parent directory.
   *
   * @param mds      The libmds object returned by libmds_init()
   * @param parent   Fileid of the parent directory
   * @param name     Filename of the directory entry to find
   * @param[out] ino Inode number of the directory entry found
   *
   * @return Returns 0 on success and sets \a ino or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if the parent does not have an entry with \a name.
   * @retval -ENOTDIR if the parent is not a directory.
   */
  int libmds_lookup(struct libmds *mds, const libmds_fileid_t *parent,
                    const char *name, libmds_ino_t *ino);

  /**
   * List the entries of a directory.
   *
   * @param mds    The libmds object returned by libmds_init()
   * @param dir    Fileid of the directory
   * @param pos    Index of the first directory entry to list
   * @param gen    Generation number to detect changes during listing
   * @param cb     Callback function to receive each directory entry
   * @param user   User data passed as last argument to \a cb
   *
   * @return Returns 0 on success or a negative error code.
   * @retval -ENODEV if the parent volume does not exist.
   * @retval -ENOENT if a file with inode number \a dir does not exist.
   * @retval -ENOTDIR if the parent is not a directory.
   * @retval -EOF if \a pos is past the end of the directory.
   * @retval -ESTALE if \a gen doesn't match current directory.
   */
  int libmds_readdir(struct libmds *mds, const libmds_fileid_t *dir,
                     uint64_t pos, uint64_t gen,
                     libmds_readdir_fn cb, void *user);

  /**
   * Query the attributes of a file.
   *
   * @param mds     The libmds object returned by libmds_init()
   * @param file    Fileid of the file
   * @param[out] st Pointer to the attributes to get
   *
   * @return Returns 0 on success or a negative error code.
   * @retval -ENODEV if the volume does not exist.
   * @retval -ENOENT if the given file does not exist.
   */
  int libmds_getattr(struct libmds *mds, const libmds_fileid_t *file,
                     struct stat *st);

  /**
   * Set the attributes of a file.
   *
   * @param mds   The libmds object returned by libmds_init()
   * @param file  Fileid of the file
   * @param mask  Bitmask of LIBMDS_ATTR_ values to set
   * @param st    Pointer to the attributes to set
   *
   * @return Returns 0 on success or a negative error code.
   * @retval -ENODEV if the volume does not exist.
   * @retval -ENOENT if the given file does not exist.
   */
  int libmds_setattr(struct libmds *mds, const libmds_fileid_t *file,
                     int mask, const struct stat *st);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* LIBCEPH_MDS_H */
