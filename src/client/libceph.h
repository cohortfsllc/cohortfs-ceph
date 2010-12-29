#ifndef CEPH_LIB_H
#define CEPH_LIB_H
#define __USE_UNIX98 1
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <sys/statvfs.h>
#include <utime.h>
#include <stdbool.h>
#include <unistd.h>
#include <dirent.h>
#include <stdint.h>
#include <errno.h>

#define CEPH_SETATTR_MODE   1
#define CEPH_SETATTR_UID    2
#define CEPH_SETATTR_GID    4
#define CEPH_SETATTR_MTIME  8
#define CEPH_SETATTR_ATIME 16
#define CEPH_SETATTR_SIZE  32
#define CEPH_SETATTR_CTIME 64

#ifndef __cplusplus
#define CEPH_INO_ROOT  1
#define CEPH_NOSNAP  ((uint64_t)(-2))
#endif /* __cplusplus */

struct stat_precise {
  ino_t st_ino;
  dev_t st_dev;
  mode_t st_mode;
  nlink_t st_nlink;
  uid_t st_uid;
  gid_t st_gid;
  dev_t st_rdev;
  off_t st_size;
  blksize_t st_blksize;
  blkcnt_t st_blocks;
  time_t st_atime_sec;
  time_t st_atime_micro;
  time_t st_mtime_sec;
  time_t st_mtime_micro;
  time_t st_ctime_sec;
  time_t st_ctime_micro;
};

/* Import these definitions into the land of C */

#ifdef __cplusplus
#include "Client.h"
#else /* !__cplusplus */

struct ceph_file_layout {
	/* file -> object mapping */
	uint32_t fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple
				      of page size. */
	uint32_t fl_stripe_count;    /* over this many objects */
	uint32_t fl_object_size;     /* until objects are this big, then move to
				      new objects */
	uint32_t fl_cas_hash;        /* 0 = none; 1 = sha256 */

	/* pg -> disk layout */
	uint32_t fl_object_stripe_unit;  /* for per-object parity, if any */

	/* object -> pg layout */
	uint32_t fl_pg_preferred; /* preferred primary for pg (-1 for none) */
	uint32_t fl_pg_pool;      /* namespace, crush ruleset, rep level */
} __attribute__ ((packed));


typedef struct _inodeno_t {
  uint64_t val;
} inodeno_t;

typedef struct _snapid_t {
  uint64_t val;
} snapid_t;

typedef struct __vinodeno {
  inodeno_t ino;
  snapid_t snapid;
  } vinodeno_t;

typedef struct Fh Fh;
#endif /* __cplusplus */

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

const char *ceph_version(int *major, int *minor, int *patch);

int ceph_initialize(int argc, const char **argv);
void ceph_deinitialize();

int ceph_mount();
int ceph_umount();

int ceph_statfs(const char *path, struct statvfs *stbuf);
int ceph_get_local_osd();

int ceph_getcwd(char *buf, int buflen);
int ceph_chdir(const char *s);

int ceph_opendir(const char *name, DIR **dirpp);
int ceph_closedir(DIR *dirp);
int ceph_readdir_r(DIR *dirp, struct dirent *de);
int ceph_readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask);
int ceph_getdents(DIR *dirp, char *name, int buflen);
int ceph_getdnames(DIR *dirp, char *name, int buflen);
void ceph_rewinddir(DIR *dirp); 
loff_t ceph_telldir(DIR *dirp);
void ceph_seekdir(DIR *dirp, loff_t offset);

int ceph_link (const char *existing, const char *newname);
int ceph_unlink (const char *path);
int ceph_rename(const char *from, const char *to);

// dirs
int ceph_mkdir(const char *path, mode_t mode);
int ceph_mkdirs(const char *path, mode_t mode);
int ceph_rmdir(const char *path);

// symlinks
int ceph_readlink(const char *path, char *buf, loff_t size);
int ceph_symlink(const char *existing, const char *newname);

// inode stuff
int ceph_lstat(const char *path, struct stat *stbuf);
int ceph_lstat_precise(const char *path, struct stat_precise *stbuf);

int ceph_setattr(const char *relpath, struct stat *attr, int mask);
int ceph_setattr_precise (const char *relpath, struct stat_precise *stbuf, int mask);
int ceph_chmod(const char *path, mode_t mode);
int ceph_chown(const char *path, uid_t uid, gid_t gid);
int ceph_utime(const char *path, struct utimbuf *buf);
int ceph_truncate(const char *path, loff_t size);

// file ops
#ifdef __cplusplus
int ceph_mknod(const char *path, mode_t mode, dev_t rdev=0);
int ceph_open(const char *path, int flags, mode_t mode=0);
int ceph_read(int fd, char *buf, loff_t size, loff_t offset=-1);
int ceph_write(int fd, const char *buf, loff_t size, loff_t offset=-1);
#else /* !__cplusplus */
int ceph_mknod(const char *path, mode_t mode, dev_t rdev);
int ceph_open(const char *path, int flags, mode_t mode);
int ceph_read(int fd, char *buf, loff_t size, loff_t offset);
int ceph_write(int fd, const char *buf, loff_t size, loff_t offset);
#endif /* __cpluscplus */
int ceph_close(int fd);
loff_t ceph_lseek(int fd, loff_t offset, int whence);
int ceph_ftruncate(int fd, loff_t size);
int ceph_fsync(int fd, bool syncdataonly);
int ceph_fstat(int fd, struct stat *stbuf);

int ceph_sync_fs();
int ceph_get_file_stripe_unit(int fh);
int ceph_get_file_replication(const char *path);
int ceph_get_default_preferred_pg(int fd);
int ceph_get_file_stripe_address(int fd, loff_t offset, char *buf, int buflen);
int ceph_set_default_file_stripe_unit(int stripe);
int ceph_set_default_file_stripe_count(int count);
int ceph_set_default_object_size(int size);
int ceph_set_default_file_replication(int replication);
int ceph_set_default_preferred_pg(int pg);

/* Low Level */

int ceph_ll_lookup(vinodeno_t parent, const char *name,
		   struct stat *attr, int uid, int gid);
int ceph_ll_lookup_precise(vinodeno_t parent, const char *name, struct stat_precise *attr, int uid, int gid);
bool ceph_ll_forget(vinodeno_t vino, int count);
int ceph_ll_walk(const char *name, struct stat *attr);
int ceph_ll_walk_precise(const char *name, struct stat_precise *attr);
int ceph_ll_getattr(vinodeno_t vi, struct stat *attr, int uid, int gid);
int ceph_ll_setattr(vinodeno_t vi, struct stat *st, int mask, int uid, int gid);
int ceph_ll_getattr_precise(vinodeno_t vi, struct stat_precise *attr, int uid, int gid);
int ceph_ll_setattr_precise(vinodeno_t vi, struct stat_precise *st, int mask, int uid, int gid);
int ceph_ll_open(vinodeno_t vi, int flags, Fh **filehandle, int uid, int gid);
loff_t ceph_ll_lseek(Fh* filehandle, loff_t offset, int whence);
int ceph_ll_read(Fh* filehandle, int64_t off, uint64_t len, char* buf);
int ceph_ll_fsync(Fh *fh, int syncdataonly);
int ceph_ll_write(Fh* filehandle, int64_t off, uint64_t len, const char *data);
int ceph_ll_close(Fh* filehandle);
int ceph_ll_getxattr(vinodeno_t vino, const char *name, void *value, size_t size, int uid, int gid);
int ceph_ll_lenxattr_by_idx(vinodeno_t vino, unsigned idx, int uid, int gid);
int ceph_ll_getxattr_by_idx(vinodeno_t vino, int idx, void *value,
			    size_t size, int uid, int gid);
int ceph_ll_getxattridx(vinodeno_t vino, const char *name, int uid,
			int gid);
int ceph_ll_listxattr_chunks(vinodeno_t vino, char *names, size_t size,
			     int *cookie, int *eol, int uid, int gid);
int ceph_ll_setxattr(vinodeno_t vino, const char *name,
		     const void *value, size_t size,
		     int flags, int uid, int gid);
int ceph_ll_setxattr_by_idx(vinodeno_t vino, int idx, const void *value,
			    size_t size, int flags, int uid, int gid);
int ceph_ll_removexattr(vinodeno_t vino, const char *name, int uid, int gid);
int ceph_ll_removexattr_by_idx(vinodeno_t vino, int idx, int uid, int gid);
int ceph_ll_create(vinodeno_t parent, const char *name, mode_t mode,
		   int flags, Fh **filehandle, struct stat *attr, int uid, int gid);
int ceph_ll_create_precise(vinodeno_t parent, const char *name, mode_t mode,
			   int flags, Fh **filehandle,
			   struct stat_precise *attr, int uid, int gid);
int ceph_ll_mkdir(vinodeno_t parent, const char *name,
		  mode_t mode, struct stat *attr, int uid, int gid);
int ceph_ll_mkdir_precise(vinodeno_t parent, const char *name,
			  mode_t mode, struct stat_precise *attr,
			  int uid, int gid);
int ceph_ll_link(vinodeno_t obj, vinodeno_t newparrent,
		 const char *name, struct stat *attr,
		 int uid, int gid);
int ceph_ll_link_precise(vinodeno_t obj, vinodeno_t newparrent,
			 const char *name, struct stat_precise *attr,
			 int uid, int gid);
int ceph_ll_truncate(vinodeno_t obj, uint64_t length, int uid, int gid);
int ceph_ll_opendir(vinodeno_t vino, void **dirpp, int uid, int gid);
int ceph_ll_readdir(DIR* d, struct dirent *de, struct stat *st);
int ceph_ll_releasedir(DIR* dir);
int ceph_ll_rename(vinodeno_t parent, const char *name,
		   vinodeno_t newparent, const char *newname,
		   int uid, int gid);
int ceph_ll_unlink(vinodeno_t vino, const char *name, int uid, int gid);
int ceph_ll_statfs(vinodeno_t vino, struct statvfs *stbuf);
int ceph_ll_readlink(vinodeno_t vino, char **value, int uid, int gid);
int ceph_ll_symlink(vinodeno_t parent, const char *name, const char *value, struct stat *attr, int uid, int gid);
int ceph_ll_symlink_precise(vinodeno_t parent, const char *name, const char *value, struct stat_precise *attr, int uid, int gid);
int ceph_ll_rmdir(vinodeno_t vino, const char *name, int uid, int gid);
uint32_t ceph_ll_stripe_unit(vinodeno_t vino);
uint32_t ceph_ll_file_layout(vinodeno_t vino, struct ceph_file_layout *layout);
uint64_t ceph_ll_snap_seq(vinodeno_t vino);
int ceph_ll_get_stripe_osd(vinodeno_t vino, uint64_t blockno,
			     struct ceph_file_layout* layout);
int ceph_ll_num_osds(void);
int ceph_ll_osdaddr(int osd, char* buf, size_t size);
uint64_t ceph_ll_get_internal_offset(vinodeno_t vino, uint64_t blockno);
uint64_t ceph_ll_get_crc32(vinodeno_t vino, uint64_t blockid,
			   uint32_t* crc32,
			   struct ceph_file_layout* layout);
uint64_t ceph_ll_read_block(vinodeno_t vino, uint64_t blockid, char* bl,
		       uint64_t offset, uint64_t length,
		       struct ceph_file_layout* layout);
int ceph_ll_write_block(vinodeno_t vino, uint64_t blockid,
			char* buf, uint64_t offset,
			uint64_t length, struct ceph_file_layout* layout,
			uint64_t snapseq);
#ifdef __cplusplus
}
#endif

#endif
