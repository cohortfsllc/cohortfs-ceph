/*
 * ceph_fs.cc - Some Ceph functions that are shared between kernel space and
 * user space.
 *
 */

#include <errno.h>

/*
 * Some non-inline ceph helpers
 */
#include "include/types.h"


int ceph_flags_to_mode(int flags)
{
	int mode = 0;

#ifdef O_DIRECTORY  /* fixme */
	if ((flags & O_DIRECTORY) == O_DIRECTORY)
		return CEPH_FILE_MODE_PIN;
#endif

	switch (flags & O_ACCMODE) {
	case O_WRONLY:
		mode = CEPH_FILE_MODE_WR;
		break;
	case O_RDONLY:
		mode = CEPH_FILE_MODE_RD;
		break;
	case O_RDWR:
	case O_ACCMODE: /* this is what the VFS does */
		mode = CEPH_FILE_MODE_RDWR;
		break;
	}

	return mode;
}

int ceph_caps_for_mode(int mode)
{
	int caps = CEPH_CAP_PIN;

	if (mode & CEPH_FILE_MODE_RD)
		caps |= CEPH_CAP_FILE_SHARED |
			CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE;
	if (mode & CEPH_FILE_MODE_WR)
		caps |= CEPH_CAP_FILE_EXCL |
			CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER |
			CEPH_CAP_AUTH_SHARED | CEPH_CAP_AUTH_EXCL |
			CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
	if (mode & CEPH_FILE_MODE_LAZY)
		caps |= CEPH_CAP_FILE_LAZYIO;

	return caps;
}
