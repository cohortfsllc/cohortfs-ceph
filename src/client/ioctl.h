#ifndef FS_CEPH_IOCTL_H
#define FS_CEPH_IOCTL_H

#if defined(__linux__)
#include <linux/ioctl.h>
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/ioctl.h>
#include <sys/types.h>
#endif

#define CEPH_IOCTL_MAGIC 0x97

/*
 * Extract identity, address of the OSD and object storing a given
 * file offset.
 */
struct ceph_ioctl_dataloc {
	uint64_t file_offset;		/* in+out: file offset */
	uint64_t object_offset;		/* out: offset in object */
	uint64_t object_no;		/* out: object # */
	uint64_t object_size;		/* out: object size */
	char object_name[64];	     /* out: object name */
	uint64_t block_offset;		/* out: offset in block */
	uint64_t block_size;		/* out: block length */
	int64_t osd;		       /* out: osd # */
	struct sockaddr_storage osd_addr; /* out: osd address */
};

#define CEPH_IOC_GET_DATALOC _IOWR(CEPH_IOCTL_MAGIC, 3,	\
				   struct ceph_ioctl_dataloc)

#define CEPH_IOC_LAZYIO _IO(CEPH_IOCTL_MAGIC, 4)

#endif
