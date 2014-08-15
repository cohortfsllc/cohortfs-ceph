/*
 * Copyright (C) 2007 Oracle.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License v2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA.
 */

#ifndef __IOCTL_
#define __IOCTL_

#if defined(__linux__)
#include <linux/ioctl.h>
#elif defined(__FreeBSD__)
#include <sys/ioctl.h>
#endif

#define BTRFS_IOCTL_MAGIC 0x94
#define BTRFS_VOL_NAME_MAX 255

/* this should be 4k */
#define BTRFS_PATH_NAME_MAX 4087
struct btrfs_ioctl_vol_args {
	int64_t fd;
	char name[BTRFS_PATH_NAME_MAX + 1];
};

#define BTRFS_SUBVOL_CREATE_ASYNC	(1ULL << 0)

#define BTRFS_SUBVOL_NAME_MAX 4039
struct btrfs_ioctl_vol_args_v2 {
	int64_t fd;
	uint64_t transid;
	uint64_t flags;
	uint64_t unused[4];
	char name[BTRFS_SUBVOL_NAME_MAX + 1];
};

#define BTRFS_INO_LOOKUP_PATH_MAX 4080
struct btrfs_ioctl_ino_lookup_args {
	uint64_t treeid;
	uint64_t objectid;
	char name[BTRFS_INO_LOOKUP_PATH_MAX];
};

struct btrfs_ioctl_search_key {
	/* which root are we searching.	 0 is the tree of tree roots */
	uint64_t tree_id;

	/* keys returned will be >= min and <= max */
	uint64_t min_objectid;
	uint64_t max_objectid;

	/* keys returned will be >= min and <= max */
	uint64_t min_offset;
	uint64_t max_offset;

	/* max and min transids to search for */
	uint64_t min_transid;
	uint64_t max_transid;

	/* keys returned will be >= min and <= max */
	uint32_t min_type;
	uint32_t max_type;

	/*
	 * how many items did userland ask for, and how many are we
	 * returning
	 */
	uint32_t nr_items;

	/* align to 64 bits */
	uint32_t unused;

	/* some extra for later */
	uint64_t unused1;
	uint64_t unused2;
	uint64_t unused3;
	uint64_t unused4;
};

struct btrfs_ioctl_search_header {
	uint64_t transid;
	uint64_t objectid;
	uint64_t offset;
	uint32_t type;
	uint32_t len;
};

#define BTRFS_SEARCH_ARGS_BUFSIZE (4096 - sizeof(struct btrfs_ioctl_search_key))
/*
 * the buf is an array of search headers where
 * each header is followed by the actual item
 * the type field is expanded to 32 bits for alignment
 */
struct btrfs_ioctl_search_args {
	struct btrfs_ioctl_search_key key;
	char buf[BTRFS_SEARCH_ARGS_BUFSIZE];
};

struct btrfs_ioctl_clone_range_args {
  int64_t src_fd;
  uint64_t src_offset, src_length;
  uint64_t dest_offset;
};

/* flags for the defrag range ioctl */
#define BTRFS_DEFRAG_RANGE_COMPRESS 1
#define BTRFS_DEFRAG_RANGE_START_IO 2

struct btrfs_ioctl_defrag_range_args {
	/* start of the defrag operation */
	uint64_t start;

	/* number of bytes to defrag, use (u64)-1 to say all */
	uint64_t len;

	/*
	 * flags for the operation, which can include turning
	 * on compression for this one defrag
	 */
	uint64_t flags;

	/*
	 * any extent bigger than this will be considered
	 * already defragged.  Use 0 to take the kernel default
	 * Use 1 to say every single extent must be rewritten
	 */
	uint32_t extent_thresh;

	/* spare for later */
	uint32_t unused[5];
};

struct btrfs_ioctl_space_info {
	uint64_t flags;
	uint64_t total_bytes;
	uint64_t used_bytes;
};

struct btrfs_ioctl_space_args {
	uint64_t space_slots;
	uint64_t total_spaces;
	struct btrfs_ioctl_space_info spaces[0];
};

#define BTRFS_IOC_SNAP_CREATE _IOW(BTRFS_IOCTL_MAGIC, 1, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_DEFRAG _IOW(BTRFS_IOCTL_MAGIC, 2, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_RESIZE _IOW(BTRFS_IOCTL_MAGIC, 3, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_SCAN_DEV _IOW(BTRFS_IOCTL_MAGIC, 4, \
				   struct btrfs_ioctl_vol_args)
/* trans start and trans end are dangerous, and only for
 * use by applications that know how to avoid the
 * resulting deadlocks
 */
#define BTRFS_IOC_TRANS_START  _IO(BTRFS_IOCTL_MAGIC, 6)
#define BTRFS_IOC_TRANS_END    _IO(BTRFS_IOCTL_MAGIC, 7)
#define BTRFS_IOC_SYNC	       _IO(BTRFS_IOCTL_MAGIC, 8)

#define BTRFS_IOC_CLONE	       _IOW(BTRFS_IOCTL_MAGIC, 9, int)
#define BTRFS_IOC_ADD_DEV _IOW(BTRFS_IOCTL_MAGIC, 10, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_RM_DEV _IOW(BTRFS_IOCTL_MAGIC, 11, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_BALANCE _IOW(BTRFS_IOCTL_MAGIC, 12, \
				   struct btrfs_ioctl_vol_args)

#define BTRFS_IOC_CLONE_RANGE _IOW(BTRFS_IOCTL_MAGIC, 13, \
				  struct btrfs_ioctl_clone_range_args)

#define BTRFS_IOC_SUBVOL_CREATE _IOW(BTRFS_IOCTL_MAGIC, 14, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_SNAP_DESTROY _IOW(BTRFS_IOCTL_MAGIC, 15, \
				struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_DEFRAG_RANGE _IOW(BTRFS_IOCTL_MAGIC, 16, \
				struct btrfs_ioctl_defrag_range_args)
#define BTRFS_IOC_TREE_SEARCH _IOWR(BTRFS_IOCTL_MAGIC, 17, \
				   struct btrfs_ioctl_search_args)
#define BTRFS_IOC_INO_LOOKUP _IOWR(BTRFS_IOCTL_MAGIC, 18, \
				   struct btrfs_ioctl_ino_lookup_args)
#define BTRFS_IOC_DEFAULT_SUBVOL _IOW(BTRFS_IOCTL_MAGIC, 19, u64)
#define BTRFS_IOC_SPACE_INFO _IOWR(BTRFS_IOCTL_MAGIC, 20, \
				    struct btrfs_ioctl_space_args)
#define BTRFS_IOC_START_SYNC _IOR(BTRFS_IOCTL_MAGIC, 24, uint64_t)
#define BTRFS_IOC_WAIT_SYNC  _IOW(BTRFS_IOCTL_MAGIC, 22, uint64_t)
#define BTRFS_IOC_SNAP_CREATE_V2 _IOW(BTRFS_IOCTL_MAGIC, 23, \
				   struct btrfs_ioctl_vol_args_v2)
#endif
