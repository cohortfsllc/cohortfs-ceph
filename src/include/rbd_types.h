/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RBD_TYPES_H
#define CEPH_RBD_TYPES_H

#if defined(__linux__)
#include <linux/types.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#endif

#include "rbd/features.h"

/* New-style rbd image 'foo' consists of objects
 *   rbd_id.foo              - id of image
 *   rbd_header.<id>         - image metadata
 *   rbd_data.<id>.00000000
 *   rbd_data.<id>.00000001
 *   ...                     - data
 */

#define RBD_HEADER_PREFIX      "rbd_header."
#define RBD_DATA_PREFIX        "rbd_data."
#define RBD_ID_PREFIX          "rbd_id."

/*
 * old-style rbd image 'foo' consists of objects
 *   foo.rbd      - image metadata
 *   rb.<idhi>.<idlo>.00000000
 *   rb.<idhi>.<idlo>.00000001
 *   ...          - data
 */

#define RBD_SUFFIX	 	".rbd"
#define RBD_DIRECTORY           "rbd_directory"
#define RBD_INFO                "rbd_info"

#define RBD_LOCK_NAME		"rbd_lock"

#define RBD_DEFAULT_OBJ_ORDER	22   /* 4MB */

#define RBD_MAX_OBJ_NAME_SIZE	96
#define RBD_MAX_BLOCK_NAME_SIZE 24

#define RBD_COMP_NONE		0
#define RBD_CRYPT_NONE		0

#define RBD_HEADER_TEXT		"<<< Rados Block Device Image >>>\n"
#define RBD_HEADER_SIGNATURE	"RBD"
#define RBD_HEADER_VERSION	"001.005"

struct rbd_info {
	__le64 max_id;
} __attribute__ ((packed));


struct rbd_obj_header_ondisk {
	char text[40];
	char block_name[RBD_MAX_BLOCK_NAME_SIZE];
	char signature[4];
	char version[8];
	struct {
		__u8 order;
	} __attribute__((packed)) options;
	__le64 image_size;
} __attribute__((packed));


#endif
