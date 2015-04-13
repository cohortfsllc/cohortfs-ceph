#ifndef CEPH_CLS_VERSION_CLIENT_H
#define CEPH_CLS_VERSION_CLIENT_H

#include "include/types.h"
#include "osdc/RadosClient.h"

/*
 * version objclass
 */

void cls_version_set(rados::ObjOpUse op, obj_version& ver);

/* increase anyway */
void cls_version_inc(rados::ObjOpUse op);

/* conditional increase, return -EAGAIN if condition fails */
void cls_version_inc(rados::ObjOpUse op, obj_version& ver, VersionCond cond);

void cls_version_read(rados::ObjOpUse op, obj_version *objv);

int cls_version_read(Objecter* o, VolumeRef vol, const oid_t& oid,
		     obj_version *ver);

void cls_version_check(rados::ObjOpUse op, obj_version& ver,
		       VersionCond cond);

#endif
