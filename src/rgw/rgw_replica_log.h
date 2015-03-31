// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 */

#ifndef RGW_REPLICA_LOG_H_
#define RGW_REPLICA_LOG_H_

#include <string>
#include "cls/replica_log/cls_replica_log_types.h"
#include "include/types.h"
#include "include/ceph_time.h"
#include "include/rados/librados.hpp"
#include "rgw_common.h"

class RGWRados;
class CephContext;

#define META_REPLICA_LOG_OBJ_PREFIX "meta.replicalog."
#define DATA_REPLICA_LOG_OBJ_PREFIX "data.replicalog."

typedef cls_replica_log_item_marker RGWReplicaItemMarker;
typedef cls_replica_log_progress_marker RGWReplicaProgressMarker;

struct RGWReplicaBounds {
  string marker;
  ceph::real_time oldest_time;
  list<RGWReplicaProgressMarker> markers;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *oid);
};

class RGWReplicaLogger {
protected:
  CephContext *cct;
  RGWRados *store;
  int open_ioctx(librados::IoCtx& ctx, const string& pool);

  RGWReplicaLogger(RGWRados *_store);

  int update_bound(const string& oid_t, const string& pool,
		   const string& daemon_id, const string& marker,
		   const ceph::real_time& time,
		   const list<RGWReplicaItemMarker> *entries);
  int delete_bound(const string& oid_t, const string& pool,
		   const string& daemon_id);
  int get_bounds(const string& oid_t, const string& pool,
		 RGWReplicaBounds& bounds);
};

class RGWReplicaObjectLogger : private RGWReplicaLogger {
  string pool;
  string prefix;

  void get_shard_oid(int id, string& oid_t) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", id);
    oid_t = prefix + buf;
  }

public:
  RGWReplicaObjectLogger(RGWRados *_store,
		const string& _pool,
		const string& _prefix);

  int create_log_objects(int shards);
  int update_bound(int shard, const string& daemon_id, const string& marker,
		   const ceph::real_time& time,
		   const list<RGWReplicaItemMarker> *entries) {
    string oid_t;
    get_shard_oid(shard, oid_t);
    return RGWReplicaLogger::update_bound(oid_t, pool,
					  daemon_id, marker, time, entries);
  }
  int delete_bound(int shard, const string& daemon_id) {
    string oid_t;
    get_shard_oid(shard, oid_t);
    return RGWReplicaLogger::delete_bound(oid_t, pool,
					  daemon_id);
  }
  int get_bounds(int shard, RGWReplicaBounds& bounds) {
    string oid_t;
    get_shard_oid(shard, oid_t);
    return RGWReplicaLogger::get_bounds(oid_t, pool, bounds);
  }
};

class RGWReplicaBucketLogger : private RGWReplicaLogger {
  string pool;
  string prefix;
public:
  RGWReplicaBucketLogger(RGWRados *_store);
  int update_bound(const rgw_bucket& bucket, const string& daemon_id,
		   const string& marker, const ceph::real_time& time,
		   const list<RGWReplicaItemMarker> *entries) {
    return RGWReplicaLogger::update_bound(prefix+bucket.name, pool,
					  daemon_id, marker, time, entries);
  }
  int delete_bound(const rgw_bucket& bucket, const string& daemon_id) {
    return RGWReplicaLogger::delete_bound(prefix+bucket.name, pool,
					  daemon_id);
  }
  int get_bounds(const rgw_bucket& bucket, RGWReplicaBounds& bounds) {
    return RGWReplicaLogger::get_bounds(prefix+bucket.name, pool,
					bounds);
  }
};

#endif /* RGW_REPLICA_LOG_H_ */
