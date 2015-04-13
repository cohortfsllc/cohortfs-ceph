// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 * Copyright 2013 Inktank
 */

#ifndef CLS_REPLICA_LOG_CLIENT_H_
#define CLS_REPLICA_LOG_CLIENT_H_

#include "cls_replica_log_types.h"

/**
 * Prepare a progress marker object to send out.
 *
 * @param progress The marker object to prepare
 * @param entity The ID of the entity setting the progress
 * @param marker The marker key the entity has gotten to
 * @param time The timestamp associated with the marker
 * param entries A list of in-progress entries prior to the marker
 */
void cls_replica_log_prepare_marker(
	cls_replica_log_progress_marker& progress,
	const string& entity, const string& marker,
	const ceph::real_time& time,
	const list<pair<string, ceph::real_time> > *entries);

/**
 * Extract a progress marker object into its components.
 *
 * @param progress The marker object to extract data from
 * @param entity [out] The ID of the entity the progress is associated with
 * @param marker [out] The marker key the entity has gotten to
 * @param time [out] The timestamp associated with the marker
 * @param entries [out] List of in-progress entries prior to the marker
 */
void cls_replica_log_extract_marker(
  const cls_replica_log_progress_marker& progress,
  string& entity, string& marker, ceph::real_time& time,
  list<pair<string, ceph::real_time> >& entries);

/**
 * Add a progress marker update to a write op. The op will return 0 on
 * success, -EEXIST if the marker conflicts with an existing one, or
 * -EINVAL if the marker is in conflict (ie, before) the daemon's existing
 * marker.
 *
 * @param op The op to add the update to
 * @param progress The progress marker to send
 */
void cls_replica_log_update_bound(rados::ObjOpUse op,
				  const cls_replica_log_progress_marker& progress);

/**
 * Remove an entity's progress marker from the replica log. The op will return
 * 0 on success, -ENOENT if the entity does not exist on the replica log, or
 * -ENOTEMPTY if the items list on the marker is not empty.
 *
 * @param op The op to add the delete to
 * @param entity The entity whose progress should be removed
 */
void cls_replica_log_delete_bound(rados::ObjOpUse op,
				  const string& entity);

/**
 * Read the bounds on a replica log.
 *
 */
int cls_replica_log_get_bounds(
  rados::Objecter* objecter, const VolumeRef& vol, const oid_t& oid,
  string& position_marker, ceph::real_time& oldest_time,
  list<cls_replica_log_progress_marker>& markers);

#endif /* CLS_REPLICA_LOG_CLIENT_H_ */
