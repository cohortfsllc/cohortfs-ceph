// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MDS_SNAP_H
#define CEPH_MDS_SNAP_H

#include "mdstypes.h"
#include "common/snap_types.h"

/*
 * generic snap descriptor.
 */
struct SnapInfo {
  snapid_t snapid;
  utime_t stamp;
  string name;

  string long_name; ///< cached _$ino_$name
  
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapInfo*>& ls);
};
WRITE_CLASS_ENCODER(SnapInfo)

ostream& operator<<(ostream& out, const SnapInfo &sn);


/*
 * SnapRealm - a subtree that shares the same set of snapshots.
 */
struct SnapRealm;
struct CapabilityGroup;
class CInode;
class MDCache;
struct MDRequest;



#include "Capability.h"

struct snaplink_t {
  inodeno_t ino;
  snapid_t first;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<snaplink_t*>& ls);
};
WRITE_CLASS_ENCODER(snaplink_t)

ostream& operator<<(ostream& out, const snaplink_t &l);


// carry data about a specific version of a SnapRealm
struct sr_t {
  snapid_t seq;                     // basically, a version/seq # for changes to _this_ realm.
  snapid_t created;                 // when this realm was created.
  snapid_t last_created;            // last snap created in _this_ realm.
  snapid_t last_destroyed;          // seq for last removal
  map<snapid_t, SnapInfo> snaps;

  sr_t() : seq(0), created(0), last_created(0), last_destroyed(0) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<sr_t*>& ls);
};
WRITE_CLASS_ENCODER(sr_t);

#endif
