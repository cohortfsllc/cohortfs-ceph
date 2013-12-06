// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_SNAPREALM_H
#define CEPH_CLIENT_SNAPREALM_H

#include "include/types.h"
#include "common/snap_types.h"
#include "include/xlist.h"

#include "SnapRealmMap.h"

class CapObject;
class Inode;

struct SnapRealm {
 private:
  SnapRealmMap *container;
  int nref; // remove from container on nref->0

 public:
  inodeno_t ino;
  snapid_t created;
  snapid_t seq;
  
  inodeno_t parent;
  snapid_t parent_since;
  vector<snapid_t> prior_parent_snaps;  // snaps prior to parent_since
  vector<snapid_t> my_snaps;

  SnapRealm *pparent;
  set<SnapRealm*> pchildren;

private:
  SnapContext cached_snap_context;  // my_snaps + parent snaps + past_parent_snaps
  friend ostream& operator<<(ostream& out, const SnapRealm& r);

public:
  xlist<CapObject*> inodes_with_caps;

  SnapRealm(SnapRealmMap *container, inodeno_t i) :
    container(container), nref(0), ino(i), created(0), seq(0),
    pparent(NULL) { }

  int get_num_refs() const { return nref; }
  void get() {
    ++nref;
  }
  void put() {
    if (--nref == 0) {
      container->remove(ino);
      delete this;
    }
  }

  void build_snap_context();
  void invalidate_cache() {
    cached_snap_context.clear();
  }

  const SnapContext& get_snap_context() {
    if (cached_snap_context.seq == 0)
      build_snap_context();
    return cached_snap_context;
  }

  void dump(Formatter *f) const;
};

inline ostream& operator<<(ostream& out, const SnapRealm& r) {
  return out << "snaprealm(" << r.ino << " nref=" << r.nref << " c=" << r.created << " seq=" << r.seq
	     << " parent=" << r.parent
	     << " my_snaps=" << r.my_snaps
	     << " cached_snapc=" << r.cached_snap_context
	     << ")";
}

#endif
