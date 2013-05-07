// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004- Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "snap.h"
#include "MDCache.h"
#include "MDS.h"

#include "messages/MClientSnap.h"

/*
 * SnapRealm
 */

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mdcache->mds->get_nodeid(), srnode.seq, this)
static ostream& _prefix(std::ostream *_dout, int whoami,
			uint64_t seq, SnapRealm *realm) {
  return *_dout << " mds." << whoami
      << ".cache.snaprealm(seq " << seq << " " << realm << ") ";
}

ostream& operator<<(ostream& out, const SnapRealm& realm) 
{
  out << "snaprealm(seq " << realm.srnode.seq
      << " lc " << realm.srnode.last_created
      << " cr " << realm.srnode.created;
  out << " snaps=" << realm.srnode.snaps;
  out << " " << &realm << ")";
  return out;
}


// get list of snaps for this realm
void SnapRealm::build_snap_set(set<snapid_t> &s, snapid_t& max_seq,
                               snapid_t& max_last_created, snapid_t& max_last_destroyed,
			       snapid_t first, snapid_t last)
{
  dout(10) << "build_snap_set [" << first << "," << last << "] on " << *this << dendl;

  if (srnode.seq > max_seq)
    max_seq = srnode.seq;
  if (srnode.last_created > max_last_created)
    max_last_created = srnode.last_created;
  if (srnode.last_destroyed > max_last_destroyed)
    max_last_destroyed = srnode.last_destroyed;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       p++)
    s.insert(p->first);
}


void SnapRealm::check_cache()
{
  if (cached_seq >= srnode.seq)
    return;

  cached_snaps.clear();
  cached_snap_context.clear();

  cached_last_created = srnode.last_created;
  cached_last_destroyed = srnode.last_destroyed;
  cached_seq = srnode.seq;
  build_snap_set(cached_snaps, cached_seq, cached_last_created, cached_last_destroyed,
		 0, CEPH_NOSNAP);

  cached_snap_trace.clear();
  build_snap_trace(cached_snap_trace);
  
  dout(10) << "check_cache rebuilt " << cached_snaps
	   << " seq " << srnode.seq
	   << " cached_seq " << cached_seq
	   << " cached_last_created " << cached_last_created
	   << " cached_last_destroyed " << cached_last_destroyed
	   << ")" << dendl;
}

const set<snapid_t>& SnapRealm::get_snaps()
{
  check_cache();
  dout(10) << "get_snaps " << cached_snaps
	   << " (seq " << srnode.seq << " cached_seq " << cached_seq << ")"
	   << dendl;
  return cached_snaps;
}

/*
 * build vector in reverse sorted order
 */
const SnapContext& SnapRealm::get_snap_context()
{
  check_cache();

  if (!cached_snap_context.seq) {
    cached_snap_context.seq = cached_seq;
    cached_snap_context.snaps.assign(cached_snaps.rbegin(),
                                     cached_snaps.rend());
  }

  return cached_snap_context;
}

void SnapRealm::get_snap_info(map<snapid_t,SnapInfo*>& infomap, snapid_t first, snapid_t last)
{
  const set<snapid_t>& snaps = get_snaps();
  dout(10) << "get_snap_info snaps " << snaps << dendl;

  // include my snaps within interval [first,last]
  for (map<snapid_t, SnapInfo>::iterator p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       p++)
    infomap[p->first] = &p->second;
}

const string& SnapRealm::get_snapname(snapid_t snapid)
{
  map<snapid_t, SnapInfo>::const_iterator s = srnode.snaps.find(snapid);
  assert(s != srnode.snaps.end());
  return s->second.name;
}

snapid_t SnapRealm::resolve_snapname(const string& n, snapid_t first, snapid_t last)
{
  // first try me
  dout(10) << "resolve_snapname '" << n << "' in [" << first << "," << last << "]" << dendl;

  for (map<snapid_t, SnapInfo>::iterator p = srnode.snaps.lower_bound(first); // first element >= first
       p != srnode.snaps.end() && p->first <= last;
       p++) {
    dout(15) << " ? " << p->second << dendl;
    if (p->second.name == n)
      return p->first;
  }

  return 0;
}

const bufferlist& SnapRealm::get_snap_trace()
{
  check_cache();
  return cached_snap_trace;
}

void SnapRealm::build_snap_trace(bufferlist& snapbl)
{
  SnapRealmInfo info(MDS_INO_ROOT, srnode.created, srnode.seq, 0);

  info.h.parent = 0;

  info.my_snaps.reserve(srnode.snaps.size());
  for (map<snapid_t,SnapInfo>::reverse_iterator p = srnode.snaps.rbegin();
       p != srnode.snaps.rend();
       p++)
    info.my_snaps.push_back(p->first);

  dout(10) << "build_snap_trace my_snaps " << info.my_snaps << dendl;

  ::encode(info, snapbl);
}

