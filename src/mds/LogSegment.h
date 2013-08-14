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

#ifndef CEPH_LOGSEGMENT_H
#define CEPH_LOGSEGMENT_H

#include "include/dlist.h"
#include "include/elist.h"
#include "include/interval_set.h"
#include "include/Context.h"
#include "mdstypes.h"
#include "CDentry.h"
#include "CDirFrag.h"
#include "CInode.h"
#include "CDirStripe.h"

#include <ext/hash_set>
using __gnu_cxx::hash_set;

class MDS;
class MDSlaveUpdate;

class LogSegment {
 public:
  uint64_t offset, end;
  int num_events;
  uint64_t trimmable_at;

  // dirty items
  elist<CDirFrag*>    dirty_dirfrags, new_dirfrags;
  elist<CDirStripe*> dirty_stripes, new_stripes;
  elist<CInode*>  dirty_inodes;
  elist<CDentry*> dirty_dentries;

  elist<CInode*>  open_files;
  elist<CInode*>  renamed_files;

  elist<MDSlaveUpdate*> slave_updates;
  
  set<CInode*> truncating_inodes;

  map<int, hash_set<version_t> > pending_commit_tids;  // mdstable
  set<metareqid_t> uncommitted_masters;

  // client request ids
  map<int, tid_t> last_client_tids;

  // table version
  version_t inotablev;
  version_t sessionmapv;
  map<int,version_t> tablev;

  // try to expire
  void try_to_expire(MDS *mds, C_GatherBuilder &gather_bld);

  // cons
  LogSegment(loff_t off) :
    offset(off), end(off), num_events(0), trimmable_at(0),
    dirty_dirfrags(member_offset(CDirFrag, item_dirty)),
    new_dirfrags(member_offset(CDirFrag, item_new)),
    dirty_stripes(member_offset(CDirStripe, item_dirty)),
    new_stripes(member_offset(CDirStripe, item_new)),
    dirty_inodes(member_offset(CInode, item_dirty)),
    dirty_dentries(member_offset(CDentry, item_dirty)),
    open_files(member_offset(CInode, item_open_file)),
    renamed_files(member_offset(CInode, item_renamed_file)),
    slave_updates(0), // passed to begin() manually
    inotablev(0), sessionmapv(0)
  { }
};

#endif
