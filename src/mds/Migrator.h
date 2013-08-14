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
 * Handles the import and export of  mds authorities and actual cache data.
 * See src/doc/exports.txt for a description.
 */

#ifndef CEPH_MDS_MIGRATOR_H
#define CEPH_MDS_MIGRATOR_H

#include "include/types.h"

#include <map>
#include <list>
using std::map;
using std::list;


class MDS;
class CDirStripe;
class CInode;
class CDentry;

class Migrator {
 private:
  MDS *mds;
  MDCache *cache;

 public:
  Migrator(MDS *m, MDCache *c) : mds(m), cache(c) {}

  void dispatch(Message*);

  // export
  void encode_export_inode(CInode *in, bufferlist& bl,
			   map<client_t,entity_inst_t>& exported_client_map);
  void encode_export_inode_caps(CInode *in, bufferlist& bl,
				map<client_t,entity_inst_t>& exported_client_map);
  void finish_export_inode(CInode *in, utime_t now, list<Context*>& finished);
  void finish_export_inode_caps(CInode *in);

  // import
  typedef map<client_t, Capability::Export> client_cap_export_map;
  typedef map<CInode*, client_cap_export_map> inode_cap_export_map;

  void decode_import_inode(CDentry *dn, bufferlist::iterator& blp,
                           int oldauth, EMetaBlob *le,
                           LogSegment *ls, uint64_t log_offset,
                           inode_cap_export_map& cap_imports);
  void decode_import_inode_caps(CInode *in, bufferlist::iterator &blp,
                                inode_cap_export_map& cap_imports);
  void finish_import_inode_caps(CInode *in, int from,
                                client_cap_export_map& cap_map);

};


#endif
