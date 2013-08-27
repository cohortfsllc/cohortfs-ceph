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

#ifndef CEPH_INODECONTAINER_H
#define CEPH_INODECONTAINER_H

#include "mdstypes.h"


class CDentry;
class CInode;
class CDirStripe;
class Context;
class MDCache;
class MDRequest;
class SimpleLock;

class InodeContainer {
 private:
  friend class MDCache; // allow MDCache to set inode
  MDCache *mdcache;

  CInode *in; // container inode or replica

 public:
  InodeContainer(MDCache *mdcache) : mdcache(mdcache), in(0) {}

  CInode* get_inode() { return in; }

  // create the container inode
  CInode* create();

  // open the inode container or discover from root
  void open(Context *c);

  // create a null dentry and add its lock to xlocks
  CDentry* xlock_dentry(MDRequest *mdr, inodeno_t ino,
                        set<SimpleLock*> &xlocks);

  // run the placement algorithm for the given inode number
  stripeid_t place(inodeno_t ino) const;
};

#endif

