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

#ifndef CEPH_STRAY_H
#define CEPH_STRAY_H

#include "include/types.h"
#include "include/elist.h"

class CDentry;
class CInode;
class CDirStripe;
class MDS;

class Stray {
 private:
  MDS *mds;

  // maintain a list of objects to purge once all references are dropped
  elist<CInode*> inodes;
  elist<CDirStripe*> stripes;

  // purge objects from storage
  void purge(CInode *in);
  void purge(CDirStripe *stripe);

  friend class C_StrayPurged;

  // unlink and journal objects
  void purged(CInode *in);
  void purged(CDirStripe *stripe);

  friend class C_StrayLogged;

  // remove from cache after purge
  void logged(CDentry *dn, CInode *in);
  void logged(CDirStripe *stripe);

 public:
  Stray(MDS *mds);

  void add(CInode *in);
  void add(CDirStripe *stripe);

  // eval all stray objects
  void scan();

  // call purge if the object is not referenced
  void eval(CInode *in);
  void eval(CDirStripe *stripe);
};

#endif
