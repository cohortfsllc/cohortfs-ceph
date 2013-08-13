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



#ifndef CEPH_MDBALANCER_H
#define CEPH_MDBALANCER_H

#include "include/types.h"


class CDir;
class CInode;
class CStripe;
class MDS;
class Message;

class MDBalancer {
 protected:
  MDS *mds;

 public:
  MDBalancer(MDS *m) : mds(m) {}

  mds_load_t get_load(utime_t);

  int proc_message(Message *m);

  void tick() {}
  void try_rebalance() {}
  void queue_split(CDir *dir) {}

  void hit_inode(utime_t now, CInode *in, int type, int who=-1) {}
  void hit_dir(utime_t now, CDir *dir, int type,
               int who=-1, double amount=1.0) {}
  void hit_stripe(utime_t now, CStripe *stripe, int type,
                  int who=-1, double amount=1.0) {}
};

#endif
