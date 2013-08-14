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


class CDirFrag;
class CInode;
class CDirStripe;
class MDS;
class Message;

class MDBalancer {
 private:
  MDS *mds;

  utime_t last_fragment;
  set<dirfrag_t> split_queue, merge_queue;

 public:
  MDBalancer(MDS *m) : mds(m) {}

  mds_load_t get_load(utime_t);

  int proc_message(Message *m);

  void tick();

  void do_fragmenting();

  void export_empties();
  //set up the rebalancing targets for export and do one if the
  //MDSMap is up to date
  void prep_rebalance(int beat);
  /*check if the monitor has recorded the current export targets;
    if it has then do the actual export. Otherwise send off our
    export targets message again*/
  void try_rebalance() {}
  void find_exports(CDirStripe *stripe, double amount, 
                    list<CDirStripe*>& exports, double& have,
                    set<CDirStripe*>& already_exporting);


  void subtract_export(CDirStripe *ex, utime_t now);
  void add_import(CDirStripe *im, utime_t now);

  void hit_inode(utime_t now, CInode *in, int type, int who=-1) {}
  void hit_dir(utime_t now, CDirFrag *dir, int type, int who=-1, double amount=1.0);
  void hit_stripe(utime_t now, CDirStripe *stripe, int type, int who=-1, double amount=1.0) {}


  void show_imports(bool external=false);

  void queue_split(CDirFrag *dir);
  void queue_merge(CDirFrag *dir);
};

#endif
