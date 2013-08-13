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

#include "mdstypes.h"

#include "MDBalancer.h"
#include "MDS.h"
#include "MDCache.h"
#include "CDirFrag.h"

#include "msg/Messenger.h"

#include <fstream>
#include <iostream>

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".bal "


/* This function DOES put the passed message before returning */
int MDBalancer::proc_message(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_HEARTBEAT:
    m->put();
    break;

  default:
    dout(1) << " balancer unknown message " << m->get_type() << dendl;
    assert(0);
    m->put();
    break;
  }

  return 0;
}


void MDBalancer::tick()
{
  utime_t now = ceph_clock_now(g_ceph_context);

  if (g_conf->mds_bal_frag && g_conf->mds_bal_fragment_interval > 0 &&
      now.sec() - last_fragment.sec() > g_conf->mds_bal_fragment_interval) {
    last_fragment = now;
    do_fragmenting();
  }
}


double mds_load_t::mds_load()
{
  switch(g_conf->mds_bal_mode) {
  case 0:
    return
      .8 * auth.meta_load() +
      .2 * all.meta_load() +
      req_rate +
      10.0 * queue_len;

  case 1:
    return req_rate + 10.0*queue_len;

  case 2:
    return cpu_load_avg;

  }
  assert(0);
  return 0;
}

mds_load_t MDBalancer::get_load(utime_t now)
{
  mds_load_t load(now);

  load.req_rate = mds->get_req_rate();
  load.queue_len = mds->messenger->get_dispatch_queue_len();

  ifstream cpu("/proc/loadavg");
  if (cpu.is_open())
    cpu >> load.cpu_load_avg;

  dout(15) << "get_load " << load << dendl;
  return load;
}

void MDBalancer::queue_split(CDirFrag *dir)
{
  split_queue.insert(dir->dirfrag());
}

void MDBalancer::queue_merge(CDirFrag *dir)
{
  merge_queue.insert(dir->dirfrag());
}

void MDBalancer::do_fragmenting()
{
  if (split_queue.empty() && merge_queue.empty()) {
    dout(20) << "do_fragmenting has nothing to do" << dendl;
    return;
  }

  if (!split_queue.empty()) {
    dout(10) << "do_fragmenting " << split_queue.size() << " dirs marked for possible splitting" << dendl;

    set<dirfrag_t> q;
    q.swap(split_queue);

    for (set<dirfrag_t>::iterator i = q.begin();
	 i != q.end();
	 ++i) {
      CDirFrag *dir = mds->mdcache->get_dirfrag(*i);
      if (!dir ||
	  !dir->is_auth())
	continue;

      dout(10) << "do_fragmenting splitting " << *dir << dendl;
      mds->mdcache->split_dir(dir, g_conf->mds_bal_split_bits);
    }
  }

  if (!merge_queue.empty()) {
    dout(10) << "do_fragmenting " << merge_queue.size() << " dirs marked for possible merging" << dendl;

    set<dirfrag_t> q;
    q.swap(merge_queue);

    for (set<dirfrag_t>::iterator i = q.begin();
	 i != q.end();
	 ++i) {
      CDirFrag *dir = mds->mdcache->get_dirfrag(*i);
      if (!dir ||
	  !dir->is_auth() ||
	  dir->get_frag() == frag_t())  // ok who's the joker?
	continue;

      dout(10) << "do_fragmenting merging " << *dir << dendl;

      CStripe *stripe = dir->get_stripe();

      frag_t fg = dir->get_frag();
      while (fg != frag_t()) {
	frag_t sibfg = fg.get_sibling();
	list<CDirFrag*> sibs;
	bool complete = stripe->get_dirfrags_under(sibfg, sibs);
	if (!complete) {
	  dout(10) << "  not all sibs under " << sibfg << " in cache (have " << sibs << ")" << dendl;
	  break;
	}
	bool all = true;
	for (list<CDirFrag*>::iterator p = sibs.begin(); p != sibs.end(); ++p) {
	  CDirFrag *sib = *p;
	  if (!sib->is_auth() || !sib->should_merge()) {
	    all = false;
	    break;
	  }
	}
	if (!all) {
	  dout(10) << "  not all sibs under " << sibfg << " " << sibs << " should_merge" << dendl;
	  break;
	}
	dout(10) << "  all sibs under " << sibfg << " " << sibs << " should merge" << dendl;
	fg = fg.parent();
      }

      if (fg != dir->get_frag())
	mds->mdcache->merge_dir(stripe, fg);
    }
  }
}


void MDBalancer::hit_dir(utime_t now, CDirFrag *dir, int type, int who, double amount)
{
  // hit me
  double v = dir->pop_me.get(type).hit(now, amount);

  //if (dir->ino() == inodeno_t(0x10000000000))
  //dout(0) << "hit_dir " << type << " pop " << v << " in " << *dir << dendl;

  // split/merge
  if (g_conf->mds_bal_frag && g_conf->mds_bal_fragment_interval > 0 &&
      !dir->get_inode()->is_base() && // not root/base (for now at least)
      dir->is_auth()) {

    dout(20) << "hit_dir " << type << " pop is " << v << ", frag " << dir->dirfrag()
	     << " size " << dir->get_num_head_items() << dendl;

    // split
    if (g_conf->mds_bal_split_size > 0 &&
	((dir->get_num_head_items() > (unsigned)g_conf->mds_bal_split_size) ||
	 (v > g_conf->mds_bal_split_rd && type == META_POP_IRD) ||
	 (v > g_conf->mds_bal_split_wr && type == META_POP_IWR))) {
      pair<set<dirfrag_t>::iterator, bool> result =
          split_queue.insert(dir->dirfrag());
      if (result.second)
        dout(10) << "hit_dir " << type << " pop is " << v << ", putting in split_queue: " << *dir << dendl;
    }

    // merge?
    if (dir->get_frag() != frag_t() &&
	(dir->get_num_head_items() < (unsigned)g_conf->mds_bal_merge_size)) {
      pair<set<dirfrag_t>::iterator, bool> result =
          merge_queue.insert(dir->dirfrag());
      if (result.second)
        dout(10) << "hit_dir " << type << " pop is " << v << ", putting in merge_queue: " << *dir << dendl;
    }
  }

  hit_stripe(now, dir->get_stripe(), type, who, amount);
}

