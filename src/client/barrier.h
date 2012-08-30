// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 *
 * Copyright (C) 2012 Linux Box Corporation.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef BARRIER_H
#define BARRIER_H

#include "include/types.h"

#include <string>
#include <set>
#include <map>
#include <fstream>
#include <exception>

using std::list;
using std::set;
using std::map;
using std::fstream;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/interval_set.h"

#include "common/Mutex.h"
#include "common/Cond.h"

#include "common/config.h"

class Client;

typedef std::pair<uint64_t,uint64_t> barrier_interval;

class Barrier
{
private:
  interval_set<uint64_t> span;
  list<barrier_interval> intervals;
  Cond cond;
    
public:
  Barrier()
  { }
  ~Barrier()
  { }

  friend class BarrierContext;
};

class BarrierContext
{
private:
  Client *cl;
  uint64_t ino;
  Mutex lock;

  // writes not claimed by a commit
  list<barrier_interval> outstanding_writes;
  interval_set<uint64_t> outstanding_write_span;

  // commits in progress, with their claimed writes
  list<Barrier*> active_commits;
  interval_set<uint64_t> active_commit_interval;
    
public:
  BarrierContext(Client *c, uint64_t ino) : 
    cl(c), ino(ino), lock("BarrierContext")
  { };

  void write_barrier(barrier_interval &iv)
  {
    Mutex::Locker locker(lock);
    bool done = false;

    if (! active_commit_interval.intersects(iv.first, iv.second)) {
      outstanding_writes.push_back(iv);
      return;
    }

    /* find blocking commit */
    list<Barrier*>::iterator iter;
    for (iter = active_commits.begin();
	 !done && (iter != active_commits.end());
	 ++iter) {
      Barrier* barrier = *iter;
      while (barrier->span.intersects(iv.first, iv.second)) {
	/*  wait on this */
	barrier->cond.Wait(lock);
	done = true;
      }
    }

    /* past the barrier, append interval */
    outstanding_writes.push_back(iv);
  }

  void commit_barrier(barrier_interval &civ)
  {
    Mutex::Locker locker(lock);

    /* we commit outstanding writes--if none exist, we don't care */
    if (outstanding_writes.size() == 0)
      return;

    Barrier *barrier = new Barrier();
    list<barrier_interval>:: iterator iter, iter2;

    iter = outstanding_writes.begin();
    while (iter != outstanding_writes.end()) {
      barrier_interval &iv = *iter;
      interval_set<uint64_t> cvs;
      cvs.insert(civ.first, civ.second);
      if (cvs.intersects(iv.first, iv.second)) {
	  barrier->intervals.push_back(iv);
	  barrier->span.insert(iv.first, iv.second);
	  /* avoid iter invalidate */
	  iter2 = iter++;
	  outstanding_writes.erase(iter2);
      } else {
	iter++;
      }
    }

    active_commits.push_back(barrier);
    /* and wait on this */
    barrier->cond.Wait(lock);
  }

  void complete(barrier_interval &iv)
  {
    Mutex::Locker locker(lock);
    list<Barrier*>::iterator b_iter;
    list<barrier_interval>:: iterator bi_iter;

    /* first look for iv in outstanding writes */
    if (outstanding_writes.size() > 0) {
      if (outstanding_write_span.intersects(iv.first, iv.second)) {
	for (bi_iter = outstanding_writes.begin();
	     bi_iter != outstanding_writes.end();
	     ++bi_iter) {
	  if (*bi_iter == iv) {
	    outstanding_write_span.erase(iv.first, iv.second);
	    outstanding_writes.erase(bi_iter);
	    goto out;
	  }
	}
      }
    }

    /* bi not outstanding, so it must be claimed by some commit */
    for (b_iter = active_commits.begin(); b_iter != active_commits.end();
	 ++b_iter) {
      Barrier* barrier = *b_iter;
      if (barrier->span.intersects(iv.first, iv.second)) {
	for (bi_iter = barrier->intervals.begin();
	     bi_iter != barrier->intervals.end();
	     ++bi_iter) {
	  if (*bi_iter == iv) {
	    barrier->span.erase(bi_iter->first, bi_iter->second);
	    barrier->intervals.erase(bi_iter);
	    break;
	  }
	}
	/* signal waiters */
	barrier->cond.Signal();
	/* dispose cleared barrier */
	if (barrier->intervals.size() == 0) {
	  active_commits.erase(b_iter);
	  delete barrier;
	}
	break; /* out */
      }
    }
  out:
    return;
  }

  ~BarrierContext();
};

#endif
