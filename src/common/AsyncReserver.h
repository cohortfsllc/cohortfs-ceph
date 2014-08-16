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
 * Foundation.	See file COPYING.
 *
 */

#ifndef ASYNC_RESERVER_H
#define ASYNC_RESERVER_H

#include <map>
#include <utility>
#include <list>

#include "common/Mutex.h"
#include "common/Finisher.h"

/**
 * Manages a configurable number of asyncronous reservations.
 *
 * Memory usage is linear with the number of items queued and
 * linear with respect to the total number of priorities used
 * over all time.
 */
template <typename T>
class AsyncReserver {
  Finisher *f;
  unsigned max_allowed;
  Mutex lock;

  map<unsigned, list<pair<T, Context*> > > queues;
  map<T, pair<unsigned, typename list<pair<T, Context*> >::iterator > > queue_pointers;
  set<T> in_progress;

  void do_queues() {
    typename map<unsigned, list<pair<T, Context*> > >::reverse_iterator it;
    for (it = queues.rbegin();
	 it != queues.rend() && in_progress.size() < max_allowed;
	 ++it) {
      while (in_progress.size() < max_allowed &&
	     !it->second.empty()) {
	pair<T, Context*> p = it->second.front();
	queue_pointers.erase(p.first);
	it->second.pop_front();
	f->queue(p.second);
	in_progress.insert(p.first);
      }
    }
  }
public:
  AsyncReserver(
    Finisher *f,
    unsigned max_allowed)
    : f(f), max_allowed(max_allowed) {}

  void set_max(unsigned max) {
    Mutex::Locker l(lock);
    max_allowed = max;
    do_queues();
  }

  /**
   * Requests a reservation
   *
   * Note, on_reserved may be called following cancel_reservation.  Thus,
   * the callback must be safe in that case.  Callback will be called
   * with no locks held.  cancel_reservation must be called to release the
   * reservation slot.
   */
  void request_reservation(
    T item,		      ///< [in] reservation key
    Context *on_reserved,     ///< [in] callback to be called on reservation
    unsigned prio
    ) {
    Mutex::Locker l(lock);
    assert(!queue_pointers.count(item) &&
	   !in_progress.count(item));
    queues[prio].push_back(make_pair(item, on_reserved));
    queue_pointers.insert(make_pair(item, make_pair(prio,--(queues[prio]).end())));
    do_queues();
  }

  /**
   * Cancels reservation
   *
   * Frees the reservation under key for use.
   * Note, after cancel_reservation, the reservation_callback may or
   * may not still be called.
   */
  void cancel_reservation(
    T item		     ///< [in] key for reservation to cancel
    ) {
    Mutex::Locker l(lock);
    if (queue_pointers.count(item)) {
      unsigned prio = queue_pointers[item].first;
      delete queue_pointers[item].second->second;
      queues[prio].erase(queue_pointers[item].second);
      queue_pointers.erase(item);
    } else {
      in_progress.erase(item);
    }
    do_queues();
  }
  static const unsigned MAX_PRIORITY = (unsigned)-1;
};

#endif
