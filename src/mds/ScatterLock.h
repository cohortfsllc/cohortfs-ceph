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


#ifndef CEPH_SCATTERLOCK_H
#define CEPH_SCATTERLOCK_H

#include "SimpleLock.h"

class ScatterLock : public SimpleLock {
  struct more_bits_t {
    bool dirty, flushing;
    bool scatter_wanted;
    utime_t last_scatter;
    xlist<ScatterLock*>::item item_updated;
    utime_t update_stamp;
    bool stale;

    more_bits_t(ScatterLock *lock) :
      dirty(false), flushing(false), scatter_wanted(false),
      item_updated(lock), stale(false)
    {}

    bool empty() const {
      return dirty == false &&
	flushing == false &&
	scatter_wanted == false &&
	!item_updated.is_on_list() &&
	!stale;
    }
  };
  more_bits_t *_more;

  bool have_more() const { return _more ? true : false; }
  void try_clear_more() {
    if (_more && _more->empty()) {
      delete _more;
      _more = NULL;
    }
  }
  more_bits_t *more() {
    if (!_more)
      _more = new more_bits_t(this);
    return _more;
  }

public:
  ScatterLock(MDSCacheObject *o, LockType *lt) : 
    SimpleLock(o, lt), _more(NULL)
  {}
  ~ScatterLock() {
    if (_more) {
      _more->item_updated.remove_myself();   // FIXME this should happen sooner, i think...
      delete _more;
    }
  }

  bool is_scatterlock() const {
    return true;
  }

  bool is_sync_and_unlocked() const {
    return
      SimpleLock::is_sync_and_unlocked() && 
      !is_dirty() &&
      !is_flushing();
  }

  bool can_scatter_pin(client_t loner) {
    /*
      LOCK : NOT okay because it can MIX and force replicas to journal something
      TSYN : also not okay for same reason
      EXCL : also not okay

      MIX  : okay, replica can stall before sending AC_SYNCACK
      SYNC : okay, replica can stall before sending AC_MIXACK or AC_LOCKACK
    */   
    return
      get_state() == LOCK_SYNC ||
      get_state() == LOCK_MIX;
  }

  xlist<ScatterLock*>::item *get_updated_item() { return &more()->item_updated; }

  utime_t get_update_stamp() {
    return more()->update_stamp;
  }

  void set_update_stamp(utime_t t) { more()->update_stamp = t; }

  void set_scatter_wanted() {
    more()->scatter_wanted = true;
  }
  void clear_scatter_wanted() {
    if (have_more())
      _more->scatter_wanted = false;
  }
  bool get_scatter_wanted() const {
    return have_more() ? _more->scatter_wanted : false; 
  }

  bool is_dirty() const {
    return have_more() ? _more->dirty : false;
  }
  bool is_flushing() const {
    return have_more() ? _more->flushing : false;
  }

  void mark_dirty() { 
    if (!more()->dirty) {
      if (!_more->flushing) 
	parent->get(MDSCacheObject::PIN_DIRTYSCATTERED);
      _more->dirty = true;
    }
  }
  void start_flush() {
    more()->flushing |= more()->dirty;
    more()->dirty = false;
  }
  void finish_flush() {
    if (more()->flushing) {
      _more->flushing = false;
      if (!_more->dirty) {
	parent->put(MDSCacheObject::PIN_DIRTYSCATTERED);
	parent->clear_dirty_scattered(get_type());
      }
      try_clear_more();
    }
  }
  void clear_dirty() {
    start_flush();
    finish_flush();
  }

  void set_last_scatter(utime_t t) { more()->last_scatter = t; }
  utime_t get_last_scatter() {
    return more()->last_scatter;
  }

  void infer_state_from_strong_rejoin(int rstate, bool locktoo) {
    if (rstate == LOCK_MIX || 
	rstate == LOCK_MIX_LOCK || // replica still has wrlocks?
	rstate == LOCK_MIX_SYNC || // "
	rstate == LOCK_MIX_TSYN)  // "
      state = LOCK_MIX;
    else if (locktoo && rstate == LOCK_LOCK)
      state = LOCK_LOCK;
  }

  virtual void print(ostream& out) const {
    out << "(";
    _print(out);
    if (is_dirty())
      out << " dirty";
    if (is_flushing())
      out << " flushing";
    out << ")";
  }
};

#endif
