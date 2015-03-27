// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_OSD_OBJECTCONTEXT_H
#define CEPH_OSD_OBJECTCONTEXT_H

#include "osd_types.h"
#include "OpRequest.h"

/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */
struct ObjectContext {
  ObjectState obs;

private:
  std::mutex lock; /* XXX */

  void on_last_ref(ceph::os::Object* oh); // returns Object ref

public:
  std::atomic<uint64_t> nref;
  std::condition_variable cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;

  /* any entity in obs.oi.watchers MUST be in either watchers or
   * unconnected_watchers. */
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  void get() { nref.fetch_add(1, std::memory_order_relaxed); }

  void put() {
    if (nref.fetch_sub(1, std::memory_order_release) == 1) {
      std::atomic_thread_fence(std::memory_order_acquire);
      on_last_ref(obs.oh);
    }
  }

  struct RWState {
    enum State {
      RWNONE,
      RWREAD,
      RWWRITE
    };

    static const char *get_state_name(State s) {
      switch (s) {
      case RWNONE: return "none";
      case RWREAD: return "read";
      case RWWRITE: return "write";
      default: return "???";
      }
    }

    const char *get_state_name() const {
      return get_state_name(state);
    }

    State state;		 ///< rw state
    uint64_t count;		 ///< number of readers or writers
    OpRequest::Queue waiters;	 ///< ops waiting on state change

    RWState()
      : state(RWNONE), count(0)
    {}

    bool get_read(OpRequest* op) {
      if (get_read_lock()) {
	return true;
      } // else
      assert(! op->q_hook.is_linked());
      op->get(); // waiters queue ref
      waiters.push_back(*op);
      return false;
    }

    /// this function adjusts the counts if necessary
    bool get_read_lock() {
      // don't starve anybody!
      if (!waiters.empty()) {
	return false;
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWREAD;
	// fall through
      case RWREAD:
	count++;
	return true;
      case RWWRITE:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }

    bool get_write(OpRequest* op) {
      if (get_write_lock()) {
	return true;
      } // else
      /* XXX code had if (op) check */
      op->get();
      waiters.push_back(*op);
      return false;
    }

    bool get_write_lock() {
      // don't starve anybody!
      if (!waiters.empty()) {
	return false;
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWWRITE;
	// fall through
      case RWWRITE:
	count++;
	return true;
      case RWREAD:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }

    /// same as get_write_lock, but ignore starvation
    bool take_write_lock() {
      if (state == RWWRITE) {
	count++;
	return true;
      }
      return get_write_lock();
    }

    void dec(OpRequest::Queue& to_requeue) {
      assert(count > 0);
      count--;
      if (count == 0) {
	OpRequest::Queue::iterator i1 = to_requeue.end();
	to_requeue.splice(i1, waiters);
	state = RWNONE;
      }
    }

    void put_read(OpRequest::Queue& to_requeue) {
      assert(state == RWREAD);
      dec(to_requeue);
    }

    void put_write(OpRequest::Queue& to_requeue) {
      assert(state == RWWRITE);
      dec(to_requeue);
    }

    bool empty() const { return state == RWNONE; }
  } rwstate;
  cohort::SpinLock rwstate_lock;

  bool get_read(OpRequest* op) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    return rwstate.get_read(op);
  }

  bool get_write(OpRequest* op) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    return rwstate.get_write(op);
  }

  void put_read(OpRequest::Queue& to_wake) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    rwstate.put_read(to_wake);
  }

  void put_write(OpRequest::Queue& to_wake) {
    std::lock_guard<cohort::SpinLock> lock(rwstate_lock);
    rwstate.put_write(to_wake);
  }

  ObjectContext(const hoid_t& _oid, ceph::os::Object* oh)
    : obs(_oid, oh), nref(0),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0)
  {}

  ~ObjectContext() {
    assert(rwstate.empty());
  }

  /* do simple synchronous mutual exclusion, for now. no waitqueues
   * or anything fancy. */
  void ondisk_write_lock() {
    std::unique_lock<std::mutex> l(lock);
    writers_waiting++;
    cond.wait(l, [&](){ return !(readers_waiting || readers); });
    writers_waiting--;
    unstable_writes++;
    l.unlock();
  }

  void ondisk_write_unlock() {
    std::lock_guard<std::mutex> l(lock);
    assert(unstable_writes > 0);
    unstable_writes--;
    if (!unstable_writes && readers_waiting)
      cond.notify_all();
  }

  void ondisk_read_lock() {
    std::unique_lock<std::mutex> l(lock);
    readers_waiting++;
    cond.wait(l, [&](){ return !unstable_writes; });
    readers_waiting--;
    readers++;
  }

  void ondisk_read_unlock() {
    std::lock_guard<std::mutex> l(lock);
    assert(readers > 0);
    readers--;
    if (!readers && writers_waiting)
      cond.notify_all();
  }

  // attr cache
  map<string, bufferlist> attr_cache;

  void fill_in_setattrs(const set<string> &changing,
			ObjectModDesc *mod) {
    map<string, boost::optional<bufferlist> > to_set;
    for (set<string>::const_iterator i = changing.begin();
	 i != changing.end();
	 ++i) {
      map<string, bufferlist>::iterator iter = attr_cache.find(*i);
      if (iter != attr_cache.end()) {
	to_set[*i] = iter->second;
      } else {
	to_set[*i];
      }
    }
    mod->setattrs(to_set);
  }
}; /* ObjectContext */


typedef boost::intrusive_ptr<ObjectContext> ObjectContextRef;

void intrusive_ptr_add_ref(ObjectContext *obc);
void intrusive_ptr_release(ObjectContext *obc);

inline ostream& operator<<(ostream& out,
			   const ObjectContext::RWState& rw)
{
  return out << "rwstate(" << rw.get_state_name()
	     << " n=" << rw.count
	     << " w=" << rw.waiters.size()
	     << ")";
}

inline ostream& operator<<(ostream& out, const ObjectContext& obc)
{
  return out << "obc(" << obc.obs << " " << obc.rwstate << ")";
}

#endif
