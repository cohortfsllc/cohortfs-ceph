// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef COHORT_LRU_H
#define COHORT_LRU_H

#include <stdint.h>
#include <atomic>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/rbtree.hpp>
#include <boost/intrusive/avltree.hpp>
#include <mutex>
#include <atomic>
#include <functional>
#include "common/likely.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

namespace cohort {

  namespace lru {

    namespace bi = boost::intrusive;

    /* public flag values */
    constexpr uint32_t FLAG_NONE = 0x0000;
    constexpr uint32_t FLAG_INITIAL = 0x0001;

    enum class Edge : std::uint8_t
    {
      MRU = 0,
      LRU
    };

    typedef bi::link_mode<bi::safe_link> link_mode; // for debugging

    class Object
    {
    private:
      uint32_t lru_flags;
      std::atomic<uint32_t> lru_refcnt;
      std::atomic<uint32_t> lru_adj;
      bi::list_member_hook< link_mode > lru_hook;

      typedef bi::list< Object,
			bi::member_hook<
			  Object, bi::list_member_hook< link_mode >,
			  &Object::lru_hook >, bi::constant_time_size<true>
			> Queue;

    public:

      Object() : lru_flags(FLAG_NONE), lru_refcnt(0), lru_adj(0) {}

      virtual bool reclaim() = 0;

      virtual ~Object() {}

    private:
      template <typename LK, uint8_t N>
      friend class LRU;
    };

    /* allocator & recycler interface (create or re-use LRU objects) */
    class ObjectFactory
    {
    public:
      virtual Object* alloc(void) = 0;
      virtual void recycle(Object*) = 0;
      virtual ~ObjectFactory() {};
    };

    template <typename LK, uint8_t N=3>
    class LRU
    {
    private:

      struct Lane {
	LK lock;
	Object::Queue q;
#if 0
	Object::Queue pinned;
#endif
	CACHE_PAD(0);
	Lane() {}
      };

      Lane qlane[N];
      std::atomic<uint32_t> evict_lane;
      const uint32_t lane_hiwat;

      static constexpr uint32_t lru_adj_modulus = 5;

      static constexpr uint32_t SENTINEL_REFCNT = 1;

      /* internal flag values */
      static constexpr uint32_t FLAG_INLRU = 0x0001;
      static constexpr uint32_t FLAG_PINNED  = 0x0002; // possible future use
      static constexpr uint32_t FLAG_EVICTING = 0x0004;

      Lane& lane_of(void* addr) {
	return qlane[(uint64_t)(addr) % N];
      }

      uint32_t next_evict_lane() {
	return (evict_lane++ % N);
      }

      bool can_reclaim(Object* o) {
	return ((o->lru_refcnt == SENTINEL_REFCNT) &&
		(!(o->lru_flags & FLAG_EVICTING)));
      }

      Object* evict_block() {
	uint32_t lane_ix = next_evict_lane();
	for (int ix = 0; ix < N; ++ix, lane_ix = next_evict_lane()) {
	  Lane& lane = qlane[lane_ix];
	  /* hiwat check */
	  /* XXX try the hiwat check unlocked, then recheck locked */
	  if (lane.q.size() > lane_hiwat) {
	    lane.lock.lock();
	    if (lane.q.size() <= lane_hiwat) {
	      lane.lock.unlock();
	      continue;
	    }
	  } else
	    continue;
	  // XXXX if object at LRU has refcnt==1, take it
	  Object* o = &(lane.q.back());
	  if (can_reclaim(o)) {
	    ++(o->lru_refcnt);
	    o->lru_flags |= FLAG_EVICTING;
	    lane.lock.unlock();
	    if (o->reclaim()) {
	      lane.lock.lock();
	      --(o->lru_refcnt);
	      /* assertions that o state has not changed across relock */
	      assert(o->lru_refcnt == SENTINEL_REFCNT);
	      assert(o->lru_flags & FLAG_INLRU);
	      Object::Queue::iterator it = Object::Queue::s_iterator_to(*o);
	      lane.q.erase(it);
	      lane.lock.unlock();
	      return o;
	    } else {
	      // XXX can't make unreachable (means what?)
	      lane.lock.lock();
	      --(o->lru_refcnt);
	      o->lru_flags &= ~FLAG_EVICTING;
	      /* unlock in next block */
	    }
	  } /* can_reclaim(o) */
	  lane.lock.unlock();
	} /* each lane */
	return nullptr;
      } /* evict_block */

    public:

      LRU(uint32_t _hiwat) : evict_lane(0), lane_hiwat(_hiwat)
	  {}

      bool ref(Object* o, uint32_t flags) {
	++(o->lru_refcnt);
	if (flags & FLAG_INITIAL) {
	  if ((++(o->lru_adj) % lru_adj_modulus) == 0) {
	    Lane& lane = lane_of(o);
	    lane.lock.lock();
	    /* move to MRU */
	    Object::Queue::iterator it = Object::Queue::s_iterator_to(*o);
	    lane.q.erase(it);
	    lane.q.push_front(*o);
	    lane.lock.unlock();
	  } /* adj */
	} /* initial ref */
	return true;
      } /* ref */

      void unref(Object* o, uint32_t flags) {
	uint32_t refcnt = --(o->lru_refcnt);
	if (unlikely(refcnt == 0)) {
	  Lane& lane = lane_of(o);
	  lane.lock.lock();
	  refcnt = o->lru_refcnt.load();
	  if (unlikely(refcnt == 0)) {
	    std::cout << "FTW DELETE FSObject " << (void*) o << std::endl;
	    Object::Queue::iterator it = Object::Queue::s_iterator_to(*o);
	    lane.q.erase(it);
	    delete o;
	  }
	  lane.lock.unlock();
	}
      } /* unref */

      Object* insert(ObjectFactory* fac, Edge edge, uint32_t flags) {
	/* use supplied functor to re-use an evicted object, or
	 * allocate a new one of the descendant type */
	Object* o = evict_block();
	if (o)
	  fac->recycle(o); /* recycle existing object */
	else
	  o = fac->alloc(); /* get a new one */

	o->lru_flags = FLAG_INLRU;

	Lane& lane = lane_of(o);
	lane.lock.lock();
	switch (edge) {
	case Edge::MRU:
	  lane.q.push_front(*o);
	  break;
	case Edge::LRU:
	  lane.q.push_back(*o);
	  break;
	default:
	  abort();
	  break;
	}
	if (flags & FLAG_INITIAL)
	  o->lru_refcnt += 2; /* sentinel ref + initial */
	else
	  ++(o->lru_refcnt); /* sentinel */
	lane.lock.unlock();
	return o;
      } /* insert */

    };

    template <typename T, typename TTree, typename CLT, typename CEQ,
	      typename K, typename LK, uint8_t N=3, uint16_t CSZ=127>
    class TreeX
    {
    public:

      static constexpr uint32_t FLAG_NONE = 0x0000;
      static constexpr uint32_t FLAG_LOCK = 0x0001;
      static constexpr uint32_t FLAG_UNLOCK = 0x0002;
      static constexpr uint32_t FLAG_UNLOCK_ON_MISS = 0x0004;

      typedef T value_type;
      typedef typename TTree::iterator iterator;
      typedef std::pair<iterator, bool> check_result;
      typedef typename TTree::insert_commit_data insert_commit_data;

      struct Partition {
	LK lock;
	TTree tr;
	T* cache[CSZ];
	CACHE_PAD(0);
	Partition() : tr() {
	  memset(cache, 0, CSZ*sizeof(T*));
	}
      };

      struct Latch {
	Partition* p;
	LK* lock;
	insert_commit_data commit_data;
      };

      Partition& partition_of_scalar(uint64_t x) {
	return part[x % N];
      }

      T* find_latch(uint64_t hk, const K& k, Latch& lat, uint32_t flags) {
	uint32_t slot;
	T* v;
	lat.p = &(partition_of_scalar(hk));
	lat.lock = &lat.p->lock;
	if (flags & FLAG_LOCK)
	  lat.lock->lock();
	if (CSZ) { /* template specialize? */
	  slot = hk % CSZ;
	  v = lat.p->cache[slot];
	  if (v) {
	    if (CEQ()(*v, k)) {
	      if (flags & (FLAG_LOCK|FLAG_UNLOCK))
		lat.lock->unlock();
	      std::cout << "FTW CACHE_HIT FSObject " << (void*) v << std::endl;
	      return v;
	    }
	    v = nullptr;
	  }
	} else {
	  v = nullptr;
	}
	check_result r = lat.p->tr.insert_unique_check(
	  k, CLT(), lat.commit_data);
	if (! r.second /* !insertable (i.e., !found) */) {
	  v = &(*(r.first));
	  if (CSZ) {
	    /* fill cache slot at hk */
	    lat.p->cache[slot] = v;
	  }
	}
	if (flags & (FLAG_LOCK|FLAG_UNLOCK))
	  lat.lock->unlock();
	return v;
      } /* find_latch */

      void insert_latched(T* v, Latch& lat, uint32_t flags) {
	(void) lat.p->tr.insert_unique_commit(*v, lat.commit_data);
	if (flags & FLAG_UNLOCK)
	  lat.lock->unlock();
      } /* insert_latched */

      void remove(uint64_t hk, T* v, uint32_t flags) {
	Partition* p = &(partition_of_scalar(hk));
	iterator it = TTree::s_iterator_to(*v);
	p->tr.erase(it);
      } /* remove */

      void drain(std::function<void(T*)> uref,
		 uint32_t flags = FLAG_NONE) {
	/* clear a table, call supplied function on
	 * each element found (e.g., retuns sentinel
	 * references) */
	for (int t_ix = 0; t_ix < N; ++t_ix) {
	  Partition& p = part[t_ix];
	  if (flags & FLAG_LOCK) /* LOCKED */
	    p.lock.lock();
	  while (p.tr.size() > 0) {
	    iterator it = p.tr.begin();
	    T* v = &(*it);
	    p.tr.erase(it); /* must precede uref(v), in safe_link mode */
	    uref(v);
	  }
	  if (flags & FLAG_LOCK) /* we locked it, !LOCKED */
	    p.lock.unlock();
	} /* each partition */
      } /* drain */

    private:
      Partition part[N];
    };

  } /* namespace LRU */
} /* namespace cohort */

#endif /* COHORT_LRU_H */
