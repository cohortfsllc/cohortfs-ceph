// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef COHORT_ZFSTORE_H
#define COHORT_ZFSTORE_H

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <boost/intrusive_ptr.hpp>
#include "ObjectStore.h"
#include <libzfswrap.h>

class ZFStore : public ObjectStore
{
private:
public:
  class ZCollection;

  struct ZObject : public ceph::os::Object
  {
    mutable std::atomic<uint32_t> refcnt;
    libzfswrap_vnode_t* vno; /* opaque */
    inogen_t ino;

    std::shared_timed_mutex omap_lock;
    typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
    typedef std::shared_lock<std::shared_timed_mutex> shared_lock;

    ZObject(CephContext *cct, ZCollection* c, const hoid_t& oid)
      : ceph::os::Object(c, oid), refcnt(0)
    {}

    friend void intrusive_ptr_add_ref(const ZObject* o) {
      o->refcnt.fetch_add(1, std::memory_order_relaxed);
    }
    
    friend void intrusive_ptr_release(const ZObject* o) {
      if (o->refcnt.fetch_sub(1, std::memory_order_release) == 1) {
	std::atomic_thread_fence(std::memory_order_acquire);
	delete o;
      }
    }

    /* XXXX pending integration w/LRU */
    virtual bool reclaim() {
      return false;
    }

  }; /* ZObject */

  typedef boost::intrusive_ptr<ZObject> ObjectRef;
  typedef ceph::os::Object::ObjCache ObjCache;

  struct ZCollection : public ceph::os::Collection
  {
    CephContext *cct;

    typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
    typedef std::shared_lock<std::shared_timed_mutex> shared_lock;
    std::shared_timed_mutex attr_lock;

    ObjectRef get_object(hoid_t oid) {
      ZObject* o =
	static_cast<ZObject*>(obj_cache.find(oid.hk, oid,
					     ObjCache::FLAG_NONE));
      return o;
    }

    ObjectRef get_or_create_object(hoid_t oid) {
      ObjCache::Latch lat;
      ZObject* o =
	static_cast<ZObject*>(obj_cache.find_latch(
					 oid.hk, oid, lat,
					 ObjCache::FLAG_LOCK));
      if (!o) {
	o = new ZObject(cct, this, oid);
	intrusive_ptr_add_ref(o);
	obj_cache.insert_latched(o, lat, ObjCache::FLAG_UNLOCK);
      } else
	lat.lock->unlock();
      return o;
    }

    ZCollection(ZFStore* ms, const coll_t& cid)
      : ceph::os::Collection(ms, cid), cct(ms->cct)
    {}

    ~ZCollection();
  }; /* ZCollection */

  inline ZCollection* get_slot_collection(Transaction& t,
					  uint16_t c_ix) {
    using std::get;
    col_slot_t& c_slot = t.c_slot(c_ix);
    ZCollection* c = static_cast<ZCollection*>(get<0>(c_slot));
    if (c)
      return c;
    c = static_cast<ZCollection*>(open_collection(get<1>(c_slot)));
    if (c) {
      // update slot for queued Ops to find
      get<0>(c_slot) = c;
      // then mark it for release when t is cleaned up
      get<2>(c_slot) |= Transaction::FLAG_REF;
    }
    return c;
  } /* get_slot_collection */

  inline ObjectHandle get_slot_object(Transaction& t, ZCollection* c,
				      uint16_t o_ix, bool create) {
    using std::get;
    obj_slot_t& o_slot = t.o_slot(o_ix);
    ObjectHandle oh = get<0>(o_slot);
    if (oh)
      return oh;

    auto oid = get<1>(o_slot);
    auto object =
      create ? c->get_or_create_object(oid) : c->get_object(oid);
    oh = static_cast<ObjectHandle>(object.get());
    if (oh) {
      // update slot for queued Ops to find
      get<0>(o_slot) = oh;
      // then mark it for release when t is cleaned up
      get<2>(o_slot) |= Transaction::FLAG_REF;
    }
    return oh;
  } /* get_slot_object */

  /* XXXX */
  class ZOmapIterator : public ObjectMap::ObjectMapIteratorImpl
  {
  private:
    ObjectRef o;
  public:
    ZOmapIterator(ObjectRef o)
      : o(o)
  {}

    int seek_to_first() {
      return 0;
    }

    int upper_bound(const std::string& after) {
      return 0;
    }

    int lower_bound(const std::string& to) {
      return 0;
    }

    bool valid() {
      return true;
    }

    int next() {
      return 0;
    }

    std::string key() {
      return "k";
    }

    bufferlist value() {
      return bufferlist();
    }

    int status() {
      return 0;
    }
  }; /* ZOmapIterator */

  ZFStore(CephContext* cct, const std::string& path)
    : ObjectStore(cct, path)
  { }

  ~ZFStore()
  { }

  /* ObjectStore interface */
  int update_version_stamp() {
    return 0;
  }

  bool test_mount_in_use() {
    return false; /* XXX */
  }

  int mount();
  int umount();

  int get_max_object_name_length() {
    return 4096; /* XXX */
  }

  int mkfs();

  int mkjournal() { /* XXX */
    return 0;
  }

  virtual int statfs(struct statfs* buf);

  uint32_t get_target_version() {
    return 1;
  }

  int peek_journal_fsid(boost::uuids::uuid* fsid) { /* XXX */
    *fsid = boost::uuids::nil_uuid();
    return 0;
  }

#if 0 /* XXX */
  int write_meta(const std::string& key, const std::string& value);
  int read_meta(const std::string& key, std::string* value);
  int get_ideal_list_min();
  int get_ideal_list_max();
#endif

  bool exists(CollectionHandle ch, const hoid_t& oid);

  ObjectHandle get_object(CollectionHandle ch, const hoid_t& oid) {
    ZCollection *coll = static_cast<ZCollection*>(ch);
    // find Object as intrusive_ptr<T>, explicit ref, return
    ObjectRef o = coll->get_object(oid);
    if (!o)
      return NULL;
    intrusive_ptr_add_ref(o.get());
    return o.get();
  }

  ObjectHandle get_object(CollectionHandle ch, const hoid_t& oid,
			  bool create) {
    ZCollection *coll = static_cast<ZCollection*>(ch);
    // find Object as intrusive_ptr<T>, explicit ref, return
    ObjectRef o = (create) ? coll->get_or_create_object(oid)
      : coll->get_object(oid);
    if (!o)
      return NULL;
    intrusive_ptr_add_ref(o.get());
    return o.get();
  }

  void put_object(ObjectHandle oh) {
    ObjectRef o = static_cast<ZFStore::ZObject*>(oh);
    intrusive_ptr_release(o.get());
  }

  int stat(
    CollectionHandle ch,
    ObjectHandle oh,
    struct stat* st,
    bool allow_eio = false); // struct stat?
  int read(
    CollectionHandle ch,
    ObjectHandle oh,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(CollectionHandle ch, ObjectHandle oh,
	     uint64_t offset, size_t len, bufferlist& bl);
  int getattr(CollectionHandle ch, ObjectHandle oh,
	      const char* name, bufferptr& value);
  int getattrs(CollectionHandle ch, ObjectHandle oh,
	       map<std::string,bufferptr>& aset,
	       bool user_only = false);
  int list_collections(vector<coll_t>& ls);
  CollectionHandle open_collection(const coll_t& c);
  int close_collection(CollectionHandle ch);
  bool collection_exists(const coll_t& c);
  int collection_getattr(CollectionHandle ch, const char* name,
			 void* value, size_t size);
  int collection_getattr(CollectionHandle ch, const char* name,
			 bufferlist& bl);
  int collection_getattrs(CollectionHandle ch,
			  map<std::string,bufferptr>& aset);
  bool collection_empty(CollectionHandle ch);
  int collection_list(CollectionHandle ch, vector<hoid_t>& o);
  int collection_list_partial(CollectionHandle ch, hoid_t start,
			      int min, int max, vector<hoid_t>* ls,
			      hoid_t* next);
  int collection_list_range(CollectionHandle ch, hoid_t start,
			    hoid_t end, vector<hoid_t>* ls);
  int collection_list_partial2(CollectionHandle ch,
			       int min, int max,
			       vector<hoid_t> *vs,
			       CLPCursor& cursor);
  int omap_get(CollectionHandle ch, ObjectHandle oh,
	       bufferlist* header, map<std::string, bufferlist>* out
    );

  int omap_get_header(CollectionHandle ch, ObjectHandle oh,
    bufferlist* header, bool allow_eio = false);

  int omap_get_keys(CollectionHandle ch, ObjectHandle oh,
		    set<std::string>* keys);

  int omap_get_values(CollectionHandle ch, ObjectHandle oh,
		      const set<std::string>& keys,
		      map<std::string, bufferlist>* out);

  int omap_check_keys(CollectionHandle ch, ObjectHandle oh,
		      const set<std::string>& keys,
		      set<std::string>* out);

  /* XXX */
  ObjectMap::ObjectMapIterator get_omap_iterator(CollectionHandle ch,
						 ObjectHandle oh);

  void set_fsid(const boost::uuids::uuid& u);
  boost::uuids::uuid get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(list<Transaction*>& tls,
                         OpRequestRef op = OpRequestRef());

}; /* ZFStore */

#endif /* COHORT_ZFSTORE_H */
