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
#include "os/ObjectStore.h"
#include "common/freelist.h"
extern "C" {
#include <libzfswrap.h>
}
#include "os/zfs/FragTreeIndex.h"

static creden_t cred = {0, 0};

class ZFStore : public ObjectStore
{
private:
  static lzfw_handle_t* zhd;
  static std::atomic<uint32_t> n_instances;
  std::string root_ds;
  lzfw_vfs_t* zhfs; /* root dataset handle */
  inogen_t meta_ino;
  lzfw_vnode_t* meta_vno; /* root dataset root vno */

  ZTracer::Endpoint trace_endpoint;

  int attach_meta();

public:
  class ZCollection;

  struct ZObject : public ceph::os::Object
  {
    mutable std::atomic<uint32_t> refcnt;
    lzfw_vnode_t* vno; /* opaque */
    inogen_t ino;

    std::mutex mtx;
    std::shared_timed_mutex omap_lock;
    typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
    typedef std::shared_lock<std::shared_timed_mutex> shared_lock;

    ZObject(ZCollection* c, const hoid_t& oid)
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

    bool reclaim() {
      c->obj_cache.remove(get_oid().hk, this, cohort::lru::FLAG_NONE);
      return true;
    }

    ~ZObject() {}

    struct ZObjectFactory : public cohort::lru::ObjectFactory
    {
      ZCollection* c;
      const hoid_t oid;

      ZObjectFactory(ZCollection* c, const hoid_t& oid)
	: c(c), oid(oid) {}

      void recycle (cohort::lru::Object* o) {
	  /* re-use an existing object */
	  o->~Object(); // call lru::Object virtual dtor
	  // placement new!
	  new (o) ZObject(c, oid);
      }

      cohort::lru::Object* alloc() {
	return new ZObject(c, oid);
      }
    }; /* ZObjectFactory */

  }; /* ZObject */

  typedef boost::intrusive_ptr<ZObject> ObjectRef;
  typedef ceph::os::Object::ObjCache ObjCache;

  struct ZCollection : public ceph::os::Collection
  {
    friend class ZFStore;

    const char* META_FILE = "meta_file";

    CephContext *cct;
    const std::string& path;
    cohort_zfs::FragTreeIndex index;
    std::string ds_name;
    lzfw_vfs_t* zhfs;

    inogen_t root_ino;
    inogen_t meta_ino;
    lzfw_vnode_t* meta_vno;

    typedef std::unique_lock<std::shared_timed_mutex> unique_lock;
    typedef std::shared_lock<std::shared_timed_mutex> shared_lock;
    std::shared_timed_mutex attr_lock;

    ZCollection(ZFStore* zs, const coll_t& cid, int& r, bool create=false);

    int setattr(const std::string& k, const buffer::ptr& v);
    int rmattr(const std::string& k);

    ~ZCollection()
    {
      index.unmount();
    }
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
      t.os = this;
    }
    return c;
  } /* get_slot_collection */

  inline ZObject* get_slot_object(Transaction& t, ZCollection* c,
				  uint16_t o_ix, bool create) {
    using std::get;
    obj_slot_t& o_slot = t.o_slot(o_ix);
    ZObject* o = static_cast<ZObject*>(get<0>(o_slot));
    if (o)
      return o;
    if (create) {
      o = static_cast<ZObject*>(get_object(c, get<1>(o_slot), create));
      if (o) {
	// update slot for queued Ops to find
	get<0>(o_slot) = o;
	// then mark it for release when t is cleaned up
	get<2>(o_slot) |= Transaction::FLAG_REF;
        t.os = this;
      }
    }
    return o;
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

  ZFStore(CephContext* cct, const std::string& path);
  ~ZFStore();

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

  int statfs(struct statfs* buf);

  uint32_t get_target_version() {
    return 1;
  }

  int peek_journal_fsid(boost::uuids::uuid* fsid) { /* XXX */
    *fsid = boost::uuids::nil_uuid();
    return 0;
  }

  // read and write key->value pairs to UNMOUNTED instance
  int read_meta(const std::string& key, std::string* value);
  int write_meta(const std::string& key, const std::string& value);

#if 0 /* XXX */
  int get_ideal_list_min();
  int get_ideal_list_max();
#endif

  bool exists(CollectionHandle ch, const hoid_t& oid);

  ObjectHandle get_object(CollectionHandle ch, const hoid_t& oid) {
    return get_object(ch, oid, false);
  }

  ObjectHandle get_object(CollectionHandle ch, const hoid_t& oid,
			  bool create) {
    ZCollection *c = static_cast<ZCollection*>(ch);
    ceph::os::Object::ObjCache::Latch lat;
    ZObject* o = nullptr;
    lzfw_vnode_t* vno;

    if (c->flags & ceph::os::Collection::FLAG_CLOSED) /* atomicity? */
      return o;

    retry:
      o =
	static_cast<ZObject*>(c->obj_cache.find_latch(oid.hk, oid, lat,
			                ceph::os::Object::ObjCache::FLAG_LOCK));
      /* LATCHED */
      if (o) {
	/* need initial ref from LRU (fast path) */
	if (! obj_lru.ref(o, cohort::lru::FLAG_INITIAL)) {
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
	/* LATCHED */
      } else {
	/* allocate and insert "new" Object */
	int r = c->index.open(oid, false, &vno);
	if ((r < 0) && create) {
	  r = c->index.open(oid, true, &vno);
	}
	if (r != 0)
	  goto out; /* !LATCHED */

	ZObject::ZObjectFactory prototype(c, oid /* XXX, vno */);
	o = static_cast<ZObject*>(
			obj_lru.insert(&prototype,
				       cohort::lru::Edge::MRU,
				       cohort::lru::FLAG_INITIAL));
	if (o) {
	  c->obj_cache.insert_latched(
			o, lat,
			ceph::os::Object::ObjCache::FLAG_UNLOCK);
	  goto out; /* !LATCHED */
	} else {
	  lat.lock->unlock();
	  goto retry; /* !LATCHED */
	}
      }
      lat.lock->unlock(); /* !LATCHED */
    out:
      return o;
  } /* get_object(CollectionHandle, const hoid_t&, bool) */

  void put_object(ZObject* o) {
    obj_lru.unref(o, cohort::lru::FLAG_NONE);
  }

  void put_object(ObjectHandle oh) {
    return put_object(static_cast<ZObject*>(oh));
  }

  int stat(CollectionHandle ch, ObjectHandle oh,
	   struct stat* st, bool allow_eio = false); // struct stat?
  int read(CollectionHandle ch, ObjectHandle oh,
	   uint64_t off, size_t len, bufferlist& bl, bool allow_eio = false);
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
	       bufferlist* header, map<std::string, bufferlist>* out);
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

  /* ZFStore */
  int do_transactions(list<Transaction*> &tls, uint64_t op_seq,
                      ZTracer::Trace &trace);
  int do_transaction(Transaction& t, uint64_t op_seq, int trans_num);
  int touch(ZCollection* c, ZObject* o);
  int write(ZCollection* c, ZObject* o, off_t offset, size_t len,
	    const bufferlist& bl, bool replica);
  int zero(ZCollection* c, ZObject* o, off_t offset, size_t len);
  int truncate(ZCollection* c, ZObject* o, uint64_t size);
  int remove(ZCollection* c, ZObject* o); // XXX should this be by-id?
  int setattr(ZCollection* c, ZObject* o, const std::string& k,
	      const buffer::ptr& v);
  int setattrs(ZCollection* c, ZObject* o,
	       const map<std::string,buffer::ptr>& aset);
  int rmattr(ZCollection* c, ZObject* o, const std::string& name);
  int rmattrs(ZCollection* c, ZObject* o);
  int clone(ZCollection* c, ZObject* o, ZObject* o2);
  int clone_range(ZCollection* c, ZObject* o  /* old */, ZObject* o2 /* new */,
		  off_t srcoff, size_t len, off_t dstoff);
  int create_collection(const coll_t& c);
  int destroy_collection(ZCollection* c); // XXX should this be by-id?

}; /* ZFStore */

#endif /* COHORT_ZFSTORE_H */
