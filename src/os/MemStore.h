// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_MEMSTORE_H
#define CEPH_MEMSTORE_H

#include <unordered_map>
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "ObjectStore.h"
#include "PageSet.h"

#include <atomic>
#include <boost/intrusive_ptr.hpp>


class MemStore : public ObjectStore {
private:
  static const size_t PageSize = 64 << 10;
  typedef PageSet<PageSize> page_set;

public:
  struct Object : public ObjectStore::Object {
    mutable std::atomic<uint32_t> refcnt;
    Spinlock alloc_lock;
    RWLock omap_lock;
    page_set data;
    size_t data_len;
    map<string,bufferptr> xattr;
    bufferlist omap_header;
    map<string,bufferlist> omap;

    Object(const hobject_t& oid) :
      ObjectStore::Object(oid), refcnt(0), data_len(0)
      {}

    friend void intrusive_ptr_add_ref(const Object* o) {
      o->refcnt.fetch_add(1, std::memory_order_relaxed);
    }
    
    friend void intrusive_ptr_release(const Object* o) {
      if (o->refcnt.fetch_sub(1, std::memory_order_release) == 1) {
	std::atomic_thread_fence(std::memory_order_acquire);
	delete o; // XXX Casey?
      }
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(2, 2, bl);
      ::encode(data_len, bl);
      data.encode(bl);
      ::encode(xattr, bl);
      ::encode(omap_header, bl);
      ::encode(omap, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(2, p);
      ::decode(data_len, p);
      data.decode(p);
      ::decode(xattr, p);
      ::decode(omap_header, p);
      ::decode(omap, p);
      DECODE_FINISH(p);
    }
    void dump(Formatter *f) const {
      f->dump_int("data_len", data_len);
      f->dump_int("omap_header_len", omap_header.length());

      f->open_array_section("xattrs");
      for (map<string,bufferptr>::const_iterator p = xattr.begin();
	   p != xattr.end();
	   ++p) {
	f->open_object_section("xattr");
	f->dump_string("name", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();
      }
      f->close_section();

      f->open_array_section("omap");
      for (map<string,bufferlist>::const_iterator p = omap.begin();
	   p != omap.end();
	   ++p) {
	f->open_object_section("pair");
	f->dump_string("key", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();
      }
      f->close_section();
    }
  };
  typedef boost::intrusive_ptr<Object> ObjectRef;

  struct MemCollection : public ObjectStore::Collection {
    std::unordered_map<hobject_t, ObjectRef> object_hash;  ///< for lookup
    map<hobject_t, ObjectRef> object_map;	 ///< for iteration
    map<string,bufferptr> xattr;
    RWLock lock;   ///< for object_{map,hash}

    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.	The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    ObjectRef get_object(hobject_t oid) {
      RWLock::RLocker l(lock);
      std::unordered_map<hobject_t, ObjectRef>::iterator o =
	object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(hobject_t oid) {
      RWLock::WLocker l(lock);
      std::unordered_map<hobject_t, ObjectRef>::iterator i =
	object_hash.find(oid);
      if (i != object_hash.end())
	return i->second;
      ObjectRef o(new Object(oid));
      object_map[oid] = o;
      object_hash[oid] = o;
      return o;
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(xattr, bl);
      uint32_t s = object_map.size();
      ::encode(s, bl);
      for (map<hobject_t, ObjectRef>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
	::encode(p->first, bl);
	p->second->encode(bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(xattr, p);
      uint32_t s;
      ::decode(s, p);
      while (s--) {
	hobject_t k;
	::decode(k, p);
	ObjectRef o(new Object(k));
	o->decode(p);
	object_map.insert(make_pair(k, o));
	object_hash.insert(make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    MemCollection(const coll_t &cid) :
      ObjectStore::Collection(cid)
      {}
  };

  inline MemCollection* get_slot_collection(Transaction& t, uint16_t c_ix) {
    using std::get;
    col_slot_t& c_slot = t.c_slot(c_ix);
    MemCollection* c = static_cast<MemCollection*>(get<0>(c_slot));
    if (c)
      return c;
    c = static_cast<MemCollection*>(open_collection(get<1>(c_slot)));
    if (c) {
      // update slot for queued Ops to find
      get<0>(c_slot) = c;
      // then mark it for release when t is cleaned up
      get<2>(c_slot) |= ObjectStore::Transaction::FLAG_REF;
    }
    return c;
  } /* get_slot_collection */

  inline ObjectHandle get_slot_object(Transaction& t, MemCollection* c,
				      uint16_t o_ix, bool create) {
    using std::get;
    obj_slot_t& o_slot = t.o_slot(o_ix);
    ObjectHandle oh = get<0>(o_slot);
    if (oh)
      return oh;

    auto oid = get<1>(o_slot);
    auto object = create ? c->get_or_create_object(oid) : c->get_object(oid);
    oh = static_cast<ObjectHandle>(object.get());
    if (oh) {
      // update slot for queued Ops to find
      get<0>(o_slot) = oh;
      // then mark it for release when t is cleaned up
      get<2>(o_slot) |= ObjectStore::Transaction::FLAG_REF;
    }
    return oh;
  } /* get_slot_object */

private:
  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    ObjectRef o;
    map<string,bufferlist>::iterator it;
  public:
    OmapIteratorImpl(ObjectRef o)
      : o(o), it(o->omap.begin()) {}

    int seek_to_first() {
      RWLock::RLocker l(o->omap_lock);
      it = o->omap.begin();
      return 0;
    }
    int upper_bound(const string &after) {
      RWLock::RLocker l(o->omap_lock);
      it = o->omap.upper_bound(after);
      return 0;
    }
    int lower_bound(const string &to) {
      RWLock::RLocker l(o->omap_lock);
      it = o->omap.lower_bound(to);
      return 0;
    }
    bool valid() {
      RWLock::RLocker l(o->omap_lock);
      return it != o->omap.end();
    }
    int next() {
      RWLock::RLocker l(o->omap_lock);
      ++it;
      return 0;
    }
    string key() {
      RWLock::RLocker l(o->omap_lock);
      return it->first;
    }
    bufferlist value() {
      RWLock::RLocker l(o->omap_lock);
      return it->second;
    }
    int status() {
      return 0;
    }
  };

  // transaction work queue
  ThreadPool tx_tp;
  ObjectStore::Transaction::Queue transactions;

  class TransactionWQ : public ThreadPool::WorkQueue<Transaction> {
    MemStore *store;
  public:
    TransactionWQ(MemStore *store, time_t timeout,
		  time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<Transaction>("MemStore::TransactionWQ",
					   timeout, suicide_timeout, tp),
	store(store) {}

    bool _enqueue(Transaction *t) {
      store->transactions.push_back(*t);
      return true;
    }
    void _dequeue(Transaction *t) {
      assert(0);
    }
    bool _empty() {
      return store->transactions.empty();
    }
    Transaction* _dequeue() {
      if (store->transactions.empty())
	return NULL;
      Transaction *t = &store->transactions.front();
      store->transactions.pop_front();
      return t;
    }
    void _process(Transaction *t, ThreadPool::TPHandle &handle) {
      store->_do_transaction(*t, handle);
    }
    void _process_finish(Transaction *t) {
      store->_finish_transaction(*t);
    }
    void _clear() {
      assert(store->transactions.empty());
    }
  } tx_wq;

  map<coll_t, MemCollection*> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  Mutex apply_lock;    ///< serialize all updates

  Finisher finisher;

  void _do_transaction(Transaction &t, ThreadPool::TPHandle &handle);
  void _finish_transaction(Transaction &t);

  int _read_pages(page_set &pages, unsigned offset, size_t len,
		  bufferlist &dst);

  void _write_pages(const bufferlist& src, unsigned offset,
		    Object* o); // XXX woo!

  int _touch(MemCollection* c, ObjectHandle oh);
  int _write(MemCollection* c, ObjectHandle oh,
	     uint64_t offset, size_t len, const bufferlist& bl,
	     bool replica = false);
  int _zero(MemCollection* c, ObjectHandle oh,
	    uint64_t offset, size_t len);
  int _truncate(MemCollection* c, ObjectHandle oh,
		uint64_t size);
  int _remove(MemCollection* c, ObjectHandle oh);
  int _setattrs(MemCollection* c, ObjectHandle oh,
		map<string,bufferptr>& aset);
  int _rmattr(MemCollection* c, ObjectHandle oh,
	      const char *name);
  int _rmattrs(MemCollection* c, ObjectHandle oh);
  int _clone(MemCollection* c, ObjectHandle oh,
	     ObjectHandle noh);
  int _clone_range(MemCollection* c, ObjectHandle oh,
		   ObjectHandle noh, uint64_t srcoff, uint64_t len,
		   uint64_t dstoff);
  int _omap_clear(MemCollection* c, ObjectHandle oh);
  int _omap_setkeys(MemCollection* c, ObjectHandle oh,
		    const map<string, bufferlist> &aset);
  int _omap_rmkeys(MemCollection* c, ObjectHandle oh,
		   const set<string> &keys);
  int _omap_rmkeyrange(MemCollection* c, ObjectHandle oh,
		       const string& first, const string& last);
  int _omap_setheader(MemCollection* c, ObjectHandle oh,
		      const bufferlist &bl);

  int _create_collection(const coll_t &c);
  int _destroy_collection(MemCollection* c);
  int _collection_setattr(MemCollection* c, const char *name,
			  const void *value, size_t size);
  int _collection_setattrs(MemCollection* c, map<string,bufferptr> &aset);
  int _collection_rmattr(MemCollection* c, const char *name);

  int _save();
  int _load();

  void dump(Formatter *f);
  void dump_all();

public:
  MemStore(CephContext *cct, const string& path)
    : ObjectStore(path),
      tx_tp(g_ceph_context, "MemStore::tx_tp",
	    g_conf->filestore_op_threads, "memstore_tx_threads"),
      tx_wq(this, g_conf->filestore_op_thread_timeout,
	    g_conf->filestore_op_thread_suicide_timeout, &tx_tp),
      finisher(cct) { }
  ~MemStore() { }

  int update_version_stamp() {
    return 0;
  }
  uint32_t get_target_version() {
    return 1;
  }

  int peek_journal_fsid(boost::uuids::uuid *fsid);

  bool test_mount_in_use() {
    return false;
  }

  int mount();
  int umount();

  int get_max_object_name_length() {
    return 4096;
  }

  int mkfs();
  int mkjournal() {
    return 0;
  }

  int statfs(struct statfs *buf);
  
  bool exists(CollectionHandle ch, const hobject_t& oid);

  ObjectHandle get_object(CollectionHandle ch,
			  const hobject_t& oid) {
    MemCollection *coll = static_cast<MemCollection*>(ch);

    // find Object as intrusive_ptr<T>, explicit ref, return
    ObjectRef o = coll->get_object(oid);
    if (!o)
      return NULL;
    intrusive_ptr_add_ref(o.get());
    return o.get();
  }

  void put_object(ObjectHandle oh) {
    ObjectRef o = static_cast<MemStore::Object*>(oh);
    intrusive_ptr_release(o.get());
  }

  int stat(
    CollectionHandle ch,
    ObjectHandle oh,
    struct stat *st,
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
	      const char *name, bufferptr& value);
  int getattrs(CollectionHandle ch, ObjectHandle oh,
	       map<string,bufferptr>& aset, bool user_only = false);

  int list_collections(vector<coll_t>& ls);

  CollectionHandle open_collection(const coll_t& c);
  int close_collection(CollectionHandle ch);
  bool collection_exists(const coll_t &c);
  int collection_getattr(CollectionHandle ch, const char *name,
			 void *value, size_t size);
  int collection_getattr(CollectionHandle ch, const char *name,
			 bufferlist& bl);
  int collection_getattrs(CollectionHandle ch,
			  map<string,bufferptr> &aset);
  bool collection_empty(CollectionHandle ch);
  int collection_list(CollectionHandle ch, vector<hobject_t>& o);
  int collection_list_partial(CollectionHandle ch, hobject_t start,
			      int min, int max, vector<hobject_t> *ls, hobject_t *next);
  int collection_list_range(CollectionHandle ch, hobject_t start,
			    hobject_t end, vector<hobject_t> *ls);

  int omap_get(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh,   ///< [in] Object containing omap
    bufferlist *header,	    ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh,   ///< [in] Object containing omap
    bufferlist *header,	    ///< [out] omap header
    bool allow_eio = false  ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh,   ///< [in] Object containing omap
    set<string> *keys	    ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out	    ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    CollectionHandle ch, ///< [in] collection
    ObjectHandle oh    ///< [in] object
    );

  void set_fsid(const boost::uuids::uuid& u);
  boost::uuids::uuid get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    OpRequestRef op = OpRequestRef(),
    ThreadPool::TPHandle *handle = NULL);
};

#endif
