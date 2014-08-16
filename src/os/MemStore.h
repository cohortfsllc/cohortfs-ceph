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


class MemStore : public ObjectStore {
private:
  static const size_t PageSize = 64 << 10;
  typedef PageSet<PageSize> page_set;
public:
  struct Object {
    Spinlock alloc_lock;
    RWLock omap_lock;
    page_set data;
    size_t data_len;
    map<string,bufferptr> xattr;
    bufferlist omap_header;
    map<string,bufferlist> omap;

    Object() : data_len(0) {}

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
  typedef std::shared_ptr<Object> ObjectRef;

  struct Collection {
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
      std::unordered_map<hobject_t, ObjectRef>::iterator o = object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(hobject_t oid) {
      RWLock::WLocker l(lock);
      std::unordered_map<hobject_t, ObjectRef>::iterator i = object_hash.find(oid);
      if (i != object_hash.end())
	return i->second;
      ObjectRef o(new Object);
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
	ObjectRef o(new Object);
	o->decode(p);
	object_map.insert(make_pair(k, o));
	object_hash.insert(make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    Collection() {}
  };
  typedef std::shared_ptr<Collection> CollectionRef;

private:
  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    ObjectRef o;
    map<string,bufferlist>::iterator it;
  public:
    OmapIteratorImpl(CollectionRef c, ObjectRef o)
      : c(c), o(o), it(o->omap.begin()) {}

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

  map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  Mutex apply_lock;    ///< serialize all updates

  CollectionRef get_collection(const coll_t &cid);

  Finisher finisher;

  void _do_transaction(Transaction &t, ThreadPool::TPHandle &handle);
  void _finish_transaction(Transaction &t);

  int _read_pages(page_set &pages, unsigned offset, size_t len, bufferlist &dst);
  void _write_pages(const bufferlist& src, unsigned offset, ObjectRef o);

  int _touch(const coll_t &cid, const hobject_t& oid);
  int _write(const coll_t &cid, const hobject_t& oid, uint64_t offset,
	     size_t len, const bufferlist& bl, bool replica = false);
  int _zero(const coll_t &cid, const hobject_t& oid,
	    uint64_t offset, size_t len);
  int _truncate(const coll_t &cid, const hobject_t& oid, uint64_t size);
  int _remove(const coll_t &cid, const hobject_t& oid);
  int _setattrs(const coll_t &cid, const hobject_t& oid,
		map<string,bufferptr>& aset);
  int _rmattr(const coll_t &cid, const hobject_t& oid, const char *name);
  int _rmattrs(const coll_t &cid, const hobject_t& oid);
  int _clone(const coll_t &cid, const hobject_t& oldoid,
	     const hobject_t& newoid);
  int _clone_range(const coll_t &cid, const hobject_t& oldoid,
		   const hobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(const coll_t &cid, const hobject_t &oid);
  int _omap_setkeys(const coll_t &cid, const hobject_t &oid,
		    const map<string, bufferlist> &aset);
  int _omap_rmkeys(const coll_t &cid, const hobject_t &oid,
		   const set<string> &keys);
  int _omap_rmkeyrange(const coll_t &cid, const hobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(const coll_t &cid, const hobject_t &oid,
		      const bufferlist &bl);

  int _create_collection(const coll_t &c);
  int _destroy_collection(const coll_t &c);
  int _collection_add(const coll_t &cid, const coll_t &ocid,
		      const hobject_t& oid);
  int _collection_move_rename(const coll_t &oldcid, const hobject_t& oldoid,
			      const coll_t &cid, const hobject_t& o);
  int _collection_setattr(const coll_t &cid, const char *name,
			  const void *value, size_t size);
  int _collection_setattrs(const coll_t &cid, map<string,bufferptr> &aset);
  int _collection_rmattr(const coll_t &cid, const char *name);
  int _collection_rename(const coll_t &cid, const coll_t &ncid);

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

  int peek_journal_fsid(uuid_d *fsid);

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

  bool exists(const coll_t &cid, const hobject_t& oid);
  int stat(
    const coll_t &cid,
    const hobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    const coll_t &cid,
    const hobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(const coll_t &cid, const hobject_t& oid,
	     uint64_t offset, size_t len, bufferlist& bl);
  int getattr(const coll_t &cid, const hobject_t& oid,
	      const char *name, bufferptr& value);
  int getattrs(const coll_t &cid, const hobject_t& oid,
	       map<string,bufferptr>& aset, bool user_only = false);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(const coll_t &c);
  int collection_getattr(const coll_t &cid, const char *name,
			 void *value, size_t size);
  int collection_getattr(const coll_t &cid, const char *name, bufferlist& bl);
  int collection_getattrs(const coll_t &cid, map<string,bufferptr> &aset);
  bool collection_empty(const coll_t &c);
  int collection_list(const coll_t &cid, vector<hobject_t>& o);
  int collection_list_partial(const coll_t &cid, hobject_t start,
			      int min, int max, vector<hobject_t> *ls, hobject_t *next);
  int collection_list_range(const coll_t &cid, hobject_t start, hobject_t end,
			    vector<hobject_t> *ls);

  int omap_get(
    const coll_t &cid,	    ///< [in] Collection containing oid
    const hobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,	    ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    const coll_t &cid,	    ///< [in] Collection containing oid
    const hobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,	    ///< [out] omap header
    bool allow_eio = false  ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t &cid,	    ///< [in] Collection containing oid
    const hobject_t &oid,   ///< [in] Object containing omap
    set<string> *keys	    ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    const coll_t &cid,	    ///< [in] Collection containing oid
    const hobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t &cid,	    ///< [in] Collection containing oid
    const hobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out	    ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t &cid,	    ///< [in] collection
    const hobject_t &oid    ///< [in] object
    );

  void set_fsid(uuid_d u);
  uuid_d get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);
};

#endif
