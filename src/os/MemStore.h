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
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MEMSTORE_H
#define CEPH_MEMSTORE_H

#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "ObjectStore.h"
#include "PageSet.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

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

    Object() : omap_lock("MemStore::Object::omap_lock"), data_len(0) {}

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
  typedef ceph::shared_ptr<Object> ObjectRef;

  struct Collection {
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash; ///< for lookup
    map<ghobject_t, ObjectRef> object_map; ///< for iteration
    map<string,bufferptr> xattr;
    RWLock lock;   ///< for object_{map,hash}

    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.  The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    ObjectRef get_object(ghobject_t oid) {
      RWLock::RLocker l(lock);
      ceph::unordered_map<ghobject_t,ObjectRef>::iterator o =
	object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(ghobject_t oid) {
      RWLock::WLocker l(lock);
      ceph::unordered_map<ghobject_t,ObjectRef>::iterator i =
	object_hash.find(oid);
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
      for (map<ghobject_t, ObjectRef>::const_iterator p = object_map.begin();
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
	ghobject_t k;
	::decode(k, p);
	ObjectRef o(new Object);
	o->decode(p);
	object_map.insert(make_pair(k, o));
	object_hash.insert(make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    Collection() : lock("MemStore::Collection::lock") {}
  };
  typedef ceph::shared_ptr<Collection> CollectionRef;

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

  typedef pair <int, Transaction*> WQItem;
  typedef pair <int, list<Transaction*>& > WQItemList;

  // transaction mt work queue
  class MultiLaneWQ :
    // derive from Somnath Roy's sharded queue
    public ShardedThreadPool::ShardedWQ<WQItem, WQItemList> {

    struct Lane {
      Mutex mtx;
      Cond cond;
      Spinlock sp;
      atomic_t size;
      int lane_ix;
      MemStore *store;
      ObjectStore::Transaction::Queue transactions;
      ObjectStore::Transaction::Queue t_active;
      CACHE_PAD(0); // coerce lanes into separate cache lines
      Lane(const string& mtx_name, int _lane_ix, MemStore *_store) :
	mtx(mtx_name.c_str()), size(0), lane_ix(_lane_ix), store(_store) {}
    };

    MemStore *store;
    Lane* lanes;
    int n_lanes;

    public:
    MultiLaneWQ(MemStore *_store, time_t ti, ShardedThreadPool* tp) :
      ShardedThreadPool::ShardedWQ<WQItem, WQItemList>
      (ti, ti*10, tp), store(_store), n_lanes(tp->get_num_threads()) {
      lanes = static_cast<Lane*>(malloc(n_lanes * sizeof(Lane)));
      for(int ix = 0; ix < n_lanes; ix++) {
	Lane& lane = lanes[ix];
	string mtx_name = "MemStore:MultiLaneWQL:";
	mtx_name += ix;
	new (&lane) Lane(mtx_name, ix, store); // placement new
      }
    }

    ~MultiLaneWQ() {
      for(int ix = 0; ix < n_lanes; ix++) {
	Lane& lane = lanes[ix];
	lane.~Lane();
      }
      free(lanes);
    }

    void _process(uint32_t thread_index, heartbeat_handle_d *hb) {
      static utime_t interval = utime_from_ms(50);
      static uint32_t ctr = 0;
      Lane& lane = lanes[thread_index];
    restart:
      if (! lane.size.read())
	goto out;
      lane.sp.lock();
      if (likely(!lane.transactions.empty())) {
	ObjectStore::Transaction::Queue::const_iterator iter =
	  lane.t_active.end();
	lane.t_active.splice(iter, lane.transactions);
	lane.size.sub(lane.transactions.size());
	lane.sp.unlock();
	while (lane.t_active.size() > 0) {
	  Transaction *t = &lane.t_active.front();
	  lane.t_active.pop_front();
	  store->_do_transaction(*t);
	  store->_finish_transaction(*t); // XXX move into _do_transaction?
	}
	goto restart;
      } else {
	lane.sp.unlock();
      }
    out:
      if ((++ctr % 400) == 0) {
	Mutex::Locker l(lane.mtx);
	lane.cond.WaitInterval(store->cct, lane.mtx, interval);
      }
    }

    bool is_shard_empty(uint32_t thread_index) {
      Lane& lane = lanes[thread_index];
      Spinlock::Locker l(lane.sp);
      return lane.transactions.empty();
    }

    void _enqueue(WQItem item) {
      Lane& lane = lanes[item.first % n_lanes];
      Spinlock::Locker l(lane.sp);
      int sz = lane.size.read();
      lane.transactions.push_back(*item.second);
      lane.size.inc();
      if (unlikely(!sz)) {
	Mutex::Locker l(lane.mtx);
	lane.cond.Signal();
      }
    }

    void _enqueue(WQItemList items) {
      Lane& lane = lanes[items.first % n_lanes];
      list<Transaction*>& ilist = items.second;
      Spinlock::Locker l(lane.sp);
      int sz = lane.size.read();
      for (auto iter = ilist.begin(); iter != ilist.end(); ++iter) {
        lane.transactions.push_back(**iter);
      }
      lane.size.add(ilist.size());
      if (unlikely(!sz)) {
	Mutex::Locker l(lane.mtx);
	lane.cond.Signal();
      }
    }

    void _enqueue_front(WQItem item) {
      Lane& lane = lanes[item.first % n_lanes];
      Spinlock::Locker l(lane.sp);
      int sz = lane.size.read();
      lane.transactions.push_front(*item.second);
      lane.size.inc();
      if (unlikely(!sz)) {
	Mutex::Locker l(lane.mtx);
	lane.cond.Signal();
      }
    }

    void return_waiting_threads() {
      for(int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = lanes[ix];
	Mutex::Locker l(lane.mtx);
	lane.cond.Signal();
      }
    }

    void dump(Formatter *f) {
      for(int ix = 0; ix < n_lanes; ++ix) {
	Lane& lane = lanes[ix];
	Spinlock::Locker l(lane.sp);
	// lane.transactions.dump(f); /* XXX needed? enotsup atm */
      }
    }

  }; /* MultiLaneWQ */

  CephContext *cct;

  // transaction work queue
  ShardedThreadPool tx_stp;
  MultiLaneWQ tx_mlwq;

  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  Mutex apply_lock;    ///< serialize all updates

  CollectionRef get_collection(const coll_t &cid);

  Finisher finisher;

  void _do_transaction(Transaction &t);
  void _finish_transaction(Transaction &t);

  int _read_pages(page_set &pages, unsigned offset, size_t len,
		  bufferlist &dst);
  void _write_pages(const bufferlist& src, unsigned offset, ObjectRef o);

  int _touch(const coll_t &cid, const ghobject_t& oid);
  int _write(const coll_t &cid, const ghobject_t& oid, uint64_t offset,
	     size_t len, const bufferlist& bl,
      bool replica = false);
  int _zero(const coll_t &cid, const ghobject_t& oid, uint64_t offset,
	    size_t len);
  int _truncate(const coll_t &cid, const ghobject_t& oid, uint64_t size);
  int _remove(const coll_t &cid, const ghobject_t& oid);
  int _setattrs(const coll_t &cid, const ghobject_t& oid,
		map<string,bufferptr>& aset);
  int _rmattr(const coll_t &cid, const ghobject_t& oid, const char *name);
  int _rmattrs(const coll_t &cid, const ghobject_t& oid);
  int _clone(const coll_t &cid, const ghobject_t& oldoid,
	     const ghobject_t& newoid);
  int _clone_range(const coll_t &cid, const ghobject_t& oldoid,
		   const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(const coll_t &cid, const ghobject_t &oid);
  int _omap_setkeys(const coll_t &cid, const ghobject_t &oid,
		    const map<string, bufferlist> &aset);
  int _omap_rmkeys(const coll_t &cid, const ghobject_t &oid,
		   const set<string> &keys);
  int _omap_rmkeyrange(const coll_t &cid, const ghobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(const coll_t &cid, const ghobject_t &oid,
		      const bufferlist &bl);

  int _create_collection(const coll_t &c);
  int _destroy_collection(const coll_t &c);
  int _collection_add(const coll_t &cid, const coll_t &ocid,
		      const ghobject_t& oid);
  int _collection_move_rename(const coll_t &oldcid, const ghobject_t& oldoid,
			      const coll_t &cid, const ghobject_t& o);
  int _collection_setattr(const coll_t &cid, const char *name,
			  const void *value,
			  size_t size);
  int _collection_setattrs(const coll_t &cid, map<string,bufferptr> &aset);
  int _collection_rmattr(const coll_t &cid, const char *name);
  int _collection_rename(const coll_t &cid, const coll_t &ncid);
  int _split_collection(const coll_t &cid, uint32_t bits, uint32_t rem,
			const coll_t &dest);

  int _save();
  int _load();

  void dump(Formatter *f);
  void dump_all();

public:
  MemStore(CephContext *_cct, const string& path)
    : ObjectStore(path),
      cct(_cct),
      tx_stp(cct, "MemStore::tx_stp",
	     g_conf->filestore_op_threads /* slot threads */),
      tx_mlwq(this, g_conf->filestore_op_thread_timeout, &tx_stp),
      coll_lock("MemStore::coll_lock"),
      apply_lock("MemStore::apply_lock"),
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

  void set_allow_sharded_objects() {
  }
  bool get_allow_sharded_objects() {
    return true;
  }

  int statfs(struct statfs *buf);

  bool exists(const coll_t &cid, const ghobject_t& oid);
  int stat(
    const coll_t &cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    const coll_t &cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(const coll_t &cid, const ghobject_t& oid, uint64_t offset,
	     size_t len, bufferlist& bl);
  int getattr(const coll_t &cid, const ghobject_t& oid, const char *name,
	      bufferptr& value);
  int getattrs(const coll_t &cid, const ghobject_t& oid,
	       map<string,bufferptr>& aset, bool user_only = false);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(const coll_t &c);
  int collection_getattr(const coll_t &cid, const char *name,
			 void *value, size_t size);
  int collection_getattr(const coll_t &cid, const char *name, bufferlist& bl);
  int collection_getattrs(const coll_t &cid, map<string,bufferptr> &aset);
  bool collection_empty(const coll_t &c);
  int collection_list(const coll_t &cid, vector<ghobject_t>& o);
  int collection_list_partial(const coll_t &cid, ghobject_t start,
			      int min, int max, snapid_t snap,
			      vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(const coll_t &cid, ghobject_t start,
			    ghobject_t end,
			    snapid_t seq, vector<ghobject_t> *ls);

  int omap_get(
    const coll_t &cid,       ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    const coll_t &cid,       ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t &cid,     ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    const coll_t &cid,           ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t &cid,       ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t &cid,     ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u);
  uuid_d get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  /* XXXX virtual in ObjectStore! */
  int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
      ThreadPool::TPHandle *handle = NULL) {
  abort();
}

  int queue_transaction(pthread_t tid, Transaction *t) {
    tx_mlwq.queue(WQItem(static_cast<int>(tid), t));
    return 0;
  }

  int queue_transactions(pthread_t tid, list<Transaction*>& tls) {
#if 0
    for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
      Transaction* t = *p;
      tx_mlwq.queue(WQItem(static_cast<int>(tid), t));
    }
#else
    tx_mlwq.queue(WQItemList(static_cast<int>(tid), tls));
#endif
    return 0;
  }

};

#endif
