// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */


#ifndef CEPH_KEYVALUESTORE_H
#define CEPH_KEYVALUESTORE_H


#include <cassert>
#include <condition_variable>
#include <fstream>
#include <map>
#include <mutex>
#include <deque>

#include <boost/scoped_ptr.hpp>

#include "include/types.h"
#include "ObjectStore.h"

#include "common/WorkQueue.h"
#include "common/Finisher.h"
#include "common/fd.h"

#include "GenericObjectMap.h"
#include "SequencerPosition.h"
#include "KeyValueDB.h"

enum kvstore_types {
    KV_TYPE_NONE = 0,
    KV_TYPE_LEVELDB,
    KV_TYPE_OTHER
};


class StripObjectMap: public GenericObjectMap {
 public:

  struct StripExtent {
    uint64_t no;
    uint64_t offset;	// in key
    uint64_t len;    // in key
    StripExtent(uint64_t n, uint64_t off, size_t len):
      no(n), offset(off), len(len) {}
  };

  // -- strip object --
  struct StripObjectHeader {
    // Persistent state
    uint64_t strip_size;
    uint64_t max_size;
    vector<char> bits;
    SequencerPosition spos;

    // soft state
    Header header; // FIXME: Hold lock to avoid concurrent operations, it will
		   // also block read operation which not should be permitted.
    coll_t cid;
    oid_t oid;
    bool deleted;
    map<pair<string, string>, bufferlist> buffers;  // pair(prefix, key)

    StripObjectHeader(): strip_size(default_strip_size), max_size(0), deleted(false) {}

    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(strip_size, bl);
      ::encode(max_size, bl);
      ::encode(bits, bl);
      ::encode(spos, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      ::decode(strip_size, bl);
      ::decode(max_size, bl);
      ::decode(bits, bl);
      ::decode(spos, bl);
      DECODE_FINISH(bl);
    }
  };

  bool check_spos(const StripObjectHeader &header,
		  const SequencerPosition &spos);
  void sync_wrap(StripObjectHeader &strip_header, KeyValueDB::Transaction t,
		 const SequencerPosition &spos);

  static int file_to_extents(uint64_t offset, size_t len, uint64_t strip_size,
			     vector<StripExtent> &extents);
  int lookup_strip_header(const coll_t & cid, const oid_t &oid,
			  StripObjectHeader &header);
  int save_strip_header(StripObjectHeader &header,
			const SequencerPosition &spos,
			KeyValueDB::Transaction t);
  int create_strip_header(const coll_t &cid, const oid_t &oid,
			  StripObjectHeader &strip_header,
			  KeyValueDB::Transaction t);
  void clone_wrap(StripObjectHeader &old_header,
		  const coll_t &cid, const oid_t &oid,
		  KeyValueDB::Transaction t,
		  StripObjectHeader *origin_header,
		  StripObjectHeader *target_header);
  void rename_wrap(const coll_t &cid, const oid_t &oid,
		   KeyValueDB::Transaction t,
		   StripObjectHeader *header);
  // Already hold header to avoid lock header seq again
  int get_with_header(
    const StripObjectHeader &header,
    const string &prefix,
    map<string, bufferlist> *out
    );

  int get_values_with_header(
    const StripObjectHeader &header,
    const string &prefix,
    const set<string> &keys,
    map<string, bufferlist> *out
    );
  int get_keys_with_header(
    const StripObjectHeader &header,
    const string &prefix,
    set<string> *keys
    );

  StripObjectMap(CephContext *c, KeyValueDB *db): GenericObjectMap(c, db) {}

  static const uint64_t default_strip_size = 1024;
};


class KeyValueStore : public ObjectStore,
		      public md_config_obs_t {
 public:
  static const uint32_t target_version = 1;

 private:
  string basedir;
  std::string current_fn;
  std::string current_op_seq_fn;
  boost::uuids::uuid fsid;

  int fsid_fd, op_fd, current_fd;

  enum kvstore_types kv_type;

  deque<uint64_t> snaps;

  // ObjectMap
  boost::scoped_ptr<StripObjectMap> backend;

  Finisher ondisk_finisher;

  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;

  int _create_current();

  /// read a uuid from fd
  int read_fsid(int fd, boost::uuids::uuid *id);

  /// lock fsid_fd
  int lock_fsid();

  string strip_object_key(uint64_t no) {
    char n[100];
    snprintf(n, 100, "%lld", (long long)no);
    return string(n);
  }

  // A special coll used by store collection info, each oid in this coll
  // represent a coll_t
  static bool is_coll_obj(const coll_t &c) {
    return c.to_str().compare("COLLECTIONS") == 0;
  }
  static coll_t get_coll_for_coll() {
    return coll_t("COLLECTIONS");
  }
  static oid_t make_ghobject_for_coll(const coll_t &col) {
    return oid_t(col.to_str());
  }

  // Each transaction has side effect which may influent the following
  // operations, we need to make it visible for the following within
  // transaction by caching middle result.
  // Side effects contains:
  // 1. Creating/Deleting collection
  // 2. Creating/Deleting object
  // 3. Object modify(including omap, xattr)
  // 4. Clone or rename
  struct BufferTransaction {
    typedef pair<coll_t, oid_t> uniq_id;
    typedef map<uniq_id, StripObjectMap::StripObjectHeader> StripHeaderMap;

    //Dirty records
    StripHeaderMap strip_headers;

    KeyValueStore *store;

    SequencerPosition spos;
    KeyValueDB::Transaction t;

    int lookup_cached_header(const coll_t &cid, const oid_t &oid,
			     StripObjectMap::StripObjectHeader **strip_header,
			     bool create_if_missing);
    int get_buffer_keys(StripObjectMap::StripObjectHeader &strip_header,
			const string &prefix, const set<string> &keys,
			map<string, bufferlist> *out);
    void set_buffer_keys(StripObjectMap::StripObjectHeader &strip_header,
			 const string &prefix, map<string, bufferlist> &bl);
    int remove_buffer_keys(StripObjectMap::StripObjectHeader &strip_header,
			   const string &prefix, const set<string> &keys);
    void clear_buffer_keys(StripObjectMap::StripObjectHeader &strip_header,
			   const string &prefix);
    int clear_buffer(StripObjectMap::StripObjectHeader &strip_header);
    void clone_buffer(StripObjectMap::StripObjectHeader &old_header,
		      const coll_t &cid, const oid_t &oid);
    void rename_buffer(StripObjectMap::StripObjectHeader &old_header,
		       const coll_t &cid, const oid_t &oid);
    int submit_transaction();

    BufferTransaction(KeyValueStore *store,
		      SequencerPosition &spos): store(store), spos(spos) {
      t = store->backend->get_transaction();
    }
  };

  // -- op workqueue --
  struct Op {
    ceph::mono_time start;
    uint64_t op;
    list<Transaction*> tls;
    Context *ondisk, *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    OpRequestRef osd_op;
  };
  class OpSequencer : public Sequencer_impl {
    std::mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    list<uint64_t> jq;
    std::condition_variable cond;
   public:
    Sequencer *parent;
    std::mutex apply_lock;  // for apply mutual exclusion
    typedef std::lock_guard<std::mutex> lock_guard;
    typedef std::unique_lock<std::mutex> unique_lock;

    void queue(Op *o) {
      lock_guard l(qlock);
      q.push_back(o);
    }
    Op *peek_queue() {
      // Must be called with apply_lock locked.
      return q.front();
    }
    Op *dequeue() {
      // Must be called with apply_lock locked.
      lock_guard l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.notify_all();
      return o;
    }
    void flush() {
      unique_lock ql(qlock);

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
	seq = q.back()->op;
      if (!jq.empty() && jq.back() > seq)
	seq = jq.back();

      if (seq) {
	// everything prior to our watermark to drain through either/both
	// queues
	while ((!q.empty() && q.front()->op <= seq) ||
		(!jq.empty() && jq.front() <= seq))
	  cond.wait(ql);
      }
    }

    OpSequencer()
      : parent(0) {}
    ~OpSequencer() {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  };

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  std::condition_variable op_throttle_cond;
  std::mutex op_throttle_lock;
  Finisher op_finisher;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    KeyValueStore *store;
    OpWQ(KeyValueStore *fs, ceph::timespan timeout,
	 ceph::timespan suicide_timeout, ThreadPool *tp) :
      ThreadPool::WorkQueue<OpSequencer>("KeyValueStore::OpWQ",
					 timeout, suicide_timeout, tp),
      store(fs) {}

    bool _enqueue(OpSequencer *osr) {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) {
      assert(0);
    }
    bool _empty() {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() {
      if (store->op_queue.empty())
	return NULL;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) {
      unique_lock osral;
      store->_do_op(osr, handle, osral);
      osral.release();
      // ThreadPool worker takes a lock then immediately calls...
    }
    void _process_finish(OpSequencer *osr) {
      // Threadpool worker has taken a lock, calls us...
      unique_lock osral(osr->apply_lock, std::adopt_lock);
      store->_finish_op(osr, osral);
    }
    void _clear() {
      assert(store->op_queue.empty());
    }
  } op_wq;

  Op *build_op(list<Transaction*>& tls, Context *ondisk, Context *onreadable,
	       Context *onreadable_sync, OpRequestRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o, ThreadPool::TPHandle *handle = NULL);
  // Must be given an UNLOCKED unique_lock. Doesn't matter what it's
  // associated with.
  void _do_op(OpSequencer *osr, ThreadPool::TPHandle &handle,
	      unique_lock& osral);
  void op_queue_release_throttle(Op *o);
  // Must be given the LOCKED unique_lock from _do_op.
  void _finish_op(OpSequencer *osr, unique_lock& osral);

 public:

  KeyValueStore(CephContext *_cct, const std::string &base,
		const char *internal_name = "keyvaluestore-dev",
		bool update_to=false);
  ~KeyValueStore();

  int _detect_backend() { kv_type = KV_TYPE_LEVELDB; return 0; }
  bool test_mount_in_use();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  uint32_t get_target_version() {
    return target_version;
  }
  int peek_journal_fsid(boost::uuids::uuid *id) {
    *id = fsid;
    return 0;
  }

  int write_version_stamp();
  int mount();
  int umount();
  int get_max_object_name_length();
  int mkfs();
  int mkjournal() {return 0;}

  int statfs(struct statfs *buf);

  int _do_transactions(
    list<Transaction*> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle);
  int do_transactions(list<Transaction*> &tls, uint64_t op_seq) {
    return _do_transactions(tls, op_seq, 0);
  }
  unsigned _do_transaction(Transaction& transaction,
			   BufferTransaction &bt,
			   SequencerPosition& spos,
			   ThreadPool::TPHandle *handle);

  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
			 OpRequestRef op = OpRequestRef(),
			 ThreadPool::TPHandle *handle = NULL);


  // ------------------
  // objects

  int _generic_read(StripObjectMap::StripObjectHeader &header,
		    uint64_t offset, size_t len, bufferlist& bl,
		    bool allow_eio = false, BufferTransaction *bt = 0);
  int _generic_write(StripObjectMap::StripObjectHeader &header,
		     uint64_t offset, size_t len, const bufferlist& bl,
		     BufferTransaction &t, bool replica = false);

  bool exists(const coll_t &cid, const oid_t& oid);
  int stat(const coll_t &cid, const oid_t& oid, struct stat *st,
	   bool allow_eio = false);
  int read(const coll_t &cid, const oid_t& oid, uint64_t offset, size_t len,
	   bufferlist& bl, bool allow_eio = false);
  int fiemap(const coll_t &cid, const oid_t& oid, uint64_t offset,
	     size_t len, bufferlist& bl);

  int _touch(const coll_t &cid, const oid_t& oid, BufferTransaction &t);
  int _write(const coll_t &cid, const oid_t& oid, uint64_t offset,
	     size_t len, const bufferlist& bl, BufferTransaction &t,
	     bool replica = false);
  int _zero(const coll_t &cid, const oid_t& oid, uint64_t offset,
	    size_t len, BufferTransaction &t);
  int _truncate(const coll_t &cid, const oid_t& oid, uint64_t size,
		BufferTransaction &t);
  int _clone(const coll_t &cid, const oid_t& oldoid,
	     const oid_t& newoid, BufferTransaction &t);
  int _clone_range(const coll_t &cid, const oid_t& oldoid,
		   const oid_t& newoid, uint64_t srcoff,
		   uint64_t len, uint64_t dstoff, BufferTransaction &t);
  int _remove(const coll_t &cid, const oid_t& oid, BufferTransaction &t);


  void start_sync() {}
  void sync() {}
  void flush() {}
  void sync_and_flush() {}

  void set_fsid(const boost::uuids::uuid& u) { fsid = u; }
  boost::uuids::uuid get_fsid() { return fsid; }

  // attrs
  int getattr(const coll_t &cid, const oid_t& oid, const char *name,
	      bufferptr &bp);
  int getattrs(const coll_t &cid, const oid_t& oid,
	       map<string,bufferptr>& aset, bool user_only = false);

  int _setattrs(const coll_t &cid, const oid_t& oid,
		map<string, bufferptr>& aset, BufferTransaction &t);
  int _rmattr(const coll_t &cid, const oid_t& oid, const char *name,
	      BufferTransaction &t);
  int _rmattrs(const coll_t &cid, const oid_t& oid, BufferTransaction &t);

  int collection_getattr(const coll_t &c, const char *name, void *value, size_t size);
  int collection_getattr(const coll_t &c, const char *name, bufferlist& bl);
  int collection_getattrs(const coll_t &cid, map<string,bufferptr> &aset);

  int _collection_setattr(const coll_t &c, const char *name, const void *value,
			  size_t size, BufferTransaction &t);
  int _collection_rmattr(const coll_t &c, const char *name,
			 BufferTransaction &t);
  int _collection_setattrs(const coll_t &cid, map<string,bufferptr> &aset,
			   BufferTransaction &t);

  // collections
  int _create_collection(const coll_t &c, BufferTransaction &t);
  int _destroy_collection(const coll_t &c, BufferTransaction &t);
  int _collection_add(const coll_t &c, const coll_t &ocid, const oid_t& oid,
		      BufferTransaction &t);
  int _collection_move_rename(const coll_t &oldcid, const oid_t& oldoid,
			      const coll_t &c, const oid_t& o,
			      BufferTransaction &t);
  int _collection_remove_recursive(const coll_t &cid,
				   BufferTransaction &t);
  int _collection_rename(const coll_t &cid, const coll_t &ncid,
			 BufferTransaction &t);
  int list_collections(vector<coll_t>& ls);
  bool collection_exists(const coll_t &c);
  bool collection_empty(const coll_t &c);
  int collection_list(const coll_t &c, vector<oid_t>& oid);
  int collection_list_partial(const coll_t &c, oid_t start,
			      int min, int max,
			      vector<oid_t> *ls, oid_t *next);
  int collection_list_range(const coll_t &c, oid_t start, oid_t end,
			    vector<oid_t> *ls);
  int collection_version_current(const coll_t &c, uint32_t *version);

  // omap (see ObjectStore.h for documentation)
  int omap_get(const coll_t &c, const oid_t &oid, bufferlist *header,
	       map<string, bufferlist> *out);
  int omap_get_header(
    const coll_t &c,
    const oid_t &oid,
    bufferlist *out,
    bool allow_eio = false);
  int omap_get_keys(const coll_t &c, const oid_t &oid, set<string> *keys);
  int omap_get_values(const coll_t &c, const oid_t &oid,
		      const set<string> &keys, map<string, bufferlist> *out);
  int omap_check_keys(const coll_t &c, const oid_t &oid,
		      const set<string> &keys, set<string> *out);
  ObjectMap::ObjectMapIterator get_omap_iterator(const coll_t &c,
						 const oid_t &oid);

  void dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq,
			 OpSequencer *osr);

 private:
  void _inject_failure() {}

  // omap
  int _omap_clear(const coll_t &cid, const oid_t &oid,
		  BufferTransaction &t);
  int _omap_setkeys(const coll_t &cid, const oid_t &oid,
		    map<string, bufferlist> &aset,
		    BufferTransaction &t);
  int _omap_rmkeys(const coll_t &cid, const oid_t &oid,
		   const set<string> &keys, BufferTransaction &t);
  int _omap_rmkeyrange(const coll_t &cid, const oid_t &oid,
		       const string& first, const string& last,
		       BufferTransaction &t);
  int _omap_setheader(const coll_t &cid, const oid_t &oid,
		      const bufferlist &bl, BufferTransaction &t);

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

  std::string m_osd_rollback_to_cluster_snap;
  int m_keyvaluestore_queue_max_ops;
  int m_keyvaluestore_queue_max_bytes;

  int do_update;


  static const string OBJECT_STRIP_PREFIX;
  static const string OBJECT_XATTR;
  static const string OBJECT_OMAP;
  static const string OBJECT_OMAP_HEADER;
  static const string OBJECT_OMAP_HEADER_KEY;
  static const string COLLECTION;
  static const string COLLECTION_ATTR;
  static const uint32_t COLLECTION_VERSION = 1;

  class SubmitManager {
    std::mutex lock;
    uint64_t op_seq;
    uint64_t op_submitted;
   public:
    SubmitManager() :
	op_seq(0), op_submitted(0)
    {}
    // Pass in a unique_lock (must not be locked!)
    uint64_t op_submit_start(unique_lock& sml);
    // Pass in a unique_lock (must be locked!), it will unlock it.
    void op_submit_finish(unique_lock&, uint64_t op);
    void set_op_seq(uint64_t seq) {
      lock_guard l(lock);
      op_submitted = op_seq = seq;
    }
    uint64_t get_op_seq() {
      return op_seq;
    }
  } submit_manager;
};

WRITE_CLASS_ENCODER(StripObjectMap::StripObjectHeader)

#endif
