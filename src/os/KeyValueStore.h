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
    hoid_t oid;
    bool deleted;
    map<pair<string, string>, bufferlist> buffers;  // pair(prefix, key)

    StripObjectHeader(): strip_size(default_strip_size), max_size(0),
			 deleted(false) {}

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
  int lookup_strip_header(const coll_t&  cid, const hoid_t& oid,
			  StripObjectHeader &header);
  int save_strip_header(StripObjectHeader &header,
			const SequencerPosition &spos,
			KeyValueDB::Transaction t);
  int create_strip_header(const coll_t& cid, const hoid_t& oid,
			  StripObjectHeader &strip_header,
			  KeyValueDB::Transaction t);
  void clone_wrap(StripObjectHeader &old_header,
		  const coll_t& cid, const hoid_t& oid,
		  KeyValueDB::Transaction t,
		  StripObjectHeader *origin_header,
		  StripObjectHeader *target_header);
  void rename_wrap(const coll_t& cid, const hoid_t& oid,
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

  class KVObject : public ceph::os::Object
  {
  public:

    explicit KVObject(CollectionHandle ch, const hoid_t& oid)
      : Object(ch, oid)
      {}

    /* XXX fix */
    bool reclaim() {
      return false;
    }
  };

  inline CollectionHandle get_slot_collection(Transaction& t,
					      uint16_t c_ix) {
    using std::get;
    col_slot_t& c_slot = t.c_slot(c_ix);
    CollectionHandle ch = get<0>(c_slot);
    if (ch)
      return ch;
    ch = open_collection(get<1>(c_slot));
    if (ch) {
      // update slot for queued Ops to find
      get<0>(c_slot) = ch;
      // then mark it for release when t is cleaned up
      get<2>(c_slot) |= Transaction::FLAG_REF;
    }
    return ch;
  } /* get_slot_collection */

  inline ObjectHandle get_slot_object(Transaction& t,
				      CollectionHandle ch,
				      uint16_t o_ix, bool create) {
    using std::get;
    obj_slot_t& o_slot = t.o_slot(o_ix);
    ObjectHandle oh = static_cast<ObjectHandle>(get<0>(o_slot));
    if (oh)
      return oh;
    else {
      oh = KeyValueStore::get_object(ch, get<1>(o_slot));
      // update slot for queued Ops to find
      get<0>(o_slot) = oh;
      // then mark it for release when t is cleaned up
      get<2>(o_slot) |= Transaction::FLAG_REF;
    }
    return oh;
  }

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
  static bool is_coll_obj(const coll_t& c) {
    return c.to_str().compare("COLLECTIONS") == 0;
  }
  static coll_t get_coll_for_coll() {
    return coll_t("COLLECTIONS");
  }
  static hoid_t make_ghobject_for_coll(const coll_t& col) {
    return hoid_t(col.to_str());
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
    typedef pair<coll_t, hoid_t> uniq_id;
    typedef map<uniq_id, StripObjectMap::StripObjectHeader> StripHeaderMap;

    //Dirty records
    StripHeaderMap strip_headers;

    KeyValueStore *store;

    SequencerPosition spos;
    KeyValueDB::Transaction t;

    int lookup_cached_header(const coll_t& cid, const hoid_t& oid,
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
		      const coll_t& cid, const hoid_t& oid);
    void rename_buffer(StripObjectMap::StripObjectHeader &old_header,
		       const coll_t& cid, const hoid_t& oid);
    int submit_transaction();

    BufferTransaction(KeyValueStore *store,
		      SequencerPosition &spos): store(store), spos(spos) {
      t = store->backend->get_transaction();
    }
  };

  Finisher op_finisher;

 public:

  KeyValueStore(CephContext* _cct, const std::string& base,
		const char* internal_name = "keyvaluestore-dev",
		bool update_to=false);
  ~KeyValueStore();

  int _detect_backend() { kv_type = KV_TYPE_LEVELDB; return 0; }
  bool test_mount_in_use();
  int version_stamp_is_valid(uint32_t* version);
  int update_version_stamp();
  uint32_t get_target_version() {
    return target_version;
  }
  int peek_journal_fsid(boost::uuids::uuid* id) {
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

  int do_transactions(list<Transaction*> &tls, uint64_t op_seq);
  unsigned do_transaction(Transaction& transaction,
                          BufferTransaction &bt,
                          SequencerPosition& spos);

  int queue_transactions(list<Transaction*>& tls,
			 OpRequestRef op = OpRequestRef());


  // ------------------
  // objects

  int _generic_read(StripObjectMap::StripObjectHeader &header,
		    uint64_t offset, size_t len, bufferlist& bl,
		    bool allow_eio = false, BufferTransaction* bt = 0);
  int _generic_write(StripObjectMap::StripObjectHeader& header,
		     uint64_t offset, size_t len, const bufferlist& bl,
		     BufferTransaction& t, bool replica = false);

  bool exists(CollectionHandle ch, const hoid_t& oid);

  ObjectHandle get_object(CollectionHandle ch, const hoid_t& oid);
  ObjectHandle get_object(CollectionHandle ch, const hoid_t& oid,
			  bool create);
  void put_object(ObjectHandle oh);

  int stat(CollectionHandle ch, ObjectHandle oh, struct stat *st,
	   bool allow_eio = false);
  int read(CollectionHandle ch, ObjectHandle oh, uint64_t offset,
	   size_t len, bufferlist& bl, bool allow_eio = false);
  int fiemap(CollectionHandle ch, ObjectHandle oh, uint64_t offset,
	     size_t len, bufferlist& bl);

  int _touch(const coll_t& cid, const hoid_t& oid, BufferTransaction& t);
  int _write(const coll_t& cid, const hoid_t& oid, uint64_t offset,
	     size_t len, const bufferlist& bl, BufferTransaction& t,
	     bool replica = false);
  int _zero(const coll_t& cid, const hoid_t& oid, uint64_t offset,
	    size_t len, BufferTransaction& t);
  int _truncate(const coll_t& cid, const hoid_t& oid, uint64_t size,
		BufferTransaction& t);
  int _clone(const coll_t& cid, const hoid_t& oldoid,
	     const hoid_t& newoid, BufferTransaction& t);
  int _clone_range(const coll_t& cid, const hoid_t& oldoid,
		   const hoid_t& newoid, uint64_t srcoff,
		   uint64_t len, uint64_t dstoff, BufferTransaction& t);
  int _remove(const coll_t& cid, const hoid_t& oid,
	      BufferTransaction& t);


  void start_sync() {}
  void sync() {}
  void flush() {}
  void sync_and_flush() {}

  void set_fsid(const boost::uuids::uuid& u) { fsid = u; }
  boost::uuids::uuid get_fsid() { return fsid; }

  // attrs
  int getattr(CollectionHandle ch, ObjectHandle oh,
	      const char* name, bufferptr& bp);
  int getattrs(CollectionHandle ch, ObjectHandle oh,
	       map<string,bufferptr>& aset, bool user_only = false);

  int _setattrs(const coll_t& cid, const hoid_t& oid,
		map<string, bufferptr>& aset, BufferTransaction& t);
  int _rmattr(const coll_t& cid, const hoid_t& oid, const char* name,
	      BufferTransaction& t);
  int _rmattrs(const coll_t& cid, const hoid_t& oid,
	       BufferTransaction& t);

  int collection_getattr(CollectionHandle ch, const char* name,
			 void *value, size_t size);
  int collection_getattr(CollectionHandle ch, const char* name,
			 bufferlist& bl);
  int collection_getattrs(CollectionHandle ch,
			  map<string,bufferptr>& aset);

  int _collection_setattr(const coll_t& c, const char* name,
			  const void* value,
			  size_t size, BufferTransaction& t);
  int _collection_rmattr(const coll_t& c, const char* name,
			 BufferTransaction& t);
  int _collection_setattrs(const coll_t& cid,
			   map<string,bufferptr>& aset,
			   BufferTransaction& t);

  // collections

  int _create_collection(const coll_t& c, BufferTransaction& t);
  int _destroy_collection(const coll_t& c, BufferTransaction& t);
  int _collection_add(const coll_t& c, const coll_t& ocid,
		      const hoid_t& oid,
		      BufferTransaction& t);
  int _collection_move_rename(const coll_t& oldcid,
			      const hoid_t& oldoid,
			      const coll_t& c, const hoid_t& o,
			      BufferTransaction& t);
  int _collection_remove_recursive(const coll_t& cid,
				   BufferTransaction& t);
  int _collection_rename(const coll_t& cid, const coll_t& ncid,
			 BufferTransaction& t);
  int list_collections(vector<coll_t>& ls);
  CollectionHandle open_collection(const coll_t& c);
  int close_collection(CollectionHandle chandle);
  bool collection_exists(const coll_t& c);
  bool collection_empty(CollectionHandle ch);
  int collection_list(CollectionHandle ch, vector<hoid_t>& oid);
  int collection_list_partial(CollectionHandle ch, hoid_t start,
			      int min, int max,
			      vector<hoid_t>* ls, hoid_t* next);
  int collection_list_partial2(CollectionHandle ch,
			       int min, int max,
			       vector<hoid_t>* vs,
			       CLPCursor& cursor);
  int collection_list_range(CollectionHandle ch, hoid_t start,
			    hoid_t end, vector<hoid_t>* ls);
  int collection_version_current(CollectionHandle ch,
				 uint32_t* version);
  // omap (see ObjectStore.h for documentation)
  int omap_get(CollectionHandle ch, ObjectHandle oh,
	       bufferlist* header, map<string, bufferlist>* out);
  int omap_get_header(
    CollectionHandle ch,
    ObjectHandle oh,
    bufferlist* out,
    bool allow_eio = false);
  int omap_get_keys(CollectionHandle ch, ObjectHandle oh,
		    set<string>* keys);
  int omap_get_values(CollectionHandle ch, ObjectHandle oh,
		      const set<string>& keys,
		      map<string, bufferlist>* out);
  int omap_check_keys(CollectionHandle ch, ObjectHandle oh,
		      const set<string>& keys, set<string>* out);
  ObjectMap::ObjectMapIterator get_omap_iterator(CollectionHandle ch,
						 ObjectHandle oh);

  void dump_transactions(list<Transaction*>& ls, uint64_t seq);

 private:
  void _inject_failure() {}

  // omap
  int _omap_clear(const coll_t& cid, const hoid_t& oid,
		  BufferTransaction& t);
  int _omap_setkeys(const coll_t& cid, const hoid_t& oid,
		    map<string, bufferlist>& aset,
		    BufferTransaction& t);
  int _omap_rmkeys(const coll_t& cid, const hoid_t& oid,
		   const set<string>& keys, BufferTransaction& t);
  int _omap_rmkeyrange(const coll_t& cid, const hoid_t& oid,
		       const string& first, const string& last,
		       BufferTransaction& t);
  int _omap_setheader(const coll_t& cid, const hoid_t& oid,
		      const bufferlist& bl, BufferTransaction& t);

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t* conf,
				  const std::set <std::string>& changed);

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

  std::atomic<uint64_t> submit_op_seq;
};

WRITE_CLASS_ENCODER(StripObjectMap::StripObjectHeader)

#endif
