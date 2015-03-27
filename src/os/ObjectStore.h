// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */
#ifndef CEPH_OBJECTSTORE_H
#define CEPH_OBJECTSTORE_H

#include "include/Context.h"
#include "include/buffer.h"
#include "include/types.h"
#include "osd/osd_types.h"
#include "osd/ObjectContext.h"
#include "common/cohort_lru.h"
#include "ObjectMap.h"

#include <errno.h>
#include <sys/stat.h>
#include <vector>
#include <tuple>

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>	/* or <sys/statfs.h> */
#endif /* DARWIN */

class CephContext;

using std::vector;
using std::string;

/*
 * low-level interface to the local OSD file system
 */

static inline void encode(const map<string,bufferptr>* attrset,
			  bufferlist &bl) {
  ::encode(*attrset, bl);
}

namespace ceph {
namespace os {

class Collection;

/*********************************
 *
 * Object Contents and semantics
 *
 * All ObjectStore objects are identified as a named object
 * (oid_t and hoid_t) in a named collection (coll_t).
 * ObjectStore operations support the creation, mutation, deletion
 * and enumeration of objects within a collection.  Enumeration is
 * in sorted key order (where keys are sorted by hash). Object names
 * are globally unique.
 *
 * Each object has four distinct parts: byte data, xattrs, omap_header
 * and omap entries.
 *
 * The data portion of an object is conceptually equivalent to a
 * file in a file system. Random and Partial access for both read
 * and operations is required. The ability to have a sparse
 * implementation of the data portion of an object is beneficial for
 * some workloads, but not required. There is a system-wide limit on
 * the maximum size of an object, which is typically around 100 MB.
 *
 * Xattrs are equivalent to the extended attributes of file
 * systems. Xattrs are a set of key/value pairs.  Sub-value access
 * is not required. It is possible to enumerate the set of xattrs in
 * key order.	 At the implementation level, xattrs are used
 * exclusively internal to Ceph and the implementer can expect the
 * total size of all of the xattrs on an object to be relatively
 * small, i.e., less than 64KB. Much of Ceph assumes that accessing
 * xattrs on temporally adjacent object accesses (recent past or
 * near future) is inexpensive.
 *
 * omap_header is a single blob of data. It can be read or written
 * in total.
 *
 * Omap entries are conceptually the same as xattrs
 * but in a different address space. In other words, you can have
 * the same key as an xattr and an omap entry and they have distinct
 * values. Enumeration of xattrs doesn't include omap entries and
 * vice versa. The size and access characteristics of omap entries
 * are very different from xattrs. In particular, the value portion
 * of an omap entry can be quite large (MBs).	 More importantly, the
 * interface must support efficient range queries on omap entries even
 * when there are a large numbers of entries.
 *
 *********************************/

class Object : public cohort::lru::Object {
 private:
  typedef bi::link_mode<bi::safe_link> link_mode;

#if defined(OBJCACHE_AVL)
  typedef bi::avl_set_member_hook<link_mode> tree_hook_type;
#else
  /* RBT */
  typedef bi::set_member_hook<link_mode> tree_hook_type;
#endif
  tree_hook_type oid_hook;

  typedef std::unique_lock<std::mutex> unique_lock;

  enum class state
  {
    INIT = 0,
    CREATING,
    READY
  };

 protected:
  ObjectContext obc;
  Collection* c;

  mutable bool ready; // double-check var
  enum state obj_st;
  std::mutex mtx;
  std::condition_variable cv;
  uint32_t waiters;

  friend class ::ObjectStore;

 public:
  explicit Object(Collection* _c, const hoid_t& _oid);
  virtual ~Object();

  const hoid_t& get_oid() const {
    return obc.obs.oi.oid; // whee!
  }

  const coll_t& get_cid() const;

  ObjectContext& get_obc() { return obc; }

  bool is_ready() const {
    return ready;
  }

  void set_ready() {
    unique_lock lk(mtx, std::adopt_lock);
    obj_st = state::READY;
    ready = true;
    if (unlikely(waiters)) {
      waiters = 0;
      cv.notify_all();
    }
  }

  void release();

  typedef cohort::lru::LRU<cohort::SpinLock> ObjLRU;

  /* per-volume lookup table */
  struct OidLT
  {
    // for internal ordering
    bool operator()(const Object& lhs, const Object& rhs) const
    { return lhs.get_oid() < rhs.get_oid(); }

    // for external search by hoid_t
    bool operator()(const hoid_t& oid, const Object& o) const
    { return oid < o.get_oid(); }

    bool operator()(const Object& o, const hoid_t& oid) const
    { return o.get_oid() < oid; }
  };

  struct OidEQ
  {
    bool operator()(const Object& lhs, const Object& rhs) const
    { return lhs.get_oid() == rhs.get_oid(); }

    bool operator()(const hoid_t& oid, const Object& o) const
    { return oid == o.get_oid(); }

    bool operator()(const Object& o, const hoid_t& oid) const
    { return o.get_oid() == oid; }
  };

  typedef bi::member_hook<
      Object, tree_hook_type, &Object::oid_hook> OidHook;

#if defined(OBJCACHE_AVL)
  typedef bi::avltree<Object, bi::compare<OidLT>, OidHook> OidTree;
#else
  typedef bi::rbtree<Object, bi::compare<OidLT>, OidHook> OidTree;
#endif
  typedef cohort::lru::TreeX<Object, OidTree, OidLT, OidEQ,
                             hoid_t, cohort::SpinLock> ObjCache;

  friend class Collection;
}; /* Object */

/*******************************
 *
 * Collections
 *
 * A collection is simply a grouping of objects. Collections have
 * names (coll_t) and can be enumerated in order.  Like an
 * individual object, a collection also has a set of xattrs.
 */

class Collection : public RefCountedObject {
 public:
  ObjectStore* os;
  uint32_t flags;
  Object::ObjCache obj_cache;

  static constexpr uint32_t FLAG_NONE = 0x0000;
  static constexpr uint32_t FLAG_CLOSED = 0x0001;

  const coll_t cid;

  explicit Collection(ObjectStore* _os, const coll_t& _cid);

  ObjectStore* get_os() const {
    return os;
  }

  const coll_t& get_cid() const {
    return cid;
  }
};

} // namespace os
} // namespace ceph

typedef ceph::os::Object* ObjectHandle;
typedef ceph::os::Collection* CollectionHandle;

typedef std::tuple<CollectionHandle, coll_t, uint8_t> col_slot_t;
typedef std::tuple<ObjectHandle, hoid_t, uint8_t> obj_slot_t;

class Transaction;

class ObjectStore {
protected:
  std::string path;

public:
  CephContext* cct;

  /**
   * create - create an ObjectStore instance.
   *
   * This is invoked once at initialization time.
   *
   * @param type type of store. This is a string from the configuration file.
   * @param data path (or other descriptor) for data
   * @param journal path (or other descriptor) for journal (optional)
   */
  static ObjectStore *create(CephContext* cct,
			     const string& type,
			     const string& data,
			     const string& journal);

  struct C_DeleteTransaction : public Context {
    Transaction* t;
    C_DeleteTransaction(Transaction* t) : t(t) {}
    void finish(int r);
  };

  // synchronous wrappers
  unsigned apply_transaction(Transaction& t, Context* ondisk=0) {
    list<Transaction*> tls;
    tls.push_back(&t);
    return apply_transactions(tls, ondisk);
  }
  unsigned apply_transactions(list<Transaction*>& tls,
                              Context* ondisk=0);

  int queue_transaction_and_cleanup(Transaction* t) {
    list<Transaction *> tls;
    tls.push_back(t);
    return queue_transactions(tls, new C_DeleteTransaction(t),
			      NULL, NULL, OpRequestRef());
  }

  int queue_transaction(Transaction* t, Context* onreadable, Context* ondisk=0,
			Context* onreadable_sync=0,
			OpRequestRef op = OpRequestRef()) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(tls, onreadable, ondisk, onreadable_sync, op);
  }

  int queue_transactions(list<Transaction*>& tls,
			 Context *onreadable, Context *ondisk=0,
			 Context *onreadable_sync=0,
			 OpRequestRef op = OpRequestRef());

  virtual int queue_transactions(list<Transaction*>& tls,
                                 OpRequestRef op = OpRequestRef()) = 0;

  int queue_transactions(list<Transaction*>& tls, Context *onreadable,
                         Context *oncommit, Context *onreadable_sync,
                         Context *oncomplete, OpRequestRef op);

  int queue_transaction(Transaction* t, Context *onreadable,
                        Context *oncommit, Context *onreadable_sync,
                        Context *oncomplete, OpRequestRef op) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(tls, onreadable, oncommit,
                              onreadable_sync, oncomplete, op);
  }

 public:
  ObjectStore(CephContext* cct, const std::string& _path)
    : path(_path), cct(cct),
      obj_lru(cct->_conf->osd_os_lru_lanes,
	      cct->_conf->osd_os_lru_lane_hiwat) {}

  virtual ~ObjectStore() {}

 private:
  // no copying
  ObjectStore(const ObjectStore& o) = delete;
  const ObjectStore& operator=(const ObjectStore& o) = delete;

 public:
  void ref(ObjectHandle o) {
    obj_lru.ref(o, cohort::lru::FLAG_NONE);
  }

  void unref(ObjectHandle o) {
    obj_lru.unref(o, cohort::lru::FLAG_NONE);
  }

  // mgmt
  virtual int version_stamp_is_valid(uint32_t* version) { return 1; }
  virtual int update_version_stamp() = 0;
  virtual bool test_mount_in_use() = 0;
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int get_max_object_name_length() = 0;
  virtual int mkfs() = 0;  // wipe
  virtual int mkjournal() = 0; // journal only

  virtual int statfs(struct statfs* buf) = 0;

  /**
   * get the most recent "on-disk format version" supported
   */
  virtual uint32_t get_target_version() = 0;

  /**
   * check the journal uuid/fsid, without opening
   */
  virtual int peek_journal_fsid(boost::uuids::uuid* fsid) = 0;

  /**
   * write_meta - write a simple configuration key out-of-band
   *
   * Write a simple key/value pair for basic store configuration
   * (e.g., a uuid or magic number) to an unopened/unmounted store.
   * The default implementation writes this to a plaintext file in the
   * path.
   *
   * A newline is appended.
   *
   * @param key key name (e.g., "fsid")
   * @param value value (e.g., a uuid rendered as a string)
   * @returns 0 for success, or an error code
   */
  virtual int write_meta(const std::string& key,
			 const std::string& value);

  /**
   * read_meta - read a simple configuration key out-of-band
   *
   * Read a simple key value to an unopened/mounted store.
   *
   * Trailing whitespace is stripped off.
   *
   * @param key key name
   * @param value pointer to value string
   * @returns 0 for success, or an error code
   */
  virtual int read_meta(const std::string& key,
			std::string* value);

  /**
   * get ideal min value for collection_list_partial()
   *
   * default to some arbitrary values; the implementation will override.
   */
  virtual int get_ideal_list_min() { return 32; }

  /**
   * get ideal max value for collection_list_partial()
   *
   * default to some arbitrary values; the implementation will override.
   */
  virtual int get_ideal_list_max() { return 64; }

  /**
   * Synchronous read operations
   */


  /**
   * exists -- Test for existance of object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @returns true if object exists, false otherwise
   */
  virtual bool exists(CollectionHandle ch, const hoid_t& oid) = 0;

  /**
   * get_object -- Get an initial reference on an existing object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @returns a handle to Object if successful, else nullptr
   */
  virtual ObjectHandle get_object(const CollectionHandle ch,
				  const hoid_t& oid) = 0;

  /**
   * get_object -- Get an initial reference on a new or existing object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @returns a handle to Object if successful, else nullptr
   */
  virtual ObjectHandle get_object(const CollectionHandle ch,
				  const hoid_t& oid,
				  bool create) = 0;


  /**
   * get_object_for_init -- Get initial ref. w/create semantics
   *
   * @param ch collection for object
   * @param oid oid of object
   * @returns a handle to Object if successful, else nullptr
   */
  ObjectHandle get_object_for_init(const CollectionHandle ch,
				   const hoid_t& oid,
				   bool create) {
    ObjectHandle oh = get_object(ch, oid, create);
    if (!oh)
      return nullptr;
    if (oh->ready)
      return oh;

    ceph::os::Object::unique_lock lk(oh->mtx);
    if (oh->ready) // check again under lock
      return oh;

    switch (oh->obj_st) {
    case ceph::os::Object::state::INIT:
      /* new object */
      oh->obj_st = ceph::os::Object::state::CREATING;
      /* state terminates in Object::set_ready() */
      break;
    case ceph::os::Object::state::CREATING:
      /* unlikely */
      ++oh->waiters;
      oh->cv.wait(lk);
      break;
    default:
      /* apparently, READY:  must never get here */
      abort();
      break;
    }
    // keep the mutex locked until Object::set_ready()
    lk.release();
    return oh;
  }

  /**
   * put_object -- Put a reference on an object
   *
   * @param oh a valid ObjectHandle
   * @returns void
   */
  virtual void put_object(ObjectHandle oh) = 0;

  /**
   * stat -- get information for an object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param st output information for the object
   * @param allow_eio if false, assert on -EIO operation failure
   * @returns 0 on success, negative error code on failure.
   */
  virtual int stat(
    CollectionHandle ch,
    ObjectHandle oh,
    struct stat* st,
    bool allow_eio = false) = 0; // struct stat?

  /**
   * read -- read a byte range of data from an object
   *
   * Note: if reading from an offset past the end of the object, we
   * return 0 (not, say, -EINVAL).
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param offset location offset of first byte to be read
   * @param len number of bytes to be read
   * @param bl output bufferlist
   * @param allow_eio if false, assert on -EIO operation failure
   * @returns number of bytes read on success, or negative error code
   * on failure.
   */
  virtual int read(
    CollectionHandle ch,
    ObjectHandle oh,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false) = 0;

  /**
   * fiemap -- get extent map of data of an object
   *
   * Returns an encoded map of the extents of an object's data portion
   * (map<offset,size>).
   *
   * A non-enlightend implementation is free to return the extent (offset, len)
   * as the sole extent.
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param offset location offset of first byte to be read
   * @param len number of bytes to be read
   * @param bl output bufferlist for extent map information.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int fiemap(CollectionHandle ch, ObjectHandle oh,
		     uint64_t offset, size_t len, bufferlist& bl) = 0;

  /**
   * getattr -- get an xattr of an object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param name name of attr to read
   * @param value place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int getattr(CollectionHandle ch, ObjectHandle oh,
		      const char* name, bufferptr& value) = 0;

  /**
   * getattr -- get an xattr of an object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param name name of attr to read
   * @param value place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  int getattr(CollectionHandle ch, ObjectHandle oh,
	      const char* name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(ch, oh, name, bp);
    if (bp.length())
      value.push_back(bp);
    return r;
  }
  int getattr(CollectionHandle ch, ObjectHandle oh,
	      const string name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(ch, oh, name.c_str(), bp);
    value.push_back(bp);
    return r;
  }

  /**
   * getattrs -- get all of the xattrs of an object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param aset place to put output result.
   * @param user_only true -> only user attributes are return else all
   * attributes are returned
   * @returns 0 on success, negative error code on failure.
   */
  virtual int getattrs(CollectionHandle ch, ObjectHandle oh,
		       map<string,bufferptr>& aset,
		       bool user_only = false) = 0;

  /**
   * getattrs -- get all of the xattrs of an object
   *
   * @param ch collection for object
   * @param oid oid of object
   * @param aset place to put output result.
   * @param user_only true -> only user attributes are return else all
   * attributes are returned
   * @returns 0 on success, negative error code on failure.
   */
  int getattrs(CollectionHandle ch, ObjectHandle oh,
	       map<string,bufferlist>& aset, bool user_only = false) {
    map<string,bufferptr> bmap;
    int r = getattrs(ch, oh, bmap, user_only);
    for (map<string,bufferptr>::iterator i = bmap.begin();
	i != bmap.end();
	++i) {
      aset[i->first].append(i->second);
    }
    return r;
  }


  // collections

  class CLPCursor
  {
  public:
    hoid_t next_oid; /* replace w/OID */
    uint32_t partition;
    uint32_t gen;
  };

  /**
   * list_collections -- get all of the collections known to this ObjectStore
   *
   * @param ls list of the collections in sorted order.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int list_collections(vector<coll_t>& ls) = 0;

  /**
   * open_collection -- open a collection by id
   *
   * @returns the open handle on success, nullptr otherwise
   */
  virtual CollectionHandle open_collection(const coll_t& c) = 0;

  virtual int close_collection(CollectionHandle ch) = 0;

  virtual int collection_version_current(CollectionHandle ch,
					 uint32_t* version) {
    *version = 0;
    return 1;
  }
  /**
   * does a collection exist?
   *
   * @param cid collection
   * @returns true if it exists, false otherwise
   */
  virtual bool collection_exists(const coll_t &cid) = 0;
  /**
   * collection_getattr - get an xattr of a collection
   *
   * @param ch collection handle
   * @param name xattr name
   * @param value pointer of buffer to receive value
   * @param size size of buffer to receive value
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_getattr(CollectionHandle ch, const char* name,
				 void* value, size_t size) = 0;
  /**
   * collection_getattr - get an xattr of a collection
   *
   * @param ch collection handle
   * @param name xattr name
   * @param bl buffer to receive value
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_getattr(CollectionHandle ch, const char* name,
				 bufferlist& bl) = 0;
  /**
   * collection_getattrs - get all xattrs of a collection
   *
   * @param ch collection handle
   * @param asert map of keys and buffers that contain the values
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_getattrs(CollectionHandle ch,
				  map<string,bufferptr> &aset) = 0;
  /**
   * is a collection empty?
   *
   * @param ch collection handle
   * @returns true if empty, false otherwise
   */
  virtual bool collection_empty(CollectionHandle ch) = 0;

  /**
   * collection_list - get all objects of a collection in sorted order
   *
   * @param ch collection handle
   * @param o [out] list of objects
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_list(CollectionHandle ch,
			      vector<hoid_t>& o) = 0;

  /**
   * list partial contents of collection relative to a hash offset/position
   *
   * @param ch collection handle
   * @param start list objects that sort >= this value
   * @param min return at least this many results, unless we reach the end
   * @param max return no more than this many results
   * @param snapid return no objects with snap < snapid
   * @param ls [out] result
   * @param next [out] next item sorts >= this value
   * @return zero on success, or negative error
   */
  virtual int collection_list_partial(CollectionHandle ch,
				      hoid_t start, int min, int max,
				      vector<hoid_t>* ls,
				      hoid_t* next) = 0;

  virtual int collection_list_partial2(CollectionHandle ch,
				       int min,
				       int max,
				       vector<hoid_t>* vs,
				       CLPCursor& cursor) = 0;

  /**
   * list contents of a collection that fall in the range [start, end)
   *
   * @param ch collection handle
   * @param start list object that sort >= this value
   * @param end list objects that sort < this value
   * @param snapid return no objects with snap < snapid
   * @param ls [out] result
   * @return zero on success, or negative error
   */
  virtual int collection_list_range(CollectionHandle ch, hoid_t start,
				    hoid_t end,
				    vector<hoid_t>* ls) = 0;

  /// OMAP
  /// Get omap contents
  virtual int omap_get(
    CollectionHandle ch,  ///< [in] Collection containing oid
    ObjectHandle oh, ///< [in] Object containing omap
    bufferlist* header, ///< [out] omap header
    map<string, bufferlist>* out /// < [out] Key to value map
    ) = 0;

  /// Get omap header
  virtual int omap_get_header(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh, ///< [in] Object containing omap
    bufferlist* header, ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) = 0;

  /// Get keys defined on obj
  virtual int omap_get_keys(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh, ///< [in] Object containing omap
    set<string>* keys ///< [out] Keys defined on oid
    ) = 0;

  /// Get key values
  virtual int omap_get_values(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh, ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to get
    map<string, bufferlist>* out ///< [out] Returned keys and values
    ) = 0;

  /// Filters keys into out which are defined on obj
  virtual int omap_check_keys(
    CollectionHandle ch, ///< [in] Collection containing oid
    ObjectHandle oh, ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string>* out ///< [out] Subset of keys defined on oid
    ) = 0;

  /**
   * Returns an object map iterator
   *
   * Warning!  The returned iterator is an implicit lock on filestore
   * operations in c.  Do not use filestore methods on c while the returned
   * iterator is live.	(Filling in a transaction is no problem).
   *
   * @return iterator, null on error
   */
  virtual ObjectMap::ObjectMapIterator get_omap_iterator(
    CollectionHandle ch, ///< [in] collection
    ObjectHandle oh ///< [in] object
    ) = 0;

  virtual void sync(Context* onsync) {}
  virtual void sync() {}
  virtual void flush() {}
  virtual void sync_and_flush() {}

  virtual int dump_journal(ostream& out) { return -EOPNOTSUPP; }

  virtual int snapshot(const string& name) { return -EOPNOTSUPP; }

  /**
   * Set and get internal fsid for this instance. No external data is modified
   */
  virtual void set_fsid(const boost::uuids::uuid& u) = 0;
  virtual boost::uuids::uuid get_fsid() = 0;

  // DEBUG
  virtual void inject_data_error(const hoid_t &oid) {}
  virtual void inject_mdata_error(const hoid_t &oid) {}

protected:
  ceph::os::Object::ObjLRU obj_lru;
};

namespace ceph {
namespace os {

inline Object::Object(Collection* _c, const hoid_t& _oid)
  : obc(_oid, this), c(_c), ready(false), obj_st(state::INIT),
    waiters(0)
{
  /* each object holds a ref on it's collection */
  c->get();
}

inline Object::~Object()
{
  c->put();
}

inline const coll_t& Object::get_cid() const
{
  return c->get_cid();
}

inline void Object::release()
{
  c->os->put_object(this);
}

inline Collection::Collection(ObjectStore* os, const coll_t& cid)
  : os(os), flags(FLAG_NONE),
    obj_cache(os->cct->_conf->osd_os_objcache_partitions,
              os->cct->_conf->osd_os_objcache_cachesz),
    cid(cid)
{
}

} // namespace os
} // namespace ceph

#endif /* CEPH_OBJECTSTORE_H */
