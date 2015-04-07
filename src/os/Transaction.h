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
#ifndef CEPH_OS_TRANSACTION_H
#define CEPH_OS_TRANSACTION_H

#include "include/buffer.h"
#include "osd/osd_types.h"

class ObjectStore;

namespace ceph {
namespace os {
class Collection;
class Object;
} // namespace os
} // namespace ceph

typedef std::tuple<ceph::os::Collection*, coll_t, uint8_t> col_slot_t;
typedef std::tuple<ceph::os::Object*, hoid_t, uint8_t> obj_slot_t;

/*********************************
 * Transaction
 *
 * A Transaction represents a sequence of primitive mutation
 * operations.
 *
 * Three events in the life of a Transaction result in
 * callbacks. Any Transaction can contain any number of callback
 * objects (Context) for any combination of the three classes of
 * callbacks:
 *
 *	on_applied_sync, on_applied, and on_commit.
 *
 * The "on_applied" and "on_applied_sync" callbacks are invoked when
 * the modifications requested by the Transaction are visible to
 * subsequent ObjectStore operations, i.e., the results are
 * readable. The only conceptual difference between on_applied and
 * on_applied_sync is the specific thread and locking environment in
 * which the callbacks operate.  "on_applied_sync" is called
 * directly by an ObjectStore execution thread. It is expected to
 * execute quickly and must not acquire any locks of the calling
 * environment. Conversely, "on_applied" is called from the separate
 * Finisher thread, meaning that it can contend for calling
 * environment locks. NB, on_applied and on_applied sync are
 * sometimes called on_readable and on_readable_sync.
 *
 * The "on_commit" callback is also called from the Finisher thread
 * and indicates that all of the mutations have been durably
 * committed to stable storage (i.e., are now software/hardware
 * crashproof).
 *
 * At the implementation level, each mutation primitive (and its
 * associated data) can be serialized to a single buffer.  That
 * serialization, however, does not copy any data, but (using the
 * bufferlist library) will reference the original buffers.  This
 * implies that the buffer that contains the data being submitted
 * must remain stable until the on_commit callback completes.	 In
 * practice, bufferlist handles all of this for you and this
 * subtlety is only relevant if you are referencing an existing
 * buffer via buffer::raw_static.
 *
 * Some implementations of ObjectStore choose to implement their own
 * form of journaling that uses the serialized form of a
 * Transaction. This requires that the encode/decode logic properly
 * version itself and handle version upgrades that might change the
 * format of the encoded Transaction. This has already happened a
 * couple of times and the Transaction object contains some helper
 * variables that aid in this legacy decoding.
 *
 *
 * TRANSACTION ISOLATION
 *
 * Except as noted below, isolation is the responsibility of the
 * caller. In other words, if any storage element (storage element
 * == any of the four portions of an object as described above) is
 * altered by a transaction (including deletion), the caller
 * promises not to attempt to read that element while the
 * transaction is pending (here pending means from the time of
 * issuance until the "on_applied_sync" callback has been
 * received). Violations of isolation need not be detected by
 * ObjectStore and there is no corresponding error mechanism for
 * reporting an isolation violation (crashing would be the
 * appropriate way to report an isolation violation if detected).
 *
 * Enumeration operations may violate transaction isolation as
 * described above when a storage element is being created or
 * deleted as part of a transaction. In this case, ObjectStore is
 * allowed to consider the enumeration operation to either preceed
 * or follow the violating transaction element. In other words, the
 * presence/absence of the mutated element in the enumeration is
 * entirely at the discretion of ObjectStore. The arbitrary ordering
 * applies independently to each transaction element. For example,
 * if a transaction contains two mutating elements "create A" and
 * "delete B". And an enumeration operation is performed while this
 * transaction is pending. It is permissable for ObjectStore to
 * report any of the four possible combinations of the existance of
 * A and B.
 *
 */
class Transaction {
 public:
  enum {
    OP_NOP =		0,
    OP_TOUCH =	9,   // ch, oid
    OP_WRITE =	10,  // ch, oid, offset, len, bl
    OP_ZERO =		11,  // ch, oid, offset, len
    OP_TRUNCATE =	12,  // ch, oid, len
    OP_REMOVE =	13,  // ch, oid
    OP_SETATTR =	14,  // ch, oid, attrname, bl
    OP_SETATTRS =	15,  // ch, oid, attrset
    OP_RMATTR =	16,  // ch, oid, attrname
    OP_CLONE =	17,  // ch, oid, newoid
    OP_CLONERANGE =	18,  // ch, oid, newoid, offset, len
    OP_CLONERANGE2 =	30,  // ch, oid, newoid, srcoff, len, dstoff

    OP_TRIMCACHE =	19,  // ch, oid, offset, len  **DEPRECATED**

    OP_MKCOLL =	20,  // ch
    OP_RMCOLL =	21,  // ch
    OP_COLL_ADD =	22,  // ch, oldch, oid
    OP_COLL_REMOVE =	23,  // ch, oid
    OP_COLL_SETATTR = 24,  // ch, attrname, bl
    OP_COLL_RMATTR =	25,  // ch, attrname
    OP_COLL_SETATTRS = 26,  // ch, attrset
    OP_COLL_MOVE =	8,   // newch, oldch, oid

    OP_STARTSYNC =	27,  // start a sync

    OP_RMATTRS =	28,  // ch, oid
    OP_COLL_RENAME =	     29,  // ch, newch

    OP_OMAP_CLEAR = 31,   // ch
    OP_OMAP_SETKEYS = 32, // ch, attrset
    OP_OMAP_RMKEYS = 33,  // ch, keyset
    OP_OMAP_SETHEADER = 34, // ch, header
    OP_OMAP_RMKEYRANGE = 37,	// ch, oid, firstkey, lastkey
    OP_COLL_MOVE_RENAME = 38,	  // oldch, oldoid, newch, newoid

    OP_SETALLOCHINT = 39,  // ch, oid, object_size, write_size
  };

  static const uint32_t FLAG_NONE = 0x0000;
  static const uint32_t FLAG_REF = 0x0001;

  struct Op {
    uint32_t op;
    uint16_t c1_ix, c2_ix; // Collection slot offsets
    uint16_t o1_ix, o2_ix; // Object slot offsets
    uint64_t off, off2;
    uint64_t len;
    bufferlist data;
    string name, name2;
    map<string, bufferptr> xattrs;
    map<string, bufferlist> attrs;
    set<string> keys;
    uint64_t value1;
    uint64_t value2;

    Op(uint32_t op = OP_NOP) : op(op) {} // leave most fields uninitialized

    void encode(bufferlist &bl) const;
    void decode(bufferlist::iterator &p);
  };

  typedef vector<Op>::iterator op_iterator;
  op_iterator begin() { return ops.begin(); }
  op_iterator end() { return ops.end(); }


  col_slot_t& c_slot(uint16_t ix) {
    return col_slots[ix];
  }

  obj_slot_t& o_slot(uint16_t ix) {
    return obj_slots[ix];
  }

  ObjectStore* os;

 private:
  // Handle arrays, which UL may provide, if already open
  vector<col_slot_t> col_slots;
  vector<obj_slot_t> obj_slots;

  // Ops reference collections and objects by slot (id or handle)
  vector<Op> ops;

  // current highest slot by type
  uint16_t col_ix; // cols ix (cid or col, if available)
  uint16_t obj_ix; // obj ix (oid or obj, if available)

  uint32_t largest_data_len, largest_data_off;
  int64_t pool_override;
  bool replica;
  bool tolerate_collection_add_enoent;

  Context::List on_applied, on_commit, on_applied_sync;

  // member hook for intrusive work queue
  bi::list_member_hook<> queue_hook;

  /* copy and assignment disabled because of object handle ref\
   * counting / atomicity */
 private:
  Transaction(const Transaction &rhs) = delete;
  const Transaction& operator=(const Transaction &rhs) = delete;

  void close_collections();
  void put_objects();

 public:
  void set_tolerate_collection_add_enoent() {
    tolerate_collection_add_enoent = true;
  }
  bool get_tolerate_collection_add_enoent() const {
    return tolerate_collection_add_enoent;
  }

  /* Operations on callback contexts */
  void register_on_applied(Context* c) {
    if (!c) return;
    on_applied.push_back(*c);
  }
  void register_on_commit(Context* c) {
    if (!c) return;
    on_commit.push_back(*c);
  }
  void register_on_applied_sync(Context* c) {
    if (!c) return;
    on_applied_sync.push_back(*c);
  }
  void register_on_complete(Context* c) {
    if (!c) return;
    RunOnDeleteRef _complete(new RunOnDelete(c));
    register_on_applied(new ContainerContext<RunOnDeleteRef>(_complete));
    register_on_commit(new ContainerContext<RunOnDeleteRef>(_complete));
  }

  static void collect_contexts(
      list<Transaction* > &t,
      Context** out_on_applied,
      Context** out_on_commit,
      Context** out_on_applied_sync) {
    assert(out_on_applied);
    assert(out_on_commit);
    assert(out_on_applied_sync);
    Context::List on_applied, on_commit, on_applied_sync;
    for (auto i = t.begin(); i != t.end(); ++i) {
      on_applied.splice(on_applied.end(), (*i)->on_applied);
      on_commit.splice(on_commit.end(), (*i)->on_commit);
      on_applied_sync.splice(on_applied_sync.end(), (*i)->on_applied_sync);
    }
    *out_on_applied = C_Contexts::list_to_context(on_applied);
    *out_on_commit = C_Contexts::list_to_context(on_commit);
    *out_on_applied_sync = C_Contexts::list_to_context(on_applied_sync);
  }

  Context* get_on_applied() {
    return C_Contexts::list_to_context(on_applied);
  }

  Context* get_on_commit() {
    return C_Contexts::list_to_context(on_commit);
  }

  Context* get_on_applied_sync() {
    return C_Contexts::list_to_context(on_applied_sync);
  }

  /// For legacy transactions, provide the pool to override the
  /// encoded pool with
  void set_pool_override(int64_t pool) {
    pool_override = pool;
  }

  void set_replica() {
    replica = true;
  }

  bool get_replica() { return replica; }

  void swap(Transaction& other) {
    std::swap(os, other.os);
    std::swap(col_slots, other.col_slots);
    std::swap(obj_slots, other.obj_slots);
    std::swap(ops, other.ops);
    std::swap(col_ix, other.col_ix);
    std::swap(obj_ix, other.obj_ix);
    std::swap(largest_data_len, other.largest_data_len);
    std::swap(largest_data_off, other.largest_data_off);
    std::swap(on_applied, other.on_applied);
    std::swap(on_commit, other.on_commit);
    std::swap(on_applied_sync, other.on_applied_sync);
  }

  /// Size of largest data buffer to the "write" operation encountered so far
  uint32_t get_data_length() {
    return largest_data_len;
  }

  /// offset of buffer as aligned to destination within object.
  int get_data_alignment() {
    if (!largest_data_len)
      return -1;
    return largest_data_off & ~CEPH_PAGE_MASK;
  }

  /// Is the Transaction empty (no operations)
  bool empty() {
    return ops.empty();
  }
  /// Number of operations in the transation
  int get_num_ops() {
    return ops.size();
  }

  /**
   * Helper functions to encode the various mutation elements of a
   * transaction.  These are 1:1 with the operation codes (see
   * enumeration above).  These routines ensure that the
   * encoder/creator of a transaction gets the right data in the
   * right place. Sadly, there's no corresponding version nor any
   * form of seat belts for the decoder.
   */

  // Explicitly push a new collection slot, where the collection
  // is known by its identifier
  int push_cid(const coll_t& cid) {
    for (uint16_t i = 0; i < col_ix; i++)
      if (std::get<1>(col_slots[i]) == cid)
        return i;
    col_slots.push_back(col_slot_t(nullptr, cid, 0));
    return col_ix++;
  }

  // Explicitly push a new collection slot, where the collection
  // is known by its handle
  int push_col(ceph::os::Collection* ch);

  // Ditto, for objects
  int push_oid(const hoid_t& oid) {
    for (uint16_t i = 0; i < obj_ix; i++)
      if (std::get<1>(obj_slots[i]) == oid)
        return i;
    obj_slots.push_back(obj_slot_t(nullptr, oid, 0));
    return obj_ix++;
  }

  int push_obj(ceph::os::Object* oh);

  /// Commence a global file system sync operation.
  void start_sync() {
    ops.push_back(Op(OP_STARTSYNC));
  }

  /// noop. 'nuf said
  void nop() {
    ops.push_back(Op(OP_NOP));
  }

  /**
   * touch
   *
   * Ensure the existance of an object in a collection. Create an
   * empty object if necessary
   */
  void touch(int col_ix, int obj_ix) {
    ops.push_back(Op(OP_TOUCH));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
  }

  void touch() {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    touch(col_ix - 1, obj_ix - 1);
  }

  /**
   * Write data to an offset within an object. If the object is too
   * small, it is expanded as needed.	 It is possible to specify an
   * offset beyond the current end of an object and it will be
   * expanded as needed. Simple implementations of ObjectStore will
   * just zero the data between the old end of the object and the
   * newly provided data. More sophisticated implementations of
   * ObjectStore will omit the untouched data and store it as a
   * "hole" in the file.
   */
  void write(int col_ix, int obj_ix, uint64_t off, uint64_t len,
             const bufferlist& data) {
    ops.push_back(Op(OP_WRITE));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.off = off;
    op.len = len;
    op.data = data;
    assert(len == data.length());
    if (data.length() > largest_data_len) {
      largest_data_len = data.length();
      largest_data_off = off;
    }
  }

  void write(uint64_t off, uint64_t len, const bufferlist& data) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    write(col_ix - 1, obj_ix - 1, off, len, data);
  }

  /**
   * zero out the indicated byte range within an object. Some
   * ObjectStore instances may optimize this to release the
   * underlying storage space.
   */
  void zero(int col_ix, int obj_ix, uint64_t off, uint64_t len) {
    ops.push_back(Op(OP_ZERO));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.off = off;
    op.len = len;
  }

  void zero(uint64_t off, uint64_t len) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    zero(col_ix - 1, obj_ix - 1, off, len);
  }

  /// Discard all data in the object beyond the specified size.
  void truncate(int col_ix, int obj_ix, uint64_t off) {
    ops.push_back(Op(OP_TRUNCATE));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.off = off;
  }

  void truncate(uint64_t off) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    truncate(col_ix - 1, obj_ix - 1, off);
  }

  /// Remove an object. All four parts of the object are removed.
  void remove(int col_ix, int obj_ix) {
    ops.push_back(Op(OP_REMOVE));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
  }

  void remove() {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    remove(col_ix - 1, obj_ix - 1);
  }

  /// Set an xattr of an object
  void setattr(int col_ix, int obj_ix, const char* name,
               bufferlist& val) {
    string n(name);
    setattr(col_ix, obj_ix, n, val);
  }

  /// Set an xattr of an object
  void setattr(int col_ix, int obj_ix, const string& s,
               bufferlist& val) {
    ops.push_back(Op(OP_SETATTR));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.name = s;
    op.data = val;
  }

  /// Set an xattr of an object
  void setattr(const string& s, bufferlist& val) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    setattr(col_ix - 1, obj_ix - 1, s, val);
  }

  /// Set multiple xattrs of an object
  void setattrs(int col_ix, int obj_ix,
                map<string, bufferlist>& attrset) {
    ops.push_back(Op(OP_SETATTRS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    // encode/decode into bufferptr map
    bufferlist bl;
    ::encode(attrset, bl);
    bufferlist::iterator p = bl.begin();
    ::decode(op.xattrs, p);
  }

  void setattrs(int col_ix, int obj_ix,
                map<string, bufferptr>& attrset) {
    ops.push_back(Op(OP_SETATTRS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.xattrs.swap(attrset);
  }

  void setattrs(map<string, bufferlist>& attrset) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    setattrs(col_ix - 1, obj_ix - 1, attrset);
  }

  void setattrs(map<string, bufferptr>& attrset) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    setattrs(col_ix - 1, obj_ix - 1, attrset);
  }

  /// remove an xattr from an object
  void rmattr(int col_ix, int obj_ix, const char* name) {
    string n(name);
    rmattr(col_ix, obj_ix, n);
  }

  void rmattr(int col_ix, int obj_ix, const string& s) {
    ops.push_back(Op(OP_RMATTR));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.name = s;
  }

  void rmattr(const char* name) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    string n(name);
    rmattr(col_ix - 1, obj_ix - 1, n);
  }

  void rmattr(const string& s) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    rmattr(col_ix - 1, obj_ix - 1, s);
  }

  /// remove all xattrs from an object
  void rmattrs(int col_ix, int obj_ix) {
    ops.push_back(Op(OP_RMATTRS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
  }

  void rmattrs() {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    rmattrs(col_ix - 1, obj_ix - 1);
  }

  /**
   * Clone an object into another object.
   *
   * Low-cost (e.g., O(1)) cloning (if supported) is best, but
   * fallback to an O(n) copy is allowed.  All four parts of the
   * object are cloned (data, xattrs, omap header, omap
   * entries).
   *
   * The destination named object may already exist in
   * which case its previous contents are discarded.
   */
  void clone(int col_ix, int obj1_ix, int obj2_ix) {
    ops.push_back(Op(OP_CLONE));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj1_ix;
    op.o2_ix = obj2_ix;
  }

  /**
   * Clone a byte range from one object to another.
   *
   * The data portion of the destination object receives a copy of a
   * portion of the data from the source object. None of the other
   * three parts of an object is copied from the source.
   */
  void clone_range(int col_ix, int obj1_ix, int obj2_ix,
                   uint64_t srcoff, uint64_t srclen,
                   uint64_t dstoff) {
    ops.push_back(Op(OP_CLONERANGE2));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.o2_ix = obj2_ix;
    op.off = srcoff;
    op.len = srclen;
    op.off2 = dstoff;
  }

  /// Create a collection
  int create_collection(const coll_t& cid) {
    auto cix = push_cid(cid);
    ops.push_back(Op(OP_MKCOLL));
    Op &op = ops.back();
    op.c1_ix = cix;
    return cix;
  }

  /// remove the collection, the collection must be empty
  void remove_collection(const coll_t& cid) {
    auto cix = push_cid(cid);
    remove_collection(cix);
  }

  void remove_collection(int col_ix) {
    ops.push_back(Op(OP_RMCOLL));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    // future ops at col_ix will fail
  }

  /**
   * Add object to another collection (DEPRECATED)
   *
   * The Object is added to the new collection. This is a virtual
   * add, we now have two names for the same object.	This is only
   * used for conversion of old stores to new stores and is not
   * needed for new implementations unless they expect to make use
   * of the conversion infrastructure.
   */
  void collection_add(int col1_ix, int col2_ix, int obj_ix) {
    ops.push_back(Op(OP_COLL_ADD));
    Op &op = ops.back();
    op.c1_ix = col1_ix;
    op.c2_ix = col2_ix;
    op.o1_ix = obj_ix;
  }

  void collection_remove(int col_ix, int obj_ix) {
    ops.push_back(Op(OP_COLL_REMOVE));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
  }

  /* XXX atomicity? */
  void collection_move(
      int col1_ix, int col2_ix /* orig col */, int obj_ix) {
    collection_add(col1_ix, col2_ix, obj_ix);
    collection_remove(col2_ix, obj_ix);
  }

  void collection_move_rename(int col1_ix /* orig */,
                              int obj1_ix /* orig */,
                              int col2_ix, int obj2_ix) {
    ops.push_back(Op(OP_COLL_MOVE_RENAME));
    Op &op = ops.back();
    op.c1_ix = col1_ix;
    op.c2_ix = col2_ix;
    op.o1_ix = obj1_ix;
    op.o2_ix = obj2_ix;
  }

  /// Set an xattr on a collection
  void collection_setattr(int col_ix, const char* name,
                          bufferlist& val) {
    string n(name);
    collection_setattr(col_ix, n, val);
  }

  /// Set an xattr on a collection
  void collection_setattr(int col_ix, const string& name,
                          bufferlist& val) {
    ops.push_back(Op(OP_COLL_SETATTR));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.name = name;
    op.data = val;
  }

  void collection_setattr(const char* name, bufferlist& val) {
    assert(col_ix > 0);
    string n(name);
    collection_setattr(col_ix - 1, n, val);
  }

  void collection_setattr(const string& name, bufferlist& val) {
    assert(col_ix > 0);
    collection_setattr(col_ix - 1, name, val);
  }

  /// Remove an xattr from a collection
  void collection_rmattr(int col_ix, const char* name) {
    string n(name);
    collection_rmattr(col_ix, n);
  }

  /// Remove an xattr from a collection
  void collection_rmattr(int col_ix, const string& name) {
    ops.push_back(Op(OP_COLL_RMATTR));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.name = name;
  }

  /// Set multiple xattrs on a collection
  void collection_setattrs(int col_ix, map<string, bufferlist>& aset) {
    ops.push_back(Op(OP_COLL_SETATTRS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    // encode/decode into a bufferptr map
    bufferlist bl;
    ::encode(aset, bl);
    bufferlist::iterator p = bl.begin();
    ::decode(op.xattrs, p);
  }

  void collection_setattrs(int col_ix, map<string, bufferptr>& aset) {
    ops.push_back(Op(OP_COLL_SETATTRS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.xattrs.swap(aset);
  }

  void collection_setattrs(map<string, bufferlist>& aset) {
    assert(col_ix > 0);
    collection_setattrs(col_ix - 1, aset);
  }

  void collection_setattrs(map<string, bufferptr>& aset) {
    assert(col_ix > 0);
    collection_setattrs(col_ix - 1, aset);
  }

  /// Change the name of a collection
  void collection_rename(int col1_ix /* old */,
                         int col2_ix /* new */) {
    ops.push_back(Op(OP_COLL_RENAME));
    Op &op = ops.back();
    op.c1_ix = col1_ix;
    op.c2_ix = col2_ix;
  }

  /// Remove omap from obj
  void omap_clear(
      int col_ix, ///< [in] Collection containing oid
      int obj_ix ///< [in] Object from which to remove omap
      ) {
    ops.push_back(Op(OP_OMAP_CLEAR));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
  }

  void omap_clear() {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    omap_clear(col_ix - 1, obj_ix - 1);
  }

  /// Set keys on oid omap.  Replaces duplicate keys.
  void omap_setkeys(
      int col_ix, ///< [in] Collection containing oid
      int obj_ix, ///< [in] Object to update
      const map<string, bufferlist> &attrset ///< [in] Replacement
      ///keys and values
      ) {
    ops.push_back(Op(OP_OMAP_SETKEYS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.attrs = attrset; // TODO: swap instead of copy
  }

  void omap_setkeys(
      const map<string, bufferlist> &attrset ///< [in] Replacement
      ///keys and values
      ) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    omap_setkeys(col_ix - 1, obj_ix - 1, attrset);
  }

  /// Remove keys from oid omap
  void omap_rmkeys(
      int col_ix, ///< [in] Collection containing oid
      int obj_ix, ///< [in] Object from which to remove the omap
      const set<string> &keys ///< [in] Keys to clear
      ) {
    ops.push_back(Op(OP_OMAP_RMKEYS));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.keys = keys; // TODO: swap instead of copy
  }

  void omap_rmkeys(
      const set<string> &keys ///< [in] Keys to clear
      ) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    omap_rmkeys(col_ix - 1, obj_ix - 1, keys);
  }


  /// Remove key range from oid omap
  void omap_rmkeyrange(
      int col_ix, ///< [in] Collection containing oid
      int obj_ix, ///< [in] Object from which to remove the omap keys
      const string& first,  ///< [in] first key in range
      const string& last    ///< [in] first key past range, range is
      ///[first,last)
      ) {
        ops.push_back(Op(OP_OMAP_RMKEYRANGE));
        Op &op = ops.back();
        op.c1_ix = col_ix;
        op.o1_ix = obj_ix;
        op.name = first;
        op.name2 = last;
      }

  /// Set omap header
  void omap_setheader(
      int col_ix, ///< [in] Collection containing oid
      int obj_ix, ///< [in] Object
      const bufferlist &bl  ///< [in] Header value
      ) {
    ops.push_back(Op(OP_OMAP_SETHEADER));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.data = bl;
  }

  void omap_setheader(
      const bufferlist &bl  ///< [in] Header value
      ) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    omap_setheader(col_ix - 1, obj_ix - 1, bl);
  }

  void set_alloc_hint(
      int col_ix,
      int obj_ix,
      uint64_t expected_object_size,
      uint64_t expected_write_size
      ) {
    ops.push_back(Op(OP_SETALLOCHINT));
    Op &op = ops.back();
    op.c1_ix = col_ix;
    op.o1_ix = obj_ix;
    op.value1 = expected_object_size;
    op.value2 = expected_write_size;
  }

  void set_alloc_hint(
      uint64_t expected_object_size,
      uint64_t expected_write_size
      ) {
    assert(col_ix > 0);
    assert(obj_ix > 0);
    set_alloc_hint(col_ix - 1, obj_ix - 1, expected_object_size,
                   expected_write_size);
  }

  // etc.
  Transaction(size_t op_count_hint = 0)
    : os(nullptr), col_ix(0), obj_ix(0),
      largest_data_len(0), largest_data_off(0), replica(false),
      tolerate_collection_add_enoent(false)
  {
    ops.reserve(op_count_hint);
  }

  Transaction(bufferlist::iterator &dp)
    : os(nullptr), col_ix(0), obj_ix(0),
      largest_data_len(0), largest_data_off(0), replica(false),
      tolerate_collection_add_enoent(false)
  {
    decode(dp);
  }

  Transaction(bufferlist &nbl)
    : os(nullptr), col_ix(0), obj_ix(0),
      largest_data_len(0), largest_data_off(0), replica(false),
      tolerate_collection_add_enoent(false)
  {
    bufferlist::iterator dp = nbl.begin();
    decode(dp);
  }

  ~Transaction() {
    put_objects();
    close_collections();
  }

  // restore Transaction to a newly-constructed state for reuse
  void clear() {
    // drop existing refs
    put_objects();
    close_collections();
    os = nullptr;
    col_slots.clear();
    obj_slots.clear();
    ops.clear();
    col_ix = obj_ix = 0;
    largest_data_len = largest_data_off = 0;
    replica = tolerate_collection_add_enoent = false;
  }

  void encode(bufferlist& bl) const
  {
    ENCODE_START(8, 8, bl);
    ::encode(col_slots, bl);
    ::encode(obj_slots, bl);
    ::encode(ops, bl);
    ::encode(col_ix, bl);
    ::encode(obj_ix, bl);
    ::encode(largest_data_len, bl);
    ::encode(largest_data_off, bl);
    ::encode(tolerate_collection_add_enoent, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator &bl)
  {
    DECODE_START(8, bl);
    ::decode(col_slots, bl);
    ::decode(obj_slots, bl);
    ::decode(ops, bl);
    ::decode(col_ix, bl);
    ::decode(obj_ix, bl);
    ::decode(largest_data_len, bl);
    ::decode(largest_data_off, bl);
    ::decode(tolerate_collection_add_enoent, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f);
  static void generate_test_instances(list<Transaction*>& o);
};

WRITE_CLASS_ENCODER(Transaction::Op)
WRITE_CLASS_ENCODER(Transaction)

void encode(const col_slot_t &c_slot, bufferlist &bl);
void decode(col_slot_t &c_slot, bufferlist::iterator &p);

void encode(const obj_slot_t &o_slot, bufferlist &bl);
void decode(obj_slot_t &o_slot, bufferlist::iterator &p);

#endif /* CEPH_OS_TRANSACTION_H */
