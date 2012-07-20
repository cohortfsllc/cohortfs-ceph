// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef DBOBJECTMAP_DB_H
#define DBOBJECTMAP_DB_H

#include "include/buffer.h"
#include <set>
#include <map>
#include <string>

#include <string>
#include <vector>
#include <tr1/memory>
#include <boost/scoped_ptr.hpp>

#include "IndexManager.h"
#include "ObjectMap.h"
#include "KeyValueDB.h"
#include "osd/osd_types.h"
#include "common/Mutex.h"
#include "common/Cond.h"

/**
 * DBObjectMap: Implements ObjectMap in terms of KeyValueDB
 *
 * Prefix space structure:
 *
 * @see complete_prefix
 * @see user_prefix
 * @see sys_prefix
 *
 * - LEAF_PREFIX: Contains mapping from (coll_t,hobject_t)->seq
 * - REVERSE_LEAF_PREFIX: Contains mapping from seq->[(coll_t, hobject_t)]
 *                        @see set_map_header
 *                        @see remove_map_header
 * - SYS_PREFIX: GLOBAL_STATE_KEY - contains next seq number
 *                                  @see State
 *                                  @see write_state
 *                                  @see init
 *                                  @see generate_new_header
 * - USER_PREFIX + header_key(header->seq) + USER_PREFIX
 *              : key->value for header->seq
 * - USER_PREFIX + header_key(header->seq) + COMPLETE_PREFIX: see below
 * - USER_PREFIX + header_key(header->seq) + XATTR_PREFIX: xattrs
 * - USER_PREFIX + header_key(header->seq) + SYS_PREFIX
 *              : USER_HEADER_KEY - omap header for header->seq
 *              : HEADER_KEY - encoding of header for header->seq
 *
 * For each node (represented by a header, not counting LEAF_PREFIX space), we
 * store three mappings: the key mapping, the complete mapping, and the parent.
 * The complete mapping (COMPLETE_PREFIX space) is key->key.  Each x->y entry in
 * this mapping indicates that the key mapping contains all entries on [x,y).
 * Note, max string is represented by "", so ""->"" indicates that the parent
 * is unnecessary (@see rm_keys).  When looking up a key not contained in the
 * the complete set, we have to check the parent if we don't find it in the
 * key set.  During rm_keys, we copy keys from the parent and update the
 * complete set to reflect the change @see rm_keys.
 */
class DBObjectMap : public ObjectMap {
public:
  boost::scoped_ptr<KeyValueDB> db;

  /**
   * Each header has a unique id.  This value is updated and written *before*
   * the transaction writing th new header
   */
  uint64_t next_seq;

  /**
   * Serializes access to next_seq as well as the in_use set
   */
  Mutex header_lock;
  Cond header_cond;

  /**
   * Set of headers currently in use
   */
  set<uint64_t> in_use;

  DBObjectMap(KeyValueDB *db) : db(db), next_seq(1),
				header_lock("DBOBjectMap")
    {}

  int set_keys(
    const hobject_t &hoid,
    Index index,
    const map<string, bufferlist> &set
    );

  int set_header(
    const hobject_t &hoid,
    Index index,
    const bufferlist &bl
    );

  int get_header(
    const hobject_t &hoid,
    Index index,
    bufferlist *bl
    );

  int clear(
    const hobject_t &hoid,
    Index index
    );

  int rm_keys(
    const hobject_t &hoid,
    Index index,
    const set<string> &to_clear
    );

  int get(
    const hobject_t &hoid,
    Index index,
    bufferlist *header,
    map<string, bufferlist> *out
    );

  int get_keys(
    const hobject_t &hoid,
    Index index,
    set<string> *keys
    );

  int get_values(
    const hobject_t &hoid,
    Index index,
    const set<string> &keys,
    map<string, bufferlist> *out
    );

  int check_keys(
    const hobject_t &hoid,
    Index index,
    const set<string> &keys,
    set<string> *out
    );

  int get_xattrs(
    const hobject_t &hoid,
    Index index,
    const set<string> &to_get,
    map<string, bufferlist> *out
    );

  int get_all_xattrs(
    const hobject_t &hoid,
    Index index,
    set<string> *out
    );

  int set_xattrs(
    const hobject_t &hoid,
    Index index,
    const map<string, bufferlist> &to_set
    );

  int remove_xattrs(
    const hobject_t &hoid,
    Index index,
    const set<string> &to_remove
    );

  int clone(
    const hobject_t &hoid,
    Index index,
    const hobject_t &target,
    Index target_index
    );

  int link(
    const hobject_t &hoid,
    Index index,
    const hobject_t &target,
    Index target_index
    );

  /// Read initial state from backing store
  int init();

  /// Consistency check, debug, there must be no parallel writes
  bool check(std::ostream &out);

  /// Ensure that all previous operations are durable
  int sync();

  ObjectMapIterator get_iterator(const hobject_t &hoid,
				 Index index);

  static const string USER_PREFIX;
  static const string XATTR_PREFIX;
  static const string SYS_PREFIX;
  static const string COMPLETE_PREFIX;
  static const string HEADER_KEY;
  static const string USER_HEADER_KEY;
  static const string LEAF_PREFIX;
  static const string GLOBAL_STATE_KEY;
  static const string REVERSE_LEAF_PREFIX;

  /// persistent state for store @see generate_header
  struct State {
    uint64_t seq;
    State() : seq(0) {}
    State(uint64_t seq) : seq(seq) {}

    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(seq, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      ::decode(seq, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->dump_unsigned("seq", seq);
    }

    static void generate_test_instances(list<State*> &o) {
      o.push_back(new State(0));
      o.push_back(new State(20));
    }
  };

  struct _Header {
    uint64_t seq;
    uint64_t parent;
    uint64_t num_children;

    coll_t c;
    hobject_t hoid;

    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(seq, bl);
      ::encode(parent, bl);
      ::encode(num_children, bl);
      ::encode(c, bl);
      ::encode(hoid, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      ::decode(seq, bl);
      ::decode(parent, bl);
      ::decode(num_children, bl);
      ::decode(c, bl);
      ::decode(hoid, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->dump_unsigned("seq", seq);
      f->dump_unsigned("parent", parent);
      f->dump_unsigned("num_children", num_children);
      f->dump_stream("coll") << c;
      f->dump_stream("oid") << hoid;
    }

    static void generate_test_instances(list<_Header*> &o) {
      o.push_back(new _Header);
      o.push_back(new _Header);
      o.back()->parent = 20;
      o.back()->seq = 30;
    }

    _Header() : seq(0), parent(0), num_children(1) {}
  };

private:
  /// Implicit lock on Header->seq
  typedef std::tr1::shared_ptr<_Header> Header;

  /// String munging
  string hobject_key(coll_t c, const hobject_t &hoid);
  string map_header_key(coll_t c, const hobject_t &hoid);
  string header_key(uint64_t seq);
  string complete_prefix(Header header);
  string user_prefix(Header header);
  string sys_prefix(Header header);
  string xattr_prefix(Header header);
  string sys_parent_prefix(_Header header);
  string sys_parent_prefix(Header header) {
    return sys_parent_prefix(*header);
  }

  class EmptyIteratorImpl : public ObjectMapIteratorImpl {
  public:
    int seek_to_first() { return 0; }
    int seek_to_last() { return 0; }
    int upper_bound(const string &after) { return 0; }
    int lower_bound(const string &to) { return 0; }
    bool valid() { return false; }
    int next() { assert(0); return 0; }
    string key() { assert(0); return ""; }
    bufferlist value() { assert(0); return bufferlist(); }
    int status() { return 0; }
  };


  /// Iterator
  class DBObjectMapIteratorImpl : public ObjectMapIteratorImpl {
  public:
    DBObjectMap *map;

    /// NOTE: implicit lock on header->seq AND for all ancestors
    Header header;

    /// parent_iter == NULL iff no parent
    std::tr1::shared_ptr<DBObjectMapIteratorImpl> parent_iter;
    KeyValueDB::Iterator key_iter;
    KeyValueDB::Iterator complete_iter;

    /// cur_iter points to currently valid iterator
    std::tr1::shared_ptr<ObjectMapIteratorImpl> cur_iter;
    int r;

    /// init() called, key_iter, complete_iter, parent_iter filled in
    bool ready;
    /// past end
    bool invalid;

    DBObjectMapIteratorImpl(DBObjectMap *map, Header header) :
      map(map), header(header), r(0), ready(false), invalid(true) {}
    int seek_to_first();
    int seek_to_last();
    int upper_bound(const string &after);
    int lower_bound(const string &to);
    bool valid();
    int next();
    string key();
    bufferlist value();
    int status();

    bool on_parent() {
      return cur_iter == parent_iter;
    }

    /// skips to next valid parent entry
    int next_parent();

    /// Tests whether to_test is in complete region
    int in_complete_region(const string &to_test, ///< [in] key to test
			   string *begin,         ///< [out] beginning of region
			   string *end            ///< [out] end of region
      ); ///< @returns true if to_test is in the complete region, else false

  private:
    int init();
    bool valid_parent();
    int adjust();
  };

  typedef std::tr1::shared_ptr<DBObjectMapIteratorImpl> DBObjectMapIterator;
  DBObjectMapIterator _get_iterator(Header header) {
    return DBObjectMapIterator(new DBObjectMapIteratorImpl(this, header));
  }

  /// sys

  /// Removes node corresponding to header
  void clear_header(Header header, KeyValueDB::Transaction t);

  /// Set node containing input to new contents
  void set_header(Header input, KeyValueDB::Transaction t);

  /// Remove leaf node corresponding to hoid in c
  void remove_map_header(coll_t c, const hobject_t &hoid,
			 Header header,
			 KeyValueDB::Transaction t);

  /// Set leaf node for c and hoid to the value of header
  void set_map_header(coll_t c, const hobject_t &hoid, _Header header,
		     KeyValueDB::Transaction t);

  /// Lookup or create header for c hoid
  Header lookup_create_map_header(coll_t c, const hobject_t &hoid,
				  KeyValueDB::Transaction t);

  /**
   * Generate new header for c hoid with new seq number
   *
   * Has the side effect of syncronously saving the new DBObjectMap state
   */
  Header generate_new_header(coll_t c, const hobject_t &hoid, Header parent);

  /// Lookup leaf header for c hoid
  Header lookup_map_header(coll_t c, const hobject_t &hoid);

  /// Lookup header node for input
  Header lookup_parent(Header input);


  /// Helpers
  int _get_header(Header header, bufferlist *bl);

  /// Scan keys in header into out_keys and out_values (if nonnull)
  int scan(Header header,
	   const set<string> &in_keys,
	   set<string> *out_keys,
	   map<string, bufferlist> *out_values);

  /// Remove header and all related prefixes
  int _clear(Header header,
	     KeyValueDB::Transaction t);
  /// Adds to t operations necessary to add new_complete to the complete set
  int merge_new_complete(Header header,
			 const map<string, string> &new_complete,
			 DBObjectMapIterator iter,
			 KeyValueDB::Transaction t);

  /// Writes out State (mainly next_seq)
  int write_state(bool sync = false);

  /// 0 if the complete set now contains all of key space, < 0 on error, 1 else
  int need_parent(DBObjectMapIterator iter);

  /// Copies header entry from parent @see rm_keys
  int copy_up_header(Header header,
		     KeyValueDB::Transaction t);

  /// Sets header @see set_header
  void _set_header(Header header, const bufferlist &bl,
		   KeyValueDB::Transaction t);


  /** 
   * Removes header seq lock once Header is out of scope
   * @see lookup_parent
   * @see generate_new_header
   */
  class RemoveOnDelete {
  public:
    DBObjectMap *db;
    RemoveOnDelete(DBObjectMap *db) :
      db(db) {}
    void operator() (_Header *header) {
      Mutex::Locker l(db->header_lock);
      db->in_use.erase(header->seq);
      db->header_cond.Signal();
      delete header;
    }
  };
  friend class RemoveOnDelete;
};
WRITE_CLASS_ENCODER(DBObjectMap::_Header)
WRITE_CLASS_ENCODER(DBObjectMap::State)

#endif
