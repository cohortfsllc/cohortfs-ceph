// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef DBOBJECTMAP_DB_H
#define DBOBJECTMAP_DB_H

#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "include/buffer.h"
#include "ObjectMap.h"
#include "KeyValueDB.h"
#include "osd/osd_types.h"

/**
 * DBObjectMap: Implements ObjectMap in terms of KeyValueDB
 *
 * Prefix space structure:
 *
 * @see complete_prefix
 * @see user_prefix
 * @see sys_prefix
 *
 * - OIDO_SEQ: Contains leaf mapping from oid_t->hobj.seq and
 *		     corresponding omap header
 * - SYS_PREFIX: GLOBAL_STATE_KEY - contains next seq number
 *				    @see State
 *				    @see write_state
 *				    @see init
 *				    @see generate_new_header
 * - USER_PREFIX + header_key(header->seq) + USER_PREFIX
 *		: key->value for header->seq
 * - USER_PREFIX + header_key(header->seq) + COMPLETE_PREFIX: see below
 * - USER_PREFIX + header_key(header->seq) + XATTR_PREFIX: xattrs
 * - USER_PREFIX + header_key(header->seq) + SYS_PREFIX
 *		: USER_HEADER_KEY - omap header for header->seq
 *		: HEADER_KEY - encoding of header for header->seq
 *
 * For each node (represented by a header), we
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
  CephContext *cct;
public:
  boost::scoped_ptr<KeyValueDB> db;

  /**
   * Serializes access to next_seq as well as the in_use set
   */
  std::mutex header_lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  std::condition_variable header_cond;
  std::condition_variable map_header_cond;

  /**
   * Set of headers currently in use
   */
  set<uint64_t> in_use;
  set<oid_t> map_header_in_use;

  DBObjectMap(CephContext * c, KeyValueDB *db) : cct(c), db(db)
    {}

  int set_keys(
    const oid_t &oid,
    const map<string, bufferlist> &set,
    const SequencerPosition *spos=0
    );

  int set_header(
    const oid_t &oid,
    const bufferlist &bl,
    const SequencerPosition *spos=0
    );

  int get_header(
    const oid_t &oid,
    bufferlist *bl
    );

  int clear(
    const oid_t &oid,
    const SequencerPosition *spos=0
    );

  int clear_keys_header(
    const oid_t &oid,
    const SequencerPosition *spos=0
    );

  int rm_keys(
    const oid_t &oid,
    const set<string> &to_clear,
    const SequencerPosition *spos=0
    );

  int get(
    const oid_t &oid,
    bufferlist *header,
    map<string, bufferlist> *out
    );

  int get_keys(
    const oid_t &oid,
    set<string> *keys
    );

  int get_values(
    const oid_t &oid,
    const set<string> &keys,
    map<string, bufferlist> *out
    );

  int check_keys(
    const oid_t &oid,
    const set<string> &keys,
    set<string> *out
    );

  int get_xattrs(
    const oid_t &oid,
    const set<string> &to_get,
    map<string, bufferlist> *out
    );

  int get_all_xattrs(
    const oid_t &oid,
    set<string> *out
    );

  int set_xattrs(
    const oid_t &oid,
    const map<string, bufferlist> &to_set,
    const SequencerPosition *spos=0
    );

  int remove_xattrs(
    const oid_t &oid,
    const set<string> &to_remove,
    const SequencerPosition *spos=0
    );

  int clone(
    const oid_t &oid,
    const oid_t &target,
    const SequencerPosition *spos=0
    );

  /// Read initial state from backing store
  int init(bool upgrade = false);

  /// Upgrade store to current version
  int upgrade();

  /// Consistency check, debug, there must be no parallel writes
  bool check(std::ostream &out);

  /// Ensure that all previous operations are durable
  int sync(const oid_t *oid=0, const SequencerPosition *spos=0);

  /// Util, list all objects, there must be no other concurrent access
  int list_objects(vector<oid_t> *objs ///< [out] objects
    );

  ObjectMapIterator get_iterator(const oid_t &oid);

  static const string USER_PREFIX;
  static const string XATTR_PREFIX;
  static const string SYS_PREFIX;
  static const string COMPLETE_PREFIX;
  static const string HEADER_KEY;
  static const string USER_HEADER_KEY;
  static const string GLOBAL_STATE_KEY;
  static const string OIDO_SEQ;

  /// Legacy
  static const string LEAF_PREFIX;
  static const string REVERSE_LEAF_PREFIX;

  /// persistent state for store @see generate_header
  struct State {
    uint8_t v;
    uint64_t seq;
    State() : v(0), seq(1) {}
    State(uint64_t seq) : v(0), seq(seq) {}

    void encode(bufferlist &bl) const {
      ENCODE_START(2, 1, bl);
      ::encode(v, bl);
      ::encode(seq, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(2, bl);
      if (struct_v >= 2)
	::decode(v, bl);
      else
	v = 0;
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
  } state;

  struct _Header {
    uint64_t seq;
    uint64_t parent;
    uint64_t num_children;

    coll_t c;
    oid_t oid;

    SequencerPosition spos;

    void encode(bufferlist &bl) const {
      ENCODE_START(2, 1, bl);
      ::encode(seq, bl);
      ::encode(parent, bl);
      ::encode(num_children, bl);
      ::encode(c, bl);
      ::encode(oid, bl);
      ::encode(spos, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(2, bl);
      ::decode(seq, bl);
      ::decode(parent, bl);
      ::decode(num_children, bl);
      ::decode(c, bl);
      ::decode(oid, bl);
      if (struct_v >= 2)
	::decode(spos, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->dump_unsigned("seq", seq);
      f->dump_unsigned("parent", parent);
      f->dump_unsigned("num_children", num_children);
      f->dump_stream("coll") << c;
      f->dump_stream("oid") << oid;
    }

    static void generate_test_instances(list<_Header*> &o) {
      o.push_back(new _Header);
      o.push_back(new _Header);
      o.back()->parent = 20;
      o.back()->seq = 30;
    }

    _Header() : seq(0), parent(0), num_children(1) {}
  };

  /// String munging (public for testing)
  static string hobject_key(coll_t c, const oid_t &oid);
  static bool parse_hobject_key(const string &in,
				coll_t *c, oid_t *oid);
private:
  /// Implicit lock on Header->seq
  typedef std::shared_ptr<_Header> Header;

  string map_header_key(coll_t coll, const oid_t &oid);
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
    std::shared_ptr<DBObjectMapIteratorImpl> parent_iter;
    KeyValueDB::Iterator key_iter;
    KeyValueDB::Iterator complete_iter;

    /// cur_iter points to currently valid iterator
    std::shared_ptr<ObjectMapIteratorImpl> cur_iter;
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
			   string *begin,	  ///< [out] beginning of region
			   string *end		  ///< [out] end of region
      ); ///< @returns true if to_test is in the complete region, else false

  private:
    int init();
    bool valid_parent();
    int adjust();
  };

  typedef std::shared_ptr<DBObjectMapIteratorImpl> DBObjectMapIterator;
  DBObjectMapIterator _get_iterator(Header header) {
    return DBObjectMapIterator(new DBObjectMapIteratorImpl(this, header));
  }

  /// sys

  /// Removes node corresponding to header
  void clear_header(Header header, KeyValueDB::Transaction t);

  /// Set node containing input to new contents
  void set_header(Header input, KeyValueDB::Transaction t);

  /// Remove leaf node corresponding to oid in c
  void remove_map_header(const oid_t &oid,
			 Header header,
			 KeyValueDB::Transaction t);

  /// Set leaf node for c and oid to the value of header
  void set_map_header(const oid_t &oid, _Header header,
		      KeyValueDB::Transaction t);

  /// Set leaf node for c and oid to the value of header
  bool check_spos(const oid_t &oid,
		  Header header,
		  const SequencerPosition *spos);

  /// Lookup or create header for c obj
  Header lookup_create_map_header(const oid_t &oid,
				  KeyValueDB::Transaction t);

  /**
   * Generate new header for c oid with new seq number
   *
   * Has the side effect of syncronously saving the new DBObjectMap state
   */
  Header _generate_new_header(const oid_t &oid, Header parent);
  Header generate_new_header(const oid_t &oid, Header parent) {
    lock_guard l(header_lock);
    return _generate_new_header(oid, parent);
  }

  /// Lookup leaf header for c oid
  Header _lookup_map_header(const oid_t &oid, unique_lock& hl);
  Header lookup_map_header(const oid_t &oid) {
    unique_lock hl(header_lock);
    return _lookup_map_header(oid, hl);
  }

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
  int write_state(KeyValueDB::Transaction _t =
		  KeyValueDB::Transaction());

  /// 0 if the complete set now contains all of key space, < 0 on error, 1 else
  int need_parent(DBObjectMapIterator iter);

  /// Copies header entry from parent @see rm_keys
  int copy_up_header(Header header,
		     KeyValueDB::Transaction t);

  /// Sets header @see set_header
  void _set_header(Header header, const bufferlist &bl,
		   KeyValueDB::Transaction t);

  /**
   * Removes map header lock once Header is out of scope
   * @see lookup_map_header
   */
  class RemoveMapHeaderOnDelete {
  public:
    DBObjectMap *db;
    oid_t oid;
    RemoveMapHeaderOnDelete(DBObjectMap *db, const oid_t &oid) :
      db(db), oid(oid) {}
    void operator() (_Header *header) {
      lock_guard l(db->header_lock);
      db->map_header_in_use.erase(oid);
      db->map_header_cond.notify_all();
      delete header;
    }
  };

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
      lock_guard l(db->header_lock);
      db->in_use.erase(header->seq);
      db->header_cond.notify_all();
      delete header;
    }
  };
  friend class RemoveOnDelete;
};
WRITE_CLASS_ENCODER(DBObjectMap::_Header)
WRITE_CLASS_ENCODER(DBObjectMap::State)

#endif
