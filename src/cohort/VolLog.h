// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_VOL_LOG_H
#define CEPH_VOL_LOG_H

// re-include our assert to clobber boost's
#include "include/assert.h"
#include "osd/osd_types.h"
#include "os/ObjectStore.h"
#include <list>
using namespace std;

struct vol_history_t {
  epoch_t epoch_created;       // epoch in which volume was created
  epoch_t last_epoch_started;
  epoch_t last_epoch_clean;

  epoch_t same_up_since; // same acting set since
  epoch_t same_interval_since; // same acting AND up set since
  epoch_t same_primary_since; // same primary at least back through this epoch.

  vol_history_t()
    : epoch_created(0),
      last_epoch_started(0), last_epoch_clean(0),
      same_up_since(0), same_interval_since(0), same_primary_since(0) {}

  bool merge(const vol_history_t &other) {
    // Here, we only update the fields which cannot be calculated from the OSDmap.
    bool modified = false;
    if (epoch_created < other.epoch_created) {
      epoch_created = other.epoch_created;
      modified = true;
    }
    if (last_epoch_started < other.last_epoch_started) {
      last_epoch_started = other.last_epoch_started;
      modified = true;
    }
    if (last_epoch_clean < other.last_epoch_clean) {
      last_epoch_clean = other.last_epoch_clean;
      modified = true;
    }
    return modified;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(vol_history_t)

inline ostream& operator<<(ostream& out, const vol_history_t& h) {
  return out << "ec=" << h.epoch_created
	     << " les/c " << h.last_epoch_started << "/" << h.last_epoch_clean
	     << " " << h.same_up_since << "/" << h.same_interval_since << "/" << h.same_primary_since;
}

struct vol_stat_t {
  eversion_t version;
  eversion_t reported;
  __u32 state;
  utime_t last_fresh;   // last reported
  utime_t last_change;  // new state != previous state
  utime_t last_active;
  utime_t last_clean;
  utime_t last_unstale;

  eversion_t log_start; // (log_start,version]
  eversion_t ondisk_log_start;  // there may be more on disk

  epoch_t created;
  epoch_t last_epoch_clean;

  object_stat_collection_t stats;
  bool stats_invalid;

  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size

  epoch_t mapping_epoch;

  utime_t last_became_active;

  vol_stat_t()
    : state(0),
      created(0), last_epoch_clean(0),
      stats_invalid(false),
      log_size(0), ondisk_log_size(0),
      mapping_epoch(0)
  { }

  void add(const vol_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
  }
  void sub(const vol_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
};
WRITE_CLASS_ENCODER(vol_stat_t)

struct vol_log_entry_t {
  enum {
    MODIFY = 1,
    CLONE = 2,
    DELETE = 3,
    BACKLOG = 4,  // event invented by generate_backlog [deprecated]
    LOST_REVERT = 5, // lost new version, revert to an older version.
    LOST_DELETE = 6, // lost new version, revert to no object (deleted).
    LOST_MARK = 7,   // lost new version, now EIO
  };
  static const char *get_op_name(int op) {
    switch (op) {
    case MODIFY:
      return "modify  ";
    case CLONE:
      return "clone   ";
    case DELETE:
      return "delete  ";
    case BACKLOG:
      return "backlog ";
    case LOST_REVERT:
      return "l_revert";
    case LOST_DELETE:
      return "l_delete";
    case LOST_MARK:
      return "l_mark  ";
    default:
      return "unknown ";
    }
  }
  const char *get_op_name() const {
    return get_op_name(op);
  }

  __s32      op;
  hobject_t  soid;
  eversion_t version, prior_version, reverting_to;
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  utime_t     mtime;  // this is the _user_ mtime, mind you
  bufferlist snaps;   // only for clone entries
  bool invalid_hash; // only when decoding sobject_t based entries

  uint64_t offset;   // [soft state] my offset on disk

  vol_log_entry_t()
    : op(0), invalid_hash(false), offset(0) {}
  vol_log_entry_t(int _op, const hobject_t& _soid,
		 const eversion_t& v, const eversion_t& pv,
		 const osd_reqid_t& rid, const utime_t& mt)
    : op(_op), soid(_soid), version(v),
      prior_version(pv),
      reqid(rid), mtime(mt), invalid_hash(false),
      offset(0) {}

  bool is_clone() const { return op == CLONE; }
  bool is_modify() const { return op == MODIFY; }
  bool is_backlog() const { return op == BACKLOG; }
  bool is_lost_revert() const { return op == LOST_REVERT; }
  bool is_lost_delete() const { return op == LOST_DELETE; }
  bool is_lost_mark() const { return op == LOST_MARK; }

  bool is_update() const {
    return is_clone() || is_modify() || is_backlog() || is_lost_revert() || is_lost_mark();
  }
  bool is_delete() const {
    return op == DELETE || op == LOST_DELETE;
  }

  bool reqid_is_indexed() const {
    return reqid != osd_reqid_t() && (op == MODIFY || op == DELETE);
  }

  string get_key_name() const;
  void encode_with_checksum(bufferlist& bl) const;
  void decode_with_checksum(bufferlist::iterator& p);

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;

};
WRITE_CLASS_ENCODER(vol_log_entry_t)

ostream& operator<<(ostream& out, const vol_log_entry_t& e);


struct vol_log_t {
  /*
   *   head - newest entry (update|delete)
   *   tail - entry previous to oldest (update|delete) for which we have
   *          complete negative information.
   * i.e. we can infer vol contents for any store whose last_update >= tail.
   */
  eversion_t head;    // newest entry
  eversion_t tail;    // version prior to oldest

  list<vol_log_entry_t> log;  // the actual log.

  vol_log_t() {}

  void clear() {
    eversion_t z;
    head = tail = z;
    log.clear();
  }

  bool empty() const {
    return log.empty();
  }

  bool null() const {
    return head.version == 0 && head.epoch == 0;
  }

  size_t approx_size() const {
    return head.version - tail.version;
  }

  list<vol_log_entry_t>::const_iterator find_entry(eversion_t v) const {
    int fromhead = head.version - v.version;
    int fromtail = v.version - tail.version;
    list<vol_log_entry_t>::const_iterator p;
    if (fromhead < fromtail) {
      p = log.end();
      --p;
      while (p->version > v)
	--p;
      return p;
    } else {
      p = log.begin();
      while (p->version < v)
	++p;
      return p;
    }
  }

  list<vol_log_entry_t>::iterator find_entry(eversion_t v) {
    int fromhead = head.version - v.version;
    int fromtail = v.version - tail.version;
    list<vol_log_entry_t>::iterator p;
    if (fromhead < fromtail) {
      p = log.end();
      --p;
      while (p->version > v)
	--p;
      return p;
    } else {
      p = log.begin();
      while (p->version < v)
	++p;
      return p;
    }
  }

  void copy_after(const vol_log_t &other, eversion_t from);

  void copy_range(const vol_log_t &other, eversion_t from, eversion_t to);

  /**
   * copy up to N entries
   *
   * @param o source log
   * @param max max number of entreis to copy
   */
  void copy_up_to(const vol_log_t &other, int max);

  ostream& print(ostream& out) const;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(vol_log_t)

inline ostream& operator<<(ostream& out, const vol_log_t& log)
{
  out << "log(" << log.tail << "," << log.head << "]";
  return out;
}

struct vol_info_t {
  uuid_d volid;
  eversion_t last_update;    // last object version applied to store.
  eversion_t last_complete;  // last version vol was complete through.
  epoch_t last_epoch_started;// last epoch at which this vol started on this osd

  eversion_t log_tail;     // oldest log entry.

  interval_set<snapid_t> purged_snaps;

  vol_stat_t stats;

  vol_history_t history;

  vol_info_t()
    : last_epoch_started(0)
  { }
  vol_info_t(const uuid_d& volid)
    : volid(volid),
      last_epoch_started(0)
  { }

  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(vol_info_t)

inline ostream& operator<<(ostream& out, const vol_info_t& voli)
{
  out << voli.volid << "(";
  if (voli.dne())
    out << " DNE";
  if (voli.is_empty())
    out << " empty";
  else {
    out << " v " << voli.last_update;
    if (voli.last_complete != voli.last_update)
      out << " lc " << voli.last_complete;
    out << " (" << voli.log_tail << "," << voli.last_update << "]";
  }
  out << " local-les=" << voli.last_epoch_started;
  out << " n=" << voli.stats.stats.sum.num_objects;
  out << " " << voli.history
      << ")";
  return out;
}

struct VolLog {
  /* Exceptions */
  class read_log_error : public buffer::error {
  public:
    explicit read_log_error(const char *what) {
      snprintf(buf, sizeof(buf), "read_log_error: %s", what);
    }
    const char *what() const throw () {
      return buf;
    }
  private:
    char buf[512];
  };

  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public vol_log_t {
    hash_map<hobject_t,vol_log_entry_t*> objects;
    hash_map<osd_reqid_t,vol_log_entry_t*> caller_ops;

    // recovery pointers
    list<vol_log_entry_t>::iterator complete_to;
    version_t last_requested; // last object requested by primary

    /****/
    IndexedLog() : last_requested(0) {}

    void claim_log(const vol_log_t& o) {
      log = o.log;
      head = o.head;
      tail = o.tail;
      index();
    }

    void zero() {
      unindex();
      vol_log_t::clear();
      reset_recovery_pointers();
    }
    void reset_recovery_pointers() {
      complete_to = log.end();
      last_requested = 0;
    }

    bool logged_object(const hobject_t& oid) const {
      return objects.count(oid);
    }
    bool logged_req(const osd_reqid_t &r) const {
      return caller_ops.count(r);
    }
    eversion_t get_request_version(const osd_reqid_t &r) const {
      hash_map<osd_reqid_t,vol_log_entry_t*>::const_iterator p
	= caller_ops.find(r);
      if (p == caller_ops.end())
	return eversion_t();
      return p->second->version;
    }

    void index() {
      objects.clear();
      caller_ops.clear();
      for (list<vol_log_entry_t>::iterator i = log.begin();
	   i != log.end();
	   ++i) {
	objects[i->soid] = &(*i);
	if (i->reqid_is_indexed()) {
	  caller_ops[i->reqid] = &(*i);
	}
      }
    }

    void index(vol_log_entry_t& e) {
      if (objects.count(e.soid) == 0 ||
	  objects[e.soid]->version < e.version)
	objects[e.soid] = &e;
      if (e.reqid_is_indexed()) {
	caller_ops[e.reqid] = &e;
      }
    }
    void unindex() {
      objects.clear();
      caller_ops.clear();
    }
    void unindex(vol_log_entry_t& e) {
      if (objects.count(e.soid) && objects[e.soid]->version == e.version)
	objects.erase(e.soid);
      if (e.reqid_is_indexed() &&
	  caller_ops.count(e.reqid) &&
	  caller_ops[e.reqid] == &e)
	caller_ops.erase(e.reqid);
    }

    // actors
    void add(vol_log_entry_t& e) {
      log.push_back(e);
      assert(e.version > head);
      assert(head.version == 0 || e.version.version > head.version);
      head = e.version;

      // to our index
      objects[e.soid] = &(log.back());
      if (e.reqid_is_indexed())
	caller_ops[e.reqid] = &(log.back());
    }

    void trim(eversion_t s);

    ostream& print(ostream& out) const;
  };


protected:
  //////////////////// data members ////////////////////

  map<eversion_t, hobject_t> divergent_priors;
  IndexedLog log;

  /// Log is clean on [dirty_to, dirty_from)
  bool touched_log;
  eversion_t dirty_to;
  eversion_t dirty_from;
  bool dirty_divergent_priors;

  bool is_dirty() const {
    return !touched_log ||
      (dirty_to != eversion_t()) ||
      (dirty_from != eversion_t::max()) ||
      dirty_divergent_priors;
  }
  void mark_dirty_to(eversion_t to) {
    if (to > dirty_to)
      dirty_to = to;
  }
  void mark_dirty_from(eversion_t from) {
    if (from < dirty_from)
      dirty_from = from;
  }
  void add_divergent_prior(eversion_t version, hobject_t obj) {
    divergent_priors.insert(make_pair(version, obj));
    dirty_divergent_priors = true;
  }

  /// DEBUG
  set<string> log_keys_debug;
  static void clear_after(set<string> *log_keys_debug, const string &lb) {
    if (!log_keys_debug)
      return;
    for (set<string>::iterator i = log_keys_debug->lower_bound(lb);
	 i != log_keys_debug->end();
	 log_keys_debug->erase(i++));
  }
  static void clear_up_to(set<string> *log_keys_debug, const string &ub) {
    if (!log_keys_debug)
      return;
    for (set<string>::iterator i = log_keys_debug->begin();
	 i != log_keys_debug->end() && *i < ub;
	 log_keys_debug->erase(i++));
  }
  void check() {
    assert(log.log.size() == log_keys_debug.size());
    for (list<vol_log_entry_t>::iterator i = log.log.begin();
	 i != log.log.end();
	 ++i) {
      assert(log_keys_debug.count(i->get_key_name()));
    }
  }

  void undirty() {
    dirty_to = eversion_t();
    dirty_from = eversion_t::max();
    dirty_divergent_priors = false;
    touched_log = true;
    check();
  }
public:
  VolLog() :
    touched_log(false), dirty_from(eversion_t::max()),
    dirty_divergent_priors(false) {}

  void clear();

  //////////////////// get or set log ////////////////////

  const IndexedLog &get_log() const { return log; }

  const eversion_t &get_tail() const { return log.tail; }

  void set_tail(eversion_t tail) { log.tail = tail; }

  const eversion_t &get_head() const { return log.head; }

  void set_head(eversion_t head) { log.head = head; }

  void set_last_requested(version_t last_requested) {
    log.last_requested = last_requested;
  }

  void index() { log.index(); }

  void unindex() { log.unindex(); }

  void add(vol_log_entry_t& e) {
    mark_dirty_from(e.version);
    log.add(e);
  }

  void reset_recovery_pointers() { log.reset_recovery_pointers(); }

  static void clear_info_log(
    uuid_d volid,
    const hobject_t &infos_oid,
    const hobject_t &log_oid,
    ObjectStore::Transaction *t);

  void trim(eversion_t trim_to, vol_info_t &info);

  void claim_log(const vol_log_t &o) {
    log.claim_log(o);
    mark_dirty_to(eversion_t::max());
  }

  void activate_not_complete(vol_info_t &info) {
    log.complete_to = log.log.begin();
    if (log.complete_to == log.log.begin()) {
      info.last_complete = eversion_t();
    } else {
      log.complete_to--;
      info.last_complete = log.complete_to->version;
      log.complete_to++;
    }
    log.last_requested = 0;
  }

protected:
  bool merge_old_entry(ObjectStore::Transaction& t, const vol_log_entry_t& oe,
		       const vol_info_t& info, list<hobject_t>& remove_snap);
public:
  void rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
			    vol_info_t &info, list<hobject_t>& remove_snap,
			    bool &dirty_info, bool &dirty_big_info);

  void merge_log(ObjectStore::Transaction& t, vol_info_t &oinfo,
		 vol_log_t &olog, int from,
		 vol_info_t &info, list<hobject_t>& remove_snap,
		 bool &dirty_info, bool &dirty_big_info);

  void write_log(ObjectStore::Transaction& t, const hobject_t &log_oid);

  static void write_log(ObjectStore::Transaction& t, vol_log_t &log,
    const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors);

  static void _write_log(
    ObjectStore::Transaction& t, vol_log_t &log,
    const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors,
    eversion_t dirty_to,
    eversion_t dirty_from,
    bool dirty_divergent_priors,
    bool touch_log,
    set<string> *log_keys_debug
    );

  bool read_log(ObjectStore *store, hobject_t log_oid,
		const vol_info_t &info, ostringstream &oss) {
    return read_log(store, log_oid, info, divergent_priors,
		    log, oss, &log_keys_debug);
  }

  /// return true if the log should be rewritten
  static bool read_log(ObjectStore *store, hobject_t log_oid,
		       const vol_info_t &info,
		       map<eversion_t, hobject_t> &divergent_priors,
		       IndexedLog &log, ostringstream &oss,
		       set<string> *log_keys_debug = 0);

protected:
  static void read_log_old(ObjectStore *store, coll_t coll, hobject_t log_oid,
			   const vol_info_t &info, map<eversion_t,
			   hobject_t> &divergent_priors,
			   IndexedLog &log, ostringstream &oss,
			   set<string> *log_keys_debug);
};

#endif // CEPH_VOL_LOG_H
