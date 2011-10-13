// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_PG_H
#define CEPH_PG_H

#include <boost/statechart/custom_reaction.hpp>
#include <boost/statechart/event.hpp>
#include <boost/statechart/simple_state.hpp>
#include <boost/statechart/state.hpp>
#include <boost/statechart/state_machine.hpp>
#include <boost/statechart/transition.hpp>

// re-include our assert to clobber boost's
#include "include/assert.h" 

#include "include/types.h"
#include "osd_types.h"
#include "include/buffer.h"
#include "include/xlist.h"
#include "include/atomic.h"

#include "OSDMap.h"
#include "os/ObjectStore.h"
#include "msg/Messenger.h"
#include "messages/MOSDRepScrub.h"

#include "common/DecayCounter.h"

#include <list>
#include <memory>
#include <string>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


//#define DEBUG_RECOVERY_OIDS   // track set of recovering oids explicitly, to find counting bugs


class OSD;
class MOSDOp;
class MOSDSubOp;
class MOSDSubOpReply;
class MOSDPGInfo;
class MOSDPGLog;


struct PGRecoveryStats {
  struct per_state_info {
    uint64_t enter, exit;     // enter/exit counts
    uint64_t events;
    utime_t event_time;       // time spent processing events
    utime_t total_time;       // total time in state
    utime_t min_time, max_time;

    per_state_info() : enter(0), exit(0), events(0) {}
  };
  map<const char *,per_state_info> info;
  Mutex lock;

  PGRecoveryStats() : lock("PGRecoverStats::lock") {}

  void reset() {
    Mutex::Locker l(lock);
    info.clear();
  }
  void dump(ostream& out) {
    Mutex::Locker l(lock);
    for (map<const char *,per_state_info>::iterator p = info.begin(); p != info.end(); p++) {
      per_state_info& i = p->second;
      out << i.enter << "\t" << i.exit << "\t"
	  << i.events << "\t" << i.event_time << "\t"
	  << i.total_time << "\t"
	  << i.min_time << "\t" << i.max_time << "\t"
	  << p->first << "\n";
	       
    }
  }

  void log_enter(const char *s) {
    Mutex::Locker l(lock);
    info[s].enter++;
  }
  void log_exit(const char *s, utime_t dur, uint64_t events, utime_t event_dur) {
    Mutex::Locker l(lock);
    per_state_info &i = info[s];
    i.exit++;
    i.total_time += dur;
    if (dur > i.max_time)
      i.max_time = dur;
    if (dur < i.min_time || i.min_time == utime_t())
      i.min_time = dur;
    i.events += events;
    i.event_time += event_dur;
  }
};

struct PGPool {
  int id;
  atomic_t nref;
  int num_pg;
  string name;
  uint64_t auid;

  pg_pool_t info;      
  SnapContext snapc;   // the default pool snapc, ready to go.

  interval_set<snapid_t> cached_removed_snaps;      // current removed_snaps set
  interval_set<snapid_t> newly_removed_snaps;  // newly removed in the last epoch

  PGPool(int i, const char *_name, uint64_t au) :
    id(i), num_pg(0), auid(au) {
    if (_name)
      name = _name;
  }

  void get() { nref.inc(); }
  void put() {
    if (nref.dec() == 0)
      delete this;
  }
};


/** PG - Replica Placement Group
 *
 */

class PG {
public:
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

  std::string gen_prefix() const;

  /*
   * PG::Info - summary of PG statistics.
   *
   * some notes: 
   *  - last_complete implies we have all objects that existed as of that
   *    stamp, OR a newer object, OR have already applied a later delete.
   *  - if last_complete >= log.bottom, then we know pg contents thru log.head.
   *    otherwise, we have no idea what the pg is supposed to contain.
   */
  struct Info {
    pg_t pgid;
    eversion_t last_update;    // last object version applied to store.
    eversion_t last_complete;  // last version pg was complete through.

    eversion_t log_tail;     // oldest log entry.
    bool       log_backlog;    // do we store a complete log?

    interval_set<snapid_t> purged_snaps;

    pg_stat_t stats;

    struct History {
      epoch_t epoch_created;       // epoch in which PG was created
      epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)
      epoch_t last_epoch_clean;    // lower bound on last epoch the PG was completely clean.
      epoch_t last_epoch_split;    // as parent

      epoch_t same_up_since;       // same acting set since
      epoch_t same_interval_since;   // same acting AND up set since
      epoch_t same_primary_since;  // same primary at least back through this epoch.

      eversion_t last_scrub;
      utime_t last_scrub_stamp;

      History() : 	      
	epoch_created(0),
	last_epoch_started(0), last_epoch_clean(0), last_epoch_split(0),
	same_up_since(0), same_interval_since(0), same_primary_since(0) {}

      void merge(const History &other) {
	// Here, we only update the fields which cannot be calculated from the OSDmap.
	if (epoch_created < other.epoch_created)
	  epoch_created = other.epoch_created;
	if (last_epoch_started < other.last_epoch_started)
	  last_epoch_started = other.last_epoch_started;
	if (last_epoch_clean < other.last_epoch_clean)
	  last_epoch_clean = other.last_epoch_clean;
	if (last_epoch_split < other.last_epoch_started)
	  last_epoch_split = other.last_epoch_started;
	if (other.last_scrub > last_scrub)
	  last_scrub = other.last_scrub;
	if (other.last_scrub_stamp > last_scrub_stamp)
	  last_scrub_stamp = other.last_scrub_stamp;
      }

      void encode(bufferlist &bl) const {
	__u8 struct_v = 3;
	::encode(struct_v, bl);
	::encode(epoch_created, bl);
	::encode(last_epoch_started, bl);
	::encode(last_epoch_clean, bl);
	::encode(last_epoch_split, bl);
	::encode(same_interval_since, bl);
	::encode(same_up_since, bl);
	::encode(same_primary_since, bl);
	::encode(last_scrub, bl);
	::encode(last_scrub_stamp, bl);
      }
      void decode(bufferlist::iterator &bl) {
	__u8 struct_v;
	::decode(struct_v, bl);
	::decode(epoch_created, bl);
	::decode(last_epoch_started, bl);
	if (struct_v >= 3)
	  ::decode(last_epoch_clean, bl);
	else
	  last_epoch_clean = last_epoch_started;  // careful, it's a lie!
	::decode(last_epoch_split, bl);
	::decode(same_interval_since, bl);
	::decode(same_up_since, bl);
	::decode(same_primary_since, bl);
	if (struct_v >= 2) {
	  ::decode(last_scrub, bl);
	  ::decode(last_scrub_stamp, bl);
	}
      }
    } history;
    
    Info() : log_backlog(false) {}
    Info(pg_t p) : pgid(p), log_backlog(false) { }

    bool is_empty() const { return last_update.version == 0; }
    bool dne() const { return history.epoch_created == 0; }

    void encode(bufferlist &bl) const {
      __u8 v = 23;
      ::encode(v, bl);

      ::encode(pgid, bl);
      ::encode(last_update, bl);
      ::encode(last_complete, bl);
      ::encode(log_tail, bl);
      ::encode(log_backlog, bl);
      ::encode(stats, bl);
      history.encode(bl);
      ::encode(purged_snaps, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 v;
      ::decode(v, bl);

      if (v < 23) {
	old_pg_t opgid;
	::decode(opgid, bl);
	pgid = opgid;
      } else {
	::decode(pgid, bl);
      }
      ::decode(last_update, bl);
      ::decode(last_complete, bl);
      ::decode(log_tail, bl);
      ::decode(log_backlog, bl);
      ::decode(stats, bl);
      history.decode(bl);
      if (v >= 22)
	::decode(purged_snaps, bl);
      else {
	set<snapid_t> snap_trimq;
	::decode(snap_trimq, bl);
      }
    }
  };
  //WRITE_CLASS_ENCODER(Info::History)
  WRITE_CLASS_ENCODER(Info)

  
  /** 
   * Query - used to ask a peer for information about a pg.
   *
   * note: if version=0, type=LOG, then we just provide our full log.
   *   only if type=BACKLOG do we generate a backlog and provide that too.
   */
  struct Query {
    enum {
      INFO = 0,
      LOG = 1,
      BACKLOG = 3,
      MISSING = 4,
      FULLLOG = 5,
    };
    const char *get_type_name() const {
      switch (type) {
      case INFO: return "info";
      case LOG: return "log";
      case BACKLOG: return "backlog";
      case MISSING: return "missing";
      case FULLLOG: return "fulllog";
      default: return "???";
      }
    }

    __s32 type;
    eversion_t since;
    Info::History history;

    Query() : type(-1) {}
    Query(int t, const Info::History& h) :
      type(t), history(h) { assert(t != LOG); }
    Query(int t, eversion_t s, const Info::History& h) :
      type(t), since(s), history(h) { assert(t == LOG); }

    void encode(bufferlist &bl) const {
      ::encode(type, bl);
      ::encode(since, bl);
      history.encode(bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(type, bl);
      ::decode(since, bl);
      history.decode(bl);
    }
  };
  WRITE_CLASS_ENCODER(Query)

  
  /*
   * Log - incremental log of recent pg changes.
   *  also, serves as a recovery queue.
   *
   * when backlog is true, 
   *  objects with versions <= bottom are in log.
   *  we do not have any deletion info before that time, however.
   *  log is a "summary" in that it contains all objects in the PG.
   */
  struct Log {
    /** Entry
     */
    struct Entry {
      enum {
	LOST = 0,        // lost new version, now deleted
	MODIFY = 1,
	CLONE = 2,
	DELETE = 3,
	BACKLOG = 4,  // event invented by generate_backlog
	LOST_REVERT = 5, // lost new version, reverted to old
      };

      __s32      op;
      hobject_t  soid;
      eversion_t version, prior_version;
      osd_reqid_t reqid;  // caller+tid to uniquely identify request
      utime_t     mtime;  // this is the _user_ mtime, mind you
      bufferlist snaps;   // only for clone entries
      bool invalid_hash; // only when decoding sobject_t based entries

      uint64_t offset;   // [soft state] my offset on disk
      
      Entry() : op(0), invalid_hash(false) {}
      Entry(int _op, const hobject_t& _soid,
	    const eversion_t& v, const eversion_t& pv,
	    const osd_reqid_t& rid, const utime_t& mt) :
        op(_op), soid(_soid), version(v),
	prior_version(pv), 
	reqid(rid), mtime(mt), invalid_hash(false) {}
      
      bool is_delete() const { return op == DELETE; }
      bool is_clone() const { return op == CLONE; }
      bool is_modify() const { return op == MODIFY; }
      bool is_backlog() const { return op == BACKLOG; }
      bool is_lost() const { return op == LOST; }
      bool is_lost_revert() const { return op == LOST_REVERT; }
      bool is_update() const { return is_clone() || is_modify() || is_backlog() || is_lost_revert(); }

      bool reqid_is_indexed() const {
	return reqid != osd_reqid_t() && op != BACKLOG && op != CLONE && op != LOST && op != LOST_REVERT;
      }

      void encode(bufferlist &bl) const {
	__u8 struct_v = 3;
	::encode(struct_v, bl);
	::encode(op, bl);
	::encode(soid, bl);
	::encode(version, bl);
	::encode(prior_version, bl);
	::encode(reqid, bl);
	::encode(mtime, bl);
	if (op == CLONE)
	  ::encode(snaps, bl);
      }
      void decode(bufferlist::iterator &bl) {
	__u8 struct_v;
	::decode(struct_v, bl);
	::decode(op, bl);
	if (struct_v < 2) {
	  sobject_t old_soid;
	  ::decode(old_soid, bl);
	  soid.oid = old_soid.oid;
	  soid.snap = old_soid.snap;
	  invalid_hash = true;
	} else {
	  ::decode(soid, bl);
	}
	if (struct_v < 3)
	  invalid_hash = true;
	::decode(version, bl);
	::decode(prior_version, bl);
	::decode(reqid, bl);
	::decode(mtime, bl);
	if (op == CLONE)
	  ::decode(snaps, bl);
      }
    };
    WRITE_CLASS_ENCODER(Entry)

    /*
     *   head - newest entry (update|delete)
     *   tail - entry previous to oldest (update|delete) for which we have
     *          complete negative information.  
     * i.e. we can infer pg contents for any store whose last_update >= tail.
     */
    eversion_t head;    // newest entry
    eversion_t tail;    // version prior to oldest

    /*
     * backlog - true if log is a complete summary of pg contents.
     * updated will include all items in pg, but deleted will not
     * include negative entries for items deleted prior to 'tail'.
     */
    bool backlog;
    
    list<Entry> log;  // the actual log.

    Log() : backlog(false) {}

    void clear() {
      eversion_t z;
      head = tail = z;
      backlog = false;
      log.clear();
    }
    bool empty() const {
      return head.version == 0 && head.epoch == 0;
    }

    list<Entry>::iterator find_entry(eversion_t v) {
      int fromhead = head.version - v.version;
      int fromtail = v.version - tail.version;
      list<Entry>::iterator p;
      if (fromhead < fromtail) {
	p = log.end();
	p--;
	while (p->version > v)
	  p--;
	return p;
      } else {
	p = log.begin();
	while (p->version < v)
	  p++;
	return p;
      }      
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(head, bl);
      ::encode(tail, bl);
      ::encode(backlog, bl);
      ::encode(log, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 struct_v = 1;
      ::decode(struct_v, bl);
      ::decode(head, bl);
      ::decode(tail, bl);
      ::decode(backlog, bl);
      ::decode(log, bl);
    }

    void copy_after(const Log &other, eversion_t v);
    bool copy_after_unless_divergent(const Log &other, eversion_t split, eversion_t floor);
    void copy_non_backlog(const Log &other);
    ostream& print(ostream& out) const;
  };
  WRITE_CLASS_ENCODER(Log)

  /**
   * IndexLog - adds in-memory index of the log, by oid.
   * plus some methods to manipulate it all.
   */
  struct IndexedLog : public Log {
    hash_map<hobject_t,Entry*> objects;  // ptrs into log.  be careful!
    hash_map<osd_reqid_t,Entry*> caller_ops;

    // recovery pointers
    list<Entry>::iterator complete_to;  // not inclusive of referenced item
    version_t last_requested;           // last object requested by primary

    /****/
    IndexedLog() {}

    void zero() {
      unindex();
      Log::clear();
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
      hash_map<osd_reqid_t,Entry*>::const_iterator p = caller_ops.find(r);
      if (p == caller_ops.end())
	return eversion_t();
      return p->second->version;    
    }

    void index() {
      objects.clear();
      caller_ops.clear();
      for (list<Entry>::iterator i = log.begin();
           i != log.end();
           i++) {
        objects[i->soid] = &(*i);
	if (i->reqid_is_indexed()) {
	  //assert(caller_ops.count(i->reqid) == 0);  // divergent merge_log indexes new before unindexing old
	  caller_ops[i->reqid] = &(*i);
	}
      }
    }

    void index(Entry& e) {
      if (objects.count(e.soid) == 0 || 
          objects[e.soid]->version < e.version)
        objects[e.soid] = &e;
      if (e.reqid_is_indexed()) {
	//assert(caller_ops.count(i->reqid) == 0);  // divergent merge_log indexes new before unindexing old
	caller_ops[e.reqid] = &e;
      }
    }
    void unindex() {
      objects.clear();
      caller_ops.clear();
    }
    void unindex(Entry& e) {
      // NOTE: this only works if we remove from the _tail_ of the log!
      if (objects.count(e.soid) && objects[e.soid]->version == e.version)
        objects.erase(e.soid);
      if (e.reqid_is_indexed() &&
	  caller_ops.count(e.reqid) &&  // divergent merge_log indexes new before unindexing old
	  caller_ops[e.reqid] == &e)
	caller_ops.erase(e.reqid);
    }


    // accessors
    Entry *is_updated(const hobject_t& oid) {
      if (objects.count(oid) && objects[oid]->is_update()) return objects[oid];
      return 0;
    }
    Entry *is_deleted(const hobject_t& oid) {
      if (objects.count(oid) && objects[oid]->is_delete()) return objects[oid];
      return 0;
    }
    
    // actors
    void add(Entry& e) {
      // add to log
      log.push_back(e);
      assert(e.version > head);
      assert(head.version == 0 || e.version.version > head.version);
      head = e.version;

      // to our index
      objects[e.soid] = &(log.back());
      caller_ops[e.reqid] = &(log.back());
    }

    void trim(ObjectStore::Transaction &t, eversion_t s);
    void trim_write_ahead(eversion_t last_update);


    ostream& print(ostream& out) const;
  };
  

  /**
   * OndiskLog - some info about how we store the log on disk.
   */
  class OndiskLog {
  public:
    // ok
    uint64_t tail;                     // first byte of log. 
    uint64_t head;                        // byte following end of log.
    bool has_checksums;

    OndiskLog() : tail(0), head(0) {}

    uint64_t length() { return head - tail; }
    bool trim_to(eversion_t v, ObjectStore::Transaction& t);

    void zero() {
      tail = 0;
      head = 0;
    }

    void encode(bufferlist& bl) const {
      __u8 struct_v = 2;
      ::encode(struct_v, bl);
      ::encode(tail, bl);
      ::encode(head, bl);
    }
    void decode(bufferlist::iterator& bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      has_checksums = (struct_v >= 2);
      ::decode(tail, bl);
      ::decode(head, bl);
    }
  };
  WRITE_CLASS_ENCODER(OndiskLog)


  /*
   * Missing - summary of missing objects.
   *  kept in memory, as a supplement to Log.
   *  also used to pass missing info in messages.
   */
  struct Missing {
    struct item {
      eversion_t need, have;
      item() {}
      item(eversion_t n) : need(n) {}  // have no old version
      item(eversion_t n, eversion_t h) : need(n), have(h) {}
      void encode(bufferlist& bl) const {
	::encode(need, bl);
	::encode(have, bl);
      }
      void decode(bufferlist::iterator& bl) {
	::decode(need, bl);
	::decode(have, bl);
      }
    }; 
    WRITE_CLASS_ENCODER(item)

    map<hobject_t, item> missing;         // oid -> (need v, have v)
    map<version_t, hobject_t> rmissing;  // v -> oid

    unsigned int num_missing() const;
    bool have_missing() const;
    void swap(Missing& o);
    bool is_missing(const hobject_t& oid) const;
    bool is_missing(const hobject_t& oid, eversion_t v) const;
    eversion_t have_old(const hobject_t& oid) const;
    void add_next_event(Log::Entry& e, const Info &info);
    void revise_need(hobject_t oid, eversion_t need);
    void add(const hobject_t& oid, eversion_t need, eversion_t have);
    void rm(const hobject_t& oid, eversion_t v);
    void got(const hobject_t& oid, eversion_t v);
    void got(const std::map<hobject_t, Missing::item>::iterator &m);

    void encode(bufferlist &bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(missing, bl);
    }

    void decode(bufferlist::iterator &bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(missing, bl);

      for (map<hobject_t,item>::iterator it = missing.begin();
	   it != missing.end();
	   ++it)
	rmissing[it->second.need.version] = it->first;
    }
  };
  WRITE_CLASS_ENCODER(Missing)


  /*** PG ****/
protected:
  OSD *osd;
  PGPool *pool;

  /** locking and reference counting.
   * I destroy myself when the reference count hits zero.
   * lock() should be called before doing anything.
   * get() should be called on pointer copy (to another thread, etc.).
   * put() should be called on destruction of some previously copied pointer.
   * put_unlock() when done with the current pointer (_most common_).
   */  
  Mutex _lock;
  Cond _cond;
  atomic_t ref;

public:
  bool deleting;  // true while RemoveWQ should be chewing on us

  void lock(bool no_lockdep=false) {
    //generic_dout(0) << this << " " << info.pgid << " lock" << dendl;
    _lock.Lock(no_lockdep);
  }
  void unlock() {
    //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
    _lock.Unlock();
  }
  void assert_locked() {
    assert(_lock.is_locked());
  }
  bool is_locked() {
    return _lock.is_locked();
  }
  void wait() {
    assert(_lock.is_locked());
    _cond.Wait(_lock);
  }
  void kick() {
    assert(_lock.is_locked());
    _cond.Signal();
  }

  void get() {
    //generic_dout(0) << this << " " << info.pgid << " get " << ref.test() << dendl;
    //assert(_lock.is_locked());
    ref.inc();
  }
  void put() { 
    //generic_dout(0) << this << " " << info.pgid << " put " << ref.test() << dendl;
    if (ref.dec() == 0)
      delete this;
  }


  list<Message*> op_queue;  // op queue

  bool dirty_info, dirty_log;

public:
  struct Interval {
    vector<int> up, acting;
    epoch_t first, last;
    bool maybe_went_rw;

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(first, bl);
      ::encode(last, bl);
      ::encode(up, bl);
      ::encode(acting, bl);
      ::encode(maybe_went_rw, bl);
    }
    void decode(bufferlist::iterator& bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(first, bl);
      ::decode(last, bl);
      ::decode(up, bl);
      ::decode(acting, bl);
      ::decode(maybe_went_rw, bl);
    }
  };
  WRITE_CLASS_ENCODER(Interval)

  // pg state
  Info        info;
  const coll_t coll;
  IndexedLog  log;
  hobject_t    log_oid;
  hobject_t    biginfo_oid;
  OndiskLog   ondisklog;
  Missing     missing;
  map<hobject_t, set<int> > missing_loc;
  
  interval_set<snapid_t> snap_collections;
  map<epoch_t,Interval> past_intervals;

  interval_set<snapid_t> snap_trimq;

  /* You should not use these items without taking their respective queue locks
   * (if they have one) */
  xlist<PG*>::item recovery_item, backlog_item, scrub_item, scrub_finalize_item, snap_trim_item, remove_item, stat_queue_item;
  int recovery_ops_active;
#ifdef DEBUG_RECOVERY_OIDS
  set<hobject_t> recovering_oids;
#endif

  epoch_t generate_backlog_epoch;  // epoch we decided to build a backlog.
  utime_t replay_until;

protected:
  int         role;    // 0 = primary, 1 = replica, -1=none.
  int         state;   // see bit defns above

public:
  eversion_t  last_update_ondisk;    // last_update that has committed; ONLY DEFINED WHEN is_active()
  eversion_t  last_complete_ondisk;  // last_complete that has committed.
  eversion_t  last_update_applied;

  // primary state
 public:
  vector<int> up, acting;
  map<int,eversion_t> peer_last_complete_ondisk;
  eversion_t  min_last_complete_ondisk;  // up: min over last_complete_ondisk, peer_last_complete_ondisk
  eversion_t  pg_trim_to;

  // [primary only] content recovery state
  bool        have_master_log;
 protected:
  bool prior_set_built;

  struct PgPriorSet {
    set<int>    cur;   // current+prior OSDs, as defined by info.history.last_epoch_started.
    set<int>    down;  // down osds normally exluded from cur
    set<int>    lost;  // osds in the prior set which are lost
    map<int,epoch_t> up_thru;  // osds whose up_thru we care about
    vector<Interval> inter_up_thru;  // intervals whose up_thru we care about
    bool crashed;   /// true if past osd failures were such that clients may need to replay requests.
    bool pg_down;
    bool some_down;
    const PG *pg;
    PgPriorSet(int whoami,
	       const OSDMap &osdmap,
	       const map<epoch_t, Interval> &past_intervals,
	       const vector<int> &up,
	       const vector<int> &acting,
	       const Info &info,
	       const PG *pg);
  };

  friend std::ostream& operator<<(std::ostream& oss,
				  const struct PgPriorSet &prior);

public:    
  struct RecoveryCtx {
    utime_t start_time;
    map< int, map<pg_t, Query> > *query_map;
    map< int, MOSDPGInfo* > *info_map;
    map< int, vector<Info> > *notify_list;
    list< Context* > *context_list;
    ObjectStore::Transaction *transaction;
    RecoveryCtx() : query_map(0), info_map(0), notify_list(0),
		    context_list(0), transaction(0) {}
    RecoveryCtx(map< int, map<pg_t, Query> > *query_map,
		map< int, MOSDPGInfo* > *info_map,
		map< int, vector<Info> > *notify_list,
		list< Context* > *context_list,
		ObjectStore::Transaction *transaction)
      : query_map(query_map), info_map(info_map), 
	notify_list(notify_list),
	context_list(context_list), transaction(transaction) {}
  };

  struct NamedState {
    const char *state_name;
    utime_t enter_time;
    const char *get_state_name() { return state_name; }
    NamedState() : enter_time(ceph_clock_now(g_ceph_context)) {}
    virtual ~NamedState() {}
  };


  /* Encapsulates PG recovery process */
  class RecoveryState {
    void start_handle(RecoveryCtx *new_ctx) {
      assert(!rctx);
      rctx = new_ctx;
      if (rctx)
	rctx->start_time = ceph_clock_now(g_ceph_context);
    }

    void end_handle() {
      if (rctx) {
	utime_t dur = ceph_clock_now(g_ceph_context) - rctx->start_time;
	machine.event_time += dur;
      }
      machine.event_count++;
      rctx = 0;
    }

    struct MInfoRec : boost::statechart::event< MInfoRec > {
      int from;
      Info &info;
      MInfoRec(int from, Info &info) :
	from(from), info(info) {}
    };

    struct MLogRec : boost::statechart::event< MLogRec > {
      int from;
      MOSDPGLog *msg;
      MLogRec(int from, MOSDPGLog *msg) :
	from(from), msg(msg) {}
    };

    struct MNotifyRec : boost::statechart::event< MNotifyRec > {
      int from;
      Info &info;
      MNotifyRec(int from, Info &info) :
	from(from), info(info) {}
    };

    struct MQuery : boost::statechart::event< MQuery > {
      int from;
      const Query &query;
      epoch_t query_epoch;
      MQuery(int from, const Query &query, epoch_t query_epoch):
	from(from), query(query), query_epoch(query_epoch) {}
    };

    struct AdvMap : boost::statechart::event< AdvMap > {
      OSDMap *osdmap;
      OSDMap *lastmap;
      vector<int> newup, newacting;
      AdvMap(OSDMap *osdmap, OSDMap *lastmap, vector<int>& newup, vector<int>& newacting):
	osdmap(osdmap), lastmap(lastmap), newup(newup), newacting(newacting) {}
    };

    struct BacklogComplete : boost::statechart::event< BacklogComplete > {
      BacklogComplete() : boost::statechart::event< BacklogComplete >() {}
    };
    struct ActMap : boost::statechart::event< ActMap > {
      ActMap() : boost::statechart::event< ActMap >() {}
    };
    struct Activate : boost::statechart::event< Activate > {
      Activate() : boost::statechart::event< Activate >() {}
    };
    struct Initialize : boost::statechart::event< Initialize > {
      Initialize() : boost::statechart::event< Initialize >() {}
    };
    struct Load : boost::statechart::event< Load > {
      Load() : boost::statechart::event< Load >() {}
    };
    struct GotInfo : boost::statechart::event< GotInfo > {
      GotInfo() : boost::statechart::event< GotInfo >() {}
    };
    struct NeedUpThru : boost::statechart::event< NeedUpThru > {
      NeedUpThru() : boost::statechart::event< NeedUpThru >() {};
    };


    /* States */
    struct Initial;
    class RecoveryMachine : public boost::statechart::state_machine< RecoveryMachine, Initial > {
      RecoveryState *state;
    public:
      PG *pg;

      utime_t event_time;
      uint64_t event_count;
      
      void clear_event_counters() {
	event_time = utime_t();
	event_count = 0;
      }

      void log_enter(const char *state_name);
      void log_exit(const char *state_name, utime_t duration);

      RecoveryMachine(RecoveryState *state, PG *pg) : state(state), pg(pg), event_count(0) {}

      /* Accessor functions for state methods */
      ObjectStore::Transaction* get_cur_transaction() {
	assert(state->rctx->transaction);
	return state->rctx->transaction;
      }

      void send_query(int to, const Query &query) {
	assert(state->rctx->query_map);
	(*state->rctx->query_map)[to][pg->info.pgid] = query;
      }

      map<int, map<pg_t, Query> > *get_query_map() {
	assert(state->rctx->query_map);
	return state->rctx->query_map;
      }

      map<int, MOSDPGInfo*> *get_info_map() {
	assert(state->rctx->info_map);
	return state->rctx->info_map;
      }

      list< Context* > *get_context_list() {
	assert(state->rctx->context_list);
	return state->rctx->context_list;
      }

      void send_notify(int to, const Info &info) {
	assert(state->rctx->notify_list);
	(*state->rctx->notify_list)[to].push_back(info);
      }
    };
    friend class RecoveryMachine;

    /* States */
    struct NamedState {
      const char *state_name;
      utime_t enter_time;
      const char *get_state_name() { return state_name; }
      NamedState() : enter_time(ceph_clock_now(g_ceph_context)) {}
      virtual ~NamedState() {}
    };

    struct Crashed : boost::statechart::state< Crashed, RecoveryMachine >, NamedState {
      Crashed(my_context ctx);
    };

    struct Started;
    struct Reset;

    struct Initial : boost::statechart::state< Initial, RecoveryMachine >, NamedState {
      Initial(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< Initialize, Started >,
	boost::statechart::transition< Load, Reset >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;

      boost::statechart::result react(const MNotifyRec&);
      boost::statechart::result react(const MInfoRec&);
      boost::statechart::result react(const MLogRec&);
    };

    struct Reset : boost::statechart::state< Reset, RecoveryMachine >, NamedState {
      Reset(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const ActMap&);
    };

    struct Start;

    struct Started : boost::statechart::state< Started, RecoveryMachine, Start >, NamedState {
      Started(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::transition< boost::statechart::event_base, Crashed >
	> reactions;
      boost::statechart::result react(const AdvMap&);
    };

    struct MakePrimary : boost::statechart::event< MakePrimary > {
      MakePrimary() : boost::statechart::event< MakePrimary >() {}
    };
    struct MakeStray : boost::statechart::event< MakeStray > {
      MakeStray() : boost::statechart::event< MakeStray >() {}
    };
    struct Primary;
    struct Stray;

    struct Start : boost::statechart::state< Start, Started >, NamedState {
      Start(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< MakePrimary, Primary >,
	boost::statechart::transition< MakeStray, Stray >
	> reactions;
    };

    struct Peering;
    struct WaitActingChange;
    struct NeedNewMap : boost::statechart::event< NeedNewMap > {
      NeedNewMap() : boost::statechart::event< NeedNewMap >() {}
    };

    struct Primary : boost::statechart::state< Primary, Started, Peering >, NamedState {
      Primary(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< BacklogComplete >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::transition< NeedNewMap, WaitActingChange >
	> reactions;
      boost::statechart::result react(const BacklogComplete&);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MNotifyRec&);
    };

    struct WaitActingChange : boost::statechart::state< WaitActingChange, Primary>,
			      NamedState {
      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MLogRec >
	> reactions;
      WaitActingChange(my_context ctx);
      boost::statechart::result react(const MLogRec&);
      void exit();
    };
    
    struct GetInfo;
    struct Active;

    struct Peering : boost::statechart::state< Peering, Primary, GetInfo >, NamedState {
      std::auto_ptr< PgPriorSet > prior_set;

      Peering(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::transition< Activate, Active >,
	boost::statechart::custom_reaction< AdvMap >
	> reactions;
      boost::statechart::result react(const AdvMap &advmap);
    };

    struct Active : boost::statechart::state< Active, Primary >, NamedState {
      Active(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< AdvMap >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< MNotifyRec >,
	boost::statechart::custom_reaction< MLogRec >
	> reactions;
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const AdvMap&);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const MNotifyRec& notevt);
      boost::statechart::result react(const MLogRec& logevt);
    };

    struct ReplicaActive : boost::statechart::state< ReplicaActive, Started >, NamedState {
      ReplicaActive(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MInfoRec >
	> reactions;
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const ActMap&);
      boost::statechart::result react(const MQuery&);
    };

    struct Stray : boost::statechart::state< Stray, Started >, NamedState {
      bool backlog_requested;
      map<int, pair<Query, epoch_t> > pending_queries;

      Stray(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MQuery >,
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< MInfoRec >,
	boost::statechart::custom_reaction< BacklogComplete >,
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::transition< Activate, ReplicaActive >
	> reactions;
      boost::statechart::result react(const MQuery& query);
      boost::statechart::result react(const BacklogComplete&);
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const MInfoRec& infoevt);
      boost::statechart::result react(const ActMap&);
    };

    struct GetLog;

    struct GetInfo : boost::statechart::state< GetInfo, Peering >, NamedState {
      set<int> peer_info_requested;

      GetInfo(my_context ctx);
      void exit();
      void get_infos();

      typedef boost::mpl::list <
	boost::statechart::transition< GotInfo, GetLog >,
	boost::statechart::custom_reaction< MNotifyRec >
	> reactions;
      boost::statechart::result react(const MNotifyRec& infoevt);
    };

    struct GetMissing;
    struct GotLog : boost::statechart::event< GotLog > {
      GotLog() : boost::statechart::event< GotLog >() {}
    };

    struct GetLog : boost::statechart::state< GetLog, Peering >, NamedState {
      int newest_update_osd;
      bool need_backlog;
      bool wait_on_backlog;
      MOSDPGLog *msg;

      GetLog(my_context ctx);
      ~GetLog();
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::custom_reaction< BacklogComplete >,
	boost::statechart::custom_reaction< GotLog >
	> reactions;
      boost::statechart::result react(const MLogRec& logevt);
      boost::statechart::result react(const BacklogComplete&);
      boost::statechart::result react(const GotLog&);
    };

    struct WaitUpThru;

    struct GetMissing : boost::statechart::state< GetMissing, Peering >, NamedState {
      set<int> peer_missing_requested;

      GetMissing(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< MLogRec >,
	boost::statechart::transition< NeedUpThru, WaitUpThru >
	> reactions;
      boost::statechart::result react(const MLogRec& logevt);
    };

    struct WaitUpThru : boost::statechart::state< WaitUpThru, Peering >, NamedState {
      WaitUpThru(my_context ctx);
      void exit();

      typedef boost::mpl::list <
	boost::statechart::custom_reaction< ActMap >,
	boost::statechart::custom_reaction< MLogRec >
	> reactions;
      boost::statechart::result react(const ActMap& am);
      boost::statechart::result react(const MLogRec& logrec);
    };


    RecoveryMachine machine;
    PG *pg;
    RecoveryCtx *rctx;

  public:
    RecoveryState(PG *pg) : machine(this, pg), pg(pg), rctx(0) {
      machine.initiate();
    }

    void handle_notify(int from, Info& i, RecoveryCtx *ctx);
    void handle_info(int from, Info& i, RecoveryCtx *ctx);
    void handle_log(int from,
		    MOSDPGLog *msg,
		    RecoveryCtx *ctx);
    void handle_query(int from, const PG::Query& q,
		      epoch_t query_epoch,
		      RecoveryCtx *ctx);
    void handle_advance_map(OSDMap *osdmap, OSDMap *lastmap, 
			    vector<int>& newup, vector<int>& newacting, 
			    RecoveryCtx *ctx);
    void handle_activate_map(RecoveryCtx *ctx);
    void handle_backlog_generated(RecoveryCtx *ctx);
    void handle_create(RecoveryCtx *ctx);
    void handle_loaded(RecoveryCtx *ctx);
  } recovery_state;

protected:

  bool        need_up_thru;
  set<int>    stray_set;   // non-acting osds that have PG data.
  eversion_t  oldest_update; // acting: lowest (valid) last_update in active set
  map<int,Info>        peer_info;   // info from peers (stray or prior)
  map<int, Missing>    peer_missing;
  set<int>             peer_log_requested;  // logs i've requested (and start stamps)
  set<int>             peer_backlog_requested;
  set<int>             peer_missing_requested;
  set<int>             stray_purged;  // i deleted these strays; ignore racing PGInfo from them
  set<int>             peer_activated;

  // primary-only, recovery-only state
  set<int>             might_have_unfound;  // These osds might have objects on them
					    // which are unfound on the primary

  epoch_t last_peering_reset;

  friend class OSD;


  // pg waiters
  list<class Message*>            waiting_for_active;
  map<hobject_t, list<class Message*> > waiting_for_missing_object,
                                        waiting_for_degraded_object;
  map<eversion_t,list<Message*> > waiting_for_ondisk;
  map<eversion_t,class MOSDOp*>   replay_queue;

  void take_object_waiters(map<hobject_t, list<Message*> >& m);
  
  bool block_if_wrlocked(MOSDOp* op, object_info_t& oi);


  // stats
  Mutex pg_stats_lock;
  bool pg_stats_valid;
  pg_stat_t pg_stats_stable;

  // for ordering writes
  ObjectStore::Sequencer osr;

  void update_stats();
  void clear_stats();

public:
  void clear_primary_state();

 public:
  bool is_acting(int osd) const { 
    for (unsigned i=0; i<acting.size(); i++)
      if (acting[i] == osd) return true;
    return false;
  }
  bool is_up(int osd) const { 
    for (unsigned i=0; i<up.size(); i++)
      if (up[i] == osd) return true;
    return false;
  }
  
  bool is_all_uptodate() const;

  void generate_past_intervals();
  void trim_past_intervals();
  void build_prior(std::auto_ptr<PgPriorSet> &prior_set);
  void clear_prior();
  bool prior_set_affected(PgPriorSet &prior, const OSDMap *osdmap) const;

  bool adjust_need_up_thru(const OSDMap *osdmap);

  bool all_unfound_are_lost(const OSDMap* osdmap) const;
  void mark_obj_as_lost(ObjectStore::Transaction& t,
			const hobject_t &lost_soid);
  void mark_all_unfound_as_lost(ObjectStore::Transaction& t);

  bool calc_min_last_complete_ondisk() {
    eversion_t min = last_complete_ondisk;
    for (unsigned i=1; i<acting.size(); i++) {
      if (peer_last_complete_ondisk.count(acting[i]) == 0)
	return false;   // we don't have complete info
      eversion_t a = peer_last_complete_ondisk[acting[i]];
      if (a < min)
	min = a;
    }
    if (min == min_last_complete_ondisk)
      return false;
    min_last_complete_ondisk = min;
    return true;
  }

  virtual void calc_trim_to() = 0;

  void proc_replica_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog,
			Missing& omissing, int from);
  void proc_master_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog,
		       Missing& omissing, int from);
  bool proc_replica_info(int from, Info &info);
  bool merge_old_entry(ObjectStore::Transaction& t, Log::Entry& oe);
  void merge_log(ObjectStore::Transaction& t, Info &oinfo, Log &olog, int from);
  bool search_for_missing(const Info &oinfo, const Missing *omissing,
			  int fromosd);

  void check_for_lost_objects();
  void forget_lost_objects();

  void discover_all_missing(std::map< int, map<pg_t,PG::Query> > &query_map);
  
  bool build_backlog_map(map<eversion_t,Log::Entry>& omap);
  void assemble_backlog(map<eversion_t,Log::Entry>& omap);
  void drop_backlog();
  
  void trim_write_ahead();

  bool choose_acting(int newest_update_osd) const;
  bool recover_master_log(map< int, map<pg_t,Query> >& query_map,
			  eversion_t &oldest_update);
  eversion_t calc_oldest_known_update() const;
  void do_peer(ObjectStore::Transaction& t, list<Context*>& tfin,
	       map< int, map<pg_t,Query> >& query_map,
	       map<int, MOSDPGInfo*> *activator_map=0);
  bool choose_log_location(const PgPriorSet &prior_set,
			   bool &need_backlog,
			   bool &wait_on_backlog,
			   int &pull_from,
			   eversion_t &newest_update,
			   eversion_t &oldest_update) const;
  void build_might_have_unfound();
  void replay_queued_ops();
  void activate(ObjectStore::Transaction& t, list<Context*>& tfin,
		map< int, map<pg_t,Query> >& query_map,
		map<int, MOSDPGInfo*> *activator_map=0);
  void _activate_committed(epoch_t e);
  void all_activated_and_committed();

  void proc_primary_info(ObjectStore::Transaction &t, const Info &info);

  bool have_unfound() const { 
    return missing.num_missing() > missing_loc.size();
  }
  int get_num_unfound() const {
    return missing.num_missing() - missing_loc.size();
  }

  virtual void clean_up_local(ObjectStore::Transaction& t) = 0;

  virtual int start_recovery_ops(int max) = 0;

  void purge_strays();

  Context *finish_sync_event;

  void finish_recovery(ObjectStore::Transaction& t, list<Context*>& tfin);
  void _finish_recovery(Context *c);
  void cancel_recovery();
  void clear_recovery_state();
  virtual void _clear_recovery_state() = 0;
  void defer_recovery();
  virtual void check_recovery_op_pulls(const OSDMap *newmap) = 0;
  void start_recovery_op(const hobject_t& soid);
  void finish_recovery_op(const hobject_t& soid, bool dequeue=false);

  loff_t get_log_write_pos() {
    return 0;
  }

  friend class C_OSD_RepModify_Commit;


  // -- scrub --
  set<int> scrub_reserved_peers;
  map<int,ScrubMap> scrub_received_maps;
  bool finalizing_scrub; 
  bool scrub_reserved, scrub_reserve_failed;
  int scrub_waiting_on;
  epoch_t scrub_epoch_start;
  ScrubMap primary_scrubmap;
  MOSDRepScrub *active_rep_scrub;

  void repair_object(const hobject_t& soid, ScrubMap::object *po, int bad_peer, int ok_peer);
  bool _compare_scrub_objects(ScrubMap::object &auth,
			      ScrubMap::object &candidate,
			      ostream &errorstream);
  void _compare_scrubmaps(const map<int,ScrubMap*> &maps,  
			  map<hobject_t, set<int> > &missing,
			  map<hobject_t, set<int> > &inconsistent,
			  map<hobject_t, int> &authoritative,
			  ostream &errorstream);
  void scrub();
  void scrub_finalize();
  void scrub_clear_state();
  bool scrub_gather_replica_maps();
  void _scan_list(ScrubMap &map, vector<hobject_t> &ls);
  void _request_scrub_map(int replica, eversion_t version);
  void build_scrub_map(ScrubMap &map);
  void build_inc_scrub_map(ScrubMap &map, eversion_t v);
  virtual int _scrub(ScrubMap &map, int& errors, int& fixed) { return 0; }
  void clear_scrub_reserved();
  void scrub_reserve_replicas();
  void scrub_unreserve_replicas();
  bool scrub_all_replicas_reserved() const;
  bool sched_scrub();

  void replica_scrub(class MOSDRepScrub *op);
  void sub_op_scrub_map(class MOSDSubOp *op);
  void sub_op_scrub_reserve(class MOSDSubOp *op);
  void sub_op_scrub_reserve_reply(class MOSDSubOpReply *op);
  void sub_op_scrub_unreserve(class MOSDSubOp *op);
  void sub_op_scrub_stop(class MOSDSubOp *op);

 public:  
  PG(OSD *o, PGPool *_pool, pg_t p, const hobject_t& loid, const hobject_t& ioid) : 
    osd(o), pool(_pool),
    _lock("PG::_lock"),
    ref(0), deleting(false), dirty_info(false), dirty_log(false),
    info(p), coll(p), log_oid(loid), biginfo_oid(ioid),
    recovery_item(this), backlog_item(this), scrub_item(this), scrub_finalize_item(this), snap_trim_item(this), remove_item(this), stat_queue_item(this),
    recovery_ops_active(0),
    generate_backlog_epoch(0),
    role(0),
    state(0),
    have_master_log(true),
    recovery_state(this),
    need_up_thru(false),
    last_peering_reset(0),
    pg_stats_lock("PG::pg_stats_lock"),
    pg_stats_valid(false),
    finish_sync_event(NULL),
    finalizing_scrub(false),
    scrub_reserved(false), scrub_reserve_failed(false),
    scrub_waiting_on(0),
    active_rep_scrub(0)
  {
    pool->get();
  }
  virtual ~PG() {
    pool->put();
  }
  
 private:
  // Prevent copying
  PG(const PG& rhs);
  PG& operator=(const PG& rhs);

 public:
  pg_t       get_pgid() const { return info.pgid; }
  int        get_nrep() const { return acting.size(); }

  int        get_primary() { return acting.empty() ? -1:acting[0]; }
  
  int        get_role() const { return role; }
  void       set_role(int r) { role = r; }

  bool       is_primary() const { return role == PG_ROLE_HEAD; }
  bool       is_replica() const { return role > 0; }
  
  //int  get_state() const { return state; }
  bool state_test(int m) const { return (state & m) != 0; }
  void state_set(int m) { state |= m; }
  void state_clear(int m) { state &= ~m; }

  bool is_complete() const { return info.last_complete == info.last_update; }

  int get_state() const { return state; }
  bool       is_active() const { return state_test(PG_STATE_ACTIVE); }
  bool       is_peering() const { return state_test(PG_STATE_PEERING); }
  bool       is_crashed() const { return state_test(PG_STATE_CRASHED); }
  bool       is_down() const { return state_test(PG_STATE_DOWN); }
  bool       is_replay() const { return state_test(PG_STATE_REPLAY); }
  bool       is_clean() const { return state_test(PG_STATE_CLEAN); }
  bool       is_degraded() const { return state_test(PG_STATE_DEGRADED); }
  bool       is_stray() const { return state_test(PG_STATE_STRAY); }

  bool       is_scrubbing() const { return state_test(PG_STATE_SCRUBBING); }

  bool  is_empty() const { return info.last_update == eversion_t(0,0); }

  // pg on-disk state
  void write_info(ObjectStore::Transaction& t);
  void write_log(ObjectStore::Transaction& t);

  void add_log_entry(Log::Entry& e, bufferlist& log_bl);
  void append_log(vector<Log::Entry>& logv, eversion_t trim_to, ObjectStore::Transaction &t);

  void read_log(ObjectStore *store);
  bool check_log_for_corruption(ObjectStore *store);
  void trim(ObjectStore::Transaction& t, eversion_t v);
  void trim_ondisklog(ObjectStore::Transaction& t);
  void trim_peers();

  std::string get_corrupt_pg_log_name() const;
  void read_state(ObjectStore *store);
  coll_t make_snap_collection(ObjectStore::Transaction& t, snapid_t sn);
  void update_snap_collections(vector<Log::Entry> &log_entries);
  void adjust_local_snaps();

  void log_weirdness();

  void queue_snap_trim();
  bool queue_scrub();

  void share_pg_info();
  void share_pg_log(const eversion_t &oldver);

  void start_peering_interval(const OSDMap *lastmap,
			      const vector<int>& newup,
			      const vector<int>& newacting);
  void set_last_peering_reset();

  void fulfill_info(int from, const Query &query, 
		    pair<int, Info> &notify_info);
  void fulfill_log(int from, const Query &query, epoch_t query_epoch);
  bool acting_up_affected(const vector<int>& newup, const vector<int>& newacting);
  bool old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch);

  // recovery bits
  void handle_notify(int from, PG::Info& i, RecoveryCtx *rctx) {
    recovery_state.handle_notify(from, i, rctx);
  }
  void handle_info(int from, PG::Info& i, RecoveryCtx *rctx) {
    recovery_state.handle_info(from, i, rctx);
  }
  void handle_log(int from,
		  MOSDPGLog *msg,
		  RecoveryCtx *rctx) {
    recovery_state.handle_log(from, msg, rctx);
  }
  void handle_query(int from, const PG::Query& q,
		    epoch_t query_epoch,
		    RecoveryCtx *rctx) {
    recovery_state.handle_query(from, q, query_epoch, rctx);
  }
  void handle_advance_map(OSDMap *osdmap, OSDMap *lastmap, 
			  vector<int>& newup, vector<int>& newacting,
			  RecoveryCtx *rctx) {
    recovery_state.handle_advance_map(osdmap, lastmap, newup, newacting, rctx);
  }
  void handle_activate_map(RecoveryCtx *rctx) {
    recovery_state.handle_activate_map(rctx);
  }
  void handle_backlog_generated(RecoveryCtx *rctx) {
    recovery_state.handle_backlog_generated(rctx);
  }
  void handle_create(RecoveryCtx *rctx) {
    recovery_state.handle_create(rctx);
  }
  void handle_loaded(RecoveryCtx *rctx) {
    recovery_state.handle_loaded(rctx);
  }

  void on_removal();
  // abstract bits
  virtual void do_op(MOSDOp *op) = 0;
  virtual void do_sub_op(MOSDSubOp *op) = 0;
  virtual void do_sub_op_reply(MOSDSubOpReply *op) = 0;
  virtual bool snap_trimmer() = 0;

  virtual bool same_for_read_since(epoch_t e) = 0;
  virtual bool same_for_modify_since(epoch_t e) = 0;
  virtual bool same_for_rep_modify_since(epoch_t e) = 0;

  virtual bool is_write_in_progress() = 0;
  virtual bool is_missing_object(const hobject_t& oid) = 0;
  virtual void wait_for_missing_object(const hobject_t& oid, Message *op) = 0;

  virtual bool is_degraded_object(const hobject_t& oid) = 0;
  virtual void wait_for_degraded_object(const hobject_t& oid, Message *op) = 0;

  virtual void on_osd_failure(int osd) = 0;
  virtual void on_role_change() = 0;
  virtual void on_change() = 0;
  virtual void on_activate() = 0;
  virtual void on_shutdown() = 0;
  virtual void remove_watchers_and_notifies() = 0;

  virtual void register_unconnected_watcher(void *obc,
					    entity_name_t entity,
					    utime_t expire) = 0;
  virtual void unregister_unconnected_watcher(void *obc,
					      entity_name_t entity) = 0;
  virtual void handle_watch_timeout(void *obc,
				    entity_name_t entity,
				    utime_t expire) = 0;
};

//WRITE_CLASS_ENCODER(PG::Info::History)
WRITE_CLASS_ENCODER(PG::Info)
WRITE_CLASS_ENCODER(PG::Query)
WRITE_CLASS_ENCODER(PG::Missing::item)
WRITE_CLASS_ENCODER(PG::Missing)
WRITE_CLASS_ENCODER(PG::Log::Entry)
WRITE_CLASS_ENCODER(PG::Log)
WRITE_CLASS_ENCODER(PG::Interval)
WRITE_CLASS_ENCODER(PG::OndiskLog)

inline ostream& operator<<(ostream& out, const PG::Info::History& h) 
{
  return out << "ec=" << h.epoch_created
	     << " les/c " << h.last_epoch_started << "/" << h.last_epoch_clean
	     << " " << h.same_up_since << "/" << h.same_interval_since << "/" << h.same_primary_since;
}

inline ostream& operator<<(ostream& out, const PG::Info& pgi) 
{
  out << pgi.pgid << "(";
  if (pgi.dne())
    out << " DNE";
  if (pgi.is_empty())
    out << " empty";
  else {
    out << " v " << pgi.last_update;
    if (pgi.last_complete != pgi.last_update)
      out << " lc " << pgi.last_complete;
    out << " (" << pgi.log_tail << "," << pgi.last_update << "]"
        << (pgi.log_backlog ? "+backlog":"");
  }
  //out << " c " << pgi.epoch_created;
  out << " n=" << pgi.stats.stats.sum.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Query& q) 
{
  out << "query(" << q.get_type_name() << " " << q.since;
  if (q.type == PG::Query::LOG)
    out << " " << q.history;
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Log::Entry& e)
{
  return out << e.version << " (" << e.prior_version << ")"
             << (e.is_delete() ? " - ":
		 (e.is_clone() ? " c ":
		  (e.is_modify() ? " m ":
		   (e.is_backlog() ? " b ":
		    (e.is_lost() ? " L ":
		     (e.is_lost_revert() ? " R " :
		      " ? "))))))
             << e.soid << " by " << e.reqid << " " << e.mtime;
}

inline ostream& operator<<(ostream& out, const PG::Log& log) 
{
  out << "log(" << log.tail << "," << log.head << "]";
  if (log.backlog) out << "+backlog";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Missing::item& i) 
{
  out << i.need;
  if (i.have != eversion_t())
    out << "(" << i.have << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Missing& missing) 
{
  out << "missing(" << missing.num_missing();
  //if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, const PG::Interval& i)
{
  out << "interval(" << i.first << "-" << i.last << " " << i.up << "/" << i.acting;
  if (i.maybe_went_rw)
    out << " maybe_went_rw";
  out << ")";
  return out;
}

ostream& operator<<(ostream& out, const PG& pg);

#endif
