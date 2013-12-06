// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef COHORT_OSDVOL_H
#define COHORT_OSDVOL_H

#include <tr1/memory>
#include <boost/optional.hpp>
#include "common/Mutex.h"
#include "include/uuid.h"
#include "osd/OpRequest.h"
#include "cohort/CohortOSDMap.h"
#include "os/ObjectStore.h"
#include "osd/Watch.h"
#include "VolLog.h"
#include "messages/MOSDOpReply.h"

using namespace std;

class CohortOSDService;
typedef std::tr1::shared_ptr<CohortOSDService> CohortOSDServiceRef;

class OSDVol;
class CohortOSD;

typedef std::tr1::shared_ptr<OSDVol> OSDVolRef;

struct ObjectContext {
  int ref;
  bool registered;
  ObjectState obs;

  SnapSetContext *ssc;  // may be null

private:
  Mutex lock;
public:
  Cond cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;

  // set if writes for this object are blocked on another objects recovery
  ObjectContext *blocked_by;      // object blocking our writes
  set<ObjectContext*> blocking;   // objects whose writes we block

  // any entity in obs.oi.watchers MUST be in either watchers or
  // unconnected_watchers.
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  ObjectContext(const object_info_t &oi_, bool exists_, SnapSetContext *ssc_)
    : ref(0), registered(false), obs(oi_, exists_), ssc(ssc_),
      lock("ObjectContext::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0),
      blocked_by(0) {}

  void get() { ++ref; }

  // do simple synchronous mutual exclusion, for now.  now waitqueues or anything fancy.
  void ondisk_write_lock() {
    lock.Lock();
    writers_waiting++;
    while (readers_waiting || readers)
      cond.Wait(lock);
    writers_waiting--;
    unstable_writes++;
    lock.Unlock();
  }
  void ondisk_write_unlock() {
    lock.Lock();
    assert(unstable_writes > 0);
    unstable_writes--;
    if (!unstable_writes && readers_waiting)
      cond.Signal();
    lock.Unlock();
  }
  void ondisk_read_lock() {
    lock.Lock();
    readers_waiting++;
    while (unstable_writes)
      cond.Wait(lock);
    readers_waiting--;
    readers++;
    lock.Unlock();
  }
  void ondisk_read_unlock() {
    lock.Lock();
    assert(readers > 0);
    readers--;
    if (!readers && writers_waiting)
      cond.Signal();
    lock.Unlock();
  }
};

class OSDVol {
  friend class CohortOSD;

  struct OpContext {
    OpRequestRef op;
    osd_reqid_t reqid;
    vector<OSDOp>& ops;

    const ObjectState *obs; // Old objectstate
    const SnapSet *snapset; // Old snapset

    ObjectState new_obs;  // resulting ObjectState
    SnapSet new_snapset;  // resulting SnapSet (in case of a write)
    //pg_stat_t new_stats;  // resulting Stats
    object_stat_sum_t delta_stats;

    bool modify; // (force) modification (even if op_t is empty)
    bool user_modify;     // user-visible modification

    // side effects
    list<watch_info_t> watch_connects;
    list<watch_info_t> watch_disconnects;
    list<notify_info_t> notifies;
    struct NotifyAck {
      boost::optional<uint64_t> watch_cookie;
      uint64_t notify_id;
      NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
      NotifyAck(uint64_t notify_id, uint64_t cookie)
	: watch_cookie(cookie), notify_id(notify_id) {}
    };
    list<NotifyAck> notify_acks;

    uint64_t bytes_written, bytes_read;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer
    eversion_t reply_version;    // the version that we report the client (depends on the op)

    ObjectStore::Transaction op_t, local_t;
    vector<vol_log_entry_t> log;

    interval_set<uint64_t> modified_ranges;
    ObjectContext *obc;          // For ref counting purposes
    map<hobject_t,ObjectContext*> src_obc;
    ObjectContext *clone_obc;    // if we created a clone
    ObjectContext *snapset_obc;  // if we created/deleted a snapdir

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    ReplicatedPG *pg;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, SnapSetContext *_ssc,
	      ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(_obs), snapset(0),
      new_obs(_obs->oi, _obs->exists),
      modify(false), user_modify(false),
      bytes_written(0), bytes_read(0),
      obc(0), clone_obc(0), snapset_obc(0), data_off(0), reply(NULL), pg(_pg) { 
      if (_ssc) {
	new_snapset = _ssc->snapset;
	snapset = &_ssc->snapset;
      }
    }
    ~OpContext() {
      assert(!clone_obc);
      if (reply)
	reply->put();
    }
  };

protected:
  uuid_d volume_id;

  Mutex vol_lock;
  Mutex map_lock;

  CohortOSDServiceRef osd;

  // Ops waiting for map, should be queued at back
  list<OpRequestRef> waiting_for_map;
  CohortOSDMapRef osdmap_ref;
  CohortOSDMapRef last_persisted_osdmap_ref;
  VolLog vol_log;
  hobject_t log_oid;


  void queue_op(OpRequestRef op);
  void take_op_map_waiters();

  void update_osdmap_ref(CohortOSDMapRef newmap) {
    Mutex::Locker l(map_lock);
    osdmap_ref = newmap;
  }

  CohortOSDMapRef get_osdmap() const {
    return osdmap_ref;
  }

public:

  hobject_t biginfo_oid;

public:
  eversion_t last_update_ondisk; // last_update that has committed;
				 // ONLY DEFINED WHEN is_active()
  eversion_t last_complete_ondisk; // last_complete that has committed.
  eversion_t last_update_applied;

protected:

// for ordering writes
  std::tr1::shared_ptr<ObjectStore::Sequencer> osr;

public:
  void activate(ObjectStore::Transaction& t,
		epoch_t query_epoch,
		list<Context*>& tfin);
  void _activate_committed(epoch_t e);
  void all_activated_and_committed();

  void lock() {
    vol_lock.Lock();
  }

  void unlock() {
    vol_lock.Unlock();
  }
public:
  OSDVol(CohortOSDServiceRef o, CohortOSDMapRef curmap,
	 uuid_d u, const hobject_t& loid, const hobject_t& ioid)
    : volume_id(u), vol_lock("OSDVol::vol_lock"),
      map_lock("OSDVol::map_lock"), osd(o),
      osdmap_ref(curmap), vol_log(), log_oid(loid), biginfo_oid(ioid) { }
  virtual ~OSDVol() { };

private:
  // Prevent copying
  OSDVol(const OSDVol& rhs);
  OSDVol& operator=(const OSDVol& rhs);

public:
  uuid_d get_volume_id() const { return volume_id; }
  void init(ObjectStore::Transaction *t);
  void do_pending_flush();

private:
  void write_info(ObjectStore::Transaction& t);

public:
  static int _write_info(ObjectStore::Transaction& t, epoch_t epoch,
			 coll_t coll, hobject_t &infos_oid,
			 __u8 info_struct_v, bool dirty_big_info);
  static int read_info(ObjectStore *store, const coll_t coll,
		       bufferlist &bl, hobject_t &biginfo_oid,
		       hobject_t &infos_oid);
  void read_state(ObjectStore *store, bufferlist &bl);
  static epoch_t peek_map_epoch(ObjectStore *store, coll_t coll,
				hobject_t &infos_oid, bufferlist *bl);

  // OpRequest queueing
  bool can_discard_op(OpRequestRef op);
  bool can_discard_request(OpRequestRef op);

  static bool op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op);

  static bool split_request(OpRequestRef op, unsigned match, unsigned bits);

  static bool have_same_or_newer_map(OSDMapRef osdmap, epoch_t e) {
    return e <= osdmap->get_epoch();
  }
  bool have_same_or_newer_map(epoch_t e) {
    return e <= get_osdmap()->get_epoch();
  }

  bool op_has_sufficient_caps(OpRequestRef op);

  void do_request(OpRequestRef op);

  void do_op(OpRequestRef op);
  void do_sub_op(OpRequestRef op);
  void do_sub_op_reply(OpRequestRef op);
  int find_object_context(const hobject_t& oid,
			  ObjectContext **pobc,
			  bool can_create,
			  snapid_t *psnapid);
  ObjectContext *get_object_context(const hobject_t& soid, bool can_create);
  void put_object_context(ObjectContext *obc);
  SnapSetContext *get_snapset_context(const object_t& oid,
				      bool can_create);
  void put_snapset_context(SnapSetContext *ssc);
  void put_object_contexts(map<hobject_t,ObjectContext*>& obcv);
};

ostream& operator <<(ostream& out, const OSDVol& vol);

#endif /* !COHORT_OSDVOL_H */
