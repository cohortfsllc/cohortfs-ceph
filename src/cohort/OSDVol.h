// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef COHORT_OSDVOL_H
#define COHORT_OSDVOL_H

#include <tr1/memory>
#include "common/Mutex.h"
#include "include/uuid.h"
#include "osd/OpRequest.h"
#include "cohort/CohortOSDMap.h"
#include "os/ObjectStore.h"
#include "osd/Watch.h"

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

protected:
  Mutex vol_lock;
  Mutex map_lock;

  CohortOSDServiceRef osd;

  // Ops waiting for map, should be queued at back
  list<OpRequestRef> waiting_for_map;
  CohortOSDMapRef osdmap_ref;
  CohortOSDMapRef last_persisted_osdmap_ref;

  uuid_d volume_id;


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

  hobject_t log_oid;
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
    : vol_lock("OSDVol::vol_lock"), map_lock("OSDVol::map_lock"),
      osd(o), osdmap_ref(curmap), volume_id(u),
      log_oid(loid), biginfo_oid(ioid) { }
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
};

ostream& operator <<(ostream& out, const OSDVol& vol);

#endif /* !COHORT_OSDVOL_H */
