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

#ifndef CEPH_OSD_H
#define CEPH_OSD_H

#include <tr1/memory>
#include "boost/tuple/tuple.hpp"

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/LogClient.h"

#include "os/ObjectStore.h"
#include "OSDCap.h"

#include "common/DecayCounter.h"
#include "osd/ClassHandler.h"

#include "include/CompatSet.h"

#include "auth/KeyRing.h"
#include "messages/MOSDRepScrub.h"
#include "OpRequest.h"

#include "OSDMap.h"

#include <map>
#include <memory>
#include <tr1/memory>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "OpRequest.h"
#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/sharedptr_registry.hpp"

#define CEPH_OSD_PROTOCOL    10 /* cluster internal */


using namespace std::tr1;


enum {
  l_osd_first = 10000,
  l_osd_opq,
  l_osd_op_wip,
  l_osd_op,
  l_osd_op_inb,
  l_osd_op_outb,
  l_osd_op_lat,
  l_osd_op_r,
  l_osd_op_r_outb,
  l_osd_op_r_lat,
  l_osd_op_w,
  l_osd_op_w_inb,
  l_osd_op_w_rlat,
  l_osd_op_w_lat,
  l_osd_op_rw,
  l_osd_op_rw_inb,
  l_osd_op_rw_outb,
  l_osd_op_rw_rlat,
  l_osd_op_rw_lat,

  l_osd_sop,
  l_osd_sop_inb,
  l_osd_sop_lat,
  l_osd_sop_w,
  l_osd_sop_w_inb,
  l_osd_sop_w_lat,
  l_osd_sop_pull,
  l_osd_sop_pull_lat,
  l_osd_sop_push,
  l_osd_sop_push_inb,
  l_osd_sop_push_lat,

  l_osd_pull,
  l_osd_push,
  l_osd_push_outb,

  l_osd_rop,

  l_osd_loadavg,
  l_osd_buf,

  l_osd_pg,
  l_osd_pg_primary,
  l_osd_pg_replica,
  l_osd_pg_stray,
  l_osd_hb_to,
  l_osd_hb_from,
  l_osd_map,
  l_osd_mape,
  l_osd_mape_dup,

  l_osd_last,
};

class Messenger;
class Message;
class MonClient;
class PerfCounters;
class ObjectStore;
class OSDMap;
class MLog;
class MClass;

class Watch;
class Notification;

class AuthAuthorizeHandlerRegistry;

class OpsFlightSocketHook;
class HistoricOpsSocketHook;

extern const coll_t meta_coll;

typedef std::tr1::shared_ptr<ObjectStore::Sequencer> SequencerRef;

class DeletingState {
  Mutex lock;
  list<Context *> on_deletion_complete;
public:
  DeletingState() : lock("DeletingState::lock") {}
  void register_on_delete(Context *completion) {
    Mutex::Locker l(lock);
    on_deletion_complete.push_front(completion);
  }
  ~DeletingState() {
    Mutex::Locker l(lock);
    for (list<Context *>::iterator i = on_deletion_complete.begin();
	 i != on_deletion_complete.end();
	 ++i) {
      (*i)->complete(0);
    }
  }
}; // class DeletingState
typedef std::tr1::shared_ptr<DeletingState> DeletingStateRef;

class OSD;

class OSDService {
public:
  OSD *osd;
  const int whoami;
  ObjectStore *&store;
  LogClient &clog;
  Messenger *&cluster_messenger;
  Messenger *&client_messenger;
  PerfCounters *&logger;
  MonClient   *&monc;
  ClassHandler  *&class_handler;

  // -- superblock --
  Mutex publish_lock;
  OSDSuperblock superblock;
  OSDSuperblock get_superblock() {
    Mutex::Locker l(publish_lock);
    return superblock;
  }
  void publish_superblock(OSDSuperblock block) {
    Mutex::Locker l(publish_lock);
    superblock = block;
  }
  OSDMapRef osdmap;
  OSDMapRef get_osdmap() {
    Mutex::Locker l(publish_lock);
    return osdmap;
  }
  void publish_map(OSDMapRef map) {
    Mutex::Locker l(publish_lock);
    osdmap = map;
  }

  int get_nodeid() const { return whoami; }

  // -- scrub scheduling --
  Mutex sched_scrub_lock;
  int scrubs_pending;
  int scrubs_active;

  bool scrub_should_schedule();

  bool inc_scrubs_pending();
  void dec_scrubs_pending();
  void dec_scrubs_active();

  void reply_op_error(OpRequestRef op, int err);
  void reply_op_error(OpRequestRef op, int err, eversion_t v);

  // -- Watch --
  Mutex watch_lock;
  SafeTimer watch_timer;
  Watch *watch;

  // -- tids --
  // for ops i issue
  tid_t last_tid;
  Mutex tid_lock;
  tid_t get_tid() {
    tid_t t;
    tid_lock.Lock();
    t = ++last_tid;
    tid_lock.Unlock();
    return t;
  }


  // osd map cache (past osd maps)
  Mutex map_cache_lock;
  SharedLRU<epoch_t, const OSDMap> map_cache;
  SimpleLRU<epoch_t, bufferlist> map_bl_cache;
  SimpleLRU<epoch_t, bufferlist> map_bl_inc_cache;


  OSDMapRef get_map(epoch_t e);
  OSDMapRef add_map(OSDMap *o) {
    Mutex::Locker l(map_cache_lock);
    return _add_map(o);
  }
  OSDMapRef _add_map(OSDMap *o);

  void add_map_bl(epoch_t e, bufferlist& bl) {
    Mutex::Locker l(map_cache_lock);
    return _add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl);
  void _add_map_bl(epoch_t e, bufferlist& bl);
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    Mutex::Locker l(map_cache_lock);
    return _get_map_bl(e, bl);
  }
  bool _get_map_bl(epoch_t e, bufferlist& bl);

  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    Mutex::Locker l(map_cache_lock);
    return _add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl);
  void _add_map_inc_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map_bl(epoch_t e, bufferlist& bl);

  void clear_map_bl_cache_pins();

  void need_heartbeat_peer_update();

  OSDService(OSD *osd);

  // this virtual function soley exists to make the class polymorphic
  virtual ~OSDService() { };

  virtual OSDMap* newOSDMap() const = 0;
}; // class OSDService


typedef shared_ptr<OSDService> OSDServiceRef;


class OSD : public Dispatcher {
  /** OSD **/
protected:
  Mutex osd_lock;			// global lock
  SafeTimer timer;    // safe timer (osd_lock)

  AuthAuthorizeHandlerRegistry *authorize_handler_registry;

  Messenger   *cluster_messenger;
  Messenger   *client_messenger;
  MonClient   *monc;
  PerfCounters      *logger;
  ObjectStore *store;

  LogClient clog;

  int whoami;
  std::string dev_path, journal_path;

  class C_Tick : public Context {
    OSD *osd;
  public:
    C_Tick(OSD *o) : osd(o) {}
    void finish(int r) {
      osd->tick();
    }
  };

  Cond dispatch_cond;
  int dispatch_running;

  void create_logger();
  void tick();
  void _dispatch(Message *m);
  void dispatch_op(OpRequestRef op);

  virtual void tick_sub(const utime_t& now) = 0;
  virtual void dispatch_op_sub(OpRequestRef op) = 0;
  virtual bool _dispatch_sub(Message *m) = 0;

public:
  ClassHandler  *class_handler;
  int get_nodeid() { return whoami; }
  
  static hobject_t get_osdmap_pobject_name(epoch_t epoch) { 
    char foo[20];
    snprintf(foo, sizeof(foo), "osdmap.%d", epoch);
    return hobject_t(sobject_t(object_t(foo), 0)); 
  }
  static hobject_t get_inc_osdmap_pobject_name(epoch_t epoch) { 
    char foo[20];
    snprintf(foo, sizeof(foo), "inc_osdmap.%d", epoch);
    return hobject_t(sobject_t(object_t(foo), 0)); 
  }
  

protected:
  // -- superblock --
  OSDSuperblock superblock;

  void write_superblock();
  void write_superblock(ObjectStore::Transaction& t);
  int read_superblock();

private:
  CompatSet osd_compat;

  // -- state --
public:
  static const int STATE_BOOTING = 1;
  static const int STATE_ACTIVE = 2;
  static const int STATE_STOPPING = 3;

protected:
  int state;
  epoch_t boot_epoch;  // _first_ epoch we were marked up (after this process started)
  epoch_t up_epoch;    // _most_recent_ epoch we were marked up
  epoch_t bind_epoch;  // epoch we last did a bind to new ip:ports

public:
  bool is_booting() { return state == STATE_BOOTING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }

protected:
  ThreadPool op_tp;
  ThreadPool recovery_tp;
  ThreadPool disk_tp;
  ThreadPool command_tp;

  // -- sessions --
public:
  struct Session : public RefCountedObject {
    EntityName entity_name;
    OSDCap caps;
    int64_t auid;
    epoch_t last_sent_epoch;
    Connection *con;
    std::map<void *, entity_name_t> notifs;

    Session() : auid(-1), last_sent_epoch(0), con(0) {}
    void add_notif(void *n, entity_name_t& name) {
      notifs[n] = name;
    }
    void del_notif(void *n) {
      std::map<void *, entity_name_t>::iterator iter = notifs.find(n);
      if (iter != notifs.end())
        notifs.erase(iter);
    }
  };

private:
  // -- heartbeat --
  /// information about a heartbeat peer
  struct HeartbeatInfo {
    Connection *con;    ///< peer connection
    utime_t first_tx;   ///< time we sent our first ping request
    utime_t last_tx;    ///< last time we sent a ping request
    utime_t last_rx;    ///< last time we got a ping reply
    epoch_t epoch;      ///< most recent epoch we wanted this peer
  };
  Mutex heartbeat_lock;
  map<int, int> debug_heartbeat_drops_remaining;
  Cond heartbeat_cond;
  bool heartbeat_stop;
  bool heartbeat_need_update;   ///< true if we need to refresh our heartbeat peers
  epoch_t heartbeat_epoch;      ///< last epoch we updated our heartbeat peers
  map<int,HeartbeatInfo> heartbeat_peers;  ///< map of osd id to HeartbeatInfo
  utime_t last_mon_heartbeat;
  Messenger *hbclient_messenger, *hbserver_messenger;

protected:  
  void _add_heartbeat_peer(int p);
  void maybe_update_heartbeat_peers();
  virtual void build_heartbeat_peers_list() const = 0;
  void reset_heartbeat_peers();
  void heartbeat();
  void heartbeat_check();
  void heartbeat_entry();
  void need_heartbeat_peer_update();

private:

  struct T_Heartbeat : public Thread {
    OSD *osd;
    T_Heartbeat(OSD *o) : osd(o) {}
    void *entry() {
      osd->heartbeat_entry();
      return 0;
    }
  } heartbeat_thread;

public:
  bool heartbeat_dispatch(Message *m);

  struct HeartbeatDispatcher : public Dispatcher {
  private:
    bool ms_dispatch(Message *m) {
      return osd->heartbeat_dispatch(m);
    };
    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}
  public:
    OSD *osd;
    HeartbeatDispatcher(OSD *o) 
      : Dispatcher(g_ceph_context), osd(o)
    {
    }
  } heartbeat_dispatcher;


protected:
  // -- stats --
  Mutex stat_lock;
  osd_stat_t osd_stat;

private:
  void update_osd_stat();
  
  // -- waiters --
  list<OpRequestRef> finished;
  Mutex finished_lock;

protected:  
  void take_waiters(list<OpRequestRef>& ls) {
    finished_lock.Lock();
    finished.splice(finished.end(), ls);
    finished_lock.Unlock();
  }
  void take_waiter(OpRequestRef op) {
    finished_lock.Lock();
    finished.push_back(op);
    finished_lock.Unlock();
  }
  void do_waiters();
  
private:
  // -- op tracking --
  OpTracker op_tracker;
  void check_ops_in_flight();
  void dump_ops_in_flight(ostream& ss);
  void dump_historic_ops(ostream& ss) {
    return op_tracker.dump_historic_ops(ss);
  }
  friend class OpsFlightSocketHook;
  friend class HistoricOpsSocketHook;
  OpsFlightSocketHook *admin_ops_hook;
  HistoricOpsSocketHook *historic_ops_hook;


 protected:

  // -- osd map --
  OSDMapRef       osdmap;
  OSDMapRef get_osdmap() {
    return osdmap;
  }
  utime_t         had_map_since;
  RWLock          map_lock;
  list<OpRequestRef>  waiting_for_osdmap;

  Mutex peer_map_epoch_lock;
  map<int, epoch_t> peer_map_epoch;
  
  epoch_t get_peer_epoch(int p);
  epoch_t note_peer_epoch(int p, epoch_t e);
  void forget_peer_epoch(int p, epoch_t e);

  bool _share_map_incoming(const entity_inst_t& inst, epoch_t epoch,
			   Session *session = 0);
  void _share_map_outgoing(const entity_inst_t& inst,
			   OSDMapRef map = OSDMapRef());

  void wait_for_new_map(OpRequestRef op);
  void handle_osd_map(class MOSDMap *m);
  void note_down_osd(int osd);
  void note_up_osd(int osd);
  
  void advance_map(ObjectStore::Transaction& t, C_Contexts *tfin);
  void activate_map();

  virtual void advance_map_sub(ObjectStore::Transaction& t,
			       C_Contexts *tfin) = 0;
  virtual void activate_map_sub() = 0;

  // osd map cache (past osd maps)
  OSDMapRef get_map(epoch_t e) {
    return serviceRef->get_map(e);
  }
  OSDMapRef add_map(OSDMap *o) {
    return serviceRef->add_map(o);
  }
  void add_map_bl(epoch_t e, bufferlist& bl) {
    return serviceRef->add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl) {
    return serviceRef->pin_map_bl(e, bl);
  }
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    return serviceRef->get_map_bl(e, bl);
  }
  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    return serviceRef->add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl) {
    return serviceRef->pin_map_inc_bl(e, bl);
  }
  bool get_inc_map_bl(epoch_t e, bufferlist& bl) {
    return serviceRef->get_inc_map_bl(e, bl);
  }
  void clear_map_bl_cache_pins() {
    serviceRef->clear_map_bl_cache_pins();
  }

  MOSDMap *build_incremental_map_msg(epoch_t from, epoch_t to);
  void send_incremental_map(epoch_t since, const entity_inst_t& inst, bool lazy=false);
  void send_map(MOSDMap *m, const entity_inst_t& inst, bool lazy);

protected:
  // == monitor interaction ==
  utime_t last_mon_report;
  void do_mon_report();
  virtual void do_mon_report_sub(const utime_t& now) = 0;

  // -- boot --
  void start_boot();
  void _maybe_boot(epoch_t oldest, epoch_t newest);
  void _send_boot();
  
  friend class C_OSD_GetVersion;

  static void clear_temp(ObjectStore *store, coll_t tmp);

  // -- alive --
  epoch_t up_thru_wanted;
  epoch_t up_thru_pending;

  void queue_want_up_thru(epoch_t want);
  void send_alive();

  // -- failures --
  set<int> failure_queue;
  map<int,entity_inst_t> failure_pending;


  void queue_failure(int n) {
    failure_queue.insert(n);
  }
  void send_failures();
  void send_still_alive(epoch_t epoch, entity_inst_t i);

  tid_t get_tid() {
    return serviceRef->get_tid();
  }


  bool require_mon_peer(Message *m);
  bool require_osd_peer(OpRequestRef op);

  bool require_same_or_newer_map(OpRequestRef op, epoch_t e);

  // -- commands --
  struct Command {
    vector<string> cmd;
    tid_t tid;
    bufferlist indata;
    Connection *con;

    Command(vector<string>& c, tid_t t, bufferlist& bl, Connection *co)
      : cmd(c), tid(t), indata(bl), con(co) {
      if (con)
	con->get();
    }
    ~Command() {
      if (con)
	con->put();
    }
  };
  list<Command*> command_queue;
  struct CommandWQ : public ThreadPool::WorkQueue<Command> {
    OSD *osd;
    CommandWQ(OSD *o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<Command>("OSD::CommandWQ", ti, 0, tp), osd(o) {}

    bool _empty() {
      return osd->command_queue.empty();
    }
    bool _enqueue(Command *c) {
      osd->command_queue.push_back(c);
      return true;
    }
    void _dequeue(Command *c) {
      assert(0);
    }
    Command *_dequeue() {
      if (osd->command_queue.empty())
	return NULL;
      Command *c = osd->command_queue.front();
      osd->command_queue.pop_front();
      return c;
    }
    void _process(Command *c) {
      osd->osd_lock.Lock();
      osd->do_command(c->con, c->tid, c->cmd, c->indata);
      osd->osd_lock.Unlock();
      delete c;
    }
    void _clear() {
      while (!osd->command_queue.empty()) {
	Command *c = osd->command_queue.front();
	osd->command_queue.pop_front();
	delete c;
      }
    }
  } command_wq;

  void handle_command(class MMonCommand *m);
  void handle_command(class MCommand *m);
  void do_command(Connection *con,
		  tid_t tid,
		  vector<string>& cmd,
		  bufferlist& data);
  // true if handled, false if not handled
  virtual bool do_command_sub(Connection *con,
			      tid_t tid,
			      vector<string>& cmd,
			      bufferlist& data,
			      bufferlist& odata,
			      int& r,
			      ostringstream& ss) = 0;
  virtual bool do_command_debug_sub(vector<string>& cmd,
				    int& r,
				    ostringstream& ss) = 0;

  // -- removing --
  struct RemoveWQ : public ThreadPool::WorkQueue<boost::tuple<coll_t, SequencerRef, DeletingStateRef> > {
    ObjectStore *&store;
    list<boost::tuple<coll_t, SequencerRef, DeletingStateRef> *> remove_queue;
    RemoveWQ(ObjectStore *&o, time_t ti, ThreadPool *tp)
      : ThreadPool::WorkQueue<boost::tuple<coll_t, SequencerRef, DeletingStateRef> >("OSD::RemoveWQ", ti, 0, tp),
	store(o) {}

    bool _empty() {
      return remove_queue.empty();
    }
    bool _enqueue(boost::tuple<coll_t, SequencerRef, DeletingStateRef> *item) {
      remove_queue.push_back(item);
      return true;
    }
    void _dequeue(boost::tuple<coll_t, SequencerRef, DeletingStateRef> *item) {
      assert(0);
    }
    boost::tuple<coll_t, SequencerRef, DeletingStateRef> *_dequeue() {
      if (remove_queue.empty())
	return NULL;
      boost::tuple<coll_t, SequencerRef, DeletingStateRef> *item = remove_queue.front();
      remove_queue.pop_front();
      return item;
    }
    void _process(boost::tuple<coll_t, SequencerRef, DeletingStateRef> *item);
    void _clear() {
      while (!remove_queue.empty()) {
	delete remove_queue.front();
	remove_queue.pop_front();
      }
    }
  } remove_wq;

 private:
  bool ms_dispatch(Message *m);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer,
			    bufferlist& authorizer_reply,
			    bool& isvalid);
  void ms_handle_connect(Connection *con);
  virtual void ms_handle_connect_sub(Connection *con) = 0;
  bool ms_handle_reset(Connection *con);
  virtual void ms_handle_reset_sub(Session* session) = 0;
  void ms_handle_remote_reset(Connection *con) {}

 public:
  /* internal and external can point to the same messenger, they will still
   * be cleaned up properly*/
  OSD(int id, Messenger *internal, Messenger *external, Messenger *hbmin,
      Messenger *hbmout,
      MonClient *mc, const std::string &dev, const std::string &jdev,
      OSDService* osdSvc);
  virtual ~OSD();

  virtual OSDService* newOSDService(const OSD* osd) const = 0;
  virtual OSDMap* newOSDMap() const = 0;

  utime_t last_stats_sent;
  bool osd_stat_updated;

  // static bits
  static int find_osd_dev(char *result, int whoami);
  static ObjectStore *create_object_store(const std::string &dev, const std::string &jdev);
  static int convertfs(const std::string &dev, const std::string &jdev);
  static int do_convertfs(ObjectStore *store);
  static int convert_collection(ObjectStore *store, coll_t cid);
  static int mkfs(const std::string &dev, const std::string &jdev,
		  uuid_d fsid, int whoami);
  static int mkjournal(const std::string &dev, const std::string &jdev);
  static int flushjournal(const std::string &dev, const std::string &jdev);
  static int dump_journal(const std::string &dev, const std::string &jdev, ostream& out);
  /* remove any non-user xattrs from a map of them */
  void filter_xattrs(map<string, bufferptr>& attrs) {
    for (map<string, bufferptr>::iterator iter = attrs.begin();
	 iter != attrs.end();
	 ) {
      if (('_' != iter->first.at(0)) || (iter->first.size() == 1))
	attrs.erase(iter++);
      else ++iter;
    }
  }

private:
  static int write_meta(const std::string &base, const std::string &file,
			const char *val, size_t vallen);
  static int read_meta(const std::string &base, const std::string &file,
		       char *val, size_t vallen);
  static int write_meta(const std::string &base,
			uuid_d& cluster_fsid, uuid_d& osd_fsid, int whoami);
public:
  static int peek_meta(const std::string &dev, string& magic,
		       uuid_d& cluster_fsid, uuid_d& osd_fsid, int& whoami);
  static int peek_journal_fsid(std::string jpath, uuid_d& fsid);
  

  // startup/shutdown
  int pre_init();
  int init_super();
  virtual int init() = 0;

  void suicide(int exitcode);

  int shutdown_super();
  virtual int shutdown() = 0;

  virtual void check_replay_queue() = 0;

  // -- scrubbing --
  virtual void sched_scrub() = 0;

  void handle_signal(int signum);

  void handle_osd_ping(class MOSDPing *m);
  void handle_op(OpRequestRef op);
  void handle_sub_op(OpRequestRef op);
  void handle_sub_op_reply(OpRequestRef op);

  virtual void handle_op_sub(OpRequestRef op) = 0;
  virtual bool handle_sub_op_sub(OpRequestRef op) = 0;
  virtual bool handle_sub_op_reply_sub(OpRequestRef op) = 0;

  /// check if we can throw out op from a disconnected client
  static bool op_is_discardable(class MOSDOp *m);
  /// check if op should be (re)queued for processing

public:
  void force_remount();

  int init_op_flags(MOSDOp *op);

  void complete_notify(void *notif, void *obc);

  OSDServiceRef serviceRef;
  friend class OSDService;
};

//compatibility of the executable
extern const CompatSet::Feature ceph_osd_feature_compat[];
extern const CompatSet::Feature ceph_osd_feature_ro_compat[];
extern const CompatSet::Feature ceph_osd_feature_incompat[];

#endif // CEPH_OSD_H
