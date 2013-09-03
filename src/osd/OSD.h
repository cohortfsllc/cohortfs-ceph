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

#include "common/AsyncReserver.h"
#include <tr1/memory>
#include "boost/tuple/tuple.hpp"

#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/LogClient.h"

#include "os/ObjectStore.h"
#include "OSDCap.h"

#include "osd/ClassHandler.h"

#include "include/CompatSet.h"

#include "auth/KeyRing.h"
#include "OpRequest.h"

#include "OSDMap.h"

#include <map>
#include <memory>
#include <tr1/memory>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/sharedptr_registry.hpp"
#include "common/PrioritizedQueue.h"
#include "include/assert.h"

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

  l_osd_push_in,
  l_osd_push_inb,

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

class AuthAuthorizeHandlerRegistry;

class OpsFlightSocketHook;
class HistoricOpsSocketHook;
class TestOpsSocketHook;

extern const coll_t meta_coll;

typedef std::tr1::shared_ptr<ObjectStore::Sequencer> SequencerRef;

class OSD;
class OSDService {
public:
  OSD *osd;
  const int whoami;
  ObjectStore *&store;
  LogClient &clog;
  hobject_t infos_oid;
private:
  Messenger *&cluster_messenger;
  Messenger *&client_messenger;
public:
  PerfCounters *&logger;
  MonClient   *&monc;
  ClassHandler  *&class_handler;

  // -- superblock --
  Mutex publish_lock, pre_publish_lock;
  OSDSuperblock superblock;
  OSDSuperblock get_superblock(void) {
    return superblock;
  }
  void publish_superblock(const OSDSuperblock &block) {
    Mutex::Locker l(publish_lock);
    superblock = block;
  }

  int get_nodeid() const { return whoami; }

  OSDMapRef osdmap;
  VolMapRef volmap;
  OSDMapRef get_osdmap() {
    Mutex::Locker l(publish_lock);
    return osdmap;
  }
  void publish_map(OSDMapRef map) {
    Mutex::Locker l(publish_lock);
    osdmap = map;
  }

  /*
   * osdmap - current published amp
   * next_osdmap - pre_published map that is about to be published.
   *
   * We use the next_osdmap to send messages and initiate connections,
   * but only if the target is the same instance as the one in the map
   * epoch the current user is working from (i.e., the result is
   * equivalent to what is in next_osdmap).
   *
   * This allows the helpers to start ignoring osds that are about to
   * go down, and let OSD::handle_osd_map()/note_down_osd() mark them
   * down, without worrying about reopening connections from threads
   * working from old maps.
   */
  OSDMapRef next_osdmap;
  void pre_publish_map(OSDMapRef map) {
    Mutex::Locker l(pre_publish_lock);
    next_osdmap = map;
  }
  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch);
  pair<ConnectionRef,ConnectionRef> get_con_osd_hb(int peer, epoch_t from_epoch);  // (back, front)
  void send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch);
  void send_message_osd_cluster(Message *m, Connection *con) {
    cluster_messenger->send_message(m, con);
  }
  void send_message_osd_cluster(Message *m, const ConnectionRef& con) {
    cluster_messenger->send_message(m, con.get());
  }
  void send_message_osd_client(Message *m, Connection *con) {
    client_messenger->send_message(m, con);
  }
  void send_message_osd_client(Message *m, const ConnectionRef& con) {
    client_messenger->send_message(m, con.get());
  }
  entity_name_t get_cluster_msgr_name() {
    return cluster_messenger->get_myname();
  }

  // -- scrub scheduling --
  Mutex sched_scrub_lock;
  int scrubs_pending;
  int scrubs_active;

  bool inc_scrubs_pending();
  void inc_scrubs_active(bool reserved);
  void dec_scrubs_pending();
  void dec_scrubs_active();

  void reply_op_error(OpRequestRef op, int err);
  void reply_op_error(OpRequestRef op, int err, eversion_t v);

  // -- Backfill Request Scheduling --
  Mutex backfill_request_lock;
  SafeTimer backfill_request_timer;

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

  void clear_map_bl_cache_pins(epoch_t e);

  void need_heartbeat_peer_update();

  void init();
  void shutdown();
  virtual void init_sub() = 0;
  virtual void shutdown_sub() = 0;

  // -- OSD Full Status --
  Mutex full_status_lock;
  enum s_names { NONE, NEAR, FULL } cur_state;
  time_t last_msg;
  double cur_ratio;
  float get_full_ratio();
  float get_nearfull_ratio();
  void check_nearfull_warning(const osd_stat_t &stat);
  bool check_failsafe_full();
  bool too_full_for_backfill(double *ratio, double *max_ratio);


  // -- stopping --
  Mutex is_stopping_lock;
  Cond is_stopping_cond;
  enum {
    NOT_STOPPING,
    PREPARING_TO_STOP,
    STOPPING } state;
  bool is_stopping() {
    Mutex::Locker l(is_stopping_lock);
    return state == STOPPING;
  }
  bool is_preparing_to_stop() {
    Mutex::Locker l(is_stopping_lock);
    return state == PREPARING_TO_STOP;
  }
  bool prepare_to_stop();
  void got_stop_ack();


  OSDService(OSD *osd);
  // this virtual function soley exists to make the class polymorphic
  virtual ~OSDService() { };
  virtual OSDMap* newOSDMap(VolMapRef v) const = 0;

  virtual bool test_ops_sub(ObjectStore *store,
			    std::string command,
			    std::string args,
			    ostream &ss) = 0;
};

typedef shared_ptr<OSDService> OSDServiceRef;


class OSD : public Dispatcher,
	    public md_config_obs_t {
  /** OSD **/
public:
  // config observer bits
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

protected:
  Mutex osd_lock;			// global lock
  SafeTimer tick_timer;    // safe timer (osd_lock)

  AuthAuthorizeHandlerRegistry *authorize_handler_cluster_registry;
  AuthAuthorizeHandlerRegistry *authorize_handler_service_registry;

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
  virtual bool _dispatch_sub(Message *m) = 0;
  virtual void dispatch_op_sub(OpRequestRef op) = 0;

  void check_osdmap_features();

  // asok
  friend class OSDSocketHook;
  class OSDSocketHook *asok_hook;
  bool asok_command(string command, string args, ostream& ss);
  virtual bool asok_command_sub(string command, string args, ostream& ss) = 0;

public:
  ClassHandler  *class_handler;
  int get_nodeid() { return whoami; }

  static hobject_t get_osdmap_pobject_name(epoch_t epoch) { 
    char foo[20];
    snprintf(foo, sizeof(foo), "osdmap.%d", epoch);
    return hobject_t(sobject_t(object_t(0, foo), 0));
  }
  static hobject_t get_inc_osdmap_pobject_name(epoch_t epoch) { 
    char foo[20];
    snprintf(foo, sizeof(foo), "inc_osdmap.%d", epoch);
    return hobject_t(sobject_t(object_t(0, foo), 0));
  }

  static hobject_t make_snapmapper_oid() {
    return hobject_t(
      sobject_t(
	object_t(0, "snapmapper"),
	0));
  }

  static hobject_t make_infos_oid() {
    hobject_t oid(sobject_t(object_t(0, "infos"), CEPH_NOSNAP));
    return oid;
  }

  static void recursive_remove_collection(ObjectStore *store, coll_t tmp);

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
  static const int STATE_INITIALIZING = 1;
  static const int STATE_BOOTING = 2;
  static const int STATE_ACTIVE = 3;
  static const int STATE_STOPPING = 4;
  static const int STATE_WAITING_FOR_HEALTHY = 5;

protected:
  int state;
  epoch_t boot_epoch;  // _first_ epoch we were marked up (after this process started)
  epoch_t up_epoch;    // _most_recent_ epoch we were marked up
  epoch_t bind_epoch;  // epoch we last did a bind to new ip:ports

public:
  bool is_initializing() { return state == STATE_INITIALIZING; }
  bool is_booting() { return state == STATE_BOOTING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_stopping() { return state == STATE_STOPPING; }
  bool is_waiting_for_healthy() { return state == STATE_WAITING_FOR_HEALTHY; }

protected:
  ThreadPool op_tp;
  ThreadPool recovery_tp;
  ThreadPool disk_tp;
  ThreadPool command_tp;

  bool paused_recovery;

  // -- sessions --
public:
  struct Session : public RefCountedObject {
    EntityName entity_name;
    OSDCap caps;
    int64_t auid;
    epoch_t last_sent_epoch;
    ConnectionRef con;

    Session() : auid(-1), last_sent_epoch(0), con(0) {}
  };

private:
  // -- heartbeat --
  /// information about a heartbeat peer
  struct HeartbeatInfo {
    int peer;           ///< peer
    ConnectionRef con_front;   ///< peer connection (front)
    ConnectionRef con_back;    ///< peer connection (back)
    utime_t first_tx;   ///< time we sent our first ping request
    utime_t last_tx;    ///< last time we sent a ping request
    utime_t last_rx_front;  ///< last time we got a ping reply on the front side
    utime_t last_rx_back;   ///< last time we got a ping reply on the back side
    epoch_t epoch;      ///< most recent epoch we wanted this peer

    bool is_unhealthy(utime_t cutoff) {
      return
	! ((last_rx_front > cutoff ||
	    (last_rx_front == utime_t() && (last_tx == utime_t() ||
					    first_tx > cutoff))) &&
	   (last_rx_back > cutoff ||
	    (last_rx_back == utime_t() && (last_tx == utime_t() ||
					   first_tx > cutoff))));
    }
    bool is_healthy(utime_t cutoff) {
      return last_rx_front > cutoff && last_rx_back > cutoff;
    }

  };
  /// state attached to outgoing heartbeat connections
  struct HeartbeatSession : public RefCountedObject {
    int peer;
    HeartbeatSession(int p) : peer(p) {}
  };
  Mutex heartbeat_lock;
  map<int, int> debug_heartbeat_drops_remaining;
  Cond heartbeat_cond;
  bool heartbeat_stop;
  bool heartbeat_need_update;   ///< true if we need to refresh our heartbeat peers
  epoch_t heartbeat_epoch;      ///< last epoch we updated our heartbeat peers
  map<int,HeartbeatInfo> heartbeat_peers;  ///< map of osd id to HeartbeatInfo
  utime_t last_mon_heartbeat;
  Messenger *hbclient_messenger;
  Messenger *hb_front_server_messenger;
  Messenger *hb_back_server_messenger;
  utime_t last_heartbeat_resample;   ///< last time we chose random peers in waiting-for-healthy state

protected:

  void _add_heartbeat_peer(int p);
  void _remove_heartbeat_peer(int p);
  bool heartbeat_reset(Connection *con);
  void maybe_update_heartbeat_peers();
  virtual void build_heartbeat_peers_list() = 0;
  void reset_heartbeat_peers();
  void heartbeat();
  void heartbeat_check();
  void heartbeat_entry();
  void need_heartbeat_peer_update();

  void heartbeat_kick() {
    Mutex::Locker l(heartbeat_lock);
    heartbeat_cond.Signal();
  }

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
    bool ms_handle_reset(Connection *con) {
      return osd->heartbeat_reset(con);
    }
    void ms_handle_remote_reset(Connection *con) {}
    bool ms_verify_authorizer(Connection *con, int peer_type,
			      int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			      bool& isvalid, CryptoKey& session_key) {
      isvalid = true;
      return true;
    }
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
  void take_waiters_front(list<OpRequestRef>& ls) {
    finished_lock.Lock();
    finished.splice(finished.begin(), ls);
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
  friend class TestOpsSocketHook;
  TestOpsSocketHook *test_ops_hook;

 protected:

  // -- osd map --
  OSDMapRef osdmap;
  VolMapRef volmap;
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

  bool _share_map_incoming(entity_name_t name, Connection *con, epoch_t epoch,
			   Session *session = 0);
  void _share_map_outgoing(int peer, Connection *con,
			   OSDMapRef map = OSDMapRef());

  void wait_for_new_map(OpRequestRef op);
  void handle_osd_map(class MOSDMap *m);
  void handle_vol_map(class MVolMap *m);
  void note_down_osd(int osd);
  void note_up_osd(int osd);
  
  void advance_map(ObjectStore::Transaction& t, C_Contexts *tfin);
  void consume_map();
  void activate_map();

  virtual void advance_map_sub(ObjectStore::Transaction& t,
			       C_Contexts *tfin) = 0;
  virtual void consume_map_sub() = 0;

  // osd map cache (past osd maps)
  OSDMapRef get_map(epoch_t e) {
    return service->get_map(e);
  }
  OSDMapRef add_map(OSDMap *o) {
    return service->add_map(o);
  }
  void add_map_bl(epoch_t e, bufferlist& bl) {
    return service->add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl) {
    return service->pin_map_bl(e, bl);
  }
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    return service->get_map_bl(e, bl);
  }
  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    return service->add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl) {
    return service->pin_map_inc_bl(e, bl);
  }
  bool get_inc_map_bl(epoch_t e, bufferlist& bl) {
    return service->get_inc_map_bl(e, bl);
  }

  MOSDMap *build_incremental_map_msg(epoch_t from, epoch_t to);
  void send_incremental_map(epoch_t since, Connection *con);
  void send_map(MOSDMap *m, Connection *con);

protected:
  // -- scrubbing --
  bool scrub_random_backoff();
  bool scrub_should_schedule();

  // == monitor interaction ==
  utime_t last_mon_report;
  void do_mon_report();
  virtual void do_mon_report_sub(const utime_t& now) = 0;

  // -- boot --
  void start_boot();
  void _maybe_boot(epoch_t oldest, epoch_t newest);
  void _send_boot();

  void start_waiting_for_healthy();
  bool _is_healthy();

  friend class C_OSD_GetVersion;
  /**
   * @todo Dubious
   */
  friend struct C_CompleteSplits;

  // -- alive --
  epoch_t up_thru_wanted;
  epoch_t up_thru_pending;

  void queue_want_up_thru(epoch_t want);
  void send_alive();

  // -- failures --
  map<int,utime_t> failure_queue;
  map<int,entity_inst_t> failure_pending;


  void send_failures();
  void send_still_alive(epoch_t epoch, const entity_inst_t &i);

  tid_t get_tid() {
    return service->get_tid();
  }

  bool require_mon_peer(Message *m);
  bool require_osd_peer(OpRequestRef op);

  bool require_same_or_newer_map(OpRequestRef op,
				 epoch_t osdmap_epoch,
				 epoch_t volmap_epoch);

  // -- commands --
  struct Command {
    vector<string> cmd;
    tid_t tid;
    bufferlist indata;
    ConnectionRef con;

    Command(vector<string>& c, tid_t t, bufferlist& bl, Connection *co)
      : cmd(c), tid(t), indata(bl), con(co) {}
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
      if (osd->is_stopping()) {
	osd->osd_lock.Unlock();
	delete c;
	return;
      }
      osd->do_command(c->con.get(), c->tid, c->cmd, c->indata);
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
  void do_command(Connection *con, tid_t tid, vector<string>& cmd, bufferlist& data);
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

 private:
  bool ms_dispatch(Message *m);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key);
  void ms_handle_connect(Connection *con);
  virtual void ms_handle_connect_sub(Connection *con) = 0;
  bool ms_handle_reset(Connection *con);
  virtual void ms_handle_reset_sub(Session* session) = 0;
  void ms_handle_remote_reset(Connection *con) {}

 public:
  /* internal and external can point to the same messenger, they will still
   * be cleaned up properly*/
  OSD(int id, Messenger *internal, Messenger *external,
      Messenger *hb_client, Messenger *hb_front_server, Messenger *hb_back_server,
      MonClient *mc, const std::string &dev, const std::string &jdev);
  virtual ~OSD();

  virtual OSDService* newOSDService(OSD* osd) const = 0;
  virtual OSDMap* newOSDMap(VolMapRef v) const = 0;

  utime_t last_stats_sent;
  bool osd_stat_updated;

  // static bits
  static int find_osd_dev(char *result, int whoami);
  static ObjectStore *create_object_store(const std::string &dev,
					  const std::string &jdev);
  static int convertfs(const std::string &dev,
		       const std::string &jdev);
  static int do_convertfs(ObjectStore *store);
  static int convert_collection(ObjectStore *store, coll_t cid);
  static int mkfs(const std::string &dev,
		  const std::string &jdev, uuid_d fsid, int whoami);
  static int mkjournal(const std::string &dev,
		       const std::string &jdev);
  static int flushjournal(const std::string &dev,
			  const std::string &jdev);
  static int dump_journal(const std::string &dev,
			  const std::string &jdev, ostream& out);
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

  int init_op_flags(OpRequestRef op);
  OSDServiceRef service;
  friend class OSDService;
};

//compatibility of the executable
extern const CompatSet::Feature ceph_osd_feature_compat[];
extern const CompatSet::Feature ceph_osd_feature_ro_compat[];
extern const CompatSet::Feature ceph_osd_feature_incompat[];

#endif // CEPH_OSD_H
