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
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_OSD_H
#define CEPH_OSD_H

#include <shared_mutex>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include <boost/tuple/tuple.hpp>

#include "OSDVol.h"

#include "msg/Dispatcher.h"

#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/LogClient.h"
#include "common/AsyncReserver.h"
#include "common/ceph_context.h"

#include "osd/ClassHandler.h"

#include "include/CompatSet.h"

#include "auth/KeyRing.h"
#include "OpRequest.h"

#include "Watch.h"
#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/sharedptr_registry.hpp"
#include "common/PrioritizedQueue.h"
#include "include/lru.h"

#include "StateObserver.h"

#define CEPH_OSD_PROTOCOL    10 /* cluster internal */


class Messenger;
class Message;
class MonClient;
class OSDMap;
class MLog;
class MClass;
class ObjectStore;
class Transaction;

class Watch;
class Notification;

class AuthAuthorizeHandlerRegistry;

namespace cohort {
class OpQueue;
}

class OSD;
class OSDService {
public:
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::shared_lock<std::shared_timed_mutex> shared_map_lock;
  typedef std::unique_lock<std::shared_timed_mutex> unique_map_lock;
  OSD *osd;
  LRU lru;
  CephContext *cct;
  const int whoami;
  ObjectStore *&store;
  CollectionHandle meta_col;
  LogClient &clog;
  oid_t infos_oid;
  ObjectHandle infos_oh;

private:
  Messenger *&cluster_messenger;
  Messenger *&client_messenger;
  Messenger *&client_xio_messenger;

public:
  MonClient   *&monc;
  ClassHandler	*&class_handler;

  // -- superblock --
  // pre-publish orders before publish
  std::mutex publish_lock, pre_publish_lock;
  OSDSuperblock superblock;
  OSDSuperblock get_superblock() {
    lock_guard l(publish_lock);
    return superblock;
  }
  void publish_superblock(const OSDSuperblock &block) {
    lock_guard l(publish_lock);
    superblock = block;
  }

  int get_nodeid() const { return whoami; }

  OSDMapRef osdmap;
  OSDMapRef get_osdmap() {
    lock_guard l(publish_lock);
    return osdmap;
  }
  void publish_map(OSDMapRef map) {
    lock_guard l(publish_lock);
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
    lock_guard l(pre_publish_lock);
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
    Messenger *messenger = con->get_messenger();
    messenger->send_message(m, con);
  }
  void send_message_osd_client(Message *m, const ConnectionRef& con) {
    Messenger *messenger = con->get_messenger();
    messenger->send_message(m, con.get());
  }
  entity_name_t get_cluster_msgr_name() {
    return cluster_messenger->get_myname();
  }

  void reply_op_error(OpRequest* op, int err);
  void reply_op_error(OpRequest* op, int err, eversion_t v, version_t uv);

  // -- Watch --
  std::mutex watch_lock;
  cohort::Timer<ceph::mono_clock> watch_timer;
  uint64_t next_notif_id;
  uint64_t get_next_id(epoch_t cur_epoch) {
    lock_guard l(watch_lock);
    return (((uint64_t)cur_epoch) << 32) | ((uint64_t)(next_notif_id++));
  }

  // -- tids --
  // for ops i issue
  std::atomic<ceph_tid_t> last_tid;
  ceph_tid_t get_tid() {
    return ++last_tid;
  }

  // osd map cache (past osd maps)
  std::mutex map_cache_lock;
  SharedLRU<epoch_t, const OSDMap> map_cache;
  SimpleLRU<epoch_t, bufferlist> map_bl_cache;
  SimpleLRU<epoch_t, bufferlist> map_bl_inc_cache;

  OSDMapRef try_get_map(epoch_t e);
  OSDMapRef get_map(epoch_t e) {
    OSDMapRef ret(try_get_map(e));
    assert(ret);
    return ret;
  }
  OSDMapRef add_map(OSDMap *o) {
    lock_guard l(map_cache_lock);
    return _add_map(o);
  }
  OSDMapRef _add_map(OSDMap *o);

  void add_map_bl(epoch_t e, bufferlist& bl) {
    lock_guard l(map_cache_lock);
    return _add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl);
  void _add_map_bl(epoch_t e, bufferlist& bl);
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    lock_guard l(map_cache_lock);
    return _get_map_bl(e, bl);
  }
  bool _get_map_bl(epoch_t e, bufferlist& bl);

  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    lock_guard l(map_cache_lock);
    return _add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl);
  void _add_map_inc_bl(epoch_t e, bufferlist& bl);
  bool get_inc_map_bl(epoch_t e, bufferlist& bl);

  void clear_map_bl_cache_pins(epoch_t e);

  void need_heartbeat_peer_update();

  void shutdown();

  // -- OSD Full Status --
  std::mutex full_status_lock;
  enum s_names { NONE, NEAR, FULL } cur_state;
  ceph::real_time last_msg;
  double cur_ratio;
  float get_full_ratio();
  float get_nearfull_ratio();
  void check_nearfull_warning(const osd_stat_t &stat);
  bool check_failsafe_full();


  // -- stopping --
  std::mutex is_stopping_lock;
  std::condition_variable is_stopping_cond;
  enum {
    NOT_STOPPING,
    PREPARING_TO_STOP,
    STOPPING } state;
  bool is_stopping() {
    lock_guard l(is_stopping_lock);
    return state == STOPPING;
  }
  bool is_preparing_to_stop() {
    lock_guard l(is_stopping_lock);
    return state == PREPARING_TO_STOP;
  }
  bool prepare_to_stop();
  void got_stop_ack();

  OSDService(OSD *osd);
  ~OSDService();
};

struct C_OSD_SendMessageOnConn: public Context {
  OSDService *osd;
  Message *reply;
  ConnectionRef conn;
  C_OSD_SendMessageOnConn(
    OSDService *osd,
    Message *reply,
    ConnectionRef conn) : osd(osd), reply(reply), conn(conn) {}
  void finish(int) {
    osd->send_message_osd_cluster(reply, conn.get());
  }
};

class OSD : public Dispatcher,
	    public md_config_obs_t,
	    public OSDStateNotifier {
  /** OSD **/
public:
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  // config observer bits
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);

protected:
  std::mutex osd_lock; // global lock
  cohort::Timer<ceph::mono_clock> tick_timer; // safe timer (osd_lock)

  AuthAuthorizeHandlerRegistry *authorize_handler_cluster_registry;
  AuthAuthorizeHandlerRegistry *authorize_handler_service_registry;

  Messenger   *cluster_messenger;
  Messenger   *client_messenger;
  Messenger*  client_xio_messenger;
  MonClient   *monc; // check the "monc helpers" list before accessing directly
  ObjectStore *store;

  static thread_local OSDVol::VolCache tls_vol_cache;

  LogClient clog;

  int whoami;
  std::string dev_path, journal_path;

  std::condition_variable dispatch_cond;
  int dispatch_running;

  void tick();
  void dispatch_op(OpRequestRef op);

  void check_osdmap_features(ObjectStore *store);

  // asok
  friend class OSDSocketHook;
  class OSDSocketHook *asok_hook;
  bool asok_command(string command, cmdmap_t& cmdmap, string format, ostream& ss);

public:
  ClassHandler	*class_handler;
  int get_nodeid() { return whoami; }

  static oid_t get_osdmap_pobject_name(epoch_t epoch) {
    char foo[20];
    snprintf(foo, sizeof(foo), "osdmap.%d", epoch);
    return oid_t(foo);
  }
  static oid_t get_inc_osdmap_pobject_name(epoch_t epoch) {
    char foo[20];
    snprintf(foo, sizeof(foo), "inc_osdmap.%d", epoch);
    return oid_t(foo);
  }

  static oid_t make_vol_biginfo_oid(const boost::uuids::uuid& vol) {
    stringstream ss;
    ss << "volinfo_" << vol;
    string s;
    getline(ss, s);
    return oid_t(s.c_str());
  }

  static oid_t make_infos_oid() {
    oid_t oid("infos");
    return oid;
  }

  static void recursive_remove_collection(ObjectStore *store,
					  coll_t col);

  /**
   * get_osd_initial_compat_set()
   *
   * Get the initial feature set for this OSD.	Features
   * here are automatically upgraded.
   *
   * Return value: Initial osd CompatSet
   */
  static CompatSet get_osd_initial_compat_set();

  /**
   * get_osd_compat_set()
   *
   * Get all features supported by this OSD
   *
   * Return value: CompatSet of all supported features
   */
  static CompatSet get_osd_compat_set();


private:
  // -- superblock --
  OSDSuperblock superblock;

  void write_superblock(CollectionHandle meta);
  void write_superblock(CollectionHandle meta, Transaction& t);
  int read_superblock(CollectionHandle meta);

  CompatSet osd_compat;

  // -- state --
public:
  static const int STATE_INITIALIZING = 1;
  static const int STATE_BOOTING = 2;
  static const int STATE_ACTIVE = 3;
  static const int STATE_STOPPING = 4;
  static const int STATE_WAITING_FOR_HEALTHY = 5;

  static const char *get_state_name(int s) {
    switch (s) {
    case STATE_INITIALIZING: return "initializing";
    case STATE_BOOTING: return "booting";
    case STATE_ACTIVE: return "active";
    case STATE_STOPPING: return "stopping";
    case STATE_WAITING_FOR_HEALTHY: return "waiting_for_healthy";
    default: return "???";
    }
  }

private:
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

private:
  ThreadPool disk_tp;
  ThreadPool command_tp;

  // -- sessions --
public:
  struct Session : public RefCountedObject {
    EntityName entity_name;
    int64_t auid;
    epoch_t last_sent_epoch;
    ConnectionRef con;
    WatchConState wstate;

    Session() : auid(-1), last_sent_epoch(0), con(0) {}
  };

private:
  /**
   *  @defgroup monc helpers
   *
   *  Right now we only have the one
   */

  /**
   * Ask the Monitors for a sequence of OSDMaps.
   *
   * @param epoch The epoch to start with when replying
   * @param force_request True if this request forces a new subscription to
   * the monitors; false if an outstanding request that encompasses it is
   * sufficient.
   */
  void osdmap_subscribe(version_t epoch, bool force_request);
  /** @} monc helpers */

  // -- heartbeat --
  /// information about a heartbeat peer
  struct HeartbeatInfo {
    int peer;		///< peer
    ConnectionRef con_front;   ///< peer connection (front)
    ConnectionRef con_back;    ///< peer connection (back)
    ceph::real_time first_tx;	///< time we sent our first ping request
    ceph::real_time last_tx;	///< last time we sent a ping request
    ceph::real_time last_rx_front;  ///< last time we got a ping reply
				    ///  on the front side
    ceph::real_time last_rx_back;   ///< last time we got a ping reply
				    ///  on the back side
    epoch_t epoch;	///< most recent epoch we wanted this peer

    bool is_unhealthy(ceph::real_time cutoff) {
      return
	! ((last_rx_front > cutoff ||
	    (last_rx_front == ceph::real_time::min() &&
	     (last_tx == ceph::real_time::min() ||
	      first_tx > cutoff))) &&
	   (last_rx_back > cutoff ||
	    (last_rx_back == ceph::real_time::min() &&
	     (last_tx == ceph::real_time::min() ||
	      first_tx > cutoff))));
    }
    bool is_healthy(ceph::real_time cutoff) const {
      return last_rx_front > cutoff && last_rx_back > cutoff;
    }

  };
  /// state attached to outgoing heartbeat connections
  struct HeartbeatSession : public RefCountedObject {
    int peer;
    HeartbeatSession(int p) : peer(p) {}
  };
  std::mutex heartbeat_lock;
  map<int, int> debug_heartbeat_drops_remaining;
  std::condition_variable heartbeat_cond;
  bool heartbeat_stop;
  bool heartbeat_need_update;	///< true if we need to refresh our heartbeat peers
  epoch_t heartbeat_epoch;	///< last epoch we updated our heartbeat peers
  map<int,HeartbeatInfo> heartbeat_peers;  ///< map of osd id to HeartbeatInfo
  ceph::real_time last_mon_heartbeat;
  Messenger *hbclient_messenger;
  Messenger *hb_front_server_messenger;
  Messenger *hb_back_server_messenger;
  ceph::mono_time last_heartbeat_resample;   ///< last time we chose random peers in waiting-for-healthy state

  bool heartbeat_reset(Connection *con);
  void heartbeat();
  void heartbeat_check();
  void heartbeat_entry();
  void need_heartbeat_peer_update();

  void heartbeat_kick() {
    unique_lock l(heartbeat_lock);
    heartbeat_cond.notify_all();
  }

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
  bool require_same_or_newer_map(OpRequest* op, epoch_t epoch);

  struct HeartbeatDispatcher : public Dispatcher {
    OSD *osd;
    HeartbeatDispatcher(OSD *o) : Dispatcher(o->cct), osd(o) {}
    bool ms_dispatch(Message *m) {
      return osd->heartbeat_dispatch(m);
    };
    bool ms_handle_reset(Connection *con) {
      return osd->heartbeat_reset(con);
    }
    void ms_handle_remote_reset(Connection *con) {}
    bool ms_verify_authorizer(Connection *con, int peer_type,
			      int protocol,
			      bufferlist& authorizer_data,
			      bufferlist& authorizer_reply,
			      bool& isvalid,
			      CryptoKey& session_key) {
      isvalid = true;
      return true;
    }
  } heartbeat_dispatcher;

private:
  // -- op queue --
  std::unique_ptr<cohort::OpQueue> multi_wq;

  /* multi_wq dequeue function */
  static void static_dequeue_op(OSD* osd, OpRequest* op);
  void dequeue_op(OpRequest* op);

  /* multi_wq thread exit hook (clear tls, etc) */
  static void static_wq_thread_exit(OSD* osd);

  friend class OSDVol;

protected:

  // -- osd map --
  OSDMapRef osdmap;
  OSDMapRef get_osdmap() {
    return osdmap;
  }
  ceph::mono_time had_map_since;
  std::shared_timed_mutex map_lock;
  typedef std::unique_lock<std::shared_timed_mutex> unique_map_lock;
  typedef std::shared_lock<std::shared_timed_mutex> shared_map_lock;

  OpRequest::Queue waiting_for_osdmap;

  std::mutex peer_map_epoch_lock;
  map<int, epoch_t> peer_map_epoch;

  epoch_t get_peer_epoch(int p);
  epoch_t note_peer_epoch(int p, epoch_t e);
  void forget_peer_epoch(int p, epoch_t e);

  bool _share_map_incoming(entity_name_t name, Connection *con, epoch_t epoch,
			   Session *session = 0);
  void _share_map_outgoing(int peer, Connection *con,
			   OSDMapRef map = OSDMapRef());

  void wait_for_new_map(OpRequest* op);
  void handle_osd_map(class MOSDMap *m);
  void note_down_osd(int osd);
  void note_up_osd(int osd);

  void advance_vol(epoch_t advance_to, OSDVolRef& vol);
  void advance_map(Transaction& t, C_Contexts *tfin);
  void consume_map();
  void activate_map();

  // osd map cache (past osd maps)
  OSDMapRef get_map(epoch_t e) {
    return service.get_map(e);
  }
  OSDMapRef add_map(OSDMap *o) {
    return service.add_map(o);
  }
  void add_map_bl(epoch_t e, bufferlist& bl) {
    return service.add_map_bl(e, bl);
  }
  void pin_map_bl(epoch_t e, bufferlist &bl) {
    return service.pin_map_bl(e, bl);
  }
  bool get_map_bl(epoch_t e, bufferlist& bl) {
    return service.get_map_bl(e, bl);
  }
  void add_map_inc_bl(epoch_t e, bufferlist& bl) {
    return service.add_map_inc_bl(e, bl);
  }
  void pin_map_inc_bl(epoch_t e, bufferlist &bl) {
    return service.pin_map_inc_bl(e, bl);
  }
  bool get_inc_map_bl(epoch_t e, bufferlist& bl) {
    return service.get_inc_map_bl(e, bl);
  }

  MOSDMap *build_incremental_map_msg(epoch_t from, epoch_t to);
  void send_incremental_map(epoch_t since, Connection *con);
  void send_map(MOSDMap *m, Connection *con);

protected:
  // -- placement groups --
  std::mutex vol_lock; // XXXX TODO: replace w/lanes */
  std::map<boost::uuids::uuid, OSDVolRef> vol_map;

  bool _have_vol(const boost::uuids::uuid& volume);
  OSDVolRef _lookup_vol(const boost::uuids::uuid& volid);
  OSDVolRef _load_vol(const boost::uuids::uuid& volid);
  void trim_vols(void);
  OSDVolRef _lookup_lock_vol(const boost::uuids::uuid& volid,
			     unique_lock& vl);

  // -- boot --
  void start_boot();
  void _maybe_boot(epoch_t oldest, epoch_t newest);
  void _send_boot();
  void _collect_metadata(map<string,string> *pmeta);

  void start_waiting_for_healthy();
  bool _is_healthy();

  friend struct C_OSD_GetVersion;

  // -- alive --
  epoch_t up_thru_wanted;
  epoch_t up_thru_pending;

  void queue_want_up_thru(epoch_t want);
  void send_alive();
  void dispatch_context(OSDVol *vol, OSDMapRef curmap,
			ThreadPool::TPHandle *handle);

  // -- failures --
  map<int,ceph::real_time> failure_queue;
  map<int,entity_inst_t> failure_pending;


  void send_failures();
  void send_still_alive(epoch_t epoch, const entity_inst_t &i);

  ceph_tid_t get_tid() {
    return service.get_tid();
  }

 private:
  bool ms_dispatch(Message *m);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

 public:
  /* internal and external can point to the same messenger, they will still
   * be cleaned up properly*/
  OSD(CephContext *cct_,
      ObjectStore *store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *xio_exteral,
      Messenger *hb_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      MonClient *mc, const std::string &dev, const std::string &jdev);
  ~OSD();

  // static bits
  static int find_osd_dev(char *result, int whoami);
  static int mkfs(CephContext *cct, ObjectStore *store,
		  const string& dev,
		  const boost::uuids::uuid& fsid, int whoami);
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
  static int write_meta(ObjectStore *store, const boost::uuids::uuid& cluster_fsid,
			const boost::uuids::uuid& osd_fsid, int whoami);
public:
  static int peek_meta(ObjectStore *store, string& magic,
		       boost::uuids::uuid& cluster_fsid,
		       boost::uuids::uuid& osd_fsid, int& whoami);


  // startup/shutdown
  int pre_init();
  int init();
  void final_init();

  void suicide(int exitcode);
  int shutdown();

  void handle_signal(int signum);

  void handle_osd_ping(class MOSDPing *m);
  void handle_op(OpRequest* op, unique_lock& osd_lk);

  template <typename T, int MSGTYPE>
  void handle_replica_op(OpRequestRef op);

  /// check if we can throw out op from a disconnected client
  static inline bool op_is_discardable(MOSDOp *op) {
    // drop client request if they are not connected and can't get the
    // reply anyway.  unless this is a replayed op, in which case we
    // want to do what we can to apply it.
    if (!op->get_connection()->is_connected() &&
	op->get_version().version == 0) {
      return true;
    }
    return false;
  }

  /// check if op should be (re)queued for processing
public:
  void force_remount();

  int init_op_flags(OpRequest* op);

  OSDService service;
  friend class OSDService;
};

//compatibility of the executable
extern const CompatSet::Feature ceph_osd_feature_compat[];
extern const CompatSet::Feature ceph_osd_feature_ro_compat[];
extern const CompatSet::Feature ceph_osd_feature_incompat[];

#endif
