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

#ifndef CEPH_MONCLIENT_H
#define CEPH_MONCLIENT_H

#include <memory>

#include <boost/intrusive/set.hpp>

#include "common/Finisher.h"
#include "common/SimpleRNG.h"
#include "common/Timer.h"

#include "auth/AuthClientHandler.h"
#include "auth/RotatingKeyRing.h"

#include "msg/Dispatcher.h"
#include "msg/MessageFactory.h"
#include "msg/Messenger.h"

#include "osd/osd_types.h"

#include "MonMap.h"


class MonMap;
class MMonMap;
class MMonGetVersion;
class MMonGetVersionReply;
struct MMonSubscribeAck;
class MMonCommandAck;
class MCommandReply;
struct MAuthReply;
class MAuthRotating;
class MPing;
class LogClient;
class AuthSupported;
class AuthAuthorizeHandlerRegistry;
class AuthMethodList;

enum MonClientState {
  MC_STATE_NONE,
  MC_STATE_NEGOTIATING,
  MC_STATE_AUTHENTICATING,
  MC_STATE_HAVE_SESSION,
};

struct MonClientPinger : public Dispatcher {

  std::mutex lock;
  std::condition_variable ping_recvd_cond;
  string *result;
  bool done;

  MonClientPinger(CephContext *cct_, string *res_) :
    Dispatcher(cct_),
    result(res_),
    done(false)
  { }

  int wait_for_reply() {
    return wait_for_reply(cct->_conf->client_mount_timeout);
  }

  int wait_for_reply(ceph::timespan t) {
    std::unique_lock<std::mutex> l(lock);
    ceph::mono_time until = ceph::mono_clock::now() + t;
    done = false;

    if (!ping_recvd_cond.wait_until(l, until, [&](){ return done; })) {
      return -ETIMEDOUT;
    }
    return 0;
  }

  bool ms_dispatch(Message *m) {
    std::lock_guard<std::mutex> l(lock);
    if (m->get_type() != CEPH_MSG_PING)
      return false;

    bufferlist &payload = m->get_payload();
    if (result && payload.length() > 0) {
      bufferlist::iterator p = payload.begin();
      ::decode(*result, p);
    }
    done = true;
    ping_recvd_cond.notify_all();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) {
    std::lock_guard<std::mutex> l(lock);
    done = true;
    ping_recvd_cond.notify_all();
    return true;
  }
  void ms_handle_remote_reset(Connection *con) {}
};

class MonClient : public Dispatcher {
public:
  typedef cohort::function<void(std::error_code, version_t newest,
				version_t oldest)> version_cb;
  typedef cohort::function<void(std::error_code, const string&,
				bufferlist&)> monc_cb;

  class waiter {
  protected:
    std::mutex lock;
    std::condition_variable cond;
    bool done;
    std::error_code err;
    std::string s;
    bufferlist bl;

  public:

    waiter() : done(false) { }
    waiter(const waiter&) = delete;
    waiter(waiter&&) = delete;

    waiter& operator=(const waiter&) = delete;
    waiter& operator=(waiter&&) = delete;

    operator monc_cb() {
      return std::ref(*this);
    }

    std::tuple<std::string, bufferlist> wait() {
      std::unique_lock<std::mutex> l(lock);
      cond.wait(l, [this](){ return done; });
      if (err)
	throw std::system_error(err, std::move(s));
      return std::make_tuple(std::move(s), std::move(bl));
    }

    bool complete() {
      std::unique_lock<std::mutex> l(lock);
      return done;
    }

    void operator()(std::error_code _err, const std::string& _s,
		    bufferlist& _bl) {
      std::unique_lock<std::mutex> l(lock);
      if (done)
	return;

      done = true;
      err = _err;
      s = _s;
      bl.claim_append(_bl);
      cond.notify_one();
    }

    void reset() {
      done = false;
      bl.clear();
    }
  };

  MonMap monmap;

  struct Factory : public MessageFactory {
    Message* create(int type);
  };
  Factory factory;
private:
  MonClientState state;

  Messenger *messenger;

  string cur_mon;
  ConnectionRef cur_con;

  SimpleRNG rng;

  EntityName entity_name;

  entity_addr_t my_addr;

  std::mutex monc_lock;
  cohort::Timer<ceph::mono_clock> timer;
  Finisher finisher;

  // Added to support session signatures.  PLR

  AuthAuthorizeHandlerRegistry *authorize_handler_registry;

  bool initialized;
  bool no_keyring_disabled_cephx;

  LogClient *log_client;
  bool more_log_pending;

  void send_log();

  AuthMethodList *auth_supported;

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  void handle_monmap(MMonMap *m);

  void handle_auth(MAuthReply *m,
		   std::unique_lock<std::mutex>& l);

  // monitor session
  bool hunting;

  void tick();
  void schedule_tick();

  std::condition_variable auth_cond;

  void handle_auth_rotating_response(MAuthRotating *m);
  // monclient
  bool want_monmap;

  uint32_t want_keys;

  uint64_t global_id;

  // authenticate
private:
  std::condition_variable map_cond;
  int authenticate_err;

  list<Message*> waiting_for_session;
  bool had_a_connection;
  uint64_t reopen_interval_multiplier;

  string _pick_random_mon();
  void _finish_hunting();
  void _reopen_session(int rank, string name);
  void _reopen_session() {
    _reopen_session(-1, string());
  }
  void _send_mon_message(Message *m, bool force=false);

public:
  void set_entity_name(EntityName name) { entity_name = name; }

  int _check_auth_tickets();
  int _check_auth_rotating();
  int wait_auth_rotating(ceph::timespan timeout);

  int authenticate(ceph::timespan timeout = ceph::timespan(0));

  // mon subscriptions
private:
  map<string,ceph_mon_subscribe_item> sub_have;	 // my subs, and current versions
  ceph::mono_time sub_renew_sent, sub_renew_after;

  void _renew_subs();
  void handle_subscribe_ack(MMonSubscribeAck* m);

  bool _sub_want(string what, version_t start, unsigned flags) {
    map<string,ceph_mon_subscribe_item>::const_iterator i = sub_have.find(what);
    if (i != sub_have.cend() &&
	i->second.start == start &&
	i->second.flags == flags)
      return false;
    sub_have.emplace(what, ceph_mon_subscribe_item(start, flags));
    return true;
  }
  void _sub_got(string what, version_t got) {
    if (sub_have.count(what)) {
      if (sub_have[what].flags & CEPH_SUBSCRIBE_ONETIME)
	sub_have.erase(what);
      else
	sub_have[what].start = got + 1;
    }
  }
  void _sub_unwant(string what) {
    sub_have.erase(what);
  }

  // auth tickets
public:
  AuthClientHandler *auth;
public:
  void renew_subs() {
    std::lock_guard<std::mutex> l(monc_lock);
    _renew_subs();
  }
  bool sub_want(string what, version_t start, unsigned flags) {
    std::lock_guard<std::mutex> l(monc_lock);
    return _sub_want(what, start, flags);
  }
  void sub_got(string what, version_t have) {
    std::lock_guard<std::mutex> l(monc_lock);
    _sub_got(what, have);
  }
  void sub_unwant(string what) {
    std::lock_guard<std::mutex> l(monc_lock);
    _sub_unwant(what);
  }
  /**
   * Increase the requested subscription start point. If you do increase
   * the value, apply the passed-in flags as well; otherwise do nothing.
   */
  bool sub_want_increment(string what, version_t start, unsigned flags) {
    std::lock_guard<std::mutex> l(monc_lock);
    map<string,ceph_mon_subscribe_item>::iterator i =
	    sub_have.find(what);
    if (i == sub_have.end() || i->second.start < start) {
      ceph_mon_subscribe_item& item = sub_have[what];
      item.start = start;
      item.flags = flags;
      return true;
    }
    return false;
  }

  KeyRing *keyring;
  RotatingKeyRing *rotating_secrets;

 public:
  MonClient(CephContext *cct_);
  ~MonClient();

  int init();
  void shutdown();

  void set_log_client(LogClient *clog) {
    log_client = clog;
  }

  int build_initial_monmap();
  int get_monmap();
  int get_monmap_privately();
  /**
   * Ping monitor with ID @p mon_id and record the resulting
   * reply in @p result_reply.
   *
   * @param[in]	 mon_id Target monitor's ID
   * @param[out] Resulting reply from mon.ID, if param != NULL
   * @returns	 0 in case of success; < 0 in case of error,
   *		 -ETIMEDOUT if monitor didn't reply before timeout
   *		 expired (default: conf->client_mount_timeout).
   */
  int ping_monitor(const string &mon_id, string *result_reply);

  void send_mon_message(Message *m) {
    std::lock_guard<std::mutex> l(monc_lock);
    _send_mon_message(m);
  }
  entity_addr_t get_my_addr() const {
    return my_addr;
  }

  const boost::uuids::uuid& get_fsid() {
    return monmap.fsid;
  }

  entity_addr_t get_mon_addr(unsigned i) {
    std::lock_guard<std::mutex> l(monc_lock);
    if (i < monmap.size())
      return monmap.get_addr(i);
    return entity_addr_t();
  }
  entity_inst_t get_mon_inst(unsigned i) {
    std::lock_guard<std::mutex> l(monc_lock);
    if (i < monmap.size())
      return monmap.get_inst(i);
    return entity_inst_t();
  }
  int get_num_mon() {
    std::lock_guard<std::mutex> l(monc_lock);
    return monmap.size();
  }

  uint64_t get_global_id() const {
    return global_id;
  }

  void set_messenger(Messenger *m) { messenger = m; }

  void send_auth_message(Message *m) {
    _send_mon_message(m, true);
  }

  void set_want_keys(uint32_t want) {
    want_keys = want;
    if (auth)
      auth->set_want_keys(want | CEPH_ENTITY_TYPE_MON);
  }

  void add_want_keys(uint32_t want) {
    want_keys |= want;
    if (auth)
      auth->add_want_keys(want);
  }

  // admin commands
private:
  uint64_t last_mon_command_tid;
  struct MonCommand
    : public boost::intrusive::set_base_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>> {
    uint64_t tid;
    string target_name;
    int target_rank;
    vector<string> cmd;
    bufferlist inbl;
    monc_cb onfinish;
    uint64_t ontimeout;

    MonCommand(uint64_t t)
      : tid(t), target_rank(-1), ontimeout(0) {}
  };
  struct MoncCompare {
    bool operator()(const MonCommand& mc1, const MonCommand& mc2) const {
      return mc1.tid < mc2.tid;
    }
  };
  boost::intrusive::set<MonCommand,
			boost::intrusive::constant_time_size<false>,
			boost::intrusive::compare<MoncCompare>> mon_commands;

  void _send_command(MonCommand& r);
  void _resend_mon_commands();
  void _cancel_mon_command(uint64_t tid,
			   std::error_code r = std::make_error_code(
			     std::errc::operation_canceled));
  void _finish_command(MonCommand& r, std::error_code ret,
		       const string& rs, bufferlist& bl);
  void handle_mon_command_ack(MMonCommandAck *ack);

public:
  void start_mon_command(const vector<string>& cmd, const bufferlist& inbl,
			 monc_cb&& onfinish);
  void start_mon_command(int mon_rank,
			 const vector<string>& cmd, const bufferlist& inbl,
			 monc_cb&& onfinish);
  void start_mon_command(const string &mon_name, ///< mon name, with mon. prefix
			 const vector<string>& cmd, const bufferlist& inbl,
			 monc_cb&& onfinish);

  // version requests
public:
  /**
   * get latest known version(s) of cluster map
   *
   * @param map string name of map (e.g., 'osdmap')
   * @param onfinish callback that will be triggered on completion
   * @return (via callback) std::errc::resource_unavaiable_try_again
   *         if we need to resubmit our request
   */
  void get_version(string map, version_cb&& onfinish);

private:
  struct version_req_d {
    version_cb cb;
    version_req_d(version_cb&& _cb) : cb(_cb) {}
  };

  map<ceph_tid_t, version_req_d*> version_requests;
  ceph_tid_t version_req_id;
  void handle_get_version_reply(MMonGetVersionReply* m);


  MonClient(const MonClient &rhs);
  MonClient& operator=(const MonClient &rhs);
};

#endif
