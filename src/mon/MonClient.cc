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

#include "msg/SimpleMessenger.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MMonMap.h"
#include "messages/MAuth.h"
#include "messages/MLogAck.h"
#include "messages/MAuthReply.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MPing.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "common/ConfUtils.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "common/LogClient.h"

#include "MonClient.h"
#include "MonMap.h"

#include "auth/Auth.h"
#include "auth/KeyRing.h"
#include "auth/AuthMethodList.h"

#include "include/str_list.h"
#include "include/addr_parsing.h"

#include "common/config.h"


#define dout_subsys ceph_subsys_monc
#undef dout_prefix
#define dout_prefix *_dout << "monclient" << (hunting ? "(hunting)":"") << ": "

MonClient::MonClient(CephContext *cct_) :
  Dispatcher(cct_),
  state(MC_STATE_NONE),
  messenger(NULL),
  cur_con(NULL),
  rng(getpid()),
  finisher(cct_),
  authorize_handler_registry(NULL),
  initialized(false),
  no_keyring_disabled_cephx(false),
  log_client(NULL),
  more_log_pending(false),
  auth_supported(NULL),
  hunting(true),
  want_monmap(true),
  want_keys(0), global_id(0),
  authenticate_err(0),
  had_a_connection(false),
  reopen_interval_multiplier(1),
  auth(NULL),
  keyring(NULL),
  rotating_secrets(NULL),
  last_mon_command_tid(0),
  version_req_id(0)
{
}

MonClient::~MonClient()
{
  delete auth_supported;
  delete auth;
  delete keyring;
  delete rotating_secrets;
}

int MonClient::build_initial_monmap()
{
  ldout(cct, 10) << "build_initial_monmap" << dendl;
  return monmap.build_initial(cct, cerr);
}

int MonClient::get_monmap()
{
  ldout(cct, 10) << "get_monmap" << dendl;
  std::unique_lock<std::mutex> l(monc_lock);

  _sub_want("monmap", 0, 0);
  if (cur_mon.empty())
    _reopen_session();

  map_cond.wait(l, [&](){ return !want_monmap; });

  ldout(cct, 10) << "get_monmap done" << dendl;
  return 0;
}

int MonClient::get_monmap_privately()
{
  ldout(cct, 10) << "get_monmap_privately" << dendl;
  std::unique_lock<std::mutex> l(monc_lock);

  bool temp_msgr = false;
  SimpleMessenger* smessenger = NULL;
  if (!messenger) {
    messenger = smessenger = new SimpleMessenger(cct,
						 entity_name_t::CLIENT(-1),
						 "temp_mon_client", getpid(),
						 &factory);
    messenger->add_dispatcher_head(this);
    smessenger->start();
    temp_msgr = true;
  }

  int attempt = 10;

  ldout(cct, 10) << "have " << monmap.epoch << " fsid " << monmap.fsid << dendl;

  while (monmap.fsid.is_nil()) {
    cur_mon = _pick_random_mon();

    /* XXX note, MonClient doesn't have multi-messenger problems, because
     * a Messenger of desired type is already selected */
    cur_con = messenger->get_connection(monmap.get_inst(cur_mon));
    ldout(cct, 10) << "querying mon." << cur_mon << " " << cur_con->get_peer_addr() << dendl;
    messenger->send_message(new MMonGetMap, cur_con);

    if (--attempt == 0)
      break;

    map_cond.wait_for(l, cct->_conf->mon_client_hunt_interval);

    if (monmap.fsid.is_nil()) {
      messenger->mark_down(cur_con);  // nope, clean that connection up
    }
  }

  if (temp_msgr) {
    messenger->mark_down(cur_con);
    cur_con.reset(NULL);
    l.unlock();
    messenger->shutdown();
    if (smessenger)
      smessenger->wait();
    delete messenger;
    messenger = 0;
    l.lock();
  }

  hunting = true;  // reset this to true!
  cur_mon.clear();

  cur_con.reset(NULL);

  if (!monmap.fsid.is_nil())
    return 0;
  return -1;
}


/**
 * Ping the monitor with id @p mon_id and set the resulting reply in
 * the provided @p result_reply, if this last parameter is not NULL.
 *
 * So that we don't rely on the MonClient's default messenger, set up
 * during connect(), we create our own messenger to comunicate with the
 * specified monitor.  This is advantageous in the following ways:
 *
 * - Isolate the ping procedure from the rest of the MonClient's operations,
 *   allowing us to not acquire or manage the big monc_lock, thus not
 *   having to block waiting for some other operation to finish before we
 *   can proceed.
 *   * for instance, we can ping mon.FOO even if we are currently hunting
 *     or blocked waiting for auth to complete with mon.BAR.
 *
 * - Ping a monitor prior to establishing a connection (using connect())
 *   and properly establish the MonClient's messenger.	This frees us
 *   from dealing with the complex foo that happens in connect().
 *
 * We also don't rely on MonClient as a dispatcher for this messenger,
 * unlike what happens with the MonClient's default messenger.	This allows
 * us to sandbox the whole ping, having it much as a separate entity in
 * the MonClient class, considerably simplifying the handling and dispatching
 * of messages without needing to consider monc_lock.
 *
 * Current drawback is that we will establish a messenger for each ping
 * we want to issue, instead of keeping a single messenger instance that
 * would be used for all pings.
 */
int MonClient::ping_monitor(const string &mon_id, string *result_reply)
{
  ldout(cct, 10) << __func__ << dendl;

  if (mon_id.empty()) {
    ldout(cct, 10) << __func__ << " specified mon id is empty!" << dendl;
    return -EINVAL;
  } else if (!monmap.contains(mon_id)) {
    ldout(cct, 10) << __func__ << " no such monitor 'mon." << mon_id << "'"
		   << dendl;
    return -ENOENT;
  }

  MonClientPinger *pinger = new MonClientPinger(cct, result_reply);

  Messenger *smsgr = new SimpleMessenger(cct,
					 entity_name_t::CLIENT(-1),
					 "temp_ping_client", getpid(),
					 &factory);
  smsgr->add_dispatcher_head(pinger);
  smsgr->start();

  ConnectionRef con = smsgr->get_connection(monmap.get_inst(mon_id));
  ldout(cct, 10) << __func__ << " ping mon." << mon_id
		 << " " << con->get_peer_addr() << dendl;
  smsgr->send_message(new MPing, con);

  std::unique_lock<std::mutex> pl(pinger->lock);
  int ret = pinger->wait_for_reply(cct->_conf->client_mount_timeout);
  if (ret == 0) {
    ldout(cct,10) << __func__ << " got ping reply" << dendl;
  } else {
    ret = -ret;
  }
  pl.unlock();

  smsgr->mark_down(con);
  smsgr->shutdown();
  smsgr->wait();
  delete smsgr;
  delete pinger;
  return ret;
}

Message* MonClient::Factory::create(int type)
{
  switch (type) {
  case CEPH_MSG_MON_MAP:                return new MMonMap;
  case CEPH_MSG_AUTH_REPLY:             return new MAuthReply;
  case CEPH_MSG_MON_SUBSCRIBE_ACK:      return new MMonSubscribeAck;
  case CEPH_MSG_MON_GET_VERSION_REPLY:  return new MMonGetVersionReply;
  case MSG_MON_COMMAND_ACK:             return new MMonCommandAck;
  case MSG_LOGACK:                      return new MLogAck;
  default:                              return nullptr;
  }
}

bool MonClient::ms_dispatch(Message *m)
{
  if (my_addr == entity_addr_t())
    my_addr = messenger->get_myaddr();

  // we only care about these message types
  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
  case CEPH_MSG_AUTH_REPLY:
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
  case CEPH_MSG_MON_GET_VERSION_REPLY:
  case MSG_MON_COMMAND_ACK:
  case MSG_LOGACK:
    break;
  default:
    return false;
  }

  std::unique_lock<std::mutex> l(monc_lock);

  // ignore any messages outside our current session
  if (m->get_connection() != cur_con) {
    ldout(cct, 10) << "discarding stray monitor message " << *m << dendl;
    m->put();
    return true;
  }

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_monmap(static_cast<MMonMap*>(m));
    break;
  case CEPH_MSG_AUTH_REPLY:
    handle_auth(static_cast<MAuthReply*>(m), l);
    break;
  case CEPH_MSG_MON_SUBSCRIBE_ACK:
    handle_subscribe_ack(static_cast<MMonSubscribeAck*>(m));
    break;
  case CEPH_MSG_MON_GET_VERSION_REPLY:
    handle_get_version_reply(static_cast<MMonGetVersionReply*>(m));
    break;
  case MSG_MON_COMMAND_ACK:
    handle_mon_command_ack(static_cast<MMonCommandAck*>(m));
    break;
  case MSG_LOGACK:
    if (log_client) {
      log_client->handle_log_ack(static_cast<MLogAck*>(m));
      if (more_log_pending) {
	send_log();
      }
    } else {
      m->put();
    }
    break;
  }
  return true;
}

void MonClient::send_log()
{
  if (log_client) {
    Message *lm = log_client->get_mon_log_message();
    if (lm)
      _send_mon_message(lm);
    more_log_pending = log_client->are_pending();
  }
}

void MonClient::handle_monmap(MMonMap *m)
{
  ldout(cct, 10) << "handle_monmap " << *m << dendl;
  bufferlist::iterator p = m->monmapbl.begin();
  ::decode(monmap, p);
  uint64_t epoch = monmap.get_epoch();

  assert(!cur_mon.empty());
  ldout(cct, 10) << " got monmap " << monmap.epoch
		 << ", mon." << cur_mon << " is now rank "
		 << monmap.get_rank(cur_mon)
		 << dendl;
  ldout(cct, 10) << "dump:\n";
  monmap.print(*_dout);
  *_dout << dendl;

  _sub_got("monmap", epoch);

  /* XXXX
   *
   * For the moment, this check always fails when cur_con is an
   * XioConnection, probably because the shifted port isn't advertised
   * in the MonMap.  We could fix this a couple of ways, probably the
   * correct one is publishing Xio endpoints in the MonMap.  However,
   * this requires running down several loose ends, so deferring for
   * now.
   */

  if (false /* !monmap.get_addr_name(cur_con->get_peer_addr(), cur_mon) */) {
    ldout(cct, 10) << "mon." << cur_mon << " went away" << dendl;
    _reopen_session();	// can't find the mon we were talking to (above)
  } else {
    _finish_hunting();
  }

  map_cond.notify_all();
  want_monmap = false;

  m->put();
}

// ----------------------

int MonClient::init()
{
  ldout(cct, 10) << "init" << dendl;

  messenger->add_dispatcher_head(this);

  entity_name = cct->_conf->name;

  std::unique_lock<std::mutex> l(monc_lock);

  string method;
    if (cct->_conf->auth_supported.length() != 0)
      method = cct->_conf->auth_supported;
    else if (entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	     entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
	     entity_name.get_type() == CEPH_ENTITY_TYPE_MON)
      method = cct->_conf->auth_cluster_required;
    else
      method = cct->_conf->auth_client_required;
  auth_supported = new AuthMethodList(cct, method);
  ldout(cct, 10) << "auth_supported " << auth_supported->get_supported_set() << " method " << method << dendl;

  int r = 0;
  keyring = new KeyRing; // initializing keyring anyway

  if (auth_supported->is_supported_auth(CEPH_AUTH_CEPHX)) {
    r = keyring->from_ceph_context(cct);
    if (r == -ENOENT) {
      auth_supported->remove_supported_auth(CEPH_AUTH_CEPHX);
      if (auth_supported->get_supported_set().size() > 0) {
	r = 0;
	no_keyring_disabled_cephx = true;
      } else {
	lderr(cct) << "ERROR: missing keyring, cannot use cephx for authentication" << dendl;
      }
    }
  }

  if (r < 0) {
    return r;
  }

  rotating_secrets = new RotatingKeyRing(cct, cct->get_module_type(), keyring);

  initialized = true;

  l.unlock();

  finisher.start();
  schedule_tick();

  return 0;
}

void MonClient::shutdown()
{
  ldout(cct, 10) << __func__ << "shutdown" << dendl;
  std::unique_lock<std::mutex> l(monc_lock);
  while (!version_requests.empty()) {
    version_requests.begin()->second->cb(
      std::make_error_code(std::errc::operation_canceled), 0, 0);
    ldout(cct, 20) << __func__ << " canceling and discarding version request "
		   << version_requests.begin()->second << dendl;
    delete version_requests.begin()->second;
    version_requests.erase(version_requests.begin());
  }

  while (!waiting_for_session.empty()) {
    ldout(cct, 20) << __func__ << " discarding pending message " << *waiting_for_session.front() << dendl;
    waiting_for_session.front()->put();
    waiting_for_session.pop_front();
  }

  l.unlock();

  timer.suspend();
  if (initialized) {
    finisher.stop();
  }

  l.lock();
  cur_con->get_messenger()->mark_down(cur_con);
  cur_con.reset(NULL);

  l.unlock();
}

int MonClient::authenticate(ceph::timespan timeout)
{
  std::unique_lock<std::mutex> l(monc_lock);

  if (state == MC_STATE_HAVE_SESSION) {
    ldout(cct, 5) << "already authenticated" << dendl;;
    return 0;
  }

  _sub_want("monmap", monmap.get_epoch() ? monmap.get_epoch() + 1 : 0, 0);
  if (cur_mon.empty())
    _reopen_session();

  ceph::mono_time until = ceph::mono_clock::now() + timeout;
  while (state != MC_STATE_HAVE_SESSION && !authenticate_err) {
    if (timeout > ceph::timespan(0)) {
      if (auth_cond.wait_until(l, until) == std::cv_status::timeout) {
	authenticate_err = -ETIMEDOUT;
      }
    } else {
      auth_cond.wait(l);
    }
  }

  if (state == MC_STATE_HAVE_SESSION) {
    ldout(cct, 5) << "authenticate success, global_id " << global_id << dendl;
  }

  if (authenticate_err < 0 && no_keyring_disabled_cephx) {
    lderr(cct) << "authenticate NOTE: no keyring found; disabled cephx authentication" << dendl;
  }

  return authenticate_err;
}

void MonClient::handle_auth(MAuthReply *m,
			    std::unique_lock<std::mutex>& l)
{
  ldout(cct, 5) << "handle_auth " << *m << dendl;
  bufferlist::iterator p = m->result_bl.begin();
  if (state == MC_STATE_NEGOTIATING) {
    if (!auth || (int)m->protocol != auth->get_protocol()) {
      delete auth;
      auth = get_auth_client_handler(cct, m->protocol, rotating_secrets);
      if (!auth) {
	ldout(cct, 10) << "no handler for protocol " << m->protocol << dendl;
	if (m->result == -ENOTSUP) {
	  ldout(cct, 10) << "none of our auth protocols are supported by the server"
			 << dendl;
	  authenticate_err = m->result;
	  auth_cond.notify_all();
	}
	m->put();
	return;
      }
      auth->set_want_keys(want_keys);
      auth->init(entity_name);
      auth->set_global_id(global_id);
    } else {
      auth->reset();
    }
    state = MC_STATE_AUTHENTICATING;
  }
  assert(auth);
  if (m->global_id && m->global_id != global_id) {
    global_id = m->global_id;
    auth->set_global_id(global_id);
    ldout(cct, 10) << "my global_id is " << m->global_id << dendl;
  }

  int ret = auth->handle_response(m->result, p);
  m->put();

  if (ret == -EAGAIN) {
    MAuth *ma = new MAuth;
    ma->protocol = auth->get_protocol();
    auth->prepare_build_request();
    ret = auth->build_request(ma->auth_payload);
    _send_mon_message(ma, true);
    return;
  }

  _finish_hunting();

  authenticate_err = ret;
  if (ret == 0) {
    if (state != MC_STATE_HAVE_SESSION) {
      state = MC_STATE_HAVE_SESSION;
      while (!waiting_for_session.empty()) {
	_send_mon_message(waiting_for_session.front());
	waiting_for_session.pop_front();
      }

      _resend_mon_commands();

      if (log_client) {
	log_client->reset_session();
	send_log();
      }
    }

    _check_auth_tickets();
  }
  auth_cond.notify_all();
}


// ---------

void MonClient::_send_mon_message(Message *m, bool force)
{
  assert(!cur_mon.empty());
  if (force || state == MC_STATE_HAVE_SESSION) {
    assert(cur_con);
    ldout(cct, 10) << "_send_mon_message " << *m <<  " to mon." << cur_mon
		   << " at " << cur_con->get_peer_addr() << dendl;
    messenger->send_message(m, cur_con);
  } else {
    waiting_for_session.push_back(m);
  }
}

string MonClient::_pick_random_mon()
{
  assert(monmap.size() > 0);
  if (monmap.size() == 1) {
    return monmap.get_name(0);
  } else {
    int max = monmap.size();
    int o = -1;
    if (!cur_mon.empty()) {
      o = monmap.get_rank(cur_mon);
      if (o >= 0)
	max--;
    }

    int32_t n = rng() % max;
    if (o >= 0 && n >= o)
      n++;
    return monmap.get_name(n);
  }
}

using namespace std::placeholders;

void MonClient::_reopen_session(int rank, string name)
{
  ldout(cct, 10) << "_reopen_session rank " << rank << " name " << name << dendl;

  if (rank < 0 && name.length() == 0) {
    cur_mon = _pick_random_mon();
  } else if (name.length()) {
    cur_mon = name;
  } else {
    cur_mon = monmap.get_name(rank);
  }

  if (cur_con) {
    cur_con->get_messenger()->mark_down(cur_con);
  }
  cur_con = messenger->get_connection(monmap.get_inst(cur_mon));

  ldout(cct, 10) << "picked mon." << cur_mon << " con " << cur_con
		 << " addr " << cur_con->get_peer_addr()
		 << dendl;

  // throw out old queued messages
  while (!waiting_for_session.empty()) {
    waiting_for_session.front()->put();
    waiting_for_session.pop_front();
  }

  // throw out version check requests
  while (!version_requests.empty()) {
    finisher.queue(
      std::bind(
	version_requests.begin()->second->cb,
	std::make_error_code(std::errc::resource_unavailable_try_again),
	0, 0));
    delete version_requests.begin()->second;
    version_requests.erase(version_requests.begin());
  }

  // adjust timeouts if necessary
  if (had_a_connection) {
    reopen_interval_multiplier *= cct->_conf->mon_client_hunt_interval_backoff;
    if (reopen_interval_multiplier >
	  cct->_conf->mon_client_hunt_interval_max_multiple)
      reopen_interval_multiplier =
	  cct->_conf->mon_client_hunt_interval_max_multiple;
  }

  // restart authentication handshake
  state = MC_STATE_NEGOTIATING;
  hunting = true;

  // send an initial keepalive to ensure our timestamp is valid by the
  // time we are in an OPENED state (by sequencing this before
  // authentication).
  messenger->send_keepalive(cur_con.get());

  MAuth *m = new MAuth;
  m->protocol = 0;
  m->monmap_epoch = monmap.get_epoch();
  uint8_t struct_v = 1;
  ::encode(struct_v, m->auth_payload);
  ::encode(auth_supported->get_supported_set(), m->auth_payload);
  ::encode(entity_name, m->auth_payload);
  ::encode(global_id, m->auth_payload);
  _send_mon_message(m, true);

  if (!sub_have.empty())
    _renew_subs();
}


bool MonClient::ms_handle_reset(Connection *con)
{
  std::lock_guard<std::mutex> l(monc_lock);

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    if (cur_mon.empty() || con != cur_con) {
      ldout(cct, 10) << "ms_handle_reset stray mon " << con->get_peer_addr() << dendl;
      return true;
    } else {
      ldout(cct, 10) << "ms_handle_reset current mon " << con->get_peer_addr() << dendl;
      if (hunting)
	return true;

      ldout(cct, 0) << "hunting for new mon" << dendl;
      _reopen_session();
    }
  }
  return false;
}

void MonClient::_finish_hunting()
{
  if (hunting) {
    ldout(cct, 1) << "found mon." << cur_mon << dendl;
    hunting = false;
    had_a_connection = true;
    reopen_interval_multiplier /= 2.0;
    if (reopen_interval_multiplier < 1.0)
      reopen_interval_multiplier = 1.0;
  }
}

void MonClient::tick()
{
  ldout(cct, 10) << "tick" << dendl;

  std::unique_lock<std::mutex> l(monc_lock);

  _check_auth_tickets();

  if (hunting) {
    ldout(cct, 1) << "continuing hunt" << dendl;
    _reopen_session();
  } else if (!cur_mon.empty()) {
    // just renew as needed
    ceph::mono_time now = ceph::mono_clock::now();
    if (now > sub_renew_after)
      _renew_subs();

    messenger->send_keepalive(cur_con.get());

    if (state == MC_STATE_HAVE_SESSION) {
      send_log();

      if (cct->_conf->mon_client_ping_timeout > 0ns &&
	  cur_con->has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
	ceph::real_time lk = cur_con->get_last_keepalive_ack();
	ceph::timespan interval = ceph::real_clock::now() - lk;
	if (interval > cct->_conf->mon_client_ping_timeout) {
	  _reopen_session();
	}
      }
    }
  }

  l.unlock();

  if (hunting)
    timer.reschedule_me(cct->_conf->mon_client_hunt_interval
			* reopen_interval_multiplier);
  else
    timer.reschedule_me(cct->_conf->mon_client_ping_interval);
}

void MonClient::schedule_tick()
{
  if (hunting)
    timer.add_event(cct->_conf->mon_client_hunt_interval
		    * reopen_interval_multiplier,
		    &MonClient::tick, this);
  else
    timer.add_event(cct->_conf->mon_client_ping_interval,
		    &MonClient::tick, this);
}

// ---------

void MonClient::_renew_subs()
{
  if (sub_have.empty()) {
    ldout(cct, 10) << "renew_subs - empty" << dendl;
    return;
  }

  ldout(cct, 10) << "renew_subs" << dendl;
  if (cur_mon.empty())
    _reopen_session();
  else {
    if (sub_renew_sent == ceph::mono_time::min())
      sub_renew_sent = ceph::mono_clock::now();

    MMonSubscribe *m = new MMonSubscribe;
    m->what = sub_have;
    _send_mon_message(m);
  }
}

void MonClient::handle_subscribe_ack(MMonSubscribeAck *m)
{
  _finish_hunting();

  if (sub_renew_sent != ceph::mono_time::min()) {
    sub_renew_after = sub_renew_sent;
    sub_renew_after += m->interval;
    sub_renew_sent = ceph::mono_time::min();
  }

  m->put();
}

int MonClient::_check_auth_tickets()
{
  if (state == MC_STATE_HAVE_SESSION && auth) {
    if (auth->need_tickets()) {
      ldout(cct, 10) << "_check_auth_tickets getting new tickets!" << dendl;
      MAuth *m = new MAuth;
      m->protocol = auth->get_protocol();
      auth->prepare_build_request();
      auth->build_request(m->auth_payload);
      _send_mon_message(m);
    }

    _check_auth_rotating();
  }
  return 0;
}

int MonClient::_check_auth_rotating()
{
  if (!rotating_secrets ||
      !auth_principal_needs_rotating_keys(entity_name)) {
    ldout(cct, 20) << "_check_auth_rotating not needed by " << entity_name << dendl;
    return 0;
  }

  if (!auth || state != MC_STATE_HAVE_SESSION) {
    ldout(cct, 10) << "_check_auth_rotating waiting for auth session" << dendl;
    return 0;
  }

  ceph::real_time cutoff = ceph::real_clock::now() -
    MIN(30s, cct->_conf->auth_service_ticket_ttl / 4);
  if (!rotating_secrets->need_new_secrets(cutoff)) {
    ldout(cct, 10) << "_check_auth_rotating have uptodate secrets (they expire after " << cutoff << ")" << dendl;
    rotating_secrets->dump_rotating();
    return 0;
  }

  ldout(cct, 10) << "_check_auth_rotating renewing rotating keys (they expired before " << cutoff << ")" << dendl;
  MAuth *m = new MAuth;
  m->protocol = auth->get_protocol();
  if (auth->build_rotating_request(m->auth_payload)) {
    _send_mon_message(m);
  } else {
    m->put();
  }
  return 0;
}

int MonClient::wait_auth_rotating(ceph::timespan timeout)
{
  std::unique_lock<std::mutex> l(monc_lock);
  ceph::mono_time until = ceph::mono_clock::now() + timeout;

  if (auth->get_protocol() == CEPH_AUTH_NONE)
    return 0;

  if (!rotating_secrets)
    return 0;

  while (auth_principal_needs_rotating_keys(entity_name) &&
	 rotating_secrets->need_new_secrets()) {
    ceph::mono_time now = ceph::mono_clock::now();
    if (now >= until) {
      return -ETIMEDOUT;
    }
    auth_cond.wait_until(l, until);
  }
  ldout(cct, 10) << "wait_auth_rotating done" << dendl;
  return 0;
}

// ---------

void MonClient::_send_command(MonCommand& r)
{
  if (r.target_rank >= 0 &&
      r.target_rank != monmap.get_rank(cur_mon)) {
    ldout(cct, 10) << "_send_command " << r.tid << " " << r.cmd
		   << " wants rank " << r.target_rank
		   << ", reopening session"
		   << dendl;
    if (r.target_rank >= (int)monmap.size()) {
      ldout(cct, 10) << " target " << r.target_rank << " >= max mon "
		     << monmap.size() << dendl;
      bufferlist bl;
      // Replace with proper monitor error codes
      _finish_command(r, std::make_error_code(
			std::errc::no_such_file_or_directory),
		      "mon rank dne", bl);
      return;
    }
    _reopen_session(r.target_rank, string());
    return;
  }

  if (r.target_name.length() &&
      r.target_name != cur_mon) {
    ldout(cct, 10) << "_send_command " << r.tid << " " << r.cmd
		   << " wants mon " << r.target_name
		   << ", reopening session"
		   << dendl;
    if (!monmap.contains(r.target_name)) {
      ldout(cct, 10) << " target " << r.target_name << " not present in monmap"
		     << dendl;
      bufferlist bl;
      _finish_command(r, std::make_error_code(
			std::errc::no_such_file_or_directory), "mon dne", bl);
      return;
    }
    _reopen_session(-1, r.target_name);
    return;
  }

  ldout(cct, 10) << "_send_command " << r.tid << " " << r.cmd << dendl;
  MMonCommand *m = new MMonCommand(monmap.fsid);
  m->set_tid(r.tid);
  m->cmd = r.cmd;
  m->set_data(r.inbl);
  _send_mon_message(m);
  return;
}

void MonClient::_resend_mon_commands()
{
  // resend any requests
  for (auto& mc : mon_commands) {
    _send_command(mc);
  }
}

void MonClient::handle_mon_command_ack(MMonCommandAck *ack)
{
  if (mon_commands.empty()) {
    ack->put();
    return;
  }
  uint64_t tid = ack->get_tid();
  auto p = tid ? mon_commands.find(tid) : mon_commands.begin();

  if (tid == 0) {
    ldout(cct, 10) << "handle_mon_command_ack has tid 0, assuming it is "
		   << p->tid << dendl;
  } else {
    if (p == mon_commands.end()) {
      ldout(cct, 10) << "handle_mon_command_ack " << ack->get_tid()
		     << " not found" << dendl;
      ack->put();
      return;
    }
  }

  MonCommand& r = *p;
  if (r.ontimeout) {
    timer.cancel_event(r.ontimeout);
    r.ontimeout = 0;
  }

  ldout(cct, 10) << "handle_mon_command_ack " << r.tid << " " << r.cmd
		 << dendl;
  _finish_command(r, std::error_code(-ack->r, std::generic_category()),
		  ack->rs, ack->get_data());
  ack->put();
}

void MonClient::_cancel_mon_command(uint64_t tid, std::error_code r)
{
  std::unique_lock<std::mutex> l(monc_lock);
  auto it = mon_commands.find(tid);
  if (it == mon_commands.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  MonCommand& cmd = *it;
  bufferlist bl;
  _finish_command(cmd, r, "", bl);
}

void MonClient::_finish_command(MonCommand& r, std::error_code err,
				const string& rs, bufferlist& bl)
{
  ldout(cct, 10) << "_finish_command " << r.tid << " = " << err << " " << rs
		 << dendl;
  if (r.onfinish)
    finisher.queue(
      [&r, err, &rs, &bl]() {
	r.onfinish(err, rs, bl);
      });
  mon_commands.erase(r.tid);
  delete &r;
}

void MonClient::start_mon_command(const vector<string>& cmd,
				  const bufferlist& inbl,
				  monc_cb&& onfinish)
{
  std::lock_guard<std::mutex> l(monc_lock);
  MonCommand& r = *new MonCommand(++last_mon_command_tid);
  r.cmd = cmd;
  r.inbl = inbl;
  r.onfinish.swap(onfinish);
  if (cct->_conf->rados_mon_op_timeout > 0ns) {
    r.ontimeout =
      timer.add_event(cct->_conf->rados_mon_op_timeout,
		      &MonClient::_cancel_mon_command, this, r.tid,
		      std::make_error_code(std::errc::operation_canceled));
  }
  mon_commands.insert(r);
  _send_command(r);
}

void MonClient::start_mon_command(const string &mon_name,
				  const vector<string>& cmd,
				  const bufferlist& inbl,
				  monc_cb&& onfinish)
{
  std::lock_guard<std::mutex> l(monc_lock);
  MonCommand& r = *new MonCommand(++last_mon_command_tid);
  r.target_name = mon_name;
  r.cmd = cmd;
  r.inbl = inbl;
  r.onfinish.swap(onfinish);
  mon_commands.insert(r);
  _send_command(r);
}

void MonClient::start_mon_command(int rank,
				  const vector<string>& cmd,
				  const bufferlist& inbl,
				  monc_cb&& onfinish)
{
  std::lock_guard<std::mutex> l(monc_lock);
  MonCommand& r = *new MonCommand(++last_mon_command_tid);
  r.target_rank = rank;
  r.cmd = cmd;
  r.inbl = inbl;
  r.onfinish.swap(onfinish);
  mon_commands.insert(r);
  _send_command(r);
}

// ---------

void MonClient::get_version(string map, version_cb&& onfinish)
{
  version_req_d *req = new version_req_d(std::move(onfinish));
  ldout(cct, 10) << "get_version " << map << " req " << req << dendl;
  std::lock_guard<std::mutex> l(monc_lock);
  MMonGetVersion *m = new MMonGetVersion();
  m->what = map;
  m->handle = ++version_req_id;
  version_requests[m->handle] = req;
  _send_mon_message(m);
}

void MonClient::handle_get_version_reply(MMonGetVersionReply* m)
{
  map<ceph_tid_t, version_req_d*>::iterator iter = version_requests.find(m->handle);
  if (iter == version_requests.end()) {
    ldout(cct, 0) << __func__ << " version request with handle " << m->handle
		  << " not found" << dendl;
  } else {
    version_req_d *req = iter->second;
    ldout(cct, 10) << __func__ << " finishing " << req << " version " << m->version << dendl;
    version_requests.erase(iter);
    finisher.queue(std::bind(req->cb, std::error_code(),
			     m->version, m->oldest_version));
    delete req;
  }
  m->put();
}
