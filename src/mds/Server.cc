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

#include <boost/lexical_cast.hpp>
#include "include/assert.h"  // lexical_cast includes system assert.h

#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Migrator.h"
#include "MDBalancer.h"
#include "AnchorClient.h"
#include "InoTable.h"
#include "SnapClient.h"
#include "Mutation.h"

#include "msg/Messenger.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MLock.h"

#include "messages/MDentryUnlink.h"

#include "events/EString.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "include/filepath.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "include/compat.h"
#include "osd/OSDMap.h"

#include <errno.h>
#include <fcntl.h>

#include <list>
#include <iostream>
using namespace std;

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".server "

void Server::create_logger()
{
  PerfCountersBuilder plb(g_ceph_context, "mds_server", l_mdss_first, l_mdss_last);
  plb.add_u64_counter(l_mdss_hcreq,"hcreq"); // handle client req
  plb.add_u64_counter(l_mdss_hsreq, "hsreq"); // slave
  plb.add_u64_counter(l_mdss_hcsess, "hcsess");    // client session
  plb.add_u64_counter(l_mdss_dcreq, "dcreq"); // dispatch client req
  plb.add_u64_counter(l_mdss_dsreq, "dsreq"); // slave
  logger = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}


/* This function DOES put the passed message before returning*/
void Server::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_RECONNECT:
    handle_client_reconnect((MClientReconnect*)m);
    return;
  }

  // active?
  if (!mds->is_active() && 
      !(mds->is_stopping() && m->get_source().is_mds())) {
    if ((mds->is_reconnect() || mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) &&
	m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
	((MClientRequest*)m)->is_replay()) {
      dout(3) << "queuing replayed op" << dendl;
      mds->enqueue_replay(new C_MDS_RetryMessage(mds, m));
      return;
    } else if (mds->is_clientreplay() &&
	       (m->get_type() == CEPH_MSG_CLIENT_SESSION ||
		(m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
		 ((MClientRequest*)m)->is_replay()))) {
      // replaying!
    } else if (mds->is_clientreplay() && m->get_type() == MSG_MDS_SLAVE_REQUEST &&
	       (((MMDSSlaveRequest*)m)->is_reply() ||
		!mds->mdsmap->is_active(m->get_source().num()))) {
      // slave reply or the master is also in the clientreplay stage
    } else {
      dout(3) << "not active yet, waiting" << dendl;
      mds->wait_for_active(new C_MDS_RetryMessage(mds, m));
      return;
    }
  }

  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session((MClientSession*)m);
    return;
  case CEPH_MSG_CLIENT_REQUEST:
    handle_client_request((MClientRequest*)m);
    return;
  case MSG_MDS_SLAVE_REQUEST:
    handle_slave_request((MMDSSlaveRequest*)m);
    return;
  }

  dout(1) << "server unknown message " << m->get_type() << dendl;
  assert(0);
}



// ----------------------------------------------------------
// SESSION management

class C_MDS_session_finish : public Context {
  MDS *mds;
  Session *session;
  uint64_t state_seq;
  bool open;
  version_t cmapv;
  interval_set<inodeno_t> inos;
  version_t inotablev;
public:
  C_MDS_session_finish(MDS *m, Session *se, uint64_t sseq, bool s, version_t mv) :
    mds(m), session(se), state_seq(sseq), open(s), cmapv(mv), inotablev(0) { }
  C_MDS_session_finish(MDS *m, Session *se, uint64_t sseq, bool s, version_t mv, interval_set<inodeno_t>& i, version_t iv) :
    mds(m), session(se), state_seq(sseq), open(s), cmapv(mv), inos(i), inotablev(iv) { }
  void finish(int r) {
    assert(r == 0);
    mds->server->_session_logged(session, state_seq, open, cmapv, inos, inotablev);
  }
};

Session *Server::get_session(Message *m)
{
  Session *session = (Session *)m->get_connection()->get_priv();
  if (session) {
    dout(20) << "get_session have " << session << " " << session->inst
	     << " state " << session->get_state_name() << dendl;
    session->put();  // not carry ref
  } else {
    dout(20) << "get_session dne for " << m->get_source_inst() << dendl;
  }
  return session;
}

/* This function DOES put the passed message before returning*/
void Server::handle_client_session(MClientSession *m)
{
  version_t pv;
  Session *session = get_session(m);

  dout(3) << "handle_client_session " << *m << " from " << m->get_source() << dendl;
  assert(m->get_source().is_client()); // should _not_ come from an mds!

  if (!session) {
    dout(0) << " ignoring sessionless msg " << *m << dendl;
    m->put();
    return;
  }

  uint64_t sseq = 0;
  switch (m->get_op()) {
  case CEPH_SESSION_REQUEST_OPEN:
    if (session->is_opening() ||
	session->is_open() ||
	session->is_stale() ||
	session->is_killing()) {
      dout(10) << "currently open|opening|stale|killing, dropping this req" << dendl;
      m->put();
      return;
    }
    assert(session->is_closed() ||
	   session->is_closing());
    sseq = mds->sessionmap.set_state(session, Session::STATE_OPENING);
    mds->sessionmap.touch_session(session);
    pv = ++mds->sessionmap.projected;
    mdlog->start_submit_entry(new ESession(m->get_source_inst(), true, pv),
			      new C_MDS_session_finish(mds, session, sseq, true, pv));
    mdlog->flush();
    break;

  case CEPH_SESSION_REQUEST_RENEWCAPS:
    if (session->is_open() ||
	session->is_stale()) {
      mds->sessionmap.touch_session(session);
      if (session->is_stale()) {
	mds->sessionmap.set_state(session, Session::STATE_OPEN);
	mds->locker->resume_stale_caps(session);
	mds->sessionmap.touch_session(session);
      }
      mds->messenger->send_message(new MClientSession(CEPH_SESSION_RENEWCAPS, m->get_seq()), 
				   m->get_connection());
    } else {
      dout(10) << "ignoring renewcaps on non open|stale session (" << session->get_state_name() << ")" << dendl;
    }
    break;
    
  case CEPH_SESSION_REQUEST_CLOSE:
    {
      if (session->is_closed() || 
	  session->is_closing() ||
	  session->is_killing()) {
	dout(10) << "already closed|closing|killing, dropping this req" << dendl;
	m->put();
	return;
      }
      if (session->is_importing()) {
	dout(10) << "ignoring close req on importing session" << dendl;
	m->put();
	return;
      }
      assert(session->is_open() || 
	     session->is_stale() || 
	     session->is_opening());
      if (m->get_seq() < session->get_push_seq()) {
	dout(10) << "old push seq " << m->get_seq() << " < " << session->get_push_seq() 
		 << ", dropping" << dendl;
	m->put();
	return;
      }
      if (m->get_seq() != session->get_push_seq()) {
	dout(0) << "old push seq " << m->get_seq() << " != " << session->get_push_seq() 
		<< ", BUGGY!" << dendl;
	assert(0);
      }
      journal_close_session(session, Session::STATE_CLOSING);
    }
    break;

  default:
    assert(0);
  }
  m->put();
}

void Server::_session_logged(Session *session, uint64_t state_seq, bool open, version_t pv,
			     interval_set<inodeno_t>& inos, version_t piv)
{
  dout(10) << "_session_logged " << session->inst << " state_seq " << state_seq << " " << (open ? "open":"close")
	   << " " << pv << dendl;

  if (piv) {
    mds->inotable->apply_release_ids(inos);
    assert(mds->inotable->get_version() == piv);
  }

  // apply
  if (session->get_state_seq() != state_seq) {
    dout(10) << " journaled state_seq " << state_seq << " != current " << session->get_state_seq()
	     << ", noop" << dendl;
    // close must have been canceled (by an import?), or any number of other things..
  } else if (open) {
    assert(session->is_opening());
    mds->sessionmap.set_state(session, Session::STATE_OPEN);
    mds->sessionmap.touch_session(session);
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_OPEN), session->connection);
  } else if (session->is_closing() ||
	     session->is_killing()) {
    // kill any lingering capabilities, leases, requests
    while (!session->caps.empty()) {
      Capability *cap = session->caps.front();
      CInode *in = cap->get_inode();
      dout(20) << " killing capability " << ccap_string(cap->issued()) << " on " << *in << dendl;
      mds->locker->remove_client_cap(in, session->inst.name.num());
    }
    while (!session->leases.empty()) {
      ClientLease *r = session->leases.front();
      CDentry *dn = (CDentry*)r->parent;
      dout(20) << " killing client lease of " << *dn << dendl;
      dn->remove_client_lease(r, mds->locker);
    }
    
    if (session->is_closing()) {
      // reset session
      mds->send_message_client(new MClientSession(CEPH_SESSION_CLOSE), session);
      mds->sessionmap.set_state(session, Session::STATE_CLOSED);
      session->clear();
    } else if (session->is_killing()) {
      // destroy session, close connection
      mds->messenger->mark_down(session->inst.addr); 
      mds->sessionmap.remove_session(session);
    } else {
      assert(0);
    }
  } else {
    assert(0);
  }
  mds->sessionmap.version++;  // noop
}

version_t Server::prepare_force_open_sessions(map<client_t,entity_inst_t>& cm,
					      map<client_t,uint64_t>& sseqmap)
{
  version_t pv = ++mds->sessionmap.projected;
  dout(10) << "prepare_force_open_sessions " << pv 
	   << " on " << cm.size() << " clients"
	   << dendl;
  for (map<client_t,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_or_add_session(p->second);
    if (session->is_closed() || 
	session->is_closing() ||
	session->is_killing())
      sseqmap[p->first] = mds->sessionmap.set_state(session, Session::STATE_OPENING);
    else
      assert(session->is_open() ||
	     session->is_opening() ||
	     session->is_stale());
    session->inc_importing();
    mds->sessionmap.touch_session(session);
  }
  return pv;
}

void Server::finish_force_open_sessions(map<client_t,entity_inst_t>& cm,
					map<client_t,uint64_t>& sseqmap)
{
  /*
   * FIXME: need to carefully consider the race conditions between a
   * client trying to close a session and an MDS doing an import
   * trying to force open a session...  
   */
  dout(10) << "finish_force_open_sessions on " << cm.size() << " clients,"
	   << " v " << mds->sessionmap.version << " -> " << (mds->sessionmap.version+1) << dendl;
  for (map<client_t,entity_inst_t>::iterator p = cm.begin(); p != cm.end(); ++p) {
    Session *session = mds->sessionmap.get_session(p->second.name);
    assert(session);
    
    if (sseqmap.count(p->first)) {
      uint64_t sseq = sseqmap[p->first];
      if (session->get_state_seq() != sseq) {
	dout(10) << "force_open_sessions skipping changed " << session->inst << dendl;
      } else {
	dout(10) << "force_open_sessions opened " << session->inst << dendl;
	mds->sessionmap.set_state(session, Session::STATE_OPEN);
	mds->sessionmap.touch_session(session);
	Message *m = new MClientSession(CEPH_SESSION_OPEN);
	if (session->connection)
	  messenger->send_message(m, session->connection);
	else
	  session->preopen_out_queue.push_back(m);
      }
    } else {
      dout(10) << "force_open_sessions skipping already-open " << session->inst << dendl;
      assert(session->is_open() || session->is_stale());
    }
    session->dec_importing();
  }
  mds->sessionmap.version++;
}

struct C_MDS_TerminatedSessions : public Context {
  Server *server;
  C_MDS_TerminatedSessions(Server *s) : server(s) {}
  void finish(int r) {
    server->terminating_sessions = false;
  }
};

void Server::terminate_sessions()
{
  dout(2) << "terminate_sessions" << dendl;

  // kill them off.  clients will retry etc.
  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (set<Session*>::const_iterator p = sessions.begin();
       p != sessions.end();
       ++p) {
    Session *session = *p;
    if (session->is_closing() ||
	session->is_killing() ||
	session->is_closed())
      continue;
    journal_close_session(session, Session::STATE_CLOSING);
  }

  mdlog->wait_for_safe(new C_MDS_TerminatedSessions(this));
}


void Server::find_idle_sessions()
{
  dout(10) << "find_idle_sessions.  laggy until " << mds->laggy_until << dendl;
  
  // timeout/stale
  //  (caps go stale, lease die)
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mds_session_timeout;  
  while (1) {
    Session *session = mds->sessionmap.get_oldest_session(Session::STATE_OPEN);
    if (!session) break;
    dout(20) << "laggiest active session is " << session->inst << dendl;
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "laggiest active session is " << session->inst << " and sufficiently new (" 
	       << session->last_cap_renew << ")" << dendl;
      break;
    }

    dout(10) << "new stale session " << session->inst << " last " << session->last_cap_renew << dendl;
    mds->sessionmap.set_state(session, Session::STATE_STALE);
    mds->locker->revoke_stale_caps(session);
    mds->locker->remove_stale_leases(session);
    mds->send_message_client(new MClientSession(CEPH_SESSION_STALE, session->get_push_seq()), session);
  }

  // autoclose
  cutoff = now;
  cutoff -= g_conf->mds_session_autoclose;

  // don't kick clients if we've been laggy
  if (mds->laggy_until > cutoff) {
    dout(10) << " laggy_until " << mds->laggy_until << " > cutoff " << cutoff
	     << ", not kicking any clients to be safe" << dendl;
    return;
  }

  while (1) {
    Session *session = mds->sessionmap.get_oldest_session(Session::STATE_STALE);
    if (!session)
      break;
    if (session->is_importing()) {
      dout(10) << "stopping at importing session " << session->inst << dendl;
      break;
    }
    assert(session->is_stale());
    if (session->last_cap_renew >= cutoff) {
      dout(20) << "oldest stale session is " << session->inst << " and sufficiently new (" 
	       << session->last_cap_renew << ")" << dendl;
      break;
    }
    
    utime_t age = now;
    age -= session->last_cap_renew;
    mds->clog.info() << "closing stale session " << session->inst
	<< " after " << age << "\n";
    dout(10) << "autoclosing stale session " << session->inst << " last " << session->last_cap_renew << dendl;
    kill_session(session);
  }
}

void Server::kill_session(Session *session)
{
  if ((session->is_opening() ||
       session->is_open() ||
       session->is_stale()) &&
      !session->is_importing()) {
    dout(10) << "kill_session " << session << dendl;
    journal_close_session(session, Session::STATE_KILLING);
  } else {
    dout(10) << "kill_session importing or already closing/killing " << session << dendl;
    assert(session->is_closing() || 
	   session->is_closed() || 
	   session->is_killing() ||
	   session->is_importing());
  }
}

void Server::journal_close_session(Session *session, int state)
{
  uint64_t sseq = mds->sessionmap.set_state(session, state);
  version_t pv = ++mds->sessionmap.projected;
  version_t piv = 0;

  // release alloc and pending-alloc inos for this session
  // and wipe out session state, in case the session close aborts for some reason
  interval_set<inodeno_t> both;
  both.swap(session->prealloc_inos);
  both.insert(session->pending_prealloc_inos);
  session->pending_prealloc_inos.clear();
  if (both.size()) {
    mds->inotable->project_release_ids(both);
    piv = mds->inotable->get_projected_version();
  } else
    piv = 0;

  mdlog->start_submit_entry(new ESession(session->inst, false, pv, both, piv),
			    new C_MDS_session_finish(mds, session, sseq, false, pv, both, piv));
  mdlog->flush();

  // clean up requests, too
  elist<MDRequest*>::iterator p = session->requests.begin(member_offset(MDRequest,
									item_session_request));
  while (!p.end()) {
    MDRequest *mdr = *p;
    ++p;
    mdcache->request_kill(mdr);
  }
}

void Server::reconnect_clients()
{
  mds->sessionmap.get_client_set(client_reconnect_gather);

  if (client_reconnect_gather.empty()) {
    dout(7) << "reconnect_clients -- no sessions, doing nothing." << dendl;
    reconnect_gather_finish();
    return;
  }

  // clients will get the mdsmap and discover we're reconnecting via the monitor.
  
  reconnect_start = ceph_clock_now(g_ceph_context);
  dout(1) << "reconnect_clients -- " << client_reconnect_gather.size() << " sessions" << dendl;
  mds->sessionmap.dump();
}

/* This function DOES put the passed message before returning*/
void Server::handle_client_reconnect(MClientReconnect *m)
{
  dout(7) << "handle_client_reconnect " << m->get_source() << dendl;
  int from = m->get_source().num();
  Session *session = get_session(m);
  assert(session);

  if (!mds->is_reconnect() && mds->get_want_state() == CEPH_MDS_STATE_RECONNECT) {
    dout(10) << " we're almost in reconnect state (mdsmap delivery race?); waiting" << dendl;
    mds->wait_for_reconnect(new C_MDS_RetryMessage(mds, m));
    return;
  }

  utime_t delay = ceph_clock_now(g_ceph_context);
  delay -= reconnect_start;
  dout(10) << " reconnect_start " << reconnect_start << " delay " << delay << dendl;

  if (!mds->is_reconnect()) {
    // XXX maybe in the future we can do better than this?
    dout(1) << " no longer in reconnect state, ignoring reconnect, sending close" << dendl;
    mds->clog.info() << "denied reconnect attempt (mds is "
       << ceph_mds_state_name(mds->get_state())
       << ") from " << m->get_source_inst()
       << " after " << delay << " (allowed interval " << g_conf->mds_reconnect_timeout << ")\n";
    mds->messenger->send_message(new MClientSession(CEPH_SESSION_CLOSE), m->get_connection());
    m->put();
    return;
  }

  // notify client of success with an OPEN
  mds->messenger->send_message(new MClientSession(CEPH_SESSION_OPEN), m->get_connection());
    
  if (session->is_closed()) {
    dout(10) << " session is closed, will make best effort to reconnect " 
	     << m->get_source_inst() << dendl;
    mds->sessionmap.set_state(session, Session::STATE_OPENING);
    version_t pv = ++mds->sessionmap.projected;
    uint64_t sseq = session->get_state_seq();
    mdlog->start_submit_entry(new ESession(session->inst, true, pv),
			      new C_MDS_session_finish(mds, session, sseq, true, pv));
    mdlog->flush();
    mds->clog.debug() << "reconnect by new " << session->inst
	<< " after " << delay << "\n";
  } else {
    mds->clog.debug() << "reconnect by " << session->inst
	<< " after " << delay << "\n";
  }

  // caps
  for (map<inodeno_t, cap_reconnect_t>::iterator p = m->caps.begin();
       p != m->caps.end();
       ++p) {
    // make sure our last_cap_id is MAX over all issued caps
    if (p->second.capinfo.cap_id > mdcache->last_cap_id)
      mdcache->last_cap_id = p->second.capinfo.cap_id;
    
    CInode *in = mdcache->get_inode(p->first);
    if (in && in->state_test(CInode::STATE_PURGING))
      continue;
    if (in && in->is_auth()) {
      // we recovered it, and it's ours.  take note.
      dout(15) << "open cap realm " << inodeno_t(p->second.capinfo.snaprealm)
	       << " on " << *in << dendl;
      in->reconnect_cap(from, p->second.capinfo, session);
      recover_filelocks(in, p->second.flockbl, m->get_orig_source().num());
      continue;
    }
      
    filepath path(p->second.path, (uint64_t)p->second.capinfo.pathbase);
    if ((in && !in->is_auth()) ||
	!mds->mdcache->path_is_mine(path)) {
      // not mine.
      dout(0) << "non-auth " << p->first << " " << path
	      << ", will pass off to authority" << dendl;
      
      // mark client caps stale.
      inode_t fake_inode;
      fake_inode.ino = p->first;
      MClientCaps *stale = new MClientCaps(CEPH_CAP_OP_EXPORT, p->first, 0, 0, 0);
      //stale->head.migrate_seq = 0; // FIXME ******
      mds->send_message_client_counted(stale, session);

      // add to cap export list.
      mdcache->rejoin_export_caps(p->first, from, p->second);
    } else {
      // mine.  fetch later.
      dout(0) << "missing " << p->first << " " << path
	      << " (mine), will load later" << dendl;
      mdcache->rejoin_recovered_caps(p->first, from, p->second, 
				     -1);  // "from" me.
    }
  }

  // remove from gather set
  client_reconnect_gather.erase(from);
  if (client_reconnect_gather.empty())
    reconnect_gather_finish();

  m->put();
}



void Server::reconnect_gather_finish()
{
  dout(7) << "reconnect_gather_finish.  failed on " << failed_reconnects << " clients" << dendl;
  mds->reconnect_done();
}

void Server::reconnect_tick()
{
  utime_t reconnect_end = reconnect_start;
  reconnect_end += g_conf->mds_reconnect_timeout;
  if (ceph_clock_now(g_ceph_context) >= reconnect_end &&
      !client_reconnect_gather.empty()) {
    dout(10) << "reconnect timed out" << dendl;
    for (set<client_t>::iterator p = client_reconnect_gather.begin();
	 p != client_reconnect_gather.end();
	 p++) {
      Session *session = mds->sessionmap.get_session(entity_name_t::CLIENT(p->v));
      dout(1) << "reconnect gave up on " << session->inst << dendl;
      failed_reconnects++;
    }
    client_reconnect_gather.clear();
    reconnect_gather_finish();
  }
}

void Server::recover_filelocks(CInode *in, bufferlist locks, int64_t client)
{
  if (!locks.length()) return;
  int numlocks;
  ceph_filelock lock;
  bufferlist::iterator p = locks.begin();
  ::decode(numlocks, p);
  for (int i = 0; i < numlocks; ++i) {
    ::decode(lock, p);
    lock.client = client;
    in->fcntl_locks.held_locks.insert(pair<uint64_t, ceph_filelock>
				      (lock.start, lock));
    ++in->fcntl_locks.client_held_lock_counts[client];
  }
  ::decode(numlocks, p);
  for (int i = 0; i < numlocks; ++i) {
    ::decode(lock, p);
    lock.client = client;
    in->flock_locks.held_locks.insert(pair<uint64_t, ceph_filelock>
				      (lock.start, lock));
    ++in->flock_locks.client_held_lock_counts[client];
  }
}

void Server::recall_client_state(float ratio)
{
  int max_caps_per_client = (int)(g_conf->mds_cache_size * .8);
  int min_caps_per_client = 100;

  dout(10) << "recall_client_state " << ratio
	   << ", caps per client " << min_caps_per_client << "-" << max_caps_per_client
	   << dendl;

  set<Session*> sessions;
  mds->sessionmap.get_client_session_set(sessions);
  for (set<Session*>::const_iterator p = sessions.begin();
       p != sessions.end();
       ++p) {
    Session *session = *p;
    if (!session->is_open() ||
	!session->inst.name.is_client())
      continue;

    dout(10) << " session " << session->inst
	     << " caps " << session->caps.size()
	     << ", leases " << session->leases.size()
	     << dendl;

    if (session->caps.size() > min_caps_per_client) {	
      int newlim = (int)(session->caps.size() * ratio);
      if (newlim > max_caps_per_client)
	newlim = max_caps_per_client;
      MClientSession *m = new MClientSession(CEPH_SESSION_RECALL_STATE);
      m->head.max_caps = newlim;
      mds->send_message_client(m, session);
    }
  }
 
}


/*******
 * some generic stuff for finishing off requests
 */
/* This function takes responsibility for the passed mdr*/
void Server::journal_and_reply(MDRequest *mdr, CInode *in, CDentry *dn, LogEvent *le, Context *fin)
{
  dout(10) << "journal_and_reply tracei " << in << " tracedn " << dn << dendl;

  // note trace items for eventual reply.
  mdr->tracei = in;
  if (in)
    mdr->pin(in);

  mdr->tracedn = dn;
  if (dn)
    mdr->pin(dn);

  early_reply(mdr, in, dn);
  
  mdr->committing = true;
  mdlog->submit_entry(le, fin);
  
  if (mdr->client_request && mdr->client_request->is_replay()) {
    if (mds->queue_one_replay()) {
      dout(10) << " queued next replay op" << dendl;
    } else {
      dout(10) << " journaled last replay op, flushing" << dendl;
      mdlog->flush();
    }
  } else if (mdr->did_early_reply)
    mds->locker->drop_rdlocks(mdr);
  else
    mdlog->flush();
}

/*
 * send generic response (just an error code), clean up mdr
 */
void Server::reply_request(MDRequest *mdr, int r, CInode *tracei, CDentry *tracedn)
{
  reply_request(mdr, new MClientReply(mdr->client_request, r), tracei, tracedn);
}

void Server::early_reply(MDRequest *mdr, CInode *tracei, CDentry *tracedn)
{
  if (!g_conf->mds_early_reply)
    return;

  if (mdr->are_slaves()) {
    dout(10) << "early_reply - there are slaves, not allowed." << dendl;
    mds->mdlog->flush();
    return; 
  }

  if (mdr->alloc_ino) {
    dout(10) << "early_reply - allocated ino, not allowed" << dendl;
    return;
  }

  MClientRequest *req = mdr->client_request;
  entity_inst_t client_inst = req->get_source_inst();
  if (client_inst.name.is_mds())
    return;

  if (req->is_replay()) {
    dout(10) << " no early reply on replay op" << dendl;
    mds->mdlog->flush();
    return;
  }


  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply->set_unsafe();

  // mark xlocks "done", indicating that we are exposing uncommitted changes.
  //
  //_rename_finish() does not send dentry link/unlink message to replicas.
  // so do not set xlocks on dentries "done", the xlocks prevent dentries
  // that have projected linkages from getting new replica.
  mds->locker->set_xlocks_done(mdr, mdr->client_request->get_op() == CEPH_MDS_OP_RENAME);

  char buf[80];
  dout(10) << "early_reply " << reply->get_result() 
	   << " (" << strerror_r(-reply->get_result(), buf, sizeof(buf))
	   << ") " << *req << dendl;

  if (tracei || tracedn) {
    if (tracei)
      mdr->cap_releases.erase(tracei->vino());
    if (tracedn)
      mdr->cap_releases.erase(tracedn->get_dir()->get_inode()->vino());

    set_trace_dist(mdr->session, reply, tracei, tracedn, mdr->snapid,
		   mdr->client_request->get_dentry_wanted());
  }

  reply->set_extra_bl(mdr->reply_extra_bl);
  messenger->send_message(reply, req->get_connection());

  mdr->did_early_reply = true;

  mds->logger->inc(l_mds_reply);
  utime_t lat = ceph_clock_now(g_ceph_context) - mdr->client_request->get_recv_stamp();
  mds->logger->tinc(l_mds_replyl, lat);
  dout(20) << "lat " << lat << dendl;
}

/*
 * send given reply
 * include a trace to tracei
 * Clean up mdr
 */
void Server::reply_request(MDRequest *mdr, MClientReply *reply, CInode *tracei, CDentry *tracedn) 
{
  MClientRequest *req = mdr->client_request;
  
  char buf[80];
  dout(10) << "reply_request " << reply->get_result() 
	   << " (" << strerror_r(-reply->get_result(), buf, sizeof(buf))
	   << ") " << *req << dendl;

  // note successful request in session map?
  if (req->may_write() && mdr->session && reply->get_result() == 0)
    mdr->session->add_completed_request(mdr->reqid.tid);

  // give any preallocated inos to the session
  apply_allocated_inos(mdr);

  // get tracei/tracedn from mdr?
  snapid_t snapid = mdr->snapid;
  if (!tracei)
    tracei = mdr->tracei;
  if (!tracedn)
    tracedn = mdr->tracedn;

  bool is_replay = mdr->client_request->is_replay();
  bool did_early_reply = mdr->did_early_reply;
  Session *session = mdr->session;
  entity_inst_t client_inst = req->get_source_inst();
  int dentry_wanted = req->get_dentry_wanted();

  if (!did_early_reply && !is_replay) {

    mds->logger->inc(l_mds_reply);
    utime_t lat = ceph_clock_now(g_ceph_context) - mdr->client_request->get_recv_stamp();
    mds->logger->tinc(l_mds_replyl, lat);
    dout(20) << "lat " << lat << dendl;
    
    if (tracei)
      mdr->cap_releases.erase(tracei->vino());
    if (tracedn)
      mdr->cap_releases.erase(tracedn->get_dir()->get_inode()->vino());
  }

  // note client connection to direct my reply
  Connection *client_con = req->get_connection();
  client_con->get();

  // drop non-rdlocks before replying, so that we can issue leases
  mdcache->request_drop_non_rdlocks(mdr);

  // reply at all?
  if (client_inst.name.is_mds()) {
    reply->put();   // mds doesn't need a reply
    reply = 0;
  } else {
    // send reply.
    if (!did_early_reply &&   // don't issue leases if we sent an earlier reply already
	(tracei || tracedn)) {
      if (is_replay) {
	if (tracei)
	  mdcache->try_reconnect_cap(tracei, session);
      } else {
	// include metadata in reply
	set_trace_dist(session, reply, tracei, tracedn, snapid, dentry_wanted);
      }
    }

    reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
    messenger->send_message(reply, client_con);
  }
  client_con->put();
  
  // clean up request
  mdcache->request_finish(mdr);
  mdr = 0;
  req = 0;

  // take a closer look at tracei, if it happens to be a remote link
  if (tracei && 
      tracei->get_parent_dn() &&
      tracei->get_parent_dn()->get_projected_linkage()->is_remote())
    mdcache->eval_remote(tracei->get_parent_dn());
}


void Server::encode_infinite_lease(bufferlist& bl)
{
  LeaseStat e;
  e.seq = 0;
  e.mask = -1;
  e.duration_ms = -1;
  ::encode(e, bl);
  dout(20) << "encode_infinite_lease " << e << dendl;
}

void Server::encode_null_lease(bufferlist& bl)
{
  LeaseStat e;
  e.seq = 0;
  e.mask = 0;
  e.duration_ms = 0;
  ::encode(e, bl);
  dout(20) << "encode_null_lease " << e << dendl;
}


/*
 * pass inode OR dentry (not both, or we may get confused)
 *
 * trace is in reverse order (i.e. root inode comes last)
 */
void Server::set_trace_dist(Session *session, MClientReply *reply,
			    CInode *in, CDentry *dn,
			    snapid_t snapid,
			    int dentry_wanted)
{
  // inode, dentry, dir, ..., inode
  bufferlist bl;
  client_t client = session->get_client();
  utime_t now = ceph_clock_now(g_ceph_context);

  dout(20) << "set_trace_dist snapid " << snapid << dendl;

  //assert((bool)dn == (bool)dentry_wanted);  // not true for snapshot lookups

  // realm
  SnapRealm *realm = mdcache->get_snaprealm();
  reply->snapbl = realm->get_snap_trace();
  dout(10) << "set_trace_dist snaprealm " << *realm << " len=" << reply->snapbl.length() << dendl;

  // dir + dentry?
  if (dn) {
    reply->head.is_dentry = 1;
    CStripe *stripe = dn->get_stripe();
    CInode *diri = stripe->get_inode();

    diri->encode_inodestat(bl, session, NULL, snapid);
    dout(20) << "set_trace_dist added diri " << *diri << dendl;

    ::encode(dn->get_name(), bl);
    if (snapid == CEPH_NOSNAP)
      mds->locker->issue_client_lease(dn, client, bl, now, session);
    else
      encode_null_lease(bl);
    dout(20) << "set_trace_dist added dn   " << snapid << " " << *dn << dendl;
  } else
    reply->head.is_dentry = 0;

  // inode
  if (in) {
    in->encode_inodestat(bl, session, NULL, snapid);
    dout(20) << "set_trace_dist added in   " << *in << dendl;
    reply->head.is_target = 1;
  } else
    reply->head.is_target = 0;

  reply->set_trace(bl);
}




/***
 * process a client request
 * This function DOES put the passed message before returning
 */
void Server::handle_client_request(MClientRequest *req)
{
  dout(4) << "handle_client_request " << *req << dendl;

  if (logger) logger->inc(l_mdss_hcreq);

  if (!mdcache->is_open()) {
    dout(5) << "waiting for root" << dendl;
    mdcache->wait_for_open(new C_MDS_RetryMessage(mds, req));
    return;
  }

  // active session?
  Session *session = 0;
  if (req->get_source().is_client()) {
    session = get_session(req);
    if (!session) {
      dout(5) << "no session for " << req->get_source() << ", dropping" << dendl;
      req->put();
      return;
    }
    if (session->is_closed() ||
	session->is_closing() ||
	session->is_killing()) {
      dout(5) << "session closed|closing|killing, dropping" << dendl;
      req->put();
      return;
    }
  }

  // old mdsmap?
  if (req->get_mdsmap_epoch() < mds->mdsmap->get_epoch()) {
    // send it?  hrm, this isn't ideal; they may get a lot of copies if
    // they have a high request rate.
  }

  // completed request?
  if (req->is_replay() ||
      (req->get_retry_attempt() &&
       req->get_op() != CEPH_MDS_OP_OPEN && 
       req->get_op() != CEPH_MDS_OP_CREATE)) {
    assert(session);
    if (session->have_completed_request(req->get_reqid().tid)) {
      dout(5) << "already completed " << req->get_reqid() << dendl;
      mds->messenger->send_message(new MClientReply(req, 0), req->get_connection());

      if (req->is_replay())
	mds->queue_one_replay();

      req->put();
      return;
    }
  }

  // trim completed_request list
  if (req->get_oldest_client_tid() > 0) {
    dout(15) << " oldest_client_tid=" << req->get_oldest_client_tid() << dendl;
    session->trim_completed_requests(req->get_oldest_client_tid());
  }

  // request_start may drop the request, get a reference for cap release
  if (!req->releases.empty() && req->get_source().is_client() && !req->is_replay())
    req->get();

  // register + dispatch
  MDRequest *mdr = mdcache->request_start(req);
  if (mdr) {
    if (session) {
      mdr->session = session;
      session->requests.push_back(&mdr->item_session_request);
    }
  }

  // process embedded cap releases?
  //  (only if NOT replay!)
  if (!req->releases.empty() && req->get_source().is_client() && !req->is_replay()) {
    client_t client = req->get_source().num();
    for (vector<MClientRequest::Release>::iterator p = req->releases.begin();
	 p != req->releases.end();
	 p++)
      mds->locker->process_request_cap_release(mdr, client, p->item, p->dname);
    req->put();
  }

  if (mdr)
    dispatch_client_request(mdr);
  return;
}

/* This function takes responsibility for the passed mdr*/
void Server::dispatch_client_request(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  if (logger) logger->inc(l_mdss_dcreq);

  dout(7) << "dispatch_client_request " << *req << dendl;

  // we shouldn't be waiting on anyone.
  assert(mdr->more()->waiting_on_slave.empty());
  
  switch (req->get_op()) {
  case CEPH_MDS_OP_LOOKUPHASH:
    handle_client_lookup_hash(mdr);
    break;

  case CEPH_MDS_OP_LOOKUPINO:
    handle_client_lookup_ino(mdr);
    break;

    // inodes ops.
  case CEPH_MDS_OP_LOOKUP:
  case CEPH_MDS_OP_LOOKUPSNAP:
    handle_client_getattr(mdr, true);
    break;

  case CEPH_MDS_OP_GETATTR:
    handle_client_getattr(mdr, false);
    break;

  case CEPH_MDS_OP_LOOKUPPARENT:
    handle_client_lookup_parent(mdr);
    break;

  case CEPH_MDS_OP_SETATTR:
    handle_client_setattr(mdr);
    break;
  case CEPH_MDS_OP_SETLAYOUT:
    handle_client_setlayout(mdr);
    break;
  case CEPH_MDS_OP_SETDIRLAYOUT:
    handle_client_setdirlayout(mdr);
    break;
  case CEPH_MDS_OP_SETXATTR:
    handle_client_setxattr(mdr);
    break;
  case CEPH_MDS_OP_RMXATTR:
    handle_client_removexattr(mdr);
    break;

  case CEPH_MDS_OP_READDIR:
    handle_client_readdir(mdr);
    break;

  case CEPH_MDS_OP_SETFILELOCK:
    handle_client_file_setlock(mdr);
    break;

  case CEPH_MDS_OP_GETFILELOCK:
    handle_client_file_readlock(mdr);
    break;

    // funky.
  case CEPH_MDS_OP_CREATE:
    if (req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid))
      handle_client_open(mdr);  // already created.. just open
    else
      handle_client_openc(mdr);
    break;

  case CEPH_MDS_OP_OPEN:
    handle_client_open(mdr);
    break;

    // namespace.
    // no prior locks.
  case CEPH_MDS_OP_MKNOD:
    handle_client_mknod(mdr);
    break;
  case CEPH_MDS_OP_LINK:
    handle_client_link(mdr);
    break;
  case CEPH_MDS_OP_UNLINK:
  case CEPH_MDS_OP_RMDIR:
    handle_client_unlink(mdr);
    break;
  case CEPH_MDS_OP_RENAME:
    handle_client_rename(mdr);
    break;
  case CEPH_MDS_OP_MKDIR:
    handle_client_mkdir(mdr);
    break;
  case CEPH_MDS_OP_SYMLINK:
    handle_client_symlink(mdr);
    break;


    // snaps
  case CEPH_MDS_OP_LSSNAP:
  case CEPH_MDS_OP_MKSNAP:
  case CEPH_MDS_OP_RMSNAP:
    dout(1) << "snapshots not supported" << dendl;
    reply_request(mdr, -EOPNOTSUPP);
    break;


  default:
    dout(1) << " unknown client op " << req->get_op() << dendl;
    reply_request(mdr, -EOPNOTSUPP);
  }
}


// ---------------------------------------
// SLAVE REQUESTS

/* This function DOES put the passed message before returning*/
void Server::handle_slave_request(MMDSSlaveRequest *m)
{
  dout(4) << "handle_slave_request " << m->get_reqid() << " from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (logger) logger->inc(l_mdss_hsreq);

  // reply?
  if (m->is_reply())
    return handle_slave_request_reply(m);

  // am i a new slave?
  MDRequest *mdr = NULL;
  if (mdcache->have_request(m->get_reqid())) {
    // existing?
    mdr = mdcache->request_get(m->get_reqid());

    // is my request newer?
    if (mdr->attempt > m->get_attempt()) {
      dout(10) << "local request " << *mdr << " attempt " << mdr->attempt << " > " << m->get_attempt()
	       << ", dropping " << *m << dendl;
      m->put();
      return;
    }


    if (mdr->attempt < m->get_attempt()) {
      // mine is old, close it out
      dout(10) << "local request " << *mdr << " attempt " << mdr->attempt << " < " << m->get_attempt()
	       << ", closing out" << dendl;
      mdcache->request_finish(mdr);
      mdr = NULL;
    } else if (mdr->slave_to_mds != from) {
      dout(10) << "local request " << *mdr << " not slave to mds." << from << dendl;
      m->put();
      return;
    }
  }
  if (!mdr) {
    // new?
    if (m->get_op() == MMDSSlaveRequest::OP_FINISH) {
      dout(10) << "missing slave request for " << m->get_reqid() 
	       << " OP_FINISH, must have lost race with a forward" << dendl;
      m->put();
      return;
    }
    mdr = mdcache->request_start_slave(m->get_reqid(), m->get_attempt(), m->get_source().num());
  }
  assert(mdr->slave_request == 0);     // only one at a time, please!  
  mdr->slave_request = m;
  
  dispatch_slave_request(mdr);
}

/* This function DOES put the passed message before returning*/
void Server::handle_slave_request_reply(MMDSSlaveRequest *m)
{
  int from = m->get_source().num();
  
  if (m->get_op() == MMDSSlaveRequest::OP_COMMITTED) {
    metareqid_t r = m->get_reqid();
    mds->mdcache->committed_master_slave(r, from);
    m->put();
    return;
  }

  MDRequest *mdr = mdcache->request_get(m->get_reqid());
  if (!mdr) {
    dout(10) << "handle_slave_request_reply ignoring reply from unknown reqid " << m->get_reqid() << dendl;
    m->put();
    return;
  }
  if (m->get_attempt() != mdr->attempt) {
    dout(10) << "handle_slave_request_reply " << *mdr << " ignoring reply from other attempt "
	     << m->get_attempt() << dendl;
    m->put();
    return;
  }

  switch (m->get_op()) {
  case MMDSSlaveRequest::OP_XLOCKACK:
    {
      // identify lock, master request
      SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
					       m->get_object_info());
      mdr->more()->slaves.insert(from);
      dout(10) << "got remote xlock on " << *lock << " on " << *lock->get_parent() << dendl;
      mdr->xlocks.insert(lock);
      mdr->locks.insert(lock);
      mdr->finish_locking(lock);
      lock->get_xlock(mdr, mdr->get_client());
      lock->finish_waiters(SimpleLock::WAIT_REMOTEXLOCK);
    }
    break;
    
  case MMDSSlaveRequest::OP_WRLOCKACK:
    {
      // identify lock, master request
      SimpleLock *lock = mds->locker->get_lock(m->get_lock_type(),
					       m->get_object_info());
      mdr->more()->slaves.insert(from);
      dout(10) << "got remote wrlock on " << *lock << " on " << *lock->get_parent() << dendl;
      mdr->remote_wrlocks[lock] = from;
      mdr->locks.insert(lock);
      mdr->finish_locking(lock);
      lock->finish_waiters(SimpleLock::WAIT_REMOTEXLOCK);
    }
    break;

  case MMDSSlaveRequest::OP_AUTHPINACK:
    handle_slave_auth_pin_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_LINKPREPACK:
    handle_slave_link_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_MKDIRACK:
    handle_slave_mkdir_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RMDIRPREPACK:
    handle_slave_rmdir_prep_ack(mdr, m);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREPACK:
    handle_slave_rename_prep_ack(mdr, m);
    break;

  default:
    assert(0);
  }
  
  // done with reply.
  m->put();
}

/* This function DOES put the mdr->slave_request before returning*/
void Server::dispatch_slave_request(MDRequest *mdr)
{
  dout(7) << "dispatch_slave_request " << *mdr << " " << *mdr->slave_request << dendl;

  if (mdr->aborted) {
    dout(7) << " abort flag set, finishing" << dendl;
    mdcache->request_finish(mdr);
    return;
  }

  if (logger) logger->inc(l_mdss_dsreq);

  int op = mdr->slave_request->get_op();
  switch (op) {
  case MMDSSlaveRequest::OP_XLOCK:
  case MMDSSlaveRequest::OP_WRLOCK:
    {
      // identify object
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());

      if (!lock) {
	dout(10) << "don't have object, dropping" << dendl;
	assert(0); // can this happen, if we auth pinned properly.
      }
      if (op == MMDSSlaveRequest::OP_XLOCK && !lock->get_parent()->is_auth()) {
	dout(10) << "not auth for remote xlock attempt, dropping on " 
		 << *lock << " on " << *lock->get_parent() << dendl;
      } else {
	// use acquire_locks so that we get auth_pinning.
	set<SimpleLock*> rdlocks;
	set<SimpleLock*> wrlocks = mdr->wrlocks;
	set<SimpleLock*> xlocks = mdr->xlocks;

	int replycode = 0;
	switch (op) {
	case MMDSSlaveRequest::OP_XLOCK:
	  xlocks.insert(lock);
	  replycode = MMDSSlaveRequest::OP_XLOCKACK;
	  break;
	case MMDSSlaveRequest::OP_WRLOCK:
	  wrlocks.insert(lock);
	  replycode = MMDSSlaveRequest::OP_WRLOCKACK;
	  break;
	default:
	  assert(0);
	}
	
	if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	  return;
	
	// ack
	MMDSSlaveRequest *r = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, replycode);
	r->set_lock_type(lock->get_type());
	lock->get_parent()->set_object_info(r->get_object_info());
	mds->send_message(r, mdr->slave_request->get_connection());
      }

      // done.
      mdr->slave_request->put();
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_UNXLOCK:
  case MMDSSlaveRequest::OP_UNWRLOCK:
    {  
      SimpleLock *lock = mds->locker->get_lock(mdr->slave_request->get_lock_type(),
					       mdr->slave_request->get_object_info());
      assert(lock);
      bool need_issue = false;
      switch (op) {
      case MMDSSlaveRequest::OP_UNXLOCK:
	mds->locker->xlock_finish(lock, mdr, &need_issue);
	break;
      case MMDSSlaveRequest::OP_UNWRLOCK:
	mds->locker->wrlock_finish(lock, mdr, &need_issue);
	break;
      }
      if (need_issue)
	mds->locker->issue_caps((CInode*)lock->get_parent());

      // done.  no ack necessary.
      mdr->slave_request->put();
      mdr->slave_request = 0;
    }
    break;

  case MMDSSlaveRequest::OP_DROPLOCKS:
    mds->locker->drop_locks(mdr);
    mdr->slave_request->put();
    mdr->slave_request = 0;
    break;

  case MMDSSlaveRequest::OP_AUTHPIN:
    handle_slave_auth_pin(mdr);
    break;

  case MMDSSlaveRequest::OP_LINKPREP:
  case MMDSSlaveRequest::OP_UNLINKPREP:
    handle_slave_link_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_MKDIR:
    handle_slave_mkdir(mdr);
    break;

  case MMDSSlaveRequest::OP_RMDIRPREP:
    handle_slave_rmdir_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_RENAMEPREP:
    handle_slave_rename_prep(mdr);
    break;

  case MMDSSlaveRequest::OP_FINISH:
    // finish off request.
    mdcache->request_finish(mdr);
    break;

  default: 
    assert(0);
  }
}

/* This function DOES put the mdr->slave_request before returning*/
void Server::handle_slave_auth_pin(MDRequest *mdr)
{
  dout(10) << "handle_slave_auth_pin " << *mdr << dendl;

  // build list of objects
  list<MDSCacheObject*> objects;
  CInode *auth_pin_freeze = NULL;
  bool fail = false;

  for (vector<MDSCacheObjectInfo>::iterator p = mdr->slave_request->get_authpins().begin();
       p != mdr->slave_request->get_authpins().end();
       ++p) {
    MDSCacheObject *object = mdcache->get_object(*p);
    if (!object) {
      dout(10) << " don't have " << *p << dendl;
      fail = true;
      break;
    }

    objects.push_back(object);
    if (*p == mdr->slave_request->get_authpin_freeze())
      auth_pin_freeze = (CInode*)object;
  }
  
  // can we auth pin them?
  if (!fail) {
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      if (!(*p)->is_auth()) {
	dout(10) << " not auth for " << **p << dendl;
	fail = true;
	break;
      }
      if (!mdr->can_auth_pin(*p)) {
	// wait
	dout(10) << " waiting for authpinnable on " << **p << dendl;
	(*p)->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
	mdr->drop_local_auth_pins();
	return;
      }
    }
  }

  // auth pin!
  if (fail) {
    mdr->drop_local_auth_pins();  // just in case
  } else {
    /* handle_slave_rename_prep() call freeze_inode() to wait for all other operations
     * on the source inode to complete. This happens after all locks for the rename
     * operation are acquired. But to acquire locks, we need auth pin locks' parent
     * objects first. So there is an ABBA deadlock if someone auth pins the source inode
     * after locks are acquired and before Server::handle_slave_rename_prep() is called.
     * The solution is freeze the inode and prevent other MDRequests from getting new
     * auth pins.
     */
    if (auth_pin_freeze) {
      dout(10) << " freezing auth pin on " << *auth_pin_freeze << dendl;
      if (!mdr->freeze_auth_pin(auth_pin_freeze)) {
	auth_pin_freeze->add_waiter(CInode::WAIT_FROZEN, new C_MDS_RetryRequest(mdcache, mdr));
	mds->mdlog->flush();
	return;
      }
    }
    for (list<MDSCacheObject*>::iterator p = objects.begin();
	 p != objects.end();
	 ++p) {
      dout(10) << "auth_pinning " << **p << dendl;
      mdr->auth_pin(*p);
    }
  }

  // ack!
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_AUTHPINACK);
  
  // return list of my auth_pins (if any)
  for (set<MDSCacheObject*>::iterator p = mdr->auth_pins.begin();
       p != mdr->auth_pins.end();
       ++p) {
    MDSCacheObjectInfo info;
    (*p)->set_object_info(info);
    reply->get_authpins().push_back(info);
  }

  if (auth_pin_freeze)
    auth_pin_freeze->set_object_info(reply->get_authpin_freeze());

  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // clean up this request
  mdr->slave_request->put();
  mdr->slave_request = 0;
  return;
}

/* This function DOES NOT put the passed ack before returning*/
void Server::handle_slave_auth_pin_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_auth_pin_ack on " << *mdr << " " << *ack << dendl;
  int from = ack->get_source().num();

  // added auth pins?
  set<MDSCacheObject*> pinned;
  for (vector<MDSCacheObjectInfo>::iterator p = ack->get_authpins().begin();
       p != ack->get_authpins().end();
       ++p) {
    MDSCacheObject *object = mdcache->get_object(*p);
    assert(object);  // we pinned it
    dout(10) << " remote has pinned " << *object << dendl;
    if (!mdr->is_auth_pinned(object))
      mdr->remote_auth_pins.insert(object);
    if (*p == ack->get_authpin_freeze())
      mdr->set_remote_frozen_auth_pin((CInode *)object);
    pinned.insert(object);
  }

  // removed auth pins?
  set<MDSCacheObject*>::iterator p = mdr->remote_auth_pins.begin();
  while (p != mdr->remote_auth_pins.end()) {
    if ((*p)->authority().first == from &&
	pinned.count(*p) == 0) {
      dout(10) << " remote has unpinned " << **p << dendl;
      set<MDSCacheObject*>::iterator o = p;
      ++p;
      mdr->remote_auth_pins.erase(o);
    } else {
      ++p;
    }
  }
  
  // note slave
  mdr->more()->slaves.insert(from);

  // clear from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  // go again?
  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}


// ---------------------------------------
// HELPERS


/** validate_dentry_dir
 *
 * verify that the dir exists and would own the dname.
 * do not check if the dentry exists.
 */
CDir *Server::validate_dentry_dir(MDRequest *mdr, CInode *diri, const string& dname)
{
  // make sure parent is a dir?
  if (!diri->is_dir()) {
    dout(7) << "validate_dentry_dir: not a dir" << dendl;
    reply_request(mdr, -ENOTDIR);
    return NULL;
  }

  // XXX: avoid taking the hash when stripe count is 1 and fragtree is empty
  __u32 dnhash = diri->hash_dentry_name(dname);
  int stripeid = diri->pick_stripe(dnhash);
  CStripe *stripe = try_open_auth_stripe(diri, stripeid, mdr);
  if (!stripe)
    return 0;

  frag_t fg = stripe->pick_dirfrag(dnhash);
  CDir *dir = stripe->get_or_open_dirfrag(fg);

  // frozen?
  if (dir->is_frozen()) {
    dout(7) << "dir is frozen " << *dir << dendl;
    dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return NULL;
  }
  
  return dir;
}


/** prepare_null_dentry
 * prepare a null (or existing) dentry in given dir. 
 * wait for any dn lock.
 */
CDentry* Server::prepare_null_dentry(MDRequest *mdr, CDir *dir, const string& dname, bool okexist)
{
  dout(10) << "prepare_null_dentry " << dname << " in " << *dir << dendl;
  assert(dir->is_auth());
  
  client_t client = mdr->get_client();

  // does it already exist?
  CDentry *dn = dir->lookup(dname);
  if (dn) {
    /*
    if (dn->lock.is_xlocked_by_other(mdr)) {
      dout(10) << "waiting on xlocked dentry " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    */
    if (!dn->get_linkage(client, mdr)->is_null()) {
      // name already exists
      dout(10) << "dentry " << dname << " exists in " << *dir << dendl;
      if (!okexist) {
        reply_request(mdr, -EEXIST);
        return 0;
      }
    }

    return dn;
  }

  // make sure dir is complete
  if (!dir->is_complete() && (!dir->has_bloom() || dir->is_in_bloom(dname))) {
    dout(7) << " incomplete dir contents for " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }
  
  // create
  dn = dir->add_null_dentry(dname);
  dn->mark_new();
  dout(10) << "prepare_null_dentry added " << *dn << dendl;
  return dn;
}

// allocate an inode number for the given request
inodeno_t Server::prepare_new_inodeno(MDRequest *mdr, inodeno_t useino)
{
  inodeno_t ino;

  // already allocated for this request
  if (mdr->used_prealloc_ino)
    return mdr->used_prealloc_ino;
  if (mdr->alloc_ino)
    return mdr->alloc_ino;

  // assign ino
  if (mdr->session->prealloc_inos.size()) {
    mdr->used_prealloc_ino = ino = mdr->session->take_ino(useino);  // prealloc -> used
    dout(10) << "prepare_new_inodeno used_prealloc " << mdr->used_prealloc_ino
	     << " (" << mdr->session->prealloc_inos
	     << ", " << mdr->session->prealloc_inos.size() << " left)"
	     << dendl;
  } else {
    mdr->alloc_ino = ino = mds->inotable->project_alloc_id();
    dout(10) << "prepare_new_inodeno alloc " << mdr->alloc_ino << dendl;
  }

  if (useino && useino != ino) {
    dout(0) << "WARNING: client specified " << useino << " and i allocated " << ino << dendl;
    mds->clog.error() << mdr->client_request->get_source()
       << " specified ino " << useino
       << " but mds." << mds->whoami << " allocated " << ino << "\n";
    //assert(0); // just for now.
  }
    
  int got = g_conf->mds_client_prealloc_inos - mdr->session->get_num_projected_prealloc_inos();
  if (got > 0) {
    mds->inotable->project_alloc_ids(mdr->prealloc_inos, got);
    assert(mdr->prealloc_inos.size());  // or else fix projected increment semantics
    mdr->session->pending_prealloc_inos.insert(mdr->prealloc_inos);
    dout(10) << "prepare_new_inodeno prealloc " << mdr->prealloc_inos << dendl;
  }

  dout(10) << "prepare_new_inodeno " << ino << dendl;
  return ino;
}

/** prepare_new_inode
 *
 * create a new inode.  set c/m/atime.  hit dir pop.
 */
CInode* Server::prepare_new_inode(MDRequest *mdr, CDir *dir, inodeno_t useino,
                                  unsigned mode, ceph_file_layout *layout)
{
  CInode *in = new CInode(mdcache, mds->get_nodeid());

  in->inode.ino = useino;
  in->inode.version = 1;
  in->inode.nlink = 1;   // FIXME

  in->inode.mode = mode;

  memset(&in->inode.dir_layout, 0, sizeof(in->inode.dir_layout));
  if (in->inode.is_dir())
    in->inode.dir_layout.dl_dir_hash = g_conf->mds_default_dir_hash;

  if (layout)
    in->inode.layout = *layout;
  else if (in->inode.is_dir())
    memset(&in->inode.layout, 0, sizeof(in->inode.layout));
  else
    in->inode.layout = mds->mdcache->default_file_layout;

  in->inode.truncate_size = -1ull;  // not truncated, yet!
  in->inode.truncate_seq = 1; /* starting with 1, 0 is kept for no-truncation logic */

  CInode *diri = dir->get_inode();

  dout(10) << oct << " dir mode 0" << diri->inode.mode << " new mode 0" << mode << dec << dendl;

  MClientRequest *req = mdr->client_request;
  if (diri->inode.mode & S_ISGID) {
    dout(10) << " dir is sticky" << dendl;
    in->inode.gid = diri->inode.gid;
    if (S_ISDIR(mode)) {
      dout(10) << " new dir also sticky" << dendl;      
      in->inode.mode |= S_ISGID;
    }
  } else 
    in->inode.gid = req->get_caller_gid();

  in->inode.uid = req->get_caller_uid();

  in->inode.ctime = in->inode.mtime = in->inode.atime = mdr->now;   // now

  if (req->get_data().length()) {
    bufferlist::iterator p = req->get_data().begin();

    // xattrs on new inode?
    map<string,bufferptr> xattrs;
    ::decode(xattrs, p);
    for (map<string,bufferptr>::iterator p = xattrs.begin(); p != xattrs.end(); ++p) {
      dout(10) << "prepare_new_inode setting xattr " << p->first << dendl;
      in->xattrs[p->first] = p->second;
    }
  }

  dout(10) << "prepare_new_inode " << *in << dendl;
  mdcache->add_inode(in);  // add
  return in;
}

void Server::journal_allocated_inos(MDRequest *mdr, EMetaBlob *blob)
{
  if (mdr->used_prealloc_ino)
    mds->sessionmap.projected++;
  if (!mdr->prealloc_inos.empty())
    mds->sessionmap.projected++;
  dout(20) << "journal_allocated_inos sessionmapv " << mds->sessionmap.projected
	   << " inotablev " << mds->inotable->get_projected_version()
	   << dendl;
  blob->set_ino_alloc(mdr->alloc_ino,
		      mdr->used_prealloc_ino,
		      mdr->prealloc_inos,
		      mdr->client_request->get_source(),
		      mds->sessionmap.projected,
		      mds->inotable->get_projected_version());
}

void Server::apply_allocated_inos(MDRequest *mdr)
{
  Session *session = mdr->session;
  dout(10) << "apply_allocated_inos " << mdr->alloc_ino
	   << " / " << mdr->prealloc_inos
	   << " / " << mdr->used_prealloc_ino << dendl;

  if (mdr->alloc_ino) {
    mds->inotable->apply_alloc_id(mdr->alloc_ino);
  }
  if (mdr->prealloc_inos.size()) {
    session->pending_prealloc_inos.subtract(mdr->prealloc_inos);
    session->prealloc_inos.insert(mdr->prealloc_inos);
    mds->sessionmap.version++;
    mds->inotable->apply_alloc_ids(mdr->prealloc_inos);
  }
  if (mdr->used_prealloc_ino) {
    session->used_inos.erase(mdr->used_prealloc_ino);
    mds->sessionmap.version++;
  }
}

struct C_MDS_RollbackAllocatedInos : public Context {
  MDS *mds;
  Server *server;
  MDRequest *mdr;
  C_MDS_RollbackAllocatedInos(MDS *mds, Server *server, MDRequest *mdr)
      : mds(mds), server(server), mdr(mdr) {}
  void finish(int r) {
    server->apply_allocated_inos(mdr);
    mdr->put();
  }
};

void Server::rollback_allocated_inos(MDRequest *mdr)
{
  if (mdr->used_prealloc_ino) {
    dout(10) << "rollback_allocated_inos used_prealloc_ino "
        << mdr->used_prealloc_ino << dendl;
    // return to prealloc_inos
    mdr->session->used_inos.erase(mdr->used_prealloc_ino);
    mdr->session->prealloc_inos.insert(mdr->used_prealloc_ino);
    mdr->used_prealloc_ino = 0;
    // we don't have to journal unless we preallocated more
  }

  if (mdr->alloc_ino) {
    dout(10) << "rollback_allocated_inos alloc_ino "
        << mdr->alloc_ino << dendl;
    // must journal as a prealloc_ino
    mdr->prealloc_inos.insert(mdr->alloc_ino);
    mdr->alloc_ino = 0;
  }
  
  if (mdr->prealloc_inos.empty()) {
    // nothing to journal
    dout(20) << "rollback_allocated_inos nothing to journal" << dendl;
    return;
  }

  EUpdate *le = new EUpdate(mds->mdlog, "rollback_allocated_inos");
  journal_allocated_inos(mdr, &le->metablob);
  mds->mdlog->start_submit_entry(le, new C_MDS_RollbackAllocatedInos(mds, this, mdr->get()));
}



CDir *Server::traverse_to_auth_dir(MDRequest *mdr, vector<CDentry*> &trace, filepath refpath)
{
  // figure parent dir vs dname
  if (refpath.depth() == 0) {
    dout(7) << "can't do that to root" << dendl;
    reply_request(mdr, -EINVAL);
    return 0;
  }
  string dname = refpath.last_dentry();
  refpath.pop_dentry();
  
  dout(10) << "traverse_to_auth_dir dirpath " << refpath << " dname " << dname << dendl;

  // traverse to parent dir
  CInode *diri;
  int r = mdcache->path_traverse(mdr, NULL, NULL, refpath, &trace, &diri, MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0; // delayed
  if (r < 0) {
    reply_request(mdr, r);
    return 0;
  }

  // is it an auth dir?
  CDir *dir = validate_dentry_dir(mdr, diri, dname);
  if (!dir)
    return 0; // forwarded or waiting for freeze

  dout(10) << "traverse_to_auth_dir " << *dir << dendl;
  return dir;
}

class C_MDS_TryFindInode : public Context {
  Server *server;
  MDRequest *mdr;
public:
  C_MDS_TryFindInode(Server *s, MDRequest *r) : server(s), mdr(r) {
    mdr->get();
  }
  virtual void finish(int r) {
    if (r == -ESTALE) // :( find_ino_peers failed
      server->reply_request(mdr, r);
    else
      server->dispatch_client_request(mdr);
    mdr->put();
  }
};

/* If this returns null, the request has been handled
 * as appropriate: forwarded on, or the client's been replied to */
CInode* Server::rdlock_path_pin_ref(MDRequest *mdr, int n,
				    set<SimpleLock*> &rdlocks,
				    bool want_auth,
				    bool no_want_auth, /* for readdir, who doesn't want auth _even_if_ it's
							  a snapped dir */
				    ceph_file_layout **layout,
				    bool no_lookup)    // true if we cannot return a null dentry lease
{
  MClientRequest *req = mdr->client_request;
  const filepath& refpath = n ? req->get_filepath2() : req->get_filepath();
  dout(10) << "rdlock_path_pin_ref " << *mdr << " " << refpath << dendl;

  if (mdr->done_locking)
    return mdr->in[n];

  // traverse
  int r = mdcache->path_traverse(mdr, NULL, NULL, refpath, &mdr->dn[n], &mdr->in[n], MDS_TRAVERSE_FORWARD);
  if (r > 0)
    return NULL; // delayed
  if (r < 0) {  // error
    if (r == -ENOENT && n == 0 && mdr->dn[n].size()) {
      reply_request(mdr, r, NULL, no_lookup ? NULL : mdr->dn[n][mdr->dn[n].size()-1]);
    } else if (r == -ESTALE) {
      dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
      Context *c = new C_MDS_TryFindInode(this, mdr);
      mdcache->find_ino_peers(refpath.get_ino(), c);
    } else {
      dout(10) << "FAIL on error " << r << dendl;
      reply_request(mdr, r);
    }
    return 0;
  }
  CInode *ref = mdr->in[n];
  dout(10) << "ref is " << *ref << dendl;

  // fw to inode auth?
  if (mdr->snapid != CEPH_NOSNAP && !no_want_auth)
    want_auth = true;

  if (want_auth) {
    if (ref->is_ambiguous_auth()) {
      dout(10) << "waiting for single auth on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
    if (!ref->is_auth()) {
      dout(10) << "fw to auth for " << *ref << dendl;
      mdcache->request_forward(mdr, ref->authority().first);
      return 0;
    }

    // auth_pin?
    //   do NOT proceed if freezing, as cap release may defer in that case, and
    //   we could deadlock when we try to lock @ref.
    // if we're already auth_pinned, continue; the release has already been processed.
    if (ref->is_frozen() || ref->is_frozen_auth_pin() ||
	(ref->is_freezing() && !mdr->is_auth_pinned(ref))) {
      dout(7) << "waiting for !frozen/authpinnable on " << *ref << dendl;
      ref->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      /* If we have any auth pins, this will deadlock.
       * But the only way to get here if we've already got auth pins
       * is because we're on an inode with snapshots that got updated
       * between dispatches of this request. So we're going to drop
       * our locks and our auth pins and reacquire them later.
       *
       * This is safe since we're only in this function when working on
       * a single MDS request; otherwise we'd be in
       * rdlock_path_xlock_dentry.
       */
      mds->locker->drop_locks(mdr, NULL);
      mdr->drop_local_auth_pins();
      return 0;
    }

    mdr->auth_pin(ref);
  }

  for (int i=0; i<(int)mdr->dn[n].size(); i++) 
    rdlocks.insert(&mdr->dn[n][i]->lock);

  // set and pin ref
  mdr->pin(ref);
  return ref;
}


/** rdlock_path_xlock_dentry
 * traverse path to the directory that could/would contain dentry.
 * make sure i am auth for that dentry, forward as necessary.
 * create null dentry in place (or use existing if okexist).
 * get rdlocks on traversed dentries, xlock on new dentry.
 */
CDentry* Server::rdlock_path_xlock_dentry(MDRequest *mdr, int n,
					  set<SimpleLock*>& rdlocks, set<SimpleLock*>& wrlocks, set<SimpleLock*>& xlocks,
					  bool okexist, bool mustexist, bool alwaysxlock,
					  ceph_file_layout **layout)
{
  MClientRequest *req = mdr->client_request;
  const filepath& refpath = n ? req->get_filepath2() : req->get_filepath();

  dout(10) << "rdlock_path_xlock_dentry " << *mdr << " " << refpath << dendl;

  client_t client = mdr->get_client();

  if (mdr->done_locking)
    return mdr->dn[n].back();

  CDir *dir = traverse_to_auth_dir(mdr, mdr->dn[n], refpath);
  if (!dir) return 0;
  dout(10) << "rdlock_path_xlock_dentry dir " << *dir << dendl;

  // make sure we can auth_pin (or have already authpinned) dir
  if (dir->is_frozen()) {
    dout(7) << "waiting for !frozen/authpinnable on " << *dir << dendl;
    dir->add_waiter(CInode::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }

  CInode *diri = dir->get_inode();
  if (!mdr->reqid.name.is_mds()) {
    if (diri->is_system() && !diri->is_root()) {
      reply_request(mdr, -EROFS);
      return 0;
    }
    if (!diri->is_base() && diri->get_projected_parent_dir()->get_inode()->is_stray()) {
      reply_request(mdr, -ENOENT);
      return 0;
    }
  }

  // make a null dentry?
  const string &dname = refpath.last_dentry();
  CDentry *dn;
  if (mustexist) {
    dn = dir->lookup(dname);

    // make sure dir is complete
    if (!dn && !dir->is_complete() &&
        (!dir->has_bloom() || dir->is_in_bloom(dname))) {
      dout(7) << " incomplete dir contents for " << *dir << ", fetching" << dendl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }

    // readable?
    if (dn && !dn->lock.can_read(client) && dn->lock.get_xlock_by() != mdr) {
      dout(10) << "waiting on xlocked dentry " << *dn << dendl;
      dn->lock.add_waiter(SimpleLock::WAIT_RD, new C_MDS_RetryRequest(mdcache, mdr));
      return 0;
    }
      
    // exists?
    if (!dn || dn->get_linkage(client, mdr)->is_null()) {
      dout(7) << "dentry " << dname << " dne in " << *dir << dendl;
      reply_request(mdr, -ENOENT);
      return 0;
    }    
  } else {
    dn = prepare_null_dentry(mdr, dir, dname, okexist);
    if (!dn) 
      return 0;
  }

  mdr->dn[n].push_back(dn);
  mdr->in[n] = dn->get_projected_linkage()->get_inode();

  // -- lock --
  // NOTE: rename takes the same set of locks for srcdn
  for (int i=0; i<(int)mdr->dn[n].size(); i++) 
    rdlocks.insert(&mdr->dn[n][i]->lock);
  if (alwaysxlock || dn->get_linkage(client, mdr)->is_null())
    xlocks.insert(&dn->lock);                 // new dn, xlock
  else
    rdlocks.insert(&dn->lock);  // existing dn, rdlock

  // also xlock stripe for mtime
  wrlocks.insert(&dn->get_stripe()->linklock);
  wrlocks.insert(&dn->get_stripe()->nestlock);
  return dn;
}





/**
 * try_open_auth_stripe -- open stripe, or forward to stripe auth
 *
 * @param diri base inode
 * @param stripeid stripe index
 * @param mdr request
 * @returns the pointer, or NULL if it had to be delayed (but mdr is taken care of)
 */
CStripe* Server::try_open_auth_stripe(CInode *diri, int stripeid, MDRequest *mdr)
{
  // are we the stripe auth?
  int who = diri->get_stripe_auth(stripeid);
  if (who != mds->get_nodeid())
  {
    dout(7) << "try_open_auth_stripe: fw to stripe auth mds." << who << dendl;
    mdcache->request_forward(mdr, who);
    return 0;
  }

  // fetch the stripe if it's not open
  CStripe *stripe = diri->get_or_open_stripe(stripeid);
  if (!stripe->is_open()) {
    stripe->fetch(new C_MDS_RetryRequest(mdcache, mdr));
    return 0;
  }
  return stripe;
}


// ===============================================================================
// STAT

void Server::handle_client_getattr(MDRequest *mdr, bool is_lookup)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  if (req->get_filepath().depth() == 0 && is_lookup) {
    // refpath can't be empty for lookup but it can for
    // getattr (we do getattr with empty refpath for mount of '/')
    reply_request(mdr, -EINVAL);
    return;
  }

  CInode *ref = rdlock_path_pin_ref(mdr, 0, rdlocks, false, false, NULL, !is_lookup);
  if (!ref) return;

  /*
   * if client currently holds the EXCL cap on a field, do not rdlock
   * it; client's stat() will result in valid info if _either_ EXCL
   * cap is held or MDS rdlocks and reads the value here.
   *
   * handling this case here is easier than weakening rdlock
   * semantics... that would cause problems elsewhere.
   */
  client_t client = mdr->get_client();
  int issued = 0;
  Capability *cap = ref->get_client_cap(client);
  if (cap && (mdr->snapid == CEPH_NOSNAP ||
	      mdr->snapid <= cap->client_follows))
    issued = cap->issued();

  int mask = req->head.args.getattr.mask;
  if ((mask & CEPH_CAP_LINK_SHARED) && (issued & CEPH_CAP_LINK_EXCL) == 0) rdlocks.insert(&ref->linklock);
  if ((mask & CEPH_CAP_AUTH_SHARED) && (issued & CEPH_CAP_AUTH_EXCL) == 0) rdlocks.insert(&ref->authlock);
  if ((mask & CEPH_CAP_FILE_SHARED) && (issued & CEPH_CAP_FILE_EXCL) == 0) rdlocks.insert(&ref->filelock);
  if ((mask & CEPH_CAP_XATTR_SHARED) && (issued & CEPH_CAP_XATTR_EXCL) == 0) rdlocks.insert(&ref->xattrlock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mds->balancer->hit_inode(ceph_clock_now(g_ceph_context), ref, META_POP_IRD,
			   mdr->client_request->get_source().num());

  // reply
  dout(10) << "reply to stat on " << *req << dendl;
  reply_request(mdr, 0, ref,
		is_lookup ? mdr->dn[0].back() : 0);
}

/* This function will clean up the passed mdr*/
void Server::handle_client_lookup_parent(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  CInode *in = mdcache->get_inode(req->get_filepath().get_ino());
  if (!in) {
    reply_request(mdr, -ESTALE);
    return;
  }
  if (in->is_base()) {
    reply_request(mdr, -EINVAL);
    return;
  }

  CDentry *dn = in->get_projected_parent_dn();

  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  rdlocks.insert(&dn->lock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  reply_request(mdr, 0, in, dn);  // reply
}

struct C_MDS_LookupHash2 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupHash2(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_hash_2(mdr, r);
  }
};

/* This function DOES clean up the mdr before returning*/
/*
 * filepath:  ino
 * filepath2: dirino/<hash as base-10 %d>
 *
 * This dirino+hash is optional.
 */
void Server::handle_client_lookup_hash(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  inodeno_t ino = req->get_filepath().get_ino();
  inodeno_t dirino = req->get_filepath2().get_ino();

  CInode *in = 0;

  if (ino) {
    in = mdcache->get_inode(ino);
    if (in && in->state_test(CInode::STATE_PURGING)) {
      reply_request(mdr, -ESTALE);
      return;
    }
    if (!in && !dirino) {
      dout(10) << " no dirino, looking up ino " << ino << " directly" << dendl;
      _lookup_ino(mdr);
      return;
    }
  }
  if (!in) {
    // try the directory
    CInode *diri = mdcache->get_inode(dirino);
    if (!diri) {
      mdcache->find_ino_peers(dirino,
			      new C_MDS_LookupHash2(this, mdr), -1);
      return;
    }
    if (diri->state_test(CInode::STATE_PURGING)) {
      reply_request(mdr, -ESTALE);
      return;
    }
    dout(10) << " have diri " << *diri << dendl;
    unsigned hash = atoi(req->get_filepath2()[0].c_str());
    int stripeid = diri->pick_stripe(hash);
    CStripe *stripe = diri->get_stripe(stripeid);
    if (!stripe) {
      if (!diri->is_auth()) {
	if (diri->is_ambiguous_auth()) {
	  // wait
	  dout(7) << " waiting for single auth in " << *diri << dendl;
	  diri->add_waiter(CInode::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	  return;
	} 
	mdcache->request_forward(mdr, diri->authority().first);
	return;
      }
      stripe = diri->get_or_open_stripe(stripeid);
    }
    assert(stripe);
    dout(10) << " have stripe " << *stripe << dendl;
    if (!stripe->is_auth()) {
      if (stripe->is_ambiguous_auth()) {
	// wait
	dout(7) << " waiting for single auth in " << *stripe << dendl;
	stripe->add_waiter(CStripe::WAIT_SINGLEAUTH, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      } 
      mdcache->request_forward(mdr, stripe->authority().first);
      return;
    }
    frag_t fg = stripe->pick_dirfrag(hash);
    dout(10) << " fg is " << fg << dendl;
    CDir *dir = stripe->get_dirfrag(fg);
    if (!dir->is_complete()) {
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    reply_request(mdr, -ESTALE);
    return;
  }

  dout(10) << "reply to lookup_hash on " << *in << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply_request(mdr, reply, in, in->get_parent_dn());
}

struct C_MDS_LookupHash3 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupHash3(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_hash_3(mdr, r);
  }
};

void Server::_lookup_hash_2(MDRequest *mdr, int r)
{
  inodeno_t dirino = mdr->client_request->get_filepath2().get_ino();
  dout(10) << "_lookup_hash_2 " << mdr << " checked peers for dirino " << dirino << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }

  // okay fine, try the dir object then!
  mdcache->find_ino_dir(dirino, new C_MDS_LookupHash3(this, mdr));
}

void Server::_lookup_hash_3(MDRequest *mdr, int r)
{
  inodeno_t dirino = mdr->client_request->get_filepath2().get_ino();
  dout(10) << "_lookup_hash_3 " << mdr << " checked dir object for dirino " << dirino
	   << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }
  dout(10) << "_lookup_hash_3 " << mdr << " trying the ino itself" << dendl;
  _lookup_ino(mdr);
}

/***************/

struct C_MDS_LookupIno2 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupIno2(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_ino_2(mdr, r);
  }
};

/* This function DOES clean up the mdr before returning*/
/*
 * filepath:  ino
 */
void Server::handle_client_lookup_ino(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  inodeno_t ino = req->get_filepath().get_ino();
  CInode *in = mdcache->get_inode(ino);
  if (in && in->state_test(CInode::STATE_PURGING)) {
    reply_request(mdr, -ESTALE);
    return;
  }
  if (!in) {
    _lookup_ino(mdr);
    return;
  }

  dout(10) << "reply to lookup_ino " << *in << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply_request(mdr, reply, in, in->get_parent_dn());
}

void Server::_lookup_ino(MDRequest *mdr)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino " << mdr << " checking peers for ino " << ino << dendl;
  mdcache->find_ino_peers(ino,
			  new C_MDS_LookupIno2(this, mdr), -1);
}

struct C_MDS_LookupIno3 : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_LookupIno3(Server *s, MDRequest *r) : server(s), mdr(r) {}
  void finish(int r) {
    server->_lookup_ino_3(mdr, r);
  }
};

void Server::_lookup_ino_2(MDRequest *mdr, int r)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino_2 " << mdr << " checked peers for ino " << ino
	   << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }

  // okay fine, maybe it's a directory though...
  mdcache->find_ino_dir(ino, new C_MDS_LookupIno3(this, mdr));
}

void Server::_lookup_ino_3(MDRequest *mdr, int r)
{
  inodeno_t ino = mdr->client_request->get_filepath().get_ino();
  dout(10) << "_lookup_ino_3 " << mdr << " checked dir obj for ino " << ino
	   << " and got r=" << r << dendl;
  if (r == 0) {
    dispatch_client_request(mdr);
    return;
  }

  // give up
  if (r == -ENOENT || r == -ENODATA)
    r = -ESTALE;
  reply_request(mdr, r);
}


/* This function takes responsibility for the passed mdr*/
void Server::handle_client_open(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  int flags = req->head.args.open.flags;
  int cmode = ceph_flags_to_mode(req->head.args.open.flags);

  bool need_auth = !file_mode_is_readonly(cmode) || (flags & O_TRUNC);

  dout(7) << "open on " << req->get_filepath() << dendl;

  if (cmode < 0) {
    reply_request(mdr, -EINVAL);
    return;
  }
  
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, need_auth);
  if (!cur)
    return;

  if (cur->is_frozen() || cur->state_test(CInode::STATE_EXPORTINGCAPS)) {
    assert(!need_auth);
    mdr->done_locking = false;
    CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
    if (!cur)
      return;
  }

  if (mdr->snapid != CEPH_NOSNAP && mdr->client_request->may_write()) {
    reply_request(mdr, -EROFS);
    return;
  }

  // can only open a dir with mode FILE_MODE_PIN, at least for now.
  if (cur->inode.is_dir())
    cmode = CEPH_FILE_MODE_PIN;

  dout(10) << "open flags = " << flags
	   << ", filemode = " << cmode
	   << ", need_auth = " << need_auth
	   << dendl;
  
  // regular file?
  /*if (!cur->inode.is_file() && !cur->inode.is_dir()) {
    dout(7) << "not a file or dir " << *cur << dendl;
    reply_request(mdr, -ENXIO);                 // FIXME what error do we want?
    return;
    }*/
  if ((req->head.args.open.flags & O_DIRECTORY) && !cur->inode.is_dir()) {
    dout(7) << "specified O_DIRECTORY on non-directory " << *cur << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  // snapped data is read only
  if (mdr->snapid != CEPH_NOSNAP &&
      (cmode & CEPH_FILE_MODE_WR)) {
    dout(7) << "snap " << mdr->snapid << " is read-only " << *cur << dendl;
    reply_request(mdr, -EPERM);
    return;
  }

  // O_TRUNC
  if ((flags & O_TRUNC) &&
      !(req->get_retry_attempt() &&
	mdr->session->have_completed_request(req->get_reqid().tid))) {
    assert(cur->is_auth());

    wrlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    // wait for pending truncate?
    inode_t *pi = cur->get_projected_inode();
    if (pi->is_truncating()) {
      dout(10) << " waiting for pending truncate from " << pi->truncate_from
	       << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
      cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    
    do_open_truncate(mdr, cmode);
    return;
  }

  // sync filelock if snapped.
  //  this makes us wait for writers to flushsnaps, ensuring we get accurate metadata,
  //  and that data itself is flushed so that we can read the snapped data off disk.
  if (mdr->snapid != CEPH_NOSNAP && !cur->is_dir()) {
    rdlocks.insert(&cur->filelock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;
  }

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if (cur->is_file() || cur->is_dir()) {
    if (mdr->snapid == CEPH_NOSNAP) {
      // register new cap
      Capability *cap = mds->locker->issue_new_caps(cur, cmode, mdr->session, 0, req->is_replay());
      if (cap)
	dout(12) << "open issued caps " << ccap_string(cap->pending())
		 << " for " << req->get_source()
		 << " on " << *cur << dendl;
    } else {
      int caps = ceph_caps_for_mode(cmode);
      dout(12) << "open issued IMMUTABLE SNAP caps " << ccap_string(caps)
	       << " for " << req->get_source()
	       << " snapid " << mdr->snapid
	       << " on " << *cur << dendl;
      mdr->snap_caps = caps;
    }
  }

  // increase max_size?
  if (cmode & CEPH_FILE_MODE_WR)
    mds->locker->check_inode_max_size(cur);

  // make sure this inode gets into the journal
  if (cur->is_auth() && cur->last == CEPH_NOSNAP &&
      !cur->item_open_file.is_on_list()) {
    LogSegment *ls = mds->mdlog->get_current_segment();
    EOpen *le = new EOpen(mds->mdlog);
    mdlog->start_entry(le);
    le->add_clean_inode(cur);
    ls->open_files.push_back(&cur->item_open_file);
    mds->mdlog->submit_entry(le);
  }
  
  // hit pop
  mdr->now = ceph_clock_now(g_ceph_context);
  if (cmode == CEPH_FILE_MODE_RDWR ||
      cmode == CEPH_FILE_MODE_WR) 
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IWR);
  else
    mds->balancer->hit_inode(mdr->now, cur, META_POP_IRD, 
			     mdr->client_request->get_source().num());

  CDentry *dn = 0;
  if (req->get_dentry_wanted()) {
    assert(mdr->dn[0].size());
    dn = mdr->dn[0].back();
  }

  reply_request(mdr, 0, cur, dn);
}



class C_MDS_openc_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *newi;
  CDentry *dn, *inodn;
public:
  C_MDS_openc_finish(MDS *mds, MDRequest *mdr, CInode *newi,
                     CDentry *dn, CDentry *inodn)
      : mds(mds), mdr(mdr), newi(newi), dn(dn), inodn(inodn) {}
  void finish(int r) {
    assert(r == 0);

    dn->pop_projected_linkage();
    inodn->pop_projected_linkage();

    // dirty inode, dn, dir
    newi->mark_dirty(mdr->ls);

    mdr->apply();

    mds->locker->share_inode_max_size(newi);

    mds->mdcache->send_dentry_link(dn);
    mds->mdcache->send_dentry_link(inodn);

    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);
    mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_extra_bl(mdr->reply_extra_bl);
    mds->server->reply_request(mdr, reply);
  }
};

/* This function takes responsibility for the passed mdr*/
void Server::handle_client_openc(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = mdr->get_client();

  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << dendl;

  int cmode = ceph_flags_to_mode(req->head.args.open.flags);
  if (cmode < 0) {
    reply_request(mdr, -EINVAL);
    return;
  }

  if (!(req->head.args.open.flags & O_EXCL)) {
    int r = mdcache->path_traverse(mdr, NULL, NULL, req->get_filepath(),
				   &mdr->dn[0], NULL, MDS_TRAVERSE_FORWARD);
    if (r > 0) return;
    if (r == 0) {
      // it existed.
      rollback_allocated_inos(mdr);
      handle_client_open(mdr);
      return;
    }
    if (r < 0 && r != -ENOENT) {
      if (r == -ESTALE) {
	dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
	Context *c = new C_MDS_TryFindInode(this, mdr);
	mdcache->find_ino_peers(req->get_filepath().get_ino(), c);
      } else {
	dout(10) << "FAIL on error " << r << dendl;
	reply_request(mdr, r);
      }
      return;
    }
    // r == -ENOENT
  }

  bool excl = (req->head.args.open.flags & O_EXCL);
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks,
                                         !excl, false, false, &dir_layout);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  // set layout
  ceph_file_layout layout;
  if (dir_layout)
    layout = *dir_layout;
  else
    layout = mds->mdcache->default_file_layout;

  // fill in any special params from client
  if (req->head.args.open.stripe_unit)
    layout.fl_stripe_unit = req->head.args.open.stripe_unit;
  if (req->head.args.open.stripe_count)
    layout.fl_stripe_count = req->head.args.open.stripe_count;
  if (req->head.args.open.object_size)
    layout.fl_object_size = req->head.args.open.object_size;
  if (req->get_connection()->has_feature(CEPH_FEATURE_CREATEPOOLID) &&
      (__s32)req->head.args.open.pool >= 0) {
    layout.fl_pg_pool = req->head.args.open.pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }

  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << " invalid initial file layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout.fl_pg_pool << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  // allocate an inode number
  const inodeno_t ino = prepare_new_inodeno(mdr, inodeno_t(req->head.ino));

  // create/xlock a dentry in the inode container
  CDentry *inodn = mdcache->get_container()->xlock_dentry(mdr, ino, xlocks);
  if (!inodn)
    return;

  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  CDentry::linkage_t *dnl = dn->get_projected_linkage();

  if (!dnl->is_null()) {
    // it existed.  
    assert(req->head.args.open.flags & O_EXCL);
    dout(10) << "O_EXCL, target exists, failing with -EEXIST" << dendl;
    reply_request(mdr, -EEXIST, dnl->get_inode(), dn);
    return;
  }

  // created null dn.
    
  // create inode.
  mdr->now = ceph_clock_now(g_ceph_context);

  SnapRealm *realm = mdcache->get_snaprealm();
  snapid_t follows = realm->get_newest_seq();

  // it's a file.
  const unsigned mode = req->head.args.open.mode | S_IFREG;
  CInode *in = prepare_new_inode(mdr, dn->get_dir(), ino, mode, &layout);
  assert(in);

  // primary link to inode container
  inodn->push_projected_linkage(in);
  // remote link to filesystem namespace
  dn->push_projected_linkage(in->ino(), in->d_type());

  if (cmode & CEPH_FILE_MODE_WR) {
    in->inode.client_ranges[client].range.first = 0;
    in->inode.client_ranges[client].range.last = in->inode.get_layout_size_increment();
    in->inode.client_ranges[client].follows = follows;
  }

  in->inode.add_parent(dn->get_stripe()->dirstripe(),
                       mds->get_nodeid(), dn->get_name());

  if (follows >= dn->first)
    dn->first = follows+1;
  in->first = dn->first;
  
  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "openc");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);

  le->metablob.add_dentry(inodn, true);
  le->metablob.add_inode(in, true);

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(),
                                    PREDIRTY_DIR, 1);
  le->metablob.add_dentry(dn, true);

  // do the open
  mds->locker->issue_new_caps(in, cmode, mdr->session, realm, req->is_replay());
  in->authlock.set_state(LOCK_EXCL);
  in->xattrlock.set_state(LOCK_EXCL);

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(in->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&in->item_open_file);

  C_MDS_openc_finish *fin = new C_MDS_openc_finish(mds, mdr, in, dn, inodn);

  if (mdr->client_request->get_connection()->has_feature(CEPH_FEATURE_REPLY_CREATE_INODE)) {
    dout(10) << "adding ino to reply to indicate inode was created" << dendl;
    // add the file created flag onto the reply if create_flags features is supported
    ::encode(in->inode.ino, mdr->reply_extra_bl);
  }

  journal_and_reply(mdr, in, dn, le, fin);
}



void Server::handle_client_readdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = req->get_source().num();
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *diri = rdlock_path_pin_ref(mdr, 0, rdlocks, false, true);
  if (!diri) return;

  // it's a directory, right?
  if (!diri->is_dir()) {
    // not a dir
    dout(10) << "reply to " << *req << " readdir -ENOTDIR" << dendl;
    reply_request(mdr, -ENOTDIR);
    return;
  }
  // which stripe?
  stripeid_t stripeid = req->head.args.readdir.stripe;
  dout(10) << " stripe " << stripeid << dendl;

  CStripe *stripe = try_open_auth_stripe(diri, stripeid, mdr);
  if (!stripe) return;

  rdlocks.insert(&diri->filelock);
  rdlocks.insert(&stripe->dirfragtreelock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  const string &offset_str = req->get_path2();
  snapid_t snapid = mdr->snapid;

  dout(10) << "handle_client_readdir on " << *stripe
      << " snapid " << snapid << " offset '" << offset_str << "'" << dendl;

  frag_t fg;
  if (offset_str.length()) {
    // start with the dir fragment that contains offset_str
    fg = stripe->pick_dirfrag(offset_str);
  } else {
    // first fragment
    fg = stripe->get_fragtree()[0];
  }

  // does the frag exist?
  CDir *dir = stripe->get_or_open_dirfrag(fg);
  if (!dir->is_complete()) {
    if (dir->is_frozen()) {
      dout(7) << "dir is frozen " << *dir << dendl;
      dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
    // fetch
    dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << dendl;
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), true);
    return;
  }

  CDir::map_t::iterator it;
  if (offset_str.empty())
    it = dir->begin();
  else // first entry after 'offset_str'
    it = dir->items.upper_bound(dentry_key_t(snapid, offset_str.c_str()));

  // if we're at the end of a fragment, find the next non-empty one
  while (it == dir->end() && !fg.is_rightmost()) {
    // open next fragment
    fg = fg.next();
    dir = stripe->get_or_open_dirfrag(fg);
    if (!dir->is_complete()) {
      if (dir->is_frozen()) {
        dout(7) << "dir is frozen " << *dir << dendl;
        dir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
        return;
      }
      // fetch
      dout(10) << " incomplete dir contents for readdir on " << *dir << ", fetching" << dendl;
      dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), true);
      return;
    }
    it = dir->begin();
  }

#ifdef MDS_VERIFY_FRAGSTAT
  dir->verify_fragstat();
#endif

  mdr->now = ceph_clock_now(g_ceph_context);

  // purge stale snap data?
  const set<snapid_t> *snaps = 0;
  SnapRealm *realm = mdcache->get_snaprealm();
  if (realm->get_last_destroyed() > stripe->fnode.snap_purged_thru) {
    snaps = &realm->get_snaps();
    dout(10) << " last_destroyed " << realm->get_last_destroyed() << " > " << stripe->fnode.snap_purged_thru
	     << ", doing snap purge with " << *snaps << dendl;
    stripe->fnode.snap_purged_thru = realm->get_last_destroyed();
    assert(snapid == CEPH_NOSNAP || snaps->count(snapid));  // just checkin'! 
  }

  // build dir contents
  bufferlist dnbl;

  unsigned max = req->head.args.readdir.max_entries;
  if (!max)
    max = dir->get_num_any();  // whatever, something big.
  unsigned max_bytes = req->head.args.readdir.max_bytes;
  if (!max_bytes)
    max_bytes = 512 << 10;  // 512 KB?

  // start final blob
  bufferlist dirbl;

  // count bytes available.
  //  this isn't perfect, but we should capture the main variable/unbounded size items!
  int front_bytes = sizeof(__u32) + sizeof(__u8)*2;
  int bytes_left = max_bytes - front_bytes;
  bytes_left -= realm->get_snap_trace().length();

  __u32 numfiles = 0;
  while (it != dir->end() && numfiles < max) {
    CDentry *dn = it->second;
    it++;

    if (dn->state_test(CDentry::STATE_PURGING))
      continue;

    bool dnp = dn->use_projected(client, mdr);
    CDentry::linkage_t *dnl = dnp ? dn->get_projected_linkage() : dn->get_linkage();

    if (dnl->is_null())
      continue;
    if (snaps && dn->last != CEPH_NOSNAP)
      if (dir->try_trim_snap_dentry(dn, *snaps))
	continue;
    if (dn->last < snapid || dn->first > snapid) {
      dout(20) << "skipping non-overlapping snap " << *dn << dendl;
      continue;
    }

    CInode *in = dnl->get_inode();
    if (in && in->ino() == CEPH_INO_CEPH)
      continue;

    // remote link?
    // better for the MDS to do the work, if we think the client will stat any of these files.
    if (dnl->is_remote() && !in) {
      in = mdcache->get_inode(dnl->get_remote_ino());
      if (in) {
	dn->link_remote(dnl, in);
      } else if (dn->state_test(CDentry::STATE_BADREMOTEINO)) {
	dout(10) << "skipping bad remote ino on " << *dn << dendl;
	continue;
      } else {
	// touch everything i _do_ have
	for (CDir::map_t::iterator p = dir->begin(); p != dir->end(); p++)
	  if (!p->second->get_linkage()->is_null())
	    mdcache->lru.lru_touch(p->second);

	// already issued caps and leases, reply immediately.
	if (dnbl.length() > 0) {
	  mdcache->open_remote_dentry(dn, dnp, new C_NoopContext);
	  dout(10) << " open remote dentry after caps were issued, stopping at "
		   << dnbl.length() << " < " << bytes_left << dendl;
	  break;
	}

	mds->locker->drop_locks(mdr);
	mdr->drop_local_auth_pins();
	mdcache->open_remote_dentry(dn, dnp, new C_MDS_RetryRequest(mdcache, mdr));
	return;
      }
    }
    assert(in);

    if ((int)(dnbl.length() + dn->name.length() + sizeof(__u32) + sizeof(LeaseStat)) > bytes_left) {
      dout(10) << " ran out of room, stopping at " << dnbl.length() << " < " << bytes_left << dendl;
      break;
    }
    
    unsigned start_len = dnbl.length();

    // dentry
    dout(12) << "including    dn " << *dn << dendl;
    ::encode(dn->name, dnbl);
    mds->locker->issue_client_lease(dn, client, dnbl, mdr->now, mdr->session);

    // inode
    dout(12) << "including inode " << *in << dendl;
    int r = in->encode_inodestat(dnbl, mdr->session, realm, snapid, bytes_left - (int)dnbl.length());
    if (r < 0) {
      // chop off dn->name, lease
      dout(10) << " ran out of room, stopping at " << start_len << " < " << bytes_left << dendl;
      bufferlist keep;
      keep.substr_of(dnbl, 0, start_len);
      dnbl.swap(keep);
      break;
    }
    assert(r >= 0);
    numfiles++;

    // touch dn
    mdcache->lru.lru_touch(dn);
  }
  
  __u8 end = (it == dir->end());
  __u8 complete = (end && offset_str.empty()); // FIXME: what purpose does this serve
  
  // finish final blob
  ::encode(numfiles, dirbl);
  ::encode(end, dirbl);
  ::encode(complete, dirbl);
  dirbl.claim_append(dnbl);
  
  if (snaps)
    dir->log_mark_dirty();

  // yay, reply
  dout(10) << "reply to " << *req << " readdir num=" << numfiles
	   << " bytes=" << dirbl.length()
	   << " end=" << (int)end
	   << " complete=" << (int)complete
	   << dendl;
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_extra_bl(dirbl);
  dout(10) << "reply to " << *req << " readdir num=" << numfiles << " end=" << (int)end
	   << " complete=" << (int)complete << dendl;

  // bump popularity.  NOTE: this doesn't quite capture it.
  mds->balancer->hit_dir(ceph_clock_now(g_ceph_context), dir, META_POP_IRD, -1, numfiles);
  
  // reply
  reply_request(mdr, reply, diri);
}



// ===============================================================================
// INODE UPDATES


/* 
 * finisher for basic inode updates
 */
class C_MDS_inode_update_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
  bool truncating_smaller, changed_ranges;
public:
  C_MDS_inode_update_finish(MDS *m, MDRequest *r, CInode *i,
			    bool sm=false, bool cr=false) :
    mds(m), mdr(r), in(i), truncating_smaller(sm), changed_ranges(cr) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    mdr->apply();

    // notify any clients
    if (truncating_smaller && in->inode.is_truncating()) {
      mds->locker->issue_truncate(in);
      mds->mdcache->truncate_inode(in, mdr->ls);
    }

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    mds->server->reply_request(mdr, 0);

    if (changed_ranges)
      mds->locker->share_inode_max_size(in);
  }
};

void Server::handle_client_file_setlock(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // get the inode to operate on, and set up any locks needed for that
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  xlocks.insert(&cur->flocklock);
  /* acquire_locks will return true if it gets the locks. If it fails,
     it will redeliver this request at a later date, so drop the request.
   */
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks)) {
    dout(0) << "handle_client_file_setlock could not get locks!" << dendl;
    return;
  }

  // copy the lock change into a ceph_filelock so we can store/apply it
  ceph_filelock set_lock;
  set_lock.start = req->head.args.filelock_change.start;
  set_lock.length = req->head.args.filelock_change.length;
  set_lock.client = req->get_orig_source().num();
  set_lock.pid = req->head.args.filelock_change.pid;
  set_lock.pid_namespace = req->head.args.filelock_change.pid_namespace;
  set_lock.type = req->head.args.filelock_change.type;
  bool will_wait = req->head.args.filelock_change.wait;

  dout(0) << "handle_client_file_setlock: " << set_lock << dendl;

  ceph_lock_state_t *lock_state = NULL;

  // get the appropriate lock state
  switch (req->head.args.filelock_change.rule) {
  case CEPH_LOCK_FLOCK:
    lock_state = &cur->flock_locks;
    break;

  case CEPH_LOCK_FCNTL:
    lock_state = &cur->fcntl_locks;
    break;

  default:
    dout(0) << "got unknown lock type " << set_lock.type
	    << ", dropping request!" << dendl;
    return;
  }

  dout(10) << " state prior to lock change: " << *lock_state << dendl;;
  if (CEPH_LOCK_UNLOCK == set_lock.type) {
    list<ceph_filelock> activated_locks;
    list<Context*> waiters;
    if (lock_state->is_waiting(set_lock)) {
      dout(10) << " unlock removing waiting lock " << set_lock << dendl;
      lock_state->remove_waiting(set_lock);
    } else {
      dout(10) << " unlock attempt on " << set_lock << dendl;
      lock_state->remove_lock(set_lock, activated_locks);
      cur->take_waiting(CInode::WAIT_FLOCK, waiters);
    }
    reply_request(mdr, 0);
    /* For now we're ignoring the activated locks because their responses
     * will be sent when the lock comes up again in rotation by the MDS.
     * It's a cheap hack, but it's easy to code. */
    mds->queue_waiters(waiters);
  } else {
    dout(10) << " lock attempt on " << set_lock << dendl;
    if (mdr->more()->flock_was_waiting &&
	!lock_state->is_waiting(set_lock)) {
      dout(10) << " was waiting for lock but not anymore, must have been canceled " << set_lock << dendl;
      reply_request(mdr, -EINTR);
    } else if (!lock_state->add_lock(set_lock, will_wait, mdr->more()->flock_was_waiting)) {
      dout(10) << " it failed on this attempt" << dendl;
      // couldn't set lock right now
      if (!will_wait) {
	reply_request(mdr, -EWOULDBLOCK);
      } else {
	dout(10) << " added to waiting list" << dendl;
	assert(lock_state->is_waiting(set_lock));
	mdr->more()->flock_was_waiting = true;
	mds->locker->drop_locks(mdr);
	mdr->drop_local_auth_pins();
	cur->add_waiter(CInode::WAIT_FLOCK, new C_MDS_RetryRequest(mdcache, mdr));
      }
    } else
      reply_request(mdr, 0);
  }
  dout(10) << " state after lock change: " << *lock_state << dendl;
}

void Server::handle_client_file_readlock(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  // get the inode to operate on, and set up any locks needed for that
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  /* acquire_locks will return true if it gets the locks. If it fails,
     it will redeliver this request at a later date, so drop the request.
  */
  rdlocks.insert(&cur->flocklock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks)) {
    dout(0) << "handle_client_file_readlock could not get locks!" << dendl;
    return;
  }
  
  // copy the lock change into a ceph_filelock so we can store/apply it
  ceph_filelock checking_lock;
  checking_lock.start = req->head.args.filelock_change.start;
  checking_lock.length = req->head.args.filelock_change.length;
  checking_lock.client = req->get_orig_source().num();
  checking_lock.pid = req->head.args.filelock_change.pid;
  checking_lock.type = req->head.args.filelock_change.type;

  // get the appropriate lock state
  ceph_lock_state_t *lock_state = NULL;
  switch (req->head.args.filelock_change.rule) {
  case CEPH_LOCK_FLOCK:
    lock_state = &cur->flock_locks;
    break;

  case CEPH_LOCK_FCNTL:
    lock_state = &cur->fcntl_locks;
    break;

  default:
    dout(0) << "got unknown lock type " << checking_lock.type
	    << ", dropping request!" << dendl;
    return;
  }
  lock_state->look_for_lock(checking_lock);

  bufferlist lock_bl;
  ::encode(checking_lock, lock_bl);

  MClientReply *reply = new MClientReply(req);
  reply->set_extra_bl(lock_bl);
  reply_request(mdr, reply);
}

void Server::handle_client_setattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  if (cur->ino() < MDS_INO_SYSTEM_BASE && !cur->is_base()) {
    reply_request(mdr, -EPERM);
    return;
  }

  __u32 mask = req->head.args.setattr.mask;

  // xlock inode
  if (mask & (CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID))
    xlocks.insert(&cur->authlock);
  if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME|CEPH_SETATTR_SIZE))
    xlocks.insert(&cur->filelock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // trunc from bigger -> smaller?
  inode_t *pi = cur->get_projected_inode();

  uint64_t old_size = MAX(pi->size, req->head.args.setattr.old_size);
  bool truncating_smaller = false;
  if (mask & CEPH_SETATTR_SIZE) {
    truncating_smaller = req->head.args.setattr.size < old_size;
    if (truncating_smaller && pi->is_truncating()) {
      dout(10) << " waiting for pending truncate from " << pi->truncate_from
	       << " to " << pi->truncate_size << " to complete on " << *cur << dendl;
      cur->add_waiter(CInode::WAIT_TRUNC, new C_MDS_RetryRequest(mdcache, mdr));
      mds->mdlog->flush();
      return;
    }
  }

  bool changed_ranges = false;

  // project update
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setattr");
  mdlog->start_entry(le);

  pi = cur->project_inode();

  utime_t now = ceph_clock_now(g_ceph_context);

  if (mask & CEPH_SETATTR_MODE)
    pi->mode = (pi->mode & ~07777) | (req->head.args.setattr.mode & 07777);
  if (mask & CEPH_SETATTR_UID)
    pi->uid = req->head.args.setattr.uid;
  if (mask & CEPH_SETATTR_GID)
    pi->gid = req->head.args.setattr.gid;

  if (mask & CEPH_SETATTR_MTIME)
    pi->mtime = req->head.args.setattr.mtime;
  if (mask & CEPH_SETATTR_ATIME)
    pi->atime = req->head.args.setattr.atime;
  if (mask & (CEPH_SETATTR_ATIME | CEPH_SETATTR_MTIME))
    pi->time_warp_seq++;   // maybe not a timewarp, but still a serialization point.
  if (mask & CEPH_SETATTR_SIZE) {
    if (truncating_smaller) {
      pi->truncate(old_size, req->head.args.setattr.size);
      le->metablob.add_truncate_start(cur->ino());
    } else {
      pi->size = req->head.args.setattr.size;
      pi->rstat.rbytes = pi->size;
    }
    pi->mtime = now;

    // adjust client's max_size?
    map<client_t,client_writeable_range_t> new_ranges;
    mds->locker->calc_new_client_ranges(cur, pi->size, new_ranges);
    if (pi->client_ranges != new_ranges) {
      dout(10) << " client_ranges " << pi->client_ranges << " -> " << new_ranges << dendl;
      pi->client_ranges = new_ranges;
      changed_ranges = true;
    }
  }

  pi->ctime = now;

  // log + wait
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur,
								   truncating_smaller, changed_ranges));

  // flush immediately if there are readers/writers waiting
  if (cur->get_caps_wanted() & (CEPH_CAP_FILE_RD|CEPH_CAP_FILE_WR))
    mds->mdlog->flush();
}

/* Takes responsibility for mdr */
void Server::do_open_truncate(MDRequest *mdr, int cmode)
{
  CInode *in = mdr->in[0];
  client_t client = mdr->get_client();
  assert(in);

  dout(10) << "do_open_truncate " << *in << dendl;

  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "open_truncate");
  mdlog->start_entry(le);

  // prepare
  inode_t *pi = in->project_inode();
  pi->mtime = pi->ctime = ceph_clock_now(g_ceph_context);

  uint64_t old_size = MAX(pi->size, mdr->client_request->head.args.open.old_size);
  if (old_size > 0) {
    pi->truncate(old_size, 0);
    le->metablob.add_truncate_start(in->ino());
  }

  bool changed_ranges = false;
  if (cmode & CEPH_FILE_MODE_WR) {
    pi->client_ranges[client].range.first = 0;
    pi->client_ranges[client].range.last = pi->get_layout_size_increment();
    pi->client_ranges[client].follows = mdcache->get_snaprealm()->get_newest_seq();
    changed_ranges = true;
  }
  
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, in);
  
  // do the open
  SnapRealm *realm = mdcache->get_snaprealm();
  mds->locker->issue_new_caps(in, cmode, mdr->session, realm, mdr->client_request->is_replay());

  // make sure ino gets into the journal
  le->metablob.add_opened_ino(in->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&in->item_open_file);
  
  journal_and_reply(mdr, in, 0, le, new C_MDS_inode_update_finish(mds, mdr, in, old_size > 0,
								  changed_ranges));
}


/* This function cleans up the passed mdr */
void Server::handle_client_setlayout(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  if(cur->is_base()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }
  if (cur->is_dir()) {
    reply_request(mdr, -EISDIR);
    return;
  }
  
  if (cur->get_projected_inode()->size ||
      cur->get_projected_inode()->truncate_seq > 1) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // validate layout
  ceph_file_layout layout = cur->get_projected_inode()->layout;

  if (req->head.args.setlayout.layout.fl_object_size > 0)
    layout.fl_object_size = req->head.args.setlayout.layout.fl_object_size;
  if (req->head.args.setlayout.layout.fl_stripe_unit > 0)
    layout.fl_stripe_unit = req->head.args.setlayout.layout.fl_stripe_unit;
  if (req->head.args.setlayout.layout.fl_stripe_count > 0)
    layout.fl_stripe_count=req->head.args.setlayout.layout.fl_stripe_count;
  if (req->head.args.setlayout.layout.fl_cas_hash > 0)
    layout.fl_cas_hash = req->head.args.setlayout.layout.fl_cas_hash;
  if (req->head.args.setlayout.layout.fl_object_stripe_unit > 0)
    layout.fl_object_stripe_unit = req->head.args.setlayout.layout.fl_object_stripe_unit;
  if (req->head.args.setlayout.layout.fl_pg_pool > 0) {
    layout.fl_pg_pool = req->head.args.setlayout.layout.fl_pg_pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }
  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << "bad layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout.fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout.fl_pg_pool << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  xlocks.insert(&cur->filelock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // project update
  inode_t *pi = cur->project_inode();
  pi->layout = layout;
  pi->ctime = ceph_clock_now(g_ceph_context);
  
  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);
  
  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}

void Server::handle_client_setdirlayout(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CInode *cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true, false, &dir_layout);
  if (!cur) return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  if (!cur->is_dir()) {
    reply_request(mdr, -ENOTDIR);
    return;
  }

  xlocks.insert(&cur->policylock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // validate layout
  default_file_layout *layout = new default_file_layout;
  if (cur->get_projected_dir_layout())
    layout->layout = *cur->get_projected_dir_layout();
  else if (dir_layout)
    layout->layout = *dir_layout;
  else
    layout->layout = mds->mdcache->default_file_layout;

  if (req->head.args.setlayout.layout.fl_object_size > 0)
    layout->layout.fl_object_size = req->head.args.setlayout.layout.fl_object_size;
  if (req->head.args.setlayout.layout.fl_stripe_unit > 0)
    layout->layout.fl_stripe_unit = req->head.args.setlayout.layout.fl_stripe_unit;
  if (req->head.args.setlayout.layout.fl_stripe_count > 0)
    layout->layout.fl_stripe_count=req->head.args.setlayout.layout.fl_stripe_count;
  if (req->head.args.setlayout.layout.fl_cas_hash > 0)
    layout->layout.fl_cas_hash = req->head.args.setlayout.layout.fl_cas_hash;
  if (req->head.args.setlayout.layout.fl_object_stripe_unit > 0)
    layout->layout.fl_object_stripe_unit = req->head.args.setlayout.layout.fl_object_stripe_unit;
  if (req->head.args.setlayout.layout.fl_pg_pool > 0) {
    layout->layout.fl_pg_pool = req->head.args.setlayout.layout.fl_pg_pool;

    // make sure we have as new a map as the client
    if (req->get_mdsmap_epoch() > mds->mdsmap->get_epoch()) {
      delete layout;
      mds->wait_for_mdsmap(req->get_mdsmap_epoch(), new C_MDS_RetryRequest(mdcache, mdr));
      return;
    }
  }
  if (!ceph_file_layout_is_valid(&layout->layout)) {
    dout(10) << "bad layout" << dendl;
    reply_request(mdr, -EINVAL);
    delete layout;
    return;
  }
  if (!mds->mdsmap->is_data_pool(layout->layout.fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout->layout.fl_pg_pool << dendl;
    reply_request(mdr, -EINVAL);
    delete layout;
    return;
  }

  cur->project_inode();
  cur->get_projected_node()->dir_layout = layout;

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setlayout");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}




// XATTRS

int Server::parse_layout_vxattr(string name, string value, ceph_file_layout *layout)
{
  dout(20) << "parse_layout_vxattr name " << name << " value '" << value << "'" << dendl;
  try {
    if (name == "layout") {
      // XXX implement me
    } else if (name == "layout.object_size") {
      layout->fl_object_size = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.stripe_unit") {
      layout->fl_stripe_unit = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.stripe_count") {
      layout->fl_stripe_count = boost::lexical_cast<unsigned>(value);
    } else if (name == "layout.pool") {
      try {
	layout->fl_pg_pool = boost::lexical_cast<unsigned>(value);
      } catch (boost::bad_lexical_cast const&) {
	int64_t pool = mds->osdmap->lookup_pg_pool_name(value);
	if (pool < 0) {
	  dout(10) << " unknown pool " << value << dendl;
	  return -EINVAL;
	}
	layout->fl_pg_pool = pool;
      }
    } else {
      dout(10) << " unknown layout vxattr " << name << dendl;
      return -EINVAL;
    }
  } catch (boost::bad_lexical_cast const&) {
    dout(10) << "bad vxattr value, unable to parse int for " << name << dendl;
    return -EINVAL;
  }

  if (!ceph_file_layout_is_valid(layout)) {
    dout(10) << "bad layout" << dendl;
    return -EINVAL;
  }
  if (!mds->mdsmap->is_data_pool(layout->fl_pg_pool)) {
    dout(10) << " invalid data pool " << layout->fl_pg_pool << dendl;
    return -EINVAL;
  }
  return 0;
}

void Server::handle_set_vxattr(MDRequest *mdr, CInode *cur,
			       ceph_file_layout *dir_layout,
			       set<SimpleLock*> rdlocks,
			       set<SimpleLock*> wrlocks,
			       set<SimpleLock*> xlocks)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  bufferlist bl = req->get_data();
  string value (bl.c_str(), bl.length());
  dout(10) << "handle_set_vxattr " << name << " val " << value.length() << " bytes on " << *cur << dendl;

  // layout?
  if (name.find("ceph.file.layout") == 0 ||
      name.find("ceph.dir.layout") == 0) {
    inode_t *pi;
    string rest;
    if (name.find("ceph.dir.layout") == 0) {
      if (!cur->is_dir()) {
	reply_request(mdr, -EINVAL);
	return;
      }

      default_file_layout *dlayout = new default_file_layout;
      if (cur->get_projected_dir_layout())
	dlayout->layout = *cur->get_projected_dir_layout();
      else if (dir_layout)
	dlayout->layout = *dir_layout;
      else
	dlayout->layout = mds->mdcache->default_file_layout;

      rest = name.substr(name.find("layout"));
      int r = parse_layout_vxattr(rest, value, &dlayout->layout);
      if (r < 0) {
	reply_request(mdr, r);
	return;
      }

      xlocks.insert(&cur->policylock);
      if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	return;

      pi = cur->project_inode();
      cur->get_projected_node()->dir_layout = dlayout;
    } else {
      if (!cur->is_file()) {
	reply_request(mdr, -EINVAL);
	return;
      }
      ceph_file_layout layout = cur->get_projected_inode()->layout;
      rest = name.substr(name.find("layout"));
      int r = parse_layout_vxattr(rest, value, &layout);
      if (r < 0) {
	reply_request(mdr, r);
	return;
      }

      xlocks.insert(&cur->filelock);
      if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
	return;

      pi = cur->project_inode();
      pi->layout = layout;
      pi->ctime = ceph_clock_now(g_ceph_context);
    }

    // log + wait
    mdr->ls = mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mdlog, "set vxattr layout");
    mdlog->start_entry(le);
    le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
    mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
    mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

    journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
    return;
  }

  dout(10) << " unknown vxattr " << name << dendl;
  reply_request(mdr, -EINVAL);
}

void Server::handle_remove_vxattr(MDRequest *mdr, CInode *cur,
				  set<SimpleLock*> rdlocks,
				  set<SimpleLock*> wrlocks,
				  set<SimpleLock*> xlocks)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  if (name == "ceph.dir.layout") {
    if (!cur->is_dir()) {
      reply_request(mdr, -ENODATA);
      return;
    }
    if (cur->is_root()) {
      dout(10) << "can't remove layout policy on the root directory" << dendl;
      reply_request(mdr, -EINVAL);
      return;
    }

    if (!cur->get_projected_dir_layout()) {
      reply_request(mdr, -ENODATA);
      return;
    }

    xlocks.insert(&cur->policylock);
    if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
      return;

    cur->project_inode();
    cur->get_projected_node()->dir_layout = NULL;

    // log + wait
    mdr->ls = mdlog->get_current_segment();
    EUpdate *le = new EUpdate(mdlog, "remove dir layout vxattr");
    mdlog->start_entry(le);
    le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
    mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
    mdcache->journal_dirty_inode(mdr, &le->metablob, cur);

    journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
    return;
  }

  reply_request(mdr, -ENODATA);
}

class C_MDS_inode_xattr_update_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *in;
public:

  C_MDS_inode_xattr_update_finish(MDS *m, MDRequest *r, CInode *i) :
    mds(m), mdr(r), in(i) { }
  void finish(int r) {
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    
    mdr->apply();

    mds->balancer->hit_inode(mdr->now, in, META_POP_IWR);   

    mds->server->reply_request(mdr, 0);
  }
};

void Server::handle_client_setxattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CInode *cur;

  ceph_file_layout *dir_layout = NULL;
  if (name.find("ceph.dir.layout") == 0)
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true, false, &dir_layout);
  else
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  if (cur->is_base()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  int flags = req->head.args.setxattr.flags;

  // magic ceph.* namespace?
  if (name.find("ceph.") == 0) {
    handle_set_vxattr(mdr, cur, dir_layout, rdlocks, wrlocks, xlocks);
    return;
  }

  xlocks.insert(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if ((flags & CEPH_XATTR_CREATE) && cur->xattrs.count(name)) {
    dout(10) << "setxattr '" << name << "' XATTR_CREATE and EEXIST on " << *cur << dendl;
    reply_request(mdr, -EEXIST);
    return;
  }
  if ((flags & CEPH_XATTR_REPLACE) && !cur->xattrs.count(name)) {
    dout(10) << "setxattr '" << name << "' XATTR_REPLACE and ENODATA on " << *cur << dendl;
    reply_request(mdr, -ENODATA);
    return;
  }

  int len = req->get_data().length();
  dout(10) << "setxattr '" << name << "' len " << len << " on " << *cur << dendl;

  // project update
  map<string,bufferptr> *px = new map<string,bufferptr>;
  inode_t *pi = cur->project_inode(px);
  pi->ctime = ceph_clock_now(g_ceph_context);
  pi->xattr_version++;
  px->erase(name);
  (*px)[name] = buffer::create(len);
  if (len)
    req->get_data().copy(0, len, (*px)[name].c_str());

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "setxattr");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0, PREDIRTY_PRIMARY, false);
  mdcache->journal_cow_inode(mdr, &le->metablob, cur);
  le->metablob.add_inode(cur, true);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}

void Server::handle_client_removexattr(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  string name(req->get_path2());
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CInode *cur;
  if (name == "ceph.dir.layout")
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true, false, &dir_layout);
  else
    cur = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
  if (!cur)
    return;

  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  if (cur->is_base()) {
    reply_request(mdr, -EINVAL);   // for now
    return;
  }

  if (name.find("ceph.") == 0) {
    handle_remove_vxattr(mdr, cur, rdlocks, wrlocks, xlocks);
    return;
  }

  xlocks.insert(&cur->xattrlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  map<string, bufferptr> *pxattrs = cur->get_projected_xattrs();
  if (pxattrs->count(name) == 0) {
    dout(10) << "removexattr '" << name << "' and ENODATA on " << *cur << dendl;
    reply_request(mdr, -ENODATA);
    return;
  }

  dout(10) << "removexattr '" << name << "' on " << *cur << dendl;

  // project update
  map<string,bufferptr> *px = new map<string,bufferptr>;
  inode_t *pi = cur->project_inode(px);
  pi->ctime = ceph_clock_now(g_ceph_context);
  pi->xattr_version++;
  px->erase(name);

  // log + wait
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "removexattr");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, cur, 0,
                                    PREDIRTY_PRIMARY, false);
  mdcache->journal_cow_inode(mdr, &le->metablob, cur);
  le->metablob.add_inode(cur, true);

  journal_and_reply(mdr, cur, 0, le, new C_MDS_inode_update_finish(mds, mdr, cur));
}


// =================================================================
// DIRECTORY and NAMESPACE OPS


// ------------------------------------------------

// MKNOD

class C_MDS_mknod_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CInode *newi;
  CDentry *dn, *inodn;
public:
  C_MDS_mknod_finish(MDS *mds, MDRequest *mdr, CInode *newi,
                     CDentry *dn, CDentry *inodn)
      : mds(mds), mdr(mdr), newi(newi), dn(dn), inodn(inodn) {}
  void finish(int r) {
    assert(r == 0);

    // link the inode
    dn->pop_projected_linkage();
    inodn->pop_projected_linkage();

    newi->mark_dirty(mdr->ls);

    // mkdir?
    if (newi->inode.is_dir()) { 
      list<CStripe*> stripes;
      newi->get_stripes(stripes);
      for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
        CStripe *stripe = *s;
        assert(stripe);
        stripe->mark_dirty(mdr->ls);
        stripe->mark_new(mdr->ls);

        CDir *dir = stripe->get_dirfrag(frag_t());
        assert(dir);
        dir->mark_dirty(mdr->ls);
        dir->mark_new(mdr->ls);
      }
    }

    mdr->apply();

    mds->mdcache->send_dentry_link(dn);
    // don't send MDentryLink to nodes that got a slave_mkdir
    mds->mdcache->send_dentry_link(inodn, &newi->get_stripe_auth());

    if (newi->inode.is_file())
      mds->locker->share_inode_max_size(newi);

    // hit pop
    mds->balancer->hit_inode(mdr->now, newi, META_POP_IWR);
    mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

    // reply
    MClientReply *reply = new MClientReply(mdr->client_request, 0);
    reply->set_result(0);
    mds->server->reply_request(mdr, reply);
  }
};


void Server::handle_client_mknod(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = mdr->get_client();
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  ceph_file_layout *dir_layout = NULL;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false,
					 &dir_layout);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  // allocate an inode number
  const inodeno_t ino = prepare_new_inodeno(mdr, inodeno_t(req->head.ino));

  // create/xlock a dentry in the inode container
  CDentry *inodn = mdcache->get_container()->xlock_dentry(mdr, ino, xlocks);
  if (!inodn)
    return;

  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // set layout
  ceph_file_layout layout;
  if (dir_layout)
    layout = *dir_layout;
  else
    layout = mds->mdcache->default_file_layout;

  if (!ceph_file_layout_is_valid(&layout)) {
    dout(10) << " invalid initial file layout" << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }

  SnapRealm *realm = mdcache->get_snaprealm();
  snapid_t follows = realm->get_newest_seq();
  mdr->now = ceph_clock_now(g_ceph_context);

  const unsigned mode = req->head.args.mknod.mode;
  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), ino, mode, &layout);
  assert(newi);

  // primary link to inode container
  inodn->push_projected_linkage(newi);
  // remote link to filesystem namespace
  dn->push_projected_linkage(newi->ino(), newi->d_type());

  newi->inode.rdev = req->head.args.mknod.rdev;
  if ((newi->inode.mode & S_IFMT) == 0)
    newi->inode.mode |= S_IFREG;
  newi->inode.add_parent(dn->get_stripe()->dirstripe(),
                         mds->get_nodeid(), dn->get_name());

  // if the client created a _regular_ file via MKNOD, it's highly likely they'll
  // want to write to it (e.g., if they are reexporting NFS)
  if (S_ISREG(newi->inode.mode)) {
    dout(15) << " setting a client_range too, since this is a regular file" << dendl;
    newi->inode.client_ranges[client].range.first = 0;
    newi->inode.client_ranges[client].range.last = newi->inode.get_layout_size_increment();
    newi->inode.client_ranges[client].follows = follows;

    // issue a cap on the file
    int cmode = CEPH_FILE_MODE_RDWR;
    Capability *cap = mds->locker->issue_new_caps(newi, cmode, mdr->session, realm, req->is_replay());
    if (cap) {
      cap->set_wanted(0);

      // put locks in excl mode
      newi->filelock.set_state(LOCK_EXCL);
      newi->authlock.set_state(LOCK_EXCL);
      newi->xattrlock.set_state(LOCK_EXCL);
      cap->issue_norevoke(CEPH_CAP_AUTH_EXCL|CEPH_CAP_AUTH_SHARED|
			  CEPH_CAP_XATTR_EXCL|CEPH_CAP_XATTR_SHARED|
			  CEPH_CAP_ANY_FILE_WR);
    }
  }

  if (follows >= dn->first)
    dn->first = follows + 1;
  newi->first = dn->first;

  dout(10) << "mknod mode " << newi->inode.mode << " rdev " << newi->inode.rdev << dendl;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mknod");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);
 
  le->metablob.add_dentry(inodn, true);
  le->metablob.add_inode(newi, true);

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(),
                                    PREDIRTY_DIR, 1);
  le->metablob.add_dentry(dn, true);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, newi, dn, inodn));
}



// MKDIR
/* This function takes responsibility for the passed mdr*/
void Server::handle_client_mkdir(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  // allocate an inode number
  const inodeno_t ino = prepare_new_inodeno(mdr, inodeno_t(req->head.ino));

  // create/xlock a dentry in the inode container
  CDentry *inodn = mdcache->get_container()->xlock_dentry(mdr, ino, xlocks);
  if (!inodn)
    return;

  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // new inode
  SnapRealm *realm = mdcache->get_snaprealm();
  snapid_t follows = realm->get_newest_seq();
  mdr->now = ceph_clock_now(g_ceph_context);

  CInode *newi = mdr->in[0];
  if (!newi) {
    // initialize the inode on the first pass
    unsigned mode = req->head.args.mkdir.mode;
    mode &= ~S_IFMT;
    mode |= S_IFDIR;
    mdr->in[0] = newi = prepare_new_inode(mdr, dn->get_dir(), ino, mode);

    newi->inode.add_parent(dn->get_stripe()->dirstripe(),
                           mds->get_nodeid(), dn->get_name());

    // issue a cap on the directory.  do this before initializing
    // stripes, so replicas get the correct lock state
    int cmode = CEPH_FILE_MODE_RDWR;
    Capability *cap = mds->locker->issue_new_caps(
        newi, cmode, mdr->session, realm, req->is_replay());
    if (cap) {
      cap->set_wanted(0);

      // put locks in excl mode
      newi->filelock.set_state(LOCK_EXCL);
      newi->authlock.set_state(LOCK_EXCL);
      newi->xattrlock.set_state(LOCK_EXCL);
      cap->issue_norevoke(CEPH_CAP_AUTH_EXCL|CEPH_CAP_AUTH_SHARED|
                          CEPH_CAP_XATTR_EXCL|CEPH_CAP_XATTR_SHARED);
    }

    // stripe over all active nodes
    set<int> nodes;
    mds->mdsmap->get_active_mds_set(nodes);

    vector<int> stripe_auth(nodes.begin(), nodes.end());
    newi->set_stripe_auth(stripe_auth);

    // send slave requests to create stripes on other nodes
    for (size_t i = 0; i < stripe_auth.size(); i++) {
      int who = stripe_auth[i];

      if (who == mds->get_nodeid()) {
        // local request
        CStripe *newstripe = newi->get_or_open_stripe(i);
        CDir *newdir = newstripe->get_or_open_dirfrag(frag_t());
        newdir->mark_complete();
        newstripe->mark_open();
      } else {
        // remote slave request
        MMDSSlaveRequest *req = new MMDSSlaveRequest(
            mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_MKDIR);
        dn->set_object_info(req->get_object_info());
        // construct a path so the slave can discover
        dn->make_path(req->path);
        req->now = mdr->now;
        // stripe ids to create
        req->stripes.push_back(i);
        // send replicas
        mdcache->replicate_stripe(inodn->get_stripe(), who, req->srci_replica);
        mdcache->replicate_dir(inodn->get_dir(), who, req->srci_replica);
        mdcache->replicate_dentry(inodn, who, req->srci_replica);
        mdcache->replicate_inode(newi, who, req->srci_replica);
        mds->send_message_mds(req, who);

        assert(mdr->more()->waiting_on_slave.count(who) == 0);
        mdr->more()->waiting_on_slave.insert(who);
      }
    }

    // wait for slave acks
    if (!mdr->more()->waiting_on_slave.empty()) {
      dout(15) << "waiting on dirstripe creation from "
          << mdr->more()->waiting_on_slave << dendl;
      return;
    }
  }
  assert(newi);
  assert(mdr->more()->waiting_on_slave.empty());

  dout(12) << " follows " << follows << dendl;
  if (follows >= dn->first)
    dn->first = follows + 1;
  newi->first = dn->first;

  // primary link to inode container
  inodn->push_projected_linkage(newi);
  // remote link to filesystem namespace
  dn->push_projected_linkage(newi->ino(), newi->d_type());

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "mkdir");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);

  le->metablob.add_dentry(inodn, true);
  le->metablob.add_inode(newi, true);

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(),
                                    PREDIRTY_DIR, 1);
  le->metablob.add_dentry(dn, true);

  // add new stripes to the journal
  list<CStripe*> stripes;
  newi->get_stripes(stripes);
  for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
    CStripe *newstripe = *s;
    CDir *newdir = newstripe->get_dirfrag(frag_t());
    le->metablob.add_new_dir(newdir); // dirty AND complete AND new
  }

  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  // make sure this inode gets into the journal
  le->metablob.add_opened_ino(newi->ino());
  LogSegment *ls = mds->mdlog->get_current_segment();
  ls->open_files.push_back(&newi->item_open_file);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, newi, dn, inodn));
}

class C_MDS_SlaveMkdirFinish : public Context {
 private:
  Server *server;
  MDRequest *mdr;
  CInode *in;
 public:
  C_MDS_SlaveMkdirFinish(Server *server, MDRequest *mdr, CInode *in)
      : server(server), mdr(mdr), in(in) {}
  void finish(int r) {
    server->slave_mkdir_finish(mdr, in);
  }
};

void Server::handle_slave_mkdir(MDRequest *mdr)
{
  MMDSSlaveRequest *req = mdr->slave_request;
  int from = req->get_source().num();

  dout(7) << "handle_slave_mkdir " << *mdr
      << " from mds." << from << dendl;

  // decode replicas
  list<Context*> finished;
  bufferlist::iterator blp = req->srci_replica.begin();
  CInode *container = mdcache->get_container()->get_inode();
  CStripe *stripe = mdcache->add_replica_stripe(blp, container, from, finished);
  CDir *dir = mdcache->add_replica_dir(blp, stripe, finished);
  CDentry *dn = mdcache->add_replica_dentry(blp, dir, finished);
  CInode *in = mdcache->add_replica_inode(blp, dn, from, finished);
  mds->queue_waiters(finished);

  // journal new stripes
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(
      mdlog, "slave_mkdir", mdr->reqid, mdr->slave_to_mds,
      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::MKDIR);

  le->commit.add_dentry(dn, false);
  le->commit.add_inode(in, false);

  for (vector<stripeid_t>::iterator s = req->stripes.begin();
       s != req->stripes.end(); ++s) {
    CStripe *newstripe = in->add_stripe(new CStripe(in, *s, mds->get_nodeid()));
    newstripe->mark_open();

    CDir *newdir = newstripe->get_or_open_dirfrag(frag_t());
    newdir->mark_complete();

    le->commit.add_new_dir(newdir);
  }

  // initialize rollback
  mkdir_rollback rollback;
  rollback.reqid = mdr->reqid;
  rollback.ino = in->ino();
  swap(rollback.stripes, req->stripes);
  ::encode(rollback, le->rollback);
  mdr->more()->rollback_bl = le->rollback;

  mdlog->start_submit_entry(le, new C_MDS_SlaveMkdirFinish(this, mdr, in));
}

class C_MDS_SlaveMkdirCommit : public Context {
 private:
  Server *server;
  MDRequest *mdr;
 public:
  C_MDS_SlaveMkdirCommit(Server *server, MDRequest *mdr)
      : server(server), mdr(mdr) {}
  void finish(int r) {
    if (r == 0)
      server->slave_mkdir_commit(mdr);
    else
      server->do_mkdir_rollback(mdr->more()->rollback_bl,
                                mdr->slave_to_mds, mdr);
  }
};

void Server::slave_mkdir_finish(MDRequest *mdr, CInode *in)
{
  dout(10) << "slave_mkdir_finish " << *mdr << dendl;

  // mark new stripes dirty
  list<CStripe*> stripes;
  in->get_stripes(stripes);
  for (list<CStripe*>::iterator s = stripes.begin(); s != stripes.end(); ++s) {
    CStripe *stripe = *s;
    stripe->mark_dirty(mdr->ls);
    CDir *dir = stripe->get_dirfrag(frag_t());
    dir->mark_dirty(mdr->ls);
  }

  // reply with an ack
  MMDSSlaveRequest *ack = new MMDSSlaveRequest(
      mdr->reqid, mdr->attempt, MMDSSlaveRequest::OP_MKDIRACK);
  mds->send_message_mds(ack, mdr->slave_to_mds);

  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveMkdirCommit(this, mdr);

  mdr->slave_request->put();
  mdr->slave_request = 0;
}

struct C_MDS_CommittedSlave : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_CommittedSlave(Server *s, MDRequest *m) : server(s), mdr(m) {}
  void finish(int r) {
    server->_committed_slave(mdr);
  }
};

void Server::slave_mkdir_commit(MDRequest *mdr)
{
  dout(10) << "slave_mkdir_commit " << *mdr << dendl;

  // drop our pins, etc.
  mdr->cleanup();

  // write a commit to the journal
  ESlaveUpdate *le = new ESlaveUpdate(
      mdlog, "slave_mkdir_commit", mdr->reqid, mdr->slave_to_mds,
      ESlaveUpdate::OP_COMMIT, ESlaveUpdate::MKDIR);
  mdlog->start_submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
  mdlog->flush();
}

void Server::handle_slave_mkdir_ack(MDRequest *mdr, MMDSSlaveRequest *req)
{
  int from = req->get_source().num();

  dout(7) << "handle_slave_mkdir_ack " << *req << " " << *mdr
      << " from mds." << from << dendl;

  // note slave
  mdr->more()->slaves.insert(from);
  
  // witnessed!
  assert(mdr->more()->witnessed.count(from) == 0);
  mdr->more()->witnessed.insert(from);

  set<int> &waiters = mdr->more()->waiting_on_slave;
  assert(waiters.count(from));
  waiters.erase(from);

  if (waiters.empty())
    dispatch_client_request(mdr);
  else
    dout(15) << "mkdir still waiting on " << waiters << dendl;
}

class C_MDS_MkdirRollback : public Context {
 private:
  Server *server;
  Mutation *mut;
  MDRequest *mdr;
 public:
  C_MDS_MkdirRollback(Server *server, Mutation *mut, MDRequest *mdr)
      : server(server), mut(mut), mdr(mdr) {}
  void finish(int r) {
    server->_mkdir_rollback_finish(mut, mdr);
  }
};

void Server::do_mkdir_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  mkdir_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);

  dout(10) << "do_mkdir_rollback on " << rollback.ino
      << " stripes " << rollback.stripes << dendl;

  mdcache->add_rollback(rollback.reqid, master);
  assert(mdr || mds->is_resolve());

  Mutation *mut = new Mutation(rollback.reqid);
  mut->ls = mdlog->get_current_segment();

  CInode *in = mdcache->get_inode(rollback.ino);
  assert(in);
  dout(10) << " target is " << *in << dendl;

  // remove the stripes created by handle_slave_mkdir()
  for (vector<stripeid_t>::iterator s = rollback.stripes.begin();
       s != rollback.stripes.end(); ++s) {
    CStripe *stripe = in->get_stripe(*s);
    assert(stripe);
    in->close_stripe(stripe);
  }

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(
      mdlog, "slave_mkdir_rollback", rollback.reqid, master,
      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::MKDIR);
  mdlog->start_submit_entry(le, new C_MDS_MkdirRollback(this, mut, mdr));
  mdlog->flush();
}

void Server::_mkdir_rollback_finish(Mutation *mut, MDRequest *mdr)
{
  dout(10) << "_mkdir_rollback_finish" << dendl;

  mut->apply();
  if (mdr)
    mdcache->request_finish(mdr);

  mdcache->finish_rollback(mut->reqid);

  mut->cleanup();
  delete mut;
}

// SYMLINK

void Server::handle_client_symlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  set<SimpleLock*> rdlocks, wrlocks, xlocks;
  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false);
  if (!dn) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  // allocate an inode number
  const inodeno_t ino = prepare_new_inodeno(mdr, inodeno_t(req->head.ino));

  // create/xlock a dentry in the inode container
  CDentry *inodn = mdcache->get_container()->xlock_dentry(mdr, ino, xlocks);
  if (!inodn)
    return;

  CInode *diri = dn->get_dir()->get_inode();
  rdlocks.insert(&diri->authlock);
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  mdr->now = ceph_clock_now(g_ceph_context);
  snapid_t follows = mdcache->get_snaprealm()->get_newest_seq();

  const unsigned mode = S_IFLNK | 0777;
  CInode *newi = prepare_new_inode(mdr, dn->get_dir(), ino, mode);
  assert(newi);

  // it's a symlink
  dn->push_projected_linkage(newi);

  newi->symlink = req->get_path2();
  newi->inode.size = newi->symlink.length();
  newi->inode.rstat.rbytes = newi->inode.size;
  newi->inode.add_parent(dn->get_stripe()->dirstripe(),
                         mds->get_nodeid(), dn->get_name());

  if (follows >= dn->first)
    dn->first = follows + 1;
  newi->first = dn->first;

  // prepare finisher
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "symlink");
  mdlog->start_entry(le);
  le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
  journal_allocated_inos(mdr, &le->metablob);

  le->metablob.add_dentry(inodn, true);
  le->metablob.add_inode(newi, true);

  mdcache->predirty_journal_parents(mdr, &le->metablob, newi, dn->get_dir(),
                                    PREDIRTY_DIR, 1);
  le->metablob.add_dentry(dn, true);

  journal_and_reply(mdr, newi, dn, le, new C_MDS_mknod_finish(mds, mdr, newi, dn, inodn));
}





// LINK

void Server::handle_client_link(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;

  dout(7) << "handle_client_link " << req->get_filepath()
	  << " to " << req->get_filepath2()
	  << dendl;

  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  CDentry *dn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, false, false, false);
  if (!dn) return;
  CInode *targeti = rdlock_path_pin_ref(mdr, 1, rdlocks, false);
  if (!targeti) return;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  CDir *dir = dn->get_dir();
  dout(7) << "handle_client_link link " << dn->get_name() << " in " << *dir << dendl;
  dout(7) << "target is " << *targeti << dendl;
  if (targeti->is_dir()) {
    dout(7) << "target is a dir, failing..." << dendl;
    reply_request(mdr, -EINVAL);
    return;
  }
  
  xlocks.insert(&targeti->linklock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  // pick mtime
  if (mdr->now == utime_t())
    mdr->now = ceph_clock_now(g_ceph_context);

  // go!
  assert(g_conf->mds_kill_link_at != 1);

  // local or remote?
  if (targeti->is_auth()) 
    _link_local(mdr, dn, targeti);
  else 
    _link_remote(mdr, true, dn, targeti);
}


class C_MDS_link_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
  CInode *targeti;
public:
  C_MDS_link_local_finish(MDS *mds, MDRequest *mdr, CDentry *dn, CInode *ti)
      : mds(mds), mdr(mdr), dn(dn), targeti(ti) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_local_finish(mdr, dn, targeti);
  }
};


void Server::_link_local(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_local " << *dn << " to " << *targeti << dendl;

  mdr->ls = mdlog->get_current_segment();

  // project inode update
  inode_t *pi = targeti->project_inode();
  pi->nlink++;
  pi->ctime = mdr->now;
  targeti->project_added_parent(dn->inoparent());

  snapid_t follows = mdcache->get_snaprealm()->get_newest_seq();
  if (follows >= dn->first)
    dn->first = follows;

  // log + wait
  EUpdate *le = new EUpdate(mdlog, "link_local");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);      // new dn
  mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, 0, PREDIRTY_PRIMARY);           // targeti

  // do this after predirty_*, to avoid funky extra dnl arg
  dn->push_projected_linkage(targeti->ino(), targeti->d_type());

  le->metablob.add_dentry(dn, true);  // new remote
  mdcache->journal_dirty_inode(mdr, &le->metablob, targeti);

  journal_and_reply(mdr, targeti, dn, le, new C_MDS_link_local_finish(mds, mdr, dn, targeti));
}

void Server::_link_local_finish(MDRequest *mdr, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_local_finish " << *dn << " to " << *targeti << dendl;

  // link and unlock the NEW dentry
  dn->pop_projected_linkage();
  dn->mark_dirty(mdr->ls);

  // target inode
  targeti->pop_and_dirty_projected_inode(mdr->ls);

  mdr->apply();
  
  mds->mdcache->send_dentry_link(dn);

  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);
}


// link / unlink remote

class C_MDS_link_remote_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  bool inc;
  CDentry *dn;
  CInode *targeti;
 public:
  C_MDS_link_remote_finish(MDS *mds, MDRequest *mdr, bool inc,
                           CDentry *dn, CInode *targeti)
      : mds(mds), mdr(mdr), inc(inc), dn(dn), targeti(targeti) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_link_remote_finish(mdr, inc, dn, targeti);
  }
};

void Server::_link_remote(MDRequest *mdr, bool inc, CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote " 
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  // 1. send LinkPrepare to dest (journal nlink++ prepare)
  int linkauth = targeti->authority().first;
  if (mdr->more()->witnessed.count(linkauth) == 0) {
    dout(10) << " targeti auth must prepare nlink++/--" << dendl;

    int op;
    if (inc)
      op = MMDSSlaveRequest::OP_LINKPREP;
    else 
      op = MMDSSlaveRequest::OP_UNLINKPREP;
    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, op);
    targeti->set_object_info(req->get_object_info());
    req->get_object_info().dname = dn->get_name();
    req->now = mdr->now;
    mds->send_message_mds(req, linkauth);

    assert(mdr->more()->waiting_on_slave.count(linkauth) == 0);
    mdr->more()->waiting_on_slave.insert(linkauth);
    return;
  }
  dout(10) << " targeti auth has prepared nlink++/--" << dendl;

  assert(g_conf->mds_kill_link_at != 2);

  // add to event
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, inc ? "link_remote":"unlink_remote");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  if (inc) {
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, 1);
    dn->push_projected_linkage(targeti->ino(), targeti->d_type());
    le->metablob.add_dentry(dn, true); // new remote
  } else {
    mdcache->predirty_journal_parents(mdr, &le->metablob, targeti, dn->get_dir(), PREDIRTY_DIR, -1);
    mdcache->journal_cow_dentry(mdr, &le->metablob, dn);
    le->metablob.add_dentry(dn, true);
  }

  if (mdr->more()->dst_reanchor_atid)
    le->metablob.add_table_transaction(TABLE_ANCHOR, mdr->more()->dst_reanchor_atid);

  journal_and_reply(mdr, targeti, dn, le, new C_MDS_link_remote_finish(mds, mdr, inc, dn, targeti));
}

void Server::_link_remote_finish(MDRequest *mdr, bool inc,
				 CDentry *dn, CInode *targeti)
{
  dout(10) << "_link_remote_finish "
	   << (inc ? "link ":"unlink ")
	   << *dn << " to " << *targeti << dendl;

  assert(g_conf->mds_kill_link_at != 3);

  if (inc) {
    // link the new dentry
    dn->pop_projected_linkage();
  } else {
    // unlink main dentry
    dn->get_dir()->unlink_inode(dn);
  }
  dn->mark_dirty(mdr->ls);

  mdr->apply();

  if (inc)
    mds->mdcache->send_dentry_link(dn);
  else
    mds->mdcache->send_dentry_unlink(dn, NULL);
  
  // commit anchor update?
  if (mdr->more()->dst_reanchor_atid) 
    mds->anchorclient->commit(mdr->more()->dst_reanchor_atid, mdr->ls);

  // bump target popularity
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);
  mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

  // reply
  MClientReply *reply = new MClientReply(mdr->client_request, 0);
  reply_request(mdr, reply);

  if (!inc)
    // removing a new dn?
    dn->get_dir()->try_remove_unlinked_dn(dn);
}


// remote linking/unlinking

class C_MDS_SlaveLinkPrep : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
public:
  C_MDS_SlaveLinkPrep(Server *s, MDRequest *r, CInode *t) :
    server(s), mdr(r), targeti(t) { }
  void finish(int r) {
    assert(r == 0);
    server->_logged_slave_link(mdr, targeti);
  }
};

/* This function DOES put the mdr->slave_request before returning*/
void Server::handle_slave_link_prep(MDRequest *mdr)
{
  MDSCacheObjectInfo &info = mdr->slave_request->get_object_info();

  dout(10) << "handle_slave_link_prep " << *mdr << " on " << info << dendl;

  assert(g_conf->mds_kill_link_at != 4);

  CInode *targeti = mdcache->get_inode(info.ino);
  assert(targeti);
  dout(10) << "targeti " << *targeti << dendl;
  CDentry *dn = targeti->get_parent_dn();
  CDentry::linkage_t *dnl = dn->get_linkage();
  assert(dnl->is_primary());

  mdr->now = mdr->slave_request->now;

  mdr->auth_pin(targeti);

  assert(g_conf->mds_kill_link_at != 5);

  // journal it
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_prep", mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE, ESlaveUpdate::LINK);
  mdlog->start_entry(le);

  CInode *in = dnl->get_inode();
  inode_t *pi = in->project_inode();
  mdr->add_projected_inode(in);

  link_rollback rollback;
  rollback.parent.stripe = dn->get_stripe()->dirstripe();
  rollback.parent.who = mdr->slave_to_mds;
  rollback.parent.name = dn->get_name();

  // update journaled target inode
  bool inc;
  if (mdr->slave_request->get_op() == MMDSSlaveRequest::OP_LINKPREP) {
    inc = true;
    pi->nlink++;
    in->project_added_parent(rollback.parent);
    le->commit.add_inode(in, false, &rollback.parent);
  } else {
    inc = false;
    pi->nlink--;
    in->project_removed_parent(rollback.parent);
    le->commit.add_inode(in, false, NULL, &rollback.parent);
  }

  rollback.reqid = mdr->reqid;
  rollback.ino = targeti->ino();
  rollback.old_ctime = targeti->inode.ctime;
  fnode_t *pf = targeti->get_parent_stripe()->get_projected_fnode();
  rollback.old_dir_mtime = pf->fragstat.mtime;
  rollback.old_dir_rctime = pf->rstat.rctime;
  rollback.was_inc = inc;
  ::encode(rollback, le->rollback);
  mdr->more()->rollback_bl = le->rollback;

  pi->ctime = mdr->now;

  dout(10) << " projected inode " << pi << " v " << pi->version << dendl;

  // commit case
  mdcache->predirty_journal_parents(mdr, &le->commit, dnl->get_inode(), 0, PREDIRTY_SHALLOW|PREDIRTY_PRIMARY, 0);
  mdcache->journal_dirty_inode(mdr, &le->commit, targeti);

  mdlog->submit_entry(le, new C_MDS_SlaveLinkPrep(this, mdr, targeti));
  mdlog->flush();
}

class C_MDS_SlaveLinkCommit : public Context {
  Server *server;
  MDRequest *mdr;
  CInode *targeti;
public:
  C_MDS_SlaveLinkCommit(Server *s, MDRequest *r, CInode *t) :
    server(s), mdr(r), targeti(t) { }
  void finish(int r) {
    server->_commit_slave_link(mdr, r, targeti);
  }
};

void Server::_logged_slave_link(MDRequest *mdr, CInode *targeti) 
{
  dout(10) << "_logged_slave_link " << *mdr
	   << " " << *targeti << dendl;

  assert(g_conf->mds_kill_link_at != 6);

  // update the target
  mdr->apply();

  // hit pop
  mds->balancer->hit_inode(mdr->now, targeti, META_POP_IWR);

  // ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						 MMDSSlaveRequest::OP_LINKPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);
  
  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveLinkCommit(this, mdr, targeti);

  // done.
  mdr->slave_request->put();
  mdr->slave_request = 0;
}


void Server::_commit_slave_link(MDRequest *mdr, int r, CInode *targeti)
{  
  dout(10) << "_commit_slave_link " << *mdr
	   << " r=" << r
	   << " " << *targeti << dendl;

  assert(g_conf->mds_kill_link_at != 7);

  if (r == 0) {
    // drop our pins, etc.
    mdr->cleanup();

    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_commit", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_COMMIT, ESlaveUpdate::LINK);
    mdlog->start_submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    do_link_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

void Server::_committed_slave(MDRequest *mdr)
{
  dout(10) << "_committed_slave " << *mdr << dendl;

  assert(g_conf->mds_kill_link_at != 8);

  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt, 
					       MMDSSlaveRequest::OP_COMMITTED);
  mds->send_message_mds(req, mdr->slave_to_mds);
  mds->mdcache->request_finish(mdr);
}

struct C_MDS_LoggedLinkRollback : public Context {
  Server *server;
  Mutation *mut;
  MDRequest *mdr;
  C_MDS_LoggedLinkRollback(Server *s, Mutation *m, MDRequest *r) : server(s), mut(m), mdr(r) {}
  void finish(int r) {
    server->_link_rollback_finish(mut, mdr);
  }
};

void Server::do_link_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  link_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);

  dout(10) << "do_link_rollback on " << rollback.reqid 
	   << (rollback.was_inc ? " inc":" dec") 
	   << " ino " << rollback.ino
	   << dendl;

  assert(g_conf->mds_kill_link_at != 9);

  mds->mdcache->add_rollback(rollback.reqid, master); // need to finish this update before resolve finishes
  assert(mdr || mds->is_resolve());

  Mutation *mut = new Mutation(rollback.reqid);
  mut->ls = mds->mdlog->get_current_segment();

  CInode *in = mds->mdcache->get_inode(rollback.ino);
  assert(in);
  dout(10) << " target is " << *in << dendl;
  assert(!in->is_projected());
  
  inode_t *pi = in->project_inode();
  mut->add_projected_inode(in);

  // parent dir rctime
  CDir *parent = in->get_projected_parent_dir();
  CStripe *stripe = parent->get_stripe();
  fnode_t *pf = stripe->project_fnode();
  mut->add_projected_fnode(stripe);
  if (pf->fragstat.mtime == pi->ctime) {
    pf->fragstat.mtime = rollback.old_dir_mtime;
    if (pf->rstat.rctime == pi->ctime)
      pf->rstat.rctime = rollback.old_dir_rctime;
    mut->add_updated_lock(&in->filelock);
    mut->add_updated_lock(&in->nestlock);
  }

  // inode
  pi->ctime = rollback.old_ctime;
  if (rollback.was_inc) {
    pi->nlink--;
    in->project_removed_parent(rollback.parent);
  } else {
    pi->nlink++;
    in->project_added_parent(rollback.parent);
  }

  // journal it
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_link_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::LINK);
  mdlog->start_entry(le);
  le->commit.add_stripe_context(stripe);
  le->commit.add_dentry(in->get_projected_parent_dn(), true);
  if (rollback.was_inc)
    le->commit.add_inode(in, true, &rollback.parent);
  else
    le->commit.add_inode(in, true, NULL, &rollback.parent);
  
  mdlog->submit_entry(le, new C_MDS_LoggedLinkRollback(this, mut, mdr));
  mdlog->flush();
}

void Server::_link_rollback_finish(Mutation *mut, MDRequest *mdr)
{
  dout(10) << "_link_rollback_finish" << dendl;

  assert(g_conf->mds_kill_link_at != 10);

  mut->apply();
  if (mdr)
    mds->mdcache->request_finish(mdr);

  mds->mdcache->finish_rollback(mut->reqid);

  mut->cleanup();
  delete mut;
}


/* This function DOES NOT put the passed message before returning*/
void Server::handle_slave_link_prep_ack(MDRequest *mdr, MMDSSlaveRequest *m)
{
  dout(10) << "handle_slave_link_prep_ack " << *mdr 
	   << " " << *m << dendl;
  int from = m->get_source().num();

  assert(g_conf->mds_kill_link_at != 11);

  // note slave
  mdr->more()->slaves.insert(from);
  
  // witnessed!
  assert(mdr->more()->witnessed.count(from) == 0);
  mdr->more()->witnessed.insert(from);
  
  // remove from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  assert(mdr->more()->waiting_on_slave.empty());

  dispatch_client_request(mdr);  // go again!
}


bool _discover_all_stripes(MDRequest *mdr, CInode *in)
{
  C_GatherBuilder gather(g_ceph_context);
  MDCache *mdcache = in->mdcache;

  // discover any stripes that aren't already cached
  for (size_t stripeid = 0; stripeid < in->get_stripe_count(); ++stripeid) {
    CStripe *stripe = in->get_stripe(stripeid);
    if (!stripe)
      mdcache->discover_dir_stripe(in, stripeid, gather.new_sub(),
                                   in->get_stripe_auth(stripeid));
    else
      mdr->pin(stripe);
  }

  if (!gather.has_subs())
    return true;

  gather.set_finisher(new C_MDS_RetryRequest(mdcache, mdr));
  gather.activate();
  return false;
}

// check if a directory is non-empty (i.e. we can rmdir it).
bool _dir_is_nonempty(MDRequest *mdr, CInode *in)
{
  MDS *mds = in->mdcache->mds;

  dout(10) << "dir_is_nonempty " << *in << dendl;

  for (size_t stripeid = 0; stripeid < in->get_stripe_count(); ++stripeid) {
    CStripe *stripe = in->get_stripe(stripeid);
    assert(stripe);
    fnode_t *pf = stripe->get_projected_fnode();
    if (pf->fragstat.size() > 0) {
      dout(10) << "dir_is_nonempty projected dir size still "
          << pf->fragstat.size() << " on " << *in << dendl;
      return true;
    }
  }

  return false;
}


// UNLINK

void Server::handle_client_unlink(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  client_t client = mdr->get_client();

  // rmdir or unlink?
  bool rmdir = req->get_op() == CEPH_MDS_OP_RMDIR;

  if (req->get_filepath().depth() == 0) {
    reply_request(mdr, -EINVAL);
    return;
  }

  // traverse to path
  vector<CDentry*> trace;
  CInode *in;
  int r = mdcache->path_traverse(mdr, NULL, NULL, req->get_filepath(),
                                 &trace, &in, MDS_TRAVERSE_FORWARD);
  if (r > 0) return;
  if (r < 0) {
    reply_request(mdr, r);
    return;
  }
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }

  CDentry *dn = trace.back();
  assert(dn);
  if (!dn->is_auth()) {
    mdcache->request_forward(mdr, dn->authority().first);
    return;
  }

  CDentry::linkage_t *dnl = dn->get_linkage(client, mdr);
  assert(!dnl->is_null());

  if (rmdir) {
    dout(7) << "handle_client_rmdir on " << *dn << dendl;
  } else {
    dout(7) << "handle_client_unlink on " << *dn << dendl;
  }
  dout(7) << "dn links to " << *in << dendl;

  // rmdir vs is_dir
  if (in->is_dir()) {
    if (!rmdir) {
      dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << dendl;
      reply_request(mdr, -EISDIR);
      return;
    }
    // discover stripes for empty directory checks
    if (!_discover_all_stripes(mdr, in))
      return;
  } else {
    if (rmdir) {
      // unlink
      dout(7) << "handle_client_rmdir on non-dir " << *in << ", returning error" << dendl;
      reply_request(mdr, -ENOTDIR);
      return;
    }
  }

  // lock
  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  for (size_t i = 0; i < trace.size()-1; i++)
    rdlocks.insert(&trace[i]->lock);
  xlocks.insert(&dn->lock);
  wrlocks.insert(&dn->get_dir()->get_inode()->filelock);
  wrlocks.insert(&dn->get_dir()->get_inode()->nestlock);
  xlocks.insert(&in->linklock);

  // rdlock stripes to prevent creates while verifying emptiness
  for (size_t i = 0; i < in->get_stripe_count(); ++i)
    rdlocks.insert(&in->get_stripe(i)->linklock);

  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks))
    return;

  if (in->is_dir() &&
      _dir_is_nonempty(mdr, in)) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  // yay!
  if (mdr->now == utime_t())
    mdr->now = ceph_clock_now(g_ceph_context);

  if (in->is_dir()) {
    // stripe auths need to be witnesses
    set<int> witnesses(in->get_stripe_auth().begin(),
                       in->get_stripe_auth().end());
    witnesses.erase(mds->get_nodeid());

    dout(10) << " witnesses " << witnesses << ", have " << mdr->more()->witnessed << dendl;

    for (set<int>::iterator p = witnesses.begin(); p != witnesses.end(); ++p) {
      if (mdr->more()->witnessed.count(*p))
	dout(10) << " already witnessed by mds." << *p << dendl;
      else if (mdr->more()->waiting_on_slave.count(*p))
	dout(10) << " already waiting on witness mds." << *p << dendl;
      else
	_rmdir_prepare_witness(mdr, *p, dn);
    }
    if (!mdr->more()->waiting_on_slave.empty())
      return;  // we're waiting for a witness.
  }

  // ok!
  if (dnl->is_remote() && !dnl->get_inode()->is_auth())
    _link_remote(mdr, false, dn, dnl->get_inode());
  else
    _unlink_local(mdr, dn);
}

class C_MDS_unlink_local_finish : public Context {
  MDS *mds;
  MDRequest *mdr;
  CDentry *dn;
 public:
  C_MDS_unlink_local_finish(MDS *mds, MDRequest *mdr, CDentry *dn)
      : mds(mds), mdr(mdr), dn(dn) {}
  void finish(int r) {
    assert(r == 0);
    mds->server->_unlink_local_finish(mdr, dn);
  }
};

void Server::_unlink_local(MDRequest *mdr, CDentry *dn)
{
  dout(10) << "_unlink_local " << *dn << dendl;

  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();

  mdr->ls = mdlog->get_current_segment();

  // prepare log entry
  EUpdate *le = new EUpdate(mdlog, "unlink_local");
  mdlog->start_entry(le);
  le->metablob.add_client_req(mdr->reqid, mdr->client_request->get_oldest_client_tid());
  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mds->mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  // the unlinked dentry
  inode_t *pi = in->project_inode();
  mdr->add_projected_inode(in);
  pi->nlink--;
  pi->ctime = mdr->now;
  in->project_removed_parent(dn->inoparent());

  // remote link.  update remote inode.
  assert(dnl->is_remote());

  mdcache->predirty_journal_parents(mdr, &le->metablob, in,
                                    dn->get_dir(), PREDIRTY_DIR, -1);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in,
                                    0, PREDIRTY_PRIMARY);
  mdcache->journal_dirty_inode(mdr, &le->metablob, in);

  mdcache->journal_cow_dentry(mdr, &le->metablob, dn);

  dn->push_projected_linkage();
  le->metablob.add_dentry(dn, true);

  if (in->is_dir()) {
    dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
    le->metablob.renamed_dirino = in->ino();
  }

  journal_and_reply(mdr, 0, dn, le, new C_MDS_unlink_local_finish(mds, mdr, dn));
}

void Server::_unlink_local_finish(MDRequest *mdr, CDentry *dn)
{
  dout(10) << "_unlink_local_finish " << *dn << dendl;

  // unlink main dentry
  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();

  dn->mark_dirty(mdr->ls);
  mdr->apply();

  mds->mdcache->send_dentry_unlink(dn, mdr);
 
  // bump pop
  mds->balancer->hit_dir(mdr->now, dn->get_dir(), META_POP_IWR);

  // reply
  reply_request(mdr, 0);

  // removing a new dn?
  dn->get_dir()->try_remove_unlinked_dn(dn);
}

void Server::_rmdir_prepare_witness(MDRequest *mdr, int who, CDentry *dn)
{
  dout(10) << "_rmdir_prepare_witness mds." << who << " for " << *mdr << dendl;
  
  MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
					       MMDSSlaveRequest::OP_RMDIRPREP);
  req->path = filepath(dn->name, dn->get_stripe()->dirstripe().ino);

  req->now = mdr->now;
 
  mds->send_message_mds(req, who);
 
  assert(mdr->more()->waiting_on_slave.count(who) == 0);
  mdr->more()->waiting_on_slave.insert(who);
}

CDentry* lookup_inoparent(MDCache *mdcache, const inoparent_t &parent)
{
  CStripe *stripe = mdcache->get_dirstripe(parent.stripe);
  if (!stripe)
    return NULL;
  CDir *dir = stripe->get_dirfrag(stripe->pick_dirfrag(parent.name));
  if (!dir)
    return NULL;
  return dir->lookup(parent.name);
}

struct C_MDS_SlaveRmdirPrep : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *dn;
  C_MDS_SlaveRmdirPrep(Server *server, MDRequest *mdr, CDentry *dn)
    : server(server), mdr(mdr), dn(dn) {}
  void finish(int r) {
    server->_logged_slave_rmdir(mdr, dn);
  }
};

void Server::handle_slave_rmdir_prep(MDRequest *mdr)
{
  filepath &path = mdr->slave_request->path;

  dout(10) << "handle_slave_rmdir_prep " << *mdr << " " << path << dendl;

  vector<CDentry*> trace;
  CInode *in;
  int r = mdcache->path_traverse(mdr, NULL, NULL, path, &trace,
                                 &in, MDS_TRAVERSE_DISCOVERXLOCK);
  assert(r == 0);
  CDentry *dn = trace.back();
  dout(10) << " dn " << *dn << dendl;
  mdr->pin(dn);

  mdr->now = mdr->slave_request->now;

  rmdir_rollback rollback;
  rollback.reqid = mdr->reqid;
  rollback.dir = dn->get_dir()->dirfrag();
  rollback.dname = dn->name;
  rollback.ino = in->ino();
  ::encode(rollback, mdr->more()->rollback_bl);
  dout(20) << " rollback is " << mdr->more()->rollback_bl.length() << " bytes" << dendl;

  dn->push_projected_linkage();

  ESlaveUpdate *le =  new ESlaveUpdate(mdlog, "slave_rmdir", mdr->reqid, mdr->slave_to_mds,
				       ESlaveUpdate::OP_PREPARE, ESlaveUpdate::RMDIR);
  mdlog->start_entry(le);
  le->rollback = mdr->more()->rollback_bl;

  le->commit.add_inode(in, true);
  // slave: no need to journal original dentry

  dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
  le->commit.renamed_dirino = in->ino();

  mdlog->submit_entry(le, new C_MDS_SlaveRmdirPrep(this, mdr, dn));
  mdlog->flush();
}

struct C_MDS_SlaveRmdirCommit : public Context {
  Server *server;
  MDRequest *mdr;
  C_MDS_SlaveRmdirCommit(Server *s, MDRequest *r)
    : server(s), mdr(r) { }
  void finish(int r) {
    server->_commit_slave_rmdir(mdr, r);
  }
};

void Server::_logged_slave_rmdir(MDRequest *mdr, CDentry *dn)
{
  dout(10) << "_logged_slave_rmdir " << *mdr << " on " << *dn << dendl;

  // update our cache now, so we are consistent with what is in the journal
  // when we journal a subtree map
  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();

  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
						 MMDSSlaveRequest::OP_RMDIRPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);

  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveRmdirCommit(this, mdr);

  // done.
  mdr->slave_request->put();
  mdr->slave_request = 0;
}

void Server::handle_slave_rmdir_prep_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_rmdir_prep_ack " << *mdr 
	   << " " << *ack << dendl;

  int from = ack->get_source().num();

  mdr->more()->slaves.insert(from);
  mdr->more()->witnessed.insert(from);

  // remove from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}

void Server::_commit_slave_rmdir(MDRequest *mdr, int r)
{
  dout(10) << "_commit_slave_rmdir " << *mdr << " r=" << r << dendl;
 
  if (r == 0) {
    // write a commit to the journal
    ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rmdir_commit", mdr->reqid, mdr->slave_to_mds,
					ESlaveUpdate::OP_COMMIT, ESlaveUpdate::RMDIR);
    mdlog->start_entry(le);
    mdr->cleanup();

    mdlog->submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    // abort
    do_rmdir_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
  }
}

struct C_MDS_LoggedRmdirRollback : public Context {
  Server *server;
  MDRequest *mdr;
  metareqid_t reqid;
  CDentry *dn;
  C_MDS_LoggedRmdirRollback(Server *s, MDRequest *m, metareqid_t mr, CDentry *d)
    : server(s), mdr(m), reqid(mr), dn(d) {}
  void finish(int r) {
    server->_rmdir_rollback_finish(mdr, reqid, dn);
  }
};

void Server::do_rmdir_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  // unlink the other rollback methods, the rmdir rollback is only
  // needed to record the subtree changes in the journal for inode
  // replicas who are auth for empty dirfrags.  no actual changes to
  // the file system are taking place here, so there is no Mutation.

  rmdir_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);
 
  dout(10) << "do_rmdir_rollback on " << rollback.reqid << dendl;
  mds->mdcache->add_rollback(rollback.reqid, master); // need to finish this update before resolve finishes
  assert(mdr || mds->is_resolve());

  CDir *dir = mds->mdcache->get_dirfrag(rollback.dir);
  CDentry *dn = dir->lookup(rollback.dname);
  assert(dn);
  CInode *in = mds->mdcache->get_inode(rollback.ino);
  assert(in);
  dout(10) << "relinking " << *dn << " to " << *in << dendl;

  dn->push_projected_linkage(in);

  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rmdir_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RMDIR);
  mdlog->start_entry(le);

  le->commit.add_dentry(dn, true);
  le->commit.add_inode(in, true);
  // slave: no need to journal straydn
 
  dout(10) << " noting renamed (unlinked) dir ino " << in->ino() << " in metablob" << dendl;
  le->commit.renamed_dirino = in->ino();

  mdlog->submit_entry(le, new C_MDS_LoggedRmdirRollback(this, mdr, rollback.reqid, dn));
  mdlog->flush();
}

void Server::_rmdir_rollback_finish(MDRequest *mdr, metareqid_t reqid, CDentry *dn)
{
  dout(10) << "_rmdir_rollback_finish " << reqid << dendl;

  dn->pop_projected_linkage();

  if (mdr)
    mds->mdcache->request_finish(mdr);

  mds->mdcache->finish_rollback(reqid);
}


// ======================================================

void _rename_slaves(MDS *mds, MDRequest *mdr,
                    CDentry *srcdn, CInode *srcin,
                    CDentry *destdn, CInode *destin)
{
  set<int> &slaves = mdr->more()->waiting_on_slave;

  // must include the mds for every object involved, including replicas
  slaves.insert(srcdn->authority().first);
  srcdn->list_replicas(slaves);

  slaves.insert(srcin->authority().first);
  srcin->list_replicas(slaves);

  if (destin) {
    slaves.insert(destin->authority().first);
    destin->list_replicas(slaves);
  }

  destdn->list_replicas(slaves);
  slaves.erase(mds->get_nodeid());

  for (set<int>::iterator s = slaves.begin(); s != slaves.end(); ++s) {
    MMDSSlaveRequest *req = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
                                                 MMDSSlaveRequest::OP_RENAMEPREP);
    req->now = mdr->now;
    req->src.dn = srcdn->inoparent();
    req->src.ino = srcin->ino();
    req->dest.dn = destdn->inoparent();
    if (destin) {
      req->dest.ino = destin->ino();

      // list stripes to remove
      for (stripeid_t i = 0; i < destin->get_stripe_count(); ++i)
        if (*s == destin->get_stripe_auth(i))
          req->stripes.push_back(*s);
    }

    mds->send_message_mds(req, *s);
  }
}


class C_MDS_rename_finish : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *srcdn;
  CDentry *destdn;
public:
  C_MDS_rename_finish(Server *server, MDRequest *mdr,
		      CDentry *srcdn, CDentry *destdn)
      : server(server), mdr(mdr), srcdn(srcdn), destdn(destdn) {}
  void finish(int r) {
    assert(r == 0);
    server->_rename_finish(mdr, srcdn, destdn);
  }
};


/** handle_client_rename
 *
 * rename master is the destdn auth.  this is because cached inodes
 * must remain connected.  thus, any replica of srci, must also
 * replicate destdn, and possibly straydn, so that srci (and
 * destdn->inode) remain connected during the rename.
 *
 * to do this, we freeze srci, then master (destdn auth) verifies that
 * all other nodes have also replciated destdn and straydn.  note that
 * destdn replicas need not also replicate srci.  this only works when 
 * destdn is master.
 *
 * This function takes responsibility for the passed mdr.
 */
void Server::handle_client_rename(MDRequest *mdr)
{
  MClientRequest *req = mdr->client_request;
  dout(7) << "handle_client_rename " << *req << dendl;

  filepath destpath = req->get_filepath();
  filepath srcpath = req->get_filepath2();
  if (destpath.depth() == 0 || srcpath.depth() == 0) {
    reply_request(mdr, -EINVAL);
    return;
  }
  const string &destname = destpath.last_dentry();

  vector<CDentry*>& srctrace = mdr->dn[1];
  vector<CDentry*>& desttrace = mdr->dn[0];

  set<SimpleLock*> rdlocks, wrlocks, xlocks;

  CDentry *destdn = rdlock_path_xlock_dentry(mdr, 0, rdlocks, wrlocks, xlocks, true, false, true);
  if (!destdn) return;
  dout(10) << " destdn " << *destdn << dendl;
  if (mdr->snapid != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CDentry::linkage_t *destdnl = destdn->get_projected_linkage();
  CDir *destdir = destdn->get_dir();
  assert(destdir->is_auth());

  int r = mdcache->path_traverse(mdr, NULL, NULL, srcpath, &srctrace, NULL, MDS_TRAVERSE_DISCOVER);
  if (r > 0)
    return; // delayed
  if (r < 0) {
    if (r == -ESTALE) {
      dout(10) << "FAIL on ESTALE but attempting recovery" << dendl;
      Context *c = new C_MDS_TryFindInode(this, mdr);
      mdcache->find_ino_peers(srcpath.get_ino(), c);
    } else {
      dout(10) << "FAIL on error " << r << dendl;
      reply_request(mdr, r);
    }
    return;

  }
  assert(!srctrace.empty());
  CDentry *srcdn = srctrace[srctrace.size()-1];
  dout(10) << " srcdn " << *srcdn << dendl;
  if (srcdn->last != CEPH_NOSNAP) {
    reply_request(mdr, -EROFS);
    return;
  }
  CDentry::linkage_t *srcdnl = srcdn->get_projected_linkage();
  CInode *srci = srcdnl->get_inode();
  dout(10) << " srci " << *srci << dendl;

  CInode *oldin = 0;
  if (!destdnl->is_null()) {
    //dout(10) << "dest dn exists " << *destdn << dendl;
    oldin = mdcache->get_dentry_inode(destdn, mdr, true);
    if (!oldin) return;
    dout(10) << " oldin " << *oldin << dendl;
    
    // mv /some/thing /to/some/existing_other_thing
    if (oldin->is_dir() && !srci->is_dir()) {
      reply_request(mdr, -EISDIR);
      return;
    }
    if (!oldin->is_dir() && srci->is_dir()) {
      reply_request(mdr, -ENOTDIR);
      return;
    }

    if (srci == oldin && !srcdn->get_dir()->get_inode()->is_stray()) {
      reply_request(mdr, 0);  // no-op.  POSIX makes no sense.
      return;
    }

    // get replicas of its stripes for rdlocking
    if (oldin->is_dir() && !_discover_all_stripes(mdr, oldin))
      return;
  }

  // -- some sanity checks --

  // src+dest traces _must_ share a common ancestor for locking to prevent orphans
  if (destpath.get_ino() != srcpath.get_ino() &&
      !(req->get_source().is_mds() &&
	MDS_INO_IS_MDSDIR(srcpath.get_ino()))) {  // <-- mds 'rename' out of stray dir is ok!
    // do traces share a dentry?
    CDentry *common = 0;
    for (unsigned i=0; i < srctrace.size(); i++) {
      for (unsigned j=0; j < desttrace.size(); j++) {
	if (srctrace[i] == desttrace[j]) {
	  common = srctrace[i];
	  break;
	}
      }
      if (common)
	break;
    }

    if (common) {
      dout(10) << "rename src and dest traces share common dentry " << *common << dendl;
    } else {
      CInode *srcbase = srctrace[0]->get_dir()->get_inode();
      CInode *destbase = destdir->get_inode();
      if (!desttrace.empty())
	destbase = desttrace[0]->get_dir()->get_inode();

      // ok, extend srctrace toward root until it is an ancestor of desttrace.
      while (srcbase != destbase &&
	     !srcbase->is_projected_ancestor_of(destbase)) {
	srctrace.insert(srctrace.begin(),
			srcbase->get_projected_parent_dn());
	dout(10) << "rename prepending srctrace with " << *srctrace[0] << dendl;
	srcbase = srcbase->get_projected_parent_dn()->get_dir()->get_inode();
      }

      // then, extend destpath until it shares the same parent inode as srcpath.
      while (destbase != srcbase) {
	desttrace.insert(desttrace.begin(),
			 destbase->get_projected_parent_dn());
	rdlocks.insert(&desttrace[0]->lock);
	dout(10) << "rename prepending desttrace with " << *desttrace[0] << dendl;
	destbase = destbase->get_projected_parent_dn()->get_dir()->get_inode();
      }
      dout(10) << "rename src and dest traces now share common ancestor " << *destbase << dendl;
    }
  }

  // src == dest?
  if (srcdn->get_dir() == destdir && srcdn->name == destname) {
    dout(7) << "rename src=dest, noop" << dendl;
    reply_request(mdr, 0);
    return;
  }

  // dest a child of src?
  // e.g. mv /usr /usr/foo
  CDentry *pdn = destdir->get_inode()->parent;
  while (pdn) {
    if (pdn == srcdn) {
      dout(7) << "cannot rename item to be a child of itself" << dendl;
      reply_request(mdr, -EINVAL);
      return;
    }
    pdn = pdn->get_dir()->get_inode()->parent;
  }

  // is this a stray migration, reintegration or merge? (sanity checks!)
  if (mdr->reqid.name.is_mds() &&
      !(MDS_INO_IS_MDSDIR(srcpath.get_ino()) &&
	MDS_INO_IS_STRAY(destpath.get_ino())) &&
      !(destdnl->is_remote() &&
	destdnl->get_remote_ino() == srci->ino())) {
    reply_request(mdr, -EINVAL);  // actually, this won't reply, but whatev.
    return;
  }

  // -- locks --
  map<SimpleLock*, int> remote_wrlocks;

  // srctrace items.  this mirrors locks taken in rdlock_path_xlock_dentry
  for (int i=0; i<(int)srctrace.size(); i++) 
    rdlocks.insert(&srctrace[i]->lock);
  xlocks.insert(&srcdn->lock);
  int srcdirauth = srcdn->get_dir()->authority().first;
  if (srcdirauth != mds->whoami) {
    dout(10) << " will remote_wrlock srcdir scatterlocks on mds." << srcdirauth << dendl;
    remote_wrlocks[&srcdn->get_stripe()->linklock] = srcdirauth;
    remote_wrlocks[&srcdn->get_stripe()->nestlock] = srcdirauth;
  } else {
    wrlocks.insert(&srcdn->get_stripe()->linklock);
    wrlocks.insert(&srcdn->get_stripe()->nestlock);
  }

  // we need to update srci's ctime.  xlock its least contended lock to do that...
  xlocks.insert(&srci->linklock);

  // xlock oldin (for nlink--)
  if (oldin) {
    xlocks.insert(&oldin->linklock);

    // rdlock oldin stripes to prevent creates while verifying emptiness
    for (size_t i = 0; i < oldin->get_stripe_count(); ++i)
      rdlocks.insert(&oldin->get_stripe(i)->linklock);
  }

  CInode *auth_pin_freeze = !srcdn->is_auth() && srcdnl->is_primary() ? srci : NULL;
  if (!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks,
				  &remote_wrlocks, NULL, auth_pin_freeze))
    return;

  if (oldin &&
      oldin->is_dir() &&
      _dir_is_nonempty(mdr, oldin)) {
    reply_request(mdr, -ENOTEMPTY);
    return;
  }

  assert(g_conf->mds_kill_rename_at != 1);

  // send slave requests
  dout(10) << "slaves " << mdr->more()->witnessed << dendl;
  if (mdr->more()->witnessed.empty()) {
    _rename_slaves(mds, mdr, srcdn, srci, destdn, oldin);

    if (!mdr->more()->waiting_on_slave.empty()) {
      dout(10) << "waiting on slave requests to "
          << mdr->more()->waiting_on_slave << dendl;
      return;
    }
  }
  assert(g_conf->mds_kill_rename_at != 2);

  // start journal entry
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "rename");
  mdlog->start_entry(le);

  // destdn
  destdn->push_projected_linkage(srci->ino(), srci->d_type());
  le->metablob.add_dentry(destdn, true);
  if (oldin)
    dout(10) << "destdn from " << oldin->ino()
        << " to " << srci->ino() << dendl;
  else
    dout(10) << "destdn from null to " << srci->ino() << dendl;

  // srcdn
  srcdn->push_projected_linkage();
  if (srcdn->is_auth()) {
    // change linkage from srci to null
    le->metablob.add_dentry(srcdn, true);
    dout(10) << "srcdn from " << srci->ino() << " to null" << dendl;
  }

  // srcin
  if (srci->is_auth()) {
    mdr->add_projected_inode(srci);
    srci->project_inode();

    // remove srcdn parent
    mdcache->predirty_journal_parents(mdr, &le->metablob, srci, NULL,
                                      PREDIRTY_DIR, -1);

    srci->project_renamed_parent(srcdn->inoparent(), destdn->inoparent());

    // add destdn parent
    mdcache->predirty_journal_parents(mdr, &le->metablob, srci, NULL,
                                      PREDIRTY_DIR, 1);

    dout(10) << "src inode parent from " << *srcdn << " to " << *destdn << dendl;
    le->metablob.add_inode(srci, destdn, srcdn);
  }

  // destin
  if (oldin && oldin->is_auth()) {
    mdr->add_projected_inode(oldin);
    oldin->project_inode()->nlink--;

    // remove destdn parent
    mdcache->predirty_journal_parents(mdr, &le->metablob, oldin, NULL,
                                      PREDIRTY_DIR, -1);

    oldin->project_removed_parent(destdn->inoparent());

    le->metablob.add_inode(oldin, NULL, destdn);
    dout(10) << "dest inode removed parent " << *destdn << dendl;
  }

  le->metablob.add_client_req(mdr->reqid, req->get_oldest_client_tid());

  if (!mdr->more()->witnessed.empty()) {
    dout(20) << " noting uncommitted_slaves " << mdr->more()->witnessed << dendl;
    le->reqid = mdr->reqid;
    le->had_slaves = true;
    mdcache->add_uncommitted_master(mdr->reqid, mdr->ls, mdr->more()->witnessed);
  }

  C_MDS_rename_finish *fin = new C_MDS_rename_finish(this, mdr, srcdn, destdn);
  journal_and_reply(mdr, srci, destdn, le, fin);
}


void Server::_rename_finish(MDRequest *mdr, CDentry *srcdn, CDentry *destdn)
{
  dout(10) << "_rename_finish " << *mdr << dendl;

  // apply
  srcdn->get_dir()->unlink_inode(srcdn);
  srcdn->pop_projected_linkage();
  if (!destdn->get_linkage()->is_null())
    destdn->get_dir()->unlink_inode(destdn);
  destdn->pop_projected_linkage();
  mdr->apply();

  // reply
  reply_request(mdr, 0);
}


bool Server::_need_force_journal(CInode *diri)
{
  // journal the directory inode if we're auth for any of its stripes
  const vector<int> &auth = diri->get_stripe_auth();
  return find(auth.begin(), auth.end(), mds->whoami) != auth.end();
}

// ------------
// SLAVE

class C_MDS_SlaveRenamePrep : public Context {
  Server *server;
  MDRequest *mdr;
  CDentry *srcdn, *destdn;
public:
  C_MDS_SlaveRenamePrep(Server *server, MDRequest *mdr,
                        CDentry *srcdn, CDentry *destdn)
      : server(server), mdr(mdr), srcdn(srcdn), destdn(destdn) {}
  void finish(int r) {
    server->_logged_slave_rename(mdr, srcdn, destdn);
  }
};

class C_MDS_SlaveRenameCommit : public Context {
  Server *server;
  MDRequest *mdr;
public:
  C_MDS_SlaveRenameCommit(Server *server, MDRequest *mdr)
      : server(server), mdr(mdr) {}
  void finish(int r) {
    server->_commit_slave_rename(mdr, r);
  }
};

/* This function DOES put the mdr->slave_request before returning*/
void Server::handle_slave_rename_prep(MDRequest *mdr)
{
  MMDSSlaveRequest *req = mdr->slave_request;
  dout(10) << "handle_slave_rename_prep " << *mdr 
	   << " " << req->src.dn << " to " << req->dest.dn
	   << dendl;

  // encode everything we'd need to roll this back
  rename_rollback rollback;
  rollback.reqid = mdr->reqid;
 
  // start journal entry
  mdr->ls = mdlog->get_current_segment();
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_prep",
                                      mdr->reqid, mdr->slave_to_mds,
				      ESlaveUpdate::OP_PREPARE,
                                      ESlaveUpdate::RENAME);
  mdlog->start_entry(le);

  // srcdn
  rollback.src.dn = req->src.dn;
  CDentry *srcdn = lookup_inoparent(mdcache, req->src.dn);
  if (srcdn) {
    fnode_t *pf = srcdn->get_stripe()->get_projected_fnode();
    rollback.src.mtime = pf->fragstat.mtime;
    rollback.src.rctime = pf->rstat.rctime;

    CDentry::linkage_t *dnl = srcdn->get_projected_linkage();
    rollback.src.ino = dnl->get_remote_ino();
    rollback.src.d_type = dnl->get_remote_d_type();

    // change linkage from srcino to null
    srcdn->push_projected_linkage();
    le->commit.add_dentry(srcdn, srcdn->is_auth());
  }

  // destdn
  rollback.dest.dn = req->dest.dn;
  CDentry *destdn = lookup_inoparent(mdcache, req->dest.dn);
  if (destdn) {
    fnode_t *pf = destdn->get_stripe()->get_projected_fnode();
    rollback.dest.mtime = pf->fragstat.mtime;
    rollback.dest.rctime = pf->rstat.rctime;

    CDentry::linkage_t *dnl = destdn->get_projected_linkage();
    rollback.dest.ino = dnl->get_remote_ino();
    rollback.dest.d_type = dnl->get_remote_d_type();

    // change linkage from destino to srcino
    destdn->push_projected_linkage(req->src.ino, req->src.d_type);
    le->commit.add_dentry(destdn, destdn->is_auth());
  }

  // srcino
  CInode *srcin = mdcache->get_inode(req->src.ino);
  if (srcin && srcin->is_auth()) {
    rollback.src.ctime = srcin->inode.ctime;

    mdr->add_projected_inode(srcin);
    srcin->project_inode();

    // remove srcdn parent
    mdcache->predirty_journal_parents(mdr, &le->commit, srcin, NULL,
                                      PREDIRTY_DIR, -1);

    srcin->project_renamed_parent(req->src.dn, req->dest.dn);

    // add destdn parent
    mdcache->predirty_journal_parents(mdr, &le->commit, srcin, NULL,
                                      PREDIRTY_DIR, 1);

    le->commit.add_inode(srcin, true, &req->dest.dn, &req->src.dn);
  }

  // destino
  CInode *destin = mdcache->get_inode(req->dest.ino);
  if (destin && destin->is_auth()) {
    rollback.dest.ctime = destin->inode.ctime;

    mdr->add_projected_inode(destin);
    destin->project_inode()->nlink--;

    // remove destdn parent
    mdcache->predirty_journal_parents(mdr, &le->commit, destin, NULL,
                                      PREDIRTY_DIR, -1);

    destin->project_removed_parent(req->dest.dn);

    le->commit.add_inode(destin, true, NULL, &req->dest.dn);
  }

  // destino stripes
  for (vector<stripeid_t>::iterator s = req->stripes.begin();
       s != req->stripes.end(); ++s) {
    assert(destin);
    CStripe *stripe = destin->get_stripe(*s);
    assert(stripe);
    assert(stripe->is_auth());

    // flag and journal as unlinked
    stripe->state_set(CStripe::STATE_UNLINKED);

    le->commit.add_stripe(stripe, true, false, true);
  }
  swap(rollback.stripes, req->stripes);

  // set up commit waiter
  mdr->more()->slave_commit = new C_MDS_SlaveRenameCommit(this, mdr);

  ::encode(rollback, mdr->more()->rollback_bl);
  dout(20) << " rollback is " << mdr->more()->rollback_bl.length() << " bytes" << dendl;

  // journal.
  le->rollback = mdr->more()->rollback_bl;

  mdlog->submit_entry(le, new C_MDS_SlaveRenamePrep(this, mdr, srcdn, destdn));
  mdlog->flush();
}

void Server::_logged_slave_rename(MDRequest *mdr, CDentry *srcdn,
                                  CDentry *destdn)
{
  dout(10) << "_logged_slave_rename " << *mdr << dendl;

  // apply mutations
  if (srcdn) {
    srcdn->get_dir()->unlink_inode(srcdn);
    srcdn->pop_projected_linkage();
  }
  if (destdn) {
    destdn->get_dir()->unlink_inode(destdn);
    destdn->pop_projected_linkage();
  }
  mdr->apply();

  // send ack
  MMDSSlaveRequest *reply = new MMDSSlaveRequest(mdr->reqid, mdr->attempt,
                                                 MMDSSlaveRequest::OP_RENAMEPREPACK);
  mds->send_message_mds(reply, mdr->slave_to_mds);

  // done.
  mdr->slave_request->put();
  mdr->slave_request = 0;
}

void Server::_commit_slave_rename(MDRequest *mdr, int r)
{
  dout(10) << "_commit_slave_rename " << *mdr << " r=" << r << dendl;

  ESlaveUpdate *le;
  list<Context*> finished;
  if (r == 0) {
    // write a commit to the journal
    le = new ESlaveUpdate(mdlog, "slave_rename_commit", mdr->reqid, mdr->slave_to_mds,
			  ESlaveUpdate::OP_COMMIT, ESlaveUpdate::RENAME);
    mdlog->start_entry(le);

    mds->queue_waiters(finished);
    mdr->cleanup();

    mdlog->submit_entry(le, new C_MDS_CommittedSlave(this, mdr));
    mdlog->flush();
  } else {
    // abort
    //  rollback_bl may be empty if we froze the inode but had to provide an expanded
    // witness list from the master, and they failed before we tried prep again.
    if (mdr->more()->rollback_bl.length())
      do_rename_rollback(mdr->more()->rollback_bl, mdr->slave_to_mds, mdr);
    else
      dout(10) << " rollback_bl empty, not rollback back rename (master failed after getting extra witnesses?)" << dendl;
  }
}

struct C_MDS_LoggedRenameRollback : public Context {
  Server *server;
  Mutation *mut;
  MDRequest *mdr;
  CDentry *srcdn;
  CDentry *destdn;
  C_MDS_LoggedRenameRollback(Server *server, Mutation *mut, MDRequest *mdr,
			     CDentry *srcdn, CDentry *destdn)
      : server(server), mut(mut), mdr(mdr), srcdn(srcdn), destdn(destdn) {}
  void finish(int r) {
    server->_rename_rollback_finish(mut, mdr, srcdn, destdn);
  }
};

void Server::do_rename_rollback(bufferlist &rbl, int master, MDRequest *mdr)
{
  rename_rollback rollback;
  bufferlist::iterator p = rbl.begin();
  ::decode(rollback, p);

  dout(10) << "do_rename_rollback on " << rollback.reqid << dendl;
  // need to finish this update before sending resolve to claim the subtree
  mdcache->add_rollback(rollback.reqid, master);
  assert(mdr || mds->is_resolve());

  Mutation *mut = new Mutation(rollback.reqid);
  mut->ls = mds->mdlog->get_current_segment();
  
  // start journal entry
  ESlaveUpdate *le = new ESlaveUpdate(mdlog, "slave_rename_rollback", rollback.reqid, master,
				      ESlaveUpdate::OP_ROLLBACK, ESlaveUpdate::RENAME);
  mdlog->start_entry(le);

  CDentry *srcdn = lookup_inoparent(mdcache, rollback.src.dn);
  if (srcdn) {
    dout(10) << "   srcdn " << *srcdn << dendl;
    assert(srcdn->get_linkage()->is_null());

    // restore and journal linkage
    srcdn->push_projected_linkage(rollback.src.ino, rollback.src.d_type);
    if (srcdn->is_auth())
      le->commit.add_dentry(srcdn, true);
  } else
    dout(10) << "   srcdn not found" << dendl;

  CDentry *destdn = lookup_inoparent(mdcache, rollback.dest.dn);
  if (destdn) {
    dout(10) << "  destdn " << *destdn << dendl;

    // restore and journal linkage
    destdn->push_projected_linkage(rollback.dest.ino, rollback.dest.d_type);
    if (destdn->is_auth())
      le->commit.add_dentry(destdn, true);
  } else
    dout(10) << "  destdn not found" << dendl;

  CInode *in = mdcache->get_inode(rollback.src.ino);
  if (in) {
    inode_t *pi = 0;
    if (in->is_auth()) {
      pi = in->project_inode();
      mut->add_projected_inode(in);
    } else
      pi = in->get_projected_inode();
    if (pi->ctime == rollback.ctime)
      pi->ctime = rollback.src.ctime;

    mut->now = pi->ctime;
    mdcache->predirty_journal_parents(mut, &le->commit, in,
                                      NULL, PREDIRTY_DIR, -1);

    // restore and journal inoparent
    in->project_renamed_parent(rollback.dest.dn, rollback.src.dn);

    mdcache->predirty_journal_parents(mut, &le->commit, in,
                                      NULL, PREDIRTY_DIR, 1);

    if (in->is_auth())
      le->commit.add_inode(in, true, &rollback.src.dn, &rollback.dest.dn);
  }

  CInode *target = mdcache->get_inode(rollback.dest.ino);
  if (target) {
    inode_t *ti = NULL;
    if (target->is_auth()) {
      ti = target->project_inode();
      mut->add_projected_inode(target);
    } else 
      ti = target->get_projected_inode();
    if (ti->ctime == rollback.ctime)
      ti->ctime = rollback.dest.ctime;
    ti->nlink++;

    // restore and journal inoparent
    target->project_added_parent(rollback.dest.dn);

    mut->now = ti->ctime;
    mdcache->predirty_journal_parents(mut, &le->commit, target,
                                      NULL, PREDIRTY_DIR, 1);

    if (target->is_auth())
      le->commit.add_inode(target, true, &rollback.dest.dn);

    // clear and journal unlinked flag
    for (vector<stripeid_t>::iterator s = rollback.stripes.begin();
         s != rollback.stripes.end(); ++s) {
      CStripe *stripe = target->get_stripe(*s);
      assert(stripe);
      assert(stripe->is_auth());

      stripe->state_clear(CStripe::STATE_UNLINKED);

      le->commit.add_stripe(stripe, true);
    }
  }

  if (srcdn)
    dout(0) << " srcdn back to " << *srcdn << dendl;
  if (in)
    dout(0) << "  srci back to " << *in << dendl;
  if (destdn)
    dout(0) << " destdn back to " << *destdn << dendl;
  if (target)
    dout(0) << "  desti back to " << *target << dendl;

  mdlog->submit_entry(le, new C_MDS_LoggedRenameRollback(this, mut, mdr,
							 srcdn, destdn));
  mdlog->flush();
}

void Server::_rename_rollback_finish(Mutation *mut, MDRequest *mdr,
                                     CDentry *srcdn, CDentry *destdn)
{
  dout(10) << "_rename_rollback_finish" << mut->reqid << dendl;

  if (destdn) {
    destdn->get_dir()->unlink_inode(destdn);
    destdn->pop_projected_linkage();
  }
  if (srcdn) {
    srcdn->pop_projected_linkage();
    if (srcdn->is_auth())
      srcdn->mark_dirty(mut->ls);
  }

  mut->apply();
  mds->locker->drop_locks(mut);

  if (mdr)
    mdcache->request_finish(mdr);

  mdcache->finish_rollback(mut->reqid);

  mut->cleanup();
  delete mut;
}

/* This function DOES put the passed message before returning*/
void Server::handle_slave_rename_prep_ack(MDRequest *mdr, MMDSSlaveRequest *ack)
{
  dout(10) << "handle_slave_rename_prep_ack " << *mdr 
	   << " witnessed by " << ack->get_source()
	   << " " << *ack << dendl;
  int from = ack->get_source().num();

  // note slave
  mdr->more()->slaves.insert(from);
  mdr->more()->witnessed.insert(from);

  // remove from waiting list
  assert(mdr->more()->waiting_on_slave.count(from));
  mdr->more()->waiting_on_slave.erase(from);

  if (mdr->more()->waiting_on_slave.empty())
    dispatch_client_request(mdr);  // go again!
  else 
    dout(10) << "still waiting on slaves " << mdr->more()->waiting_on_slave << dendl;
}

