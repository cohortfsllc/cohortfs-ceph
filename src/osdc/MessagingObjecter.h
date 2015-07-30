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

namespace rados {
  namespace rados_detail {
    class MessagingObjecter : public Dispatcher,
			      public Objecter<MessagingObjecter> {
    public:
      Messenger *messenger;
      MonClient *monc;

      // -- osd sessions --
      struct OSDSession : public set_base_hook<link_mode<normal_link>>,
			  public RefCountedObject {
	shared_timed_mutex lock;
	typedef std::unique_lock<shared_timed_mutex> unique_lock;
	typedef std::shared_lock<shared_timed_mutex> shared_lock;

	static constexpr const uint32_t max_ops_inflight = 5;
	set<SubOp> subops_inflight;

	static constexpr const uint64_t max_ops_queued = 100;
	set<SubOp> subops_queued;

	int osd;
	int incarnation;
	int num_locks;
	ConnectionRef con;

	OSDSession(int o) :
	  osd(o), incarnation(0), con(nullptr) { }
	~OSDSession() {
	  // Caller is responsible for re-assigning or
	  // destroying any ops that were assigned to us
	  assert(subops_inflight.empty());
	}

	bool operator==(const OSDSession& r) {
	  return osd == r.osd;
	}
	bool operator!=(const OSDSession& r) {
	  return osd != r.osd;
	}

	bool operator>(const OSDSession& r) {
	  return osd > r.osd;
	}
	bool operator<(const OSDSession& r) {
	  return osd < r.osd;
	}
	bool operator>=(const OSDSession& r) {
	  return osd >= r.osd;
	}
	bool operator<=(const OSDSession &r) {
	  return osd <= r.osd;
	}

      };
      set<OSDSession> osd_sessions;

      void _send_subop(SubOp& op, MOSDOp *m = nullptr);

      int _calc_targets(Op& t, Op::unique_lock& ol);
      OSDSession* _map_session(SubOp& subop, Op::unique_lock& ol,
			       const shunique_lock& lc);

      void _session_subop_assign(OSDSession& to, SubOp& subop);
      void _session_subop_remove(OSDSession& from, SubOp& subop);

      int _assign_subop_target_session(SubOp& op, shared_lock& lc,
				       bool src_session_locked,
				       bool dst_session_locked);

      void _kick_requests(OSDSession& session);
      OSDSession* _get_session(int osd, const shunique_lock& shl);

      void resend_mon_ops();

    public:
      MessagingObjecter(CephContext *_cct, Messenger *m, MonClient *mc,
			ceph::timespan mon_timeout = 0s,
			ceph::timespan osd_timeout = 0s) :
	Dispatcher(_cct), Objecter(_cct, mon_timeout, osd_timeout),
	messenger(m), monc(mc) { }

      ~MessagingObjecter() {
	while (osd_sessions.empty()) {
	  auto p = osd_sessions.begin();
	  close_session(*p);
	}
      }


      // messages
    public:
      bool ms_dispatch(Message *m);
      bool ms_can_fast_dispatch_any() const {
	return false;
      }
      bool ms_can_fast_dispatch(Message *m) const {
	switch (m->get_type()) {
	case CEPH_MSG_OSD_OPREPLY:
	  return true;
	default:
	  return false;
	}
      }

      void ms_fast_dispatch(Message *m) {
	ms_dispatch(m);
      }

      void _scan_sessions(bool skipped_map, bool was_full,
			  map<ceph_tid_t, SubOp*>& need_resend) {
	// osd addr changes?
	for (auto p = osd_sessions.begin(); p != osd_sessions.end(); ) {
	  OSDSession& s = *p;
	  shunique_lock sl(s.lock, acquire_unique);
	  _scan_requests(s.subops_inflight, sl, skipped_map, was_full,
			 need_resend);
	  sl.unlock();
	  ++p;
	  if (!osdmap->is_up(s.osd) ||
	      (s.con &&
	       s.con->get_peer_addr() != osdmap->get_inst(s.osd).addr)) {
	    close_session(s);
	  }
	}
      }

      void _resend_subop(SubOp* s) {
	OSDSession* s = subop->session;
	bool mapped_session = false;
	if (!s) {
	  typename Op::unique_lock ol(subop->parent.lock);
	  s = _map_session(*subop, ol, shl);
	  ol.unlock();
	  mapped_session = true;
	} else {
	  get_session(*s);
	}
	typename OSDSession::unique_lock sl(s->lock);
	if (mapped_session) {
	  _session_subop_assign(*s, *subop);
	}
	if (subop->parent.should_resend) {
	  if (!subop->is_homeless() && !subop->parent.paused) {
	    _send_subop(*subop);
	  }
	} else {
	  _cancel_op(subop->parent);
	}
	sl.unlock();
	put_session(*s);
      }

      /**
       * Look up OSDSession by OSD id.
       */
      OSDSession* get_session(
	int osd, const shunique_lock& shl) {
	if (osd < 0) {
	  ldout(cct, 20) << __func__ << " osd=" << osd << ". Refusign to "
			 << "put up with this." << dendl;
	  throw std::system_error(errc::invalid_osd);
	}

	auto p = osd_sessions.find(osd, oc);
	if (p != osd_sessions.end()) {
	  OSDSession& s = *p;
	  s.get();
	  return &s;
	}
	if (!shl.owns_lock()) {
	  throw std::system_error(errc::need_unique_lock);
	}
	OSDSession *s = new OSDSession(osd);
	osd_sessions.insert(*s);
	s->con = messenger->get_connection(osdmap->get_inst(osd));
	s->get();
	ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		       << s->nref << dendl;
	return s;
      }

      void put_session(OSDSession& s) {
	s.put();
      }

      void get_session(OSDSession& s) {
	s.get();
      }

      void _reopen_session(OSDSession& s) {
	entity_inst_t inst = osdmap->get_inst(s.osd);
	ldout(cct, 10) << "reopen_session osd." << s.osd
		       << " session, addr now " << inst << dendl;
	if (s.con) {
	  messenger->mark_down(s.con);
	}
	s.con = messenger->get_connection(inst);
	s.incarnation++;
      }

      void close_session(OSDSession& s) {
	ldout(cct, 10) << "close_session for osd." << s.osd << dendl;
	if (s.con) {
	  messenger->mark_down(s.con);
	}
	OSDSession::unique_lock sl(s.lock);

	std::list<SubOp*> homeless_ops;

	while (!s.subops_inflight.empty()) {
	  auto& so = *(s.subops_inflight.begin());
	  ldout(cct, 10) << " op " << so.tid << dendl;
	  _session_subop_remove(s, so);
	  homeless_ops.push_back(&so);
	}

	osd_sessions.erase(s);
	sl.unlock();
	put_session(s);

	// Assign any leftover ops to the homeless session
	{
	  OSDSession::unique_lock wl(rwlock);
	  for (auto i : homeless_ops) {
	    homeless_subops.insert(*i);
	  }
	}
      }
      void wait_for_latest_osdmap(thunk&& fin) {
	ldout(cct, 10) << __func__ << dendl;
	monc->get_version("osdmap", Objecter_GetVersion(*this, std::move(fin)));
      }

      void _maybe_request_map();
      void kick_requests(OSDSession& session);

      void tick_sub() {
	unsigned laggy_ops;
	std::set<OSDSession*> toping;

	do {
	  laggy_ops = 0;
	  for (auto& s : osd_sessions) {
	    for (auto& subop : s.subops_inflight) {
	      assert(subop.session);
	      if (subop.session && subop.stamp < cutoff) {
		ldout(cct, 2) << " tid " << subop.tid << " on osd."
			      << subop.session->osd << " is laggy" << dendl;
		toping.insert(subop.session);
		++laggy_ops;
	      }
	    }
	  }

	  if (!homeless_subops.empty() || !toping.empty()) {
	    _maybe_request_map();
	  }
	} while (r == -EAGAIN);

	if (!toping.empty()) {
	  // send a ping to these osds, to ensure we detect any session resets
	  // (osd reply message policy is lossy)
	  for (auto i : toping)
	    messenger->send_message(new MPing, (*i)->con);
	}
      }

      ceph_tid_t op_submit(Op& op, Op::unique_lock& ol,
					       shunique_lock& shl) {
	op.trace.keyval("tid", op.tid);
	op.trace.event("op_submit", &trace_endpoint);

	// pick target

	// This function no longer creates the session
	const bool check_for_latest_map = _calc_targets(op, ol)
	  == TARGET_VOLUME_DNE;

	// Try to get sessions, including a retry if we need to take write lock
	for (auto& subop : op.subops) {
	  OSDSession *session = nullptr;
	  if (subop.tid == 0)
	    subop.tid = ++last_tid;
	  if (subop.osd == -1) {
	    subop.session = nullptr;
	    homeless_subops.insert(subop);
	    continue;
	  }
	  try {
	    session = _get_session(subop.osd, shl);
	  } catch (std::system_error& e) {
	    if (e.code() == errc::need_unique_lock) {
	      shl.unlock();
	      shl.lock();
	      session = _get_session(subop.osd, shl);
	    } else {
	      throw;
	    }
	  }
	  assert(session);  // may be homeless
	  OSDSession::unique_lock sl(session->lock);
	  _session_subop_assign(*session, subop);
	  sl.unlock();
	}

	// We may need to take wlock if we will need to
	// _set_op_map_check later.
	if (check_for_latest_map && shl.owns_lock_shared()) {
	  shl.unlock();
	  shl.lock();
	}

	inflight_ops.insert(op);

	// add to gather set(s)
	if (op.onack) {
	  ++num_unacked;
	} else {
	  ldout(cct, 20) << " note: not requesting ack" << dendl;
	}
	if (op.oncommit) {
	  ++num_uncommitted;
	} else {
	  ldout(cct, 20) << " note: not requesting commit" << dendl;
	}

	// send?
	ldout(cct, 10) << "_op_submit oid " << op.oid
		       << " " << op.volume << " "
		       << " tid " << op.tid << dendl;

	assert(op.flags & (CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_WRITE));

	bool need_send = false;

	if ((op.flags & CEPH_OSD_FLAG_WRITE) &&
	    osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
	  op.paused = true;
	  _maybe_request_map();
	} else if ((op.flags & CEPH_OSD_FLAG_READ) &&
		   osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
	  op.paused = true;
	  _maybe_request_map();
	} else if ((op.flags & CEPH_OSD_FLAG_WRITE) &&
		   osdmap_full_flag()) {
	  op.paused = true;
	  _maybe_request_map();
	} else if (count_if(op.subops.cbegin(),
			    op.subops.cend(),
			    not1(mem_fun_ref(&SubOp::is_homeless)))
		   >= op.volume->quorum()) {
	  need_send = true;
	} else {
	  _maybe_request_map();
	}

	MOSDOp *m = NULL;
	for (auto& subop : op.subops) {
	  if (need_send && !subop.is_homeless()) {
	    OSDSession::shared_lock sl(subop.session->lock);
	    m = _prepare_osd_subop(subop);
	    _send_subop(subop, m);
	    sl.unlock();
	  }
	  put_session(*subop.session);
	}

	// Last chance to touch Op here, after giving up session lock it can be
	// freed at any time by response handler.
	ceph_tid_t tid = op.tid;
	if (check_for_latest_map) {
	  _send_op_map_check(op);
	}

	ldout(cct, 5) << num_unacked << " unacked, " << num_uncommitted
		      << " uncommitted" << dendl;

	return tid;
      }

      void _finish_subop(SubOp& subop) {
	for (auto& op : subop.ops) {
	  bufferlist bl;
	  if (op.f) {
	    op.f(std::make_error_code(std::errc::timed_out), bl);
	    op.f = nullptr;
	  }
	}
	if (subop.session) {
	  OSDSession::unique_lock wl(subop.session->lock);
	  _session_subop_remove(*subop.session, subop);
	}
      }

      void send_subop(SubOp& subop, MOSDOp *m) {
	if (!m) {
	  assert(subop.tid > 0);
	  m = _prepare_osd_subop(subop);
	}

	ldout(cct, 15) << __func__ << " " << subop.tid << " to osd."
		       << subop.session->osd << " op " << subop.ops[0]
		       << dendl;

	ConnectionRef con = subop.session->con;
	assert(con);

	subop.incarnation = subop.session->incarnation;

	m->set_tid(subop.tid);

	messenger->send_message(m, subop.session->con);
      }

      void handle_osd_subop_reply(MOSDOpReply *m) {
	ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

	// get pio
	ceph_tid_t tid = m->get_tid();

	int osd_num = (int)m->get_source().num();

	shared_lock rl(rwlock);

	auto siter = osd_sessions.find(osd_num, oc);
	if (siter == osd_sessions.end()) {
	  ldout(cct, 7) << "handle_osd_op_reply " << tid
			<< (m->is_ondisk() ? " ondisk":(m->is_onnvram() ?
							" onnvram":" ack"))
			<< " ... unknown osd" << dendl;
	  m->put();
	  return;
	}

	OSDSession& s = *siter;
	get_session(s);
	rl.unlock();


	OSDSession::unique_lock sl(s.lock);

	auto iter = s.subops_inflight.find(tid, oc);
	if (iter == s.subops_inflight.end()) {
	  ldout(cct, 7) << "handle_osd_subop_reply " << tid
			<< (m->is_ondisk() ? " ondisk" :
			    (m->is_onnvram() ? " onnvram" : " ack"))
			<< " ... stray" << dendl;
	  sl.unlock();
	  put_session(s);
	  m->put();
	  return;
	}


	ldout(cct, 7) << "handle_osd_subop_reply " << tid
		      << (m->is_ondisk() ? " ondisk"
			  : (m->is_onnvram() ? " onnvram" : " ack"))
		      << " v " << m->get_replay_version() << " uv "
		      << m->get_user_version()
		      << " attempt " << m->get_retry_attempt()
		      << dendl;
	SubOp& subop = *iter;

	if (m->get_retry_attempt() >= 0) {
	  if (m->get_retry_attempt() != (subop.attempts - 1)) {
	    ldout(cct, 7) << " ignoring reply from attempt "
			  << m->get_retry_attempt()
			  << " from " << m->get_source_inst()
			  << "; last attempt " << (subop.attempts - 1)
			  << " sent to " << subop.session->con->get_peer_addr()
			  << dendl;
	    m->put();
	    sl.unlock();
	    put_session(s);
	    return;
	  }
	} else {
	  // we don't know the request attempt because the server is old,
	  // so just accept this one.  we may do ACK callbacks we
	  // shouldn't have, but that is better than doing callbacks out
	  // of order.
	}

	std::error_code rc = m->get_result();
	if (rc == std::errc::resource_unavailable_try_again) {
	  ldout(cct, 7) << " got " << rc << ", resubmitting" << dendl;

	  // new tid
	  _session_subop_remove(s, subop);
	  subop.tid = ++last_tid;
	  _session_subop_assign(s, subop);

	  _send_subop(subop);
	  sl.unlock();
	  put_session(s);
	  m->put();
	  return;
	}
	subop.parent.get();
	sl.unlock();
	Op::unique_lock ol(subop.parent.lock); // So we can't race with _finish_op
	if (subop.parent.finished) {
	  ol.unlock();
	  subop.parent.put();
	  m->put();
	  put_session(s);
	  return;
	}

	// per-op result demuxing
	vector<OSDOp> out_ops;
	m->claim_ops(out_ops);

	if (out_ops.size() != subop.ops.size())
	  ldout(cct, 0) << "WARNING: tid " << subop.tid << " reply ops " << out_ops
			<< " != request ops " << subop.ops
			<< " from " << m->get_source_inst() << dendl;

	for (uint32_t i = 0; i < out_ops.size(); ++i) {
	  ldout(cct, 10) << " op " << i << " rval " << out_ops[i].rval
			 << " len " << out_ops[i].outdata.length() << dendl;
	  if (subop.ops[i].f) {
	    try {
	      subop.ops[i].f(out_ops[i].rval, out_ops[i].outdata);
	    } catch (const std::system_error& e) {
	      if (!out_ops[i].rval)
		out_ops[i].rval = e.code();
	      if (!rc)
		rc = e.code();
	    }
	  }
	  subop.ops[i].f = nullptr;
	}

	if (subop.parent.reply_epoch &&
	    *subop.parent.reply_epoch < m->get_map_epoch())
	  *subop.parent.reply_epoch = m->get_map_epoch();


	if (subop.parent.onack) {
	  ldout(cct, 15) << "handle_osd_subop_reply ack" << dendl;
	  subop.replay_version = m->get_replay_version();
	  if (!subop.done)
	    ++subop.parent.acks; // Don't double count acks
	}

	if (subop.parent.oncommit && (m->is_ondisk() || rc)) {
	  ldout(cct, 15) << "handle_osd_subop_reply safe" << dendl;
	  ++subop.parent.commits; // Don't double count acks
	}

	if (!subop.parent.rc)
	  subop.parent.rc = rc;

	subop.done = true;

	possibly_complete_op(subop.parent, ol);
	assert(!ol);
	subop.parent.put();
	m->put();
	put_session(s);
      }

      void fs_stats_submit(StatfsOp& op) {
	ldout(cct, 10) << "fs_stats_submit" << op.tid << dendl;
	monc->send_mon_message(new MStatfs(monc->get_fsid(), op.tid, 0));
	op.last_submit = ceph::mono_clock::now();
      }

      void ms_handle_connect(Connection *con) {
	ldout(cct, 10) << "ms_handle_connect " << con << dendl;

	if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
	  resend_mon_ops();
      }

      bool ms_handle_reset(Connection *con) {
	if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
	  int osd = osdmap->identify_osd(con->get_peer_addr());
	  if (osd >= 0) {
	    ldout(cct, 1) << "ms_handle_reset on osd." << osd << dendl;
	    unique_lock wl(rwlock);

	    auto p = osd_sessions.find(osd, oc);
	    if (p != osd_sessions.end()) {
	      OSDSession& session = *p;
	      OSDSession::unique_lock sl(session.lock);
	      _reopen_session(session);
	      _kick_requests(session);
	      sl.unlock();
	      wl.unlock();
	      maybe_request_map();
	    } else {
	      wl.unlock();
	    }
	  } else {
	    ldout(cct, 10) << "ms_handle_reset on unknown osd addr "
			   << con->get_peer_addr() << dendl;
	  }
	  return true;
	}
	return false;
      }

      void ms_handle_remote_reset(Connection *con) {
	/*
	 * treat these the same.
	 */
	ms_handle_reset(con);
      }

      bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
			     bool force_new) {
	if (dest_type == CEPH_ENTITY_TYPE_MON)
	  return true;
	*authorizer = monc->auth->build_authorizer(dest_type);
	return *authorizer != NULL;
      }

      void blacklist_self(bool set) {
	ldout(cct, 10) << "blacklist_self " << (set ? "add" : "rm") << dendl;

	vector<string> cmd;
	cmd.push_back("{\"prefix\":\"osd blacklist\", ");
	if (set)
	  cmd.push_back("\"blacklistop\":\"add\",");
	else
	  cmd.push_back("\"blacklistop\":\"rm\",");
	stringstream ss;
	ss << messenger->get_myaddr();
	cmd.push_back("\"addr\":\"" + ss.str() + "\"");

	MMonCommand *m = new MMonCommand(monc->get_fsid());
	m->cmd = cmd;

	monc->send_mon_message(m);
      }

      void create_volume(const string& name,
			 op_callback&& onfinish) {
	ldout(cct, 10) << "create_volume name=" << name << dendl;

	if (osdmap->vol_exists(name) > 0)
	  onfinish(vol_errc::exists);

	vector<string> cmd;
	cmd.push_back("{\"prefix\":\"osd volume create\", ");
	cmd.push_back("\"volumeName\":\"" + name + "\"");

	bufferlist bl;
	monc->start_mon_command(
	  cmd, bl,
	  [onfinish = std::move(onfinish)](std::error_code err,
					   const string& s,
					   bufferlist& bl) mutable {
	    onfinish(err); });
      }

      void delete_volume(const string& name, op_callback&& onfinish) {
	ldout(cct, 10) << "delete_volume name=" << name << dendl;

	vector<string> cmd;
	cmd.push_back("{\"prefix\":\"osd volume delete\", ");
	cmd.push_back("\"volumeName\":\"" + name + "\"");

	bufferlist bl;
	monc->start_mon_command(
	  cmd, bl,
	  [onfinish = std::move(onfinish)](std::error_code err,
					   const string& s,
					   bufferlist& bl) mutable {
	    onfinish(err); });
      }
    };
  };
};
