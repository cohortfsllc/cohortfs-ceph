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
  namespace detail {
    bool MessagingObjecter::ms_dispatch(Message *m) {
      ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;

      switch (m->get_type()) {
	// these we exlusively handle
      case CEPH_MSG_OSD_OPREPLY:
	handle_osd_subop_reply(static_cast<MOSDOpReply*>(m));
	return true;

      case CEPH_MSG_STATFS_REPLY:
	handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
	return true;

	// these we give others a chance to inspect

	// MDS, OSD
      case CEPH_MSG_OSD_MAP:
	handle_osd_map(static_cast<MOSDMap*>(m));
	return true;
      }
      return false;
    }

    /**
     * Look up OSDSession by OSD id.
     */
    OSDSession* MessagingObjecter::_get_session(
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

    void MessagingObjecter::put_session(OSDSession& s) {
      s.put();
    }

    void MessagingObjecter::get_session(OSDSession& s) {
      s.get();
    }

    void MessagingObjecter::_reopen_session(OSDSession& s) {
      entity_inst_t inst = osdmap->get_inst(s.osd);
      ldout(cct, 10) << "reopen_session osd." << s.osd
		     << " session, addr now " << inst << dendl;
      if (s.con)
	messenger->mark_down(s.con);
      s.con = messenger->get_connection(inst);
      s.incarnation++;
    }

    void MessagingObjecter::close_session(OSDSession& s) {
      ldout(cct, 10) << "close_session for osd." << s.osd << dendl;
      if (s.con)
	messenger->mark_down(s.con);

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

    void MessagingObjecter::_maybe_request_map() {
      int flag = 0;
      if (osdmap_full_flag()) {
	ldout(cct, 10) << "_maybe_request_map subscribing (continuous) to "
		       << "next osd map (FULL flag is set)" << dendl;
      } else {
	ldout(cct, 10) << "_maybe_request_map subscribing (onetime) to next "
		       << "osd map" << dendl;
	flag = CEPH_SUBSCRIBE_ONETIME;
      }
      epoch_t epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
      if (monc->sub_want("osdmap", epoch, flag)) {
	monc->renew_subs();
      }
    }

    void MessagingObjecter::kick_requests(OSDSession& session) {
      ldout(cct, 10) << "kick_requests for osd." << session.osd << dendl;

      unique_lock wl(rwlock);

      OSDSession::unique_lock sl(session.lock);
      _kick_requests(session);
      sl.unlock();
    }

    void MessagingObjecter::_kick_requests(OSDSession& session) {
      // resend ops
      map<ceph_tid_t,SubOp*> resend;  // resend in tid order

      for (auto &subop : session.subops_inflight) {
	if (subop.parent.should_resend) {
	  if (!subop.parent.paused)
	    resend[subop.tid] = &subop;
	} else {
	  _cancel_op(subop.parent); // TODO: WTF?
	}
      }
      while (!resend.empty()) {
	_send_subop(*resend.begin()->second);
	resend.erase(resend.begin());
      }
    }

    void Objecter::resend_mon_ops() {
      unique_lock wl(rwlock);

      ldout(cct, 10) << "resend_mon_ops" << dendl;

      for (auto& statfs_op : statfs_ops) {
	_fs_stats_submit(statfs_op);
      }

      for (auto kv : check_latest_map_ops) {
	monc->get_version("osdmap", Op_Map_Latest(*this, kv.second->tid));
      }
    }

    MessagingObjecter::OSDSession* MessagingObjecter::_map_session(
      SubOp& subop, typename Op::unique_lock& ol, const shunique_lock& shl) {
      int r = _calc_targets(subop.parent, ol);
      if (r == TARGET_VOLUME_DNE) {
	throw std::system_error(vol_errc::no_such_volume);
      }
      return  _get_session(subop.osd, shl);
    }

    void MessagingObjecter::_session_subop_assign(OSDSession& to,
						  SubOp& subop) {
      assert(subop.session == NULL);
      assert(subop.tid);

      get_session(to);
      subop.session = &to;
      to.subops_inflight.insert(subop);
    }

    void MessagingObjecter::_session_subop_remove(OSDSession& from,
						  SubOp& subop) {
      assert(subop.session == &from);

      from.subops_inflight.erase(subop);
      put_session(from);
      subop.session = nullptr;

      ldout(cct, 15) << __func__ << " " << from.osd << " " << subop.tid
		     << dendl;
    }
  };
};
