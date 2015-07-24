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
    class MessagingObjecter : public Dispatcher,
			      public Objecter<MessagingObjecter> {
    public:
      Messenger *messenger;
      MonClient *monc;

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
      void put_session(OSDSession& s);
      void get_session(OSDSession& s);
      void _reopen_session(OSDSession& session);
      void close_session(OSDSession& session);

      void resend_mon_ops();

    public:
      MessagingObjecter(CephContext *_cct, Messenger *m, MonClient *mc,
			ceph::timespan mon_timeout = 0s,
			ceph::timespan osd_timeout = 0s) :
	Dispatcher(_cct), Objecter(mon_timeout, osd_timeout),
	messenger(m), monc(mc) { }
      ~MessagingObjecter();

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
    };
  };
};
