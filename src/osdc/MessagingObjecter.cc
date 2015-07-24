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
  };
};
