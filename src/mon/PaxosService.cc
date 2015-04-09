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

#include <cassert>
#include "PaxosService.h"
#include "Monitor.h"
#include "MonitorDBStore.h"


#include "common/config.h"
#include "common/Formatter.h"

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, paxos, service_name, get_first_committed(), get_last_committed())
static ostream& _prefix(std::ostream *_dout, Monitor *mon, Paxos *paxos, string service_name,
			version_t fc, version_t lc) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").paxosservice(" << service_name << " " << fc << ".." << lc << ") ";
}

bool PaxosService::dispatch(PaxosServiceMessage *m,
			    std::unique_lock<std::mutex> &l)
{
  ldout(mon->cct, 10) << "dispatch " << *m << " from " << m->get_orig_source_inst() << dendl;

  if (mon->is_shutdown()) {
    m->put();
    return true;
  }

  // make sure this message isn't forwarded from a previous election epoch
  if (m->rx_election_epoch &&
      m->rx_election_epoch < mon->get_epoch()) {
    ldout(mon->cct, 10) << " discarding forwarded message from previous election epoch "
	     << m->rx_election_epoch << " < " << mon->get_epoch() << dendl;
    m->put();
    return true;
  }

  // make sure the client is still connected.  note that a proxied
  // connection will be disconnected with a null message; don't drop
  // those.  also ignore loopback (e.g., log) messages.
  if (!m->get_connection()->is_connected() &&
      m->get_connection() != mon->con_self &&
      m->get_connection()->get_messenger() != NULL) {
    ldout(mon->cct, 10) << " discarding message from disconnected client "
	     << m->get_source_inst() << " " << *m << dendl;
    m->put();
    return true;
  }

  // make sure our map is readable and up to date
  if (!is_readable(m->version)) {
    ldout(mon->cct, 10) << " waiting for paxos -> readable (v" << m->version << ")" << dendl;
    wait_for_readable(CB_RetryMessage(this, m), m->version);
    return true;
  }

  // preprocess
  if (preprocess_query(m, l))
    return true;  // easy!

  // leader?
  if (!mon->is_leader()) {
    mon->forward_request_leader(m);
    return true;
  }

  // writeable?
  if (!is_writeable()) {
    ldout(mon->cct, 10) << " waiting for paxos -> writeable" << dendl;
    wait_for_writeable(CB_RetryMessage(this, m));
    return true;
  }

  // update
  if (prepare_update(m, l)) {
    ceph::timespan delay = 0ns;
    if (should_propose(delay)) {
      if (delay == 0ns) {
	propose_pending(l);
      } else {
	// delay a bit
	if (!proposal_timer) {
	  proposal_timer = mon->timer.add_event(
	    delay, &PaxosService::propose_pending_timed, this);

	  ldout(mon->cct, 10) << " setting proposal_timer " << proposal_timer
			      << " with delay of " << delay << dendl;
	} else {
	  ldout(mon->cct, 10) << " proposal_timer already set" << dendl;
	}
      }
    } else {
      ldout(mon->cct, 10) << " not proposing" << dendl;
    }
  }
  return true;
}

void PaxosService::refresh(bool *need_bootstrap)
{
  // update cached versions
  cached_first_committed = mon->store->get(get_service_name(), first_committed_name);
  cached_last_committed = mon->store->get(get_service_name(), last_committed_name);

  version_t new_format = get_value("format_version");
  if (new_format != format_version) {
    ldout(mon->cct, 1) << __func__ << " upgraded, format " << format_version << " -> " << new_format << dendl;
    on_upgrade();
  }
  format_version = new_format;

  ldout(mon->cct, 10) << __func__ << dendl;

  update_from_paxos(need_bootstrap);
}


void PaxosService::remove_legacy_versions()
{
  ldout(mon->cct, 10) << __func__ << dendl;
  if (!mon->store->exists(get_service_name(), "conversion_first"))
    return;

  version_t cf = mon->store->get(get_service_name(), "conversion_first");
  version_t fc = get_first_committed();

  ldout(mon->cct, 10) << __func__ << " conversion_first " << cf
	   << " first committed " << fc << dendl;

  MonitorDBStore::Transaction t;
  if (cf < fc) {
    trim(&t, cf, fc);
  }
  t.erase(get_service_name(), "conversion_first");
  mon->store->apply_transaction(t);
}

bool PaxosService::should_propose(ceph::timespan& delay)
{
  // simple default policy: quick startup, then some damping.
  if (get_last_committed() <= 1)
    delay = 0ns;
  else {
    ceph::real_time now = ceph::real_clock::now();
    if ((now - paxos->last_commit_time) >
	mon->cct->_conf->paxos_propose_interval)
      delay = mon->cct->_conf->paxos_min_wait;
    else
      delay = mon->cct->_conf->paxos_propose_interval +
	paxos->last_commit_time - now;
  }
  return true;
}

void PaxosService::propose_pending_timed()
{
  Monitor::unique_lock l(mon->lock);
  proposal_timer = 0;
  propose_pending(l);
}

void PaxosService::propose_pending(Monitor::unique_lock& l)
{
  ldout(mon->cct, 10) << "propose_pending" << dendl;
  assert(have_pending);
  assert(!proposing);
  assert(mon->is_leader());
  assert(is_active());

  if (proposal_timer) {
    ldout(mon->cct, 10) << " canceling proposal_timer" << proposal_timer
			<< dendl;
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  /**
   * @note The value we propose is encoded in a bufferlist, passed to
   *	   Paxos::propose_new_value and it is obtained by calling a
   *	   function that must be implemented by the class implementing us.
   *	   I.e., the function encode_pending will be the one responsible
   *	   to encode whatever is pending on the implementation class into a
   *	   bufferlist, so we can then propose that as a value through Paxos.
   */
  MonitorDBStore::Transaction t;
  bufferlist bl;

  if (should_stash_full())
    encode_full(&t);

  encode_pending(&t);
  have_pending = false;

  if (format_version > 0) {
    t.put(get_service_name(), "format_version", format_version);
  }

  ldout(mon->cct, 30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t.dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  t.encode(bl);

  // apply to paxos
  proposing = true;
  paxos->propose_new_value(bl, l, CB_Committed(this));
}

bool PaxosService::should_stash_full()
{
  version_t latest_full = get_version_latest_full();
  /* @note The first member of the condition is moot and it is here just for
   *	   clarity's sake. The second member would end up returing true
   *	   nonetheless because, in that event,
   *	      latest_full == get_trim_to() == 0.
   */
  return (!latest_full ||
	  (latest_full <= get_trim_to()) ||
	  (get_last_committed() - latest_full > (unsigned)mon->cct->_conf->paxos_stash_full_interval));
}

void PaxosService::restart(Monitor::unique_lock& l)
{
  ldout(mon->cct, 10) << "restart" << dendl;
  if (proposal_timer) {
    ldout(mon->cct, 10) << " canceling proposal_timer " << proposal_timer << dendl;
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  waiting_for_finished_proposal(-EAGAIN, l);

  if (have_pending) {
    discard_pending();
    have_pending = false;
  }
  proposing = false;

  on_restart();
}

void PaxosService::election_finished(Monitor::unique_lock& l)
{
  ldout(mon->cct, 10) << "election_finished" << dendl;

  waiting_for_finished_proposal(-EAGAIN, l);

  // make sure we update our state
  _active(l);
}

void PaxosService::_active(Monitor::unique_lock& l)
{
  if (is_proposing()) {
    ldout(mon->cct, 10) << "_acting - proposing" << dendl;
    return;
  }
  if (!is_active()) {
    ldout(mon->cct, 10) << "_active - not active" << dendl;
    wait_for_active(CB_Active(this));
    return;
  }
  ldout(mon->cct, 10) << "_active" << dendl;

  remove_legacy_versions();

  // create pending state?
  if (mon->is_leader() && is_active()) {
    ldout(mon->cct, 7) << "_active creating new pending" << dendl;
    if (!have_pending) {
      create_pending();
      have_pending = true;
    }

    if (get_last_committed() == 0) {
      // create initial state
      create_initial();
      propose_pending(l);
      return;
    }
  } else {
    if (!mon->is_leader()) {
      ldout(mon->cct, 7) << __func__ << " we are not the leader, hence we propose nothing!" << dendl;
    } else if (!is_active()) {
      ldout(mon->cct, 7) << __func__ << " we are not active, hence we propose nothing!" << dendl;
    }
  }

  // wake up anyone who came in while we were proposing.  note that
  // anyone waiting for the previous proposal to commit is no longer
  // on this list; it is on Paxos's.
  waiting_for_finished_proposal(0, l);

  if (is_active() && mon->is_leader())
    upgrade_format(l);

  // NOTE: it's possible that this will get called twice if we commit
  // an old paxos value.  Implementations should be mindful of that.
  if (is_active())
    on_active(l);
}


void PaxosService::shutdown(Monitor::unique_lock& l)
{
  cancel_events();

  if (proposal_timer) {
    ldout(mon->cct, 10) << " canceling proposal_timer " << proposal_timer << dendl;
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  waiting_for_finished_proposal(-EAGAIN, l);

  on_shutdown();
}

void PaxosService::maybe_trim(Monitor::unique_lock& l)
{
  if (!is_writeable())
    return;

  version_t trim_to = get_trim_to();
  if (trim_to < get_first_committed())
    return;

  version_t to_remove = trim_to - get_first_committed();
  if (mon->cct->_conf->paxos_service_trim_min > 0 &&
      to_remove < (version_t)mon->cct->_conf->paxos_service_trim_min) {
    ldout(mon->cct, 10) << __func__ << " trim_to " << trim_to << " would only trim " << to_remove
	     << " < paxos_service_trim_min " << mon->cct->_conf->paxos_service_trim_min << dendl;
    return;
  }

  if (mon->cct->_conf->paxos_service_trim_max > 0 &&
      to_remove > (version_t)mon->cct->_conf->paxos_service_trim_max) {
    ldout(mon->cct, 10) << __func__ << " trim_to " << trim_to << " would only trim " << to_remove
	     << " > paxos_service_trim_max, limiting to " << mon->cct->_conf->paxos_service_trim_max
	     << dendl;
    trim_to = get_first_committed() + mon->cct->_conf->paxos_service_trim_max;
    to_remove = trim_to - get_first_committed();
  }

  ldout(mon->cct, 10) << __func__ << " trimming to " << trim_to << ", " << to_remove << " states" << dendl;
  MonitorDBStore::Transaction t;
  trim(&t, get_first_committed(), trim_to);
  put_first_committed(&t, trim_to);

  // let the service add any extra stuff
  encode_trim_extra(&t, trim_to);

  bufferlist bl;
  t.encode(bl);
  paxos->propose_new_value(bl, l, NULL);
}

void PaxosService::trim(MonitorDBStore::Transaction *t,
			version_t from, version_t to)
{
  ldout(mon->cct, 10) << __func__ << " from " << from << " to " << to << dendl;
  assert(from != to);

  for (version_t v = from; v < to; ++v) {
    ldout(mon->cct, 20) << __func__ << " " << v << dendl;
    t->erase(get_service_name(), v);

    string full_key = mon->store->combine_strings("full", v);
    if (mon->store->exists(get_service_name(), full_key)) {
      ldout(mon->cct, 20) << __func__ << " " << full_key << dendl;
      t->erase(get_service_name(), full_key);
    }
  }
  if (mon->cct->_conf->mon_compact_on_trim) {
    ldout(mon->cct, 20) << " compacting prefix " << get_service_name() << dendl;
    t->compact_range(get_service_name(), stringify(from - 1), stringify(to));
  }
}

