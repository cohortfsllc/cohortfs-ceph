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
#include "MDLog.h"
#include "MDS.h"
#include "MDCache.h"
#include "LogEvent.h"

#include "osdc/Journaler.h"

#include "common/entity_name.h"

#include "events/ESubtreeMap.h"

#include "common/config.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_mds
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds \
    || l <= cct->_conf->debug_mds_log
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".log "

using rados::op_callback;
using rados::CB_Waiter;

// cons/des
MDLog::~MDLog()
{
  if (journaler) { delete journaler; journaler = 0; }
  if (replay_thread) { delete replay_thread; replay_thread = 0; }
}


void MDLog::init_journaler(const AVolRef& v)
{
  // inode
  ino = MDS_INO_LOG_OFFSET + mds->get_nodeid();

  // log streamer
  if (journaler) delete journaler;
  journaler = new Journaler(ino, v, CEPH_FS_ONDISK_MAGIC, mds->objecter,
			    mds->timer);
  assert(journaler->is_readonly());
  journaler->set_write_error_handler(new C_MDL_WriteError(this));
}

void MDLog::handle_journaler_write_error(int r)
{
  MDS::unique_lock ml(mds->mds_lock, std::defer_lock);
  if (r == -EBLACKLISTED) {
    lderr(mds->cct) << "we have been blacklisted (fenced), respawning..." << dendl;
    mds->respawn(ml);
  } else {
    lderr(mds->cct) << "unhandled error " << cpp_strerror(r) << ", shutting down..." << dendl;
    mds->suicide(ml);
  }
}

void MDLog::write_head(op_callback&& c)
{
  journaler->write_head(std::move(c));
}

uint64_t MDLog::get_read_pos()
{
  return journaler->get_read_pos();
}

uint64_t MDLog::get_write_pos()
{
  return journaler->get_write_pos();
}

uint64_t MDLog::get_safe_pos()
{
  return journaler->get_write_safe_pos();
}



void MDLog::create(const AVolRef &v, op_callback&& c)
{
  ldout(mds->cct, 5) << "create empty log" << dendl;
  init_journaler(v);
  journaler->set_writeable();
  journaler->create();
  journaler->write_head(std::move(c));
}

void MDLog::open(const AVolRef& v, Context *c)
{
  ldout(mds->cct, 5) << "open discovering log bounds" << dendl;
  init_journaler(v);
  journaler->recover(c);

  // either append() or replay() will follow.
}

void MDLog::append()
{
  ldout(mds->cct, 5) << "append positioning at end and marking writeable" << dendl;
  journaler->set_read_pos(journaler->get_write_pos());
  journaler->set_expire_pos(journaler->get_write_pos());

  journaler->set_writeable();
}



// -------------------------------------------------

void MDLog::start_entry(LogEvent *e)
{
  assert(cur_event == NULL);
  cur_event = e;
  e->set_start_off(get_write_pos());
}

void MDLog::submit_entry(LogEvent *le, Context* c)
{
  assert(!mds->is_any_replay());
  assert(le == cur_event);
  cur_event = NULL;

  if (!mds->cct->_conf->mds_log) {
    // hack: log is disabled.
    if (c) {
      c->complete(0);
    }
    return;
  }

  // let the event register itself in the segment
  assert(!segments.empty());
  le->_segment = segments.rbegin()->second;
  le->_segment->num_events++;
  le->update_segment();

  le->set_stamp(ceph::real_clock::now());

  num_events++;
  assert(!capped);

  // encode it, with event type
  {
    bufferlist bl;
    le->encode_with_header(bl);

    ldout(mds->cct, 5) << "submit_entry " << journaler->get_write_pos() << "~" << bl.length()
	    << " : " << *le << dendl;

    // journal it.
    journaler->append_entry(bl);  // bl is destroyed.
  }

  le->_segment->end = journaler->get_write_pos();

  unflushed++;

  if (c)
    journaler->wait_for_flush(c);

  // start a new segment?
  //  FIXME: should this go elsewhere?
#if 0
// XXX segments?  do we need them? mdw 20150215
  uint64_t last_seg = get_last_segment_offset();
  uint64_t period = journaler->get_layout_period();
#endif
  // start a new segment if there are none or if we reach end of last segment
  if (le->get_type() == EVENT_SUBTREEMAP ||
      (le->get_type() == EVENT_IMPORTFINISH && mds->is_resolve())) {
    // avoid infinite loop when ESubtreeMap is very large.
    // don not insert ESubtreeMap among EImportFinish events that finish
    // disambiguate imports. Because the ESubtreeMap reflects the subtree
    // state when all EImportFinish events are replayed.
#if 0
  } else if (journaler->get_write_pos()/period != last_seg/period) {
    ldout(mds->cct, 10) << "submit_entry also starting new segment: last = " << last_seg
	     << ", cur pos = " << journaler->get_write_pos() << dendl;
    start_new_segment();
#endif
  } else if (mds->cct->_conf->mds_debug_subtrees &&
	     le->get_type() != EVENT_SUBTREEMAP_TEST) {
    // debug: journal this every time to catch subtree replay bugs.
    // use a different event id so it doesn't get interpreted as a
    // LogSegment boundary on replay.
    LogEvent *sle = mds->mdcache->create_subtree_map();
    sle->set_type(EVENT_SUBTREEMAP_TEST);
    submit_entry(sle);
  }

  delete le;
}

void MDLog::wait_for_safe(Context *c)
{
  if (mds->cct->_conf->mds_log) {
    // wait
    journaler->wait_for_flush(c);
  } else {
    // hack: bypass.
    c->complete(0);
  }
}

void MDLog::flush()
{
  if (unflushed)
    journaler->flush();
  unflushed = 0;
}

void MDLog::cap()
{
  ldout(mds->cct, 5) << "cap" << dendl;
  capped = true;
}


// -----------------------------
// segments

void MDLog::start_new_segment(Context *onsync)
{
  prepare_new_segment();
  journal_segment_subtree_map();
  if (onsync) {
    wait_for_safe(onsync);
    flush();
  }
}

void MDLog::prepare_new_segment()
{
  ldout(mds->cct, 7) << __func__ << " at " << journaler->get_write_pos() << dendl;

  segments[journaler->get_write_pos()] = new LogSegment(journaler->get_write_pos());

  // Adjust to next stray dir
  ldout(mds->cct, 10) << "Advancing to next stray directory on mds " << mds->get_nodeid()
	   << dendl;
  mds->mdcache->advance_stray();
}

void MDLog::journal_segment_subtree_map()
{
  ldout(mds->cct, 7) << __func__ << dendl;
  submit_entry(mds->mdcache->create_subtree_map());
}

void MDLog::trim(int m)
{
  int max_segments = mds->cct->_conf->mds_log_max_segments;
  int max_events = mds->cct->_conf->mds_log_max_events;
  if (m >= 0)
    max_events = m;

  // trim!
  ldout(mds->cct, 10) << "trim "
	   << segments.size() << " / " << max_segments << " segments, "
	   << num_events << " / " << max_events << " events"
	   << ", " << expiring_segments.size() << " (" << expiring_events << ") expiring"
	   << ", " << expired_segments.size() << " (" << expired_events << ") expired"
	   << dendl;

  if (segments.empty())
    return;

  // hack: only trim for a few seconds at a time
  ceph::mono_time stop = ceph::mono_clock::now();
  stop += 2s;

  map<uint64_t,LogSegment*>::iterator p = segments.begin();
  while (p != segments.end() &&
	 ((max_events >= 0 &&
	   num_events - expiring_events - expired_events > max_events) ||
	  (max_segments >= 0 &&
	   segments.size() - expiring_segments.size() - expired_segments.size() > (unsigned)max_segments))) {

    if (stop < ceph::mono_clock::now())
      break;

    int num_expiring_segments = (int)expiring_segments.size();
    if (num_expiring_segments >= mds->cct->_conf->mds_log_max_expiring)
      break;

    int op_prio = CEPH_MSG_PRIO_LOW +
		  (CEPH_MSG_PRIO_HIGH - CEPH_MSG_PRIO_LOW) *
		  num_expiring_segments / mds->cct->_conf->mds_log_max_expiring;

    // look at first segment
    LogSegment *ls = p->second;
    assert(ls);
    ++p;
    if (ls == get_current_segment()) {
      ldout(mds->cct, 5) << "trim segment " << ls->offset << ", is current seg, can't expire" << dendl;
      continue;
    }

    if (ls->end > journaler->get_write_safe_pos()) {
      ldout(mds->cct, 5) << "trim segment " << ls->offset << ", not fully flushed yet, safe "
	      << journaler->get_write_safe_pos() << " < end " << ls->end << dendl;
      break;
    }
    if (expiring_segments.count(ls)) {
      ldout(mds->cct, 5) << "trim already expiring segment " << ls->offset << ", " << ls->num_events << " events" << dendl;
    } else if (expired_segments.count(ls)) {
      ldout(mds->cct, 5) << "trim already expired segment " << ls->offset << ", " << ls->num_events << " events" << dendl;
    } else {
      try_expire(ls, op_prio);
    }
  }

  // discard expired segments
  _trim_expired_segments();
}


void MDLog::try_expire(LogSegment *ls, int op_prio)
{
  auto& mexp = cohort::MultiCallback
    ::create<MaybeExpiredSegment>(this, ls, op_prio);
  ls->try_to_expire(mds, mexp, op_prio);
  if (!mexp.empty()) {
    assert(expiring_segments.count(ls) == 0);
    expiring_segments.insert(ls);
    expiring_events += ls->num_events;
    ldout(mds->cct, 5) << "try_expire expiring segment " << ls->offset << dendl;
    mexp.activate();
  } else {
    ldout(mds->cct, 10) << "try_expire expired segment " << ls->offset << dendl;
    _expired(ls);
    mexp.discard();
  }
}

void MDLog::_maybe_expired(LogSegment *ls, int op_prio)
{
  ldout(mds->cct, 10) << "_maybe_expired segment " << ls->offset << " " << ls->num_events << " events" << dendl;
  assert(expiring_segments.count(ls));
  expiring_segments.erase(ls);
  expiring_events -= ls->num_events;
  try_expire(ls, op_prio);
}

void MDLog::_trim_expired_segments()
{
  // trim expired segments?
  bool trimmed = false;
  while (!segments.empty()) {
    LogSegment *ls = segments.begin()->second;
    if (!expired_segments.count(ls)) {
      ldout(mds->cct, 10) << "_trim_expired_segments waiting for " << ls->offset << " to expire" << dendl;
      break;
    }

    ldout(mds->cct, 10) << "_trim_expired_segments trimming expired " << ls->offset << dendl;
    expired_events -= ls->num_events;
    expired_segments.erase(ls);
    num_events -= ls->num_events;

    // this was the oldest segment, adjust expire pos
    if (journaler->get_expire_pos() < ls->offset)
      journaler->set_expire_pos(ls->offset);

    segments.erase(ls->offset);
    delete ls;
    trimmed = true;
  }

  if (trimmed)
    journaler->write_head(0);
}

void MDLog::_expired(LogSegment *ls)
{
  ldout(mds->cct, 5) << "_expired segment " << ls->offset << " " << ls->num_events << " events" << dendl;

  if (!capped && ls == peek_current_segment()) {
    ldout(mds->cct, 5) << "_expired not expiring " << ls->offset << ", last one and !capped" << dendl;
  } else {
    // expired.
    expired_segments.insert(ls);
    expired_events += ls->num_events;
  }
}



void MDLog::replay(const AVolRef& v, Context *c)
{
  assert(journaler->is_active());
  assert(journaler->is_readonly());

  // empty?
  if (journaler->get_read_pos() == journaler->get_write_pos()) {
    ldout(mds->cct, 10) << "replay - journal empty, done." << dendl;
    if (c) {
      c->complete(0);
    }
    return;
  }

  // add waiter
  if (c)
    waitfor_replay.push_back(*c);

  // go!
  ldout(mds->cct, 10) << "replay start, from " << journaler->get_read_pos()
	   << " to " << journaler->get_write_pos() << dendl;

  assert(num_events == 0 || already_replayed);
  already_replayed = true;

  if (!replay_thread)
    replay_thread = new ReplayThread(this, v);
  else if (replay_thread->vol != v)
    replay_thread->vol = v;
  replay_thread->create();
  replay_thread->detach();
}

class C_MDL_Replay : public Context {
  MDLog *mdlog;
public:
  C_MDL_Replay(MDLog *l) : mdlog(l) {}
  void finish(int r) {
    MDS::unique_lock ml(mdlog->mds->mds_lock);
    mdlog->replay_cond.notify_all();
    ml.unlock(); // make sure we're waiting
  }
};



// i am a separate thread
void MDLog::_replay_thread(const AVolRef& v)
{
  MDS::unique_lock ml(mds->mds_lock);
  ldout(mds->cct, 10) << "_replay_thread start" << dendl;

  // loop
  int r = 0;
  while (1) {
    // wait for read?
    while (!journaler->is_readable() &&
	   journaler->get_read_pos() < journaler->get_write_pos() &&
	   !journaler->get_error()) {
      journaler->wait_for_readable(new C_OnFinisher(new C_MDL_Replay(this),
						    &mds->finisher));
      replay_cond.wait(ml);
    }
    if (journaler->get_error()) {
      r = journaler->get_error();
      ldout(mds->cct, 0) << "_replay journaler got error " << r
			 << ", aborting" << dendl;
      if (r == -ENOENT) {
	// journal has been trimmed by somebody else?
	assert(journaler->is_readonly());
	r = -EAGAIN;
      } else if (r == -EINVAL) {
	if (journaler->get_read_pos() < journaler->get_expire_pos()) {
	  // this should only happen if you're following somebody else
	  assert(journaler->is_readonly());
	  ldout(mds->cct, 0) << "expire_pos is higher than read_pos, "\
			     << "returning EAGAIN" << dendl;
	  r = -EAGAIN;
	} else {
	  /* re-read head and check it
	   * Given that replay happens in a separate thread and
	   * the MDS is going to either shut down or restart when
	   * we return this error, doing it synchronously is fine
	   * -- as long as we drop the main mds lock--. */
	  CB_Waiter w;
	  journaler->reread_head(w);
	  ml.unlock();
	  int err = w.wait();
	  if (err) { // well, crap
	    ldout(mds->cct, 0) << "got error while reading head: "
			       << cpp_strerror(err)
			       << dendl;
	    ml.lock();
	    mds->suicide(ml);
	    ml.unlock();
	  }
	  ml.lock();
	  standby_trim_segments();
	  if (journaler->get_read_pos() < journaler->get_expire_pos()) {
	    ldout(mds->cct, 0) << "expire_pos is higher than read_pos, "
			       << "returning EAGAIN" << dendl;
	    r = -EAGAIN;
	  }
	}
      }
      break;
    }

    if (!journaler->is_readable() &&
	journaler->get_read_pos() == journaler->get_write_pos())
      break;

    assert(journaler->is_readable());

    // read it
    uint64_t pos = journaler->get_read_pos();
    bufferlist bl;
    bool r = journaler->try_read_entry(bl);
    if (!r && journaler->get_error())
      continue;
    assert(r);

    // unpack event
    LogEvent *le = LogEvent::decode(bl);
    if (!le) {
      ldout(mds->cct, 0) << "_replay " << pos << "~" << bl.length()
			 << " / " << journaler->get_write_pos()
	      << " -- unable to decode event" << dendl;
      ldout(mds->cct, 0) << "dump of unknown or corrupt event:\n";
      bl.hexdump(*_dout);
      *_dout << dendl;

      assert(!!"corrupt log event" ==
	     mds->cct->_conf->mds_log_skip_corrupt_events);
      continue;
    }
    le->set_start_off(pos);

    // new segment?
    if (le->get_type() == EVENT_SUBTREEMAP ||
	le->get_type() == EVENT_RESETJOURNAL) {
      segments[pos] = new LogSegment(pos);
    }

    // have we seen an import map yet?
    if (segments.empty()) {
      ldout(mds->cct, 10) << "_replay " << pos << "~" << bl.length()
			  << " / " << journaler->get_write_pos()
			  << " " << le->get_stamp()
			  << " -- waiting for subtree_map.  (skipping "
			  << *le << ")" << dendl;
    } else {
      ldout(mds->cct, 10) << "_replay " << pos << "~" << bl.length()
			  << " / " << journaler->get_write_pos()
			  << " " << le->get_stamp() << ": " << *le << dendl;
      le->_segment = get_current_segment();    // replay may need this
      le->_segment->num_events++;
      le->_segment->end = journaler->get_read_pos();
      num_events++;

      le->replay(mds, v);
    }
    delete le;

    // drop lock for a second, so other events/messages (e.g. beacon
    // timer!) can go off
    ml.unlock();
    ml.lock();
  }

  // done!
  if (r == 0) {
    assert(journaler->get_read_pos() == journaler->get_write_pos());
    ldout(mds->cct, 10) << "_replay - complete, " << num_events
			<< " events" << dendl;
  }

  ldout(mds->cct, 10) << "_replay_thread kicking waiters" << dendl;
  finish_contexts(waitfor_replay, r);

  ldout(mds->cct, 10) << "_replay_thread finish" << dendl;
  ml.unlock();
}

void MDLog::standby_trim_segments()
{
  ldout(mds->cct, 10) << "standby_trim_segments" << dendl;
  uint64_t expire_pos = journaler->get_expire_pos();
  ldout(mds->cct, 10) << " expire_pos=" << expire_pos << dendl;
  bool removed_segment = false;
  while (have_any_segments()) {
    LogSegment *seg = get_oldest_segment();
    if (seg->end > expire_pos)
      break;
    ldout(mds->cct, 10) << " removing segment " << seg->offset << dendl;
    seg->dirty_dirfrags.clear_list();
    seg->new_dirfrags.clear_list();
    seg->dirty_inodes.clear_list();
    seg->dirty_dentries.clear_list();
    seg->open_files.clear_list();
    seg->dirty_parent_inodes.clear_list();
    seg->dirty_dirfrag_dir.clear_list();
    seg->dirty_dirfrag_nest.clear_list();
    seg->dirty_dirfrag_dirfragtree.clear_list();
    remove_oldest_segment();
    removed_segment = true;
  }

  if (removed_segment) {
    ldout(mds->cct, 20) << " calling mdcache->trim!" << dendl;
    mds->mdcache->trim(-1);
  } else
    ldout(mds->cct, 20) << " removed no segments!" << dendl;
}
