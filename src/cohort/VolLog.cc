// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 */

#include "VolLog.h"
#include "osd/SnapMapper.h"
#include "vol/Volume.h"

#define dout_subsys ceph_subsys_osd

string vol_log_entry_t::get_key_name() const
{
  return version.get_key_name();
}

void vol_log_entry_t::encode_with_checksum(bufferlist& bl) const
{
  bufferlist ebl(sizeof(*this)*2);
  encode(ebl);
  __u32 crc = ebl.crc32c(0);
  ::encode(ebl, bl);
  ::encode(crc, bl);
}

void vol_log_entry_t::decode_with_checksum(bufferlist::iterator& p)
{
  bufferlist bl;
  ::decode(bl, p);
  __u32 crc;
  ::decode(crc, p);
  if (crc != bl.crc32c(0))
    throw buffer::malformed_input("bad checksum on vol_log_entry_t");
  bufferlist::iterator q = bl.begin();
  decode(q);
}

void vol_log_entry_t::encode(bufferlist &bl) const
{
  ENCODE_START(7, 4, bl);
  ::encode(op, bl);
  ::encode(soid, bl);
  ::encode(version, bl);

  /**
   * Added with reverting_to:
   * Previous code used prior_version to encode
   * what we now call reverting_to.  This will
   * allow older code to decode reverting_to
   * into prior_version as expected.
   */
  if (op == LOST_REVERT)
    ::encode(reverting_to, bl);
  else
    ::encode(prior_version, bl);

  ::encode(reqid, bl);
  ::encode(mtime, bl);
  if (op == LOST_REVERT)
    ::encode(prior_version, bl);
  ::encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void vol_log_entry_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 4, 4, bl);
  ::decode(op, bl);
  if (struct_v < 2) {
    sobject_t old_soid;
    ::decode(old_soid, bl);
    soid.oid = old_soid.oid;
    soid.snap = old_soid.snap;
    invalid_hash = true;
  } else {
    ::decode(soid, bl);
  }
  if (struct_v < 3)
    invalid_hash = true;
  ::decode(version, bl);

  if (struct_v >= 6 && op == LOST_REVERT)
    ::decode(reverting_to, bl);
  else
    ::decode(prior_version, bl);

  ::decode(reqid, bl);
  ::decode(mtime, bl);

  if (op == LOST_REVERT) {
    if (struct_v >= 6) {
      ::decode(prior_version, bl);
    } else {
      reverting_to = prior_version;
    }
  }
  if (struct_v >= 7 ||  // for v >= 7, this is for all ops.
      op == CLONE) {    // for v < 7, it's only present for CLONE.
    ::decode(snaps, bl);
  }

  DECODE_FINISH(bl);
}

void vol_log_entry_t::dump(Formatter *f) const
{
  f->dump_string("op", get_op_name());
  f->dump_stream("object") << soid;
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << version;
  f->dump_stream("reqid") << reqid;
  f->dump_stream("mtime") << mtime;
  if (snaps.length() > 0) {
    vector<snapid_t> v;
    bufferlist c = snaps;
    bufferlist::iterator p = c.begin();
    try {
      ::decode(v, p);
    } catch (...) {
      v.clear();
    }
    f->open_object_section("snaps");
    for (vector<snapid_t>::iterator p = v.begin(); p != v.end(); ++p)
      f->dump_unsigned("snap", *p);
    f->close_section();
  }
}

ostream& operator<<(ostream& out, const vol_log_entry_t& e)
{
  out << e.version << " (" << e.prior_version << ") "
      << e.get_op_name() << ' ' << e.soid << " by " << e.reqid << " " << e.mtime;
  if (e.snaps.length()) {
    vector<snapid_t> snaps;
    bufferlist c = e.snaps;
    bufferlist::iterator p = c.begin();
    try {
      ::decode(snaps, p);
    } catch (...) {
      snaps.clear();
    }
    out << " snaps " << snaps;
  }
  return out;
}

void vol_log_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(head, bl);
  ::encode(tail, bl);
  ::encode(log, bl);
  ENCODE_FINISH(bl);
}

void vol_log_t::decode(bufferlist::iterator &bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  ::decode(head, bl);
  ::decode(tail, bl);
  if (struct_v < 2) {
    bool backlog;
    ::decode(backlog, bl);
  }
  ::decode(log, bl);
  DECODE_FINISH(bl);

  // handle hobject_t format change
  if (struct_v < 4) {
    for (list<vol_log_entry_t>::iterator i = log.begin();
	 i != log.end();
	 ++i) {
    }
  }
}

void vol_log_t::dump(Formatter *f) const
{
  f->dump_stream("head") << head;
  f->dump_stream("tail") << head;
  f->open_array_section("log");
  for (list<vol_log_entry_t>::const_iterator p = log.begin(); p != log.end(); ++p) {
    f->open_object_section("entry");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void vol_log_t::copy_after(const vol_log_t &other, eversion_t v)
{
  head = other.head;
  tail = other.tail;
  for (list<vol_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       ++i) {
    assert(i->version > other.tail);
    if (i->version <= v) {
      // make tail accurate.
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

void vol_log_t::copy_range(const vol_log_t &other, eversion_t from,
			   eversion_t to)
{
  list<vol_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
  assert(i != other.log.rend());
  while (i->version > to) {
    ++i;
    assert(i != other.log.rend());
  }
  assert(i->version == to);
  head = to;
  for ( ; i != other.log.rend(); ++i) {
    if (i->version <= from) {
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

void vol_log_t::copy_up_to(const vol_log_t &other, int max)
{
  int n = 0;
  head = other.head;
  tail = other.tail;
  for (list<vol_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       ++i) {
    if (n++ >= max) {
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

ostream& vol_log_t::print(ostream& out) const
{
  out << *this << std::endl;
  for (list<vol_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p)
    out << *p << std::endl;
  return out;
}

//////////////////// VolLog::IndexedLog ////////////////////

void VolLog::IndexedLog::trim(eversion_t s)
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is " << complete_to->version
		    << " on " << *this << dendl;
  }

  while (!log.empty()) {
    vol_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    unindex(e);         // remove from index,
    log.pop_front();    // from log
  }

  // raise tail?
  if (tail < s)
    tail = s;
}

ostream& VolLog::IndexedLog::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<vol_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}

//////////////////// VolLog ////////////////////

void VolLog::clear() {
  divergent_priors.clear();
  log.zero();
  log_keys_debug.clear();
  undirty();
}

void VolLog::clear_info_log(uuid_d vol, const hobject_t &infos_oid,
			    const hobject_t &log_oid,
			    ObjectStore::Transaction *t)
{
  set<string> keys_to_remove;
  keys_to_remove.insert(Volume::get_epoch_key(vol));
  keys_to_remove.insert(Volume::get_biginfo_key(vol));
  keys_to_remove.insert(Volume::get_info_key(vol));

  t->remove(coll_t::META_COLL, log_oid);
  t->omap_rmkeys(coll_t::META_COLL, infos_oid, keys_to_remove);
}

void VolLog::trim(eversion_t trim_to, vol_info_t &info)
{
  // trim?
  if (trim_to > log.tail) {
    /* If we are trimming, we must be complete up to trim_to, time
     * to throw out any divergent_priors
     */
    divergent_priors.clear();
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(trim_to);
    info.log_tail = log.tail;

    if (log.log.empty()) {
      mark_dirty_to(eversion_t::max());
    } else {
      mark_dirty_to(log.log.front().version);
    }
  }
}

/*
 * merge an old (possibly divergent) log entry into the new log.  this
 * happens _after_ new log items have been assimilated.  thus, we
 * assume the index already references newer entries (if present), and
 * missing has been updated accordingly.
 *
 * return true if entry is not divergent.
 */
bool VolLog::merge_old_entry(ObjectStore::Transaction& t,
			     const vol_log_entry_t& oe,
			     const vol_info_t& info,
			     list<hobject_t>& remove_snap)
{
  if (log.objects.count(oe.soid)) {
    vol_log_entry_t &ne = *log.objects[oe.soid];  // new(er?) entry

    if (ne.version > oe.version) {
      dout(20) << "merge_old_entry  had " << oe << " new " << ne
	       << " : older, missing" << dendl;
      assert(ne.is_delete());
      return false;
    }
    if (ne.version == oe.version) {
      dout(20) << "merge_old_entry  had " << oe << " new " << ne << " : same" << dendl;
      return true;
    }
  } else if (oe.op == vol_log_entry_t::CLONE) {
    assert(oe.soid.snap != CEPH_NOSNAP);
    dout(20) << "merge_old_entry  had " << oe
	     << ", clone with no non-divergent log entries, "
	     << "deleting" << dendl;
    remove_snap.push_back(oe.soid);
  } else if (oe.prior_version > info.log_tail) {
    /**
     * oe.prior_version is a previously divergent log entry
     * oe.soid must have already been handled and the missing
     * set updated appropriately
     */
    dout(20) << "merge_old_entry  had oe " << oe
	     << " with divergent prior_version " << oe.prior_version
	     << " oe.soid " << oe.soid
	     << " must already have been merged" << dendl;
  } else {
    if (!oe.is_delete()) {
      dout(20) << "merge_old_entry  had " << oe << " deleting" << dendl;
      remove_snap.push_back(oe.soid);
    }
    dout(20) << "merge_old_entry  had " << oe << " updating missing to "
	     << oe.prior_version << dendl;
    if (oe.prior_version > eversion_t()) {
      add_divergent_prior(oe.prior_version, oe.soid);
    }
  }
  return false;
}

/**
 * rewind divergent entries at the head of the log
 *
 * This rewinds entries off the head of our log that are divergent.
 * This is used by replicas during activation.
 *
 * @param t transaction
 * @param newhead new head to rewind to
 */
void VolLog::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead,
                      vol_info_t &info, list<hobject_t>& remove_snap,
                      bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "rewind_divergent_log truncate divergent future " << newhead << dendl;
  assert(newhead > log.tail);

  list<vol_log_entry_t>::iterator p = log.log.end();
  list<vol_log_entry_t> divergent;
  while (true) {
    if (p == log.log.begin()) {
      // yikes, the whole thing is divergent!
      divergent.swap(log.log);
      break;
    }
    --p;
    mark_dirty_from(p->version);
    if (p->version == newhead) {
      ++p;
      divergent.splice(divergent.begin(), log.log, p, log.log.end());
      break;
    }
    assert(p->version > newhead);
    dout(10) << "rewind_divergent_log future divergent " << *p << dendl;
    log.unindex(*p);
  }

  log.head = newhead;
  info.last_update = newhead;
  if (info.last_complete > newhead)
    info.last_complete = newhead;

  for (list<vol_log_entry_t>::iterator d = divergent.begin(); d != divergent.end(); ++d)
    merge_old_entry(t, *d, info, remove_snap);

  dirty_info = true;
  dirty_big_info = true;
}

void VolLog::merge_log(ObjectStore::Transaction& t,
                      vol_info_t &oinfo, vol_log_t &olog, int fromosd,
                      vol_info_t &info, list<hobject_t>& remove_snap,
                      bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "merge_log " << olog << " from osd." << fromosd
           << " into " << log << dendl;

  // Check preconditions

  // If our log is empty, the incoming log needs to have not been trimmed.
  assert(!log.null() || olog.tail == eversion_t());
  // The logs must overlap.
  assert(log.head >= olog.tail && olog.head >= log.tail);

  bool changed = false;

  // extend on tail?
  //  this is just filling in history.  it does not affect our
  //  missing set, as that should already be consistent with our
  //  current log.
  if (olog.tail < log.tail) {
    mark_dirty_to(log.log.begin()->version); // last clean entry
    dout(10) << "merge_log extending tail to " << olog.tail << dendl;
    list<vol_log_entry_t>::iterator from = olog.log.begin();
    list<vol_log_entry_t>::iterator to;
    for (to = from;
	 to != olog.log.end();
	 ++to) {
      if (to->version > log.tail)
	break;
      log.index(*to);
      dout(15) << *to << dendl;
    }
    assert(to != olog.log.end() ||
	   (olog.head == info.last_update));
      
    // splice into our log.
    log.log.splice(log.log.begin(),
		   olog.log, from, to);
      
    info.log_tail = log.tail = olog.tail;
    changed = true;
  }

  if (oinfo.stats.reported < info.stats.reported)   // make sure reported always increases
    oinfo.stats.reported = info.stats.reported;

  // do we have divergent entries to throw out?
  if (olog.head < log.head) {
    rewind_divergent_log(t, olog.head, info, remove_snap, dirty_info, dirty_big_info);
    changed = true;
  }

  // extend on head?
  if (olog.head > log.head) {
    dout(10) << "merge_log extending head to " << olog.head << dendl;
      
    // find start point in olog
    list<vol_log_entry_t>::iterator to = olog.log.end();
    list<vol_log_entry_t>::iterator from = olog.log.end();
    eversion_t lower_bound = olog.tail;
    while (1) {
      if (from == olog.log.begin())
	break;
      --from;
      dout(20) << "  ? " << *from << dendl;
      if (from->version <= log.head) {
	dout(20) << "merge_log cut point (usually last shared) is " << *from << dendl;
	lower_bound = from->version;
	++from;
	break;
      }
    }
    mark_dirty_from(lower_bound);

    // index, update missing, delete deleted
    for (list<vol_log_entry_t>::iterator p = from; p != to; ++p) {
      vol_log_entry_t &ne = *p;
      dout(20) << "merge_log " << ne << dendl;
      log.index(ne);
    }

    // move aside divergent items
    list<vol_log_entry_t> divergent;
    while (!log.empty()) {
      vol_log_entry_t &oe = *log.log.rbegin();
      /*
       * look at eversion.version here.  we want to avoid a situation like:
       *  our log: 100'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
       *  new log: 122'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
       *  lower_bound = 100'9
       * i.e, same request, different version.  If the eversion.version is > the
       * lower_bound, we it is divergent.
       */
      if (oe.version.version <= lower_bound.version)
	break;
      dout(10) << "merge_log divergent " << oe << dendl;
      divergent.push_front(oe);
      log.unindex(oe);
      log.log.pop_back();
    }

    // splice
    log.log.splice(log.log.end(), 
		   olog.log, from, to);
    log.index();   

    info.last_update = log.head = olog.head;
    info.purged_snaps = oinfo.purged_snaps;

    // process divergent items
    if (!divergent.empty()) {
      for (list<vol_log_entry_t>::iterator d = divergent.begin(); d != divergent.end(); ++d)
	merge_old_entry(t, *d, info, remove_snap);
    }

    changed = true;
  }

  if (changed) {
    dirty_info = true;
    dirty_big_info = true;
  }
}

void VolLog::write_log(
  ObjectStore::Transaction& t, const hobject_t &log_oid)
{
  if (is_dirty()) {
    dout(10) << "write_log with: "
	     << "dirty_to: " << dirty_to
	     << ", dirty_from: " << dirty_from
	     << ", dirty_divergent_priors: " << dirty_divergent_priors
	     << dendl;
    _write_log(t, log, log_oid, divergent_priors,
	       dirty_to,
	       dirty_from,
	       dirty_divergent_priors,
	       !touched_log,
               &log_keys_debug);
    undirty();
  } else {
    dout(10) << "log is not dirty" << dendl;
  }
}

void VolLog::write_log(ObjectStore::Transaction& t, vol_log_t &log,
    const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors)
{
  _write_log(t, log, log_oid, divergent_priors, eversion_t::max(), eversion_t(),
	     true, true, 0);
}

void VolLog::_write_log(
  ObjectStore::Transaction& t, vol_log_t &log,
  const hobject_t &log_oid, map<eversion_t, hobject_t> &divergent_priors,
  eversion_t dirty_to,
  eversion_t dirty_from,
  bool dirty_divergent_priors,
  bool touch_log,
  set<string> *log_keys_debug
  )
{
//dout(10) << "write_log, clearing up to " << dirty_to << dendl;
  if (touch_log)
    t.touch(coll_t(), log_oid);
  if (dirty_to != eversion_t()) {
    t.omap_rmkeyrange(
      coll_t(), log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    clear_up_to(log_keys_debug, dirty_to.get_key_name());
  }
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    //   dout(10) << "write_log, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll_t(), log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    clear_after(log_keys_debug, dirty_from.get_key_name());
  }

  map<string,bufferlist> keys;
  for (list<vol_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version < dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    keys[p->get_key_name()].claim(bl);
  }

  for (list<vol_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() && p->version >= dirty_from &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    keys[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = keys.begin();
	 i != keys.end();
	 ++i) {
      assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  if (dirty_divergent_priors) {
    //dout(10) << "write_log: writing divergent_priors" << dendl;
    ::encode(divergent_priors, keys["divergent_priors"]);
  }

  t.omap_setkeys(coll_t::META_COLL, log_oid, keys);
}

bool VolLog::read_log(ObjectStore *store, hobject_t log_oid,
		      const vol_info_t &info,
		      map<eversion_t, hobject_t> &divergent_priors,
		      IndexedLog &log,ostringstream &oss,
		      set<string> *log_keys_debug)
{
  dout(10) << "read_log" << dendl;
  bool rewrite_log = false;

  // legacy?
  struct stat st;
  int r = store->stat(coll_t::META_COLL, log_oid, &st);
  assert(r == 0);
  if (st.st_size > 0) {
    read_log_old(store, coll_t(), log_oid, info, divergent_priors, log,
		 oss, log_keys_debug);
    rewrite_log = true;
  } else {
    log.tail = info.log_tail;
    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(coll_t::META_COLL, log_oid);
    if (p) for (p->seek_to_first(); p->valid() ; p->next()) {
      bufferlist bl = p->value();//Copy bufferlist before creating iterator
      bufferlist::iterator bp = bl.begin();
      if (p->key() == "divergent_priors") {
	::decode(divergent_priors, bp);
	dout(20) << "read_log " << divergent_priors.size() << " divergent_priors" << dendl;
      } else {
	vol_log_entry_t e;
	e.decode_with_checksum(bp);
	dout(20) << "read_log " << e << dendl;
	if (!log.log.empty()) {
	  vol_log_entry_t last_e(log.log.back());
	  assert(last_e.version.version < e.version.version);
	  assert(last_e.version.epoch <= e.version.epoch);
	}
	log.log.push_back(e);
	log.head = e.version;
	if (log_keys_debug)
	  log_keys_debug->insert(e.get_key_name());
      }
    }
  }
  log.head = info.last_update;
  log.index();

  // build missing
  if (info.last_complete < info.last_update) {
    dout(10) << "read_log checking for missing items over interval (" << info.last_complete
	     << "," << info.last_update << "]" << dendl;

    set<hobject_t> did;
    for (list<vol_log_entry_t>::reverse_iterator i = log.log.rbegin();
	 i != log.log.rend();
	 ++i) {
      if (i->version <= info.last_complete) break;
      if (did.count(i->soid)) continue;
      did.insert(i->soid);

      if (i->is_delete()) continue;

      bufferlist bv;
      int r = store->getattr(coll_t(), i->soid, OI_ATTR, bv);
      if (r >= 0) {
	object_info_t oi(bv);
	if (oi.version < i->version) {
	  dout(15) << "read_log " << *i << " (have "
		   << oi.version << ")" << dendl;
	}
      }
    }
    for (map<eversion_t, hobject_t>::reverse_iterator i =
	   divergent_priors.rbegin();
	 i != divergent_priors.rend();
	 ++i) {
      if (i->first <= info.last_complete) break;
      if (did.count(i->second)) continue;
      did.insert(i->second);
      bufferlist bv;
      int r = store->getattr(coll_t(), i->second, OI_ATTR, bv);
      if (r >= 0) {
	object_info_t oi(bv);
	/**
	 * 1) we see this entry in the divergent priors mapping
	 * 2) we didn't see an entry for this object in the log
	 *
	 * From 1 & 2 we know that either the object does not exist
	 * or it is at the version specified in the divergent_priors
	 * map since the object would have been deleted atomically
	 * with the addition of the divergent_priors entry, an older
	 * version would not have been recovered, and a newer version
	 * would show up in the log above.
	 */
	assert(oi.version == i->first);
      }
    }
  }
  dout(10) << "read_log done" << dendl;
  return rewrite_log;
}

void VolLog::read_log_old(ObjectStore *store, coll_t coll, hobject_t log_oid,
			  const vol_info_t &info,
			  map<eversion_t, hobject_t> &divergent_priors,
			  IndexedLog &log, ostringstream &oss,
			  set<string> *log_keys_debug)
{
  // load bounds, based on old OndiskLog encoding.
  uint64_t ondisklog_tail = 0;
  uint64_t ondisklog_head = 0;
  uint64_t ondisklog_zero_to;
  bool ondisklog_has_checksums;

  bufferlist blb;
  store->collection_getattr(coll, "ondisklog", blb);
  {
    bufferlist::iterator bl = blb.begin();
    DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
    ondisklog_has_checksums = (struct_v >= 2);
    ::decode(ondisklog_tail, bl);
    ::decode(ondisklog_head, bl);
    if (struct_v >= 4)
      ::decode(ondisklog_zero_to, bl);
    else
      ondisklog_zero_to = 0;
    if (struct_v >= 5)
      ::decode(divergent_priors, bl);
    DECODE_FINISH(bl);
  }
  uint64_t ondisklog_length = ondisklog_head - ondisklog_tail;
  dout(10) << "read_log " << ondisklog_tail << "~" << ondisklog_length << dendl;
 
  log.tail = info.log_tail;

  // In case of sobject_t based encoding, may need to list objects in the store
  // to find hashes
  vector<hobject_t> ls;
  
  if (ondisklog_head > 0) {
    // read
    bufferlist bl;
    store->read(coll_t::META_COLL, log_oid, ondisklog_tail, ondisklog_length, bl);
    if (bl.length() < ondisklog_length) {
      std::ostringstream oss;
      oss << "read_log got " << bl.length() << " bytes, expected "
	  << ondisklog_head << "-" << ondisklog_tail << "="
	  << ondisklog_length;
      throw read_log_error(oss.str().c_str());
    }
    
    vol_log_entry_t e;
    bufferlist::iterator p = bl.begin();
    assert(log.empty());
    eversion_t last;
    bool reorder = false;
    bool listed_collection = false;

    while (!p.end()) {
      uint64_t pos = ondisklog_tail + p.get_off();
      if (ondisklog_has_checksums) {
	bufferlist ebl;
	::decode(ebl, p);
	__u32 crc;
	::decode(crc, p);
	
	__u32 got = ebl.crc32c(0);
	if (crc == got) {
	  bufferlist::iterator q = ebl.begin();
	  ::decode(e, q);
	} else {
	  std::ostringstream oss;
	  oss << "read_log " << pos << " bad crc got " << got << " expected" << crc;
	  throw read_log_error(oss.str().c_str());
	}
      } else {
	::decode(e, p);
      }
      dout(20) << "read_log " << pos << " " << e << dendl;

      // [repair] in order?
      if (e.version < last) {
	dout(0) << "read_log " << pos << " out of order entry " << e
		<< " follows " << last << dendl;
	oss << info.volid << " log has out of order entry "
	    << e << " following " << last << "\n";
	reorder = true;
      }

      if (e.version <= log.tail) {
	dout(20) << "read_log  ignoring entry at " << pos << " below log.tail"
		 << dendl;
	continue;
      }
      if (last.version == e.version.version) {
	dout(0) << "read_log  got dup " << e.version << " (last was "
		<< last << ", dropping that one)" << dendl;
	log.log.pop_back();
	oss << info.volid << " read_log got dup "
	    << e.version << " after " << last << "\n";
      }

      if (e.invalid_hash) {
	// We need to find the object in the store to get the hash
	if (!listed_collection) {
	  store->collection_list(coll, ls);
	  listed_collection = true;
	}
	bool found = false;
	for (vector<hobject_t>::iterator i = ls.begin();
	     i != ls.end();
	     ++i) {
	  if (i->oid == e.soid.oid && i->snap == e.soid.snap) {
	    e.soid = *i;
	    found = true;
	    break;
	  }
	}
	if (!found) {
	  // Didn't find the correct hash
	  std::ostringstream oss;
	  oss << "Could not find hash for hoid " << e.soid << std::endl;
	  throw read_log_error(oss.str().c_str());
	}
      }

      e.offset = pos;
      uint64_t endpos = ondisklog_tail + p.get_off();
      log.log.push_back(e);
      if (log_keys_debug)
	log_keys_debug->insert(e.get_key_name());
      last = e.version;

      // [repair] at end of log?
      if (!p.end() && e.version == info.last_update) {
	oss << info.volid << " log has extra data at "
	    << endpos << "~" << (ondisklog_head-endpos) << " after "
	    << info.last_update << "\n";

	dout(0) << "read_log " << endpos << " *** extra gunk at end of log, "
		<< "adjusting ondisklog_head" << dendl;
	ondisklog_head = endpos;
	break;
      }
    }
  
    if (reorder) {
      dout(0) << "read_log reordering log" << dendl;
      map<eversion_t, vol_log_entry_t> m;
      for (list<vol_log_entry_t>::iterator p = log.log.begin(); p != log.log.end(); ++p)
	m[p->version] = *p;
      log.log.clear();
      for (map<eversion_t, vol_log_entry_t>::iterator p = m.begin(); p != m.end(); ++p)
	log.log.push_back(p->second);
    }
  }
}

void vol_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(27, 26, bl);
  ::encode(volid, bl);
  ::encode(last_update, bl);
  ::encode(last_complete, bl);
  ::encode(log_tail, bl);
  ::encode(stats, bl);
  history.encode(bl);
  ::encode(purged_snaps, bl);
  ::encode(last_epoch_started, bl);
  ENCODE_FINISH(bl);
}

void vol_history_t::encode(bufferlist &bl) const
{
  ENCODE_START(6, 4, bl);
  ::encode(epoch_created, bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(same_interval_since, bl);
  ::encode(same_up_since, bl);
  ::encode(same_primary_since, bl);
  ENCODE_FINISH(bl);
}

void vol_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(13, 8, bl);
  ::encode(version, bl);
  ::encode(reported, bl);
  ::encode(state, bl);
  ::encode(log_start, bl);
  ::encode(ondisk_log_start, bl);
  ::encode(created, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(stats, bl);
  ::encode(log_size, bl);
  ::encode(ondisk_log_size, bl);
  ::encode(last_fresh, bl);
  ::encode(last_change, bl);
  ::encode(last_active, bl);
  ::encode(last_clean, bl);
  ::encode(last_unstale, bl);
  ::encode(mapping_epoch, bl);
  ::encode(stats_invalid, bl);
  ::encode(last_became_active, bl);
  ENCODE_FINISH(bl);
}
