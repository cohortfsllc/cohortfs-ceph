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
 *
 */

#include "PGLog.h"
#include "PG.h"
#include "OSDriver.h"
#include "../include/unordered_map.h"

#define dout_subsys ceph_subsys_osd

//////////////////// PGLog::IndexedLog ////////////////////

void PGLog::IndexedLog::split_into(
  pg_t child_pgid,
  unsigned split_bits,
  PGLog::IndexedLog *olog)
{
  list<pg_log_entry_t> oldlog;
  oldlog.swap(log);

  eversion_t old_tail;
  olog->head = head;
  olog->tail = tail;
  unsigned mask = ~((~0)<<split_bits);
  for (list<pg_log_entry_t>::iterator i = oldlog.begin();
       i != oldlog.end();
       ) {
    if ((i->soid.hash & mask) == child_pgid.m_seed) {
      olog->log.push_back(*i);
    } else {
      log.push_back(*i);
    }
    oldlog.erase(i++);
  }

  olog->index();
  index();
  olog->can_rollback_to = can_rollback_to;
}

void PGLog::IndexedLog::trim(
  LogEntryHandler *handler,
  eversion_t s,
  set<eversion_t> *trimmed)
{
  if (complete_to != log.end() &&
      complete_to->version <= s) {
    generic_dout(0) << " bad trim to " << s << " when complete_to is " << complete_to->version
		    << " on " << *this << dendl;
  }

  while (!log.empty()) {
    pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    generic_dout(20) << "trim " << e << dendl;
    if (trimmed)
      trimmed->insert(e.version);
    handler->trim(e);
    unindex(e);         // remove from index,
    log.pop_front();    // from log
  }

  // raise tail?
  if (tail < s)
    tail = s;
}

ostream& PGLog::IndexedLog::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) {
    out << *p << " " << (logged_object(p->soid) ? "indexed":"NOT INDEXED") << std::endl;
    assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }
  return out;
}

//////////////////// PGLog ////////////////////

void PGLog::clear() {
  log.zero();
  log_keys_debug.clear();
  undirty();
}

void PGLog::clear_info_log(
  pg_t pgid,
  const hobject_t &infos_oid,
  const hobject_t &log_oid,
  ObjectStore::Transaction *t) {

  set<string> keys_to_remove;
  keys_to_remove.insert(PG::get_epoch_key(pgid));
  keys_to_remove.insert(PG::get_info_key(pgid));

  t->remove(coll_t::META_COLL, log_oid);
  t->omap_rmkeys(coll_t::META_COLL, infos_oid, keys_to_remove);
}

void PGLog::trim(
  LogEntryHandler *handler,
  eversion_t trim_to,
  pg_info_t &info)
{
  // trim?
  if (trim_to > log.tail) {
    // We shouldn't be trimming the log past last_complete
    assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(handler, trim_to, &trimmed);
    info.log_tail = log.tail;
  }
}

void PGLog::write_log(
  ObjectStore::Transaction& t, const hobject_t &log_oid)
{
  if (is_dirty()) {
    dout(10) << "write_log with: "
	     << "dirty_to: " << dirty_to
	     << ", dirty_from: " << dirty_from
	     << ", writeout_from: " << writeout_from
	     << ", trimmed: " << trimmed
	     << dendl;
    _write_log(
      t, log, log_oid,
      dirty_to,
      dirty_from,
      writeout_from,
      trimmed,
      !touched_log,
      (pg_log_debug ? &log_keys_debug : 0));
    undirty();
  } else {
    dout(10) << "log is not dirty" << dendl;
  }
}

void PGLog::write_log(ObjectStore::Transaction& t, pg_log_t &log,
		      const hobject_t &log_oid)
{
  _write_log(t, log, log_oid, eversion_t::max(), eversion_t(), eversion_t(),
	     set<eversion_t>(), true, 0);
}

void PGLog::_write_log(
  ObjectStore::Transaction& t, pg_log_t &log,
  const hobject_t &log_oid,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  const set<eversion_t> &trimmed,
  bool touch_log,
  set<string> *log_keys_debug
  )
{
  set<string> to_remove;
  for (set<eversion_t>::const_iterator i = trimmed.begin();
       i != trimmed.end();
       ++i) {
    to_remove.insert(i->get_key_name());
    if (log_keys_debug) {
      assert(log_keys_debug->count(i->get_key_name()));
      log_keys_debug->erase(i->get_key_name());
    }
  }

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
  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version < dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    keys[p->get_key_name()].claim(bl);
  }

  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
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

  ::encode(log.can_rollback_to, keys["can_rollback_to"]);

  t.omap_rmkeys(coll_t::META_COLL, log_oid, to_remove);
  t.omap_setkeys(coll_t::META_COLL, log_oid, keys);
}

bool PGLog::read_log(ObjectStore *store, coll_t coll, hobject_t log_oid,
		     const pg_info_t &info, IndexedLog &log,
		     ostringstream &oss, set<string> *log_keys_debug)
{
  dout(10) << "read_log" << dendl;
  bool rewrite_log = false;

  // legacy?
  struct stat st;
  int r = store->stat(coll_t::META_COLL, log_oid, &st);
  assert(r == 0);
  if (st.st_size > 0) {
    read_log_old(store, coll, log_oid, info, log, oss, log_keys_debug);
    rewrite_log = true;
  } else {
    log.tail = info.log_tail;
    // will get overridden below if it had been recorded
    log.can_rollback_to = info.last_update;
    ObjectMap::ObjectMapIterator p = store->get_omap_iterator(coll_t::META_COLL, log_oid);
    if (p) for (p->seek_to_first(); p->valid() ; p->next()) {
      bufferlist bl = p->value();//Copy bufferlist before creating iterator
      bufferlist::iterator bp = bl.begin();
      if (p->key() == "can_rollback_to") {
	bufferlist bl = p->value();
	bufferlist::iterator bp = bl.begin();
	::decode(log.can_rollback_to, bp);
      } else {
	pg_log_entry_t e;
	e.decode_with_checksum(bp);
	dout(20) << "read_log " << e << dendl;
	if (!log.log.empty()) {
	  pg_log_entry_t last_e(log.log.back());
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

  if (info.last_complete < info.last_update) {
    set<hobject_t> did;
    for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
	 i != log.log.rend();
	 ++i) {
      if (i->version <= info.last_complete) break;
      if (i->soid > info.last_backfill) continue;
      if (did.count(i->soid)) continue;
      did.insert(i->soid);

      if (i->is_delete()) continue;

      bufferlist bv;
      int r = store->getattr(coll, i->soid, OI_ATTR, bv);
      if (r >= 0) {
	object_info_t oi(bv);
      }
    }
  }
  dout(10) << "read_log done" << dendl;
  return rewrite_log;
}

void PGLog::read_log_old(ObjectStore *store, coll_t coll, hobject_t log_oid,
			 const pg_info_t &info,
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
    DECODE_FINISH(bl);
  }
  uint64_t ondisklog_length = ondisklog_head - ondisklog_tail;
  dout(10) << "read_log " << ondisklog_tail << "~" << ondisklog_length << dendl;
 
  log.tail = info.log_tail;

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
    
    pg_log_entry_t e;
    bufferlist::iterator p = bl.begin();
    assert(log.empty());
    eversion_t last;
    bool reorder = false;

    while (!p.end()) {
      uint64_t pos = ondisklog_tail + p.get_off();
      if (ondisklog_has_checksums) {
	bufferlist ebl;
	::decode(ebl, p);
	uint32_t crc;
	::decode(crc, p);
	
	uint32_t got = ebl.crc32c(0);
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
	dout(0) << "read_log " << pos << " out of order entry " << e << " follows " << last << dendl;
	oss << info.pgid << " log has out of order entry "
	      << e << " following " << last << "\n";
	reorder = true;
      }

      if (e.version <= log.tail) {
	dout(20) << "read_log  ignoring entry at " << pos << " below log.tail" << dendl;
	continue;
      }
      if (last.version == e.version.version) {
	dout(0) << "read_log  got dup " << e.version << " (last was " << last << ", dropping that one)" << dendl;
	log.log.pop_back();
	oss << info.pgid << " read_log got dup "
	      << e.version << " after " << last << "\n";
      }

      assert(!e.invalid_hash);

      if (e.invalid_pool) {
	e.soid.pool = info.pgid.pool();
      }

      e.offset = pos;
      uint64_t endpos = ondisklog_tail + p.get_off();
      log.log.push_back(e);
      if (log_keys_debug)
	log_keys_debug->insert(e.get_key_name());
      last = e.version;

      // [repair] at end of log?
      if (!p.end() && e.version == info.last_update) {
	oss << info.pgid << " log has extra data at "
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
      map<eversion_t, pg_log_entry_t> m;
      for (list<pg_log_entry_t>::iterator p = log.log.begin(); p != log.log.end(); ++p)
	m[p->version] = *p;
      log.log.clear();
      for (map<eversion_t, pg_log_entry_t>::iterator p = m.begin(); p != m.end(); ++p)
	log.log.push_back(p->second);
    }
  }
}
