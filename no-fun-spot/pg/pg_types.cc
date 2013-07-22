// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <string>
#include <iostream>
#include "osd/osd_types.h"
#include "pg_types.h"
#include "PG.h"


// -- pg_t --

int pg_t::print(char *o, int maxlen) const
{
  if (preferred() >= 0)
    return snprintf(o, maxlen, "%llu.%xp%d", (unsigned long long)pool(), ps(), preferred());
  else
    return snprintf(o, maxlen, "%llu.%x", (unsigned long long)pool(), ps());
}

bool pg_t::parse(const char *s)
{
  uint64_t ppool;
  uint32_t pseed;
  int32_t pref;
  int r = sscanf(s, "%llu.%xp%d", (long long unsigned *)&ppool, &pseed, &pref);
  if (r < 2)
    return false;
  m_pool = ppool;
  m_seed = pseed;
  if (r == 3)
    m_preferred = pref;
  else
    m_preferred = -1;
  return true;
}

bool pg_t::is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *children) const
{
  assert(m_seed < old_pg_num);
  if (new_pg_num <= old_pg_num)
    return false;

  bool split = false;
  if (true) {
    int old_bits = pg_pool_t::calc_bits_of(old_pg_num);
    int old_mask = (1 << old_bits) - 1;
    for (int n = 1; ; n++) {
      int next_bit = (n << (old_bits-1));
      unsigned s = next_bit | m_seed;

      if (s < old_pg_num || s == m_seed)
	continue;
      if (s >= new_pg_num)
	break;
      if ((unsigned)ceph_stable_mod(s, old_pg_num, old_mask) == m_seed) {
	split = true;
	if (children)
	  children->insert(pg_t(s, m_pool, m_preferred));
      }
    }
  }
  if (false) {
    // brute force
    int old_bits = pg_pool_t::calc_bits_of(old_pg_num);
    int old_mask = (1 << old_bits) - 1;
    for (unsigned x = old_pg_num; x < new_pg_num; ++x) {
      unsigned o = ceph_stable_mod(x, old_pg_num, old_mask);
      if (o == m_seed) {
	split = true;
	children->insert(pg_t(x, m_pool, m_preferred));
      }
    }
  }
  return split;
}

unsigned pg_t::get_split_bits(unsigned pg_num) const {
  if (pg_num == 1)
    return 0;
  assert(pg_num > 1);

  // Find unique p such that pg_num \in [2^(p-1), 2^p)
  unsigned p = pg_pool_t::calc_bits_of(pg_num);

  if ((m_seed % (1<<(p-1))) < (pg_num % (1<<(p-1))))
    return p;
  else
    return p - 1;
}

pg_t pg_t::get_parent() const
{
  unsigned bits = pg_pool_t::calc_bits_of(m_seed);
  assert(bits);
  pg_t retval = *this;
  retval.m_seed &= ~((~0)<<(bits - 1));
  return retval;
}

void pg_t::dump(Formatter *f) const
{
  f->dump_unsigned("pool", m_pool);
  f->dump_unsigned("seed", m_seed);
  f->dump_int("preferred_osd", m_preferred);
}

void pg_t::generate_test_instances(list<pg_t*>& o)
{
  o.push_back(new pg_t);
  o.push_back(new pg_t(1, 2, -1));
  o.push_back(new pg_t(13123, 3, -1));
  o.push_back(new pg_t(131223, 4, 23));
}

ostream& operator<<(ostream& out, const pg_t &pg)
{
  out << pg.pool() << '.';
  out << hex << pg.ps() << dec;

  if (pg.preferred() >= 0)
    out << 'p' << pg.preferred();

  //out << "=" << hex << (__uint64_t)pg << dec;
  return out;
}

// ---

// ---

std::string pg_state_string(int state)
{
  ostringstream oss;
  if (state & PG_STATE_STALE)
    oss << "stale+";
  if (state & PG_STATE_CREATING)
    oss << "creating+";
  if (state & PG_STATE_ACTIVE)
    oss << "active+";
  if (state & PG_STATE_CLEAN)
    oss << "clean+";
  if (state & PG_STATE_RECOVERY_WAIT)
    oss << "recovery_wait+";
  if (state & PG_STATE_RECOVERING)
    oss << "recovering+";
  if (state & PG_STATE_DOWN)
    oss << "down+";
  if (state & PG_STATE_REPLAY)
    oss << "replay+";
  if (state & PG_STATE_SPLITTING)
    oss << "splitting+";
  if (state & PG_STATE_DEGRADED)
    oss << "degraded+";
  if (state & PG_STATE_REMAPPED)
    oss << "remapped+";
  if (state & PG_STATE_SCRUBBING)
    oss << "scrubbing+";
  if (state & PG_STATE_DEEP_SCRUB)
    oss << "deep+";
  if (state & PG_STATE_SCRUBQ)
    oss << "scrubq+";
  if (state & PG_STATE_INCONSISTENT)
    oss << "inconsistent+";
  if (state & PG_STATE_PEERING)
    oss << "peering+";
  if (state & PG_STATE_REPAIR)
    oss << "repair+";
  if ((state & PG_STATE_BACKFILL_WAIT) &&
      !(state &PG_STATE_BACKFILL))
    oss << "wait_backfill+";
  if (state & PG_STATE_BACKFILL)
    oss << "backfilling+";
  if (state & PG_STATE_BACKFILL_TOOFULL)
    oss << "backfill_toofull+";
  if (state & PG_STATE_INCOMPLETE)
    oss << "incomplete+";
  string ret(oss.str());
  if (ret.length() > 0)
    ret.resize(ret.length() - 1);
  else
    ret = "inactive";
  return ret;
}

// -- pool_snap_info_t --
void pool_snap_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("snapid", snapid);
  f->dump_stream("stamp") << stamp;
  f->dump_string("name", name);
}

void pool_snap_info_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(snapid, bl);
    ::encode(stamp, bl);
    ::encode(name, bl);
    return;
  }
  ENCODE_START(2, 2, bl);
  ::encode(snapid, bl);
  ::encode(stamp, bl);
  ::encode(name, bl);
  ENCODE_FINISH(bl);
}

void pool_snap_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(snapid, bl);
  ::decode(stamp, bl);
  ::decode(name, bl);
  DECODE_FINISH(bl);
}

void pool_snap_info_t::generate_test_instances(list<pool_snap_info_t*>& o)
{
  o.push_back(new pool_snap_info_t);
  o.push_back(new pool_snap_info_t);
  o.back()->snapid = 1;
  o.back()->stamp = utime_t(1, 2);
  o.back()->name = "foo";
}

// -- pg_pool_t --

void pg_pool_t::dump(Formatter *f) const
{
  f->dump_unsigned("flags", get_flags());
  f->dump_int("type", get_type());
  f->dump_int("size", get_size());
  f->dump_int("min_size", get_min_size());
  f->dump_int("crush_ruleset", get_crush_ruleset());
  f->dump_int("object_hash", get_object_hash());
  f->dump_int("pg_num", get_pg_num());
  f->dump_int("pg_placement_num", get_pgp_num());
  f->dump_unsigned("crash_replay_interval", get_crash_replay_interval());
  f->dump_stream("last_change") << get_last_change();
  f->dump_unsigned("auid", get_auid());
  f->dump_string("snap_mode", is_pool_snaps_mode() ? "pool" : "selfmanaged");
  f->dump_unsigned("snap_seq", get_snap_seq());
  f->dump_unsigned("snap_epoch", get_snap_epoch());
  f->open_object_section("pool_snaps");
  for (map<snapid_t, pool_snap_info_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
    f->open_object_section("pool_snap_info");
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_stream("removed_snaps") << removed_snaps;
  f->dump_int("quota_max_bytes", quota_max_bytes);
  f->dump_int("quota_max_objects", quota_max_objects);
}


int pg_pool_t::calc_bits_of(int t)
{
  int b = 0;
  while (t > 0) {
    t = t >> 1;
    b++;
  }
  return b;
}

void pg_pool_t::calc_pg_masks()
{
  pg_num_mask = (1 << calc_bits_of(pg_num-1)) - 1;
  pgp_num_mask = (1 << calc_bits_of(pgp_num-1)) - 1;
}

/*
 * we have two snap modes:
 *  - pool global snaps
 *    - snap existence/non-existence defined by snaps[] and snap_seq
 *  - user managed snaps
 *    - removal governed by removed_snaps
 *
 * we know which mode we're using based on whether removed_snaps is empty.
 */
bool pg_pool_t::is_pool_snaps_mode() const
{
  return removed_snaps.empty() && get_snap_seq() > 0;
}

bool pg_pool_t::is_unmanaged_snaps_mode() const
{
  return removed_snaps.size() && get_snap_seq() > 0;
}

bool pg_pool_t::is_removed_snap(snapid_t s) const
{
  if (is_pool_snaps_mode())
    return s <= get_snap_seq() && snaps.count(s) == 0;
  else
    return removed_snaps.contains(s);
}

/*
 * build set of known-removed sets from either pool snaps or
 * explicit removed_snaps set.
 */
void pg_pool_t::build_removed_snaps(interval_set<snapid_t>& rs) const
{
  if (is_pool_snaps_mode()) {
    rs.clear();
    for (snapid_t s = 1; s <= get_snap_seq(); s = s + 1)
      if (snaps.count(s) == 0)
	rs.insert(s);
  } else {
    rs = removed_snaps;
  }
}

snapid_t pg_pool_t::snap_exists(const char *s) const
{
  for (map<snapid_t,pool_snap_info_t>::const_iterator p = snaps.begin();
       p != snaps.end();
       ++p)
    if (p->second.name == s)
      return p->second.snapid;
  return 0;
}

void pg_pool_t::add_snap(const char *n, utime_t stamp)
{
  assert(!is_unmanaged_snaps_mode());
  snapid_t s = get_snap_seq() + 1;
  snap_seq = s;
  snaps[s].snapid = s;
  snaps[s].name = n;
  snaps[s].stamp = stamp;
}

void pg_pool_t::add_unmanaged_snap(uint64_t& snapid)
{
  if (removed_snaps.empty()) {
    assert(!is_pool_snaps_mode());
    removed_snaps.insert(snapid_t(1));
    snap_seq = 1;
  }
  snapid = snap_seq = snap_seq + 1;
}

void pg_pool_t::remove_snap(snapid_t s)
{
  assert(snaps.count(s));
  snaps.erase(s);
  snap_seq = snap_seq + 1;
}

void pg_pool_t::remove_unmanaged_snap(snapid_t s)
{
  assert(is_unmanaged_snaps_mode());
  removed_snaps.insert(s);
  snap_seq = snap_seq + 1;
  removed_snaps.insert(get_snap_seq());
}

SnapContext pg_pool_t::get_snap_context() const
{
  vector<snapid_t> s(snaps.size());
  unsigned i = 0;
  for (map<snapid_t, pool_snap_info_t>::const_reverse_iterator p = snaps.rbegin();
       p != snaps.rend();
       ++p)
    s[i++] = p->first;
  return SnapContext(get_snap_seq(), s);
}

/*
 * map a raw pg (with full precision ps) into an actual pg, for storage
 */
pg_t pg_pool_t::raw_pg_to_pg(pg_t pg) const
{
  pg.set_ps(ceph_stable_mod(pg.ps(), pg_num, pg_num_mask));
  return pg;
}
  
/*
 * map raw pg (full precision ps) into a placement seed.  include
 * pool id in that value so that different pools don't use the same
 * seeds.
 */
ps_t pg_pool_t::raw_pg_to_pps(pg_t pg) const
{
  if (flags & FLAG_HASHPSPOOL) {
    // Hash the pool id so that pool PGs do not overlap.
    return
      crush_hash32_2(CRUSH_HASH_RJENKINS1,
		     ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask),
		     pg.pool());
  } else {
    // Legacy behavior; add ps and pool together.  This is not a great
    // idea because the PGs from each pool will essentially overlap on
    // top of each other: 0.5 == 1.4 == 2.3 == ...
    return
      ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask) +
      pg.pool();
  }
}

void pg_pool_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    // this encoding matches the old struct ceph_pg_pool
    __u8 struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ::encode(crush_ruleset, bl);
    ::encode(object_hash, bl);
    ::encode(pg_num, bl);
    ::encode(pgp_num, bl);
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    ::encode(lpg_num, bl);
    ::encode(lpgp_num, bl);
    ::encode(last_change, bl);
    ::encode(snap_seq, bl);
    ::encode(snap_epoch, bl);

    __u32 n = snaps.size();
    ::encode(n, bl);
    n = removed_snaps.num_intervals();
    ::encode(n, bl);

    ::encode(auid, bl);

    ::encode_nohead(snaps, bl, features);
    removed_snaps.encode_nohead(bl);
    return;
  }

  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    __u8 struct_v = 4;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ::encode(crush_ruleset, bl);
    ::encode(object_hash, bl);
    ::encode(pg_num, bl);
    ::encode(pgp_num, bl);
    __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    ::encode(lpg_num, bl);
    ::encode(lpgp_num, bl);
    ::encode(last_change, bl);
    ::encode(snap_seq, bl);
    ::encode(snap_epoch, bl);
    ::encode(snaps, bl, features);
    ::encode(removed_snaps, bl);
    ::encode(auid, bl);
    ::encode(flags, bl);
    ::encode(crash_replay_interval, bl);
    return;
  }

  ENCODE_START(8, 5, bl);
  ::encode(type, bl);
  ::encode(size, bl);
  ::encode(crush_ruleset, bl);
  ::encode(object_hash, bl);
  ::encode(pg_num, bl);
  ::encode(pgp_num, bl);
  __u32 lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
  ::encode(lpg_num, bl);
  ::encode(lpgp_num, bl);
  ::encode(last_change, bl);
  ::encode(snap_seq, bl);
  ::encode(snap_epoch, bl);
  ::encode(snaps, bl, features);
  ::encode(removed_snaps, bl);
  ::encode(auid, bl);
  ::encode(flags, bl);
  ::encode(crash_replay_interval, bl);
  ::encode(min_size, bl);
  ::encode(quota_max_bytes, bl);
  ::encode(quota_max_objects, bl);
  ENCODE_FINISH(bl);
}

void pg_pool_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
  ::decode(type, bl);
  ::decode(size, bl);
  ::decode(crush_ruleset, bl);
  ::decode(object_hash, bl);
  ::decode(pg_num, bl);
  ::decode(pgp_num, bl);
  {
    __u32 lpg_num, lpgp_num;
    ::decode(lpg_num, bl);
    ::decode(lpgp_num, bl);
  }
  ::decode(last_change, bl);
  ::decode(snap_seq, bl);
  ::decode(snap_epoch, bl);

  if (struct_v >= 3) {
    ::decode(snaps, bl);
    ::decode(removed_snaps, bl);
    ::decode(auid, bl);
  } else {
    __u32 n, m;
    ::decode(n, bl);
    ::decode(m, bl);
    ::decode(auid, bl);
    ::decode_nohead(n, snaps, bl);
    removed_snaps.decode_nohead(m, bl);
  }

  if (struct_v >= 4) {
    ::decode(flags, bl);
    ::decode(crash_replay_interval, bl);
  } else {
    flags = 0;

    // if this looks like the 'data' pool, set the
    // crash_replay_interval appropriately.  unfortunately, we can't
    // be precise here.  this should be good enough to preserve replay
    // on the data pool for the majority of cluster upgrades, though.
    if (crush_ruleset == 0 && auid == 0)
      crash_replay_interval = 60;
    else
      crash_replay_interval = 0;
  }
  if (struct_v >= 7) {
    ::decode(min_size, bl);
  } else {
    min_size = size - size/2;
  }
  if (struct_v >= 8) {
    ::decode(quota_max_bytes, bl);
    ::decode(quota_max_objects, bl);
  }
  DECODE_FINISH(bl);
  calc_pg_masks();
}

void pg_pool_t::generate_test_instances(list<pg_pool_t*>& o)
{
  pg_pool_t a;
  o.push_back(new pg_pool_t(a));

  a.type = TYPE_REP;
  a.size = 2;
  a.crush_ruleset = 3;
  a.object_hash = 4;
  a.pg_num = 6;
  a.pgp_num = 5;
  a.last_change = 9;
  a.snap_seq = 10;
  a.snap_epoch = 11;
  a.auid = 12;
  a.crash_replay_interval = 13;
  a.quota_max_bytes = 473;
  a.quota_max_objects = 474;
  o.push_back(new pg_pool_t(a));

  a.snaps[3].name = "asdf";
  a.snaps[3].snapid = 3;
  a.snaps[3].stamp = utime_t(123, 4);
  a.snaps[6].name = "qwer";
  a.snaps[6].snapid = 6;
  a.snaps[6].stamp = utime_t(23423, 4);
  o.push_back(new pg_pool_t(a));

  a.removed_snaps.insert(2);   // not quite valid to combine with snaps!
  a.quota_max_bytes = 2473;
  a.quota_max_objects = 4374;
  o.push_back(new pg_pool_t(a));
}

ostream& operator<<(ostream& out, const pg_pool_t& p)
{
  out << p.get_type_name()
      << " size " << p.get_size()
      << " min_size " << p.get_min_size()
      << " crush_ruleset " << p.get_crush_ruleset()
      << " object_hash " << p.get_object_hash_name()
      << " pg_num " << p.get_pg_num()
      << " pgp_num " << p.get_pgp_num()
      << " last_change " << p.get_last_change()
      << " owner " << p.get_auid();
  if (p.flags)
    out << " flags " << p.flags;
  if (p.crash_replay_interval)
    out << " crash_replay_interval " << p.crash_replay_interval;
  if (p.quota_max_bytes)
    out << " max_bytes " << p.quota_max_bytes;
  if (p.quota_max_objects)
    out << " max_objects " << p.quota_max_objects;
  return out;
}

// -- pg_stat_t --

void pg_stat_t::dump(Formatter *f) const
{
  f->dump_stream("version") << version;
  f->dump_stream("reported") << reported;
  f->dump_string("state", pg_state_string(state));
  f->dump_stream("last_fresh") << last_fresh;
  f->dump_stream("last_change") << last_change;
  f->dump_stream("last_active") << last_active;
  f->dump_stream("last_clean") << last_clean;
  f->dump_stream("last_became_active") << last_became_active;
  f->dump_stream("last_unstale") << last_unstale;
  f->dump_unsigned("mapping_epoch", mapping_epoch);
  f->dump_stream("log_start") << log_start;
  f->dump_stream("ondisk_log_start") << ondisk_log_start;
  f->dump_unsigned("created", created);
  f->dump_unsigned("last_epoch_clean", last_epoch_clean);
  f->dump_stream("parent") << parent;
  f->dump_unsigned("parent_split_bits", parent_split_bits);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_stream("last_deep_scrub") << last_deep_scrub;
  f->dump_stream("last_deep_scrub_stamp") << last_deep_scrub_stamp;
  f->dump_stream("last_clean_scrub_stamp") << last_clean_scrub_stamp;
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
  f->dump_stream("stats_invalid") << stats_invalid;
  stats.dump(f);
  f->open_array_section("up");
  for (vector<int>::const_iterator p = up.begin(); p != up.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (vector<int>::const_iterator p = acting.begin(); p != acting.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
}

void pg_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(13, 8, bl);
  ::encode(version, bl);
  ::encode(reported, bl);
  ::encode(state, bl);
  ::encode(log_start, bl);
  ::encode(ondisk_log_start, bl);
  ::encode(created, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(parent, bl);
  ::encode(parent_split_bits, bl);
  ::encode(last_scrub, bl);
  ::encode(last_scrub_stamp, bl);
  ::encode(stats, bl);
  ::encode(log_size, bl);
  ::encode(ondisk_log_size, bl);
  ::encode(up, bl);
  ::encode(acting, bl);
  ::encode(last_fresh, bl);
  ::encode(last_change, bl);
  ::encode(last_active, bl);
  ::encode(last_clean, bl);
  ::encode(last_unstale, bl);
  ::encode(mapping_epoch, bl);
  ::encode(last_deep_scrub, bl);
  ::encode(last_deep_scrub_stamp, bl);
  ::encode(stats_invalid, bl);
  ::encode(last_clean_scrub_stamp, bl);
  ::encode(last_became_active, bl);
  ENCODE_FINISH(bl);
}

void pg_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(13, 8, 8, bl);
  ::decode(version, bl);
  ::decode(reported, bl);
  ::decode(state, bl);
  ::decode(log_start, bl);
  ::decode(ondisk_log_start, bl);
  ::decode(created, bl);
  if (struct_v >= 7)
    ::decode(last_epoch_clean, bl);
  else
    last_epoch_clean = 0;
  if (struct_v < 6) {
    old_pg_t opgid;
    ::decode(opgid, bl);
    parent = opgid;
  } else {
    ::decode(parent, bl);
  }
  ::decode(parent_split_bits, bl);
  ::decode(last_scrub, bl);
  ::decode(last_scrub_stamp, bl);
  if (struct_v <= 4) {
    ::decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    ::decode(num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    ::decode(stats.sum.num_object_clones, bl);
    ::decode(stats.sum.num_object_copies, bl);
    ::decode(stats.sum.num_objects_missing_on_primary, bl);
    ::decode(stats.sum.num_objects_degraded, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    if (struct_v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
    if (struct_v >= 3) {
      ::decode(up, bl);
    }
    if (struct_v == 4) {
      ::decode(stats.sum.num_objects_unfound, bl);  // sigh.
    }
    ::decode(acting, bl);
  } else {
    ::decode(stats, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    ::decode(up, bl);
    ::decode(acting, bl);
    if (struct_v >= 9) {
      ::decode(last_fresh, bl);
      ::decode(last_change, bl);
      ::decode(last_active, bl);
      ::decode(last_clean, bl);
      ::decode(last_unstale, bl);
      ::decode(mapping_epoch, bl);
      if (struct_v >= 10) {
        ::decode(last_deep_scrub, bl);
        ::decode(last_deep_scrub_stamp, bl);
      }
    }
  }
  if (struct_v < 11) {
    stats_invalid = false;
  } else {
    ::decode(stats_invalid, bl);
  }
  if (struct_v >= 12) {
    ::decode(last_clean_scrub_stamp, bl);
  } else {
    last_clean_scrub_stamp = utime_t();
  }
  if (struct_v >= 13) {
    ::decode(last_became_active, bl);
  } else {
    last_became_active = last_active;
  }
  DECODE_FINISH(bl);
}

void pg_stat_t::generate_test_instances(list<pg_stat_t*>& o)
{
  pg_stat_t a;
  o.push_back(new pg_stat_t(a));

  a.version = eversion_t(1, 3);
  a.reported = eversion_t(1, 2);
  a.state = 123;
  a.mapping_epoch = 998;
  a.last_fresh = utime_t(1002, 1);
  a.last_change = utime_t(1002, 2);
  a.last_active = utime_t(1002, 3);
  a.last_clean = utime_t(1002, 4);
  a.last_unstale = utime_t(1002, 5);
  a.log_start = eversion_t(1, 4);
  a.ondisk_log_start = eversion_t(1, 5);
  a.created = 6;
  a.last_epoch_clean = 7;
  a.parent = pg_t(1, 2, 3);
  a.parent_split_bits = 12;
  a.last_scrub = eversion_t(9, 10);
  a.last_scrub_stamp = utime_t(11, 12);
  a.last_deep_scrub = eversion_t(13, 14);
  a.last_deep_scrub_stamp = utime_t(15, 16);
  a.last_clean_scrub_stamp = utime_t(17, 18);
  list<object_stat_collection_t*> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = *l.back();
  a.log_size = 99;
  a.ondisk_log_size = 88;
  a.up.push_back(123);
  a.acting.push_back(456);
  o.push_back(new pg_stat_t(a));
}


// -- pool_stat_t --

void pool_stat_t::dump(Formatter *f) const
{
  stats.dump(f);
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
}

void pool_stat_t::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    __u8 v = 4;
    ::encode(v, bl);
    ::encode(stats, bl);
    ::encode(log_size, bl);
    ::encode(ondisk_log_size, bl);
    return;
  }

  ENCODE_START(5, 5, bl);
  ::encode(stats, bl);
  ::encode(log_size, bl);
  ::encode(ondisk_log_size, bl);
  ENCODE_FINISH(bl);
}

void pool_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
  if (struct_v >= 4) {
    ::decode(stats, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
  } else {
    ::decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    ::decode(num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    ::decode(stats.sum.num_object_clones, bl);
    ::decode(stats.sum.num_object_copies, bl);
    ::decode(stats.sum.num_objects_missing_on_primary, bl);
    ::decode(stats.sum.num_objects_degraded, bl);
    ::decode(log_size, bl);
    ::decode(ondisk_log_size, bl);
    if (struct_v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
    if (struct_v >= 3) {
      ::decode(stats.sum.num_objects_unfound, bl);
    }
  }
  DECODE_FINISH(bl);
}

void pool_stat_t::generate_test_instances(list<pool_stat_t*>& o)
{
  pool_stat_t a;
  o.push_back(new pool_stat_t(a));

  list<object_stat_collection_t*> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = *l.back();
  a.log_size = 123;
  a.ondisk_log_size = 456;
  o.push_back(new pool_stat_t(a));
}


// -- pg_history_t --

void pg_history_t::encode(bufferlist &bl) const
{
  ENCODE_START(6, 4, bl);
  ::encode(epoch_created, bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(last_epoch_split, bl);
  ::encode(same_interval_since, bl);
  ::encode(same_up_since, bl);
  ::encode(same_primary_since, bl);
  ::encode(last_scrub, bl);
  ::encode(last_scrub_stamp, bl);
  ::encode(last_deep_scrub, bl);
  ::encode(last_deep_scrub_stamp, bl);
  ::encode(last_clean_scrub_stamp, bl);
  ENCODE_FINISH(bl);
}

void pg_history_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 4, 4, bl);
  ::decode(epoch_created, bl);
  ::decode(last_epoch_started, bl);
  if (struct_v >= 3)
    ::decode(last_epoch_clean, bl);
  else
    last_epoch_clean = last_epoch_started;  // careful, it's a lie!
  ::decode(last_epoch_split, bl);
  ::decode(same_interval_since, bl);
  ::decode(same_up_since, bl);
  ::decode(same_primary_since, bl);
  if (struct_v >= 2) {
    ::decode(last_scrub, bl);
    ::decode(last_scrub_stamp, bl);
  }
  if (struct_v >= 5) {
    ::decode(last_deep_scrub, bl);
    ::decode(last_deep_scrub_stamp, bl);
  }
  if (struct_v >= 6) {
    ::decode(last_clean_scrub_stamp, bl);
  }
  DECODE_FINISH(bl);
}

void pg_history_t::dump(Formatter *f) const
{
  f->dump_int("epoch_created", epoch_created);
  f->dump_int("last_epoch_started", last_epoch_started);
  f->dump_int("last_epoch_clean", last_epoch_clean);
  f->dump_int("last_epoch_split", last_epoch_split);
  f->dump_int("same_up_since", same_up_since);
  f->dump_int("same_interval_since", same_interval_since);
  f->dump_int("same_primary_since", same_primary_since);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_stream("last_deep_scrub") << last_deep_scrub;
  f->dump_stream("last_deep_scrub_stamp") << last_deep_scrub_stamp;
  f->dump_stream("last_clean_scrub_stamp") << last_clean_scrub_stamp;
}

void pg_history_t::generate_test_instances(list<pg_history_t*>& o)
{
  o.push_back(new pg_history_t);
  o.push_back(new pg_history_t);
  o.back()->epoch_created = 1;
  o.back()->last_epoch_started = 2;
  o.back()->last_epoch_clean = 3;
  o.back()->last_epoch_split = 4;
  o.back()->same_up_since = 5;
  o.back()->same_interval_since = 6;
  o.back()->same_primary_since = 7;
  o.back()->last_scrub = eversion_t(8, 9);
  o.back()->last_scrub_stamp = utime_t(10, 11);
  o.back()->last_deep_scrub = eversion_t(12, 13);
  o.back()->last_deep_scrub_stamp = utime_t(14, 15);
  o.back()->last_clean_scrub_stamp = utime_t(16, 17);
}


// -- pg_info_t --

void pg_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(27, 26, bl);
  ::encode(pgid, bl);
  ::encode(last_update, bl);
  ::encode(last_complete, bl);
  ::encode(log_tail, bl);
  ::encode(last_backfill, bl);
  ::encode(stats, bl);
  history.encode(bl);
  ::encode(purged_snaps, bl);
  ::encode(last_epoch_started, bl);
  ENCODE_FINISH(bl);
}

void pg_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(27, 26, 26, bl);
  if (struct_v < 23) {
    old_pg_t opgid;
    ::decode(opgid, bl);
    pgid = opgid;
  } else {
    ::decode(pgid, bl);
  }
  ::decode(last_update, bl);
  ::decode(last_complete, bl);
  ::decode(log_tail, bl);
  if (struct_v < 25) {
    bool log_backlog;
    ::decode(log_backlog, bl);
  }
  if (struct_v >= 24)
    ::decode(last_backfill, bl);
  ::decode(stats, bl);
  history.decode(bl);
  if (struct_v >= 22)
    ::decode(purged_snaps, bl);
  else {
    set<snapid_t> snap_trimq;
    ::decode(snap_trimq, bl);
  }
  if (struct_v < 27) {
    last_epoch_started = history.last_epoch_started;
  } else {
    ::decode(last_epoch_started, bl);
  }
  DECODE_FINISH(bl);
}

// -- pg_info_t --

void pg_info_t::dump(Formatter *f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_stream("last_update") << last_update;
  f->dump_stream("last_complete") << last_complete;
  f->dump_stream("log_tail") << log_tail;
  f->dump_stream("last_backfill") << last_backfill;
  f->dump_stream("purged_snaps") << purged_snaps;
  f->open_object_section("history");
  history.dump(f);
  f->close_section();
  f->open_object_section("stats");
  stats.dump(f);
  f->close_section();

  f->dump_int("empty", is_empty());
  f->dump_int("dne", dne());
  f->dump_int("incomplete", is_incomplete());
  f->dump_int("last_epoch_started", last_epoch_started);
}

void pg_info_t::generate_test_instances(list<pg_info_t*>& o)
{
  o.push_back(new pg_info_t);
  o.push_back(new pg_info_t);
  list<pg_history_t*> h;
  pg_history_t::generate_test_instances(h);
  o.back()->history = *h.back();
  o.back()->pgid = pg_t(1, 2, -1);
  o.back()->last_update = eversion_t(3, 4);
  o.back()->last_complete = eversion_t(5, 6);
  o.back()->log_tail = eversion_t(7, 8);
  o.back()->last_backfill = hobject_t(object_t("objname"), "key", 123, 456, -1);
  list<pg_stat_t*> s;
  pg_stat_t::generate_test_instances(s);
  o.back()->stats = *s.back();
}

// -- pg_notify_t --
void pg_notify_t::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(query_epoch, bl);
  ::encode(epoch_sent, bl);
  ::encode(info, bl);
  ENCODE_FINISH(bl);
}

void pg_notify_t::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(query_epoch, bl);
  ::decode(epoch_sent, bl);
  ::decode(info, bl);
  DECODE_FINISH(bl);
}

void pg_notify_t::dump(Formatter *f) const
{
  f->dump_stream("query_epoch") << query_epoch;
  f->dump_stream("epoch_sent") << epoch_sent;
  {
    f->open_object_section("info");
    info.dump(f);
    f->close_section();
  }
}

void pg_notify_t::generate_test_instances(list<pg_notify_t*>& o)
{
  o.push_back(new pg_notify_t(1,1,pg_info_t()));
  o.push_back(new pg_notify_t(3,10,pg_info_t()));
}

ostream &operator<<(ostream &lhs, const pg_notify_t &notify)
{
  return lhs << "(query_epoch:" << notify.query_epoch
	     << ", epoch_sent:" << notify.epoch_sent
	     << ", info:" << notify.info << ")";
}

// -- pg_interval_t --

void pg_interval_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(first, bl);
  ::encode(last, bl);
  ::encode(up, bl);
  ::encode(acting, bl);
  ::encode(maybe_went_rw, bl);
  ENCODE_FINISH(bl);
}

void pg_interval_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(first, bl);
  ::decode(last, bl);
  ::decode(up, bl);
  ::decode(acting, bl);
  ::decode(maybe_went_rw, bl);
  DECODE_FINISH(bl);
}

void pg_interval_t::dump(Formatter *f) const
{
  f->dump_unsigned("first", first);
  f->dump_unsigned("last", last);
  f->dump_int("maybe_went_rw", maybe_went_rw ? 1 : 0);
  f->open_array_section("up");
  for (vector<int>::const_iterator p = up.begin(); p != up.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("acting");
  for (vector<int>::const_iterator p = acting.begin(); p != acting.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
}

void pg_interval_t::generate_test_instances(list<pg_interval_t*>& o)
{
  o.push_back(new pg_interval_t);
  o.push_back(new pg_interval_t);
  o.back()->up.push_back(1);
  o.back()->acting.push_back(2);
  o.back()->acting.push_back(3);
  o.back()->first = 4;
  o.back()->last = 5;
  o.back()->maybe_went_rw = true;
}

bool pg_interval_t::check_new_interval(
  const vector<int> &old_acting,
  const vector<int> &new_acting,
  const vector<int> &old_up,
  const vector<int> &new_up,
  epoch_t same_interval_since,
  epoch_t last_epoch_clean,
  OSDMapRef osdmap,
  OSDMapRef lastmap,
  int64_t pool_id,
  pg_t pgid,
  map<epoch_t, pg_interval_t> *past_intervals,
  std::ostream *out)
{
  // remember past interval
  if (new_acting != old_acting || new_up != old_up ||
      (!(lastmap->get_pools().count(pool_id))) ||
      (lastmap->get_pools().find(pool_id)->second.min_size !=
       osdmap->get_pools().find(pool_id)->second.min_size)  ||
      pgid.is_split(lastmap->get_pg_num(pgid.pool()),
        osdmap->get_pg_num(pgid.pool()), 0)) {
    pg_interval_t& i = (*past_intervals)[same_interval_since];
    i.first = same_interval_since;
    i.last = osdmap->get_epoch() - 1;
    i.acting = old_acting;
    i.up = old_up;

    if (!i.acting.empty() &&
	i.acting.size() >=
	osdmap->get_pools().find(pool_id)->second.min_size) {
      if (out)
	*out << "generate_past_intervals " << i
	     << ": not rw,"
	     << " up_thru " << lastmap->get_up_thru(i.acting[0])
	     << " up_from " << lastmap->get_up_from(i.acting[0])
	     << " last_epoch_clean " << last_epoch_clean
	     << std::endl;
      if (lastmap->get_up_thru(i.acting[0]) >= i.first &&
	  lastmap->get_up_from(i.acting[0]) <= i.first) {
	i.maybe_went_rw = true;
	if (out)
	  *out << "generate_past_intervals " << i
	       << " : primary up " << lastmap->get_up_from(i.acting[0])
	       << "-" << lastmap->get_up_thru(i.acting[0])
	       << std::endl;
      } else if (last_epoch_clean >= i.first &&
		 last_epoch_clean <= i.last) {
	// If the last_epoch_clean is included in this interval, then
	// the pg must have been rw (for recovery to have completed).
	// This is important because we won't know the _real_
	// first_epoch because we stop at last_epoch_clean, and we
	// don't want the oldest interval to randomly have
	// maybe_went_rw false depending on the relative up_thru vs
	// last_epoch_clean timing.
	i.maybe_went_rw = true;
	if (out)
	  *out << "generate_past_intervals " << i
	       << " : includes last_epoch_clean " << last_epoch_clean
	       << " and presumed to have been rw"
	       << std::endl;
      } else {
	i.maybe_went_rw = false;
	if (out)
	  *out << "generate_past_intervals " << i
	       << " : primary up " << lastmap->get_up_from(i.acting[0])
	       << "-" << lastmap->get_up_thru(i.acting[0])
	       << " does not include interval"
	       << std::endl;
      }
    } else {
      i.maybe_went_rw = false;
      if (out)
	*out << "generate_past_intervals " << i << " : empty" << std::endl;
    }
    return true;
  } else {
    return false;
  }
}

ostream& operator<<(ostream& out, const pg_interval_t& i)
{
  out << "interval(" << i.first << "-" << i.last << " " << i.up << "/" << i.acting;
  if (i.maybe_went_rw)
    out << " maybe_went_rw";
  out << ")";
  return out;
}



// -- pg_query_t --

void pg_query_t::encode(bufferlist &bl, uint64_t features) const {
  if (features & CEPH_FEATURE_QUERY_T) {
    ENCODE_START(2, 2, bl);
    ::encode(type, bl);
    ::encode(since, bl);
    history.encode(bl);
    ::encode(epoch_sent, bl);
    ENCODE_FINISH(bl);
  } else {
    ::encode(type, bl);
    ::encode(since, bl);
    history.encode(bl);
  }
}

void pg_query_t::decode(bufferlist::iterator &bl) {
  bufferlist::iterator bl2 = bl;
  try {
    DECODE_START(2, bl);
    ::decode(type, bl);
    ::decode(since, bl);
    history.decode(bl);
    ::decode(epoch_sent, bl);
    DECODE_FINISH(bl);
  } catch (...) {
    bl = bl2;
    ::decode(type, bl);
    ::decode(since, bl);
    history.decode(bl);
  }
}

void pg_query_t::dump(Formatter *f) const
{
  f->dump_string("type", get_type_name());
  f->dump_stream("since") << since;
  f->dump_stream("epoch_sent") << epoch_sent;
  f->open_object_section("history");
  history.dump(f);
  f->close_section();
}
void pg_query_t::generate_test_instances(list<pg_query_t*>& o)
{
  o.push_back(new pg_query_t());
  list<pg_history_t*> h;
  pg_history_t::generate_test_instances(h);
  o.push_back(new pg_query_t(pg_query_t::INFO, *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::MISSING, *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::LOG, eversion_t(4, 5), *h.back(), 4));
  o.push_back(new pg_query_t(pg_query_t::FULLLOG, *h.back(), 5));
}


// -- pg_log_entry_t --

string pg_log_entry_t::get_key_name() const
{
  return version.get_key_name();
}

void pg_log_entry_t::encode_with_checksum(bufferlist& bl) const
{
  bufferlist ebl(sizeof(*this)*2);
  encode(ebl);
  __u32 crc = ebl.crc32c(0);
  ::encode(ebl, bl);
  ::encode(crc, bl);
}

void pg_log_entry_t::decode_with_checksum(bufferlist::iterator& p)
{
  bufferlist bl;
  ::decode(bl, p);
  __u32 crc;
  ::decode(crc, p);
  if (crc != bl.crc32c(0))
    throw buffer::malformed_input("bad checksum on pg_log_entry_t");
  bufferlist::iterator q = bl.begin();
  decode(q);
}

void pg_log_entry_t::encode(bufferlist &bl) const
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

void pg_log_entry_t::decode(bufferlist::iterator &bl)
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
  if (struct_v < 5)
    invalid_pool = true;

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

void pg_log_entry_t::dump(Formatter *f) const
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

void pg_log_entry_t::generate_test_instances(list<pg_log_entry_t*>& o)
{
  o.push_back(new pg_log_entry_t());
  hobject_t oid(object_t("objname"), "key", 123, 456, 0);
  o.push_back(new pg_log_entry_t(MODIFY, oid, eversion_t(1,2), eversion_t(3,4),
				 osd_reqid_t(entity_name_t::CLIENT(777), 8, 999), utime_t(8,9)));
}

ostream& operator<<(ostream& out, const pg_log_entry_t& e)
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


// -- pg_log_t --

void pg_log_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(head, bl);
  ::encode(tail, bl);
  ::encode(log, bl);
  ENCODE_FINISH(bl);
}
 
void pg_log_t::decode(bufferlist::iterator &bl, int64_t pool)
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
    for (list<pg_log_entry_t>::iterator i = log.begin();
	 i != log.end();
	 ++i) {
      if (i->soid.pool == -1)
	i->soid.pool = pool;
    }
  }
}

void pg_log_t::dump(Formatter *f) const
{
  f->dump_stream("head") << head;
  f->dump_stream("tail") << head;
  f->open_array_section("log");
  for (list<pg_log_entry_t>::const_iterator p = log.begin(); p != log.end(); ++p) {
    f->open_object_section("entry");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void pg_log_t::generate_test_instances(list<pg_log_t*>& o)
{
  o.push_back(new pg_log_t);

  // this is nonsensical:
  o.push_back(new pg_log_t);
  o.back()->head = eversion_t(1,2);
  o.back()->tail = eversion_t(3,4);
  list<pg_log_entry_t*> e;
  pg_log_entry_t::generate_test_instances(e);
  for (list<pg_log_entry_t*>::iterator p = e.begin(); p != e.end(); ++p)
    o.back()->log.push_back(**p);
}

void pg_log_t::copy_after(const pg_log_t &other, eversion_t v) 
{
  head = other.head;
  tail = other.tail;
  for (list<pg_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
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

void pg_log_t::copy_range(const pg_log_t &other, eversion_t from, eversion_t to)
{
  list<pg_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
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

void pg_log_t::copy_up_to(const pg_log_t &other, int max)
{
  int n = 0;
  head = other.head;
  tail = other.tail;
  for (list<pg_log_entry_t>::const_reverse_iterator i = other.log.rbegin();
       i != other.log.rend();
       ++i) {
    if (n++ >= max) {
      tail = i->version;
      break;
    }
    log.push_front(*i);
  }
}

ostream& pg_log_t::print(ostream& out) const 
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) 
    out << *p << std::endl;
  return out;
}


// -- pg_missing_t --

void pg_missing_t::encode(bufferlist &bl) const
{
  ENCODE_START(3, 2, bl);
  ::encode(missing, bl);
  ENCODE_FINISH(bl);
}

void pg_missing_t::decode(bufferlist::iterator &bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  ::decode(missing, bl);
  DECODE_FINISH(bl);

  if (struct_v < 3) {
    // Handle hobject_t upgrade
    map<hobject_t, item> tmp;
    for (map<hobject_t, item>::iterator i = missing.begin();
	 i != missing.end();
      ) {
      if (i->first.pool == -1) {
	hobject_t to_insert(i->first);
	to_insert.pool = pool;
	tmp[to_insert] = i->second;
	missing.erase(i++);
      } else {
	++i;
      }
    }
    missing.insert(tmp.begin(), tmp.end());
  }

  for (map<hobject_t,item>::iterator it = missing.begin();
       it != missing.end();
       ++it)
    rmissing[it->second.need.version] = it->first;
}

void pg_missing_t::dump(Formatter *f) const
{
  f->open_array_section("missing");
  for (map<hobject_t,item>::const_iterator p = missing.begin(); p != missing.end(); ++p) {
    f->open_object_section("item");
    f->dump_stream("object") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void pg_missing_t::generate_test_instances(list<pg_missing_t*>& o)
{
  o.push_back(new pg_missing_t);
  o.push_back(new pg_missing_t);
  o.back()->add(hobject_t(object_t("foo"), "foo", 123, 456, 0), eversion_t(5, 6), eversion_t(5, 1));
}

ostream& operator<<(ostream& out, const pg_missing_t::item& i) 
{
  out << i.need;
  if (i.have != eversion_t())
    out << "(" << i.have << ")";
  return out;
}

ostream& operator<<(ostream& out, const pg_missing_t& missing) 
{
  out << "missing(" << missing.num_missing();
  //if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}


unsigned int pg_missing_t::num_missing() const
{
  return missing.size();
}

bool pg_missing_t::have_missing() const
{
  return !missing.empty();
=======
>>>>>>> Remove Placement Groups Work
}

void osd_stat_t::generate_test_instances(std::list<osd_stat_t*>& o)
{
  o.push_back(new osd_stat_t);

  o.push_back(new osd_stat_t);
  o.back()->kb = 1;
  o.back()->kb_used = 2;
  o.back()->kb_avail = 3;
  o.back()->hb_in.push_back(5);
  o.back()->hb_in.push_back(6);
  o.back()->hb_out = o.back()->hb_in;
  o.back()->hb_out.push_back(7);
  o.back()->snap_trim_queue_len = 8;
  o.back()->num_snap_trimming = 99;
}

// -- pg_create_t --

void pg_create_t::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(created, bl);
  ::encode(parent, bl);
  ::encode(split_bits, bl);
  ENCODE_FINISH(bl);
}

void pg_create_t::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(created, bl);
  ::decode(parent, bl);
  ::decode(split_bits, bl);
  DECODE_FINISH(bl);
}

void pg_create_t::dump(Formatter *f) const
{
  f->dump_unsigned("created", created);
  f->dump_stream("parent") << parent;
  f->dump_int("split_bits", split_bits);
}

void pg_create_t::generate_test_instances(list<pg_create_t*>& o)
{
  o.push_back(new pg_create_t);
  o.push_back(new pg_create_t(1, pg_t(3, 4, -1), 2));
}

// -- ScrubMap --

void ScrubMap::merge_incr(const ScrubMap &l)
{
  assert(valid_through == l.incr_since);
  attrs = l.attrs;
  valid_through = l.valid_through;

  for (map<hobject_t,object>::const_iterator p = l.objects.begin();
       p != l.objects.end();
       ++p){
    if (p->second.negative) {
      map<hobject_t,object>::iterator q = objects.find(p->first);
      if (q != objects.end()) {
	objects.erase(q);
      }
    } else {
      objects[p->first] = p->second;
    }
  }
}          

void ScrubMap::encode(bufferlist& bl) const
{
  ENCODE_START(3, 2, bl);
  ::encode(objects, bl);
  ::encode(attrs, bl);
  bufferlist old_logbl;  // not used
  ::encode(old_logbl, bl);
  ::encode(valid_through, bl);
  ::encode(incr_since, bl);
  ENCODE_FINISH(bl);
}

void ScrubMap::decode(bufferlist::iterator& bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  ::decode(objects, bl);
  ::decode(attrs, bl);
  bufferlist old_logbl;   // not used
  ::decode(old_logbl, bl);
  ::decode(valid_through, bl);
  ::decode(incr_since, bl);
  DECODE_FINISH(bl);

  // handle hobject_t upgrade
  if (struct_v < 3) {
    map<hobject_t, object> tmp;
    tmp.swap(objects);
    for (map<hobject_t, object>::iterator i = tmp.begin();
	 i != tmp.end();
	 ++i) {
      hobject_t first(i->first);
      if (first.pool == -1)
	first.pool = pool;
      objects[first] = i->second;
    }
  }
}

void ScrubMap::dump(Formatter *f) const
{
  f->dump_stream("valid_through") << valid_through;
  f->dump_stream("incremental_since") << incr_since;
  f->open_array_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin(); p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
  f->open_array_section("objects");
  for (map<hobject_t,object>::const_iterator p = objects.begin(); p != objects.end(); ++p) {
    f->open_object_section("object");
    f->dump_string("name", p->first.oid.name);
    f->dump_unsigned("hash", p->first.hash);
    f->dump_string("key", p->first.get_key());
    f->dump_int("snapid", p->first.snap);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void ScrubMap::generate_test_instances(list<ScrubMap*>& o)
{
  o.push_back(new ScrubMap);
  o.push_back(new ScrubMap);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  o.back()->attrs["foo"] = buffer::copy("foo", 3);
  o.back()->attrs["bar"] = buffer::copy("barval", 6);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0)] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0)] = *obj.back();
}

// -- ScrubMap::object --

void ScrubMap::object::encode(bufferlist& bl) const
{
  ENCODE_START(6, 2, bl);
  ::encode(size, bl);
  ::encode(negative, bl);
  ::encode(attrs, bl);
  ::encode(digest, bl);
  ::encode(digest_present, bl);
  ::encode(nlinks, bl);
  ::encode(snapcolls, bl);
  ::encode(omap_digest, bl);
  ::encode(omap_digest_present, bl);
  ::encode(read_error, bl);
  ENCODE_FINISH(bl);
}

void ScrubMap::object::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
  ::decode(size, bl);
  ::decode(negative, bl);
  ::decode(attrs, bl);
  if (struct_v >= 3) {
    ::decode(digest, bl);
    ::decode(digest_present, bl);
  }
  if (struct_v >= 4) {
    ::decode(nlinks, bl);
    ::decode(snapcolls, bl);
  } else {
    /* Indicates that encoder was not aware of this field since stat must
     * return nlink >= 1 */
    nlinks = 0;
  }
  if (struct_v >= 5) {
    ::decode(omap_digest, bl);
    ::decode(omap_digest_present, bl);
  }
  if (struct_v >= 6) {
    ::decode(read_error, bl);
  }
  DECODE_FINISH(bl);
}

void ScrubMap::object::dump(Formatter *f) const
{
  f->dump_int("size", size);
  f->dump_int("negative", negative);
  f->open_array_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin(); p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
}

void ScrubMap::object::generate_test_instances(list<object*>& o)
{
  o.push_back(new object);
  o.push_back(new object);
  o.back()->negative = true;
  o.push_back(new object);
  o.back()->size = 123;
  o.back()->attrs["foo"] = buffer::copy("foo", 3);
  o.back()->attrs["bar"] = buffer::copy("barval", 6);
}

