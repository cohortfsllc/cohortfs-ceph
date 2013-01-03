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


// -- coll_t --

bool coll_t::is_temp(pg_t& pgid) const
{
  const char *cstr(str.c_str());
  if (!pgid.parse(cstr))
    return false;
  const char *tmp_start = strchr(cstr, '_');
  if (!tmp_start)
    return false;
  if (strncmp(tmp_start, "_TEMP", 5) == 0)
    return true;
  return false;
}

bool coll_t::is_pg(pg_t& pgid, snapid_t& snap) const
{
  const char *cstr(str.c_str());

  if (!pgid.parse(cstr))
    return false;
  const char *snap_start = strchr(cstr, '_');
  if (!snap_start)
    return false;
  if (strncmp(snap_start, "_head", 5) == 0)
    snap = CEPH_NOSNAP;
  else
    snap = strtoull(snap_start+1, 0, 16);
  return true;
}

bool coll_t::is_removal(uint64_t *seq, pg_t *pgid) const
{
  if (str.substr(0, 11) != string("FORREMOVAL_"))
    return false;

  stringstream ss(str.substr(11));
  ss >> *seq;
  char sep;
  ss >> sep;
  assert(sep == '_');
  string pgid_str;
  ss >> pgid_str;
  if (!pgid->parse(pgid_str.c_str())) {
    assert(0);
    return false;
  }
  return true;
}

void coll_t::encode(bufferlist& bl) const
{
  __u8 struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(str, bl);
}

void coll_t::decode(bufferlist::iterator& bl)
{
  __u8 struct_v;
  ::decode(struct_v, bl);
  switch (struct_v) {
  case 1: {
    pg_t pgid;
    snapid_t snap;

    ::decode(pgid, bl);
    ::decode(snap, bl);
    // infer the type
    if (pgid == pg_t() && snap == 0)
      str = "meta";
    else
      str = pg_and_snap_to_str(pgid, snap);
    break;
  }

  case 2: {
    __u8 type;
    pg_t pgid;
    snapid_t snap;
    
    ::decode(type, bl);
    ::decode(pgid, bl);
    ::decode(snap, bl);
    switch (type) {
    case 0:
      str = "meta";
      break;
    case 1:
      str = "temp";
      break;
    case 2:
      str = pg_and_snap_to_str(pgid, snap);
      break;
    default: {
      ostringstream oss;
      oss << "coll_t::decode(): can't understand type " << (int) type;
      throw std::domain_error(oss.str());
    }
    }
    break;
  }

  case 3:
    ::decode(str, bl);
    break;
    
  default: {
    ostringstream oss;
    oss << "coll_t::decode(): don't know how to decode version "
	<< (int) struct_v;
    throw std::domain_error(oss.str());
  }
  }
}

void coll_t::dump(Formatter *f) const
{
  f->dump_string("name", str);
}

void coll_t::generate_test_instances(list<coll_t*>& o)
{
  o.push_back(new coll_t);
  o.push_back(new coll_t("meta"));
  o.push_back(new coll_t("temp"));
  o.push_back(new coll_t("foo"));
  o.push_back(new coll_t("bar"));
}

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
  if (state & PG_STATE_RECOVERING)
    oss << "recovering+";
  if (state & PG_STATE_DOWN)
    oss << "down+";
  if (state & PG_STATE_REPLAY)
    oss << "replay+";
  if (state & PG_STATE_STRAY)
    oss << "stray+";
  if (state & PG_STATE_SPLITTING)
    oss << "splitting+";
  if (state & PG_STATE_DEGRADED)
    oss << "degraded+";
  if (state & PG_STATE_REMAPPED)
    oss << "remapped+";
  if (state & PG_STATE_SCRUBBING)
    oss << "scrubbing+";
  if (state & PG_STATE_SCRUBQ)
    oss << "scrubq+";
  if (state & PG_STATE_INCONSISTENT)
    oss << "inconsistent+";
  if (state & PG_STATE_PEERING)
    oss << "peering+";
  if (state & PG_STATE_REPAIR)
    oss << "repair+";
  if (state & PG_STATE_BACKFILL)
    oss << "backfill+";
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

void pool_snap_info_t::encode(bufferlist& bl) const
{
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
       p++)
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
       p++)
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
  return ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask) + pg.pool();
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

    ::encode_nohead(snaps, bl);
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
    ::encode(snaps, bl);
    ::encode(removed_snaps, bl);
    ::encode(auid, bl);
    ::encode(flags, bl);
    ::encode(crash_replay_interval, bl);
    return;
  }

  ENCODE_START(6, 5, bl);
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
  ::encode(snaps, bl);
  ::encode(removed_snaps, bl);
  ::encode(auid, bl);
  ::encode(flags, bl);
  ::encode(crash_replay_interval, bl);
  ENCODE_FINISH(bl);
}

void pg_pool_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
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
  o.push_back(new pg_pool_t(a));

  a.snaps[3].name = "asdf";
  a.snaps[3].snapid = 3;
  a.snaps[3].stamp = utime_t(123, 4);
  a.snaps[6].name = "qwer";
  a.snaps[6].snapid = 6;
  a.snaps[6].stamp = utime_t(23423, 4);
  o.push_back(new pg_pool_t(a));

  a.removed_snaps.insert(2);   // not quite valid to combine with snaps!
  o.push_back(new pg_pool_t(a));
}

ostream& operator<<(ostream& out, const pg_pool_t& p)
{
  out << p.get_type_name()
      << " size " << p.get_size()
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
  f->dump_stream("last_unstale") << last_unstale;
  f->dump_unsigned("mapping_epoch", mapping_epoch);
  f->dump_stream("log_start") << log_start;
  f->dump_stream("ondisk_log_start") << ondisk_log_start;
  f->dump_unsigned("created", created);
  f->dump_unsigned("last_epoch_clean", created);
  f->dump_stream("parent") << parent;
  f->dump_unsigned("parent_split_bits", parent_split_bits);
  f->dump_stream("last_scrub") << last_scrub;
  f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
  f->dump_unsigned("log_size", log_size);
  f->dump_unsigned("ondisk_log_size", ondisk_log_size);
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
  ENCODE_START(9, 8, bl);
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
  ENCODE_FINISH(bl);
}

void pg_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(9, 8, 8, bl);
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
    }
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

void pool_stat_t::encode(bufferlist &bl) const
{
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
  ENCODE_START(4, 4, bl);
  ::encode(epoch_created, bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_epoch_clean, bl);
  ::encode(last_epoch_split, bl);
  ::encode(same_interval_since, bl);
  ::encode(same_up_since, bl);
  ::encode(same_primary_since, bl);
  ::encode(last_scrub, bl);
  ::encode(last_scrub_stamp, bl);
  ENCODE_FINISH(bl);
}

void pg_history_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, bl);
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
}


// -- pg_info_t --

void pg_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(26, 26, bl);
  ::encode(pgid, bl);
  ::encode(last_update, bl);
  ::encode(last_complete, bl);
  ::encode(log_tail, bl);
  ::encode(last_backfill, bl);
  ::encode(stats, bl);
  history.encode(bl);
  ::encode(purged_snaps, bl);
  ENCODE_FINISH(bl);
}

void pg_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(26, 26, 26, bl);
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

ostream &operator<<(ostream &lhs, const pg_notify_t notify)
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
  map<epoch_t, pg_interval_t> *past_intervals,
  std::ostream *out)
{
  // remember past interval
  if (new_acting != old_acting || new_up != old_up) {
    pg_interval_t& i = (*past_intervals)[same_interval_since];
    i.first = same_interval_since;
    i.last = osdmap->get_epoch() - 1;
    i.acting = old_acting;
    i.up = old_up;

    if (i.acting.size()) {
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

void pg_log_entry_t::encode(bufferlist &bl) const
{
  ENCODE_START(5, 4, bl);
  ::encode(op, bl);
  ::encode(soid, bl);
  ::encode(version, bl);
  ::encode(prior_version, bl);
  ::encode(reqid, bl);
  ::encode(mtime, bl);
  if (op == CLONE)
    ::encode(snaps, bl);
  ENCODE_FINISH(bl);
}

void pg_log_entry_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
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
  ::decode(prior_version, bl);
  ::decode(reqid, bl);
  ::decode(mtime, bl);
  if (op == CLONE)
    ::decode(snaps, bl);
  if (struct_v < 5)
    invalid_pool = true;
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
  return out << e.version << " (" << e.prior_version << ") "
             << e.get_op_name() << ' ' << e.soid << " by " << e.reqid << " " << e.mtime;
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
       i++) {
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
       p++) 
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
}

void pg_missing_t::swap(pg_missing_t& o)
{
  missing.swap(o.missing);
  rmissing.swap(o.rmissing);
}

bool pg_missing_t::is_missing(const hobject_t& oid) const
{
  return (missing.find(oid) != missing.end());
}

bool pg_missing_t::is_missing(const hobject_t& oid, eversion_t v) const
{
  map<hobject_t, item>::const_iterator m = missing.find(oid);
  if (m == missing.end())
    return false;
  const pg_missing_t::item &item(m->second);
  if (item.need > v)
    return false;
  return true;
}

eversion_t pg_missing_t::have_old(const hobject_t& oid) const
{
  map<hobject_t, item>::const_iterator m = missing.find(oid);
  if (m == missing.end())
    return eversion_t();
  const pg_missing_t::item &item(m->second);
  return item.have;
}

/*
 * this needs to be called in log order as we extend the log.  it
 * assumes missing is accurate up through the previous log entry.
 */
void pg_missing_t::add_next_event(const pg_log_entry_t& e)
{
  if (e.is_update()) {
    if (e.prior_version == eversion_t() || e.is_clone()) {
      // new object.
      //assert(missing.count(e.soid) == 0);  // might already be missing divergent item.
      if (missing.count(e.soid))  // already missing divergent item
	rmissing.erase(missing[e.soid].need.version);
      missing[e.soid] = item(e.version, eversion_t());  // .have = nil
    } else if (missing.count(e.soid)) {
      // already missing (prior).
      //assert(missing[e.soid].need == e.prior_version);
      rmissing.erase(missing[e.soid].need.version);
      missing[e.soid].need = e.version;  // leave .have unchanged.
    } else if (e.is_backlog()) {
      // May not have prior version
      assert(0 == "these don't exist anymore");
    } else {
      // not missing, we must have prior_version (if any)
      missing[e.soid] = item(e.version, e.prior_version);
    }
    rmissing[e.version.version] = e.soid;
  } else
    rm(e.soid, e.version);
}

void pg_missing_t::revise_need(hobject_t oid, eversion_t need)
{
  if (missing.count(oid)) {
    rmissing.erase(missing[oid].need.version);
    missing[oid].need = need;            // no not adjust .have
  } else {
    missing[oid] = item(need, eversion_t());
  }
  rmissing[need.version] = oid;
}

void pg_missing_t::revise_have(hobject_t oid, eversion_t have)
{
  if (missing.count(oid)) {
    missing[oid].have = have;
  }
}

void pg_missing_t::add(const hobject_t& oid, eversion_t need, eversion_t have)
{
  missing[oid] = item(need, have);
  rmissing[need.version] = oid;
}

void pg_missing_t::rm(const hobject_t& oid, eversion_t v)
{
  std::map<hobject_t, pg_missing_t::item>::iterator p = missing.find(oid);
  if (p != missing.end() && p->second.need <= v)
    rm(p);
}

void pg_missing_t::rm(const std::map<hobject_t, pg_missing_t::item>::iterator &m)
{
  rmissing.erase(m->second.need.version);
  missing.erase(m);
}

void pg_missing_t::got(const hobject_t& oid, eversion_t v)
{
  std::map<hobject_t, pg_missing_t::item>::iterator p = missing.find(oid);
  assert(p != missing.end());
  assert(p->second.need <= v);
  got(p);
}

void pg_missing_t::got(const std::map<hobject_t, pg_missing_t::item>::iterator &m)
{
  rmissing.erase(m->second.need.version);
  missing.erase(m);
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
