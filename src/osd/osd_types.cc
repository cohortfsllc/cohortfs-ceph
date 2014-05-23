// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "osd_types.h"
#include "include/ceph_features.h"
extern "C" {
#include "crush/hash.h"
}
#include "PG.h"
#include "OSDMap.h"

const char *ceph_osd_flag_name(unsigned flag)
{
  switch (flag) {
  case CEPH_OSD_FLAG_ACK: return "ack";
  case CEPH_OSD_FLAG_ONNVRAM: return "onnvram";
  case CEPH_OSD_FLAG_ONDISK: return "ondisk";
  case CEPH_OSD_FLAG_RETRY: return "retry";
  case CEPH_OSD_FLAG_READ: return "read";
  case CEPH_OSD_FLAG_WRITE: return "write";
  case CEPH_OSD_FLAG_PEERSTAT_OLD: return "peerstat_old";
  case CEPH_OSD_FLAG_PARALLELEXEC: return "parallelexec";
  case CEPH_OSD_FLAG_PGOP: return "pgop";
  case CEPH_OSD_FLAG_EXEC: return "exec";
  case CEPH_OSD_FLAG_EXEC_PUBLIC: return "exec_public";
  case CEPH_OSD_FLAG_RWORDERED: return "rwordered";
  case CEPH_OSD_FLAG_SKIPRWLOCKS: return "skiprwlocks";
  default: return "???";
  }
}

string ceph_osd_flag_string(unsigned flags)
{
  string s;
  for (unsigned i=0; i<32; ++i) {
    if (flags & (1u<<i)) {
      if (s.length())
	s += "+";
      s += ceph_osd_flag_name(1u << i);
    }
  }
  if (s.length())
    return s;
  return string("-");
}

// -- osd_reqid_t --
void osd_reqid_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(name, bl);
  ::encode(tid, bl);
  ::encode(inc, bl);
  ENCODE_FINISH(bl);
}

void osd_reqid_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(name, bl);
  ::decode(tid, bl);
  ::decode(inc, bl);
  DECODE_FINISH(bl);
}

void osd_reqid_t::dump(Formatter *f) const
{
  f->dump_stream("name") << name;
  f->dump_int("inc", inc);
  f->dump_unsigned("tid", tid);
}

void osd_reqid_t::generate_test_instances(list<osd_reqid_t*>& o)
{
  o.push_back(new osd_reqid_t);
  o.push_back(new osd_reqid_t(entity_name_t::CLIENT(123), 1, 45678));
}

// -- object_locator_t --

void object_locator_t::encode(bufferlist& bl) const
{
  // verify that nobody's corrupted the locator
  assert(hash == -1 || key.empty());
  uint8_t encode_compat = 3;
  ENCODE_START(6, encode_compat, bl);
  ::encode(pool, bl);
  int32_t preferred = -1;  // tell old code there is no preferred osd (-1).
  ::encode(preferred, bl);
  ::encode(key, bl);
  ::encode(nspace, bl);
  ::encode(hash, bl);
  if (hash != -1)
    encode_compat = MAX(encode_compat, 6); // need to interpret the hash
  ENCODE_FINISH_NEW_COMPAT(bl, encode_compat);
}

void object_locator_t::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, p);
  if (struct_v < 2) {
    int32_t op;
    ::decode(op, p);
    pool = op;
    int16_t pref;
    ::decode(pref, p);
  } else {
    ::decode(pool, p);
    int32_t preferred;
    ::decode(preferred, p);
  }
  ::decode(key, p);
  if (struct_v >= 5)
    ::decode(nspace, p);
  if (struct_v >= 6)
    ::decode(hash, p);
  else
    hash = -1;
  DECODE_FINISH(p);
  // verify that nobody's corrupted the locator
  assert(hash == -1 || key.empty());
}

void object_locator_t::dump(Formatter *f) const
{
  f->dump_int("pool", pool);
  f->dump_string("key", key);
  f->dump_string("namespace", nspace);
  f->dump_int("hash", hash);
}

void object_locator_t::generate_test_instances(list<object_locator_t*>& o)
{
  o.push_back(new object_locator_t);
  o.push_back(new object_locator_t(123));
  o.push_back(new object_locator_t(123, 876));
  o.push_back(new object_locator_t(1, "n2"));
  o.push_back(new object_locator_t(1234, "", "key"));
  o.push_back(new object_locator_t(12, "n1", "key2"));
}

// -- request_redirect_t --
void request_redirect_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(redirect_locator, bl);
  ::encode(redirect_object, bl);
  ::encode(osd_instructions, bl);
  ENCODE_FINISH(bl);
}

void request_redirect_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  ::decode(redirect_locator, bl);
  ::decode(redirect_object, bl);
  ::decode(osd_instructions, bl);
  DECODE_FINISH(bl);
}

void request_redirect_t::dump(Formatter *f) const
{
  f->dump_string("object", redirect_object);
  f->open_object_section("locator");
  redirect_locator.dump(f);
  f->close_section(); // locator
}

void request_redirect_t::generate_test_instances(list<request_redirect_t*>& o)
{
  object_locator_t loc(1, "redir_obj");
  o.push_back(new request_redirect_t());
  o.push_back(new request_redirect_t(loc, 0));
  o.push_back(new request_redirect_t(loc, "redir_obj"));
  o.push_back(new request_redirect_t(loc));
}

void objectstore_perf_stat_t::dump(Formatter *f) const
{
  f->dump_unsigned("commit_latency_ms", filestore_commit_latency);
  f->dump_unsigned("apply_latency_ms", filestore_apply_latency);
}

void objectstore_perf_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(filestore_commit_latency, bl);
  ::encode(filestore_apply_latency, bl);
  ENCODE_FINISH(bl);
}

void objectstore_perf_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(filestore_commit_latency, bl);
  ::decode(filestore_apply_latency, bl);
  DECODE_FINISH(bl);
}

void objectstore_perf_stat_t::generate_test_instances(std::list<objectstore_perf_stat_t*>& o)
{
  o.push_back(new objectstore_perf_stat_t());
  o.push_back(new objectstore_perf_stat_t());
  o.back()->filestore_commit_latency = 20;
  o.back()->filestore_apply_latency = 30;
}

// -- osd_stat_t --
void osd_stat_t::dump(Formatter *f) const
{
  f->dump_unsigned("kb", kb);
  f->dump_unsigned("kb_used", kb_used);
  f->dump_unsigned("kb_avail", kb_avail);
  f->open_array_section("hb_in");
  for (vector<int>::const_iterator p = hb_in.begin(); p != hb_in.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_array_section("hb_out");
  for (vector<int>::const_iterator p = hb_out.begin(); p != hb_out.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();
  f->open_object_section("op_queue_age_hist");
  op_queue_age_hist.dump(f);
  f->close_section();
  f->open_object_section("fs_perf_stat");
  fs_perf_stat.dump(f);
  f->close_section();
}

void osd_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(4, 2, bl);
  ::encode(kb, bl);
  ::encode(kb_used, bl);
  ::encode(kb_avail, bl);
  ::encode(hb_in, bl);
  ::encode(hb_out, bl);
  ::encode(op_queue_age_hist, bl);
  ::encode(fs_perf_stat, bl);
  ENCODE_FINISH(bl);
}

void osd_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 2, 2, bl);
  ::decode(kb, bl);
  ::decode(kb_used, bl);
  ::decode(kb_avail, bl);
  ::decode(hb_in, bl);
  ::decode(hb_out, bl);
  if (struct_v >= 3)
    ::decode(op_queue_age_hist, bl);
  if (struct_v >= 4)
    ::decode(fs_perf_stat, bl);
  DECODE_FINISH(bl);
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
}

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

pg_t pg_t::get_ancestor(unsigned old_pg_num) const
{
  int old_bits = pg_pool_t::calc_bits_of(old_pg_num);
  int old_mask = (1 << old_bits) - 1;
  pg_t ret = *this;
  ret.m_seed = ceph_stable_mod(m_seed, old_pg_num, old_mask);
  return ret;
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
  assert(p); // silence coverity #751330 

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


// -- coll_t --

const coll_t coll_t::META_COLL("meta");

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

bool coll_t::is_pg(pg_t& pgid) const
{
  const char *cstr(str.c_str());

  if (!pgid.parse(cstr))
    return false;
  return true;
}

bool coll_t::is_pg_prefix(pg_t& pgid) const
{
  const char *cstr(str.c_str());

  if (!pgid.parse(cstr))
    return false;
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
  uint8_t struct_v = 3;
  ::encode(struct_v, bl);
  ::encode(str, bl);
}

void coll_t::decode(bufferlist::iterator& bl)
{
  uint8_t struct_v;
  ::decode(struct_v, bl);
  switch (struct_v) {
  case 1: {
    pg_t pgid;

    ::decode(pgid, bl);
    if (pgid == pg_t())
      str = "meta";
    else
      str = pg_to_str(pgid);
    break;
  }

  case 2: {
    uint8_t type;
    pg_t pgid;

    ::decode(type, bl);
    ::decode(pgid, bl);
    switch (type) {
    case 0:
      str = "meta";
      break;
    case 1:
      str = "temp";
      break;
    case 2:
      str = pg_to_str(pgid);
      break;
    default: {
      ostringstream oss;
      oss << "coll_t::decode(): can't understand type " << type;
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
	<< struct_v;
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
  if (state & PG_STATE_CREATING)
    oss << "creating+";
  if (state & PG_STATE_ACTIVE)
    oss << "active+";
  if (state & PG_STATE_DOWN)
    oss << "down+";
  if (state & PG_STATE_REMAPPED)
    oss << "remapped+";
  string ret(oss.str());
  if (ret.length() > 0)
    ret.resize(ret.length() - 1);
  else
    ret = "inactive";
  return ret;
}


// -- eversion_t --
string eversion_t::get_key_name() const
{
  char key[40];
  snprintf(
    key, sizeof(key), "%010u.%020llu", epoch, (long long unsigned)version);
  return string(key);
}


// -- pg_pool_t --

void pg_pool_t::dump(Formatter *f) const
{
  f->dump_unsigned("flags", get_flags());
  f->dump_string("flags_names", get_flags_string());
  f->dump_int("type", get_type());
  f->dump_int("size", get_size());
  f->dump_int("min_size", get_min_size());
  f->dump_int("crush_ruleset", get_crush_ruleset());
  f->dump_int("object_hash", get_object_hash());
  f->dump_int("pg_num", get_pg_num());
  f->dump_int("pg_placement_num", get_pgp_num());
  f->dump_stream("last_change") << get_last_change();
  f->dump_unsigned("auid", get_auid());
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

unsigned pg_pool_t::get_pg_num_divisor(pg_t pgid) const
{
  if (pg_num == pg_num_mask + 1)
    return pg_num;                    // power-of-2 split
  unsigned mask = pg_num_mask >> 1;
  if ((pgid.ps() & mask) < (pg_num & mask))
    return pg_num_mask + 1;           // smaller bin size (already split)
  else
    return (pg_num_mask + 1) >> 1;    // bigger bin (not yet split)
}

static string make_hash_str(const string &inkey, const string &nspace)
{
  if (nspace.empty())
    return inkey;
  return nspace + '\037' + inkey;
}

uint32_t pg_pool_t::hash_key(const string& key, const string& ns) const
{
  string n = make_hash_str(key, ns);
  return ceph_str_hash(object_hash, n.c_str(), n.length());
}

uint32_t pg_pool_t::raw_hash_to_pg(uint32_t v) const
{
  return ceph_stable_mod(v, pg_num, pg_num_mask);
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

uint32_t pg_pool_t::get_random_pg_position(pg_t pg, uint32_t seed) const
{
  uint32_t r = crush_hash32_2(CRUSH_HASH_RJENKINS1, seed, 123);
  if (pg_num == pg_num_mask + 1) {
    r &= ~pg_num_mask;
  } else {
    unsigned smaller_mask = pg_num_mask >> 1;
    if ((pg.ps() & smaller_mask) < (pg_num & smaller_mask)) {
      r &= ~pg_num_mask;
    } else {
      r &= ~smaller_mask;
    }
  }
  r |= pg.ps();
  return r;
}

void pg_pool_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_PGPOOL3) == 0) {
    // this encoding matches the old struct ceph_pg_pool
    uint8_t struct_v = 2;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ::encode(crush_ruleset, bl);
    ::encode(object_hash, bl);
    ::encode(pg_num, bl);
    ::encode(pgp_num, bl);
    uint32_t lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    ::encode(lpg_num, bl);
    ::encode(lpgp_num, bl);
    ::encode(last_change, bl);

    ::encode(auid, bl);

    return;
  }

  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    uint8_t struct_v = 4;
    ::encode(struct_v, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ::encode(crush_ruleset, bl);
    ::encode(object_hash, bl);
    ::encode(pg_num, bl);
    ::encode(pgp_num, bl);
    uint32_t lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
    ::encode(lpg_num, bl);
    ::encode(lpgp_num, bl);
    ::encode(last_change, bl);
    ::encode(auid, bl);
    ::encode(flags, bl);
    ::encode(crash_replay_interval, bl);
    return;
  }

  uint8_t encode_compat = 5;
  ENCODE_START(14, encode_compat, bl);
  ::encode(type, bl);
  ::encode(size, bl);
  ::encode(crush_ruleset, bl);
  ::encode(object_hash, bl);
  ::encode(pg_num, bl);
  ::encode(pgp_num, bl);
  uint32_t lpg_num = 0, lpgp_num = 0;  // tell old code that there are no localized pgs.
  ::encode(lpg_num, bl);
  ::encode(lpgp_num, bl);
  ::encode(last_change, bl);
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
  DECODE_START_LEGACY_COMPAT_LEN(14, 5, 5, bl);
  ::decode(type, bl);
  ::decode(size, bl);
  ::decode(crush_ruleset, bl);
  ::decode(object_hash, bl);
  ::decode(pg_num, bl);
  ::decode(pgp_num, bl);
  {
    uint32_t lpg_num, lpgp_num;
    ::decode(lpg_num, bl);
    ::decode(lpgp_num, bl);
  }
  ::decode(last_change, bl);

  if (struct_v >= 3) {
    ::decode(auid, bl);
  } else {
    uint32_t n, m;
    ::decode(n, bl);
    ::decode(m, bl);
    ::decode(auid, bl);
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

  a.type = TYPE_REPLICATED;
  a.size = 2;
  a.crush_ruleset = 3;
  a.object_hash = 4;
  a.pg_num = 6;
  a.pgp_num = 5;
  a.last_change = 9;
  a.auid = 12;
  a.crash_replay_interval = 13;
  a.quota_max_bytes = 473;
  a.quota_max_objects = 474;
  o.push_back(new pg_pool_t(a));

  o.push_back(new pg_pool_t(a));

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
    out << " flags " << p.get_flags_string();
  if (p.crash_replay_interval)
    out << " crash_replay_interval " << p.crash_replay_interval;
  if (p.quota_max_bytes)
    out << " max_bytes " << p.quota_max_bytes;
  if (p.quota_max_objects)
    out << " max_objects " << p.quota_max_objects;
  return out;
}


// -- object_stat_sum_t --

void object_stat_sum_t::dump(Formatter *f) const
{
  f->dump_int("num_bytes", num_bytes);
  f->dump_int("num_objects", num_objects);
  f->dump_int("num_read", num_rd);
  f->dump_int("num_read_kb", num_rd_kb);
  f->dump_int("num_write", num_wr);
  f->dump_int("num_write_kb", num_wr_kb);
  f->dump_int("num_objects_omap", num_objects_omap);
}

void object_stat_sum_t::encode(bufferlist& bl) const
{
  ENCODE_START(9, 3, bl);
  ::encode(num_bytes, bl);
  ::encode(num_objects, bl);
  ::encode(num_rd, bl);
  ::encode(num_rd_kb, bl);
  ::encode(num_wr, bl);
  ::encode(num_wr_kb, bl);
  ::encode(num_objects_omap, bl);
  ENCODE_FINISH(bl);
}

void object_stat_sum_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(9, 3, 3, bl);
  ::decode(num_bytes, bl);
  if (struct_v < 3) {
    uint64_t num_kb;
    ::decode(num_kb, bl);
  }
  ::decode(num_objects, bl);
  ::decode(num_rd, bl);
  ::decode(num_rd_kb, bl);
  ::decode(num_wr, bl);
  ::decode(num_wr_kb, bl);
  if (struct_v >= 8) {
    ::decode(num_objects_omap, bl);
  } else {
    num_objects_omap = 0;
  }
  DECODE_FINISH(bl);
}

void object_stat_sum_t::generate_test_instances(list<object_stat_sum_t*>& o)
{
  object_stat_sum_t a;
  o.push_back(new object_stat_sum_t(a));

  a.num_bytes = 1;
  a.num_objects = 3;
  a.num_rd = 9; a.num_rd_kb = 10;
  a.num_wr = 11; a.num_wr_kb = 12;
  o.push_back(new object_stat_sum_t(a));
}

void object_stat_sum_t::add(const object_stat_sum_t& o)
{
  num_bytes += o.num_bytes;
  num_objects += o.num_objects;
  num_rd += o.num_rd;
  num_rd_kb += o.num_rd_kb;
  num_wr += o.num_wr;
  num_wr_kb += o.num_wr_kb;
  num_objects_omap += o.num_objects_omap;
}

void object_stat_sum_t::sub(const object_stat_sum_t& o)
{
  num_bytes -= o.num_bytes;
  num_objects -= o.num_objects;
  num_rd -= o.num_rd;
  num_rd_kb -= o.num_rd_kb;
  num_wr -= o.num_wr;
  num_wr_kb -= o.num_wr_kb;
  num_objects_omap -= o.num_objects_omap;
}


// -- object_stat_collection_t --

void object_stat_collection_t::dump(Formatter *f) const
{
  f->open_object_section("stat_sum");
  sum.dump(f);
  f->close_section();
  f->open_object_section("stat_cat_sum");
  for (map<string,object_stat_sum_t>::const_iterator p = cat_sum.begin(); p != cat_sum.end(); ++p) {
    f->open_object_section(p->first.c_str());
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void object_stat_collection_t::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(sum, bl);
  ::encode(cat_sum, bl);
  ENCODE_FINISH(bl);
}

void object_stat_collection_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(sum, bl);
  ::decode(cat_sum, bl);
  DECODE_FINISH(bl);
}

void object_stat_collection_t::generate_test_instances(list<object_stat_collection_t*>& o)
{
  object_stat_collection_t a;
  o.push_back(new object_stat_collection_t(a));
  list<object_stat_sum_t*> l;
  object_stat_sum_t::generate_test_instances(l);
  char n[2] = { 'a', 0 };
  for (list<object_stat_sum_t*>::iterator p = l.begin(); p != l.end(); ++p) {
    a.add(**p, n);
    n[0]++;
    o.push_back(new object_stat_collection_t(a));
  }
}


// -- pg_stat_t --

void pg_stat_t::dump(Formatter *f) const
{
  f->dump_stream("version") << version;
  f->dump_stream("reported_seq") << reported_seq;
  f->dump_stream("reported_epoch") << reported_epoch;
  f->dump_string("state", pg_state_string(state));
  f->dump_stream("last_fresh") << last_fresh;
  f->dump_stream("last_change") << last_change;
  f->dump_stream("last_active") << last_active;
  f->dump_stream("last_became_active") << last_became_active;
  f->dump_stream("last_unstale") << last_unstale;
  f->dump_unsigned("created", created);
  f->dump_stream("parent") << parent;
  f->dump_unsigned("parent_split_bits", parent_split_bits);
  stats.dump(f);
  f->dump_int("osd", osd);
}

void pg_stat_t::dump_brief(Formatter *f) const
{
  f->dump_string("state", pg_state_string(state));
  f->dump_int("osd", osd);
}

void pg_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(17, 8, bl);
  ::encode(version, bl);
  ::encode(reported_seq, bl);
  ::encode(reported_epoch, bl);
  ::encode(state, bl);
  ::encode(created, bl);
  ::encode(parent, bl);
  ::encode(parent_split_bits, bl);
  ::encode(stats, bl);
  ::encode(last_fresh, bl);
  ::encode(last_change, bl);
  ::encode(last_active, bl);
  ::encode(last_unstale, bl);
  ::encode(last_became_active, bl);
  ::encode(osd, bl);
  ENCODE_FINISH(bl);
}

void pg_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(17, 8, 8, bl);
  ::decode(version, bl);
  ::decode(reported_seq, bl);
  ::decode(reported_epoch, bl);
  ::decode(state, bl);
  ::decode(created, bl);
  if (struct_v < 6) {
    old_pg_t opgid;
    ::decode(opgid, bl);
    parent = opgid;
  } else {
    ::decode(parent, bl);
  }
  ::decode(parent_split_bits, bl);
  if (struct_v <= 4) {
    ::decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    ::decode(num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    if (struct_v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
    }
  } else {
    ::decode(stats, bl);
    if (struct_v >= 9) {
      ::decode(last_fresh, bl);
      ::decode(last_change, bl);
      ::decode(last_active, bl);
      ::decode(last_unstale, bl);
    }
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
  a.reported_epoch = 1;
  a.reported_seq = 2;
  a.state = 123;
  a.last_fresh = utime_t(1002, 1);
  a.last_change = utime_t(1002, 2);
  a.last_active = utime_t(1002, 3);
  a.last_unstale = utime_t(1002, 5);
  a.created = 6;
  a.parent = pg_t(1, 2, 3);
  a.parent_split_bits = 12;
  list<object_stat_collection_t*> l;
  object_stat_collection_t::generate_test_instances(l);
  a.stats = *l.back();
  a.osd = 456;
  o.push_back(new pg_stat_t(a));

  a.osd = 124;
  o.push_back(new pg_stat_t(a));
}


// -- pool_stat_t --

void pool_stat_t::dump(Formatter *f) const
{
  stats.dump(f);
}

void pool_stat_t::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSDENC) == 0) {
    uint8_t v = 4;
    ::encode(v, bl);
    ::encode(stats, bl);
    return;
  }

  ENCODE_START(5, 5, bl);
  ::encode(stats, bl);
  ENCODE_FINISH(bl);
}

void pool_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
  if (struct_v >= 4) {
    ::decode(stats, bl);
  } else {
    ::decode(stats.sum.num_bytes, bl);
    uint64_t num_kb;
    ::decode(num_kb, bl);
    ::decode(stats.sum.num_objects, bl);
    if (struct_v >= 2) {
      ::decode(stats.sum.num_rd, bl);
      ::decode(stats.sum.num_rd_kb, bl);
      ::decode(stats.sum.num_wr, bl);
      ::decode(stats.sum.num_wr_kb, bl);
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
  o.push_back(new pool_stat_t(a));
}


// -- pg_history_t --

void pg_history_t::encode(bufferlist &bl) const
{
  ENCODE_START(6, 4, bl);
  ::encode(epoch_created, bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_epoch_split, bl);
  ::encode(same_interval_since, bl);
  ::encode(same_up_since, bl);
  ::encode(same_primary_since, bl);
  ENCODE_FINISH(bl);
}

void pg_history_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 4, 4, bl);
  ::decode(epoch_created, bl);
  ::decode(last_epoch_started, bl);
  ::decode(last_epoch_split, bl);
  ::decode(same_interval_since, bl);
  ::decode(same_up_since, bl);
  ::decode(same_primary_since, bl);
  DECODE_FINISH(bl);
}

void pg_history_t::dump(Formatter *f) const
{
  f->dump_int("epoch_created", epoch_created);
  f->dump_int("last_epoch_started", last_epoch_started);
  f->dump_int("last_epoch_split", last_epoch_split);
  f->dump_int("same_up_since", same_up_since);
  f->dump_int("same_interval_since", same_interval_since);
  f->dump_int("same_primary_since", same_primary_since);
}

void pg_history_t::generate_test_instances(list<pg_history_t*>& o)
{
  o.push_back(new pg_history_t);
  o.push_back(new pg_history_t);
  o.back()->epoch_created = 1;
  o.back()->last_epoch_started = 2;
  o.back()->last_epoch_split = 4;
  o.back()->same_up_since = 5;
  o.back()->same_interval_since = 6;
  o.back()->same_primary_since = 7;
}


// -- pg_info_t --

void pg_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(30, 26, bl);
  ::encode(pgid, bl);
  ::encode(last_update, bl);
  ::encode(stats, bl);
  history.encode(bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_user_version, bl);
  ENCODE_FINISH(bl);
}

void pg_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(29, 26, 26, bl);
  ::decode(pgid, bl);
  ::decode(last_update, bl);
  ::decode(stats, bl);
  history.decode(bl);
  if (struct_v < 27) {
    last_epoch_started = history.last_epoch_started;
  } else {
    ::decode(last_epoch_started, bl);
  }
  if (struct_v >= 28)
    ::decode(last_user_version, bl);
  else
    last_user_version = last_update.version;
  DECODE_FINISH(bl);
}

// -- pg_info_t --

void pg_info_t::dump(Formatter *f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_stream("last_update") << last_update;
  f->dump_int("last_user_version", last_user_version);
  f->open_object_section("history");
  history.dump(f);
  f->close_section();
  f->open_object_section("stats");
  stats.dump(f);
  f->close_section();

  f->dump_int("empty", is_empty());
  f->dump_int("dne", dne());
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
  o.back()->last_user_version = 2;
  {
    list<pg_stat_t*> s;
    pg_stat_t::generate_test_instances(s);
    o.back()->stats = *s.back();
  }
}

// -- pg_notify_t --
void pg_notify_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 1, bl);
  ::encode(query_epoch, bl);
  ::encode(epoch_sent, bl);
  ::encode(info, bl);
  ENCODE_FINISH(bl);
}

void pg_notify_t::decode(bufferlist::iterator &bl)
{
  DECODE_START(2, bl);
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
  o.push_back(new pg_notify_t(3, 1, pg_info_t()));
  o.push_back(new pg_notify_t(0, 3, pg_info_t()));
}

ostream &operator<<(ostream &lhs, const pg_notify_t &notify)
{
  lhs << "(query_epoch:" << notify.query_epoch
      << ", epoch_sent:" << notify.epoch_sent
      << ", info:" << notify.info;
  return lhs << ")";
}

// -- pg_query_t --

void pg_query_t::encode(bufferlist &bl, uint64_t features) const {
  if (features & CEPH_FEATURE_QUERY_T) {
    ENCODE_START(3, 2, bl);
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
    DECODE_START(3, bl);
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
}

// -- ObjectModDesc --
void ObjectModDesc::visit(Visitor *visitor) const
{
  bufferlist::iterator bp = bl.begin();
  try {
    while (!bp.end()) {
      DECODE_START(1, bp);
      uint8_t code;
      ::decode(code, bp);
      switch (code) {
      case APPEND: {
	uint64_t size;
	::decode(size, bp);
	visitor->append(size);
	break;
      }
      case SETATTRS: {
	map<string, boost::optional<bufferlist> > attrs;
	::decode(attrs, bp);
	visitor->setattrs(attrs);
	break;
      }
      case DELETE: {
	version_t old_version;
	::decode(old_version, bp);
	visitor->rmobject(old_version);
	break;
      }
      case CREATE: {
	visitor->create();
	break;
      }
      default:
	assert(0 == "Invalid rollback code");
      }
      DECODE_FINISH(bp);
    }
  } catch (...) {
    assert(0 == "Invalid encoding");
  }
}

struct DumpVisitor : public ObjectModDesc::Visitor {
  Formatter *f;
  DumpVisitor(Formatter *f) : f(f) {}
  void append(uint64_t old_size) {
    f->open_object_section("op");
    f->dump_string("code", "APPEND");
    f->dump_unsigned("old_size", old_size);
    f->close_section();
  }
  void setattrs(map<string, boost::optional<bufferlist> > &attrs) {
    f->open_object_section("op");
    f->dump_string("code", "SETATTRS");
    f->open_array_section("attrs");
    for (map<string, boost::optional<bufferlist> >::iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      f->dump_string("attr_name", i->first);
    }
    f->close_section();
    f->close_section();
  }
  void rmobject(version_t old_version) {
    f->open_object_section("op");
    f->dump_string("code", "RMOBJECT");
    f->dump_unsigned("old_version", old_version);
    f->close_section();
  }
  void create() {
    f->open_object_section("op");
    f->dump_string("code", "CREATE");
    f->close_section();
  }
};

void ObjectModDesc::dump(Formatter *f) const
{
  f->open_object_section("object_mod_desc");
  f->dump_stream("can_local_rollback") << can_local_rollback;
  f->dump_stream("stashed") << stashed;
  {
    f->open_array_section("ops");
    DumpVisitor vis(f);
    visit(&vis);
    f->close_section();
  }
  f->close_section();
}

void ObjectModDesc::generate_test_instances(list<ObjectModDesc*>& o)
{
  map<string, boost::optional<bufferlist> > attrs;
  attrs[OI_ATTR];
  attrs["asdf"];
  o.push_back(new ObjectModDesc());
  o.back()->append(100);
  o.back()->setattrs(attrs);
  o.push_back(new ObjectModDesc());
  o.back()->rmobject(1001);
  o.push_back(new ObjectModDesc());
  o.back()->create();
  o.back()->setattrs(attrs);
  o.push_back(new ObjectModDesc());
  o.back()->create();
  o.back()->setattrs(attrs);
  o.back()->append(1000);
}

void ObjectModDesc::encode(bufferlist &_bl) const
{
  ENCODE_START(1, 1, _bl);
  ::encode(can_local_rollback, _bl);
  ::encode(stashed, _bl);
  ::encode(bl, _bl);
  ENCODE_FINISH(_bl);
}
void ObjectModDesc::decode(bufferlist::iterator &_bl)
{
  DECODE_START(1, _bl);
  ::decode(can_local_rollback, _bl);
  ::decode(stashed, _bl);
  ::decode(bl, _bl);
  DECODE_FINISH(_bl);
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


// -- osd_peer_stat_t --

void osd_peer_stat_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void osd_peer_stat_t::decode(bufferlist::iterator& bl)
{
  DECODE_START(1, bl);
  ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void osd_peer_stat_t::dump(Formatter *f) const
{
  f->dump_stream("stamp") << stamp;
}

void osd_peer_stat_t::generate_test_instances(list<osd_peer_stat_t*>& o)
{
  o.push_back(new osd_peer_stat_t);
  o.push_back(new osd_peer_stat_t);
  o.back()->stamp = utime_t(1, 2);
}

ostream& operator<<(ostream& out, const osd_peer_stat_t &stat)
{
  return out << "stat(" << stat.stamp << ")";
}


// -- OSDSuperblock --

void OSDSuperblock::encode(bufferlist &bl) const
{
  ENCODE_START(6, 5, bl);
  ::encode(cluster_fsid, bl);
  ::encode(whoami, bl);
  ::encode(current_epoch, bl);
  ::encode(oldest_map, bl);
  ::encode(newest_map, bl);
  ::encode(weight, bl);
  compat_features.encode(bl);
  ::encode(mounted, bl);
  ::encode(osd_fsid, bl);
  ::encode(last_map_marked_full, bl);
  ENCODE_FINISH(bl);
}

void OSDSuperblock::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  if (struct_v < 3) {
    string magic;
    ::decode(magic, bl);
  }
  ::decode(cluster_fsid, bl);
  ::decode(whoami, bl);
  ::decode(current_epoch, bl);
  ::decode(oldest_map, bl);
  ::decode(newest_map, bl);
  ::decode(weight, bl);
  if (struct_v >= 2) {
    compat_features.decode(bl);
  } else { //upgrade it!
    compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  }
  ::decode(mounted, bl);
  if (struct_v >= 4)
    ::decode(osd_fsid, bl);
  if (struct_v >= 6)
    ::decode(last_map_marked_full, bl);
  DECODE_FINISH(bl);
}

void OSDSuperblock::dump(Formatter *f) const
{
  f->dump_stream("cluster_fsid") << cluster_fsid;
  f->dump_stream("osd_fsid") << osd_fsid;
  f->dump_int("whoami", whoami);
  f->dump_int("current_epoch", current_epoch);
  f->dump_int("oldest_map", oldest_map);
  f->dump_int("newest_map", newest_map);
  f->dump_float("weight", weight);
  f->open_object_section("compat");
  compat_features.dump(f);
  f->close_section();
  f->dump_int("last_epoch_mounted", mounted);
  f->dump_int("last_map_marked_full", last_map_marked_full);
}

void OSDSuperblock::generate_test_instances(list<OSDSuperblock*>& o)
{
  OSDSuperblock z;
  o.push_back(new OSDSuperblock(z));
  memset(&z.cluster_fsid, 1, sizeof(z.cluster_fsid));
  memset(&z.osd_fsid, 2, sizeof(z.osd_fsid));
  z.whoami = 3;
  z.current_epoch = 4;
  z.oldest_map = 5;
  z.newest_map = 9;
  z.mounted = 8;
  o.push_back(new OSDSuperblock(z));
  z.last_map_marked_full = 7;
  o.push_back(new OSDSuperblock(z));
}

// -- watch_info_t --

void watch_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(4, 3, bl);
  ::encode(cookie, bl);
  ::encode(timeout_seconds, bl);
  ::encode(addr, bl);
  ENCODE_FINISH(bl);
}

void watch_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, bl);
  ::decode(cookie, bl);
  if (struct_v < 2) {
    uint64_t ver;
    ::decode(ver, bl);
  }
  ::decode(timeout_seconds, bl);
  if (struct_v >= 4) {
    ::decode(addr, bl);
  }
  DECODE_FINISH(bl);
}

void watch_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("cookie", cookie);
  f->dump_unsigned("timeout_seconds", timeout_seconds);
  f->open_object_section("addr");
  addr.dump(f);
  f->close_section();
}

void watch_info_t::generate_test_instances(list<watch_info_t*>& o)
{
  o.push_back(new watch_info_t);
  o.push_back(new watch_info_t);
  o.back()->cookie = 123;
  o.back()->timeout_seconds = 99;
  entity_addr_t ea;
  ea.set_nonce(1);
  ea.set_family(AF_INET);
  ea.set_in4_quad(0, 127);
  ea.set_in4_quad(1, 0);
  ea.set_in4_quad(2, 1);
  ea.set_in4_quad(3, 2);
  ea.set_port(2);
  o.back()->addr = ea;
}


// -- object_info_t --

void object_info_t::copy_user_bits(const object_info_t& other)
{
  // these bits are copied from head->clone.
  size = other.size;
  mtime = other.mtime;
  last_reqid = other.last_reqid;
  truncate_seq = other.truncate_seq;
  truncate_size = other.truncate_size;
  flags = other.flags;
  category = other.category;
  user_version = other.user_version;
}

ps_t object_info_t::legacy_object_locator_to_ps(const object_t &oid, 
						const object_locator_t &loc) {
  ps_t ps;
  if (loc.key.length())
    // Hack, we don't have the osd map, so we don't really know the hash...
    ps = ceph_str_hash(CEPH_STR_HASH_RJENKINS, loc.key.c_str(), 
		       loc.key.length());
  else
    ps = ceph_str_hash(CEPH_STR_HASH_RJENKINS, oid.name.c_str(),
		       oid.name.length());
  return ps;
}

void object_info_t::encode(bufferlist& bl) const
{
  object_locator_t myoloc(soid);
  map<entity_name_t, watch_info_t> old_watchers;
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator i =
	 watchers.begin();
       i != watchers.end();
       ++i) {
    old_watchers.insert(make_pair(i->first.second, i->second));
  }
  ENCODE_START(13, 8, bl);
  ::encode(soid, bl);
  ::encode(myoloc, bl);	//Retained for compatibility
  ::encode(category, bl);
  ::encode(version, bl);
  ::encode(prior_version, bl);
  ::encode(last_reqid, bl);
  ::encode(size, bl);
  ::encode(mtime, bl);
  ::encode(wrlock_by, bl);
  ::encode(truncate_seq, bl);
  ::encode(truncate_size, bl);
  ::encode(old_watchers, bl);
  /* shenanigans to avoid breaking backwards compatibility in the disk format.
   * When we can, switch this out for simply putting the version_t on disk. */
  eversion_t user_eversion(0, user_version);
  ::encode(user_eversion, bl);
  ::encode(watchers, bl);
  uint32_t _flags = flags;
  ::encode(_flags, bl);
  ENCODE_FINISH(bl);
}

void object_info_t::decode(bufferlist::iterator& bl)
{
  object_locator_t myoloc;
  DECODE_START_LEGACY_COMPAT_LEN(13, 8, 8, bl);
  map<entity_name_t, watch_info_t> old_watchers;
  ::decode(soid, bl);
  ::decode(myoloc, bl);
  if (struct_v == 6) {
    hobject_t hoid(soid.oid, myoloc.key, soid.hash, 0 , "");
    soid = hoid;
  }

  if (struct_v >= 5)
    ::decode(category, bl);
  ::decode(version, bl);
  ::decode(prior_version, bl);
  ::decode(last_reqid, bl);
  ::decode(size, bl);
  ::decode(mtime, bl);
  ::decode(wrlock_by, bl);
  ::decode(truncate_seq, bl);
  ::decode(truncate_size, bl);
  if (struct_v >= 3) {
    // if this is struct_v >= 13, we will overwrite this
    // below since this field is just here for backwards
    // compatibility
    uint8_t lo;
    ::decode(lo, bl);
    flags = (flag_t)lo;
  } else {
    flags = (flag_t)0;
  }
  if (struct_v >= 4) {
    ::decode(old_watchers, bl);
    eversion_t user_eversion;
    ::decode(user_eversion, bl);
    user_version = user_eversion.version;
  }
  if (struct_v < 10)
    soid.pool = myoloc.pool;
  if (struct_v >= 11) {
    ::decode(watchers, bl);
  } else {
    for (map<entity_name_t, watch_info_t>::iterator i = old_watchers.begin();
	 i != old_watchers.end();
	 ++i) {
      watchers.insert(
	make_pair(
	  make_pair(i->second.cookie, i->first), i->second));
    }
  }
  if (struct_v >= 13) {
    uint32_t _flags;
    ::decode(_flags, bl);
    flags = (flag_t)_flags;
  }
  DECODE_FINISH(bl);
}

void object_info_t::dump(Formatter *f) const
{
  f->open_object_section("oid");
  soid.dump(f);
  f->close_section();
  f->dump_string("category", category);
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << prior_version;
  f->dump_stream("last_reqid") << last_reqid;
  f->dump_unsigned("user_version", user_version);
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("flags", (int)flags);
  f->dump_stream("wrlock_by") << wrlock_by;
  f->dump_unsigned("truncate_seq", truncate_seq);
  f->dump_unsigned("truncate_size", truncate_size);
  f->open_object_section("watchers");
  for (map<pair<uint64_t, entity_name_t>,watch_info_t>::const_iterator p =
         watchers.begin(); p != watchers.end(); ++p) {
    stringstream ss;
    ss << p->first.second;
    f->open_object_section(ss.str().c_str());
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void object_info_t::generate_test_instances(list<object_info_t*>& o)
{
  o.push_back(new object_info_t());
  
  // fixme
}


ostream& operator<<(ostream& out, const object_info_t& oi)
{
  out << oi.soid << "(" << oi.version
      << " " << oi.last_reqid;
  out << " wrlock_by=" << oi.wrlock_by;
  if (oi.flags)
    out << " " << oi.get_flag_string();
  out << " s " << oi.size;
  out << " uv" << oi.user_version;
  out << ")";
  return out;
}

// -- OSDOp --

ostream& operator<<(ostream& out, const OSDOp& op)
{
  out << ceph_osd_op_name(op.op.op);
  if (ceph_osd_op_type_data(op.op.op)) {
    // data extent
    switch (op.op.op) {
    case CEPH_OSD_OP_STAT:
    case CEPH_OSD_OP_DELETE:
    case CEPH_OSD_OP_LIST_WATCHERS:
    case CEPH_OSD_OP_ASSERT_VER:
      out << " v" << op.op.assert_ver.ver;
      break;
    case CEPH_OSD_OP_TRUNCATE:
      out << " " << op.op.extent.offset;
      break;
    case CEPH_OSD_OP_MASKTRUNC:
    case CEPH_OSD_OP_TRIMTRUNC:
      out << " " << op.op.extent.truncate_seq << "@" << (int64_t)op.op.extent.truncate_size;
      break;
    case CEPH_OSD_OP_WATCH:
      out << (op.op.watch.flag ? " add":" remove")
	  << " cookie " << op.op.watch.cookie << " ver " << op.op.watch.ver;
      break;
    case CEPH_OSD_OP_SETALLOCHINT:
      out << " object_size " << op.op.alloc_hint.expected_object_size
	  << " write_size " << op.op.alloc_hint.expected_write_size;
      break;
    default:
      out << " " << op.op.extent.offset << "~" << op.op.extent.length;
      if (op.op.extent.truncate_seq)
	out << " [" << op.op.extent.truncate_seq << "@" << (int64_t)op.op.extent.truncate_size << "]";
    }
  } else if (ceph_osd_op_type_attr(op.op.op)) {
    // xattr name
    if (op.op.xattr.name_len && op.indata.length()) {
      out << " ";
      op.indata.write(0, op.op.xattr.name_len, out);
    }
    if (op.op.xattr.value_len)
      out << " (" << op.op.xattr.value_len << ")";
    if (op.op.op == CEPH_OSD_OP_CMPXATTR)
      out << " op " << (int)op.op.xattr.cmp_op << " mode " << (int)op.op.xattr.cmp_mode;
  } else if (ceph_osd_op_type_exec(op.op.op)) {
    // class.method
    if (op.op.cls.class_len && op.indata.length()) {
      out << " ";
      op.indata.write(0, op.op.cls.class_len, out);
      out << ".";
      op.indata.write(op.op.cls.class_len, op.op.cls.method_len, out);
    }
  } else if (ceph_osd_op_type_pg(op.op.op)) {
    switch (op.op.op) {
    case CEPH_OSD_OP_PGLS:
    case CEPH_OSD_OP_PGLS_FILTER:
      out << " start_epoch " << op.op.pgls.start_epoch;
      break;
    case CEPH_OSD_OP_PG_HITSET_LS:
      break;
    }
  } else if (ceph_osd_op_type_multi(op.op.op)) {
    switch (op.op.op) {
    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      out << " v" << op.op.watch.ver
	  << " of " << op.oid;
      break;
    case CEPH_OSD_OP_SRC_CMPXATTR:
      out << " " << op.oid;
      if (op.op.xattr.name_len && op.indata.length()) {
	out << " ";
	op.indata.write(0, op.op.xattr.name_len, out);
      }
      if (op.op.xattr.value_len)
	out << " (" << op.op.xattr.value_len << ")";
      if (op.op.op == CEPH_OSD_OP_CMPXATTR)
	out << " op " << (int)op.op.xattr.cmp_op << " mode " << (int)op.op.xattr.cmp_mode;
      break;
    }
  }
  return out;
}


void OSDOp::split_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& in)
{
  bufferlist::iterator datap = in.begin();
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ceph_osd_op_type_multi(ops[i].op.op)) {
      ::decode(ops[i].oid, datap);
    }
    if (ops[i].op.payload_len) {
      datap.copy(ops[i].op.payload_len, ops[i].indata);
    }
  }
}

void OSDOp::merge_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& out)
{
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ceph_osd_op_type_multi(ops[i].op.op)) {
      ::encode(ops[i].oid, out);
    }
    if (ops[i].indata.length()) {
      ops[i].op.payload_len = ops[i].indata.length();
      out.append(ops[i].indata);
    }
  }
}

void OSDOp::split_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& in)
{
  bufferlist::iterator datap = in.begin();
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ops[i].op.payload_len) {
      datap.copy(ops[i].op.payload_len, ops[i].outdata);
    }
  }
}

void OSDOp::merge_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& out)
{
  for (unsigned i = 0; i < ops.size(); i++) {
    if (ops[i].outdata.length()) {
      ops[i].op.payload_len = ops[i].outdata.length();
      out.append(ops[i].outdata);
    }
  }
}
