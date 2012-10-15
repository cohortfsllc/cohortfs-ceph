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

#include "osd_types.h"
#include "include/ceph_features.h"
#include "OSDMap.h"
extern "C" {
#include "crush/hash.h"
}
// #include "pg/PG.h"

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
  ENCODE_START(4, 3, bl);
  ::encode(pool, bl);
  int32_t preferred = -1;  // tell old code there is no preferred osd (-1).
  ::encode(preferred, bl);
  ::encode(key, bl);
  ENCODE_FINISH(bl);
}

void object_locator_t::decode(bufferlist::iterator& p)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 3, 3, p);
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
  DECODE_FINISH(p);
}

void object_locator_t::dump(Formatter *f) const
{
  f->dump_int("pool", pool);
  f->dump_string("key", key);
}

void object_locator_t::generate_test_instances(list<object_locator_t*>& o)
{
  o.push_back(new object_locator_t);
  o.push_back(new object_locator_t(123));
  o.push_back(new object_locator_t(1234, "key"));
  o.push_back(new object_locator_t(12, "key2"));
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
  f->dump_int("snap_trim_queue_len", snap_trim_queue_len);
  f->dump_int("num_snap_trimming", num_snap_trimming);
}

void osd_stat_t::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(kb, bl);
  ::encode(kb_used, bl);
  ::encode(kb_avail, bl);
  ::encode(snap_trim_queue_len, bl);
  ::encode(num_snap_trimming, bl);
  ::encode(hb_in, bl);
  ::encode(hb_out, bl);
  ENCODE_FINISH(bl);
}

void osd_stat_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(kb, bl);
  ::decode(kb_used, bl);
  ::decode(kb_avail, bl);
  ::decode(snap_trim_queue_len, bl);
  ::decode(num_snap_trimming, bl);
  ::decode(hb_in, bl);
  ::decode(hb_out, bl);
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
  o.back()->snap_trim_queue_len = 8;
  o.back()->num_snap_trimming = 99;
}

// -- eversion_t --
string eversion_t::get_key_name() const
{
  char key[40];
  snprintf(
    key, sizeof(key), "%010u.%020llu", epoch, (long long unsigned)version);
  return string(key);
}

// -- object_stat_sum_t --

void object_stat_sum_t::dump(Formatter *f) const
{
  f->dump_int("num_bytes", num_bytes);
  f->dump_int("num_objects", num_objects);
  f->dump_int("num_object_clones", num_object_clones);
  f->dump_int("num_object_copies", num_object_copies);
  f->dump_int("num_objects_missing_on_primary", num_objects_missing_on_primary);
  f->dump_int("num_objects_degraded", num_objects_degraded);
  f->dump_int("num_objects_unfound", num_objects_unfound);
  f->dump_int("num_read", num_rd);
  f->dump_int("num_read_kb", num_rd_kb);
  f->dump_int("num_write", num_wr);
  f->dump_int("num_write_kb", num_wr_kb);
  f->dump_int("num_scrub_errors", num_scrub_errors);
  f->dump_int("num_shallow_scrub_errors", num_shallow_scrub_errors);
  f->dump_int("num_deep_scrub_errors", num_deep_scrub_errors);
  f->dump_int("num_objects_recovered", num_objects_recovered);
  f->dump_int("num_bytes_recovered", num_bytes_recovered);
  f->dump_int("num_keys_recovered", num_keys_recovered);
}

void object_stat_sum_t::encode(bufferlist& bl) const
{
  ENCODE_START(6, 3, bl);
  ::encode(num_bytes, bl);
  ::encode(num_objects, bl);
  ::encode(num_object_clones, bl);
  ::encode(num_object_copies, bl);
  ::encode(num_objects_missing_on_primary, bl);
  ::encode(num_objects_degraded, bl);
  ::encode(num_objects_unfound, bl);
  ::encode(num_rd, bl);
  ::encode(num_rd_kb, bl);
  ::encode(num_wr, bl);
  ::encode(num_wr_kb, bl);
  ::encode(num_scrub_errors, bl);
  ::encode(num_objects_recovered, bl);
  ::encode(num_bytes_recovered, bl);
  ::encode(num_keys_recovered, bl);
  ::encode(num_shallow_scrub_errors, bl);
  ::encode(num_deep_scrub_errors, bl);
  ENCODE_FINISH(bl);
}

void object_stat_sum_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
  ::decode(num_bytes, bl);
  if (struct_v < 3) {
    uint64_t num_kb;
    ::decode(num_kb, bl);
  }
  ::decode(num_objects, bl);
  ::decode(num_object_clones, bl);
  ::decode(num_object_copies, bl);
  ::decode(num_objects_missing_on_primary, bl);
  ::decode(num_objects_degraded, bl);
  if (struct_v >= 2)
    ::decode(num_objects_unfound, bl);
  ::decode(num_rd, bl);
  ::decode(num_rd_kb, bl);
  ::decode(num_wr, bl);
  ::decode(num_wr_kb, bl);
  if (struct_v >= 4)
    ::decode(num_scrub_errors, bl);
  else
    num_scrub_errors = 0;
  if (struct_v >= 5) {
    ::decode(num_objects_recovered, bl);
    ::decode(num_bytes_recovered, bl);
    ::decode(num_keys_recovered, bl);
  } else {
    num_objects_recovered = 0;
    num_bytes_recovered = 0;
    num_keys_recovered = 0;
  }
  if (struct_v >= 6) {
    ::decode(num_shallow_scrub_errors, bl);
    ::decode(num_deep_scrub_errors, bl);
  } else {
    num_shallow_scrub_errors = 0;
    num_deep_scrub_errors = 0;
  }
  DECODE_FINISH(bl);
}

void object_stat_sum_t::generate_test_instances(list<object_stat_sum_t*>& o)
{
  object_stat_sum_t a;
  o.push_back(new object_stat_sum_t(a));

  a.num_bytes = 1;
  a.num_objects = 3;
  a.num_object_clones = 4;
  a.num_object_copies = 5;
  a.num_objects_missing_on_primary = 6;
  a.num_objects_degraded = 7;
  a.num_objects_unfound = 8;
  a.num_rd = 9; a.num_rd_kb = 10;
  a.num_wr = 11; a.num_wr_kb = 12;
  a.num_objects_recovered = 14;
  a.num_bytes_recovered = 15;
  a.num_keys_recovered = 16;
  a.num_deep_scrub_errors = 17;
  a.num_shallow_scrub_errors = 18;
  a.num_scrub_errors = a.num_deep_scrub_errors + a.num_shallow_scrub_errors;
  o.push_back(new object_stat_sum_t(a));
}

void object_stat_sum_t::add(const object_stat_sum_t& o)
{
  num_bytes += o.num_bytes;
  num_objects += o.num_objects;
  num_object_clones += o.num_object_clones;
  num_object_copies += o.num_object_copies;
  num_objects_missing_on_primary += o.num_objects_missing_on_primary;
  num_objects_degraded += o.num_objects_degraded;
  num_rd += o.num_rd;
  num_rd_kb += o.num_rd_kb;
  num_wr += o.num_wr;
  num_wr_kb += o.num_wr_kb;
  num_objects_unfound += o.num_objects_unfound;
  num_scrub_errors += o.num_scrub_errors;
  num_shallow_scrub_errors += o.num_shallow_scrub_errors;
  num_deep_scrub_errors += o.num_deep_scrub_errors;
  num_objects_recovered += o.num_objects_recovered;
  num_bytes_recovered += o.num_bytes_recovered;
  num_keys_recovered += o.num_keys_recovered;
}

void object_stat_sum_t::sub(const object_stat_sum_t& o)
{
  num_bytes -= o.num_bytes;
  num_objects -= o.num_objects;
  num_object_clones -= o.num_object_clones;
  num_object_copies -= o.num_object_copies;
  num_objects_missing_on_primary -= o.num_objects_missing_on_primary;
  num_objects_degraded -= o.num_objects_degraded;
  num_rd -= o.num_rd;
  num_rd_kb -= o.num_rd_kb;
  num_wr -= o.num_wr;
  num_wr_kb -= o.num_wr_kb;
  num_objects_unfound -= o.num_objects_unfound;
  num_scrub_errors -= o.num_scrub_errors;
  num_shallow_scrub_errors -= o.num_shallow_scrub_errors;
  num_deep_scrub_errors -= o.num_deep_scrub_errors;
  num_objects_recovered -= o.num_objects_recovered;
  num_bytes_recovered -= o.num_bytes_recovered;
  num_keys_recovered -= o.num_keys_recovered;
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

// -- object_stat_sum_t --

void object_stat_sum_t::dump(Formatter *f) const
{
  f->dump_int("num_bytes", num_bytes);
  f->dump_int("num_objects", num_objects);
  f->dump_int("num_object_clones", num_object_clones);
  f->dump_int("num_object_copies", num_object_copies);
  f->dump_int("num_objects_missing_on_primary", num_objects_missing_on_primary);
  f->dump_int("num_objects_degraded", num_objects_degraded);
  f->dump_int("num_objects_unfound", num_objects_unfound);
  f->dump_int("num_read", num_rd);
  f->dump_int("num_read_kb", num_rd_kb);
  f->dump_int("num_write", num_wr);
  f->dump_int("num_write_kb", num_wr_kb);
  f->dump_int("num_scrub_errors", num_scrub_errors);
  f->dump_int("num_shallow_scrub_errors", num_shallow_scrub_errors);
  f->dump_int("num_deep_scrub_errors", num_deep_scrub_errors);
  f->dump_int("num_objects_recovered", num_objects_recovered);
  f->dump_int("num_bytes_recovered", num_bytes_recovered);
  f->dump_int("num_keys_recovered", num_keys_recovered);
}

void object_stat_sum_t::encode(bufferlist& bl) const
{
  ENCODE_START(6, 3, bl);
  ::encode(num_bytes, bl);
  ::encode(num_objects, bl);
  ::encode(num_object_clones, bl);
  ::encode(num_object_copies, bl);
  ::encode(num_objects_missing_on_primary, bl);
  ::encode(num_objects_degraded, bl);
  ::encode(num_objects_unfound, bl);
  ::encode(num_rd, bl);
  ::encode(num_rd_kb, bl);
  ::encode(num_wr, bl);
  ::encode(num_wr_kb, bl);
  ::encode(num_scrub_errors, bl);
  ::encode(num_objects_recovered, bl);
  ::encode(num_bytes_recovered, bl);
  ::encode(num_keys_recovered, bl);
  ::encode(num_shallow_scrub_errors, bl);
  ::encode(num_deep_scrub_errors, bl);
  ENCODE_FINISH(bl);
}

void object_stat_sum_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 3, 3, bl);
  ::decode(num_bytes, bl);
  if (struct_v < 3) {
    uint64_t num_kb;
    ::decode(num_kb, bl);
  }
  ::decode(num_objects, bl);
  ::decode(num_object_clones, bl);
  ::decode(num_object_copies, bl);
  ::decode(num_objects_missing_on_primary, bl);
  ::decode(num_objects_degraded, bl);
  if (struct_v >= 2)
    ::decode(num_objects_unfound, bl);
  ::decode(num_rd, bl);
  ::decode(num_rd_kb, bl);
  ::decode(num_wr, bl);
  ::decode(num_wr_kb, bl);
  if (struct_v >= 4)
    ::decode(num_scrub_errors, bl);
  else
    num_scrub_errors = 0;
  if (struct_v >= 5) {
    ::decode(num_objects_recovered, bl);
    ::decode(num_bytes_recovered, bl);
    ::decode(num_keys_recovered, bl);
  } else {
    num_objects_recovered = 0;
    num_bytes_recovered = 0;
    num_keys_recovered = 0;
  }
  if (struct_v >= 6) {
    ::decode(num_shallow_scrub_errors, bl);
    ::decode(num_deep_scrub_errors, bl);
  } else {
    num_shallow_scrub_errors = 0;
    num_deep_scrub_errors = 0;
  }
  DECODE_FINISH(bl);
}

void object_stat_sum_t::generate_test_instances(list<object_stat_sum_t*>& o)
{
  object_stat_sum_t a;
  o.push_back(new object_stat_sum_t(a));

  a.num_bytes = 1;
  a.num_objects = 3;
  a.num_object_clones = 4;
  a.num_object_copies = 5;
  a.num_objects_missing_on_primary = 6;
  a.num_objects_degraded = 7;
  a.num_objects_unfound = 8;
  a.num_rd = 9; a.num_rd_kb = 10;
  a.num_wr = 11; a.num_wr_kb = 12;
  a.num_objects_recovered = 14;
  a.num_bytes_recovered = 15;
  a.num_keys_recovered = 16;
  a.num_deep_scrub_errors = 17;
  a.num_shallow_scrub_errors = 18;
  a.num_scrub_errors = a.num_deep_scrub_errors + a.num_shallow_scrub_errors;
  o.push_back(new object_stat_sum_t(a));
}

void object_stat_sum_t::add(const object_stat_sum_t& o)
{
  num_bytes += o.num_bytes;
  num_objects += o.num_objects;
  num_object_clones += o.num_object_clones;
  num_object_copies += o.num_object_copies;
  num_objects_missing_on_primary += o.num_objects_missing_on_primary;
  num_objects_degraded += o.num_objects_degraded;
  num_rd += o.num_rd;
  num_rd_kb += o.num_rd_kb;
  num_wr += o.num_wr;
  num_wr_kb += o.num_wr_kb;
  num_objects_unfound += o.num_objects_unfound;
  num_scrub_errors += o.num_scrub_errors;
  num_shallow_scrub_errors += o.num_shallow_scrub_errors;
  num_deep_scrub_errors += o.num_deep_scrub_errors;
  num_objects_recovered += o.num_objects_recovered;
  num_bytes_recovered += o.num_bytes_recovered;
  num_keys_recovered += o.num_keys_recovered;
}

void object_stat_sum_t::sub(const object_stat_sum_t& o)
{
  num_bytes -= o.num_bytes;
  num_objects -= o.num_objects;
  num_object_clones -= o.num_object_clones;
  num_object_copies -= o.num_object_copies;
  num_objects_missing_on_primary -= o.num_objects_missing_on_primary;
  num_objects_degraded -= o.num_objects_degraded;
  num_rd -= o.num_rd;
  num_rd_kb -= o.num_rd_kb;
  num_wr -= o.num_wr;
  num_wr_kb -= o.num_wr_kb;
  num_objects_unfound -= o.num_objects_unfound;
  num_scrub_errors -= o.num_scrub_errors;
  num_shallow_scrub_errors -= o.num_shallow_scrub_errors;
  num_deep_scrub_errors -= o.num_deep_scrub_errors;
  num_objects_recovered -= o.num_objects_recovered;
  num_bytes_recovered -= o.num_bytes_recovered;
  num_keys_recovered -= o.num_keys_recovered;
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
  ENCODE_START(5, 5, bl);
  ::encode(cluster_fsid, bl);
  ::encode(whoami, bl);
  ::encode(current_epoch, bl);
  ::encode(oldest_map, bl);
  ::encode(newest_map, bl);
  ::encode(weight, bl);
  compat_features.encode(bl);
  ::encode(clean_thru, bl);
  ::encode(mounted, bl);
  ::encode(osd_fsid, bl);
  ENCODE_FINISH(bl);
}

void OSDSuperblock::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 5, 5, bl);
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
  ::decode(clean_thru, bl);
  ::decode(mounted, bl);
  if (struct_v >= 4)
    ::decode(osd_fsid, bl);
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
  f->dump_int("clean_thru", clean_thru);
  f->dump_int("last_epoch_mounted", mounted);
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
  z.clean_thru = 7;
  o.push_back(new OSDSuperblock(z));
}

// -- SnapSet --

void SnapSet::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(seq, bl);
  ::encode(head_exists, bl);
  ::encode(snaps, bl);
  ::encode(clones, bl);
  ::encode(clone_overlap, bl);
  ::encode(clone_size, bl);
  ENCODE_FINISH(bl);
}

void SnapSet::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(seq, bl);
  ::decode(head_exists, bl);
  ::decode(snaps, bl);
  ::decode(clones, bl);
  ::decode(clone_overlap, bl);
  ::decode(clone_size, bl);
  DECODE_FINISH(bl);
}

void SnapSet::dump(Formatter *f) const
{
  SnapContext sc(seq, snaps);
  f->open_object_section("snap_context");
  sc.dump(f);
  f->close_section();
  f->dump_int("head_exists", head_exists);
  f->open_array_section("clones");
  for (vector<snapid_t>::const_iterator p = clones.begin(); p != clones.end(); ++p) {
    f->open_object_section("clone");
    f->dump_unsigned("snap", *p);
    f->dump_unsigned("size", clone_size.find(*p)->second);
    f->dump_stream("overlap") << clone_overlap.find(*p)->second;
    f->close_section();
  }
  f->close_section();
}

void SnapSet::generate_test_instances(list<SnapSet*>& o)
{
  o.push_back(new SnapSet);
  o.push_back(new SnapSet);
  o.back()->head_exists = true;
  o.back()->seq = 123;
  o.back()->snaps.push_back(123);
  o.back()->snaps.push_back(12);
  o.push_back(new SnapSet);
  o.back()->head_exists = true;
  o.back()->seq = 123;
  o.back()->snaps.push_back(123);
  o.back()->snaps.push_back(12);
  o.back()->clones.push_back(12);
  o.back()->clone_size[12] = 12345;
  o.back()->clone_overlap[12];
}

ostream& operator<<(ostream& out, const SnapSet& cs)
{
  return out << cs.seq << "=" << cs.snaps << ":"
	     << cs.clones
	     << (cs.head_exists ? "+head":"");
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
  lost = other.lost;
  category = other.category;
  uses_tmap = other.uses_tmap;
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
  map<entity_name_t, watch_info_t> old_watchers;
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator i =
	 watchers.begin();
       i != watchers.end();
       ++i) {
    old_watchers.insert(make_pair(i->first.second, i->second));
  }
  ENCODE_START(11, 8, bl);
  ::encode(soid, bl);
  ::encode(oloc, bl);
  ::encode(category, bl);
  ::encode(version, bl);
  ::encode(prior_version, bl);
  ::encode(last_reqid, bl);
  ::encode(size, bl);
  ::encode(mtime, bl);
  if (soid.snap == CEPH_NOSNAP)
    ::encode(wrlock_by, bl);
  else
    ::encode(snaps, bl);
  ::encode(truncate_seq, bl);
  ::encode(truncate_size, bl);
  ::encode(lost, bl);
  ::encode(old_watchers, bl);
  ::encode(user_version, bl);
  ::encode(uses_tmap, bl);
  ::encode(watchers, bl);
  ENCODE_FINISH(bl);
}

void object_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(11, 8, 8, bl);
  map<entity_name_t, watch_info_t> old_watchers;
  if (struct_v >= 2 && struct_v <= 5) {
    sobject_t obj;
    ::decode(obj, bl);
    ::decode(oloc, bl);
    soid = hobject_t(obj.oid, oloc.key, obj.snap, 0, 0);
    soid.hash = legacy_object_locator_to_ps(soid.oid, oloc);
  } else if (struct_v >= 6) {
    ::decode(soid, bl);
    ::decode(oloc, bl);
    if (struct_v == 6) {
      hobject_t hoid(soid.oid, oloc.key, soid.snap, soid.hash, 0);
      soid = hoid;
    }
  }
    
  if (struct_v >= 5)
    ::decode(category, bl);
  ::decode(version, bl);
  ::decode(prior_version, bl);
  ::decode(last_reqid, bl);
  ::decode(size, bl);
  ::decode(mtime, bl);
  if (soid.snap == CEPH_NOSNAP)
    ::decode(wrlock_by, bl);
  else
    ::decode(snaps, bl);
  ::decode(truncate_seq, bl);
  ::decode(truncate_size, bl);
  if (struct_v >= 3)
    ::decode(lost, bl);
  else
    lost = false;
  if (struct_v >= 4) {
    ::decode(old_watchers, bl);
    ::decode(user_version, bl);
  }
  if (struct_v >= 9)
    ::decode(uses_tmap, bl);
  else
    uses_tmap = true;
  if (struct_v < 10)
    soid.pool = oloc.pool;
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
  DECODE_FINISH(bl);
}

void object_info_t::dump(Formatter *f) const
{
  f->open_object_section("oid");
  soid.dump(f);
  f->close_section();
  f->open_object_section("locator");
  oloc.dump(f);
  f->close_section();
  f->dump_string("category", category);
  f->dump_stream("version") << version;
  f->dump_stream("prior_version") << prior_version;
  f->dump_stream("last_reqid") << last_reqid;
  f->dump_unsigned("size", size);
  f->dump_stream("mtime") << mtime;
  f->dump_unsigned("lost", lost);
  f->dump_stream("wrlock_by") << wrlock_by;
  f->open_array_section("snaps");
  for (vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p)
    f->dump_unsigned("snap", *p);
  f->close_section();
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
  if (oi.soid.snap == CEPH_NOSNAP)
    out << " wrlock_by=" << oi.wrlock_by;
  else
    out << " " << oi.snaps;
  if (oi.lost)
    out << " LOST";
  out << ")";
  return out;
}

// -- ObjectRecovery --
void ObjectRecoveryProgress::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(first, bl);
  ::encode(data_complete, bl);
  ::encode(data_recovered_to, bl);
  ::encode(omap_recovered_to, bl);
  ::encode(omap_complete, bl);
  ENCODE_FINISH(bl);
}

void ObjectRecoveryProgress::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(first, bl);
  ::decode(data_complete, bl);
  ::decode(data_recovered_to, bl);
  ::decode(omap_recovered_to, bl);
  ::decode(omap_complete, bl);
  DECODE_FINISH(bl);
}

ostream &operator<<(ostream &out, const ObjectRecoveryProgress &prog)
{
  return prog.print(out);
}

void ObjectRecoveryProgress::generate_test_instances(
  list<ObjectRecoveryProgress*>& o)
{
  o.push_back(new ObjectRecoveryProgress);
  o.back()->first = false;
  o.back()->data_complete = true;
  o.back()->omap_complete = true;
  o.back()->data_recovered_to = 100;

  o.push_back(new ObjectRecoveryProgress);
  o.back()->first = true;
  o.back()->data_complete = false;
  o.back()->omap_complete = false;
  o.back()->data_recovered_to = 0;
}

ostream &ObjectRecoveryProgress::print(ostream &out) const
{
  return out << "ObjectRecoveryProgress("
	     << ( first ? "" : "!" ) << "first, "
	     << "data_recovered_to:" << data_recovered_to
	     << ", data_complete:" << ( data_complete ? "true" : "false" )
	     << ", omap_recovered_to:" << omap_recovered_to
	     << ", omap_complete:" << ( omap_complete ? "true" : "false" )
	     << ")";
}

void ObjectRecoveryProgress::dump(Formatter *f) const
{
  f->dump_int("first?", first);
  f->dump_int("data_complete?", data_complete);
  f->dump_unsigned("data_recovered_to", data_recovered_to);
  f->dump_int("omap_complete?", omap_complete);
  f->dump_string("omap_recovered_to", omap_recovered_to);
}

void ObjectRecoveryInfo::encode(bufferlist &bl) const
{
  ENCODE_START(2, 1, bl);
  ::encode(soid, bl);
  ::encode(version, bl);
  ::encode(size, bl);
  ::encode(oi, bl);
  ::encode(ss, bl);
  ::encode(copy_subset, bl);
  ::encode(clone_subset, bl);
  ENCODE_FINISH(bl);
}

void ObjectRecoveryInfo::decode(bufferlist::iterator &bl,
				int64_t pool)
{
  DECODE_START(2, bl);
  ::decode(soid, bl);
  ::decode(version, bl);
  ::decode(size, bl);
  ::decode(oi, bl);
  ::decode(ss, bl);
  ::decode(copy_subset, bl);
  ::decode(clone_subset, bl);
  DECODE_FINISH(bl);

  if (struct_v < 2) {
    if (soid.pool == -1)
      soid.pool = pool;
    map<hobject_t, interval_set<uint64_t> > tmp;
    tmp.swap(clone_subset);
    for (map<hobject_t, interval_set<uint64_t> >::iterator i = tmp.begin();
	 i != tmp.end();
	 ++i) {
      hobject_t first(i->first);
      if (first.pool == -1)
	first.pool = pool;
      clone_subset[first].swap(i->second);
    }
  }
}

void ObjectRecoveryInfo::generate_test_instances(
  list<ObjectRecoveryInfo*>& o)
{
  o.push_back(new ObjectRecoveryInfo);
  o.back()->soid = hobject_t(sobject_t("key", CEPH_NOSNAP));
  o.back()->version = eversion_t(0,0);
  o.back()->size = 100;
}


void ObjectRecoveryInfo::dump(Formatter *f) const
{
  f->dump_stream("object") << soid;
  f->dump_stream("at_version") << version;
  f->dump_stream("size") << size;
  {
    f->open_object_section("object_info");
    oi.dump(f);
    f->close_section();
  }
  {
    f->open_object_section("snapset");
    ss.dump(f);
    f->close_section();
  }
  f->dump_stream("copy_subset") << copy_subset;
  f->dump_stream("clone_subset") << clone_subset;
}

ostream& operator<<(ostream& out, const ObjectRecoveryInfo &inf)
{
  return inf.print(out);
}

ostream &ObjectRecoveryInfo::print(ostream &out) const
{
  return out << "ObjectRecoveryInfo("
	     << soid << "@" << version
	     << ", copy_subset: " << copy_subset
	     << ", clone_subset: " << clone_subset
	     << ")";
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
    case CEPH_OSD_OP_LIST_SNAPS:
      break;
    case CEPH_OSD_OP_TRUNCATE:
      out << " " << op.op.extent.offset;
      break;
    case CEPH_OSD_OP_MASKTRUNC:
    case CEPH_OSD_OP_TRIMTRUNC:
      out << " " << op.op.extent.truncate_seq << "@" << (int64_t)op.op.extent.truncate_size;
      break;
    case CEPH_OSD_OP_ROLLBACK:
      out << " " << snapid_t(op.op.snap.snapid);
      break;
    case CEPH_OSD_OP_WATCH:
      out << (op.op.watch.flag ? " add":" remove")
	  << " cookie " << op.op.watch.cookie << " ver " << op.op.watch.ver;
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
    }
  } else if (ceph_osd_op_type_multi(op.op.op)) {
    switch (op.op.op) {
    case CEPH_OSD_OP_CLONERANGE:
      out << " " << op.op.clonerange.offset << "~" << op.op.clonerange.length
	  << " from " << op.soid
	  << " offset " << op.op.clonerange.src_offset;
      break;
    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      out << " v" << op.op.watch.ver
	  << " of " << op.soid;
      break;
    case CEPH_OSD_OP_SRC_CMPXATTR:
      out << " " << op.soid;
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
      ::decode(ops[i].soid, datap);
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
      ::encode(ops[i].soid, out);
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
