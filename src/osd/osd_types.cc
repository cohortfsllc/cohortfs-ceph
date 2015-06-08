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
 * Foundation.	See file COPYING.
 *
 */

#include <boost/uuid/string_generator.hpp>
#include "osd_types.h"
#include "include/ceph_features.h"
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

// -- coll_t --

const coll_t coll_t::META_COLL("meta");

bool coll_t::is_vol(boost::uuids::uuid& vol) const
{
  boost::uuids::string_generator parse;

  try {
    vol = parse(str);
  } catch (std::runtime_error& e) {
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
  ::decode(str, bl);
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


// -- vol_info_t --

void vol_info_t::encode(bufferlist &bl) const
{
  ENCODE_START(30, 26, bl);
  ::encode(volume, bl);
  ::encode(last_update, bl);
  ::encode(last_epoch_started, bl);
  ::encode(last_user_version, bl);
  ENCODE_FINISH(bl);
}

void vol_info_t::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(29, 26, 26, bl);
  ::decode(volume, bl);
  ::decode(last_update, bl);
  ::decode(last_epoch_started, bl);
  ::decode(last_user_version, bl);
  DECODE_FINISH(bl);
}

// -- pg_info_t --

void vol_info_t::dump(Formatter *f) const
{
  f->dump_stream("volume") << volume;
  f->dump_stream("last_update") << last_update;
  f->dump_int("last_user_version", last_user_version);

  f->dump_int("empty", is_empty());
  f->dump_int("last_epoch_started", last_epoch_started);
}

void vol_info_t::generate_test_instances(list<vol_info_t*>& o)
{
  o.push_back(new vol_info_t());
  o.push_back(new vol_info_t());
  o.back()->last_update = eversion_t(3, 4);
  o.back()->last_user_version = 2;
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
  ::encode(timeout, bl);
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
  ::decode(timeout, bl);
  if (struct_v >= 4) {
    ::decode(addr, bl);
  }
  DECODE_FINISH(bl);
}

void watch_info_t::dump(Formatter *f) const
{
  f->dump_unsigned("cookie", cookie);
  f->dump_stream("timeout_seconds") << timeout;
  f->open_object_section("addr");
  addr.dump(f);
  f->close_section();
}

void watch_info_t::generate_test_instances(list<watch_info_t*>& o)
{
  o.push_back(new watch_info_t);
  o.push_back(new watch_info_t);
  o.back()->cookie = 123;
  o.back()->timeout = 99s;
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
  total_real_length = other.total_real_length;
  flags = other.flags;
  user_version = other.user_version;
}

void object_info_t::encode(bufferlist& bl) const
{
  ENCODE_START(13, 8, bl);
  ::encode(oid, bl);
  ::encode(version, bl);
  ::encode(prior_version, bl);
  ::encode(last_reqid, bl);
  ::encode(size, bl);
  ::encode(mtime, bl);
  ::encode(wrlock_by, bl);
  ::encode(truncate_seq, bl);
  ::encode(truncate_size, bl);
  ::encode(total_real_length, bl);
  ::encode(user_version, bl);
  ::encode(watchers, bl);
  uint32_t _flags = flags;
  ::encode(_flags, bl);
  ENCODE_FINISH(bl);
}

void object_info_t::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(13, 8, 8, bl);
  ::decode(oid, bl);
  ::decode(version, bl);
  ::decode(prior_version, bl);
  ::decode(last_reqid, bl);
  ::decode(size, bl);
  ::decode(mtime, bl);
  ::decode(wrlock_by, bl);
  ::decode(truncate_seq, bl);
  ::decode(truncate_size, bl);
  ::decode(total_real_length, bl);
  ::decode(user_version, bl);
  ::decode(watchers, bl);
  uint32_t _flags;
  ::decode(_flags, bl);
  flags = (flag_t)_flags;
  DECODE_FINISH(bl);
}

void object_info_t::dump(Formatter *f) const
{
  f->open_object_section("oid_t");
  oid.dump(f);
  f->close_section();
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
  f->dump_unsigned("total_real_length", total_real_length);
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
  out << oi.oid << "(" << oi.version
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
