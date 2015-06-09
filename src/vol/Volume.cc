// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012, CohortFS, LLC <info@cohortfs.com> All rights
 * reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 *
 */

#include "Volume.h"
#include "osd/OSDMap.h"
#include <sstream>

#ifdef USING_UNICODE
#include <unicode/uchar.h>
#define L_IS_WHITESPACE(c) (u_isUWhiteSpace(c))
#define L_IS_PRINTABLE(c) (u_hasBinaryProperty((c), UCHAR_POSIX_PRINT))
#else
#include <ctype.h>
#define L_IS_WHITESPACE(c) (isspace(c))
#define L_IS_PRINTABLE(c) (isprint(c))
#endif

#include "common/MultiCallback.h"

using namespace std::literals;
using std::stringstream;
using std::system_error;
using ceph::buffer_errc;

const char* vol_category_t::name() const noexcept {
  return "volume";
}

std::string vol_category_t::message(int ev) const {
  switch (static_cast<vol_errc>(ev)) {
  case vol_errc::no_such_volume:
    return "no such volume"s;
  case vol_errc::invalid_name:
    return "invalid volume name"s;
  case vol_errc::exists:
    return "volume with name or UUID already exists"s;
  default:
    return "unknown error"s;
  }
}

const std::error_category& vol_category() {
  static vol_category_t instance;
  return instance;
}

void Volume::dump(Formatter *f) const
{
  f->dump_stream("uuid") << id;
  f->dump_stream("name") << name;
  f->dump_stream("placer") << placer_id;
}

void Volume::encode(bufferlist& bl) const
{
  int version = 0;
  ::encode(version, bl);
  ::encode(id, bl);
  ::encode(name, bl);
  ::encode(placer_id, bl);
}

void Volume::validate_name(const string &name)
{
  if (name.empty())
    throw std::system_error(vol_errc::invalid_name,
			   "volume name may not be empty"s);

  if (L_IS_WHITESPACE(*name.begin()))
    throw std::system_error(
      vol_errc::invalid_name,
      "volume name may not begin with space characters"s);

  if (L_IS_WHITESPACE(*name.rbegin()))
    throw std::system_error(vol_errc::invalid_name,
			    "volume name may not end with space characters"s);

  if (std::any_of(name.cbegin(), name.cend(),
		  [](char c) { return !L_IS_PRINTABLE(c); }))
    throw std::system_error(
      vol_errc::invalid_name,
      "volume name can only contain printable characters"s);


  try {
    boost::uuids::string_generator parse;
    parse(name);
    throw std::system_error(vol_errc::invalid_name,
			    "volume name cannot match the form of UUIDs"s);
  } catch (std::runtime_error& e) {
  }
}

void Volume::valid() const
{
  validate_name(name);

  if (id.is_nil())
    throw std::system_error(
      std::make_error_code(std::errc::invalid_argument),
      "UUID cannot be zero."s);
}

void Volume::decode(bufferlist::iterator& bl)
{
  int v;
  ::decode(v, bl);
  if (v != 0) {
    throw system_error(buffer_errc::malformed_input, "Bad version."s);
  }

  ::decode(id, bl);
  ::decode(name, bl);
  ::decode(placer_id, bl);
}

AVolRef Volume::attach(CephContext *cct, const OSDMap& o) const {
  return AVolRef(new AttachedVol(cct, o, *this));
}

/* AttachedVol */

const uint64_t AttachedVol::one_op = 4194304;

AttachedVol::AttachedVol(CephContext *cct, const OSDMap& o, const Volume& _vol)
  : v(_vol)
{
  placer = o.lookup_placer(v.placer_id)->attach(cct);
}

struct C_GetAttrs : public Context {
  bufferlist bl;
  map<string,bufferlist>& attrset;
  Context *fin;
  C_GetAttrs(map<string, bufferlist>& set, Context *c)
    : attrset(set), fin(c) {}
  void finish(int r) {
    if (r >= 0) {
      bufferlist::iterator p = bl.begin();
      ::decode(attrset, p);
    }
    fin->complete(r);
  }
};

AttachedVol::StripulatedOp::StripulatedOp(
  const AttachedPlacer& pl) : pl(pl), logical_operations(0)

{
  ops.resize(pl.get_chunk_count());
}

void AttachedVol::StripulatedOp::add_op(const int op)
{
  ++logical_operations;
  for (auto &v : ops)
    v.emplace_back(op);
}

void AttachedVol::StripulatedOp::add_version(const uint64_t ver)
{
  for (auto &v : ops)
    v.back().op.assert_ver.ver = ver;
}

void AttachedVol::StripulatedOp::add_obj(const oid_t &o)
{
  for (auto &v : ops)
    v.back().oid = o;
}

void AttachedVol::StripulatedOp::add_single_return(OSDOp::opfun_t&& f)
{
  ops[0].back().f = f;
}

void AttachedVol::StripulatedOp::add_metadata(const bufferlist& bl)
{
  budget += bl.length() * ops.size();
  for (auto &v : ops) {
    v.back().indata.append(bl);
  }
}

void AttachedVol::StripulatedOp::add_metadata_range(const uint64_t off,
						     const uint64_t len)
{
  uint64_t total_real_length = off + len;
  budget += total_real_length;
  for (auto &o : ops) {
    o.back().op.extent.offset = off;
    o.back().op.extent.length = len;
    o.back().op.extent.total_real_length = total_real_length;
  }
}

void AttachedVol::StripulatedOp::add_data(const uint64_t off,
					  const bufferlist& in)
{
  bufferlist bl(in);
  vector<AttachedPlacer::StrideExtent> out(ops.size());
  uint64_t total_real_length = off + bl.length();
  pl.add_data(off, bl, out);
  budget += bl.length();

  for (auto it = out.begin(); it != out.end(); it++) {
    int opi = it - out.begin();
    ops[opi].back().indata.claim_append(it->bl);
    ops[opi].back().op.extent.offset = it->offset;
    ops[opi].back().op.extent.length = it->length;
    ops[opi].back().op.extent.total_real_length = total_real_length;
  }
}

void AttachedVol::StripulatedOp::add_data_range(const uint64_t off,
						    const uint64_t len)
{
  const uint32_t stripe_size = pl.get_stripe_unit() *
    pl.get_data_chunk_count();
  uint64_t actual_len = len;
  uint64_t total_real_length = off + len;
  uint64_t resid = (off + len) % stripe_size;
  if (resid)
    actual_len += (stripe_size - resid);

  for (auto &o : ops) {
    o.back().op.extent.offset = off / pl.get_data_chunk_count();
    o.back().op.extent.length = actual_len
      / pl.get_data_chunk_count();
    o.back().op.extent.total_real_length = total_real_length;
  }
}

void AttachedVol::StripulatedOp::add_truncate(const uint64_t truncate_size,
					      const uint32_t truncate_seq)
{
  // The truncate_size is the truncate_size of the entire OBJECT, not
  // of any individual stride. This seems the only sane way to go
  // about things when we have padding to length and the like.
  for (auto &v : ops) {
    v.back().op.extent.truncate_size = truncate_size;
    v.back().op.extent.truncate_seq = truncate_seq;
  }
}

class MultiRead
  : public cohort::ComplicatedMultiCallback<void(std::error_code,
						 bufferlist&), uint32_t> {
  friend cohort::MultiCallback;
  const AttachedPlacer& pl;
  uint64_t off;
  map<int, bufferlist> resultbl;
  rados::read_callback f;
  int r;

  MultiRead(const AttachedPlacer& pl, uint64_t _off,
	    rados::read_callback&& _f)
    : pl(pl), off(_off), f(_f), r(0), total_real_length(UINT64_MAX) { }

public:
  uint64_t total_real_length;

  virtual void work(uint32_t stride, std::error_code r, bufferlist& bl) {
    if (stride < UINT32_MAX)
      resultbl[stride].claim_append(bl);
  };

  void finish() {
    bufferlist bl;
    int s = pl.get_data(resultbl, &bl);
    if (s != 0) {
      r = s;
    }

    if (r < 0) {
      f(std::error_code(-r, std::generic_category()), bl);
    } else {
      if ((total_real_length < UINT64_MAX) &&
	  (off + bl.length()) > total_real_length) {
	bufferlist tbl;
	tbl.substr_of(bl, 0, total_real_length - off);
	f(std::error_code(), tbl);
      } else {
	f(std::error_code(), bl);
      }
    }
  }
};

void AttachedVol::StripulatedOp::read(
  uint64_t off, uint64_t len, uint64_t truncate_size,
  uint32_t truncate_seq, rados::read_callback&& f)
{
  budget += len;
  const uint32_t stripe_size = pl.get_stripe_unit() *
    pl.get_data_chunk_count();
  uint64_t actual_len = len;
  uint64_t resid = (off + len) % stripe_size;
  if (resid)
    actual_len += (stripe_size - resid);
  auto& mr = cohort::MultiCallback::create<MultiRead>(pl, off, std::move(f));

  add_op(CEPH_OSD_OP_STAT);
  add_stat_cb([ &rtl = mr.total_real_length,
		sub = mr.subsidiary(UINT32_MAX) ]
	      (std::error_code e, uint64_t s, ceph::real_time m) mutable {
		rtl = s;
		bufferlist bl;
		sub(e, bl); });

  add_op(CEPH_OSD_OP_READ);
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    ops[stride].back().op.extent.offset = off
      / pl.get_data_chunk_count();
    ops[stride].back().op.extent.length = actual_len
      / pl.get_data_chunk_count();
    ops[stride].back().f = std::move(mr.subsidiary(stride));
  }
  mr.activate();
}

void AttachedVol::StripulatedOp::read_full(
  rados::read_callback&& f)
{
  read(0, CEPH_READ_ENTIRE, 0, 0, std::move(f));
}

void AttachedVol::StripulatedOp::add_xattr(const string &name,
					    const bufferlist& data)
{
  // At least for the moment, just send the same attribute on every stride
  budget += (name.length() + data.length()) * ops.size();
  for (auto &v : ops) {
    OSDOp &op = v.back();
    op.op.xattr.name_len = name.length();
    op.op.xattr.value_len = data.length();
    op.indata.append(name);
    op.indata.append(data);
  }
}

void AttachedVol::StripulatedOp::add_xattr(const string &name)
{
  // At least for the moment, just read the same attribute on every stride
  budget += name.length();
  OSDOp &op = ops[0].back();
  op.op.xattr.name_len = name.length();
  op.indata.append(name);
}

void AttachedVol::StripulatedOp::add_xattr_cmp(const string &name,
						const uint8_t cmp_op,
						const uint8_t cmp_mode,
						const bufferlist& data)
{
  // Only do anything on the first For client-side recovery we
  // probably want to do it on ALL and just have the completion find
  // the first success and use it.
  budget += (name.length() + data.length());
  OSDOp &op = ops[0].back();
  op.op.xattr.name_len = name.length();
  op.op.xattr.value_len = data.length();
  op.op.xattr.cmp_op = cmp_op;
  op.op.xattr.cmp_mode = cmp_mode;
  op.indata.append(name);
  op.indata.append(data);

  // Do nothing in all the other ops.
  for (auto i = (ops.begin() + 1); i != ops.end(); ++i)
    i->back().op.op = 0;
}

void AttachedVol::StripulatedOp::add_call(const string &cname,
					  const string &method,
					  const bufferlist &indata,
					  rados::read_callback&& cb)
{
  bool das_macht_nichts = false;
  // Calls are hard.
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.cls.class_len = cname.length();
    osd_op.op.cls.method_len = method.length();
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname.data(), osd_op.op.cls.class_len);
    osd_op.indata.append(method.data(), osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
    if (das_macht_nichts) {
      osd_op.f = nullptr;
    } else {
      osd_op.f = std::move(cb);
      das_macht_nichts = true;
    }
  }
}

void AttachedVol::StripulatedOp::add_watch(const uint64_t cookie,
					   const uint64_t ver,
					   const uint8_t flag,
					   const bufferlist& inbl)
{
  // Watches might be hard
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.watch.cookie = cookie;
    osd_op.op.watch.ver = ver;
    osd_op.op.watch.flag = flag;
    osd_op.indata.append(inbl);
  }
}

void AttachedVol::StripulatedOp::add_alloc_hint(
  const uint64_t expected_object_size, const uint64_t expected_write_size)
{
  const uint32_t stripe_size = pl.get_stripe_unit() *
    pl.get_data_chunk_count();
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.alloc_hint.expected_object_size
      = expected_object_size / stripe_size;
    osd_op.op.alloc_hint.expected_write_size
      = expected_write_size / stripe_size;
  }
}

void AttachedVol::StripulatedOp::set_op_flags(const uint32_t flags)
{
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.flags = osd_op.op.flags | flags;
  }
}

void AttachedVol::StripulatedOp::clear_op_flags(const uint32_t flags)
{
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.flags = osd_op.op.flags & ~flags;
  }
}

class MultiStat : public cohort::SimpleMultiCallback<
  std::error_code, bufferlist&> {
  friend cohort::MultiCallback;

  bool got_one;
  uint64_t size;
  std::error_code e;
  ceph::real_time mtime;
  rados::stat_callback f;

  MultiStat(const AttachedPlacer &pl, rados::stat_callback&& _f)
    : got_one(false), size(0), f(std::move(_f)) { }

public:

  void work(std::error_code err, bufferlist& bl) {
    if (err && !e) {
      e = err;
      return;
    }

    bufferlist::iterator p = bl.begin();
    try {
      uint64_t _size;
      ceph::real_time _mtime;
      ::decode(_size, p);
      ::decode(_mtime, p);
      ::decode(size, p);
      got_one = true;
      mtime = std::max(mtime, _mtime);
    } catch (std::system_error& se) {
      if (!e)
	e = se.code();
    }
  }

  void finish() {
    if (got_one) {
      e.clear();
    }

    f(e, size, mtime);
  }
};

void AttachedVol::StripulatedOp::add_stat_cb(rados::stat_callback&& cb)
{
  auto& ms =  cohort::MultiCallback::create<MultiStat>(pl, std::move(cb));
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    OSDOp &osd_op = ops[stride].back();
    osd_op.f = ms.subsidiary();
  }
  ms.activate();
}

rados::ObjectOperation AttachedVol::StripulatedOp::clone()
{
  return rados::ObjectOperation(new StripulatedOp(*this));
}


rados::ObjectOperation AttachedVol::op() const
{
  return rados::ObjectOperation(new StripulatedOp(*placer));
}

void AttachedVol::StripulatedOp::realize(
  const oid_t& o,
  const cohort::function<void(oid_t&&, vector<OSDOp>&&)>& f)
{
  for(size_t i = 0; i < ops.size(); ++i) {
    f(oid_t(o, i < pl.get_data_chunk_count() ? chunktype::data : chunktype::ecc,
	  i),
      std::move(ops[i]));
  }
}

size_t AttachedVol::place(const oid_t& object,
			  const OSDMap& map,
			  const cohort::function<void(int)>& f) const {
  return placer->place(object, v.id, map, f);
}

int AttachedVol::get_cohort_placer(struct cohort_placer *p) const {
  int r = placer->get_cohort_placer(p);
  if (r < 0)
    return r;
  memcpy(p->volume_id, v.id.data, sizeof(p->volume_id));
  return 0;
}
