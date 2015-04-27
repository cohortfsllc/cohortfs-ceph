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

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
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

using namespace std::literals;
using std::stringstream;
using std::system_error;
using ceph::buffer_err;

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

bool Volume::valid_name(const string &name, std::stringstream &ss)
{
  if (name.empty()) {
    ss << "volume name may not be empty";
    return false;
  }

  if (L_IS_WHITESPACE(*name.begin())) {
    ss << "volume name may not begin with space characters";
    return false;
  }

  if (L_IS_WHITESPACE(*name.rbegin())) {
    ss << "volume name may not end with space characters";
    return false;
  }

  for (string::const_iterator c = name.begin(); c != name.end(); ++c) {
    if (!L_IS_PRINTABLE(*c)) {
      ss << "volume name can only contain printable characters";
      return false;
    }
  }


  try {
    boost::uuids::string_generator parse;
    parse(name);
    ss << "volume name cannot match the form of UUIDs";
    return false;
  } catch (std::runtime_error& e) {
    return true;
  }

  return true;
}

bool Volume::valid(std::stringstream& ss) const
{
  if (!valid_name(name, ss)) {
    return false;
  }

  if (id.is_nil()) {
    ss << "UUID cannot be zero.";
    return false;
  }

  return true;
}

VolumeRef Volume::decode_volume(bufferlist::iterator& bl)
{
  int v;
  std::shared_ptr<Volume> vol(new Volume);

  ::decode(v, bl);
  if (v != 0) {
    throw system_error(buffer_err::malformed_input, "Bad version."s);
  }

  ::decode(vol->id, bl);
  ::decode(vol->name, bl);
  ::decode(vol->placer_id, bl);

  return std::const_pointer_cast<const Volume>(vol);
}

AVolRef Volume::attach(CephContext *cct, const OSDMap& o) const {
  return AVolRef(new AttachedVol(cct, o, shared_from_this()));
}

VolumeRef Volume::create(CephContext *cct,
			 const string& name,
			 boost::uuids::uuid placer_id,
			 std::stringstream& ss)
{
  if (!valid_name(name, ss)) {
    return VolumeRef();
  }

  // The caller should ensure that the placer_id is a valid
  // placer_id. Either that or pass us the OSDMap.

  std::shared_ptr<Volume> vol(new Volume(name,
					 boost::uuids::random_generator()(),
					 placer_id));
  return vol;
}

/* AttachedVol */

const uint64_t AttachedVol::one_op = 4194304;

AttachedVol::AttachedVol(CephContext *cct, const OSDMap& o, VolumeRef _vol)
  : vol(_vol)
{
  PlacerRef p;
  // Come back later and add a check and exception, when we get some
  // Cohort-specific exception codes.
  o.find_by_uuid(vol->placer_id, p);
  placer = p->attach(cct);
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

void AttachedVol::StripulatedOp::add_single_return(bufferlist* bl,
						    int* rval,
						    Context *ctx)
{
  ops[0].back().ctx = ctx;
  ops[0].back().out_bl = bl;
  ops[0].back().out_rval = rval;
}

// This will go away in a few days
struct C_StupidStupid : public Context {
  std::function<void(int, bufferlist&&)> f;
  bufferlist bl;

  C_StupidStupid(std::function<void(int, bufferlist&&)>&& _f) {
    f.swap(_f);
  }
  void finish(int r) {
    f(r, std::move(bl));
  }
};

void AttachedVol::StripulatedOp::add_single_return(
  std::function<void(int, bufferlist&&)>&& f)
{
  C_StupidStupid* c  = new C_StupidStupid(std::move(f));
  ops[0].back().ctx = c;
  ops[0].back().out_bl = &c->bl;
  ops[0].back().out_rval = nullptr;
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

struct C_MultiRead : public Context {
  const AttachedPlacer& pl;
  uint64_t off;
  map<int, bufferlist> resultbl;
  bufferlist *outbl;
  int* rval;
  Context *onfinish;
  uint64_t total_real_length;
  std::function<void(int, bufferlist&&)> f;

  C_MultiRead(const AttachedPlacer& pl, uint64_t _off, bufferlist *_outbl,
	      int* rval, Context *onfinish)
    : pl(pl), off(_off), outbl(_outbl), rval(rval), onfinish(onfinish),
      total_real_length(UINT64_MAX) { }

  C_MultiRead(const AttachedPlacer& pl, uint64_t _off,
	      std::function<void(int, bufferlist&&)>&& _f)
    : pl(pl), off(_off), outbl(nullptr), rval(nullptr), onfinish(nullptr),
      total_real_length(UINT64_MAX), f(_f) { }

  void finish(int r) {
    bufferlist bl;
    int s = pl.get_data(resultbl, &bl);
    if (s != 0) {
      r = s;
      goto done;
    }

    if (outbl) {
      if ((total_real_length < UINT64_MAX) &&
	  (off + bl.length()) > total_real_length) {
	outbl->substr_of(bl, 0, total_real_length - off);
      } else {
	outbl->claim_append(bl);
      }
      r = outbl->length();
    }


  done:

    if (rval) {
      *rval = outbl->length();
    }

    if (onfinish) {
      onfinish->complete(outbl->length());
    }

    if (f) {
      if (r < 0) {
	f(r, bufferlist());
      } else {
	bufferlist tbl;
	if ((total_real_length < UINT64_MAX) &&
	    (off + bl.length()) > total_real_length) {
	  tbl.substr_of(bl, 0, total_real_length - off);
	} else {
	  tbl.claim_append(bl);
	}
	f(tbl.length(), std::move(tbl));
      }
    }
  }
};

void AttachedVol::StripulatedOp::read(uint64_t off, uint64_t len,
				       bufferlist *bl, uint64_t truncate_size,
				       uint32_t truncate_seq, int *rval,
				       Context* ctx)
{
  budget += len;
  const uint32_t stripe_size = pl.get_stripe_unit() *
    pl.get_data_chunk_count();
  uint64_t actual_len = len;
  uint64_t resid = (off + len) % stripe_size;
  if (resid)
    actual_len += (stripe_size - resid);
  C_MultiRead* mr = new C_MultiRead(pl, off, bl, rval, ctx);
  C_GatherBuilder gather;

  add_op(CEPH_OSD_OP_STAT);
  add_stat_ctx(&mr->total_real_length, nullptr, nullptr, gather.new_sub());
  add_op(CEPH_OSD_OP_READ);
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    ops[stride].back().op.extent.offset = off
      / pl.get_data_chunk_count();
    ops[stride].back().op.extent.length = actual_len
      / pl.get_data_chunk_count();
    ops[stride].back().ctx = gather.new_sub();
    auto p = mr->resultbl.emplace(stride, len / stripe_size);
    ops[stride].back().out_bl = &(p.first->second);
  }
  gather.set_finisher(mr);
  gather.activate();
}

void AttachedVol::StripulatedOp::read(
  uint64_t off, uint64_t len, uint64_t truncate_size,
  uint32_t truncate_seq, std::function<void(int, bufferlist&&)>&& f)
{
  budget += len;
  const uint32_t stripe_size = pl.get_stripe_unit() *
    pl.get_data_chunk_count();
  uint64_t actual_len = len;
  uint64_t resid = (off + len) % stripe_size;
  if (resid)
    actual_len += (stripe_size - resid);
  C_MultiRead* mr = new C_MultiRead(pl, off, std::move(f));
  C_GatherBuilder gather;

  add_op(CEPH_OSD_OP_STAT);
  add_stat_ctx(&mr->total_real_length, nullptr, nullptr, gather.new_sub());
  add_op(CEPH_OSD_OP_READ);
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    ops[stride].back().op.extent.offset = off
      / pl.get_data_chunk_count();
    ops[stride].back().op.extent.length = actual_len
      / pl.get_data_chunk_count();
    ops[stride].back().ctx = gather.new_sub();
    auto p = mr->resultbl.emplace(stride, len / stripe_size);
    ops[stride].back().out_bl = &(p.first->second);
  }
  gather.set_finisher(mr);
  gather.activate();
}

void AttachedVol::StripulatedOp::read_full(bufferlist *bl,
		      int *rval, Context* ctx)
{
  C_MultiRead* mr = new C_MultiRead(pl, 0, bl, rval, ctx);
  C_GatherBuilder gather;

  add_op(CEPH_OSD_OP_STAT);
  add_stat_ctx(&mr->total_real_length, nullptr, nullptr, gather.new_sub());
  add_op(CEPH_OSD_OP_READ);
  ops[0].back().op.extent.offset = 0;
  ops[0].back().op.extent.length = CEPH_READ_ENTIRE;
  ops[0].back().ctx = gather.new_sub();
  ops[0].back().out_bl = bl;
  gather.set_finisher(mr);
  gather.activate();
}

void AttachedVol::StripulatedOp::read_full(
  std::function<void(int, bufferlist&&)>&& f)
{
  C_MultiRead* mr = new C_MultiRead(pl, 0, std::move(f));
  C_GatherBuilder gather;

  add_op(CEPH_OSD_OP_STAT);
  add_stat_ctx(&mr->total_real_length, nullptr, nullptr, gather.new_sub());
  add_op(CEPH_OSD_OP_READ);
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    ops[stride].back().op.extent.offset = 0;
    ops[stride].back().op.extent.length = CEPH_READ_ENTIRE;
    ops[stride].back().ctx = gather.new_sub();
    auto p = mr->resultbl.emplace(stride, 0);
    ops[stride].back().out_bl = &(p.first->second);
  }
  gather.set_finisher(mr);
  gather.activate();
}

void AttachedVol::StripulatedOp::add_sparse_read_ctx(
  uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
  bufferlist *data_bl, int *rval, Context *ctx)
{
  puts("Sparse read is currently not supported.");
  abort();
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

void AttachedVol::StripulatedOp::add_xattr(const string &name,
					    bufferlist* data)
{
  // At least for the moment, just read the same attribute on every stride
  budget += name.length();
  OSDOp &op = ops[0].back();
  op.op.xattr.name_len = name.length();
  op.indata.append(name);
  op.out_bl = data;
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
					   bufferlist *const outbl,
					   Context *const ctx,
					   int *const rval)
{
  // Calls are hard.
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.cls.class_len = cname.length();
    osd_op.op.cls.method_len = method.length();
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname.data(), osd_op.op.cls.class_len);
    osd_op.indata.append(method.data(), osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
    osd_op.ctx = ctx;
    osd_op.out_bl = outbl;
    osd_op.out_rval = rval;
  }
}
void AttachedVol::StripulatedOp::add_call(const string &cname,
					   const string &method,
					   const bufferlist &indata,
					   std::function<void(
					     int,bufferlist&&)>&& cb)
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
      osd_op.ctx = nullptr;
      osd_op.out_bl = nullptr;
      osd_op.out_rval = nullptr;
    } else {
      C_StupidStupid* c = new C_StupidStupid(std::move(cb));
      osd_op.ctx = c;
      osd_op.out_bl = &c->bl;
      osd_op.out_rval = nullptr;
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

struct C_MultiStat : public Context {
  vector<bufferlist> bls;
  uint64_t *s;
  ceph::real_time *m;
  int *rval;
  Context *ctx;
  // Hack
  std::function<void(int, uint64_t, ceph::real_time)> f;

  C_MultiStat(const AttachedPlacer &pl, uint64_t *_s, ceph::real_time *_m,
	      int *_rval, Context *_ctx)
    : bls(pl.get_chunk_count()), s(_s), m(_m),
      rval(_rval), ctx(_ctx) { }
  C_MultiStat(const AttachedPlacer &pl, uint64_t *_s, ceph::real_time *_m,
	      int *_rval, std::function<void(
		int, uint64_t, ceph::real_time)>&& _f)
    : bls(pl.get_chunk_count()), s(_s), m(_m),
      rval(_rval), ctx(0), f(std::move(_f)) { }

  void finish(int r) {
    bool got_one = false;
    uint64_t rtl = 0;
    ceph::real_time mtime;
    for (auto &b : bls) {
      bufferlist::iterator p = b.begin();
      try {
	uint64_t _size;
	ceph::real_time _mtime;
	::decode(_size, p);
	::decode(_mtime, p);
	::decode(rtl, p);
	got_one = true;
	mtime = std::max(mtime, _mtime);
      } catch (std::system_error& e) {
	if (r != 0)
	  r = -EINVAL;
      }
    }

    if (got_one) {
      if (s) {
	*s = rtl;
      }
      if (m)
	*m = mtime;
      r = 0;
    }

    if (ctx)
      ctx->complete(r);

    if (f)
      f(r, rtl, mtime);

    if (rval)
      *rval = r;
  }
};

void AttachedVol::StripulatedOp::add_stat_ctx(uint64_t *s,
					       ceph::real_time *m,
					       int *rval,
					       Context *ctx)
{
  C_GatherBuilder gather;
  C_MultiStat *f = new C_MultiStat(pl, s, m, rval, ctx);
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    OSDOp &osd_op = ops[stride].back();
    osd_op.out_bl = &(f->bls[stride]);
    osd_op.ctx = gather.new_sub();
  }
  gather.set_finisher(f);
  gather.activate();
}

void AttachedVol::StripulatedOp::add_stat_cb(
  std::function<void(int, uint64_t, ceph::real_time)>&& cb)
{
  C_GatherBuilder gather;
  C_MultiStat *f = new C_MultiStat(pl, nullptr, nullptr, nullptr,
				   std::move(cb));
  for (uint32_t stride = 0; stride < pl.get_chunk_count(); ++stride) {
    OSDOp &osd_op = ops[stride].back();
    osd_op.out_bl = &(f->bls[stride]);
    osd_op.ctx = gather.new_sub();
  }
  gather.set_finisher(f);
  gather.activate();
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
  const std::function<void(oid_t&&, vector<OSDOp>&&)>& f)
{
  for(size_t i = 0; i < ops.size(); ++i) {
    f(oid_t(o, i < pl.get_data_chunk_count() ? chunktype::data : chunktype::ecc,
	  i),
      std::move(ops[i]));
  }
}

size_t AttachedVol::place(const oid_t& object,
			  const OSDMap& map,
			  const std::function<void(int)>& f) const {
  return placer->place(object, vol->id, map, f);
}

int AttachedVol::get_cohort_placer(struct cohort_placer *p) const {
  int r = placer->get_cohort_placer(p);
  if (r < 0)
    return r;
  memcpy(p->volume_id, vol->id.data, sizeof(p->volume_id));
  return 0;
}
