// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#include <atomic>
#include <cstring>
#include <cstdio>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <dlfcn.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "osd/OSDMap.h"
#include "CohortVolume.h"
#include "osdc/Objecter.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "include/str_map.h"

using std::shared_ptr;
using std::to_string;
using std::min;
using std::unique_ptr;

const uint64_t CohortVolume::one_op = 4194304;

typedef void (*place_func)(void*, const uint8_t[16], size_t, const char*,
			   bool(*)(void*, int),
			   bool(*)(void*, int));

VolumeRef CohortVolFactory(bufferlist::iterator& bl, uint8_t v, vol_type t)
{
  CohortVolume *vol = new CohortVolume(t);
  vol->decode_payload(bl, v);
  return VolumeRef(vol);
}

int CohortVolume::update(const shared_ptr<const Volume>& v)
{
  return 0;
}

void CohortVolume::init(OSDMap *map) const
{
  inited = map->find_by_uuid(placer_id, placer);
}

void CohortVolume::dump(Formatter *f) const
{
  inherited::dump(f);
}

void CohortVolume::decode_payload(bufferlist::iterator& bl, uint8_t v)
{
  inherited::decode_payload(bl, v);
}

void CohortVolume::encode(bufferlist& bl) const
{
  inherited::encode(bl);
}

VolumeRef CohortVolume::create(CephContext *cct,
			       const string& name,
			       PlacerRef placer,
			       std::stringstream& ss)

{
  CohortVolume *v = new CohortVolume(CohortVol);

  if (!valid_name(name, ss)) {
    goto error;
  }

  v->id = boost::uuids::random_generator()();
  v->name = name;
  v->placer = placer;
  if (!v->placer)
    goto error;

  return VolumeRef(v);

error:

  delete v;
  return VolumeRef();
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

CohortVolume::StripulatedOp::StripulatedOp(
  const Placer& pl) : pl(pl), logical_operations(0)

{
  ops.resize(pl.get_chunk_count());
}

void CohortVolume::StripulatedOp::add_op(const int op)
{
  ++logical_operations;
  for (auto &v : ops)
    v.emplace_back(op);
}

void CohortVolume::StripulatedOp::add_version(const uint64_t ver)
{
  for (auto &v : ops)
    v.back().op.assert_ver.ver = ver;
}

void CohortVolume::StripulatedOp::add_obj(const oid_t &o)
{
  for (auto &v : ops)
    v.back().oid = o;
}

void CohortVolume::StripulatedOp::add_single_return(bufferlist* bl,
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

void CohortVolume::StripulatedOp::add_single_return(
  std::function<void(int, bufferlist&&)>&& f)
{
  C_StupidStupid* c  = new C_StupidStupid(std::move(f));
  ops[0].back().ctx = c;
  ops[0].back().out_bl = &c->bl;
  ops[0].back().out_rval = nullptr;
}

void CohortVolume::StripulatedOp::add_metadata(const bufferlist& bl)
{
  budget += bl.length() * ops.size();
  for (auto &v : ops) {
    v.back().indata.append(bl);
  }
}

void CohortVolume::StripulatedOp::add_metadata_range(const uint64_t off,
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

void CohortVolume::StripulatedOp::add_data(const uint64_t off,
						   const bufferlist& in)
{
  bufferlist bl(in);
  vector<Placer::StrideExtent> out(ops.size());
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

void CohortVolume::StripulatedOp::add_data_range(const uint64_t off,
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

void CohortVolume::StripulatedOp::add_truncate(const uint64_t truncate_size,
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
  const Placer& pl;
  uint64_t off;
  map<int, bufferlist> resultbl;
  bufferlist *outbl;
  int* rval;
  Context *onfinish;
  uint64_t total_real_length;
  std::function<void(int, bufferlist&&)> f;

  C_MultiRead(const Placer& pl, uint64_t _off, bufferlist *_outbl, int* rval,
	      Context *onfinish)
    : pl(pl), off(_off), outbl(_outbl), rval(rval), onfinish(onfinish),
      total_real_length(UINT64_MAX) { }

  C_MultiRead(const Placer& pl, uint64_t _off,
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

void CohortVolume::StripulatedOp::read(uint64_t off, uint64_t len,
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

void CohortVolume::StripulatedOp::read(
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

void CohortVolume::StripulatedOp::read_full(bufferlist *bl,
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

void CohortVolume::StripulatedOp::read_full(
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

void CohortVolume::StripulatedOp::add_sparse_read_ctx(
  uint64_t off, uint64_t len, std::map<uint64_t,uint64_t> *m,
  bufferlist *data_bl, int *rval, Context *ctx)
{
  puts("Sparse read is currently not supported.");
  abort();
}


void CohortVolume::StripulatedOp::add_xattr(const string &name,
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

void CohortVolume::StripulatedOp::add_xattr(const string &name,
					    bufferlist* data)
{
  // At least for the moment, just read the same attribute on every stride
  budget += name.length();
  OSDOp &op = ops[0].back();
  op.op.xattr.name_len = name.length();
  op.indata.append(name);
  op.out_bl = data;
}

void CohortVolume::StripulatedOp::add_xattr_cmp(const string &name,
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

void CohortVolume::StripulatedOp::add_call(const string &cname,
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
void CohortVolume::StripulatedOp::add_call(const string &cname,
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

void CohortVolume::StripulatedOp::add_watch(const uint64_t cookie,
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

void CohortVolume::StripulatedOp::add_alloc_hint(
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

void CohortVolume::StripulatedOp::set_op_flags(const uint32_t flags)
{
  for (auto &v : ops) {
    OSDOp &osd_op = v.back();
    osd_op.op.flags = osd_op.op.flags | flags;
  }
}

void CohortVolume::StripulatedOp::clear_op_flags(const uint32_t flags)
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

  C_MultiStat(const Placer &pl, uint64_t *_s, ceph::real_time *_m,
	      int *_rval, Context *_ctx)
    : bls(pl.get_chunk_count()), s(_s), m(_m),
      rval(_rval), ctx(_ctx) { }
  C_MultiStat(const Placer &pl, uint64_t *_s, ceph::real_time *_m,
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
      } catch (ceph::buffer::error& e) {
	if (r != 0)
	  r = -EDOM;
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

void CohortVolume::StripulatedOp::add_stat_ctx(uint64_t *s,
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

void CohortVolume::StripulatedOp::add_stat_cb(
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

rados::ObjectOperation CohortVolume::StripulatedOp::clone()
{
  return rados::ObjectOperation(new StripulatedOp(*this));
}


rados::ObjectOperation CohortVolume::op() const
{
  assert(placer->is_attached());
  return rados::ObjectOperation(new StripulatedOp(*placer));
}

void CohortVolume::StripulatedOp::realize(
  const oid_t& o,
  const std::function<void(oid_t&&, vector<OSDOp>&&)>& f)
{
  for(size_t i = 0; i < ops.size(); ++i) {
    f(oid_t(o, i < pl.get_data_chunk_count() ? chunktype::data : chunktype::ecc,
	  i),
      std::move(ops[i]));
  }
}
