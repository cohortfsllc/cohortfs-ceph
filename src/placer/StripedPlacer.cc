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
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <dlfcn.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "osd/OSDMap.h"
#include "StripedPlacer.h"
#include "include/str_map.h"

using std::shared_ptr;
using std::to_string;
using std::min;
using std::unique_ptr;

const uint64_t StripedPlacer::one_op = 4194304;

PlacerRef StripedPlacerFactory(bufferlist::iterator& bl, uint8_t v)
{
  StripedPlacer *placer = new StripedPlacer();
  placer->decode_payload(bl, v);
  return PlacerRef(placer);
}

/* Epoch should be the current epoch of the OSDMap. */


StripedPlacer::~StripedPlacer(void)
{
}

int StripedPlacer::update(const shared_ptr<const Placer>& pl)
{
  return 0;
}

uint32_t StripedPlacer::num_rules(void)
{
  return 1;
}

size_t StripedPlacer::place(const oid_t& object,
			    const boost::uuids::uuid& id,
			    const OSDMap& map,
			    const std::function<void(int)>& f) const
{
  ssize_t count = 0;
  for (uint32_t i = 0; i < stripe_width; i++) {
    if (map.is_in(i)) {
      f(i);
      count++;
    } else
      f(-1);
  }

  return count;
}

void StripedPlacer::dump(Formatter *f) const
{
  inherited::dump(f);
  f->dump_stream("stripe_unit") << stripe_unit;
  f->dump_stream("stripe_width") << stripe_width;
}

void StripedPlacer::decode_payload(bufferlist::iterator& bl, uint8_t v)
{
  inherited::decode_payload(bl, v);

  ::decode(stripe_unit, bl);
  ::decode(stripe_width, bl);
}

void StripedPlacer::encode(bufferlist& bl) const
{
  inherited::encode(bl);

  ::encode(stripe_unit, bl);
  ::encode(stripe_width, bl);
}

//static string indent(size_t k)
//{
//return string(k / 8, '\t') + string(k % 8, ' ');
//}

PlacerRef StripedPlacer::create(CephContext *cct,
				const string& name,
				const int64_t _stripe_unit,
				const int64_t _stripe_width,
				std::stringstream& ss)
{
  StripedPlacer *v = new StripedPlacer();
  std::stringstream es;

  if (!valid_name(name, ss)) {
    goto error;
  }

  v->id = boost::uuids::random_generator()();
  v->name = name;
  v->stripe_unit = _stripe_unit;
  v->stripe_width = _stripe_width;

  return PlacerRef(v);

error:

  delete v;
  return PlacerRef();
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

/* This seems to work, but involves a *lot* of math and storage */
void StripedPlacer::stride_extent(const uint64_t off, const uint64_t len,
				  const size_t stride, uint64_t &strideoff,
				  uint64_t &stridelen) {
  size_t first = stride_idx(off);
  size_t span = extent_units(off, len);
  if ((len == 0) ||
      ((span < stripe_width) &&
       (stride > ((first + span - 1) % stripe_width)))) {
    // empty stride
    strideoff = 0;
    stridelen = 0;
    return;
  }

  uint64_t last_byte = off + len - 1;
  size_t last = stride_idx(last_byte);
  uint64_t stride_last_byte;

  if (first == stride) {
    strideoff = stride_offset(off);
  } else {
    uint32_t stride_num = (stride > first) ? stride - first :
      stripe_width - first + stride;
    uint32_t first_stride_off = off - (off % stripe_unit);
    uint64_t offset_into_object_of_beginning_of_stride = first_stride_off +
      stride_num * stripe_unit;

    strideoff = stride_offset(offset_into_object_of_beginning_of_stride);
  }

  if (len == 0) {
    stridelen = 0;
    return;
  }

  if (last == stride) {
    stride_last_byte = stride_offset(last_byte);
  } else {
    uint32_t stride_num = (stride < last) ? last - stride :
      stripe_width + last - stride;
    uint32_t last_stride_end = last_byte - (last_byte % stripe_unit) + stripe_unit - 1;
    uint64_t offset_into_object_of_end_of_stride = last_stride_end - stride_num * stripe_unit;
    stride_last_byte = stride_offset(offset_into_object_of_end_of_stride);
  }
  stridelen = stride_last_byte + 1 - strideoff;
}

void StripedPlacer::make_strides(const oid_t& oid,
				 uint64_t offset, uint64_t len,
				 uint64_t truncate_size, uint32_t truncate_seq,
				 vector<StrideExtent>& strides) const
{
#if 0
  buffer::list::iterator i(&blin);
  size_t stride;
  uint64_t thislen;
  len = min(len, (uint64_t)blin.length());

  for (stride = 0; stride < stripe_width; ++stride) {
    stride_extent(offset, len, stride, strides[stride].offset,
		  strides[stride].length);
    assert(strides[stride].length <= one_op);
  }

  stride = stride_idx(offset);
  /* Special case on incomplete first block */
  if (offset % stripe_unit != 0) {
    thislen = min(len, (uint64_t) (stripe_unit - offset % stripe_unit));
  } else {
    thislen = min(len, (uint64_t)stripe_unit);
  }
  i.copy(thislen, strides[stride].bl);
  stride = (stride + 1) % stripe_width;

  while (i.get_off() < len) {
    uint64_t thislen = min(i.get_remaining(), stripe_unit);
    i.copy(thislen, strides[stride].bl);
    stride = (stride + 1) % stripe_width;
  }
#endif
}

void StripedPlacer::repair(vector<StrideExtent>& extents,
			   const OSDMap& map) const
{
  return;
}

void StripedPlacer::serialize_data(bufferlist &bl) const
{
  return;
}

void StripedPlacer::serialize_code(bufferlist &bl) const
{
  return;
}

int StripedPlacer::encode(const set<int> &want_to_encode,
			  const bufferlist &in,
			  map<int, bufferlist> *encoded) const
{
  bufferlist out = in;
  size_t stridesize = stride_size(in.length());

  for (unsigned int i = 0; i < stripe_width; i++) {
    bufferlist &stride = (*encoded)[i];
    stride.substr_of(out, i * stridesize, stridesize);
  }
  return 0;
};

void StripedPlacer::add_data(const uint64_t off, bufferlist& in,
			      vector<StrideExtent>& strides) const
{
  uint32_t stride, len, num_strides;
  uint64_t last_byte = off + in.length() - 1;
  uint64_t curoff;
  bufferlist bl;

  /* Build the stride extant list */
  curoff = off;
  num_strides = 0;
  do {
    stride = stride_idx(curoff);
    strides[stride].offset = stride_offset(curoff);
    curoff += (stripe_unit - (curoff % stripe_unit));
    num_strides++;
  } while (num_strides < stripe_width && curoff < last_byte);

  curoff = last_byte;
  num_strides = 0;
  do {
    stride = stride_idx(curoff);
    strides[stride].length = stride_offset(curoff) + 1 - strides[stride].offset;
    if ((curoff + 1) % stripe_unit)
      // Point at end of last stride
      curoff += (stripe_unit - ((curoff + 1) % stripe_unit));
    if (curoff < stripe_unit)
      // Next stride is empty
      break;
    curoff -= stripe_unit;
    num_strides++;
  } while (num_strides < stripe_width && curoff > off);

  /* Build buffer lists for strides */
  curoff = off;
  stride = stride_idx(curoff);
  if (curoff % stripe_unit) {
    /* Partial first first */
    len = stripe_unit - (off % stripe_unit);
    if (len > last_byte + 1 - off) {
      // Entire write is within this unit
      len = last_byte + 1 - off;
    }
    bl.substr_of(in, curoff - off, len);
    strides[stride].bl.claim_append(bl);
    curoff += len;
  }
  while (curoff < last_byte + 1) {
    stride = stride_idx(curoff);
    if (last_byte + 1 - curoff < stripe_unit) {
      /* Partial last unit */
      len = last_byte + 1 - curoff;
      bl.substr_of(in, curoff - off, len);
      strides[stride].bl.claim_append(bl);
      break;
    }
    bl.substr_of(in, curoff - off, stripe_unit);
    strides[stride].bl.claim_append(bl);
    curoff += stripe_unit;
  }
}

int StripedPlacer::get_data(map<int, bufferlist> &strides,
			    bufferlist *decoded) const
{
  for (unsigned int i = 0; i < stripe_width; i++) {
    decoded->claim_append(strides[i]);
  }
  return 0;
};

