// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_STRIPEDPLACER_H
#define COHORT_STRIPEDPLACER_H

#include "placer/Placer.h"
#include "osdc/ObjectOperation.h"

class StripedPlacer;
typedef std::shared_ptr<StripedPlacer> StripedPlacerRef;

class StripedPlacer : public Placer, public AttachedPlacer
{
  typedef Placer inherited;

protected:
  uint32_t stripe_unit;
  uint32_t stripe_width;

protected:

  StripedPlacer()
    : Placer(StripedPlacerType),
    stripe_unit(0), stripe_width(0) { }

  /* Size of a stride for a given write */
  size_t stride_size(const uint64_t len) const {
    size_t size = len / stripe_width;
    if (len % stripe_width)
      size++;
    return size;
  }

  /* Index of the stride containing offset */
  size_t stride_idx(const uint64_t off) const {
    return (off / stripe_unit) % stripe_width;
  }
  /* Offset into stride of object offset */
  uint64_t stride_offset(const uint64_t off) const {
    uint64_t stripe_size = stripe_width * stripe_unit;
    return (off / stripe_size) * stripe_unit + off % stripe_unit;
  }
  /* Number of stripe units contained in the extent */
  size_t extent_units(const uint64_t off, const uint64_t len) const {
    if (len == 0) {
      return 0;
    }
    uint32_t first = off / stripe_unit;
    uint32_t last = (off + len - 1) / stripe_unit;
    return last - first + 1;
  }

  void stride_extent(const uint64_t off, const uint64_t len,
		     const size_t stride, uint64_t &strideoff,
		     uint64_t &stridelen);

  int encode(const set<int> &want_to_encode,
	     const bufferlist &in,
	     map<int, bufferlist> *encoded) const;
public:
  ~StripedPlacer();

  static const uint64_t one_op;

  virtual APlacerRef attach(CephContext* cct) const {
    return APlacerRef(dynamic_cast<const AttachedPlacer*>(this));
  }

  virtual size_t op_size() const noexcept {
    return one_op * stripe_width;
  }

  virtual uint32_t quorum() const noexcept {
    return stripe_width;
  }

  virtual uint32_t num_rules(void);

  virtual size_t place(const oid_t& object,
		       const boost::uuids::uuid& id,
		       const OSDMap& map,
		       const std::function<void(int)>& f) const;

  virtual int update(const std::shared_ptr<const Placer>& pl);

  virtual void dump(Formatter *f) const;
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);
  virtual void encode(bufferlist& bl) const;

  friend PlacerRef StripedPlacerFactory(bufferlist::iterator& bl, uint8_t v);

  static PlacerRef create(CephContext *cct,
			  const string& name,
			  const int64_t _stripe_unit,
			  const int64_t _stripe_width,
			  std::stringstream& ss);

  size_t get_chunk_count() const {
    return stripe_width;
  }

  size_t get_data_chunk_count() const {
    return stripe_width;
  }

  virtual uint32_t get_stripe_unit() const {
    return stripe_unit;
  };

  virtual void make_strides(const oid_t& oid,
			    uint64_t offset, uint64_t len,
			    uint64_t truncate_size, uint32_t truncate_seq,
			    vector<StrideExtent>& extents) const;

  virtual void repair(vector<StrideExtent>& extents,
		      const OSDMap& map) const;

  virtual void serialize_data(bufferlist &bl) const;
  virtual void serialize_code(bufferlist &bl) const;

  // Data and metadata operations using the placer
  virtual void add_data(const uint64_t off, bufferlist& in,
			vector<StrideExtent>& out) const;
  virtual int get_data(map<int, bufferlist> &strides,
			    bufferlist *decoded) const;

  virtual int get_cohort_placer(struct cohort_placer *placer) const {
    placer->type = StripedPlacerType;
    placer->striped.stripe_unit = stripe_unit;
    placer->striped.stripe_width = stripe_width;
    return 0;
  };

  virtual PlacerRef clone() const {
    return PlacerRef(new StripedPlacer(*this));
  }
};

#endif // COHORT_STRIPEDPLACER_H
