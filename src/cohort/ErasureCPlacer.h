// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_ERASURECPLACER_H
#define COHORT_ERASURECPLACER_H

#include "vol/Placer.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "common/RWLock.h"
#include "osdc/ObjectOperation.h"

class ErasureCPlacer;
typedef std::shared_ptr<ErasureCPlacer> ErasureCPlacerRef;

class ErasureCPlacer : public Placer
{
  typedef Placer inherited;

protected:
  string erasure_plugin;
  map<string, string> erasure_params;
  uint64_t suggested_unit; // Specified by the user
  bufferlist place_text;
  vector<std::string> symbols;

private:
  /* These are internal and are not serialized */
  mutable bool attached;
  mutable Mutex lock;
  mutable vector<void*> entry_points;
  mutable void *place_shared;
  mutable ceph::ErasureCodeInterfaceRef erasure;
  mutable uint32_t stripe_unit; // Actually used after consulting with
				// erasure code plugin
  int compile(CephContext* cct) const;


protected:

  ErasureCPlacer()
	: Placer(ErasureCPlacerType), place_text(), symbols(), attached(false), entry_points(),
      place_shared(NULL), stripe_unit(0) { }

  /* Index of the stride containing offset */
  size_t stride_idx(const uint64_t off) {
    return (off / stripe_unit) % erasure->get_data_chunk_count();
  }
  /* Offset into stride of object offset */
  uint64_t stride_offset(const uint64_t off) {
    uint64_t sw = erasure->get_data_chunk_count() * stripe_unit;
    return (off / sw) * stripe_unit + off % stripe_unit;
  }
  /* Number of stripe units contained in the extent */
  size_t extent_units(const uint64_t off, const uint64_t len) {
    if (len == 0) {
      return 0;
    }
    uint32_t first = off / stripe_unit;
    uint32_t last = (off + len - 1) / stripe_unit;
    return std::min(last - first + 1, erasure->get_data_chunk_count());
  }

  void stride_extent(const uint64_t off, const uint64_t len,
		     const size_t stride, uint64_t &strideoff,
		     uint64_t &stridelen);

public:
  ~ErasureCPlacer();

  static const uint64_t one_op;

  virtual bool is_attached() const {
	  return attached;
  };
  int _attach(CephContext* cct, stringstream *ss = nullptr) const;

  virtual int attach(CephContext* cct) {
	  if (attached)
		  return 0;
	  return _attach(cct);
  }

  virtual void detach();

  virtual ssize_t op_size() const {
    assert(attached);
	  return one_op * erasure->get_chunk_count();
  }

  virtual int32_t quorum() const {
	  if (!attached) {
      abort();
    }
	  return erasure->get_data_chunk_count();
  }

  virtual uint32_t num_rules(void);

  virtual ssize_t place(const object_t& object,
		       const OSDMap& map,
		       const std::function<void(int)>& f) const;

  virtual int update(const std::shared_ptr<const Placer>& pl);

  virtual void dump(Formatter *f) const;
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);
  virtual void encode(bufferlist& bl) const;

  friend PlacerRef ErasureCPlacerFactory(bufferlist::iterator& bl, uint8_t v);

  static PlacerRef create(CephContext *cct,
		  const string& name,
		  int64_t _suggested_width,
		  const string& erasure_plugin,
		  const string& erasure_params,
		  const string& place_text, const string& symbols,
		  std::stringstream& ss);

  size_t get_chunk_count() const {
    return erasure->get_chunk_count();
  }

  size_t get_data_chunk_count() const {
    return erasure->get_data_chunk_count();
  }

  virtual uint32_t get_stripe_unit() const {
    return stripe_unit;
  };

  virtual void make_strides(const object_t& oid,
			    uint64_t offset, uint64_t len,
			    uint64_t truncate_size, uint32_t truncate_seq,
			    vector<StrideExtent>& extents);

  virtual void repair(vector<StrideExtent>& extents,
		      const OSDMap& map);

  virtual void serialize_data(bufferlist &bl);
  virtual void serialize_code(bufferlist &bl);

  int encode(const set<int> &want_to_encode,
		     const bufferlist &in,
		     map<int, bufferlist> *encoded) const {
    return erasure->encode(want_to_encode, in, encoded);
  };
  int decode_concat(const map<int, bufferlist> &chunks,
		    bufferlist *decoded) const {
    return erasure->decode_concat(chunks, decoded);
  };
};

#endif // COHORT_ERASURECPLACER_H
