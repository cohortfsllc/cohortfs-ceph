// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_COHORTVOLUME_H
#define COHORT_COHORTVOLUME_H

#include <mutex>
#include "vol/Volume.h"
#include "ErasureCPlacer.h"

/* Superclass of all Cohort volume types, supporting dynamically
   generated placement. */

class CohortVolume : public Volume
{
  typedef Volume inherited;
  friend struct C_MultiStat;
  friend struct C_MultiRead;

protected:
  mutable PlacerRef placer;
  CohortVolume(vol_type t)
    : Volume(t) {}
  virtual PlacerRef getPlacer() const {
	  return PlacerRef(std::static_pointer_cast<Placer>(placer));
  }

  public:

  ~CohortVolume() {}

  static const uint64_t one_op;

  virtual size_t op_size() const {
	  return placer->op_size();
  }

  virtual uint32_t quorum() const {
	  return placer->quorum();
  }

  virtual int update(const std::shared_ptr<const Volume>& v);

  virtual void init(OSDMap *map) const;
  virtual void dump(Formatter *f) const;
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);
  virtual void encode(bufferlist& bl) const;

  friend VolumeRef CohortVolFactory(bufferlist::iterator& bl, uint8_t v,
				    vol_type t);

  static VolumeRef create(CephContext *cct, const string& name,
			  PlacerRef placer,
			  std::stringstream& ss);

  virtual std::unique_ptr<rados::ObjOp> op() const;

  class StripulatedOp : public rados::ObjOp {
    friend CohortVolume;
    const Placer& pl;
    // ops[n][m] is the mth operation in the nth stride
    vector<vector<OSDOp> > ops;
    size_t logical_operations;

    virtual ~StripulatedOp() { }

    StripulatedOp(const Placer& pl);
    virtual size_t size() {
      return logical_operations;
    }
    virtual size_t width() {
      return pl.get_chunk_count();
    }
    virtual void read(uint64_t off, uint64_t len, bufferlist *bl,
		      uint64_t truncate_size, uint32_t truncate_seq,
		      int *rval = NULL, Context* ctx = NULL);
    virtual void read_full(bufferlist *bl,
		      int *rval = NULL, Context* ctx = NULL);
    virtual void read(uint64_t off, uint64_t len, uint64_t truncate_size,
		      uint32_t truncate_seq,
		      std::function<void(int, bufferlist&&)>&& f);
    virtual void read_full(std::function<void(int, bufferlist&&)>&& f);
    virtual void add_op(const int op);
    virtual void add_version(const uint64_t ver);
    virtual void add_obj(const oid_t& o);
    virtual void add_single_return(bufferlist* bl, int* rval = NULL,
				   Context *ctx = NULL);
    virtual void add_single_return(std::function<void(int, bufferlist&&)>&& f);

    virtual void add_metadata(const bufferlist& bl);
    virtual void add_metadata_range(const uint64_t off, const uint64_t len);
    virtual void add_data(const uint64_t off, const bufferlist& bl);
    virtual void add_data_range(const uint64_t off, const uint64_t len);
    virtual void add_xattr(const string &name, const bufferlist& data);
    virtual void add_xattr(const string &name, bufferlist* data);
    virtual void add_xattr_cmp(const string &name, uint8_t cmp_op,
			       const uint8_t cmp_mode,
			       const bufferlist& data);

    virtual void add_call(const string &cname, const string &method,
			  const bufferlist &indata, bufferlist *const outbl,
			  Context *const ctx, int *const prval);
    virtual void add_call(const string &cname, const string &method,
			  const bufferlist &indata,
			  std::function<void(int, bufferlist&&)>&& cb);

    virtual void add_watch(const uint64_t cookie, const uint64_t ver,
			   const uint8_t flag, const bufferlist& inbl);

    virtual void add_alloc_hint(const uint64_t expected_object_size,
				const uint64_t expected_write_size);
    virtual void add_truncate(const uint64_t truncate_size,
			      const uint32_t truncate_seq);

    virtual void add_sparse_read_ctx(uint64_t off, uint64_t len,
				     std::map<uint64_t,uint64_t> *m,
				     bufferlist *data_bl, int *rval,
				     Context *ctx);
    virtual void set_op_flags(const uint32_t flags);
    virtual void clear_op_flags(const uint32_t flags);
    virtual void add_stat_ctx(uint64_t *s, ceph::real_time *m, int *rval,
			      Context *ctx = NULL);
    virtual void add_stat_cb(std::function<void(
			       int, uint64_t, ceph::real_time)>&& cb);
    virtual std::unique_ptr<ObjOp> clone();
    virtual void realize(
      const oid_t& o,
      const std::function<void(oid_t&&, vector<OSDOp>&&)>& f);
  };
};

#endif // COHORT_COHORTVOLUME_H
