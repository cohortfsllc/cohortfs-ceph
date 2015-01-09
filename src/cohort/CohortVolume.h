// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2013, CohortFS, LLC <info@cohortfs.com>
 * All rights reserved.
 *
 * DO NOT DISTRIBUTE THIS FILE.  EVER.
 */

#ifndef COHORT_COHORTVOLUME_H
#define COHORT_COHORTVOLUME_H

#include "vol/Volume.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "common/RWLock.h"
#include "osdc/ObjectOperation.h"

/* Superclass of all Cohort volume types, supporting dynamically
   generated placement. */

class CohortVolume : public Volume
{
  typedef Volume inherited;
  friend struct C_MultiStat;
  friend struct C_MultiRead;

protected:
  string erasure_plugin;
  map<string, string> erasure_params;
  uint32_t suggested_unit; // Specified by the user
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
  int compile(std::stringstream &ss) const;


protected:

  CohortVolume(vol_type t)
    : Volume(t), place_text(), symbols(), attached(false), entry_points(),
      place_shared(NULL), stripe_unit(0) { }

  public:

  ~CohortVolume();

  static const uint64_t one_op;

  int _attach(std::stringstream &ss) const;

  virtual int attach(std::stringstream &ss) {
    if (attached)
      return 0;
    return _attach(ss);
  }

  virtual void detach();

  virtual ssize_t op_size() const {
    if (!attached) {
      std::stringstream(ss);
      int r = _attach(ss);
      if (r < 0)
	return r;
    }
    return one_op * erasure->get_chunk_count();
  }

  virtual uint32_t num_rules(void);

  virtual ssize_t place(const object_t& object,
			const OSDMap& map,
			const std::function<void(int)>& f) const;

  virtual int update(const std::shared_ptr<const Volume>& v);

  virtual void dump(Formatter *f) const;
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);
  virtual void encode(bufferlist& bl) const;

  friend VolumeRef CohortVolFactory(bufferlist::iterator& bl, uint8_t v,
				    vol_type t);

  static VolumeRef create(const string& name, uint32_t _suggested_width,
			  const string& erasure_plugin,
			  const string& erasure_params,
			  const string& place_text, const string& symbols,
			  std::stringstream& ss);

  virtual std::unique_ptr<ObjOp> op() const;

  class StripulatedOp : public ObjOp {
    friend CohortVolume;
    const CohortVolume& v;
    // ops[n][m] is the mth operation in the nth stride
    vector<vector<OSDOp> > ops;
    size_t logical_operations;

    virtual ~StripulatedOp() { }

    StripulatedOp(const CohortVolume& v);
    virtual size_t size() {
      return logical_operations;
    }
    virtual size_t width() {
      return v.erasure->get_chunk_count();
    }
    virtual void add_op(const int op);
    virtual void add_version(const uint64_t ver);
    virtual void add_oid(const hobject_t &oid);
    virtual void add_single_return(bufferlist* bl, int* rval = NULL,
				   Context *ctx = NULL);

    virtual void add_replicated_data(const bufferlist& bl);
    virtual void add_striped_data(const uint64_t off,
				  const bufferlist& bl);
    virtual void add_striped_range(const uint64_t off,
				   const uint64_t len);
    virtual void add_xattr(const string &name, const bufferlist& data);
    virtual void add_xattr(const string &name, bufferlist* data);
    virtual void add_xattr_cmp(const string &name, uint8_t cmp_op,
			       const uint8_t cmp_mode,
			       const bufferlist& data);

    virtual void add_call(const string &cname, const string &method,
			  const bufferlist &indata, bufferlist *const outbl,
			  Context *const ctx, int *const prval);

    virtual void add_watch(const uint64_t cookie, const uint64_t ver,
			   const uint8_t flag, const bufferlist& inbl);

    virtual void add_alloc_hint(const uint64_t expected_object_size,
				const uint64_t expected_write_size);
    virtual void add_truncate(const uint64_t truncate_size,
			      const uint32_t truncate_seq);

    virtual void add_read_ctx(const uint64_t off, const uint64_t len,
			      bufferlist *bl, int *rval = NULL,
			      Context *ctx = NULL);
    virtual void add_sparse_read_ctx(uint64_t off, uint64_t len,
				     std::map<uint64_t,uint64_t> *m,
				     bufferlist *data_bl, int *rval,
				     Context *ctx);
    virtual void set_op_flags(const uint32_t flags);
    virtual void clear_op_flags(const uint32_t flags);
    virtual void add_stat_ctx(uint64_t *s, utime_t *m, int *rval,
			      Context *ctx = NULL);
    virtual std::unique_ptr<ObjOp> clone();
    virtual void realize(
      const object_t& oid,
      const std::function<void(hobject_t&&, vector<OSDOp>&&)>& f);
  };
};

#endif // COHORT_COHORTVOLUME_H
