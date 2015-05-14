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

#ifndef VOL_VOLUME_H
#define VOL_VOLUME_H

#include <functional>
#include <string>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/nil_generator.hpp>

#include "include/types.h"
#include "include/stringify.h"
#include "include/encoding.h"
#include "osd/osd_types.h"
#include "osdc/ObjectOperation.h"
#include "placer/Placer.h"

class OSDMap;

namespace ceph {
  class Formatter;
};

class Volume;
typedef std::shared_ptr<const Volume> VolumeRef;

class AttachedVol : public boost::intrusive_ref_counter<AttachedVol> {
  friend Volume;
  friend struct C_MultiStat;
  friend struct C_MultiRead;

private:
  APlacerRef placer;

  AttachedVol(CephContext* cct, const OSDMap& o, VolumeRef v);
public:
  VolumeRef v;
  static const uint64_t one_op;
  const VolumeRef vol;
  static string get_epoch_key(const boost::uuids::uuid& vol) {
    return stringify(vol) + "_epoch";
  }
  static string get_info_key(const boost::uuids::uuid& vol) {
    return stringify(vol) + "_info";
  }
  size_t place(const oid_t& object,
	       const OSDMap& map,
	       const std::function<void(int)>& f) const;

  int get_cohort_placer(struct cohort_placer *p) const;

  std::unique_ptr<rados::ObjOp> op() const;
  class StripulatedOp : public rados::ObjOp {
    friend class AttachedVol;
    const AttachedPlacer& pl;
    // ops[n][m] is the mth operation in the nth stride
    vector<vector<OSDOp> > ops;
    size_t logical_operations;

    virtual ~StripulatedOp() { }

    StripulatedOp(const AttachedPlacer& pl);
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

  uint32_t quorum() const {
    return placer->quorum();
  }
  // Returns minimum number of subops that need to be placed to continue
  size_t op_size() const {
    return placer->op_size();
  }
};
typedef boost::intrusive_ptr<const AttachedVol> AVolRef;

inline ostream& operator<<(ostream& out, const Volume& vol);
class Volume : public std::enable_shared_from_this<Volume> {
  friend AttachedVol;
private:
  boost::uuids::uuid placer_id;

  Volume() :
    placer_id(boost::uuids::nil_uuid()), id(boost::uuids::nil_uuid()),
    name() { }

  Volume(const string& _name, boost::uuids::uuid _id,
	 boost::uuids::uuid _placer_id) :
    placer_id(_placer_id), id(_id), name(_name) { }

public:

  /* It seems a bit icky to have a type field like this when we
     already have type information encoded in the class. */
  boost::uuids::uuid id;
  string name;

  virtual ~Volume() { };

  static bool valid_name(const string& name, std::stringstream& ss);
  virtual bool valid(std::stringstream& ss) const;
  static VolumeRef decode_volume(bufferlist::iterator& bl);
  /* Attach a volume. Remains attached until the last reference is
     dropped. */
  AVolRef attach(CephContext *cct, const OSDMap& o) const;
  static VolumeRef create(CephContext *cct,
			  const string& name,
			  boost::uuids::uuid placer_id,
			  std::stringstream& ss);
  /* Each child class should call its parent's dump method as it's first
     action. */
  void dump(Formatter *f) const;
  /* Each child class should call its parent's encode method as it's first
     action. */
  void encode(bufferlist& bl) const;
  /* Dummy decode for WRITE_CLASS_ENCODER */
  void decode(bufferlist& bl) { assert(false); };
  void decode(bufferlist::iterator& bl) { assert(false); };
  static string get_epoch_key(const boost::uuids::uuid& vol) {
    return stringify(vol) + "_epoch";
  }
  static string get_info_key(const boost::uuids::uuid& vol) {
    return stringify(vol) + "_info";
  }
};

WRITE_CLASS_ENCODER(Volume)

inline ostream& operator<<(ostream& out, const Volume& vol) {
  return out << vol.name << "(" << vol.id << ")";
}

#endif // VOL_VOLUME_H
