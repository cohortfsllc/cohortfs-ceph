// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014, CohortFS, LLC <info@cohortfs.com> All rights
 * reserved.
 *
 * This file is licensed under what is commonly known as the New BSD
 * License (or the Modified BSD License, or the 3-Clause BSD
 * License). See file COPYING.
 *
 */

#ifndef VOL_PLACER_H
#define VOL_PLACER_H

#include <cerrno>
#include <string>
#include <map>
#include <vector>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "common/Formatter.h"
#include "include/cohort_error.h"
#include "include/cephfs/placement.h"
#include "include/ceph_time.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "include/types.h"
#include "osd/osd_types.h"

class OSDMap;
class Placer;
typedef std::unique_ptr<const Placer> PlacerRef;

enum class placer_errc { no_such_placer, exists };

class placer_category_t : public std::error_category {
  virtual const char* name() const noexcept;
  virtual std::string message(int ev) const;
  virtual std::error_condition default_error_condition(int ev) const noexcept {
    switch (static_cast<placer_errc>(ev)) {
    case placer_errc::no_such_placer:
      return std::errc::no_such_device;
    case placer_errc::exists:
      return cohort::errc::object_already_exists;
    default:
      return std::error_condition(ev, *this);
    }
  }
};

const std::error_category& placer_category();

static inline std::error_condition make_error_condition(placer_errc e) {
  return std::error_condition(static_cast<int>(e), placer_category());
}

static inline std::error_code make_error_code(placer_errc e) {
  return std::error_code(static_cast<int>(e), placer_category());
}

namespace std {
  template <>
  struct is_error_code_enum<placer_errc> : public std::true_type {};
};

class AttachedPlacer : public boost::intrusive_ref_counter<AttachedPlacer> {
public:
  static const uint64_t one_op;

  struct StrideExtent {
    uint64_t offset;
    uint64_t length;
    bufferlist bl;
    uint64_t truncate_size;
    uint32_t truncate_seq;

    StrideExtent():
      offset(),
      length(),
      bl(),
      truncate_size(),
      truncate_seq()
      { }
  };

  virtual ~AttachedPlacer() { }
  virtual size_t place(const oid_t& object,
		       const boost::uuids::uuid& id,
		       const OSDMap& map,
		       const std::function<void(int)>& f) const = 0;
  // Returns negative POSIX error code on error.
  virtual size_t op_size() const noexcept = 0;
  // Returns minimum number of subops that need to be placed to continue
  virtual uint32_t quorum() const noexcept = 0;

  virtual void make_strides(const oid_t& oid,
			    uint64_t offset, uint64_t len,
			    uint64_t truncate_size, uint32_t truncate_seq,
			    vector<StrideExtent>& strides) const = 0;

  virtual void repair(vector<StrideExtent>& extants,
		      const OSDMap& map) const = 0;

  virtual void serialize_data(bufferlist &bl) const = 0;
  virtual void serialize_code(bufferlist &bl) const = 0;

  virtual size_t get_chunk_count() const = 0;
  virtual size_t get_data_chunk_count() const = 0;
  virtual uint32_t get_stripe_unit() const = 0;

  // Data and metadata operations using the placer
  virtual void add_data(const uint64_t off, bufferlist& in,
			vector<StrideExtent>& out) const = 0;
  virtual int get_data(map<int, bufferlist> &strides,
		       bufferlist *decoded) const = 0;
  // C API helpers
  virtual int get_cohort_placer(struct cohort_placer *placer) const {
    placer->type = NotAPlacerType;
    return -1;
  };
};

typedef boost::intrusive_ptr<const AttachedPlacer> APlacerRef;

inline ostream& operator<<(ostream& out, const Placer& pl);
class Placer  {
private:
  static const std::string typestrings[];

  typedef PlacerRef(*factory)(bufferlist::iterator& bl, uint8_t v);

  /* Factory Protocol

     Know then that a factory is called with a bufferlist from which
     it is to decode itself.  It is also called with the version and
     volume type. (The volume type should be obvious since it's what
     the factory is dispatched on, but just in case someone decides
     to use the same function for two streams that are almost
     identical.)

     The very first thing the factory must do is allocate (with new)
     an volume of the required type with the type constructor.

     It must then call the decode_payload() on the new volume.  (This
     is virtual, so that volume classes that share functionality can
     share a parent and have common structure decoded by it.  Any
     decode_payload in a child of Volume must call its parent's
     decode_payload before doing anything, as well.)

     The factory method must then do its own specific implementation
     and return a shared pointer to the volume.

     Factories may inspect common variables AFTER they have been set,
     but MUST NOT duplicate generic validity checks that are the
     domain of the common decode method. */
  static const factory factories[];

protected:
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);

  Placer(const placer_type t) :
    type(t), id(boost::uuids::nil_uuid()), name() { }

  Placer(const placer_type t, const string n) :
    type(t), id(boost::uuids::nil_uuid()), name(n) { }

public:

  /* It seems a bit icky to have a type field like this when we
     already have type information encoded in the class. */
  placer_type type;
  boost::uuids::uuid id;
  string name;
  epoch_t last_update;

  virtual ~Placer() { };

  static bool valid_name(const string& name, std::stringstream& ss);
  virtual bool valid(std::stringstream& ss) const;
  static const string& type_string(placer_type type);
  static PlacerRef decode_placer(bufferlist::iterator& bl);
  /* Each child class should call its parent's dump method as it's first
     action. */
  virtual void dump(Formatter *f) const;
  /* Each child class should call its parent's encode method as it's first
     action. */
  virtual void encode(bufferlist& bl) const;
  /* Dummy decode for WRITE_CLASS_ENCODER */
  void decode(bufferlist& bl) { assert(false); };
  void decode(bufferlist::iterator& bl) { assert(false); };

  virtual PlacerRef clone() const = 0;

  virtual APlacerRef attach(CephContext *cct) const = 0;
};

WRITE_CLASS_ENCODER(Placer)

inline ostream& operator<<(ostream& out, const Placer& placer) {
	return out << placer.name << "(" << placer.id << ","
		<< Placer::type_string(placer.type) << ")";
}

#endif // VOL_PLACER_H
