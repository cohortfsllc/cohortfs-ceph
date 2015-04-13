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

#include <string>
#include <map>
#include <vector>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <functional>
#include <cerrno>
#include "include/types.h"
#include "common/Formatter.h"
#include <boost/uuid/uuid.hpp>
#include "include/stringify.h"
#include "include/encoding.h"
#include "osd/osd_types.h"
#include "Placer.h"

class OSDMap;
namespace rados {
  class ObjOp;
};

enum vol_type {
  CohortVol,
  NotAVolType
};

class Volume;
typedef std::shared_ptr<const Volume> VolumeRef;

inline ostream& operator<<(ostream& out, const Volume& vol);
class Volume
{
private:
  static const std::string typestrings[];

  typedef std::shared_ptr<Volume>(*factory)(bufferlist::iterator& bl,
					    uint8_t v, vol_type t);

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
     decode_payload before doing anything, as well.)  The resulting
     volume is invalid until init() is called on it.

     The factory method must then do its own specific implementation
     and return a shared pointer to the volume.

     Factories may inspect common variables AFTER they have been set,
     but MUST NOT duplicate generic validity checks that are the
     domain of the common decode method. */
  static const factory factories[];

protected:
  boost::uuids::uuid placer_id;
  mutable bool inited;

  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);

  Volume(const vol_type t) :
    placer_id(boost::uuids::nil_uuid()), inited(false), type(t), id(boost::uuids::nil_uuid()), name() { }

  Volume(const vol_type t, const string n) :
    placer_id(boost::uuids::nil_uuid()), inited(false), type(t), id(boost::uuids::nil_uuid()), name(n) { }

  virtual PlacerRef getPlacer() const = 0;

public:

  /* It seems a bit icky to have a type field like this when we
     already have type information encoded in the class. */
  vol_type type;
  boost::uuids::uuid id;
  string name;
  epoch_t last_update;

  virtual ~Volume() { };

  static bool valid_name(const string& name, std::stringstream& ss);
  virtual bool valid(std::stringstream& ss) const;
  static const string& type_string(vol_type type);
  static VolumeRef decode_volume(bufferlist::iterator& bl);
  virtual void init(OSDMap *map) const = 0;
  /* Each child class should call its parent's dump method as it's first
     action. */
  virtual void dump(Formatter *f) const;
  /* Each child class should call its parent's encode method as it's first
     action. */
  virtual void encode(bufferlist& bl) const;
  /* Dummy decode for WRITE_CLASS_ENCODER */
  void decode(bufferlist& bl) { assert(false); };
  void decode(bufferlist::iterator& bl) { assert(false); };
  static string get_epoch_key(const boost::uuids::uuid& vol) {
    return stringify(vol) + "_epoch";
  }
  static string get_info_key(const boost::uuids::uuid& vol) {
    return stringify(vol) + "_info";
  }

  // Attach performs post-decode initialization of the volume. If you
  // call attach, you are guaranteed that the following functions will
  // execute, otherwise they will abort the program.
  virtual int attach(CephContext *cct) const {
    return getPlacer()->attach(cct);
  };
  // This function exists, at present, for the use of the monitor at
  // volume creation time, so it can attempt to instantiate a volume
  // and return a useful error on failure. It is not thread safe.
  virtual void detach(void) {
    return getPlacer()->detach();
  };
  // Returns negative POSIX error code on error.
  virtual size_t place(const oid_t& object,
		       const OSDMap& map,
		       const std::function<void(int)>& f) const {
    return getPlacer()->place(object, map, f);
  }

  int get_cohort_placer(struct cohort_placer *placer) const {
    int r = getPlacer()->get_cohort_placer(placer);
    if (r < 0)
      return r;
    memcpy(placer->volume_id, id.data, sizeof(placer->volume_id));
    return 0;
  };

  virtual std::unique_ptr<rados::ObjOp> op() const = 0;
  // Returns negative POSIX error code on error.
  virtual size_t op_size() const = 0;
  // Returns minimum number of subops that need to be placed to continue
  virtual uint32_t quorum() const = 0;
};

WRITE_CLASS_ENCODER(Volume)

inline ostream& operator<<(ostream& out, const Volume& vol) {
  return out << vol.name << "(" << vol.id << ","
	     << Volume::type_string(vol.type) << ")";
}

#endif // VOL_VOLUME_H
