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
#include "include/utime.h"
#include "osd/osd_types.h"

class OSDMap;
class ObjOp;

enum vol_type {
  CohortVol,
  NotAVolType
};

class Volume;
typedef std::shared_ptr<Volume> VolumeRef;

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
     decode_payload before doing anything, as well.)

     The factory method must then do its own specific implementation
     and return a shared pointer to the volume.

     Factories may inspect common variables AFTER they have been set,
     but MUST NOT duplicate generic validity checks that are the
     domain of the common decode method. */
  static const factory factories[];

protected:
  virtual void decode_payload(bufferlist::iterator& bl, uint8_t v);

  Volume(const vol_type t) :
    type(t), id(boost::uuids::nil_uuid()), name() { }

  Volume(const vol_type t, const string n) :
    type(t), id(boost::uuids::nil_uuid()), name(n) { }


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
  // execute without error. Otherwise, you have to check the return
  // values. This function returns non-zero on error.
  virtual int attach(std::stringstream &ss) {
    return 0;
  };
  int attach(CephContext *cct) {
    std::stringstream ss;
    int r = attach(ss);
    if (r < 0) {
      lsubdout(cct, volume, -1) << "volume: " << *this <<
	": unable to attach: " << ss.str() << dendl;
    }
    return r;
  };
  // This function exists, at present, for the use of the monitor at
  // volume creation time, so it can attempt to instantiate a volume
  // and return a useful error on failure. It is not thread safe.
  virtual void detach(void) {};
  // Returns negative POSIX error code on error.
  virtual ssize_t place(const object_t& object,
			const OSDMap& map,
			const std::function<void(int)>& f) const = 0;
  // Returns NULL on error.
  virtual std::unique_ptr<ObjOp> op() const = 0;
  // Returns negative POSIX error code on error.
  virtual ssize_t op_size() const = 0;

};

WRITE_CLASS_ENCODER(Volume)

inline ostream& operator<<(ostream& out, const Volume& vol) {
  return out << vol.name << "(" << vol.id << ","
	     << Volume::type_string(vol.type) << ")";
}

#endif // VOL_VOLUME_H
