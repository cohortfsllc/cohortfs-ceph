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

#include <errno.h>
#include <string>
#include <map>
#include <vector>
#include <tr1/memory>
#include "include/types.h"
#include "common/Formatter.h"
#include "include/uuid.h"
#include "include/stringify.h"
#include "include/encoding.h"

class OSDMap;

using namespace std;
using namespace std::tr1;

enum vol_type {
  CohortVol,
  NotAVolType
};

class Volume;
typedef shared_ptr<Volume> VolumeRef;
typedef shared_ptr<const Volume> VolumeCRef;

class Volume
{
private:
  static const std::string typestrings[];

  typedef VolumeRef(*factory)(bufferlist::iterator& bl, __u8 v, vol_type t);

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
  virtual void decode_payload(bufferlist::iterator& bl, __u8 v);

  Volume(const vol_type t) :
    type(t), uuid(INVALID_VOLUME), name() { }

  Volume(const vol_type t, const string n) :
    type(t), uuid(INVALID_VOLUME), name(n) { }


public:

  /* It seems a bit icky to have a type field like this when we
     already have type information encoded in the class. */
  vol_type type;
  uuid_d uuid;
  string name;
  epoch_t last_update;

  virtual ~Volume() { };

  static bool valid_name(const string& name, string& error);
  virtual bool valid(string& error);
  static const string& type_string(vol_type type);
  static VolumeRef decode_volume(bufferlist::iterator& bl);
  /* Each child class should call its parent's encode class as it's first
     action. */
  virtual void encode(bufferlist& bl) const;
  /* Dummy decode for WRITE_CLASS_ENCODER */
  void decode(bufferlist& bl) { assert(false); };
  void decode(bufferlist::iterator& bl) { assert(false); };

  static string get_epoch_key(uuid_d vol) {
    return stringify(vol) + "_epoch";
  }
  static string get_info_key(uuid_d vol) {
    return stringify(vol) + "_info";
  }
  static string get_biginfo_key(uuid_d vol) {
    return stringify(vol) + "_biginfo";
  }

  virtual int place(const object_t& object,
		    const OSDMap& map,
		    const unsigned int rule_index,
		    vector<int>& osds) = 0;
};

WRITE_CLASS_ENCODER(Volume)

inline ostream& operator<<(ostream& out, const Volume& vol) {
  return out << Volume::type_string(vol.type) << " : "
	     << vol.uuid << " : " << vol.name;
}

#endif // VOL_VOLUME_H
