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
#include "include/encoding.h"
#include "common/Formatter.h"
#include "include/uuid.h"

using namespace std;
using namespace std::tr1;

enum vol_type {
  CohortVolFS,
  CohortVolBlock,
  CohortVolDeDupFS,
  CohortVolDeDupBlock,
  NotAVolType
};

WRITE_RAW_ENCODER(vol_type);

class Volume;
typedef shared_ptr<Volume> VolumeRef;
typedef shared_ptr<const Volume> VolumeCRef;

class Volume {
private:
  static const std::string typestrings[];

protected:
  Volume(const vol_type t, const string n,
	 const bufferlist &p) :
    type(t), uuid(0), name(n) { }


public:
  vol_type type;
  uuid_d uuid;
  string name;
  epoch_t last_update;

  virtual ~Volume() { };

  virtual void encode(bufferlist& bl) const;
  virtual void decode(bufferlist::iterator& bl);
  virtual void dump(Formatter *f) const;
  void decode(bufferlist& bl);

  static bool valid_name(const string& name, string& error);
  virtual bool valid(string& error);

  virtual int update(VolumeCRef v);

  static const string& type_string(vol_type type);
};

#endif // VOL_VOLUME_H
