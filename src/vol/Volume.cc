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

#include <boost/uuid/string_generator.hpp>
#include "Volume.h"
#include "osd/OSDMap.h"
#include <sstream>

#ifdef USING_UNICODE
#include <unicode/uchar.h>
#define L_IS_WHITESPACE(c) (u_isUWhiteSpace(c))
#define L_IS_PRINTABLE(c) (u_hasBinaryProperty((c), UCHAR_POSIX_PRINT))
#else
#include <ctype.h>
#define L_IS_WHITESPACE(c) (isspace(c))
#define L_IS_PRINTABLE(c) (isprint(c))
#endif

WRITE_RAW_ENCODER(vol_type);

using std::stringstream;

const std::string Volume::typestrings[] = {
  "CohortVol", "NotAVolType"
};

VolumeRef CohortVolFactory(bufferlist::iterator& bl, uint8_t v, vol_type t);

const Volume::factory Volume::factories[] = {
  CohortVolFactory, NULL
};


void Volume::dump(Formatter *f) const
{
  f->dump_int("type", type);
  f->dump_stream("uuid") << id;
  f->dump_stream("name") << name;
  f->dump_stream("last_update") << last_update;
  f->dump_stream("placer") << getPlacer()->id;
}

void Volume::decode_payload(bufferlist::iterator& bl, uint8_t v)
{
  ::decode(id, bl);
  ::decode(name, bl);
  ::decode(last_update, bl);
  ::decode(placer_id, bl);
}

void Volume::encode(bufferlist& bl) const
{
  int version = 0;
  ::encode(version, bl);
  ::encode(type, bl);
  ::encode(id, bl);
  ::encode(name, bl);
  ::encode(last_update, bl);
  ::encode(getPlacer()->id, bl);
}

bool Volume::valid_name(const string &name, std::stringstream &ss)
{
  if (name.empty()) {
    ss << "volume name may not be empty";
    return false;
  }

  if (L_IS_WHITESPACE(*name.begin())) {
    ss << "volume name may not begin with space characters";
    return false;
  }

  if (L_IS_WHITESPACE(*name.rbegin())) {
    ss << "volume name may not end with space characters";
    return false;
  }

  for (string::const_iterator c = name.begin(); c != name.end(); ++c) {
    if (!L_IS_PRINTABLE(*c)) {
      ss << "volume name can only contain printable characters";
      return false;
    }
  }


  try {
    boost::uuids::string_generator parse;
    parse(name);
    ss << "volume name cannot match the form of UUIDs";
    return false;
  } catch (std::runtime_error& e) {
    return true;
  }

  return true;
}

bool Volume::valid(std::stringstream& ss) const
{
  if (!inited) {
    ss << "Has not been initialized";
    return false;
  }

  if (!valid_name(name, ss)) {
    return false;
  }

  if (id.is_nil()) {
    ss << "UUID cannot be zero.";
    return false;
  }

  return true;
}

const string& Volume::type_string(vol_type type)
{
  if ((type < 0) || (type >= NotAVolType)) {
    return typestrings[NotAVolType];
  } else {
    return typestrings[type];
  }
}

VolumeRef Volume::decode_volume(bufferlist::iterator& bl)
{
  int v;
  vol_type t;

  ::decode(v, bl);
  if (v != 0) {
    throw buffer::malformed_input("Bad version.");
  }

  ::decode(t, bl);
  if (t < 0 || t >= NotAVolType || factories[t] == NULL)
    throw buffer::malformed_input("Bad (or unimplemented) volume type.");

  return factories[t](bl, v, t);
}
