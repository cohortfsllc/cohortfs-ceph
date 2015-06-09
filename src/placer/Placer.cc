// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include <boost/uuid/string_generator.hpp>
#include "Placer.h"
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

const uint64_t AttachedPlacer::one_op = 4194304;

using namespace std::literals;

WRITE_RAW_ENCODER(placer_type);

const char* placer_category_t::name() const noexcept {
  return "placer";
}

std::string placer_category_t::message(int ev) const {
  switch (static_cast<placer_errc>(ev)) {
  case placer_errc::no_such_placer:
    return "no such placer"s;
  case placer_errc::exists:
    return "placer exists"s;
  default:
    return "unknown error"s;
  }
}

const std::error_category& placer_category() {
  static placer_category_t instance;
  return instance;
}


using namespace std::literals;
using std::system_error;
using ceph::buffer_errc;
using std::stringstream;

const std::string Placer::typestrings[] = {
  "NotAPlacerType",
  "ErasureCPlacer",
  "StripedPlacer",
  "MaxPlacerType"
};

PlacerRef ErasureCPlacerFactory(bufferlist::iterator& bl, uint8_t v);
PlacerRef StripedPlacerFactory(bufferlist::iterator& bl, uint8_t v);

const Placer::factory Placer::factories[] = {
  NULL,
  ErasureCPlacerFactory,
  StripedPlacerFactory,
  NULL
};


void Placer::dump(Formatter *f) const
{
  f->dump_int("type", type);
  f->dump_stream("uuid") << id;
  f->dump_stream("name") << name;
  f->dump_stream("last_update") << last_update;
}

void Placer::decode_payload(bufferlist::iterator& bl, uint8_t v)
{
  ::decode(id, bl);
  ::decode(name, bl);
  ::decode(last_update, bl);
}

void Placer::encode(bufferlist& bl) const
{
  int version = 0;
  ::encode(version, bl);
  ::encode(type, bl);
  ::encode(id, bl);
  ::encode(name, bl);
  ::encode(last_update, bl);
}

bool Placer::valid_name(const string &name, std::stringstream &ss)
{
  if (name.empty()) {
    ss << "placer name may not be empty";
    return false;
  }

  if (L_IS_WHITESPACE(*name.begin())) {
    ss << "placer name may not begin with space characters";
    return false;
  }

  if (L_IS_WHITESPACE(*name.rbegin())) {
    ss << "placer name may not end with space characters";
    return false;
  }

  for (string::const_iterator c = name.begin(); c != name.end(); ++c) {
    if (!L_IS_PRINTABLE(*c)) {
      ss << "placer name can only contain printable characters";
      return false;
    }
  }


  try {
    boost::uuids::string_generator parse;
    parse(name);
    ss << "placer name cannot match the form of UUIDs";
    return false;
  } catch (std::runtime_error& e) {
    return true;
  }

  return true;
}

bool Placer::valid(std::stringstream& ss) const
{
  if (!valid_name(name, ss)) {
    return false;
  }

  if (id.is_nil()) {
    ss << "UUID cannot be zero.";
    return false;
  }

  return true;
}

const string& Placer::type_string(placer_type type)
{
  if ((type <= NotAPlacerType) || (type >= MaxPlacerType)) {
    return typestrings[NotAPlacerType];
  } else {
    return typestrings[type];
  }
}

PlacerRef Placer::decode_placer(bufferlist::iterator& bl)
{
  int v;
  placer_type t;

  ::decode(v, bl);
  if (v != 0) {
    throw system_error(buffer_errc::malformed_input, "Bad version."s);
  }

  ::decode(t, bl);
  if (t <= NotAPlacerType || t >= MaxPlacerType || factories[t] == NULL)
    throw system_error(buffer_errc::malformed_input,
		       "Bad (or unimplemented) placer type."s);

  return factories[t](bl, v);
}
