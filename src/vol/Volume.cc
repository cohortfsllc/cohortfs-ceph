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

#include "Volume.h"
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

#define dout_subsys ceph_subsys_mon

using std::stringstream;

const std::string Volume::typestrings[] = {
  "VolFS", "VolBlock", "VolDeDupFS", "VolDeDupBlock","NotAVolType"};

void Volume::encode(bufferlist& bl) const
{
  __u8 v = 1;
  ::encode(v, bl);
  ::encode(type, bl);
  ::encode(uuid, bl);
  ::encode(name, bl);
  ::encode(last_update, bl);
}

void Volume::decode(bufferlist::iterator& bl)
{
  __u8 v;
  ::decode(v, bl);
  ::decode(type, bl);
  ::decode(uuid, bl);
  ::decode(name, bl);
  ::decode(last_update, bl);
}

void Volume::decode(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  decode(p);
}

bool Volume::valid_name(const string &name, string &error)
{
  if (name.empty()) {
    error = "volume name may not be empty";
    return false;
  }

  if (L_IS_WHITESPACE(*name.begin())) {
    error = "volume name may not begin with space characters";
    return false;
  }

  if (L_IS_WHITESPACE(*name.rbegin())) {
    error = "volume name may not end with space characters";
    return false;
  }

  for (string::const_iterator c = name.begin(); c != name.end(); ++c) {
    if (!L_IS_PRINTABLE(*c)) {
      error = "volume name can only contain printable characters";
      return false;
    }
  }

  try {
    uuid_d::parse(name);
    error = "volume name cannot match the form of UUIDs";
    return false;
  } catch (const std::invalid_argument &ia) {
    return true;
  }

}

bool Volume::valid(string& error)
{
  if (!valid_name(name, error)) {
    return false;
  }

  if (uuid == 0) {
    error = "UUID may not be 0.";
    return false;
  }

  return true;
}

int Volume::update(VolumeCRef v)
{
#warning To be done.
  return 0;
}

const string& Volume::type_string(vol_type type)
{
  if ((type < 0) || (type >= NotAVolType)) {
    return typestrings[NotAVolType];
  } else {
    return typestrings[type];
  }
}

void Volume::dump(Formatter *f) const
{
  char uuid_buf[uuid_d::uuid_d::char_rep_buf_size];
  uuid.print(uuid_buf);
  string uuid_str(uuid_buf);
  f->dump_string("uuid", uuid_str);
  f->dump_string("name", name);
  f->dump_string("type", type_string(type));
}

