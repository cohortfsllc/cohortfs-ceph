// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#include "os/FileNamer.h"

#define STR(s) #s
#define XSTR(s) STR(s)
#define HASH_LEN 16 // hex digits for 64-bit value

using namespace cohort;

HashPrefixFileNamer::HashPrefixFileNamer(size_t max_len)
  : max_len(max_len)
{
  assert(max_len > HASH_LEN); // must leave room for entire hash
}

std::string HashPrefixFileNamer::get_name(const object_t &oid, uint64_t hash)
{
  // allocate a string of the required length
  const size_t oid_len = std::min(oid.name.size(), (max_len - HASH_LEN));
  std::string name(HASH_LEN + oid_len, 0);

  // fill in the hash prefix
#define HASH_FMT ("%0" XSTR(HASH_LEN) "lX")
  int count = snprintf(&name[0], name.size(), HASH_FMT, hash);
  assert(count == HASH_LEN);

  // append the object name, truncating if necessary
  std::copy(oid.name.begin(), oid.name.begin() + oid_len,
            name.begin() + count);

  return name;
}
