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

#ifndef CEPH_FILENAMER_H
#define CEPH_FILENAMER_H

#include "include/object.h"

namespace cohort {

struct FileNamer {
  /// Return a filename for the given object name
  virtual std::string get_name(const object_t &oid, uint64_t hash) = 0;

  virtual ~FileNamer() {}
};

class HashPrefixFileNamer : public FileNamer {
 private:
  const size_t max_len;
 public:
  HashPrefixFileNamer(size_t max_len);

  std::string get_name(const object_t &oid, uint64_t hash);
};

struct DedupFileNamer : public FileNamer {
  std::string get_name(const object_t &oid, uint64_t) { return oid.name; }
};

} // namespace cohort

#endif // CEPH_FILENAMER_H
