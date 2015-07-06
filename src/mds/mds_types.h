/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_TYPES_H
#define COHORT_MDS_TYPES_H

#include <sys/stat.h>
#include "include/ceph_time.h"
#include "ceph_mds.h"

namespace cohort {
namespace mds {

typedef libmds_ino_t ino_t;
typedef libmds_volume_t volume_t;
typedef libmds_fileid_t fileid_t;
typedef libmds_identity_t identity_t;

struct ObjAttr {
  uint64_t filesize;
  int mode;
  int user, group;
  ceph::real_time atime, mtime, ctime;
  int nlinks;
  int rawdev;
};

struct dirptr {
  int64_t cookie;
  unsigned char verifier[8];
};

struct read_delegation {
  int foo;	// need something here
  // probably a list of segments and indication which osd to go to.
};

struct write_delegation {
  int foo;	// need something here
};

// access mask
typedef int accessmask;
constexpr unsigned ACCESS_READ  = 0x1;
constexpr unsigned ACCESS_WRITE = 0x2;

} // namespace mds
} // namespace cohort

#endif /* COHORT_MDS_TYPES_H */
