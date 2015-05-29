/* -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- */
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_MDS_TYPES_H
#define COHORT_MDS_TYPES_H

#include "include/ceph_time.h"

namespace cohort {
namespace mds {

// attribute mask
constexpr unsigned ATTR_SIZE    = 0x001;
constexpr unsigned ATTR_MODE    = 0x002;
constexpr unsigned ATTR_GROUP   = 0x004;
constexpr unsigned ATTR_OWNER   = 0x008;
constexpr unsigned ATTR_ATIME   = 0x010;
constexpr unsigned ATTR_MTIME   = 0x020;
constexpr unsigned ATTR_CTIME   = 0x040;
constexpr unsigned ATTR_NLINKS  = 0x080;
constexpr unsigned ATTR_TYPE    = 0x100;
constexpr unsigned ATTR_RAWDEV  = 0x200;

struct ObjAttr {
  // int mask;
  uint64_t filesize;
  int mode;
  int user, group;
  ceph::real_time atime, mtime, ctime;
  int nlinks;
  int type; // TODO: encode type in mode?
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

constexpr unsigned MAX_NGROUPS = 32;

struct identity {
  int uid;
  int gid;
  int ngroups;
  int groups[MAX_NGROUPS];
};

// access mask
typedef int accessmask;
constexpr unsigned ACCESS_READ  = 0x1;
constexpr unsigned ACCESS_WRITE = 0x2;

} // namespace mds
} // namespace cohort

#endif /* COHORT_MDS_TYPES_H */
