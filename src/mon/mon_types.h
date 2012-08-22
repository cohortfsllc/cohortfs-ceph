// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MON_TYPES_H
#define CEPH_MON_TYPES_H

#define PAXOS_PGMAP      0  // before osd, for pg kick to behave
#define PAXOS_MDSMAP     1
#define PAXOS_OSDMAP     2
#define PAXOS_LOG        3
#define PAXOS_MONMAP     4
#define PAXOS_AUTH       5
#define PAXOS_VOLMAP     6
#define PAXOS_NUM        7

inline const char *get_paxos_name(int p) {
  switch (p) {
  case PAXOS_MDSMAP: return "mdsmap";
  case PAXOS_MONMAP: return "monmap";
  case PAXOS_OSDMAP: return "osdmap";
  case PAXOS_PGMAP: return "pgmap";
  case PAXOS_LOG: return "logm";
  case PAXOS_AUTH: return "auth";
  case PAXOS_VOLMAP: return "volmap";
  default: assert(0); return 0;
  }
}

#define CEPH_MON_ONDISK_MAGIC "ceph mon volume v012"

#endif
