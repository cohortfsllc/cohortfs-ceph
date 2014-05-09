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


#ifndef CEPH_MOSDPGCREATE_H
#define CEPH_MOSDPGCREATE_H

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * PGCreate - instruct an OSD to create a pg, if it doesn't already exist
 */

struct MOSDPGCreate : public Message {

  const static int HEAD_VERSION = 2;

  version_t          epoch;
  map<pg_t,pg_create_t> mkpg;

  MOSDPGCreate()
    : Message(MSG_OSD_PG_CREATE, HEAD_VERSION) {}
  MOSDPGCreate(epoch_t e)
    : Message(MSG_OSD_PG_CREATE, HEAD_VERSION),
      epoch(e) { }
private:
  ~MOSDPGCreate() {}

public:  
  const char *get_type_name() const { return "pg_create"; }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);
    ::encode(mkpg, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    if (header.version >= 2) {
      ::decode(mkpg, p);
    } else {
      uint32_t n;
      ::decode(n, p);
      while (n--) {
	pg_t pgid;
	epoch_t created;   // epoch pg created
	pg_t parent;       // split from parent (if != pg_t())
	int32_t split_bits;
	::decode(pgid, p);
	::decode(created, p);
	::decode(parent, p);
	::decode(split_bits, p);
	mkpg[pgid] = pg_create_t(created, parent, split_bits);
      }
    }
  }

  void print(ostream& out) const {
    out << "osd_pg_create(";
    for (map<pg_t,pg_create_t>::const_iterator i = mkpg.begin();
         i != mkpg.end();
         ++i) {
      out << "pg" << i->first << "," << i->second.created << "; ";
    }
    out << ")";
  }
};

#endif
