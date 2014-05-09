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

#ifndef CEPH_MDS_ETABLESERVER_H
#define CEPH_MDS_ETABLESERVER_H

#include "common/config.h"
#include "include/types.h"

#include "../mds_table_types.h"
#include "../LogEvent.h"

struct ETableServer : public LogEvent {
  uint16_t table;
  int16_t op;
  uint64_t reqid;
  int32_t bymds;
  bufferlist mutation;
  version_t tid;
  version_t version;

  ETableServer() : LogEvent(EVENT_TABLESERVER), table(0), op(0),
		   reqid(0), bymds(0), tid(0), version(0) { }
  ETableServer(int t, int o, uint64_t ri, int m, version_t ti, version_t v) :
    LogEvent(EVENT_TABLESERVER),
    table(t), op(o), reqid(ri), bymds(m), tid(ti), version(v) { }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ETableServer*>& ls);

  void print(ostream& out) const {
    out << "ETableServer " << get_mdstable_name(table) 
	<< " " << get_mdstableserver_opname(op);
    if (reqid) out << " reqid " << reqid;
    if (bymds >= 0) out << " mds." << bymds;
    if (tid) out << " tid " << tid;
    if (version) out << " version " << version;
    if (mutation.length()) out << " mutation=" << mutation.length() << " bytes";
  }  

  void update_segment();
  void replay(MDS *mds);  
};

#endif
