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
 * Foundation.	See file COPYING.
 *
 */


#ifndef CEPH_MOSDMAP_H
#define CEPH_MOSDMAP_H

#include "msg/Message.h"
#include "osd/OSDMap.h"
#include "include/ceph_features.h"

class MOSDMap : public Message {

  static const int HEAD_VERSION = 3;

 public:
  boost::uuids::uuid fsid;
  map<epoch_t, bufferlist> maps;
  map<epoch_t, bufferlist> incremental_maps;
  epoch_t oldest_map, newest_map;

  epoch_t get_first() const {
    epoch_t e = 0;
    map<epoch_t, bufferlist>::const_iterator i = maps.begin();
    if (i != maps.end())  e = i->first;
    i = incremental_maps.begin();
    if (i != incremental_maps.end() &&
	(e == 0 || i->first < e)) e = i->first;
    return e;
  }
  epoch_t get_last() const {
    epoch_t e = 0;
    map<epoch_t, bufferlist>::const_reverse_iterator i = maps.rbegin();
    if (i != maps.rend())  e = i->first;
    i = incremental_maps.rbegin();
    if (i != incremental_maps.rend() &&
	(e == 0 || i->first > e)) e = i->first;
    return e;
  }
  epoch_t get_oldest() {
    return oldest_map;
  }
  epoch_t get_newest() {
    return newest_map;
  }


  MOSDMap() : Message(CEPH_MSG_OSD_MAP, HEAD_VERSION) { }
  MOSDMap(const boost::uuids::uuid &f, OSDMap *oc = 0)
    : Message(CEPH_MSG_OSD_MAP, HEAD_VERSION),
      fsid(f),
      oldest_map(0), newest_map(0) {
    if (oc)
      oc->encode(maps[oc->get_epoch()]);
  }
private:
  ~MOSDMap() {}

public:
  // marshalling
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(incremental_maps, p);
    ::decode(maps, p);
    ::decode(oldest_map, p);
    ::decode(newest_map, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
    ::encode(incremental_maps, payload);
    ::encode(maps, payload);
    ::encode(oldest_map, payload);
    ::encode(newest_map, payload);
  }

  const char *get_type_name() const { return "omap"; }
  void print(ostream& out) const {
    out << "osd_map(" << get_first() << ".." << get_last();
    if (oldest_map || newest_map)
      out << " src has " << oldest_map << ".." << newest_map;
    out << ")";
  }
};

#endif
