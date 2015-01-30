// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLS_STATELOG_TYPES_H
#define CEPH_CLS_STATELOG_TYPES_H

#include "include/ceph_time.h"
#include "include/encoding.h"
#include "include/types.h"

#include "common/Formatter.h"

class JSONObj;

struct cls_statelog_entry {
  string client_id;
  string op_id;
  string object;
  ceph::real_time timestamp;
  bufferlist data;
  uint32_t state; /* user defined state */

  cls_statelog_entry() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(client_id, bl);
    ::encode(op_id, bl);
    ::encode(object, bl);
    ::encode(timestamp, bl);
    ::encode(data, bl);
    ::encode(state, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(client_id, bl);
    ::decode(op_id, bl);
    ::decode(object, bl);
    ::decode(timestamp, bl);
    ::decode(data, bl);
    ::decode(state, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(cls_statelog_entry)


#endif


