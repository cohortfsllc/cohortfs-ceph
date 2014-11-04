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


#ifndef CEPH_MOSDOP_H
#define CEPH_MOSDOP_H

#include <boost/uuid/uuid_io.hpp>
#include "msg/Message.h"
#include "osd/osd_types.h"
#include "include/ceph_features.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class OSD;

class MOSDOp : public Message {

  static const int HEAD_VERSION = 4;
  static const int COMPAT_VERSION = 3;

private:
  uint32_t client_inc;
  uint32_t osdmap_epoch;
  uint32_t flags;
  utime_t mtime;
  eversion_t reassert_version;
  int32_t retry_attempt;   // 0 is first attempt.  -1 if we don't know.

  hobject_t oid;
  boost::uuids::uuid volume;
public:
  vector<OSDOp> ops;

  friend class MOSDOpReply;

  osd_reqid_t get_reqid() const {
    return osd_reqid_t(get_orig_source(),
		       client_inc,
		       header.tid);
  }
  int get_client_inc() { return client_inc; }
  ceph_tid_t get_client_tid() { return header.tid; }

  hobject_t& get_oid() { return oid; }

  const boost::uuids::uuid& get_volume() const {
    return volume;
  }

  epoch_t  get_map_epoch() { return osdmap_epoch; }

  eversion_t get_version() { return reassert_version; }

  utime_t get_mtime() { return mtime; }

  MOSDOp()
    : Message(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION) { }
  MOSDOp(int inc, long tid, hobject_t& _oid,
	 const boost::uuids::uuid& _volume,
	 epoch_t _osdmap_epoch, int _flags)
    : Message(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      client_inc(inc),
      osdmap_epoch(_osdmap_epoch), flags(_flags), retry_attempt(-1),
      oid(_oid), volume(_volume) {
    set_tid(tid);
  }
private:
  ~MOSDOp() {}

public:
  void set_version(eversion_t v) { reassert_version = v; }
  void set_mtime(utime_t mt) { mtime = mt; }

  // ops
  void add_simple_op(int o, uint64_t off, uint64_t len) {
    OSDOp osd_op;
    osd_op.op.op = o;
    osd_op.op.extent.offset = off;
    osd_op.op.extent.length = len;
    ops.push_back(osd_op);
  }
  void write(uint64_t off, uint64_t len, bufferlist& bl) {
    add_simple_op(CEPH_OSD_OP_WRITE, off, len);
    ops.back().indata.claim(bl);
    header.data_off = off;
  }
  void writefull(bufferlist& bl) {
    add_simple_op(CEPH_OSD_OP_WRITEFULL, 0, bl.length());
    ops.back().indata.claim(bl);
    header.data_off = 0;
  }
  void zero(uint64_t off, uint64_t len) {
    add_simple_op(CEPH_OSD_OP_ZERO, off, len);
  }
  void truncate(uint64_t off) {
    add_simple_op(CEPH_OSD_OP_TRUNCATE, off, 0);
  }
  void remove() {
    add_simple_op(CEPH_OSD_OP_DELETE, 0, 0);
  }

  void read(uint64_t off, uint64_t len) {
    add_simple_op(CEPH_OSD_OP_READ, off, len);
  }
  void stat() {
    add_simple_op(CEPH_OSD_OP_STAT, 0, 0);
  }

  // flags
  int get_flags() const { return flags; }

  bool wants_ack() const { return flags & CEPH_OSD_FLAG_ACK; }
  bool wants_ondisk() const { return flags & CEPH_OSD_FLAG_ONDISK; }
  bool wants_onnvram() const { return flags & CEPH_OSD_FLAG_ONNVRAM; }

  void set_want_ack(bool b) { flags |= CEPH_OSD_FLAG_ACK; }
  void set_want_onnvram(bool b) { flags |= CEPH_OSD_FLAG_ONNVRAM; }
  void set_want_ondisk(bool b) { flags |= CEPH_OSD_FLAG_ONDISK; }

  bool is_retry_attempt() const { return flags & CEPH_OSD_FLAG_RETRY; }
  void set_retry_attempt(unsigned a) {
    if (a)
      flags |= CEPH_OSD_FLAG_RETRY;
    else
      flags &= ~CEPH_OSD_FLAG_RETRY;
    retry_attempt = a;
  }

  /**
   * get retry attempt
   *
   * 0 is the first attempt.
   *
   * @return retry attempt, or -1 if we don't know
   */
  int get_retry_attempt() const {
    return retry_attempt;
  }

  // marshalling
  virtual void encode_payload(uint64_t features) {

    OSDOp::merge_osd_op_vector_in_data(ops, data);

    ::encode(client_inc, payload);
    ::encode(osdmap_epoch, payload);
    ::encode(flags, payload);
    ::encode(mtime, payload);
    ::encode(reassert_version, payload);

    ::encode(volume, payload);
    ::encode(oid, payload);

    uint16_t num_ops = ops.size();
    ::encode(num_ops, payload);
    for (unsigned i = 0; i < ops.size(); i++)
      ::encode(ops[i].op, payload);

    ::encode(retry_attempt, payload);
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    // new decode
    ::decode(client_inc, p);
    ::decode(osdmap_epoch, p);
    ::decode(flags, p);
    ::decode(mtime, p);
    ::decode(reassert_version, p);

    ::decode(volume, p);
    ::decode(oid, p);

    uint16_t num_ops;
    ::decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      ::decode(ops[i].op, p);

    ::decode(retry_attempt, p);
    OSDOp::split_osd_op_vector_in_data(ops, data);
  }

  void clear_buffers() {
    ops.clear();
  }

  const char *get_type_name() const { return "osd_op"; }
  void print(ostream& out) const {
    out << "osd_op(" << get_reqid();
    out << " ";
    out << volume << ":";
    out << oid;

    out << " " << ops;
    if (is_retry_attempt())
      out << " RETRY=" << get_retry_attempt();
    if (reassert_version != eversion_t())
      out << " reassert_version=" << reassert_version;
    out << " " << ceph_osd_flag_string(get_flags());
    out << " e" << osdmap_epoch;
    out << ")";
  }
};


#endif
