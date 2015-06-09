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


#ifndef CEPH_MOSDOPREPLY_H
#define CEPH_MOSDOPREPLY_H

#include "MOSDOp.h"
#include "common/errno.h"
#include "common/freelist.h"
#include "include/cohort_error.h"

#define OSDOPREPLY_FREELIST 1024

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOpReply : public Message {
#if OSDOPREPLY_FREELIST
 private:
  typedef cohort::CharArrayAlloc<MOSDOpReply> Alloc;
  typedef cohort::FreeList<MOSDOpReply, Alloc> FreeList;
  static Alloc alloc;
  static FreeList freelist;
 public:
  static void *operator new(size_t num_bytes) {
    return freelist.alloc();
  }
  void operator delete(void *p) {
    return freelist.free(static_cast<MOSDOpReply*>(p));
  }
#endif
 private:
  static const int HEAD_VERSION = 6;
  static const int COMPAT_VERSION = 2;

  oid_t oid;
  boost::uuids::uuid volume;
  vector<OSDOp> ops;
  int64_t flags;
  std::error_code result;
  eversion_t bad_replay_version;
  eversion_t replay_version;
  version_t user_version;
  epoch_t osdmap_epoch;
  int32_t retry_attempt;

public:
  oid_t get_oid() const { return oid; }
  const boost::uuids::uuid& get_volume() const { return volume; }
  int      get_flags() const { return flags; }

  bool     is_ondisk() const { return get_flags() & CEPH_OSD_FLAG_ONDISK; }
  bool     is_onnvram() const { return get_flags() & CEPH_OSD_FLAG_ONNVRAM; }

  std::error_code get_result() const { return result; }
  eversion_t get_replay_version() const { return replay_version; }
  version_t get_user_version() const { return user_version; }

  void set_result(std::error_code r) { result = r; }

  void set_reply_versions(eversion_t v, version_t uv) {
    replay_version = v;
    user_version = uv;
    bad_replay_version = v;
    if (uv) {
      bad_replay_version.version = uv;
    }
  }

  /* Don't fill in replay_version for non-write ops */
  void set_enoent_reply_versions(eversion_t v, version_t uv) {
    user_version = uv;
    bad_replay_version = v;
  }

  void add_flags(int f) { flags |= f; }

  void claim_op_out_data(vector<OSDOp>& o) {
    assert(ops.size() == o.size());
    for (unsigned i = 0; i < o.size(); i++) {
      ops[i].outdata.claim(o[i].outdata);
    }
  }
  void claim_ops(vector<OSDOp>& o) {
    o.swap(ops);
  }

  /**
   * get retry attempt
   *
   * If we don't know the attempt (because the server is old), return -1.
   */
  int get_retry_attempt() const {
    return retry_attempt;
  }

  // osdmap
  epoch_t get_map_epoch() const { return osdmap_epoch; }

public:
  MOSDOpReply()
    : Message(CEPH_MSG_OSD_OPREPLY, HEAD_VERSION, COMPAT_VERSION) { }
  MOSDOpReply(MOSDOp *req, std::error_code r, epoch_t e, int acktype,
	      bool ignore_out_data)
    : Message(CEPH_MSG_OSD_OPREPLY, HEAD_VERSION, COMPAT_VERSION) {
    set_tid(req->get_tid());
    ops = req->ops;
    result = r;
    flags =
      (req->flags & ~(CEPH_OSD_FLAG_ONDISK|CEPH_OSD_FLAG_ONNVRAM|CEPH_OSD_FLAG_ACK)) | acktype;
    oid = req->oid;
    volume = req->volume;
    osdmap_epoch = e;
    user_version = 0;
    retry_attempt = req->get_retry_attempt();

    // zero out ops payload_len and possibly out data
    for (unsigned i = 0; i < ops.size(); i++) {
      ops[i].op.payload_len = 0;
      if (ignore_out_data)
	ops[i].outdata.clear();
    }
  }
private:
  ~MOSDOpReply() {}

public:
  virtual void encode_payload(uint64_t features) {

    OSDOp::merge_osd_op_vector_out_data(ops, data);

    ::encode(oid, payload);
    ::encode(volume, payload);
    ::encode(flags, payload);
    ::encode(result, payload);
    ::encode(bad_replay_version, payload);
    ::encode(osdmap_epoch, payload);

    uint32_t num_ops = ops.size();
    ::encode(num_ops, payload);
    for (unsigned i = 0; i < num_ops; i++)
      ::encode(ops[i].op, payload);

    ::encode(retry_attempt, payload);

    for (unsigned i = 0; i < num_ops; i++)
      ::encode(ops[i].rval, payload);

    ::encode(replay_version, payload);
    ::encode(user_version, payload);
    encode_trace(payload);

    trace.keyval("Id", get_tid());
    trace.keyval("Result", result.value());
  }
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(oid, p);
    ::decode(volume, p);
    ::decode(flags, p);
    ::decode(result, p);
    ::decode(bad_replay_version, p);
    ::decode(osdmap_epoch, p);

    uint32_t num_ops = ops.size();
    ::decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      ::decode(ops[i].op, p);

    ::decode(retry_attempt, p);

    for (unsigned i = 0; i < num_ops; ++i)
      ::decode(ops[i].rval, p);

    OSDOp::split_osd_op_vector_out_data(ops, data);
    ::decode(replay_version, p);
    ::decode(user_version, p);
    decode_trace(p, "MOSDOpReply", true);

    trace.keyval("Id", get_tid());
    trace.keyval("Result", result.value());
  }

  const char *get_type_name() const { return "osd_op_reply"; }

  void print(ostream& out) const {
    out << "osd_op_reply(" << get_tid()
	<< " " << oid << " " << ops
	<< " v" << get_replay_version()
	<< " uv" << get_user_version();
    if (is_ondisk())
      out << " ondisk";
    else if (is_onnvram())
      out << " onnvram";
    else
      out << " ack";
    out << " = " << get_result();
    if (get_result()) {
      out << " (" << get_result() << ")";
    }
    out << ")";
  }

};


#endif
