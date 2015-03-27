// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser Generansactionl Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_OPCONTEXT_H
#define CEPH_OSD_OPCONTEXT_H

#include "osd_types.h"
#include "os/Transaction.h"
#include "messages/MOSDOpReply.h"

class OpRequest;
class ObjectContext;
typedef boost::intrusive_ptr<ObjectContext> ObjectContextRef;

/*
 * Capture all object state associated with an in-progress read or write.
 */
struct OpContext {
  OpRequest *op;
  ObjectState new_obs;  // resulting ObjectState
  object_stat_sum_t delta_stats;

  bool modify; // (force) modification (even if op_t is empty)
  bool user_modify; // user-visible modification

  struct WatchesNotifies {
    // side effects
    vector<watch_info_t> watch_connects;
    vector<watch_info_t> watch_disconnects;
    vector<notify_info_t> notifies;
    struct NotifyAck {
      boost::optional<uint64_t> watch_cookie;
      uint64_t notify_id;
      NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
      NotifyAck(uint64_t notify_id, uint64_t cookie)
          : watch_cookie(cookie), notify_id(notify_id) {}
    };
    vector<NotifyAck> notify_acks;
  };
  WatchesNotifies* watches_notifies;

  ceph::real_time mtime;
  eversion_t at_version;       // vol's current version pointer
  version_t user_at_version;   // vol's current user version pointer

  int current_osd_subop_num;

  Transaction op_t;

  interval_set<uint64_t> modified_ranges;
  ObjectContextRef obc;
  map<hoid_t,ObjectContextRef> src_obc;

  int data_off; // FIXME: may want to kill this msgr hint off at some point!

  MOSDOpReply* reply;

  ceph::mono_time readable_stamp;  // when applied on all replicas
  OSDVol* vol;

  int num_read;    ///< count read ops
  int num_write;   ///< count update ops

  // pending async reads <off, len> -> <outbl, outr>
  list<pair<pair<uint64_t, uint64_t>,
      pair<bufferlist*, Context*> > > pending_async_reads;
  int async_read_result;
  unsigned inflightreads;
  friend struct OnReadComplete;

  bool has_watches() {
    return (!!watches_notifies);
  }
  WatchesNotifies* get_watches() {
    if (! watches_notifies)
      watches_notifies = new WatchesNotifies();
    return watches_notifies;
  }
  void start_async_reads(OSDVol* vol);
  void finish_read(OSDVol* vol);
  bool async_reads_complete() {
    return inflightreads == 0;
  }

  ObjectModDesc mod_desc;

  enum { W_LOCK, R_LOCK, NONE } lock_to_release;

  Context* on_finish;

  OpContext(const OpContext& other);
  const OpContext& operator=(const OpContext& other);

  OpContext()
    : modify(false), user_modify(false),
      watches_notifies(nullptr),
      user_at_version(0),
      current_osd_subop_num(0),
      data_off(0), reply(nullptr), vol(nullptr),
      num_read(0),
      num_write(0),
      async_read_result(0),
      inflightreads(0),
      lock_to_release(NONE),
      on_finish(nullptr)
  {}

  ~OpContext() {
    assert(lock_to_release == NONE);
    if (reply)
      reply->put();
    for (list<pair<pair<uint64_t, uint64_t>,
         pair<bufferlist*, Context*> > >::iterator i =
         pending_async_reads.begin();
         i != pending_async_reads.end();
         pending_async_reads.erase(i++)) {
      delete i->second.second;
    }
    if (watches_notifies)
      delete watches_notifies;
    assert(on_finish == NULL);
  }
  void finish(int r) {
    if (on_finish) {
      on_finish->complete(r);
      on_finish = NULL;
    }
  }
};

#endif // CEPH_OSD_OPCONTEXT_H
