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


#ifndef CEPH_CLIENT_READDIR_WORKER_H
#define CEPH_CLIENT_READDIR_WORKER_H

#include "include/types.h"
#include "common/Thread.h"
#include "common/Cond.h"

class CephContext;
class Client;
class Mutex;
class MetaRequest;
struct MetaSession;
struct DirReader;
struct DirStripeReader;

class ReaddirWorker : protected Thread {
private:
  Client *client;
  CephContext *cct;
  Mutex &mutex;
  Cond cond;
  bool shutdown;

  epoch_t mdsmap_epoch; // current epoch if waiting for new MDSMap
  bool cond_done_ignored; // passed to C_Cond() but otherwise ignored
  int cond_result_ignored; // passed to C_Cond() but otherwise ignored

  struct request_info {
    MetaRequest *request;
    DirReader *dir;
    DirStripeReader *stripe;
    uint32_t chunk;
    MetaSession *opening_session;
  };
  typedef list<request_info> req_list;

  // requests waiting to be sent
  req_list waiting_to_send;

  bool send_request(request_info &req);

  // sent requests waiting for a reply
  req_list waiting_for_reply;

  bool handle_reply(request_info &req);

  MetaRequest* create_request(DirReader *dir, DirStripeReader *stripe,
			      uint32_t max_entries) const;
  void* entry();

public:
  ReaddirWorker(Client *client);

  // functions called from an external thread; expected to hold client_lock

  void read_stripe(DirReader *dir, DirStripeReader *stripe);
  void read_chunk(DirReader *dir, uint32_t chunk_index);

  void stripe_readahead(DirReader *dir, DirStripeReader *stripe,
		        uint32_t chunk_index);

  void stop();
};

#endif
