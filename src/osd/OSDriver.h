// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef OSDRIVER_H
#define OSDRIVER_H

#include <string>
#include <set>
#include <utility>
#include <string.h>

#include "common/map_cacher.hpp"
#include "common/oid.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "os/ObjectStore.h"

class OSDriver : public MapCacher::StoreDriver<std::string, bufferlist> {
  ObjectStore *os;
  coll_t cid;
  oid_t hoid;

public:
  class OSTransaction : public MapCacher::Transaction<std::string, bufferlist> {
    friend class OSDriver;
    coll_t cid;
    oid_t hoid;
    ObjectStore::Transaction *t;
    OSTransaction(
      coll_t cid,
      const oid_t &hoid,
      ObjectStore::Transaction *t)
      : cid(cid), hoid(hoid), t(t) {}
  public:
    void set_keys(
      const std::map<std::string, bufferlist> &to_set) {
      t->omap_setkeys(cid, hoid, to_set);
    }
    void remove_keys(
      const std::set<std::string> &to_remove) {
      t->omap_rmkeys(cid, hoid, to_remove);
    }
    void add_callback(
      Context *c) {
      t->register_on_applied(c);
    }
  };

  OSTransaction get_transaction(
    ObjectStore::Transaction *t) {
    return OSTransaction(cid, hoid, t);
  }

  OSDriver(ObjectStore *os, coll_t cid) :
    os(os), cid(cid) {}
  int get_keys(
    const std::set<std::string> &keys,
    std::map<std::string, bufferlist> *out);
  int get_next(
    const std::string &key,
    pair<std::string, bufferlist> *next);
};

#endif
