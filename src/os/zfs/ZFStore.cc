// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ZFStore.h"

static std::mutex mtx;
static std::atomic<bool> initialized;
std::atomic<uint32_t> ZFStore::n_instances;
lzfw_handle_t* ZFStore::zhd;

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "zfstore(" << path << ") "

/* Factory method */
ObjectStore* ZFStore_factory(CephContext* cct,
			     const std::string& data,
			     const std::string& journal)
{
  return new ZFStore(cct, data);
}

/* DLL machinery */
extern "C" {
  void* objectstore_dllinit()
  {
    return reinterpret_cast<void*>(ZFStore_factory);
  }
} /* extern "C" */

ZFStore::ZFStore(CephContext* cct, const std::string& path)
  : ObjectStore(cct, path)
{
  if (!initialized) {
    std::unique_lock<std::mutex> l(mtx);
    if (!initialized) {
      zhd = lzfw_init();
      if (!zhd) {
	dout(-1) << "lzfw_init() failed" << dendl;
      }
      initialized = true;
    }
  }
  ++n_instances;
}

ZFStore::~ZFStore()
{
  --n_instances;
}
