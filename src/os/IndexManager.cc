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

#include <map>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include <errno.h>
#include <mutex>
#include <condition_variable>

#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"

#include "IndexManager.h"
#include "FlatIndex.h"
#include "CollectionIndex.h"

#include "chain_xattr.h"

static int set_version(const char *path, uint32_t version) {
  bufferlist bl;
  ::encode(version, bl);
  return chain_setxattr(path, "user.cephos.collection_version", bl.c_str(),
		     bl.length());
}

void IndexManager::put_index(coll_t c) {
  unique_lock l(lock);
  assert(col_indices.count(c));
  col_indices.erase(c);
  cond.notify_all();
}

int IndexManager::init_index(coll_t c, const char *path, uint32_t version) {
  lock_guard l(lock);
  int r = set_version(path, version);
  if (r < 0)
    return r;
  FlatIndex index(c, path);
  return index.init();
}

int IndexManager::build_index(coll_t c, const char *path, Index *index) {
  // No need to check
  *index = Index(new FlatIndex(c, path),
		 RemoveOnDelete(c, this));
  return 0;
}

int IndexManager::get_index(coll_t c, const char *path, Index *index) {
  unique_lock l(lock);
  while (1) {
    if (!col_indices.count(c)) {
      int r = build_index(c, path, index);
      if (r < 0)
	return r;
      (*index)->set_ref(*index);
      col_indices[c] = (*index);
      break;
    } else {
      cond.wait(l);
    }
  }
  return 0;
}
