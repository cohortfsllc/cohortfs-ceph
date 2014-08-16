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

#ifndef CEPH_FLATINDEX_H
#define CEPH_FLATINDEX_H

#include <string>
#include <map>
#include <set>
#include <vector>

#include "CollectionIndex.h"

/**
 * FlatIndex implements the collection layout prior to CollectionIndex
 *
 * This class should only be used for converting old filestores.
 */
class FlatIndex : public CollectionIndex {
  std::weak_ptr<CollectionIndex> self_ref;
  string base_path;
  coll_t collection;
public:
  FlatIndex(coll_t collection, string base_path) : base_path(base_path),
						   collection(collection) {}

  /// @see CollectionIndex
  uint32_t collection_version() { return FLAT_INDEX_TAG; }

  coll_t coll() const { return collection; }

  /// @see CollectionIndex
  void set_ref(std::shared_ptr<CollectionIndex> ref);

  /// @see CollectionIndex
  int cleanup();

  /// @see CollectionIndex
  int init();

  /// @see CollectionIndex
  int created(
    const hobject_t &oid,
    const char *path
    );

  /// @see CollectionIndex
  int unlink(
    const hobject_t &oid
    );

  /// @see CollectionIndex
  int lookup(
    const hobject_t &oid,
    IndexedPath *path,
    int *exist
    );

  /// @see CollectionIndex
  int collection_list(
    vector<hobject_t> *ls
    );

  /// @see CollectionIndex
  int collection_list_partial(
    const hobject_t &start,
    int min_count,
    int max_count,
    vector<hobject_t> *ls,
    hobject_t *next
    );
};

#endif
