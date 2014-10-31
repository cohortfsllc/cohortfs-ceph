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

#ifndef OS_COLLECTIONINDEX_H
#define OS_COLLECTIONINDEX_H

#include <string>
#include <vector>

#include "osd/osd_types.h"
#include "common/oid.h"

/**
 * CollectionIndex provides an interface for manipulating indexed collections
 */
class CollectionIndex {
public:
  static const uint32_t FLAT_INDEX_TAG = 0;
  static const uint32_t HASH_INDEX_TAG = 1;
  static const uint32_t HASH_INDEX_TAG_2 = 2;
  static const uint32_t HOBJECT_WITH_POOL = 3;

  /**
   * For tracking Filestore collection versions.
   *
   * @return Collection version represented by the Index implementation
   */
  virtual uint32_t collection_version() = 0;

  /**
   * Returns the collection managed by this CollectionIndex
   */
  virtual coll_t coll() const = 0;

  /**
   * For setting the internal weak_ptr to a shared_ptr to this.
   *
   * @see IndexManager
   */
  virtual void set_ref(std::shared_ptr<CollectionIndex> ref) = 0;

  /**
   * Initializes the index.
   *
   * @return Error Code, 0 for success
   */
  virtual int init() = 0;

  /**
   * Cleanup before replaying journal
   *
   * Index implemenations may need to perform compound operations
   * which may leave the collection unstable if interupted.  cleanup
   * is called on mount to allow the CollectionIndex implementation
   * to stabilize.
   *
   * @see HashIndex
   * @return Error Code, 0 for success
   */
  virtual int cleanup() = 0;

  /**
   * Removes oid from the collection
   *
   * @return Error Code, 0 for success
   */
  virtual int unlink(
    const hoid_t& oid ///< [in] Object to remove
    ) = 0;

  /**
   * Moves objects matching <match> in the lsb <bits>
   *
   * dest and this must be the same subclass
   *
   * @return Error Code, 0 for success
   */
  virtual int split(
    uint32_t match,				//< [in] value to match
    uint32_t bits,				//< [in] bits to check
    std::shared_ptr<CollectionIndex> dest  //< [in] destination index
    ) { assert(0); return 0; }


  /// List contents of collection by hash
  virtual int collection_list_partial(
    const hoid_t& start, ///< [in] object at which to start
    int min_count, ///< [in] get at least min_count objects
    int max_count, ///< [in] return at most max_count objects
    vector<hoid_t>* ls,  ///< [out] Listed objects
    hoid_t *next ///< [out] Next object to list
    ) = 0;

  /// List contents of collection.
  virtual int collection_list(
    vector<hoid_t>* ls ///< [out] Listed Objects
    ) = 0;

  /// Call prior to removing directory
  virtual int prep_delete() { return 0; }

  /// Virtual destructor
  virtual ~CollectionIndex() {}
};

#endif
