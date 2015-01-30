// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OSDC_WRITEBACKHANDLER_H
#define CEPH_OSDC_WRITEBACKHANDLER_H

#include "include/Context.h"
#include "include/types.h"
#include "osd/osd_types.h"

class WritebackHandler {
 public:
  WritebackHandler() {}
  virtual ~WritebackHandler() {}

  virtual void read(const oid& obj, const boost::uuids::uuid& volume,
		    uint64_t off, uint64_t len, bufferlist *pbl,
		    uint64_t trunc_size, uint32_t trunc_seq,
		    Context *onfinish) = 0;
  /**
   * check if a given extent read result may change due to a write
   *
   * Check if the content we see at the given read offset may change
   * due to a write to this object.
   *
   * @param obj object
   * @param read_off read offset
   * @param read_len read length
   */
  virtual bool may_copy_on_write(const oid& obj, uint64_t read_off,
				 uint64_t read_len) = 0;
  virtual ceph_tid_t write(const oid& obj, const boost::uuids::uuid& volume,
			   uint64_t off, uint64_t len,
			   const bufferlist &bl, ceph::real_time mtime,
			   uint64_t trunc_size, uint32_t trunc_seq,
			   Context *oncommit) = 0;
  virtual ceph_tid_t lock(const oid& obj, const boost::uuids::uuid& volume,
			  int op, int flags, Context *onack, Context *oncommit) {
    assert(0 == "this WritebackHandler does not support the lock operation");
  }
};

#endif
