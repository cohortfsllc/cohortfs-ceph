// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_TEST_OSDC_FAKEWRITEBACK_H
#define CEPH_TEST_OSDC_FAKEWRITEBACK_H

#include <atomic>
#include "include/Context.h"
#include "include/types.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

class Finisher;
class Mutex;

class FakeWriteback : public WritebackHandler {
public:
  FakeWriteback(CephContext *cct, Mutex *lock, uint64_t delay_ns);
  virtual ~FakeWriteback();

  virtual void read(const object_t& oid, const boost::uuids::uuid& volume,
		    uint64_t off, uint64_t len,
		    bufferlist *pbl, uint64_t trunc_size,  uint32_t trunc_seq,
		    Context *onfinish);

  virtual ceph_tid_t write(const object_t& oid, const boost::uuids::uuid& volume,
			   uint64_t off, uint64_t len,
			   const bufferlist &bl,
			   utime_t mtime, uint64_t trunc_size,
			   uint32_t trunc_seq, Context *oncommit);

  virtual bool may_copy_on_write(const object_t&, uint64_t, uint64_t);
private:
  CephContext *m_cct;
  Mutex *m_lock;
  uint64_t m_delay_ns;
  std::atomic<ceph_tid_t> m_tid;
  Finisher *m_finisher;
};

#endif
