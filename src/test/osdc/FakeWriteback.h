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
  FakeWriteback(CephContext *cct, std::mutex *lock, ceph::timespan delay);
  virtual ~FakeWriteback();

  virtual void read(const oid& obj, const boost::uuids::uuid& volume,
		    uint64_t off, uint64_t len,
		    bufferlist *pbl, uint64_t trunc_size, uint32_t trunc_seq,
		    Context *onfinish);

  virtual ceph_tid_t write(const oid& obj, const boost::uuids::uuid& volume,
			   uint64_t off, uint64_t len, const bufferlist &bl,
			   ceph::real_time mtime, uint64_t trunc_size,
			   uint32_t trunc_seq, Context *oncommit);

  virtual bool may_copy_on_write(const oid&, uint64_t, uint64_t);
private:
  CephContext *m_cct;
  std::mutex *m_lock;
  ceph::timespan m_delay;
  std::atomic<ceph_tid_t> m_tid;
  Finisher *m_finisher;
};

#endif
