// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cassert>
#include <mutex>
#include <thread>

#include <errno.h>
#include <time.h>

#include "include/ceph_time.h"
#include "common/debug.h"
#include "common/Finisher.h"

#include "FakeWriteback.h"

#define dout_subsys ceph_subsys_objectcacher
#undef dout_prefix
#define dout_prefix *_dout << "FakeWriteback(" << this << ") "

typedef std::lock_guard<std::mutex> lock_guard;
typedef std::unique_lock<std::mutex> unique_lock;

class C_Delay : public Context {
  CephContext *m_cct;
  Context *m_con;
  ceph::timespan m_delay;
  std::mutex *m_lock;
  bufferlist *m_bl;
  uint64_t m_off;

public:
  C_Delay(CephContext *cct, Context *c, std::mutex *lock, uint64_t off,
	  bufferlist *pbl, ceph::timespan delay)
    : m_cct(cct), m_con(c), m_delay(delay), m_lock(lock), m_bl(pbl),
      m_off(off) {}
  void finish(int r) {
    std::this_thread::sleep_for(m_delay);
    if (m_bl) {
      buffer::ptr bp(r);
      bp.zero();
      m_bl->append(bp);
      ldout(m_cct, 20) << "finished read " << m_off << "~" << r << dendl;
    }
    unique_lock ml(*m_lock);
    m_con->complete(r);
    ml.unlock();
  }
};

FakeWriteback::FakeWriteback(CephContext *cct, std::mutex* lock,
			     ceph::timespan delay)
  : m_cct(cct), m_lock(lock), m_delay(delay)
{
  m_finisher = new Finisher(cct);
  m_finisher->start();
}

FakeWriteback::~FakeWriteback()
{
  m_finisher->stop();
  delete m_finisher;
}

void FakeWriteback::read(const oid& obj,
			 const boost::uuids::uuid& vol,
			 uint64_t off, uint64_t len,
			 bufferlist *pbl, uint64_t trunc_size,
			 uint32_t trunc_seq, Context *onfinish)
{
  C_Delay *wrapper = new C_Delay(m_cct, onfinish, m_lock, off, pbl, m_delay);
  m_finisher->queue(wrapper, len);
}

ceph_tid_t FakeWriteback::write(const oid& obj,
				const boost::uuids::uuid& vol,
				uint64_t off, uint64_t len,
				const bufferlist &bl, ceph::real_time mtime,
				uint64_t trunc_size, uint32_t trunc_seq,
				Context *oncommit)
{
  C_Delay *wrapper = new C_Delay(m_cct, oncommit, m_lock, off, NULL, m_delay);
  m_finisher->queue(wrapper, 0);
  return ++m_tid;
}

bool FakeWriteback::may_copy_on_write(const oid&, uint64_t, uint64_t)
{
  return false;
}
