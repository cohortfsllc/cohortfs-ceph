// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OSDC_OBJECTERWRITEBACKHANDLER_H
#define CEPH_OSDC_OBJECTERWRITEBACKHANDLER_H

#include "osdc/Objecter.h"
#include "osdc/WritebackHandler.h"

class ObjecterWriteback : public WritebackHandler {
public:
  ObjecterWriteback(Objecter *o) : m_objecter(o) {}
  virtual ~ObjecterWriteback() {}

  virtual void read(const object_t& oid,
		    const boost::uuids::uuid& volume,
		    uint64_t off, uint64_t len,
		    bufferlist *pbl, uint64_t trunc_size,  uint32_t trunc_seq,
		    Context *onfinish) {
    VolumeRef volref;
    m_objecter->osdmap->find_by_uuid(volume, volref);
    m_objecter->read_trunc(oid, volref, off, len, pbl, 0,
			   trunc_size, trunc_seq, onfinish);
  }

  virtual bool may_copy_on_write(const object_t& oid, uint64_t read_off,
				 uint64_t read_len) {
    return false;
  }

  virtual ceph_tid_t write(const object_t& oid,
			   const boost::uuids::uuid& volume,
		      uint64_t off, uint64_t len,
		      const bufferlist &bl, utime_t mtime, uint64_t trunc_size,
		      uint32_t trunc_seq, Context *oncommit) {
    VolumeRef volref;
    m_objecter->osdmap->find_by_uuid(volume, volref);
    return m_objecter->write_trunc(oid, volref, off, len, bl,
				   mtime, 0, trunc_size, trunc_seq, NULL,
				   oncommit);
  }

  virtual ceph_tid_t lock(const object_t& oid,
			  const boost::uuids::uuid& volume,
			  int op, int flags, Context *onack,
			  Context *oncommit) {
    VolumeRef volref;
    m_objecter->osdmap->find_by_uuid(volume, volref);
    return m_objecter->lock(oid, volref, op, flags, onack, oncommit);
  }

private:
  Objecter *m_objecter;
};

#endif
