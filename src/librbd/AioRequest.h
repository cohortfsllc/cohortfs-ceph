// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOREQUEST_H
#define CEPH_LIBRBD_AIOREQUEST_H

#include <map>

#include "include/buffer.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"

namespace librbd {

  struct AioCompletion;
  struct ImageCtx;

  /**
   * This class represents an I/O operation to a single RBD data object.
   * Its subclasses encapsulate logic for dealing with special cases
   * for I/O due to layering.
   */
  class AioRequest
  {
  public:
    AioRequest();
    AioRequest(ImageCtx *ictx, const std::string &oid,
	       uint64_t objectno, uint64_t off, uint64_t len,
	       Context *completion,
	       bool hide_enoent);
    virtual ~AioRequest();

    void complete(int r)
    {
      if (should_complete(r)) {
	if (m_hide_enoent && r == -ENOENT)
	  r = 0;
	m_completion->complete(r);
	delete this;
      }
    }

    virtual bool should_complete(int r) = 0;
    virtual int send() = 0;

  protected:
    ImageCtx *m_ictx;
    librados::IoCtx *m_ioctx;
    std::string m_oid;
    uint64_t m_object_no, m_object_off, m_object_len;
    Context *m_completion;
    ceph::bufferlist m_read_data;
    bool m_hide_enoent;
  };

  class AioRead : public AioRequest {
  public:
    AioRead(ImageCtx *ictx, const std::string &oid,
	    uint64_t objectno, uint64_t offset, uint64_t len,
	    vector<pair<uint64_t,uint64_t> >& be,
	    bool sparse,
	    Context *completion)
      : AioRequest(ictx, oid, objectno, offset, len, completion,
		   false),
	m_buffer_extents(be),
	m_sparse(sparse) {
    }
    virtual ~AioRead() {}
    virtual bool should_complete(int r);
    virtual int send();

    ceph::bufferlist &data() {
      return m_read_data;
    }
    std::map<uint64_t, uint64_t> m_ext_map;

    friend class C_AioRead;

  private:
    vector<pair<uint64_t,uint64_t> > m_buffer_extents;
    bool m_sparse;
  };

  class AbstractWrite : public AioRequest {
  public:
    AbstractWrite();
    AbstractWrite(ImageCtx *ictx, const std::string &oid,
		  uint64_t object_no, uint64_t object_off, uint64_t len,
		  vector<pair<uint64_t,uint64_t> >& objectx,
		  Context *completion,
		  bool hide_enoent);
    virtual ~AbstractWrite() {}
    virtual bool should_complete(int r);
    virtual int send();

  protected:
    vector<pair<uint64_t,uint64_t> > m_object_image_extents;
    librados::ObjectWriteOperation m_write;
  };

  class AioWrite : public AbstractWrite {
  public:
    AioWrite(ImageCtx *ictx, const std::string &oid,
	     uint64_t object_no, uint64_t object_off,
	     vector<pair<uint64_t,uint64_t> >& objectx,
	     const ceph::bufferlist &data,
	     Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, object_off, data.length(),
		      objectx,
		      completion, false),
	m_write_data(data) {
      add_write_ops(m_write);
    }
    virtual ~AioWrite() {}

  private:
    void add_write_ops(librados::ObjectWriteOperation &wr);
    ceph::bufferlist m_write_data;
  };

  class AioRemove : public AbstractWrite {
  public:
    AioRemove(ImageCtx *ictx, const std::string &oid,
	      uint64_t object_no,
	      vector<pair<uint64_t,uint64_t> >& objectx,
	      Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, 0, 0,
		      objectx,
		      completion,
		      true) {
      m_write.remove();
    }
    virtual ~AioRemove() {}
  };

  class AioTruncate : public AbstractWrite {
  public:
    AioTruncate(ImageCtx *ictx, const std::string &oid,
		uint64_t object_no, uint64_t object_off,
		vector<pair<uint64_t,uint64_t> >& objectx,
		Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, object_off, 0,
		      objectx,
		      completion,
		      true) {
      m_write.truncate(object_off);
    }
    virtual ~AioTruncate() {}

  };

  class AioZero : public AbstractWrite {
  public:
    AioZero(ImageCtx *ictx, const std::string &oid,
	    uint64_t object_no, uint64_t object_off, uint64_t object_len,
	    vector<pair<uint64_t,uint64_t> >& objectx,
	    Context *completion)
      : AbstractWrite(ictx, oid,
		      object_no, object_off, object_len,
		      objectx,
		      completion,
		      true) {
      m_write.zero(object_off, object_len);
    }
    virtual ~AioZero() {}
  };

}

#endif
