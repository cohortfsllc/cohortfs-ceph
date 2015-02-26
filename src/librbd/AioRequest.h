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
    AioRequest(ImageCtx *ictx, const std::string &oid_t,
	       uint64_t off, uint64_t len,
	       Context *completion,
	       bool hide_enoent);
    virtual ~AioRequest();

    void complete(int r)
    {
      if (m_hide_enoent && r == -ENOENT)
	r = 0;
      m_completion->complete(r);
      delete this;
    }

    virtual int send() = 0;

  protected:
    ImageCtx *m_ictx;
    librados::IoCtx *m_ioctx;
    std::string m_oid;
    uint64_t m_off, m_len;
    Context *m_completion;
    ceph::bufferlist m_read_data;
    bool m_hide_enoent;
  };

  class AioRead : public AioRequest {
  public:
    AioRead(ImageCtx *ictx, const std::string &oid_t,
	    uint64_t offset, uint64_t len,
	    Context *completion)
      : AioRequest(ictx, oid_t, offset, len, completion,
		   false) {
    }
    virtual ~AioRead() {}
    virtual int send();

    ceph::bufferlist &data() {
      return m_read_data;
    }

    friend class C_AioRead;
  };

  class AbstractWrite : public AioRequest {
  public:
    AbstractWrite(ImageCtx *ictx, const std::string &oid_t,
		  uint64_t object_off, uint64_t len,
		  Context *completion,
		  bool hide_enoent);
    virtual ~AbstractWrite() {}
    virtual int send();

  protected:
    librados::ObjectWriteOperation m_write;
  };

  class AioWrite : public AbstractWrite {
  public:
    AioWrite(ImageCtx *ictx, const std::string &oid_t,
	     uint64_t object_off,
	     const ceph::bufferlist &data,
	     Context *completion)
      : AbstractWrite(ictx, oid_t,
		      object_off, data.length(),
		      completion, false),
	m_write_data(data) {
      m_write.write(m_off, m_write_data);

    }
    virtual ~AioWrite() {}

  private:
    void add_write_ops(librados::ObjectWriteOperation &wr);
    ceph::bufferlist m_write_data;
  };

  class AioRemove : public AbstractWrite {
  public:
    AioRemove(ImageCtx *ictx, const std::string &oid_t,
	      Context *completion)
      : AbstractWrite(ictx, oid_t,
		      0, 0,
		      completion,
		      true) {
      m_write.remove();
    }
    virtual ~AioRemove() {}
  };

  class AioTruncate : public AbstractWrite {
  public:
    AioTruncate(ImageCtx *ictx, const std::string &oid_t,
		uint64_t off, Context *completion)
      : AbstractWrite(ictx, oid_t,
		      off, 0,
		      completion,
		      true) { }
    virtual ~AioTruncate() {}
  };

  class AioZero : public AbstractWrite {
  public:
    AioZero(ImageCtx *ictx, const std::string &oid_t,
	    uint64_t object_off, uint64_t object_len,
	    Context *completion)
      : AbstractWrite(ictx, oid_t,
		      object_off, object_len,
		      completion,
		      true) {
      m_write.truncate(object_off);
    }
    virtual ~AioZero() {}
  };

}

#endif
