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
	       uint64_t off, uint64_t len,
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
    uint64_t m_off, m_len;
    Context *m_completion;
    ceph::bufferlist m_read_data;
    bool m_hide_enoent;
  };

  class AioRead : public AioRequest {
  public:
    AioRead(ImageCtx *ictx, const std::string &oid,
	    uint64_t offset, uint64_t len,
	    Context *completion)
      : AioRequest(ictx, oid, offset, len, completion,
		   false) {
    }
    virtual ~AioRead() {}
    virtual bool should_complete(int r);
    virtual int send();

    ceph::bufferlist &data() {
      return m_read_data;
    }

    friend class C_AioRead;
  };

  class AbstractWrite : public AioRequest {
  public:
    AbstractWrite();
    AbstractWrite(ImageCtx *ictx, const std::string &oid,
		  uint64_t object_off, uint64_t len,
		  Context *completion,
		  bool hide_enoent);
    virtual ~AbstractWrite() {}
    virtual bool should_complete(int r);
    virtual int send() = 0;
  };

  class AioWrite : public AbstractWrite {
  public:
    AioWrite(ImageCtx *ictx, const std::string &oid,
	     uint64_t object_off,
	     const ceph::bufferlist &data,
	     Context *completion)
      : AbstractWrite(ictx, oid,
		      data.length(), object_off,
		      completion, false),
	m_write_data(data) { }
    virtual ~AioWrite() {}
    virtual int send();

  private:
    ceph::bufferlist m_write_data;
  };

  class AioRemove : public AbstractWrite {
  public:
    AioRemove(ImageCtx *ictx, const std::string &oid,
	      Context *completion)
      : AbstractWrite(ictx, oid,
		      0, 0,
		      completion,
		      true) { }
    virtual ~AioRemove() {}
    virtual int send();
  };

  class AioTruncate : public AbstractWrite {
  public:
    AioTruncate(ImageCtx *ictx, const std::string &oid,
		uint64_t off, Context *completion)
      : AbstractWrite(ictx, oid,
		      off, 0,
		      completion,
		      true) { }
    virtual ~AioTruncate() {}
    virtual int send();
  };

  class AioZero : public AbstractWrite {
  public:
    AioZero(ImageCtx *ictx, const std::string &oid,
	    uint64_t object_off, uint64_t object_len,
	    Context *completion)
      : AbstractWrite(ictx, oid,
		      object_off, object_len,
		      completion,
		      true) { }
    virtual ~AioZero() {}
    virtual int send();
  };

}

#endif
