// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

/*
 * NB This version IGNORES rbd_cache and behaves as if
 *  it were always false.  This is because that feature depends
 *  on objectcacher, which needs to be rewritten to be useful
 *  with cohort volumes.  Adam promised he'll do just that.
 *  Sometime.  Soon.  Just not yet.  -mdw 20150105.
 */

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "osdc/RadosClient.h"

#include "librbd/Image.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Image: "

namespace librbd {
  using namespace rados;
  using std::unique_ptr;

  const string Image::rbd_suffix = ".rbd";
  const char Image::header_text[] = "<<< Rados Block Device Image >>>\n";
  const char Image::header_signature[] = "RBD";
  const char Image::header_version[] = "001.005";

  Image::Image(RadosClient* _rc,
	       const AVolRef& v,
	       const string& name)
    : rc(_rc),
      read_only(false),
      empty(false),
      name(name),
      volume(v),
      header_oid(header_name(name)),
      image_oid(image_name(name))
  {
    memset(&header, 0, sizeof(header));
    read_header();
    size = header.image_size;
    f = new Flusher;
  }

  Image::Image(RadosClient* rados,
	       const AVolRef& v,
	       const string& name,
	       read_only_t)
    : Image(rados, v, name)
  {
    read_only = true;
  }

  bool Image::check_exists(RadosClient* rc,
			   const AVolRef& volume,
			   const string &name, uint64_t *size)
  {
    int r = rc->objecter->stat(header_name(name), volume, size, NULL);
    if (r == 0) {
      return true;
    } else if (r == -ENOENT) {
      return false;
    } else {
      throw std::error_condition(-r, std::generic_category());
    }
  }

  void Image::trim_image(uint64_t newsize)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    if (newsize < size) {

      int r = rc->objecter->trunc(image_oid, volume, size, 0);
      if (r < 0) {
	lderr(rc->cct) << "warning: failed to trim object : "
		       << cpp_strerror(r) << dendl;
      }
    }
  }

  void Image::init_rbd_header(rbd_obj_header_ondisk& ondisk, uint64_t size) {
    memset(&ondisk, 0, sizeof(ondisk));

    memcpy(&ondisk.text, header_text, sizeof(header_text));
    memcpy(&ondisk.signature, header_signature,
	   sizeof(header_signature));
    memcpy(&ondisk.version, header_version, sizeof(header_version));

    ondisk.image_size = size;
  }

  void Image::create(RadosClient* rc,
		     const AVolRef& volume,
		     const string& imgname,
		     uint64_t size)
  {
    ldout(rc->cct, 2) << "creating rbd image..." << dendl;
    struct rbd_obj_header_ondisk header;
    init_rbd_header(header, size);

    bufferlist bl;
    bl.append((const char *)&header, sizeof(header));

    string header_oid = header_name(imgname);
    int r = rc->objecter->write_full(header_oid, volume, bl);
    if (r < 0) {
      lderr(rc->cct) << "Error writing image header: " << cpp_strerror(r)
		     << dendl;
      throw std::error_condition(-r, std::generic_category());
    }

    ldout(rc->cct, 2) << "done." << dendl;
  }

  void Image::rename(RadosClient* rc,
		     const AVolRef& volume,
		     const string& srcname,
		     const string& dstname)
  {
    ldout(rc->cct, 20) << "rename " << volume << " " << srcname << " -> "
		       << dstname << dendl;

    uint64_t src_size;
    if (!check_exists(rc, volume, srcname, &src_size)) {
      throw std::make_error_condition(std::errc::no_such_file_or_directory);
    }

    string src_oid = header_name(srcname);
    string dst_oid = header_name(dstname);

    bufferlist databl;
    int r = rc->objecter->read(src_oid, volume, 0, src_size, &databl);
    if (r < 0) {
      lderr(rc->cct) << "error reading source object: " << src_oid << ": "
		     << cpp_strerror(r) << dendl;
      throw std::error_condition(-r, std::generic_category());
    }

    if (check_exists(rc, volume, dstname)) {
      lderr(rc->cct) << "rbd image " << dstname << " already exists" << dendl;
      throw std::make_error_condition(std::errc::file_exists);
    }

    unique_ptr<ObjOp> op = volume->op();
    op->create(true);
    op->write_full(databl);
    r = rc->objecter->mutate(dst_oid, volume, op);
    if (r < 0) {
      lderr(rc->cct) << "error writing destination object: " << dst_oid << ": "
		     << cpp_strerror(r) << dendl;
      throw std::error_condition(-r,
				 std::generic_category());
    }

    r = rc->objecter->remove(src_oid, volume);
    if (r < 0 && r != -ENOENT) {
      lderr(rc->cct) << "warning: couldn't remove old source object ("
		     << src_oid << ")" << dendl;
    }
  }

  void Image::read_header()
  {
    bufferlist header_bl;
    read_header_bl(header_bl);
    if (header_bl.length() < (int)sizeof(header))
      throw std::make_error_condition(std::errc::io_error);
    memcpy(&header, header_bl.c_str(), sizeof(header));
  }

  void Image::read_header_bl(bufferlist& header_bl) const
  {
    int r;
    uint64_t off = 0;
    size_t lenread;
    do {
      bufferlist bl;
      r = rc->objecter->read(header_oid, volume, off, volume->op_size(), &bl);
      if (r < 0)
	throw std::error_condition(-r, std::generic_category());
      lenread = bl.length();
      header_bl.claim_append(bl);
      off += r;
    } while (lenread == volume->op_size());

    if (memcmp(header_text, header_bl.c_str(), sizeof(header_text))) {
      lderr(rc->cct) << "unrecognized header format" << dendl;
     throw std::make_error_condition(std::errc::no_such_device_or_address);
    }
  }

  uint64_t Image::get_size() const
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    return size;
  }

  void Image::remove(RadosClient* rc,
		     const AVolRef& volume,
		     const string& imgname)
  {
    ldout(rc->cct, 20) << "remove " << imgname << dendl;

    ldout(rc->cct, 2) << "removing header..." << dendl;
    int r = rc->objecter->remove(header_name(imgname), volume);
    if (r < 0 && r != -ENOENT) {
      lderr(rc->cct) << "error removing header: " << cpp_strerror(-r) << dendl;
      throw std::error_condition(-r, std::generic_category());
    }

    r = rc->objecter->remove(image_name(imgname), volume);
    if (r < 0 && r != -ENOENT) {
      lderr(rc->cct) << "error removing image: " << cpp_strerror(-r) << dendl;
      throw std::error_condition(-r, std::generic_category());
    }

    ldout(rc->cct, 2) << "done." << dendl;
  }

  void Image::flush()
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    int r = f->flush();
    if (r < 0) {
      throw std::error_condition(-r,
				 std::generic_category());
    }
  }

  void Image::flush(op_callback&& cb)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    f->flush(std::move(cb));
  }

  void Image::resize(uint64_t newsize)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    if (read_only)
      throw std::make_error_condition(std::errc::read_only_file_system);

    if (newsize == size) {
      ldout(rc->cct, 2) << "no change in size (" << this->size << " -> "
			<< size << ")" << dendl;
      return;
    }

    if (newsize > size) {
      ldout(rc->cct, 2) << "expanding image " << this->size << " -> " << size
			<< dendl;
    } else {
      ldout(rc->cct, 2) << "shrinking image " << this->size << " -> " << size
			<< dendl;
      trim_image(newsize);
    }
    size = newsize;

    int r;
    // rewrite header
    bufferlist bl;
    header.image_size = size;
    bl.append((const char *)&header, sizeof(header));
    r = rc->objecter->write_full(header_oid, volume, bl);

    if (r < 0) {
      lderr(rc->cct) << "error writing header: " << cpp_strerror(-r) << dendl;
      throw std::error_condition(-r, std::generic_category());
    }
  }

  void Image::write_sync(uint64_t off, size_t len, const bufferlist& bl)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    if (read_only)
      throw std::make_error_condition(std::errc::read_only_file_system);

    clip_io(off, &len);

    int r = rc->objecter->write(image_oid, volume, off, len, bl);
    if (r < 0) {
      throw std::error_condition(-r, std::generic_category());
    }
  }

  void Image::write(uint64_t off, size_t len, const bufferlist& bl,
		    op_callback&& ack, op_callback&& safe)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    if (read_only)
      throw std::make_error_condition(std::errc::read_only_file_system);

    clip_io(off, &len);

    rc->objecter->write(image_oid, volume, off, len, bl,
			std::move(ack), f->completion(std::move(safe)));
  }

  void Image::read_sync(uint64_t off, size_t len, bufferlist* bl) const
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    clip_io(off, &len);

    int r = rc->objecter->read(image_oid, volume, off, len, bl);
    if (r < 0)
      throw std::error_condition(-r, std::generic_category());
    if (bl->length() < len) {
      bl->append_zero(len - bl->length());
    }
  }

  struct CB_Padder {
    size_t len;
    bufferlist* bl;
    op_callback cb;

    CB_Padder(size_t _len, bufferlist* _bl, op_callback&& _cb)
      : len(_len), bl(_bl) {
      cb.swap(_cb);
    }

    void operator()(int r) {
      if ((r >= 0) && (bl->length() < len)) {
	bl->append_zero(len - bl->length());
      }
      cb(r);
    }
  };

  void Image::read(uint64_t off, size_t len, bufferlist* bl,
		   op_callback&& cb) const
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    clip_io(off, &len);

    rc->objecter->read(image_oid, volume, off, len, bl,
		       CB_Padder(len, bl, std::move(cb)));
  }

  struct RCB_Padder {
    read_callback cb;
    size_t len;

    RCB_Padder(size_t _len, read_callback&& _cb) : len(_len) {
      cb.swap(_cb);
    }

    void operator()(int r, bufferlist&& bl) {
      if ((r >= 0) && (bl.length() < len)) {
	bl.append_zero(len - bl.length());
      }
      cb(r, std::move(bl));
    }
  };

  void Image::read(uint64_t off, size_t len, read_callback&& cb) const
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    clip_io(off, &len);

    rc->objecter->read(image_oid, volume, off, len,
		       RCB_Padder(len, std::move(cb)));
  }

  void Image::discard(uint64_t off, size_t len, op_callback&& cb)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    if (read_only)
      throw std::make_error_condition(std::errc::read_only_file_system);

    ldout(rc->cct, 20) << "aio_discard " << *this << " off = " << off
		       << " len = " << len << dendl;

    clip_io(off, &len);

    if (off + len <= size) {
      rc->objecter->trunc(image_oid, volume, off, 0, nullptr,
			  f->completion(std::move(cb)));
    } else {
      rc->objecter->zero(image_oid, volume, off, len, nullptr,
			 f->completion(std::move(cb)));
    }
  }

  void Image::discard_sync(uint64_t off, size_t len)
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    if (read_only)
      throw std::make_error_condition(std::errc::read_only_file_system);

    clip_io(off, &len);

    int r;
    if (off + len <= size) {
      r = rc->objecter->trunc(image_oid, volume, off);
    } else {
      r = rc->objecter->zero(image_oid, volume, off, len);
    }

    if (r < 0)
      throw std::error_condition(-r, std::generic_category());
  }

  class CB_CopyRead {
    Image& m_dest;
    uint64_t m_offset;
  public:
    bufferlist m_bl;
    CB_CopyRead(Image& dest, uint64_t offset)
      : m_dest(dest), m_offset(offset) { }

    void operator()(int r) {
      if (r < 0) {
	lderr(m_dest.rc->cct) << "error reading from source image at offset "
			      << m_offset << ": " << cpp_strerror(r) << dendl;
	return;
      }
      assert(m_bl.length() == (size_t)r);

      if (m_bl.is_zero()) {
	return;
      }

      m_dest.write(m_offset, m_bl.length(), m_bl);
    }
  };

  void Image::copy(Image& src, Image& dest)
  {
    uint64_t src_size = src.get_size();
    uint64_t dest_size = dest.get_size();

    Flusher rf;

    if (src.empty || dest.empty)
      throw std::make_error_condition(std::errc::invalid_argument);

    if (dest_size < src_size) {
      lderr(src.rc->cct) << "src size " << src_size << " >= dest size "
			 << dest_size << dendl;
      throw std::make_error_condition(std::errc::invalid_argument);
    }
    size_t len = std::min(src.volume->op_size(),
			  dest.volume->op_size());

    for (uint64_t offset = 0; offset < src_size; offset += len) {
      CB_CopyRead cb(dest, offset);
      src.read(offset, len, &cb.m_bl, rf.completion(cb));
    }
    rf.flush();
    dest.flush();
  }

  void Image::read_iterate(
    uint64_t off, size_t len,
    function<void(uint64_t, size_t, const bufferlist&)> cb) const
  {
    if (empty)
      throw std::make_error_condition(std::errc::invalid_argument);
    ldout(rc->cct, 20) << "read_iterate " << *this << " off = " << off
		       << " len = " << len << dendl;

    uint64_t mylen = len;
    clip_io(off, &mylen);

    uint64_t left = mylen;
    uint64_t read_len = volume->op_size();

    bufferlist bl;
    while (left > 0) {
      read_sync(off, read_len, &bl);
      cb(off, bl.length(), bl);
      left -= bl.length();
      off += bl.length();
      bl.clear();
    }
  }

  void Image::clip_io(uint64_t off, uint64_t *len) const
  {
    // special-case "len == 0" requests: always valid
    if (*len == 0)
      return;

    // can't start past end
    if (off >= size)
      throw std::make_error_condition(std::errc::invalid_argument);

    // clip requests that extend past end to just end
    if ((off + *len) > size)
      *len = (uint64_t)(size - off);
  }
}
