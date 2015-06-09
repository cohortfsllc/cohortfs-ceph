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

namespace rbd {
  using namespace rados;
  using namespace std::literals;
  using std::unique_ptr;

  const string Image::rbd_suffix = ".rbd";
  const char Image::header_text[] = "<<< Rados Block Device Image >>>\n";
  const char Image::header_signature[] = "RBD";
  const char Image::header_version[] = "001.005";

  const char* rbd_category_t::name() const noexcept {
    return "rbd";
  }

  std::string rbd_category_t::message(int ev) const {
    switch (static_cast<errc>(ev)) {
    case errc::no_such_image:
      return "no such image"s;
    case errc::unassociated:
      return "image class unassociated with image"s;
    case errc::exists:
      return "image with name already exists"s;
    case errc::invalid_header:
      return "invalid data in image header"s;
    case errc::read_only:
      return "attempted write to image opened read-only"s;
    case errc::illegal_offset:
      return "attempted access to nonexistent range of image"s;
    default:
      return "unknown error"s;
    }
  }

  const std::error_category& rbd_category() {
    static rbd_category_t instance;
    return instance;
  }


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
    try {
      tie(*size, std::ignore) = rc->objecter->stat(header_name(name), volume);
    } catch (std::system_error& e) {
      if (e.code() == std::errc::no_such_file_or_directory)
	return false;
      else
	throw;
    }

    return true;
  }

  void Image::trim_image(uint64_t newsize)
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    if (newsize < size) {
      rc->objecter->trunc(image_oid, volume, newsize, 0);
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
    rc->objecter->write_full(header_oid, volume, bl);
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
      throw std::system_error(errc::no_such_image, srcname);
    }

    string src_oid = header_name(srcname);
    string dst_oid = header_name(dstname);

    bufferlist databl;
    databl = rc->objecter->read(src_oid, volume, 0, src_size);

    if (check_exists(rc, volume, dstname)) {
      lderr(rc->cct) << "rbd image " << dstname << " already exists" << dendl;
      throw std::system_error(errc::exists, dstname);
    }

    unique_ptr<ObjOp> op = volume->op();
    op->create(true);
    op->write_full(databl);
    rc->objecter->mutate(dst_oid, volume, op);
    try {
      rc->objecter->remove(src_oid, volume);
    } catch (std::system_error& err) {
      if (err.code() != std::errc::no_such_file_or_directory)
	throw;
    }
  }

  void Image::read_header()
  {
    bufferlist header_bl;
    read_header_bl(header_bl);
    if (header_bl.length() < (int)sizeof(header))
      throw std::system_error(errc::invalid_header);
    memcpy(&header, header_bl.c_str(), sizeof(header));
  }

  void Image::read_header_bl(bufferlist& header_bl) const
  {
    uint64_t off = 0;
    size_t lenread;
    do {
      bufferlist bl = rc->objecter->read(header_oid, volume, off,
					 volume->op_size());
      lenread = bl.length();
      header_bl.claim_append(bl);
      off += lenread;
    } while (lenread == volume->op_size());

    if (memcmp(header_text, header_bl.c_str(), sizeof(header_text))) {
      lderr(rc->cct) << "unrecognized header format" << dendl;
      throw std::system_error(errc::invalid_header);
    }
  }

  uint64_t Image::get_size() const
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    return size;
  }

  void Image::remove(RadosClient* rc,
		     const AVolRef& volume,
		     const string& imgname)
  {
    ldout(rc->cct, 20) << "remove " << imgname << dendl;

    ldout(rc->cct, 2) << "removing header..." << dendl;
    rc->objecter->remove(header_name(imgname), volume);
    try {
      rc->objecter->remove(image_name(imgname), volume);
    } catch (std::system_error& e) {
      if (e.code() != std::errc::no_such_file_or_directory)
	throw;
    }
    ldout(rc->cct, 2) << "done." << dendl;
  }

  void Image::flush()
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    f->flush();
  }

  void Image::flush(op_callback&& cb)
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    f->flush(std::move(cb));
  }

  void Image::resize(uint64_t newsize)
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    if (read_only)
      throw std::system_error(errc::read_only);

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

    // rewrite header
    bufferlist bl;
    header.image_size = size;
    bl.append((const char *)&header, sizeof(header));
    rc->objecter->write_full(header_oid, volume, bl);
  }

  void Image::write_sync(uint64_t off, size_t len, const bufferlist& bl)
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    if (read_only)
      throw std::system_error(errc::read_only);

    clip_io(off, &len);

    rc->objecter->write(image_oid, volume, off, len, bl);
  }

  void Image::write(uint64_t off, size_t len, const bufferlist& bl,
		    op_callback&& ack, op_callback&& safe)
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    if (read_only)
      throw std::system_error(errc::read_only);

    clip_io(off, &len);

    rc->objecter->write(image_oid, volume, off, len, bl,
			std::move(ack), f->completion(std::move(safe)));
  }

  bufferlist Image::read_sync(uint64_t off, size_t len) const
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    clip_io(off, &len);

    bufferlist bl = rc->objecter->read(image_oid, volume, off, len);
    if (bl.length() < len) {
      bl.append_zero(len - bl.length());
    }
    return bl;
  }

  struct RCB_Padder {
    read_callback cb;
    size_t len;

    RCB_Padder(size_t _len, read_callback&& _cb) : len(_len) {
      cb.swap(_cb);
    }

    void operator()(std::error_code r, bufferlist& bl) {
      if (!r && (bl.length() < len)) {
	bl.append_zero(len - bl.length());
      }
      cb(r, bl);
    }
  };

  void Image::read(uint64_t off, size_t len, read_callback&& cb) const
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    clip_io(off, &len);

    rc->objecter->read(image_oid, volume, off, len,
		       RCB_Padder(len, std::move(cb)));
  }

  void Image::discard(uint64_t off, size_t len, op_callback&& cb)
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    if (read_only)
      throw std::system_error(errc::read_only);

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
      throw std::system_error(errc::unassociated);
    if (read_only)
      throw std::system_error(errc::read_only);

    clip_io(off, &len);

    if (off + len <= size) {
      rc->objecter->trunc(image_oid, volume, off);
    } else {
      rc->objecter->zero(image_oid, volume, off, len);
    }
  }

  class CB_CopyRead {
    Image& dest;
    uint64_t offset;
  public:
    CB_CopyRead(Image& _dest, uint64_t _offset)
      : dest(_dest), offset(_offset) { }

    void operator()(std::error_code r, bufferlist& bl) {
      if (r) {
	lderr(dest.rc->cct) << "error reading from source image at offset "
			    << offset << ": " << r << dendl;
	return;
      }

      if (bl.is_zero()) {
	return;
      }

      dest.write(offset, bl.length(), bl);
    }
  };

  void Image::copy(Image& src, Image& dest)
  {
    uint64_t src_size = src.get_size();
    uint64_t dest_size = dest.get_size();

    if (src.empty || dest.empty)
      throw std::system_error(errc::unassociated);

    if (dest_size < src_size) {
      lderr(src.rc->cct) << "src size " << src_size << " >= dest size "
			 << dest_size << dendl;
      throw std::system_error(errc::unassociated);
    }
    size_t len = std::min(src.volume->op_size(),
			  dest.volume->op_size());

    for (uint64_t offset = 0; offset < src_size; offset += len) {
      CB_CopyRead cb(dest, offset);
      src.read(offset, len, cb);
    }
    dest.flush();
  }

  void Image::read_iterate(
    uint64_t off, size_t len,
    function<void(uint64_t, size_t, const bufferlist&)> cb) const
  {
    if (empty)
      throw std::system_error(errc::unassociated);
    ldout(rc->cct, 20) << "read_iterate " << *this << " off = " << off
		       << " len = " << len << dendl;

    uint64_t mylen = len;
    clip_io(off, &mylen);

    uint64_t left = mylen;
    uint64_t read_len = volume->op_size();

    bufferlist bl;
    while (left > 0) {
      bl = read_sync(off, read_len);
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
      throw std::system_error(errc::illegal_offset);

    // clip requests that extend past end to just end
    if ((off + *len) > size)
      *len = (uint64_t)(size - off);
  }
}
