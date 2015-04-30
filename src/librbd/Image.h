// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include <string>
#include <functional>

#include <mutex>
#include <shared_mutex>
#include "librados/RadosClient.h"
#include "include/buffer.h"
#include "include/types.h"

class CephContext;

namespace librbd {
  using std::string;
  using std::function;
  using ceph::bufferlist;
  using librados::RadosClient;

  struct read_only_t { };
  constexpr read_only_t read_only { };

  class Image {
    friend class CB_CopyRead;
  private:
    static const string rbd_suffix;
    static const char header_text[];
    static const char header_signature[];
    static const char header_version[];

    struct rbd_obj_header_ondisk {
      char text[40];
      char signature[4];
      char version[8];
      uint64_t image_size;
    };


    librados::RadosClient* rc;
    struct rbd_obj_header_ondisk header;
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    bool empty;

    string name;
    VolumeRef volume;
    uint64_t size;
    oid_t header_oid;
    oid_t image_oid;
    OSDC::Flusher* f;
  public:

    Image() noexcept : rc(nullptr), read_only(false),
      empty(true), f(nullptr) {}
    Image(librados::RadosClient* rados,
	  const VolumeRef& v,
	  const string& name);
    Image(librados::RadosClient* rados,
	  const VolumeRef& v,
	  const string &name,
	  read_only_t);
    Image(Image&& i) noexcept : Image() {
      swap(i);
    }
    Image(const Image&) = delete;

    Image& operator=(Image&& i) noexcept {
      Image(std::move(i)).swap(*this);
      return *this;
    }
    Image& operator=(const Image&) = delete;

    uint64_t get_size() const;
    static void remove(RadosClient* rc,
		       const VolumeRef& volume,
		       const string& imgname);
    void flush();
    void flush(OSDC::op_callback&& cb);
    void resize(uint64_t newsize);
    static void copy(Image& src, Image& dest);

    void write_sync(uint64_t off, size_t len, const bufferlist& bl);
    void write(uint64_t off, size_t len, const bufferlist& bl,
	       OSDC::op_callback&& ack = nullptr,
	       OSDC::op_callback&& safe = nullptr);

    void read_sync(uint64_t off, size_t len, bufferlist* bl) const;
    void read(uint64_t off, size_t len, bufferlist* bl,
	      OSDC::op_callback&& cb = nullptr) const;
    void read(uint64_t off, size_t len, OSDC::read_callback&& cb) const;
    void read_iterate(uint64_t off, size_t len,
		      function<void(uint64_t, size_t,
				    const bufferlist&)> cb) const;
    void read_iterate(function<void(uint64_t, size_t,
				    const bufferlist&)> cb) const {
      read_iterate(0, size, cb);
    }

    void discard(uint64_t off, size_t len, OSDC::op_callback&& cb = nullptr);
    void discard_sync(uint64_t off, size_t len);
    static void create(RadosClient* rc,
		       const VolumeRef& volume,
		       const string& imgname,
		       uint64_t size);
    static void rename(RadosClient* rc,
		       const VolumeRef& volume,
		       const string& srcname,
		       const string& dstname);

    void swap(Image& i) noexcept {
      std::swap(rc, i.rc);
      std::swap(header, i.header);
      std::swap(read_only, i.read_only);
      std::swap(empty, i.empty);
      std::swap(name, i.name);
      std::swap(volume, i.volume);
      std::swap(size, i.size);
      std::swap(header_oid, i.header_oid);
      std::swap(image_oid, i.image_oid);
      std::swap(f, i.f);
    }

  private:
    static bool check_exists(RadosClient* rc,
			     const VolumeRef& volume,
			     const string& name, uint64_t *size = nullptr);
    void trim_image(uint64_t newsize);
    static const string header_name(const string &name) {
      return name + rbd_suffix;
    }

    static const string image_name(const string &name) {
      return "rb." + name + ".image";
    }

    static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
				uint64_t size);

    void read_header();
    void read_header_bl(bufferlist& header_bl) const;
    void clip_io(uint64_t off, uint64_t *len) const;

    friend ostream& operator<<(ostream& out, const Image& i);
  };

  inline ostream& operator<<(ostream& out, const Image& i) {
    return out << i.volume << ':' << i.name;
  }

  inline void swap(Image& i1, Image& i2) noexcept {
    i1.swap(i2);
  }
};

#endif
