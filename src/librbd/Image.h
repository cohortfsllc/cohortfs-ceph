// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_IMAGECTX_H
#define CEPH_LIBRBD_IMAGECTX_H

#include <string>
#include <functional>

#include <mutex>
#include <shared_mutex>
#include "osdc/RadosClient.h"
#include "include/buffer.h"
#include "include/types.h"

class CephContext;

namespace rbd {
  enum class errc { no_such_image, unassociated, exists, invalid_header,
      read_only, illegal_offset };

  class rbd_category_t : public std::error_category {
    virtual const char* name() const noexcept;
    virtual std::string message(int ev) const;
    virtual std::error_condition default_error_condition(
      int ev)const noexcept {
      switch (static_cast<errc>(ev)) {
      case errc::no_such_image:
	return std::errc::no_such_file_or_directory;
      case errc::unassociated:
	return std::errc::bad_file_descriptor;
      case errc::exists:
	return cohort::errc::object_already_exists;
      case errc::invalid_header:
	return cohort::errc::parse_error;
      case errc::read_only:
	return std::errc::read_only_file_system;
      case errc::illegal_offset:
	return std::errc::invalid_argument;
      default:
	return std::error_condition(ev, *this);
      }
    }
  };

  const std::error_category& rbd_category();

  static inline std::error_condition make_error_condition(errc e) {
    return std::error_condition(
      static_cast<int>(e), rbd_category());
  }

  static inline std::error_code make_error_code(errc e) {
    return std::error_code(
      static_cast<int>(e), rbd_category());
  }
};

namespace std {
  template <>
  struct is_error_code_enum<rbd::errc> : public std::true_type {};
};

namespace rbd {
  using std::string;
  using std::function;
  using ceph::bufferlist;
  using rados::RadosClient;

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


    rados::RadosClient* rc;
    struct rbd_obj_header_ondisk header;
    // whether the image was opened read-only. cannot be changed after opening
    bool read_only;
    bool empty;

    string name;
    AVolRef volume;
    uint64_t size;
    oid_t header_oid;
    oid_t image_oid;
    rados::Flusher* f;
  public:

    Image() noexcept : rc(nullptr), read_only(false),
      empty(true), f(nullptr) {}
    Image(rados::RadosClient* rados,
	  const AVolRef& v,
	  const string& name);
    Image(rados::RadosClient* rados,
	  const AVolRef& v,
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
		       const AVolRef& volume,
		       const string& imgname);
    void flush();
    void flush(rados::op_callback&& cb);
    void resize(uint64_t newsize);
    static void copy(Image& src, Image& dest);

    void write_sync(uint64_t off, size_t len, const bufferlist& bl);
    void write(uint64_t off, size_t len, const bufferlist& bl,
	       rados::op_callback&& ack = nullptr,
	       rados::op_callback&& safe = nullptr);

    bufferlist read_sync(uint64_t off, size_t len) const;
    void read(uint64_t off, size_t len, rados::read_callback&& cb) const;
    void read_iterate(uint64_t off, size_t len,
		      function<void(uint64_t, size_t,
				    const bufferlist&)> cb) const;
    void read_iterate(function<void(uint64_t, size_t,
				    const bufferlist&)> cb) const {
      read_iterate(0, size, cb);
    }

    void discard(uint64_t off, size_t len, rados::op_callback&& cb = nullptr);
    void discard_sync(uint64_t off, size_t len);
    static void create(RadosClient* rc,
		       const AVolRef& volume,
		       const string& imgname,
		       uint64_t size);
    static void rename(RadosClient* rc,
		       const AVolRef& volume,
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
			     const AVolRef& volume,
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
