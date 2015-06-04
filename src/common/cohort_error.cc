// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/cohort_error.h"
#include "include/buffer.h"
#include "vol/Volume.h"
#include "placer/Placer.h"
#include "osdc/rados_err.h"

using namespace std::literals;

namespace cohort {
  template<typename T, typename U>
  bool in(T var, U x) {
    return var == x;
  }
  template<typename T, typename U, typename... Rest>
  bool in(T var, U x, Rest... rest) {
    return var == x || in(var, rest...);
  }

  const char* cohort_category_t::name() const noexcept {
    return "cohort";
  }
  std::string cohort_category_t::message(int ev) const {
    switch (static_cast<errc>(ev)) {
    case errc::parse_error:
      return "parse error"s;
    case errc::no_such_object:
      return "no such object"s;
    case errc::object_already_exists:
      return "object already exists"s;
    case errc::insufficient_resources:
      return "insufficient resources held for operation"s;
    default:
      return "unknown error"s;
    }
  }

  bool cohort_category_t::equivalent(const std::error_code& code,
				     int condition) const noexcept {
    switch (static_cast<errc>(condition)) {
    case errc::parse_error:
      return in(code, ceph::buffer_err::end_of_buffer,
		ceph::buffer_err::malformed_input);

    case errc::no_such_object:
      return in(code, vol_err::no_such_volume, placer_err::no_such_placer);

    case errc::object_already_exists:
      return in(code, vol_err::exists, placer_err::exists,
		std::errc::file_exists);

    case errc::insufficient_resources:
      return in(code, rados::errc::need_unique_lock);

    default:
      return false;
    }
    return false;
  }

  const std::error_category& cohort_category() {
    static cohort_category_t instance;
    return instance;
  }
};
