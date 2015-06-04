// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <system_error>

#include "include/cohort_error.h"

namespace rados {
  enum class errc { invalid_osd, need_unique_lock };

  class rados_category_t : public std::error_category {
    virtual const char* name() const noexcept;
    virtual std::string message(int ev) const;
    virtual std::error_condition default_error_condition(
      int ev) const noexcept {
      switch (static_cast<errc>(ev)) {
      case errc::invalid_osd:
	return std::errc::invalid_argument;
      case errc::need_unique_lock:
	return cohort::errc::insufficient_resources;
      default:
	return std::error_condition(ev, *this);
      }
    }
  };

  const std::error_category& rados_category();

  static inline std::error_condition make_error_condition(errc e) {
    return std::error_condition(static_cast<int>(e), rados_category());
  }

  static inline std::error_code make_error_code(errc e) {
    return std::error_code(static_cast<int>(e), rados_category());
  }
};

namespace std {
  template <>
  struct is_error_code_enum<rados::errc> : public std::true_type {};
};
