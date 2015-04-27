// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/cohort_error.h"
#include "include/buffer.h"

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

  const char* err_category_t::name() const noexcept {
    return "cohort condition";
  }
  std::string err_category_t::message(int ev) const {
    switch (static_cast<err>(ev)) {
    case err::parse_error:
      return "parse error"s;
    default:
      return "unknown error"s;
    }
  }

  bool err_category_t::equivalent(const std::error_code& code,
				  int condition) const noexcept {
    switch (static_cast<err>(condition)) {
    case err::parse_error:
      return in(code, ceph::buffer_err::end_of_buffer,
		ceph::buffer_err::malformed_input);

    default:
      return false;
    }
    return false;
  }

  const std::error_category& err_category() {
    static err_category_t instance;
    return instance;
  }
}
