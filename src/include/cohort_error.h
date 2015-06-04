// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COHORT_ERROR_H
#define COHORT_ERROR_H

#include <system_error>

namespace cohort {
  enum class errc {
    parse_error, no_such_object, object_already_exists, insufficient_resources
  };

  class cohort_category_t : public std::error_category {
    virtual const char* name() const noexcept;
    virtual std::string message(int ev) const;
    virtual bool equivalent(const std::error_code& code,
			    int condition) const noexcept;
  };

  const std::error_category& cohort_category();

  static inline std::error_condition make_error_condition(errc e) {
    return std::error_condition(static_cast<int>(e), cohort_category());
  }

  static inline std::error_code make_error_code(errc e) {
    return std::error_code(static_cast<int>(e), cohort_category());
  }
};

namespace std {
  template <>
  struct is_error_condition_enum<cohort::errc> : public std::true_type {};
};

#endif // !COHORT_ERROR_H
