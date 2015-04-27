// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <system_error>

namespace cohort {
  enum class err {
    parse_error
  };

  class err_category_t : public std::error_category {
    virtual const char* name() const noexcept;
    virtual std::string message(int ev) const;
    virtual bool equivalent(const std::error_code& code,
			    int condition) const noexcept;
  };

  const std::error_category& err_category();

  std::error_condition make_error_condition(err e) {
    return std::error_condition(
      static_cast<int>(e),
      err_category());
  }

  std::error_code make_error_code(err e) {
    return std::error_code(
      static_cast<int>(e),
      err_category());
  }
};

namespace std {
  template <>
  struct is_error_condition_enum<cohort::err> : public std::true_type {};
};
