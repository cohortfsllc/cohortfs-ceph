// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rados_err.h"
#include <string>

using namespace std::literals;

namespace rados {
  const char* rados_category_t::name() const noexcept {
    return "rados";
  }

  std::string rados_category_t::message(int ev) const {
    switch (static_cast<errc>(ev)) {
    case errc::invalid_osd:
      return "invalid OSD ID"s;
    case errc::need_unique_lock:
      return "lock must be uniquely owned"s;
    default:
      return "unknown error"s;
    }
  }

  const std::error_category& rados_category() {
    static rados_category_t instance;
    return instance;
  }
};
