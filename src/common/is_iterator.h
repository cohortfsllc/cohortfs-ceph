// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMMON_IS_ITERATOR_H
#define COMMON_IS_ITERATOR_H

#include <iterator>
#include <map>

namespace cohort {
  namespace detail {
    template<typename I>
    struct is_iterator_imp {
      static constexpr char test(...) { return 0; }

      template<typename J,
	       typename=typename std::iterator_traits<J>::difference_type,
	       typename=typename std::iterator_traits<J>::pointer,
	       typename=typename std::iterator_traits<J>::reference,
	       typename=typename std::iterator_traits<J>::value_type,
	       typename=typename std::iterator_traits<J>::iterator_category>
      static constexpr long test(J&&) { return 0; };

      static constexpr bool value = std::is_same<
	decltype(test(std::declval<I>())),long>();
    };
  };

  template<typename I>
  struct is_iterator : public std::integral_constant<
    bool, detail::is_iterator_imp<I>::value> {
  };
};

#endif // !COMMON_IS_ITERATOR_H
