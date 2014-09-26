// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_STREAM_H
#define CEPH_STREAM_H

#include <boost/type_traits.hpp> // is_base_and_derived

/*
 * Stream output operators are templated to support std::ostream and types
 * derived from std::ostream, as well as other types not derived from
 * std::ostream that still support operator<<.
 *
 * Types derived from std::ostream present a problem for templated operator<<,
 * because all default operator<< return std::ostream&. If the operator takes
 * stream type T and returns type T, it won't work for derived types like
 * std::stringstream; if the operator calls other stream operators that
 * return std::ostream, it can't return the result as a std::stringstream.
 *
 * We add a StrmRet template to solve this, for example:
 *
 * template<typename T>
 * inline typename StrmRet<T>::type operator<<(T &out, const foo &f) {
 *   return out << f.bar;
 * }
 *
 * Which will generate the following functions, with T=std::stringstream:
 *   std::ostream& operator<<(std::stringstream&, const foo &f)
 * and with T=lttng_stream:
 *   lttng_stream& operator<<(lttng_stream&, const foo &f)
 */

// default template keeps input type
template<typename T, typename Base, bool IsDerived>
struct BaseIfDerivedImpl { typedef T type; };

// specialization replaces type with base
template<typename T, typename Base>
struct BaseIfDerivedImpl<T, Base, true> { typedef Base type; };

// chooses type T, unless it inherits from Base
template<typename T, typename Base>
struct BaseIfDerived : public BaseIfDerivedImpl<T, Base,
  boost::is_base_and_derived<Base, T>::value> {};

// StrmRet<T>::type is set to T, or ostream if derived from ostream
template<typename T>
struct StrmRet : public BaseIfDerived<T, std::ostream> {};


#endif
