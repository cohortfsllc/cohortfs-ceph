// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_STREAM_H
#define CEPH_STREAM_H

#include <ostream>

/*
 * Stream output operators are templated to support std::ostream and types
 * derived from std::ostream, as well as other types not derived from
 * std::ostream that still support operator<< (see lttng_stream).
 *
 * Types derived from std::ostream present a problem for templated operator<<,
 * because it creates ambiguities between our operator and std::ostream's
 * default operators.
 *
 * Taking 'std::stringstream << int' as an example, the compiler doesn't know
 * whether to prefer a conversion from stringstream to ostream, which matches
 * the default ostream::operator<<(int), over a conversion from int to a
 * ceph-defined type like client_t, which would match our generated
 * operator<<(T=stringstream&, client_t).
 *
 * To resolve this ambiguity, we disable instantiations for any type which
 * derives from std::ostream, making sure that we only generate
 * operator<<(T=ostream&, client_t). The compiler will clearly prefer the
 * default ostream::operator<<(int) in this case.
 *
 * We add a StrmRet template to accomplish this, for example:
 *
 * template<typename T>
 * inline typename StrmRet<T>::type operator<<(T &out, const client_t &c) {
 *   return out << c.v;
 * }
 *
 */

#if __cplusplus <= 199711L // not C++11, use boost type_traits

#include <boost/type_traits.hpp>

template<typename Base, typename T>
struct disable_if_derived : public boost::disable_if<
  boost::is_base_and_derived<Base, T> > {};

#else // use C++11 type_traits

#include <type_traits>

// std::is_base_of<T, T> is true, so check std::is_same as well
template<typename Base, typename T>
struct disable_if_derived : public std::enable_if<
  !std::is_base_of<Base, T>::value || std::is_same<Base, T>::value, T> {};

#endif // C++11

// StrmRet<T>::type is set to T, unless T is derived from ostream
template<typename T>
struct StrmRet : public disable_if_derived<std::ostream, T> {};

#endif // CEPH_STREAM_H
