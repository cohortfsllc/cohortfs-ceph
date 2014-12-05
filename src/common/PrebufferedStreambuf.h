// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_PREBUFFEREDSTREAMBUF_H
#define CEPH_COMMON_PREBUFFEREDSTREAMBUF_H

#include <iosfwd>
#include <string>
#include <streambuf>

/**
 * streambuf using existing buffer, overflowing into a std::string
 *
 * A simple streambuf that uses a preallocated buffer for small
 * strings, and overflows into a std::string when necessary.  If the
 * preallocated buffer size is chosen well, we can optimize for the
 * common case and overflow to a slower heap-allocated buffer when
 * necessary.
 */
class PrebufferedStreambuf
  : public std::basic_streambuf<char, std::basic_string<char>::traits_type>
{
  typedef std::char_traits<char> traits_ty;
  typedef traits_ty::int_type int_type;
  typedef traits_ty::pos_type pos_type;
  typedef traits_ty::off_type off_type;

  std::string &m_buf;
  pos_type m_len;

protected:
  // support pubseekpos() and pubseekoff()
  virtual std::streampos seekpos(std::streampos sp,
      std::ios_base::openmode which = std::ios_base::in | std::ios_base::out);
  virtual std::streampos seekoff(std::streamoff off, std::ios_base::seekdir way,
      std::ios_base::openmode which = std::ios_base::in | std::ios_base::out);

public:
  PrebufferedStreambuf(std::string &str);

  // called when the buffer fills up
  int_type overflow(int_type c);

  // called when we read and need more data
  int_type underflow();

  /// return a const reference to the string
  const std::string& str();
};

#endif
