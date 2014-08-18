// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/PrebufferedStreambuf.h"

using namespace std;

PrebufferedStreambuf::PrebufferedStreambuf(size_t len)
{
  // reserve memory for the buffer
  m_buf.resize(len);

  // init output buffer
  setp(&m_buf[0], &m_buf[0] + len);

  // so we underflow on first read
  setg(pptr(), pptr(), pptr());
}

PrebufferedStreambuf::int_type PrebufferedStreambuf::overflow(int_type c)
{
  int old_len = m_buf.size();
  if (m_buf.size() < m_buf.capacity()) {
    // overflow after get_str()
    m_buf.resize(m_buf.capacity());
  } else {
    // double buffer length
    m_buf.resize(old_len * 2);
  }

  // update output sequence
  m_buf[old_len] = c;
  setp(&m_buf[old_len + 1], &m_buf[0] + m_buf.size());

  // update input sequence (resize may have moved the buffer)
  setg(&m_buf[0], &m_buf[gptr()-eback()], pptr());

  return traits_ty::not_eof(c);
}

PrebufferedStreambuf::int_type PrebufferedStreambuf::underflow()
{
  if (gptr() == pptr())
    return traits_ty::eof();

  // update end of input sequence
  setg(eback(), gptr(), pptr());
  return *gptr();
}

streampos PrebufferedStreambuf::seekpos(streampos sp, ios_base::openmode which)
{
  if (eback() + sp > egptr())
    return streampos(-1);
  if (which & ios_base::in)
    setg(eback(), eback() + sp, egptr());
  if (which & ios_base::out)
    setp(eback() + sp, epptr());
  return sp;
}

streampos PrebufferedStreambuf::seekoff(streamoff off, ios_base::seekdir way,
					ios_base::openmode which)
{
  if (way == ios_base::beg)
    return seekpos(off, which);

  if (way == ios_base::end)
    return seekpos(egptr() - eback() + off, which);

  if (way == ios_base::cur) {
    // fail if both in|out
    if (which == ios_base::in) {
      if (gptr() + off > egptr() || gptr() + off < eback())
	return streampos(-1);
      setg(eback(), gptr() + off, egptr());
      return gptr() - eback();
    }
    if (which == ios_base::out) {
      if (pptr() + off > epptr() || pptr() + off < pbase())
	return streampos(-1);
      setp(pptr() + off, epptr());
      return pptr() - eback();
    }
  }
  return streampos(-1);
}

const string& PrebufferedStreambuf::str()
{
  // resize the buffer to end at pptr()
  m_buf.resize(pptr()-eback());

  // update end of output sequence (next write overflows)
  setp(pptr(), pptr());

  // update end of input sequence
  setg(eback(), gptr(), pptr());

  return m_buf;
}
