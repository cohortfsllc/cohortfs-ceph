
#include "common/PrebufferedStreambuf.h"

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

const std::string& PrebufferedStreambuf::get_str()
{
  // resize the buffer to end at pptr()
  m_buf.resize(pptr()-eback());

  // update end of output sequence (next write overflows)
  setp(pptr(), pptr());

  // update end of input sequence
  setg(eback(), gptr(), pptr());

  return m_buf;
}
