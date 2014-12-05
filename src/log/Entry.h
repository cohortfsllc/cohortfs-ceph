// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRY_H
#define __CEPH_LOG_ENTRY_H

#include "include/utime.h"
#include "common/PrebufferedStreambuf.h"
#include <pthread.h>
#include <string>

#define CEPH_LOG_ENTRY_PREALLOC 80

namespace ceph {
namespace log {

using std::ostream;

struct Entry {
  utime_t m_stamp;
  pthread_t m_thread;
  short m_prio, m_subsys;
  Entry *m_next;
  std::string str;

  PrebufferedStreambuf m_streambuf;

  Entry()
    : m_thread(0), m_prio(0), m_subsys(0),
      m_next(NULL),
      str(CEPH_LOG_ENTRY_PREALLOC, 0),
      m_streambuf(str)
  {}
  Entry(utime_t s, pthread_t t, short pr, short sub)
    : m_stamp(s), m_thread(t), m_prio(pr), m_subsys(sub),
      m_next(NULL),
      str(CEPH_LOG_ENTRY_PREALLOC, 0),
      m_streambuf(str)
  {}
  Entry(utime_t s, pthread_t t, short pr, short sub, std::string &prealloc)
    : m_stamp(s), m_thread(t), m_prio(pr), m_subsys(sub),
      m_next(NULL),
      m_streambuf(prealloc)
  {}

  void set_str(const std::string &s) {
    ostream os(&m_streambuf);
    os << s;
  }

  const std::string& get_str() {
    return m_streambuf.str();
  }
};

}
}

#endif
