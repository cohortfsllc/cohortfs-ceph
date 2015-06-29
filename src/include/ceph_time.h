// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */


#ifndef CEPH_TIME__
#define CEPH_TIME__

#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>

/* XXX for parse_date */
#include <cstdio>
#include <cstring>
#include <ctime>
#include "common/strtol.h"

/* Typedefs for timekeeping, to cut down on the amount of template
   foo. */

typedef uint64_t ceph_timerep;

using namespace std::literals::chrono_literals;

namespace ceph {
  // We can change the time precisiou/representation later if we want.
  typedef std::chrono::duration<ceph_timerep, std::nano> timespan;
  typedef std::chrono::duration<int64_t, std::nano> signedspan;

#if (defined(_POSIX_TIMERS) && _POSIX_TIMERS > 0)
  class real_clock {
  public:
    typedef timespan duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef std::chrono::time_point<real_clock, duration> time_point;
    static constexpr const bool is_steady = false;

    static time_point now() noexcept {
      struct timespec ts;
      int __attribute__((unused)) r = clock_gettime(CLOCK_REALTIME, &ts);
      assert(r == 0); // No errors should be possible
      return time_point(ts.tv_sec * 1s + ts.tv_nsec * 1ns);
    }
    static time_t to_time_t(const time_point& t) noexcept {
      return std::chrono::duration_cast<std::chrono::seconds>(
	t.time_since_epoch()).count();
    }
    static time_point from_time_t(time_t t) noexcept {
      return time_point(t * 1s);
    }
  };
#else // !(_POSIX_TIMERS > 0)
  class real_clock {
  public:
    typedef timespan duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef std::chrono::time_point<real_clock, duration> time_point;
    static constexpr const bool is_steady = false;

    static time_point now() noexcept {
      return time_point(std::chrono::duration_cast<timespan>(
			  std::chrono::system_clock::now()
			  .time_since_epoch()));
    }
    static time_t to_time_t(const time_point& t) noexcept {
      return std::chrono::system_clock::to_time_t(
	std::chrono::system_clock::time_point(
	  std::chrono::duration_cast<std::chrono::system_clock::duration>(
	    t.time_since_epoch())));
    }
    static time_point from_time_t(time_t t) noexcept {
      return time_point(std::chrono::duration_cast<timespan>(
			  std::chrono::system_clock::from_time_t(t)
			  .time_since_epoch()));
    }
  };
#endif // !(_POSIX_TIMERS > 0)

#ifdef _POSIX_MONOTONOIC_CLOCK
  class mono_clock {
  public:
    typedef timespan duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef std::chrono::time_point<mono_clock, duration> time_point;
    static constexpr const bool is_steady = true;

    static time_point now() noexcept {
      struct timespec ts;
      int __attribute__((unused)) r = clock_gettime(CLOCK_MONOTONIC, &ts);
      assert(r == 0); // No errors should be possible
      return time_point(ts.tv_sec * 1s + ts.tv_nsec * 1ns);
    }
  };
#else // !_POSIX_MONOTONOIC_CLOCK
  class mono_clock {
  public:
    typedef timespan duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef std::chrono::time_point<mono_clock, duration> time_point;
    static constexpr const bool is_steady = true;

    static time_point now() noexcept {
      return time_point(std::chrono::duration_cast<timespan>(
			  std::chrono::steady_clock::now()
			  .time_since_epoch()));
    }
  };
#endif // !_POSIX_MONOTONOIC_CLOCK

  // This is a FRACTIONAL TIME IN SECONDS
  typedef real_clock::time_point real_time;
  typedef mono_clock::time_point mono_time;

  inline real_time rep_to_time(ceph_timerep ts) {
    return real_time(timespan(ts));
  }

  inline ceph_timerep time_to_rep(real_time rt) {
    return rt.time_since_epoch().count();
  }

  inline struct timespec time_to_timespec(real_time rt) {
    struct timespec ts;
    ts.tv_sec = real_clock::to_time_t(rt);
    // ceph::real_time is represented as a count of nanoseconds, but
    // just in case we ever change it
    ts.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(
      rt.time_since_epoch() % 1s).count();
    return ts;
  }

  inline real_time time_from_timespec(struct timespec ts) {
    return real_clock::from_time_t(ts.tv_sec) + ts.tv_nsec * 1ns;
  }

  /* XXX */
  inline int parse_date(const std::string& s, ceph::real_time& val)
  {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    val = ceph::real_time::min();

    const char *p = strptime(s.c_str(), "%Y-%m-%d", &tm);
    if (p) {
      if (*p == ' ') {
	p++;
	p = strptime(p, " %H:%M:%S", &tm);
	if (!p)
	  return -EINVAL;
	if (*p == '.') {
	  ++p;
	  unsigned i;
	  char buf[10]; /* 9 digit + null termination */
	  for (i = 0; (i < sizeof(buf) - 1) && isdigit(*p); ++i, ++p) {
	    buf[i] = *p;
	  }
	  for (; i < sizeof(buf) - 1; ++i) {
	    buf[i] = '0';
	  }
	  buf[i] = '\0';
	  std::string err;
	  val += ceph::timespan((ceph_timerep)strict_strtol(buf, 10, &err));
	  if (!err.empty()) {
	    return -EINVAL;
	  }
	}
      }
    } else {
      int sec, usec;
      int r = sscanf(s.c_str(), "%d.%d", &sec, &usec);
      if (r != 2) {
	return -EINVAL;
      }
      time_t tt = sec;
      gmtime_r(&tt, &tm);
      val += std::chrono::microseconds(usec);
      time_t t = timegm(&tm);
      val += std::chrono::seconds(t);
    }
    return 0;
  } /* parse_date */

  inline std::ostream& asctime(const ceph::real_time rt, std::ostream& out) {
    struct timespec ts = time_to_timespec(rt);
    out.setf(std::ios::right);
    char oldfill = out.fill();
    out.fill('0');
    if (ts.tv_sec < ((time_t)(60*60*24*365*10))) {
      // raw seconds.  this looks like a relative time.
      uint64_t usec = ts.tv_nsec/1000;
      out << (long)ts.tv_sec << "." << std::setw(6) << usec;
    } else {
      // localtime.  this looks like an absolute time.
      //  aim for http://en.wikipedia.org/wiki/ISO_8601
      struct tm bdt;
      time_t tt = ts.tv_sec;
      gmtime_r(&tt, &bdt);
      char buf[128];
      asctime_r(&bdt, buf);
      int len = strlen(buf);
      if (buf[len - 1] == '\n')
	buf[len - 1] = '\0';
     out << buf;
    }
    out.fill(oldfill);
    out.unsetf(std::ios::right);
    return out;
  } /* asctime */

  inline std::ostream& gmtime(const ceph::real_time rt, std::ostream& out) {
    struct timespec ts = time_to_timespec(rt);
    uint64_t usec = ts.tv_nsec/1000;
    out.setf(std::ios::right);
    char oldfill = out.fill();
    out.fill('0');
    if (ts.tv_sec < ((time_t)(60*60*24*365*10))) {
      // raw seconds.  this looks like a relative time.
      out << (long)ts.tv_sec << "." << std::setw(6) << usec;
    } else {
      // localtime.  this looks like an absolute time.
      //  aim for http://en.wikipedia.org/wiki/ISO_8601
      struct tm bdt;
      time_t tt = ts.tv_sec;
      gmtime_r(&tt, &bdt);
      out << std::setw(4) << (bdt.tm_year+1900)	 // 2007 -> '07'
	  << '-' << std::setw(2) << (bdt.tm_mon+1)
	  << '-' << std::setw(2) << bdt.tm_mday
	  << ' '
	  << std::setw(2) << bdt.tm_hour
	  << ':' << std::setw(2) << bdt.tm_min
	  << ':' << std::setw(2) << bdt.tm_sec;
      out << "." << std::setw(6) << usec;
      out << "Z";
    }
    out.fill(oldfill);
    out.unsetf(std::ios::right);
    return out;
  } /* gmtime */

  inline std::ostream& localtime(const ceph::real_time rt, std::ostream& out) {
    struct timespec ts = time_to_timespec(rt);
    uint64_t usec = ts.tv_nsec/1000;
    out.setf(std::ios::right);
    char oldfill = out.fill();
    out.fill('0');
    if (ts.tv_sec < ((time_t)(60*60*24*365*10))) {
      // raw seconds.  this looks like a relative time.
      out << (long)ts.tv_sec << "." << std::setw(6) << usec;
    } else {
      // localtime.  this looks like an absolute time.
      //  aim for http://en.wikipedia.org/wiki/ISO_8601
      struct tm bdt;
      time_t tt = ts.tv_sec;
      localtime_r(&tt, &bdt);
      out << std::setw(4) << (bdt.tm_year+1900)	 // 2007 -> '07'
	  << '-' << std::setw(2) << (bdt.tm_mon+1)
	  << '-' << std::setw(2) << bdt.tm_mday
	  << ' '
	  << std::setw(2) << bdt.tm_hour
	  << ':' << std::setw(2) << bdt.tm_min
	  << ':' << std::setw(2) << bdt.tm_sec;
      out << "." << std::setw(6) << usec;
      //out << '_' << bdt.tm_zone;
    }
    out.fill(oldfill);
    out.unsetf(std::ios::right);
    return out;
  } /* localtime */

  inline std::ostream& print_real_time(std::ostream& out,
				       real_time t)
  {
    out.setf(std::ios::right);
    char oldfill = out.fill();
    out.fill('0');
    // localtime.  this looks like an absolute time.
    //  aim for http://en.wikipedia.org/wiki/ISO_8601
    struct tm bdt;
    time_t tt = real_clock::to_time_t(t);
    gmtime_r(&tt, &bdt);
    out << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
	<< '-' << std::setw(2) << (bdt.tm_mon+1)
	<< '-' << std::setw(2) << bdt.tm_mday
	<< ' '
	<< std::setw(2) << bdt.tm_hour
	<< ':' << std::setw(2) << bdt.tm_min
	<< ':' << std::setw(2) << bdt.tm_sec;
    out << "." << std::setw(6)
	<< std::chrono::duration_cast<std::chrono::microseconds>(
	  t.time_since_epoch() % std::chrono::seconds(1)).count();
    out << "Z";
    out.fill(oldfill);
    out.unsetf(std::ios::right);
    return out;
  }
};

inline std::ostream& operator<<(std::ostream& out,
				const ceph::real_time& t)
{
  return ceph::print_real_time(out, t);
}

inline std::ostream& operator<<(std::ostream& out,
				const ceph::timespan& t)
{
  return out << std::chrono::duration<double>(t).count() << " s";
}

// Since it's only for debugging, I don't care that it's junk.

static inline std::ostream& operator<<(std::ostream& out,
				       const ceph::mono_time& t)
{
  return out << t.time_since_epoch();
}

namespace std {
  inline ceph::timespan abs(ceph::signedspan x) {
    return ceph::timespan(abs(x.count()));
  }
}

inline ceph::timespan pow(ceph::timespan t, double d) {
  return std::chrono::duration_cast<ceph::timespan>(
    std::chrono::duration<double>(
      pow(std::chrono::duration_cast<std::chrono::duration<double> >(
	  t).count(), d)));
}


#endif // CEPH_TIME__
