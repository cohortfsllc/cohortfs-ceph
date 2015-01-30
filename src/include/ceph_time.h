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

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iomanip>

/* Typedefs for timekeeping, to cut down on the amount of template
   foo. */

typedef uint64_t ceph_timespec;

using namespace std::literals::chrono_literals;

namespace ceph {
  // We can change the time precisiou/representation later if we want.
  typedef std::chrono::duration<ceph_timespec, std::nano> timespan;
  typedef std::chrono::duration<int64_t, std::nano> signedspan;

  // This is a FRACTIONAL TIME IN SECONDS
  typedef std::chrono::system_clock real_clock;
  typedef std::chrono::steady_clock mono_clock;
  typedef std::chrono::time_point<real_clock, timespan> real_time;
  typedef std::chrono::time_point<mono_clock, timespan> mono_time;

  inline ceph::timespan span_from_double(double sec) {
    return std::chrono::duration_cast<ceph::timespan>(
      std::chrono::duration<double>(sec));
  }

  inline double span_to_double(ceph::timespan t) {
    return std::chrono::duration_cast<std::chrono::duration<double> >(
      t).count();
  }

  inline real_time spec_to_time(ceph_timespec ts) {
    return real_time(timespan(ts));
  };

  inline ceph_timespec time_to_spec(real_time rt) {
    return rt.time_since_epoch().count();
  };

  inline struct timespec time_to_timespec(real_time rt) {
    struct timespec ts;
    ts.tv_sec = real_clock::to_time_t(rt);
    ts.tv_nsec = (rt.time_since_epoch() % 1s).count();
    return ts;
  }

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
  return out << ceph::span_to_double(t) << " s";
}

// Since it's only for debugging, I don't care that it's junk.

inline std::ostream& operator<<(std::ostream& out,
				const ceph::mono_time& t)
{
  return out << t.time_since_epoch();
}

namespace std {
  inline ceph::timespan abs(ceph::signedspan x) {
    return ceph::timespan(abs(x.count()));
  }
}

#endif // CEPH_TIME__
