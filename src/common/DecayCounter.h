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

#ifndef CEPH_DECAYCOUNTER_H
#define CEPH_DECAYCOUNTER_H

#include <math.h>
#include "include/ceph_time.h"
#include "common/Formatter.h"

/**
 *
 * TODO: normalize value based on some fucntion of half_life,
 *  so that it can be interpreted as an approximation of a
 *  moving average of N seconds.  currently, changing half-life
 *  skews the scale of the value, even at steady state.
 *
 */

using ceph::Formatter;

class DecayRate {
  double k;		// k = ln(.5)/half_life

  friend class DecayCounter;

public:
  DecayRate() : k(0) {}
  DecayRate(double hl) { set_halflife(hl); }
  void set_halflife(double hl) {
    k = ::log(.5) / hl;
  }
};

class DecayCounter {
 protected:
public:
  double val;		// value
  double delta;		// delta since last decay
  double vel;		// recent velocity
  ceph::real_time last_decay;	// time of last decay

 public:

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<DecayCounter*>& ls);

  DecayCounter(const ceph::real_time &now)
    : val(0), delta(0), vel(0), last_decay(now)
  {
  }

  // these two functions are for the use of our dencoder testing infrastructure
  DecayCounter() : val(0), delta(0), vel(0), last_decay() {}

  /**
   * reading
   */

  double get(ceph::real_time now, const DecayRate& rate) {
    decay(now, rate);
    return val;
  }

  double get_last() {
    return val;
  }

  double get_last_vel() {
    return vel;
  }

  ceph::real_time get_last_decay() {
    return last_decay;
  }

  /**
   * adjusting
   */

  double hit(ceph::real_time now, const DecayRate& rate, double v = 1.0) {
    decay(now, rate);
    delta += v;
    return val+delta;
  }

  void adjust(double a) {
    val += a;
  }
  void adjust(ceph::real_time now, const DecayRate& rate, double a) {
    decay(now, rate);
    val += a;
  }
  void scale(double f) {
    val *= f;
    delta *= f;
    vel *= f;
  }

  /**
   * decay etc.
   */

  void reset(ceph::real_time now) {
    last_decay = now;
    val = delta = 0;
  }

  void decay(ceph::real_time now, const DecayRate &rate);
};

inline void encode(const DecayCounter &c, bufferlist &bl) { c.encode(bl); }
inline void decode(DecayCounter &c, bufferlist::iterator &p) {
  c.decode(p);
}

#endif
