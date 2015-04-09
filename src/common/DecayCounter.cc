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
 * Foundation.  See file COPYING.
 *
 */

#include "DecayCounter.h"
#include "Formatter.h"
#include "include/encoding.h"

void DecayCounter::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(val, bl);
  ::encode(delta, bl);
  ENCODE_FINISH(bl);
}

void DecayCounter::decode(bufferlist::iterator &p)
{
  DECODE_START(1, p);
  if (struct_v < 2) {
    double half_life;
    ::decode(half_life, p);
  }
  if (struct_v < 3) {
    double k;
    ::decode(k, p);
  }
  ::decode(val, p);
  ::decode(delta, p);
  ::decode(vel, p);
  DECODE_FINISH(p);
}

void DecayCounter::dump(Formatter *f) const
{
  f->dump_float("value", val);
  f->dump_float("delta", delta);
  f->dump_float("velocity", vel);
}

void DecayCounter::generate_test_instances(std::list<DecayCounter*>& ls)
{
  DecayCounter *counter = new DecayCounter(ceph::real_clock::now());
  counter->val = 3.0;
  counter->delta = 2.0;
  counter->vel = 1.0;
  ls.push_back(counter);
  counter = new DecayCounter(ceph::real_clock::now());
  ls.push_back(counter);
}

void DecayCounter::decay(ceph::real_time now, const DecayRate &rate)
{
  std::chrono::duration<double> el = now - last_decay;

  if (el >= 1s) {
    // calculate new value
    double newval = (val+delta) * exp(el.count() * rate.k);
    if (newval < .01)
      newval = 0.0;

    // calculate velocity approx
    vel += (newval - val) * el.count();
    vel *= exp(el.count() * rate.k);

    val = newval;
    delta = 0;
    last_decay = now;
  }
}
