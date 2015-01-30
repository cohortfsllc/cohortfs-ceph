// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <mutex>
#include "include/ceph_time.h"
#include "include/rados/librados.hpp"

#ifndef TESTOPSTAT_H
#define TESTOPSTAT_H

class TestOp;

class TestOpStat {
public:
  std::mutex stat_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;

  TestOpStat()	{}

  class TypeStatus {
  public:
    map<TestOp*,ceph::mono_time> inflight;
    std::multiset<ceph::timespan> latencies;
    void export_latencies(map<double, ceph::timespan> &in) const;

    void begin(TestOp *in)
    {
      assert(!inflight.count(in));
      inflight[in] = ceph::mono_clock::now();
    }

    void end(TestOp *in)
    {
      assert(inflight.count(in));
      latencies.insert(ceph::mono_clock::now() - inflight[in]);
      inflight.erase(in);
    }
  };
  map<string,TypeStatus> stats;

  void begin(TestOp *in);
  void end(TestOp *in);
  friend std::ostream & operator<<(std::ostream &, TestOpStat&);
};

std::ostream & operator<<(std::ostream &out, TestOpStat &rhs);

#endif
