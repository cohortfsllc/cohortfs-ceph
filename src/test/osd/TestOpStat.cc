// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>
#include "RadosModel.h"
#include "TestOpStat.h"

void TestOpStat::begin(TestOp *in) {
  unique_lock sl(stat_lock);
  stats[in->getType()].begin(in);
  sl.unlock();
}

void TestOpStat::end(TestOp *in) {
  unique_lock sl(stat_lock);
  stats[in->getType()].end(in);
  sl.unlock();
}

void TestOpStat::TypeStatus::export_latencies(
  map<double, ceph::timespan> &in) const
{
  auto i = in.begin();
  auto j = latencies.begin();
  int count = 0;
  while (j != latencies.end() && i != in.end()) {
    count++;
    if ((((double)count)/((double)latencies.size())) * 100 >= i->first) {
      i->second = *j;
      ++i;
    }
    ++j;
  }
}

std::ostream & operator<<(std::ostream &out, TestOpStat &rhs)
{
  TestOpStat::unique_lock sl(rhs.stat_lock);
  for (map<string,TestOpStat::TypeStatus>::iterator i = rhs.stats.begin();
       i != rhs.stats.end();
       ++i) {
    map<double, ceph::timespan> latency;
    latency[10] = 0ns;
    latency[50] = 0ns;
    latency[90] = 0ns;
    latency[99] = 0ns;
    i->second.export_latencies(latency);

    out << i->first << " latency: " << std::endl;
    for (auto j = latency.begin(); j != latency.end(); ++j) {
      if (j->second == 0ns) break;
      out << "\t" << j->first << "th percentile: "
	  << j->second / 1000 << "ms" << std::endl;
    }
  }
  sl.unlock();
  return out;
}
