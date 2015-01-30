// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef DETAILEDSTATCOLLECTERH
#define DETAILEDSTATCOLLECTERH

#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <ostream>

#include <boost/tuple/tuple.hpp>
#include <boost/scoped_ptr.hpp>

#include "include/ceph_time.h"
#include "stat_collector.h"
#include "common/Formatter.h"

class DetailedStatCollector : public StatCollector {
public:
  class AdditionalPrinting {
  public:
    virtual void operator()(std::ostream *) = 0;
    virtual ~AdditionalPrinting() {}
  };
private:
  struct Op {
    std::string type;
    ceph::mono_time start;
    ceph::timespan latency;
    uint64_t size;
    uint64_t seq;
    Op(
      std::string type,
      ceph::mono_time start,
      ceph::timespan latency,
      uint64_t size,
      uint64_t seq)
      : type(type), start(start), latency(latency),
	size(size), seq(seq) {}
    void dump(ostream *out, Formatter *f);
  };
  class Aggregator {
    uint64_t recent_size;
    uint64_t total_size;
    ceph::timespan recent_latency;
    ceph::timespan total_latency;
    ceph::mono_time last;
    ceph::mono_time first;
    uint64_t recent_ops;
    uint64_t total_ops;
    bool started;
  public:
    Aggregator();

    void add(const Op &op);
    void dump(Formatter *f);
  };
  const ceph::timespan bin_size;
  boost::scoped_ptr<Formatter> f;
  ostream *out;
  ostream *summary_out;
  boost::scoped_ptr<AdditionalPrinting> details;
  ceph::mono_time last_dump;

  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  std::mutex lock;
  std::condition_variable cond;

  std::map<std::string, Aggregator> aggregators;

  std::map<uint64_t, std::pair<uint64_t, ceph::mono_time> > not_applied;
  std::map<uint64_t, std::pair<uint64_t, ceph::mono_time> > not_committed;
  std::map<uint64_t, std::pair<uint64_t, ceph::mono_time> > not_read;

  uint64_t cur_seq;

  void dump(
    const std::string &type,
    boost::tuple<ceph::mono_time, ceph::mono_time, uint64_t, uint64_t> stuff);
public:
  DetailedStatCollector(
    ceph::timespan bin_size,
    Formatter *formatter,
    ostream *out,
    ostream *summary_out,
    AdditionalPrinting *details = 0
    );

  uint64_t next_seq();
  void start_write(uint64_t seq, uint64_t size);
  void start_read(uint64_t seq, uint64_t size);
  void write_applied(uint64_t seq);
  void write_committed(uint64_t seq);
  void read_complete(uint64_t seq);

};

#endif
