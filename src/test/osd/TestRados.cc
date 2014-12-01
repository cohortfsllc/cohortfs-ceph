// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/version.h"

#include <iostream>
#include <sstream>
#include <map>
#include <numeric>
#include <string>
#include <vector>
#include <stdlib.h>
#include <unistd.h>

#include "test/osd/RadosModel.h"


using namespace std;

class WeightedTestGenerator : public TestOpGenerator
{
public:

  WeightedTestGenerator(int ops,
			int objects,
			map<TestOpType, unsigned int> op_weights,
			TestOpStat *stats,
			int max_seconds) :
    m_nextop(NULL), m_op(0), m_ops(ops), m_seconds(max_seconds),
    m_objects(objects), m_stats(stats),
    m_total_weight(0)
  {
    m_start = time(0);
    for (map<TestOpType, unsigned int>::const_iterator it = op_weights.begin();
	 it != op_weights.end();
	 ++it) {
      m_total_weight += it->second;
      m_weight_sums.insert(pair<TestOpType, unsigned int>(it->first,
							  m_total_weight));
    }
  }

  TestOp *next(RadosTestContext &context)
  {
    TestOp *retval = NULL;

    ++m_op;
    if (m_op <= m_objects) {
      stringstream oid;
      oid << m_op;
      cout << m_op << ": write initial oid " << oid.str() << std::endl;
      context.oid_not_flushing.insert(oid.str());
      return new WriteOp(m_op, &context, oid.str(), false);
    } else if (m_op >= m_ops) {
      return NULL;
    }

    if (m_nextop) {
      retval = m_nextop;
      m_nextop = NULL;
      return retval;
    }

    while (retval == NULL) {
      unsigned int rand_val = rand() % m_total_weight;

      time_t now = time(0);
      if (m_seconds && now - m_start > m_seconds)
	break;

      for (map<TestOpType, unsigned int>::const_iterator it = m_weight_sums.begin();
	   it != m_weight_sums.end();
	   ++it) {
	if (rand_val < it->second) {
	  retval = gen_op(context, it->first);
	  break;
	}
      }
    }
    return retval;
  }

private:

  TestOp *gen_op(RadosTestContext &context, TestOpType type)
  {
    string oid, oid2;
    //cout << "oids not in use " << context.oid_not_in_use.size() << std::endl;
    assert(context.oid_not_in_use.size());

    switch (type) {
    case TEST_OP_READ:
      oid = *(rand_choose(context.oid_not_in_use));
      return new ReadOp(m_op, &context, oid, m_stats);

    case TEST_OP_WRITE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "write oid " << oid << std::endl;
      return new WriteOp(m_op, &context, oid, false, m_stats);

    case TEST_OP_DELETE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "delete oid " << oid << std::endl;
      return new DeleteOp(m_op, &context, oid, m_stats);

    case TEST_OP_SETATTR:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "setattr oid " << oid << std::endl;
      return new SetAttrsOp(m_op, &context, oid, m_stats);

    case TEST_OP_RMATTR:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "rmattr oid " << oid << std::endl;
      return new RemoveAttrsOp(m_op, &context, oid, m_stats);

    case TEST_OP_WATCH:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "watch oid " << oid << std::endl;
      return new WatchOp(m_op, &context, oid, m_stats);

    case TEST_OP_APPEND:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "append oid " << oid << std::endl;
      return new WriteOp(m_op, &context, oid, true, m_stats);

    default:
      cerr << m_op << ": Invalid op type " << type << std::endl;
      assert(0);
    }
  }

  TestOp *m_nextop;
  int m_op;
  int m_ops;
  int m_seconds;
  int m_objects;
  time_t m_start;
  TestOpStat *m_stats;
  map<TestOpType, unsigned int> m_weight_sums;
  unsigned int m_total_weight;
};

int main(int argc, char **argv)
{
  int ops = 1000;
  int objects = 50;
  int max_in_flight = 16;
  int64_t size = 4000000; // 4 MB
  int64_t min_stride_size = -1, max_stride_size = -1;
  int max_seconds = 0;

  struct {
    TestOpType op;
    const char *name;
  } op_types[] = {
    { TEST_OP_READ, "read"},
    { TEST_OP_WRITE, "write"},
    { TEST_OP_DELETE, "delete"},
    { TEST_OP_SETATTR, "setattr"},
    { TEST_OP_RMATTR, "rmattr"},
    { TEST_OP_WATCH, "watch"},
    { TEST_OP_APPEND, "append"},
    { TEST_OP_READ /* grr */, NULL },
  };

  map<TestOpType, unsigned int> op_weights;
  string volume_name = "data";
  bool no_omap = false;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--max-ops") == 0)
      ops = atoi(argv[++i]);
    else if (strcmp(argv[i], "--volume") == 0)
      volume_name = argv[++i];
    else if (strcmp(argv[i], "--max-seconds") == 0)
      max_seconds = atoi(argv[++i]);
    else if (strcmp(argv[i], "--objects") == 0)
      objects = atoi(argv[++i]);
    else if (strcmp(argv[i], "--max-in-flight") == 0)
      max_in_flight = atoi(argv[++i]);
    else if (strcmp(argv[i], "--size") == 0)
      size = atoi(argv[++i]);
    else if (strcmp(argv[i], "--min-stride-size") == 0)
      min_stride_size = atoi(argv[++i]);
    else if (strcmp(argv[i], "--max-stride-size") == 0)
      max_stride_size = atoi(argv[++i]);
    else if (strcmp(argv[i], "--no-omap") == 0)
      no_omap = true;
    else if (strcmp(argv[i], "--op") == 0) {
      i++;
      if (i == argc) {
	cerr << "Missing op after --op" << std::endl;
	return 1;
      }
      int j;
      for (j = 0; op_types[j].name; ++j) {
	if (strcmp(op_types[j].name, argv[i]) == 0) {
	  break;
	}
      }
      if (!op_types[j].name) {
	cerr << "unknown op " << argv[i] << std::endl;
	exit(1);
      }
      i++;
      if (i == argc) {
	cerr << "Weight unspecified." << std::endl;
	return 1;
      }
      int weight = atoi(argv[i]);
      if (weight < 0) {
	cerr << "Weights must be nonnegative." << std::endl;
	return 1;
      } else if (weight > 0) {
	cout << "adding op weight " << op_types[j].name << " -> " << weight << std::endl;
	op_weights.insert(pair<TestOpType, unsigned int>(op_types[j].op, weight));
      }
    } else {
      cerr << "unknown arg " << argv[i] << std::endl;
      //usage();
      exit(1);
    }
  }

  if (op_weights.empty()) {
    cerr << "No operations specified" << std::endl;
    //usage();
    exit(1);
  }

  if (min_stride_size < 0)
    min_stride_size = size / 10;
  if (max_stride_size < 0)
    max_stride_size = size / 5;

  cout << pretty_version_to_str() << std::endl;
  cout << "Configuration:" << std::endl
       << "\tNumber of operations: " << ops << std::endl
       << "\tNumber of objects: " << objects << std::endl
       << "\tMax in flight operations: " << max_in_flight << std::endl
       << "\tObject size (in bytes): " << size << std::endl
       << "\tWrite stride min: " << min_stride_size << std::endl
       << "\tWrite stride max: " << max_stride_size << std::endl;

  if (min_stride_size > max_stride_size) {
    cerr << "Error: min_stride_size cannot be more than max_stride_size"
	 << std::endl;
    return 1;
  }

  if (min_stride_size > size || max_stride_size > size) {
    cerr << "Error: min_stride_size and max_stride_size must be "
	 << "smaller than object size" << std::endl;
    return 1;
  }

  if (max_in_flight * 2 > objects) {
    cerr << "Error: max_in_flight must be <= than the number of objects / 2"
	 << std::endl;
    return 1;
  }

  char *id = getenv("CEPH_CLIENT_ID");
  RadosTestContext context(
    volume_name,
    max_in_flight,
    size,
    min_stride_size,
    max_stride_size,
    no_omap,
    id);

  TestOpStat stats;
  WeightedTestGenerator gen = WeightedTestGenerator(
    ops, objects,
    op_weights, &stats, max_seconds);
  int r = context.init();
  if (r < 0) {
    cerr << "Error initializing rados test context: "
	 << cpp_strerror(r) << std::endl;
    exit(1);
  }
  context.loop(&gen);

  context.shutdown();
  cerr << context.errors << " errors." << std::endl;
  cerr << stats << std::endl;
  return 0;
}
