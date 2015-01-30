// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_time.h"
#include "include/types.h"
#include "common/Thread.h"
#include "common/debug.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

using std::cout;
using std::cerr;

static CephContext* cct;

struct T : public Thread {
  int num;
  set<int> myset;
  map<int,string> mymap;
  T(int n) : num(n) {
    myset.insert(123);
    myset.insert(456);
    mymap[1] = "foo";
    mymap[10] = "bar";
  }

  void *entry() {
    while (num-- > 0)
      generic_dout(0) << "this is a typical log line.  set "
		      << myset << " and map " << mymap << dendl;
    return 0;
  }
};

int main(int argc, const char **argv)
{
  int threads = atoi(argv[1]);
  int num = atoi(argv[2]);

  cout << threads << " threads, " << num << " lines per thread" << std::endl;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  cct = global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, 0);

  auto start = ceph::mono_clock::now();

  list<T*> ls;
  for (int i=0; i<threads; i++) {
    T *t = new T(num);
    t->create();
    ls.push_back(t);
  }

  for (int i=0; i<threads; i++) {
    T *t = ls.front();
    ls.pop_front();
    t->join();
    delete t;
  }

  ceph::timespan t = ceph::mono_clock::now() - start;
  cout << " flushing.. " << t << " so far ..." << std::endl;

  cct->_log->flush();

  auto end = ceph::mono_clock::now();
  ceph::timespan dur = end - start;

  cout << dur << std::endl;
  return 0;
}
