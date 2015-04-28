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

#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>

#include <gtest/gtest.h>

#include "os/file/FileStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

static CephContext* cct;

typedef std::lock_guard<std::mutex> lock_guard;
typedef std::unique_lock<std::mutex> unique_lock;

void usage(const string &name) {
  std::cerr << "Usage: " << name << " [xattr|omap] store_path store_journal"
	    << std::endl;
}

const int THREADS = 5;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) ++retval;
  return retval;
}

class OnApplied : public Context {
public:
  std::mutex *lock;
  std::condition_variable *cond;
  int *in_progress;
  Transaction *t;
  OnApplied(std::mutex *lock,
	    std::condition_variable *cond,
	    int *in_progress,
	    Transaction *t)
    : lock(lock), cond(cond),
      in_progress(in_progress), t(t) {
    lock_guard l(*lock);
    (*in_progress)++;
  }

  void finish(int r) {
    lock_guard l(*lock);
    (*in_progress)--;
    cond->notify_all();
  }
};

uint64_t get_time() {
  time_t start;
  time(&start);
  return start * 1000;
}

double print_time(uint64_t ms) {
  return ((double)ms)/1000;
}

uint64_t do_run(ObjectStore *store, int attrsize, int numattrs,
		int run,
		int transsize, int ops,
		ostream &out) {
  std::mutex lock;
  std::condition_variable cond;
  int in_flight = 0;
  Transaction t;
  map<string, set<string>> collections;
  for (int i = 0; i < 3*THREADS; ++i) {
    stringstream coll_stream;
    coll_stream << "coll_" << i << "_" << run;
    const string coll_str = coll_stream.str();
    t.create_collection(coll_t(coll_str));
    set<string> objects;
    for (int i = 0; i < transsize; ++i) {
      stringstream obj_stream;
      obj_stream << i;
      const string obj_str = obj_stream.str();
      t.push_oid(hoid_t(oid_t(obj_str)));
      t.touch();
      objects.insert(obj_str);
    }
    collections[coll_str] = objects;
  }
  store->apply_transaction(t);

  bufferlist bl;
  for (int i = 0; i < attrsize; ++i) {
    bl.append('\0');
  }

  uint64_t start = get_time();
  for (int i = 0; i < ops; ++i) {
    {
      unique_lock l(lock);
      while (in_flight >= THREADS)
	cond.wait(l);
    }
    Transaction *t = new Transaction;
    auto c = rand_choose(collections);
    for (auto &oid : c->second) {
      uint16_t c_ix = t->push_cid(coll_t(c->first));
      uint16_t o_ix = t->push_oid(hoid_t(oid_t(oid)));
      for (int j = 0; j < numattrs; ++j) {
	stringstream ss;
	ss << i << ", " << j << ", " << oid;
	t->setattr(c_ix, o_ix, ss.str().c_str(), bl);
      }
    }
    store->queue_transaction(t, new OnApplied(&lock, &cond, &in_flight, t));
  }
  {
    unique_lock l(lock);
    while (in_flight)
      cond.wait(l);
  }
  return get_time() - start;
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  cct = global_init(0, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct);
  if (args[0] == string("omap")) {
    std::cerr << "using omap xattrs" << std::endl;
    cct->_conf->set_val("filestore_xattr_use_omap", "true");
  } else {
    std::cerr << "not using omap xattrs" << std::endl;
    cct->_conf->set_val("filestore_xattr_use_omap", "false");
  }
  cct->_conf->apply_changes(NULL);

  std::cerr << "args: " << args << std::endl;
  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  string store_path(args[1]);
  string store_dev(args[2]);

  boost::scoped_ptr<ObjectStore> store(new FileStore(cct, store_path, store_dev));

  std::cerr << "mkfs starting" << std::endl;
  assert(!store->mkfs());
  assert(!store->mount());
  std::cerr << "mounted" << std::endl;

  std::cerr << "attrsize\tnumattrs\ttranssize\tops\ttime" << std::endl;
  int runs = 0;
  int total_size = 11;
  for (int i = 6; i < total_size; ++i) {
    for (int j = (total_size - i); j >= 0; --j) {
      std::cerr << "starting run " << runs << std::endl;
      ++runs;
      uint64_t time = do_run(store.get(), (1 << i), (1 << j), runs,
			     10,
			     1000, std::cout);
      std::cout << (1 << i) << "\t"
		<< (1 << j) << "\t"
		<< 10 << "\t"
		<< 1000 << "\t"
		<< print_time(time) << std::endl;
    }
  }
  store->umount();
  return 0;
}
