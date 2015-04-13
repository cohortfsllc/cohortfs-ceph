// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OBJ_BENCHER_H
#define CEPH_OBJ_BENCHER_H

#include <mutex>

#include "common/config.h"
#include "common/ceph_context.h"

struct bench_interval_data {
  double min_bandwidth;
  double max_bandwidth;
};

struct bench_history {
  vector<double> bandwidth;
  vector<ceph::timespan> latency;
};

struct bench_data {
  bool done; //is the benchmark is done
  int object_size; //the size of the objects
  int trans_size; //size of the write/read to perform
  // same as object_size for write tests
  int in_flight; //number of reads/writes being waited on
  int started;
  int finished;
  ceph::timespan min_latency;
  ceph::timespan max_latency;
  ceph::timespan avg_latency;
  struct bench_interval_data idata; // data that is updated by time intervals and not by events
  struct bench_history history; // data history, used to calculate stddev
  ceph::timespan cur_latency; //latency of last completed transaction
  ceph::mono_time start_time; //start time for benchmark
  char *object_contents; //pointer to the contents written to each object
};

const int OP_WRITE     = 1;
const int OP_SEQ_READ  = 2;
const int OP_RAND_READ = 3;

class ObjBencher {
  bool show_time;
public:
  CephContext *cct;
protected:
  std::mutex lock;
  typedef std::unique_lock<std::mutex> unique_lock;
  typedef std::lock_guard <std::mutex> lock_guard;

  static void *status_printer(void *bencher);

  struct bench_data data;

  int fetch_bench_metadata(const std::string& metadata_file, int* object_size,
			   int* num_objects, int* prevPid);

  int write_bench(int secondsToRun, int maxObjects, int concurrentios,
		  const string& run_name_meta);
  int seq_read_bench(int secondsToRun, int concurrentios, int num_objects,
		     int writePid);
  int rand_read_bench(int secondsToRun, int num_objects, int concurrentios,
		      int writePid);

  int clean_up(int num_objects, int prevPid, int concurrentios);

  virtual int completions_init(int concurrentios) = 0;
  virtual void completions_done() = 0;

  virtual int create_completion(int i, void (*cb)(void *, void*), void *arg) = 0;
  virtual void release_completion(int slot) = 0;

  virtual bool completion_is_done(int slot) = 0;
  virtual int completion_wait(int slot) = 0;
  virtual int completion_ret(int slot) = 0;

  virtual int aio_read(const std::string& oid_t, int slot, bufferlist *pbl,
		       size_t len) = 0;
  virtual int aio_write(const std::string& oid_t, int slot, bufferlist& bl,
			size_t len) = 0;
  virtual int aio_remove(const std::string& oid_t, int slot) = 0;
  virtual int sync_read(const std::string& oid_t, bufferlist& bl,
			size_t len) = 0;
  virtual int sync_write(const std::string& oid_t, bufferlist& bl,
			 size_t len) = 0;
  virtual int sync_remove(const std::string& oid_t) = 0;

  ostream& out(ostream& os);
  ostream& out(ostream& os, const ceph::real_time& t);
public:
  ObjBencher(CephContext *cct_) : show_time(false), cct(cct_) {}
  virtual ~ObjBencher() {}
  int aio_bench(
    int operation, int secondsToRun, int maxObjectsToCreate,
    int concurrentios, int op_size, bool cleanup, const string& run_name);
  int clean_up(const string& prefix, int concurrentios, const string& run_name);

  void set_show_time(bool dt) {
    show_time = dt;
  }
};


#endif
