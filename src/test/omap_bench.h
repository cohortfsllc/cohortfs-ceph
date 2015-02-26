// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Generate latency statistics for a configurable number of object map write
 * operations of configurable size.
 *
 *  Created on: May 21, 2012
 *      Author: Eleanor Cawthon
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef OMAP_BENCH_HPP_
#define OMAP_BENCH_HPP_

#include <mutex>
#include <condition_variable>
#include "include/rados/librados.hpp"
#include <string>
#include <map>
#include <cfloat>

using ceph::bufferlist;

struct o_bench_data {
  ceph::timespan avg_latency;
  ceph::timespan min_latency;
  ceph::timespan max_latency;
  ceph::timespan total_latency;
  int started_ops;
  int completed_ops;
  std::map<ceph::timespan,int> freq_map;
  pair<ceph::timespan,int> mode;
  o_bench_data()
    : avg_latency(0ns), min_latency(ceph::timespan::max()),
      max_latency(0ns), total_latency(0ns),
      started_ops(0), completed_ops(0)
  {}
};

class OmapBench;

typedef int (*omap_generator_t)(const int omap_entries, const int key_size,
				const int value_size,
				std::map<std::string,bufferlist> *out_omap);
typedef int (OmapBench::*test_t)(omap_generator_t omap_gen);


class Writer{
protected:
  string oid_t;
  ceph::mono_time begin_time;
  ceph::mono_time end_time;
  std::map<std::string,bufferlist> omap;
  OmapBench *ob;
  friend class OmapBench;
public:
  Writer(OmapBench *omap_bench);
  virtual ~Writer(){};
  virtual void start_time();
  virtual void stop_time();
  virtual ceph::timespan get_time();
  virtual string get_oid();
  virtual std::map<std::string,bufferlist> & get_omap();
};

class AioWriter : public Writer{
protected:
  librados::AioCompletion * aioc;
  friend class OmapBench;

public:
  AioWriter(OmapBench *omap_bench);
  ~AioWriter();
  virtual librados::AioCompletion * get_aioc();
  virtual void set_aioc(librados::callback_t complete,
      librados::callback_t safe);
};

class OmapBench{
protected:
  librados::IoCtx io_ctx;
  librados::Rados rados;
  struct o_bench_data data;
  test_t test;
  omap_generator_t omap_generator;

  //aio things
  std::condition_variable thread_is_free;
  std::mutex thread_is_free_lock;
  std::mutex data_lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  int busythreads_count;
  librados::callback_t comp;
  librados::callback_t safe;

  string pool_name;
  string rados_id;
  string prefix;
  int threads;
  int objects;
  int entries_per_omap;
  int key_size;
  int value_size;
  int increment;

  friend class Writer;
  friend class AioWriter;

public:
  OmapBench()
    : test(&OmapBench::test_write_objects_in_parallel),
      omap_generator(generate_uniform_omap),
      busythreads_count(0),
      comp(NULL), safe(aio_is_safe),
      pool_name("data"),
      rados_id("admin"),
      prefix(rados_id+".oid."),
      threads(3), objects(100), entries_per_omap(10), key_size(10),
      value_size(100), increment(10)
  {}
  /**
   * Parses command line args, initializes rados and ioctx
   */
  int setup(int argc, const char** argv);

  /**
   * Callback for when an AioCompletion (called from an AioWriter)
   * is safe. deletes the AioWriter that called it,
   * Updates data, updates busythreads, and signals thread_is_free.
   *
   * @param c provided by aio_write - not used
   * @param arg the AioWriter that contains this AioCompletion
   */
  static void aio_is_safe(rados_completion_t c, void *arg);

  /**
   * Generates a random string len characters long
   */
  static string random_string(int len);

  /*
   * runs the test specified by test using the omap generator specified by
   * omap_generator
   *
   * @return error code
   */
  int run();

  /*
   * Prints all keys and values for all omap entries for all objects
   */
  int print_written_omap();

  /*
   * Displays relevant constants and the histogram generated through a test
   */
  void print_results();

  /**
   * Writes an object with the specified AioWriter.
   *
   * @param aiow the AioWriter to write with
   * @param omap the omap to write
   * @post: an asynchronous omap_set is launched
   */
  int write_omap_asynchronously(AioWriter *aiow,
      const std::map<std::string,bufferlist> &map);


  /**
   * Generates an omap with omap_entries entries, each with keys key_size
   * characters long and with string values value_size characters long.
   *
   * @param out_map pointer to the map to be created
   * @return error code
   */
  static int generate_uniform_omap(const int omap_entries, const int key_size,
      const int value_size, std::map<std::string,bufferlist> * out_omap);

  /**
   * The same as generate_uniform_omap except that string lengths are picked
   * randomly between 1 and the int arguments
   */
  static int generate_non_uniform_omap(const int omap_entries,
      const int key_size,
      const int value_size, std::map<std::string,bufferlist> * out_omap);

  static int generate_small_non_random_omap(const int omap_entries,
      const int key_size, const int value_size,
      std::map<std::string,bufferlist> * out_omap);

  /*
   * Uses aio_write to write omaps generated by omap_gen to OBJECTS objects
   * using THREADS AioWriters at a time.
   *
   * @param omap_gen the method used to generate the omaps.
   */
  int test_write_objects_in_parallel(omap_generator_t omap_gen);

};



#endif /* OMAP_BENCH_HPP_ */

