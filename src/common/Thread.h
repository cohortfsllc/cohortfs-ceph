 // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_THREAD_H
#define CEPH_THREAD_H

#include <pthread.h>

class Thread {
 private:
  pthread_t thread_id;

 public:
  Thread(const Thread& other);
  const Thread& operator=(const Thread& other);

  Thread();
  virtual ~Thread();

 protected:
  virtual void *entry() = 0;

 private:
  static void *_entry_func(void *arg);

 public:
  const pthread_t &get_thread_id();
  bool is_started();
  bool am_self();
  int kill(int signal);
  int try_create(size_t stacksize);
  void create(size_t stacksize = 0);
  void set_affinity(cpu_set_t &cpuset);
  int join(void **prval = 0);
  int detach();
};

#endif
